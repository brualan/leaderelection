//go:build integration

// The integration suite talks to a real MySQL, both directly and through
// ProxySQL. Run it with:
//
//	make test-int
//
// or point it at your own database:
//
//	LEADERELECTION_MYSQL_DSN=user:pass@tcp(host:3306)/db go test -tags=integration ./mysql/...
//
// Deliberately no testcontainers: keeping the driver module's dependencies to
// the MySQL driver alone matters more for a library than the convenience does,
// and the containers are one docker compose away.
package mysql_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	godriver "github.com/go-sql-driver/mysql"

	"github.com/brualan/leaderelection"
	"github.com/brualan/leaderelection/mysql"
	"github.com/brualan/leaderelection/storetest"
)

// Environment variables naming the two paths to the database. A missing direct
// DSN skips the suite; a missing pooler DSN merely drops that route, because a
// developer pointing this at their own MySQL usually has no pooler in front of
// it -- but CI always sets both.
const (
	envDirect = "LEADERELECTION_MYSQL_DSN"
	envPooled = "LEADERELECTION_PROXYSQL_DSN"
)

// route is one way of reaching the database.
type route struct {
	name string
	dsn  string
}

func routes(t testing.TB) []route {
	t.Helper()

	direct := os.Getenv(envDirect)
	if direct == "" {
		t.Skipf("%s is not set; run `make test-int` or point it at a database", envDirect)
	}
	out := []route{{name: "direct", dsn: direct}}
	if pooled := os.Getenv(envPooled); pooled != "" {
		out = append(out, route{name: "proxysql", dsn: pooled})
	}
	return out
}

func open(t testing.TB, r route, opts ...mysql.Option) *mysql.Store {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	store, err := mysql.Connect(ctx, r.dsn, opts...)
	if err != nil {
		t.Fatalf("Connect(%s): %v", r.name, err)
	}
	t.Cleanup(store.Close)

	if err := store.EnsureSchema(ctx); err != nil {
		t.Fatalf("EnsureSchema(%s): %v", r.name, err)
	}
	return store
}

// TestAcceptance is the whole point of the pooler route: the identical suite,
// with no exemptions, over a connection the pooler may move between backends.
func TestAcceptance(t *testing.T) {
	for _, r := range routes(t) {
		t.Run(r.name, func(t *testing.T) {
			t.Parallel()
			storetest.Run(t, storetest.Suite{Store: open(t, r)})
		})
	}
}

// The store reads timestamps whichever way the DSN told the driver to deliver
// them, and an application that set parseTime for its own queries must not get
// a different election because of it.
func TestAcceptanceWithParseTime(t *testing.T) {
	r := routes(t)[0]
	separator := "?"
	if strings.Contains(r.dsn, "?") {
		separator = "&"
	}
	storetest.Run(t, storetest.Suite{
		Store: open(t, route{name: r.name, dsn: r.dsn + separator + "parseTime=true&loc=Local"}),
	})
}

// EnsureSchema runs on every replica on every start, so it has to tolerate
// running concurrently with itself.
func TestEnsureSchemaIsIdempotentUnderRace(t *testing.T) {
	for _, r := range routes(t) {
		t.Run(r.name, func(t *testing.T) {
			ctx := context.Background()
			var wg sync.WaitGroup
			errs := make(chan error, 8)
			for range 8 {
				wg.Add(1)
				go func() {
					defer wg.Done()
					store := open(t, r)
					if err := store.EnsureSchema(ctx); err != nil {
						errs <- err
					}
				}()
			}
			wg.Wait()
			close(errs)
			for err := range errs {
				// A concurrent CREATE TABLE IF NOT EXISTS can lose a race with
				// itself and report a duplicate; that is still success from the
				// caller's point of view, and must not surface.
				t.Fatalf("concurrent EnsureSchema: %v", err)
			}
		})
	}
}

// The timestamps in a Lease must come from the server, not from this process. A
// client clock hours away from the server's must change nothing.
//
// Unlike the PostgreSQL store, the expiry and the "now" it is measured against
// are read by two statements, because MySQL cannot return a row from a write.
// So the TTL is a little short of what was asked for rather than exactly it,
// and short is the safe direction: the elector's local deadline stays inside
// the lease the database actually granted.
func TestTimestampsIgnoreTheClientClock(t *testing.T) {
	for _, r := range routes(t) {
		t.Run(r.name, func(t *testing.T) {
			store := open(t, r)
			ctx := context.Background()
			name := uniqueName(t)

			lease, err := store.Acquire(ctx, name, "a", 30*time.Second, nil)
			if err != nil {
				t.Fatalf("Acquire: %v", err)
			}
			if ttl := lease.TTL(); ttl <= 29*time.Second || ttl > 30*time.Second {
				t.Fatalf("TTL = %v, want just under 30s", ttl)
			}

			// The server's clock and this process's clock are the same machine
			// here, so the check that means something is internal consistency:
			// every timestamp in the row has to come from one clock.
			if lease.Now.Before(lease.AcquiredAt) {
				t.Fatalf("now %v precedes acquired_at %v", lease.Now, lease.AcquiredAt)
			}
			if !lease.RenewedAt.Equal(lease.AcquiredAt) {
				t.Fatalf("acquired_at %v and renewed_at %v differ on a fresh term; "+
					"one statement wrote them both", lease.AcquiredAt, lease.RenewedAt)
			}
			// Microsecond precision is what makes a sub-second lease expressible
			// at all; a column defined without it would round every timestamp to
			// the second and quietly extend or retire leases.
			if lease.ExpiresAt.Sub(lease.AcquiredAt) != 30*time.Second {
				t.Fatalf("expires_at %v is not exactly 30s after acquired_at %v; "+
					"the column has lost sub-second precision", lease.ExpiresAt, lease.AcquiredAt)
			}
		})
	}
}

// MySQL evaluates the assignments in an UPDATE from left to right, and lets a
// later one see what an earlier one wrote. The acquire statement is built on
// that, so it is worth asserting against the server rather than trusting the
// manual -- a version that changed it would otherwise show up as an election
// that quietly stops handing over.
func TestAssignmentOrderIsLeftToRight(t *testing.T) {
	for _, r := range routes(t) {
		t.Run(r.name, func(t *testing.T) {
			ctx := context.Background()
			db := openDB(t, r)

			table := fmt.Sprintf("assignment_order_%d", time.Now().UnixNano())
			if _, err := db.ExecContext(ctx,
				"CREATE TABLE `"+table+"` (a BIGINT NOT NULL, b BIGINT NOT NULL)"); err != nil {
				t.Fatalf("create: %v", err)
			}
			t.Cleanup(func() {
				_, _ = db.ExecContext(context.Background(), "DROP TABLE `"+table+"`")
			})

			if _, err := db.ExecContext(ctx, "INSERT INTO `"+table+"` (a, b) VALUES (1, 0)"); err != nil {
				t.Fatalf("insert: %v", err)
			}
			if _, err := db.ExecContext(ctx, "UPDATE `"+table+"` SET a = a + 1, b = a"); err != nil {
				t.Fatalf("update: %v", err)
			}

			var a, b int64
			if err := db.QueryRowContext(ctx, "SELECT a, b FROM `"+table+"`").Scan(&a, &b); err != nil {
				t.Fatalf("select: %v", err)
			}
			if a != 2 || b != 2 {
				t.Fatalf("after `SET a = a + 1, b = a` the row is (a=%d, b=%d), want (2, 2). "+
					"This server does not let a later assignment see an earlier one, and the "+
					"acquire statement depends on it doing so; see the comment on acquireSQL.", a, b)
			}
		})
	}
}

// Connect turns off server-side prepared statements, because a prepared
// statement lives on one backend connection and a pooler is free to hand the
// next statement to another. Counting them on the server is the only check that
// would notice a regression here.
func TestConnectLeavesNoPreparedStatements(t *testing.T) {
	ctx := context.Background()
	r := routes(t)[0]
	admin := openDB(t, r)

	before := preparedStatementCount(t, admin)

	store := open(t, r)
	name := uniqueName(t)
	lease, err := store.Acquire(ctx, name, "a", time.Minute, nil)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	for range 5 {
		if lease, err = store.Renew(ctx, name, "a", lease.Fence, time.Minute, nil); err != nil {
			t.Fatalf("Renew: %v", err)
		}
	}

	if after := preparedStatementCount(t, admin); after > before {
		t.Fatalf("the server prepared %d statements for an election that ran %d operations; "+
			"a prepared statement belongs to one backend connection and a pooler will not keep it",
			after-before, 6)
	}
}

func preparedStatementCount(t *testing.T, db *sql.DB) int64 {
	t.Helper()
	var name string
	var value int64
	err := db.QueryRowContext(context.Background(),
		"SHOW GLOBAL STATUS LIKE 'Com_stmt_prepare'").Scan(&name, &value)
	if err != nil {
		t.Fatalf("reading Com_stmt_prepare: %v", err)
	}
	return value
}

// A pooler is free to give the next statement to a different backend. Anything
// session-scoped -- a prepared statement above all -- breaks here and nowhere
// else.
func TestSurvivesConnectionChurn(t *testing.T) {
	for _, r := range routes(t) {
		t.Run(r.name, func(t *testing.T) {
			store := open(t, r)
			ctx := context.Background()
			name := uniqueName(t)

			lease, err := store.Acquire(ctx, name, "a", time.Minute, nil)
			if err != nil {
				t.Fatalf("Acquire: %v", err)
			}
			// Enough iterations to cycle through every backend connection in the
			// pooler's small pool several times over.
			for i := range 60 {
				if lease, err = store.Renew(ctx, name, "a", lease.Fence, time.Minute, nil); err != nil {
					t.Fatalf("Renew #%d: %v", i, err)
				}
				if _, err := store.Inspect(ctx, name); err != nil {
					t.Fatalf("Inspect #%d: %v", i, err)
				}
			}
		})
	}
}

// Many participants renewing and contending at once is the steady state of a
// real deployment, not an edge case. On MySQL it is also where the upsert's row
// lock does its work: two participants that both saw an expired lease arrive
// here at the same instant.
func TestConcurrentElectionsUnderLoad(t *testing.T) {
	for _, r := range routes(t) {
		t.Run(r.name, func(t *testing.T) {
			store := open(t, r)
			ctx := context.Background()

			const elections, participants = 4, 8
			var wg sync.WaitGroup
			winners := make([]sync.Map, elections)

			for e := range elections {
				name := fmt.Sprintf("%s-%d", uniqueName(t), e)
				for p := range participants {
					wg.Add(1)
					go func() {
						defer wg.Done()
						identity := fmt.Sprintf("p-%d", p)
						lease, err := store.Acquire(ctx, name, identity, 30*time.Second, nil)
						if err != nil {
							t.Errorf("Acquire: %v", err)
							return
						}
						if lease.HeldBy(identity) {
							winners[e].Store(identity, lease.Fence)
						}
					}()
				}
			}
			wg.Wait()

			for e := range elections {
				count := 0
				winners[e].Range(func(_, _ any) bool { count++; return true })
				if count != 1 {
					t.Fatalf("election %d had %d simultaneous leaders", e, count)
				}
			}
		})
	}
}

// An identity is opaque bytes, and MySQL's default collations are not: they
// fold case, ignore accents and pad trailing spaces. A store that let any of
// those through would treat two participants as one, and the guard on every
// renewal would be looser than identity.
func TestIdentitiesThatDifferOnlyInCaseOrSpacing(t *testing.T) {
	for _, r := range routes(t) {
		t.Run(r.name, func(t *testing.T) {
			store := open(t, r)
			ctx := context.Background()

			for _, pair := range [][2]string{
				{"worker-a", "worker-A"},
				{"worker-a", "worker-a "},
				{"resume", "résumé"},
			} {
				name := uniqueName(t)
				held, err := store.Acquire(ctx, name, pair[0], time.Minute, nil)
				if err != nil {
					t.Fatalf("Acquire(%q): %v", pair[0], err)
				}
				// The impostor must lose the election...
				other, err := store.Acquire(ctx, name, pair[1], time.Minute, nil)
				if err != nil {
					t.Fatalf("Acquire(%q): %v", pair[1], err)
				}
				if other.HeldBy(pair[1]) {
					t.Fatalf("%q was granted a lease held by %q", pair[1], pair[0])
				}
				// ...and must not be able to renew or release the real one's
				// term, which is the failure that actually produces two leaders.
				if _, err := store.Renew(ctx, name, pair[1], held.Fence, time.Minute, nil); !errors.Is(err, leaderelection.ErrLeaseLost) {
					t.Fatalf("Renew as %q against %q's lease: got %v, want ErrLeaseLost", pair[1], pair[0], err)
				}
				if err := store.Release(ctx, name, pair[1], held.Fence); err != nil {
					t.Fatalf("Release as %q: %v", pair[1], err)
				}
				if after, err := store.Inspect(ctx, name); err != nil {
					t.Fatalf("Inspect: %v", err)
				} else if !after.HeldBy(pair[0]) {
					t.Fatalf("%q released a term belonging to %q", pair[1], pair[0])
				}
			}
		})
	}
}

// A custom table must be honoured everywhere, including in the DDL, and must
// not collide with the default one.
func TestCustomTable(t *testing.T) {
	for _, r := range routes(t) {
		t.Run(r.name, func(t *testing.T) {
			ctx := context.Background()
			custom := open(t, r, mysql.WithTable("", "custom_leader_election"))
			shared := open(t, r)

			name := uniqueName(t)
			if _, err := custom.Acquire(ctx, name, "a", time.Minute, nil); err != nil {
				t.Fatalf("Acquire on the custom table: %v", err)
			}
			// The default table knows nothing about it.
			if _, err := shared.Inspect(ctx, name); !errors.Is(err, leaderelection.ErrNoLease) {
				t.Fatalf("Inspect on the default table: got %v, want ErrNoLease", err)
			}
		})
	}
}

// A missing table is a deployment error, and must be reported as one rather
// than as a lost election.
func TestMissingTableIsFatal(t *testing.T) {
	for _, r := range routes(t) {
		t.Run(r.name, func(t *testing.T) {
			ctx := context.Background()
			store, err := mysql.Connect(ctx, r.dsn, mysql.WithTable("", "no_such_election_table"))
			if err != nil {
				t.Fatalf("Connect: %v", err)
			}
			t.Cleanup(store.Close)

			_, err = store.Inspect(ctx, "whatever")
			if err == nil {
				t.Fatal("Inspect against a missing table succeeded")
			}
			if !mysql.Fatal(err) {
				t.Fatalf("Fatal(%v) = false, want true", err)
			}
			if mysql.Retryable(err) {
				t.Fatalf("Retryable(%v) = true; retrying will never help", err)
			}
			// And the write half must be just as loud: reporting a broken
			// deployment as a lost election is how an outage becomes silence.
			if _, err := store.Acquire(ctx, "whatever", "a", time.Minute, nil); !mysql.Fatal(err) {
				t.Fatalf("Acquire against a missing table: Fatal(%v) = false", err)
			}
		})
	}
}

// A closed handle stands in for the database becoming unreachable. Every
// operation must report failure rather than inventing a lease.
func TestClosedHandleFailsEveryOperation(t *testing.T) {
	r := routes(t)[0]
	ctx := context.Background()

	store, err := mysql.Connect(ctx, r.dsn)
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	if err := store.EnsureSchema(ctx); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}
	name := uniqueName(t)
	lease, err := store.Acquire(ctx, name, "a", time.Minute, nil)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	store.Close()

	if _, err := store.Inspect(ctx, name); err == nil {
		t.Fatal("Inspect on a closed handle succeeded")
	}
	if got, err := store.Acquire(ctx, name, "a", time.Minute, nil); err == nil && got.HeldBy("a") {
		t.Fatal("Acquire on a closed handle reported a held lease")
	}
	if _, err := store.Renew(ctx, name, "a", lease.Fence, time.Minute, nil); err == nil {
		t.Fatal("Renew on a closed handle succeeded")
	} else if errors.Is(err, leaderelection.ErrLeaseLost) {
		t.Fatal("an unreachable database was reported as a lost lease; " +
			"the two demand different operator responses")
	}
	if err := store.Release(ctx, name, "a", lease.Fence); err == nil {
		t.Fatal("Release on a closed handle succeeded")
	}
}

// New wraps a handle the application already owns, and must leave it open: an
// application that also uses that handle for its own work would otherwise find
// it closed when the election shut down.
func TestNewBorrowsTheCallersHandle(t *testing.T) {
	r := routes(t)[0]
	ctx := context.Background()

	db := openDB(t, r)

	store, err := mysql.New(db, mysql.WithTable("", mysql.DefaultTable))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := store.EnsureSchema(ctx); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}

	name := uniqueName(t)
	lease, err := store.Acquire(ctx, name, "a", time.Minute, nil)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	if !lease.HeldBy("a") {
		t.Fatal("Acquire over a borrowed handle did not take the lease")
	}

	store.Close()
	if _, err := db.ExecContext(ctx, "SELECT 1"); err != nil {
		t.Fatalf("the borrowed handle was closed by Store.Close: %v", err)
	}
}

// The whole acceptance suite over a handle the caller built, with none of the
// settings Connect applies. This is the deployment where an application hands
// the election its existing *sql.DB, and the thing most likely to break in it
// is the timestamps: without parseTime the driver returns raw bytes.
func TestAcceptanceOverABorrowedHandle(t *testing.T) {
	r := routes(t)[0]
	db := openDB(t, r)

	store, err := mysql.New(db)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := store.EnsureSchema(context.Background()); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}
	storetest.Run(t, storetest.Suite{Store: store})
}

// openDB returns a plain *sql.DB with nothing this package configures, for the
// tests that need to look at the server or to hand the store somebody else's
// handle.
func openDB(t testing.TB, r route) *sql.DB {
	t.Helper()
	connector, err := godriver.NewConnector(mustParseDSN(t, r.dsn))
	if err != nil {
		t.Fatalf("NewConnector: %v", err)
	}
	db := sql.OpenDB(connector)
	t.Cleanup(func() { _ = db.Close() })
	if err := db.PingContext(context.Background()); err != nil {
		t.Fatalf("Ping(%s): %v", r.name, err)
	}
	return db
}

func mustParseDSN(t testing.TB, dsn string) *godriver.Config {
	t.Helper()
	cfg, err := godriver.ParseDSN(dsn)
	if err != nil {
		t.Fatalf("ParseDSN(%q): %v", dsn, err)
	}
	return cfg
}

func uniqueName(t *testing.T) string {
	t.Helper()
	label := strings.ReplaceAll(t.Name(), "/", "-")
	return fmt.Sprintf("it-%s-%d", label, time.Now().UnixNano())
}

// FuzzStore replays the contract model against a real database.
//
// Every operation here is a round trip, so this explores far less per second
// than the same target against memstore. What it adds is the subject: SQL
// guards spread across four statements, reached over the pooled connection,
// where one that quietly depends on session state has nowhere to hide. Driving
// it with -fuzz is worthwhile occasionally; running its corpus, as `go test`
// does, is worthwhile every time.
func FuzzStore(f *testing.F) {
	available := routes(f)
	storetest.FuzzOperations(f, storetest.Suite{Store: open(f, available[len(available)-1])})
}
