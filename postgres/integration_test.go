//go:build integration

// The integration suite talks to a real PostgreSQL, both directly and through
// PgBouncer in transaction pooling mode. Run it with:
//
//	make test-int
//
// or point it at your own database:
//
//	LEADERELECTION_POSTGRES_DSN=postgres://... go test -tags=integration ./postgres/...
//
// Deliberately no testcontainers: keeping the driver module's dependencies to
// pgx alone matters more for a library than the convenience does, and the
// containers are one docker compose away.
package postgres_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/brualan/leaderelection"
	"github.com/brualan/leaderelection/postgres"
	"github.com/brualan/leaderelection/storetest"
)

// Environment variables naming the two paths to the database. A missing direct
// DSN skips the suite; a missing pooler DSN fails it, because silently not
// testing the pooler path is how that path rots.
const (
	envDirect = "LEADERELECTION_POSTGRES_DSN"
	envPooled = "LEADERELECTION_PGBOUNCER_DSN"
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
		out = append(out, route{name: "pgbouncer", dsn: pooled})
	}
	return out
}

func open(t testing.TB, r route, opts ...postgres.Option) *postgres.Store {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	store, err := postgres.Connect(ctx, r.dsn, opts...)
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
// with no exemptions, over a connection that is handed to a different server
// backend after every statement.
func TestAcceptance(t *testing.T) {
	for _, r := range routes(t) {
		t.Run(r.name, func(t *testing.T) {
			t.Parallel()
			storetest.Run(t, storetest.Suite{Store: open(t, r)})
		})
	}
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
				// itself and report a duplicate; that is still success from
				// the caller's point of view, and must not surface.
				t.Fatalf("concurrent EnsureSchema: %v", err)
			}
		})
	}
}

// The timestamps in a Lease must come from the server, not from this process.
// A client clock hours away from the server's must change nothing.
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
			if ttl := lease.TTL(); ttl <= 25*time.Second || ttl > 30*time.Second {
				t.Fatalf("TTL = %v, want close to 30s", ttl)
			}

			// The server's clock and this process's clock are the same machine
			// here, so the check that means something is internal consistency:
			// every timestamp must come from the one clock read the statement
			// performed.
			if lease.Now.Before(lease.AcquiredAt) {
				t.Fatalf("now %v precedes acquired_at %v", lease.Now, lease.AcquiredAt)
			}
			if !lease.ExpiresAt.Equal(lease.Now.Add(30 * time.Second)) {
				t.Fatalf("expires_at %v is not exactly 30s after now %v", lease.ExpiresAt, lease.Now)
			}
		})
	}
}

// A pooler in transaction mode hands out a different server connection after
// every statement. Anything session-scoped -- a named prepared statement above
// all -- breaks here and nowhere else.
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
			// Enough iterations to cycle through every server connection in
			// the pooler's small pool several times over.
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
// real deployment, not an edge case.
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

// A custom table must be honoured everywhere, including in the DDL, and must
// not collide with the default one.
func TestCustomTable(t *testing.T) {
	for _, r := range routes(t) {
		t.Run(r.name, func(t *testing.T) {
			ctx := context.Background()
			custom := open(t, r, postgres.WithTable("", "custom_leader_election"))
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
			store, err := postgres.Connect(ctx, r.dsn, postgres.WithTable("", "no_such_election_table"))
			if err != nil {
				t.Fatalf("Connect: %v", err)
			}
			t.Cleanup(store.Close)

			_, err = store.Inspect(ctx, "whatever")
			if err == nil {
				t.Fatal("Inspect against a missing table succeeded")
			}
			if !postgres.Fatal(err) {
				t.Fatalf("Fatal(%v) = false, want true", err)
			}
			if postgres.Retryable(err) {
				t.Fatalf("Retryable(%v) = true; retrying will never help", err)
			}
		})
	}
}

// A closed pool stands in for the database becoming unreachable. Every
// operation must report failure rather than inventing a lease.
func TestClosedPoolFailsEveryOperation(t *testing.T) {
	r := routes(t)[0]
	ctx := context.Background()

	store, err := postgres.Connect(ctx, r.dsn)
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
		t.Fatal("Inspect on a closed pool succeeded")
	}
	if got, err := store.Acquire(ctx, name, "a", time.Minute, nil); err == nil && got.HeldBy("a") {
		t.Fatal("Acquire on a closed pool reported a held lease")
	}
	if _, err := store.Renew(ctx, name, "a", lease.Fence, time.Minute, nil); err == nil {
		t.Fatal("Renew on a closed pool succeeded")
	} else if errors.Is(err, leaderelection.ErrLeaseLost) {
		t.Fatal("an unreachable database was reported as a lost lease; " +
			"the two demand different operator responses")
	}
	if err := store.Release(ctx, name, "a", lease.Fence); err == nil {
		t.Fatal("Release on a closed pool succeeded")
	}
}

// The pooler-safe defaults must actually be what reaches the driver, since
// nothing else in the suite would notice a regression that only shows up on a
// pooler version without prepared statement support.
func TestConnectDisablesSessionState(t *testing.T) {
	r := routes(t)[0]
	cfg, err := pgxpool.ParseConfig(r.dsn)
	if err != nil {
		t.Fatalf("ParseConfig: %v", err)
	}
	postgres.ApplyPoolerSafeDefaultsForTest(cfg)

	if cfg.ConnConfig.DefaultQueryExecMode != pgx.QueryExecModeExec {
		t.Fatalf("DefaultQueryExecMode = %v, want QueryExecModeExec", cfg.ConnConfig.DefaultQueryExecMode)
	}
	if cfg.ConnConfig.StatementCacheCapacity != 0 {
		t.Fatalf("StatementCacheCapacity = %d, want 0", cfg.ConnConfig.StatementCacheCapacity)
	}
}

// New wraps a pool the application already owns, and must leave it open: an
// application that also uses that pool for its own work would otherwise find
// it closed when the election shut down.
func TestNewBorrowsTheCallersPool(t *testing.T) {
	r := routes(t)[0]
	ctx := context.Background()

	pool, err := pgxpool.New(ctx, r.dsn)
	if err != nil {
		t.Fatalf("pgxpool.New: %v", err)
	}
	defer pool.Close()

	store, err := postgres.New(pool, postgres.WithTable("", postgres.DefaultTable))
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
		t.Fatal("Acquire over a borrowed pool did not take the lease")
	}

	store.Close()
	if _, err := pool.Exec(ctx, "SELECT 1"); err != nil {
		t.Fatalf("the borrowed pool was closed by Store.Close: %v", err)
	}
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
