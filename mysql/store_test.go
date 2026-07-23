package mysql

import (
	"context"
	"database/sql"
	sqldriver "database/sql/driver"
	"errors"
	"strings"
	"testing"
	"time"

	driver "github.com/go-sql-driver/mysql"

	"github.com/brualan/leaderelection"
)

var errBoom = errors.New("connection reset by peer")

func newTestStore(t *testing.T, f *fakeDB, opts ...Option) *Store {
	t.Helper()
	store, err := newStore(openFake(t, f), func() {}, opts...)
	if err != nil {
		t.Fatalf("newStore: %v", err)
	}
	return store
}

func TestNewRejectsANilHandle(t *testing.T) {
	if _, err := New(nil); err == nil {
		t.Fatal("New(nil) succeeded")
	}
}

func TestNewRejectsABadTableName(t *testing.T) {
	if _, err := New(openFake(t, &fakeDB{}), WithTable("", "drop table x;")); err == nil {
		t.Fatal("New accepted an unusable table name")
	}
}

func TestStoreClosesWhatItOwns(t *testing.T) {
	closed := false
	store, err := newStore(openFake(t, &fakeDB{}), func() { closed = true })
	if err != nil {
		t.Fatalf("newStore: %v", err)
	}
	store.Close()
	if !closed {
		t.Fatal("Close did not run the release function")
	}
}

func TestTableIsQuotedAndQualified(t *testing.T) {
	for _, tc := range []struct {
		database, table, want string
	}{
		{"", "", "`leader_election`"},
		{"", "custom", "`custom`"},
		{"coord", "custom", "`coord`.`custom`"},
	} {
		var opts []Option
		if tc.table != "" {
			opts = append(opts, WithTable(tc.database, tc.table))
		}
		store := newTestStore(t, &fakeDB{}, opts...)
		if got := store.Table(); got != tc.want {
			t.Fatalf("Table() = %s, want %s", got, tc.want)
		}
		// The quoted name must be what actually reaches the statements.
		if !strings.Contains(store.acquireSQL, tc.want) {
			t.Fatalf("acquire statement does not reference %s", tc.want)
		}
	}
}

// A statement that fails must never be reported as a lost election: the two
// mean different things to the loop above and to the operator.
func TestWriteFailuresAreNotLostLeases(t *testing.T) {
	store := newTestStore(t, &fakeDB{execErr: errBoom, row: leaseRow()})
	ctx := context.Background()

	if _, err := store.Acquire(ctx, "e", "a", time.Minute, nil); !errors.Is(err, errBoom) {
		t.Fatalf("Acquire: got %v, want %v", err, errBoom)
	}
	if _, err := store.Renew(ctx, "e", "a", 7, time.Minute, nil); !errors.Is(err, errBoom) {
		t.Fatalf("Renew: got %v, want %v", err, errBoom)
	} else if errors.Is(err, leaderelection.ErrLeaseLost) {
		t.Fatal("an unreachable database was reported as a lost lease")
	}
	if err := store.Release(ctx, "e", "a", 7); !errors.Is(err, errBoom) {
		t.Fatalf("Release: got %v, want %v", err, errBoom)
	}
	if err := store.EnsureSchema(ctx); !errors.Is(err, errBoom) {
		t.Fatalf("EnsureSchema: got %v, want %v", err, errBoom)
	}
}

// The read-back is the other half of every write, and a failure there must
// surface just as loudly -- especially for Renew, where the tempting mistake is
// to trust the write that already succeeded.
func TestReadBackFailuresAreNotLostLeases(t *testing.T) {
	store := newTestStore(t, &fakeDB{queryErr: errBoom, row: leaseRow()})
	ctx := context.Background()

	if _, err := store.Inspect(ctx, "e"); !errors.Is(err, errBoom) {
		t.Fatalf("Inspect: got %v, want %v", err, errBoom)
	} else if errors.Is(err, leaderelection.ErrNoLease) {
		t.Fatal("a failed query was reported as a missing lease")
	}
	if _, err := store.Acquire(ctx, "e", "a", time.Minute, nil); !errors.Is(err, errBoom) {
		t.Fatalf("Acquire: got %v, want %v", err, errBoom)
	}
	if _, err := store.Renew(ctx, "e", "a", 7, time.Minute, nil); !errors.Is(err, errBoom) {
		t.Fatalf("Renew: got %v, want %v", err, errBoom)
	} else if errors.Is(err, leaderelection.ErrLeaseLost) {
		t.Fatal("a database that answered the write and not the read was reported as a lost lease")
	}
}

func TestNoRowMeansDifferentThingsPerOperation(t *testing.T) {
	ctx := context.Background()
	// row is nil, so every read comes back empty.
	empty := func() *fakeDB { return &fakeDB{} }

	// Nobody has ever held this election.
	if _, err := newTestStore(t, empty()).Inspect(ctx, "e"); !errors.Is(err, leaderelection.ErrNoLease) {
		t.Fatalf("Inspect: got %v, want ErrNoLease", err)
	}
	// The record this caller was renewing no longer exists, so it is certainly
	// not the leader -- but that is a lost lease, not an outage.
	if _, err := newTestStore(t, empty()).Renew(ctx, "e", "a", 1, time.Minute, nil); !errors.Is(err, leaderelection.ErrLeaseLost) {
		t.Fatalf("Renew: got %v, want ErrLeaseLost", err)
	}
	// The upsert always leaves a row behind, so an empty read afterwards is a
	// broken statement or a vanished table, and must not be mistaken for a lost
	// race.
	_, err := newTestStore(t, empty()).Acquire(ctx, "e", "a", time.Minute, nil)
	if err == nil {
		t.Fatal("Acquire that wrote no row succeeded")
	}
	if errors.Is(err, leaderelection.ErrLeaseLost) || errors.Is(err, leaderelection.ErrNoLease) {
		t.Fatalf("Acquire reported a routine outcome for a broken statement: %v", err)
	}
}

func TestRowStreamFailures(t *testing.T) {
	ctx := context.Background()
	errStream := errors.New("unexpected EOF")

	badTimestamp := leaseRow()
	badTimestamp[2] = []byte("yesterday afternoon")
	wrongType := leaseRow()
	wrongType[4] = int64(1)

	for _, tc := range []struct {
		name string
		db   *fakeDB
	}{
		{"the stream died before the row", &fakeDB{row: leaseRow(), rowErr: errStream}},
		{"the stream died as it was drained", &fakeDB{row: leaseRow(), closeErr: errStream}},
		{"a timestamp did not parse", &fakeDB{row: badTimestamp}},
		{"a timestamp arrived as something else", &fakeDB{row: wrongType}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := newTestStore(t, tc.db).Inspect(ctx, "e")
			if err == nil {
				t.Fatal("Inspect succeeded")
			}
			if errors.Is(err, leaderelection.ErrNoLease) {
				t.Fatalf("a broken result stream was reported as a missing lease: %v", err)
			}
		})
	}
}

func TestSuccessfulReadDecodesTheLease(t *testing.T) {
	store := newTestStore(t, &fakeDB{row: leaseRow()})
	lease, err := store.Inspect(context.Background(), "shipping")
	if err != nil {
		t.Fatalf("Inspect: %v", err)
	}
	if lease.Name != "shipping" {
		t.Fatalf("Name = %q, want shipping", lease.Name)
	}
	if lease.Holder != "holder" || lease.Fence != 7 {
		t.Fatalf("holder/fence = %q/%d, want holder/7", lease.Holder, lease.Fence)
	}
	if string(lease.Payload) != "payload" {
		t.Fatalf("Payload = %q, want payload", lease.Payload)
	}
	// The row deliberately delivers its three timestamps the three ways a
	// driver can: parsed, as bytes and as a string. They must decode to the
	// same clock, which is what makes the TTL below meaningful.
	if lease.TTL() != time.Minute {
		t.Fatalf("TTL = %v, want 1m", lease.TTL())
	}
	if !lease.AcquiredAt.Equal(lease.Now) {
		t.Fatalf("acquired_at %v and now %v disagree; the parsed and the raw forms decoded differently",
			lease.AcquiredAt, lease.Now)
	}
}

// A renewal is reported on what the read-back shows, never on the write having
// succeeded. Every way the record can fail to show this caller's live term is a
// lost lease.
func TestRenewIsDecidedByTheReadBack(t *testing.T) {
	ctx := context.Background()

	held := leaseRow()
	otherHolder := leaseRow()
	otherHolder[0] = []byte("somebody-else")
	otherFence := leaseRow()
	otherFence[1] = int64(8)
	expired := leaseRow()
	expired[4] = "2026-07-22 11:59:00.000000" // before now

	for _, tc := range []struct {
		name string
		row  []sqldriver.Value
		want error
	}{
		{"the term is ours and live", held, nil},
		{"somebody else holds it", otherHolder, leaderelection.ErrLeaseLost},
		{"the term moved on", otherFence, leaderelection.ErrLeaseLost},
		{"it expired underneath us", expired, leaderelection.ErrLeaseLost},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := newTestStore(t, &fakeDB{row: tc.row})
			lease, err := store.Renew(ctx, "e", "holder", 7, time.Minute, nil)
			if !errors.Is(err, tc.want) {
				t.Fatalf("Renew: got %v, want %v", err, tc.want)
			}
			if tc.want == nil && !lease.HeldBy("holder") {
				t.Fatal("a successful renewal did not report the lease as held")
			}
		})
	}
}

// The upsert names the identity in five places and the payload in two, and a
// misplaced one would guard the wrong thing. This pins the argument list
// against the statement it is written for.
func TestAcquireArgumentsLineUpWithTheStatement(t *testing.T) {
	f := &fakeDB{row: leaseRow()}
	store := newTestStore(t, f)

	if _, err := store.Acquire(context.Background(), "e", "a", 15*time.Second, []byte("p")); err != nil {
		t.Fatalf("Acquire: %v", err)
	}

	// The read-back ran last, so the recorded statement is the wrong one to
	// inspect; what matters is that both ran.
	if f.execs != 1 || f.queries != 1 {
		t.Fatalf("Acquire ran %d writes and %d reads, want one of each", f.execs, f.queries)
	}

	args := acquireArgs("e", "a", micros(15*time.Second), []byte("p"))
	if got := strings.Count(store.acquireSQL, "?"); got != len(args) {
		t.Fatalf("the acquire statement has %d parameters and Acquire passes %d", got, len(args))
	}
	// Positions 4 and 8 are the payload, everything from 5 on that is not a
	// payload is the identity, and the two microsecond values must match.
	for i, want := range []any{"e", "a", int64(15_000_000), []byte("p"), "a", "a", "a", "a", []byte("p"), "a", int64(15_000_000)} {
		if got, ok := args[i].([]byte); ok {
			if string(got) != string(want.([]byte)) {
				t.Fatalf("argument %d = %q, want %q", i+1, got, want)
			}
			continue
		}
		if args[i] != want {
			t.Fatalf("argument %d = %v, want %v", i+1, args[i], want)
		}
	}
}

// The TTL crosses into SQL as microseconds, and getting that conversion wrong
// would silently scale every lease in the system.
func TestTTLIsSentAsMicroseconds(t *testing.T) {
	f := &fakeDB{row: leaseRow()}
	store := newTestStore(t, f)

	if _, err := store.Renew(context.Background(), "e", "holder", 7, 250*time.Millisecond, nil); err != nil {
		t.Fatalf("Renew: %v", err)
	}
	// The renewal's first argument is the TTL; the read-back that follows takes
	// only the election name.
	if got := f.lastExecArgs[0].Value; got != int64(250_000) {
		t.Fatalf("renew ttl argument = %v, want 250000 microseconds", got)
	}
	if got := f.lastQueryArgs; len(got) != 1 {
		t.Fatalf("the read-back ran with %d arguments, want 1", len(got))
	}
	if got := micros(time.Minute); got != 60_000_000 {
		t.Fatalf("micros(1m) = %d, want 60000000", got)
	}
}

func TestReleaseAndEnsureSchemaAreSingleWrites(t *testing.T) {
	ctx := context.Background()
	f := &fakeDB{}
	store := newTestStore(t, f)

	if err := store.Release(ctx, "e", "a", 3); err != nil {
		t.Fatalf("Release: %v", err)
	}
	if !strings.Contains(f.lastExecSQL, "UPDATE") || len(f.lastExecArgs) != 3 {
		t.Fatalf("Release ran %q with %d args", f.lastExecSQL, len(f.lastExecArgs))
	}
	// Releasing is the one operation with nothing to read back: there is no
	// lease to report, only a term to end.
	if f.queries != 0 {
		t.Fatalf("Release performed %d reads, want none", f.queries)
	}

	if err := store.EnsureSchema(ctx); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}
	if !strings.Contains(f.lastExecSQL, "CREATE TABLE IF NOT EXISTS") {
		t.Fatalf("EnsureSchema ran %q", f.lastExecSQL)
	}
}

// The context must reach the driver rather than being dropped, or an operation
// timeout upstairs would not actually bound anything.
func TestContextReachesTheDriver(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	store := newTestStore(t, &fakeDB{row: leaseRow()})
	if _, err := store.Inspect(ctx, "e"); err == nil {
		t.Fatal("Inspect on a cancelled context succeeded")
	}
	if _, err := store.Acquire(ctx, "e", "a", time.Minute, nil); err == nil {
		t.Fatal("Acquire on a cancelled context succeeded")
	}
	if _, err := store.Renew(ctx, "e", "holder", 7, time.Minute, nil); err == nil {
		t.Fatal("Renew on a cancelled context succeeded")
	}
	if err := store.Release(ctx, "e", "a", 7); err == nil {
		t.Fatal("Release on a cancelled context succeeded")
	}
	if err := store.EnsureSchema(ctx); err == nil {
		t.Fatal("EnsureSchema on a cancelled context succeeded")
	}
}

func TestConnectRejectsABadDSN(t *testing.T) {
	if _, err := Connect(context.Background(), "not a dsn"); err == nil {
		t.Fatal("Connect accepted a malformed DSN")
	}
}

// A connector that cannot be built at all -- not merely one that cannot reach
// the server yet -- has to surface rather than yield a half-built Store.
func TestConnectReportsConnectorFailure(t *testing.T) {
	restore := newConnector
	t.Cleanup(func() { newConnector = restore })
	sentinel := errors.New("connector refused to start")
	newConnector = func(*driver.Config) (sqlDriverConnector, error) { return nil, sentinel }

	_, err := Connect(context.Background(), "u:p@tcp(127.0.0.1:3306)/db")
	if !errors.Is(err, sentinel) {
		t.Fatalf("Connect: got %v, want %v", err, sentinel)
	}
}

func TestConnectRejectsABadTableName(t *testing.T) {
	// The table name is validated before anything is dialled, so this also
	// checks that a misconfiguration does not cost a connection attempt.
	_, err := Connect(context.Background(), "u:p@tcp(127.0.0.1:3306)/db", WithTable("", "bad name"))
	if err == nil {
		t.Fatal("Connect accepted an unusable table name")
	}
}

// sql.OpenDB is lazy, so without an explicit ping a DSN naming a host that is
// not there would only fail on the first election.
func TestConnectFailsWhenTheServerIsNotThere(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Port 1 is refused rather than filtered, so this fails immediately rather
	// than waiting out a dial timeout.
	_, err := Connect(ctx, "u:p@tcp(127.0.0.1:1)/db")
	if err == nil {
		t.Fatal("Connect succeeded against a port with nothing behind it")
	}
	if !Retryable(err) {
		t.Fatalf("Retryable(%v) = false; an unreachable server is exactly what retrying is for", err)
	}
}

func TestPoolerSafeDefaults(t *testing.T) {
	cfg, err := driver.ParseDSN("u:p@tcp(127.0.0.1:3306)/db")
	if err != nil {
		t.Fatalf("ParseDSN: %v", err)
	}
	applyPoolerSafeDefaults(cfg)

	// A server-side prepared statement lives on one backend connection, and a
	// pooler hands the next statement to a different one.
	if !cfg.InterpolateParams {
		t.Fatal("parameters are still bound with a server-side prepared statement")
	}
	if cfg.MultiStatements {
		t.Fatal("multi-statement mode was left on")
	}
	if cfg.Loc != time.UTC {
		t.Fatalf("Loc = %v, want UTC: the columns hold UTC and nothing else", cfg.Loc)
	}
}

// The DSN decides whether the driver parses timestamps for us; the store has to
// read them either way, because that setting belongs to the application.
func TestPoolerSafeDefaultsLeaveParseTimeAlone(t *testing.T) {
	for _, parseTime := range []bool{false, true} {
		dsn := "u:p@tcp(127.0.0.1:3306)/db"
		if parseTime {
			dsn += "?parseTime=true"
		}
		cfg, err := driver.ParseDSN(dsn)
		if err != nil {
			t.Fatalf("ParseDSN: %v", err)
		}
		applyPoolerSafeDefaults(cfg)
		if cfg.ParseTime != parseTime {
			t.Fatalf("ParseTime = %v, want the DSN's %v", cfg.ParseTime, parseTime)
		}
	}
}

func TestPoolBoundsAreSet(t *testing.T) {
	db := openFake(t, &fakeDB{})
	applyPoolBounds(db)

	stats := db.Stats()
	if stats.MaxOpenConnections < 4 {
		t.Fatalf("MaxOpenConnections = %d, want at least 4", stats.MaxOpenConnections)
	}
}

// Every timestamp a driver can hand over must decode, and anything else must be
// refused rather than silently read as the zero time -- which would report a
// lease as long expired and hand the election to somebody else.
func TestTimestampScanning(t *testing.T) {
	want := time.Date(2026, 7, 22, 12, 0, 0, 500_000_000, time.UTC)

	for _, tc := range []struct {
		name string
		src  any
		ok   bool
	}{
		{"parsed by the driver", want, true},
		{"raw bytes", []byte("2026-07-22 12:00:00.5"), true},
		{"a string", "2026-07-22 12:00:00.5", true},
		{"unparseable", "not a timestamp", false},
		{"the wrong type entirely", int64(0), false},
		{"NULL", nil, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var d dbTime
			err := d.Scan(tc.src)
			switch {
			case tc.ok && err != nil:
				t.Fatalf("Scan(%v): %v", tc.src, err)
			case !tc.ok && err == nil:
				t.Fatalf("Scan(%v) accepted something unusable as %v", tc.src, d.t)
			case !tc.ok:
				return
			}
			if !d.t.Equal(want) {
				t.Fatalf("Scan(%v) = %v, want %v", tc.src, d.t, want)
			}
			if _, offset := d.t.Zone(); offset != 0 {
				t.Fatalf("Scan(%v) produced a non-UTC time %v; the columns hold UTC", tc.src, d.t)
			}
		})
	}

	// A column somebody's migration defined without sub-second precision still
	// has to scan, even though this package's own schema never produces one.
	var whole dbTime
	if err := whole.Scan("2026-07-22 12:00:00"); err != nil {
		t.Fatalf("Scan of a whole-second timestamp: %v", err)
	}
	if !whole.t.Equal(want.Truncate(time.Second)) {
		t.Fatalf("Scan of a whole-second timestamp = %v, want %v", whole.t, want.Truncate(time.Second))
	}
}

func TestStoreSatisfiesTheInterface(t *testing.T) {
	var _ leaderelection.Store = (*Store)(nil)
}

// sql.ErrNoRows is the store's internal signal for "no such record", and every
// caller translates it. It must never escape as itself: a caller checking
// against the leaderelection sentinels would miss it entirely.
func TestErrNoRowsDoesNotEscape(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t, &fakeDB{})

	if _, err := store.Inspect(ctx, "e"); errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("Inspect leaked %v", err)
	}
	if _, err := store.Renew(ctx, "e", "a", 1, time.Minute, nil); errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("Renew leaked %v", err)
	}
	if _, err := store.Acquire(ctx, "e", "a", time.Minute, nil); errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("Acquire leaked %v", err)
	}
}
