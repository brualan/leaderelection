package postgres

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/brualan/leaderelection"
)

// fakeQuerier stands in for a pool so that the driver's failure paths can be
// reached deliberately. Provoking a real database into failing at exactly the
// right moment -- mid-scan, mid-stream -- is not reliably possible, and these
// are the paths where a mistake turns an outage into two leaders.
type fakeQuerier struct {
	rows    pgx.Rows
	queryFn func(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	queryOK bool

	execTag pgconn.CommandTag
	execErr error

	lastSQL  string
	lastArgs []any
}

func (f *fakeQuerier) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	f.lastSQL, f.lastArgs = sql, args
	if f.queryFn != nil {
		return f.queryFn(ctx, sql, args...)
	}
	if !f.queryOK {
		return nil, errBoom
	}
	return f.rows, nil
}

func (f *fakeQuerier) Exec(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	f.lastSQL, f.lastArgs = sql, args
	return f.execTag, f.execErr
}

var errBoom = errors.New("connection reset by peer")

// fakeRows implements pgx.Rows over a scripted sequence of outcomes.
type fakeRows struct {
	remaining int
	scanErr   error
	err       error
	// errAfterScan is returned by Err only once the row has been consumed,
	// which is how a stream that dies partway through reports itself.
	errAfterScan error
	scanned      bool
	closed       bool
}

func (r *fakeRows) Close() { r.closed = true }

func (r *fakeRows) Err() error {
	if r.scanned {
		return r.errAfterScan
	}
	return r.err
}

func (r *fakeRows) CommandTag() pgconn.CommandTag                { return pgconn.CommandTag{} }
func (r *fakeRows) FieldDescriptions() []pgconn.FieldDescription { return nil }

func (r *fakeRows) Next() bool {
	if r.remaining <= 0 {
		return false
	}
	r.remaining--
	return true
}

func (r *fakeRows) Scan(dest ...any) error {
	r.scanned = true
	if r.scanErr != nil {
		return r.scanErr
	}
	now := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	values := []any{"holder", int64(7), now, now, now.Add(time.Minute), []byte("payload"), now}
	for i, d := range dest {
		switch p := d.(type) {
		case *string:
			value, ok := values[i].(string)
			if !ok {
				return errors.New("unexpected scan value type")
			}
			*p = value
		case *int64:
			value, ok := values[i].(int64)
			if !ok {
				return errors.New("unexpected scan value type")
			}
			*p = value
		case *time.Time:
			value, ok := values[i].(time.Time)
			if !ok {
				return errors.New("unexpected scan value type")
			}
			*p = value
		case *[]byte:
			value, ok := values[i].([]byte)
			if !ok {
				return errors.New("unexpected scan value type")
			}
			*p = value
		}
	}
	return nil
}

func (r *fakeRows) Values() ([]any, error) { return nil, nil }
func (r *fakeRows) RawValues() [][]byte    { return nil }
func (r *fakeRows) Conn() *pgx.Conn        { return nil }

func newTestStore(t *testing.T, q *fakeQuerier, opts ...Option) *Store {
	t.Helper()
	store, err := newStore(q, func() {}, opts...)
	if err != nil {
		t.Fatalf("newStore: %v", err)
	}
	return store
}

func TestNewRejectsANilPool(t *testing.T) {
	if _, err := New(nil); err == nil {
		t.Fatal("New(nil) succeeded")
	}
}

func TestNewRejectsABadTableName(t *testing.T) {
	if _, err := newStore(&fakeQuerier{}, func() {}, WithTable("", "drop table x;")); err == nil {
		t.Fatal("newStore accepted an unusable table name")
	}
}

func TestStoreClosesWhatItOwns(t *testing.T) {
	closed := false
	store, err := newStore(&fakeQuerier{}, func() { closed = true })
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
		schema, table, want string
	}{
		{"", "", `"leader_election"`},
		{"", "custom", `"custom"`},
		{"coord", "custom", `"coord"."custom"`},
	} {
		var opts []Option
		if tc.table != "" {
			opts = append(opts, WithTable(tc.schema, tc.table))
		}
		store := newTestStore(t, &fakeQuerier{}, opts...)
		if got := store.Table(); got != tc.want {
			t.Fatalf("Table() = %s, want %s", got, tc.want)
		}
		// The quoted name must be what actually reaches the statements.
		if !strings.Contains(store.acquireSQL, tc.want) {
			t.Fatalf("acquire statement does not reference %s", tc.want)
		}
	}
}

// A query that fails must never be reported as a lost election: the two mean
// different things to the loop above and to the operator.
func TestQueryFailuresAreNotLostLeases(t *testing.T) {
	store := newTestStore(t, &fakeQuerier{})
	ctx := context.Background()

	if _, err := store.Inspect(ctx, "e"); !errors.Is(err, errBoom) {
		t.Fatalf("Inspect: got %v, want %v", err, errBoom)
	} else if errors.Is(err, leaderelection.ErrNoLease) {
		t.Fatal("a failed query was reported as a missing lease")
	}

	if _, err := store.Acquire(ctx, "e", "a", time.Minute, nil); !errors.Is(err, errBoom) {
		t.Fatalf("Acquire: got %v, want %v", err, errBoom)
	}

	if _, err := store.Renew(ctx, "e", "a", 1, time.Minute, nil); !errors.Is(err, errBoom) {
		t.Fatalf("Renew: got %v, want %v", err, errBoom)
	} else if errors.Is(err, leaderelection.ErrLeaseLost) {
		t.Fatal("an unreachable database was reported as a lost lease")
	}
}

func TestNoRowsMeansDifferentThingsPerOperation(t *testing.T) {
	ctx := context.Background()
	empty := func() *fakeQuerier { return &fakeQuerier{queryOK: true, rows: &fakeRows{}} }

	// Nobody has ever held this election.
	if _, err := newTestStore(t, empty()).Inspect(ctx, "e"); !errors.Is(err, leaderelection.ErrNoLease) {
		t.Fatalf("Inspect: got %v, want ErrNoLease", err)
	}
	// The guard did not match: this caller is no longer the leader.
	if _, err := newTestStore(t, empty()).Renew(ctx, "e", "a", 1, time.Minute, nil); !errors.Is(err, leaderelection.ErrLeaseLost) {
		t.Fatalf("Renew: got %v, want ErrLeaseLost", err)
	}
	// The upsert always produces a row, so an empty result is a broken
	// statement, not a lost race -- and must not be mistaken for one.
	_, err := newTestStore(t, empty()).Acquire(ctx, "e", "a", time.Minute, nil)
	if err == nil {
		t.Fatal("Acquire with no returned row succeeded")
	}
	if errors.Is(err, leaderelection.ErrLeaseLost) || errors.Is(err, leaderelection.ErrNoLease) {
		t.Fatalf("Acquire reported a routine outcome for a broken statement: %v", err)
	}
}

func TestRowStreamFailures(t *testing.T) {
	ctx := context.Background()
	errStream := errors.New("unexpected EOF")

	for _, tc := range []struct {
		name string
		rows *fakeRows
	}{
		{"stream failed before any row", &fakeRows{err: errStream}},
		{"scan failed", &fakeRows{remaining: 1, scanErr: errStream}},
		{"stream failed after the row", &fakeRows{remaining: 1, errAfterScan: errStream}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := newTestStore(t, &fakeQuerier{queryOK: true, rows: tc.rows})
			if _, err := store.Inspect(ctx, "e"); !errors.Is(err, errStream) {
				t.Fatalf("Inspect: got %v, want %v", err, errStream)
			}
			if !tc.rows.closed {
				t.Fatal("rows were not closed")
			}
		})
	}
}

func TestSuccessfulReadDecodesTheLease(t *testing.T) {
	store := newTestStore(t, &fakeQuerier{queryOK: true, rows: &fakeRows{remaining: 1}})
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
	if lease.TTL() != time.Minute {
		t.Fatalf("TTL = %v, want 1m", lease.TTL())
	}
}

func TestReleaseAndEnsureSchemaUseExec(t *testing.T) {
	ctx := context.Background()

	ok := &fakeQuerier{}
	store := newTestStore(t, ok)
	if err := store.Release(ctx, "e", "a", 3); err != nil {
		t.Fatalf("Release: %v", err)
	}
	if !strings.Contains(ok.lastSQL, "UPDATE") || len(ok.lastArgs) != 3 {
		t.Fatalf("Release ran %q with %d args", ok.lastSQL, len(ok.lastArgs))
	}
	if err := store.EnsureSchema(ctx); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}
	if !strings.Contains(ok.lastSQL, "CREATE TABLE IF NOT EXISTS") {
		t.Fatalf("EnsureSchema ran %q", ok.lastSQL)
	}

	bad := &fakeQuerier{execErr: errBoom}
	store = newTestStore(t, bad)
	if err := store.Release(ctx, "e", "a", 3); !errors.Is(err, errBoom) {
		t.Fatalf("Release: got %v, want %v", err, errBoom)
	}
	if err := store.EnsureSchema(ctx); !errors.Is(err, errBoom) {
		t.Fatalf("EnsureSchema: got %v, want %v", err, errBoom)
	}
}

// The TTL crosses into SQL as microseconds, and getting that conversion wrong
// would silently scale every lease in the system.
func TestTTLIsSentAsMicroseconds(t *testing.T) {
	// A fresh row set per call: a pgx.Rows is consumed once.
	q := &fakeQuerier{queryFn: func(context.Context, string, ...any) (pgx.Rows, error) {
		return &fakeRows{remaining: 1}, nil
	}}
	store := newTestStore(t, q)

	if _, err := store.Acquire(context.Background(), "e", "a", 15*time.Second, []byte("p")); err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	if got := q.lastArgs[2]; got != float64(15_000_000) {
		t.Fatalf("ttl argument = %v, want 15000000 microseconds", got)
	}

	if _, err := store.Renew(context.Background(), "e", "a", 1, 250*time.Millisecond, nil); err != nil {
		t.Fatalf("Renew: %v", err)
	}
	if got := q.lastArgs[3]; got != float64(250_000) {
		t.Fatalf("ttl argument = %v, want 250000 microseconds", got)
	}
}

// The context must reach the driver rather than being dropped, or an operation
// timeout upstairs would not actually bound anything.
func TestContextReachesTheDriver(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	seen := make(chan struct{}, 1)
	q := &fakeQuerier{queryFn: func(ctx context.Context, _ string, _ ...any) (pgx.Rows, error) {
		if ctx.Err() != nil {
			seen <- struct{}{}
		}
		return nil, ctx.Err()
	}}
	store := newTestStore(t, q)

	if _, err := store.Inspect(ctx, "e"); err == nil {
		t.Fatal("Inspect on a cancelled context succeeded")
	}
	select {
	case <-seen:
	default:
		t.Fatal("the driver did not receive the cancelled context")
	}
}

func TestConnectRejectsABadDSN(t *testing.T) {
	if _, err := Connect(context.Background(), "://not a dsn"); err == nil {
		t.Fatal("Connect accepted a malformed DSN")
	}
}

// A pool that cannot be created at all -- not merely one that cannot reach the
// server yet -- has to surface rather than yield a half-built Store.
func TestConnectReportsPoolCreationFailure(t *testing.T) {
	restore := newPool
	t.Cleanup(func() { newPool = restore })
	sentinel := errors.New("pool refused to start")
	newPool = func(context.Context, *pgxpool.Config) (*pgxpool.Pool, error) {
		return nil, sentinel
	}

	_, err := Connect(context.Background(), "postgres://user:pass@127.0.0.1:5432/db?sslmode=disable")
	if !errors.Is(err, sentinel) {
		t.Fatalf("Connect: got %v, want %v", err, sentinel)
	}
}

func TestConnectRejectsABadTableName(t *testing.T) {
	// The pool is built before the table name is validated, so this also
	// checks that the failure path does not leak it.
	_, err := Connect(context.Background(),
		"postgres://user:pass@127.0.0.1:1/db?sslmode=disable",
		WithTable("", "bad name"))
	if err == nil {
		t.Fatal("Connect accepted an unusable table name")
	}
}

func TestPoolerSafeDefaults(t *testing.T) {
	cfg, err := pgxpool.ParseConfig("postgres://user:pass@127.0.0.1:5432/db?sslmode=disable")
	if err != nil {
		t.Fatalf("ParseConfig: %v", err)
	}
	applyPoolerSafeDefaults(cfg)

	// Named prepared statements are per-session, and a transaction pooler
	// gives back a different session after every statement.
	if cfg.ConnConfig.DefaultQueryExecMode != pgx.QueryExecModeExec {
		t.Fatalf("DefaultQueryExecMode = %v, want QueryExecModeExec", cfg.ConnConfig.DefaultQueryExecMode)
	}
	if cfg.ConnConfig.StatementCacheCapacity != 0 || cfg.ConnConfig.DescriptionCacheCapacity != 0 {
		t.Fatal("statement caches were left enabled")
	}
	if cfg.MaxConnLifetime > 30*time.Minute || cfg.MaxConnLifetime == 0 {
		t.Fatalf("MaxConnLifetime = %v, want a bound of at most 30m", cfg.MaxConnLifetime)
	}
	if cfg.HealthCheckPeriod > 15*time.Second || cfg.HealthCheckPeriod == 0 {
		t.Fatalf("HealthCheckPeriod = %v, want at most 15s", cfg.HealthCheckPeriod)
	}
}

// A caller who knows there is no pooler in the path must be able to say so.
func TestPoolerSafeDefaultsRespectTheDSN(t *testing.T) {
	cfg, err := pgxpool.ParseConfig(
		"postgres://user:pass@127.0.0.1:5432/db?sslmode=disable" +
			"&default_query_exec_mode=cache_describe&pool_max_conn_lifetime=5m&pool_health_check_period=2s")
	if err != nil {
		t.Fatalf("ParseConfig: %v", err)
	}
	applyPoolerSafeDefaults(cfg)

	if cfg.ConnConfig.DefaultQueryExecMode != pgx.QueryExecModeCacheDescribe {
		t.Fatalf("DefaultQueryExecMode = %v, want the DSN's CacheDescribe", cfg.ConnConfig.DefaultQueryExecMode)
	}
	if cfg.MaxConnLifetime != 5*time.Minute {
		t.Fatalf("MaxConnLifetime = %v, want the DSN's 5m", cfg.MaxConnLifetime)
	}
	if cfg.HealthCheckPeriod != 2*time.Second {
		t.Fatalf("HealthCheckPeriod = %v, want the DSN's 2s", cfg.HealthCheckPeriod)
	}
}

func TestStoreSatisfiesTheInterface(t *testing.T) {
	var _ leaderelection.Store = (*Store)(nil)
}
