// Package mysql implements [leaderelection.Store] on top of MySQL using
// database/sql and the go-sql-driver/mysql driver. It works against MySQL 8.0
// and later and against MariaDB 10.5 and later.
//
// It is designed to behave identically on a direct connection and through a
// pooler such as ProxySQL, which is the usual way a fleet spread across
// clusters reaches a shared database. Nothing here uses session-scoped state:
// no user variables, no GET_LOCK, no temporary tables, no SET, no transactions,
// and no server-side prepared statements. Every write is a single autocommit
// statement.
//
// The election's guarantees depend on all timestamps coming from the database
// server, so every read returns the server's current time alongside the lease.
// See the leaderelection package documentation for why.
//
// # Differences from the PostgreSQL store
//
// MySQL has no RETURNING clause, so Acquire and Renew write and then read the
// row back, rather than doing both in one statement. Both operations therefore
// cost two round trips instead of one, and an OperationTimeout has to cover
// both. The safety argument does not change: a lease is reported as held only
// when a snapshot of the row says it is held, so a race between the write and
// the read can only ever understate the caller's standing. See sql.go.
package mysql

import (
	"context"
	"database/sql"
	sqldriver "database/sql/driver"
	"errors"
	"fmt"
	"runtime"
	"time"

	driver "github.com/go-sql-driver/mysql"

	"github.com/brualan/leaderelection"
)

// DefaultTable is the table name used when none is configured.
const DefaultTable = "leader_election"

// Store is a MySQL-backed [leaderelection.Store].
type Store struct {
	db    *sql.DB
	close func()

	table string

	acquireSQL string
	renewSQL   string
	releaseSQL string
	inspectSQL string
	schemaSQL  string
}

// Option configures a [Store].
type Option func(*options)

type options struct {
	database string
	table    string
}

// WithTable selects the table holding the leases. database may be empty to use
// the connection's default schema.
//
// Both identifiers must be simple unquoted names; anything else is rejected
// rather than quoted, because the table name is the only part of these
// statements that cannot be a bind parameter.
func WithTable(database, table string) Option {
	return func(o *options) {
		o.database = database
		o.table = table
	}
}

// New returns a Store that uses db. It does not query or modify the schema.
// Provision the table separately with [SchemaSQL] before running elections.
//
// The caller keeps ownership of the handle: [Store.Close] does not close it.
// Use this when the application already has a *sql.DB, and [Connect] when it
// does not.
//
// A handle built by the caller needs no particular DSN settings. In particular
// the store reads timestamps whether or not the DSN asked for parseTime, and it
// interprets them as UTC either way, because the columns hold UTC and nothing
// else. It does not, however, get the pool bounds and the pooler-safe parameter
// handling that [Connect] applies; see there for what those are and why.
func New(db *sql.DB, opts ...Option) (*Store, error) {
	if db == nil {
		return nil, errors.New("leaderelection/mysql: db must not be nil")
	}
	return newStore(db, func() {}, opts...)
}

// Connect builds a connection pool from dsn and returns a Store that owns it.
// It does not create or migrate tables; provision them separately with [SchemaSQL].
//
// The pool is configured to work through a connection pooler as well as
// directly:
//
//   - Parameters are interpolated into the statement rather than bound with a
//     server-side prepared statement. A prepared statement lives on one backend
//     connection, so a pooler that hands the next statement to a different one
//     produces "unknown prepared statement handler" at random; it also costs two
//     extra round trips per operation, which this package runs several times a
//     minute forever. Interpolation is done by the driver, which quotes and
//     escapes according to the connection's character set.
//   - Multi-statement mode is refused. Every statement here is a single
//     statement by construction, and leaving the door open combines badly with
//     interpolation for no gain.
//   - Timestamps are read as UTC, which is what the columns hold.
//   - Connection lifetimes and the pool size are bounded, so the pool sheds
//     connections that a pooler restart left in an unusable state instead of
//     discovering them one failed election at a time.
//
// Settings expressed in dsn are honoured, with one exception. The driver does
// not distinguish "the DSN asked for interpolateParams=false" from "the DSN
// said nothing", so a DSN that asks for it explicitly still gets interpolation.
// A caller who needs server-side prepared statements against a direct
// connection can build the handle themselves and pass it to [New].
func Connect(ctx context.Context, dsn string, opts ...Option) (*Store, error) {
	cfg, err := driver.ParseDSN(dsn)
	if err != nil {
		return nil, fmt.Errorf("leaderelection/mysql: parse dsn: %w", err)
	}
	applyPoolerSafeDefaults(cfg)

	connector, err := newConnector(cfg)
	if err != nil {
		return nil, fmt.Errorf("leaderelection/mysql: connect: %w", err)
	}
	db := sql.OpenDB(connector)
	applyPoolBounds(db)

	store, err := newStore(db, func() {
		_ = db.Close() //nolint:errcheck // Store.Close has no error result; shutdown is best effort.
	}, opts...)
	if err != nil {
		_ = db.Close() //nolint:errcheck // Preserve the construction or connection error.
		return nil, err
	}
	// sql.OpenDB is lazy, so without this a DSN naming a host that does not
	// exist would only fail on the first election, minutes later and nowhere
	// near the configuration that caused it.
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close() //nolint:errcheck // Preserve the construction or connection error.
		return nil, fmt.Errorf("leaderelection/mysql: connect: %w", wrap(err))
	}
	return store, nil
}

// newConnector is indirected so that the failure path above can be exercised.
// The driver only rejects configurations ParseDSN has already accepted, so
// there is no DSN that reaches it.
var newConnector = func(cfg *driver.Config) (sqlDriverConnector, error) {
	return driver.NewConnector(cfg)
}

// sqlDriverConnector names what [sql.OpenDB] wants, so that newConnector can be
// replaced with something that fails.
type sqlDriverConnector = sqldriver.Connector

// applyPoolerSafeDefaults sets the connection options that make the store work
// behind a pooler, leaving anything the DSN set explicitly alone.
func applyPoolerSafeDefaults(cfg *driver.Config) {
	// False is the driver's default, so seeing it means the DSN did not ask for
	// a mode and we are free to choose one that survives a pooler.
	cfg.InterpolateParams = true
	cfg.MultiStatements = false
	// The columns hold UTC. Loc only matters when the DSN also asked for
	// parseTime, but setting it unconditionally keeps the two paths through
	// scanTime producing the same instant.
	cfg.Loc = time.UTC
}

// applyPoolBounds keeps the handle from growing without limit and from holding
// connections a pooler has already forgotten about.
func applyPoolBounds(db *sql.DB) {
	// An election is a handful of statements a minute. The bound exists so that
	// a stalled database cannot turn every caller into another connection; it
	// matches what pgxpool picks for the PostgreSQL store.
	maxConns := max(4, runtime.NumCPU())
	db.SetMaxOpenConns(maxConns)
	db.SetMaxIdleConns(maxConns)
	db.SetConnMaxLifetime(30 * time.Minute)
	db.SetConnMaxIdleTime(5 * time.Minute)
}

func newStore(db *sql.DB, close func(), opts ...Option) (*Store, error) {
	o := options{table: DefaultTable}
	for _, opt := range opts {
		opt(&o)
	}
	qualified, err := qualify(o.database, o.table)
	if err != nil {
		return nil, err
	}
	return &Store{
		db:         db,
		close:      close,
		table:      qualified,
		acquireSQL: fmt.Sprintf(acquireSQL, qualified),
		renewSQL:   fmt.Sprintf(renewSQL, qualified),
		releaseSQL: fmt.Sprintf(releaseSQL, qualified),
		inspectSQL: fmt.Sprintf(inspectSQL, qualified),
		schemaSQL:  fmt.Sprintf(schemaSQL, qualified),
	}, nil
}

// Close releases resources the Store owns. A handle passed to [New] belongs to
// the caller and is left open; one created by [Connect] is closed.
func (s *Store) Close() { s.close() }

// Table returns the quoted, fully qualified table this Store operates on.
func (s *Store) Table() string { return s.table }

// EnsureSchema creates the leases table if it does not exist. It requires DDL
// privileges and does not validate or upgrade an existing table.
//
// Deprecated: provision tables through a separate migration process using
// [SchemaSQL]. Use [Store.CheckSchema] for a read-only startup check.
func (s *Store) EnsureSchema(ctx context.Context) error {
	if _, err := s.db.ExecContext(ctx, s.schemaSQL); err != nil {
		return fmt.Errorf("leaderelection/mysql: ensure schema: %w", wrap(err))
	}
	return nil
}

// Acquire implements [leaderelection.Store].
func (s *Store) Acquire(ctx context.Context, name, identity string, ttl time.Duration, payload []byte) (leaderelection.Lease, error) {
	if _, err := s.db.ExecContext(ctx, s.acquireSQL, acquireArgs(name, identity, micros(ttl), payload)...); err != nil {
		return leaderelection.Lease{}, fmt.Errorf("leaderelection/mysql: acquire: %w", wrap(err))
	}

	lease, err := s.queryLease(ctx, name)
	if err != nil {
		// The upsert always leaves a row behind, so "no rows" here is not a
		// lost election but a vanished table or a row deleted underneath us.
		if errors.Is(err, sql.ErrNoRows) {
			return leaderelection.Lease{}, fmt.Errorf("leaderelection/mysql: acquire wrote no row for %q", name)
		}
		return leaderelection.Lease{}, fmt.Errorf("leaderelection/mysql: acquire: %w", err)
	}
	return lease, nil
}

// Renew implements [leaderelection.Store].
//
// The read-back is what decides the outcome, not the row count the update
// reported. Two things follow from that, and both are deliberate. A renewal
// whose row the update did not match cannot be reported as success, because the
// snapshot would have to show this identity holding this fence unexpired -- and
// that is exactly the state the update's guard describes, so it could not have
// been skipped. And a renewal that did apply but whose term ended before the
// read is reported as lost, which is the conservative answer and the correct
// one: by then somebody else may already be leading.
func (s *Store) Renew(ctx context.Context, name, identity string, fence int64, ttl time.Duration, payload []byte) (leaderelection.Lease, error) {
	if _, err := s.db.ExecContext(ctx, s.renewSQL, micros(ttl), payload, name, identity, fence); err != nil {
		return leaderelection.Lease{}, fmt.Errorf("leaderelection/mysql: renew: %w", wrap(err))
	}

	lease, err := s.queryLease(ctx, name)
	if err != nil {
		// The record is gone, so whatever this caller held, it does not hold it
		// now. That is a lost lease, not an outage.
		if errors.Is(err, sql.ErrNoRows) {
			return leaderelection.Lease{}, leaderelection.ErrLeaseLost
		}
		return leaderelection.Lease{}, fmt.Errorf("leaderelection/mysql: renew: %w", err)
	}
	if lease.Fence != fence || !lease.HeldBy(identity) {
		return leaderelection.Lease{}, leaderelection.ErrLeaseLost
	}
	return lease, nil
}

// Release implements [leaderelection.Store].
func (s *Store) Release(ctx context.Context, name, identity string, fence int64) error {
	// A guard that matches nothing is success: the caller wanted to stop being
	// the leader, and it already is not one.
	if _, err := s.db.ExecContext(ctx, s.releaseSQL, name, identity, fence); err != nil {
		return fmt.Errorf("leaderelection/mysql: release: %w", wrap(err))
	}
	return nil
}

// Inspect implements [leaderelection.Store].
func (s *Store) Inspect(ctx context.Context, name string) (leaderelection.Lease, error) {
	lease, err := s.queryLease(ctx, name)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return leaderelection.Lease{}, leaderelection.ErrNoLease
		}
		return leaderelection.Lease{}, fmt.Errorf("leaderelection/mysql: inspect: %w", err)
	}
	return lease, nil
}

// queryLease reads one election's record together with the server's clock.
//
// It reports [sql.ErrNoRows] when the record does not exist; every caller turns
// that into whatever "no record" means for the operation it is serving.
//
// QueryRowContext rather than QueryContext because it also drains and closes
// the result, which is what surfaces an error that only appears at the end of
// the stream -- a connection dropped mid-row, say. A hand-rolled loop that
// returned as soon as it had scanned would report that outage as a successful
// read of whatever it happened to have decoded.
func (s *Store) queryLease(ctx context.Context, name string) (leaderelection.Lease, error) {
	lease := leaderelection.Lease{Name: name}
	var acquired, renewed, expires, now dbTime

	err := s.db.QueryRowContext(ctx, s.inspectSQL, name).Scan(
		&lease.Holder,
		&lease.Fence,
		&acquired,
		&renewed,
		&expires,
		&lease.Payload,
		&now,
	)
	if err != nil {
		return leaderelection.Lease{}, wrap(err)
	}

	lease.AcquiredAt, lease.RenewedAt = acquired.t, renewed.t
	lease.ExpiresAt, lease.Now = expires.t, now.t
	return lease, nil
}

// dbTime scans a DATETIME(6) whichever way the driver chose to deliver it.
//
// The DSN decides that: with parseTime the driver hands over a time.Time, and
// without it the raw bytes MySQL sent. A store that only handled one of them
// would work or not work depending on a DSN parameter the application chose for
// its own reasons, and would fail by producing a scan error on the first
// election rather than at construction. Both forms are read as UTC, because
// that is what the columns hold -- see schemaSQL.
type dbTime struct{ t time.Time }

// datetimeLayout is MySQL's wire format for DATETIME(6). The fractional part is
// optional so that a column defined without sub-second precision, which is not
// this package's schema but may be somebody's migration, still scans.
const datetimeLayout = "2006-01-02 15:04:05.999999"

func (d *dbTime) Scan(src any) error {
	switch v := src.(type) {
	case time.Time:
		// The driver parsed it. It used the location Connect pinned to UTC; a
		// handle passed to New may have picked another one, in which case the
		// wall-clock reading is wrong by a fixed offset -- the same offset for
		// every timestamp in the row, so every duration derived from them is
		// still exact, which is all the election ever uses.
		d.t = v
		return nil
	case []byte:
		return d.parse(string(v))
	case string:
		return d.parse(v)
	}
	return fmt.Errorf("leaderelection/mysql: cannot read a timestamp from %T", src)
}

func (d *dbTime) parse(s string) error {
	t, err := time.ParseInLocation(datetimeLayout, s, time.UTC)
	if err != nil {
		return fmt.Errorf("leaderelection/mysql: unreadable timestamp %q: %w", s, err)
	}
	d.t = t
	return nil
}

// acquireArgs lays out the parameters [acquireSQL] expects.
//
// The identity appears five times and the payload twice because each condition
// and the value it guards are separate expressions, and MySQL has no way to
// name one of them once. Keeping the list here, next to nothing else, is what
// makes it checkable against the statement.
func acquireArgs(name, identity string, us int64, payload []byte) []any {
	return []any{
		// INSERT ... VALUES
		name, identity, us, payload,
		// holder      = CASE WHEN ... OR holder = ? THEN ?
		identity, identity,
		// renewed_at  = CASE WHEN holder = ?
		identity,
		// payload     = CASE WHEN holder = ? THEN ?
		identity, payload,
		// expires_at  = CASE WHEN holder = ? THEN ... INTERVAL ? MICROSECOND
		identity, us,
	}
}

// micros converts a duration to the whole microseconds the statements add to
// the server clock. Microseconds are DATETIME(6)'s own resolution, so nothing
// that a lease could express is lost.
func micros(d time.Duration) int64 {
	return d.Microseconds()
}
