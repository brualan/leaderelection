// Package postgres implements [leaderelection.Store] on top of PostgreSQL
// using pgx.
//
// It is designed to behave identically on a direct connection and through
// PgBouncer in transaction pooling mode, which is the usual way a fleet
// spread across clusters reaches a shared database. Nothing here uses
// session-scoped state: no session advisory locks, no LISTEN/NOTIFY, no
// temporary tables, no SET, and no server-side prepared statements. Every
// operation is a single autocommit statement.
//
// The election's guarantees depend on all timestamps coming from the database
// server, so every statement returns the server's current time alongside the
// lease. See the leaderelection package documentation for why.
package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/brualan/leaderelection"
)

// DefaultTable is the table name used when none is configured.
const DefaultTable = "leader_election"

// querier is the slice of pgx a Store uses. Both *pgxpool.Pool and *pgx.Conn
// satisfy it, and so does a mock, which is how the driver's error paths are
// tested without inventing database failures.
type querier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

// Store is a PostgreSQL-backed [leaderelection.Store].
type Store struct {
	db    querier
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
	schema string
	table  string
}

// WithTable selects the table holding the leases. schema may be empty to use
// the connection's search path.
//
// Both identifiers must be simple unquoted names; anything else is rejected
// rather than quoted, because the table name is the only part of these
// statements that cannot be a bind parameter.
func WithTable(schema, table string) Option {
	return func(o *options) {
		o.schema = schema
		o.table = table
	}
}

// New returns a Store that uses db. It does not query or modify the schema.
// Provision the table separately with [SchemaSQL] before running elections.
//
// The caller keeps ownership of the pool: [Store.Close] does not close it. Use
// this when the application already has a pool, and [Connect] when it does
// not.
//
// A pool built by the caller must tolerate a connection pooler if one is in
// the path. In particular its query exec mode must not rely on named prepared
// statements; see [Connect] for the settings this package would have chosen.
func New(db *pgxpool.Pool, opts ...Option) (*Store, error) {
	if db == nil {
		return nil, errors.New("leaderelection/postgres: pool must not be nil")
	}
	return newStore(db, func() {}, opts...)
}

// Connect builds a pool from dsn and returns a Store that owns it.
// It does not create or migrate tables; provision them separately with [SchemaSQL].
//
// The pool is configured to work through PgBouncer in transaction pooling mode
// as well as directly:
//
//   - The query exec mode is [pgx.QueryExecModeExec], which sends each
//     statement without naming a prepared statement on the server. Named
//     prepared statements are per-session, so under transaction pooling they
//     end up on whichever server connection the pooler hands out next and
//     produce "prepared statement does not exist" at random. The cost is
//     negligible for the handful of statements this package runs.
//   - The statement and description caches are disabled for the same reason.
//   - Connection lifetimes and health checks are bounded, so the pool sheds
//     connections that a pooler restart left in an unusable state instead of
//     discovering them one failed election at a time.
//
// Settings expressed in dsn are honoured, with one exception. pgx does not
// distinguish "the DSN asked for cache_statement" from "the DSN said nothing",
// so a DSN that asks for it explicitly is still moved to exec mode. Any other
// exec mode is left alone, and a caller who wants statement caching against a
// direct connection can build the pool themselves and pass it to [New].
func Connect(ctx context.Context, dsn string, opts ...Option) (*Store, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("leaderelection/postgres: parse dsn: %w", err)
	}
	applyPoolerSafeDefaults(cfg)

	pool, err := newPool(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("leaderelection/postgres: connect: %w", err)
	}
	store, err := newStore(pool, pool.Close, opts...)
	if err != nil {
		pool.Close()
		return nil, err
	}
	return store, nil
}

// newPool is indirected so that the failure path below can be exercised.
// pgxpool only fails construction for configurations ParseConfig already
// rejects, so there is no DSN that reaches it.
var newPool = pgxpool.NewWithConfig

// applyPoolerSafeDefaults sets the pool options that make the store work
// behind a transaction pooler, leaving anything the DSN set explicitly alone.
func applyPoolerSafeDefaults(cfg *pgxpool.Config) {
	if cfg.ConnConfig.DefaultQueryExecMode == pgx.QueryExecModeCacheStatement {
		// CacheStatement is pgx's default, so seeing it means the DSN did not
		// ask for a mode and we are free to choose one that survives a pooler.
		cfg.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeExec
	}
	cfg.ConnConfig.StatementCacheCapacity = 0
	cfg.ConnConfig.DescriptionCacheCapacity = 0

	if cfg.MaxConnLifetime == 0 || cfg.MaxConnLifetime > 30*time.Minute {
		cfg.MaxConnLifetime = 30 * time.Minute
	}
	if cfg.HealthCheckPeriod == 0 || cfg.HealthCheckPeriod > 15*time.Second {
		cfg.HealthCheckPeriod = 15 * time.Second
	}
}

func newStore(db querier, close func(), opts ...Option) (*Store, error) {
	o := options{table: DefaultTable}
	for _, opt := range opts {
		opt(&o)
	}
	qualified, err := qualify(o.schema, o.table)
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

// Close releases resources the Store owns. A pool passed to [New] belongs to
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
	if _, err := s.db.Exec(ctx, s.schemaSQL); err != nil {
		return fmt.Errorf("leaderelection/postgres: ensure schema: %w", wrap(err))
	}
	return nil
}

// Acquire implements [leaderelection.Store].
func (s *Store) Acquire(ctx context.Context, name, identity string, ttl time.Duration, payload []byte) (leaderelection.Lease, error) {
	lease, err := s.queryLease(ctx, s.acquireSQL, name, identity, micros(ttl), payload)
	if err != nil {
		// The upsert always produces a row, so "no rows" here is not a lost
		// election but a broken statement or a vanished table.
		if errors.Is(err, pgx.ErrNoRows) {
			return leaderelection.Lease{}, fmt.Errorf("leaderelection/postgres: acquire returned no row for %q", name)
		}
		return leaderelection.Lease{}, fmt.Errorf("leaderelection/postgres: acquire: %w", err)
	}
	lease.Name = name
	return lease, nil
}

// Renew implements [leaderelection.Store].
func (s *Store) Renew(ctx context.Context, name, identity string, fence int64, ttl time.Duration, payload []byte) (leaderelection.Lease, error) {
	lease, err := s.queryLease(ctx, s.renewSQL, name, identity, fence, micros(ttl), payload)
	if err != nil {
		// The guard matched nothing: somebody else holds the lease, or ours
		// expired. Either way this caller is not the leader any more.
		if errors.Is(err, pgx.ErrNoRows) {
			return leaderelection.Lease{}, leaderelection.ErrLeaseLost
		}
		return leaderelection.Lease{}, fmt.Errorf("leaderelection/postgres: renew: %w", err)
	}
	lease.Name = name
	return lease, nil
}

// Release implements [leaderelection.Store].
func (s *Store) Release(ctx context.Context, name, identity string, fence int64) error {
	// A guard that matches nothing is success: the caller wanted to stop being
	// the leader, and it already is not one.
	if _, err := s.db.Exec(ctx, s.releaseSQL, name, identity, fence); err != nil {
		return fmt.Errorf("leaderelection/postgres: release: %w", wrap(err))
	}
	return nil
}

// Inspect implements [leaderelection.Store].
func (s *Store) Inspect(ctx context.Context, name string) (leaderelection.Lease, error) {
	lease, err := s.queryLease(ctx, s.inspectSQL, name)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return leaderelection.Lease{}, leaderelection.ErrNoLease
		}
		return leaderelection.Lease{}, fmt.Errorf("leaderelection/postgres: inspect: %w", err)
	}
	lease.Name = name
	return lease, nil
}

// queryLease runs a statement whose RETURNING or SELECT list is the standard
// lease projection and decodes the single row it produces.
func (s *Store) queryLease(ctx context.Context, sql string, args ...any) (leaderelection.Lease, error) {
	rows, err := s.db.Query(ctx, sql, args...)
	if err != nil {
		return leaderelection.Lease{}, wrap(err)
	}
	defer rows.Close()

	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return leaderelection.Lease{}, wrap(err)
		}
		return leaderelection.Lease{}, pgx.ErrNoRows
	}

	var lease leaderelection.Lease
	if err := rows.Scan(
		&lease.Holder,
		&lease.Fence,
		&lease.AcquiredAt,
		&lease.RenewedAt,
		&lease.ExpiresAt,
		&lease.Payload,
		&lease.Now,
	); err != nil {
		return leaderelection.Lease{}, wrap(err)
	}
	// Draining is what surfaces an error that only appears at the end of the
	// result stream, such as a connection dropped mid-row.
	rows.Close()
	if err := rows.Err(); err != nil {
		return leaderelection.Lease{}, wrap(err)
	}
	return lease, nil
}

// micros converts a duration to the float8 microseconds the statements
// multiply an interval by. Microseconds are Postgres's own resolution for
// timestamptz, so nothing is lost, and float8 represents every value in that
// unit exactly for any duration this package will ever see.
func micros(d time.Duration) float64 {
	return float64(d.Microseconds())
}
