package mysql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"sync"
	"testing"
	"time"
)

// A database/sql driver whose every outcome is scripted.
//
// Provoking a real server into failing at exactly the right moment -- during
// the scan, at the end of the result stream, on the statement after the one
// that mattered -- is not reliably possible, and those are the paths where a
// mistake turns an outage into two leaders. Everything here exists so the store
// can be pushed down them on demand.
//
// It is a driver rather than a mock of the store's own calls on purpose: the
// code under test then runs against the real database/sql, so the scan
// conversions, the ErrNoRows handling and the draining are the production ones.

// fakeDB scripts one connection's worth of behaviour. It is shared by every
// connection database/sql opens, so it takes a lock.
type fakeDB struct {
	mu sync.Mutex

	// connectErr fails the handshake, the way an unreachable server does.
	connectErr error
	// execErr and queryErr fail the statement itself.
	execErr  error
	queryErr error

	// row is the single row a query yields. A nil row is an empty result.
	row []driver.Value
	// rowErr replaces the row, standing in for a stream that died.
	rowErr error
	// closeErr fails the result set as it is drained, which is where an error
	// that only shows at the end of the stream comes from.
	closeErr error

	// The write and the read are recorded apart, because every operation but
	// Release runs both and a single slot would only ever show the last one.
	lastExecSQL   string
	lastExecArgs  []driver.NamedValue
	lastQuerySQL  string
	lastQueryArgs []driver.NamedValue
	execs         int
	queries       int
}

// openFake returns a *sql.DB backed by f, bounded to one connection so that
// lastSQL means what it says.
func openFake(t *testing.T, f *fakeDB) *sql.DB {
	t.Helper()
	db := sql.OpenDB(f)
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// leaseRow is a well-formed row in the projection every read uses.
func leaseRow() []driver.Value {
	now := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	return []driver.Value{
		[]byte("holder"),
		int64(7),
		now,                                  // a driver with parseTime on
		[]byte("2026-07-22 12:00:00.000000"), // and one without
		"2026-07-22 12:01:00.000000",         // and one that hands over a string
		[]byte("payload"),
		now,
	}
}

func (f *fakeDB) Connect(context.Context) (driver.Conn, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.connectErr != nil {
		return nil, f.connectErr
	}
	return &fakeConn{db: f}, nil
}

func (f *fakeDB) Driver() driver.Driver { return fakeDriver{} }

// fakeDriver exists only because driver.Connector must name one. Nothing opens
// a connection through it: sql.OpenDB goes to Connect above.
type fakeDriver struct{}

func (fakeDriver) Open(string) (driver.Conn, error) { return nil, errors.New("unused") }

// fakeConn implements the context-aware interfaces directly, so database/sql
// never falls back to preparing a statement -- which is also how the real
// driver behaves once Connect has turned parameter interpolation on.
type fakeConn struct{ db *fakeDB }

func (c *fakeConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("leaderelection/mysql: the store must not prepare statements")
}
func (c *fakeConn) Close() error { return nil }
func (c *fakeConn) Begin() (driver.Tx, error) {
	return nil, errors.New("the store must not open transactions")
}

func (c *fakeConn) ExecContext(_ context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	c.db.mu.Lock()
	defer c.db.mu.Unlock()
	c.db.lastExecSQL, c.db.lastExecArgs = query, args
	c.db.execs++
	if c.db.execErr != nil {
		return nil, c.db.execErr
	}
	return driver.RowsAffected(1), nil
}

func (c *fakeConn) QueryContext(_ context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	c.db.mu.Lock()
	defer c.db.mu.Unlock()
	c.db.lastQuerySQL, c.db.lastQueryArgs = query, args
	c.db.queries++
	if c.db.queryErr != nil {
		return nil, c.db.queryErr
	}
	return &fakeRows{db: c.db, remaining: 1}, nil
}

type fakeRows struct {
	db        *fakeDB
	remaining int
}

func (r *fakeRows) Columns() []string {
	return []string{"holder", "fence", "acquired_at", "renewed_at", "expires_at", "payload", "now"}
}

func (r *fakeRows) Close() error { return r.db.closeErr }

func (r *fakeRows) Next(dest []driver.Value) error {
	r.db.mu.Lock()
	defer r.db.mu.Unlock()
	if r.db.rowErr != nil {
		return r.db.rowErr
	}
	if r.remaining == 0 || r.db.row == nil {
		return io.EOF
	}
	r.remaining--
	copy(dest, r.db.row)
	return nil
}

// The fake has to behave enough like a driver for database/sql to drive it, and
// a mistake there would look like a bug in the store.
func TestFakeDriverIsWellFormed(t *testing.T) {
	f := &fakeDB{row: leaseRow()}
	db := openFake(t, f)

	if err := db.PingContext(context.Background()); err != nil {
		t.Fatalf("Ping: %v", err)
	}
	// The store must never reach either of these; the fake refuses them so that
	// a change which starts preparing or transacting fails here rather than in
	// production behind a pooler.
	if stmt, err := db.PrepareContext(context.Background(), "SELECT 1"); err == nil {
		defer func() { _ = stmt.Close() }()
		t.Fatal("the fake accepted a prepared statement")
	}
	if _, err := db.BeginTx(context.Background(), nil); err == nil {
		t.Fatal("the fake accepted a transaction")
	}
	if f.Driver() == nil {
		t.Fatal("Driver() returned nil")
	}
	if _, err := (fakeDriver{}).Open(""); err == nil {
		t.Fatal("fakeDriver.Open succeeded")
	}
}
