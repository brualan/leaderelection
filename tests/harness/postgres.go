package harness

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

// Faults that live inside the database rather than on the way to it.
//
// A proxy can break the path; it cannot make the server itself refuse to
// write, hold a lock, or throw away the sessions the participants are using.
// Those are the failures an operator causes -- a failover, a migration, a
// maintenance window -- and they reach the election through a healthy network,
// which is what makes them worth a suite of their own.
//
// Everything here runs psql inside the postgres container, so the faults do
// not travel over the proxies and cannot be cut off by the very partition a
// scenario is testing.

// DefaultTable is the table the participants use, mirroring
// postgres.DefaultTable. It is spelled out rather than imported so that this
// file does not pull the driver module in for a string.
const DefaultTable = "leader_election"

// Postgres runs administrative statements against the test database.
type Postgres struct {
	docker *Docker
}

// NewPostgres returns a controller for the database in the compose project.
func NewPostgres(docker *Docker) *Postgres { return &Postgres{docker: docker} }

// Exec runs one statement and returns its output.
func (p *Postgres) Exec(t *testing.T, sql string) string {
	t.Helper()
	return p.docker.Exec(t, "postgres", "sh", "-c", psql(sql))
}

// LockTable takes an ACCESS EXCLUSIVE lock on the election table and holds it
// for d, in a session of its own.
//
// This is the cruellest thing an operator can do to a lease without touching
// the network. Every connection stays up, every packet arrives, and every
// statement blocks -- so a leader's renewals hang exactly as they would behind
// a blackholed route, with nothing at the network layer to blame. A migration
// tool doing an ALTER TABLE produces precisely this.
func (p *Postgres) LockTable(t *testing.T, d time.Duration) {
	t.Helper()
	p.hold(t, fmt.Sprintf("LOCK TABLE %s IN ACCESS EXCLUSIVE MODE", DefaultTable), d)
}

// LockRow takes a row lock on one election's record and holds it for d.
//
// Narrower than [Postgres.LockTable] and more interesting for it: reads still
// succeed, so a follower keeps seeing a healthy election with a live leader
// while that leader cannot renew a thing.
func (p *Postgres) LockRow(t *testing.T, election string, d time.Duration) {
	t.Helper()
	p.hold(t, fmt.Sprintf("SELECT 1 FROM %s WHERE name = %s FOR UPDATE",
		DefaultTable, quote(election)), d)
}

// hold runs a statement inside a transaction that then sleeps, so whatever the
// statement took stays taken for d.
func (p *Postgres) hold(t *testing.T, sql string, d time.Duration) {
	t.Helper()
	statement := fmt.Sprintf("BEGIN; %s; SELECT pg_sleep(%.3f); COMMIT;", sql, d.Seconds())
	p.docker.ExecDetached(t, "postgres", "sh", "-c", psql(statement))
	// The lock is taken asynchronously, and a scenario that cut its faults in
	// before the lock existed would be testing nothing. Waiting for the session
	// to appear is cheap and makes the scenario mean what it says.
	p.awaitSession(t, "pg_sleep")
}

// awaitSession blocks until a backend running the given text exists.
func (p *Postgres) awaitSession(t *testing.T, needle string) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		out := p.Exec(t, "SELECT count(*) FROM pg_stat_activity "+
			"WHERE pid <> pg_backend_pid() AND datname = 'leaderelection' "+
			"AND query LIKE '%"+needle+"%'")
		if strings.TrimSpace(out) != "0" {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("no backend running %q appeared within 15s", needle)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// TerminateBackends kills every session that has touched the election table.
//
// This is what a failover does to the connections it inherits, and what an
// operator does to a database that is misbehaving. From the participant's side
// a statement dies mid-flight with no answer and no way to know whether it
// ran, which is the outcome the whole design treats as "not leader".
// It reports how many sessions it killed, so that a scenario can tell a
// participant that survived the fault from one that never saw it.
func (p *Postgres) TerminateBackends(t *testing.T) int {
	t.Helper()
	out := p.Exec(t, "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "+
		"WHERE pid <> pg_backend_pid() AND datname = 'leaderelection' AND query LIKE '%"+DefaultTable+"%'")
	killed := 0
	for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
		if strings.TrimSpace(line) != "" {
			killed++
		}
	}
	return killed
}

// SetReadOnly makes every new transaction read-only, or restores writes.
//
// A promoted standby and a half-finished failover both look like this: reads
// work, the connection is fine, and every write comes back as SQLSTATE 25006.
// Nobody may lead while it lasts, and everybody must recover when it lifts.
func (p *Postgres) SetReadOnly(t *testing.T, readOnly bool) {
	t.Helper()
	value := "off"
	if readOnly {
		value = "on"
	}
	p.Exec(t, "ALTER SYSTEM SET default_transaction_read_only = "+value)
	p.Exec(t, "SELECT pg_reload_conf()")

	if readOnly {
		// Restoring on the way out even if the scenario fails: a database left
		// read-only would fail everything that runs after it, for a reason
		// nowhere near the real one.
		t.Cleanup(func() {
			//nolint:errcheck // Best-effort recovery if the faulted database is unavailable.
			_ = p.docker.TryExec("postgres", "sh", "-c",
				psql("ALTER SYSTEM SET default_transaction_read_only = off"))
			_ = p.docker.TryExec("postgres", "sh", "-c", psql("SELECT pg_reload_conf()")) //nolint:errcheck // Best-effort recovery if the faulted database is unavailable.
		})
	}
}

// psql wraps a statement in the command that runs it inside the container.
func psql(sql string) string {
	// -tA keeps the output a bare value, so a caller that reads one does not
	// have to parse a table to find it.
	return "PGPASSWORD=leaderelection psql -tA -v ON_ERROR_STOP=1 -h 127.0.0.1 " +
		"-U leaderelection -d leaderelection -c " + shellQuote(sql)
}

// shellQuote quotes for the shell running inside the container.
func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

// quote renders a SQL string literal. Election names are generated by the
// harness, but a helper that only works for well-behaved input is a trap for
// whoever adds the first scenario with an awkward one.
func quote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}
