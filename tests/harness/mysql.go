package harness

import (
	"fmt"
	"io"
	"log"
	"strconv"
	"strings"
	"testing"
	"time"

	driver "github.com/go-sql-driver/mysql"
)

// The MySQL counterpart of postgres.go: faults that live inside the database
// rather than on the way to it.
//
// The failures are the same ones -- a migration holding a lock, a failover
// leaving the server read-only, an administrator disposing of the sessions --
// but nothing about how they are caused carries over, because the two servers
// disagree about almost every detail of how an operator breaks them. What has
// to carry over is the election's answer, which is that nobody leads until the
// database can prove who does.
//
// Everything here runs the mysql client inside the mysql container, so the
// faults do not travel over the proxies and cannot be cut off by the very
// partition a scenario is testing.

func init() {
	// The MySQL driver reports every dropped connection through a package-level
	// logger of its own. Under chaos that is thousands of lines about faults the
	// scenario injected on purpose, and it buries the one line that matters.
	//
	// Silenced here rather than in the store: the logger belongs to the
	// application, and a library that reassigns it takes away a decision that
	// was never its to make.
	_ = driver.SetLogger(log.New(io.Discard, "", 0))
}

// MySQL runs administrative statements against the test database.
type MySQL struct {
	docker *Docker
}

// NewMySQL returns a controller for the MySQL server in the compose project.
func NewMySQL(docker *Docker) *MySQL { return &MySQL{docker: docker} }

// Exec runs one or more statements and returns their output.
func (m *MySQL) Exec(t *testing.T, sql string) string {
	t.Helper()
	return m.docker.Exec(t, "mysql", "sh", "-c", mysqlClient(sql))
}

// LockTable takes a table-level write lock on the election table and holds it
// for d, in a session of its own.
//
// This is the cruellest thing an operator can do to a lease without touching
// the network. Every connection stays up, every packet arrives, and every
// statement blocks -- so a leader's renewals hang exactly as they would behind
// a blackholed route, with nothing at the network layer to blame. A migration
// tool doing an ALTER TABLE produces precisely this.
func (m *MySQL) LockTable(t *testing.T, d time.Duration) {
	t.Helper()
	m.hold(t, fmt.Sprintf("LOCK TABLES %s WRITE", DefaultTable), "UNLOCK TABLES", d)
}

// LockRow takes a row lock on one election's record and holds it for d.
//
// Narrower than [MySQL.LockTable] and more interesting for it: reads still
// succeed, so a follower keeps seeing a healthy election with a live leader
// while that leader cannot renew a thing.
func (m *MySQL) LockRow(t *testing.T, election string, d time.Duration) {
	t.Helper()
	m.hold(t,
		fmt.Sprintf("START TRANSACTION; SELECT 1 FROM %s WHERE name = %s FOR UPDATE",
			DefaultTable, quote(election)),
		"COMMIT", d)
}

// hold runs a statement, sleeps for d in the same session so that whatever the
// statement took stays taken, and then releases it.
func (m *MySQL) hold(t *testing.T, take, release string, d time.Duration) {
	t.Helper()
	statement := fmt.Sprintf("%s; DO SLEEP(%.3f); %s", take, d.Seconds(), release)
	m.docker.ExecDetached(t, "mysql", "sh", "-c", mysqlClient(statement))
	// The lock is taken asynchronously, and a scenario that cut its faults in
	// before the lock existed would be testing nothing. Waiting for the session
	// to appear is cheap and makes the scenario mean what it says.
	m.awaitSession(t, "SLEEP")
}

// awaitSession blocks until a session running the given text exists.
func (m *MySQL) awaitSession(t *testing.T, needle string) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		out := m.Exec(t, "SELECT count(*) FROM information_schema.processlist "+
			"WHERE id <> CONNECTION_ID() AND info LIKE '%"+needle+"%'")
		if strings.TrimSpace(out) != "0" {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("no session running %q appeared within 15s", needle)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// TerminateBackends kills every session belonging to the application user.
//
// This is what a failover does to the connections it inherits, and what an
// operator does to a database that is misbehaving. From the participant's side
// a statement dies mid-flight with no answer and no way to know whether it ran,
// which is the outcome the whole design treats as "not leader".
//
// It reports how many sessions it killed, so that a scenario can tell a
// participant that survived the fault from one that never saw it.
func (m *MySQL) TerminateBackends(t *testing.T) int {
	t.Helper()

	// Two steps rather than one, because MySQL has no way to kill the result of
	// a query: KILL takes a literal id. The window between reading the ids and
	// killing them is harmless -- an id that has gone away by then is simply an
	// error this ignores.
	out := m.Exec(t, "SELECT id FROM information_schema.processlist "+
		"WHERE user = 'leaderelection' AND id <> CONNECTION_ID()")

	killed := 0
	var kills strings.Builder
	for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
		id := strings.TrimSpace(line)
		if id == "" {
			continue
		}
		if _, err := strconv.ParseUint(id, 10, 64); err != nil {
			t.Fatalf("processlist returned %q, which is not a session id", id)
		}
		fmt.Fprintf(&kills, "KILL %s; ", id)
		killed++
	}
	if killed > 0 {
		// A session that ended on its own between the two statements makes KILL
		// fail, and that is not the scenario failing.
		_ = m.docker.TryExec("mysql", "sh", "-c", mysqlClient(kills.String()))
	}
	return killed
}

// SetReadOnly refuses every write, or allows them again.
//
// A promoted replica and a half-finished failover both look like this: reads
// work, the connection is fine, and every write comes back as error 1290. The
// application user has no SUPER privilege, so read_only applies to it and to
// nothing this harness does. Nobody may lead while it lasts, and everybody must
// recover when it lifts.
func (m *MySQL) SetReadOnly(t *testing.T, readOnly bool) {
	t.Helper()
	value := "OFF"
	if readOnly {
		value = "ON"
	}
	m.Exec(t, "SET GLOBAL read_only = "+value)

	if readOnly {
		// Restoring on the way out even if the scenario fails: a database left
		// read-only would fail everything that runs after it, for a reason
		// nowhere near the real one.
		t.Cleanup(func() {
			_ = m.docker.TryExec("mysql", "sh", "-c", mysqlClient("SET GLOBAL read_only = OFF"))
		})
	}
}

// mysqlClient wraps statements in the command that runs them inside the
// container.
func mysqlClient(sql string) string {
	// -N -B keeps the output bare and tab separated, so a caller that reads one
	// value does not have to parse a table to find it. root rather than the
	// application user because half of these need privileges the application
	// deliberately does not have -- and because a fault must not be one of the
	// sessions it is about to kill.
	return "MYSQL_PWD=root mysql -h 127.0.0.1 -u root -N -B -D leaderelection -e " + shellQuote(sql)
}
