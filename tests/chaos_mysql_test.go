//go:build chaos

// Chaos scenarios for the MySQL store, run against real MySQL and ProxySQL.
//
//	make test-chaos
//
// The election makes one promise and it is not dialect-specific, so these are
// the same faults the PostgreSQL scenarios inject and they end with the same
// assertion: no two participants believed themselves leader at overlapping
// times. What differs is the machinery -- a different pooler, a different way
// for an operator to take a lock, a different error for a read-only server --
// and the point of running both is that the promise survives all of it.
//
// Two scenarios here have no PostgreSQL counterpart, and they are the reason
// this file is worth more than a copy. The MySQL store cannot return a row from
// a write, so Acquire and Renew are a write followed by a read. Everything that
// can go wrong between two statements can go wrong there, and nowhere in the
// PostgreSQL store.
package tests

import (
	"testing"
	"time"

	"github.com/brualan/leaderelection/tests/harness"
)

// Both routes to MySQL get the same treatment, for the same reason the
// PostgreSQL scenarios run twice: a pooler fails differently from a direct
// connection, and a scenario that only ever ran directly proves less than half
// of what it looks like it proves.
var mysqlUpstreams = []struct {
	name string
	addr string
}{
	{"direct", "mysql:3306"},
	{"proxysql", "proxysql:6033"},
}

func newMySQLCluster(t *testing.T, upstream string, nodes int) *harness.Cluster {
	t.Helper()
	return newClusterOn(t, harness.MySQLBackend, upstream, nodes)
}

// The baseline: nothing is broken, leadership is stable, and exactly one node
// holds it. A scenario suite whose baseline is flaky proves nothing.
func TestMySQLSteadyState(t *testing.T) {
	for _, up := range mysqlUpstreams {
		t.Run(up.name, func(t *testing.T) {
			c := newMySQLCluster(t, up.addr, 3)
			leader := c.AwaitLeader(t, 20*time.Second)

			c.Sample(t, 6*time.Second)

			if !leader.Elector.IsLeader() {
				t.Fatalf("%s lost leadership with nothing broken", leader.Name)
			}
			if terms := c.Observer.Terms(); terms != 1 {
				t.Fatalf("leadership changed hands %d times with nothing broken", terms)
			}
			c.Observer.Check(t)
		})
	}
}

// The leader loses its path to the database while the others keep theirs. This
// is the partition that can produce two leaders, because the isolated node has
// no way to learn that it was replaced.
func TestMySQLLeaderIsPartitioned(t *testing.T) {
	for _, up := range mysqlUpstreams {
		t.Run(up.name, func(t *testing.T) {
			c := newMySQLCluster(t, up.addr, 3)
			leader := c.AwaitLeader(t, 20*time.Second)

			leader.Proxy.Cut(t)

			// The isolated node must give up on its own, before the database
			// could hand its lease to anyone else.
			deadline := time.Now().Add(harness.LeaseDuration)
			for leader.Elector.IsLeader() {
				if time.Now().After(deadline) {
					t.Fatalf("%s still believed it was leader %v after losing the database",
						leader.Name, harness.LeaseDuration)
				}
				time.Sleep(5 * time.Millisecond)
			}

			successor := c.AwaitLeaderOtherThan(t, leader, 20*time.Second)
			t.Logf("%s took over from the partitioned %s", successor.Name, leader.Name)

			// Healing must not resurrect the old term.
			leader.Proxy.Heal(t)
			c.Sample(t, 5*time.Second)
			c.Observer.Check(t)
		})
	}
}

// The path stays up and the packets stop moving. Connections remain
// established, so nothing tells the client anything is wrong; its queries
// simply never return.
//
// This is the case where the watchdog is the only thing standing between a
// stalled leader and a second one, because every other signal says the database
// is fine.
func TestMySQLLeaderIsBlackholed(t *testing.T) {
	for _, up := range mysqlUpstreams {
		t.Run(up.name, func(t *testing.T) {
			c := newMySQLCluster(t, up.addr, 3)
			leader := c.AwaitLeader(t, 20*time.Second)

			leader.Proxy.Blackhole(t)

			deadline := time.Now().Add(harness.LeaseDuration)
			for leader.Elector.IsLeader() {
				if time.Now().After(deadline) {
					t.Fatalf("%s kept leading through a silent path for %v; "+
						"a hung query must not be mistaken for a healthy one",
						leader.Name, harness.LeaseDuration)
				}
				time.Sleep(5 * time.Millisecond)
			}

			c.AwaitLeaderOtherThan(t, leader, 20*time.Second)
			leader.Proxy.ClearToxics(t)
			c.Sample(t, 5*time.Second)
			c.Observer.Check(t)
		})
	}
}

// The leader's renewals reach the database and their answers never come back.
//
// This is the scenario the MySQL store exists to be tested against. A renewal
// is a write and then a read, so a leader here writes a perfectly good renewal,
// extends its own lease on the server, and then learns nothing at all. The
// database thinks it is still the leader. It has to decide that it is not,
// using only its own watchdog -- and it has to decide that before the lease it
// just successfully extended could be handed to anybody else.
func TestMySQLLeaderSeesNoReplies(t *testing.T) {
	c := newMySQLCluster(t, "mysql:3306", 3)
	leader := c.AwaitLeader(t, 20*time.Second)

	leader.Proxy.BlackholeReplies(t)

	deadline := time.Now().Add(harness.LeaseDuration)
	for leader.Elector.IsLeader() {
		if time.Now().After(deadline) {
			t.Fatalf("%s kept leading for %v on renewals it could not confirm; "+
				"a write whose answer never arrived is an unknown outcome, not a success",
				leader.Name, harness.LeaseDuration)
		}
		time.Sleep(5 * time.Millisecond)
	}

	// Nobody else may lead until the lease the stalled leader kept renewing
	// actually runs out, and after that exactly one of them may.
	c.AwaitLeaderOtherThan(t, leader, 4*harness.LeaseDuration)
	leader.Proxy.ClearToxics(t)
	c.Sample(t, 5*time.Second)
	c.Observer.Check(t)
}

// Connections die a short time after they are established, so an operation is
// as likely to be cut between its write and its read-back as anywhere else.
//
// The MySQL store's two-statement Acquire and Renew make this the interesting
// fault: a write that committed and a read that never happened leave the
// participant unable to say whether it is the leader. It must answer "no", and
// it must keep answering "no" for as long as the path keeps failing -- while
// the record on the server says otherwise.
func TestMySQLConnectionsDieMidOperation(t *testing.T) {
	c := newMySQLCluster(t, "proxysql:6033", 3)
	c.AwaitLeader(t, 20*time.Second)

	for _, n := range c.Nodes {
		// Short enough that an operation rarely finishes on a connection it
		// started on, long enough that the handshake usually completes -- so the
		// pool keeps producing connections that die in use rather than never
		// working at all.
		n.Proxy.ResetPeer(t, 40*time.Millisecond)
	}

	// Whether anybody manages to lead through this is not the assertion. That
	// no two participants do is.
	c.Sample(t, 4*harness.LeaseDuration)

	for _, n := range c.Nodes {
		n.Proxy.ClearToxics(t)
	}
	c.AwaitLeader(t, 45*time.Second)
	c.Sample(t, 5*time.Second)
	c.Observer.Check(t)
}

// A migration tool takes a table-level write lock on the election table.
//
// Every connection stays established and every statement blocks -- including
// the reads, which is what makes this stricter than a row lock. Nobody can
// prove anything about the lease, so nobody may lead.
func TestMySQLTableIsLocked(t *testing.T) {
	c := newMySQLCluster(t, "mysql:3306", 3)
	db := harness.NewMySQL(harness.NewDocker(harness.ComposeFile()))

	c.AwaitLeader(t, 20*time.Second)

	// Held for long enough that the incumbent cannot renew even once inside it.
	db.LockTable(t, 3*harness.LeaseDuration)

	c.AwaitNoLeader(t, 2*harness.LeaseDuration)
	c.Sample(t, harness.LeaseDuration)

	c.AwaitLeader(t, 45*time.Second)
	c.Observer.Check(t)
}

// A stray transaction holds a row lock on this election's record.
//
// Narrower than a table lock and nastier for it: reads still work, so every
// participant keeps seeing a healthy election with a live leader, while that
// leader's renewals -- and everyone else's takeover attempts -- block. The lease
// expires with the record perfectly readable and nobody able to touch it.
//
// On MySQL the reads are what makes this worth a scenario of its own, because
// the store reads after every write. A leader whose renewal blocked but whose
// read-back succeeded sees a record that still says it is the leader, with a
// lease that is running out. Believing the record over its own watchdog is
// exactly the mistake this scenario would catch.
func TestMySQLElectionRowIsLocked(t *testing.T) {
	c := newMySQLCluster(t, "mysql:3306", 3)
	db := harness.NewMySQL(harness.NewDocker(harness.ComposeFile()))

	leader := c.AwaitLeader(t, 20*time.Second)
	db.LockRow(t, c.Election, 3*harness.LeaseDuration)

	deadline := time.Now().Add(harness.LeaseDuration)
	for leader.Elector.IsLeader() {
		if time.Now().After(deadline) {
			t.Fatalf("%s kept leading %v while its renewals were blocked on a row lock",
				leader.Name, harness.LeaseDuration)
		}
		time.Sleep(5 * time.Millisecond)
	}

	// And nobody may take over while the record cannot be written, however
	// healthy it looks to whoever reads it.
	c.AwaitNoLeader(t, harness.LeaseDuration)
	c.Sample(t, harness.LeaseDuration)

	c.AwaitLeader(t, 45*time.Second)
	c.Observer.Check(t)
}

// Sessions are killed out from under the participants, repeatedly, the way a
// failover disposes of the connections it inherits.
//
// A statement dies mid-flight with no answer and no way to know whether it ran.
// The election has exactly one correct response to that, and it is to assume it
// did not stay the leader.
func TestMySQLSessionsAreKilled(t *testing.T) {
	c := newMySQLCluster(t, "mysql:3306", 3)
	db := harness.NewMySQL(harness.NewDocker(harness.ComposeFile()))

	c.AwaitLeader(t, 20*time.Second)

	killed := 0
	for range 6 {
		killed += db.TerminateBackends(t)
		c.Sample(t, harness.RenewInterval)
	}
	// A round that killed nothing is a round that tested nothing, and the
	// scenario would pass just as happily with the fault removed.
	if killed == 0 {
		t.Fatal("no session was killed; the participants were not connected")
	}
	t.Logf("killed %d sessions across six rounds", killed)

	// The pools have to rebuild themselves out of this without any help.
	c.AwaitLeader(t, 45*time.Second)
	c.Sample(t, 5*time.Second)
	c.Observer.Check(t)
}

// A failover leaves the database read-only. Reads succeed, writes come back as
// error 1290, and the connection reports itself perfectly healthy throughout.
//
// Nobody may lead while a lease cannot be written, and everybody must recover
// when writes come back -- without a restart, and without having decided
// somewhere that this participant is permanently broken.
func TestMySQLGoesReadOnly(t *testing.T) {
	c := newMySQLCluster(t, "mysql:3306", 3)
	db := harness.NewMySQL(harness.NewDocker(harness.ComposeFile()))

	c.AwaitLeader(t, 20*time.Second)

	db.SetReadOnly(t, true)
	c.AwaitNoLeader(t, 3*harness.LeaseDuration)
	c.Sample(t, harness.LeaseDuration)

	db.SetReadOnly(t, false)
	c.AwaitLeader(t, 45*time.Second)
	c.Observer.Check(t)
}

// The database freezes with every connection intact. Nobody may lead while it
// is frozen, and leadership must resume once it thaws.
func TestMySQLIsPaused(t *testing.T) {
	c := newMySQLCluster(t, "mysql:3306", 3)
	docker := harness.NewDocker(harness.ComposeFile())

	c.AwaitLeader(t, 20*time.Second)

	docker.Pause(t, "mysql")
	// Every participant is now equally blind, so the correct outcome is that
	// none of them leads.
	c.AwaitNoLeader(t, 2*harness.LeaseDuration)
	c.Sample(t, 3*time.Second)

	docker.Unpause(t, "mysql")
	c.AwaitLeader(t, 45*time.Second)
	c.Observer.Check(t)
}

// The database is killed rather than stopped.
//
// A stopped server closes its connections and flushes on the way out. A killed
// one leaves every connection dangling and comes back through InnoDB crash
// recovery, so the pools above it hold handles to a server that no longer
// exists and the return is slower and less orderly than a restart.
func TestMySQLIsKilled(t *testing.T) {
	c := newMySQLCluster(t, "mysql:3306", 3)
	docker := harness.NewDocker(harness.ComposeFile())

	c.AwaitLeader(t, 20*time.Second)

	docker.Kill(t, "mysql")
	c.AwaitNoLeader(t, 2*harness.LeaseDuration)

	docker.Start(t, "mysql")
	c.AwaitLeader(t, 90*time.Second)
	c.Observer.Check(t)
}

// The pooler is restarted underneath live clients. Every backend connection it
// was holding disappears, and the pools above it must notice and rebuild rather
// than handing out connections that no longer work.
func TestProxySQLRestarts(t *testing.T) {
	c := newMySQLCluster(t, "proxysql:6033", 3)
	docker := harness.NewDocker(harness.ComposeFile())

	c.AwaitLeader(t, 20*time.Second)

	docker.Restart(t, "proxysql")

	// Leadership may or may not change hands here; either is correct. What is
	// not correct is two leaders, or nobody leading once the pooler is back.
	c.AwaitLeader(t, 60*time.Second)
	c.Sample(t, 5*time.Second)
	c.Observer.Check(t)
}

// The leader dies without releasing anything, the way a killed process does. A
// successor must wait out the lease rather than taking over immediately, and
// must not take over sooner than that.
func TestMySQLLeaderCrashes(t *testing.T) {
	for _, up := range mysqlUpstreams {
		t.Run(up.name, func(t *testing.T) {
			c := newMySQLCluster(t, up.addr, 3)
			leader := c.AwaitLeader(t, 20*time.Second)

			killedAt := time.Now()
			c.Kill(t, leader)

			successor := c.AwaitLeaderOtherThan(t, leader, 3*harness.LeaseDuration)
			failover := time.Since(killedAt)

			// Taking over faster than the lease would mean the lease was not
			// being honoured.
			if failover < harness.LeaseDuration-harness.SafetyMargin {
				t.Fatalf("%s took over %v after the crash, sooner than the lease allows (%v)",
					successor.Name, failover, harness.LeaseDuration-harness.SafetyMargin)
			}
			t.Logf("failover after an unannounced crash: %v", failover)
			c.Observer.Check(t)
		})
	}
}

// A leader that shuts down cleanly releases its lease, so a successor should
// take over in far less than a lease duration.
func TestMySQLGracefulHandoverIsFast(t *testing.T) {
	for _, up := range mysqlUpstreams {
		t.Run(up.name, func(t *testing.T) {
			c := newMySQLCluster(t, up.addr, 3)
			leader := c.AwaitLeader(t, 20*time.Second)

			stoppedAt := time.Now()
			c.Shutdown(t, leader)

			successor := c.AwaitLeaderOtherThan(t, leader, 2*harness.LeaseDuration)
			failover := time.Since(stoppedAt)

			if failover >= harness.LeaseDuration {
				t.Fatalf("%s took %v to take over from a clean shutdown; releasing "+
					"the lease is supposed to make this far shorter than the lease itself (%v)",
					successor.Name, failover, harness.LeaseDuration)
			}
			t.Logf("failover after a clean shutdown: %v", failover)
			c.Observer.Check(t)
		})
	}
}

// Every participant reads the database's clock through an offset of its own,
// days wide and in both directions.
//
// The library's central claim is that this changes nothing, because no absolute
// timestamp from the database is ever compared against anything but another
// timestamp from the same read. It is worth asserting against this store
// separately: its timestamps arrive as DATETIME values with no time zone at
// all, decoded by the client, which is one more place for an absolute time to
// creep into a comparison.
func TestMySQLParticipantsSeeDifferentDatabaseClocks(t *testing.T) {
	c := harness.NewCluster(t, harness.Options{
		Nodes:     3,
		Backend:   harness.MySQLBackend,
		Upstream:  "mysql:3306",
		BasePort:  ports(t, 3),
		NodeStore: harness.Skew(72*time.Hour, -48*time.Hour, 90*time.Second),
	})

	leader := c.AwaitLeader(t, 20*time.Second)
	c.Sample(t, 5*time.Second)
	if !leader.Elector.IsLeader() {
		t.Fatalf("%s lost leadership with nothing broken but its calendar", leader.Name)
	}

	// And a handover has to work too: the successor's idea of when the lease
	// expires comes from a clock days away from its predecessor's.
	c.Kill(t, leader)
	successor := c.AwaitLeaderOtherThan(t, leader, 3*harness.LeaseDuration)
	t.Logf("%s took over from %s across a %v clock difference",
		successor.Name, leader.Name, 120*time.Hour)

	c.Sample(t, 5*time.Second)
	c.Observer.Check(t)
}
