//go:build chaos

// Chaos scenarios where the database itself is the fault.
//
//	make test-chaos
//
// Everything in chaos_test.go breaks the path to the database. These scenarios
// leave the path perfect and break what is at the end of it, which is the
// shape of every failure an operator causes: a migration taking a lock, a
// failover leaving a node read-only, an administrator killing sessions, a
// server that did not shut down so much as stop existing.
//
// They matter because the client-side symptoms are indistinguishable from a
// healthy database that is merely busy. There is no connection error to react
// to, so the only thing standing between a stalled leader and a second one is
// the lease.
package tests

import (
	"testing"
	"time"

	"github.com/brualan/leaderelection/tests/harness"
)

// A migration tool takes an ACCESS EXCLUSIVE lock on the election table.
//
// Every connection stays established and every statement blocks -- including
// the reads, which is what makes this stricter than a row lock. Nobody can
// prove anything about the lease, so nobody may lead.
func TestTableIsLockedByAnotherSession(t *testing.T) {
	c := newCluster(t, "postgres:5432", 3)
	db := harness.NewPostgres(harness.NewDocker(harness.ComposeFile()))

	c.AwaitLeader(t, 20*time.Second)

	// Held for long enough that the incumbent cannot renew even once inside it.
	db.LockTable(t, 3*harness.LeaseDuration)

	c.AwaitNoLeader(t, 2*harness.LeaseDuration)
	c.Sample(t, harness.LeaseDuration)

	c.AwaitLeader(t, 30*time.Second)
	c.Observer.Check(t)
}

// A stray transaction holds a row lock on this election's record.
//
// Narrower than a table lock and nastier for it: reads still work, so every
// participant keeps seeing a healthy election with a live leader, while that
// leader's renewals -- and everyone else's takeover attempts -- block. The
// lease expires with the record perfectly readable and nobody able to touch it.
func TestElectionRowIsLocked(t *testing.T) {
	c := newCluster(t, "postgres:5432", 3)
	db := harness.NewPostgres(harness.NewDocker(harness.ComposeFile()))

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
	// expired the lease looks to whoever reads it.
	c.AwaitNoLeader(t, harness.LeaseDuration)
	c.Sample(t, harness.LeaseDuration)

	c.AwaitLeader(t, 30*time.Second)
	c.Observer.Check(t)
}

// Sessions are killed out from under the participants, repeatedly, the way a
// failover disposes of the connections it inherits.
//
// A statement dies mid-flight with no answer and no way to know whether it
// ran. The election has exactly one correct response to that, and it is to
// assume it did not stay the leader.
func TestBackendsAreTerminated(t *testing.T) {
	c := newCluster(t, "postgres:5432", 3)
	db := harness.NewPostgres(harness.NewDocker(harness.ComposeFile()))

	c.AwaitLeader(t, 20*time.Second)

	killed := 0
	for range 6 {
		killed += db.TerminateBackends(t)
		c.Sample(t, harness.RenewInterval)
	}
	// A round that killed nothing is a round that tested nothing, and the
	// scenario would pass just as happily with the fault removed.
	if killed == 0 {
		t.Fatal("no session was terminated; the participants were not connected")
	}
	t.Logf("terminated %d sessions across six rounds", killed)

	// The pools have to rebuild themselves out of this without any help.
	c.AwaitLeader(t, 30*time.Second)
	c.Sample(t, 5*time.Second)
	c.Observer.Check(t)
}

// A failover leaves the database read-only. Reads succeed, writes come back as
// SQLSTATE 25006, and the connection reports itself perfectly healthy
// throughout.
//
// Nobody may lead while a lease cannot be written, and everybody must recover
// when writes come back -- without a restart, and without having decided
// somewhere that this participant is permanently broken.
func TestDatabaseGoesReadOnly(t *testing.T) {
	c := newCluster(t, "postgres:5432", 3)
	db := harness.NewPostgres(harness.NewDocker(harness.ComposeFile()))

	c.AwaitLeader(t, 20*time.Second)

	db.SetReadOnly(t, true)
	c.AwaitNoLeader(t, 3*harness.LeaseDuration)
	c.Sample(t, harness.LeaseDuration)

	db.SetReadOnly(t, false)
	c.AwaitLeader(t, 30*time.Second)
	c.Observer.Check(t)
}

// The database is killed rather than stopped.
//
// A stopped server closes its connections and flushes on the way out. A killed
// one leaves every connection dangling and comes back through crash recovery,
// so the pools above it hold handles to a server that no longer exists and the
// return is slower and less orderly than a restart.
func TestPostgresIsKilled(t *testing.T) {
	c := newCluster(t, "postgres:5432", 3)
	docker := harness.NewDocker(harness.ComposeFile())

	c.AwaitLeader(t, 20*time.Second)

	docker.Kill(t, "postgres")
	c.AwaitNoLeader(t, 2*harness.LeaseDuration)

	docker.Start(t, "postgres")
	c.AwaitLeader(t, 60*time.Second)
	c.Observer.Check(t)
}
