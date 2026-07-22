//go:build soak

// Long-running scenarios, run by hand rather than on every change:
//
//	make test-soak
//	LEADERELECTION_SOAK_DURATION=15m make test-soak
//
// They are separated from the chaos suite by cost, not by kind. A rolling
// restart of eight participants, a pool starved until queries queue, an hour
// of randomised faults -- each of them reaches interleavings the scripted
// scenarios cannot, and none of them belongs in a run somebody is waiting for.
//
// The assertion is the same one every scenario in this module ends with: no
// two participants believed themselves leader at overlapping times.
package tests

import (
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/brualan/leaderelection/tests/harness"
)

// EnvSoakDuration sets how long the randomised soak runs for.
const EnvSoakDuration = "LEADERELECTION_SOAK_DURATION"

func soakDuration(t *testing.T) time.Duration {
	t.Helper()
	raw := os.Getenv(EnvSoakDuration)
	if raw == "" {
		return 5 * time.Minute
	}
	d, err := time.ParseDuration(raw)
	if err != nil {
		t.Fatalf("%s=%q: %v", EnvSoakDuration, raw, err)
	}
	return d
}

// Everything that can go wrong, going wrong at random, for as long as you are
// willing to wait.
//
// The scripted scenarios each isolate one fault so that its consequences are
// legible. This one gives that up on purpose: faults overlap, a participant is
// replaced while the pooler is paused, a lock is taken while a path is
// blackholed. The combinations are where the remaining bugs live, because
// every individual fault has a test above proving it was thought about.
func TestSoakWithChurn(t *testing.T) {
	c := newCluster(t, "pgbouncer:5432", 4)
	docker := harness.NewDocker(harness.ComposeFile())
	db := harness.NewPostgres(docker)

	c.AwaitLeader(t, 30*time.Second)

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	deadline := time.Now().Add(soakDuration(t))

	// The invariant is sampled continuously in the background while faults come
	// and go in the foreground.
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-stop:
				return
			default:
			}
			c.Leader(t)
			time.Sleep(5 * time.Millisecond)
		}
	}()

	replacements := 0
	for time.Now().Before(deadline) {
		node := c.Nodes[rng.Intn(len(c.Nodes))]

		switch rng.Intn(9) {
		case 0:
			node.Proxy.Cut(t)
			time.Sleep(jitter(rng, harness.LeaseDuration))
			node.Proxy.Heal(t)
		case 1:
			node.Proxy.Blackhole(t)
			time.Sleep(jitter(rng, harness.LeaseDuration))
			node.Proxy.ClearToxics(t)
		case 2:
			node.Proxy.BlackholeReplies(t)
			time.Sleep(jitter(rng, harness.LeaseDuration))
			node.Proxy.ClearToxics(t)
		case 3:
			node.Proxy.ResetPeer(t, 0)
			time.Sleep(jitter(rng, harness.LeaseDuration))
			node.Proxy.ClearToxics(t)
		case 4:
			// Some connections degraded and others fine, which is what a
			// congested path actually looks like from a pool.
			node.Proxy.LatencyOn(t, time.Duration(rng.Intn(400))*time.Millisecond,
				50*time.Millisecond, 0.5)
			time.Sleep(jitter(rng, harness.LeaseDuration))
			node.Proxy.ClearToxics(t)
		case 5:
			// A participant leaves and a fresh one takes its place, the way a
			// deployment replaces a pod.
			c.Replace(t, node)
			replacements++
		case 6:
			db.TerminateBackends(t)
			time.Sleep(jitter(rng, harness.RenewInterval))
		case 7:
			db.LockRow(t, c.Election, jitter(rng, harness.LeaseDuration))
			time.Sleep(jitter(rng, harness.LeaseDuration))
		default:
			time.Sleep(jitter(rng, harness.LeaseDuration))
		}
	}

	close(stop)
	<-done

	for _, n := range c.Nodes {
		n.Proxy.ClearToxics(t)
		n.Proxy.Heal(t)
	}

	// Having survived the storm, the cluster must settle back into having a
	// leader rather than deadlocking.
	c.AwaitLeader(t, 60*time.Second)
	c.Observer.Check(t)

	t.Logf("%d participants replaced, %d leadership terms", replacements, c.Observer.Terms())
	if terms := c.Observer.Terms(); terms < 3 {
		t.Fatalf("only %d terms during a soak with injected faults; the faults did not bite", terms)
	}
}

// A deployment rolling through the whole fleet, one participant at a time.
//
// Nothing is broken here, which is what makes it worth its own scenario: this
// is the routine operation, and the failure it looks for is a handover that
// briefly has two participants in it rather than one. Every replacement is a
// fresh identity, so a term can never be continued by the process that
// replaced the one holding it.
func TestRollingRestart(t *testing.T) {
	c := newCluster(t, "pgbouncer:5432", 4)
	c.AwaitLeader(t, 30*time.Second)

	for round := range 3 {
		for i := range c.Nodes {
			replacement := c.Replace(t, c.Nodes[i])
			// A rolling deployment waits for the fleet to be healthy before
			// moving on, and so does this.
			c.AwaitLeader(t, 3*harness.LeaseDuration)
			c.Sample(t, harness.RenewInterval)
			t.Logf("round %d: %s replaced", round, replacement.Name)
		}
	}

	c.AwaitLeader(t, 30*time.Second)
	c.Observer.Check(t)
}

// More participants than the pooler has server connections.
//
// Queries do not fail here, they queue -- and then time out inside the pooler
// rather than at either end of it. A participant cannot tell a query that
// waited too long for a server connection from one the server never answered,
// and it must not treat the difference as permission to keep leading.
func TestPoolerExhaustion(t *testing.T) {
	// The compose file gives PgBouncer a default pool size of five, so eight
	// participants cannot all be talking to the database at once.
	c := newCluster(t, "pgbouncer:5432", 8)

	c.AwaitLeader(t, 45*time.Second)
	c.Sample(t, 20*time.Second)
	c.Observer.Check(t)

	// Contention alone must not cost anybody a term: the pool is small, not
	// broken, and eight followers polling a live lease is the ordinary case
	// this library is deployed into.
	if terms := c.Observer.Terms(); terms != 1 {
		t.Fatalf("leadership changed hands %d times with nothing broken but a busy pool", terms)
	}
}

// Two unrelated elections sharing one table, both under faults.
//
// Elections are supposed to be independent, and the table is supposed to be
// shareable. Under load and with row locks flying around, an implementation
// that took a table-level lock somewhere -- or that matched a name too loosely
// -- would show up as one election stalling the other.
func TestTwoElectionsShareOneTable(t *testing.T) {
	first := newCluster(t, "postgres:5432", 3)
	second := newCluster(t, "postgres:5432", 3)

	first.AwaitLeader(t, 30*time.Second)
	second.AwaitLeader(t, 30*time.Second)

	// Break the first election thoroughly. The second must not notice.
	for _, n := range first.Nodes {
		n.Proxy.Blackhole(t)
	}
	first.AwaitNoLeader(t, 2*harness.LeaseDuration)

	secondLeader := second.Leader(t)
	if secondLeader == nil {
		t.Fatal("the second election lost its leader when the first one lost the database")
	}
	second.Sample(t, 2*harness.LeaseDuration)
	if !secondLeader.Elector.IsLeader() {
		t.Fatalf("%s lost leadership of the second election while the first was partitioned",
			secondLeader.Name)
	}

	for _, n := range first.Nodes {
		n.Proxy.ClearToxics(t)
	}
	recovered := first.AwaitLeader(t, 45*time.Second)
	t.Logf("first election recovered under %s while the second held %s throughout",
		recovered.Name, secondLeader.Name)

	first.Observer.Check(t)
	second.Observer.Check(t)
	if terms := second.Observer.Terms(); terms != 1 {
		t.Fatalf("the second election went through %d terms while the first was broken", terms)
	}
}
