//go:build chaos

// Chaos scenarios for the leader election, run against real PostgreSQL and
// PgBouncer with faults injected between the participants and the database.
//
//	make test-chaos
//
// Every scenario ends with the same assertion: no two participants believed
// themselves leader at overlapping times. Everything before that is setup.
package tests

import (
	"math/rand"
	"testing"
	"time"

	"github.com/brualan/leaderelection/tests/harness"
)

// Both paths to the database get the same treatment. A pooler in transaction
// mode fails differently from a direct connection -- it can be restarted out
// from under a live client, and it can run out of server connections -- so a
// scenario that only ever ran directly proves less than half of what it looks
// like it proves.
var upstreams = []struct {
	name string
	addr string
}{
	{"direct", "postgres:5432"},
	{"pgbouncer", "pgbouncer:5432"},
}

// The baseline: nothing is broken, leadership is stable, and exactly one node
// holds it. A scenario suite whose baseline is flaky proves nothing.
func TestSteadyState(t *testing.T) {
	for _, up := range upstreams {
		t.Run(up.name, func(t *testing.T) {
			c := newCluster(t, up.addr, 3)
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
func TestLeaderIsPartitioned(t *testing.T) {
	for _, up := range upstreams {
		t.Run(up.name, func(t *testing.T) {
			c := newCluster(t, up.addr, 3)
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
// stalled leader and a second one, because every other signal says the
// database is fine.
func TestLeaderIsBlackholed(t *testing.T) {
	for _, up := range upstreams {
		t.Run(up.name, func(t *testing.T) {
			c := newCluster(t, up.addr, 3)
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

// The database freezes with every connection intact. Nobody may lead while it
// is frozen, and leadership must resume once it thaws.
func TestDatabaseIsPaused(t *testing.T) {
	c := newCluster(t, "postgres:5432", 3)
	docker := harness.NewDocker(harness.ComposeFile())

	c.AwaitLeader(t, 20*time.Second)

	docker.Pause(t, "postgres")
	// Every participant is now equally blind, so the correct outcome is that
	// none of them leads.
	c.AwaitNoLeader(t, 2*harness.LeaseDuration)
	c.Sample(t, 3*time.Second)

	docker.Unpause(t, "postgres")
	c.AwaitLeader(t, 30*time.Second)
	c.Observer.Check(t)
}

// The database goes away and comes back. Unlike a pause, this drops every
// connection, so the pools have to rebuild before anyone can lead again.
func TestDatabaseRestarts(t *testing.T) {
	c := newCluster(t, "postgres:5432", 3)
	docker := harness.NewDocker(harness.ComposeFile())

	c.AwaitLeader(t, 20*time.Second)

	docker.Stop(t, "postgres")
	c.AwaitNoLeader(t, 2*harness.LeaseDuration)

	docker.Start(t, "postgres")
	c.AwaitLeader(t, 45*time.Second)
	c.Observer.Check(t)
}

// The pooler is restarted underneath live clients. Every server connection it
// was holding disappears, and the pools above it must notice and rebuild
// rather than handing out connections that no longer work.
func TestPgBouncerRestarts(t *testing.T) {
	c := newCluster(t, "pgbouncer:5432", 3)
	docker := harness.NewDocker(harness.ComposeFile())

	c.AwaitLeader(t, 20*time.Second)

	docker.Restart(t, "pgbouncer")

	// Leadership may or may not change hands here; either is correct. What is
	// not correct is two leaders, or nobody leading once the pooler is back.
	c.AwaitLeader(t, 45*time.Second)
	c.Sample(t, 5*time.Second)
	c.Observer.Check(t)
}

// The pooler is told to hold every query, which is what happens during a
// planned failover. Queries are neither answered nor refused.
func TestPgBouncerPauses(t *testing.T) {
	c := newCluster(t, "pgbouncer:5432", 3)
	docker := harness.NewDocker(harness.ComposeFile())

	c.AwaitLeader(t, 20*time.Second)

	admin(t, docker, "PAUSE")
	// Resuming in cleanup too, so that a failed assertion cannot leave the
	// pooler paused for whatever runs next.
	t.Cleanup(func() { _ = tryAdmin(docker, "RESUME") })

	c.AwaitNoLeader(t, 3*harness.LeaseDuration)

	admin(t, docker, "RESUME")
	c.AwaitLeader(t, 30*time.Second)
	c.Observer.Check(t)
}

// The leader dies without releasing anything, the way a killed process does.
// A successor must wait out the lease rather than taking over immediately, and
// must not take over sooner than that.
func TestLeaderCrashes(t *testing.T) {
	for _, up := range upstreams {
		t.Run(up.name, func(t *testing.T) {
			c := newCluster(t, up.addr, 3)
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
// take over in far less than a lease duration. This is the difference between
// a rolling deploy that stalls and one that does not.
func TestGracefulHandoverIsFast(t *testing.T) {
	for _, up := range upstreams {
		t.Run(up.name, func(t *testing.T) {
			c := newCluster(t, up.addr, 3)
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

// Every participant is on a slow, jittery path, the way participants in
// different regions are. Leadership must survive rather than flapping.
func TestHighLatencyIsSurvivable(t *testing.T) {
	c := newCluster(t, "postgres:5432", 3)

	for _, n := range c.Nodes {
		// Well within the operation timeout, so a healthy leader should keep
		// renewing; enough to make the timing anything but comfortable.
		n.Proxy.Latency(t, 120*time.Millisecond, 60*time.Millisecond)
	}

	c.AwaitLeader(t, 30*time.Second)
	c.Sample(t, 10*time.Second)
	c.Observer.Check(t)
}

// A long run with faults arriving at random, which is the only way to reach
// interleavings a scripted scenario never produces.
func TestRandomisedSoak(t *testing.T) {
	if testing.Short() {
		t.Skip("soak test skipped in short mode")
	}

	c := newCluster(t, "pgbouncer:5432", 4)
	c.AwaitLeader(t, 30*time.Second)

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	deadline := time.Now().Add(60 * time.Second)

	// The invariant is sampled continuously in the background while faults
	// come and go in the foreground.
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

	for time.Now().Before(deadline) {
		node := c.Nodes[rng.Intn(len(c.Nodes))]
		switch rng.Intn(4) {
		case 0:
			node.Proxy.Cut(t)
			time.Sleep(jitter(rng, harness.LeaseDuration))
			node.Proxy.Heal(t)
		case 1:
			node.Proxy.Blackhole(t)
			time.Sleep(jitter(rng, harness.LeaseDuration))
			node.Proxy.ClearToxics(t)
		case 2:
			node.Proxy.Latency(t, time.Duration(rng.Intn(300))*time.Millisecond, 50*time.Millisecond)
			time.Sleep(jitter(rng, harness.LeaseDuration))
			node.Proxy.ClearToxics(t)
		case 3:
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
	c.AwaitLeader(t, 45*time.Second)
	c.Observer.Check(t)

	if terms := c.Observer.Terms(); terms < 2 {
		t.Fatalf("only %d leadership terms during a soak with injected faults; "+
			"the faults did not bite and the run proved nothing", terms)
	}
}

// A leader that steps down locally and then recovers must come back with a
// different fence token.
//
// The window this exercises is real and easy to miss: the local deadline is
// deliberately earlier than the database's, by exactly SafetyMargin. Inside
// that window the participant has already given up leadership while its lease
// is still valid on the server, so re-acquiring it gives the database no
// reason to renumber anything. Handing the same token to the new term would
// leave a downstream system unable to tell a write left in flight by the
// abandoned term apart from this one's.
//
// One node on purpose: with no contender, the lease can only come back to the
// participant that just abandoned it.
func TestRecoveringFromAnOwnStepDownTakesAFreshFence(t *testing.T) {
	const (
		lease  = 6 * time.Second
		margin = 3 * time.Second
	)
	c := harness.NewCluster(t, harness.Options{
		Nodes:         1,
		Upstream:      "postgres:5432",
		BasePort:      ports(t, 1),
		LeaseDuration: lease,
		SafetyMargin:  margin,
	})

	leader := c.AwaitLeader(t, 30*time.Second)
	first, ok := leader.Elector.Fence()
	if !ok {
		t.Fatal("leader reported no fence")
	}

	// Queries hang, so the participant steps down at lease-margin with the
	// database still holding a valid lease for it.
	leader.Proxy.Blackhole(t)
	c.AwaitNoLeader(t, 2*lease)

	// Heal well inside the remaining margin, so the lease it finds is its own
	// and still live.
	leader.Proxy.ClearToxics(t)

	c.AwaitLeader(t, 30*time.Second)
	second, ok := leader.Elector.Fence()
	if !ok {
		t.Fatal("recovered leader reported no fence")
	}
	if second <= first {
		t.Fatalf("recovered with fence %d, the same token the abandoned term used (%d)",
			second, first)
	}
	t.Logf("stepped down on fence %d, recovered on %d", first, second)
	c.Observer.Check(t)
}

// The path carries requests to the database and drops every answer.
//
// This is the disagreement a symmetric fault cannot produce. The database sees
// renewals arriving and honours them, so the lease stays alive and stays this
// participant's; the participant sees queries that never return and cannot
// prove it still holds anything. Both are behaving correctly and they believe
// different things, so the only safe move is the one that costs leadership.
func TestRepliesAreLostButRequestsArrive(t *testing.T) {
	c := newCluster(t, "postgres:5432", 3)
	leader := c.AwaitLeader(t, 20*time.Second)

	leader.Proxy.BlackholeReplies(t)

	deadline := time.Now().Add(harness.LeaseDuration)
	for leader.Elector.IsLeader() {
		if time.Now().After(deadline) {
			t.Fatalf("%s kept leading %v after its replies stopped arriving; "+
				"a renewal it cannot see the result of is not a renewal",
				leader.Name, harness.LeaseDuration)
		}
		time.Sleep(5 * time.Millisecond)
	}

	// The database was told about every one of those renewals, so the lease it
	// holds outlives the participant that gave up on it. A successor therefore
	// waits out an expiry measured from the *last renewal that landed*, not
	// from the step-down -- and it must not take over before then.
	successor := c.AwaitLeaderOtherThan(t, leader, 4*harness.LeaseDuration)
	t.Logf("%s took over from %s, whose renewals were arriving all along",
		successor.Name, leader.Name)

	leader.Proxy.ClearToxics(t)
	c.Sample(t, 5*time.Second)
	c.Observer.Check(t)
}

// Connections are reset the moment they are used. The path is up, DNS is fine,
// the pooler is happy -- and every statement dies with an RST.
//
// A pool cannot cache its way out of this: each replacement connection is
// established successfully and destroyed on first use, so the failure looks
// new every time.
func TestConnectionsAreResetMidQuery(t *testing.T) {
	for _, up := range upstreams {
		t.Run(up.name, func(t *testing.T) {
			c := newCluster(t, up.addr, 3)
			leader := c.AwaitLeader(t, 20*time.Second)

			leader.Proxy.ResetPeer(t, 0)

			deadline := time.Now().Add(harness.LeaseDuration)
			for leader.Elector.IsLeader() {
				if time.Now().After(deadline) {
					t.Fatalf("%s kept leading %v after every connection it made was reset",
						leader.Name, harness.LeaseDuration)
				}
				time.Sleep(5 * time.Millisecond)
			}

			c.AwaitLeaderOtherThan(t, leader, 3*harness.LeaseDuration)
			leader.Proxy.ClearToxics(t)
			c.Sample(t, 5*time.Second)
			c.Observer.Check(t)
		})
	}
}

// The path is throttled until a query cannot finish inside the operation
// timeout. Nothing fails; everything is merely too slow.
//
// This is the failure that produces no error anywhere until the election has
// already given up leadership, which is why it belongs in the suite: the
// timeout is the only thing that notices.
func TestBandwidthCollapse(t *testing.T) {
	c := newCluster(t, "postgres:5432", 3)
	leader := c.AwaitLeader(t, 20*time.Second)

	// A kilobyte a second, against statements of several hundred bytes each
	// way. Comfortably slower than the operation timeout, comfortably faster
	// than nothing at all.
	leader.Proxy.Bandwidth(t, 1)

	deadline := time.Now().Add(harness.LeaseDuration)
	for leader.Elector.IsLeader() {
		if time.Now().After(deadline) {
			t.Fatalf("%s kept leading %v on a path too slow to renew over",
				leader.Name, harness.LeaseDuration)
		}
		time.Sleep(5 * time.Millisecond)
	}

	c.AwaitLeaderOtherThan(t, leader, 3*harness.LeaseDuration)
	leader.Proxy.ClearToxics(t)
	c.AwaitLeader(t, 30*time.Second)
	c.Observer.Check(t)
}

// A path that breaks and heals faster than the lease it is carrying.
//
// Flapping is where a stale term is most likely to be resurrected: a
// participant gives up leadership, the path comes back before the database has
// given the lease to anyone else, and it finds its own lease still sitting
// there. Taking it up again is legitimate; carrying on with the old term's
// fence is not.
func TestFlappingPath(t *testing.T) {
	c := newCluster(t, "pgbouncer:5432", 3)
	c.AwaitLeader(t, 20*time.Second)

	// Aimed at whoever currently holds the lease -- a fault that lands on a
	// follower is not a flap, it is a follower having a quiet moment -- and
	// held for exactly one lease, which is the duration most likely to leave
	// the participant stepping down while its lease is still valid in the
	// database.
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		leader := c.AwaitLeader(t, 20*time.Second)
		leader.Proxy.Cut(t)
		c.Sample(t, harness.LeaseDuration)
		leader.Proxy.Heal(t)
		c.Sample(t, harness.RenewInterval)
	}

	c.AwaitLeader(t, 30*time.Second)
	c.Observer.Check(t)

	// Flapping that never cost anybody a term means the faults landed between
	// renewals every time, and the scenario proved nothing.
	if terms := c.Observer.Terms(); terms < 3 {
		t.Fatalf("only %d terms after 30s of flapping; the faults did not bite", terms)
	}
}

// Every participant loses the database at once.
//
// Nobody can prove anything, so nobody may lead. This is the outcome that
// distinguishes a lease from a lock: the correct answer to "the database is
// gone" is that the service stops, not that whoever held it last carries on.
func TestEveryoneIsPartitioned(t *testing.T) {
	c := newCluster(t, "postgres:5432", 3)
	c.AwaitLeader(t, 20*time.Second)

	for _, n := range c.Nodes {
		n.Proxy.Cut(t)
	}
	c.AwaitNoLeader(t, 2*harness.LeaseDuration)
	c.Sample(t, 3*time.Second)

	for _, n := range c.Nodes {
		n.Proxy.Heal(t)
	}
	c.AwaitLeader(t, 30*time.Second)
	c.Observer.Check(t)
}

// Every participant reads the database's clock as something different, and all
// of them are wrong by days.
//
// The library's central claim is that this changes nothing, because no
// absolute timestamp from the database is ever compared against anything but
// another timestamp from the same read. Handing each participant a different
// offset means that whichever of them leads is leading with a clock that
// agrees with nobody, so the claim is tested rather than assumed.
func TestParticipantsSeeDifferentDatabaseClocks(t *testing.T) {
	c := harness.NewCluster(t, harness.Options{
		Nodes:     3,
		Upstream:  "postgres:5432",
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
