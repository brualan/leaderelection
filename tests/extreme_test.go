//go:build chaos && soak

package tests

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/brualan/leaderelection"
	"github.com/brualan/leaderelection/tests/harness"
)

// Explicit opt-in keeps a normal combined-tag test run from unexpectedly
// taking half an hour. The duration measures fault scenarios, not compilation.
func TestExtremeChaos30Minutes(t *testing.T) {
	if os.Getenv("LEADERELECTION_EXTREME") != "1" {
		t.Skip("run make test-extreme")
	}
	duration := 30 * time.Minute
	if raw := os.Getenv("LEADERELECTION_EXTREME_DURATION"); raw != "" {
		var err error
		duration, err = time.ParseDuration(raw)
		if err != nil || duration < 30*time.Minute {
			t.Fatal("extreme duration must be at least 30m")
		}
	}
	seed := time.Now().UnixNano()
	if raw := os.Getenv("LEADERELECTION_EXTREME_SEED"); raw != "" {
		var err error
		seed, err = strconv.ParseInt(raw, 10, 64)
		if err != nil {
			t.Fatal(err)
		}
	}
	scenarios := []struct {
		name string
		run  func(*testing.T)
	}{
		{"synchronized_48_candidate_herd", TestExtremeHerd},
		{"overlapping_network_faults_and_clock_offsets", TestExtremeMixedNetwork},
		{"compound_infrastructure_failure", TestExtremeInfrastructure},
		{"stale_worker_ignores_cancellation", TestExtremeFencedSink},
		{"actual_pool_starvation", TestPoolerServerConnectionsAreExhausted},
		{"whole_process_kill_and_freeze", TestRealProcessFailure},
		{"cascading_failures", TestCascadingNetworkFailures},
		{"postgres_row_lock", TestElectionRowIsLocked},
		{"postgres_table_lock", TestTableIsLockedByAnotherSession},
		{"mysql_row_lock", TestMySQLElectionRowIsLocked},
		{"mysql_table_lock", TestMySQLTableIsLocked},
		{"postgres_read_only", TestDatabaseGoesReadOnly},
		{"mysql_read_only", TestMySQLGoesReadOnly},
		{"lost_postgres_replies", TestRepliesAreLostButRequestsArrive},
		{"lost_mysql_replies", TestMySQLLeaderSeesNoReplies},
		{"connection_resets", TestConnectionsAreResetMidQuery},
		{"mysql_connection_resets", TestMySQLConnectionsDieMidOperation},
		{"flapping", TestFlappingPath},
		{"bandwidth_collapse", TestBandwidthCollapse},
		{"all_paths_partitioned", TestEveryoneIsPartitioned},
		{"rolling_replacement", TestRollingRestart},
		{"election_isolation", TestTwoElectionsShareOneTable},
	}
	rng := rand.New(rand.NewSource(seed))
	started := time.Now()
	t.Logf("EXTREME START duration=%s seed=%d scenarios=%d start=%s", duration, seed, len(scenarios), started.UTC().Format(time.RFC3339))
	completed := 0
	for cycle := 0; time.Since(started) < duration || cycle == 0; cycle++ {
		// Every family runs once per cycle; random order never omits a failure.
		for _, i := range rng.Perm(len(scenarios)) {
			s := scenarios[i]
			t.Logf("EXTREME PROGRESS elapsed=%s completed=%d next=%s", time.Since(started).Round(time.Second), completed, s.name)
			if !t.Run(fmt.Sprintf("cycle_%02d/%s", cycle, s.name), s.run) {
				t.Fatal("extreme campaign failed; refusing to report a completed run")
			}
			completed++
			if cycle > 0 && time.Since(started) >= duration {
				break
			}
		}
	}
	if time.Since(started) < 30*time.Minute {
		t.Fatal("campaign ended before the required 30 minutes")
	}
	t.Logf("EXTREME PASS elapsed=%s completed=%d seed=%d", time.Since(started), completed, seed)
}

type acquireBarrier struct {
	leaderelection.Store
	gate         <-chan struct{}
	arrived      *atomic.Int64
	once         sync.Once
	active, peak *atomic.Int64
}

func (b *acquireBarrier) Acquire(ctx context.Context, name, id string, ttl time.Duration, payload []byte) (leaderelection.Lease, error) {
	b.once.Do(func() { b.arrived.Add(1) })
	select {
	case <-b.gate:
	case <-ctx.Done():
		return leaderelection.Lease{}, ctx.Err()
	}
	n := b.active.Add(1)
	defer b.active.Add(-1)
	for p := b.peak.Load(); n > p && !b.peak.CompareAndSwap(p, n); p = b.peak.Load() {
	}
	return b.Store.Acquire(ctx, name, id, ttl, payload)
}

func TestExtremeHerd(t *testing.T) {
	for _, backend := range harness.Backends {
		for _, up := range backend.Upstreams() {
			t.Run(up.Name, func(t *testing.T) {
				const nodes = 48
				gate := make(chan struct{})
				var arrived, active, peak atomic.Int64
				var open sync.Once
				release := func() { open.Do(func() { close(gate) }) }
				defer release()
				c := harness.NewCluster(t, harness.Options{
					Nodes: nodes, Backend: backend, Upstream: up.Addr, BasePort: ports(t, nodes),
					NodeStore: func(store leaderelection.Store, _ int) leaderelection.Store {
						return &acquireBarrier{Store: store, gate: gate, arrived: &arrived, active: &active, peak: &peak}
					},
				})
				deadline := time.Now().Add(20 * time.Second)
				for arrived.Load() < nodes {
					if time.Now().After(deadline) {
						release()
						t.Fatalf("only %d/%d candidates reached acquire barrier", arrived.Load(), nodes)
					}
					time.Sleep(5 * time.Millisecond)
				}
				released := time.Now()
				release()
				leader := c.AwaitLeader(t, 20*time.Second)
				t.Logf("herd candidates=%d peak_concurrent_acquires=%d initial_election=%s", nodes, peak.Load(), time.Since(released))
				if peak.Load() < 2 {
					t.Fatal("the synchronized burst did not produce concurrent acquires")
				}
				previous, _ := leader.Elector.Fence()
				for round := 0; round < 6; round++ {
					before := time.Now()
					c.Shutdown(t, leader)
					leader = c.AwaitLeaderOtherThan(t, leader, 15*time.Second)
					fence, ok := leader.Elector.Fence()
					if !ok || fence <= previous {
						t.Fatal("release storm reused a fence")
					}
					previous = fence
					t.Logf("release handover=%d latency=%s fence=%d", round, time.Since(before), fence)
					c.Observer.Check(t)
				}
				c.Sample(t, harness.LeaseDuration)
				c.Observer.Check(t)
			})
		}
	}
}

func TestExtremeMixedNetwork(t *testing.T) {
	for _, backend := range harness.Backends {
		for _, up := range backend.Upstreams() {
			t.Run(up.Name, func(t *testing.T) {
				const nodes = 16
				c := harness.NewCluster(t, harness.Options{Nodes: nodes, Backend: backend, Upstream: up.Addr, BasePort: ports(t, nodes), NodeStore: harness.Skew(-7*24*time.Hour, 9*24*time.Hour, -time.Hour, time.Hour)})
				c.AwaitLeader(t, 20*time.Second)
				for round := 0; round < 3; round++ {
					leader := c.AwaitLeader(t, 20*time.Second)
					oldFence, _ := leader.Elector.Fence()
					for i, n := range c.Nodes {
						if n == leader {
							continue
						}
						switch (i + round) % 5 {
						case 0:
							n.Proxy.BlackholeReplies(t)
						case 1:
							n.Proxy.LatencyOn(t, 1100*time.Millisecond, 400*time.Millisecond, 0.75)
						case 2:
							n.Proxy.ResetPeer(t, 50*time.Millisecond)
						case 3:
							n.Proxy.Bandwidth(t, 1)
							// One fifth of followers retains a healthy route for a successor.
						}
					}
					leader.Proxy.Blackhole(t)
					successor := c.AwaitLeaderOtherThan(t, leader, 25*time.Second)
					next, ok := successor.Elector.Fence()
					if !ok || next <= oldFence {
						t.Fatal("mixed failures reused a fence")
					}
					// Kill/replace an isolated incumbent while other broken paths remain.
					c.Replace(t, leader)
					c.Sample(t, time.Second)
					for _, n := range c.Nodes {
						n.Proxy.ClearToxics(t)
						n.Proxy.Heal(t)
					}
					c.AwaitLeader(t, 25*time.Second)
					c.Sample(t, harness.LeaseDuration)
					c.Observer.Check(t)
				}
			})
		}
	}
}

func TestExtremeInfrastructure(t *testing.T) {
	for _, backend := range harness.Backends {
		t.Run(backend.Name, func(t *testing.T) {
			c := newClusterOn(t, backend, backend.Pooled, 8)
			d := harness.NewDocker(harness.ComposeFile())
			leader := c.AwaitLeader(t, 20*time.Second)
			leader.Proxy.BlackholeReplies(t)
			d.Pause(t, backend.Pooler)
			c.AwaitNoLeader(t, 2*harness.LeaseDuration)
			if backend == harness.PostgresBackend {
				harness.NewPostgres(d).TerminateBackends(t)
			} else {
				harness.NewMySQL(d).TerminateBackends(t)
			}
			c.Sample(t, harness.LeaseDuration)
			d.Unpause(t, backend.Pooler)
			for _, n := range c.Nodes {
				n.Proxy.ClearToxics(t)
			}
			c.AwaitLeader(t, 30*time.Second)
			// Hard crash both ends of the pooled path, with clients still running.
			d.Kill(t, backend.Pooler)
			d.Kill(t, backend.Service)
			c.AwaitNoLeader(t, 2*harness.LeaseDuration)
			d.Start(t, backend.Service)
			d.Start(t, backend.Pooler)
			c.AwaitLeader(t, 45*time.Second)
			c.Sample(t, harness.LeaseDuration)
			c.Observer.Check(t)
		})
	}
}

// An in-memory receiver models atomic receiver-side fencing. The stale worker
// deliberately ignores cancellation. This does NOT claim the library can stop
// such a worker or that a real external API automatically enforces fences.
type extremeSink struct {
	mu                 sync.Mutex
	fence              int64
	accepted, rejected int
}

func (s *extremeSink) activate(fence int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if fence > s.fence {
		s.fence = fence
	}
}
func (s *extremeSink) write(fence int64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if fence != s.fence {
		s.rejected++
		return false
	}
	s.accepted++
	return true
}
func TestExtremeFencedSink(t *testing.T) {
	for _, backend := range harness.Backends {
		for _, up := range backend.Upstreams() {
			t.Run(up.Name, func(t *testing.T) {
				c := newClusterOn(t, backend, up.Addr, 4)
				leader := c.AwaitLeader(t, 20*time.Second)
				old, _ := leader.Elector.Fence()
				sink := &extremeSink{}
				sink.activate(old)
				if !sink.write(old) {
					t.Fatal("initial writer rejected")
				}
				stop := make(chan struct{})
				done := make(chan struct{})
				go func() {
					defer close(done)
					ticker := time.NewTicker(time.Millisecond)
					defer ticker.Stop()
					for {
						select {
						case <-stop:
							return
						case <-ticker.C:
							sink.write(old)
						}
					}
				}()
				defer func() { close(stop); <-done }()
				leader.Proxy.BlackholeReplies(t)
				successor := c.AwaitLeaderOtherThan(t, leader, 25*time.Second)
				fresh, ok := successor.Elector.Fence()
				if !ok || fresh <= old {
					t.Fatal("successor did not obtain a greater fence")
				}
				sink.activate(fresh)
				if sink.write(old) {
					t.Fatal("stale external write accepted after successor activation")
				}
				for i := 0; i < 100; i++ {
					if !sink.write(fresh) {
						t.Fatal("current writer rejected")
					}
					if sink.write(old) {
						t.Fatal("cancel-ignoring stale writer accepted")
					}
				}
				c.Sample(t, time.Second)
				leader.Proxy.ClearToxics(t)
				c.Sample(t, harness.LeaseDuration)
				if leader.Elector.IsLeader() {
					t.Fatal("healing resurrected stale leadership")
				}
				sink.mu.Lock()
				t.Logf("receiver accepted=%d rejected_stale=%d old=%d fresh=%d", sink.accepted, sink.rejected, old, fresh)
				sink.mu.Unlock()
				c.Observer.Check(t)
			})
		}
	}
}
