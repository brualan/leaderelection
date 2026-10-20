//go:build chaos

package tests

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/brualan/leaderelection/tests/harness"
)

// Concurrent collectors must not prevent renewal/watchdog progress. Snapshot
// invariants and cumulative failures must survive an outage followed by a
// successful observation that clears the advisory error flag.
func TestMetricsCollectionDuringNetworkFailure(t *testing.T) {
	for _, backend := range harness.Backends {
		for _, up := range backend.Upstreams() {
			t.Run(up.Name, func(t *testing.T) {
				c := newClusterOn(t, backend, up.Addr, 8)
				stop := make(chan struct{})
				failures := make(chan string, 4)
				var wg sync.WaitGroup
				for range 4 {
					wg.Add(1)
					go func() {
						defer wg.Done()
						ticker := time.NewTicker(time.Millisecond)
						defer ticker.Stop()
						for {
							select {
							case <-stop:
								return
							case <-ticker.C:
								for _, node := range c.Nodes {
									m := node.Elector.Metrics()
									if m.TermsEndedTotal > m.TermsStartedTotal || m.LeaseLossesTotal+m.LocalExpirationsTotal > m.TermsEndedTotal || m.RenewalsAcceptedTotal > m.Store.Renew.CallsTotal || (m.IsLeader && m.LeaseRemainingSeconds <= 0) || (!m.IsLeader && m.LeaseRemainingSeconds != 0) || m.Store.Renew.TimeoutsTotal > m.Store.Renew.ErrorsTotal {
										failures <- fmt.Sprintf("incoherent snapshot for %s: %+v", node.Name, m)
										return
									}
								}
							}
						}
					}()
				}
				var stopOnce sync.Once
				stopCollectors := func() { stopOnce.Do(func() { close(stop) }); wg.Wait() }
				defer stopCollectors()
				leader := c.AwaitLeader(t, 20*time.Second)
				deadline := time.Now().Add(2 * harness.LeaseDuration)
				for leader.Elector.Metrics().RenewalsAcceptedTotal == 0 {
					if time.Now().After(deadline) {
						t.Fatal("collector load prevented a healthy renewal")
					}
					c.Leader(t)
					time.Sleep(5 * time.Millisecond)
				}
				leader.Proxy.BlackholeReplies(t)
				c.AwaitLeaderOtherThan(t, leader, 25*time.Second)
				c.Sample(t, time.Second)
				broken := leader.Elector.Metrics()
				if broken.IsLeader || broken.LocalExpirationsTotal != 1 || broken.LeaderWorkActive || broken.Store.Renew.ErrorsTotal+broken.Store.Renew.CanceledTotal == 0 {
					t.Fatalf("metrics did not expose self-fencing/failure: %+v", broken)
				}
				leader.Proxy.ClearToxics(t)
				deadline = time.Now().Add(10 * time.Second)
				for leader.Elector.Metrics().HasError {
					if time.Now().After(deadline) {
						t.Fatal("no successful observation after recovery")
					}
					c.Leader(t)
					time.Sleep(5 * time.Millisecond)
				}
				recovered := leader.Elector.Metrics()
				if recovered.Store.Renew.ErrorsTotal < broken.Store.Renew.ErrorsTotal || recovered.Store.Renew.CanceledTotal < broken.Store.Renew.CanceledTotal || recovered.LocalExpirationsTotal != 1 {
					t.Fatal("recovery reset cumulative failure counters")
				}
				stopCollectors()
				select {
				case failure := <-failures:
					t.Fatal(failure)
				default:
				}
				c.Observer.Check(t)
			})
		}
	}
}
