//go:build chaos

package tests

import (
	"testing"
	"time"

	"github.com/brualan/leaderelection/tests/harness"
)

// Compound failures across every supported route: a degraded leader and an
// isolated follower leave just one reachable candidate; then that candidate
// loses replies too. Recovery starts with the original, stale participant.
func TestCascadingNetworkFailures(t *testing.T) {
	for _, backend := range harness.Backends {
		for _, up := range backend.Upstreams() {
			t.Run(up.Name, func(t *testing.T) {
				c := newClusterOn(t, backend, up.Addr, 3)
				original := c.AwaitLeader(t, 20*time.Second)
				firstFence, ok := original.Elector.Fence()
				if !ok {
					t.Fatal("initial leader has no fence")
				}
				var survivor *harness.Node
				for _, node := range c.Nodes {
					if node == original {
						continue
					}
					if survivor == nil {
						survivor = node
					} else {
						node.Proxy.Cut(t)
					}
				}
				original.Proxy.Latency(t, 2*harness.OperationTimeout, 0)
				successor := c.AwaitLeaderOtherThan(t, original, 20*time.Second)
				if successor != survivor {
					t.Fatal("isolated participant became leader")
				}
				secondFence, ok := successor.Elector.Fence()
				if !ok || secondFence <= firstFence {
					t.Fatal("successor did not receive a fresh fence")
				}
				successor.Proxy.BlackholeReplies(t)
				c.AwaitNoLeader(t, 2*harness.LeaseDuration)
				// Sample alone permits leaders; explicitly require absence throughout the
				// outage, so a transient unsafe re-election cannot pass unnoticed.
				deadline := time.Now().Add(harness.LeaseDuration)
				for time.Now().Before(deadline) {
					if leader := c.Leader(t); leader != nil {
						t.Fatalf("%s led during the total outage", leader.Name)
					}
					time.Sleep(5 * time.Millisecond)
				}
				original.Proxy.ClearToxics(t)
				recovered := c.AwaitLeader(t, 20*time.Second)
				if recovered != original {
					t.Fatal("unreachable participant won recovery")
				}
				recoveredFence, ok := recovered.Elector.Fence()
				if !ok || recoveredFence <= secondFence {
					t.Fatal("recovered participant reused a previous fence")
				}
				for _, node := range c.Nodes {
					node.Proxy.ClearToxics(t)
					node.Proxy.Heal(t)
				}
				c.Sample(t, 2*harness.LeaseDuration)
				if !original.Elector.IsLeader() {
					t.Fatal("recovery failed to stabilize")
				}
				c.Observer.Check(t)
			})
		}
	}
}
