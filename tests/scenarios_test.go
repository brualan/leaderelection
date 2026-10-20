//go:build chaos || soak

// Helpers shared by every scenario suite in this module, whichever tag it is
// built under.
package tests

import (
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/brualan/leaderelection/tests/harness"
)

// Ports are handed out per test so that scenarios can run without colliding on
// the published range. The compose file publishes 17000-17063, and keeps that
// range below the ephemeral one for the reason given there.
const (
	firstPort = 17000
	lastPort  = 17063
)

var nextPort = firstPort

// TestMain clears any proxies an earlier run left behind before the first
// scenario asks for a port, so that a run killed halfway through cannot fail
// the next one for a reason that has nothing to do with it.
//
// It fails the whole suite rather than warning. A chaos suite that cannot reach
// toxiproxy is about to fail every scenario in it, and one message naming the
// cause beats thirty that do not.
func TestMain(m *testing.M) {
	if os.Getenv(processHelperEnv) == "1" {
		os.Exit(runElectionProcess())
	}
	if err := harness.ResetProxies(harness.ToxiproxyURL()); err != nil {
		fmt.Fprintf(os.Stderr, "clearing stale proxies: %v\nrun `make up`\n", err)
		os.Exit(1)
	}
	os.Exit(m.Run())
}

// ports reserves n consecutive published ports, wrapping when the range runs
// out. Scenarios run one at a time, so a wrap only ever reuses ports whose
// proxies the previous scenario has already torn down.
func ports(t *testing.T, n int) int {
	t.Helper()
	if nextPort+n > lastPort {
		nextPort = firstPort
	}
	base := nextPort
	nextPort += n
	return base
}

func newCluster(t *testing.T, upstream string, nodes int) *harness.Cluster {
	t.Helper()
	return newClusterOn(t, harness.PostgresBackend, upstream, nodes)
}

// newClusterOn is the same thing for a scenario that names its database. Every
// scenario that can run against either backend goes through here.
func newClusterOn(t *testing.T, backend *harness.Backend, upstream string, nodes int) *harness.Cluster {
	t.Helper()
	return harness.NewCluster(t, harness.Options{
		Nodes:    nodes,
		Backend:  backend,
		Upstream: upstream,
		BasePort: ports(t, nodes),
	})
}

// admin runs a command on PgBouncer's admin console.
func admin(t *testing.T, docker *harness.Docker, command string) {
	t.Helper()
	docker.Exec(t, "pgbouncer", "sh", "-c", adminCommand(command))
}

func tryAdmin(docker *harness.Docker, command string) error {
	return docker.TryExec("pgbouncer", "sh", "-c", adminCommand(command))
}

func adminCommand(command string) string {
	return "PGPASSWORD=leaderelection psql -h 127.0.0.1 -p 5432 " +
		"-U leaderelection -d pgbouncer -c '" + command + "'"
}

func jitter(rng *rand.Rand, base time.Duration) time.Duration {
	return base/2 + time.Duration(rng.Int63n(int64(base)))
}

// unseat takes the lease away from the participant holding it, by cutting it
// off for longer than it is allowed to keep believing it leads.
//
// The hold is at least a whole lease, and that is the part worth being careful
// about. A participant steps down SafetyMargin *before* its lease expires, so
// anything shorter than LeaseDuration is a coin toss: the fault lands, the
// renewals it interrupted were not needed yet, and the leader carries on having
// noticed nothing. A randomised scenario that wants handovers has to ask for
// them; asking for them is what this does.
func unseat(t *testing.T, rng *rand.Rand, leader *harness.Node) {
	t.Helper()
	if rng.Intn(2) == 0 {
		// A severed path and a silent one fail differently: one kills the
		// in-flight query and the other leaves it hanging with nothing to
		// react to. Both must cost the leader its term.
		leader.Proxy.Cut(t)
	} else {
		leader.Proxy.Blackhole(t)
	}
	time.Sleep(harness.LeaseDuration + jitter(rng, harness.LeaseDuration))
	leader.Proxy.ClearToxics(t)
	leader.Proxy.Heal(t)
}
