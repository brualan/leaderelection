//go:build chaos || soak

// Helpers shared by every scenario suite in this module, whichever tag it is
// built under.
package tests

import (
	"math/rand"
	"testing"
	"time"

	"github.com/brualan/leaderelection/tests/harness"
)

// Ports are handed out per test so that scenarios can run without colliding on
// the published range. The compose file publishes 57000-57063.
const (
	firstPort = 57000
	lastPort  = 57063
)

var nextPort = firstPort

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
	return harness.NewCluster(t, harness.Options{
		Nodes:    nodes,
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
