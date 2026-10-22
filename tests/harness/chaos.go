// Package harness runs a fleet of leader-election participants against real
// infrastructure and breaks that infrastructure underneath them.
//
// The participants live in one process, which is what makes the safety check
// possible: they share a monotonic clock, so the intervals during which each
// believed itself leader can be compared directly. Two intervals that overlap
// by a nanosecond is the failure this library exists to prevent, and nothing
// short of a common clock can detect it.
package harness

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os/exec"
	"strings"
	"testing"
	"time"
)

// Toxiproxy drives the proxy sitting between the participants and the
// database. Its REST API is small enough that a dependency would cost more
// than it saves.
type Toxiproxy struct {
	base   string
	client *http.Client
}

// NewToxiproxy returns a client for the control API, failing the test if it is
// not reachable: a chaos test that silently ran without chaos is worse than
// one that did not run.
func NewToxiproxy(t *testing.T, base string) *Toxiproxy {
	t.Helper()
	p := &Toxiproxy{base: strings.TrimSuffix(base, "/"), client: &http.Client{Timeout: 10 * time.Second}}
	if err := p.do(http.MethodGet, "/proxies", nil, nil); err != nil {
		t.Fatalf("toxiproxy is not reachable at %s: %v; run `make up`", base, err)
	}
	return p
}

// ResetProxies removes every proxy registered with the toxiproxy at base.
//
// Each proxy is deleted when the test that created it ends, which covers a test
// that fails and not a run that is killed -- a timeout, a ^C, a panic on the
// wrong goroutine. Those leave proxies holding the published ports, and the
// next run fails at startup with a bind error that says nothing about what
// caused it. Clearing the slate before the first scenario is cheaper than
// diagnosing that twice.
//
// It also means one toxiproxy serves one suite at a time: a second suite
// starting would delete the first one's proxies. They could never have shared
// it anyway, because they draw from the same published port range.
func ResetProxies(base string) error {
	tp := &Toxiproxy{
		base:   strings.TrimSuffix(base, "/"),
		client: &http.Client{Timeout: 10 * time.Second},
	}

	var proxies map[string]json.RawMessage
	if err := tp.do(http.MethodGet, "/proxies", nil, &proxies); err != nil {
		return fmt.Errorf("list proxies at %s: %w", base, err)
	}
	for name := range proxies {
		if err := tp.do(http.MethodDelete, "/proxies/"+name, nil, nil); err != nil {
			return fmt.Errorf("remove stale proxy %s: %w", name, err)
		}
	}
	return nil
}

// Proxy is one TCP path from the test process to a database endpoint.
type Proxy struct {
	tp       *Toxiproxy
	Name     string
	Listen   string // host:port as seen from the test process
	upstream string
}

// CreateProxy publishes a proxy listening on port inside the toxiproxy
// container and forwarding to upstream. The proxy is removed when the test
// ends, so a crashed test cannot leave a partition behind for the next one.
func (p *Toxiproxy) CreateProxy(t *testing.T, name string, port int, upstream string) *Proxy {
	t.Helper()

	// Remove a leftover from an interrupted run before recreating it.
	_ = p.do(http.MethodDelete, "/proxies/"+name, nil, nil)

	body := map[string]any{
		"name":     name,
		"listen":   fmt.Sprintf("0.0.0.0:%d", port),
		"upstream": upstream,
		"enabled":  true,
	}
	if err := p.do(http.MethodPost, "/proxies", body, nil); err != nil {
		t.Fatalf("create proxy %s: %v", name, err)
	}
	t.Cleanup(func() { _ = p.do(http.MethodDelete, "/proxies/"+name, nil, nil) })

	return &Proxy{
		tp:       p,
		Name:     name,
		Listen:   fmt.Sprintf("127.0.0.1:%d", port),
		upstream: upstream,
	}
}

// Cut severs the path abruptly, closing every established connection.
//
// This is a cable pulled out, not a server refusing connections: in-flight
// queries die without an answer, which is the case a client cannot
// distinguish from a slow one.
func (p *Proxy) Cut(t *testing.T) {
	t.Helper()
	if err := p.tp.do(http.MethodPost, "/proxies/"+p.Name, map[string]any{"enabled": false}, nil); err != nil {
		t.Fatalf("cut %s: %v", p.Name, err)
	}
}

// Heal restores the path.
func (p *Proxy) Heal(t *testing.T) {
	t.Helper()
	if err := p.tp.do(http.MethodPost, "/proxies/"+p.Name, map[string]any{"enabled": true}, nil); err != nil {
		t.Fatalf("heal %s: %v", p.Name, err)
	}
}

// Blackhole accepts data and never delivers it, in both directions.
//
// Unlike [Proxy.Cut] the connection stays open, so the client sees a query
// that simply never returns. That is the shape of a paused server or a
// silently dropped route, and it is the case where a leader has to step down
// with no help at all from the database.
func (p *Proxy) Blackhole(t *testing.T) {
	t.Helper()
	for _, stream := range []string{"upstream", "downstream"} {
		p.addToxic(t, "blackhole_"+stream, "timeout", stream, 1, map[string]any{"timeout": 0})
	}
}

// BlackholeReplies delivers everything the participant sends and nothing the
// database answers.
//
// This is the asymmetry a symmetric blackhole cannot produce, and it is the
// one that matters: the database sees renewals arriving and succeeding, so it
// keeps the lease alive and hands it to nobody, while the participant sees
// queries that never return and has to conclude it may no longer be the
// leader. Both sides are behaving correctly and they disagree about who leads.
func (p *Proxy) BlackholeReplies(t *testing.T) {
	t.Helper()
	p.addToxic(t, "blackhole_downstream", "timeout", "downstream", 1, map[string]any{"timeout": 0})
}

// ResetPeer closes connections with an RST after the given delay, which is
// what a firewall dropping state or a server killed mid-query looks like.
//
// Unlike [Proxy.Cut] this leaves the path open: the next connection succeeds
// and is reset in its turn, so the pool above it keeps handing out connections
// that die the moment they are used.
func (p *Proxy) ResetPeer(t *testing.T, after time.Duration) {
	t.Helper()
	for _, stream := range []string{"upstream", "downstream"} {
		p.addToxic(t, "reset_"+stream, "reset_peer", stream, 1, map[string]any{
			"timeout": after.Milliseconds(),
		})
	}
}

// Bandwidth throttles the path to rate kilobytes per second.
//
// A throttled path is worse than a broken one for a lease: it stays up, the
// connections stay healthy, and the queries simply take longer than the
// operation timeout allows. Nothing anywhere reports an error until the
// election has already given up.
func (p *Proxy) Bandwidth(t *testing.T, rate int) {
	t.Helper()
	for _, stream := range []string{"upstream", "downstream"} {
		p.addToxic(t, "bandwidth_"+stream, "bandwidth", stream, 1, map[string]any{"rate": rate})
	}
}

// Latency adds a fixed delay with jitter in both directions, which is what a
// participant in another region experiences on a good day and what a congested
// path looks like on a bad one.
func (p *Proxy) Latency(t *testing.T, latency, jitter time.Duration) {
	t.Helper()
	p.LatencyOn(t, latency, jitter, 1)
}

// LatencyOn adds latency to the given fraction of connections.
//
// A fault that hits every connection is the easy case: the participant sees
// one consistent world and reacts to it. A fault that hits some of them leaves
// a pool where the next connection may be fine or may be hopeless, which is
// what a degraded network actually looks like.
func (p *Proxy) LatencyOn(t *testing.T, latency, jitter time.Duration, toxicity float64) {
	t.Helper()
	for _, stream := range []string{"upstream", "downstream"} {
		p.addToxic(t, "latency_"+stream, "latency", stream, toxicity, map[string]any{
			"latency": latency.Milliseconds(),
			"jitter":  jitter.Milliseconds(),
		})
	}
}

// ClearToxics removes every fault from the path, leaving it healthy.
func (p *Proxy) ClearToxics(t *testing.T) {
	t.Helper()
	var toxics []struct {
		Name string `json:"name"`
	}
	if err := p.tp.do(http.MethodGet, "/proxies/"+p.Name+"/toxics", nil, &toxics); err != nil {
		t.Fatalf("list toxics on %s: %v", p.Name, err)
	}
	for _, toxic := range toxics {
		if err := p.tp.do(http.MethodDelete, "/proxies/"+p.Name+"/toxics/"+toxic.Name, nil, nil); err != nil {
			t.Fatalf("remove toxic %s from %s: %v", toxic.Name, p.Name, err)
		}
	}
}

// addToxic installs one fault on the path. toxicity is the fraction of
// connections it applies to, from 0 to 1.
func (p *Proxy) addToxic(t *testing.T, name, kind, stream string, toxicity float64, attrs map[string]any) {
	t.Helper()
	body := map[string]any{
		"name":       name,
		"type":       kind,
		"stream":     stream,
		"toxicity":   toxicity,
		"attributes": attrs,
	}
	// Replace rather than stack, so a scenario can re-apply a fault.
	_ = p.tp.do(http.MethodDelete, "/proxies/"+p.Name+"/toxics/"+name, nil, nil)
	if err := p.tp.do(http.MethodPost, "/proxies/"+p.Name+"/toxics", body, nil); err != nil {
		t.Fatalf("add toxic %s to %s: %v", name, p.Name, err)
	}
}

func (p *Toxiproxy) do(method, path string, body, out any) error {
	var reader *bytes.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return err
		}
		reader = bytes.NewReader(encoded)
	} else {
		reader = bytes.NewReader(nil)
	}

	ctx, cancel := context.WithTimeout(context.Background(), p.client.Timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, method, p.base+path, reader)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := p.client.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode >= 300 {
		var buf bytes.Buffer
		_, _ = buf.ReadFrom(resp.Body)
		return fmt.Errorf("%s %s: %s: %s", method, path, resp.Status, strings.TrimSpace(buf.String()))
	}
	if out != nil {
		return json.NewDecoder(resp.Body).Decode(out)
	}
	return nil
}

// Docker drives the containers themselves, for the failures a proxy cannot
// produce: a server frozen mid-query, a server that restarts and forgets its
// connections, a pooler that comes back with an empty pool.
type Docker struct {
	composeFile string
}

// NewDocker returns a controller for the compose project holding the test
// infrastructure.
func NewDocker(composeFile string) *Docker { return &Docker{composeFile: composeFile} }

// Pause freezes a container's processes with SIGSTOP.
//
// This is the cruellest failure for a lease-based election and the one worth
// testing hardest: connections stay established, the kernel keeps accepting
// data, and queries simply never come back. Nothing about the network says
// anything is wrong, so the leader has only its own clock to go on.
func (d *Docker) Pause(t *testing.T, service string) {
	t.Helper()
	d.compose(t, "pause", service)
	t.Cleanup(func() { _ = d.try("unpause", service) })
}

// Unpause resumes a paused container.
func (d *Docker) Unpause(t *testing.T, service string) {
	t.Helper()
	d.compose(t, "unpause", service)
}

// Stop shuts a container down, closing every connection.
func (d *Docker) Stop(t *testing.T, service string) {
	t.Helper()
	d.compose(t, "stop", service)
	t.Cleanup(func() { _ = d.try("start", service) })
}

// Start brings a container back and waits for it to be healthy.
func (d *Docker) Start(t *testing.T, service string) {
	t.Helper()
	d.compose(t, "start", service)
	d.WaitHealthy(t, service, 60*time.Second)
}

// Kill stops a container the way a crash does: SIGKILL, no chance to shut
// anything down.
//
// A stopped PostgreSQL closes its connections and flushes; a killed one leaves
// them dangling and comes back through crash recovery, which is a slower and
// less orderly return than the one [Docker.Stop] produces.
func (d *Docker) Kill(t *testing.T, service string) {
	t.Helper()
	d.compose(t, "kill", "-s", "KILL", service)
	t.Cleanup(func() { _ = d.try("start", service) })
}

// Restart bounces a container, which is how a pooler loses every server
// connection it was holding on behalf of its clients.
func (d *Docker) Restart(t *testing.T, service string) {
	t.Helper()
	d.compose(t, "restart", service)
	d.WaitHealthy(t, service, 60*time.Second)
}

// WaitHealthy blocks until the service reports healthy.
func (d *Docker) WaitHealthy(t *testing.T, service string, within time.Duration) {
	t.Helper()
	deadline := time.Now().Add(within)
	for {
		out, err := d.output("ps", "--format", "json", service)
		if err == nil && strings.Contains(out, `"Health":"healthy"`) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s did not become healthy within %v: %s", service, within, out)
		}
		time.Sleep(250 * time.Millisecond)
	}
}

// Exec runs a command inside a service container.
func (d *Docker) Exec(t *testing.T, service string, args ...string) string {
	t.Helper()
	out, err := d.output(append([]string{"exec", "-T", service}, args...)...)
	if err != nil {
		t.Fatalf("exec in %s: %v: %s", service, err, out)
	}
	return out
}

// TryExec runs a command inside a service container and reports failure
// instead of failing the test, for cleanup paths.
func (d *Docker) TryExec(service string, args ...string) error {
	return d.try(append([]string{"exec", "-T", service}, args...)...)
}

// ExecDetached starts a command inside a service container and returns without
// waiting for it.
//
// It is how a scenario holds something -- a lock, a transaction -- for a while
// without holding the test goroutine for the same while.
func (d *Docker) ExecDetached(t *testing.T, service string, args ...string) {
	t.Helper()
	d.compose(t, append([]string{"exec", "-T", "-d", service}, args...)...)
}

func (d *Docker) compose(t *testing.T, args ...string) {
	t.Helper()
	if out, err := d.output(args...); err != nil {
		t.Fatalf("docker compose %s: %v: %s", strings.Join(args, " "), err, out)
	}
}

func (d *Docker) try(args ...string) error {
	_, err := d.output(args...)
	return err
}

func (d *Docker) output(args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	full := append([]string{"compose", "-f", d.composeFile}, args...)
	// Arguments are internal test-fixture commands, passed directly without a shell.
	cmd := exec.CommandContext(ctx, "docker", full...) //nolint:gosec
	out, err := cmd.CombinedOutput()
	return string(out), err
}
