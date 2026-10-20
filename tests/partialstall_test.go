//go:build chaos || partialstall

package tests

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/brualan/leaderelection"
	"github.com/brualan/leaderelection/tests/harness"
)

// A byte gate forwards a prefix and then silently discards payload in one
// direction. It does not send EOF/RST or impose an established-socket timeout.
// TCP stays open until the peer closes it (e.g. its own operation timeout), or
// until test cleanup. The opposite direction remains working.
type partialByteProxy struct {
	listener             net.Listener
	upstream             string
	wg                   sync.WaitGroup
	mu                   sync.Mutex
	connections          map[net.Conn]struct{}
	closed               bool
	direction            string
	remaining, forwarded int
	stalled              chan struct{}
	signaled             bool
}

func newPartialByteProxy(t *testing.T, upstream string) *partialByteProxy {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &partialByteProxy{listener: listener, upstream: upstream, connections: make(map[net.Conn]struct{}), stalled: make(chan struct{})}
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		for {
			client, err := listener.Accept()
			if err != nil {
				return
			}
			p.mu.Lock()
			if p.closed {
				p.mu.Unlock()
				_ = client.Close()
				return
			}
			p.connections[client] = struct{}{}
			p.wg.Add(1)
			p.mu.Unlock()
			go p.serve(client)
		}
	}()
	t.Cleanup(p.close)
	return p
}

func (p *partialByteProxy) serve(client net.Conn) {
	defer p.wg.Done()
	defer func() { _ = client.Close(); p.mu.Lock(); delete(p.connections, client); p.mu.Unlock() }()
	// Bound connection setup only. The fault itself has no timer/deadline.
	backend, err := net.DialTimeout("tcp", p.upstream, 3*time.Second)
	if err != nil {
		return
	}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		_ = backend.Close()
		return
	}
	p.connections[backend] = struct{}{}
	p.mu.Unlock()
	defer func() { _ = backend.Close(); p.mu.Lock(); delete(p.connections, backend); p.mu.Unlock() }()
	done := make(chan struct{}, 2)
	go func() { p.copy(backend, client, "request"); done <- struct{}{} }()
	go func() { p.copy(client, backend, "reply"); done <- struct{}{} }()
	<-done
	// Only a real peer/socket error or EOF ends a stream; consuming the byte
	// budget never returns from copy and therefore never closes either socket.
	_ = client.Close()
	_ = backend.Close()
	<-done
}

func (p *partialByteProxy) copy(dst, src net.Conn, direction string) {
	buf := make([]byte, 32*1024)
	for {
		n, err := src.Read(buf)
		if n > 0 {
			allowed := n
			p.mu.Lock()
			gated := p.direction == direction
			if gated {
				allowed = min(n, p.remaining)
				p.remaining -= allowed
			}
			p.mu.Unlock()
			if allowed > 0 {
				written, writeErr := dst.Write(buf[:allowed])
				if gated {
					p.mu.Lock()
					p.forwarded += written
					if p.remaining == 0 && !p.signaled && writeErr == nil && written == allowed {
						p.signaled = true
						close(p.stalled)
					}
					p.mu.Unlock()
				}
				if writeErr != nil || written != allowed {
					return
				}
			}
			// Silently consume the rest; never write it to dst and never send EOF.
		}
		if err != nil {
			return
		}
	}
}

func (p *partialByteProxy) arm(direction string, bytes int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.direction = direction
	p.remaining = bytes
}
func (p *partialByteProxy) heal() { p.mu.Lock(); p.direction = ""; p.mu.Unlock() }
func (p *partialByteProxy) close() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	connections := make([]net.Conn, 0, len(p.connections))
	for c := range p.connections {
		connections = append(connections, c)
	}
	p.mu.Unlock()
	_ = p.listener.Close()
	for _, c := range connections {
		_ = c.Close()
	}
	p.wg.Wait()
}

// A partial request leaves the database waiting for the rest of the frame; a
// partial reply leaves the driver waiting despite a potentially committed
// renewal. Both must terminate through the client's timeout and self-fencing,
// not through an injector-generated reset. Run sequentially with other suites.
func TestPartialProtocolBytesThenSilentStall(t *testing.T) {
	for _, route := range []struct {
		backend       *harness.Backend
		name, address string
	}{
		{harness.PostgresBackend, "postgres/direct", "127.0.0.1:55432"},
		{harness.PostgresBackend, "postgres/pgbouncer", "127.0.0.1:56432"},
		{harness.MySQLBackend, "mysql/direct", "127.0.0.1:53306"},
		{harness.MySQLBackend, "mysql/proxysql", "127.0.0.1:56033"},
	} {
		for _, direction := range []string{"request", "reply"} {
			t.Run(route.name+"/"+direction, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				setupCtx, setupCancel := context.WithTimeout(ctx, 20*time.Second)
				defer setupCancel()
				admin, err := route.backend.Open(setupCtx, fmt.Sprintf(route.backend.DSNTemplate, route.backend.Admin))
				if err != nil {
					t.Fatal(err)
				}
				if err := admin.EnsureSchema(setupCtx); err != nil {
					admin.Close()
					t.Fatal(err)
				}
				admin.Close()
				proxy := newPartialByteProxy(t, route.address)
				name := fmt.Sprintf("partial-%d", time.Now().UnixNano())
				observer := harness.NewObserver()
				var stores []harness.Store
				var runs []<-chan error
				// Register after the proxy cleanup so cancellation and driver shutdown run
				// first. Closing the proxy is an emergency cleanup after test assertions.
				t.Cleanup(func() {
					cancel()
					proxy.close()
					for _, done := range runs {
						select {
						case err := <-done:
							if err != nil {
								t.Errorf("Run: %v", err)
							}
						case <-time.After(10 * time.Second):
							t.Error("Run did not stop after connection cleanup")
						}
					}
					for _, store := range stores {
						store.Close()
					}
				})
				start := func(id, address string) *leaderelection.Elector {
					t.Helper()
					store, err := route.backend.Open(setupCtx, fmt.Sprintf(route.backend.DSNTemplate, address))
					if err != nil {
						t.Fatal(err)
					}
					stores = append(stores, store)
					e, err := leaderelection.New(leaderelection.Config{
						Store: store, Name: name, Identity: name + "-" + id,
						LeaseDuration: harness.LeaseDuration, RenewInterval: harness.RenewInterval,
						RetryPeriod: harness.RetryPeriod, OperationTimeout: harness.OperationTimeout, SafetyMargin: harness.SafetyMargin,
						Callbacks: leaderelection.Callbacks{OnStartedLeading: func(ctx context.Context, fence int64) {
							observer.Began(id, fence)
							<-ctx.Done()
							observer.Ended(id, fence)
						}},
					})
					if err != nil {
						t.Fatal(err)
					}
					done := make(chan error, 1)
					runs = append(runs, done)
					go func() { done <- e.Run(ctx) }()
					return e
				}
				wait := func(what string, within time.Duration, condition func() bool) {
					t.Helper()
					deadline := time.Now().Add(within)
					for !condition() {
						if time.Now().After(deadline) {
							t.Fatalf("timed out waiting for %s", what)
						}
						time.Sleep(2 * time.Millisecond)
					}
				}
				first := start("first", proxy.listener.Addr().String())
				wait("healthy established leader and renewal", 10*time.Second, func() bool { return first.Metrics().IsLeader && first.Metrics().RenewalsAcceptedTotal > 0 })
				oldFence, _ := first.Fence()
				followers := []*leaderelection.Elector{start("second", route.address), start("third", route.address)}
				faultAt := time.Now()
				proxy.arm(direction, 2)
				select {
				case <-proxy.stalled:
				case <-time.After(2 * harness.LeaseDuration):
					t.Fatal("partial-byte fault never triggered")
				}
				proxy.mu.Lock()
				forwarded := proxy.forwarded
				proxy.mu.Unlock()
				if forwarded != 2 {
					t.Fatalf("forwarded %d bytes after arming, want exactly 2", forwarded)
				}
				wait("watchdog to fence old leader", harness.LeaseDuration, func() bool { return !first.IsLeader() })
				if time.Since(faultAt) > harness.LeaseDuration+100*time.Millisecond {
					t.Fatal("step-down exceeded lease duration")
				}
				var successor *leaderelection.Elector
				wait("healthy successor", 20*time.Second, func() bool {
					found := 0
					for _, e := range followers {
						if e.IsLeader() {
							successor = e
							found++
						}
					}
					if found > 1 {
						t.Fatal("two healthy followers became leader")
					}
					return found == 1
				})
				fresh, ok := successor.Fence()
				if !ok || fresh <= oldFence {
					t.Fatal("successor did not obtain a fresh fence")
				}
				wait("old callback and operation cancellation", 2*harness.LeaseDuration, func() bool {
					m := first.Metrics()
					return !m.LeaderWorkActive && m.Store.Renew.InFlight == 0 && m.LocalExpirationsTotal == 1
				})
				failed := first.Metrics()
				if failed.Store.Renew.ErrorsTotal+failed.Store.Renew.CanceledTotal == 0 {
					t.Fatal("driver did not terminate the stalled renewal")
				}
				proxy.heal()
				wait("observation after healing", 15*time.Second, func() bool { return !first.Metrics().HasError })
				if first.IsLeader() {
					t.Fatal("healing resurrected stale term")
				}
				observer.Check(t)
				t.Logf("partial %s: forwarded=%d bytes; old_fence=%d successor_fence=%d renew_errors=%d timeouts=%d canceled=%d", direction, forwarded, oldFence, fresh, failed.Store.Renew.ErrorsTotal, failed.Store.Renew.TimeoutsTotal, failed.Store.Renew.CanceledTotal)
			})
		}
	}
}
