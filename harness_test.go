package leaderelection

import (
	"context"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/brualan/leaderelection/internal/clock"
)

// The election loop is driven entirely by a fake clock, so these tests assert
// on timing without depending on it. Virtual time is pushed forward in small
// steps until the condition under test holds; a step is far smaller than any
// interval the loop uses, so quantisation never hides a violation.
//
// One limit is worth knowing before trusting a measurement here. Time only
// moves while a timer is armed, but during a term two always are -- the renew
// timer and the watchdog -- so virtual time keeps advancing through windows
// where the loop is runnable but not yet scheduled. Measurements are therefore
// sound as upper bounds and only approximate as lower bounds. Assertions that
// need a lower bound should key off something the loop counts, such as how
// many calls it made, rather than off elapsed virtual time.
const (
	virtualStep = 25 * time.Millisecond
	// realTimeout bounds how long a test will spin before declaring the loop
	// stuck. It has nothing to do with the virtual timeline.
	realTimeout = 10 * time.Second
)

// Test timings. They satisfy the config invariant with room to spare, and are
// far enough apart that an off-by-one interval is obvious in a failure.
const (
	testLease   = 15 * time.Second
	testRenew   = 5 * time.Second
	testRetry   = 3 * time.Second
	testOpLimit = 3 * time.Second
	testMargin  = 1500 * time.Millisecond

	// harnessJitterFactor is what a jittered interval shrinks to, given the
	// package's 0.2 jitter fraction and the harness's fixed 0.5 draw.
	harnessJitterFactor = 1 - 0.2*0.5
)

// epoch is an arbitrary fixed origin for virtual time.
var epoch = time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)

// fakeStore answers store calls from programmable handlers. Handlers run on
// the elector's own goroutine, so one that blocks blocks the elector -- which
// is how the "database stopped answering" scenarios are built.
type fakeStore struct {
	mu sync.Mutex

	onInspect func(ctx context.Context, n int) (Lease, error)
	onAcquire func(ctx context.Context, n int) (Lease, error)
	onRenew   func(ctx context.Context, n int, fence int64) (Lease, error)
	onRelease func(ctx context.Context, n int, fence int64) error

	counts   map[string]int
	lastSeen map[string]storeArgs
}

type storeArgs struct {
	name     string
	identity string
	fence    int64
	ttl      time.Duration
	payload  []byte
}

func newFakeStore() *fakeStore {
	return &fakeStore{
		counts:   map[string]int{},
		lastSeen: map[string]storeArgs{},
	}
}

// dispatch bumps the call counter and captures the handler under one lock, so
// that a test which swaps a handler after observing call N knows the swap
// affects call N+1 and no earlier one. Splitting those two steps leaves a
// window where a call is counted but answered by the new handler, and every
// timing measurement downstream inherits the ambiguity.
func (f *fakeStore) dispatch(op string, args storeArgs) (int, any) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.counts[op]++
	f.lastSeen[op] = args
	switch op {
	case "inspect":
		return f.counts[op], f.onInspect
	case "acquire":
		return f.counts[op], f.onAcquire
	case "renew":
		return f.counts[op], f.onRenew
	default:
		return f.counts[op], f.onRelease
	}
}

func (f *fakeStore) count(op string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.counts[op]
}

func (f *fakeStore) args(op string) storeArgs {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.lastSeen[op]
}

// setInspect and friends replace a handler mid-test, which is how a healthy
// database is turned into a broken one.
func (f *fakeStore) setInspect(h func(context.Context, int) (Lease, error)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.onInspect = h
}

func (f *fakeStore) setAcquire(h func(context.Context, int) (Lease, error)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.onAcquire = h
}

func (f *fakeStore) setRenew(h func(context.Context, int, int64) (Lease, error)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.onRenew = h
}

func (f *fakeStore) setRelease(h func(context.Context, int, int64) error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.onRelease = h
}

func (f *fakeStore) Inspect(ctx context.Context, name string) (Lease, error) {
	n, h := f.dispatch("inspect", storeArgs{name: name})
	return h.(func(context.Context, int) (Lease, error))(ctx, n)
}

func (f *fakeStore) Acquire(ctx context.Context, name, identity string, ttl time.Duration, payload []byte) (Lease, error) {
	n, h := f.dispatch("acquire", storeArgs{name: name, identity: identity, ttl: ttl, payload: payload})
	return h.(func(context.Context, int) (Lease, error))(ctx, n)
}

func (f *fakeStore) Renew(ctx context.Context, name, identity string, fence int64, ttl time.Duration, payload []byte) (Lease, error) {
	n, h := f.dispatch("renew", storeArgs{name: name, identity: identity, fence: fence, ttl: ttl, payload: payload})
	return h.(func(context.Context, int, int64) (Lease, error))(ctx, n, fence)
}

func (f *fakeStore) Release(ctx context.Context, name, identity string, fence int64) error {
	n, h := f.dispatch("release", storeArgs{name: name, identity: identity, fence: fence})
	return h.(func(context.Context, int, int64) error)(ctx, n, fence)
}

// harness wires an Elector to a fake clock and a fake store and runs it.
type harness struct {
	t     *testing.T
	clk   *clock.Fake
	store *fakeStore
	e     *Elector

	// Callback observations. Each is appended to under mu.
	mu           sync.Mutex
	startedTerms []int64
	stoppedCount int
	newLeaders   []string
	leaderCtx    context.Context

	cancel   context.CancelFunc
	done     chan error
	finished bool
	runErr   error
}

// newHarness builds an elector whose store always grants leadership on the
// first attempt. Tests override handlers before calling run.
func newHarness(t *testing.T, adjust func(*Config)) *harness {
	t.Helper()

	h := &harness{
		t:     t,
		clk:   clock.NewFake(epoch),
		store: newFakeStore(),
		done:  make(chan error, 1),
	}

	// Default behaviour: an empty election that this participant wins and
	// keeps. Individual tests replace whichever handler they are about.
	h.store.onInspect = func(context.Context, int) (Lease, error) {
		return Lease{}, ErrNoLease
	}
	h.store.onAcquire = func(context.Context, int) (Lease, error) {
		return h.grant("me", 1), nil
	}
	h.store.onRenew = func(_ context.Context, _ int, fence int64) (Lease, error) {
		return h.grant("me", fence), nil
	}
	h.store.onRelease = func(context.Context, int, int64) error { return nil }

	cfg := Config{
		Store:            h.store,
		Name:             "test-election",
		Identity:         "me",
		LeaseDuration:    testLease,
		RenewInterval:    testRenew,
		RetryPeriod:      testRetry,
		OperationTimeout: testOpLimit,
		SafetyMargin:     testMargin,
		clock:            h.clk,
		// Fixed randomness keeps delays reproducible. With the package's
		// jitter fraction of 0.2 and a draw of 0.5, every jittered interval
		// comes out at exactly 90% of its nominal value.
		randFloat: func() float64 { return 0.5 },
		Callbacks: Callbacks{
			OnStartedLeading: func(ctx context.Context, fence int64) {
				h.mu.Lock()
				h.startedTerms = append(h.startedTerms, fence)
				h.leaderCtx = ctx
				h.mu.Unlock()
				<-ctx.Done()
			},
			OnStoppedLeading: func() {
				h.mu.Lock()
				h.stoppedCount++
				h.mu.Unlock()
			},
			OnNewLeader: func(identity string, _ int64) {
				h.mu.Lock()
				h.newLeaders = append(h.newLeaders, identity)
				h.mu.Unlock()
			},
		},
	}
	if adjust != nil {
		adjust(&cfg)
	}

	e, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	h.e = e
	return h
}

// grant builds a lease that holder owns for the full lease duration, as the
// database would report it at this instant of virtual time.
func (h *harness) grant(holder string, fence int64) Lease {
	return h.leaseWithTTL(holder, fence, testLease)
}

func (h *harness) leaseWithTTL(holder string, fence int64, ttl time.Duration) Lease {
	// The fake database's clock is deliberately offset from the fake local
	// clock by a large, fixed amount. Any code that compares a database
	// timestamp against a local one will produce nonsense, and these tests
	// will notice.
	dbNow := h.clk.Now().Add(dbClockOffset)
	return Lease{
		Name:       "test-election",
		Holder:     holder,
		Fence:      fence,
		AcquiredAt: dbNow,
		RenewedAt:  dbNow,
		ExpiresAt:  dbNow.Add(ttl),
		Now:        dbNow,
	}
}

// dbClockOffset is how far the fake database's clock sits from the fake local
// clock. Real participants in other clusters can be off by this much and more.
const dbClockOffset = 72 * time.Hour

func (h *harness) run() {
	h.t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	h.cancel = cancel
	go func() { h.done <- h.e.Run(ctx) }()
	h.t.Cleanup(h.stop)
}

// stop cancels the elector and waits for Run to return. It keeps virtual time
// moving meanwhile, since shutdown itself may need a timer. It is idempotent,
// because tests call it explicitly and t.Cleanup calls it again.
func (h *harness) stop() {
	h.t.Helper()
	if h.cancel == nil || h.finished {
		return
	}
	h.cancel()
	deadline := time.Now().Add(realTimeout)
	for {
		select {
		case err := <-h.done:
			h.runErr = err
			h.finished = true
			return
		default:
		}
		if time.Now().After(deadline) {
			h.t.Fatal("Run did not return after its context was cancelled")
		}
		h.clk.Advance(virtualStep)
		runtime.Gosched()
	}
}

// advanceUntil moves virtual time forward until cond holds, failing the test
// if it never does.
func (h *harness) advanceUntil(what string, cond func() bool) {
	h.t.Helper()
	deadline := time.Now().Add(realTimeout)
	for !cond() {
		if time.Now().After(deadline) {
			h.t.Fatalf("timed out waiting for %s; virtual time advanced %v, status %+v",
				what, h.clk.Now().Sub(epoch), h.e.Status())
		}
		// Only move virtual time while something is actually waiting on it.
		// Advancing through a window where the loop is running rather than
		// sleeping would let virtual time run away from the code under test,
		// and every duration this harness measures would be inflated by
		// however busy the machine happens to be.
		if h.clk.Timers() == 0 {
			runtime.Gosched()
			continue
		}
		h.clk.Advance(virtualStep)
		runtime.Gosched()
	}
}

// awaitWithoutTime waits for cond without letting virtual time move.
//
// It is how a test asserts that something happens because of what just
// happened rather than because a timer fired: if cond only becomes true once
// the clock advances, this fails.
func (h *harness) awaitWithoutTime(what string, cond func() bool) {
	h.t.Helper()
	deadline := time.Now().Add(realTimeout)
	for !cond() {
		if time.Now().After(deadline) {
			h.t.Fatalf("%s did not happen without the clock advancing", what)
		}
		runtime.Gosched()
	}
}

// awaitLeadership advances until the elector considers itself leader.
func (h *harness) awaitLeadership() {
	h.t.Helper()
	h.advanceUntil("leadership", h.e.IsLeader)
}

// awaitTerms advances until OnStartedLeading has been entered n times.
// IsLeader flips before that callback is scheduled, so a test that inspects
// what the application was told has to wait for the callback rather than for
// the flag.
func (h *harness) awaitTerms(n int) {
	h.t.Helper()
	h.advanceUntil("OnStartedLeading", func() bool { return len(h.terms()) >= n })
}

// awaitCalls advances until op has been called at least n times.
func (h *harness) awaitCalls(op string, n int) {
	h.t.Helper()
	h.advanceUntil(op+" calls", func() bool { return h.store.count(op) >= n })
}

func (h *harness) terms() []int64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]int64(nil), h.startedTerms...)
}

func (h *harness) stopped() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.stoppedCount
}

func (h *harness) observedLeaders() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]string(nil), h.newLeaders...)
}

func (h *harness) leaderContext() context.Context {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.leaderCtx
}

// runError stops the elector if it is still running and returns what Run
// returned.
func (h *harness) runError() error {
	h.t.Helper()
	h.stop()
	return h.runErr
}

// validUntil reports the deadline of the current term, or the zero time.
func (h *harness) validUntil() time.Time {
	return h.e.Status().ValidUntil
}
