package leaderelection

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/brualan/leaderelection/internal/clock"
)

// mutexAwareClock reports one timestamp while the elector's mutex is held and
// another while it is free. It makes the ordering of "read the clock" versus
// "take the mutex" observable: a check that reads the clock before locking
// sees the stale value, one that reads under the lock sees the current one.
type mutexAwareClock struct {
	mu      *sync.Mutex // the elector's mutex
	stale   time.Time   // reported while the mutex is unheld
	current time.Time   // reported while the mutex is held
}

func (c *mutexAwareClock) Now() time.Time {
	if c.mu.TryLock() {
		c.mu.Unlock()
		return c.stale
	}
	return c.current
}

func (c *mutexAwareClock) NewTimer(time.Duration) clock.Timer { panic("not used") }
func (c *mutexAwareClock) WithTimeout(context.Context, time.Duration) (context.Context, context.CancelFunc) {
	panic("not used")
}

// A leadership check must evaluate the clock after taking the mutex. A caller
// stalled on the lock would otherwise be judged against a stale "now" and
// answered "leader" after its deadline has passed.
func TestLeadershipChecksReadTimeUnderLock(t *testing.T) {
	for _, check := range []struct {
		name string
		run  func(e *Elector) bool
	}{
		{"IsLeader", func(e *Elector) bool { return e.IsLeader() }},
		{"Fence", func(e *Elector) bool {
			_, ok := e.Fence()
			return ok
		}},
		{"Status", func(e *Elector) bool { return e.Status().IsLeader }},
	} {
		t.Run(check.name, func(t *testing.T) {
			mac := &mutexAwareClock{stale: epoch, current: epoch.Add(time.Hour)}
			e, err := newTestElector(func(cfg *Config) { cfg.clock = mac })
			if err != nil {
				t.Fatal(err)
			}
			mac.mu = &e.mu
			// The deadline lies between the stale and the current timestamp:
			// only a check that reads the clock under the lock sees it passed.
			e.mu.Lock()
			e.term = &term{fence: 7, deadline: epoch.Add(10 * time.Second)}
			e.mu.Unlock()

			if check.run(e) {
				t.Error("reported leader although the deadline had already passed when the mutex was acquired")
			}
		})
	}
}

func newTestElector(adjust func(*Config)) (*Elector, error) {
	cfg := Config{
		Store:            newFakeStore(),
		Name:             "regression",
		Identity:         "me",
		LeaseDuration:    testLease,
		RenewInterval:    testRenew,
		RetryPeriod:      testRetry,
		OperationTimeout: testOpLimit,
		SafetyMargin:     testMargin,
	}
	if adjust != nil {
		adjust(&cfg)
	}
	return New(cfg)
}

// New must own its copy of Config.Payload. A caller mutating the slice it
// passed must not change what the elector publishes, and the elector's
// renewals must not race the caller's buffer.
func TestNewClonesPayload(t *testing.T) {
	payload := []byte("https://leader-a:8080")
	e, err := newTestElector(func(cfg *Config) { cfg.Payload = payload })
	if err != nil {
		t.Fatal(err)
	}
	payload[0] = 'X'
	if got := string(e.cfg.Payload); got != "https://leader-a:8080" {
		t.Errorf("mutating the caller's slice after New changed the elector's payload: %q", got)
	}
}

// handoverStore models a database where A's renewals fail with ErrLeaseLost
// and the lost lease is expired in place, so a follower can take over.
type handoverStore struct {
	mu        sync.Mutex
	holder    string
	fence     int64
	expiresAt time.Time
}

func (s *handoverStore) leaseLocked() Lease {
	now := time.Now()
	return Lease{
		Name: "regression", Holder: s.holder, Fence: s.fence,
		AcquiredAt: now, RenewedAt: now, ExpiresAt: s.expiresAt, Now: now,
	}
}

func (s *handoverStore) Inspect(context.Context, string) (Lease, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.holder == "" {
		return Lease{}, ErrNoLease
	}
	return s.leaseLocked(), nil
}

func (s *handoverStore) Acquire(_ context.Context, _, identity string, ttl time.Duration, _ []byte) (Lease, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.holder != "" && s.holder != identity && s.expiresAt.After(time.Now()) {
		return s.leaseLocked(), nil
	}
	s.holder = identity
	s.fence++
	s.expiresAt = time.Now().Add(ttl)
	return s.leaseLocked(), nil
}

func (s *handoverStore) Renew(_ context.Context, _, identity string, _ int64, ttl time.Duration, _ []byte) (Lease, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	// A's renewals always fail: A lost the lease. B, once it leads, renews
	// happily ever after.
	if identity == "A" {
		s.expiresAt = time.Now().Add(-time.Second)
		return Lease{}, ErrLeaseLost
	}
	if s.holder != identity {
		return Lease{}, ErrLeaseLost
	}
	s.expiresAt = time.Now().Add(ttl)
	return s.leaseLocked(), nil
}

func (s *handoverStore) Release(context.Context, string, string, int64) error { return nil }

// The next leader does not wait for the previous leader's OnStartedLeading to
// return: once the lease is lost, another participant acquires it and starts
// its own callback while the predecessor's is still running. This is the
// behaviour fencing tokens exist for; the test pins it down so documentation
// and code never drift back into implying a global handover barrier.
func TestNextLeaderDoesNotWaitForPredecessorCallback(t *testing.T) {
	store := &handoverStore{}
	cfg := func(identity string, onStart func(ctx context.Context, fence int64)) Config {
		return Config{
			Store: store, Name: "regression", Identity: identity,
			LeaseDuration: 2 * time.Second, RenewInterval: 400 * time.Millisecond,
			RetryPeriod: 150 * time.Millisecond, OperationTimeout: 100 * time.Millisecond,
			SafetyMargin: 300 * time.Millisecond,
			Callbacks:    Callbacks{OnStartedLeading: onStart},
		}
	}

	never := make(chan struct{})
	aStarted := make(chan struct{})
	var aCallbackRunning atomicFlag
	aDone := make(chan error, 1)
	a, err := New(cfg("A", func(context.Context, int64) {
		aCallbackRunning.v.Store(true)
		close(aStarted)
		<-never // leader work that ignores its context
	}))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { aDone <- a.Run(ctx) }()
	<-aStarted

	bStarted := make(chan struct{})
	bDone := make(chan error, 1)
	b, err := New(cfg("B", func(ctx context.Context, _ int64) {
		close(bStarted)
		<-ctx.Done()
	}))
	if err != nil {
		t.Fatal(err)
	}
	go func() { bDone <- b.Run(ctx) }()

	select {
	case <-bStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("B never became leader")
	}
	if !aCallbackRunning.v.Load() {
		t.Error("B is leading but A's OnStartedLeading already returned; expected overlap")
	}

	// Let both Runs wind down: release A's wedged callback, then cancel.
	close(never)
	cancel()
	for name, done := range map[string]chan error{"A": aDone, "B": bDone} {
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Errorf("%s's Run did not return after cancellation", name)
		}
	}
}
