package leaderelection

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestMetricsBeforeRunIsReadOnlySnapshot(t *testing.T) {
	h := newHarness(t, nil)
	want := Metrics{Election: "test-election"}
	if got := h.e.Metrics(); got != want {
		t.Fatalf("initial metrics: %+v", got)
	}
	for range 10 {
		_ = h.e.Metrics()
	}
	if h.store.count("inspect") != 0 || h.store.count("acquire") != 0 {
		t.Fatal("collecting metrics performed Store I/O")
	}
	copy := h.e.Metrics()
	copy.Store.Renew.CallsTotal = 999
	copy.TermsStartedTotal = 999
	if got := h.e.Metrics(); got != want {
		t.Fatalf("caller changed underlying metrics: %+v", got)
	}
}

func TestMetricsOperationOutcomeClassification(t *testing.T) {
	for _, tc := range []struct {
		name                                 string
		err                                  error
		errors, timeouts, canceled, expected uint64
	}{
		{name: "success"},
		{name: "missing", err: fmt.Errorf("wrapped: %w", ErrNoLease), expected: 1},
		{name: "lost", err: fmt.Errorf("wrapped: %w", ErrLeaseLost), expected: 1},
		{name: "cancel", err: fmt.Errorf("wrapped: %w", context.Canceled), canceled: 1},
		{name: "timeout", err: fmt.Errorf("wrapped: %w", context.DeadlineExceeded), errors: 1, timeouts: 1},
		{name: "database", err: errDatabase, errors: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t, nil)
			h.store.setInspect(func(context.Context, int) (Lease, error) { h.clk.Advance(time.Second); return Lease{}, tc.err })
			_, err := h.e.inspect(context.Background())
			if !errors.Is(err, tc.err) {
				t.Fatalf("returned error changed: %v", err)
			}
			m := h.e.Metrics().Store.Inspect
			if m.CallsTotal != 1 || m.ErrorsTotal != tc.errors || m.TimeoutsTotal != tc.timeouts || m.CanceledTotal != tc.canceled || m.ExpectedOutcomesTotal != tc.expected || m.InFlight != 0 || m.DurationSecondsTotal != 1 || m.LastDurationSeconds != 1 {
				t.Fatalf("unexpected metrics: %+v", m)
			}
		})
	}
}

func TestMetricsTrackEachStoreOperationAndAccumulatedDuration(t *testing.T) {
	h := newHarness(t, nil)
	h.store.setInspect(func(context.Context, int) (Lease, error) {
		h.clk.Advance(100 * time.Millisecond)
		return h.grant("other", 1), nil
	})
	h.store.setAcquire(func(context.Context, int) (Lease, error) {
		h.clk.Advance(200 * time.Millisecond)
		return h.grant("other", 1), nil
	})
	h.store.setRenew(func(context.Context, int, int64) (Lease, error) {
		h.clk.Advance(300 * time.Millisecond)
		return h.grant("me", 1), nil
	})
	h.store.setRelease(func(context.Context, int, int64) error { h.clk.Advance(400 * time.Millisecond); return nil })
	for range 2 {
		_, _ = h.e.inspect(context.Background())
		_, _ = h.e.acquire(context.Background())
		_, _ = h.e.renew(context.Background(), 1)
		_ = h.e.release(context.Background(), 1)
	}
	m := h.e.Metrics()
	for _, tc := range []struct {
		name    string
		m       OperationMetrics
		seconds float64
	}{
		{"inspect", m.Store.Inspect, .1}, {"acquire", m.Store.Acquire, .2}, {"renew", m.Store.Renew, .3}, {"release", m.Store.Release, .4},
	} {
		if tc.m.CallsTotal != 2 || tc.m.ErrorsTotal != 0 || tc.m.LastDurationSeconds != tc.seconds || tc.m.DurationSecondsTotal != 2*tc.seconds {
			t.Fatalf("%s: %+v", tc.name, tc.m)
		}
	}
	if m.TermsStartedTotal != 0 || m.RenewalsAcceptedTotal != 0 {
		t.Fatal("raw Store success was confused with promotion/accepted renewal")
	}
}

func TestMetricsExposeHungOperationAndNormalizedTimeout(t *testing.T) {
	h := newHarness(t, nil)
	entered := make(chan struct{})
	h.store.setRenew(func(ctx context.Context, _ int, _ int64) (Lease, error) {
		close(entered)
		<-ctx.Done()
		return Lease{}, ctx.Err()
	})
	done := make(chan error, 1)
	go func() { _, err := h.e.renew(context.Background(), 1); done <- err }()
	<-entered
	m := h.e.Metrics().Store.Renew
	if m.InFlight != 1 || m.CallsTotal != 0 {
		t.Fatalf("hung call not visible: %+v", m)
	}
	h.clk.Advance(testOpLimit)
	if err := <-done; !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("timeout lost: %v", err)
	}
	m = h.e.Metrics().Store.Renew
	if m.InFlight != 0 || m.CallsTotal != 1 || m.TimeoutsTotal != 1 || m.ErrorsTotal != 1 || m.CanceledTotal != 0 || m.DurationSecondsTotal != testOpLimit.Seconds() {
		t.Fatalf("timeout metrics: %+v", m)
	}
}

func TestMetricsDistinguishObservationsFromAcceptedRenewals(t *testing.T) {
	h := newHarness(t, nil)
	h.e.observe(h.grant("other", 1))
	h.e.observe(h.grant("other", 1))
	h.clk.Advance(2 * time.Second)
	m := h.e.Metrics()
	if !m.HasObservation || m.ObservationAgeSeconds != 2 || m.HasSuccessfulRenewal || m.LeaderChangesObservedTotal != 1 {
		t.Fatalf("observation metrics: %+v", m)
	}
	h.e.observe(h.grant("other", 2))
	h.e.recordError(errDatabase)
	if !h.e.Metrics().HasError || h.e.Metrics().LeaderChangesObservedTotal != 2 {
		t.Fatal("changed fence/error not recorded")
	}
	h.e.observe(h.grant("other", 2))
	if h.e.Metrics().HasError {
		t.Fatal("successful observation did not clear error gauge")
	}
}

func TestMetricsTrackLiveTermRenewalAndLoss(t *testing.T) {
	h := newHarness(t, nil)
	h.run()
	h.awaitTerms(1)
	m := h.e.Metrics()
	if !m.Running || !m.IsLeader || !m.LeaderWorkActive || m.LeaderWorkCanceled || m.TermsStartedTotal != 1 || m.TermsEndedTotal != 0 || m.LeaseRemainingSeconds <= 0 || m.LeaseRemainingSeconds > (testLease-testMargin).Seconds() {
		t.Fatalf("live term: %+v", m)
	}
	h.advanceUntil("accepted renewal", func() bool { return h.e.Metrics().RenewalsAcceptedTotal > 0 })
	if !h.e.Metrics().HasSuccessfulRenewal {
		t.Fatal("accepted renewal was not recorded")
	}
	h.store.setInspect(func(context.Context, int) (Lease, error) { return Lease{}, errDatabase })
	h.store.setRenew(func(context.Context, int, int64) (Lease, error) { return Lease{}, ErrLeaseLost })
	h.advanceUntil("term lost", func() bool { return h.e.Metrics().LeaseLossesTotal == 1 })
	h.stop()
	m = h.e.Metrics()
	if m.Running || m.IsLeader || m.LeaderWorkActive || m.LeaderWorkCanceled || m.LeaseRemainingSeconds != 0 || m.TermsEndedTotal != 1 || m.LocalExpirationsTotal != 0 || !m.HasSuccessfulRenewal || m.Store.Renew.ExpectedOutcomesTotal != 1 {
		t.Fatalf("ended term: %+v", m)
	}
}

func TestMetricsExposeUncooperativeLeaderCallback(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	h := newHarness(t, func(c *Config) {
		c.Callbacks.OnStartedLeading = func(context.Context, int64) { close(entered); <-release }
	})
	h.run()
	// Unblock before harness cleanup even if an assertion fails.
	t.Cleanup(unblock)
	h.awaitWithoutTime("callback entry", func() bool {
		select {
		case <-entered:
			return true
		default:
			return false
		}
	})
	h.cancel()
	h.awaitWithoutTime("step-down", func() bool { return h.e.Metrics().TermsEndedTotal == 1 })
	m := h.e.Metrics()
	if m.IsLeader || !m.Running || !m.LeaderWorkActive || !m.LeaderWorkCanceled {
		t.Fatalf("stuck callback not visible: %+v", m)
	}
	unblock()
	h.stop()
	m = h.e.Metrics()
	if m.Running || m.LeaderWorkActive || m.LeaderWorkCanceled {
		t.Fatalf("callback was not cleared: %+v", m)
	}
}

func TestMetricsDeadlineCheckDoesNotTriggerStepDown(t *testing.T) {
	h := newHarness(t, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	current := &term{ctx: ctx, cancel: cancel, deadline: h.clk.Now().Add(testLease)}
	h.e.term = current
	h.e.metrics.termsStarted = 1
	h.clk.Advance(testLease - testMargin)
	m := h.e.Metrics()
	if m.IsLeader || m.LeaseRemainingSeconds != 0 || m.TermsEndedTotal != 0 || h.e.term != current {
		t.Fatal("snapshot extended or ended an expired term")
	}
	h.e.stepDown(current, ErrLeaseExpiredLocally)
	h.e.stepDown(current, ErrLeaseLost)
	m = h.e.Metrics()
	if m.TermsEndedTotal != 1 || m.LocalExpirationsTotal != 1 || m.LeaseLossesTotal != 0 {
		t.Fatalf("non-idempotent step-down counters: %+v", m)
	}
}

func TestMetricsCountersSurviveRunReuse(t *testing.T) {
	h := newHarness(t, nil)
	for fence := int64(1); fence <= 2; fence++ {
		token := fence
		h.store.setAcquire(func(context.Context, int) (Lease, error) { return h.grant("me", token), nil })
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		done := make(chan error, 1)
		go func() { done <- h.e.Run(ctx) }()
		h.awaitWithoutTime("promotion", func() bool { return h.e.Metrics().TermsStartedTotal == uint64(token) })
		cancel()
		h.awaitWithoutTime("Run exit", func() bool {
			select {
			case err := <-done:
				if err != nil {
					t.Fatal(err)
				}
				return true
			default:
				return false
			}
		})
	}
	m := h.e.Metrics()
	if m.TermsStartedTotal != 2 || m.TermsEndedTotal != 2 || m.Store.Acquire.CallsTotal != 2 || m.Store.Release.CallsTotal != 2 || m.Running {
		t.Fatalf("counters reset on reuse: %+v", m)
	}
}

func TestMetricsConcurrentCollectionDuringFailures(t *testing.T) {
	h := newHarness(t, nil)
	h.run()
	h.awaitTerms(1)
	var wg sync.WaitGroup
	stop := make(chan struct{})
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					m := h.e.Metrics()
					if m.Store.Renew.TimeoutsTotal > m.Store.Renew.ErrorsTotal {
						t.Error("incoherent operation snapshot")
						return
					}
				}
			}
		}()
	}
	defer func() { close(stop); wg.Wait() }()
	h.store.setRenew(func(ctx context.Context, _ int, _ int64) (Lease, error) { <-ctx.Done(); return Lease{}, ctx.Err() })
	h.store.setInspect(func(context.Context, int) (Lease, error) { return Lease{}, errDatabase })
	h.advanceUntil("local expiry", func() bool { return h.e.Metrics().LocalExpirationsTotal == 1 })
	h.stop()
}

func TestMetricsNewTermDoesNotInheritOldRenewalAge(t *testing.T) {
	h := newHarness(t, nil)
	first := h.e.promote(context.Background(), h.grant("me", 1), h.clk.Now())
	if !h.e.extend(first, h.grant("me", 1), h.clk.Now()) {
		t.Fatal("renewal rejected")
	}
	h.e.endTerm(context.Background(), first)
	if !h.e.Metrics().HasSuccessfulRenewal {
		t.Fatal("ended term lost its last renewal")
	}
	second := h.e.promote(context.Background(), h.grant("me", 2), h.clk.Now())
	defer h.e.endTerm(context.Background(), second)
	m := h.e.Metrics()
	if m.HasSuccessfulRenewal || m.LastSuccessfulRenewalAgeSeconds != 0 || m.RenewalsAcceptedTotal != 1 {
		t.Fatalf("new term inherited old renewal or counters reset: %+v", m)
	}
}

type metricBlockingError struct {
	entered, release chan struct{}
	once             sync.Once
}

func (e *metricBlockingError) Error() string { return "custom Store error" }
func (e *metricBlockingError) Is(error) bool {
	e.once.Do(func() { close(e.entered) })
	<-e.release
	return false
}

func TestMetricsErrorClassificationCannotBlockWatchdogMutex(t *testing.T) {
	h := newHarness(t, nil)
	blocking := &metricBlockingError{entered: make(chan struct{}), release: make(chan struct{})}
	var once sync.Once
	unblock := func() { once.Do(func() { close(blocking.release) }) }
	t.Cleanup(unblock)
	h.store.setInspect(func(context.Context, int) (Lease, error) { return Lease{}, blocking })
	done := make(chan struct{})
	go func() { defer close(done); _, _ = h.e.inspect(context.Background()) }()
	<-blocking.entered
	collected := make(chan Metrics, 1)
	go func() { collected <- h.e.Metrics() }()
	select {
	case m := <-collected:
		if m.Store.Inspect.InFlight != 1 {
			t.Fatal("blocked classifier missing from in-flight count")
		}
	case <-time.After(time.Second):
		t.Fatal("custom error classification held the watchdog mutex")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	current := &term{ctx: ctx, cancel: cancel, deadline: h.clk.Now().Add(testLease)}
	h.e.mu.Lock()
	h.e.term = current
	h.e.mu.Unlock()
	stepped := make(chan struct{})
	go func() { defer close(stepped); h.e.stepDown(current, ErrLeaseExpiredLocally) }()
	select {
	case <-stepped:
	case <-time.After(time.Second):
		t.Fatal("watchdog blocked by metrics classification")
	}
	unblock()
	<-done
	if h.e.Metrics().Store.Inspect.ErrorsTotal != 1 {
		t.Fatal("Store error was not counted after classification")
	}
}
