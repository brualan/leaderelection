package leaderelection

import (
	"context"
	"testing"
	"time"
)

// Simulate time spent in the network or an observation callback after the
// server took its snapshot. A positive server TTL alone cannot authorize work.
func TestCampaignRejectsAReplyWhoseLocalDeadlineHasPassed(t *testing.T) {
	for _, delay := range []time.Duration{testLease - testMargin, testLease, 2 * testLease} {
		t.Run(delay.String(), func(t *testing.T) {
			h := newHarness(t, nil)
			h.store.setAcquire(func(context.Context, int) (Lease, error) {
				lease := h.grant("me", 1)
				h.clk.Advance(delay)
				return lease, nil
			})
			term, err := h.e.campaign(context.Background())
			if term != nil {
				h.e.endTerm(context.Background(), term)
			}
			if err != nil {
				t.Fatal(err)
			}
			if term != nil {
				t.Fatal("started a term after the conservative deadline passed")
			}
		})
	}
}

func TestCampaignCapsAnInflatedTTL(t *testing.T) {
	h := newHarness(t, nil)
	h.store.setAcquire(func(context.Context, int) (Lease, error) { return h.leaseWithTTL("me", 1, 10*testLease), nil })
	sent := h.clk.Now()
	term, err := h.e.campaign(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if term == nil {
		t.Fatal("no term")
	}
	defer h.e.endTerm(context.Background(), term)
	if term.deadline.After(sent.Add(testLease)) {
		t.Fatal("acquisition trusted a TTL longer than the requested lease")
	}
}

// The renewal goroutine can run before a descheduled watchdog. Once the
// conservative deadline has passed, that term must never become valid again.
func TestLateRenewalCannotReviveAnExpiredLocalTerm(t *testing.T) {
	h := newHarness(t, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	term := &term{fence: 1, ctx: ctx, cancel: cancel, deadline: h.clk.Now().Add(testLease)}
	h.e.term = term
	h.clk.Advance(testLease - testMargin)
	if h.e.IsLeader() {
		t.Fatal("leader at deadline")
	}
	h.e.extend(term, h.grant("me", 1), h.clk.Now())
	if h.e.IsLeader() {
		t.Fatal("late renewal resurrected an expired term")
	}
	if _, ok := h.e.Fence(); ok {
		t.Fatal("expired term exposes a fence")
	}
	if h.e.Status().IsLeader {
		t.Fatal("status resurrected an expired term")
	}
}

func TestCampaignDoesNotPromoteAfterObservationCancelsRun(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	h := newHarness(t, func(c *Config) {
		c.Callbacks.OnNewLeader = func(string, int64) { cancel() }
	})
	term, err := h.e.campaign(ctx)
	if term != nil {
		h.e.endTerm(context.Background(), term)
	}
	if err != nil {
		t.Fatal(err)
	}
	if term != nil {
		t.Fatal("started leadership after Run was cancelled")
	}
}

func TestCampaignRechecksDeadlineAfterObservationCallback(t *testing.T) {
	h := newHarness(t, nil)
	h.e.cfg.Callbacks.OnNewLeader = func(string, int64) { h.clk.Advance(testLease) }
	term, err := h.e.campaign(context.Background())
	if term != nil {
		h.e.endTerm(context.Background(), term)
	}
	if err != nil {
		t.Fatal(err)
	}
	if term != nil {
		t.Fatal("observation callback exhausted the lease but leadership still started")
	}
}

func TestRenewalDeadlineBoundary(t *testing.T) {
	for _, offset := range []time.Duration{-time.Nanosecond, 0, time.Nanosecond} {
		t.Run(offset.String(), func(t *testing.T) {
			h := newHarness(t, nil)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			term := &term{ctx: ctx, cancel: cancel, deadline: h.clk.Now().Add(testLease), fence: 1}
			h.e.term = term
			h.clk.Advance(testLease - testMargin + offset)
			extended := h.e.extend(term, h.grant("me", 1), h.clk.Now())
			if want := offset < 0; extended != want {
				t.Fatalf("extended = %v, want %v at offset %v", extended, want, offset)
			}
		})
	}
}

func TestRenewalLoopEndsATermWhenWatchdogHasNotRun(t *testing.T) {
	h := newHarness(t, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	current := &term{ctx: ctx, cancel: cancel, fence: 1, deadline: h.clk.Now().Add(testLease), done: make(chan struct{})}
	close(current.done)
	h.e.term = current
	h.store.setRenew(func(context.Context, int, int64) (Lease, error) {
		lease := h.grant("me", 1)
		h.clk.Advance(testLease)
		return lease, nil
	})
	done := make(chan struct{})
	go func() { defer close(done); h.e.lead(ctx, current) }()
	h.clk.WaitForTimers(1)
	h.clk.Advance(testRenew)
	h.awaitWithoutTime("renewal loop to end the expired term", func() bool {
		select {
		case <-done:
			return true
		default:
			return false
		}
	})
	if ctx.Err() == nil {
		t.Fatal("expired application's context was not cancelled")
	}
	if h.e.IsLeader() {
		t.Fatal("expired term still leads")
	}
	if h.e.Status().LastError != ErrLeaseExpiredLocally {
		t.Fatalf("unexpected reason: %v", h.e.Status().LastError)
	}
	if h.store.count("release") != 0 {
		t.Fatal("released an uncertain lease")
	}
}

func TestShutdownRacingWithSuccessfulRenewalStillReleases(t *testing.T) {
	h := newHarness(t, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	current := &term{ctx: ctx, cancel: cancel, fence: 1, deadline: h.clk.Now().Add(testLease), done: make(chan struct{})}
	close(current.done)
	h.e.term = current
	h.store.setRenew(func(context.Context, int, int64) (Lease, error) { cancel(); return h.grant("me", 1), nil })
	done := make(chan struct{})
	go func() { defer close(done); h.e.lead(ctx, current) }()
	h.clk.WaitForTimers(1)
	h.clk.Advance(testRenew)
	h.awaitWithoutTime("orderly shutdown after renewal", func() bool {
		select {
		case <-done:
			return true
		default:
			return false
		}
	})
	if h.store.count("release") != 1 {
		t.Fatal("normal shutdown did not release the lease")
	}
	if h.e.Status().LastError != nil {
		t.Fatalf("normal shutdown reported a failure: %v", h.e.Status().LastError)
	}
}
