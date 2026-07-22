package leaderelection

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/brualan/leaderelection/internal/assert"
)

// errDatabase stands in for anything the driver might report: a dropped
// connection, a pooler refusing to hand one out, a query cancelled by the
// server. The loop must treat them all identically.
var errDatabase = errors.New("database unreachable")

func TestAcquiresLeadershipOnEmptyElection(t *testing.T) {
	h := newHarness(t, nil)
	h.run()
	h.awaitTerms(1)

	assert.DeepEqual(t, h.terms(), []int64{1}, "leadership terms")
	assert.Equal(t, h.store.args("acquire").identity, "me", "acquire identity")
	assert.Equal(t, h.store.args("acquire").ttl, testLease, "acquire ttl")

	fence, ok := h.e.Fence()
	assert.True(t, ok, "Fence reports a term")
	assert.Equal(t, fence, int64(1), "fence token")
}

func TestKeepsLeadershipWhileRenewalsSucceed(t *testing.T) {
	h := newHarness(t, nil)
	h.run()
	h.awaitLeadership()

	// Run well past the point where an unrenewed lease would have expired, so
	// that the watchdog has to wake, observe an extended deadline, and re-arm.
	h.awaitCalls("renew", 8)

	assert.True(t, h.e.IsLeader(), "still leader after 8 renewals")
	assert.DeepEqual(t, h.terms(), []int64{1}, "leadership terms")
	assert.Equal(t, h.stopped(), 0, "OnStoppedLeading calls")
	assert.Equal(t, h.store.args("renew").fence, int64(1), "renewals carry the term's fence")

	// Virtual time is now far beyond the original deadline; leadership is only
	// still valid because renewals kept moving it.
	assert.True(t, h.validUntil().After(h.clk.Now()), "deadline is ahead of now")
	assert.True(t, h.clk.Now().Sub(epoch) > testLease, "test outlived one lease duration")
}

// TestStepsDownWhenDatabaseStopsAnswering is the safety test this library
// exists for. The database neither fails nor closes the connection: it simply
// stops replying, which is what a paused server, a black-holed route or a
// pooler that swallowed the request all look like from here.
//
// The leader cannot distinguish that from a healthy database, so it must
// assume the worst and step down before its lease could be handed to anyone
// else -- without any help from the database it can no longer reach.
func TestStepsDownWhenDatabaseStopsAnswering(t *testing.T) {
	h := newHarness(t, nil)
	h.run()
	h.awaitTerms(1)
	h.awaitCalls("renew", 1)

	// From now on the database accepts requests and never answers.
	hung := make(chan struct{}, 1)
	h.store.setRenew(func(ctx context.Context, _ int, _ int64) (Lease, error) {
		select {
		case hung <- struct{}{}:
		default:
		}
		<-ctx.Done()
		return Lease{}, ctx.Err()
	})
	h.advanceUntil("a renewal to hang", func() bool {
		select {
		case <-hung:
			return true
		default:
			return false
		}
	})

	// Renewals run one at a time on the loop's own goroutine, so once one has
	// entered the hanging handler every earlier renewal has already been
	// applied. The deadline is therefore final, and this is the instant by
	// which we must have stepped down.
	deadline := h.validUntil()
	assert.True(t, !deadline.IsZero(), "leader has a deadline")

	h.advanceUntil("step-down", func() bool { return !h.e.IsLeader() })

	steppedDownAt := h.clk.Now()
	if steppedDownAt.After(deadline) {
		t.Fatalf("stepped down %v after the lease deadline; a successor could already have taken over",
			steppedDownAt.Sub(deadline))
	}

	// The application must have been told too, and told before the lease could
	// actually have passed to anyone else. IsLeader flips at the safety margin
	// because it recomputes rather than trusting the watchdog; the watchdog
	// itself, and therefore the cancellation, lands within that margin. Both
	// are before the database's own expiry, which is what matters.
	expiry := deadline.Add(testMargin)
	h.advanceUntil("leader context cancellation", func() bool {
		return h.leaderContext().Err() != nil
	})
	if cancelledBy := h.clk.Now(); cancelledBy.After(expiry) {
		t.Fatalf("leader context was cancelled %v after the lease expired in the database; "+
			"the application kept its context past the point a successor could start",
			cancelledBy.Sub(expiry))
	}

	// It kept trying to renew rather than giving up at the first failure.
	assert.True(t, h.store.count("renew") >= 2, "retried the renewal")
	assert.ErrorIs(t, h.e.Status().LastError, ErrLeaseExpiredLocally, "recorded reason")
}

// A leader that cannot reach the database must not release on the way out: the
// call would block for the full operation timeout to accomplish nothing.
func TestDoesNotReleaseAfterSteppingDownUnreachable(t *testing.T) {
	h := newHarness(t, nil)
	h.run()
	h.awaitLeadership()

	h.store.setRenew(func(ctx context.Context, _ int, _ int64) (Lease, error) {
		<-ctx.Done()
		return Lease{}, ctx.Err()
	})
	h.advanceUntil("step-down", func() bool { return !h.e.IsLeader() })
	h.advanceUntil("OnStoppedLeading", func() bool { return h.stopped() >= 1 })

	assert.Equal(t, h.store.count("release"), 0, "release attempts")
}

func TestStepsDownImmediatelyWhenLeaseIsLost(t *testing.T) {
	h := newHarness(t, nil)
	h.run()
	h.awaitTerms(1)

	h.store.setRenew(func(context.Context, int, int64) (Lease, error) {
		return Lease{}, ErrLeaseLost
	})

	before := h.clk.Now()
	h.advanceUntil("step-down", func() bool { return !h.e.IsLeader() })

	// Losing the lease is unambiguous, so there is nothing to wait for. The
	// timing-free version of that claim is that exactly one renewal happened:
	// the one that came back rejected. A step-down that waited for the
	// watchdog would have retried several times first.
	assert.Equal(t, h.store.count("renew"), 1, "renewals before stepping down")

	if elapsed := h.clk.Now().Sub(before); elapsed >= testLease-testMargin {
		t.Fatalf("took %v to step down after an explicit loss, which is the watchdog deadline (%v) "+
			"rather than the next renewal (%v)", elapsed, testLease-testMargin, testRenew)
	}
	// An explicit loss is handled on the renewal's own code path, so the
	// application's context is cancelled there and then. Waiting for it
	// without advancing the clock is what distinguishes that from a
	// cancellation that merely happens to arrive on the next tick.
	h.awaitWithoutTime("leader context cancellation", func() bool {
		return h.leaderContext().Err() != nil
	})

	h.advanceUntil("OnStoppedLeading", func() bool { return h.stopped() >= 1 })
	assert.ErrorIs(t, h.e.Status().LastError, ErrLeaseLost, "recorded reason")
	assert.Equal(t, h.store.count("release"), 0, "release attempts after losing the lease")
}

// A store that reports success while returning somebody else's lease is
// malfunctioning. The data is trusted over the status code, because the
// alternative is two leaders.
func TestStepsDownWhenRenewalReturnsForeignLease(t *testing.T) {
	for _, tc := range []struct {
		name  string
		lease func(h *harness) Lease
	}{
		{"different holder", func(h *harness) Lease { return h.grant("someone-else", 1) }},
		{"different fence", func(h *harness) Lease { return h.grant("me", 99) }},
		{"already expired", func(h *harness) Lease { return h.leaseWithTTL("me", 1, 0) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t, nil)
			h.run()
			h.awaitLeadership()

			h.store.setRenew(func(context.Context, int, int64) (Lease, error) {
				return tc.lease(h), nil
			})
			h.advanceUntil("step-down", func() bool { return !h.e.IsLeader() })
			h.advanceUntil("OnStoppedLeading", func() bool { return h.stopped() >= 1 })
			assert.ErrorIs(t, h.e.Status().LastError, ErrLeaseLost, "recorded reason")
		})
	}
}

func TestSurvivesTransientRenewalFailure(t *testing.T) {
	h := newHarness(t, nil)
	h.run()
	h.awaitTerms(1)

	// Fail the next two renewals outright, then recover. The lease has room
	// for this: the config invariant guarantees at least two attempts fit.
	h.store.setRenew(func(_ context.Context, n int, fence int64) (Lease, error) {
		if n <= 3 {
			return Lease{}, errDatabase
		}
		return h.grant("me", fence), nil
	})

	h.awaitCalls("renew", 6)
	assert.True(t, h.e.IsLeader(), "leadership survived two failed renewals")
	assert.DeepEqual(t, h.terms(), []int64{1}, "no new term was started")
	assert.Equal(t, h.stopped(), 0, "OnStoppedLeading calls")
}

func TestWaitsWhileAnotherParticipantHoldsTheLease(t *testing.T) {
	h := newHarness(t, nil)
	h.store.setInspect(func(context.Context, int) (Lease, error) {
		return h.grant("someone-else", 7), nil
	})
	h.run()

	h.awaitCalls("inspect", 3)
	assert.False(t, h.e.IsLeader(), "must not lead while another participant holds the lease")
	// A live lease is answered by a read, never by a write: a fleet of
	// followers polling must not generate a row version each.
	assert.Equal(t, h.store.count("acquire"), 0, "acquire attempts against a live lease")

	// The follower still learns who the leader is, and reports it.
	assert.DeepEqual(t, h.observedLeaders(), []string{"someone-else"}, "observed leaders")
	status := h.e.Status()
	assert.Equal(t, status.Leader, "someone-else", "Status.Leader")
	assert.Equal(t, status.LeaderFence, int64(7), "Status.LeaderFence")
	assert.False(t, status.IsLeader, "Status.IsLeader")
}

func TestTakesOverAnExpiredLease(t *testing.T) {
	h := newHarness(t, nil)
	h.store.setInspect(func(context.Context, int) (Lease, error) {
		// Held by another participant, but already expired on the database's
		// own clock.
		return h.leaseWithTTL("someone-else", 7, 0), nil
	})
	h.store.setAcquire(func(context.Context, int) (Lease, error) {
		return h.grant("me", 8), nil
	})
	h.run()
	h.awaitTerms(1)

	assert.DeepEqual(t, h.terms(), []int64{8}, "took over with a fresh fence")
	assert.DeepEqual(t, h.observedLeaders(), []string{"someone-else", "me"}, "observed leaders")
}

// A lease that arrives with less time left than the safety margin is unusable:
// promoting would be followed instantly by a step-down, and the application
// would see a term it could do nothing with.
func TestDoesNotPromoteOnALeaseInsideTheSafetyMargin(t *testing.T) {
	h := newHarness(t, nil)
	h.store.setAcquire(func(context.Context, int) (Lease, error) {
		return h.leaseWithTTL("me", 1, testMargin/2), nil
	})
	h.run()

	h.awaitCalls("acquire", 3)
	assert.False(t, h.e.IsLeader(), "must not promote inside the safety margin")
	assert.Equal(t, len(h.terms()), 0, "OnStartedLeading calls")
}

func TestLosingTheElectionIsNotAnError(t *testing.T) {
	h := newHarness(t, nil)
	h.store.setAcquire(func(context.Context, int) (Lease, error) {
		// Somebody won the race between our read and our write.
		return h.grant("someone-else", 2), nil
	})
	h.run()

	h.awaitCalls("acquire", 2)
	assert.False(t, h.e.IsLeader(), "lost the race")
	assert.NoError(t, h.e.Status().LastError, "a lost election is not a failure")
}

func TestRetriesAfterDatabaseFailures(t *testing.T) {
	for _, tc := range []struct {
		name  string
		apply func(*harness)
	}{
		{"inspect fails", func(h *harness) {
			h.store.setInspect(func(context.Context, int) (Lease, error) {
				return Lease{}, errDatabase
			})
		}},
		{"acquire fails", func(h *harness) {
			h.store.setAcquire(func(context.Context, int) (Lease, error) {
				return Lease{}, errDatabase
			})
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t, nil)
			tc.apply(h)
			h.run()

			h.advanceUntil("retries", func() bool {
				return h.store.count("inspect") >= 3
			})
			assert.False(t, h.e.IsLeader(), "never leads while the database is failing")
			assert.ErrorIs(t, h.e.Status().LastError, errDatabase, "Status.LastError")
		})
	}
}

// A hung Inspect must not wedge the loop: the operation timeout has to bound
// it even though no watchdog is involved outside a term.
func TestOperationTimeoutBoundsAHungCall(t *testing.T) {
	h := newHarness(t, nil)
	h.store.setInspect(func(ctx context.Context, _ int) (Lease, error) {
		<-ctx.Done()
		return Lease{}, ctx.Err()
	})
	h.run()

	h.awaitCalls("inspect", 2)
	assert.False(t, h.e.IsLeader(), "never leads without a confirmed lease")
	assert.ErrorIs(t, h.e.Status().LastError, context.DeadlineExceeded, "Status.LastError")
}

func TestReleasesTheLeaseOnShutdown(t *testing.T) {
	h := newHarness(t, nil)
	h.run()
	h.awaitLeadership()

	h.stop()

	assert.Equal(t, h.store.count("release"), 1, "release calls")
	assert.Equal(t, h.store.args("release").fence, int64(1), "release fence")
	assert.Equal(t, h.store.args("release").identity, "me", "release identity")
	assert.Equal(t, h.stopped(), 1, "OnStoppedLeading calls")
	assert.False(t, h.e.IsLeader(), "not leader after shutdown")
	assert.NoError(t, h.runError(), "Run return value")
}

func TestDisableReleaseSkipsTheHandover(t *testing.T) {
	h := newHarness(t, func(c *Config) { c.DisableRelease = true })
	h.run()
	h.awaitLeadership()

	h.stop()
	assert.Equal(t, h.store.count("release"), 0, "release calls")
}

// A failed release is a missed optimisation, not a failure: the lease expires
// on its own and the successor simply waits longer.
func TestReleaseFailureDoesNotFailShutdown(t *testing.T) {
	h := newHarness(t, nil)
	h.store.onRelease = func(context.Context, int, int64) error { return errDatabase }
	h.run()
	h.awaitLeadership()

	h.stop()
	assert.NoError(t, h.runError(), "Run return value")
	assert.Equal(t, h.stopped(), 1, "OnStoppedLeading calls")
}

func TestShutdownCancelsTheLeaderContext(t *testing.T) {
	h := newHarness(t, nil)
	h.run()
	h.awaitTerms(1)

	leaderCtx := h.leaderContext()
	assert.NoError(t, leaderCtx.Err(), "leader context is live during the term")

	h.stop()
	assert.Error(t, leaderCtx.Err(), "leader context is cancelled after shutdown")
}

// Run blocks until the application's leader goroutine has returned. Anything
// less would let a successor start while this process is still writing.
func TestShutdownWaitsForTheLeaderCallback(t *testing.T) {
	released := make(chan struct{})
	finished := make(chan struct{})

	h := newHarness(t, func(c *Config) {
		c.Callbacks.OnStartedLeading = func(ctx context.Context, _ int64) {
			<-ctx.Done()
			<-released
			close(finished)
		}
	})
	h.run()
	h.awaitLeadership()

	h.cancel()

	// While the callback is still running, Run must not have returned.
	h.advanceUntil("leader context cancellation", func() bool {
		select {
		case <-finished:
			t.Fatal("callback finished before it was released")
		default:
		}
		return true
	})
	select {
	case <-h.done:
		t.Fatal("Run returned while the leader callback was still running")
	default:
	}

	close(released)
	<-finished
	h.stop()
}

func TestRunRejectsConcurrentUse(t *testing.T) {
	h := newHarness(t, nil)
	h.run()
	h.awaitLeadership()

	assert.ErrorIs(t, h.e.Run(context.Background()), ErrAlreadyRunning, "second Run")
}

func TestRunIsReusableAfterShutdown(t *testing.T) {
	h := newHarness(t, nil)
	h.run()
	h.awaitLeadership()
	h.stop()

	// The single-instance guard must release, or a supervisor that restarts
	// the loop after a shutdown would be locked out permanently.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	assert.NoError(t, h.e.Run(ctx), "Run after shutdown")
}

func TestFenceIsUnavailableWhenNotLeading(t *testing.T) {
	h := newHarness(t, nil)
	h.store.setInspect(func(context.Context, int) (Lease, error) {
		return h.grant("someone-else", 3), nil
	})
	h.run()
	h.awaitCalls("inspect", 2)

	_, ok := h.e.Fence()
	assert.False(t, ok, "Fence while following")
}

// IsLeader is deliberately not a cached flag. Even if the watchdog goroutine
// were starved, a caller past the deadline must be told it is not the leader.
func TestIsLeaderRechecksTheDeadline(t *testing.T) {
	h := newHarness(t, nil)
	h.run()
	h.awaitLeadership()

	// Freeze renewals so the deadline cannot move, then look at the elector's
	// own bookkeeping rather than waiting for the watchdog.
	h.store.setRenew(func(ctx context.Context, _ int, _ int64) (Lease, error) {
		<-ctx.Done()
		return Lease{}, ctx.Err()
	})

	h.e.mu.Lock()
	term := h.e.term
	h.e.mu.Unlock()
	if term == nil {
		t.Fatal("expected a live term")
	}

	h.advanceUntil("deadline to pass", func() bool {
		h.e.mu.Lock()
		defer h.e.mu.Unlock()
		return !h.clk.Now().Before(term.deadline.Add(-testMargin))
	})
	assert.False(t, h.e.IsLeader(), "IsLeader past the deadline")
}

func TestStatusReportsPayloadAndTimings(t *testing.T) {
	payload := []byte("https://leader.example:8443")
	h := newHarness(t, func(c *Config) { c.Payload = payload })
	h.store.setAcquire(func(context.Context, int) (Lease, error) {
		lease := h.grant("me", 4)
		lease.Payload = payload
		return lease, nil
	})
	h.run()
	h.awaitLeadership()

	status := h.e.Status()
	assert.True(t, status.IsLeader, "Status.IsLeader")
	assert.Equal(t, status.Fence, int64(4), "Status.Fence")
	assert.Equal(t, status.Identity, "me", "Status.Identity")
	assert.Equal(t, status.Leader, "me", "Status.Leader")
	assert.Equal(t, string(status.LeaderPayload), string(payload), "Status.LeaderPayload")
	assert.True(t, !status.ObservedAt.IsZero(), "Status.ObservedAt")
	assert.NoError(t, status.LastError, "Status.LastError")
	assert.DeepEqual(t, h.store.args("acquire").payload, payload, "payload sent to the store")

	// ValidUntil is expressed on the local clock even though the lease was
	// granted on a database clock three days away.
	remaining := status.ValidUntil.Sub(h.clk.Now())
	if remaining <= 0 || remaining > testLease-testMargin {
		t.Fatalf("ValidUntil is %v away, want within (0, %v]", remaining, testLease-testMargin)
	}
}

func TestStatusBeforeFirstObservation(t *testing.T) {
	h := newHarness(t, nil)
	status := h.e.Status()

	assert.False(t, status.IsLeader, "Status.IsLeader")
	assert.Equal(t, status.Leader, "", "Status.Leader")
	assert.Equal(t, status.Identity, "me", "Status.Identity")
	assert.True(t, status.ValidUntil.IsZero(), "Status.ValidUntil")
}

// The application may be told about a new leader only when one actually
// changes, or a follower that polls every few seconds would drown in
// notifications.
func TestOnNewLeaderFiresOnlyOnChange(t *testing.T) {
	h := newHarness(t, nil)
	h.store.setInspect(func(context.Context, int) (Lease, error) {
		return h.grant("someone-else", 3), nil
	})
	h.run()
	h.awaitCalls("inspect", 4)

	assert.DeepEqual(t, h.observedLeaders(), []string{"someone-else"}, "observed leaders")
}

func TestNilCallbacksAreOptional(t *testing.T) {
	h := newHarness(t, func(c *Config) { c.Callbacks = Callbacks{} })
	h.run()
	h.awaitLeadership()
	h.awaitCalls("renew", 1)
	h.stop()

	assert.NoError(t, h.runError(), "Run return value")
}

// Once a term ends, the loop must pause before campaigning again rather than
// spinning against a database that just failed it.
func TestPausesBeforeRecampaigning(t *testing.T) {
	h := newHarness(t, nil)
	h.run()
	h.awaitLeadership()

	h.store.setRenew(func(context.Context, int, int64) (Lease, error) {
		return Lease{}, ErrLeaseLost
	})
	h.advanceUntil("step-down", func() bool { return !h.e.IsLeader() })

	acquiresBefore := h.store.count("acquire")
	steppedDownAt := h.clk.Now()

	h.advanceUntil("re-campaign", func() bool {
		return h.store.count("acquire") > acquiresBefore
	})
	// pollDelay is RetryPeriod shrunk by the harness's fixed jitter draw, and
	// nothing shortens it further.
	want := time.Duration(float64(testRetry) * harnessJitterFactor)
	if gap := h.clk.Now().Sub(steppedDownAt); gap < want {
		t.Fatalf("re-campaigned after only %v, want at least %v", gap, want)
	}
}

func TestElectionNameIsPassedThrough(t *testing.T) {
	h := newHarness(t, func(c *Config) { c.Name = "shipping-reconciler" })
	h.run()
	h.awaitLeadership()

	assert.Equal(t, h.store.args("inspect").name, "shipping-reconciler", "inspect name")
	assert.Equal(t, h.store.args("acquire").name, "shipping-reconciler", "acquire name")
}

func TestRunReturnsWhenContextIsAlreadyCancelled(t *testing.T) {
	h := newHarness(t, nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	assert.NoError(t, h.e.Run(ctx), "Run with a cancelled context")
	assert.Equal(t, h.store.count("inspect"), 0, "store calls")
}

func TestExtendOnlyMovesTheDeadlineForward(t *testing.T) {
	h := newHarness(t, nil)
	h.run()
	h.awaitLeadership()

	h.e.mu.Lock()
	term := h.e.term
	before := term.deadline
	h.e.mu.Unlock()

	// A reply that overtook an earlier one carries a shorter remaining TTL.
	// Honouring it would shorten a deadline the database already granted.
	h.e.extend(term, h.leaseWithTTL("me", 1, time.Second), h.clk.Now().Add(-time.Hour))

	h.e.mu.Lock()
	after := term.deadline
	h.e.mu.Unlock()
	assert.True(t, after.Equal(before), "deadline unchanged by a stale reply")

	// A term that has already ended must not be extendable at all.
	h.e.stepDown(term, nil)
	h.e.extend(term, h.grant("me", 1), h.clk.Now())
	assert.False(t, h.e.IsLeader(), "extending a dead term did not revive it")
}

func TestStepDownIsIdempotent(t *testing.T) {
	h := newHarness(t, nil)
	h.run()
	h.awaitLeadership()

	h.e.mu.Lock()
	term := h.e.term
	h.e.mu.Unlock()

	h.e.stepDown(term, ErrLeaseLost)
	h.e.stepDown(term, errDatabase)

	// The first reason wins; a later caller must not overwrite why the term
	// ended.
	assert.ErrorIs(t, h.e.Status().LastError, ErrLeaseLost, "recorded reason")
	assert.False(t, h.e.IsLeader(), "not leader after stepping down")
}

// When a term is ended from outside while a renewal is still in flight, that
// renewal comes back reporting its own cancellation. Recording it would
// overwrite the reason the term actually ended, and an operator reading
// Status would be told "context canceled" instead of what happened.
func TestRenewalFailingDuringTeardownDoesNotMaskTheReason(t *testing.T) {
	h := newHarness(t, nil)
	h.run()
	h.awaitTerms(1)

	inRenew := make(chan struct{}, 1)
	h.store.setRenew(func(ctx context.Context, _ int, _ int64) (Lease, error) {
		select {
		case inRenew <- struct{}{}:
		default:
		}
		<-ctx.Done()
		return Lease{}, ctx.Err()
	})
	h.advanceUntil("a renewal in flight", func() bool {
		select {
		case <-inRenew:
			return true
		default:
			return false
		}
	})

	h.e.mu.Lock()
	term := h.e.term
	h.e.mu.Unlock()
	if term == nil {
		t.Fatal("expected a live term")
	}

	// End the term while that renewal is parked, so its failure arrives after
	// the real reason has already been recorded.
	h.e.stepDown(term, ErrLeaseLost)

	h.advanceUntil("OnStoppedLeading", func() bool { return h.stopped() >= 1 })
	assert.ErrorIs(t, h.e.Status().LastError, ErrLeaseLost, "recorded reason")
	assert.False(t, h.e.IsLeader(), "not leader after the term ended")
}

// A fence token must never start a second term.
//
// The path that gets there: the watchdog gives up leadership because renewals
// stopped landing, but the lease is still ours and still valid in the database
// -- the local deadline is deliberately the earlier of the two. Re-acquiring
// it is legitimate and the store rightly leaves the fence alone, because
// nobody else could have led in between. Handing that same token to a second
// term would leave a downstream system unable to tell an in-flight write from
// the abandoned term apart from this one's.
func TestASpentFenceNeverStartsASecondTerm(t *testing.T) {
	h := newHarness(t, nil)
	h.run()
	h.awaitTerms(1)

	// Renewals stop landing, so the watchdog steps us down. The database was
	// never told, so it still has us as the holder of a live lease.
	h.store.setRenew(func(ctx context.Context, _ int, _ int64) (Lease, error) {
		<-ctx.Done()
		return Lease{}, ctx.Err()
	})
	h.advanceUntil("step-down", func() bool { return !h.e.IsLeader() })
	h.advanceUntil("OnStoppedLeading", func() bool { return h.stopped() >= 1 })

	// The database is reachable again and still reports us as the holder. Only
	// once the lease has been given up does a fresh token appear.
	var released atomic.Bool
	h.store.setInspect(func(context.Context, int) (Lease, error) {
		if released.Load() {
			return h.leaseWithTTL("me", 1, 0), nil
		}
		return h.grant("me", 1), nil
	})
	h.store.setAcquire(func(context.Context, int) (Lease, error) {
		if released.Load() {
			return h.grant("me", 2), nil
		}
		return h.grant("me", 1), nil
	})
	h.store.setRelease(func(_ context.Context, _ int, fence int64) error {
		if fence == 1 {
			released.Store(true)
		}
		return nil
	})

	h.advanceUntil("second term", func() bool { return len(h.terms()) >= 2 })

	terms := h.terms()
	if terms[1] <= terms[0] {
		t.Fatalf("second term reused fence %d from the first; a spent token must not lead twice",
			terms[1])
	}
	assert.True(t, h.store.count("release") >= 1, "gave the spent lease up")
}

// Releasing the spent lease can itself fail, and that is a database failure
// like any other: report it and retry rather than leading on a spent token.
func TestFailingToReleaseASpentFenceDoesNotPromote(t *testing.T) {
	h := newHarness(t, nil)
	h.run()
	h.awaitTerms(1)

	h.store.setRenew(func(ctx context.Context, _ int, _ int64) (Lease, error) {
		<-ctx.Done()
		return Lease{}, ctx.Err()
	})
	h.advanceUntil("step-down", func() bool { return !h.e.IsLeader() })

	h.store.setInspect(func(context.Context, int) (Lease, error) { return h.grant("me", 1), nil })
	h.store.setAcquire(func(context.Context, int) (Lease, error) { return h.grant("me", 1), nil })
	h.store.setRelease(func(context.Context, int, int64) error { return errDatabase })

	// Two attempts is enough to show it keeps trying rather than either giving
	// up or promoting anyway. LastError is deliberately not asserted here: the
	// inspect that precedes each attempt succeeds, and a success clears it.
	h.advanceUntil("release attempts", func() bool { return h.store.count("release") >= 2 })
	assert.False(t, h.e.IsLeader(), "must not lead on a spent token")
	assert.Equal(t, len(h.terms()), 1, "leadership terms")
}

// A store that reports more time remaining than was asked for must not be able
// to push the local deadline past the lease's real expiry.
func TestAnInflatedTTLCannotExtendTheDeadline(t *testing.T) {
	h := newHarness(t, nil)
	h.store.setAcquire(func(context.Context, int) (Lease, error) {
		return h.grant("me", 1), nil
	})
	h.run()
	h.awaitTerms(1)

	h.store.setRenew(func(_ context.Context, _ int, fence int64) (Lease, error) {
		// Ten times the lease we asked for.
		return h.leaseWithTTL("me", fence, 10*testLease), nil
	})
	h.awaitCalls("renew", 1)

	h.advanceUntil("the deadline to settle", func() bool { return h.store.count("renew") >= 2 })
	if remaining := h.validUntil().Sub(h.clk.Now()); remaining > testLease {
		t.Fatalf("deadline is %v away, further than the %v lease that was actually granted",
			remaining, testLease)
	}
}

// The observed leader changing term is a change worth reporting, even when the
// identity is unchanged: it means the previous term ended.
func TestOnNewLeaderFiresWhenOnlyTheFenceChanges(t *testing.T) {
	h := newHarness(t, nil)
	var fence atomic.Int64
	fence.Store(1)
	h.store.setInspect(func(context.Context, int) (Lease, error) {
		return h.grant("someone-else", fence.Load()), nil
	})
	h.run()
	h.awaitCalls("inspect", 2)

	fence.Store(2)
	h.advanceUntil("the new term to be observed", func() bool {
		return len(h.observedLeaders()) >= 2
	})
	assert.DeepEqual(t, h.observedLeaders(), []string{"someone-else", "someone-else"},
		"observed leaders")
}

// Status hands out a snapshot, not a window into the elector.
func TestStatusPayloadIsACopy(t *testing.T) {
	h := newHarness(t, nil)
	h.store.setInspect(func(context.Context, int) (Lease, error) {
		lease := h.grant("someone-else", 3)
		lease.Payload = []byte("endpoint")
		return lease, nil
	})
	h.run()
	h.awaitCalls("inspect", 1)
	h.advanceUntil("payload", func() bool { return len(h.e.Status().LeaderPayload) > 0 })

	h.e.Status().LeaderPayload[0] = 'X'
	assert.Equal(t, string(h.e.Status().LeaderPayload), "endpoint", "payload after a caller's edit")
}
