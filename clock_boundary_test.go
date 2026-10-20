package leaderelection

import (
	"runtime"
	"testing"
	"time"
)

// Characterize a documented unsupported fault, rather than claiming that the
// election survives it. No host/container clock is changed. The database model
// jumps ahead while the incumbent's monotonic clock stands still.
func TestDatabaseForwardClockJumpRequiresReceiverFencing(t *testing.T) {
	s := newSim(t, simOptions{nodes: 3})
	s.run(3*simLease, nil)
	incumbent := s.leader()
	if incumbent == nil {
		t.Fatal("no incumbent before the clock jump")
	}
	old, ok := incumbent.elector.Fence()
	if !ok {
		t.Fatal("incumbent has no fence")
	}
	s.store.advance(10 * simLease)
	fresh, err := s.store.execute(func(now time.Time) (Lease, error) {
		return s.store.acquire(now, "sim-election", "clock-jump-successor", simLease, nil), nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if fresh.Holder != "clock-jump-successor" || fresh.Fence <= old {
		t.Fatal("clock jump did not permit a successor")
	}
	if !incumbent.elector.IsLeader() {
		t.Fatal("characterization did not expose the still-live local lease")
	}
	// A receiver activated with the successor's token rejects the old token.
	if old >= fresh.Fence {
		t.Fatal("stale write cannot be distinguished by fencing")
	}
	t.Logf("UNSUPPORTED CLOCK FAULT: DB replaced fence %d with %d while incumbent still considers itself leader; receiver-side fencing is required", old, fresh.Fence)
	// Do not run the normal safety oracle after intentionally violating its
	// clock assumptions. Cleanup still stops and joins every participant.
}

// Clock drift beyond SafetyMargin is also outside the safety contract. A slow
// monotonic clock is different from a harmless wall-clock offset.
func TestUnboundedSlowClockRequiresReceiverFencing(t *testing.T) {
	s := newSim(t, simOptions{nodes: 3})
	s.settle(20 * simLease)
	incumbent := s.leader()
	if incumbent == nil {
		t.Fatal("no incumbent")
	}
	incumbent.rate = 500
	s.breakPath(incumbent, simHung, 0, 10*simLease)
	// Deliberately bypass the ordinary safety oracle while breaking its bounded
	// drift assumption; require the unsafe state explicitly instead.
	for elapsed := time.Duration(0); elapsed < 4*time.Second; elapsed += simStep {
		s.awaitParked()
		s.mu.Lock()
		s.trueNow += simStep
		s.mu.Unlock()
		s.advanceClocks()
		for i := 0; i < 5; i++ {
			runtime.Gosched()
		}
	}
	if !incumbent.elector.IsLeader() {
		t.Fatal("slow clock did not preserve the stale local lease")
	}
	old, _ := incumbent.elector.Fence()
	var fresh int64
	for _, node := range s.nodes {
		if node != incumbent {
			if fence, ok := node.elector.Fence(); ok {
				fresh = fence
			}
		}
	}
	if fresh <= old {
		t.Fatal("characterization did not produce a successor while old local lease remained live")
	}
	t.Logf("UNSUPPORTED CLOCK FAULT: rate=0.5, stale fence=%d and successor fence=%d; bounded drift or receiver fencing required", old, fresh)
}
