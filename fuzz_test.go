package leaderelection

import (
	"math"
	"testing"
	"time"
)

// Property-based tests for the two pieces of arithmetic the safety argument
// rests on: the timing invariant that decides which configurations are
// accepted, and the duration a Lease reports.
//
// Both are small enough to read and believe. That is exactly why they are worth
// fuzzing: a reader believes them for the inputs a reader thinks of, and a
// participant configured by someone else's Helm chart supplies the rest.

// FuzzElectionUnderFaults runs a whole election -- several participants, one
// shared record, clocks that disagree on how fast time passes -- against a
// schedule of faults taken from the fuzz input.
//
// Three properties are asserted, and the third is the one that is easy to
// forget. Safety: no two participants believe they lead at the same instant of
// virtual time, sampled at every step. Fencing: no two terms anywhere share a
// token. Liveness: once the schedule runs out of instructions and every path
// is healthy again, somebody leads. An election that stays safe by never
// electing anyone has not survived the storm; it has only stopped.
//
// See simulation_test.go for what the adversary is and is not allowed to do.
func FuzzElectionUnderFaults(f *testing.F) {
	// A few schedules by hand: nothing at all, one long fault, and a couple of
	// mixed ones. The fuzzer is better at the rest than any list would be.
	f.Add([]byte(nil), uint8(3), uint8(0))
	f.Add([]byte{0, 2, 200, 0}, uint8(3), uint8(0))
	f.Add([]byte{0, 1, 40, 30, 1, 2, 90, 0, 2, 4, 120, 10}, uint8(3), uint8(0b01001001))
	f.Add([]byte{0, 5, 90, 1, 3, 60, 2, 2, 250, 0, 4, 30, 1, 1, 200, 90}, uint8(4), uint8(0b00011011))

	f.Fuzz(func(t *testing.T, schedule []byte, participants, skew uint8) {
		// Two participants are enough for a handover; four is enough for one to
		// be left behind. More than that costs simulation time without reaching
		// states the fourth does not.
		nodes := 2 + int(participants)%3

		// Clock rates in parts per thousand, two bits of the skew byte each.
		// The floor is the drift the safety margin is required to absorb: with
		// margin m and lease L a participant is safe down to 1-m/L of true
		// time, which is 0.867 here.
		rates := make([]int, nodes)
		for i := range rates {
			rates[i] = 930 + int((skew>>(2*i))&3)*70
		}

		s := newSim(t, simOptions{nodes: nodes, rates: rates})

		// Long enough for the whole schedule to be spent, bounded so that one
		// oversized input cannot dominate a fuzzing session.
		budget := min(max(time.Duration(len(schedule))*simFaultEvery, 20*time.Second), 2*time.Minute)
		s.run(budget, &simScript{data: schedule})

		s.settle(20 * simLease)
		s.verify()
	})
}

// FuzzConfigValidate asserts that every configuration [New] accepts really can
// survive a lost renewal.
//
// Validate exists to reject configurations where a single slow round trip costs
// leadership. If it can be talked into accepting one -- by arithmetic that
// overflows, say -- the library ships a foot-gun that only fires in production,
// under load, on the deployment whose numbers came from somewhere else.
func FuzzConfigValidate(f *testing.F) {
	// The defaults, a scaled-down set like the chaos suite's, and the shapes
	// most likely to break the arithmetic rather than the logic.
	f.Add(int64(DefaultLeaseDuration), int64(DefaultRenewInterval), int64(DefaultRetryPeriod),
		int64(DefaultOperationTimeout), int64(0))
	f.Add(int64(3*time.Second), int64(900*time.Millisecond), int64(500*time.Millisecond),
		int64(700*time.Millisecond), int64(400*time.Millisecond))
	f.Add(int64(0), int64(0), int64(0), int64(0), int64(0))
	f.Add(int64(math.MaxInt64), int64(math.MaxInt64), int64(math.MaxInt64), int64(math.MaxInt64), int64(0))
	f.Add(int64(math.MaxInt64), int64(math.MaxInt64/4), int64(1), int64(math.MaxInt64/4), int64(1))
	f.Add(int64(-1), int64(1), int64(1), int64(1), int64(-1))
	f.Add(int64(time.Second), int64(time.Nanosecond), int64(time.Nanosecond), int64(time.Nanosecond), int64(1))

	f.Fuzz(func(t *testing.T, lease, renew, retry, opTimeout, margin int64) {
		cfg := Config{
			Store:            newFakeStore(),
			Name:             "fuzz",
			Identity:         "me",
			LeaseDuration:    time.Duration(lease),
			RenewInterval:    time.Duration(renew),
			RetryPeriod:      time.Duration(retry),
			OperationTimeout: time.Duration(opTimeout),
			SafetyMargin:     time.Duration(margin),
		}

		e, err := New(cfg)
		if err != nil {
			// Rejection needs no further justification: a configuration the
			// library refuses cannot hurt anyone.
			return
		}

		// From here on, cfg is what the elector will actually run with, defaults
		// filled in. Every claim Validate makes must hold of it.
		got := e.cfg
		switch {
		case got.LeaseDuration <= 0:
			t.Fatalf("accepted LeaseDuration %v", got.LeaseDuration)
		case got.RenewInterval <= 0:
			t.Fatalf("accepted RenewInterval %v", got.RenewInterval)
		case got.RetryPeriod <= 0:
			t.Fatalf("accepted RetryPeriod %v", got.RetryPeriod)
		case got.OperationTimeout <= 0:
			t.Fatalf("accepted OperationTimeout %v", got.OperationTimeout)
		case got.SafetyMargin < 0:
			t.Fatalf("accepted SafetyMargin %v", got.SafetyMargin)
		case got.SafetyMargin >= got.LeaseDuration:
			t.Fatalf("accepted SafetyMargin %v against LeaseDuration %v",
				got.SafetyMargin, got.LeaseDuration)
		}

		// The invariant itself, recomputed independently. Reusing
		// Config.renewalBudget here would prove only that it agrees with
		// itself; this sum is built in a wider domain so that an overflow the
		// production code failed to notice shows up as one.
		budget, ok := renewalBudgetChecked(got)
		if !ok {
			t.Fatalf("accepted a configuration whose renewal budget overflows: "+
				"lease=%v renew=%v op=%v", got.LeaseDuration, got.RenewInterval, got.OperationTimeout)
		}
		if available := int64(got.LeaseDuration) - int64(got.SafetyMargin); budget > available {
			t.Fatalf("accepted a configuration that cannot survive one lost renewal: "+
				"needs %dns, has %dns (lease=%v renew=%v op=%v margin=%v)",
				budget, available, got.LeaseDuration, got.RenewInterval,
				got.OperationTimeout, got.SafetyMargin)
		}

		// Validate must agree with itself: a defaulted config that New accepted
		// cannot be one Validate rejects.
		if err := got.Validate(); err != nil {
			t.Fatalf("New accepted a configuration Validate rejects: %v", err)
		}
	})
}

// renewalBudgetChecked is [Config.renewalBudget] computed so that an overflow
// is reported rather than wrapped into a small, innocent-looking number.
func renewalBudgetChecked(c Config) (int64, bool) {
	terms := []int64{
		int64(c.RenewInterval),
		int64(c.OperationTimeout),
		int64(c.OperationTimeout),
		int64(renewRetryDelayFor(c.RenewInterval)),
	}
	var sum int64
	for _, term := range terms {
		if term > math.MaxInt64-sum {
			return 0, false
		}
		sum += term
	}
	return sum, true
}

// FuzzLeaseArithmetic asserts that the three questions the election asks of a
// Lease stay mutually consistent for any timestamps a store might report,
// including the ones no sane store would.
//
// The elector reads a lease and decides whether to keep leading. If TTL could
// go negative, or Expired could disagree with TTL, that decision would be made
// on arithmetic rather than on facts.
func FuzzLeaseArithmetic(f *testing.F) {
	f.Add(int64(0), int64(time.Minute), "a", "a")
	f.Add(int64(0), int64(-time.Minute), "a", "b")
	f.Add(int64(math.MaxInt64), int64(math.MinInt64), "", "")
	f.Add(int64(math.MinInt64), int64(math.MaxInt64), "a", "")

	f.Fuzz(func(t *testing.T, nowOffset, expiryOffset int64, holder, identity string) {
		// Anchored at a fixed origin and offset by the fuzzed amounts, so that
		// the pair can straddle any distance -- including one far larger than a
		// Duration can hold, which is where saturation has to behave.
		base := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
		lease := Lease{
			Holder:    holder,
			Now:       base.Add(time.Duration(nowOffset)),
			ExpiresAt: base.Add(time.Duration(nowOffset)).Add(time.Duration(expiryOffset)),
		}

		ttl := lease.TTL()
		if ttl < 0 {
			t.Fatalf("TTL = %v, want non-negative (now=%v expires=%v)", ttl, lease.Now, lease.ExpiresAt)
		}
		// A lease is expired exactly when there is no time left on it. Anything
		// else would let the elector believe a lease is live while reporting
		// that none of it remains, or the reverse.
		if expired := lease.Expired(); expired != (ttl == 0) {
			t.Fatalf("Expired = %v but TTL = %v (now=%v expires=%v)",
				expired, ttl, lease.Now, lease.ExpiresAt)
		}
		if held, want := lease.HeldBy(identity), holder == identity && !lease.Expired(); held != want {
			t.Fatalf("HeldBy(%q) = %v, want %v (holder=%q expired=%v)",
				identity, held, want, holder, lease.Expired())
		}
	})
}
