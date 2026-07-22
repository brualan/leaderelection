package backoff

import (
	"math"
	"testing"
	"time"
)

// A backoff delay is fed straight to a timer. A delay that came out negative --
// or that wrapped on its way from float64 to Duration -- makes that timer fire
// immediately, and the retry loop it was supposed to pace turns into a spin
// against a database that is already in trouble.
//
// The arithmetic is four lines long and every one of them is a place where a
// float64 can leave the range a Duration can hold.

// FuzzBackoffNext asserts the two properties every delay must have, for any
// parameters and any sequence of random draws: it is never negative, and it
// never exceeds the ceiling it was given.
func FuzzBackoffNext(f *testing.F) {
	f.Add(int64(3*time.Second), int64(15*time.Second), uint8(16), uint8(51), []byte{0, 128, 255})
	f.Add(int64(math.MaxInt64), int64(math.MaxInt64), uint8(16), uint8(51), []byte{0})
	f.Add(int64(1), int64(math.MaxInt64), uint8(255), uint8(0), []byte{0, 0, 0, 0})
	f.Add(int64(0), int64(0), uint8(0), uint8(255), []byte{255})
	f.Add(int64(-1), int64(-1), uint8(16), uint8(128), []byte{7})

	f.Fuzz(func(t *testing.T, base, ceiling int64, factorSteps, jitterSteps uint8, draws []byte) {
		if len(draws) == 0 {
			draws = []byte{0}
		}
		// The factor and the jitter fraction are the library's own choices
		// rather than a caller's, so they are fuzzed across the range such a
		// choice could plausibly take instead of across every float64.
		factor := 1 + float64(factorSteps)/16
		jitter := float64(jitterSteps) / 255

		var i int
		next := func() float64 {
			v := float64(draws[i%len(draws)]) / 256
			i++
			return v
		}

		b := New(time.Duration(base), time.Duration(ceiling), factor, jitter, next)
		// Well past maxAttempt, so the exponent cap is exercised rather than
		// merely present.
		for attempt := range 64 {
			d := b.Next()
			if d < 0 {
				t.Fatalf("attempt %d: delay %v is negative (base=%v max=%v factor=%v jitter=%v)",
					attempt, d, time.Duration(base), time.Duration(ceiling), factor, jitter)
			}
			if limit := max(time.Duration(ceiling), 0); d > limit {
				t.Fatalf("attempt %d: delay %v exceeds the ceiling %v (base=%v factor=%v jitter=%v)",
					attempt, d, limit, time.Duration(base), factor, jitter)
			}
		}

		// Reset means the next attempt starts the sequence again, which is what
		// the elector relies on after a success. With the same draw, the delay
		// must match the one the sequence opened with.
		fixed := func() float64 { return 0.5 }
		fresh := New(time.Duration(base), time.Duration(ceiling), factor, jitter, fixed)
		first := fresh.Next()
		for range 8 {
			fresh.Next()
		}
		fresh.Reset()
		if got := fresh.Next(); got != first {
			t.Fatalf("after Reset the delay was %v, want the opening %v", got, first)
		}
	})
}

// FuzzJitter covers the steady-state path, which does not grow but is subject
// to the same conversion at the end of it.
func FuzzJitter(f *testing.F) {
	f.Add(int64(3*time.Second), uint8(51), uint8(128))
	f.Add(int64(math.MaxInt64), uint8(0), uint8(0))
	f.Add(int64(math.MinInt64), uint8(255), uint8(255))

	f.Fuzz(func(t *testing.T, d int64, jitterSteps, draw uint8) {
		jitter := float64(jitterSteps) / 255
		got := Jitter(time.Duration(d), jitter, func() float64 { return float64(draw) / 256 })

		if got < 0 {
			t.Fatalf("Jitter(%v, %v) = %v, want non-negative", time.Duration(d), jitter, got)
		}
		// Jitter only ever shortens. Config.Validate budgets for the unjittered
		// interval on exactly that assumption, so a draw that lengthened one
		// would spend time the invariant never reserved.
		if limit := max(time.Duration(d), 0); got > limit {
			t.Fatalf("Jitter(%v, %v) = %v, longer than the interval it jittered",
				time.Duration(d), jitter, got)
		}
	})
}
