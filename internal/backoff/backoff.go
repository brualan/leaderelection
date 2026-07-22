// Package backoff computes retry delays with jitter.
//
// Jitter matters here beyond the usual load-smoothing argument: every follower
// in every cluster polls the same row, and an unjittered retry period makes
// them converge into a synchronised herd that all wake, all read, and all
// contend at the same instant.
package backoff

import (
	"math/rand/v2"
	"time"
)

// maxAttempt caps the exponent so that Factor^attempt cannot overflow into a
// negative or absurd duration on a long-running follower.
const maxAttempt = 32

// Backoff produces an exponentially growing, jittered delay. The zero value is
// not usable; construct one with [New].
type Backoff struct {
	base    time.Duration
	max     time.Duration
	factor  float64
	jitter  float64
	rand    func() float64
	attempt int
}

// New returns a Backoff that starts at base, multiplies by factor per attempt,
// and is capped at max. jitter is the fraction of the computed delay that is
// randomly subtracted, in [0, 1]: 0.2 yields a delay in [0.8d, d].
//
// randFloat supplies values in [0, 1) and may be nil, in which case the global
// source is used. Tests inject it to make delays deterministic.
func New(base, max time.Duration, factor, jitter float64, randFloat func() float64) *Backoff {
	if randFloat == nil {
		randFloat = rand.Float64
	}
	return &Backoff{base: base, max: max, factor: factor, jitter: jitter, rand: randFloat}
}

// Next returns the delay for the current attempt and advances the sequence.
func (b *Backoff) Next() time.Duration {
	d := float64(b.base)
	for i := 0; i < b.attempt; i++ {
		d *= b.factor
	}
	if b.attempt < maxAttempt {
		b.attempt++
	}
	if d > float64(b.max) {
		d = float64(b.max)
	}
	d -= d * b.jitter * b.rand()
	return clamp(d, b.max)
}

// Reset returns the sequence to its first attempt. It is called whenever an
// operation succeeds.
func (b *Backoff) Reset() { b.attempt = 0 }

// Jitter returns d reduced by a random fraction of up to jitter. It is used
// for the steady-state polling interval, which does not grow but still must
// not be synchronised across participants.
func Jitter(d time.Duration, jitter float64, randFloat func() float64) time.Duration {
	if randFloat == nil {
		randFloat = rand.Float64
	}
	// Jitter only ever shortens: Config.Validate budgets for the unjittered
	// interval, so a result longer than d would spend time the timing invariant
	// never reserved for it.
	return clamp(float64(d)-float64(d)*jitter*randFloat(), d)
}

// clamp turns a computed delay into a Duration inside [0, ceiling] that a
// timer can be armed with.
//
// Both bounds have teeth. A negative delay makes the timer fire at once,
// turning the retry it was pacing into a spin against a database that is
// already struggling -- and a delay near the top of the range gets there by
// converting through a float64 that rounds *above* the largest Duration, an
// out-of-range conversion that wraps to the most negative one. A ceiling of
// LeaseDuration is enough to reach that, so the ceiling is re-applied here in
// Duration arithmetic rather than trusted from the float64 comparison that
// cannot tell those two values apart.
func clamp(d float64, ceiling time.Duration) time.Duration {
	ceiling = max(ceiling, 0)
	switch {
	case !(d > 0): // also catches NaN
		return 0
	case d >= float64(ceiling):
		// At the top of the range a Duration and the float64 it converts to
		// are not the same number: the conversion rounds, sometimes upwards,
		// and converting back is then out of range. Comparing in float64 and
		// answering with the Duration keeps both ends honest.
		return ceiling
	default:
		return time.Duration(d)
	}
}
