// Package clock abstracts the passage of time so that the election loop can be
// driven deterministically by tests.
//
// The election loop reads time for exactly two purposes: to schedule its next
// action, and to compute how long it may keep believing it is the leader. The
// second one is a safety property, so the tests that exercise it must not
// depend on wall-clock timing to pass.
package clock

import (
	"context"
	"time"
)

// Clock provides the subset of the time package the election loop needs.
type Clock interface {
	// Now returns the current time. Callers must only use differences between
	// values returned by the same Clock; with [System] those differences are
	// monotonic and unaffected by wall-clock adjustments.
	Now() time.Time

	// NewTimer returns a timer that fires once after d.
	NewTimer(d time.Duration) Timer

	// WithTimeout derives a context that is cancelled after d has elapsed on
	// this Clock.
	WithTimeout(ctx context.Context, d time.Duration) (context.Context, context.CancelFunc)
}

// Timer is a single-shot timer. A Timer has one owning goroutine: Reset and
// Stop must not be called concurrently with a receive from C.
type Timer interface {
	// C is the channel on which the firing time is delivered.
	C() <-chan time.Time
	// Reset restarts the timer to fire after d. It is safe to call after the
	// timer has fired and been received from.
	Reset(d time.Duration)
	// Stop prevents the timer from firing. It is idempotent.
	Stop()
}

// System returns a Clock backed by the time package.
func System() Clock { return systemClock{} }

type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now() }

func (systemClock) NewTimer(d time.Duration) Timer {
	return &systemTimer{timer: time.NewTimer(d)}
}

func (systemClock) WithTimeout(ctx context.Context, d time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, d)
}

type systemTimer struct {
	timer *time.Timer
}

func (t *systemTimer) C() <-chan time.Time { return t.timer.C }

func (t *systemTimer) Reset(d time.Duration) {
	// Drain a value that fired before Stop took effect. Go 1.23 made this
	// unnecessary, but only when the *main* module opts in to the new timer
	// semantics, which is the importing program's choice and not ours.
	if !t.timer.Stop() {
		select {
		case <-t.timer.C:
		default:
		}
	}
	t.timer.Reset(d)
}

func (t *systemTimer) Stop() { t.timer.Stop() }
