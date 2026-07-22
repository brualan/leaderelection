package clock

import (
	"context"
	"sync"
	"time"
)

// Fake is a Clock whose time only moves when a test moves it.
//
// Two properties make it usable for testing a concurrent loop:
//
//   - [Fake.Advance] fires due timers in chronological order and does not
//     return until every fired value has been received. When it returns, the
//     goroutines it woke are running rather than merely runnable.
//   - [Fake.WaitForTimers] blocks until at least n timers are armed, which
//     lets a test wait for the loop to reach its next scheduling point instead
//     of guessing.
//
// Fake is safe for concurrent use.
type Fake struct {
	mu     sync.Mutex
	cond   *sync.Cond
	now    time.Time
	seq    uint64
	timers map[*fakeTimer]struct{}
}

// NewFake returns a Fake clock positioned at start.
func NewFake(start time.Time) *Fake {
	f := &Fake{
		now:    start,
		timers: make(map[*fakeTimer]struct{}),
	}
	f.cond = sync.NewCond(&f.mu)
	return f
}

// Now returns the current fake time.
func (f *Fake) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.now
}

// NewTimer returns a timer that fires once the fake clock reaches d from now.
func (f *Fake) NewTimer(d time.Duration) Timer {
	t := &fakeTimer{clock: f, c: make(chan time.Time)}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.armLocked(t, d)
	return t
}

// WithTimeout derives a context cancelled once the fake clock advances past d.
//
// Because the deadline is virtual, the context is cancelled rather than
// expired: ctx.Err reports [context.Canceled] while [context.Cause] reports
// [context.DeadlineExceeded]. Code under test should consult context.Cause,
// which behaves identically under the system clock.
func (f *Fake) WithTimeout(parent context.Context, d time.Duration) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancelCause(parent)
	timer := f.NewTimer(d)
	go func() {
		select {
		case <-timer.C():
			cancel(context.DeadlineExceeded)
		case <-ctx.Done():
			timer.Stop()
		}
	}()
	// Stop the timer on the calling goroutine rather than leaving it to the
	// one above. A test that counts armed timers to decide whether the code
	// under test has settled must not race against that goroutine being
	// scheduled.
	return ctx, func() {
		timer.Stop()
		cancel(context.Canceled)
	}
}

// Advance moves the clock forward by d, firing every timer that becomes due.
//
// Timers fire in deadline order, with the clock positioned at each timer's own
// deadline as it fires, so a timer armed by a goroutine that Advance just woke
// still observes a consistent view of time. Advance returns only after every
// fired value has been received or its timer has been stopped.
func (f *Fake) Advance(d time.Duration) {
	f.mu.Lock()
	target := f.now.Add(d)
	f.mu.Unlock()

	for {
		f.mu.Lock()
		next := f.earliestDueLocked(target)
		if next == nil {
			if target.After(f.now) {
				f.now = target
			}
			f.cond.Broadcast()
			f.mu.Unlock()
			return
		}
		if next.deadline.After(f.now) {
			f.now = next.deadline
		}
		now, cancel := f.now, next.cancel
		f.disarmLocked(next)
		f.mu.Unlock()

		// Blocking send: the point of Advance is that the woken goroutine has
		// actually observed the tick by the time we return. The cancel case
		// keeps us from deadlocking against a timer stopped mid-flight.
		select {
		case next.c <- now:
		case <-cancel:
		}
	}
}

// WaitForTimers blocks until at least n timers are armed. It is the
// synchronisation point a test uses to know the loop under test has finished
// reacting and is waiting for time to pass again.
func (f *Fake) WaitForTimers(n int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for len(f.timers) < n {
		f.cond.Wait()
	}
}

// Timers reports how many timers are currently armed.
func (f *Fake) Timers() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.timers)
}

func (f *Fake) earliestDueLocked(target time.Time) *fakeTimer {
	var best *fakeTimer
	for t := range f.timers {
		if t.deadline.After(target) {
			continue
		}
		if best == nil || t.deadline.Before(best.deadline) || (t.deadline.Equal(best.deadline) && t.seq < best.seq) {
			best = t
		}
	}
	return best
}

// armLocked schedules t for d from now, cancelling any previous arming.
func (f *Fake) armLocked(t *fakeTimer, d time.Duration) {
	f.cancelLocked(t)
	f.seq++
	t.seq = f.seq
	t.deadline = f.now.Add(d)
	// A fresh arming gets a fresh cancel channel, and the flag has to be
	// cleared with it. Leaving it set would make every later Stop a no-op, so
	// an Advance parked on a delivery to this timer could never be released.
	t.cancel = make(chan struct{})
	t.cancelled = false
	f.timers[t] = struct{}{}
	f.cond.Broadcast()
}

// disarmLocked removes t from the armed set without cancelling an in-flight
// delivery.
func (f *Fake) disarmLocked(t *fakeTimer) {
	delete(f.timers, t)
	f.cond.Broadcast()
}

// cancelLocked removes t from the armed set and releases any in-flight
// delivery blocked in Advance.
func (f *Fake) cancelLocked(t *fakeTimer) {
	delete(f.timers, t)
	if t.cancel != nil && !t.cancelled {
		t.cancelled = true
		close(t.cancel)
	}
	f.cond.Broadcast()
}

type fakeTimer struct {
	clock *Fake
	c     chan time.Time

	// Guarded by clock.mu.
	seq       uint64
	deadline  time.Time
	cancel    chan struct{}
	cancelled bool
}

func (t *fakeTimer) C() <-chan time.Time { return t.c }

func (t *fakeTimer) Reset(d time.Duration) {
	t.clock.mu.Lock()
	defer t.clock.mu.Unlock()
	t.clock.armLocked(t, d)
}

func (t *fakeTimer) Stop() {
	t.clock.mu.Lock()
	defer t.clock.mu.Unlock()
	t.clock.cancelLocked(t)
}
