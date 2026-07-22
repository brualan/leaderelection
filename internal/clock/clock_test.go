package clock

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

var origin = time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)

func TestSystemClockAdvances(t *testing.T) {
	c := System()

	before := c.Now()
	timer := c.NewTimer(time.Millisecond)
	<-timer.C()
	if !c.Now().After(before) {
		t.Fatal("system clock did not advance across a timer")
	}
	timer.Stop()
}

// Reset has to be safe on a timer that already fired and was not drained,
// because the pre-1.23 timer semantics are still what an importing program may
// opt into.
func TestSystemTimerResetAfterFiring(t *testing.T) {
	timer := System().NewTimer(time.Millisecond)
	time.Sleep(20 * time.Millisecond) // let it fire, deliberately undrained

	timer.Reset(time.Millisecond)
	select {
	case <-timer.C():
	case <-time.After(time.Second):
		t.Fatal("timer did not fire after Reset")
	}

	// Resetting a drained timer is the ordinary path.
	timer.Reset(time.Millisecond)
	select {
	case <-timer.C():
	case <-time.After(time.Second):
		t.Fatal("timer did not fire after a second Reset")
	}
	timer.Stop()
}

func TestSystemWithTimeout(t *testing.T) {
	ctx, cancel := System().WithTimeout(context.Background(), time.Millisecond)
	defer cancel()

	<-ctx.Done()
	if !errors.Is(ctx.Err(), context.DeadlineExceeded) {
		t.Fatalf("ctx.Err() = %v, want DeadlineExceeded", ctx.Err())
	}
}

func TestFakeStartsWhereItIsPut(t *testing.T) {
	f := NewFake(origin)
	if !f.Now().Equal(origin) {
		t.Fatalf("Now() = %v, want %v", f.Now(), origin)
	}
	if f.Timers() != 0 {
		t.Fatalf("Timers() = %d, want 0", f.Timers())
	}
}

func TestFakeAdvanceFiresDueTimers(t *testing.T) {
	f := NewFake(origin)
	timer := f.NewTimer(10 * time.Second)

	fired := make(chan time.Time, 1)
	go func() { fired <- <-timer.C() }()
	f.WaitForTimers(1)

	// Short of the deadline nothing happens, and the clock still moves.
	f.Advance(9 * time.Second)
	select {
	case at := <-fired:
		t.Fatalf("timer fired early at %v", at)
	default:
	}
	if !f.Now().Equal(origin.Add(9 * time.Second)) {
		t.Fatalf("Now() = %v, want %v", f.Now(), origin.Add(9*time.Second))
	}

	f.Advance(time.Second)
	at := <-fired
	// The timer must observe the clock at its own deadline, not wherever the
	// caller happened to advance to.
	if !at.Equal(origin.Add(10 * time.Second)) {
		t.Fatalf("timer fired at %v, want %v", at, origin.Add(10*time.Second))
	}
	if f.Timers() != 0 {
		t.Fatalf("Timers() = %d after firing, want 0", f.Timers())
	}
}

// One Advance may sweep past several deadlines at once. Each timer must still
// be told the clock stood at its own deadline, not wherever the sweep ended,
// or a goroutine woken mid-sweep would compute its next deadline from a time
// that had not arrived yet.
func TestFakeAdvanceSweepsSeveralDeadlines(t *testing.T) {
	f := NewFake(origin)

	var (
		mu       sync.Mutex
		observed = map[time.Duration]time.Duration{}
		wg       sync.WaitGroup
	)
	// Armed out of order on purpose: delivery follows deadlines, not arming.
	for _, d := range []time.Duration{30 * time.Second, 10 * time.Second, 20 * time.Second} {
		timer := f.NewTimer(d)
		wg.Add(1)
		go func() {
			defer wg.Done()
			at := <-timer.C()
			mu.Lock()
			observed[d] = at.Sub(origin)
			mu.Unlock()
		}()
	}
	f.WaitForTimers(3)

	f.Advance(time.Minute)
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	for _, d := range []time.Duration{10 * time.Second, 20 * time.Second, 30 * time.Second} {
		if got, ok := observed[d]; !ok || got != d {
			t.Fatalf("timer armed for %v observed %v (present=%v), want %v", d, got, ok, d)
		}
	}
	if !f.Now().Equal(origin.Add(time.Minute)) {
		t.Fatalf("Now() = %v, want the advance target %v", f.Now(), origin.Add(time.Minute))
	}
}

func TestFakeTimerReset(t *testing.T) {
	f := NewFake(origin)
	timer := f.NewTimer(10 * time.Second)

	// Resetting replaces the pending deadline rather than adding one.
	timer.Reset(30 * time.Second)
	if f.Timers() != 1 {
		t.Fatalf("Timers() = %d after Reset, want 1", f.Timers())
	}

	fired := make(chan time.Time, 1)
	go func() { fired <- <-timer.C() }()
	f.WaitForTimers(1)

	f.Advance(20 * time.Second)
	select {
	case at := <-fired:
		t.Fatalf("timer fired at %v despite being reset past that point", at)
	default:
	}

	f.Advance(20 * time.Second)
	if at := <-fired; !at.Equal(origin.Add(30 * time.Second)) {
		t.Fatalf("timer fired at %v, want %v", at, origin.Add(30*time.Second))
	}
}

func TestFakeTimerStop(t *testing.T) {
	f := NewFake(origin)
	timer := f.NewTimer(10 * time.Second)

	timer.Stop()
	timer.Stop() // idempotent

	f.Advance(time.Minute)
	select {
	case at := <-timer.C():
		t.Fatalf("stopped timer fired at %v", at)
	default:
	}

	// A stopped timer is reusable, which is what the watchdog loop relies on.
	timer.Reset(5 * time.Second)
	fired := make(chan time.Time, 1)
	go func() { fired <- <-timer.C() }()
	f.WaitForTimers(1)
	f.Advance(5 * time.Second)
	<-fired
}

// Advance blocks until every timer it fires has been received from. A timer
// stopped while a delivery is in flight must release it rather than deadlock
// the advancing goroutine.
func TestFakeAdvanceIsReleasedByStop(t *testing.T) {
	f := NewFake(origin)
	timer := f.NewTimer(time.Second)

	done := make(chan struct{})
	go func() {
		defer close(done)
		f.Advance(2 * time.Second)
	}()

	// Nobody is receiving, so Advance is parked on the delivery. Stopping the
	// timer is what lets it through.
	timer.Stop()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Advance did not return after the pending timer was stopped")
	}
}

func TestFakeWaitForTimers(t *testing.T) {
	f := NewFake(origin)

	waited := make(chan struct{})
	go func() {
		f.WaitForTimers(2)
		close(waited)
	}()

	select {
	case <-waited:
		t.Fatal("WaitForTimers returned before any timer was armed")
	case <-time.After(10 * time.Millisecond):
	}

	f.NewTimer(time.Second)
	f.NewTimer(time.Second)
	<-waited

	// Already-satisfied waits return immediately.
	f.WaitForTimers(1)
}

func TestFakeWithTimeout(t *testing.T) {
	f := NewFake(origin)
	ctx, cancel := f.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	f.WaitForTimers(1)

	f.Advance(9 * time.Second)
	if ctx.Err() != nil {
		t.Fatal("context expired before its virtual deadline")
	}

	f.Advance(time.Second)
	<-ctx.Done()
	// The deadline is virtual, so cancellation is how it surfaces; the reason
	// is carried by the cause, which behaves the same under the system clock.
	if !errors.Is(context.Cause(ctx), context.DeadlineExceeded) {
		t.Fatalf("context.Cause = %v, want DeadlineExceeded", context.Cause(ctx))
	}
}

func TestFakeWithTimeoutCancelledEarly(t *testing.T) {
	f := NewFake(origin)
	ctx, cancel := f.WithTimeout(context.Background(), 10*time.Second)
	f.WaitForTimers(1)

	cancel()
	<-ctx.Done()
	if !errors.Is(context.Cause(ctx), context.Canceled) {
		t.Fatalf("context.Cause = %v, want Canceled", context.Cause(ctx))
	}

	// Cancelling must retire the timer on the calling goroutine, so a test
	// that counts armed timers is not racing an unscheduled goroutine.
	if n := f.Timers(); n != 0 {
		t.Fatalf("Timers() = %d after cancel, want 0", n)
	}
	// Advancing past the original deadline must not resurrect anything.
	f.Advance(time.Minute)
}

func TestFakeWithTimeoutFollowsItsParent(t *testing.T) {
	f := NewFake(origin)
	parent, cancelParent := context.WithCancel(context.Background())
	ctx, cancel := f.WithTimeout(parent, time.Hour)
	defer cancel()

	cancelParent()
	<-ctx.Done()
}

// A timer armed for a deadline already behind the clock fires on the next
// advance rather than being lost.
func TestFakeTimerInThePast(t *testing.T) {
	f := NewFake(origin)
	timer := f.NewTimer(-time.Second)

	fired := make(chan time.Time, 1)
	go func() { fired <- <-timer.C() }()
	f.WaitForTimers(1)

	f.Advance(0)
	at := <-fired
	// Time must not run backwards even though the deadline was in the past.
	if at.Before(origin) {
		t.Fatalf("timer fired at %v, before the clock's own position %v", at, origin)
	}
	if f.Now().Before(origin) {
		t.Fatalf("Now() = %v moved backwards from %v", f.Now(), origin)
	}
}

// A timer that has been re-armed must still be able to release an Advance that
// is parked on delivering to it. Arming allocates a fresh cancel channel, and
// forgetting to clear the flag alongside it turns every later Stop into a
// no-op -- which deadlocks the advancing goroutine rather than failing
// anything, so nothing points at the cause.
func TestFakeAdvanceIsReleasedByStopAfterReset(t *testing.T) {
	f := NewFake(origin)
	timer := f.NewTimer(time.Second)

	// Fire and consume once, then re-arm: this is the watchdog's loop.
	go f.Advance(time.Second)
	<-timer.C()
	timer.Reset(time.Second)
	f.WaitForTimers(1)

	done := make(chan struct{})
	go func() {
		defer close(done)
		f.Advance(2 * time.Second)
	}()

	// Nobody is receiving this time, so Advance is parked on the delivery.
	timer.Stop()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Advance was not released by Stop after the timer had been re-armed")
	}
}
