package backoff

import (
	"testing"
	"time"
)

// fixed returns a deterministic stand-in for the random source.
func fixed(v float64) func() float64 { return func() float64 { return v } }

func TestBackoffGrowsAndCaps(t *testing.T) {
	b := New(time.Second, 8*time.Second, 2, 0, fixed(0))

	want := []time.Duration{
		time.Second, 2 * time.Second, 4 * time.Second,
		8 * time.Second, 8 * time.Second, 8 * time.Second,
	}
	for i, w := range want {
		if got := b.Next(); got != w {
			t.Fatalf("attempt %d: got %v, want %v", i, got, w)
		}
	}
}

func TestBackoffReset(t *testing.T) {
	b := New(time.Second, time.Minute, 2, 0, fixed(0))
	b.Next()
	b.Next()

	b.Reset()
	if got := b.Next(); got != time.Second {
		t.Fatalf("after Reset: got %v, want %v", got, time.Second)
	}
}

func TestBackoffJitterSubtracts(t *testing.T) {
	// Jitter only ever reduces the delay, so a retry never lands later than
	// the nominal schedule -- which matters when the schedule is sized
	// against a lease that is running out.
	b := New(10*time.Second, time.Minute, 2, 0.2, fixed(1))
	if got := b.Next(); got != 8*time.Second {
		t.Fatalf("full jitter: got %v, want %v", got, 8*time.Second)
	}

	b = New(10*time.Second, time.Minute, 2, 0.2, fixed(0))
	if got := b.Next(); got != 10*time.Second {
		t.Fatalf("no jitter: got %v, want %v", got, 10*time.Second)
	}
}

// The exponent is capped so that a follower left polling a dead database for
// weeks cannot overflow its delay into something nonsensical.
func TestBackoffDoesNotOverflow(t *testing.T) {
	b := New(time.Hour, 24*time.Hour, 10, 0, fixed(0))
	for i := range maxAttempt * 4 {
		got := b.Next()
		if got <= 0 || got > 24*time.Hour {
			t.Fatalf("attempt %d: delay %v escaped (0, %v]", i, got, 24*time.Hour)
		}
	}
	if b.attempt > maxAttempt {
		t.Fatalf("attempt counter reached %d, want it capped at %d", b.attempt, maxAttempt)
	}
}

func TestBackoffClampsToZero(t *testing.T) {
	// A jitter fraction above 1 would otherwise produce a negative delay,
	// which as a timer duration fires instantly and spins.
	b := New(time.Second, time.Minute, 2, 1.5, fixed(1))
	if got := b.Next(); got != 0 {
		t.Fatalf("got %v, want 0", got)
	}
}

func TestBackoffUsesGlobalRandomnessByDefault(t *testing.T) {
	b := New(time.Second, time.Minute, 2, 0.5, nil)
	for range 20 {
		if d := b.Next(); d < 0 {
			t.Fatalf("negative delay %v", d)
		}
	}
}

func TestJitter(t *testing.T) {
	if got := Jitter(10*time.Second, 0.2, fixed(0)); got != 10*time.Second {
		t.Fatalf("got %v, want %v", got, 10*time.Second)
	}
	if got := Jitter(10*time.Second, 0.2, fixed(1)); got != 8*time.Second {
		t.Fatalf("got %v, want %v", got, 8*time.Second)
	}
	if got := Jitter(time.Second, 2, fixed(1)); got != 0 {
		t.Fatalf("got %v, want 0", got)
	}

	// Followers must not converge into a synchronised herd, so the default
	// source has to actually vary.
	seen := map[time.Duration]struct{}{}
	for range 50 {
		seen[Jitter(time.Second, 0.5, nil)] = struct{}{}
	}
	if len(seen) < 2 {
		t.Fatal("jitter with the global source produced a constant delay")
	}
}

// A base above the ceiling is a misconfiguration rather than a crash, and the
// ceiling wins: a delay must never exceed the maximum a caller asked for.
func TestBackoffCapWinsOverBase(t *testing.T) {
	b := New(10*time.Second, 3*time.Second, 2, 0, fixed(0))
	for i := range 5 {
		if got := b.Next(); got != 3*time.Second {
			t.Fatalf("attempt %d: got %v, want the cap %v", i, got, 3*time.Second)
		}
	}
}

// Full jitter takes the delay to zero rather than negative, which as a timer
// duration would fire instantly and spin.
func TestJitterOfOneReachesZero(t *testing.T) {
	if got := Jitter(time.Hour, 1, fixed(1)); got != 0 {
		t.Fatalf("got %v, want 0", got)
	}
	if got := Jitter(time.Hour, 1, fixed(0)); got != time.Hour {
		t.Fatalf("got %v, want the full duration", got)
	}
}
