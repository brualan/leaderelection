package storetest

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/brualan/leaderelection"
)

// The acceptance suite above asks a store a fixed list of questions. This asks
// it arbitrary ones.
//
// Every guard in a store implementation is a conjunction -- name, holder,
// fence and expiry, all four -- expressed in SQL, and each dialect expresses
// it differently. A missing clause does not show up as a broken test; it shows
// up as a store that is right about every sequence somebody thought to write
// down. Running arbitrary sequences against a model of what the contract says
// should happen is how the ones nobody thought of get covered.
//
// Expiry is deliberately out of scope: the model would have to predict the
// database's clock, and a test that sleeps for a lease per operation is not
// one a fuzzer can drive. The suite above covers expiry directly.

// modelTTL is long enough that nothing expires during a run, however slow the
// machine or the database.
const modelTTL = 10 * time.Minute

// FuzzOperations drives arbitrary operation sequences against a store and
// checks each result against a model of the contract.
//
// Implementations wire it up as a fuzz target of their own:
//
//	func FuzzStore(f *testing.F) {
//		storetest.FuzzOperations(f, storetest.Suite{Store: memstore.New()})
//	}
func FuzzOperations(f *testing.F, s Suite) {
	if s.Store == nil {
		f.Fatal("storetest: Suite.Store is required")
	}

	// One of each shape: a plain takeover, a guard being tested with a stale
	// fence, a release followed by a reclaim.
	f.Add([]byte{0x00, 0x10, 0x20, 0x30})
	f.Add([]byte{0x00, 0x41, 0x01, 0x11, 0x21})
	f.Add([]byte{0x00, 0x22, 0x00, 0x01, 0x12, 0x23, 0x31})
	f.Add([]byte{0x03, 0x13, 0x23, 0x33, 0x43, 0x53})

	f.Fuzz(func(t *testing.T, program []byte) {
		// A long program against a real database is a long test. The interesting
		// sequences are short; the rest is repetition.
		if len(program) > 64 {
			program = program[:64]
		}

		election := name(t)
		m := &model{}

		for step, code := range program {
			// Two fields per byte: which participant acts, and what it does.
			identity := identities[int(code)%len(identities)]
			switch int(code>>4) % 4 {
			case 0:
				m.checkAcquire(t, s, step, election, identity)
			case 1:
				m.checkRenew(t, s, step, election, identity, m.guess(code))
			case 2:
				m.checkRelease(t, s, step, election, identity, m.guess(code))
			default:
				m.checkInspect(t, s, step, election)
			}
		}
		// Whatever the program did, the record must still describe the state
		// the model believes it left behind.
		m.checkInspect(t, s, len(program), election)
	})
}

// identities are the participants a program can act as. Three is enough for a
// holder, a challenger, and a third that only ever gets in the way.
var identities = []string{"a", "b", "c"}

// model is what the contract says the record should look like.
type model struct {
	exists bool
	holder string
	fence  int64
	// held is false once the lease has been released. With no expiry in play,
	// that is the only way a live lease becomes claimable.
	held bool
}

// guess picks the fence a caller presents. Only one of these is the right one,
// which is the point: three quarters of every guarded call must be rejected.
func (m *model) guess(code byte) int64 {
	switch int(code>>2) % 4 {
	case 0:
		return m.fence
	case 1:
		return m.fence - 1
	case 2:
		return m.fence + 1
	default:
		return 0
	}
}

func (m *model) checkAcquire(t *testing.T, s Suite, step int, election, identity string) {
	t.Helper()

	lease, err := s.Store.Acquire(ctx(t), election, identity, modelTTL, nil)
	if err != nil {
		t.Fatalf("step %d: Acquire(%s): %v", step, identity, err)
	}

	switch {
	case !m.exists:
		m.exists, m.holder, m.held = true, identity, true
		m.fence = 1
	case !m.held:
		// Reclaiming a lease nobody holds starts a new term, whoever the
		// claimant is: while it was free, anyone could have led.
		m.fence++
		m.holder, m.held = identity, true
	case m.holder == identity:
		// An uninterrupted term keeps its token.
	}
	m.assertMatches(t, step, fmt.Sprintf("Acquire(%s)", identity), lease)

	if got, want := lease.HeldBy(identity), m.holder == identity && m.held; got != want {
		t.Fatalf("step %d: Acquire(%s): HeldBy = %v, want %v (%s)",
			step, identity, got, want, m)
	}
}

func (m *model) checkRenew(t *testing.T, s Suite, step int, election, identity string, fence int64) {
	t.Helper()

	lease, err := s.Store.Renew(ctx(t), election, identity, fence, modelTTL, nil)
	entitled := m.exists && m.held && m.holder == identity && m.fence == fence

	if !entitled {
		if !errors.Is(err, leaderelection.ErrLeaseLost) {
			t.Fatalf("step %d: Renew(%s, fence %d) returned %v, want ErrLeaseLost (%s)",
				step, identity, fence, err, m)
		}
		// A rejected renewal must leave the term exactly as it found it.
		m.checkInspect(t, s, step, election)
		return
	}
	if err != nil {
		t.Fatalf("step %d: Renew(%s, fence %d): %v (%s)", step, identity, fence, err, m)
	}
	m.assertMatches(t, step, fmt.Sprintf("Renew(%s)", identity), lease)
}

func (m *model) checkRelease(t *testing.T, s Suite, step int, election, identity string, fence int64) {
	t.Helper()

	// Releasing is never an error: the end state the caller wanted -- that it
	// is not the leader -- already holds when the guard does not match.
	if err := s.Store.Release(ctx(t), election, identity, fence); err != nil {
		t.Fatalf("step %d: Release(%s, fence %d): %v", step, identity, fence, err)
	}
	if m.exists && m.held && m.holder == identity && m.fence == fence {
		m.held = false
	}
	m.checkInspect(t, s, step, election)
}

func (m *model) checkInspect(t *testing.T, s Suite, step int, election string) {
	t.Helper()

	lease, err := s.Store.Inspect(ctx(t), election)
	if !m.exists {
		if !errors.Is(err, leaderelection.ErrNoLease) {
			t.Fatalf("step %d: Inspect returned %v, want ErrNoLease", step, err)
		}
		return
	}
	if err != nil {
		t.Fatalf("step %d: Inspect: %v (%s)", step, err, m)
	}
	m.assertMatches(t, step, "Inspect", lease)
}

// assertMatches checks a returned lease against the model.
func (m *model) assertMatches(t *testing.T, step int, what string, lease leaderelection.Lease) {
	t.Helper()

	if lease.Holder != m.holder {
		t.Fatalf("step %d: %s: holder = %q, want %q (%s)", step, what, lease.Holder, m.holder, m)
	}
	if lease.Fence != m.fence {
		t.Fatalf("step %d: %s: fence = %d, want %d (%s)", step, what, lease.Fence, m.fence, m)
	}
	// A released lease is claimable, and a live one is not. Getting this
	// backwards is how a store lets a successor wait out a lease that was
	// handed back, or lets two participants hold one that was not.
	if expired := lease.Expired(); expired == m.held {
		t.Fatalf("step %d: %s: Expired = %v with the lease %s (%s)",
			step, what, expired, heldOrNot(m.held), m)
	}
}

func (m *model) String() string {
	if !m.exists {
		return "model: no record"
	}
	return fmt.Sprintf("model: %s holds fence %d, %s", m.holder, m.fence, heldOrNot(m.held))
}

func heldOrNot(held bool) string {
	if held {
		return "live"
	}
	return "released"
}
