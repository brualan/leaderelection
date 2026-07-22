// Package storetest is the acceptance suite every [leaderelection.Store]
// implementation must pass.
//
// The election's safety argument assumes a precise set of behaviours from the
// store: that a takeover is atomic, that a fence token never repeats within an
// election, that a guarded renewal fails rather than silently succeeding
// against someone else's lease. Those behaviours are easy to get subtly wrong
// in SQL, and differently wrong in each dialect. Running one suite against
// every implementation -- including the in-memory reference -- is what keeps
// the guarantee from being dialect-specific.
//
// A conformant store must pass this suite unchanged when reached directly and
// when reached through a connection pooler.
package storetest

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/brualan/leaderelection"
)

// Suite configures a run of the acceptance tests.
type Suite struct {
	// Store is the implementation under test. Subtests isolate themselves by
	// using distinct election names, so one Store serves the whole suite.
	Store leaderelection.Store

	// ExpiryTTL is the lease duration used by tests that must watch a lease
	// expire in real time. It has to comfortably exceed a round trip to the
	// database: too short and a slow reply expires the lease mid-test.
	// Defaults to DefaultExpiryTTL.
	ExpiryTTL time.Duration

	// Concurrency is how many goroutines contend in the race tests. Defaults
	// to DefaultConcurrency.
	Concurrency int

	// Now, if set, reports the database's current time. Tests use it only to
	// report diagnostics; the suite never compares it against local time.
	Now func(ctx context.Context) (time.Time, error)
}

// Defaults for [Suite].
const (
	DefaultExpiryTTL   = 750 * time.Millisecond
	DefaultConcurrency = 16

	// longTTL is used by every test that does not want expiry to interfere.
	longTTL = time.Minute
)

// Run executes the acceptance suite against s.Store.
func Run(t *testing.T, s Suite) {
	t.Helper()
	if s.Store == nil {
		t.Fatal("storetest: Suite.Store is required")
	}
	if s.ExpiryTTL == 0 {
		s.ExpiryTTL = DefaultExpiryTTL
	}
	if s.Concurrency == 0 {
		s.Concurrency = DefaultConcurrency
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tc.fn(t, s)
		})
	}
}

type testCase struct {
	name string
	fn   func(*testing.T, Suite)
}

var cases = []testCase{
	{"InspectUnknown", testInspectUnknown},
	{"AcquireCreates", testAcquireCreates},
	{"AcquireIsIdempotentForHolder", testAcquireIsIdempotentForHolder},
	{"AcquireDeniedWhileHeld", testAcquireDeniedWhileHeld},
	{"AcquireAfterExpiryByOther", testAcquireAfterExpiryByOther},
	{"AcquireAfterExpiryBySameHolder", testAcquireAfterExpiryBySameHolder},
	{"RenewExtends", testRenewExtends},
	{"RenewRejectsWrongIdentity", testRenewRejectsWrongIdentity},
	{"RenewRejectsWrongFence", testRenewRejectsWrongFence},
	{"RenewRejectsExpired", testRenewRejectsExpired},
	{"RenewRejectsUnknown", testRenewRejectsUnknown},
	{"ReleaseFreesImmediately", testReleaseFreesImmediately},
	{"ReleaseIgnoresWrongGuard", testReleaseIgnoresWrongGuard},
	{"ReleaseIsIdempotent", testReleaseIsIdempotent},
	{"ReleaseOfAnExpiredLeaseIsANoOp", testReleaseOfAnExpiredLeaseIsANoOp},
	{"PayloadRoundTrips", testPayloadRoundTrips},
	{"PayloadVisibleToFollowers", testPayloadVisibleToFollowers},
	{"TimestampsAreDatabaseSide", testTimestampsAreDatabaseSide},
	{"AcquiredAtStableAcrossRenewals", testAcquiredAtStableAcrossRenewals},
	{"ElectionsAreIndependent", testElectionsAreIndependent},
	{"IdentityIsOpaque", testIdentityIsOpaque},
	{"ContextCancellationIsReported", testContextCancellationIsReported},
	{"ConcurrentAcquireElectsOne", testConcurrentAcquireElectsOne},
	{"ConcurrentTakeoverElectsOne", testConcurrentTakeoverElectsOne},
	{"FenceIncreasesAcrossTerms", testFenceIncreasesAcrossTerms},
}

func testInspectUnknown(t *testing.T, s Suite) {
	_, err := s.Store.Inspect(ctx(t), name(t))
	if !errors.Is(err, leaderelection.ErrNoLease) {
		t.Fatalf("Inspect on unknown election: got %v, want ErrNoLease", err)
	}
}

func testAcquireCreates(t *testing.T, s Suite) {
	n := name(t)
	lease := mustAcquire(t, s, n, "a", longTTL, nil)

	if !lease.HeldBy("a") {
		t.Fatalf("HeldBy(a) = false, want true (holder=%q expires_at=%v now=%v)",
			lease.Holder, lease.ExpiresAt, lease.Now)
	}
	if lease.Fence <= 0 {
		t.Fatalf("Fence = %d, want positive", lease.Fence)
	}
	if lease.Name != n {
		t.Fatalf("Name = %q, want %q", lease.Name, n)
	}
	assertTTLNear(t, lease, longTTL)

	// Inspect must agree with what Acquire reported.
	got := mustInspect(t, s, n)
	if got.Holder != "a" || got.Fence != lease.Fence {
		t.Fatalf("Inspect = (%q, %d), want (a, %d)", got.Holder, got.Fence, lease.Fence)
	}
}

func testAcquireIsIdempotentForHolder(t *testing.T, s Suite) {
	n := name(t)
	first := mustAcquire(t, s, n, "a", longTTL, nil)
	second := mustAcquire(t, s, n, "a", longTTL, nil)

	if second.Fence != first.Fence {
		t.Fatalf("Fence changed on re-acquire by the same holder: %d -> %d; "+
			"an uninterrupted term must keep its token", first.Fence, second.Fence)
	}
	if !second.HeldBy("a") {
		t.Fatal("re-acquire by the holder did not keep the lease")
	}
}

func testAcquireDeniedWhileHeld(t *testing.T, s Suite) {
	n := name(t)
	held := mustAcquire(t, s, n, "a", longTTL, nil)

	lease := mustAcquire(t, s, n, "b", longTTL, nil)
	if lease.HeldBy("b") {
		t.Fatal("b acquired a lease that a already holds")
	}
	// The loser still learns who won, which is what lets a follower report the
	// leader without a second round trip.
	if lease.Holder != "a" {
		t.Fatalf("Holder = %q, want a", lease.Holder)
	}
	if lease.Fence != held.Fence {
		t.Fatalf("Fence = %d, want %d: a failed acquire must not disturb the term",
			lease.Fence, held.Fence)
	}
}

func testAcquireAfterExpiryByOther(t *testing.T, s Suite) {
	n := name(t)
	first := mustAcquire(t, s, n, "a", s.ExpiryTTL, nil)
	waitForExpiry(t, s, n)

	lease := mustAcquire(t, s, n, "b", longTTL, nil)
	if !lease.HeldBy("b") {
		t.Fatal("b failed to take over an expired lease")
	}
	if lease.Fence <= first.Fence {
		t.Fatalf("Fence = %d, want > %d: a takeover starts a new term",
			lease.Fence, first.Fence)
	}
}

func testAcquireAfterExpiryBySameHolder(t *testing.T, s Suite) {
	n := name(t)
	first := mustAcquire(t, s, n, "a", s.ExpiryTTL, nil)
	waitForExpiry(t, s, n)

	lease := mustAcquire(t, s, n, "a", longTTL, nil)
	if !lease.HeldBy("a") {
		t.Fatal("a failed to reclaim its own expired lease")
	}
	// This is the case an implementation is most likely to get wrong. While
	// the lease was expired, any other participant could have held it and
	// written to a downstream system. The reclaiming holder must therefore
	// present a new token, even though its identity never changed.
	if lease.Fence <= first.Fence {
		t.Fatalf("Fence = %d, want > %d: reclaiming an expired lease starts a new term "+
			"even for the previous holder", lease.Fence, first.Fence)
	}
}

func testRenewExtends(t *testing.T, s Suite) {
	n := name(t)
	held := mustAcquire(t, s, n, "a", longTTL, nil)

	lease, err := s.Store.Renew(ctx(t), n, "a", held.Fence, longTTL, nil)
	if err != nil {
		t.Fatalf("Renew: %v", err)
	}
	if lease.Fence != held.Fence {
		t.Fatalf("Fence = %d, want %d: renewal continues a term, it does not start one",
			lease.Fence, held.Fence)
	}
	if !lease.HeldBy("a") {
		t.Fatal("renewed lease is not held by a")
	}
	assertTTLNear(t, lease, longTTL)
	if !lease.RenewedAt.After(held.RenewedAt) && !lease.ExpiresAt.After(held.ExpiresAt) {
		t.Fatalf("renewal did not move the lease forward: renewed_at %v -> %v, expires_at %v -> %v",
			held.RenewedAt, lease.RenewedAt, held.ExpiresAt, lease.ExpiresAt)
	}
}

func testRenewRejectsWrongIdentity(t *testing.T, s Suite) {
	n := name(t)
	held := mustAcquire(t, s, n, "a", longTTL, nil)

	_, err := s.Store.Renew(ctx(t), n, "b", held.Fence, longTTL, nil)
	if !errors.Is(err, leaderelection.ErrLeaseLost) {
		t.Fatalf("Renew by a non-holder: got %v, want ErrLeaseLost", err)
	}
	assertUndisturbed(t, s, n, "a", held.Fence)
}

func testRenewRejectsWrongFence(t *testing.T, s Suite) {
	n := name(t)
	held := mustAcquire(t, s, n, "a", longTTL, nil)

	// A stale fence is what a leader that was partitioned, stepped down, and
	// came back would present.
	_, err := s.Store.Renew(ctx(t), n, "a", held.Fence-1, longTTL, nil)
	if !errors.Is(err, leaderelection.ErrLeaseLost) {
		t.Fatalf("Renew with a stale fence: got %v, want ErrLeaseLost", err)
	}
	assertUndisturbed(t, s, n, "a", held.Fence)
}

func testRenewRejectsExpired(t *testing.T, s Suite) {
	n := name(t)
	held := mustAcquire(t, s, n, "a", s.ExpiryTTL, nil)
	waitForExpiry(t, s, n)

	// Nobody has taken the lease, but it is expired, so another participant
	// may take it at any instant. Resurrecting it would let two leaders exist.
	_, err := s.Store.Renew(ctx(t), n, "a", held.Fence, longTTL, nil)
	if !errors.Is(err, leaderelection.ErrLeaseLost) {
		t.Fatalf("Renew of an expired lease: got %v, want ErrLeaseLost", err)
	}
}

func testRenewRejectsUnknown(t *testing.T, s Suite) {
	_, err := s.Store.Renew(ctx(t), name(t), "a", 1, longTTL, nil)
	if !errors.Is(err, leaderelection.ErrLeaseLost) {
		t.Fatalf("Renew of an unknown election: got %v, want ErrLeaseLost", err)
	}
}

func testReleaseFreesImmediately(t *testing.T, s Suite) {
	n := name(t)
	held := mustAcquire(t, s, n, "a", longTTL, nil)

	if err := s.Store.Release(ctx(t), n, "a", held.Fence); err != nil {
		t.Fatalf("Release: %v", err)
	}

	lease := mustAcquire(t, s, n, "b", longTTL, nil)
	if !lease.HeldBy("b") {
		t.Fatal("b could not take a released lease; release must not wait out the TTL")
	}
	if lease.Fence <= held.Fence {
		t.Fatalf("Fence = %d, want > %d", lease.Fence, held.Fence)
	}
}

func testReleaseIgnoresWrongGuard(t *testing.T, s Suite) {
	n := name(t)
	held := mustAcquire(t, s, n, "a", longTTL, nil)

	// Neither of these owns the lease, so neither may end the term -- and
	// neither is an error, because "this caller is not the leader" is already
	// true.
	if err := s.Store.Release(ctx(t), n, "b", held.Fence); err != nil {
		t.Fatalf("Release by a non-holder: %v", err)
	}
	if err := s.Store.Release(ctx(t), n, "a", held.Fence-1); err != nil {
		t.Fatalf("Release with a stale fence: %v", err)
	}
	assertUndisturbed(t, s, n, "a", held.Fence)
}

func testReleaseIsIdempotent(t *testing.T, s Suite) {
	n := name(t)
	if err := s.Store.Release(ctx(t), n, "a", 1); err != nil {
		t.Fatalf("Release of an unknown election: %v", err)
	}

	held := mustAcquire(t, s, n, "a", longTTL, nil)
	for i := range 3 {
		if err := s.Store.Release(ctx(t), n, "a", held.Fence); err != nil {
			t.Fatalf("Release #%d: %v", i+1, err)
		}
	}
}

func testReleaseOfAnExpiredLeaseIsANoOp(t *testing.T, s Suite) {
	n := name(t)
	held := mustAcquire(t, s, n, "a", s.ExpiryTTL, nil)
	waitForExpiry(t, s, n)

	before := mustInspect(t, s, n)
	if err := s.Store.Release(ctx(t), n, "a", held.Fence); err != nil {
		t.Fatalf("Release of an expired lease: %v", err)
	}

	// Nothing to give up, so nothing should have been written. A store that
	// rewrote the row here would burn a row version per participant per
	// shutdown for no reason at all.
	after := mustInspect(t, s, n)
	if !after.RenewedAt.Equal(before.RenewedAt) || !after.ExpiresAt.Equal(before.ExpiresAt) {
		t.Fatalf("releasing an expired lease rewrote the record: renewed_at %v -> %v, expires_at %v -> %v",
			before.RenewedAt, after.RenewedAt, before.ExpiresAt, after.ExpiresAt)
	}
	if after.Fence != before.Fence {
		t.Fatalf("releasing an expired lease moved the fence: %d -> %d", before.Fence, after.Fence)
	}
}

func testPayloadRoundTrips(t *testing.T, s Suite) {
	n := name(t)
	want := []byte("https://leader.cluster-a.example:8443")
	held := mustAcquire(t, s, n, "a", longTTL, want)
	if string(held.Payload) != string(want) {
		t.Fatalf("Acquire payload = %q, want %q", held.Payload, want)
	}

	next := []byte("https://leader.cluster-a.example:9443")
	renewed, err := s.Store.Renew(ctx(t), n, "a", held.Fence, longTTL, next)
	if err != nil {
		t.Fatalf("Renew: %v", err)
	}
	if string(renewed.Payload) != string(next) {
		t.Fatalf("Renew payload = %q, want %q", renewed.Payload, next)
	}

	// An empty payload must survive the round trip as an empty payload rather
	// than being confused with a missing one.
	cleared, err := s.Store.Renew(ctx(t), n, "a", held.Fence, longTTL, nil)
	if err != nil {
		t.Fatalf("Renew clearing payload: %v", err)
	}
	if len(cleared.Payload) != 0 {
		t.Fatalf("Renew payload = %q, want empty", cleared.Payload)
	}
}

func testPayloadVisibleToFollowers(t *testing.T, s Suite) {
	n := name(t)
	want := []byte("leader-endpoint")
	mustAcquire(t, s, n, "a", longTTL, want)

	// A follower discovers where the leader is without any channel other than
	// the election itself. This is the cross-cluster discovery path.
	viaInspect := mustInspect(t, s, n)
	if string(viaInspect.Payload) != string(want) {
		t.Fatalf("Inspect payload = %q, want %q", viaInspect.Payload, want)
	}
	viaLostAcquire := mustAcquire(t, s, n, "b", longTTL, []byte("b-endpoint"))
	if string(viaLostAcquire.Payload) != string(want) {
		t.Fatalf("payload after a lost acquire = %q, want the holder's %q",
			viaLostAcquire.Payload, want)
	}
}

func testTimestampsAreDatabaseSide(t *testing.T, s Suite) {
	n := name(t)
	lease := mustAcquire(t, s, n, "a", longTTL, nil)

	// Every timestamp must come from one clock read on the server, so they
	// have to be mutually consistent regardless of what the client's clock
	// says.
	if lease.Now.IsZero() || lease.ExpiresAt.IsZero() || lease.AcquiredAt.IsZero() || lease.RenewedAt.IsZero() {
		t.Fatalf("zero timestamp in %+v: all timestamps must be database-generated", lease)
	}
	if !lease.ExpiresAt.After(lease.Now) {
		t.Fatalf("expires_at %v is not after now %v", lease.ExpiresAt, lease.Now)
	}
	if lease.AcquiredAt.After(lease.Now) || lease.RenewedAt.After(lease.Now) {
		t.Fatalf("acquired_at %v / renewed_at %v is in the future relative to now %v",
			lease.AcquiredAt, lease.RenewedAt, lease.Now)
	}
	if lease.Expired() {
		t.Fatal("a lease just acquired reports as expired")
	}
}

func testAcquiredAtStableAcrossRenewals(t *testing.T, s Suite) {
	n := name(t)
	held := mustAcquire(t, s, n, "a", longTTL, nil)

	time.Sleep(20 * time.Millisecond)
	renewed, err := s.Store.Renew(ctx(t), n, "a", held.Fence, longTTL, nil)
	if err != nil {
		t.Fatalf("Renew: %v", err)
	}
	// acquired_at marks when the term began, so it must not move when the
	// lease is merely extended; operators use it to see how long a leader has
	// been leading.
	if !renewed.AcquiredAt.Equal(held.AcquiredAt) {
		t.Fatalf("acquired_at moved on renewal: %v -> %v", held.AcquiredAt, renewed.AcquiredAt)
	}
}

func testElectionsAreIndependent(t *testing.T, s Suite) {
	first, second := name(t), name(t)

	a := mustAcquire(t, s, first, "a", longTTL, nil)
	b := mustAcquire(t, s, second, "b", longTTL, nil)
	if !a.HeldBy("a") || !b.HeldBy("b") {
		t.Fatal("holding one election blocked another; elections must not share state")
	}

	if err := s.Store.Release(ctx(t), first, "a", a.Fence); err != nil {
		t.Fatalf("Release: %v", err)
	}
	if got := mustInspect(t, s, second); !got.HeldBy("b") {
		t.Fatal("releasing one election disturbed another")
	}
}

func testIdentityIsOpaque(t *testing.T, s Suite) {
	// Identities are generated from hostnames, which can contain anything a
	// DNS label or a container runtime allows. A store must treat them as
	// data -- never as SQL, and never as a pattern.
	for _, identity := range []string{
		"host-1234-0011223344556677",
		"o'brien's-pod",
		`quote"backslash\percent%underscore_`,
		"юникод-под",
		strings.Repeat("x", 200),
	} {
		n := name(t)
		lease := mustAcquire(t, s, n, identity, longTTL, nil)
		if !lease.HeldBy(identity) {
			t.Fatalf("identity %q: HeldBy = false, holder = %q", identity, lease.Holder)
		}
		// A wildcard-looking identity must not match a different one.
		other := mustAcquire(t, s, n, "someone-else", longTTL, nil)
		if other.HeldBy("someone-else") {
			t.Fatalf("identity %q was matched loosely by %q", identity, "someone-else")
		}
	}
}

func testContextCancellationIsReported(t *testing.T, s Suite) {
	n := name(t)
	held := mustAcquire(t, s, n, "a", longTTL, nil)

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()

	// A cancelled call must report failure rather than a made-up success. The
	// election treats "unknown" as "not leader", so a store that swallowed the
	// cancellation and returned a zero Lease would look like a lost election
	// instead of an outage -- survivable, but it must never look like a win.
	if lease, err := s.Store.Acquire(cancelled, n, "a", longTTL, nil); err == nil && lease.HeldBy("a") {
		t.Fatal("Acquire on a cancelled context reported a held lease")
	}
	if _, err := s.Store.Inspect(cancelled, n); err == nil {
		t.Fatal("Inspect on a cancelled context returned success")
	}
	if _, err := s.Store.Renew(cancelled, n, "a", held.Fence, longTTL, nil); err == nil {
		t.Fatal("Renew on a cancelled context returned success")
	}
	if err := s.Store.Release(cancelled, n, "a", held.Fence); err == nil {
		t.Fatal("Release on a cancelled context returned success")
	}
}

func testConcurrentAcquireElectsOne(t *testing.T, s Suite) {
	n := name(t)
	winners, fences := race(t, s, n, s.Concurrency, longTTL)

	if winners != 1 {
		t.Fatalf("%d participants won a single election; exactly one may win", winners)
	}
	if len(fences) != 1 {
		t.Fatalf("observed fence tokens %v; a single term must have a single token", fences)
	}
}

func testConcurrentTakeoverElectsOne(t *testing.T, s Suite) {
	n := name(t)
	first := mustAcquire(t, s, n, "incumbent", s.ExpiryTTL, nil)
	waitForExpiry(t, s, n)

	// The moment a lease expires is the moment every participant is entitled
	// to grab it, so this is when a non-atomic takeover shows up as two
	// leaders.
	winners, fences := race(t, s, n, s.Concurrency, longTTL)
	if winners != 1 {
		t.Fatalf("%d participants took over an expired lease; exactly one may", winners)
	}
	for f := range fences {
		if f <= first.Fence {
			t.Fatalf("winning fence %d did not advance past %d", f, first.Fence)
		}
	}
}

func testFenceIncreasesAcrossTerms(t *testing.T, s Suite) {
	n := name(t)
	var previous int64
	for i := range 5 {
		identity := fmt.Sprintf("holder-%d", i)
		lease := mustAcquire(t, s, n, identity, longTTL, nil)
		if !lease.HeldBy(identity) {
			t.Fatalf("term %d: %s failed to acquire", i, identity)
		}
		if lease.Fence <= previous {
			t.Fatalf("term %d: fence %d did not advance past %d", i, lease.Fence, previous)
		}
		previous = lease.Fence

		if err := s.Store.Release(ctx(t), n, identity, lease.Fence); err != nil {
			t.Fatalf("term %d: Release: %v", i, err)
		}
	}
}

// race has n participants call Acquire simultaneously and reports how many
// ended up holding the lease, along with the distinct fence tokens the winners
// saw.
func race(t *testing.T, s Suite, election string, n int, ttl time.Duration) (int, map[int64]struct{}) {
	t.Helper()

	var (
		start   = make(chan struct{})
		wg      sync.WaitGroup
		winners atomic.Int64
		mu      sync.Mutex
		fences  = make(map[int64]struct{})
		errs    = make(chan error, n)
	)

	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			identity := fmt.Sprintf("racer-%d", i)
			<-start
			lease, err := s.Store.Acquire(ctx(t), election, identity, ttl, nil)
			if err != nil {
				errs <- err
				return
			}
			if lease.HeldBy(identity) {
				winners.Add(1)
				mu.Lock()
				fences[lease.Fence] = struct{}{}
				mu.Unlock()
			}
		}()
	}

	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("Acquire during race: %v", err)
	}
	return int(winners.Load()), fences
}

func mustAcquire(t *testing.T, s Suite, election, identity string, ttl time.Duration, payload []byte) leaderelection.Lease {
	t.Helper()
	lease, err := s.Store.Acquire(ctx(t), election, identity, ttl, payload)
	if err != nil {
		t.Fatalf("Acquire(%q, %q): %v", election, identity, err)
	}
	return lease
}

func mustInspect(t *testing.T, s Suite, election string) leaderelection.Lease {
	t.Helper()
	lease, err := s.Store.Inspect(ctx(t), election)
	if err != nil {
		t.Fatalf("Inspect(%q): %v", election, err)
	}
	return lease
}

// assertUndisturbed checks that a rejected operation left the term intact.
func assertUndisturbed(t *testing.T, s Suite, election, holder string, fence int64) {
	t.Helper()
	lease := mustInspect(t, s, election)
	if lease.Holder != holder || lease.Fence != fence {
		t.Fatalf("term was disturbed by a rejected operation: got (%q, %d), want (%q, %d)",
			lease.Holder, lease.Fence, holder, fence)
	}
	if lease.Expired() {
		t.Fatal("term was expired by a rejected operation")
	}
}

// assertTTLNear checks the reported TTL against what was requested.
//
// The tolerance is deliberately tight. Both endpoints come from one clock read
// on the server, so the only slack needed is the time the statement itself
// took -- and a store that returned, say, half the requested TTL would leave
// the elector holding a local deadline further out than the lease it was
// actually granted.
func assertTTLNear(t *testing.T, lease leaderelection.Lease, want time.Duration) {
	t.Helper()
	got := lease.TTL()
	if got > want {
		t.Fatalf("TTL = %v, want at most the requested %v", got, want)
	}
	if slack := max(want/20, 100*time.Millisecond); got < want-slack {
		t.Fatalf("TTL = %v, want within %v of the requested %v", got, slack, want)
	}
}

// waitForExpiry blocks until the store reports the lease as expired, so that
// tests do not depend on the test process's clock agreeing with the database's.
func waitForExpiry(t *testing.T, s Suite, election string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		lease, err := s.Store.Inspect(ctx(t), election)
		if err != nil {
			t.Fatalf("Inspect while waiting for expiry: %v", err)
		}
		if lease.Expired() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("lease %q did not expire within 10s (ttl=%v, remaining=%v)",
				election, s.ExpiryTTL, lease.TTL())
		}
		// Sleep for what the database says is left, plus a nudge, rather than
		// polling blindly.
		time.Sleep(lease.TTL() + 10*time.Millisecond)
	}
}

// ctx returns a context bounded by the test's own deadline.
func ctx(t *testing.T) context.Context {
	t.Helper()
	c, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	return c
}

// name returns an election name unique to this call, so subtests and repeated
// runs against a persistent database never collide.
func name(t *testing.T) string {
	// Include the test's name so that a row left behind in a real database
	// points back at what created it.
	label := strings.Map(func(r rune) rune {
		if r == '/' || r == ' ' {
			return '-'
		}
		return r
	}, t.Name())
	if len(label) > 40 {
		label = label[len(label)-40:]
	}
	return fmt.Sprintf("st-%s-%d-%d", label, time.Now().UnixNano(), nameCounter.Add(1))
}

var nameCounter atomic.Int64
