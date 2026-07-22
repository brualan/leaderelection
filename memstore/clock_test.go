package memstore_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/brualan/leaderelection"
	"github.com/brualan/leaderelection/memstore"
	"github.com/brualan/leaderelection/storetest"
)

// virtualClock stands in for the database's clock so that expiry can be tested
// without sleeping.
type virtualClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *virtualClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *virtualClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func TestWithNowDrivesExpiry(t *testing.T) {
	clk := &virtualClock{now: time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)}
	store := memstore.New(memstore.WithNow(clk.Now))
	ctx := context.Background()

	first, err := store.Acquire(ctx, "election", "a", time.Minute, nil)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	if !first.HeldBy("a") {
		t.Fatal("a did not acquire a free lease")
	}

	// Every timestamp the store reports comes from the injected clock, which
	// is what the real stores get from the database server.
	if !first.Now.Equal(clk.Now()) {
		t.Fatalf("Lease.Now = %v, want the store's clock %v", first.Now, clk.Now())
	}

	clk.advance(30 * time.Second)
	if lease, _ := store.Acquire(ctx, "election", "b", time.Minute, nil); lease.HeldBy("b") {
		t.Fatal("b took a lease that is still live")
	}

	clk.advance(31 * time.Second)
	second, err := store.Acquire(ctx, "election", "b", time.Minute, nil)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	if !second.HeldBy("b") {
		t.Fatal("b did not take over an expired lease")
	}
	if second.Fence <= first.Fence {
		t.Fatalf("fence %d did not advance past %d", second.Fence, first.Fence)
	}
}

// The store must satisfy the interface the elector consumes, not merely look
// like it.
func TestStoreSatisfiesTheInterface(t *testing.T) {
	var _ leaderelection.Store = memstore.New()
}

// Running the suite a second time against a store with an injected clock
// covers the same semantics on a different time source.
func TestAcceptanceWithInjectedClock(t *testing.T) {
	// A clock that runs faster than real time keeps the expiry cases quick
	// while leaving the ordering of events unchanged.
	start := time.Now()
	store := memstore.New(memstore.WithNow(func() time.Time {
		return start.Add(4 * time.Since(start))
	}))
	storetest.Run(t, storetest.Suite{Store: store, ExpiryTTL: 400 * time.Millisecond})
}
