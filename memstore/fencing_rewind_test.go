package memstore

import (
	"context"
	"testing"
	"time"
)

// Fencing uniqueness rests on a precondition the store cannot enforce: the
// database must never hand back an older state than the one that granted the
// most recent term. Promoting a lagging replica, or restoring an old backup,
// rewinds the row the fence counter lives in. This test pins down the
// consequence so the requirement stays visible: after a rewind the store
// reissues a token a past term already used, and a receiver guarding writes
// with "fence <= seen" would accept a stale write. Real deployments satisfy
// the precondition with synchronous replication / durable failover; nothing
// in this package can substitute for it.
func TestRestoreFromOldSnapshotReissuesFence(t *testing.T) {
	ctx := context.Background()
	s := New()

	// A takes the lease: fence 1.
	a, err := s.Acquire(ctx, "e", "A", time.Minute, nil)
	if err != nil {
		t.Fatal(err)
	}

	// A backup of the database is taken while A holds the lease.
	s.mu.Lock()
	backup := map[string]*record{
		"e": {
			holder:     s.records["e"].holder,
			fence:      s.records["e"].fence,
			acquiredAt: s.records["e"].acquiredAt,
			renewedAt:  s.records["e"].renewedAt,
			expiresAt:  s.records["e"].expiresAt,
			payload:    append([]byte(nil), s.records["e"].payload...),
		},
	}
	s.mu.Unlock()

	// A steps down, B leads: the counter climbs to 2.
	if err := s.Release(ctx, "e", "A", a.Fence); err != nil {
		t.Fatal(err)
	}
	b, err := s.Acquire(ctx, "e", "B", time.Minute, nil)
	if err != nil {
		t.Fatal(err)
	}

	// The primary dies and is restored from the old backup: the counter is
	// back at 1.
	s.mu.Lock()
	s.records = backup
	s.mu.Unlock()

	c, err := s.Acquire(ctx, "e", "C", time.Minute, nil)
	if err != nil {
		t.Fatal(err)
	}
	if c.Fence > b.Fence {
		t.Fatalf("store became resilient to snapshot rewind (C got fence %d after B's %d); update README's durability section and this test",
			c.Fence, b.Fence)
	}
	// The expected, documented behaviour: the rewound counter reissues a token
	// a past term already used. A receiver guarding writes with
	// "fence <= seen" would accept a stale write from a rewound term.
	t.Logf("documented limitation confirmed: restored snapshot reissued fence %d, which term B (fence %d) already used", c.Fence, b.Fence)
}
