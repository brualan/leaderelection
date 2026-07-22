package harness

import (
	"context"
	"time"

	"github.com/brualan/leaderelection"
)

// SkewStore shifts every timestamp a store reports by a constant offset.
//
// It exists to test a claim the README makes and nothing else checks
// end-to-end: that a participant whose view of the database's clock is days
// out from everyone else's leads and follows exactly like the rest.
//
// The shift is uniform, which is the point. Every timestamp in a Lease --
// including the Now it is compared against -- moves together, so every
// *duration* is unchanged. A participant that only ever reasons about
// durations cannot tell the difference; one that compares a database timestamp
// against its own clock, or against a timestamp from a different read, breaks
// immediately and visibly.
type SkewStore struct {
	Base   leaderelection.Store
	Offset time.Duration
}

// Skew returns a decorator for [Options.NodeStore] that gives each participant
// its own view of the database clock, cycling through the offsets given.
//
// Handing every participant a different offset is what makes a scenario using
// this discriminating: whichever of them ends up leading, it is leading with a
// clock that agrees with nobody.
func Skew(offsets ...time.Duration) func(leaderelection.Store, int) leaderelection.Store {
	return func(base leaderelection.Store, index int) leaderelection.Store {
		if len(offsets) == 0 {
			return base
		}
		return &SkewStore{Base: base, Offset: offsets[index%len(offsets)]}
	}
}

// Acquire implements [leaderelection.Store].
func (s *SkewStore) Acquire(ctx context.Context, name, identity string, ttl time.Duration, payload []byte) (leaderelection.Lease, error) {
	lease, err := s.Base.Acquire(ctx, name, identity, ttl, payload)
	return s.shift(lease), err
}

// Renew implements [leaderelection.Store].
func (s *SkewStore) Renew(ctx context.Context, name, identity string, fence int64, ttl time.Duration, payload []byte) (leaderelection.Lease, error) {
	lease, err := s.Base.Renew(ctx, name, identity, fence, ttl, payload)
	return s.shift(lease), err
}

// Release implements [leaderelection.Store].
func (s *SkewStore) Release(ctx context.Context, name, identity string, fence int64) error {
	return s.Base.Release(ctx, name, identity, fence)
}

// Inspect implements [leaderelection.Store].
func (s *SkewStore) Inspect(ctx context.Context, name string) (leaderelection.Lease, error) {
	lease, err := s.Base.Inspect(ctx, name)
	return s.shift(lease), err
}

func (s *SkewStore) shift(lease leaderelection.Lease) leaderelection.Lease {
	lease.AcquiredAt = shift(lease.AcquiredAt, s.Offset)
	lease.RenewedAt = shift(lease.RenewedAt, s.Offset)
	lease.ExpiresAt = shift(lease.ExpiresAt, s.Offset)
	lease.Now = shift(lease.Now, s.Offset)
	return lease
}

// shift leaves a zero timestamp alone: a failed call reports one, and moving it
// would turn "no value" into a date in 1970 plus three days.
func shift(t time.Time, offset time.Duration) time.Time {
	if t.IsZero() {
		return t
	}
	return t.Add(offset)
}
