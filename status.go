package leaderelection

import (
	"bytes"
	"time"
)

// Status is a snapshot of what an [Elector] currently believes.
//
// Everything in a Status except IsLeader is advisory and may already be stale
// when it is read. IsLeader is the only field with a safety guarantee behind
// it, and it is evaluated at the moment [Elector.Status] is called.
type Status struct {
	// IsLeader reports whether this participant may act as leader right now.
	IsLeader bool

	// Fence is the token for the current term, valid only when IsLeader.
	Fence int64

	// Identity is this participant's identity.
	Identity string

	// Leader is the identity last observed holding the lease. It is empty
	// before the first successful read, and may name a participant whose
	// lease has since expired.
	Leader string

	// LeaderFence is the fence token observed alongside Leader.
	LeaderFence int64

	// LeaderPayload is the payload published by the observed leader.
	LeaderPayload []byte

	// ObservedAt is the local time of the last successful database read.
	ObservedAt time.Time

	// ValidUntil is the local instant at which this participant must stop
	// acting as leader unless it renews first. It is zero when not leading.
	//
	// It is derived from the database's own view of the lease, translated into
	// local monotonic time, and is deliberately earlier than the database's
	// expiry.
	ValidUntil time.Time

	// LastError is the most recent failure, cleared by the next success. A
	// non-nil LastError while IsLeader is true means renewal is struggling
	// and leadership is at risk.
	LastError error
}

// Status returns a snapshot of the elector's current state.
func (e *Elector) Status() Status {
	e.mu.Lock()
	defer e.mu.Unlock()

	// The clock is read under the lock so IsLeader reflects the instant the
	// snapshot was taken, not an earlier one.
	now := e.clock.Now()

	s := Status{
		Identity: e.cfg.Identity,
		Leader:   e.observedLeader,
		// Copied, not shared. A Status is a snapshot the caller owns; handing
		// out the elector's own slice would let one caller's edit surface in
		// another's snapshot.
		LeaderPayload: bytes.Clone(e.observedPayload),
		LeaderFence:   e.observedFence,
		ObservedAt:    e.observedAt,
		LastError:     e.lastErr,
	}
	if t := e.term; t != nil && e.leadingAtLocked(now) {
		s.IsLeader = true
		s.Fence = t.fence
		s.ValidUntil = t.deadline.Add(-e.cfg.SafetyMargin)
	}
	return s
}
