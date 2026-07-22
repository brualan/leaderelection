// Package memstore is an in-process implementation of
// [leaderelection.Store].
//
// It is the reference the acceptance suite validates itself against, and it is
// useful in an application's own tests: an election backed by memstore
// exercises the real [leaderelection.Elector] -- promotion, renewal, watchdog,
// fencing -- without a database.
//
// It elects a leader among goroutines in one process only. Processes do not
// share it, so it is not a substitute for the postgres or mysql stores.
package memstore

import (
	"context"
	"sync"
	"time"

	"github.com/brualan/leaderelection"
)

// Store keeps leases in memory. The zero value is not usable; call [New].
//
// Being in-process, Store is its own clock authority, which is precisely the
// role the database plays for the real stores: every timestamp it reports,
// including Lease.Now, comes from the same source.
type Store struct {
	now func() time.Time

	mu      sync.Mutex
	records map[string]*record
}

type record struct {
	holder     string
	fence      int64
	acquiredAt time.Time
	renewedAt  time.Time
	expiresAt  time.Time
	payload    []byte
}

// Option configures a [Store].
type Option func(*Store)

// WithNow overrides the clock. Tests use it to make expiry deterministic
// instead of sleeping.
func WithNow(now func() time.Time) Option {
	return func(s *Store) { s.now = now }
}

// New returns an empty Store.
func New(opts ...Option) *Store {
	s := &Store{now: time.Now, records: make(map[string]*record)}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// Acquire implements [leaderelection.Store].
func (s *Store) Acquire(ctx context.Context, name, identity string, ttl time.Duration, payload []byte) (leaderelection.Lease, error) {
	if err := ctx.Err(); err != nil {
		return leaderelection.Lease{}, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.now()
	r, ok := s.records[name]
	if !ok {
		r = &record{holder: identity, fence: 1, acquiredAt: now}
		s.records[name] = r
		r.renew(now, ttl, payload)
		return r.lease(name, now), nil
	}

	expired := !r.expiresAt.After(now)
	// Taking over is allowed when the lease is free, or when it is already
	// ours and we are simply extending it.
	if expired || r.holder == identity {
		// A term is new whenever the holder changes, and also when the same
		// holder reclaims a lease it had let expire: in between, anyone could
		// have held it, so downstream systems must see a fresh token.
		if expired || r.holder != identity {
			r.fence++
			r.acquiredAt = now
		}
		r.holder = identity
		r.renew(now, ttl, payload)
	}
	return r.lease(name, now), nil
}

// Renew implements [leaderelection.Store].
func (s *Store) Renew(ctx context.Context, name, identity string, fence int64, ttl time.Duration, payload []byte) (leaderelection.Lease, error) {
	if err := ctx.Err(); err != nil {
		return leaderelection.Lease{}, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.now()
	r, ok := s.records[name]
	if !ok || r.holder != identity || r.fence != fence || !r.expiresAt.After(now) {
		return leaderelection.Lease{}, leaderelection.ErrLeaseLost
	}
	r.renew(now, ttl, payload)
	return r.lease(name, now), nil
}

// Release implements [leaderelection.Store].
func (s *Store) Release(ctx context.Context, name, identity string, fence int64) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.now()
	r, ok := s.records[name]
	// Not ours, or already expired: the state the caller wanted already holds,
	// and touching the record would only rewrite it for nothing. The SQL
	// stores carry the same guard, and the acceptance suite holds every
	// implementation to it.
	if !ok || r.holder != identity || r.fence != fence || !r.expiresAt.After(now) {
		return nil
	}
	// Expire in place rather than deleting: the fence must keep climbing
	// across the handover, and the record is what remembers it.
	r.expiresAt = now
	return nil
}

// Inspect implements [leaderelection.Store].
func (s *Store) Inspect(ctx context.Context, name string) (leaderelection.Lease, error) {
	if err := ctx.Err(); err != nil {
		return leaderelection.Lease{}, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	r, ok := s.records[name]
	if !ok {
		return leaderelection.Lease{}, leaderelection.ErrNoLease
	}
	return r.lease(name, s.now()), nil
}

func (r *record) renew(now time.Time, ttl time.Duration, payload []byte) {
	r.renewedAt = now
	r.expiresAt = now.Add(ttl)
	r.payload = append([]byte(nil), payload...)
}

func (r *record) lease(name string, now time.Time) leaderelection.Lease {
	return leaderelection.Lease{
		Name:       name,
		Holder:     r.holder,
		Fence:      r.fence,
		Payload:    append([]byte(nil), r.payload...),
		AcquiredAt: r.acquiredAt,
		RenewedAt:  r.renewedAt,
		ExpiresAt:  r.expiresAt,
		Now:        now,
	}
}
