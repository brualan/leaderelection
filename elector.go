// Package leaderelection elects a single leader among processes that share
// nothing but a database.
//
// It exists for a deployment the built-in Kubernetes lease cannot serve:
// replicas of one service spread across separate clusters with no network path
// between them. The only thing they have in common is a Postgres or MySQL
// instance, so that is where the election happens.
//
// # Safety
//
// The guarantee is that at most one participant considers itself leader at any
// instant, and it survives network partitions, database outages, connection
// pooler restarts and abrupt process death. It rests on three choices, and it
// assumes the preconditions spelled out in the README's Guarantees section
// (durable database failover, a sufficient SafetyMargin, unique identities,
// prompt reaction to the term context):
//
// Every timestamp comes from the database. Participants in different clusters
// have unrelated clocks, so a lease expiry expressed as an absolute time would
// mean different things to different participants. Each statement returns both
// the expiry and the database's current time, and only their difference -- a
// duration -- is ever used.
//
// That duration is then measured against the local monotonic clock, starting
// from the moment the request was sent rather than when the reply arrived.
// The database began counting somewhere in between, so the resulting local
// deadline is always earlier than the true one.
//
// A watchdog enforces that deadline independently of the renewal loop. This is
// what makes an unreachable database safe rather than merely inconvenient: a
// renewal that hangs forever cannot delay the step-down, because nothing about
// stepping down waits on the database. Uncertainty always resolves to "not
// leader" -- if this process cannot prove it still holds the lease, it stops
// acting as though it does, and it does so before the lease could possibly be
// handed to anyone else.
//
// # Fencing
//
// Stepping down promptly bounds how long a stalled leader can act, but it
// cannot unsend a write already in flight. Systems that must be protected from
// that -- an object store, a queue, another database -- need a fencing token.
//
// Every term is handed a [Lease.Fence], through
// [Callbacks.OnStartedLeading] and [Elector.Fence]. Across the whole election
// those tokens are strictly increasing: no two terms anywhere ever share one,
// whether they belong to the same participant or to different ones. Attach the
// token to external writes and have the receiving system reject anything
// stamped with one older than the highest it has seen.
//
// That claim assumes the database never hands back a state older than the
// one that granted the most recent term: a failover to a lagging replica, or
// a restore from an old backup, rewinds the counter and can reissue a token.
// See the README, "Durability across database failover".
package leaderelection

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/brualan/leaderelection/internal/backoff"
	"github.com/brualan/leaderelection/internal/clock"
)

// Elector runs one participant in one election. Use [New] to build one and
// [Elector.Run] to drive it.
type Elector struct {
	cfg   Config
	clock clock.Clock

	running atomicFlag

	mu sync.Mutex
	// term is non-nil while this participant holds a lease.
	term *term
	// spentFence is the highest token a term has already run under. A token
	// is never reused for a second term, so that a downstream system can tell
	// two terms apart even when the database had no reason to renumber them.
	spentFence int64
	// Last successful observation of the election, leader or not.
	observedLeader  string
	observedFence   int64
	observedPayload []byte
	observedAt      time.Time
	lastErr         error
	// Metrics counters and outstanding application callback, guarded by mu.
	metrics    metricCounters
	leaderWork *term
}

// term is a single leadership term.
type term struct {
	fence  int64
	ctx    context.Context
	cancel context.CancelFunc
	// deadline is the local instant at which the database's copy of this
	// lease expires, as computed conservatively from the last confirmation.
	// Guarded by Elector.mu; it only ever moves forward.
	deadline time.Time
	// reason records why the term ended, nil for an orderly shutdown. It is
	// what tells endTerm whether releasing the lease is still meaningful.
	// Guarded by Elector.mu.
	reason error
	// done is closed once OnStartedLeading has returned.
	done chan struct{}
}

// New validates cfg, fills in defaults, and returns an Elector.
//
// New takes ownership of cfg.Payload: it is copied, so the caller may reuse
// or mutate its own buffer freely afterwards.
func New(cfg Config) (*Elector, error) {
	cfg, err := cfg.withDefaults()
	if err != nil {
		return nil, err
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	cfg.Payload = bytes.Clone(cfg.Payload)
	return &Elector{cfg: cfg, clock: cfg.clock}, nil
}

// Run participates in the election until ctx is cancelled.
//
// It blocks. It returns nil on a clean shutdown, which makes it well behaved
// inside an errgroup, and [ErrAlreadyRunning] if this Elector is already
// running. Database failures are never returned: they are transient by
// assumption, reported through [Status.LastError], and handled by giving up
// leadership rather than by giving up.
//
// On return, leadership has been fully relinquished: the OnStartedLeading
// context is cancelled, that callback has returned, OnStoppedLeading has run,
// and the lease has been released unless [Config.DisableRelease] is set.
func (e *Elector) Run(ctx context.Context) error {
	if !e.running.set() {
		return ErrAlreadyRunning
	}
	defer e.running.clear()

	// Failures to reach the database back off; failures to win an election do
	// not, because a contested election is the healthy case.
	failures := backoff.New(
		e.cfg.RetryPeriod, max(e.cfg.RetryPeriod, e.cfg.LeaseDuration), 2,
		defaultJitterFraction, e.cfg.randFloat)

	for ctx.Err() == nil {
		won, err := e.campaign(ctx)
		switch {
		case err != nil:
			e.recordError(err)
			e.cfg.Logger.LogAttrs(ctx, slogLevelWarn, "leaderelection: campaign failed",
				attrName(e.cfg.Name), attrIdentity(e.cfg.Identity), attrError(err))
			if !e.wait(ctx, failures.Next()) {
				return nil
			}
		case won != nil:
			failures.Reset()
			e.lead(ctx, won)
			// A term just ended. Pausing before re-campaigning keeps a
			// process that lost the database from hammering it, and gives a
			// healthy successor a chance to take over.
			if !e.wait(ctx, e.pollDelay()) {
				return nil
			}
		default:
			failures.Reset()
			if !e.wait(ctx, e.pollDelay()) {
				return nil
			}
		}
	}
	return nil
}

// IsLeader reports whether this participant may act as leader right now.
//
// It is safe to call from any goroutine and is the authoritative answer: it
// re-checks the term deadline on every call rather than trusting a flag, so a
// descheduled watchdog goroutine cannot widen the window during which a stale
// leader believes itself current. When in any doubt it returns false.
func (e *Elector) IsLeader() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	// Read the clock under the lock: a caller stalled on the mutex must be
	// judged against the moment it actually got in, not an earlier one.
	return e.term != nil && e.leadingAtLocked(e.clock.Now())
}

// Fence returns the fence token of the current term, and whether there is one.
// Use it to stamp writes to systems outside the election.
func (e *Elector) Fence() (int64, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.term == nil || !e.leadingAtLocked(e.clock.Now()) {
		return 0, false
	}
	return e.term.fence, true
}

// leadingAtLocked reports whether the current term is still valid at now.
// Callers hold e.mu and have already checked that e.term is non-nil.
func (e *Elector) leadingAtLocked(now time.Time) bool {
	return now.Before(e.term.deadline.Add(-e.cfg.SafetyMargin))
}

// campaign performs one attempt to become leader. It returns the new term on
// success and nil when another participant holds the lease; a non-nil error
// means the database could not be reached, which is distinct from losing the
// election.
func (e *Elector) campaign(ctx context.Context) (*term, error) {
	// Read before writing. Followers outnumber leaders, and an unconditional
	// upsert per poll per participant would write a row version each time for
	// no reason.
	lease, err := e.inspect(ctx)
	switch {
	case err == nil:
		e.observe(lease)
		if !lease.Expired() && lease.Holder != e.cfg.Identity {
			return nil, nil
		}
	case errors.Is(err, ErrNoLease):
		// Nobody has ever held this election; create the record below.
	default:
		return nil, err
	}

	sentAt := e.clock.Now()
	lease, err = e.acquire(ctx)
	if err != nil {
		return nil, err
	}
	e.observe(lease)
	if !lease.HeldBy(e.cfg.Identity) {
		return nil, nil
	}
	// A lease that is already inside its safety margin is not worth taking:
	// promotion would be followed immediately by a step-down, and the
	// application would see a term it could do nothing useful with. This
	// happens when the round trip itself ate most of the lease.
	if lease.TTL() <= e.cfg.SafetyMargin {
		e.cfg.Logger.LogAttrs(ctx, slogLevelWarn, "leaderelection: acquired lease already inside safety margin",
			attrName(e.cfg.Name), attrIdentity(e.cfg.Identity), attrTTL(lease.TTL()))
		return nil, nil
	}

	// A token we have already led with must not start a second term.
	//
	// This happens after a local step-down: the watchdog gives up leadership
	// because the lease could not be confirmed, but the lease itself is still
	// ours and still valid in the database -- the local deadline is
	// deliberately the earlier of the two. Re-acquiring it is legitimate, and
	// the store correctly leaves the fence alone because nobody else could
	// have led in between. But the previous term's work was already told to
	// stop, and a write from it that is still in flight must not be
	// indistinguishable from this term's. Giving the lease up forces the next
	// acquisition to mint a fresh token.
	if e.hasLedWith(lease.Fence) {
		e.cfg.Logger.LogAttrs(ctx, slogLevelWarn, "leaderelection: releasing a lease whose fence is already spent",
			attrName(e.cfg.Name), attrIdentity(e.cfg.Identity), attrFence(lease.Fence))
		if err := e.release(ctx, lease.Fence); err != nil {
			return nil, err
		}
		return nil, nil
	}

	return e.promote(ctx, lease, sentAt), nil
}

// hasLedWith reports whether a term has already run under this fence token.
func (e *Elector) hasLedWith(fence int64) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.spentFence != 0 && fence <= e.spentFence
}

// promote installs a new term and starts the watchdog and the application's
// leader goroutine.
func (e *Elector) promote(ctx context.Context, lease Lease, sentAt time.Time) *term {
	// Anchor at sentAt, not at the time the reply arrived: the database began
	// counting down somewhere between the two, and anchoring at the earlier
	// bound keeps our deadline strictly ahead of the real expiry.
	t := &term{
		fence:    lease.Fence,
		deadline: sentAt.Add(min(lease.TTL(), e.cfg.LeaseDuration)),
		done:     make(chan struct{}),
	}
	// Deriving from ctx means a cancelled Run tears the term down without
	// waiting for the renewal loop to notice.
	t.ctx, t.cancel = context.WithCancel(ctx)

	e.mu.Lock()
	// The response (or OnNewLeader callback) may have consumed the entire
	// useful lease. Never start application work on that stale confirmation.
	if ctx.Err() != nil || !e.clock.Now().Before(t.deadline.Add(-e.cfg.SafetyMargin)) {
		e.mu.Unlock()
		t.cancel()
		return nil
	}
	e.term = t
	e.leaderWork = t
	e.metrics.termsStarted++
	e.metrics.lastRenewalAt = time.Time{}
	e.spentFence = lease.Fence
	e.mu.Unlock()

	e.cfg.Logger.LogAttrs(ctx, slogLevelInfo, "leaderelection: acquired leadership",
		attrName(e.cfg.Name), attrIdentity(e.cfg.Identity), attrFence(t.fence))

	go e.watch(t)
	go func() {
		defer func() {
			e.mu.Lock()
			if e.leaderWork == t {
				e.leaderWork = nil
			}
			e.mu.Unlock()
			close(t.done)
		}()
		if e.cfg.Callbacks.OnStartedLeading != nil {
			e.cfg.Callbacks.OnStartedLeading(t.ctx, t.fence)
		}
	}()
	return t
}

// watch enforces the term deadline independently of the renewal loop.
//
// This goroutine is the reason an unreachable database is safe. It touches
// nothing that can block on the network, so no amount of database misbehaviour
// -- a hung connection, a paused server, a pooler swallowing the request --
// can postpone the step-down it performs.
func (e *Elector) watch(t *term) {
	timer := e.clock.NewTimer(e.remaining(t))
	defer timer.Stop()

	for {
		select {
		case <-t.ctx.Done():
			return
		case <-timer.C():
		}
		if r := e.remaining(t); r > 0 {
			// A renewal landed while we were asleep; the deadline moved out.
			timer.Reset(r)
			continue
		}
		e.stepDown(t, ErrLeaseExpiredLocally)
		return
	}
}

// remaining reports how long until this term must end. It is negative once the
// safety margin has been entered.
func (e *Elector) remaining(t *term) time.Duration {
	now := e.clock.Now()
	e.mu.Lock()
	defer e.mu.Unlock()
	return t.deadline.Add(-e.cfg.SafetyMargin).Sub(now)
}

// lead renews the lease until the term ends, then tears it down. It returns
// once leadership has been fully relinquished.
func (e *Elector) lead(ctx context.Context, t *term) {
	defer e.endTerm(ctx, t)

	timer := e.clock.NewTimer(e.cfg.RenewInterval)
	defer timer.Stop()

	for {
		select {
		case <-t.ctx.Done():
			return
		case <-timer.C():
		}

		sentAt := e.clock.Now()
		lease, err := e.renew(t.ctx, t.fence) //nolint:contextcheck // The term context inherits ctx and also cancels on leadership loss.
		switch {
		case err == nil:
			if !lease.HeldBy(e.cfg.Identity) || lease.Fence != t.fence {
				// The store reported success for a lease that is not ours.
				// Trust the data over the status and step down.
				e.recordError(ErrLeaseLost)
				e.stepDown(t, ErrLeaseLost)
				return
			}
			e.observe(lease)
			if !e.extend(t, lease, sentAt) {
				// An orderly shutdown can race with a successful reply. Keep
				// its normal teardown (including release), and preserve any
				// reason already recorded by the watchdog.
				if t.ctx.Err() == nil {
					e.stepDown(t, ErrLeaseExpiredLocally)
				}
				return
			}
			timer.Reset(e.cfg.RenewInterval)

		case errors.Is(err, ErrLeaseLost):
			e.recordError(err)
			e.cfg.Logger.LogAttrs(ctx, slogLevelWarn, "leaderelection: lease lost",
				attrName(e.cfg.Name), attrIdentity(e.cfg.Identity), attrFence(t.fence))
			e.stepDown(t, err)
			return

		default:
			if t.ctx.Err() != nil {
				// The term ended while this call was in flight, so the error
				// is nothing but the echo of our own cancellation. Recording
				// it would overwrite the reason we actually stepped down.
				return
			}
			e.recordError(err)
			e.cfg.Logger.LogAttrs(ctx, slogLevelWarn, "leaderelection: renewal failed",
				attrName(e.cfg.Name), attrIdentity(e.cfg.Identity), attrError(err))
			// Retry well inside the renew interval. The watchdog is already
			// counting down, and these attempts are the only thing that can
			// stop it from firing.
			timer.Reset(e.renewRetryDelay())
		}
	}
}

// extend moves a still-live term's deadline out after a confirmed renewal.
// False means the caller must finish teardown rather than continue renewing.
func (e *Elector) extend(t *term, lease Lease, sentAt time.Time) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	now := e.clock.Now()
	if e.term != t || t.ctx.Err() != nil || !e.leadingAtLocked(now) {
		// A delayed watchdog must not allow a renewal to resurrect a term
		// which callers have already observed as expired.
		return false
	}
	// Never trust a TTL longer than the one we asked for. A store that
	// reported an inflated remaining time -- through a bug, or a clock the
	// server itself disagrees with -- would otherwise push the local deadline
	// past the real expiry, which is the one thing this calculation exists to
	// stay behind.
	granted := min(lease.TTL(), e.cfg.LeaseDuration)
	// Only ever forward. A reply that overtook an earlier one must not be
	// able to shorten a deadline already granted.
	if d := sentAt.Add(granted); d.After(t.deadline) {
		t.deadline = d
	}
	e.metrics.renewalsAccepted++
	e.metrics.lastRenewalAt = now
	return true
}

// stepDown ends the term: this participant stops being the leader and the
// application's context is cancelled. It is idempotent and safe to call from
// any goroutine.
func (e *Elector) stepDown(t *term, reason error) {
	lost := errors.Is(reason, ErrLeaseLost)
	expired := errors.Is(reason, ErrLeaseExpiredLocally)
	e.mu.Lock()
	if e.term != t {
		e.mu.Unlock()
		return
	}
	e.term = nil
	e.metrics.termsEnded++
	switch {
	case lost:
		e.metrics.leaseLosses++
	case expired:
		e.metrics.localExpirations++
	}
	t.reason = reason
	if reason != nil {
		e.lastErr = reason
	}
	e.mu.Unlock()

	// Cancelling is what the application observes. Everything else about
	// ending a term can wait; this cannot.
	t.cancel()
}

// endTerm completes the teardown started by stepDown and hands the lease back.
func (e *Elector) endTerm(ctx context.Context, t *term) {
	e.stepDown(t, nil)

	// Wait for the application's leader goroutine to return before this
	// Elector proceeds to a new term or releases the lease. This is a local
	// ordering only: it says nothing about other processes. The moment the
	// lease is lost, a participant elsewhere can acquire it and start its own
	// OnStartedLeading while this one is still running -- which is exactly
	// why external effects need fencing tokens.
	<-t.done

	if e.cfg.Callbacks.OnStoppedLeading != nil {
		e.cfg.Callbacks.OnStoppedLeading()
	}

	e.mu.Lock()
	reason := t.reason
	e.mu.Unlock()

	// Releasing is only worth a round trip when we still plausibly hold the
	// lease. If we stepped down because we lost it, or because we could not
	// confirm it, the release would either be rejected by its own fence guard
	// or block for the full operation timeout against a database we already
	// know we cannot reach -- delaying shutdown to accomplish nothing.
	if !e.cfg.DisableRelease && reason == nil {
		// ctx is usually already cancelled at this point, since shutdown is
		// the common way to get here, so the release gets a fresh deadline.
		if err := e.release(context.WithoutCancel(ctx), t.fence); err != nil {
			e.cfg.Logger.LogAttrs(ctx, slogLevelWarn, "leaderelection: release failed",
				attrName(e.cfg.Name), attrIdentity(e.cfg.Identity), attrError(err))
		}
	}

	e.cfg.Logger.LogAttrs(ctx, slogLevelInfo, "leaderelection: leadership ended",
		attrName(e.cfg.Name), attrIdentity(e.cfg.Identity), attrFence(t.fence))
}

// observe records a successful read of the election and notifies the
// application when the observed leader changes.
func (e *Elector) observe(lease Lease) {
	e.mu.Lock()
	changed := e.observedLeader != lease.Holder || e.observedFence != lease.Fence
	if changed {
		e.metrics.leaderChangesObserved++
	}
	e.observedLeader = lease.Holder
	e.observedFence = lease.Fence
	// The lease came from a store, and a store implementation is free to
	// reuse its buffers. Keeping our own copy is what makes Status safe to
	// hand out.
	e.observedPayload = bytes.Clone(lease.Payload)
	e.observedAt = e.clock.Now()
	e.lastErr = nil
	e.mu.Unlock()

	if changed && e.cfg.Callbacks.OnNewLeader != nil {
		e.cfg.Callbacks.OnNewLeader(lease.Holder, lease.Fence)
	}
}

func (e *Elector) recordError(err error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.lastErr = err
}

// wait sleeps for d, reporting false if ctx was cancelled first.
func (e *Elector) wait(ctx context.Context, d time.Duration) bool {
	timer := e.clock.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C():
		return true
	}
}

func (e *Elector) pollDelay() time.Duration {
	return backoff.Jitter(e.cfg.RetryPeriod, defaultJitterFraction, e.cfg.randFloat)
}

// renewRetryDelay paces retries after a failed renewal, jittered so that
// concurrent participants do not resonate. Config.Validate budgets for the
// unjittered value, and jitter only ever shortens it.
func (e *Elector) renewRetryDelay() time.Duration {
	return backoff.Jitter(renewRetryDelayFor(e.cfg.RenewInterval), defaultJitterFraction, e.cfg.randFloat)
}

const minRenewRetryDelay = 50 * time.Millisecond

// atomicFlag guards Run against being entered twice.
type atomicFlag struct {
	v atomic.Bool
}

func (f *atomicFlag) set() bool { return f.v.CompareAndSwap(false, true) }

func (f *atomicFlag) clear() { f.v.Store(false) }
