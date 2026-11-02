package leaderelection

import (
	"context"
	"log/slog"
	"math"
	"time"

	"github.com/brualan/leaderelection/internal/clock"
)

// Defaults for [Config]. They are tuned for participants that reach a shared
// database across cluster boundaries, where round trips are tens of
// milliseconds rather than sub-millisecond.
const (
	DefaultLeaseDuration    = 15 * time.Second
	DefaultRenewInterval    = 5 * time.Second
	DefaultRetryPeriod      = 3 * time.Second
	DefaultOperationTimeout = 3 * time.Second

	// DefaultSafetyMarginFraction is the fraction of LeaseDuration reserved as
	// the safety margin when none is configured, subject to
	// MinSafetyMargin.
	DefaultSafetyMarginFraction = 10
	// MinSafetyMargin is the floor for a defaulted safety margin.
	MinSafetyMargin = time.Second

	// defaultJitterFraction smears polling across participants.
	defaultJitterFraction = 0.2
)

// Callbacks are invoked as leadership changes. All of them are optional, and
// all of them are called from the goroutine running [Elector.Run] except
// OnStartedLeading, which gets its own goroutine.
//
// Callbacks must not call back into the Elector that invoked them.
type Callbacks struct {
	// OnStartedLeading runs for the duration of a leadership term, in its own
	// goroutine. Its context is cancelled the instant the Elector stops
	// believing it is the leader -- including when the database has merely
	// become unreachable -- so leader-only work must respect it promptly.
	//
	// The cancellation bounds how long the work runs after the term ends; it
	// does not prevent overlap. Once this process loses the lease, a
	// participant elsewhere acquires it and starts its own callback while
	// this one may still be running.
	//
	// fence is the token for this term. Use it to guard writes to systems that
	// cannot participate in the election themselves.
	OnStartedLeading func(ctx context.Context, fence int64)

	// OnStoppedLeading runs after a term ends, once the OnStartedLeading
	// goroutine has returned. Run does not proceed until it returns.
	OnStoppedLeading func()

	// OnNewLeader runs when a new term is observed: a different identity
	// holding the lease, or the same identity holding it under a new fence.
	// The identity may be this participant's own.
	//
	// It is advisory: use it for logging and for routing requests towards the
	// leader, never to decide whether this process is the leader.
	OnNewLeader func(identity string, fence int64)
}

// Config describes one participant in one election.
type Config struct {
	// Store is the database-backed lease store. Required.
	Store Store

	// Name identifies the election. Participants contending for the same
	// leadership must use the same Name; unrelated elections use different
	// ones and may share a table. Required.
	Name string

	// Identity distinguishes this participant from every other one. When
	// empty, a value combining hostname, PID and random bytes is generated.
	//
	// Identity must be unique across restarts. Reusing a stable identity -- a
	// StatefulSet pod name, for instance -- lets a restarted process renew a
	// lease that its own stalled predecessor may still believe it holds.
	Identity string

	// Payload is opaque data published alongside the lease, readable by
	// followers through [Elector.Status]. A leader typically advertises the
	// address at which other clusters can reach it. Optional.
	//
	// [New] copies the slice: the elector owns its copy, and the caller's
	// buffer may be reused or mutated after New returns.
	Payload []byte

	// LeaseDuration is how long a lease stays valid after a successful
	// acquire or renew. It sets the worst-case failover time when a leader
	// disappears without releasing. Defaults to DefaultLeaseDuration.
	LeaseDuration time.Duration

	// RenewInterval is how often the leader extends its lease. Defaults to
	// DefaultRenewInterval.
	RenewInterval time.Duration

	// RetryPeriod is how often a follower polls for a claimable lease.
	// Defaults to DefaultRetryPeriod. Actual delays are jittered.
	RetryPeriod time.Duration

	// OperationTimeout bounds a single database call. It must be small
	// relative to LeaseDuration: a call that hangs for longer than the lease
	// would otherwise be indistinguishable from a healthy one.
	// Defaults to DefaultOperationTimeout.
	OperationTimeout time.Duration

	// SafetyMargin is how long before the lease's local expiry the Elector
	// gives up leadership. It absorbs the difference in rate between this
	// process's clock and the database's, plus the time the application needs
	// to notice its context was cancelled.
	//
	// Defaults to LeaseDuration/DefaultSafetyMarginFraction, floored at
	// MinSafetyMargin.
	SafetyMargin time.Duration

	// DisableRelease keeps the Elector from releasing its lease when Run
	// returns. Releasing lets a successor take over immediately instead of
	// waiting out LeaseDuration, so leaving this false is almost always right.
	DisableRelease bool

	// Logger receives structured logs about elections and failures. Defaults
	// to a logger that discards everything.
	Logger *slog.Logger

	// Callbacks are invoked as leadership changes. Optional.
	Callbacks Callbacks

	// clock and randFloat are injected by tests to make the loop
	// deterministic. Production code leaves them nil.
	clock     clock.Clock
	randFloat func() float64
}

// Validate reports whether the configuration is usable, applying no defaults.
// [New] calls it after defaulting, so callers rarely need it directly.
func (c Config) Validate() error {
	switch {
	case c.Store == nil:
		return &ConfigError{Field: "Store", Reason: "must not be nil"}
	case c.Name == "":
		return &ConfigError{Field: "Name", Reason: "must not be empty"}
	case c.Identity == "":
		return &ConfigError{Field: "Identity", Reason: "must not be empty"}
	case c.LeaseDuration <= 0:
		return &ConfigError{Field: "LeaseDuration", Reason: "must be positive"}
	case c.RenewInterval <= 0:
		return &ConfigError{Field: "RenewInterval", Reason: "must be positive"}
	case c.RetryPeriod <= 0:
		return &ConfigError{Field: "RetryPeriod", Reason: "must be positive"}
	case c.OperationTimeout <= 0:
		return &ConfigError{Field: "OperationTimeout", Reason: "must be positive"}
	case c.SafetyMargin < 0:
		return &ConfigError{Field: "SafetyMargin", Reason: "must not be negative"}
	case c.SafetyMargin >= c.LeaseDuration:
		return &ConfigError{Field: "SafetyMargin", Reason: "must be less than LeaseDuration"}
	}

	// The core timing invariant. A leader must be able to lose one renewal
	// outright -- request sent, OperationTimeout burned, nothing back -- and
	// still have a second attempt *complete* before it must step down at
	// LeaseDuration-SafetyMargin. Without this, a single slow round trip
	// costs leadership, and the election flaps under exactly the conditions
	// it exists to survive.
	//
	// Spelling out that worst case from the start of the term:
	//
	//	RenewInterval        the first attempt is not made before this
	//	OperationTimeout     the first attempt burns all of it and fails
	//	renewRetryDelay      the pause before trying again
	//	OperationTimeout     the second attempt must fit inside the rest
	//
	// Charging OperationTimeout twice is what makes this a real bound. A
	// check that charged it once would pass configurations where the second
	// attempt has less time than a single call is allowed to take.
	switch need, fits := c.renewalBudget(); {
	case !fits:
		return &ConfigError{
			Field: "RenewInterval",
			Reason: "RenewInterval+2*OperationTimeout+retry does not fit in a duration, " +
				"so no lease could ever cover it",
		}
	case need > c.LeaseDuration-c.SafetyMargin:
		return &ConfigError{
			Field: "RenewInterval",
			Reason: "a lost renewal needs RenewInterval+2*OperationTimeout+retry (" + need.String() +
				") but only LeaseDuration-SafetyMargin (" +
				(c.LeaseDuration - c.SafetyMargin).String() + ") is available",
		}
	}
	return nil
}

// renewalBudget is the time the worst-case renewal sequence needs: one attempt
// that burns its whole timeout and fails, then a second that has to finish.
//
// It reports false when that sum does not fit in a Duration. Letting it wrap
// instead would turn an impossible budget into a small negative one, and the
// comparison above would wave through exactly the configuration it exists to
// reject. Callers must be positive durations, which Validate has already
// established by the time it asks.
func (c Config) renewalBudget() (time.Duration, bool) {
	sum := c.RenewInterval
	for _, term := range [...]time.Duration{
		c.OperationTimeout,
		c.OperationTimeout,
		renewRetryDelayFor(c.RenewInterval),
	} {
		if term > math.MaxInt64-sum {
			return 0, false
		}
		sum += term
	}
	return sum, true
}

// renewRetryDelayFor is the pause between a failed renewal and the next
// attempt. It is a fraction of the renew interval so that several attempts fit
// inside what remains of the lease, with a floor that keeps a very short
// interval from turning into a spin.
func renewRetryDelayFor(renewInterval time.Duration) time.Duration {
	return max(renewInterval/4, minRenewRetryDelay)
}

// withDefaults returns a copy with every unset field populated.
func (c Config) withDefaults() (Config, error) {
	if c.LeaseDuration == 0 {
		c.LeaseDuration = DefaultLeaseDuration
	}
	if c.RenewInterval == 0 {
		c.RenewInterval = DefaultRenewInterval
	}
	if c.RetryPeriod == 0 {
		c.RetryPeriod = DefaultRetryPeriod
	}
	if c.OperationTimeout == 0 {
		c.OperationTimeout = DefaultOperationTimeout
	}
	if c.SafetyMargin == 0 {
		c.SafetyMargin = max(c.LeaseDuration/DefaultSafetyMarginFraction, MinSafetyMargin)
	}
	if c.Identity == "" {
		identity, err := GenerateIdentity()
		if err != nil {
			return c, err
		}
		c.Identity = identity
	}
	if c.Logger == nil {
		c.Logger = slog.New(discardHandler{})
	}
	if c.clock == nil {
		c.clock = clock.System()
	}
	return c, nil
}

// discardHandler drops every record. log/slog gained an exported equivalent in
// Go 1.24; this module supports 1.23.
type discardHandler struct{}

func (discardHandler) Enabled(context.Context, slog.Level) bool  { return false }
func (discardHandler) Handle(context.Context, slog.Record) error { return nil }
func (h discardHandler) WithAttrs([]slog.Attr) slog.Handler      { return h }
func (h discardHandler) WithGroup(string) slog.Handler           { return h }
