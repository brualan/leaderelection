package leaderelection

import (
	"context"
	"errors"
	"log/slog"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/brualan/leaderelection/internal/assert"
)

func validConfig() Config {
	return Config{
		Store:            newFakeStore(),
		Name:             "election",
		Identity:         "me",
		LeaseDuration:    testLease,
		RenewInterval:    testRenew,
		RetryPeriod:      testRetry,
		OperationTimeout: testOpLimit,
		SafetyMargin:     testMargin,
	}
}

func TestConfigValidateRejectsBadValues(t *testing.T) {
	// Validate applies no defaults, so it rejects a zero duration. New fills
	// those in first, so it accepts the same config -- which is the point of
	// having both. defaultable marks the fields where the two legitimately
	// disagree.
	for _, tc := range []struct {
		name        string
		field       string
		defaultable bool
		mut         func(*Config)
	}{
		{name: "no store", field: "Store", mut: func(c *Config) { c.Store = nil }},
		{name: "no name", field: "Name", mut: func(c *Config) { c.Name = "" }},
		{name: "no identity", field: "Identity", defaultable: true, mut: func(c *Config) { c.Identity = "" }},
		{name: "zero lease", field: "LeaseDuration", defaultable: true, mut: func(c *Config) { c.LeaseDuration = 0 }},
		{name: "negative lease", field: "LeaseDuration", mut: func(c *Config) { c.LeaseDuration = -time.Second }},
		{name: "zero renew interval", field: "RenewInterval", defaultable: true, mut: func(c *Config) { c.RenewInterval = 0 }},
		{name: "zero retry period", field: "RetryPeriod", defaultable: true, mut: func(c *Config) { c.RetryPeriod = 0 }},
		{name: "zero operation timeout", field: "OperationTimeout", defaultable: true, mut: func(c *Config) { c.OperationTimeout = 0 }},
		{name: "negative safety margin", field: "SafetyMargin", mut: func(c *Config) { c.SafetyMargin = -time.Second }},
		{name: "safety margin swallows the lease", field: "SafetyMargin", mut: func(c *Config) { c.SafetyMargin = testLease }},

		// The timing invariant: a leader must be able to lose one renewal
		// outright and still get a second attempt in before it has to step
		// down. Each of these breaks it from a different direction, and no
		// default can rescue any of them.
		{name: "renew interval too long", field: "RenewInterval", mut: func(c *Config) { c.RenewInterval = 7 * time.Second }},
		{name: "operation timeout too long", field: "RenewInterval", mut: func(c *Config) { c.OperationTimeout = 6 * time.Second }},
		{name: "lease too short", field: "RenewInterval", mut: func(c *Config) { c.LeaseDuration = 12 * time.Second }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := validConfig()
			tc.mut(&cfg)

			err := cfg.Validate()
			assert.Error(t, err, "Validate")

			var cerr *ConfigError
			if !errors.As(err, &cerr) {
				t.Fatalf("Validate returned %T, want *ConfigError", err)
			}
			assert.Equal(t, cerr.Field, tc.field, "ConfigError.Field")
			if !strings.Contains(cerr.Error(), tc.field) {
				t.Fatalf("error message %q does not name the field", cerr.Error())
			}

			_, err = New(cfg)
			if tc.defaultable {
				assert.NoError(t, err, "New on a defaultable field")
			} else if err == nil {
				t.Fatal("New accepted a config that no default can fix")
			}
		})
	}
}

func TestConfigValidateAcceptsDefaults(t *testing.T) {
	cfg := Config{Store: newFakeStore(), Name: "election"}
	full, err := cfg.withDefaults()
	assert.NoError(t, err, "withDefaults")
	assert.NoError(t, full.Validate(), "Validate on defaults")

	assert.Equal(t, full.LeaseDuration, DefaultLeaseDuration, "LeaseDuration")
	assert.Equal(t, full.RenewInterval, DefaultRenewInterval, "RenewInterval")
	assert.Equal(t, full.RetryPeriod, DefaultRetryPeriod, "RetryPeriod")
	assert.Equal(t, full.OperationTimeout, DefaultOperationTimeout, "OperationTimeout")
	assert.Equal(t, full.SafetyMargin, DefaultLeaseDuration/DefaultSafetyMarginFraction, "SafetyMargin")
	assert.True(t, full.Identity != "", "Identity was generated")
	assert.True(t, full.Logger != nil, "Logger was defaulted")
	assert.True(t, full.clock != nil, "clock was defaulted")
}

// The defaults must not merely be valid; they must leave usable slack. A
// configuration that only just satisfies the invariant would flap on the first
// slow round trip.
func TestDefaultsLeaveRoomForARetry(t *testing.T) {
	cfg, err := Config{Store: newFakeStore(), Name: "election"}.withDefaults()
	assert.NoError(t, err, "withDefaults")

	budget := cfg.LeaseDuration - cfg.SafetyMargin
	need := 2*cfg.RenewInterval + cfg.OperationTimeout
	if need >= budget {
		t.Fatalf("defaults leave no slack: need %v of a %v budget", need, budget)
	}
}

// A short lease must still get a safety margin big enough to be meaningful,
// which is what the floor is for.
func TestSafetyMarginHasAFloor(t *testing.T) {
	cfg, err := Config{
		Store:         newFakeStore(),
		Name:          "election",
		LeaseDuration: 5 * time.Second,
	}.withDefaults()
	assert.NoError(t, err, "withDefaults")
	assert.Equal(t, cfg.SafetyMargin, MinSafetyMargin, "SafetyMargin")
}

func TestExplicitValuesSurviveDefaulting(t *testing.T) {
	cfg := validConfig()
	cfg.Logger = slog.New(slog.NewTextHandler(nopWriter{}, nil))
	full, err := cfg.withDefaults()
	assert.NoError(t, err, "withDefaults")

	assert.Equal(t, full.Identity, "me", "Identity")
	assert.Equal(t, full.LeaseDuration, testLease, "LeaseDuration")
	assert.Equal(t, full.SafetyMargin, testMargin, "SafetyMargin")
	assert.True(t, full.Logger == cfg.Logger, "Logger")
}

// Identity generation is the one defaulting step that can fail, and a failure
// must abort rather than fall back to something predictable.
func TestDefaultingFailsWhenIdentityCannotBeGenerated(t *testing.T) {
	restore := randRead
	t.Cleanup(func() { randRead = restore })
	randRead = func([]byte) (int, error) { return 0, errNoRandomness }

	_, err := Config{Store: newFakeStore(), Name: "election"}.withDefaults()
	assert.ErrorIs(t, err, errNoRandomness, "withDefaults")

	_, err = New(Config{Store: newFakeStore(), Name: "election"})
	assert.ErrorIs(t, err, errNoRandomness, "New")
}

var errNoRandomness = errors.New("no randomness available")

// The default logger must be inert, including for handlers that a caller's
// slog wrapper might reach for.
func TestDiscardHandlerIsInert(t *testing.T) {
	h := discardHandler{}
	assert.False(t, h.Enabled(context.Background(), slog.LevelError), "Enabled")
	assert.NoError(t, h.Handle(context.Background(), slog.Record{}), "Handle")
	assert.True(t, h.WithAttrs([]slog.Attr{slog.String("k", "v")}) == h, "WithAttrs")
	assert.True(t, h.WithGroup("g") == h, "WithGroup")

	logger := slog.New(h)
	logger.With("k", "v").WithGroup("g").Error("this must go nowhere")
}

type nopWriter struct{}

func (nopWriter) Write(p []byte) (int, error) { return len(p), nil }

// The timing invariant has to be a real bound, not an approximation that only
// happens to hold for the defaults. Its promise is that a leader can lose one
// renewal outright and still get a second attempt to finish, so the second
// attempt must be budgeted the full OperationTimeout it is entitled to take.
func TestInvariantChargesTheOperationTimeoutTwice(t *testing.T) {
	// A long OperationTimeout relative to RenewInterval. Charging the timeout
	// once puts this comfortably inside the budget; charging it twice, as the
	// worst case actually requires, does not.
	cfg := validConfig()
	cfg.LeaseDuration = 15 * time.Second
	cfg.SafetyMargin = time.Second
	cfg.RenewInterval = time.Second
	cfg.OperationTimeout = 11 * time.Second

	if once := 2*cfg.RenewInterval + cfg.OperationTimeout; once > cfg.LeaseDuration-cfg.SafetyMargin {
		t.Fatalf("test case is not discriminating: charging once already needs %v", once)
	}
	assert.Error(t, cfg.Validate(), "Validate")

	// The worst-case sequence the invariant describes, spelled out.
	budget := cfg.LeaseDuration - cfg.SafetyMargin
	firstAttemptFails := cfg.RenewInterval + cfg.OperationTimeout
	secondAttemptEnds := firstAttemptFails + renewRetryDelayFor(cfg.RenewInterval) + cfg.OperationTimeout
	if secondAttemptEnds <= budget {
		t.Fatalf("second attempt would have finished at %v within a %v budget; "+
			"the config should not have been rejected", secondAttemptEnds, budget)
	}
}

// The defaults must clear the invariant with room, not sit on it.
func TestDefaultsClearTheInvariantWithRoom(t *testing.T) {
	cfg, err := Config{Store: newFakeStore(), Name: "election"}.withDefaults()
	assert.NoError(t, err, "withDefaults")

	need, fits := cfg.renewalBudget()
	assert.True(t, fits, "the defaults' renewal budget fits in a duration")
	budget := cfg.LeaseDuration - cfg.SafetyMargin
	if need > budget {
		t.Fatalf("defaults do not satisfy their own invariant: need %v of %v", need, budget)
	}
	if slack := budget - need; slack < cfg.OperationTimeout/4 {
		t.Fatalf("defaults leave only %v of slack; a single slow round trip would cost leadership", slack)
	}
}

// A configuration built from absurd numbers must be rejected on its merits
// rather than accepted because the arithmetic wrapped.
//
// Nobody configures a lease of 292 years on purpose, but the durations in a
// deployment come from templated configuration, and a check that can be
// defeated by making the numbers bigger is not a check. Found by
// FuzzConfigValidate.
func TestInvariantSurvivesAbsurdDurations(t *testing.T) {
	cfg := validConfig()
	cfg.LeaseDuration = math.MaxInt64
	cfg.RenewInterval = math.MaxInt64
	cfg.OperationTimeout = math.MaxInt64
	cfg.SafetyMargin = 0

	// The real budget is over three times the lease. Summed without care it
	// overflows into a negative duration, which compares as though there were
	// budget to spare.
	_, fits := cfg.renewalBudget()
	assert.False(t, fits, "a budget that cannot be represented reports so")
	assert.Error(t, cfg.Validate(), "Validate")
}

// The retry delay is what the invariant budgets for, so the loop and the
// validation must agree on it exactly.
func TestRenewRetryDelayHasAFloor(t *testing.T) {
	assert.Equal(t, renewRetryDelayFor(4*time.Second), time.Second, "quarter of the interval")
	assert.Equal(t, renewRetryDelayFor(time.Millisecond), minRenewRetryDelay, "floor for a tiny interval")

	h := newHarness(t, nil)
	// Jitter only ever shortens, so the loop never waits longer than the
	// validation assumed.
	if got := h.e.renewRetryDelay(); got > renewRetryDelayFor(testRenew) {
		t.Fatalf("renewRetryDelay = %v, longer than the budgeted %v",
			got, renewRetryDelayFor(testRenew))
	}
}
