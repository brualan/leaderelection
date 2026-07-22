package leaderelection

import (
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/brualan/leaderelection/internal/assert"
)

// Lease arithmetic is the hinge of the whole safety argument, and it is
// deliberately expressed only in terms of the database's own clock. These
// cases pin down that no local time ever enters the calculation.
func TestLeaseTTL(t *testing.T) {
	// A database clock far from anything a client would report.
	dbNow := time.Date(1999, 1, 1, 0, 0, 0, 0, time.UTC)

	for _, tc := range []struct {
		name    string
		lease   Lease
		ttl     time.Duration
		expired bool
	}{
		{
			name:  "live",
			lease: Lease{Now: dbNow, ExpiresAt: dbNow.Add(10 * time.Second)},
			ttl:   10 * time.Second,
		},
		{
			name:    "expiring exactly now counts as expired",
			lease:   Lease{Now: dbNow, ExpiresAt: dbNow},
			ttl:     0,
			expired: true,
		},
		{
			// A negative remainder must clamp rather than propagate: a
			// negative TTL added to a local instant would produce a deadline
			// in the past and, worse, a plausible-looking one.
			name:    "long expired",
			lease:   Lease{Now: dbNow, ExpiresAt: dbNow.Add(-time.Hour)},
			ttl:     0,
			expired: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.lease.TTL(), tc.ttl, "TTL")
			assert.Equal(t, tc.lease.Expired(), tc.expired, "Expired")
		})
	}
}

func TestLeaseHeldBy(t *testing.T) {
	dbNow := time.Date(1999, 1, 1, 0, 0, 0, 0, time.UTC)
	live := Lease{Holder: "a", Now: dbNow, ExpiresAt: dbNow.Add(time.Minute)}
	dead := Lease{Holder: "a", Now: dbNow, ExpiresAt: dbNow}

	assert.True(t, live.HeldBy("a"), "holder of a live lease")
	assert.False(t, live.HeldBy("b"), "non-holder of a live lease")
	assert.False(t, dead.HeldBy("a"), "holder of an expired lease")
	assert.False(t, Lease{}.HeldBy(""), "the zero lease")
}

func TestConfigErrorMessage(t *testing.T) {
	err := &ConfigError{Field: "LeaseDuration", Reason: "must be positive"}
	msg := err.Error()
	assert.True(t, strings.Contains(msg, "LeaseDuration"), "message names the field")
	assert.True(t, strings.Contains(msg, "must be positive"), "message gives the reason")
	assert.True(t, strings.HasPrefix(msg, "leaderelection:"), "message is prefixed")
}

func TestGenerateIdentity(t *testing.T) {
	first, err := GenerateIdentity()
	assert.NoError(t, err, "GenerateIdentity")
	second, err := GenerateIdentity()
	assert.NoError(t, err, "GenerateIdentity")

	// Uniqueness across incarnations is what stops a restarted process from
	// renewing the lease its own predecessor still believes it holds.
	if first == second {
		t.Fatalf("two identities came out identical: %q", first)
	}
	host, _ := os.Hostname()
	if host != "" && !strings.HasPrefix(first, host+"-") {
		t.Fatalf("identity %q does not start with the hostname %q", first, host)
	}
	if len(strings.Split(first, "-")) < 3 {
		t.Fatalf("identity %q lacks the hostname-pid-random shape", first)
	}
}

func TestGenerateIdentityWithoutAHostname(t *testing.T) {
	restore := hostname
	t.Cleanup(func() { hostname = restore })

	for _, tc := range []struct {
		name string
		host func() (string, error)
	}{
		{"hostname fails", func() (string, error) { return "", errors.New("no hostname") }},
		{"hostname is empty", func() (string, error) { return "", nil }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			hostname = tc.host
			identity, err := GenerateIdentity()
			assert.NoError(t, err, "GenerateIdentity")
			// Losing the hostname costs readability, never uniqueness.
			assert.True(t, strings.HasPrefix(identity, "unknown-"), "identity falls back")
		})
	}
}

func TestGenerateIdentityFailsWithoutRandomness(t *testing.T) {
	restore := randRead
	t.Cleanup(func() { randRead = restore })
	sentinel := errors.New("entropy pool drained")
	randRead = func([]byte) (int, error) { return 0, sentinel }

	// Falling back to a predictable identity here would quietly break the
	// guarantee, so this must fail loudly instead.
	_, err := GenerateIdentity()
	assert.ErrorIs(t, err, sentinel, "GenerateIdentity")
}
