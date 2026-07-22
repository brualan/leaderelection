package leaderelection

import (
	"crypto/rand"
	"encoding/hex"
	"os"
	"strconv"
)

// identityRandomBytes is the length of the random suffix. Eight bytes make an
// accidental collision between two live participants implausible even in a
// fleet that restarts constantly.
const identityRandomBytes = 8

// Indirection for the two system calls identity generation depends on, so
// that their failure paths can be exercised. Both are effectively infallible
// in practice, which is exactly why they would otherwise go untested.
var (
	randRead = rand.Read
	hostname = os.Hostname
)

// GenerateIdentity builds an identity for this process: hostname, PID and a
// random suffix, joined by hyphens.
//
// The random suffix is not decoration. Leader election is only safe if a
// restarted process cannot renew the lease that its own predecessor held: the
// predecessor may still be alive, blocked in a syscall or a stop-the-world
// pause, and still convinced it is the leader. Distinguishing incarnations by
// identity is what forces the successor to wait out the lease instead.
//
// Identity generation fails only if the system's randomness source does, in
// which case the caller must not fall back to a predictable value.
func GenerateIdentity() (string, error) {
	suffix := make([]byte, identityRandomBytes)
	if _, err := randRead(suffix); err != nil {
		return "", err
	}
	// A missing or empty hostname costs readability, not correctness: the
	// random suffix already guarantees uniqueness.
	host, err := hostname()
	if err != nil || host == "" {
		host = "unknown"
	}
	return host + "-" + strconv.Itoa(os.Getpid()) + "-" + hex.EncodeToString(suffix), nil
}
