package leaderelection

import (
	"log/slog"
	"time"
)

// Log levels used by the election loop. Losing an election is normal and logs
// at info; failing to reach the database, or stepping down because of it,
// logs at warn. Nothing here logs at error: every failure this package
// encounters is one it is designed to absorb.
const (
	slogLevelInfo = slog.LevelInfo
	slogLevelWarn = slog.LevelWarn
)

// Attribute constructors, kept together so that log keys stay consistent
// across the package and remain greppable in an operator's log pipeline.

func attrName(name string) slog.Attr { return slog.String("election", name) }

func attrIdentity(identity string) slog.Attr { return slog.String("identity", identity) }

func attrFence(fence int64) slog.Attr { return slog.Int64("fence", fence) }

func attrError(err error) slog.Attr { return slog.String("error", err.Error()) }

func attrTTL(ttl time.Duration) slog.Attr { return slog.Duration("ttl", ttl) }
