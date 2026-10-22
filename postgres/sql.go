package postgres

import (
	"fmt"
	"regexp"
	"strings"
)

// Every statement below obeys three rules, and breaking any of them breaks a
// supported deployment:
//
// One statement per operation. PgBouncer in transaction pooling mode gives
// back a different server connection after every transaction, so nothing may
// span two of them. Single autocommit statements are also the only shape that
// cannot leave a half-applied election behind.
//
// All time from the server. now() is the transaction timestamp, so every
// reference to it inside one statement returns the same instant, and every
// returned timestamp -- including the caller's view of "now" -- comes from one
// clock. Participants in other clusters therefore never compare their own
// clock against anyone else's.
//
// No session state. No prepared statement names, no SET, no advisory locks, no
// LISTEN. The statements work identically on a direct connection and through a
// pooler.

// acquireSQL starts or continues a leadership term.
//
// It is expressed as a single upsert whose SET clauses are conditional, rather
// than as a guarded update, so that it always returns the resulting row. A
// participant that loses the race still learns who won and with which fence,
// which is what lets a follower report the leader without a second query.
//
// The takeover condition is "the lease is free, or it is already mine". The
// fence increments on exactly one of those: reclaiming an expired lease. That
// covers a different holder taking over, and equally the previous holder
// coming back to a lease it let lapse -- in between, anyone could have led, so
// downstream systems must see a new token either way. Extending a lease that
// never lapsed is the same term and keeps its token.
const acquireSQL = `
INSERT INTO %[1]s AS l (name, holder, fence, acquired_at, renewed_at, expires_at, payload)
VALUES ($1, $2, 1, now(), now(), now() + ($3::float8 * interval '1 microsecond'), $4)
ON CONFLICT (name) DO UPDATE SET
    holder      = CASE WHEN l.expires_at <= now() OR l.holder = excluded.holder
                       THEN excluded.holder      ELSE l.holder      END,
    fence       = CASE WHEN l.expires_at <= now()
                       THEN l.fence + 1          ELSE l.fence       END,
    acquired_at = CASE WHEN l.expires_at <= now()
                       THEN excluded.acquired_at ELSE l.acquired_at END,
    renewed_at  = CASE WHEN l.expires_at <= now() OR l.holder = excluded.holder
                       THEN excluded.renewed_at  ELSE l.renewed_at  END,
    expires_at  = CASE WHEN l.expires_at <= now() OR l.holder = excluded.holder
                       THEN excluded.expires_at  ELSE l.expires_at  END,
    payload     = CASE WHEN l.expires_at <= now() OR l.holder = excluded.holder
                       THEN excluded.payload     ELSE l.payload     END
RETURNING holder, fence, acquired_at, renewed_at, expires_at, payload, now()`

// renewSQL extends a lease the caller still holds.
//
// The guard is name, holder, fence and non-expiry together. Dropping the
// expiry check would let a leader that was partitioned long enough to be
// replaced resurrect its own term; dropping the fence check would let it
// renew the term of whoever replaced it.
const renewSQL = `
UPDATE %[1]s SET
    renewed_at = now(),
    expires_at = now() + ($4::float8 * interval '1 microsecond'),
    payload    = $5
WHERE name = $1 AND holder = $2 AND fence = $3 AND expires_at > now()
RETURNING holder, fence, acquired_at, renewed_at, expires_at, payload, now()`

// releaseSQL ends a term early so a successor need not wait out the lease.
//
// The row is expired in place rather than deleted, because the fence has to
// keep climbing across the handover and this row is what remembers it.
const releaseSQL = `
UPDATE %[1]s SET renewed_at = now(), expires_at = now()
WHERE name = $1 AND holder = $2 AND fence = $3 AND expires_at > now()`

// inspectSQL reads the election without writing to it. Followers use this so
// that a large fleet polling a live lease does not produce a row version per
// participant per poll.
const inspectSQL = `
SELECT holder, fence, acquired_at, renewed_at, expires_at, payload, now()
FROM %[1]s WHERE name = $1`

// schemaSQL is the initial DDL for the external migration process.
//
// fillfactor leaves room on the page for updated row versions. The row is
// rewritten every renewal -- several times a minute, forever -- and keeping
// the new version on the same page makes those updates HOT, so the primary
// key index is not touched and autovacuum can reclaim the old versions
// without an index scan.
const schemaSQL = `
CREATE TABLE IF NOT EXISTS %[1]s (
    name        text        PRIMARY KEY,
    holder      text        NOT NULL,
    fence       bigint      NOT NULL,
    acquired_at timestamptz NOT NULL,
    renewed_at  timestamptz NOT NULL,
    expires_at  timestamptz NOT NULL,
    payload     bytea
) WITH (fillfactor = 70)`

// Schema returns the initial DDL for an external migration tool.
// See [SchemaSQL] to use the same options and defaults as [New].
//
// schema may be empty to use the connection's search path. Both identifiers
// are validated, and Schema returns an error rather than emitting anything it
// could not validate.
func Schema(schema, table string) (string, error) {
	qualified, err := qualify(schema, table)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(fmt.Sprintf(schemaSQL, qualified)) + ";\n", nil
}

// identifierPattern is deliberately narrower than what Postgres accepts. A
// table name is the one part of these statements that cannot be a bind
// parameter, so it is the one place where a caller's string reaches the
// planner as syntax. Rejecting everything but the unambiguous case is worth
// more than accommodating exotic names.
var identifierPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_$]*$`)

// maxIdentifierLength is Postgres's NAMEDATALEN-1. Longer identifiers are
// silently truncated by the server, which would quietly point the queries at a
// different table than the caller named.
const maxIdentifierLength = 63

// qualify validates and quotes a table reference.
func qualify(schema, table string) (string, error) {
	if err := validateIdentifier("table", table); err != nil {
		return "", err
	}
	if schema == "" {
		return `"` + table + `"`, nil
	}
	if err := validateIdentifier("schema", schema); err != nil {
		return "", err
	}
	return `"` + schema + `"."` + table + `"`, nil
}

func validateIdentifier(kind, name string) error {
	switch {
	case name == "":
		return fmt.Errorf("leaderelection/postgres: %s name must not be empty", kind)
	case len(name) > maxIdentifierLength:
		return fmt.Errorf("leaderelection/postgres: %s name %q is longer than %d bytes and would be truncated by the server",
			kind, name, maxIdentifierLength)
	case !identifierPattern.MatchString(name):
		return fmt.Errorf("leaderelection/postgres: %s name %q must match %s",
			kind, name, identifierPattern)
	}
	return nil
}
