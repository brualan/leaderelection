package mysql

import (
	"fmt"
	"regexp"
	"strings"
)

// Every statement below obeys three rules, and breaking any of them breaks a
// supported deployment:
//
// One statement per write. A pooler such as ProxySQL is free to hand the next
// statement to a different backend, so nothing that mutates the election may
// span two of them. A single autocommit statement is also the only shape that
// cannot leave a half-applied election behind.
//
// All time from the server. UTC_TIMESTAMP(6) is fixed for the duration of one
// statement, so every reference to it inside one statement returns the same
// instant, and every timestamp the store reports -- including the caller's view
// of "now" -- comes from one clock read. Participants in other clusters
// therefore never compare their own clock against anyone else's. It is the UTC
// form rather than NOW(6) because NOW(6) is rendered in the session's time
// zone, and a fleet whose connections do not all agree on that would be
// comparing timestamps from two different clocks without knowing it.
//
// No session state. No user variables, no SET, no GET_LOCK, no temporary
// tables, no transactions. The statements work identically on a direct
// connection and through a pooler.

// MySQL has no RETURNING, so Acquire and Renew are a write followed by
// [inspectSQL]. That is a real difference from the PostgreSQL store and it is
// worth being precise about why it is still safe.
//
// The write is what changes the election, and it is one statement, so the
// election is never half-changed. The read that follows is a fresh snapshot of
// the row and of the server clock taken together. The store reports a lease as
// held only when that snapshot says so -- never because the write reported
// success -- so the worst a race between the two can do is show a term that has
// already ended, which resolves to "not the leader". Uncertainty resolving that
// way is the whole design; resolving it the other way is the bug this ordering
// makes unreachable.

// acquireSQL starts or continues a leadership term.
//
// The takeover condition is "the lease is free, or it is already mine". The
// fence increments on exactly one of those: reclaiming an expired lease. That
// covers a different holder taking over, and equally the previous holder coming
// back to a lease it let lapse -- in between, anyone could have led, so
// downstream systems must see a new token either way. Extending a lease that
// never lapsed is the same term and keeps its token.
//
// The order of the assignments is load-bearing, and this is the one place where
// this package's SQL cannot be read as if it were PostgreSQL's. MySQL evaluates
// the assignments in an UPDATE -- including the one behind ON DUPLICATE KEY
// UPDATE -- from left to right, and a later assignment sees the value an
// earlier one has already written. So:
//
//   - fence and acquired_at come first, while expires_at still holds the value
//     the row had on disk. Their condition is "the lease had lapsed".
//   - holder comes next, still reading the old expires_at and the old holder.
//   - renewed_at, payload and expires_at come last, and their condition is the
//     *new* holder. That is not a shortcut: the new holder equals this caller's
//     identity in exactly the cases where the takeover condition held, and
//     equals somebody else's in exactly the cases where it did not. Testing one
//     already-written column is what keeps the last three assignments from
//     needing a value that has been overwritten.
//
// Moving any assignment above changes what the statement means. Reordering them
// into PostgreSQL's arrangement, where every reference is to the old row, makes
// a takeover from a different holder stop updating the expiry -- which the
// acceptance suite fails on, loudly, rather than silently electing nobody.
const acquireSQL = `
INSERT INTO %[1]s (name, holder, fence, acquired_at, renewed_at, expires_at, payload)
VALUES (?, ?, 1, UTC_TIMESTAMP(6), UTC_TIMESTAMP(6), UTC_TIMESTAMP(6) + INTERVAL ? MICROSECOND, ?)
ON DUPLICATE KEY UPDATE
    fence       = CASE WHEN expires_at <= UTC_TIMESTAMP(6)
                       THEN fence + 1        ELSE fence       END,
    acquired_at = CASE WHEN expires_at <= UTC_TIMESTAMP(6)
                       THEN UTC_TIMESTAMP(6) ELSE acquired_at END,
    holder      = CASE WHEN expires_at <= UTC_TIMESTAMP(6) OR holder = ?
                       THEN ?                ELSE holder      END,
    renewed_at  = CASE WHEN holder = ?
                       THEN UTC_TIMESTAMP(6) ELSE renewed_at  END,
    payload     = CASE WHEN holder = ?
                       THEN ?                ELSE payload     END,
    expires_at  = CASE WHEN holder = ?
                       THEN UTC_TIMESTAMP(6) + INTERVAL ? MICROSECOND ELSE expires_at END`

// renewSQL extends a lease the caller still holds.
//
// The guard is name, holder, fence and non-expiry together. Dropping the expiry
// check would let a leader that was partitioned long enough to be replaced
// resurrect its own term; dropping the fence check would let it renew the term
// of whoever replaced it. Unlike the assignments above, a WHERE clause is
// evaluated against the row as it was before any assignment ran, so nothing
// here depends on ordering.
const renewSQL = `
UPDATE %[1]s SET
    renewed_at = UTC_TIMESTAMP(6),
    expires_at = UTC_TIMESTAMP(6) + INTERVAL ? MICROSECOND,
    payload    = ?
WHERE name = ? AND holder = ? AND fence = ? AND expires_at > UTC_TIMESTAMP(6)`

// releaseSQL ends a term early so a successor need not wait out the lease.
//
// The row is expired in place rather than deleted, because the fence has to
// keep climbing across the handover and this row is what remembers it.
const releaseSQL = `
UPDATE %[1]s SET renewed_at = UTC_TIMESTAMP(6), expires_at = UTC_TIMESTAMP(6)
WHERE name = ? AND holder = ? AND fence = ? AND expires_at > UTC_TIMESTAMP(6)`

// inspectSQL reads the election without writing to it. Followers use this so
// that a large fleet polling a live lease does not produce a row version per
// participant per poll, and Acquire and Renew use it to read back what they
// wrote, because MySQL cannot return it from the write itself.
const inspectSQL = `
SELECT holder, fence, acquired_at, renewed_at, expires_at, payload, UTC_TIMESTAMP(6)
FROM %[1]s WHERE name = ?`

// schemaSQL is the initial DDL for the external migration process.
//
// Three choices in here are not cosmetic.
//
// name and holder are VARBINARY. An identity is opaque bytes -- it comes from a
// hostname, a pod name, a container id -- and two identities that differ must
// never compare equal. MySQL's default collations are case- and
// accent-insensitive and pad trailing spaces, so a text column would make
// "worker-A" and "worker-a " the same participant, and the guard on every
// renewal would be comparing something looser than identity. Binary columns
// compare byte for byte, which is the only comparison this design can use.
//
// The timestamps are DATETIME(6) rather than TIMESTAMP. TIMESTAMP is stored as
// UTC and converted on the way in and out using the session's time zone, so a
// fleet whose sessions do not agree on that would read back values shifted
// against each other. DATETIME stores exactly what UTC_TIMESTAMP(6) produced.
// The (6) is microseconds: whole-second resolution would round a sub-second
// lease into nothing.
//
// There is no fill-factor knob because InnoDB does not need one. The table is
// the clustered primary key index, so rewriting the row on every renewal
// touches no secondary index, and a row whose length does not change is updated
// where it lies.
const schemaSQL = `
CREATE TABLE IF NOT EXISTS %[1]s (
    name        VARBINARY(255) NOT NULL,
    holder      VARBINARY(255) NOT NULL,
    fence       BIGINT         NOT NULL,
    acquired_at DATETIME(6)    NOT NULL,
    renewed_at  DATETIME(6)    NOT NULL,
    expires_at  DATETIME(6)    NOT NULL,
    payload     BLOB           NULL,
    PRIMARY KEY (name)
) ENGINE=InnoDB`

// Schema returns the initial DDL for an external migration tool.
// See [SchemaSQL] to use the same options and defaults as [New].
//
// database may be empty to use the connection's default schema. Both
// identifiers are validated, and Schema returns an error rather than emitting
// anything it could not validate.
func Schema(database, table string) (string, error) {
	qualified, err := qualify(database, table)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(fmt.Sprintf(schemaSQL, qualified)) + ";\n", nil
}

// identifierPattern is deliberately narrower than what MySQL accepts. A table
// name is the one part of these statements that cannot be a bind parameter, so
// it is the one place where a caller's string reaches the parser as syntax.
// Rejecting everything but the unambiguous case is worth more than
// accommodating exotic names.
var identifierPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_$]*$`)

// maxIdentifierLength is MySQL's limit for a table or schema name. Anything
// longer is an error rather than a silent truncation, but rejecting it here
// means the caller learns at construction rather than on the first query.
const maxIdentifierLength = 64

// qualify validates and quotes a table reference.
func qualify(database, table string) (string, error) {
	if err := validateIdentifier("table", table); err != nil {
		return "", err
	}
	if database == "" {
		return "`" + table + "`", nil
	}
	if err := validateIdentifier("database", database); err != nil {
		return "", err
	}
	return "`" + database + "`.`" + table + "`", nil
}

func validateIdentifier(kind, name string) error {
	switch {
	case name == "":
		return fmt.Errorf("leaderelection/mysql: %s name must not be empty", kind)
	case len(name) > maxIdentifierLength:
		return fmt.Errorf("leaderelection/mysql: %s name %q is longer than the %d bytes MySQL allows",
			kind, name, maxIdentifierLength)
	case !identifierPattern.MatchString(name):
		return fmt.Errorf("leaderelection/mysql: %s name %q must match %s",
			kind, name, identifierPattern)
	}
	return nil
}
