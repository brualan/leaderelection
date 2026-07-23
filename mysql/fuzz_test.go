package mysql

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	driver "github.com/go-sql-driver/mysql"
)

// The table name is the one part of these statements that cannot be a bind
// parameter: it reaches the parser as syntax. Everything else about the store
// is guarded by the protocol, and this is not, so it gets a fuzzer.

// FuzzQualify asserts that no string qualify accepts can be anything other than
// a plain backquoted identifier.
//
// The check that matters is not "does it reject the obvious attack" -- a table
// argument comes from the application's own configuration, not from a request
// -- but "is the output still one identifier". A name carrying a backquote, a
// dot or a newline would change the shape of every statement in the package,
// and the statements are built by formatting rather than by parsing, so nothing
// downstream would notice.
func FuzzQualify(f *testing.F) {
	f.Add("", "leader_election")
	f.Add("app", "leader_election")
	f.Add("", "le`; DROP TABLE users; -- ")
	f.Add("mysql", "user")
	f.Add("", strings.Repeat("x", 65))
	f.Add("", "läse")
	f.Add("", "0leading")
	f.Add("", "with space")

	f.Fuzz(func(t *testing.T, database, table string) {
		qualified, err := qualify(database, table)
		if err != nil {
			// Rejection is always a safe answer: the caller gets an error
			// instead of a statement.
			if qualified != "" {
				t.Fatalf("qualify returned %q alongside an error", qualified)
			}
			return
		}

		// What came back must be one backquoted identifier, or two joined by a
		// single dot -- and nothing that could end the identifier early or start
		// a second statement.
		parts := strings.Split(qualified, ".")
		if len(parts) != 1 && len(parts) != 2 {
			t.Fatalf("qualify(%q, %q) = %q, want one or two parts", database, table, qualified)
		}
		for _, part := range parts {
			unquoted, ok := strings.CutPrefix(part, "`")
			if !ok {
				t.Fatalf("qualify(%q, %q) = %q: part %q is not quoted", database, table, qualified, part)
			}
			unquoted, ok = strings.CutSuffix(unquoted, "`")
			if !ok {
				t.Fatalf("qualify(%q, %q) = %q: part %q is not closed", database, table, qualified, part)
			}
			if strings.ContainsAny(unquoted, "`'\";\\\n\r\t ") {
				t.Fatalf("qualify(%q, %q) = %q: %q would not survive being quoted",
					database, table, qualified, unquoted)
			}
			if len(unquoted) == 0 || len(unquoted) > maxIdentifierLength {
				t.Fatalf("qualify(%q, %q) = %q: %q is %d bytes, which MySQL will not accept",
					database, table, qualified, unquoted, len(unquoted))
			}
		}

		// And the statements built from it must still be single statements,
		// which is what a pooler requires of every write.
		for name, statement := range map[string]string{
			"acquire": fmt.Sprintf(acquireSQL, qualified),
			"renew":   fmt.Sprintf(renewSQL, qualified),
			"release": fmt.Sprintf(releaseSQL, qualified),
			"inspect": fmt.Sprintf(inspectSQL, qualified),
		} {
			if strings.Contains(statement, ";") {
				t.Fatalf("%s statement built with %q contains a terminator:\n%s",
					name, qualified, statement)
			}
		}
	})
}

// FuzzClassification asserts that the two things the package says about an
// error stay mutually exclusive whatever the error is.
//
// Retryable means "try again, and do not assume you are still the leader".
// Fatal means "nobody will lead until a human fixes something". An error that is
// both would be alerted on and retried forever; one that is neither is merely
// uninformative, which is allowed.
func FuzzClassification(f *testing.F) {
	f.Add(uint16(1213), "deadlock found", 0)
	f.Add(uint16(1146), "table doesn't exist", 2)
	f.Add(uint16(0), "", 1)
	f.Add(uint16(1105), "unknown error", 3)
	f.Add(uint16(1317), "query execution was interrupted", 4)

	f.Fuzz(func(t *testing.T, number uint16, message string, wrapping int) {
		var err error = &driver.MySQLError{Number: number, Message: message}

		// The package wraps what it returns, sometimes more than once, and the
		// classification is only worth anything if it survives that.
		for range wrapping % 5 {
			err = fmt.Errorf("leaderelection/mysql: %w", err)
		}

		retryable, fatal := Retryable(err), Fatal(err)
		if retryable && fatal {
			t.Fatalf("server error %d is classified as both retryable and fatal", number)
		}

		// Wrapping must not change the answer, whichever answer it is.
		if got := Retryable(errors.Join(errors.New("context"), err)); got != retryable {
			t.Fatalf("server error %d: Retryable = %v wrapped, %v unwrapped", number, got, retryable)
		}
		if got := Fatal(errors.Join(errors.New("context"), err)); got != fatal {
			t.Fatalf("server error %d: Fatal = %v wrapped, %v unwrapped", number, got, fatal)
		}
	})
}

// FuzzTimestampScanning asserts that a timestamp is either read exactly or
// refused.
//
// The failure this rules out is quiet: a value the scanner half-understood
// becomes a Lease with a plausible-looking clock, and the elector then computes
// a TTL from it. A lease that reads as longer than it is, in a store whose whole
// job is to say when a lease ends, is the one bug that ends with two leaders.
func FuzzTimestampScanning(f *testing.F) {
	f.Add("2026-07-22 12:00:00.000000")
	f.Add("2026-07-22 12:00:00")
	f.Add("0000-00-00 00:00:00")
	f.Add("2026-07-22T12:00:00Z")
	f.Add("")
	f.Add("2026-07-22 12:00:00.123456789")

	f.Fuzz(func(t *testing.T, raw string) {
		var fromString, fromBytes dbTime
		errString := fromString.Scan(raw)
		errBytes := fromBytes.Scan([]byte(raw))

		// Which of the two shapes the driver hands over depends on a DSN
		// parameter the application chose, so they must not disagree.
		if (errString == nil) != (errBytes == nil) {
			t.Fatalf("%q scanned as a string (%v) and as bytes (%v) differently", raw, errString, errBytes)
		}
		if errString != nil {
			return
		}
		if !fromString.t.Equal(fromBytes.t) {
			t.Fatalf("%q scanned to %v as a string and %v as bytes", raw, fromString.t, fromBytes.t)
		}

		// Whatever was accepted must be a UTC instant that round-trips, because
		// every duration the election computes is a difference between two of
		// these.
		if _, offset := fromString.t.Zone(); offset != 0 {
			t.Fatalf("%q scanned to the non-UTC %v", raw, fromString.t)
		}
		var again dbTime
		if err := again.Scan(fromString.t.Format(datetimeLayout)); err != nil {
			t.Fatalf("%q scanned to %v, which does not scan again: %v", raw, fromString.t, err)
		}
		if !again.t.Equal(fromString.t.Truncate(time.Microsecond)) {
			t.Fatalf("%q does not round-trip: %v then %v", raw, fromString.t, again.t)
		}
	})
}
