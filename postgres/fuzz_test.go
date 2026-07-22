package postgres

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

// The table name is the one part of these statements that cannot be a bind
// parameter: it reaches the planner as syntax. Everything else about the store
// is guarded by the protocol, and this is not, so it gets a fuzzer.

// FuzzQualify asserts that no string qualify accepts can be anything other
// than a plain quoted identifier.
//
// The check that matters is not "does it reject the obvious attack" -- a table
// argument comes from the application's own configuration, not from a request
// -- but "is the output still one identifier". A name carrying a quote, a dot
// or a newline would change the shape of every statement in the package, and
// the statements are built by formatting rather than by parsing, so nothing
// downstream would notice.
func FuzzQualify(f *testing.F) {
	f.Add("", "leader_election")
	f.Add("public", "leader_election")
	f.Add("", `le"; DROP TABLE users; --`)
	f.Add("pg_catalog", "pg_class")
	f.Add("", strings.Repeat("x", 64))
	f.Add("", "läse")
	f.Add("", "0leading")
	f.Add("", "with space")

	f.Fuzz(func(t *testing.T, schema, table string) {
		qualified, err := qualify(schema, table)
		if err != nil {
			// Rejection is always a safe answer: the caller gets an error
			// instead of a statement.
			if qualified != "" {
				t.Fatalf("qualify returned %q alongside an error", qualified)
			}
			return
		}

		// What came back must be one quoted identifier, or two joined by a
		// single dot -- and nothing that could end the identifier early or
		// start a second statement.
		parts := strings.Split(qualified, ".")
		if len(parts) != 1 && len(parts) != 2 {
			t.Fatalf("qualify(%q, %q) = %q, want one or two parts", schema, table, qualified)
		}
		for _, part := range parts {
			unquoted, ok := strings.CutPrefix(part, `"`)
			if !ok {
				t.Fatalf("qualify(%q, %q) = %q: part %q is not quoted", schema, table, qualified, part)
			}
			unquoted, ok = strings.CutSuffix(unquoted, `"`)
			if !ok {
				t.Fatalf("qualify(%q, %q) = %q: part %q is not closed", schema, table, qualified, part)
			}
			if strings.ContainsAny(unquoted, "\"';\\\n\r\t ") {
				t.Fatalf("qualify(%q, %q) = %q: %q would not survive being quoted",
					schema, table, qualified, unquoted)
			}
			if len(unquoted) == 0 || len(unquoted) > maxIdentifierLength {
				t.Fatalf("qualify(%q, %q) = %q: %q is %d bytes, which the server would truncate",
					schema, table, qualified, unquoted, len(unquoted))
			}
		}

		// And the statements built from it must still be single statements,
		// which is what the transaction pooler requires of every one of them.
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
// Fatal means "nobody will lead until a human fixes something". An error that
// is both would be alerted on and retried forever; one that is neither is
// merely uninformative, which is allowed.
func FuzzClassification(f *testing.F) {
	f.Add("08006", "connection failure", 0)
	f.Add("42P01", "undefined table", 2)
	f.Add("", "", 1)
	f.Add("XX000", "internal error", 3)
	f.Add("57014", "canceled", 4)

	f.Fuzz(func(t *testing.T, code, message string, wrapping int) {
		var err error = &pgconn.PgError{Code: code, Message: message}

		// The package wraps what it returns, sometimes more than once, and the
		// classification is only worth anything if it survives that.
		for range wrapping % 5 {
			err = fmt.Errorf("leaderelection/postgres: %w", err)
		}

		retryable, fatal := Retryable(err), Fatal(err)
		if retryable && fatal {
			t.Fatalf("SQLSTATE %q is classified as both retryable and fatal", code)
		}

		// Wrapping must not change the answer, whichever answer it is.
		if got := Retryable(errors.Join(errors.New("context"), err)); got != retryable {
			t.Fatalf("SQLSTATE %q: Retryable = %v wrapped, %v unwrapped", code, got, retryable)
		}
		if got := Fatal(errors.Join(errors.New("context"), err)); got != fatal {
			t.Fatalf("SQLSTATE %q: Fatal = %v wrapped, %v unwrapped", code, got, fatal)
		}
	})
}
