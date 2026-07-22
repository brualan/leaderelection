package postgres

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/puddle/v2"
)

func pgErr(code string) error { return &pgconn.PgError{Code: code, Message: "test"} }

func TestRetryable(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},

		// A pooler restarting, or a server closing the connection, is the
		// single most common failure in a deployment with PgBouncer in front.
		{"connection exception", pgErr(sqlStateConnectionException), true},
		{"connection failure", pgErr(sqlStateConnectionFailure), true},
		{"connection does not exist", pgErr(sqlStateConnectionDoesNotExist), true},
		{"protocol violation", pgErr(sqlStateProtocolViolation), true},

		// Two participants racing for the same row.
		{"serialization failure", pgErr(sqlStateSerializationFailure), true},
		{"deadlock", pgErr(sqlStateDeadlockDetected), true},

		// The pooler has no free server connection.
		{"too many connections", pgErr(sqlStateTooManyConnections), true},

		// A failover or an administrator bouncing the server.
		{"admin shutdown", pgErr(sqlStateAdminShutdown), true},
		{"crash shutdown", pgErr(sqlStateCrashShutdown), true},
		{"cannot connect now", pgErr(sqlStateCannotConnectNow), true},
		{"query canceled", pgErr(sqlStateQueryCanceled), true},
		{"read-only transaction", pgErr(sqlStateReadOnlySQLTxn), true},

		// These will still be broken on the next attempt.
		{"undefined table", pgErr(sqlStateUndefinedTable), false},
		{"insufficient privilege", pgErr(sqlStateInsufficientPrivilege), false},
		{"syntax error", pgErr(sqlStateSyntaxError), false},
		{"unknown code", pgErr("XX000"), false},

		// Transport-level failures carry no SQLSTATE.
		{"connect error", &pgconn.ConnectError{Config: &pgconn.Config{}}, true},
		{"network error", &net.OpError{Op: "read", Err: errors.New("connection reset by peer")}, true},
		{"unexpected EOF", io.ErrUnexpectedEOF, true},
		{"closed pool", puddle.ErrClosedPool, true},
		{"a message that merely reads like a closed pool", errors.New("closed pool"), false},
		{"wrapped network error", fmt.Errorf("renew: %w", &net.OpError{Op: "read", Err: errors.New("broken pipe")}), true},

		{"unrelated", errors.New("something else"), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := Retryable(tc.err); got != tc.want {
				t.Fatalf("Retryable(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

// An error that never reached the server is retryable by construction, and pgx
// says so through its own interface rather than through a code.
func TestRetryableTrustsSafeToRetry(t *testing.T) {
	err := safeToRetryError{}
	if !pgconn.SafeToRetry(err) {
		t.Fatal("test fixture does not satisfy pgconn.SafeToRetry")
	}
	if !Retryable(err) {
		t.Fatal("Retryable ignored SafeToRetry")
	}
}

type safeToRetryError struct{}

func (safeToRetryError) Error() string     { return "never left the client" }
func (safeToRetryError) SafeToRetry() bool { return true }

func TestFatal(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},

		// Somebody forgot the migration.
		{"undefined table", pgErr(sqlStateUndefinedTable), true},
		{"undefined column", pgErr(sqlStateUndefinedColumn), true},
		{"undefined schema", pgErr(sqlStateUndefinedSchema), true},
		{"datatype mismatch", pgErr(sqlStateDatatypeMismatch), true},

		// The application is not allowed to do what it is trying to do.
		{"insufficient privilege", pgErr(sqlStateInsufficientPrivilege), true},
		{"invalid password", pgErr(sqlStateInvalidPassword), true},
		{"invalid authorization", pgErr(sqlStateInvalidAuthorization), true},
		{"syntax error", pgErr(sqlStateSyntaxError), true},

		// These clear on their own.
		{"deadlock", pgErr(sqlStateDeadlockDetected), false},
		{"admin shutdown", pgErr(sqlStateAdminShutdown), false},
		{"transport failure", &net.OpError{Op: "dial", Err: errors.New("refused")}, false},
		{"unrelated", errors.New("something else"), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := Fatal(tc.err); got != tc.want {
				t.Fatalf("Fatal(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

// The two classifications must not overlap. An error that is both would leave
// an operator with no idea whether to wait or to act.
func TestClassificationsAreDisjoint(t *testing.T) {
	codes := []string{
		sqlStateConnectionException, sqlStateConnectionDoesNotExist, sqlStateConnectionFailure,
		sqlStateSQLClientUnableToEstablish, sqlStateSQLServerRejectedEstablish,
		sqlStateTransactionResolutionUnknown, sqlStateProtocolViolation,
		sqlStateSerializationFailure, sqlStateDeadlockDetected,
		sqlStateTooManyConnections, sqlStateOutOfMemory, sqlStateConfigLimitReached,
		sqlStateLockNotAvailable, sqlStateQueryCanceled, sqlStateAdminShutdown,
		sqlStateCrashShutdown, sqlStateCannotConnectNow, sqlStateDatabaseDropped,
		sqlStateIdleSessionTimeout, sqlStateReadOnlySQLTxn,
		sqlStateSyntaxError, sqlStateInsufficientPrivilege, sqlStateUndefinedTable,
		sqlStateUndefinedColumn, sqlStateUndefinedSchema, sqlStateDatatypeMismatch,
		sqlStateInvalidPassword, sqlStateInvalidAuthorization,
	}
	for _, code := range codes {
		err := pgErr(code)
		if Retryable(err) && Fatal(err) {
			t.Fatalf("SQLSTATE %s is classified as both retryable and fatal", code)
		}
	}
}

// wrap is a seam, not a transformation: an error must survive it intact so
// that errors.Is and errors.As keep working for the caller.
func TestWrapPreservesTheError(t *testing.T) {
	sentinel := errors.New("sentinel")
	if got := wrap(sentinel); !errors.Is(got, sentinel) {
		t.Fatalf("wrap(%v) = %v", sentinel, got)
	}
	if wrap(nil) != nil {
		t.Fatal("wrap(nil) is not nil")
	}
}

// Classification has to work on errors in the shape this package returns them,
// which is always wrapped. A check that only recognises a bare sentinel is a
// check that never fires in production.
func TestClassificationSurvivesWrapping(t *testing.T) {
	for _, tc := range []struct {
		name      string
		err       error
		retryable bool
		fatal     bool
	}{
		{"closed pool", puddle.ErrClosedPool, true, false},
		{"admin shutdown", pgErr(sqlStateAdminShutdown), true, false},
		{"undefined table", pgErr(sqlStateUndefinedTable), false, true},
		{"insufficient privilege", pgErr(sqlStateInsufficientPrivilege), false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Exactly how Store wraps a driver error before returning it.
			wrapped := fmt.Errorf("leaderelection/postgres: inspect: %w", tc.err)
			if got := Retryable(wrapped); got != tc.retryable {
				t.Fatalf("Retryable(wrapped) = %v, want %v", got, tc.retryable)
			}
			if got := Fatal(wrapped); got != tc.fatal {
				t.Fatalf("Fatal(wrapped) = %v, want %v", got, tc.fatal)
			}
			// Doubly wrapped, as an application layering its own context would
			// produce.
			twice := fmt.Errorf("reconciler: %w", wrapped)
			if got := Retryable(twice); got != tc.retryable {
				t.Fatalf("Retryable(doubly wrapped) = %v, want %v", got, tc.retryable)
			}
		})
	}
}

// A call that ran out of time is retryable: the operation timeout firing says
// nothing about whether the database is healthy, only that this attempt is
// over.
func TestRetryableCoversTimeouts(t *testing.T) {
	for _, err := range []error{
		context.DeadlineExceeded,
		fmt.Errorf("leaderelection/postgres: renew: %w", context.DeadlineExceeded),
	} {
		if !Retryable(err) {
			t.Fatalf("Retryable(%v) = false, want true", err)
		}
		if Fatal(err) {
			t.Fatalf("Fatal(%v) = true, want false", err)
		}
	}
}
