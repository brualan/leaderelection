package mysql

import (
	"context"
	"database/sql"
	sqldriver "database/sql/driver"
	"errors"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	driver "github.com/go-sql-driver/mysql"

	"github.com/brualan/leaderelection"
)

func mysqlErr(number uint16) error {
	return &driver.MySQLError{Number: number, Message: fmt.Sprintf("server error %d", number)}
}

// The classification is what an operator sees, and the two answers have to mean
// different things: retryable is "wait", fatal is "somebody has to do
// something". An error that is both would be alerted on and retried forever.
func TestClassification(t *testing.T) {
	for _, tc := range []struct {
		name      string
		number    uint16
		retryable bool
		fatal     bool
	}{
		{"too many connections", erConCount, true, false},
		{"server shutting down", erServerShutdown, true, false},
		{"aborted mid-connection", erAbortingConnection, true, false},
		{"lock wait timed out", erLockWaitTimeout, true, false},
		{"deadlock", erLockDeadlock, true, false},
		{"replica is read-only", erOptionPreventsStatement, true, false},
		{"super_read_only", erReadOnlyMode, true, false},
		{"session killed", erConnectionKilled, true, false},
		{"statement timed out", erQueryTimeout, true, false},
		{"proxysql cannot reach a backend", erProxySQLConnectTimeout, true, false},

		{"no such table", erNoSuchTable, false, true},
		{"unknown column", erBadField, false, true},
		{"unknown database", erBadDB, false, true},
		{"access denied", erAccessDenied, false, true},
		{"no privilege on the table", erTableAccessDenied, false, true},
		{"syntax error", erParse, false, true},
		{"identity longer than the column", erDataTooLong, false, true},

		// Neither is a valid answer: it means the package has nothing useful to
		// say, which is better than saying the wrong thing.
		{"something unrecognised", 1105, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := fmt.Errorf("leaderelection/mysql: acquire: %w", mysqlErr(tc.number))
			if got := Retryable(err); got != tc.retryable {
				t.Errorf("Retryable(%d) = %v, want %v", tc.number, got, tc.retryable)
			}
			if got := Fatal(err); got != tc.fatal {
				t.Errorf("Fatal(%d) = %v, want %v", tc.number, got, tc.fatal)
			}
		})
	}
}

// Every number the package names has to be reachable from one of the two
// answers, or it is dead weight that looks like coverage.
func TestEveryNamedNumberIsClassified(t *testing.T) {
	retryable := []uint16{
		erConCount, erOutOfResources, erHandshake, erServerShutdown, erAbortingConnection,
		erNetRead, erNetReadInterrupted, erNetErrorOnWrite, erNetWriteInterrupted,
		erTooManyUserConns, erUserLimitReached, erConnectionKilled, erServerOfflineMode,
		erLockWaitTimeout, erLockTableFull, erLockDeadlock,
		erOptionPreventsStatement, erQueryInterrupted, erReadOnlyMode, erQueryTimeout,
		erProxySQLConnectTimeout, erProxySQLConnectFailure,
	}
	fatal := []uint16{
		erDBAccessDenied, erAccessDenied, erAccessDeniedNoPassword, erBadNull, erBadDB,
		erBadField, erParse, erTooLongKey, erHostIsBlocked, erHostNotPrivileged,
		erTableAccessDenied, erColumnAccessDenied, erNoSuchTable, erNotSupportedAuthMode,
		erTruncatedWrongValueForField, erDataTooLong,
	}

	for _, number := range retryable {
		if !Retryable(mysqlErr(number)) || Fatal(mysqlErr(number)) {
			t.Errorf("%d is listed as retryable but classifies as retryable=%v fatal=%v",
				number, Retryable(mysqlErr(number)), Fatal(mysqlErr(number)))
		}
	}
	for _, number := range fatal {
		if !Fatal(mysqlErr(number)) || Retryable(mysqlErr(number)) {
			t.Errorf("%d is listed as fatal but classifies as retryable=%v fatal=%v",
				number, Retryable(mysqlErr(number)), Fatal(mysqlErr(number)))
		}
	}
}

// A connection that died carries no server error number. It is still the most
// common failure this package will ever see, and it is retryable.
func TestConnectionFailuresAreRetryable(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{"the driver gave up on the connection", sqldriver.ErrBadConn},
		{"the driver tore the connection down", driver.ErrInvalidConn},
		{"the network went away", &net.OpError{Op: "read", Err: errors.New("connection reset by peer")}},
		{"the stream ended early", io.ErrUnexpectedEOF},
		{"the server hung up", io.EOF},
		// context.DeadlineExceeded is itself a net.Error, so a call that ran
		// out of time lands in the same bucket as one whose socket did.
		{"the call ran out of time", context.DeadlineExceeded},
		{"the connection was handed back", sql.ErrConnDone},
	} {
		t.Run(tc.name, func(t *testing.T) {
			wrapped := fmt.Errorf("leaderelection/mysql: renew: %w", tc.err)
			if !Retryable(wrapped) {
				t.Fatalf("Retryable(%v) = false", tc.err)
			}
			if Fatal(wrapped) {
				t.Fatalf("Fatal(%v) = true", tc.err)
			}
		})
	}
}

// A cancelled context is the caller withdrawing the request, not the database
// failing. Whether to ask again is the caller's decision.
func TestCancellationIsNotClassified(t *testing.T) {
	err := fmt.Errorf("leaderelection/mysql: renew: %w", context.Canceled)
	if Retryable(err) {
		t.Fatal("Retryable(context.Canceled) = true")
	}
	if Fatal(err) {
		t.Fatal("Fatal(context.Canceled) = true")
	}
}

func TestNilAndUnknownErrors(t *testing.T) {
	if Retryable(nil) {
		t.Fatal("Retryable(nil) = true")
	}
	if Fatal(nil) {
		t.Fatal("Fatal(nil) = true")
	}
	other := errors.New("something else entirely")
	if Retryable(other) || Fatal(other) {
		t.Fatalf("an unrelated error was classified: retryable=%v fatal=%v", Retryable(other), Fatal(other))
	}
}

// The package wraps everything it returns, and a classification that only works
// on a bare driver error would be answering about errors nobody has.
func TestClassificationSurvivesWrapping(t *testing.T) {
	base := mysqlErr(erNoSuchTable)
	for depth, err := range []error{
		base,
		fmt.Errorf("a: %w", base),
		fmt.Errorf("b: %w", fmt.Errorf("a: %w", base)),
		errors.Join(errors.New("context"), base),
	} {
		if !Fatal(err) {
			t.Errorf("Fatal at wrapping depth %d = false", depth)
		}
		if Retryable(err) {
			t.Errorf("Retryable at wrapping depth %d = true", depth)
		}
	}
}

// wrap is a seam rather than a behaviour, and the test is here so that turning
// it into one is a visible change.
func TestWrapPassesThrough(t *testing.T) {
	if wrap(nil) != nil {
		t.Fatal("wrap(nil) is not nil")
	}
	if got := wrap(errBoom); !errors.Is(got, errBoom) {
		t.Fatalf("wrap(%v) = %v", errBoom, got)
	}
}

// A store over a handle that can reach nothing must report failure from every
// operation rather than inventing a lease -- and must say so in a way an
// operator can act on.
func TestOperationsFailWhenNoConnectionCanBeMade(t *testing.T) {
	ctx := context.Background()
	store := newTestStore(t, &fakeDB{connectErr: driver.ErrInvalidConn})

	if lease, err := store.Acquire(ctx, "e", "a", time.Minute, nil); err == nil && lease.HeldBy("a") {
		t.Fatal("Acquire reported a held lease with no connection")
	} else if !Retryable(err) {
		t.Fatalf("Retryable(%v) = false", err)
	}
	if _, err := store.Inspect(ctx, "e"); err == nil {
		t.Fatal("Inspect succeeded with no connection")
	}
	if _, err := store.Renew(ctx, "e", "a", 1, time.Minute, nil); err == nil {
		t.Fatal("Renew succeeded with no connection")
	} else if errors.Is(err, leaderelection.ErrLeaseLost) {
		t.Fatal("an unreachable database was reported as a lost lease")
	}
	if err := store.Release(ctx, "e", "a", 1); err == nil {
		t.Fatal("Release succeeded with no connection")
	}
}
