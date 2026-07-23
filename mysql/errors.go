package mysql

import (
	"database/sql"
	sqldriver "database/sql/driver"
	"errors"
	"io"
	"net"

	driver "github.com/go-sql-driver/mysql"
)

// Error classification exists for the operator, not for the election loop. The
// loop treats every failure the same way -- it stops believing it is the
// leader -- because an outage it misdiagnoses as recoverable is exactly how two
// leaders happen. What the classification changes is what a human sees: "the
// table is missing" and "ProxySQL cannot reach a backend" both stop the
// election, but only one of them will fix itself.

// Retryable reports whether err is a transient condition that is expected to
// clear on its own: a dropped connection, a pooler cycling, a deadlock, a lock
// wait that timed out, a server being shut down or promoted, a call that ran
// out of time.
//
// A retryable error means the operation may be attempted again. It never means
// the caller may assume it is still the leader.
//
// Errors are classified as this package returns them, which is wrapped. Every
// check here goes through errors.Is or errors.As for that reason.
func Retryable(err error) bool {
	if err == nil {
		return false
	}
	var myErr *driver.MySQLError
	if errors.As(err, &myErr) {
		return retryableNumber(myErr.Number)
	}
	// A connection that died mid-flight carries no error number. The statement
	// may or may not have run, which is precisely why the election treats an
	// unknown outcome as "not leader" rather than guessing.
	return isConnectionError(err)
}

// Fatal reports whether err describes a deployment problem that will not
// resolve without intervention: a missing table, missing privileges, a schema
// that does not match, credentials the server will not accept.
//
// It is the signal worth alerting on. A fatal error means this participant will
// never win an election until somebody fixes something.
func Fatal(err error) bool {
	var myErr *driver.MySQLError
	if !errors.As(err, &myErr) {
		return false
	}
	switch myErr.Number {
	case erDBAccessDenied,
		erAccessDenied,
		erAccessDeniedNoPassword,
		erBadNull,
		erBadDB,
		erBadField,
		erParse,
		erTooLongKey,
		erHostIsBlocked,
		erHostNotPrivileged,
		erTableAccessDenied,
		erColumnAccessDenied,
		erNoSuchTable,
		erNotSupportedAuthMode,
		erTruncatedWrongValueForField,
		erDataTooLong:
		return true
	}
	return false
}

// Server error numbers this package reasons about. They are spelled out rather
// than pulled from a dependency so that the classification is auditable against
// the MySQL server error reference.
const (
	// Resources and connection handling. A pooler with no free backend, a
	// server at max_connections, a network read that died halfway.
	erConCount            = 1040
	erOutOfResources      = 1041
	erHandshake           = 1043
	erServerShutdown      = 1053
	erAbortingConnection  = 1152
	erNetRead             = 1158
	erNetReadInterrupted  = 1159
	erNetErrorOnWrite     = 1160
	erNetWriteInterrupted = 1161
	erTooManyUserConns    = 1203
	erUserLimitReached    = 1226
	erConnectionKilled    = 1927
	erServerOfflineMode   = 3032

	// Locking. Two participants racing for the same row, or a migration holding
	// the table while the election waits behind it.
	erLockWaitTimeout = 1205
	erLockTableFull   = 1206
	erLockDeadlock    = 1213

	// Operator intervention. A promoted replica, a session killed by hand, a
	// statement that hit max_execution_time.
	erOptionPreventsStatement = 1290
	erQueryInterrupted        = 1317
	erReadOnlyMode            = 1836
	erQueryTimeout            = 3024

	// ProxySQL's own numbers for "I could not reach a backend". They arrive
	// looking like server errors because that is the only channel a pooler has,
	// and they mean exactly what a connection failure means.
	erProxySQLConnectTimeout = 9001
	erProxySQLConnectFailure = 9002

	// The deployment is wrong.
	erDBAccessDenied              = 1044
	erAccessDenied                = 1045
	erBadNull                     = 1048
	erBadDB                       = 1049
	erBadField                    = 1054
	erParse                       = 1064
	erTooLongKey                  = 1071
	erHostIsBlocked               = 1129
	erHostNotPrivileged           = 1130
	erTableAccessDenied           = 1142
	erColumnAccessDenied          = 1143
	erNoSuchTable                 = 1146
	erNotSupportedAuthMode        = 1251
	erTruncatedWrongValueForField = 1366
	erDataTooLong                 = 1406
	erAccessDeniedNoPassword      = 1698
)

func retryableNumber(number uint16) bool {
	switch number {
	case erConCount,
		erOutOfResources,
		erHandshake,
		erServerShutdown,
		erAbortingConnection,
		erNetRead,
		erNetReadInterrupted,
		erNetErrorOnWrite,
		erNetWriteInterrupted,
		erTooManyUserConns,
		erUserLimitReached,
		erConnectionKilled,
		erServerOfflineMode,
		erLockWaitTimeout,
		erLockTableFull,
		erLockDeadlock,
		erOptionPreventsStatement,
		erQueryInterrupted,
		erReadOnlyMode,
		erQueryTimeout,
		erProxySQLConnectTimeout,
		erProxySQLConnectFailure:
		return true
	}
	return false
}

// isConnectionError recognises the transport-level failures the driver reports
// without a server error number. This is what a ProxySQL restart or a severed
// network path looks like from the client: no error number, just a connection
// that is no longer there.
func isConnectionError(err error) bool {
	// database/sql surfaces ErrBadConn only after it has already given up
	// retrying on a fresh connection, and the driver returns ErrInvalidConn for
	// a connection it has torn down itself.
	if errors.Is(err, sqldriver.ErrBadConn) || errors.Is(err, driver.ErrInvalidConn) {
		return true
	}
	// The network went away underneath an established connection: a severed
	// route, a pooler killed mid-query, a timeout on the socket. This also
	// catches a call that ran out of time, because context.DeadlineExceeded is
	// itself a net.Error. It does not catch context.Canceled, which is right:
	// that one is the caller withdrawing the request, and whether to ask again
	// is the caller's decision rather than a property of the database.
	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}
	if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) {
		return true
	}
	// The connection was handed back to the pool underneath an in-flight
	// operation.
	return errors.Is(err, sql.ErrConnDone)
}

// wrap passes an error through unchanged. It marks the places where a driver
// error crosses into the store's own errors, and is the single point to change
// if that ever needs more than pass-through.
func wrap(err error) error { return err }
