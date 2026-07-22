package postgres

import (
	"errors"
	"io"
	"net"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/puddle/v2"
)

// Error classification exists for the operator, not for the election loop. The
// loop treats every failure the same way -- it stops believing it is the
// leader -- because an outage it misdiagnoses as recoverable is exactly how
// two leaders happen. What the classification changes is what a human sees:
// "the table is missing" and "PgBouncer is restarting" both stop the election,
// but only one of them will fix itself.

// Retryable reports whether err is a transient condition that is expected to
// clear on its own: a dropped connection, a pooler cycling, a deadlock, a
// serialization conflict, an administrator bouncing the server, a call that
// ran out of time.
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
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return retryableSQLState(pgErr.Code)
	}
	// Anything that never reached the server is retryable by construction.
	if pgconn.SafeToRetry(err) {
		return true
	}
	// A connection that died mid-flight reports no SQLSTATE. The statement may
	// or may not have committed, which is precisely why the election treats an
	// unknown outcome as "not leader" rather than guessing.
	return isConnectionError(err)
}

// Fatal reports whether err describes a deployment problem that will not
// resolve without intervention: a missing table, missing privileges, a schema
// that does not match.
//
// It is the signal worth alerting on. A fatal error means this participant
// will never win an election until somebody fixes something.
func Fatal(err error) bool {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return false
	}
	switch pgErr.Code {
	case sqlStateUndefinedTable,
		sqlStateUndefinedColumn,
		sqlStateUndefinedSchema,
		sqlStateInsufficientPrivilege,
		sqlStateSyntaxError,
		sqlStateDatatypeMismatch,
		sqlStateInvalidPassword,
		sqlStateInvalidAuthorization:
		return true
	}
	return false
}

// SQLSTATE codes this package reasons about. They are spelled out rather than
// pulled from a dependency so that the classification is auditable against the
// PostgreSQL error code table.
const (
	// Class 08 -- connection exception. Covers a pooler that closed the
	// connection mid-query, which is what a PgBouncer restart looks like.
	sqlStateConnectionException          = "08000"
	sqlStateConnectionDoesNotExist       = "08003"
	sqlStateConnectionFailure            = "08006"
	sqlStateSQLClientUnableToEstablish   = "08001"
	sqlStateSQLServerRejectedEstablish   = "08004"
	sqlStateTransactionResolutionUnknown = "08007"
	sqlStateProtocolViolation            = "08P01"

	// Class 40 -- transaction rollback. Two participants racing for the same
	// row produce these under stricter isolation levels.
	sqlStateSerializationFailure = "40001"
	sqlStateDeadlockDetected     = "40P01"

	// Class 53 -- insufficient resources. A pooler with no free server
	// connection, or a server at max_connections.
	sqlStateTooManyConnections = "53300"
	sqlStateOutOfMemory        = "53200"
	sqlStateConfigLimitReached = "53400"

	// Class 25 -- invalid transaction state. A standby promoted out from
	// under us, or a failover that left this node read-only.
	sqlStateReadOnlySQLTxn = "25006"

	// Class 55/57 -- operator intervention. A failover, a restart, an
	// administrator cancelling queries.
	sqlStateLockNotAvailable   = "55P03"
	sqlStateQueryCanceled      = "57014"
	sqlStateAdminShutdown      = "57P01"
	sqlStateCrashShutdown      = "57P02"
	sqlStateCannotConnectNow   = "57P03"
	sqlStateDatabaseDropped    = "57P04"
	sqlStateIdleSessionTimeout = "57P05"

	// Class 42 and friends -- the deployment is wrong.
	sqlStateSyntaxError           = "42601"
	sqlStateInsufficientPrivilege = "42501"
	sqlStateUndefinedTable        = "42P01"
	sqlStateUndefinedColumn       = "42703"
	sqlStateUndefinedSchema       = "3F000"
	sqlStateDatatypeMismatch      = "42804"
	sqlStateInvalidPassword       = "28P01"
	sqlStateInvalidAuthorization  = "28000"
)

func retryableSQLState(code string) bool {
	switch code {
	case sqlStateConnectionException,
		sqlStateConnectionDoesNotExist,
		sqlStateConnectionFailure,
		sqlStateSQLClientUnableToEstablish,
		sqlStateSQLServerRejectedEstablish,
		sqlStateTransactionResolutionUnknown,
		sqlStateProtocolViolation,
		sqlStateSerializationFailure,
		sqlStateDeadlockDetected,
		sqlStateTooManyConnections,
		sqlStateOutOfMemory,
		sqlStateConfigLimitReached,
		sqlStateLockNotAvailable,
		sqlStateQueryCanceled,
		sqlStateAdminShutdown,
		sqlStateCrashShutdown,
		sqlStateCannotConnectNow,
		sqlStateDatabaseDropped,
		sqlStateIdleSessionTimeout,
		sqlStateReadOnlySQLTxn:
		return true
	}
	return false
}

// isConnectionError recognises the transport-level failures pgx reports
// without a SQLSTATE. This is what a PgBouncer restart or a severed network
// path looks like from the client: no error code, just a connection that is
// no longer there.
func isConnectionError(err error) bool {
	// Failed to establish a connection at all.
	var connErr *pgconn.ConnectError
	if errors.As(err, &connErr) {
		return true
	}
	// The network went away underneath an established connection: a severed
	// route, a pooler killed mid-query, a timeout on the socket.
	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}
	if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) {
		return true
	}
	// A pool closed underneath an in-flight operation. The sentinel lives in
	// pgxpool's underlying pool rather than in pgxpool itself; matching on the
	// message instead would break the moment the error is wrapped, which this
	// package always does before returning it.
	return errors.Is(err, puddle.ErrClosedPool)
}

// wrap passes an error through unchanged. It marks the places where a driver
// error crosses into the store's own errors, and is the single point to change
// if that ever needs more than pass-through.
func wrap(err error) error { return err }
