package leaderelection

import "errors"

var (
	// ErrNoLease is returned by Store.Inspect when no record exists for the
	// requested election. It means "nobody has ever been leader here", not
	// "the lease is free" -- although both are claimable.
	ErrNoLease = errors.New("leaderelection: no lease record")

	// ErrLeaseLost is returned by Store.Renew when the guarded update matched
	// no row. The caller is not the leader any more, and must stop acting as
	// one before doing anything else.
	ErrLeaseLost = errors.New("leaderelection: lease lost")

	// ErrLeaseExpiredLocally is recorded in [Status.LastError] when a leader
	// stepped down because it could not confirm its lease in time.
	//
	// It does not mean the lease was taken away -- nobody may have taken it,
	// and the database may be perfectly healthy on the other side of a broken
	// network path. It means this process ran out of evidence that it was
	// still the leader, which is the only situation in which it is allowed to
	// keep acting as one.
	ErrLeaseExpiredLocally = errors.New("leaderelection: lease expired before it could be renewed")

	// ErrAlreadyRunning is returned by Elector.Run when the elector is already
	// running. An Elector drives a single participant in a single election and
	// cannot be shared between goroutines by running it twice.
	ErrAlreadyRunning = errors.New("leaderelection: elector is already running")
)

// ConfigError reports an invalid [Config]. It is returned by [New] and by
// [Config.Validate].
type ConfigError struct {
	// Field names the offending configuration field.
	Field string
	// Reason explains what is wrong with it.
	Reason string
}

func (e *ConfigError) Error() string {
	return "leaderelection: invalid config: " + e.Field + ": " + e.Reason
}
