package leaderelection

import (
	"context"
	"errors"
)

// Each database call gets its own bounded context. Without this a single
// unresponsive connection would block the renewal loop indefinitely; the
// watchdog would still step us down safely, but we would never get the chance
// to recover the lease we were entitled to keep.

func (e *Elector) inspect(ctx context.Context) (Lease, error) {
	opCtx, cancel := e.clock.WithTimeout(ctx, e.cfg.OperationTimeout)
	defer cancel()
	lease, err := e.cfg.Store.Inspect(opCtx, e.cfg.Name)
	return lease, opError(opCtx, err)
}

func (e *Elector) acquire(ctx context.Context) (Lease, error) {
	opCtx, cancel := e.clock.WithTimeout(ctx, e.cfg.OperationTimeout)
	defer cancel()
	lease, err := e.cfg.Store.Acquire(opCtx, e.cfg.Name, e.cfg.Identity, e.cfg.LeaseDuration, e.cfg.Payload)
	return lease, opError(opCtx, err)
}

func (e *Elector) renew(ctx context.Context, fence int64) (Lease, error) {
	opCtx, cancel := e.clock.WithTimeout(ctx, e.cfg.OperationTimeout)
	defer cancel()
	lease, err := e.cfg.Store.Renew(opCtx, e.cfg.Name, e.cfg.Identity, fence, e.cfg.LeaseDuration, e.cfg.Payload)
	return lease, opError(opCtx, err)
}

func (e *Elector) release(ctx context.Context, fence int64) error {
	opCtx, cancel := e.clock.WithTimeout(ctx, e.cfg.OperationTimeout)
	defer cancel()
	return opError(opCtx, e.cfg.Store.Release(opCtx, e.cfg.Name, e.cfg.Identity, fence))
}

// opError replaces a bare context.Canceled with the reason the operation's
// context was cancelled, so that a timeout is reported as a timeout rather
// than as an indistinguishable cancellation.
func opError(opCtx context.Context, err error) error {
	if err == nil || !errors.Is(err, context.Canceled) {
		return err
	}
	cause := context.Cause(opCtx)
	if cause == nil || errors.Is(cause, context.Canceled) {
		return err
	}
	return cause
}
