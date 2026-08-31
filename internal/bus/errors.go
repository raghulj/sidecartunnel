package bus

import (
	"context"
	"errors"
)

// ErrClosed is returned by Sync and Publish once Close has been called. It is never
// returned for a transport failure — that is ErrDisconnected — because the two demand
// opposite responses from the caller: a closed bus stays closed, a disconnected one is
// retried.
var ErrClosed = errors.New("bus is closed")

// ErrDisconnected is returned by Sync and Publish while the transport is down.
//
// Publish returns it immediately rather than blocking. A publish that waited for the bus
// to come back would hold whichever goroutine issued it — including a control-channel
// publish — for the whole of an outage, and messages published while disconnected are
// lost by design: at-most-once, covered by the client's reconciliation
// (docs/09-internals.md §7, docs/07-delivery.md §2).
var ErrDisconnected = errors.New("bus is disconnected")

// checkState reports the error an operation must return before doing any work, and nil
// when it may proceed. done is the channel an implementation closes when it is closing.
//
// What it buys is that an operation issued after Close returns ErrClosed rather than a
// confusing transport error, and that a cancelled context is honoured before any network
// I/O rather than after it.
func checkState(ctx context.Context, done <-chan struct{}) error {
	select {
	case <-done:
		return ErrClosed
	default:
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return nil
}
