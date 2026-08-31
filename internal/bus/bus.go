package bus

import "context"

// Message is one payload received from the bus.
type Message struct {
	// Channel is the full bus key the message arrived on, including bus.prefix — exactly
	// the string that was passed to Sync. The hub keys its map by this, never by the bare
	// channel name (FR-21).
	Channel string

	// Payload is the raw envelope bytes. The bus does not parse them and does not retain
	// them; ownership passes to the receiver.
	Payload []byte
}

// Bus is the replica-to-replica transport.
//
// The subscription surface is a single Sync rather than a Subscribe/Unsubscribe pair,
// and that is the most important decision in this package. The earlier design pushed
// subscribe and unsubscribe commands onto a bounded channel drained by one goroutine, and
// that single choice produced four separate defects (docs/13-review-findings.md S2):
//
//   - With Redis merely slow, the queue filled. The fan-out goroutine then blocked
//     pushing an unsubscribe for a slow-consumer close, and all delivery on the replica
//     stopped while every socket stayed open and /ready stayed 200.
//   - Subscribe returned an error nobody consumed, so one transient failure left a
//     channel locally subscribed and upstream dead, permanently, and silently.
//   - On reconnect the resubscribe sweep raced the live command stream in both
//     directions: a channel could end up subscribed with no local holders, or held with
//     no subscription.
//   - One channel per call meant a 30,000-channel resubscribe was 30,000 serial round
//     trips — seconds of blackout at any real scale.
//
// All four are symptoms of modelling state as events. Subscriptions are state. The hub
// owns a desired set and a dirty flag; marking dirty is a non-blocking atomic store, so
// no producer can ever stall; a failed Sync simply leaves the set dirty for the next
// attempt; and reconnect is a forced dirty rather than a sweep.
//
// Implementations must be safe for concurrent use by multiple goroutines.
type Bus interface {
	// Sync makes the bus subscription set exactly desired, adding what is missing and
	// removing what is no longer wanted. It is idempotent — calling it twice with the
	// same set is a no-op — and batched, because Redis SUBSCRIBE is variadic and a
	// per-channel round trip does not survive a 30,000-channel resubscribe.
	//
	// desired holds full bus keys. Sync must not retain the slice: the caller owns it and
	// may reuse the backing array on the next reconciliation pass.
	//
	// An error leaves the previous subscription set in place and is retried by the
	// caller's reconciler with backoff. Implementations must not swallow one: a Sync that
	// fails silently is a channel that is locally held and upstream dead, which is
	// invisible until someone asks why a channel is quiet. Every failure is logged by
	// the reconciler, which is where an operator finds it (docs/10-operations.md §7).
	Sync(ctx context.Context, desired []string) error

	// Publish sends payload on channel. channel is a full bus key.
	//
	// It is used for client events and for control messages. Ordinary application
	// publishes do not go through the gateway at all — the application publishes to Redis
	// directly, which is the point of choosing a bus over an HTTP publish API
	// (docs/04-integration.md §2).
	Publish(ctx context.Context, channel string, payload []byte) error

	// Receive returns the channel of inbound messages. It returns the same channel on
	// every call, and that channel is closed only when the Bus is closed.
	//
	// The implementation's reader goroutine must do nothing but drain the transport into
	// this channel, which is bounded by bus.intake_queue. Decoding and fan-out happen on
	// bus.dispatch_workers goroutines downstream. Redis enforces
	// client-output-buffer-limit pubsub and disconnects a subscriber that falls behind; a
	// single goroutine that decodes and fans out to 10,000 connections between socket
	// reads will fall behind during a broadcast burst, get dropped, reconnect,
	// resubscribe and be immediately behind again. That oscillation is stable, not
	// transient, and it presents to an operator as /ready's bus_reconnects climbing
	// against a perfectly healthy Redis (docs/13-review-findings.md M8).
	Receive() <-chan Message

	// Close releases the transport and closes the channel returned by Receive. It is
	// idempotent. In-flight Sync and Publish calls return an error rather than blocking.
	Close() error
}
