package hub

import "github.com/raghulj/sidecartunnel/internal/proto"

// Sink is the hub's entire view of a connection.
//
// It is four methods on purpose. The hub needs to address a connection (ID, User), hand
// it a frame (Send), and end it (Close); anything more would let hub logic reach into
// connection state and would make the two impossible to build in parallel. internal/conn
// implements this; nothing else should.
//
// Every method must be safe to call concurrently, and none may block. The hub calls Send
// while holding the read lock, so a Sink that blocks stalls delivery for every other
// subscriber on the channel — which is the failure the whole backpressure design exists
// to prevent (docs/07-delivery.md §4).
type Sink interface {
	// ID returns the connection's client id: 16 hex characters from crypto/rand, stable
	// for the connection's life. It is the join key across every log line for a
	// connection, and the value an application sends as exclude on a publish.
	//
	// It must be safe to call after the connection is closed — fan-out reads it under the
	// hub read lock, which is not synchronised with close.
	ID() string

	// User returns the opaque user id the connect webhook supplied. The gateway never
	// parses it; it exists to target revocation (FR-18) and to stamp client events.
	//
	// It is fixed for the connection's life, because a connection whose user could change
	// underneath a revocation sweep is a connection that can dodge one. Re-authorization
	// is a new connection (FR-22).
	User() string

	// Send queues one encoded frame for the connection's writer goroutine and returns
	// whether it was accepted. It never blocks.
	//
	// false means the outbound queue was full. The caller must not retry, must not wait,
	// and must not close the connection inline: it collects the refusing sinks, releases
	// the hub lock, and hands them to the closer goroutine, which closes them with
	// proto.CloseSlowConsumer (docs/09-internals.md §4.3, §4.5).
	//
	// f is shared with every other recipient of the same message. Neither the Sink nor
	// anything downstream of it may modify f.Data.
	//
	// Send on a closed connection returns true or false without panicking; a race between
	// fan-out and close is normal and must not be an error path.
	Send(f *proto.Frame) bool

	// Close ends the connection: it sends a disconnect frame carrying code and reason if
	// there is room in the outbound queue, closes the socket with code as the websocket
	// close code, and deregisters from the hub.
	//
	// It is idempotent — guarded by sync.Once — because expiry, revocation, drain and a
	// slow-consumer overflow can all decide to close the same connection at once.
	//
	// It must never be called while holding the hub lock: Close deregisters, which needs
	// the write lock (docs/09-internals.md §4.5).
	//
	// reason is a short human-readable string. It must never contain a cookie, a header
	// value, or a payload (NFR-7).
	Close(code proto.CloseCode, reason string)
}
