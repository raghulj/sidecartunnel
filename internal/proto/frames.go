package proto

import "encoding/json"

// Command is one client-to-gateway frame: a JSON object carrying an optional id and
// exactly one command key. docs/03-client-protocol.md §3.
//
// Exactly one command pointer must be non-nil. A frame with zero or several is answered
// with ErrBadRequest and the connection is left open — a client that sends a malformed
// frame is usually a client with a bug, not an attacker, and disconnecting it turns a
// recoverable bug into a reconnect loop.
//
// id is a positive integer, unique per connection, present on any command the client
// wants a reply to. It is optional on Ping and absent on nothing else in practice; a
// non-positive id on a command that needs a reply is ErrBadRequest.
type Command struct {
	// ID correlates this command with its reply. Zero means "no id supplied", which is
	// only valid for Ping.
	ID int64 `json:"id,omitempty"`

	// Connect is the first frame on every connection. Sending it twice is ErrBadRequest.
	Connect *ConnectRequest `json:"connect,omitempty"`

	// Subscribe asks for one channel.
	Subscribe *SubscribeRequest `json:"subscribe,omitempty"`

	// Unsubscribe drops one channel.
	Unsubscribe *UnsubscribeRequest `json:"unsubscribe,omitempty"`

	// Publish is a client event. M4; permitted only where the namespace sets
	// client_events (docs/03-client-protocol.md §4.4).
	Publish *PublishRequest `json:"publish,omitempty"`

	// Sync asks for the gateway's authoritative subscription set for this connection.
	Sync *SyncRequest `json:"sync,omitempty"`

	// Ping is the application-level liveness probe. It exists because browsers answer
	// websocket-level pings automatically and give JavaScript no way to observe them, so
	// a client cannot use them for its own liveness detection.
	Ping *PingRequest `json:"ping,omitempty"`
}

// ConnectRequest is the body of a connect command. Every field is optional.
type ConnectRequest struct {
	// Subs are channels to subscribe to atomically as part of connect, saving a round
	// trip on page load. Channels that fail authorization are omitted from the reply map
	// rather than failing the whole connect (docs/03-client-protocol.md §4.1).
	Subs []string `json:"subs,omitempty"`
}

// SubscribeRequest is the body of a subscribe command.
type SubscribeRequest struct {
	// Channel is matched against the connection's grants in memory, with no I/O and no
	// mutex (FR-9).
	Channel string `json:"channel"`
}

// UnsubscribeRequest is the body of an unsubscribe command.
type UnsubscribeRequest struct {
	// Channel must be one this connection currently holds; otherwise ErrNotSubscribed.
	Channel string `json:"channel"`
}

// PublishRequest is the body of a publish command — a client event. M4.
//
// A client event requires both a grant matching the channel and client_events: true on
// its namespace. Requiring only the namespace flag would let any connected client inject
// fabricated events into a channel it cannot even read
// (docs/13-review-findings.md M19).
type PublishRequest struct {
	// Channel is the target channel.
	Channel string `json:"channel"`

	// Event is required and client-supplied. docs/03-client-protocol.md §3's example
	// envelope omits it; §4.4 says plainly that it is required, and §4.4 wins — an
	// unnamed event is not something a subscriber can dispatch on.
	Event string `json:"event"`

	// Data is an opaque JSON value, passed through to subscribers untouched.
	Data json.RawMessage `json:"data"`
}

// SyncRequest is the body of a sync command. It has no fields; the connection is the
// implicit subject.
type SyncRequest struct{}

// PingRequest is the body of a ping command. It has no fields.
type PingRequest struct{}

// Reply is one gateway-to-client frame carrying an id, answering the command with that
// id. Exactly one payload pointer is non-nil. docs/03-client-protocol.md §3.
//
// Pong lives here rather than on Push because a pong answers a command: the id is echoed
// so a client with two pings in flight can correlate replies and measure round-trip time
// (docs/13-review-findings.md m12).
type Reply struct {
	// ID is the id of the command being answered. Omitted for a pong answering a ping
	// that carried none.
	ID int64 `json:"id,omitempty"`

	// Connect is the successful answer to a connect command.
	Connect *ConnectReply `json:"connect,omitempty"`

	// Subscribe is the successful answer to a subscribe command: an empty object.
	Subscribe *SubscribeReply `json:"subscribe,omitempty"`

	// Unsubscribe is the successful answer to an unsubscribe command: an empty object.
	Unsubscribe *UnsubscribeReply `json:"unsubscribe,omitempty"`

	// Publish is the successful answer to a publish command: an empty object.
	Publish *PublishReply `json:"publish,omitempty"`

	// Sync is the answer to a sync command.
	Sync *SyncReply `json:"sync,omitempty"`

	// Pong answers a ping command.
	Pong *Pong `json:"pong,omitempty"`

	// Error replaces the payload when the command failed. The connection stays open.
	Error *Error `json:"error,omitempty"`
}

// ConnectReply is the body of a successful connect reply.
type ConnectReply struct {
	// Client is 16 hex characters from crypto/rand, stable for the connection's life.
	// The application sends it back as exclude on a publish, and as X-St-Client on a
	// write, so an event does not echo to the tab that caused it. Each tab has its own,
	// which is the point (docs/04-integration.md §2.2).
	Client string `json:"client"`

	// Ping is server.ping_interval in whole seconds. Informational: the client does not
	// act on it, and the gateway's own liveness detection uses protocol-level pings.
	Ping int `json:"ping"`

	// ExpiresIn is seconds until the gateway closes with CloseExpired for
	// re-authorization. Already clamped to [app.min_expiry, app.max_expiry] — the client
	// is told the effective value, never the application's raw one
	// (docs/13-review-findings.md m11).
	ExpiresIn int `json:"expires_in"`

	// Subs maps channel to an empty object, for those in the request that succeeded.
	// Channels that failed authorization are absent; the client compares what it asked
	// for against what it got.
	Subs map[string]SubDetail `json:"subs"`
}

// MarshalJSON serializes the reply, writing subs as an empty object rather than null
// when no channel in the request was granted.
//
// docs/03-client-protocol.md §4.1 describes subs as a map of channel to {}, and §9's
// worked exchange shows a client comparing what it asked for against what it got. Go
// marshals a nil map as null, so a connect where nothing was granted — the single most
// important case for a client to handle, because it means the page will receive
// nothing — would arrive as a value the obvious client code (Object.keys, a for-in, a
// typed map decode) either throws on or misreads as "no subs field". Normalizing here
// keeps every connect reply one shape.
func (r ConnectReply) MarshalJSON() ([]byte, error) {
	// The alias sheds the method set, so json.Marshal below does not recurse.
	type alias ConnectReply
	out := alias(r)
	if out.Subs == nil {
		out.Subs = map[string]SubDetail{}
	}
	// Returned unwrapped: encoding/json is the only caller of MarshalJSON and adds its
	// own context. Every field here is a string, an int or a map of empty structs, so
	// there is no reachable failure to describe anyway.
	return json.Marshal(out)
}

// SubDetail is the per-channel value in a connect reply's subs map. It is empty today and
// exists as a struct rather than a bare null so that presence and history (M4) can add
// fields without changing the shape clients already parse.
type SubDetail struct{}

// SubscribeReply is the body of a successful subscribe reply: {}.
type SubscribeReply struct{}

// UnsubscribeReply is the body of a successful unsubscribe reply: {}.
type UnsubscribeReply struct{}

// PublishReply is the body of a successful client-event publish reply: {}.
type PublishReply struct{}

// SyncReply is the body of a sync reply.
type SyncReply struct {
	// Channels is the gateway's authoritative subscription set for this connection.
	//
	// The gateway can drop a subscription the client did not ask to drop — grant
	// narrowing, or a control-channel unsubscribe. Both send an unsubscribed push, but a
	// client that missed one has no other way to discover the divergence, and the symptom
	// is indistinguishable from a quiet channel (docs/13-review-findings.md M16).
	Channels []string `json:"channels"`
}

// MarshalJSON serializes the reply, writing channels as an empty array rather than null
// when the connection holds no subscriptions.
//
// That case is exactly what docs/03-client-protocol.md §4.5 says to call sync for — "the
// first thing to call when debugging nobody receives anything" — so it is the one a
// third-party client is most likely to hit while the author is already confused. §4.5
// and §9 both show an array, and a null that a client iterates over is a second bug
// stacked on top of the one being investigated.
func (r SyncReply) MarshalJSON() ([]byte, error) {
	type alias SyncReply
	out := alias(r)
	if out.Channels == nil {
		out.Channels = []string{}
	}
	return json.Marshal(out)
}

// Pong is the body of a pong reply: {}.
type Pong struct{}

// Error is the body of an error reply. The connection stays open.
type Error struct {
	// Code is one of the ErrCode constants.
	Code ErrCode `json:"code"`

	// Message is a short human-readable string for a developer reading a console. It is
	// not a stable API: clients switch on Code, never on Message. It must never contain a
	// cookie, a header value, or a payload (NFR-7).
	Message string `json:"message"`
}

// PushFrame is the top-level {"push": {…}} frame. Push carries the body.
//
// The wrapper exists because the wire format puts every frame's kind in a key rather than
// in a discriminator field, so the top-level object and the body are two different
// shapes. Encode is given the wrapper; handlers reason about the body.
type PushFrame struct {
	// Push is the body. Never nil on the wire.
	Push *Push `json:"push"`
}

// Push is the body of a server-initiated push. Exactly one of Pub and Unsubscribed is
// non-nil, alongside Channel. docs/03-client-protocol.md §5.1.
//
// §5.1's prose says "three shapes" and lists two. The third was refresh, which went away
// with expiry-by-re-handshake (docs/13-review-findings.md S3, m1). There are two.
//
// Ordering, normative: the gateway MUST NOT send a push for a channel before that
// channel's subscribe reply, nor after its unsubscribe reply. This is free given the
// single writer goroutine — the reply is queued to the outbound queue under the same hub
// lock that mutates the subscription, so queue order guarantees it. Without the rule a
// client legitimately receives a push for a channel it has not been told it holds, and
// two conforming implementations diverge silently.
type Push struct {
	// Channel is the channel this push concerns. It is the bare channel name, never the
	// bus key — the prefix is the gateway's business, not the client's.
	Channel string `json:"channel"`

	// Pub is a message from the bus, or from another client on a client-events namespace.
	Pub *Pub `json:"pub,omitempty"`

	// Unsubscribed says the gateway removed a subscription the client did not ask to
	// drop. The client MUST remove the channel from its registry and not resubscribe it;
	// a client that replays a revoked channel gets ErrPermissionDenied on every reconnect
	// forever (FR-17).
	Unsubscribed *Unsubscribed `json:"unsubscribed,omitempty"`
}

// Pub is a delivered message.
type Pub struct {
	// Event is the publisher's opaque event name, passed through unchanged.
	Event string `json:"event"`

	// Data is the publisher's opaque payload. Any JSON value.
	Data json.RawMessage `json:"data"`

	// From is the publishing connection's user id, present only for client events. The
	// gateway stamps it and MUST NOT let a client set it; a client-settable From is a
	// forged-identity primitive handed out for free (docs/03-client-protocol.md §4.4).
	From string `json:"from,omitempty"`
}

// Unsubscribed is the body of an unsubscribed push.
type Unsubscribed struct {
	// Reason is a short human-readable string, e.g. "grant revoked". Informational.
	Reason string `json:"reason,omitempty"`
}

// DisconnectFrame is the top-level {"disconnect": {…}} frame. Disconnect carries the body.
type DisconnectFrame struct {
	// Disconnect is the body. Never nil on the wire.
	Disconnect *Disconnect `json:"disconnect"`
}

// Disconnect is the body of a disconnect frame, sent immediately before the websocket
// close frame. docs/03-client-protocol.md §5.2.
//
// It exists so the client applies its backoff deliberately instead of treating an abrupt
// TCP reset as a network blip and retrying immediately. Draining without it turns a
// rolling deploy from a spread into a stampede.
type Disconnect struct {
	// Code is one of the CloseCode constants, mirrored in the websocket close code.
	Code CloseCode `json:"code"`

	// Reason is a short human-readable string. Never a cookie, header, or payload (NFR-7).
	Reason string `json:"reason"`

	// Reconnect tells the client whether retrying could ever succeed: true is transient,
	// false means a decision was made. Always serialized, including when false — a client
	// that has to infer it from absence will infer it wrongly.
	//
	// Clients MUST honour false. A client that retries through it turns a revocation into
	// a denial-of-service against the connect webhook.
	Reconnect bool `json:"reconnect"`

	// RetryAfter is milliseconds the client MUST wait before its next attempt, in place
	// of its own backoff for that attempt. Omitted when zero.
	//
	// The gateway sets it because the gateway knows how many connections it is dropping
	// and the client does not. Client-side jitter alone cannot solve this: at the first
	// retry min(30s, 2^n) × rand(0.5, 1.5) yields 0.5–1.5s, which is precisely the
	// one-second window docs/10-operations.md §4 models as an application outage
	// (docs/13-review-findings.md S5).
	RetryAfter int64 `json:"retry_after,omitempty"`
}

// Frame is one encoded outbound text frame, ready to be written to a socket.
//
// A Frame is created once per message and shared by every recipient of a fan-out. The
// outbound queue holds pointers to it rather than copies: at outbound_queue 256,
// max_message_size 32 KiB and 20,000 connections, a per-connection copy is 160 GiB
// against a 1 GiB budget, while sharing costs 256 × 16 bytes ≈ 4 KiB per connection
// (docs/09-internals.md §5, docs/13-review-findings.md M10).
//
// That sharing has one hard rule: Data MUST NOT be modified after the Frame is handed to
// anything. It is visible to every recipient's writer goroutine at once, so a single
// append is a data race against the whole channel.
type Frame struct {
	// Data is the encoded JSON object, without a trailing newline. Immutable once shared.
	Data []byte
}
