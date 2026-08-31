package conn

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/raghulj/sidecartunnel/internal/proto"
)

// Sink is the connection surface a registry sees: address it, hand it a frame, end it.
//
// It is structurally identical to hub.Sink and is declared here so that internal/conn and
// internal/hub build, and can be tested, apart — the dependency direction in
// docs/09-internals.md §1 is downward only, and neither package needs to import the
// other for *Conn to satisfy both.
//
// Every method is safe to call concurrently and none of them blocks.
type Sink interface {
	// ID returns the connection's client id: 16 hex characters, stable for its life, and
	// readable after close because fan-out reads it under the registry's read lock, which
	// is not synchronised with close.
	ID() string

	// User returns the opaque user id the connect webhook supplied, fixed for the
	// connection's life so a revocation sweep cannot be dodged (FR-18, FR-22).
	User() string

	// Send queues one encoded frame and reports whether it was accepted. It never blocks;
	// false means the outbound queue was full and the caller must hand this sink to the
	// closer goroutine rather than retrying or waiting (FR-15).
	Send(f *proto.Frame) bool

	// Close ends the connection with a websocket close code and a short reason. It is
	// idempotent and must never be called while holding the registry's lock.
	Close(code proto.CloseCode, reason string)
}

// Registry is the subscription bookkeeping a Conn reports to — internal/hub in
// production. A Conn holds the socket and the grants; which connection holds which
// channel is not its state, and duplicating it here is the drift
// docs/13-review-findings.md M4 is about.
//
// Every method must be safe to call from a connection's reader goroutine, and none may
// block on network I/O (docs/09-internals.md §4.1).
//
// The ack parameters are how docs/13-review-findings.md M15 is satisfied. The gateway
// MUST NOT send a push for a channel before that channel's subscribe reply, nor after its
// unsubscribe reply. An implementation therefore queues ack to the sink's outbound queue
// **inside the same critical section** that mutates the subscription, so queue order
// guarantees the ordering for free. Queueing it after releasing the lock reopens the
// window, and the resulting divergence between two conforming clients is silent.
//
// Errors are inspected with errors.As for *CommandError; anything else is reported to the
// client as proto.ErrInternal.
type Registry interface {
	// Attach registers s and subscribes it to channels, which the caller has already
	// grant-checked, deduplicated and capped. It then calls ack with the channels it
	// actually took — an implementation may omit one, for an unknown namespace, and
	// docs/03-client-protocol.md §4.1 says an omitted channel is left out of the reply
	// rather than failing the whole connect — and queues the returned frame, if any,
	// while still holding its lock.
	//
	// One call rather than a loop of Subscribe is deliberate: it is one lock acquisition,
	// and it leaves no window in which a push for a just-subscribed channel could
	// overtake the connect reply that announces it (M15).
	Attach(s Sink, channels []string, ack func(granted []string) *proto.Frame)

	// Subscribe adds one channel for s and queues ack under the same lock. It returns a
	// *CommandError for a refusal the client should see: proto.ErrAlreadySubscribed,
	// proto.ErrSubscriptionLimit or proto.ErrUnknownNamespace.
	Subscribe(s Sink, channel string, ack *proto.Frame) error

	// Unsubscribe drops one channel for s and queues ack under the same lock. It returns
	// a *CommandError carrying proto.ErrNotSubscribed when s does not hold channel.
	Unsubscribe(s Sink, channel string, ack *proto.Frame) error

	// Subscriptions returns the authoritative subscription set for s, answering the sync
	// command (docs/03-client-protocol.md §4.5).
	Subscriptions(s Sink) []string

	// Publish delivers a client event. The caller has checked the grant; the registry
	// checks client_events and the rate limit, and stamps from with s.User(), which a
	// client must never be able to set (docs/03-client-protocol.md §4.4). It returns a
	// *CommandError carrying proto.ErrPermissionDenied or proto.ErrRateLimited.
	Publish(s Sink, channel, event string, data json.RawMessage) error

	// Remove deregisters s completely. A Conn calls it exactly once, from Close, so it
	// must tolerate a sink it has never seen — a connection closed by the handshake
	// timeout never reached Attach.
	Remove(s Sink)
}

// ErrUnauthorized marks an authorization that was refused rather than one that failed.
//
// FR-6 turns on this distinction and the two must never share a close code: a refusal is
// proto.CloseUnauthorized with reconnect false, and a failure is
// proto.CloseAuthUnavailable with reconnect true and a retry_after. Reusing one code
// means every client hammers a failing application during the exact incident where that
// is most harmful. Test for it with errors.Is, never by comparing strings.
var ErrUnauthorized = errors.New("conn: authorization refused")

// Authorization is what the connect webhook decided about one connection.
type Authorization struct {
	// User is the opaque user id. The gateway never parses it; it targets revocation
	// (FR-18) and stamps client events.
	User string

	// Grants are the channel patterns this connection may hold, compiled once at connect
	// and never at match time (FR-9).
	Grants []string

	// ExpiresIn is how long the grant set is good for. At that point the connection is
	// closed with proto.CloseExpired so the client re-handshakes with whatever cookie the
	// browser currently holds (FR-22). Zero or negative means no expiry is armed.
	ExpiresIn time.Duration
}

// Authorizer obtains the authorization for one connection.
//
// It is per-connection and closes over that request's Cookie header, which is why this
// package never sees a cookie and cannot retain one: FR-22 requires the gateway to hold
// no cookie past the connect call, and the cleanest way to guarantee that is for the
// value never to enter the type that outlives the call.
//
// Implementations must honour the context, which carries app.connect_timeout. Its
// expiry is a failure, not a refusal, and closes with proto.CloseAuthUnavailable.
//
// The returned error is logged. NFR-7 makes it the implementation's responsibility that
// it contains no cookie, no Authorization header and no webhook body.
type Authorizer interface {
	// Authorize calls the application and returns its answer, or ErrUnauthorized wrapped
	// when the application refused.
	Authorize(ctx context.Context) (Authorization, error)
}

// AuthorizerFunc adapts a function to Authorizer.
type AuthorizerFunc func(ctx context.Context) (Authorization, error)

// Authorize calls f.
func (f AuthorizerFunc) Authorize(ctx context.Context) (Authorization, error) { return f(ctx) }

// CommandError is a command failure carrying the protocol error code to answer with.
//
// It exists so a Registry can refuse a command without importing any opinion about the
// wire: it returns the code, and the connection turns that into
// {"error":{"code":…,"message":…}} against the command's id and leaves the connection
// open, which is what docs/03-client-protocol.md §6 requires of every ErrCode.
type CommandError struct {
	// Code is the wire error code.
	Code proto.ErrCode

	// Message is a short string for a developer reading a console. It is not a stable
	// API — clients switch on Code — and it must never contain a cookie, a header value
	// or a payload (NFR-7).
	Message string
}

// Error implements error.
func (e *CommandError) Error() string {
	return fmt.Sprintf("conn: command failed with code %d: %s", e.Code, e.Message)
}
