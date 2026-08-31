package conn

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/raghulj/sidecartunnel/internal/glob"
	"github.com/raghulj/sidecartunnel/internal/proto"
)

// Sink is the connection surface a registry sees: address it, hand it a frame, end it.
//
// It is structurally identical to hub.Sink and stays declared here rather than being
// replaced by it. That was reconsidered when hub.Subscribe and hub.Unsubscribe grew the
// ack parameter this package's Registry always required, and the local declaration won on
// three counts:
//
//   - Importing internal/hub would couple the two packages docs/09-internals.md §1
//     requires be built and tested apart, and would drag internal/bus and internal/config
//     into this package's import graph for the sake of four method signatures it already
//     states.
//   - It would buy nothing, because *hub.Hub cannot satisfy Registry whatever this type
//     is called: Publish takes a Sink, a channel, an event and a payload here and a
//     context, a channel and bytes there, and the hub's refusals are sentinel errors that
//     something has to map onto the *CommandError codes docs/03-client-protocol.md §6
//     assigns. An adapter exists either way, and internal/server is where it belongs —
//     the package that already knows the configuration both sides were built from.
//   - Nothing is lost at the seam. Go converts between two interfaces with the same
//     method set implicitly, so a Sink flows into a parameter typed hub.Sink with no
//     assertion, and the interface value still holds the same (type, pointer) pair — which
//     matters, because the hub keys its maps by it and a copy that compared unequal would
//     leave a connection resident in the map forever.
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
	// closer goroutine rather than retrying or waiting (FR-15). A closed connection
	// reports true and drops the frame: it is not backpressure and it needs no closing.
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
	// Attach subscribes s to channels, which the caller has already grant-checked,
	// deduplicated and capped. It then calls ack with the channels it actually took — an
	// implementation may omit one, for an unknown namespace, and
	// docs/03-client-protocol.md §4.1 says an omitted channel is left out of the reply
	// rather than failing the whole connect — and queues the returned frame, if any,
	// while still holding its lock.
	//
	// One call rather than a loop of Subscribe is deliberate: it is one lock acquisition,
	// and it leaves no window in which a push for a just-subscribed channel could
	// overtake the connect reply that announces it (M15).
	//
	// It registers nothing. A connection is registered before its connect frame arrives —
	// internal/server does it at the upgrade, so a control disconnect can reach the
	// connection from the moment it exists (FR-18) — and an implementation that also
	// registered here could not tell a connection that was never added from one that
	// close has just deregistered. The second is a resurrection: SIGTERM closes a
	// connection whose reader is blocked in Authorize, the application then answers 200,
	// and the registry takes subscriptions for a connection nothing will remove again
	// (docs/13-review-findings.md M4). An unregistered sink is granted nothing, and ack
	// is called with an empty list.
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

	// Grants are the channel patterns this connection may hold, already compiled.
	//
	// It is a compiled glob.Set rather than the raw strings because the Authorizer is
	// what turns the application's answer into one, and it has to compile the list anyway
	// to tell an unusable answer from a usable one: a grant beginning "_" is a refusal
	// (proto.CloseUnauthorized, reconnect false) decided where the rest of FR-6's
	// refusal-versus-failure distinction is decided, and internal/webhook already answers
	// it that way. Compiling a second time here would be the same work done twice, with a
	// second error path that a correct Authorizer can never reach.
	//
	// A Set is immutable, so it is safe to hand to a connection whose reader is already
	// matching against the previous one (FR-9, docs/13-review-findings.md M2). The zero
	// Set is a connection with no grants, which is legal and can subscribe to nothing.
	Grants glob.Set

	// ExpiresIn is how long the grant set is good for. At that point the connection is
	// closed with proto.CloseExpired so the client re-handshakes with whatever cookie the
	// browser currently holds (FR-22). Zero or negative means no expiry is armed.
	ExpiresIn time.Duration
}

// Authorizer obtains the authorization for one connection.
//
// It is per-connection and closes over that request's Cookie header, which is the only
// place a cookie exists once the handshake is over. FR-22 requires the gateway to hold no
// cookie past the connect call, and two things enforce that rather than one convention:
//
//   - A Conn takes this reference exactly once, in doConnect, and swaps its own field for
//     nil in the same act — so the Authorizer is unreachable from the connection the
//     moment authorization returns, and the swap doubles as the "connect may be sent
//     once" guard, which is what stops the drop being quietly removed later.
//   - The implementation must itself release the cookie when the call returns.
//     internal/server's does: the request lives behind an atomic pointer that Authorize
//     swaps for nil before it calls the webhook, so a second call has nothing to send and
//     the value is garbage the instant the call unwinds.
//
// Between them, a core dump of a process holding 20,000 connections yields no session
// cookies, which is the whole point of the arrangement (docs/13-review-findings.md S3).
//
// Implementations must honour the context, which carries app.connect_timeout. Its
// expiry is a failure, not a refusal, and closes with proto.CloseAuthUnavailable.
//
// The returned error is logged. NFR-7 makes it the implementation's responsibility that
// it contains no cookie, no Authorization header and no webhook body.
type Authorizer interface {
	// Authorize calls the application and returns its answer, or ErrUnauthorized wrapped
	// when the application refused. It is called at most once per connection, and an
	// implementation must release its cookie before it returns (FR-22).
	//
	// requested is the connect frame's subs, forwarded to the application as
	// channels_requested (docs/04-integration.md §1.1). It takes an argument rather than
	// being captured with the cookie because the value does not exist until a frame has
	// been read, which is after the connection was built.
	//
	// It is a **hint and never authority**. The application answers with the grants and
	// the gateway matches against those; a channel appearing here confers nothing, and an
	// implementation that merged the two would let every client write its own
	// authorization.
	//
	// The connection bounds it before the call: at most limits.max_subscriptions_per_conn
	// entries, each at most limits.max_channel_length bytes, deduplicated. It is
	// untrusted client input on its way into an outbound request, and an unbounded list
	// is an amplification vector into the application (NFR-4).
	Authorize(ctx context.Context, requested []string) (Authorization, error)
}

// AuthorizerFunc adapts a function to Authorizer.
type AuthorizerFunc func(ctx context.Context, requested []string) (Authorization, error)

// Authorize calls f.
func (f AuthorizerFunc) Authorize(ctx context.Context, requested []string) (Authorization, error) {
	return f(ctx, requested)
}

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
