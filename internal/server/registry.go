package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/raghulj/sidecartunnel/internal/conn"
	"github.com/raghulj/sidecartunnel/internal/hub"
	"github.com/raghulj/sidecartunnel/internal/proto"
)

// registry adapts *hub.Hub to conn.Registry.
//
// The adapter exists here, in the package that already knows the configuration both sides
// were built from, and not in either of them: internal/hub must not import internal/conn,
// and internal/conn must not import internal/hub, because docs/09-internals.md §1 makes
// the dependency direction downward only and the two are built and tested apart.
//
// It does three things the hub deliberately does not:
//
//   - It maps the hub's sentinel errors onto the protocol error codes
//     docs/03-client-protocol.md §6 assigns them. The hub holds no opinion about the
//     wire, and the connection switches on *conn.CommandError.
//   - It applies the client-event policy: namespaces[].client_events and the per-namespace
//     rate limit, which are configuration the hub does not read.
//   - It stamps from with the connection's user id, which a client must never be able to
//     set, and excludes the publisher from its own event (docs/03-client-protocol.md §4.4).
//
// One registry is built per connection, because the client-event rate limit is per
// connection. It holds no lock of its own: the subscription bookkeeping is the hub's, and
// the limiter has its own.
type registry struct {
	// ctx is the server's lifetime. hub.Publish needs one and conn.Registry.Publish has
	// none to give: a client event is a bus write, and it must not outlive the process
	// that accepted it.
	ctx context.Context

	hub   *hub.Hub
	rates map[string]rate
	lim   *limiter
}

// newRegistry builds the per-connection adapter.
func newRegistry(ctx context.Context, h *hub.Hub, rates map[string]rate, clock conn.Clock) *registry {
	return &registry{ctx: ctx, hub: h, rates: rates, lim: newLimiter(clock)}
}

// Attach registers the connection and takes its connect-frame subscriptions in one
// critical section, queueing the reply the callback builds before the lock is released
// (M15).
func (r *registry) Attach(s conn.Sink, channels []string, ack func(granted []string) *proto.Frame) {
	r.hub.Attach(s, channels, ack)
}

// Subscribe adds one channel and queues the pre-encoded reply under the same lock.
func (r *registry) Subscribe(s conn.Sink, channel string, ack *proto.Frame) error {
	return commandError(r.hub.Subscribe(s, channel, ack))
}

// Unsubscribe drops one channel and queues the pre-encoded reply under the same lock.
func (r *registry) Unsubscribe(s conn.Sink, channel string, ack *proto.Frame) error {
	return commandError(r.hub.Unsubscribe(s, channel, ack))
}

// Subscriptions returns the hub's authoritative subscription set, which is what makes the
// sync command worth having: it comes from the registry and never from a copy the
// connection keeps (docs/03-client-protocol.md §4.5, M4).
func (r *registry) Subscriptions(s conn.Sink) []string { return r.hub.Subscriptions(s) }

// Remove deregisters the connection. It tolerates one it has never seen, because a
// connection closed by the handshake timeout never reached Attach.
func (r *registry) Remove(s conn.Sink) { r.hub.Remove(s) }

// Publish delivers one client event (docs/03-client-protocol.md §4.4).
//
// The grant check has already happened in the connection: a client event requires **both**
// a grant matching the channel and client_events on its namespace. Requiring only the
// namespace flag would let any connected client inject fabricated events into a channel it
// cannot even read (docs/13-review-findings.md M19).
//
// from is stamped with the connection's user id and exclude with its client id, neither of
// which a client can set. The publisher is always excluded from its own event: §4.4 makes
// that conditional on a namespace echo flag, and there is no such key in
// docs/08-config.md §3, so the unconditional rule is the one that can be honoured.
func (r *registry) Publish(s conn.Sink, channel, event string, data json.RawMessage) error {
	ns, ok := r.hub.Namespace(channel)
	if !ok {
		// FR-11: a channel whose namespace has no block fails closed.
		return &conn.CommandError{Code: proto.ErrUnknownNamespace, Message: "unknown namespace"}
	}
	if !ns.ClientEvents {
		return &conn.CommandError{Code: proto.ErrPermissionDenied, Message: "client events are not enabled for this channel"}
	}
	if allowed, abusive := r.lim.allow(ns.Name, r.rates[ns.Name]); !allowed {
		if abusive {
			// docs/03-client-protocol.md §4.4: ten violations within 60 seconds ends the
			// connection with a retry_after of 60s. Without the delay the anti-abuse
			// control amplifies load onto the connect webhook, which is the component
			// least able to absorb it (m13).
			s.Close(proto.CloseRateLimited, "rate limit exceeded repeatedly")
		}
		return &conn.CommandError{Code: proto.ErrRateLimited, Message: "rate limited"}
	}

	payload, err := json.Marshal(hub.Envelope{
		Event:   event,
		Data:    data,
		From:    s.User(),
		Exclude: s.ID(),
	})
	if err != nil {
		// coverage: the only value here that can fail to marshal is data, a
		// json.RawMessage that proto.Decode already parsed as part of a valid frame. It
		// is reported rather than assumed away so the client is told the event did not
		// happen, instead of the gateway pretending it did.
		return fmt.Errorf("server: encode client event on %q: %w", channel, err)
	}
	if err := r.hub.Publish(r.ctx, channel, payload); err != nil {
		return fmt.Errorf("server: publish client event on %q: %w", channel, err)
	}
	return nil
}

// commandError maps a hub refusal onto the protocol error code
// docs/03-client-protocol.md §6 assigns it.
//
// It uses errors.Is rather than string comparison, because the hub wraps every sentinel
// with the channel that caused it and a chain broken by a %v somewhere in the middle
// turns this into a guess (docs/14-coding-standards.md §6).
//
// Anything unrecognised — hub.ErrNotRegistered, most obviously, which means the
// connection was deregistered while a command was in flight — is left as it is, and the
// connection answers proto.ErrInternal. That is the right answer for it: the client can
// do nothing with the detail, and a connection that is gone is about to be told so.
func commandError(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, hub.ErrAlreadySubscribed):
		return &conn.CommandError{Code: proto.ErrAlreadySubscribed, Message: "already subscribed"}
	case errors.Is(err, hub.ErrNotSubscribed):
		return &conn.CommandError{Code: proto.ErrNotSubscribed, Message: "not subscribed"}
	case errors.Is(err, hub.ErrSubscriptionLimit):
		return &conn.CommandError{Code: proto.ErrSubscriptionLimit, Message: "subscription limit reached"}
	case errors.Is(err, hub.ErrUnknownNamespace):
		return &conn.CommandError{Code: proto.ErrUnknownNamespace, Message: "unknown namespace"}
	case errors.Is(err, hub.ErrReservedChannel):
		// The same code as an ungranted channel, deliberately: answering differently
		// would make the existence of a control channel detectable by trying to subscribe
		// to one (docs/06-channels.md §4).
		return &conn.CommandError{Code: proto.ErrPermissionDenied, Message: "permission denied"}
	default:
		return err
	}
}
