package conn

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/gorilla/websocket"

	"github.com/raghulj/sidecartunnel/internal/proto"
)

// read is the reader goroutine: a blocking read loop that parses one frame and handles
// the command inline. Handling is in-memory and cheap — a grant match is an atomic load
// and a string comparison — so a queue between the two would only add latency and a
// second thing to reason about (docs/09-internals.md §3, §9).
//
// It exits on a read error or once a command has closed the connection. It never writes
// to the socket; every reply goes onto the outbound queue for the writer.
func (c *Conn) read(ctx context.Context) {
	c.sock.SetPongHandler(func(string) error {
		c.pongs.Add(1)
		return nil
	})

	for {
		kind, body, err := c.sock.NextReader()
		if err != nil {
			// NFR-7: a websocket close error carries the peer's own reason string, which
			// is client-supplied text. The client id is what identifies the connection in
			// a log, so that is all this line carries.
			c.log.Debug("read loop ended", "client", c.id)
			c.abort()
			return
		}
		if kind != websocket.TextMessage {
			// docs/03-client-protocol.md §1: all application frames are text frames.
			c.Close(proto.CloseProtocolError, "binary frame")
			return
		}
		data, err := readLimited(body, c.maxFrameSize)
		if err != nil {
			if errors.Is(err, errFrameTooLarge) {
				// docs/03-client-protocol.md §7: an oversize frame is a protocol error and
				// is not retryable. There is no error code for it — proto's 107 was
				// removed as unreachable, because closing leaves no connection to answer
				// on.
				c.Close(proto.CloseProtocolError, "frame too large")
				return
			}
			c.log.Debug("frame read failed", "client", c.id)
			c.abort()
			return
		}
		if !c.handle(ctx, data) {
			return
		}
	}
}

// errFrameTooLarge marks a frame over limits.max_frame_size.
var errFrameTooLarge = errors.New("conn: frame exceeds the maximum frame size")

// readLimited reads at most limit+1 bytes and reports errFrameTooLarge if the frame was
// longer, which is what bounds the memory one client can make the gateway allocate. The
// rest of an oversize frame is never read, because the connection is closed immediately.
func readLimited(r io.Reader, limit int) ([]byte, error) {
	data, err := io.ReadAll(io.LimitReader(r, int64(limit)+1))
	if err != nil {
		return nil, fmt.Errorf("conn: read frame: %w", err)
	}
	if len(data) > limit {
		return nil, errFrameTooLarge
	}
	return data, nil
}

// handle dispatches one client frame and reports whether the reader should keep going.
//
// docs/03-client-protocol.md §4: connect must be first and may be sent once; anything
// before it, a second one, or a frame that is not exactly one command key, is
// proto.ErrBadRequest with the connection left open. A client that sends a malformed
// frame is usually a client with a bug, and disconnecting it turns a recoverable bug into
// a reconnect loop.
func (c *Conn) handle(ctx context.Context, data []byte) bool {
	cmd, err := proto.Decode(data)
	if err != nil {
		// NFR-7: the decode error quotes client-supplied text — a command key, a JSON
		// fragment — so it never reaches the log at any level.
		c.log.Debug("malformed frame", "client", c.id)
		return c.fail(0, proto.ErrBadRequest, "malformed frame")
	}

	if cmd.Connect != nil {
		if c.connectSeen.Load() {
			return c.fail(cmd.ID, proto.ErrBadRequest, "already connected")
		}
		return c.doConnect(ctx, cmd)
	}
	if !c.connectSeen.Load() {
		return c.fail(cmd.ID, proto.ErrBadRequest, "connect required")
	}

	switch {
	case cmd.Subscribe != nil:
		return c.doSubscribe(cmd)
	case cmd.Unsubscribe != nil:
		return c.doUnsubscribe(cmd)
	case cmd.Publish != nil:
		return c.doPublish(cmd)
	case cmd.Sync != nil:
		return c.doSync(cmd)
	default:
		// proto.Decode guarantees exactly one command key, so the remaining one is ping.
		return c.doPing(cmd)
	}
}

// doConnect performs the handshake: authorize, compile grants, subscribe the requested
// channels and answer (docs/03-client-protocol.md §4.1).
func (c *Conn) doConnect(ctx context.Context, cmd proto.Command) bool {
	// FR-4, first statement in the function and deliberately so: the handshake timer
	// covers receipt of this frame and nothing after it. Setting it later — after
	// authorization, say — is finding C2, and it closes every reconnecting user out with
	// a non-retryable 3001 the moment the application slows down.
	c.connectSeen.Store(true)

	authCtx, cancel := context.WithTimeout(ctx, c.connectTimeout)
	auth, err := c.auth.Authorize(authCtx)
	cancel()
	if err != nil {
		// FR-6: a refusal and a failure must not share a code. 3003 is a decision and
		// retrying cannot change it; 3008 says the application could not answer, so every
		// replica will do the same and reconnecting hard makes it worse.
		if errors.Is(err, ErrUnauthorized) {
			c.log.Info("connect refused by the application", "client", c.id)
			c.Close(proto.CloseUnauthorized, "unauthorized")
			return false
		}
		c.log.Warn("authorization unavailable", "client", c.id, "err", err)
		c.Close(proto.CloseAuthUnavailable, "authorization unavailable")
		return false
	}

	set, err := newGrantSet(auth.Grants)
	if err != nil {
		// An answer the gateway cannot compile is an answer it cannot enforce, and
		// enforcing is the only thing it does. Refusing is the safe direction.
		c.log.Warn("grant list rejected", "client", c.id, "err", err)
		c.Close(proto.CloseUnauthorized, "invalid grants")
		return false
	}
	c.grants.Store(&set)
	user := auth.User
	c.user.Store(&user)

	id := cmd.ID
	expiresIn := int(auth.ExpiresIn / time.Second)
	// Attach queues the reply inside the same critical section that takes the
	// subscriptions, so no push for a granted channel can overtake the connect reply that
	// announces it (docs/13-review-findings.md M15).
	c.registry.Attach(c, c.admissible(cmd.Connect.Subs), func(granted []string) *proto.Frame {
		subs := make(map[string]proto.SubDetail, len(granted))
		for _, channel := range granted {
			subs[channel] = proto.SubDetail{}
		}
		return c.encode(&proto.Reply{ID: id, Connect: &proto.ConnectReply{
			Client:    c.id,
			Ping:      int(c.pingInterval / time.Second),
			ExpiresIn: expiresIn,
			Subs:      subs,
		}})
	})

	c.log.Info("connected", "client", c.id, "user", user, "subs", len(cmd.Connect.Subs))
	if auth.ExpiresIn > 0 {
		// FR-22. The writer owns every timer; the channel has capacity 1 and is written
		// at most once, so this cannot block even if the writer has already gone.
		c.expires <- auth.ExpiresIn
	}
	return true
}

// admissible filters a connect frame's requested channels down to the ones worth handing
// to the Registry: granted, well-formed, deduplicated, and capped.
//
// docs/03-client-protocol.md §4.1: a channel that fails authorization is omitted from the
// reply rather than failing the whole connect. The client compares what it asked for
// against what it got.
func (c *Conn) admissible(requested []string) []string {
	out := make([]string, 0, len(requested))
	seen := make(map[string]struct{}, len(requested))
	for _, channel := range requested {
		if _, dup := seen[channel]; dup {
			continue
		}
		seen[channel] = struct{}{}
		if c.checkChannel(channel) != nil {
			continue
		}
		if len(out) == c.maxSubscriptions {
			// FR-8's cap. Silently truncating rather than erroring keeps this consistent
			// with the rule above: a channel the client does not get back is a channel it
			// must remove from its registry.
			break
		}
		out = append(out, channel)
	}
	return out
}

// doSubscribe answers a subscribe command (docs/03-client-protocol.md §4.2).
func (c *Conn) doSubscribe(cmd proto.Command) bool {
	if cmd.ID <= 0 {
		return c.fail(0, proto.ErrBadRequest, "subscribe requires an id")
	}
	if bad := c.checkChannel(cmd.Subscribe.Channel); bad != nil {
		return c.fail(cmd.ID, bad.Code, bad.Message)
	}
	ack := c.encode(&proto.Reply{ID: cmd.ID, Subscribe: &proto.SubscribeReply{}})
	if err := c.registry.Subscribe(c, cmd.Subscribe.Channel, ack); err != nil {
		return c.failWith(cmd.ID, err)
	}
	c.log.Debug("subscribed", "client", c.id, "channel", cmd.Subscribe.Channel)
	return true
}

// doUnsubscribe answers an unsubscribe command (docs/03-client-protocol.md §4.3).
//
// It deliberately does not consult the grants. A grant can be narrowed while a
// subscription is held, and a client dropping such a channel must succeed rather than be
// told it never had permission for the thing it is giving up.
func (c *Conn) doUnsubscribe(cmd proto.Command) bool {
	if cmd.ID <= 0 {
		return c.fail(0, proto.ErrBadRequest, "unsubscribe requires an id")
	}
	if !c.wellFormed(cmd.Unsubscribe.Channel) {
		return c.fail(cmd.ID, proto.ErrBadRequest, "invalid channel")
	}
	ack := c.encode(&proto.Reply{ID: cmd.ID, Unsubscribe: &proto.UnsubscribeReply{}})
	if err := c.registry.Unsubscribe(c, cmd.Unsubscribe.Channel, ack); err != nil {
		return c.failWith(cmd.ID, err)
	}
	c.log.Debug("unsubscribed", "client", c.id, "channel", cmd.Unsubscribe.Channel)
	return true
}

// doPublish answers a client event (docs/03-client-protocol.md §4.4).
//
// The grant check happens here and the client_events check happens in the Registry,
// because a client event requires both. Requiring only the namespace flag would let any
// connected client inject fabricated events into a channel it cannot even read
// (docs/13-review-findings.md M19).
func (c *Conn) doPublish(cmd proto.Command) bool {
	if cmd.ID <= 0 {
		return c.fail(0, proto.ErrBadRequest, "publish requires an id")
	}
	if cmd.Publish.Event == "" {
		// §3's example envelope omits event while §4.4 requires it, and §4.4 wins: an
		// unnamed event is not something a subscriber can dispatch on. proto leaves the
		// check here, where an ErrBadRequest can actually be sent.
		return c.fail(cmd.ID, proto.ErrBadRequest, "publish requires an event")
	}
	if bad := c.checkChannel(cmd.Publish.Channel); bad != nil {
		return c.fail(cmd.ID, bad.Code, bad.Message)
	}
	if err := c.registry.Publish(c, cmd.Publish.Channel, cmd.Publish.Event, cmd.Publish.Data); err != nil {
		return c.failWith(cmd.ID, err)
	}
	// NFR-7: the channel and the event name are logged; the payload never is.
	c.log.Debug("client event published", "client", c.id, "channel", cmd.Publish.Channel, "event", cmd.Publish.Event)
	return c.send(&proto.Reply{ID: cmd.ID, Publish: &proto.PublishReply{}})
}

// doSync answers a sync command with the Registry's authoritative subscription set
// (docs/03-client-protocol.md §4.5). It is what an integrator calls when debugging a
// channel that has gone quiet, so the answer must come from the Registry and never from a
// copy this connection keeps — a second copy is the drift M4 is about.
func (c *Conn) doSync(cmd proto.Command) bool {
	if cmd.ID <= 0 {
		return c.fail(0, proto.ErrBadRequest, "sync requires an id")
	}
	return c.send(&proto.Reply{ID: cmd.ID, Sync: &proto.SyncReply{Channels: c.registry.Subscriptions(c)}})
}

// doPing answers an application-level ping with a pong, echoing the id when one was
// supplied so a client with two pings in flight can correlate replies and measure
// round-trip time (docs/03-client-protocol.md §4.6).
//
// This pair is for the client's liveness detection: browsers answer protocol-level pings
// automatically and give JavaScript no way to observe them. The gateway's own liveness
// detection is the protocol-level ping the writer sends (FR-7).
func (c *Conn) doPing(cmd proto.Command) bool {
	return c.send(&proto.Reply{ID: cmd.ID, Pong: &proto.Pong{}})
}

// wellFormed reports whether a channel name is within limits.max_channel_length and not
// empty. A longer name is proto.ErrBadRequest (docs/07-delivery.md §7).
func (c *Conn) wellFormed(channel string) bool {
	return channel != "" && len(channel) <= c.maxChannelLength
}

// checkChannel applies every rule this package can answer from its own state: shape,
// the reserved prefix, and the grants.
//
// The reserved prefix is refused before the grants are consulted, so that a grant of "*"
// still cannot reach a control channel (docs/06-channels.md §4). Both refusals answer
// proto.ErrPermissionDenied, which is what keeps the existence of a control channel from
// being detectable by trying to subscribe to one.
func (c *Conn) checkChannel(channel string) *CommandError {
	if !c.wellFormed(channel) {
		return &CommandError{Code: proto.ErrBadRequest, Message: "invalid channel"}
	}
	if strings.HasPrefix(channel, reservedPrefix) {
		return &CommandError{Code: proto.ErrPermissionDenied, Message: "permission denied"}
	}
	if !c.Allows(channel) {
		return &CommandError{Code: proto.ErrPermissionDenied, Message: "permission denied"}
	}
	return nil
}

// send queues one reply and reports true, because a queued reply never ends the read
// loop: a full queue is a slow consumer, which the Registry's fan-out path closes.
func (c *Conn) send(reply *proto.Reply) bool {
	c.Send(c.encode(reply))
	return true
}

// fail answers a command with an error reply and leaves the connection open, which is
// what docs/03-client-protocol.md §6 requires of every error code.
func (c *Conn) fail(id int64, code proto.ErrCode, message string) bool {
	return c.send(&proto.Reply{ID: id, Error: &proto.Error{Code: code, Message: message}})
}

// failWith turns a Registry error into a reply. A *CommandError carries the code the
// client should see; anything else is a gateway fault and becomes proto.ErrInternal,
// which carries no detail on the wire because the client can do nothing with it but
// retry the command.
func (c *Conn) failWith(id int64, err error) bool {
	var cmdErr *CommandError
	if errors.As(err, &cmdErr) {
		return c.fail(id, cmdErr.Code, cmdErr.Message)
	}
	c.log.Error("registry command failed", "client", c.id, "err", err)
	return c.fail(id, proto.ErrInternal, "internal error")
}
