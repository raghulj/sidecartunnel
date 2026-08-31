package integration_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/raghulj/sidecartunnel/internal/proto"
)

// frame is one gateway-to-client frame, decoded loosely enough that a single read can
// tell a reply from a push from a disconnect.
type frame struct {
	ID          int64               `json:"id"`
	Connect     *proto.ConnectReply `json:"connect"`
	Subscribe   *json.RawMessage    `json:"subscribe"`
	Unsubscribe *json.RawMessage    `json:"unsubscribe"`
	Publish     *json.RawMessage    `json:"publish"`
	Sync        *proto.SyncReply    `json:"sync"`
	Pong        *json.RawMessage    `json:"pong"`
	Push        *proto.Push         `json:"push"`
	Error       *proto.Error        `json:"error"`
	Disconnect  *proto.Disconnect   `json:"disconnect"`
}

// String renders a frame for a failure message. It names the shape, never a payload
// (NFR-7 applies to test output too: it ends up in CI logs).
func (f frame) String() string {
	switch {
	case f.Connect != nil:
		return fmt.Sprintf("connect reply (client %s)", f.Connect.Client)
	case f.Subscribe != nil:
		return fmt.Sprintf("subscribe reply id=%d", f.ID)
	case f.Unsubscribe != nil:
		return fmt.Sprintf("unsubscribe reply id=%d", f.ID)
	case f.Publish != nil:
		return fmt.Sprintf("publish reply id=%d", f.ID)
	case f.Sync != nil:
		return fmt.Sprintf("sync reply id=%d channels=%v", f.ID, f.Sync.Channels)
	case f.Pong != nil:
		return fmt.Sprintf("pong id=%d", f.ID)
	case f.Push != nil && f.Push.Unsubscribed != nil:
		return fmt.Sprintf("unsubscribed push for %q", f.Push.Channel)
	case f.Push != nil:
		return fmt.Sprintf("push for %q", f.Push.Channel)
	case f.Error != nil:
		return fmt.Sprintf("error %d id=%d", f.Error.Code, f.ID)
	case f.Disconnect != nil:
		return fmt.Sprintf("disconnect %d reconnect=%t", f.Disconnect.Code, f.Disconnect.Reconnect)
	default:
		return "an unrecognised frame"
	}
}

// wsClient is the browser side of one connection: a real gorilla/websocket over a real
// socket, with just enough of docs/03-client-protocol.md to drive the suite.
type wsClient struct {
	t  *testing.T
	ws *websocket.Conn

	// id is the client id from the connect reply. It is what an application puts in
	// exclude and what a control disconnect names (FR-13, FR-18).
	id string

	// seq numbers commands. Every command carries a positive id, so every reply can be
	// matched to the command that caused it.
	seq atomic.Int64
}

// dial opens a websocket to this replica with the allowed Origin, failing the test if the
// handshake is refused.
func (r *replica) dial() *wsClient {
	r.t.Helper()
	c, status, err := r.dialOrigin(testOrigin)
	if err != nil {
		r.t.Fatalf("replica %d: dial: %v (handshake status %d)", r.index, err, status)
	}
	return c
}

// dialOrigin opens a websocket with an explicit Origin and returns the handshake status
// alongside the connection. An origin of "" sends no header at all.
//
// The handshake response body is drained and closed on both paths: a refused handshake
// still carries one, and leaking it holds a connection out of the transport's pool for
// the life of the test binary.
func (r *replica) dialOrigin(origin string) (*wsClient, int, error) {
	r.t.Helper()
	header := http.Header{}
	if origin != "" {
		header.Set("Origin", origin)
	}
	header.Set("Cookie", testCookie)
	header.Set("User-Agent", "sidecartunnel-integration")

	ws, resp, err := websocket.DefaultDialer.Dial(r.wsURL(), header)
	status := 0
	if resp != nil {
		status = resp.StatusCode
		_ = resp.Body.Close()
	}
	if err != nil {
		return nil, status, err
	}
	c := &wsClient{t: r.t, ws: ws}
	r.t.Cleanup(c.close)
	return c, status, nil
}

// connect performs the handshake and returns the reply. It is the first frame on every
// connection (docs/03-client-protocol.md §4.1).
func (c *wsClient) connect(subs ...string) proto.ConnectReply {
	c.t.Helper()
	id := c.next()
	c.send(map[string]any{"id": id, "connect": map[string]any{"subs": subs}})
	f := c.read()
	if f.Connect == nil {
		c.t.Fatalf("first frame is %s, want a connect reply", f)
	}
	if f.ID != id {
		c.t.Fatalf("connect reply id = %d, want %d", f.ID, id)
	}
	c.id = f.Connect.Client
	return *f.Connect
}

// subscribe subscribes to one channel and asserts the reply is the empty success object.
func (c *wsClient) subscribe(channel string) {
	c.t.Helper()
	f := c.subscribeFrame(channel)
	if f.Subscribe == nil {
		c.t.Fatalf("subscribe %q answered with %s, want a subscribe reply", channel, f)
	}
}

// subscribeFrame subscribes and returns whatever came back, for the tests that expect a
// refusal.
func (c *wsClient) subscribeFrame(channel string) frame {
	c.t.Helper()
	id := c.next()
	c.send(map[string]any{"id": id, "subscribe": map[string]any{"channel": channel}})
	f := c.read()
	if f.ID != id {
		c.t.Fatalf("reply to subscribe %q has id %d, want %d (%s)", channel, f.ID, id, f)
	}
	return f
}

// unsubscribe drops one channel and asserts the reply.
func (c *wsClient) unsubscribe(channel string) {
	c.t.Helper()
	id := c.next()
	c.send(map[string]any{"id": id, "unsubscribe": map[string]any{"channel": channel}})
	f := c.read()
	if f.Unsubscribe == nil {
		c.t.Fatalf("unsubscribe %q answered with %s, want an unsubscribe reply", channel, f)
	}
}

// sync asks the gateway for its authoritative subscription set for this connection. It is
// the only way a client can discover a subscription the gateway dropped (M16).
func (c *wsClient) sync() []string {
	c.t.Helper()
	id := c.next()
	c.send(map[string]any{"id": id, "sync": map[string]any{}})
	for {
		f := c.read()
		if f.Sync != nil {
			return f.Sync.Channels
		}
		if f.Push == nil {
			c.t.Fatalf("sync answered with %s, want a sync reply", f)
		}
	}
}

// ping round-trips an application-level ping. It is how a test asserts a connection is
// still alive and still serving commands, without waiting on anything to arrive.
func (c *wsClient) ping() {
	c.t.Helper()
	id := c.next()
	c.send(map[string]any{"id": id, "ping": map[string]any{}})
	for {
		f := c.read()
		if f.Pong != nil {
			if f.ID != id {
				c.t.Fatalf("pong id = %d, want %d", f.ID, id)
			}
			return
		}
		if f.Push == nil {
			c.t.Fatalf("ping answered with %s, want a pong", f)
		}
	}
}

// nextPush reads until the next push, failing on anything else.
func (c *wsClient) nextPush() proto.Push {
	c.t.Helper()
	f := c.read()
	if f.Push == nil {
		c.t.Fatalf("read %s, want a push", f)
	}
	return *f.Push
}

// wantPub reads the next push and asserts its channel and event.
func (c *wsClient) wantPub(channel, event string) proto.Pub {
	c.t.Helper()
	push := c.nextPush()
	if push.Channel != channel {
		c.t.Fatalf("push is for channel %q, want %q", push.Channel, channel)
	}
	if push.Pub == nil {
		c.t.Fatalf("push for %q carries no pub", channel)
	}
	if push.Pub.Event != event {
		c.t.Fatalf("push for %q carries event %q, want %q", channel, push.Pub.Event, event)
	}
	return *push.Pub
}

// send writes one command frame.
func (c *wsClient) send(v any) {
	c.t.Helper()
	data, err := json.Marshal(v)
	if err != nil {
		c.t.Fatalf("marshal command: %v", err)
	}
	if err := c.ws.SetWriteDeadline(time.Now().Add(readBudget)); err != nil {
		c.t.Fatalf("set write deadline: %v", err)
	}
	if err := c.ws.WriteMessage(websocket.TextMessage, data); err != nil {
		c.t.Fatalf("write command: %v", err)
	}
}

// read returns the next frame, failing the test rather than hanging.
func (c *wsClient) read() frame {
	c.t.Helper()
	return c.readWithin(readBudget)
}

// readWithin returns the next frame, failing if it does not arrive inside budget. FR-18
// is stated as a deadline — "within one second" — and this is how that is asserted rather
// than assumed.
func (c *wsClient) readWithin(budget time.Duration) frame {
	c.t.Helper()
	data, err := c.raw(budget)
	if err != nil {
		c.t.Fatalf("read: %v", err)
	}
	var f frame
	if err := json.Unmarshal(data, &f); err != nil {
		c.t.Fatalf("decode frame %s: %v", data, err)
	}
	return f
}

// raw reads one message, returning the error rather than failing, for the callers that
// expect the socket to end.
func (c *wsClient) raw(budget time.Duration) ([]byte, error) {
	if err := c.ws.SetReadDeadline(time.Now().Add(budget)); err != nil {
		return nil, fmt.Errorf("set read deadline: %w", err)
	}
	_, data, err := c.ws.ReadMessage()
	if err != nil {
		return nil, err
	}
	return data, nil
}

// wantDisconnect reads until the disconnect frame, then reads once more to collect the
// websocket close code, and asserts both.
//
// Both are asserted because docs/03-client-protocol.md §5.2 makes the frame authoritative
// when present and the close code the fallback when it is not. A gateway that sent one
// and not the other would leave half the clients in the world uninformed.
func (c *wsClient) wantDisconnect(want proto.CloseCode) proto.Disconnect {
	c.t.Helper()
	return c.wantDisconnectWithin(want, readBudget)
}

// wantDisconnectWithin is wantDisconnect with an explicit deadline.
func (c *wsClient) wantDisconnectWithin(want proto.CloseCode, budget time.Duration) proto.Disconnect {
	c.t.Helper()
	deadline := time.Now().Add(budget)
	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			c.t.Fatalf("no disconnect frame with code %d within %s", want, budget)
		}
		data, err := c.raw(remaining)
		if err != nil {
			c.t.Fatalf("read: %v, want a disconnect frame with code %d", err, want)
		}
		var f frame
		if err := json.Unmarshal(data, &f); err != nil {
			c.t.Fatalf("decode frame %s: %v", data, err)
		}
		if f.Disconnect == nil {
			continue
		}
		if f.Disconnect.Code != want {
			c.t.Fatalf("disconnect code = %d, want %d", f.Disconnect.Code, want)
		}
		if code := c.closeCode(); code != want {
			c.t.Fatalf("websocket close code = %d, want %d", code, want)
		}
		return *f.Disconnect
	}
}

// closeCode reads until the socket closes and returns the websocket close code.
func (c *wsClient) closeCode() proto.CloseCode {
	c.t.Helper()
	deadline := time.Now().Add(readBudget)
	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			c.t.Fatalf("no websocket close frame within %s", readBudget)
		}
		_, err := c.raw(remaining)
		if err == nil {
			continue
		}
		var closeErr *websocket.CloseError
		if errors.As(err, &closeErr) {
			return proto.CloseCode(closeErr.Code)
		}
		c.t.Fatalf("read: %v, want a websocket close frame", err)
		return 0
	}
}

// close ends the socket from the client side. It is registered by t.Cleanup for every
// client the suite dials, so a test that ends early does not leave a connection open into
// the next one.
func (c *wsClient) close() { _ = c.ws.Close() }

// next is the id for the next command.
func (c *wsClient) next() int64 { return c.seq.Add(1) }

// statusOf asserts that a handshake was refused and returns the status it was refused
// with.
//
// The status is the whole answer for a check that completes before the upgrade: there is
// no websocket on which a close code could be sent (docs/03-client-protocol.md §2, §7).
func statusOf(t *testing.T, status int, err error) int {
	t.Helper()
	if err == nil {
		t.Fatal("the handshake succeeded; want it refused")
	}
	if !errors.Is(err, websocket.ErrBadHandshake) {
		t.Fatalf("dial error = %v, want a refused handshake", err)
	}
	if status == 0 {
		t.Fatal("a refused handshake carried no response")
	}
	return status
}
