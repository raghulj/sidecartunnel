package conn

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/raghulj/sidecartunnel/internal/glob"
	"github.com/raghulj/sidecartunnel/internal/proto"
)

// TestConn_ImplementsSink pins the contract internal/hub is written against. Sink is
// declared in this package and is structurally identical to hub.Sink, so the two build
// and test apart (docs/09-internals.md §1).
func TestConn_ImplementsSink(t *testing.T) {
	var _ Sink = (*Conn)(nil)
}

func TestNew_RejectsMissingSeams(t *testing.T) {
	full := func() Options {
		return Options{Socket: newFakeSocket(), Registry: newFakeRegistry(), Authorizer: okAuth(t, "u")}
	}
	tests := []struct {
		name string
		opts func() Options
		want string
	}{
		{"no socket", func() Options { o := full(); o.Socket = nil; return o }, "Socket"},
		{"no registry", func() Options { o := full(); o.Registry = nil; return o }, "Registry"},
		{"no authorizer", func() Options { o := full(); o.Authorizer = nil; return o }, "Authorizer"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := New(tt.opts())
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("New err = %v, want one naming %q", err, tt.want)
			}
		})
	}
}

func TestNew_GeneratesSixteenHexID(t *testing.T) {
	c, err := New(Options{Socket: newFakeSocket(), Registry: newFakeRegistry(), Authorizer: okAuth(t, "u")})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if !regexp.MustCompile(`^[0-9a-f]{16}$`).MatchString(c.ID()) {
		t.Fatalf("ID = %q, want 16 hex characters", c.ID())
	}
	other, err := New(Options{Socket: newFakeSocket(), Registry: newFakeRegistry(), Authorizer: okAuth(t, "u")})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if other.ID() == c.ID() {
		t.Fatal("two connections were given the same id")
	}
}

func TestNew_KeepsSuppliedID(t *testing.T) {
	c, err := New(Options{ID: "cafebabecafebabe", Socket: newFakeSocket(), Registry: newFakeRegistry(), Authorizer: okAuth(t, "u")})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if c.ID() != "cafebabecafebabe" {
		t.Fatalf("ID = %q, want the supplied one", c.ID())
	}
}

// TestConn_UserFixedForLife covers Sink's contract: empty until the webhook answers,
// then fixed, and readable after close (FR-18 targets revocation by it).
func TestConn_UserFixedForLife(t *testing.T) {
	r := newRig(t, func(o *Options) { o.Authorizer = okAuth(t, "user-42", "room-*") })
	if got := r.conn.User(); got != "" {
		t.Fatalf("User before connect = %q, want empty", got)
	}
	r.connect()
	if got := r.conn.User(); got != "user-42" {
		t.Fatalf("User = %q, want user-42", got)
	}
	r.conn.Close(proto.CloseRevoked, "revoked")
	if got := r.conn.User(); got != "user-42" {
		t.Fatalf("User after close = %q, want user-42", got)
	}
}

// TestHandshakeTimeout_Closes3001_FR4: a socket opened and left silent closes 3001,
// reconnect false.
func TestHandshakeTimeout_Closes3001_FR4(t *testing.T) {
	r := newRig(t, func(o *Options) { o.HandshakeTimeout = 5 * time.Second })
	r.clk.Advance(5 * time.Second)

	got := r.wantDisconnect(proto.CloseHandshakeTimeout)
	if got.Reconnect {
		t.Fatal("3001 must not be retryable: a client that retries it loops forever")
	}
	if got.RetryAfter != 0 {
		t.Fatalf("retry_after = %d on a non-retryable close, want 0", got.RetryAfter)
	}
	if code := r.sock.closeCode(t); code != proto.CloseHandshakeTimeout {
		t.Fatalf("websocket close code = %d, want 3001", code)
	}
}

// TestHandshakeTimeout_StopsAtTheFrame_FR4 is the C2 regression: the timer covers only
// receipt of the connect frame. A connection whose authorization is slow must not close
// 3001 — conflating the two locks every reconnecting user out permanently.
func TestHandshakeTimeout_StopsAtTheFrame_FR4(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	r := newRig(t, func(o *Options) {
		o.HandshakeTimeout = 5 * time.Second
		o.Authorizer = AuthorizerFunc(func(context.Context, []string) (Authorization, error) {
			close(entered)
			<-release
			return Authorization{User: "u", Grants: mustGrants(t, "room-*"), ExpiresIn: time.Hour}, nil
		})
	})

	r.sock.feed(map[string]any{"id": 1, "connect": map[string]any{}})
	select {
	case <-entered:
	case <-time.After(failAfter):
		t.Fatal("authorization never started")
	}

	// The frame has arrived, so the handshake timer is spent even though authorization
	// is still outstanding.
	r.clk.Advance(time.Hour)
	close(release)

	frame := r.sock.nextWrite(t)
	if _, ok := frame["connect"]; !ok {
		t.Fatalf("first frame after a slow authorization = %v, want a connect reply, not a 3001", frame)
	}
}

// TestAuthorizationFailure_FR6 asserts the two outcomes never share a code.
func TestAuthorizationFailure_FR6(t *testing.T) {
	tests := []struct {
		name          string
		err           error
		wantCode      proto.CloseCode
		wantReconnect bool
		wantRetry     bool
	}{
		{"refused", fmt.Errorf("webhook 403: %w", ErrUnauthorized), proto.CloseUnauthorized, false, false},
		{"unavailable", errors.New("webhook 500"), proto.CloseAuthUnavailable, true, true},
		{"deadline", context.DeadlineExceeded, proto.CloseAuthUnavailable, true, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := newRig(t, func(o *Options) {
				o.Authorizer = AuthorizerFunc(func(context.Context, []string) (Authorization, error) {
					return Authorization{}, tt.err
				})
			})
			r.sock.feed(map[string]any{"id": 1, "connect": map[string]any{}})

			got := r.wantDisconnect(tt.wantCode)
			if got.Reconnect != tt.wantReconnect {
				t.Fatalf("reconnect = %v, want %v", got.Reconnect, tt.wantReconnect)
			}
			if (got.RetryAfter > 0) != tt.wantRetry {
				t.Fatalf("retry_after = %d, want positive = %v", got.RetryAfter, tt.wantRetry)
			}
			if code := r.sock.closeCode(t); code != tt.wantCode {
				t.Fatalf("websocket close code = %d, want %d", code, tt.wantCode)
			}
		})
	}
}

// TestPingTimeout_Closes3004_FR7: no pong within pong_timeout of a protocol ping.
func TestPingTimeout_Closes3004_FR7(t *testing.T) {
	r := newRig(t, func(o *Options) {
		o.PingInterval = 25 * time.Second
		o.PongTimeout = 10 * time.Second
	})
	r.connect()

	r.clk.Advance(25 * time.Second)
	if c := r.sock.nextControl(t); c.kind != websocket.PingMessage {
		t.Fatalf("control frame kind = %d, want a ping", c.kind)
	}
	r.clk.waitAlarms(t, 1) // the pong deadline
	r.clk.Advance(10 * time.Second)

	got := r.wantDisconnect(proto.ClosePingTimeout)
	if !got.Reconnect {
		t.Fatal("3004 is transient and must be retryable")
	}
	if code := r.sock.closeCode(t); code != proto.ClosePingTimeout {
		t.Fatalf("websocket close code = %d, want 3004", code)
	}
}

// TestPong_KeepsConnectionOpen_FR7 is the other half: a client that answers survives.
func TestPong_KeepsConnectionOpen_FR7(t *testing.T) {
	r := newRig(t, func(o *Options) {
		o.PingInterval = 25 * time.Second
		o.PongTimeout = 10 * time.Second
	})
	r.connect()

	r.clk.Advance(25 * time.Second)
	if c := r.sock.nextControl(t); c.kind != websocket.PingMessage {
		t.Fatalf("control frame kind = %d, want a ping", c.kind)
	}
	r.clk.waitAlarms(t, 1)

	// Queue the pong, then a ping command behind it. Its pong reply proves the reader
	// has already run the pong handler — FIFO on one goroutine, no sleep required.
	r.sock.reads <- socketRead{pong: true}
	r.sock.feed(map[string]any{"id": 7, "ping": map[string]any{}})
	if _, ok := r.sock.nextWrite(t)["pong"]; !ok {
		t.Fatal("no pong reply")
	}

	r.clk.Advance(10 * time.Second)
	r.sock.feed(map[string]any{"id": 8, "ping": map[string]any{}})
	frame := r.sock.nextWrite(t)
	if _, ok := frame["pong"]; !ok {
		t.Fatalf("frame after a healthy pong = %v, want the connection still open", frame)
	}

	// The next interval must replace the spent pong deadline rather than stack a second
	// one: a connection that accumulates one alarm per ping leaks them for its whole life.
	r.clk.Advance(15 * time.Second)
	if c := r.sock.nextControl(t); c.kind != websocket.PingMessage {
		t.Fatalf("control frame kind = %d, want a second ping", c.kind)
	}
}

// TestDrain_WriteFailureSkipsTheCloseFrame — when the tail of the queue cannot be written
// there is no point writing a close frame after it, and the socket must still be closed
// so the reader unblocks and Run returns (NFR-3).
func TestDrain_WriteFailureSkipsTheCloseFrame(t *testing.T) {
	r := newRig(t, func(o *Options) { o.OutboundQueue = 8 })
	r.connect()

	r.sock.blockWrites()
	if !r.conn.Send(&proto.Frame{Data: []byte(`{"push":{"channel":"room-1"}}`)}) {
		t.Fatal("Send was refused on an idle queue")
	}
	r.sock.waitParked(t)
	if !r.conn.Send(&proto.Frame{Data: []byte(`{"push":{"channel":"room-2"}}`)}) {
		t.Fatal("Send was refused with room in the queue")
	}

	r.conn.Close(proto.CloseRevoked, "revoked")
	r.sock.failWrite.Store(true)
	r.sock.openGate()

	select {
	case <-r.done:
	case <-time.After(failAfter):
		t.Fatal("Run did not return")
	}
	select {
	case c := <-r.sock.control:
		t.Fatalf("a close frame was written after the socket stopped accepting writes: %v", c)
	default:
	}
}

// TestClose_IsIdempotentUnderRace closes from many goroutines at once. The socket must be
// closed exactly once and exactly one disconnect frame must reach the client.
func TestClose_IsIdempotentUnderRace(t *testing.T) {
	r := newRig(t)
	r.connect()

	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			r.conn.Close(proto.CloseRevoked, fmt.Sprintf("revoked %d", i))
		}()
	}
	close(start)
	wg.Wait()

	r.wantDisconnect(proto.CloseRevoked)
	select {
	case <-r.done:
	case <-time.After(failAfter):
		t.Fatal("Run did not return")
	}
	if got := r.sock.closeCalls.Load(); got != 1 {
		t.Fatalf("socket Close called %d times, want 1", got)
	}
	select {
	case extra := <-r.sock.writes:
		t.Fatalf("a second frame was written: %s", extra)
	default:
	}
}

// TestSend_AfterCloseReturnsFalse — Sink: a race between fan-out and close is normal
// and must not be an error path.
func TestSend_AfterCloseReturnsFalse(t *testing.T) {
	r := newRig(t)
	r.connect()
	r.conn.Close(proto.CloseRevoked, "revoked")
	select {
	case <-r.done:
	case <-time.After(failAfter):
		t.Fatal("Run did not return")
	}
	if r.conn.Send(&proto.Frame{Data: []byte(`{"push":{}}`)}) {
		t.Fatal("Send on a closed connection returned true")
	}
}

// TestSlowConsumer_Closes3005_FR15 is the requirement's own acceptance criterion: the
// client that stops reading is disconnected, AND another client on the same channel
// receives every message throughout. Without the second half this test passes with the
// bug it exists to catch — a blocking send — still present.
func TestSlowConsumer_Closes3005_FR15(t *testing.T) {
	const queue = 8
	const messages = queue + 24

	reg := newFakeRegistry()
	slow := newRig(t, func(o *Options) { o.Registry = reg; o.OutboundQueue = queue })
	// The healthy client keeps the default queue depth: the assertion below is that a
	// well-behaved subscriber misses nothing, not that it survives an undersized one.
	fast := newRig(t, func(o *Options) { o.Registry = reg })
	slow.connect()
	fast.connect()

	// The slow client stops reading: its writer parks inside WriteMessage.
	slow.sock.blockWrites()

	frames := make([]*proto.Frame, messages)
	for i := range frames {
		frames[i] = &proto.Frame{Data: []byte(fmt.Sprintf(`{"push":{"channel":"room-1","pub":{"event":"e","data":%d}}}`, i))}
		reg.fanout(frames[i])
	}

	// The healthy client missed nothing.
	for i := range frames {
		got := fast.sock.nextWrite(t)
		var body struct {
			Pub struct {
				Data int `json:"data"`
			} `json:"pub"`
		}
		if err := json.Unmarshal(got["push"], &body); err != nil {
			t.Fatalf("push %d: %v", i, err)
		}
		if body.Pub.Data != i {
			t.Fatalf("healthy client received message %d, want %d — the fan-out path stalled", body.Pub.Data, i)
		}
	}

	slow.sock.openGate()
	if code := slow.sock.closeCode(t); code != proto.CloseSlowConsumer {
		t.Fatalf("slow client close code = %d, want 3005", code)
	}
}

// TestGrants_SwapWhileMatching_M2 is the defect M2 describes: grants guarded by a mutex,
// or a plainly-assigned slice, tears under -race the first time a revalidation swaps it.
func TestGrants_SwapWhileMatching_M2(t *testing.T) {
	// desk-* is in the connect answer and in every set the swapper installs, so a
	// subscribe to desk-1 must succeed on every single iteration. Anything else is a torn
	// read, which is the defect M2 names.
	r := newRig(t, func(o *Options) { o.Authorizer = okAuth(t, "u-1", "room-*", "desk-*") })
	r.connect()

	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			set, err := glob.NewSet([]string{fmt.Sprintf("room-%d-*", i), "desk-*"})
			if err != nil {
				return
			}
			r.conn.SetGrants(set)
		}
	}()

	for i := range 200 {
		r.sock.feed(map[string]any{"id": i + 10, "subscribe": map[string]any{"channel": "desk-1"}})
		frame := r.sock.nextWrite(t)
		if _, ok := frame["subscribe"]; !ok {
			t.Fatalf("frame %v: a grant that is in every set must always match", frame)
		}
	}
	close(stop)
	wg.Wait()
}

// TestFanout_FrameNeverCopiedOrMutated_M10: every connection gets the same backing array.
// A per-connection copy is 160 GiB at 20,000 connections and 256 queue slots.
func TestFanout_FrameNeverCopiedOrMutated_M10(t *testing.T) {
	reg := newFakeRegistry()
	rigs := make([]*rig, 5)
	for i := range rigs {
		rigs[i] = newRig(t, func(o *Options) { o.Registry = reg })
		rigs[i].connect()
	}

	frame := &proto.Frame{Data: []byte(`{"push":{"channel":"room-1","pub":{"event":"e","data":1}}}`)}
	before := string(frame.Data)
	reg.fanout(frame)

	for i, r := range rigs {
		select {
		case got := <-r.sock.writes:
			if &got[0] != &frame.Data[0] {
				t.Fatalf("connection %d received a copy of the shared buffer", i)
			}
		case <-time.After(failAfter):
			t.Fatalf("connection %d received nothing", i)
		}
	}
	if string(frame.Data) != before {
		t.Fatalf("the shared buffer was mutated: %q, want %q", frame.Data, before)
	}
}

// TestNoGoroutineLeaks_NFR3 — every connect/close cycle returns its two goroutines.
func TestNoGoroutineLeaks_NFR3(t *testing.T) {
	cycle := func() {
		sock := newFakeSocket()
		c, err := New(Options{
			Socket:     sock,
			Registry:   newFakeRegistry(),
			Authorizer: okAuth(t, "u", "room-*"),
			Clock:      newFakeClock(),
			Log:        slog.New(slog.DiscardHandler),
		})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		done := make(chan struct{})
		go func() {
			defer close(done)
			c.Run(context.Background())
		}()
		c.Close(proto.CloseDraining, "draining")
		select {
		case <-done:
		case <-time.After(failAfter):
			t.Fatal("Run did not return")
		}
	}

	cycle() // warm the runtime before the baseline
	base := runtime.NumGoroutine()
	for range 200 {
		cycle()
	}
	if got := runtime.NumGoroutine(); got > base+2 {
		t.Fatalf("goroutines = %d after 200 cycles, baseline %d", got, base)
	}
}

// TestExpiry_Closes3503_FR22: at expires_in the gateway closes retryably so the client
// re-handshakes with whatever cookie the browser currently holds.
func TestExpiry_Closes3503_FR22(t *testing.T) {
	r := newRig(t, func(o *Options) {
		o.Authorizer = AuthorizerFunc(func(context.Context, []string) (Authorization, error) {
			return Authorization{User: "u", Grants: mustGrants(t, "room-*"), ExpiresIn: time.Hour}, nil
		})
	})
	reply := r.connect()
	if reply.ExpiresIn != 3600 {
		t.Fatalf("expires_in = %d, want 3600", reply.ExpiresIn)
	}
	r.clk.waitAlarms(t, 1) // the expiry timer
	r.clk.Advance(time.Hour)

	got := r.wantDisconnect(proto.CloseExpired)
	if !got.Reconnect || got.RetryAfter <= 0 {
		t.Fatalf("disconnect = %+v, want reconnect true with a spread retry_after", got)
	}
}

// TestContextCancel_Closes3000 — shutdown sends the drain disconnect so clients apply
// backoff instead of stampeding (docs/09-internals.md §8).
func TestContextCancel_Closes3000(t *testing.T) {
	r := newRig(t)
	r.connect()
	r.cancel()

	got := r.wantDisconnect(proto.CloseDraining)
	if !got.Reconnect || got.RetryAfter <= 0 {
		t.Fatalf("disconnect = %+v, want reconnect true with a spread retry_after", got)
	}
}

// TestProtocolErrors_Close3006 — binary frames and oversize frames (§1, §7).
func TestProtocolErrors_Close3006(t *testing.T) {
	tests := []struct {
		name string
		read socketRead
	}{
		{"binary frame", socketRead{kind: websocket.BinaryMessage, data: []byte{0x01}}},
		{"oversize frame", socketRead{kind: websocket.TextMessage, data: []byte(`{"ping":{"pad":"` + strings.Repeat("x", 40000) + `"}}`)}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := newRig(t)
			r.connect()
			r.sock.reads <- tt.read

			got := r.wantDisconnect(proto.CloseProtocolError)
			if got.Reconnect {
				t.Fatal("3006 is not retryable: a client that speaks the protocol wrongly will do it again")
			}
		})
	}
}

// TestFrameBodyError_ClosesQuietly — a connection reset partway through a message leaves
// nobody to send a disconnect frame to.
func TestFrameBodyError_ClosesQuietly(t *testing.T) {
	r := newRig(t)
	r.connect()
	r.sock.reads <- socketRead{kind: websocket.TextMessage, bodyErr: errors.New("connection reset by peer")}
	select {
	case <-r.done:
	case <-time.After(failAfter):
		t.Fatal("Run did not return after a body read error")
	}
}

// TestReadError_ClosesWithoutADisconnectFrame — the peer is already gone, so there is
// nobody to tell. The socket must still be closed and Run must still return (NFR-3).
func TestReadError_ClosesWithoutADisconnectFrame(t *testing.T) {
	r := newRig(t)
	r.connect()
	r.sock.reads <- socketRead{err: errors.New("connection reset by peer")}

	select {
	case <-r.done:
	case <-time.After(failAfter):
		t.Fatal("Run did not return after a read error")
	}
	select {
	case frame := <-r.sock.writes:
		t.Fatalf("a frame was written to a dead peer: %s", frame)
	default:
	}
	select {
	case c := <-r.sock.control:
		t.Fatalf("a control frame was written to a dead peer: %v", c)
	default:
	}
}

// TestWriteFailure_EndsTheConnection covers every failure path the writer has: the write
// deadline, the frame write, the ping control frame, the close control frame, and the
// socket close itself. Each must end the connection rather than park a goroutine (NFR-3).
func TestWriteFailure_EndsTheConnection(t *testing.T) {
	tests := []struct {
		name  string
		drive func(*rig)
	}{
		{"write deadline", func(r *rig) {
			r.sock.failDeadline.Store(true)
			r.sock.feed(map[string]any{"id": 1, "connect": map[string]any{}})
		}},
		{"frame write", func(r *rig) {
			r.sock.failWrite.Store(true)
			r.sock.feed(map[string]any{"id": 1, "connect": map[string]any{}})
		}},
		{"ping control frame", func(r *rig) {
			r.connect()
			r.sock.failControl.Store(true)
			r.clk.Advance(25 * time.Second)
		}},
		{"close control frame", func(r *rig) {
			r.connect()
			r.sock.failControl.Store(true)
			r.conn.Close(proto.CloseRevoked, "revoked")
		}},
		{"socket close", func(r *rig) {
			r.sock.failClose.Store(true)
			r.conn.Close(proto.CloseRevoked, "revoked")
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := newRig(t, func(o *Options) { o.PingInterval = 25 * time.Second })
			tt.drive(r)
			select {
			case <-r.done:
			case <-time.After(failAfter):
				t.Fatal("Run did not return after a write failure")
			}
		})
	}
}

// TestClose_ReasonTruncated — RFC 6455 caps a control frame payload at 125 bytes, two of
// which are the close code. A longer reason must be cut, not dropped on the floor by the
// socket.
func TestClose_ReasonTruncated(t *testing.T) {
	r := newRig(t)
	r.connect()
	r.conn.Close(proto.CloseRevoked, strings.Repeat("x", 400))
	r.wantDisconnect(proto.CloseRevoked)

	for {
		c := r.sock.nextControl(t)
		if c.kind != websocket.CloseMessage {
			continue
		}
		if len(c.data) > 125 {
			t.Fatalf("close frame payload is %d bytes, want at most 125", len(c.data))
		}
		return
	}
}

// TestOffer_RefusesNilFrame — encode returns nil when a frame cannot be serialized, and
// nothing downstream may dereference it.
func TestOffer_RefusesNilFrame(t *testing.T) {
	c, err := New(Options{Socket: newFakeSocket(), Registry: newFakeRegistry(), Authorizer: okAuth(t, "u")})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if c.offer(nil) {
		t.Fatal("offer(nil) returned true")
	}
	if c.Send(nil) {
		t.Fatal("Send(nil) returned true")
	}
}

// TestConnect_NoExpiryArmsNoTimer — an authorization with no lifetime must not arm an
// expiry that fires immediately and closes the connection it just opened (FR-22).
func TestConnect_NoExpiryArmsNoTimer(t *testing.T) {
	r := newRig(t, func(o *Options) {
		o.Authorizer = AuthorizerFunc(func(context.Context, []string) (Authorization, error) {
			return Authorization{User: "u", Grants: mustGrants(t, "room-*")}, nil
		})
	})
	if got := r.connect().ExpiresIn; got != 0 {
		t.Fatalf("expires_in = %d, want 0", got)
	}
	r.clk.Advance(24 * time.Hour)
	r.sock.feed(map[string]any{"id": 2, "ping": map[string]any{}})
	if _, ok := r.sock.nextWrite(t)["pong"]; !ok {
		t.Fatal("the connection closed although no expiry was armed")
	}
}

// TestUnsubscribed_Push_FR17 — a withdrawn subscription is announced, never dropped
// silently: silence is indistinguishable from a quiet channel.
func TestUnsubscribed_Push_FR17(t *testing.T) {
	r := newRig(t)
	r.connect()
	if !r.conn.Unsubscribed("room-1", "grant revoked") {
		t.Fatal("Unsubscribed returned false on an idle queue")
	}
	frame := r.sock.nextWrite(t)
	var push proto.Push
	if err := json.Unmarshal(frame["push"], &push); err != nil {
		t.Fatalf("push: %v", err)
	}
	if push.Channel != "room-1" || push.Unsubscribed == nil || push.Unsubscribed.Reason != "grant revoked" {
		t.Fatalf("push = %+v, want an unsubscribed push for room-1", push)
	}
}

// TestRetryAfter_PerCloseCode covers docs/03-client-protocol.md §7 and §7.1.
func TestRetryAfter_PerCloseCode(t *testing.T) {
	c, err := New(Options{
		ID: "0123456789abcdef", Socket: newFakeSocket(), Registry: newFakeRegistry(),
		Authorizer: okAuth(t, "u"), RetrySpread: 60 * time.Second,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	tests := []struct {
		code      proto.CloseCode
		reconnect bool
		spread    bool
		fixed     time.Duration
	}{
		{proto.CloseDraining, true, true, 0},
		{proto.CloseHandshakeTimeout, false, false, 0},
		{proto.CloseUnauthorized, false, false, 0},
		{proto.ClosePingTimeout, true, false, 0},
		{proto.CloseSlowConsumer, true, false, 0},
		{proto.CloseProtocolError, false, false, 0},
		{proto.CloseRateLimited, true, false, 60 * time.Second},
		{proto.CloseAuthUnavailable, true, true, 0},
		{proto.CloseRevoked, false, false, 0},
		{proto.CloseExpired, true, true, 0},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprint(tt.code), func(t *testing.T) {
			if got := reconnectable(tt.code); got != tt.reconnect {
				t.Fatalf("reconnectable(%d) = %v, want %v", tt.code, got, tt.reconnect)
			}
			got := c.retryAfter(tt.code)
			switch {
			case tt.spread:
				if got <= 0 || got >= 60*time.Second {
					t.Fatalf("retry_after = %v, want a value inside the 60s spread", got)
				}
			case tt.fixed > 0:
				if got != tt.fixed {
					t.Fatalf("retry_after = %v, want %v", got, tt.fixed)
				}
			default:
				if got != 0 {
					t.Fatalf("retry_after = %v, want 0", got)
				}
			}
		})
	}
}

// TestRetryAfter_SpreadDiffersPerConnection — the point of a spread is that two
// connections do not come back at the same instant.
func TestRetryAfter_SpreadDiffersPerConnection(t *testing.T) {
	newFor := func(id string) *Conn {
		c, err := New(Options{
			ID: id, Socket: newFakeSocket(), Registry: newFakeRegistry(),
			Authorizer: okAuth(t, "u"), RetrySpread: time.Minute,
		})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		return c
	}
	a := newFor("0123456789abcdef").retryAfter(proto.CloseDraining)
	b := newFor("fedcba9876543210").retryAfter(proto.CloseDraining)
	if a == b {
		t.Fatalf("both connections drew retry_after %v; the spread is not spreading", a)
	}
}

// TestRetryAfter_Injectable lets the server supply a fleet-aware spread.
func TestRetryAfter_Injectable(t *testing.T) {
	r := newRig(t, func(o *Options) {
		o.RetryAfter = func(proto.CloseCode) time.Duration { return 1234 * time.Millisecond }
	})
	r.connect()
	r.conn.Close(proto.CloseDraining, "draining")
	got := r.wantDisconnect(proto.CloseDraining)
	if got.RetryAfter != 1234 {
		t.Fatalf("retry_after = %d ms, want 1234", got.RetryAfter)
	}
}

// TestNFR7_LogsCarryNoSecrets drives a full connect at debug and asserts nothing
// sensitive reached the log. The cookie never enters this package at all — the
// Authorizer closes over it — which is the FR-22 half of the same rule.
func TestNFR7_LogsCarryNoSecrets(t *testing.T) {
	const payload = "s3cr3t-payload-value"
	const cookieish = "sessionid=abc123def456"

	r := newRig(t, func(o *Options) { o.Authorizer = okAuth(t, "u", "room-*") })
	r.connect("room-1")
	r.sock.feed(map[string]any{"id": 2, "publish": map[string]any{
		"channel": "room-1", "event": "typing", "data": map[string]any{"note": payload},
	}})
	r.sock.nextWrite(t)
	r.sock.feedRaw(`{"connect":{"cookie":"` + cookieish + `"},"subscribe":{}}`)
	r.wantError(proto.ErrBadRequest)

	logs := r.logs.String()
	for _, secret := range []string{payload, cookieish} {
		if strings.Contains(logs, secret) {
			t.Fatalf("NFR-7: log output contains %q:\n%s", secret, logs)
		}
	}
}
