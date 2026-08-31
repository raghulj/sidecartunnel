package conn

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/raghulj/sidecartunnel/internal/proto"
)

// serve starts an httptest server whose handler upgrades and runs a real Conn over a real
// gorilla socket. It is the compile-time and run-time proof that *websocket.Conn
// satisfies Socket with no adapter, and the only test that exercises SystemClock end to
// end — everything else injects a fake clock so it can be deterministic.
func serve(t *testing.T, tweak func(*Options)) (*websocket.Conn, *fakeRegistry) {
	t.Helper()
	reg := newFakeRegistry()
	ready := make(chan struct{})

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		up := websocket.Upgrader{ReadBufferSize: 2048, WriteBufferSize: 2048}
		sock, err := up.Upgrade(w, req, nil)
		if err != nil {
			t.Errorf("upgrade: %v", err)
			return
		}
		opts := Options{Socket: sock, Registry: reg, Authorizer: okAuth("u-1", "room-*")}
		if tweak != nil {
			tweak(&opts)
		}
		c, err := New(opts)
		if err != nil {
			t.Errorf("New: %v", err)
			return
		}
		close(ready)
		c.Run(req.Context())
	}))
	t.Cleanup(srv.Close)

	client, resp, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(srv.URL, "http"), nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	resp.Body.Close()
	t.Cleanup(func() { client.Close() })
	<-ready
	return client, reg
}

// readFrame reads one text frame from the client side.
func readFrame(t *testing.T, client *websocket.Conn) map[string]json.RawMessage {
	t.Helper()
	if err := client.SetReadDeadline(time.Now().Add(failAfter)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	_, data, err := client.ReadMessage()
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	var out map[string]json.RawMessage
	if err := json.Unmarshal(data, &out); err != nil {
		t.Fatalf("frame %q: %v", data, err)
	}
	return out
}

// TestEndToEnd_RealSocket drives the whole §9 worked exchange over a real websocket.
func TestEndToEnd_RealSocket(t *testing.T) {
	client, reg := serve(t, nil)

	if err := client.WriteJSON(map[string]any{"id": 1, "connect": map[string]any{"subs": []string{"room-4410", "org-99-secret"}}}); err != nil {
		t.Fatalf("write connect: %v", err)
	}
	var reply proto.ConnectReply
	if err := json.Unmarshal(readFrame(t, client)["connect"], &reply); err != nil {
		t.Fatalf("connect reply: %v", err)
	}
	if len(reply.Client) != 16 {
		t.Fatalf("client = %q, want 16 hex characters", reply.Client)
	}
	// docs/03-client-protocol.md §9: the ungranted channel is omitted, not an error.
	if _, ok := reply.Subs["room-4410"]; !ok || len(reply.Subs) != 1 {
		t.Fatalf("subs = %v, want only room-4410", reply.Subs)
	}

	// A fan-out reaches the real socket through the writer goroutine.
	reg.fanout(&proto.Frame{Data: []byte(`{"push":{"channel":"room-4410","pub":{"event":"order.created","data":{"id":88123}}}}`)})
	if _, ok := readFrame(t, client)["push"]; !ok {
		t.Fatal("no push")
	}

	if err := client.WriteJSON(map[string]any{"id": 9, "sync": map[string]any{}}); err != nil {
		t.Fatalf("write sync: %v", err)
	}
	if got := string(readFrame(t, client)["sync"]); got != `{"channels":["room-4410"]}` {
		t.Fatalf("sync = %s", got)
	}

	// The disconnect frame arrives immediately before the websocket close frame (§5.2).
	var closeCode int
	client.SetCloseHandler(func(code int, _ string) error {
		closeCode = code
		return nil
	})
	// Collected under the lock and closed after releasing it. Closing inline would
	// deadlock against Remove, which is precisely the rule in docs/09-internals.md §4.5.
	for _, s := range reg.snapshot() {
		s.Close(proto.CloseRevoked, "revoked")
	}

	var disconnect proto.Disconnect
	if err := json.Unmarshal(readFrame(t, client)["disconnect"], &disconnect); err != nil {
		t.Fatalf("disconnect: %v", err)
	}
	if disconnect.Code != proto.CloseRevoked || disconnect.Reconnect {
		t.Fatalf("disconnect = %+v, want 3501 with reconnect false", disconnect)
	}
	if _, _, err := client.ReadMessage(); err == nil {
		t.Fatal("the socket stayed open after the close frame")
	}
	if closeCode != int(proto.CloseRevoked) {
		t.Fatalf("websocket close code = %d, want 3501", closeCode)
	}
}

// TestEndToEnd_HandshakeTimeout_FR4 is the one test that runs the real clock's timer: a
// socket opened and left silent closes 3001.
func TestEndToEnd_HandshakeTimeout_FR4(t *testing.T) {
	client, _ := serve(t, func(o *Options) { o.HandshakeTimeout = 20 * time.Millisecond })

	var disconnect proto.Disconnect
	if err := json.Unmarshal(readFrame(t, client)["disconnect"], &disconnect); err != nil {
		t.Fatalf("disconnect: %v", err)
	}
	if disconnect.Code != proto.CloseHandshakeTimeout || disconnect.Reconnect {
		t.Fatalf("disconnect = %+v, want 3001 with reconnect false", disconnect)
	}
}

// TestEndToEnd_BinaryFrame_3006 — docs/03-client-protocol.md §1: all application frames
// are text frames; a binary one is a protocol error.
func TestEndToEnd_BinaryFrame_3006(t *testing.T) {
	client, _ := serve(t, nil)
	if err := client.WriteMessage(websocket.BinaryMessage, []byte{0x01, 0x02}); err != nil {
		t.Fatalf("write: %v", err)
	}
	var disconnect proto.Disconnect
	if err := json.Unmarshal(readFrame(t, client)["disconnect"], &disconnect); err != nil {
		t.Fatalf("disconnect: %v", err)
	}
	if disconnect.Code != proto.CloseProtocolError {
		t.Fatalf("close code = %d, want 3006", disconnect.Code)
	}
}

// TestEndToEnd_OversizeFrame_3006 — limits.max_frame_size, enforced by the socket's read
// limit before any bytes reach the decoder.
func TestEndToEnd_OversizeFrame_3006(t *testing.T) {
	client, _ := serve(t, func(o *Options) { o.MaxFrameSize = 64 })
	if err := client.WriteJSON(map[string]any{"id": 1, "ping": map[string]any{"pad": strings.Repeat("x", 512)}}); err != nil {
		t.Fatalf("write: %v", err)
	}
	var disconnect proto.Disconnect
	if err := json.Unmarshal(readFrame(t, client)["disconnect"], &disconnect); err != nil {
		t.Fatalf("disconnect: %v", err)
	}
	if disconnect.Code != proto.CloseProtocolError {
		t.Fatalf("close code = %d, want 3006", disconnect.Code)
	}
}

// TestEndToEnd_ContextCancelDrains — the http request context is cancelled when the
// server shuts down, and every connection must be told rather than reset (§8).
func TestEndToEnd_ContextCancelDrains(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	reg := newFakeRegistry()
	ready := make(chan struct{})

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		sock, err := (&websocket.Upgrader{}).Upgrade(w, req, nil)
		if err != nil {
			t.Errorf("upgrade: %v", err)
			return
		}
		c, err := New(Options{Socket: sock, Registry: reg, Authorizer: okAuth("u-1", "room-*")})
		if err != nil {
			t.Errorf("New: %v", err)
			return
		}
		close(ready)
		c.Run(ctx)
	}))
	defer srv.Close()

	client, resp, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(srv.URL, "http"), nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	resp.Body.Close()
	defer client.Close()
	<-ready
	cancel()

	var disconnect proto.Disconnect
	if err := json.Unmarshal(readFrame(t, client)["disconnect"], &disconnect); err != nil {
		t.Fatalf("disconnect: %v", err)
	}
	if disconnect.Code != proto.CloseDraining || !disconnect.Reconnect || disconnect.RetryAfter <= 0 {
		t.Fatalf("disconnect = %+v, want 3000, reconnect true, with a retry_after", disconnect)
	}
}
