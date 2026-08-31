package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/raghulj/sidecartunnel/internal/bus"
	"github.com/raghulj/sidecartunnel/internal/config"
	"github.com/raghulj/sidecartunnel/internal/conn"
	"github.com/raghulj/sidecartunnel/internal/glob"
	"github.com/raghulj/sidecartunnel/internal/hub"
	"github.com/raghulj/sidecartunnel/internal/proto"
	"github.com/raghulj/sidecartunnel/internal/webhook"
)

// failAfter is the generous failure detector docs/14-coding-standards.md §2 allows in
// place of a sleep: the happy path takes microseconds and the timeout only fires when the
// test was going to fail anyway.
const failAfter = 3 * time.Second

// testOrigin is the one origin every rig allows. FR-2 is an exact string match, so the
// tests that matter are the ones that vary this by one character.
const testOrigin = "https://app.example.com"

// ---------------------------------------------------------------------------
// fake clock
// ---------------------------------------------------------------------------

// fakeClock is a deterministic conn.Clock. Tests move time with Advance and never sleep.
//
// It starts at the real wall clock rather than at a fixed date, because a Conn turns
// clock.Now() into a socket write deadline on a real socket: a fake epoch in the past
// would make every write fail with a deadline that has already passed, and a fake epoch
// in the future would disable write timeouts entirely.
type fakeClock struct {
	mu      sync.Mutex
	now     time.Time
	alarms  []*fakeAlarm
	created chan struct{}
}

func newFakeClock() *fakeClock {
	return &fakeClock{now: time.Now(), created: make(chan struct{}, 256)}
}

func (f *fakeClock) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.now
}

func (f *fakeClock) NewTimer(d time.Duration) conn.Alarm  { return f.newAlarm(d, 0) }
func (f *fakeClock) NewTicker(d time.Duration) conn.Alarm { return f.newAlarm(d, d) }

func (f *fakeClock) newAlarm(d, period time.Duration) conn.Alarm {
	f.mu.Lock()
	a := &fakeAlarm{clk: f, c: make(chan time.Time, 1), at: f.now.Add(d), period: period}
	f.alarms = append(f.alarms, a)
	f.mu.Unlock()
	select {
	case f.created <- struct{}{}:
	default:
	}
	return a
}

// Advance moves the clock forward and fires every alarm that is now due.
func (f *fakeClock) Advance(d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.now = f.now.Add(d)
	for _, a := range f.alarms {
		if a.stopped || a.at.After(f.now) {
			continue
		}
		select {
		case a.c <- f.now:
		default:
		}
		if a.period > 0 {
			a.at = f.now.Add(a.period)
			continue
		}
		a.stopped = true
	}
}

// waitAlarms blocks until n alarms have been armed, so a test never advances past a timer
// the code under test has not created yet. That is the synchronisation a sleep would
// otherwise be standing in for.
func (f *fakeClock) waitAlarms(t *testing.T, n int) {
	t.Helper()
	for i := range n {
		select {
		case <-f.created:
		case <-time.After(failAfter):
			t.Fatalf("only %d of %d alarms were armed", i, n)
		}
	}
}

type fakeAlarm struct {
	clk     *fakeClock
	c       chan time.Time
	at      time.Time
	period  time.Duration
	stopped bool
}

func (a *fakeAlarm) C() <-chan time.Time { return a.c }

func (a *fakeAlarm) Stop() {
	a.clk.mu.Lock()
	defer a.clk.mu.Unlock()
	a.stopped = true
}

// ---------------------------------------------------------------------------
// stub connect webhook
// ---------------------------------------------------------------------------

// stubWebhook stands in for *webhook.Client. It counts calls, which is not a detail: FR-2
// says a rejected Origin makes no application call at all, and the only way to assert
// "no call" is to hold something that would have recorded one.
type stubWebhook struct {
	mu     sync.Mutex
	calls  []webhook.Request
	result webhook.Result

	// respond overrides result when set, so a test can block inside the call or answer
	// differently per connection.
	respond func(ctx context.Context, req webhook.Request) webhook.Result
}

func newStubWebhook(grants ...string) *stubWebhook {
	set, err := glob.NewSet(grants)
	if err != nil {
		panic("stub grants must compile: " + err.Error())
	}
	return &stubWebhook{result: webhook.Authorized{User: "u-1", Grants: set, ExpiresIn: time.Hour}}
}

func (s *stubWebhook) Call(ctx context.Context, req webhook.Request) webhook.Result {
	s.mu.Lock()
	s.calls = append(s.calls, req)
	respond, result := s.respond, s.result
	s.mu.Unlock()
	if respond != nil {
		return respond(ctx, req)
	}
	return result
}

func (s *stubWebhook) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.calls)
}

func (s *stubWebhook) request(t *testing.T, i int) webhook.Request {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	if i >= len(s.calls) {
		t.Fatalf("webhook call %d was never made; %d calls recorded", i, len(s.calls))
	}
	return s.calls[i]
}

func (s *stubWebhook) answer(res webhook.Result) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.result = res
	s.respond = nil
}

func (s *stubWebhook) answerWith(fn func(ctx context.Context, req webhook.Request) webhook.Result) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.respond = fn
}

// ---------------------------------------------------------------------------
// rig
// ---------------------------------------------------------------------------

// rig is one server under test: a hub on a memory bus, a stub webhook, a fake clock, and
// an httptest listener speaking real websockets. Everything is torn down by t.Cleanup, so
// a leaked goroutine fails the run rather than the next test.
type rig struct {
	t    *testing.T
	cfg  *config.Config
	srv  *Server
	http *httptest.Server
	hub  *hub.Hub
	bus  *bus.MemoryBus
	web  *stubWebhook
	clk  *fakeClock
	logs *syncBuffer
}

// testConfig is the configuration every rig starts from: the documented defaults for the
// keys this package reads, with the one required key (server.allowed_origins) filled in.
func testConfig() *config.Config {
	return &config.Config{
		Server: config.Server{
			Listen:            "127.0.0.1:0",
			Path:              "/ws",
			AllowedOrigins:    []string{testOrigin},
			HandshakeTimeout:  config.Duration(5 * time.Second),
			PingInterval:      config.Duration(25 * time.Second),
			PongTimeout:       config.Duration(10 * time.Second),
			DrainTimeout:      config.Duration(20 * time.Second),
			DrainSpread:       config.Duration(60 * time.Second),
			ReadHeaderTimeout: config.Duration(5 * time.Second),
		},
		App: config.App{
			Name:           "app",
			ConnectTimeout: config.Duration(10 * time.Second),
			// The expiry clamps, because webhook.New refuses an unset app.max_expiry:
			// clampExpiry compares against the maximum first, so zero would give every
			// connection expires_in 0 and never arm the timer (FR-22).
			MinExpiry: config.Duration(60 * time.Second),
			MaxExpiry: config.Duration(6 * time.Hour),
		},
		Namespaces: []config.Namespace{{Name: "room", RateLimit: "10/s"}, {Name: "desk", ClientEvents: true, RateLimit: "10/s"}},
		Limits: config.Limits{
			MaxConnections:          25000,
			MaxSubscriptionsPerConn: 500,
			MaxConnectionsPerUser:   20,
			OutboundQueue:           256,
			MaxFrameSize:            16384,
			MaxChannelLength:        255,
			ReadBuffer:              2048,
			WriteBuffer:             2048,
		},
		Bus: config.Bus{Prefix: "st:"},
		Channels: config.Channels{
			Separator: "-",
		},
	}
}

// newRig builds a server behind an httptest listener.
func newRig(t *testing.T, mutate ...func(*config.Config)) *rig {
	t.Helper()
	r := newRigNoHTTP(t, mutate...)
	r.http = httptest.NewServer(r.srv.Handler())
	t.Cleanup(r.http.Close)
	return r
}

// newRigNoHTTP builds the same server without a listener, for the tests that drive Serve
// and ListenAndServe themselves.
func newRigNoHTTP(t *testing.T, mutate ...func(*config.Config)) *rig {
	t.Helper()
	return newRigWithOptions(t, nil, mutate...)
}

// newRigWithOptions is the builder for the tests that need to reach an Options field —
// the client id seam, most of all, which is the only way to force the collision the hub
// refuses (FR-18).
func newRigWithOptions(t *testing.T, mutateOpts func(*Options), mutate ...func(*config.Config)) *rig {
	t.Helper()
	cfg := testConfig()
	for _, m := range mutate {
		m(cfg)
	}

	b := memoryBus(t)
	h := hub.New(context.Background(), b, hub.Options{
		Prefix:                  cfg.Bus.Prefix,
		Separator:               cfg.Channels.Separator,
		Namespaces:              cfg.Namespaces,
		MaxSubscriptionsPerConn: cfg.Limits.MaxSubscriptionsPerConn,
	})
	t.Cleanup(h.Close)

	logs := &syncBuffer{}
	r := &rig{
		t:    t,
		cfg:  cfg,
		hub:  h,
		bus:  b,
		web:  newStubWebhook("room-*", "desk-*"),
		clk:  newFakeClock(),
		logs: logs,
	}

	opts := Options{
		Config:  cfg,
		Hub:     h,
		Bus:     b,
		Webhook: r.web,
		Clock:   r.clk,
		Log:     slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug})),
	}
	if mutateOpts != nil {
		mutateOpts(&opts)
	}
	srv, err := New(opts)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	r.srv = srv
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), failAfter)
		defer cancel()
		if err := srv.Drain(ctx); err != nil {
			t.Errorf("Drain: %v", err)
		}
	})
	return r
}

// wsURL is the rig's websocket endpoint.
func (r *rig) wsURL() string {
	return "ws" + strings.TrimPrefix(r.http.URL, "http") + r.cfg.Server.Path
}

// dial opens a websocket with the allowed Origin, failing the test if the handshake is
// refused.
func (r *rig) dial() *client {
	r.t.Helper()
	c, status, err := r.dialOrigin(testOrigin)
	if err != nil {
		r.t.Fatalf("dial: %v (status %d)", err, status)
	}
	return c
}

// dialOrigin opens a websocket with an explicit Origin header and returns the handshake
// status alongside the connection. An origin of "" sends no header at all, which is the
// case server.allow_missing_origin exists for.
//
// The response body is drained and closed here, on both paths, so no caller has to
// remember to: a refused handshake still carries one, and leaking it holds a connection
// from the client transport's pool for the life of the test binary.
func (r *rig) dialOrigin(origin string) (*client, int, error) {
	r.t.Helper()
	header := http.Header{}
	if origin != "" {
		header.Set("Origin", origin)
	}
	header.Set("Cookie", "session=secret-cookie-value")
	header.Set("User-Agent", "test-agent")

	ws, resp, err := websocket.DefaultDialer.Dial(r.wsURL(), header)
	if resp != nil {
		defer resp.Body.Close()
	}
	status := responseStatus(resp)
	if err != nil {
		return nil, status, err
	}
	c := &client{t: r.t, ws: ws}
	r.t.Cleanup(c.close)
	return c, status, nil
}

// responseStatus is the status of a handshake response, or 0 when there was none. A
// refused handshake still carries a response, and its status is the whole answer for the
// checks that complete before the upgrade.
func responseStatus(resp *http.Response) int {
	if resp == nil {
		return 0
	}
	return resp.StatusCode
}

// ---------------------------------------------------------------------------
// websocket client
// ---------------------------------------------------------------------------

// client is the browser side of one connection, with just enough of
// docs/03-client-protocol.md to drive the tests.
type client struct {
	t  *testing.T
	ws *websocket.Conn
}

func (c *client) close() {
	_ = c.ws.Close()
}

// send writes one command frame.
func (c *client) send(v any) {
	c.t.Helper()
	data, err := json.Marshal(v)
	if err != nil {
		c.t.Fatalf("marshal command: %v", err)
	}
	if err := c.ws.WriteMessage(websocket.TextMessage, data); err != nil {
		c.t.Fatalf("write command: %v", err)
	}
}

// connect sends the connect frame and returns the reply, failing on anything else.
func (c *client) connect(subs ...string) proto.ConnectReply {
	c.t.Helper()
	c.send(map[string]any{"id": 1, "connect": map[string]any{"subs": subs}})
	frame := c.read()
	if frame.Connect == nil {
		c.t.Fatalf("first frame = %+v, want a connect reply", frame)
	}
	return *frame.Connect
}

// read returns the next frame, failing the test rather than hanging.
func (c *client) read() reply {
	c.t.Helper()
	if err := c.ws.SetReadDeadline(time.Now().Add(failAfter)); err != nil {
		c.t.Fatalf("set read deadline: %v", err)
	}
	_, data, err := c.ws.ReadMessage()
	if err != nil {
		c.t.Fatalf("read frame: %v", err)
	}
	var f reply
	if err := json.Unmarshal(data, &f); err != nil {
		c.t.Fatalf("decode frame %s: %v", data, err)
	}
	return f
}

// wantDisconnect reads until the disconnect frame, then reads once more to collect the
// websocket close code. Both are asserted, because docs/03-client-protocol.md §5.2 says
// the frame is authoritative when present and the code is the fallback when it is not.
func (c *client) wantDisconnect(want proto.CloseCode) proto.Disconnect {
	c.t.Helper()
	deadline := time.Now().Add(failAfter)
	var got *proto.Disconnect
	for got == nil {
		if time.Now().After(deadline) {
			c.t.Fatalf("no disconnect frame with code %d within %s", want, failAfter)
		}
		if err := c.ws.SetReadDeadline(deadline); err != nil {
			c.t.Fatalf("set read deadline: %v", err)
		}
		_, data, err := c.ws.ReadMessage()
		if err != nil {
			c.t.Fatalf("read: %v, want a disconnect frame with code %d", err, want)
		}
		var f reply
		if err := json.Unmarshal(data, &f); err != nil {
			c.t.Fatalf("decode frame %s: %v", data, err)
		}
		got = f.Disconnect
	}
	if got.Code != want {
		c.t.Fatalf("disconnect code = %d, want %d", got.Code, want)
	}
	if code := c.closeCode(); code != want {
		c.t.Fatalf("websocket close code = %d, want %d", code, want)
	}
	return *got
}

// closeCode reads until the socket closes and returns the websocket close code.
func (c *client) closeCode() proto.CloseCode {
	c.t.Helper()
	if err := c.ws.SetReadDeadline(time.Now().Add(failAfter)); err != nil {
		c.t.Fatalf("set read deadline: %v", err)
	}
	for {
		_, _, err := c.ws.ReadMessage()
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

// reply is the gateway-to-client frame, decoded loosely enough that one read can tell a
// reply from a push from a disconnect.
type reply struct {
	ID          int64               `json:"id"`
	Connect     *proto.ConnectReply `json:"connect"`
	Subscribe   *json.RawMessage    `json:"subscribe"`
	Unsubscribe *json.RawMessage    `json:"unsubscribe"`
	Sync        *proto.SyncReply    `json:"sync"`
	Publish     *json.RawMessage    `json:"publish"`
	Push        *proto.Push         `json:"push"`
	Error       *proto.Error        `json:"error"`
	Disconnect  *proto.Disconnect   `json:"disconnect"`
}

// ---------------------------------------------------------------------------
// log capture
// ---------------------------------------------------------------------------

// syncBuffer is a concurrency-safe log sink: several connection goroutines log at once.
type syncBuffer struct {
	mu  sync.Mutex
	buf strings.Builder
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// statusOf asserts that a handshake was refused and returns the status it was refused
// with. The status is the whole answer for the checks that complete before the upgrade:
// there is no websocket on which a close code could be sent
// (docs/03-client-protocol.md §2, §7).
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

// authorized is the answer a stub webhook gives for a user with these grants.
func authorized(user string, grants ...string) webhook.Authorized {
	set, err := glob.NewSet(grants)
	if err != nil {
		panic("test grants must compile: " + err.Error())
	}
	return webhook.Authorized{User: user, Grants: set, ExpiresIn: time.Hour}
}

// errorsJoinChannel wraps a hub sentinel the way the hub itself does, with the channel
// that caused it. The mapping in commandError must survive that wrap, which is why it
// uses errors.Is (docs/14-coding-standards.md §6).
func errorsJoinChannel(channel string, err error) error {
	return fmt.Errorf("hub: subscribe %q: %w", channel, err)
}

// pump runs the bus consumer main owns: it drains the bus into hub.Dispatch, which is
// what turns a client event published to the bus into a push on every replica. This
// package does not own that loop — it assembles connections — so the tests that need
// delivery start it themselves.
func (r *rig) pump() {
	r.t.Helper()
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-stop:
				return
			case msg, ok := <-r.bus.Receive():
				if !ok {
					return
				}
				if err := r.hub.Dispatch(msg); err != nil {
					r.t.Errorf("dispatch: %v", err)
					return
				}
			}
		}
	}()
	r.t.Cleanup(func() {
		close(stop)
		<-done
	})
}

// contains reports whether s contains sub. It exists so an assertion about an error
// message reads as one line.
func contains(s, sub string) bool { return strings.Contains(s, sub) }

// waitFor polls cond until it holds, failing the test rather than hanging. It is the
// failure detector for state that becomes true on another goroutine — a connection that
// has finished unwinding, a listener that has bound — where there is no channel to wait
// on. It yields rather than sleeping (docs/14-coding-standards.md §2).
func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(failAfter)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("condition did not hold within %s", failAfter)
		}
		runtime.Gosched()
	}
}

// dialWith opens a websocket with an arbitrary header set.
func dialWith(t *testing.T, url string, header http.Header) (*websocket.Conn, error) {
	t.Helper()
	ws, resp, err := websocket.DefaultDialer.Dial(url, header)
	if resp != nil {
		defer resp.Body.Close()
	}
	return ws, err
}

// dialAt opens a websocket at an explicit URL, for the tests that drive a real listener
// rather than the rig's httptest one.
func dialAt(t *testing.T, url, origin string) (*websocket.Conn, error) {
	t.Helper()
	ws, resp, err := websocket.DefaultDialer.Dial(url, http.Header{"Origin": {origin}})
	if resp != nil {
		defer resp.Body.Close()
	}
	return ws, err
}

// newTestHub is a hub on a memory bus, for the tests that need one without a whole rig.
func newTestHub(t *testing.T) *hub.Hub {
	t.Helper()
	h := hub.New(context.Background(), memoryBus(t), hub.Options{})
	t.Cleanup(h.Close)
	return h
}

// memoryBus is a MemoryBus closed by t.Cleanup, so a test that leaves one open fails
// rather than leaking a goroutine into the next one.
func memoryBus(t *testing.T) *bus.MemoryBus {
	t.Helper()
	b := bus.NewMemory(64)
	t.Cleanup(func() {
		if err := b.Close(); err != nil {
			t.Errorf("bus close: %v", err)
		}
	})
	return b
}
