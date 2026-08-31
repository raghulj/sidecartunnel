package main

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/raghulj/sidecartunnel/internal/config"
	"github.com/raghulj/sidecartunnel/internal/proto"
)

// fixture is a whole gateway, built and serving on two ephemeral ports.
//
// The listeners are bound by build, before serve starts, so a request may be issued the
// moment the goroutine is launched: the connection sits in the accept queue until Serve
// picks it up. That is what makes these tests free of a readiness poll.
type fixture struct {
	*gateway
	stub   *stubWebhook
	logs   *syncBuffer
	signal chan os.Signal
	exit   chan int
}

func newFixture(t *testing.T, g grant, overrides map[string]string) *fixture {
	t.Helper()
	stub := newStubWebhook(t, g)
	env := testEnv(stub.URL)
	for k, v := range overrides {
		env[k] = v
	}
	cfg := loadConfig(t, env)

	log, logs := capturingLogger(t, slogDebug)
	gw, err := build(context.Background(), cfg, log)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	return &fixture{gateway: gw, stub: stub, logs: logs,
		signal: signalChan(4), exit: make(chan int, 1)}
}

// start runs the gateway and registers the wait for its exit code.
func (f *fixture) start(t *testing.T) {
	t.Helper()
	go func() { f.exit <- f.serve(context.Background(), f.signal) }()
	t.Cleanup(func() {
		select {
		case f.signal <- sigterm:
		default:
		}
		select {
		case <-f.exit:
		case <-time.After(waitFor):
			t.Error("the gateway did not stop within the drain budget")
		}
		f.close()
	})
}

// waitExit returns the process exit code the gateway produced.
func (f *fixture) waitExit(t *testing.T) int {
	t.Helper()
	select {
	case code := <-f.exit:
		return code
	case <-time.After(waitFor):
		t.Fatalf("the gateway did not exit within %s\n%s", waitFor, f.logs.String())
		return -1
	}
}

// probeURL is a route on the one listener this gateway runs. /health and /ready are
// served from server.listen alongside the websocket endpoint; there is no second listener
// (docs/12-roadmap.md §2).
func (f *fixture) probeURL(path string) string {
	return "http://" + f.clientLn.Addr().String() + path
}

// dial opens a websocket to the client listener with an allowlisted Origin and a cookie,
// exactly as a browser would (FR-2, FR-3).
func (f *fixture) dial(t *testing.T) *websocket.Conn {
	t.Helper()
	url := "ws://" + f.clientLn.Addr().String() + f.cfg.Server.Path
	header := http.Header{}
	header.Set("Origin", "https://app.example.com")
	header.Set("Cookie", "session=abc123")
	conn, resp, err := websocket.DefaultDialer.Dial(url, header)
	if err != nil {
		t.Fatalf("dial %s: %v", url, err)
	}
	_ = resp.Body.Close()
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// send writes one client frame.
func send(t *testing.T, conn *websocket.Conn, frame any) {
	t.Helper()
	if err := conn.WriteJSON(frame); err != nil {
		t.Fatalf("write frame: %v", err)
	}
}

// recv reads one frame as raw JSON, failing rather than hanging.
func recv(t *testing.T, conn *websocket.Conn) map[string]any {
	t.Helper()
	if err := conn.SetReadDeadline(time.Now().Add(waitFor)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	_, data, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("read frame: %v", err)
	}
	var out map[string]any
	if err := json.Unmarshal(data, &out); err != nil {
		t.Fatalf("frame %q is not JSON: %v", data, err)
	}
	t.Logf("← %s", data)
	return out
}

// TestBuild_MemoryBusWarnsOnEveryStart is docs/08-config.md §3's rule. Starting with the
// memory bus and more than one replica is undetectable by the gateway and produces
// messages that arrive for some users and not others, so the warning is unconditional.
func TestBuild_MemoryBusWarnsOnEveryStart(t *testing.T) {
	f := newFixture(t, grant{user: "u-7", channels: []string{"room-*"}, expiresIn: 3600}, nil)
	defer f.close()

	if !strings.Contains(f.logs.String(), "bus.kind is memory") {
		t.Fatalf("no memory-bus warning at startup: %q", f.logs.String())
	}
	if !strings.Contains(f.logs.String(), `"level":"WARN"`) {
		t.Fatalf("the memory-bus warning is not at warn level: %q", f.logs.String())
	}
}

// TestBuild_RedisBusIsUsableWhileRedisIsDown is NFR-8 at startup: a failed first
// connection is not a startup error. Losing Redis must not take the gateway down, so it
// comes back disconnected and keeps retrying.
func TestBuild_RedisBusIsUsableWhileRedisIsDown(t *testing.T) {
	stub := newStubWebhook(t, grant{user: "u-7", expiresIn: 3600})
	env := testEnv(stub.URL)
	env["ST_BUS__KIND"] = "redis"
	env["ST_BUS__URL"] = "redis://127.0.0.1:1/0"
	env["ST_BUS__DIAL_TIMEOUT"] = "100ms"
	cfg := loadConfig(t, env)

	g, err := build(context.Background(), cfg, discardLogger())
	if err != nil {
		t.Fatalf("build with an unreachable redis = %v; a bus that is down is not a startup error (NFR-8)", err)
	}
	defer g.close()

	if g.bus.Health().Connected {
		t.Fatal("the bus reports connected against a port nothing is listening on")
	}
}

// TestBuild_Errors covers every way the graph can refuse to be built. Each one is a
// startup failure naming what went wrong, never a gateway that starts and answers
// nothing (NFR-5).
func TestBuild_Errors(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(t *testing.T, cfg *config.Config)
		wantMsg string
	}{
		{
			name: "bus.url is not a redis url",
			mutate: func(_ *testing.T, cfg *config.Config) {
				cfg.Bus.Kind = "redis"
				cfg.Bus.URL = "redis://localhost:6379/not-a-database"
			},
			wantMsg: "bus.url",
		},
		{
			name: "no webhook secret",
			mutate: func(_ *testing.T, cfg *config.Config) {
				cfg.App.WebhookSecrets = nil
			},
			wantMsg: "app.webhook_secrets",
		},
		{
			name: "an unparseable namespace rate limit",
			mutate: func(_ *testing.T, cfg *config.Config) {
				cfg.Namespaces = []config.Namespace{{Name: "room", RateLimit: "ten per second"}}
			},
			wantMsg: "rate_limit",
		},
		{
			name: "server.listen is already bound",
			mutate: func(t *testing.T, cfg *config.Config) {
				t.Helper()
				cfg.Server.Listen = newHeldPort(t)
			},
			wantMsg: "server.listen",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stub := newStubWebhook(t, grant{user: "u-7", expiresIn: 3600})
			cfg := loadConfig(t, testEnv(stub.URL))
			tt.mutate(t, cfg)

			g, err := build(context.Background(), cfg, discardLogger())
			if err == nil {
				g.close()
				t.Fatalf("build succeeded with %s", tt.name)
			}
			if !strings.Contains(err.Error(), tt.wantMsg) {
				t.Fatalf("build error = %q, want it to name %q", err, tt.wantMsg)
			}
			for _, secret := range []string{testWebhookSecret, testControlSecret} {
				if strings.Contains(err.Error(), secret) {
					t.Fatalf("a startup error quoted a secret: %q", err)
				}
			}
		})
	}
}

// TestGateway_ProbeSurface_FR20 is everything the gateway answers over HTTP besides the
// websocket endpoint: liveness, readiness, and nothing else.
//
// The four 404s are the scope cut, pinned. The Prometheus exposition and the operator API
// both existed and both are gone (docs/12-roadmap.md §2), and a deployment still pointing a
// scrape or a runbook at either should get an unambiguous answer rather than a hang.
func TestGateway_ProbeSurface_FR20(t *testing.T) {
	f := newFixture(t, grant{user: "u-7", channels: []string{"room-*"}, expiresIn: 3600}, nil)
	f.start(t)

	if code, body := httpGet(t, f.probeURL("/health"), ""); code != http.StatusOK {
		t.Fatalf("GET /health = %d %s, want 200", code, body)
	}
	if code, body := httpGet(t, f.probeURL("/ready"), ""); code != http.StatusOK {
		t.Fatalf("GET /ready = %d %s, want 200 on a connected bus", code, body)
	}

	for _, path := range []string{"/metrics", "/channels", "/channels/room-4410", "/disconnect"} {
		if code, _ := httpGet(t, f.probeURL(path), ""); code != http.StatusNotFound {
			t.Errorf("GET %s = %d, want 404: that surface was removed", path, code)
		}
	}
}

// TestGateway_ConnectSubscribeAndDeliver_FR1_FR5_FR12 is the whole product in one test: a
// browser connects, the application authorizes it, a publish on the bus arrives at the
// socket, and the log says who subscribed to what.
func TestGateway_ConnectSubscribeAndDeliver_FR1_FR5_FR12(t *testing.T) {
	f := newFixture(t, grant{user: "u-7", channels: []string{"room-*"}, expiresIn: 3600}, nil)
	f.start(t)

	conn := f.dial(t)
	send(t, conn, map[string]any{"id": 1, "connect": map[string]any{"subs": []string{"room-4410"}}})

	reply := recv(t, conn)
	connect, ok := reply["connect"].(map[string]any)
	if !ok {
		t.Fatalf("first frame = %v, want a connect reply", reply)
	}
	if _, granted := connect["subs"].(map[string]any)["room-4410"]; !granted {
		t.Fatalf("connect reply did not grant room-4410: %v", connect)
	}
	if f.stub.callCount() != 1 {
		t.Fatalf("connect made %d webhook calls, want exactly 1", f.stub.callCount())
	}

	// The subscription is visible to an operator, in the log, keyed on the client id
	// (docs/10-operations.md §6). This is the whole of the replacement for the removed
	// GET /channels:
	// a grep for the message and the channel name answers "was anybody subscribed?",
	// which is the question the runbook's "nobody receives anything" entry asks.
	if logs := f.logs.String(); !strings.Contains(logs, `"msg":"subscribe"`) ||
		!strings.Contains(logs, `"channel":"room-4410"`) {
		t.Fatalf("the subscribe was not logged with its channel:\n%s", logs)
	}

	// A publish on the bus, which is what an application's Redis PUBLISH becomes. The
	// hub reconciles off the request path, so the subscription reaches the bus a moment
	// after the connect reply does.
	waitSubscribed(t, f.bus, 2)
	if err := f.hub.Publish(context.Background(), "room-4410",
		[]byte(`{"event":"order.created","data":{"id":88123}}`)); err != nil {
		t.Fatalf("publish: %v", err)
	}

	push := recv(t, conn)["push"].(map[string]any)
	if push["channel"] != "room-4410" {
		t.Fatalf("push = %v, want channel room-4410", push)
	}
	if push["pub"].(map[string]any)["event"] != "order.created" {
		t.Fatalf("push = %v, want event order.created", push)
	}
}

// TestGateway_UngrantedChannelIsRefused_FR5 is the invariant the whole design serves: the
// gateway matches a string against a list the application supplied and holds no policy of
// its own.
func TestGateway_UngrantedChannelIsRefused_FR5(t *testing.T) {
	f := newFixture(t, grant{user: "u-7", channels: []string{"room-*"}, expiresIn: 3600}, nil)
	f.start(t)

	conn := f.dial(t)
	send(t, conn, map[string]any{"id": 1, "connect": map[string]any{}})
	recv(t, conn)

	send(t, conn, map[string]any{"id": 2, "subscribe": map[string]any{"channel": "user-9"}})
	reply := recv(t, conn)
	errBody, ok := reply["error"].(map[string]any)
	if !ok {
		t.Fatalf("subscribe to an ungranted channel = %v, want an error reply (FR-5)", reply)
	}
	if int(errBody["code"].(float64)) != int(proto.ErrPermissionDenied) {
		t.Fatalf("error code = %v, want %d", errBody["code"], proto.ErrPermissionDenied)
	}
}

// TestGateway_ForeignOriginIsRefusedWithNoWebhookCall_FR2 is the acceptance criterion:
// the Origin check happens before anything else and makes no application call at all.
func TestGateway_ForeignOriginIsRefusedWithNoWebhookCall_FR2(t *testing.T) {
	f := newFixture(t, grant{user: "u-7", expiresIn: 3600}, nil)
	f.start(t)

	header := http.Header{}
	header.Set("Origin", "https://evil.example")
	header.Set("Cookie", "session=abc123")
	conn, resp, err := websocket.DefaultDialer.Dial("ws://"+f.clientLn.Addr().String()+f.cfg.Server.Path, header)
	if err == nil {
		_ = conn.Close()
		t.Fatal("a foreign Origin completed the handshake (FR-2)")
	}
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("foreign Origin = %d, want 403", resp.StatusCode)
	}
	_ = resp.Body.Close()
	if f.stub.callCount() != 0 {
		t.Fatalf("a foreign Origin made %d webhook calls, want 0 (FR-2)", f.stub.callCount())
	}
}

// TestGateway_DrainOnSignal_FR19 is FR-19 end to end: SIGTERM stops accepting, every
// connection is closed with 3000 and reconnect true, /ready reports 503 at once, and the
// process exits 0 inside server.drain_timeout.
func TestGateway_DrainOnSignal_FR19(t *testing.T) {
	f := newFixture(t, grant{user: "u-7", channels: []string{"room-*"}, expiresIn: 3600}, nil)
	go func() { f.exit <- f.serve(context.Background(), f.signal) }()
	defer f.close()

	conn := f.dial(t)
	send(t, conn, map[string]any{"id": 1, "connect": map[string]any{"subs": []string{"room-4410"}}})
	recv(t, conn)

	f.signal <- sigterm

	frame := recv(t, conn)
	disconnect, ok := frame["disconnect"].(map[string]any)
	if !ok {
		t.Fatalf("frame after SIGTERM = %v, want a disconnect frame", frame)
	}
	if int(disconnect["code"].(float64)) != int(proto.CloseDraining) {
		t.Fatalf("disconnect code = %v, want %d (FR-19)", disconnect["code"], proto.CloseDraining)
	}
	if disconnect["reconnect"] != true {
		t.Fatalf("disconnect = %v, want reconnect true", disconnect)
	}
	retryAfter, ok := disconnect["retry_after"].(float64)
	if !ok || retryAfter <= 0 {
		t.Fatalf("disconnect = %v, want a positive retry_after spread over server.drain_spread (S5)", disconnect)
	}
	if retryAfter > float64(f.cfg.Server.DrainSpread.Duration().Milliseconds()) {
		t.Fatalf("retry_after = %v ms, beyond server.drain_spread %s", retryAfter, f.cfg.Server.DrainSpread)
	}

	// The websocket close follows the frame, carrying the same code.
	if _, _, err := conn.ReadMessage(); !websocket.IsCloseError(err, int(proto.CloseDraining)) {
		t.Fatalf("close error = %v, want websocket close %d", err, proto.CloseDraining)
	}

	if code := f.waitExit(t); code != exitOK {
		t.Fatalf("exit = %d after a clean drain, want %d\n%s", code, exitOK, f.logs.String())
	}
}

// TestGateway_ReadyIs503WhileDraining is step 1 of docs/09-internals.md §8. The load
// balancer has to stop steering new connections here well before the sockets close, and
// /health must stay 200 throughout — a liveness probe that followed readiness would kill
// the replica mid-drain.
func TestGateway_ReadyIs503WhileDraining(t *testing.T) {
	f := newFixture(t, grant{user: "u-7", expiresIn: 3600}, nil)
	f.start(t)

	if code, _ := httpGet(t, f.probeURL("/ready"), ""); code != http.StatusOK {
		t.Fatalf("GET /ready = %d before the drain, want 200", code)
	}
	f.server.StopAccepting()
	if code, body := httpGet(t, f.probeURL("/ready"), ""); code != http.StatusServiceUnavailable {
		t.Fatalf("GET /ready = %d %s while draining, want 503", code, body)
	}
	if code, _ := httpGet(t, f.probeURL("/health"), ""); code != http.StatusOK {
		t.Fatalf("GET /health = %d while draining, want 200 (FR-20)", code)
	}
}

// TestGateway_ListenerFailureIsFatal covers the path where the listener dies under the
// server: the gateway drains and exits 1 rather than running on with a surface that is
// silently absent.
//
// There is one listener to lose now. It used to be two, and losing either was fatal for
// the same reason; the second one went with the operator API it carried
// (docs/12-roadmap.md §2).
func TestGateway_ListenerFailureIsFatal(t *testing.T) {
	f := newFixture(t, grant{user: "u-7", expiresIn: 3600}, nil)
	defer f.close()
	if err := f.clientLn.Close(); err != nil {
		t.Fatalf("close the listener: %v", err)
	}

	go func() { f.exit <- f.serve(context.Background(), f.signal) }()
	if code := f.waitExit(t); code != exitFailure {
		t.Fatalf("exit = %d after the listener failed, want %d", code, exitFailure)
	}
}

// TestGateway_DrainOverrunExitsOne asserts the other half of FR-19: a drain that does not
// finish inside its budget is reported, and the process exits anyway, because a rolling
// deploy that never completes is worse than one that drops a socket.
//
// The connection that outlasts the budget is one waiting on the application: the handler
// goroutine is tracked before authorization, so a webhook that has not answered is a
// connection the drain must wait for. Blocking it is deterministic in a way that a short
// timeout alone is not — the wait cannot finish early.
func TestGateway_DrainOverrunExitsOne(t *testing.T) {
	f := newFixture(t, grant{user: "u-7", expiresIn: 3600}, nil)
	defer f.close()
	f.cfg.Server.DrainTimeout = config.Duration(50 * time.Millisecond)

	release := f.stub.block()
	defer release()

	go func() { f.exit <- f.serve(context.Background(), f.signal) }()
	conn := f.dial(t)
	send(t, conn, map[string]any{"id": 1, "connect": map[string]any{}})
	f.waitWebhookEntered(t)

	f.signal <- sigterm
	if code := f.waitExit(t); code != exitFailure {
		t.Fatalf("exit = %d after an overrun drain, want %d\n%s", code, exitFailure, f.logs.String())
	}
	if !strings.Contains(f.logs.String(), "drain did not complete") {
		t.Fatalf("no log line about the overrun: %q", f.logs.String())
	}
}

// TestWaitForStop covers the three ways the gateway is asked to stop. A cancelled context
// and a signal are clean; a listener failure is not.
func TestWaitForStop(t *testing.T) {
	t.Run("context", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if err := waitForStop(ctx, nil, nil, discardLogger()); err != nil {
			t.Fatalf("waitForStop = %v on a cancelled context, want nil", err)
		}
	})
	t.Run("signal", func(t *testing.T) {
		sigs := signalChan(1)
		sigs <- sigterm
		if err := waitForStop(context.Background(), sigs, nil, discardLogger()); err != nil {
			t.Fatalf("waitForStop = %v on a signal, want nil", err)
		}
	})
	t.Run("listener failure", func(t *testing.T) {
		fatal := make(chan error, 1)
		fatal <- net.ErrClosed
		if err := waitForStop(context.Background(), nil, fatal, discardLogger()); err == nil {
			t.Fatal("waitForStop = nil on a listener failure, want the error")
		}
	})
}

// TestAwaitDrain_SecondSignalAbandonsTheDrain is FR-19's escape hatch: an operator who
// sends SIGTERM twice has decided the drain is not going to finish.
func TestAwaitDrain_SecondSignalAbandonsTheDrain(t *testing.T) {
	sigs := signalChan(1)
	sigs <- sigterm
	completed, err := awaitDrain(make(chan error), sigs, discardLogger())
	if completed {
		t.Fatal("awaitDrain reported the drain complete after a second signal")
	}
	if err != nil {
		t.Fatalf("awaitDrain = %v, want nil", err)
	}
}

// TestAwaitDrain_ReportsTheDrainsOwnError passes the drain's failure through untouched,
// so the caller logs what actually happened rather than a summary of it.
func TestAwaitDrain_ReportsTheDrainsOwnError(t *testing.T) {
	drained := make(chan error, 1)
	drained <- context.DeadlineExceeded
	completed, err := awaitDrain(drained, nil, discardLogger())
	if !completed {
		t.Fatal("awaitDrain reported the drain incomplete when it finished")
	}
	if err == nil {
		t.Fatal("awaitDrain swallowed the drain's error")
	}
}

// TestGateway_SecondSignalExitsWithoutCompletingTheDrain drives the same rule through the
// whole gateway, with a drain budget long enough that only the second signal can end it.
func TestGateway_SecondSignalExitsWithoutCompletingTheDrain(t *testing.T) {
	f := newFixture(t, grant{user: "u-7", expiresIn: 3600}, map[string]string{
		"ST_SERVER__DRAIN_TIMEOUT": "300s",
	})
	defer f.close()

	release := f.stub.block()
	defer release()

	go func() { f.exit <- f.serve(context.Background(), f.signal) }()
	conn := f.dial(t)
	send(t, conn, map[string]any{"id": 1, "connect": map[string]any{}})
	f.waitWebhookEntered(t)

	f.signal <- sigterm
	f.signal <- sigterm

	if code := f.waitExit(t); code != exitFailure {
		t.Fatalf("exit = %d after a second signal, want %d\n%s", code, exitFailure, f.logs.String())
	}
	if !strings.Contains(f.logs.String(), "second signal") {
		t.Fatalf("no log line about the abandoned drain: %q", f.logs.String())
	}
}

// waitWebhookEntered blocks until a connect webhook call has reached the blocked stub, so
// the test knows a connection is registered and unfinished.
func (f *fixture) waitWebhookEntered(t *testing.T) {
	t.Helper()
	select {
	case <-f.stub.entered:
	case <-time.After(waitFor):
		t.Fatal("no connect webhook call arrived")
	}
}
