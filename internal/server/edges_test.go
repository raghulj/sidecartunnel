package server

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/raghulj/sidecartunnel/internal/config"
	"github.com/raghulj/sidecartunnel/internal/hub"
	"github.com/raghulj/sidecartunnel/internal/proto"
)

// TestUpgrade_APlainRequestIsNotAWebsocket: the endpoint answers an ordinary GET the way
// gorilla does, with 400, and the reservation taken before the upgrade is returned. A
// leaked reservation is a replica that refuses everything after enough bad requests.
func TestUpgrade_APlainRequestIsNotAWebsocket(t *testing.T) {
	t.Parallel()
	r := newRig(t)

	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, r.http.URL+r.cfg.Server.Path, nil)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	req.Header.Set("Origin", testOrigin)
	resp, err := r.http.Client().Do(req)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			t.Errorf("close body: %v", err)
		}
	}()

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 for a request that is not an upgrade", resp.StatusCode)
	}
	if got := r.srv.Stats().Current; got != 0 {
		t.Fatalf("Current = %d after a failed upgrade, want 0: the reservation must be returned", got)
	}
}

// TestDuplicateClientID_RefusesTheSecondConnection_FR18: the hub refuses a client id
// already held by another connection, because overwriting the index entry would leave one
// of the two unreachable by a control disconnect. crypto/rand makes this unreachable in
// production, which is exactly why the id source is a seam.
func TestDuplicateClientID_RefusesTheSecondConnection_FR18(t *testing.T) {
	t.Parallel()
	r := newRigWithOptions(t, func(o *Options) {
		o.ClientID = func() string { return "cafebabecafebabe" }
	})
	r.http = httptest.NewServer(r.srv.Handler())
	t.Cleanup(r.http.Close)

	first := r.dial()
	first.connect()

	// The second connection gets the same id, cannot be registered, and is closed. There
	// is no close code for it: the client sees 1006 and treats it as retryable, which is
	// correct, because a second attempt gets a fresh id.
	second := r.dial()
	if code := second.closeCode(); code != proto.CloseCode(websocketAbnormalClosure) {
		t.Fatalf("close code = %d, want an abnormal closure with no protocol code", code)
	}
	// Waited for, not read once. The refused connection's reservation is returned by the
	// deferred release on the handler goroutine, which runs after the socket it just
	// closed is observable from here — so reading the count the instant the close arrives
	// races the handler's own return and sees 2.
	waitFor(t, func() bool { return r.srv.Stats().Current == 1 })
}

// TestServe_RefusesToStartWhileDraining: SIGTERM can land between the listener being
// created and Serve being called, and a replica that starts accepting after it has
// decided to drain is a replica that closes connections it has just admitted.
func TestServe_RefusesToStartWhileDraining(t *testing.T) {
	t.Parallel()
	r := newRigNoHTTP(t)

	ctx, cancel := context.WithTimeout(context.Background(), failAfter)
	defer cancel()
	if err := r.srv.Drain(ctx); err != nil {
		t.Fatalf("Drain: %v", err)
	}

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	if err := r.srv.Serve(l); !errors.Is(err, http.ErrServerClosed) {
		t.Fatalf("Serve while draining = %v, want http.ErrServerClosed", err)
	}
	if _, err := net.DialTimeout("tcp", l.Addr().String(), failAfter); err == nil {
		t.Fatal("the listener is still accepting after Serve refused to start")
	}
}

// TestDrain_ReportsAListenerThatWouldNotShutDown: the shutdown of the HTTP listener is
// bounded by the same budget as everything else, and a failure to meet it is reported
// rather than swallowed (FR-19).
func TestDrain_ReportsAListenerThatWouldNotShutDown(t *testing.T) {
	t.Parallel()
	r := newRigNoHTTP(t)

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = r.srv.Serve(l)
	}()
	waitFor(t, func() bool { return r.srv.listening() })

	// A peer that has connected and sent nothing is exactly what keeps net/http from
	// going quiescent: it is neither idle nor serving, so the shutdown waits for it. That
	// is the shape of the failure this branch reports — a socket the replica cannot
	// account for, holding the drain open past its budget.
	raw, err := net.Dial("tcp", l.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}

	spent, cancel := context.WithCancel(context.Background())
	cancel()
	if err := r.srv.Drain(spent); err == nil {
		t.Fatal("Drain reported success although its budget was already spent")
	}

	if err := raw.Close(); err != nil {
		t.Fatalf("close raw connection: %v", err)
	}
	shutdown, cancelShutdown := context.WithTimeout(context.Background(), failAfter)
	defer cancelShutdown()
	if err := r.srv.Drain(shutdown); err != nil {
		t.Fatalf("second Drain: %v", err)
	}
	wg.Wait()
}

// TestDefaults_AppliedToAZeroConfig: a Config built by hand — in a test, or in main
// before Load has run — must still produce a working server rather than a path of "" that
// nothing can reach and a drain budget of zero that expires immediately.
func TestDefaults_AppliedToAZeroConfig(t *testing.T) {
	t.Parallel()
	r := newRig(t, func(c *config.Config) {
		c.Server.Path = ""
		c.Server.ReadHeaderTimeout = 0
		c.Server.DrainTimeout = 0
		c.Server.DrainSpread = 0
	})
	r.cfg.Server.Path = defaultPath

	if got := r.srv.readHeaderTimeout(); got != defaultReadHeaderTimeout {
		t.Fatalf("ReadHeaderTimeout = %s, want the documented default %s", got, defaultReadHeaderTimeout)
	}
	if got := r.srv.drainTimeout(); got != defaultDrainTimeout {
		t.Fatalf("DrainTimeout = %s, want %s", got, defaultDrainTimeout)
	}
	if got := r.srv.drainSpread(); got != defaultDrainSpread {
		t.Fatalf("DrainSpread = %s, want %s", got, defaultDrainSpread)
	}

	c := r.dial()
	c.connect()

	ctx, cancel := context.WithTimeout(context.Background(), failAfter)
	defer cancel()
	if err := r.srv.Drain(ctx); err != nil {
		t.Fatalf("Drain: %v", err)
	}
	got := c.wantDisconnect(proto.CloseDraining)
	if got.RetryAfter <= 0 || got.RetryAfter > defaultDrainSpread.Milliseconds() {
		t.Fatalf("retry_after = %dms, want it spread across the default drain_spread", got.RetryAfter)
	}
}

// TestPublish_ReportsABusFailure: a client event the bus refused must be reported to the
// client as a failure, never answered with a success it did not have. The connection
// turns anything that is not a *conn.CommandError into error 100, which is the honest
// answer: the client can do nothing with the detail but try again.
func TestPublish_ReportsABusFailure(t *testing.T) {
	t.Parallel()
	h := newTestHubWithNamespaces(t, config.Namespace{Name: "desk", ClientEvents: true, RateLimit: "10/s"})

	// A cancelled context is the bus being unavailable from this registry's point of
	// view: hub.Publish reaches the bus, which refuses.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	rates, err := parseRates([]config.Namespace{{Name: "desk", RateLimit: "10/s"}})
	if err != nil {
		t.Fatalf("parseRates: %v", err)
	}
	reg := newRegistry(ctx, h, rates, newFakeClock())

	sink := &fakeSink{id: "c1", user: "u-1"}
	if err := h.Add(sink); err != nil {
		t.Fatalf("Add: %v", err)
	}
	if err := reg.Publish(sink, "desk-1", "typing", []byte(`{"typing":true}`)); err == nil {
		t.Fatal("Publish reported success although the bus refused the message")
	}
	if got := sink.closed(); len(got) != 0 {
		t.Fatalf("closes = %v, want none: a bus failure is a failed command, not a failed connection", got)
	}
}

// TestRegistry_DelegatesToTheHub covers the pass-through half of the adapter: Attach,
// Subscriptions and Remove are the hub's, and the adapter exists to add the wire's
// opinions to Subscribe, Unsubscribe and Publish, not to reimplement bookkeeping.
func TestRegistry_DelegatesToTheHub(t *testing.T) {
	t.Parallel()
	h := newTestHubWithNamespaces(t, config.Namespace{Name: "room", RateLimit: "10/s"})
	reg := newRegistry(context.Background(), h, map[string]rate{"room": defaultRate}, newFakeClock())
	sink := &fakeSink{id: "c1", user: "u-1"}
	// Registered at the upgrade, as serve does, because Attach registers nothing: a hub
	// that registered there could not tell a connection that was never added from one
	// close has just deregistered (docs/13-review-findings.md M4).
	if err := h.Add(sink); err != nil {
		t.Fatalf("Add: %v", err)
	}

	var granted []string
	reg.Attach(sink, []string{"room-1"}, func(g []string) *proto.Frame {
		granted = append(granted, g...)
		return nil
	})
	if len(granted) != 1 || granted[0] != "room-1" {
		t.Fatalf("granted = %v, want room-1", granted)
	}
	if got := reg.Subscriptions(sink); len(got) != 1 || got[0] != "room-1" {
		t.Fatalf("Subscriptions = %v, want room-1", got)
	}

	reg.Remove(sink)
	if got := reg.Subscriptions(sink); len(got) != 0 {
		t.Fatalf("Subscriptions after Remove = %v, want none", got)
	}
}

// newTestHubWithNamespaces is a hub on a memory bus with an explicit namespace list.
func newTestHubWithNamespaces(t *testing.T, namespaces ...config.Namespace) *hub.Hub {
	t.Helper()
	b := memoryBus(t)
	h := hub.New(context.Background(), b, hub.Options{Namespaces: namespaces})
	t.Cleanup(h.Close)
	return h
}

// fakeSink is a conn.Sink for the tests that drive the registry without a socket.
type fakeSink struct {
	id   string
	user string

	mu     sync.Mutex
	closes []proto.CloseCode
}

func (s *fakeSink) ID() string             { return s.id }
func (s *fakeSink) User() string           { return s.user }
func (s *fakeSink) Send(*proto.Frame) bool { return true }

func (s *fakeSink) Close(code proto.CloseCode, _ string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closes = append(s.closes, code)
}

func (s *fakeSink) closed() []proto.CloseCode {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]proto.CloseCode(nil), s.closes...)
}

// websocketAbnormalClosure is RFC 6455's 1006, which gorilla reports when a socket is
// closed with no close frame. docs/03-client-protocol.md §5.2 makes it retryable, which
// is what a client should do with a gateway-side failure that has no protocol code.
const websocketAbnormalClosure = 1006
