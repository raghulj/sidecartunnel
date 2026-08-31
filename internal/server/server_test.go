package server

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"slices"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/raghulj/sidecartunnel/internal/config"
	"github.com/raghulj/sidecartunnel/internal/proto"
	"github.com/raghulj/sidecartunnel/internal/webhook"
)

// TestUpgrade_OnTheConfiguredPath_FR1: the endpoint is server.path and nothing else
// answers a websocket handshake.
func TestUpgrade_OnTheConfiguredPath_FR1(t *testing.T) {
	t.Parallel()
	r := newRig(t, func(c *config.Config) { c.Server.Path = "/socket" })

	c := r.dial()
	if got := c.connect(); got.Client == "" {
		t.Fatal("connect reply carried no client id")
	}

	_, resp, err := websocket.DefaultDialer.Dial("ws"+r.http.URL[len("http"):]+"/elsewhere", http.Header{"Origin": {testOrigin}})
	if resp != nil {
		defer resp.Body.Close()
	}
	if got := statusOf(t, responseStatus(resp), err); got != http.StatusNotFound {
		t.Fatalf("status on another path = %d, want 404", got)
	}
}

// TestConnect_RepliesWithTheGrantedSubscriptions_FR1_FR5 walks the whole happy path: the
// upgrade, the webhook, the grant match, the subscriptions taken at connect, and the
// reply that announces them (docs/03-client-protocol.md §4.1).
func TestConnect_RepliesWithTheGrantedSubscriptions_FR1_FR5(t *testing.T) {
	t.Parallel()
	r := newRig(t)

	got := r.dial().connect("room-1", "org-99-secret")

	if got.Client == "" || len(got.Client) != 16 {
		t.Fatalf("client = %q, want 16 hex characters", got.Client)
	}
	if _, ok := got.Subs["room-1"]; !ok {
		t.Fatalf("subs = %v, want the granted channel", got.Subs)
	}
	// FR-5: a channel matching no grant is omitted from the reply rather than failing the
	// whole connect. The client compares what it asked for against what it got.
	if _, ok := got.Subs["org-99-secret"]; ok {
		t.Fatalf("subs = %v, want the ungranted channel omitted", got.Subs)
	}
	if got.ExpiresIn != int(time.Hour/time.Second) {
		t.Fatalf("expires_in = %d, want the clamped webhook value", got.ExpiresIn)
	}
	if s := r.srv.Stats(); s.Accepted != 1 || s.Current != 1 {
		t.Fatalf("stats = %+v, want one accepted and one current connection", s)
	}
}

// TestAuthorization_RefusalAndFailureNeverShareACode_FR6 is the distinction the whole
// design turns on. A 401 means this user may not connect and the client must stop asking;
// a 5xx means the gateway could not tell, and the client must come back. Reusing one code
// either locks every user out during an application deploy or turns a revocation into an
// infinite retry loop against an endpoint that is already failing.
func TestAuthorization_RefusalAndFailureNeverShareACode_FR6(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name          string
		result        webhook.Result
		wantCode      proto.CloseCode
		wantReconnect bool
		wantRetry     bool
	}{
		{
			name:     "401 is a decision",
			result:   webhook.Refused{Status: http.StatusUnauthorized, Err: errors.New("unauthorized")},
			wantCode: proto.CloseUnauthorized,
		},
		{
			name:          "500 is a failure",
			result:        webhook.Unavailable{Status: http.StatusInternalServerError, Err: errors.New("server error")},
			wantCode:      proto.CloseAuthUnavailable,
			wantReconnect: true,
			wantRetry:     true,
		},
		{
			// A 403 is about the request, not the user: a bad signature or a skewed
			// clock. As 3003 it would lock out every user of a drifted replica until a
			// human noticed (docs/04-integration.md §1.3).
			name:          "403 is a failure, not a refusal",
			result:        webhook.Unavailable{Status: http.StatusForbidden, Err: webhook.ErrRequestRejected},
			wantCode:      proto.CloseAuthUnavailable,
			wantReconnect: true,
			wantRetry:     true,
		},
		{
			// NFR-4: overflow of app.connect_queue is transient by construction. The
			// mechanism that protects the application must not permanently lock out the
			// users a reconnect storm caught (C2).
			name:          "a full connect queue is a failure",
			result:        webhook.Unavailable{Err: webhook.ErrQueueOverflow},
			wantCode:      proto.CloseAuthUnavailable,
			wantReconnect: true,
			wantRetry:     true,
		},
		{
			// A 2xx whose body the gateway cannot use is a decision: the application
			// answered, and asking again gets the same answer.
			name:     "an unusable body is a decision",
			result:   webhook.Refused{Status: http.StatusOK, Err: webhook.ErrMalformedResponse},
			wantCode: proto.CloseUnauthorized,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			r := newRig(t)
			r.web.answer(tt.result)

			c := r.dial()
			c.send(map[string]any{"id": 1, "connect": map[string]any{}})

			got := c.wantDisconnect(tt.wantCode)
			if got.Reconnect != tt.wantReconnect {
				t.Fatalf("reconnect = %v, want %v", got.Reconnect, tt.wantReconnect)
			}
			if (got.RetryAfter > 0) != tt.wantRetry {
				t.Fatalf("retry_after = %d, want positive = %v (docs/03-client-protocol.md §7.1)", got.RetryAfter, tt.wantRetry)
			}
		})
	}
}

// TestHandshakeTimeout_ClosesOnlyASilentSocket_FR4_C2 and its sibling below are the pair
// FR-4 demands be asserted separately.
//
// This half: a socket that never sends the connect frame is the client's own fault and
// closes 3001, reconnect false.
func TestHandshakeTimeout_ClosesOnlyASilentSocket_FR4_C2(t *testing.T) {
	t.Parallel()
	r := newRig(t, func(c *config.Config) { c.Server.HandshakeTimeout = config.Duration(5 * time.Second) })

	c := r.dial()
	// The writer arms the handshake timer and the ping ticker as its first act; waiting
	// for both is what makes advancing the clock deterministic rather than a race.
	r.clk.waitAlarms(t, 2)
	r.clk.Advance(5 * time.Second)

	got := c.wantDisconnect(proto.CloseHandshakeTimeout)
	if got.Reconnect {
		t.Fatal("3001 is reconnect false: it applies only to a client that genuinely never sent connect")
	}
	if r.web.count() != 0 {
		t.Fatal("a connection that never sent connect must not reach the application")
	}
}

// TestAuthorizationTimeout_IsNot3001_FR4_C2 is the other half, and the reason C2 was a
// critical finding: with the queue and the handshake timer conflated, a slow application
// closed every waiting connection 3001, reconnect false — a permanent, fleet-wide lockout
// produced by the very mechanism meant to protect the application.
//
// Here the connect frame has arrived and the application is slow. The handshake deadline
// passes while the gateway waits, and it must not fire; when the application finally
// fails, the close is 3008 with reconnect true and a retry_after.
func TestAuthorizationTimeout_IsNot3001_FR4_C2(t *testing.T) {
	t.Parallel()
	r := newRig(t)

	started := make(chan struct{})
	release := make(chan webhook.Result)
	r.web.answerWith(func(ctx context.Context, _ webhook.Request) webhook.Result {
		close(started)
		select {
		case res := <-release:
			return res
		case <-ctx.Done():
			// app.connect_timeout is the authorization budget, and exceeding it is a
			// failure rather than a refusal (NFR-4).
			return webhook.Unavailable{Err: ctx.Err()}
		}
	})

	c := r.dial()
	r.clk.waitAlarms(t, 2)
	c.send(map[string]any{"id": 1, "connect": map[string]any{}})

	select {
	case <-started:
	case <-time.After(failAfter):
		t.Fatal("the webhook was never called")
	}

	// The handshake deadline passes with the connect frame already received. Nothing may
	// happen: the timer covers receipt of that frame and nothing after it (FR-4).
	r.clk.Advance(time.Hour)
	release <- webhook.Unavailable{Status: http.StatusGatewayTimeout, Err: errors.New("timeout")}

	got := c.wantDisconnect(proto.CloseAuthUnavailable)
	if !got.Reconnect {
		t.Fatal("3008 is reconnect true: every replica will answer the same, and the client must come back later")
	}
	if got.RetryAfter <= 0 {
		t.Fatal("3008 carries a retry_after; without one every client retries into the same failing application")
	}
}

// TestSlowAuthorizationStillConnects_FR4: the same sequence, but the application answers.
// The connection survives the handshake deadline and gets its reply.
func TestSlowAuthorizationStillConnects_FR4(t *testing.T) {
	t.Parallel()
	r := newRig(t)

	started := make(chan struct{})
	release := make(chan struct{})
	set := newStubWebhook("room-*")
	r.web.answerWith(func(context.Context, webhook.Request) webhook.Result {
		close(started)
		<-release
		return set.result
	})

	c := r.dial()
	r.clk.waitAlarms(t, 2)
	c.send(map[string]any{"id": 1, "connect": map[string]any{"subs": []string{"room-1"}}})
	select {
	case <-started:
	case <-time.After(failAfter):
		t.Fatal("the webhook was never called")
	}

	r.clk.Advance(time.Hour)
	close(release)

	frame := c.read()
	if frame.Connect == nil {
		t.Fatalf("first frame after a slow authorization = %+v, want a connect reply and never a 3001 (C2)", frame)
	}
}

// TestMaxConnections_Answers503_NFR1: the count check happens before the upgrade, so the
// answer is an HTTP status and not a close code (docs/03-client-protocol.md §2).
func TestMaxConnections_Answers503_NFR1(t *testing.T) {
	t.Parallel()
	r := newRig(t, func(c *config.Config) { c.Limits.MaxConnections = 1 })

	r.dial().connect()

	_, status, err := r.dialOrigin(testOrigin)
	if got := statusOf(t, status, err); got != http.StatusServiceUnavailable {
		t.Fatalf("status over limits.max_connections = %d, want 503", got)
	}
	if got := r.srv.Stats().OverCapacity; got != 1 {
		t.Fatalf("OverCapacity = %d, want 1", got)
	}
	// The webhook is never asked about a connection that was refused before the upgrade.
	if got := r.web.count(); got != 1 {
		t.Fatalf("webhook calls = %d, want 1: only the admitted connection", got)
	}
}

// TestMaxConnectionsUnlimited: 0 is unlimited, and a gateway configured that way must not
// refuse the first connection.
func TestMaxConnectionsUnlimited(t *testing.T) {
	t.Parallel()
	r := newRig(t, func(c *config.Config) { c.Limits.MaxConnections = 0 })
	r.dial().connect()
	r.dial().connect()
}

// TestMaxConnectionsPerUser_RefusesAfterAuthorization_FR25: the per-user cap can only be
// enforced once the application has said who this is, so it closes the websocket rather
// than answering an HTTP status. It is a decision — 3003, reconnect false — because the
// cap exists to stop one looping client from consuming the global limit, and a retryable
// close would invite exactly that loop.
func TestMaxConnectionsPerUser_RefusesAfterAuthorization_FR25(t *testing.T) {
	t.Parallel()
	r := newRig(t, func(c *config.Config) { c.Limits.MaxConnectionsPerUser = 1 })

	r.dial().connect()

	second := r.dial()
	second.send(map[string]any{"id": 1, "connect": map[string]any{}})
	got := second.wantDisconnect(proto.CloseUnauthorized)
	if got.Reconnect {
		t.Fatal("a per-user cap that invites an immediate retry is not a cap")
	}
	if s := r.srv.Stats(); s.UserLimited != 1 {
		t.Fatalf("UserLimited = %d, want 1", s.UserLimited)
	}
}

// TestMaxConnectionsPerUser_FreedOnClose: the count follows the connections, or a user
// who reconnects twenty times is locked out of their own account for the life of the
// process.
func TestMaxConnectionsPerUser_FreedOnClose(t *testing.T) {
	t.Parallel()
	r := newRig(t, func(c *config.Config) { c.Limits.MaxConnectionsPerUser = 1 })

	first := r.dial()
	first.connect()
	first.close()
	waitFor(t, func() bool { return r.srv.Stats().Current == 0 })

	r.dial().connect()
}

// TestNew_RequiresItsDependencies: a Server that cannot work is an error at construction
// rather than a panic on the first connection.
func TestNew_RequiresItsDependencies(t *testing.T) {
	t.Parallel()
	cfg := testConfig()
	tests := []struct {
		name string
		opts func(*Options)
		want string
	}{
		{name: "no config", opts: func(o *Options) { o.Config = nil }, want: "Options.Config"},
		{name: "no hub", opts: func(o *Options) { o.Hub = nil }, want: "Options.Hub"},
		{name: "no bus", opts: func(o *Options) { o.Bus = nil }, want: "Options.Bus"},
		{
			name: "an unparseable rate limit",
			opts: func(o *Options) {
				c := testConfig()
				c.Namespaces = []config.Namespace{{Name: "desk", ClientEvents: true, RateLimit: "ten a second"}}
				o.Config = c
			},
			want: "rate_limit",
		},
		{
			name: "a trusted proxy that is not a CIDR",
			opts: func(o *Options) {
				c := testConfig()
				c.Server.TrustedProxies = []string{"10.0.0.1"}
				c.App.ConnectURL = "https://app.example.com/connect"
				c.App.WebhookSecrets = []string{"0123456789012345678901234567890123456789"}
				c.App.WebhookConcurrency = 1
				o.Config = c
				o.Webhook = nil
			},
			want: "trusted_proxies",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			opts := Options{Config: cfg, Hub: newTestHub(t), Bus: memoryBus(t), Webhook: newStubWebhook()}
			tt.opts(&opts)
			_, err := New(opts)
			if err == nil {
				t.Fatal("New succeeded; want an error naming the offending dependency")
			}
			if !contains(err.Error(), tt.want) {
				t.Fatalf("error = %v, want it to name %q", err, tt.want)
			}
		})
	}
}

// TestNew_BuildsTheWebhookClientFromConfig_FR24: left to itself, the server constructs
// the connect client, which is what puts server.trusted_proxies in the hands of the
// package that owns the X-Forwarded-For walk.
func TestNew_BuildsTheWebhookClientFromConfig_FR24(t *testing.T) {
	t.Parallel()
	cfg := testConfig()
	cfg.Server.TrustedProxies = []string{"10.0.0.0/8"}
	cfg.App.ConnectURL = "https://app.example.com/connect"
	cfg.App.WebhookSecrets = []string{"0123456789012345678901234567890123456789"}
	cfg.App.WebhookConcurrency = 8
	cfg.App.ConnectQueue = 16

	srv, err := New(Options{Config: cfg, Hub: newTestHub(t), Bus: memoryBus(t), Log: slog.New(slog.DiscardHandler)})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if srv.webhook == nil {
		t.Fatal("New built no connect client; nothing would ever authorize")
	}
}

// TestServe_ListensAndShutsDown covers the listener the process actually runs on,
// including the ReadHeaderTimeout that keeps a slowloris from parking a connection on the
// upgrade path forever.
func TestServe_ListensAndShutsDown(t *testing.T) {
	t.Parallel()
	r := newRigNoHTTP(t)

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	errs := make(chan error, 1)
	go func() { errs <- r.srv.Serve(l) }()

	c, err := dialAt(t, "ws://"+l.Addr().String()+r.cfg.Server.Path, testOrigin)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	(&client{t: t, ws: c}).connect()
	_ = c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), failAfter)
	defer cancel()
	if err := r.srv.Drain(ctx); err != nil {
		t.Fatalf("Drain: %v", err)
	}
	select {
	case err := <-errs:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			t.Fatalf("Serve: %v", err)
		}
	case <-time.After(failAfter):
		t.Fatal("Serve did not return after Drain")
	}
	if got := r.srv.readHeaderTimeout(); got != 5*time.Second {
		t.Fatalf("ReadHeaderTimeout = %s, want server.read_header_timeout", got)
	}
}

// TestListenAndServe_UsesTheConfiguredAddress: the same path main takes.
func TestListenAndServe_UsesTheConfiguredAddress(t *testing.T) {
	t.Parallel()
	r := newRigNoHTTP(t)

	errs := make(chan error, 1)
	go func() { errs <- r.srv.ListenAndServe() }()

	ctx, cancel := context.WithTimeout(context.Background(), failAfter)
	defer cancel()
	waitFor(t, func() bool { return r.srv.listening() })
	if err := r.srv.Drain(ctx); err != nil {
		t.Fatalf("Drain: %v", err)
	}
	select {
	case err := <-errs:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			t.Fatalf("ListenAndServe: %v", err)
		}
	case <-time.After(failAfter):
		t.Fatal("ListenAndServe did not return after Drain")
	}
}

// TestListenAndServe_ReportsABadAddress: a listen address that cannot be bound is a
// startup failure naming it, never a process that runs and answers nothing (NFR-5).
func TestListenAndServe_ReportsABadAddress(t *testing.T) {
	t.Parallel()
	r := newRigNoHTTP(t, func(c *config.Config) { c.Server.Listen = "256.256.256.256:1" })
	if err := r.srv.ListenAndServe(); err == nil {
		t.Fatal("ListenAndServe succeeded on an unbindable address")
	}
}

// TestConnectTimeout_IsTheAuthorizationBudget_NFR4 pins the second half of C2's fix to
// its own configuration key.
//
// app.connect_timeout bounds the whole authorization — queue wait plus call — and nothing
// else; server.handshake_timeout, an hour away here and driven by a clock this test never
// advances, bounds only receipt of the connect frame. Wire the connection's authorization
// budget to the handshake key instead and every other test in this file still passes:
// this one does not, because the close arrives an hour late.
func TestConnectTimeout_IsTheAuthorizationBudget_NFR4(t *testing.T) {
	t.Parallel()
	r := newRig(t, func(c *config.Config) {
		c.App.ConnectTimeout = config.Duration(30 * time.Millisecond)
		c.Server.HandshakeTimeout = config.Duration(time.Hour)
	})
	r.web.answerWith(func(ctx context.Context, _ webhook.Request) webhook.Result {
		// An application that never answers. The budget is the gateway's to enforce.
		<-ctx.Done()
		return webhook.Unavailable{Err: ctx.Err()}
	})

	c := r.dial()
	c.send(map[string]any{"id": 1, "connect": map[string]any{}})

	got := c.wantDisconnect(proto.CloseAuthUnavailable)
	if !got.Reconnect || got.RetryAfter <= 0 {
		t.Fatalf("disconnect = %+v, want reconnect true with a retry_after (NFR-4, FR-6)", got)
	}
}

// TestConnect_ChannelsRequestedReachTheWebhook_FR3 completes docs/04-integration.md §1.1:
// the normative request body carries channels_requested, and it is the connect frame's
// subs that fill it.
//
// The value cannot be read where the rest of the request is built. serve captures the
// cookie, the origin and the peer at the upgrade, which is before a single client frame
// has been read; the subs arrive later, on the connect frame, and reach this call through
// the Authorizer. An application reading the field must be able to tell "the client asked
// for nothing" from "the gateway never tells me", and until the seam carried them it
// could not.
//
// The second connection is the part that would rot quietly: the Request is captured by a
// closure shared with every call for that connection, so a field written onto the closure
// copy rather than onto the per-call value leaks one client's request into another's.
func TestConnect_ChannelsRequestedReachTheWebhook_FR3(t *testing.T) {
	t.Parallel()
	r := newRig(t)

	r.dial().connect("room-1", "room-2")
	if got := r.web.request(t, 0).ChannelsRequested; !slices.Equal(got, []string{"room-1", "room-2"}) {
		t.Fatalf("ChannelsRequested = %v, want the connect frame's subs (docs/04-integration.md §1.1)", got)
	}

	r.dial().connect()
	if got := r.web.request(t, 1).ChannelsRequested; len(got) != 0 {
		t.Fatalf("ChannelsRequested = %v for a connect with no subs, want none: a request carried over from another connection", got)
	}
}

// TestConnect_ChannelsRequestedConferNoGrant_FR5: the field is a hint the application may
// ignore, and asking for a channel must never be a way of getting it.
//
// The stub application answers with room-* whatever it is asked for. A client that asks
// for a channel outside that must be refused exactly as if it had never asked, because
// the gateway's entire security model is matching a string against the list the
// application supplied — a request that fed back into the grants would let every client
// write its own.
func TestConnect_ChannelsRequestedConferNoGrant_FR5(t *testing.T) {
	t.Parallel()
	r := newRig(t)

	c := r.dial()
	reply := c.connect("room-1", "vault-1")

	if _, ok := reply.Subs["vault-1"]; ok {
		t.Fatalf("subs = %v: a channel was granted because it was requested (FR-5)", reply.Subs)
	}
	if _, ok := reply.Subs["room-1"]; !ok {
		t.Fatalf("subs = %v, want the granted channel present", reply.Subs)
	}
	// The application did see the request, so the assertion above is about the grant and
	// not about a field that never arrived.
	if got := r.web.request(t, 0).ChannelsRequested; !slices.Equal(got, []string{"room-1", "vault-1"}) {
		t.Fatalf("ChannelsRequested = %v, want both channels the client asked for", got)
	}
	c.send(map[string]any{"id": 2, "subscribe": map[string]any{"channel": "vault-1"}})
	got := c.read()
	if got.Error == nil {
		t.Fatalf("subscribe to a requested-but-ungranted channel = %+v, want an error reply (FR-5)", got)
	}
	if got.Error.Code != proto.ErrPermissionDenied {
		t.Fatalf("subscribe to a requested-but-ungranted channel = error %d, want %d (FR-5)", got.Error.Code, proto.ErrPermissionDenied)
	}
}
