package server

import (
	"net/http"
	"strings"
	"testing"

	"github.com/raghulj/sidecartunnel/internal/config"
)

// TestOrigin_ExactMatchOnly_FR2 is the most important test in this package.
//
// Browsers do not apply CORS to websocket handshakes but they do attach cookies, so this
// allowlist is the only thing standing between a logged-in user and cross-site websocket
// hijacking (docs/05-authorization.md §5). The rows that matter are the near misses: a
// subdomain, a scheme change, a trailing slash, a suffix. Every one of them must be
// refused, because a wildcard or a suffix match is how an attacker who controls one
// forgotten subdomain gets everything.
func TestOrigin_ExactMatchOnly_FR2(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name          string
		allowed       []string
		allowMissing  bool
		origin        string
		wantConnected bool
	}{
		{name: "exact match", allowed: []string{testOrigin}, origin: testOrigin, wantConnected: true},
		{name: "second entry in the list", allowed: []string{"https://other.example", testOrigin}, origin: testOrigin, wantConnected: true},
		{name: "a foreign origin", allowed: []string{testOrigin}, origin: "https://evil.example"},
		{name: "a subdomain of an allowed origin", allowed: []string{testOrigin}, origin: "https://evil.app.example.com"},
		{name: "an origin the allowed one is a suffix of", allowed: []string{testOrigin}, origin: "https://notapp.example.com"},
		{name: "the same host on another scheme", allowed: []string{testOrigin}, origin: "http://app.example.com"},
		{name: "the same origin with a trailing slash", allowed: []string{testOrigin}, origin: testOrigin + "/"},
		{name: "the same origin with a port", allowed: []string{testOrigin}, origin: testOrigin + ":443"},
		{name: "case differs", allowed: []string{testOrigin}, origin: "https://APP.example.com"},
		{name: "no Origin header, allow_missing_origin off", allowed: []string{testOrigin}, origin: ""},
		{name: "no Origin header, allow_missing_origin on", allowed: []string{testOrigin}, allowMissing: true, origin: "", wantConnected: true},
		{name: "a foreign origin is still refused with allow_missing_origin on", allowed: []string{testOrigin}, allowMissing: true, origin: "https://evil.example"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			r := newRig(t, func(c *config.Config) {
				c.Server.AllowedOrigins = tt.allowed
				c.Server.AllowMissingOrigin = tt.allowMissing
			})

			c, status, err := r.dialOrigin(tt.origin)
			if tt.wantConnected {
				if err != nil {
					t.Fatalf("dial with origin %q: %v", tt.origin, err)
				}
				c.connect()
				return
			}

			// FR-2: 403 and stop. There is deliberately no close code for a rejected
			// Origin — the check completes before the upgrade, so no websocket exists on
			// which to send one (docs/03-client-protocol.md §7, M14).
			if got := statusOf(t, status, err); got != http.StatusForbidden {
				t.Fatalf("status = %d, want %d for origin %q", got, http.StatusForbidden, tt.origin)
			}
			// FR-2's acceptance criterion, and the reason this stub counts calls: the
			// application is never asked about a connection the gateway has already
			// refused.
			if got := r.web.count(); got != 0 {
				t.Fatalf("webhook calls = %d, want 0: a rejected Origin must reach no application (FR-2)", got)
			}
			if got := r.srv.Stats().OriginRejected; got != 1 {
				t.Fatalf("OriginRejected = %d, want 1", got)
			}
		})
	}
}

// TestOrigin_RejectionDoesNotLogTheCookie_NFR7: the 403 path logs, and this process sees
// every connected user's session cookie. The log must never become a credential store.
func TestOrigin_RejectionDoesNotLogTheCookie_NFR7(t *testing.T) {
	t.Parallel()
	r := newRig(t)

	_, status, err := r.dialOrigin("https://evil.example")
	if got := statusOf(t, status, err); got != http.StatusForbidden {
		t.Fatalf("status = %d, want 403", got)
	}

	if logs := r.logs.String(); strings.Contains(logs, "secret-cookie-value") {
		t.Fatalf("the cookie value reached the log (NFR-7):\n%s", logs)
	}
}

// TestOrigin_ForwardedToTheApplication_FR3: the Origin the gateway checked is the one the
// application is told about, so an application that keeps its own record can see it.
func TestOrigin_ForwardedToTheApplication_FR3(t *testing.T) {
	t.Parallel()
	r := newRig(t)
	r.dial().connect()

	got := r.web.request(t, 0)
	if got.Origin != testOrigin {
		t.Fatalf("webhook Origin = %q, want %q", got.Origin, testOrigin)
	}
	if got.Cookie != "session=secret-cookie-value" {
		t.Fatalf("webhook Cookie = %q, want the browser's header verbatim (FR-3)", got.Cookie)
	}
	if got.UserAgent != "test-agent" {
		t.Fatalf("webhook UserAgent = %q, want the handshake's", got.UserAgent)
	}
	// FR-24: the socket peer reaches the webhook client, which owns the walk over
	// X-Forwarded-For and server.trusted_proxies. This package must never do that walk
	// itself, and must never pass a client-supplied header through as the peer.
	if got.RemoteAddr == "" {
		t.Fatal("webhook RemoteAddr is empty; the webhook client cannot derive X-St-Forwarded-For without the socket peer (FR-24)")
	}
	if got.Client == "" {
		t.Fatal("webhook Client is empty; it is the join key across every log line for a connection")
	}
}

// TestForwardedFor_PassedThroughUnwalked_FR24: the inbound header reaches the webhook
// client as it arrived, because that package owns the trusted-proxy walk. Deciding here
// would put the same rule in two places, and the one that drifts is the one that forwards
// a client-supplied 127.0.0.1 to an application's localhost trust path.
func TestForwardedFor_PassedThroughUnwalked_FR24(t *testing.T) {
	t.Parallel()
	r := newRig(t)

	header := http.Header{"Origin": {testOrigin}, "X-Forwarded-For": {"203.0.113.9, 10.0.0.1"}}
	ws, err := dialWith(t, r.wsURL(), header)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = ws.Close() }()
	(&client{t: t, ws: ws}).connect()

	if got := r.web.request(t, 0).ForwardedFor; got != "203.0.113.9, 10.0.0.1" {
		t.Fatalf("webhook ForwardedFor = %q, want the inbound header verbatim (FR-24)", got)
	}
}
