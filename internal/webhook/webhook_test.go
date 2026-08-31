package webhook

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/raghulj/sidecartunnel/internal/config"
	"github.com/raghulj/sidecartunnel/internal/glob"
)

// ---------------------------------------------------------------------------
// Construction
// ---------------------------------------------------------------------------

// TestNew_Rejections: config.Validate already enforces these, but a Client built from a
// hand-made config must still fail loudly rather than deadlocking on a zero-capacity
// semaphore or panicking on an empty secret list at the first connect
// (docs/14-coding-standards.md §6).
func TestNew_Rejections(t *testing.T) {
	tests := []struct {
		name string
		mut  func(*Options)
		want string
	}{
		{
			name: "no secret",
			mut:  func(o *Options) { o.App.WebhookSecrets = nil },
			want: "webhook_secrets",
		},
		{
			name: "concurrency below one",
			mut:  func(o *Options) { o.App.WebhookConcurrency = 0 },
			want: "webhook_concurrency",
		},
		{
			name: "negative queue",
			mut:  func(o *Options) { o.App.ConnectQueue = -1 },
			want: "connect_queue",
		},
		{
			name: "negative retries",
			mut:  func(o *Options) { o.App.WebhookRetries = -1 },
			want: "webhook_retries",
		},
		{
			name: "unparseable trusted proxy",
			mut:  func(o *Options) { o.TrustedProxies = []string{"10.0.0.0/999"} },
			want: "trusted_proxies",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := Options{App: testApp("http://127.0.0.1:1")}
			tt.mut(&opts)
			_, err := New(opts)
			if err == nil {
				t.Fatal("New returned no error")
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Errorf("New error = %q, want it to name %q", err, tt.want)
			}
			if strings.Contains(err.Error(), testSecret) {
				t.Error("the error quotes the secret (NFR-7)")
			}
		})
	}
}

// TestNew_DefaultsAreUsable builds a Client with nothing optional supplied and drives a
// real call through it, which is the only way to know the defaulted http.Client, clock,
// nonce source and logger all work.
func TestNew_DefaultsAreUsable(t *testing.T) {
	app := newStubApp(t, okBody)
	c, err := New(Options{App: testApp(app.server.URL)})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	res := c.Call(t.Context(), testRequest())
	auth := mustAuthorized(t, res)
	if auth.User != "u-7" {
		t.Errorf("User = %q, want u-7", auth.User)
	}

	// The default nonce source must not repeat: an application caching seen nonces for
	// exactly-once (docs/04-integration.md §1.1) would reject a repeat as a replay.
	c.Call(t.Context(), testRequest())
	first := app.request(0).header.Get("X-St-Nonce")
	second := app.request(1).header.Get("X-St-Nonce")
	if first == "" || first == second {
		t.Errorf("nonces %q and %q; want two distinct non-empty values", first, second)
	}

	// The default timestamp must be the wall clock, within a wide margin.
	ts, err := strconv.ParseInt(app.request(0).header.Get("X-St-Timestamp"), 10, 64)
	if err != nil {
		t.Fatalf("X-St-Timestamp is not an integer: %v", err)
	}
	if skew := time.Since(time.Unix(ts, 0)); skew > time.Minute || skew < -time.Minute {
		t.Errorf("X-St-Timestamp is %s from now; the default clock is not time.Now", skew)
	}
}

// ---------------------------------------------------------------------------
// The request (§1.1) — FR-3, FR-24, C5
// ---------------------------------------------------------------------------

// TestCall_RequestShape_FR3 asserts every header docs/04-integration.md §1.1 specifies,
// and that the cookie crosses untouched. The gateway does not parse, validate, decrypt or
// shorten it: session formats belong to the application, and it cannot.
func TestCall_RequestShape_FR3(t *testing.T) {
	app := newStubApp(t, okBody)
	clock := newFakeClock(baseTime)
	app.setAppNow(clock.Now) // the stub verifies the signature and the ±300s window

	c := newTestClient(t, Options{
		App:   testApp(app.server.URL),
		Clock: clock,
		Nonce: func() string { return vecNonce },
	})

	mustAuthorized(t, c.Call(t.Context(), testRequest()))

	got := app.request(0)
	if v := got.header.Get("Cookie"); v != testCookie {
		t.Errorf("Cookie = %q, want it forwarded byte for byte as %q (FR-3)", v, testCookie)
	}
	if v := got.header.Get("X-St-Origin"); v != "https://app.example.com" {
		t.Errorf("X-St-Origin = %q", v)
	}
	if v := got.header.Get("X-St-User-Agent"); v != "Mozilla/5.0" {
		t.Errorf("X-St-User-Agent = %q", v)
	}
	if v := got.header.Get("X-St-Forwarded-For"); v != "203.0.113.9" {
		t.Errorf("X-St-Forwarded-For = %q, want the socket peer (FR-24)", v)
	}
	if v := got.header.Get("X-St-Timestamp"); v != strconv.FormatInt(baseTime.Unix(), 10) {
		t.Errorf("X-St-Timestamp = %q, want the clock's Unix seconds", v)
	}
	if v := got.header.Get("X-St-Nonce"); v != vecNonce {
		t.Errorf("X-St-Nonce = %q", v)
	}
	if v := got.header.Get("Content-Type"); v != "application/json" {
		t.Errorf("Content-Type = %q", v)
	}

	// The signature the stub already verified, checked once more against sign() so a
	// change to either side breaks this test rather than production (C5).
	want := sign([]byte(testSecret), strconv.FormatInt(baseTime.Unix(), 10), vecNonce, testCookie, got.body)
	if v := got.header.Get("X-St-Signature"); v != want {
		t.Errorf("X-St-Signature = %q, want %q", v, want)
	}

	body := decodeBody(t, got.body)
	if body["client"] != "8f2c1e04a7b3d915" {
		t.Errorf("body client = %v", body["client"])
	}
	channels, ok := body["channels_requested"].([]any)
	if !ok || len(channels) != 1 || channels[0] != "room-4410" {
		t.Errorf("body channels_requested = %v", body["channels_requested"])
	}
}

// TestCall_SignsWithTheFirstSecret_Rotation: the list exists so an application can accept
// any of several during a rotation; the gateway always signs with the first
// (docs/08-config.md §3).
func TestCall_SignsWithTheFirstSecret_Rotation(t *testing.T) {
	app := newStubApp(t, okBody) // verifies against testSecret
	cfg := testApp(app.server.URL)
	cfg.WebhookSecrets = []string{testSecret, "ffffffffffffffffffffffffffffffff"}

	c := newTestClient(t, Options{App: cfg})
	mustAuthorized(t, c.Call(t.Context(), testRequest()))

	cfg.WebhookSecrets = []string{"ffffffffffffffffffffffffffffffff", testSecret}
	c = newTestClient(t, Options{App: cfg})
	res := c.Call(t.Context(), testRequest())
	if !isUnavailable(res) {
		t.Errorf("signing with the second secret was accepted: Result = %T, want Unavailable", res)
	}
}

// TestCall_TimestampSkewAtTheBoundary drives the ±300s window from
// docs/04-integration.md §1.1 through a verifying application. The gateway's timestamp is
// its own clock, so a replica whose clock has drifted past the window is rejected on
// every call — and that must degrade the replica, not lock out its users (FR-6).
func TestCall_TimestampSkewAtTheBoundary(t *testing.T) {
	tests := []struct {
		name  string
		skew  time.Duration
		wantA bool
	}{
		{"no skew", 0, true},
		{"300s behind, on the boundary", -300 * time.Second, true},
		{"300s ahead, on the boundary", 300 * time.Second, true},
		{"301s behind, past the boundary", -301 * time.Second, false},
		{"301s ahead, past the boundary", 301 * time.Second, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			app := newStubApp(t, okBody)
			app.setAppNow(func() time.Time { return baseTime })

			clock := newFakeClock(baseTime.Add(tt.skew))
			cfg := testApp(app.server.URL)
			cfg.WebhookRetries = 0
			c := newTestClient(t, Options{App: cfg, Clock: clock})

			res := c.Call(t.Context(), testRequest())
			if _, ok := res.(Authorized); ok != tt.wantA {
				t.Fatalf("Result = %T, want Authorized == %v", res, tt.wantA)
			}
			if !tt.wantA {
				// The skewed replica is the case FR-6's 401/403 split exists for. Its
				// 403 must be transient, so the fleet degrades to the healthy replicas
				// and the operator gets an alarm, instead of every user this replica
				// serves being locked out with reconnect: false.
				u, ok := res.(Unavailable)
				if !ok {
					t.Fatalf("Result = %T, want Unavailable: a skewed clock is a gateway fault, not a decision about the user", res)
				}
				if u.CloseCode() != 3008 || !u.Reconnect() {
					t.Errorf("closes %d reconnect=%v, want 3008 true", u.CloseCode(), u.Reconnect())
				}
				if n := app.count(); n != 1 {
					t.Errorf("the application saw %d requests, want 1: a 403 must not be retried in-process", n)
				}
			}
		})
	}
}

// TestCall_ForwardedFor_FR24 drives the two ends of FR-24 through a real call. The
// spoofed row is the one that matters: forwarding a client-supplied
// X-Forwarded-For: 127.0.0.1 would let an attacker reach an application's localhost trust
// path from the public internet.
func TestCall_ForwardedFor_FR24(t *testing.T) {
	tests := []struct {
		name     string
		peer     string
		xff      string
		trusted  []string
		want     string
		wantNoIP string
	}{
		{
			name:     "untrusted peer spoofing loopback",
			peer:     "203.0.113.9:51234",
			xff:      "127.0.0.1",
			want:     "203.0.113.9",
			wantNoIP: "127.0.0.1",
		},
		{
			name:    "trusted peer, rightmost untrusted hop",
			peer:    "10.0.0.7:4000",
			xff:     "198.51.100.4, 10.0.0.3",
			trusted: []string{"10.0.0.0/8"},
			want:    "198.51.100.4",
		},
		{
			// The prepend the rightmost walk exists to ignore: the client at 10.0.0.5 is
			// not itself a configured proxy, so the walk stops there and never reaches
			// the invented hop (FR-24).
			name:    "trusted peer, spoofed prepend does not reach the application",
			peer:    "10.0.0.7:4000",
			xff:     "1.2.3.4, 10.0.0.5",
			trusted: []string{"10.0.0.7/32"},
			want:    "10.0.0.5",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			app := newStubApp(t, okBody)
			c := newTestClient(t, Options{App: testApp(app.server.URL), TrustedProxies: tt.trusted})

			req := testRequest()
			req.RemoteAddr = tt.peer
			req.ForwardedFor = tt.xff
			mustAuthorized(t, c.Call(t.Context(), req))

			got := app.request(0).header.Get("X-St-Forwarded-For")
			if got != tt.want {
				t.Errorf("X-St-Forwarded-For = %q, want %q", got, tt.want)
			}
			if tt.wantNoIP != "" && strings.Contains(got, tt.wantNoIP) {
				t.Errorf("X-St-Forwarded-For = %q, which carries the spoofed hop (FR-24)", got)
			}
		})
	}
}

// TestCall_OmitsAnEmptyChannelsRequested keeps the body to what the client actually asked
// for: a connect with no subs sends no channels_requested rather than null.
func TestCall_OmitsAnEmptyChannelsRequested(t *testing.T) {
	app := newStubApp(t, okBody)
	c := newTestClient(t, Options{App: testApp(app.server.URL)})

	req := testRequest()
	req.ChannelsRequested = nil
	mustAuthorized(t, c.Call(t.Context(), req))

	if _, present := decodeBody(t, app.request(0).body)["channels_requested"]; present {
		t.Error("channels_requested is present for a connect that requested none")
	}
}

// ---------------------------------------------------------------------------
// The status table (§1.3) — FR-6
// ---------------------------------------------------------------------------

// TestCall_StatusTable_FR6 is docs/04-integration.md §1.3, and it is the most
// consequential logic in this package. Collapsing a refusal into a failure locks every
// user out during an application deploy; collapsing a failure into a refusal turns a
// revocation into an infinite retry loop against an endpoint that is already failing.
func TestCall_StatusTable_FR6(t *testing.T) {
	tests := []struct {
		name string
		// respond writes the application's answer.
		respond func(w http.ResponseWriter, n int)
		// want is the Result type expected, as a nil pointer of that type.
		wantRefused bool
		wantStatus  int
		// wantRequests is how many times the application was called with retries at 2.
		wantRequests int
	}{
		{
			name:         "401 is a refusal and is never retried",
			respond:      func(w http.ResponseWriter, _ int) { w.WriteHeader(http.StatusUnauthorized) },
			wantRefused:  true,
			wantStatus:   401,
			wantRequests: 1,
		},
		{
			// 403 is a statement about the REQUEST, not the user: a bad signature, a
			// timestamp outside the ±300s window, an unknown key during a rotation. That
			// is a gateway-side fault, and a gateway fault must never be expressed to
			// users as a permanent refusal — a replica whose clock has drifted would
			// otherwise lock out every user it serves until a human noticed
			// (docs/04-integration.md §1.3, FR-6).
			name:        "403 is a failure, not a refusal",
			respond:     func(w http.ResponseWriter, _ int) { w.WriteHeader(http.StatusForbidden) },
			wantRefused: false,
			wantStatus:  403,
			// Transient, but not retried in-process: an immediate retry carries the same
			// bad signature or the same skewed clock, so it is load with no prospect of a
			// different answer. The client comes back after its retry_after instead
			// (docs/08-config.md §3, app.webhook_retries).
			wantRequests: 1,
		},
		{
			name:         "500 is a failure and is retried",
			respond:      func(w http.ResponseWriter, _ int) { w.WriteHeader(http.StatusInternalServerError) },
			wantRefused:  false,
			wantStatus:   500,
			wantRequests: 3,
		},
		{
			name:         "503 is a failure and is retried",
			respond:      func(w http.ResponseWriter, _ int) { w.WriteHeader(http.StatusServiceUnavailable) },
			wantRefused:  false,
			wantStatus:   503,
			wantRequests: 3,
		},
		{
			// Not in the table. Treated as a failure on purpose: a 404 means the
			// connect_url is wrong, which is an operator error, and the two candidate
			// answers are "every user is locked out until someone notices" and "every
			// user retries until someone notices". The second is recoverable.
			name:         "404 is a failure, not a refusal",
			respond:      func(w http.ResponseWriter, _ int) { w.WriteHeader(http.StatusNotFound) },
			wantRefused:  false,
			wantStatus:   404,
			wantRequests: 3,
		},
		{
			name:         "302 is a failure: the gateway does not follow redirects on a signed request",
			respond:      func(w http.ResponseWriter, _ int) { w.Header().Set("Location", "/x"); w.WriteHeader(http.StatusFound) },
			wantRefused:  false,
			wantStatus:   302,
			wantRequests: 3,
		},
		{
			name:         "2xx with a body that is not JSON is a permanent refusal",
			respond:      func(w http.ResponseWriter, _ int) { writeJSON(w, 200, "<html>login</html>") },
			wantRefused:  true,
			wantStatus:   200,
			wantRequests: 1,
		},
		{
			name:         "2xx with no body at all is a permanent refusal",
			respond:      func(w http.ResponseWriter, _ int) { w.WriteHeader(http.StatusNoContent) },
			wantRefused:  true,
			wantStatus:   204,
			wantRequests: 1,
		},
		{
			name:         "2xx missing user is a permanent refusal",
			respond:      func(w http.ResponseWriter, _ int) { writeJSON(w, 200, `{"channels":[],"expires_in":60}`) },
			wantRefused:  true,
			wantStatus:   200,
			wantRequests: 1,
		},
		{
			name:         "2xx with an empty user is a permanent refusal",
			respond:      func(w http.ResponseWriter, _ int) { writeJSON(w, 200, `{"user":"","channels":[],"expires_in":60}`) },
			wantRefused:  true,
			wantStatus:   200,
			wantRequests: 1,
		},
		{
			name:         "2xx missing channels is a permanent refusal",
			respond:      func(w http.ResponseWriter, _ int) { writeJSON(w, 200, `{"user":"u-7","expires_in":60}`) },
			wantRefused:  true,
			wantStatus:   200,
			wantRequests: 1,
		},
		{
			name: "2xx with null channels is a permanent refusal",
			respond: func(w http.ResponseWriter, _ int) {
				writeJSON(w, 200, `{"user":"u-7","channels":null,"expires_in":60}`)
			},
			wantRefused:  true,
			wantStatus:   200,
			wantRequests: 1,
		},
		{
			name:         "2xx missing expires_in is a permanent refusal",
			respond:      func(w http.ResponseWriter, _ int) { writeJSON(w, 200, `{"user":"u-7","channels":[]}`) },
			wantRefused:  true,
			wantStatus:   200,
			wantRequests: 1,
		},
		{
			name: "2xx with a reserved grant is a permanent refusal",
			respond: func(w http.ResponseWriter, _ int) {
				writeJSON(w, 200, `{"user":"u-7","channels":["_control"],"expires_in":60}`)
			},
			wantRefused:  true,
			wantStatus:   200,
			wantRequests: 1,
		},
		{
			name:         "2xx whose channels are not strings is a permanent refusal",
			respond:      func(w http.ResponseWriter, _ int) { writeJSON(w, 200, `{"user":"u-7","channels":[7],"expires_in":60}`) },
			wantRefused:  true,
			wantStatus:   200,
			wantRequests: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			app := newStubApp(t, tt.respond)
			cfg := testApp(app.server.URL)
			cfg.WebhookRetries = 2
			c := newTestClient(t, Options{App: cfg})

			res := c.Call(t.Context(), testRequest())

			switch v := res.(type) {
			case Refused:
				if !tt.wantRefused {
					t.Fatalf("Result = Refused(%v), want Unavailable: a transient failure must be retryable (FR-6)", v)
				}
				if v.Status != tt.wantStatus {
					t.Errorf("Refused.Status = %d, want %d", v.Status, tt.wantStatus)
				}
				if v.CloseCode() != 3003 || v.Reconnect() {
					t.Errorf("Refused closes %d reconnect=%v, want 3003 false", v.CloseCode(), v.Reconnect())
				}
			case Unavailable:
				if tt.wantRefused {
					t.Fatalf("Result = Unavailable(%v), want Refused: retrying a refusal is a denial-of-service against the application (FR-6)", v)
				}
				if v.Status != tt.wantStatus {
					t.Errorf("Unavailable.Status = %d, want %d", v.Status, tt.wantStatus)
				}
				if v.CloseCode() != 3008 || !v.Reconnect() {
					t.Errorf("Unavailable closes %d reconnect=%v, want 3008 true", v.CloseCode(), v.Reconnect())
				}
			default:
				t.Fatalf("Result = %T, want a refusal or a failure", res)
			}

			if n := app.count(); n != tt.wantRequests {
				t.Errorf("the application saw %d requests, want %d", n, tt.wantRequests)
			}
		})
	}
}

// TestCall_RetryCountIsExactly_WebhookRetries. FR-6's acceptance criterion names the
// number, so the number is asserted rather than "more than one".
func TestCall_RetryCountIsExactly_WebhookRetries(t *testing.T) {
	for _, retries := range []int{0, 1, 3, 5} {
		t.Run(strconv.Itoa(retries), func(t *testing.T) {
			app := newStubApp(t, func(w http.ResponseWriter, _ int) { w.WriteHeader(http.StatusBadGateway) })
			cfg := testApp(app.server.URL)
			cfg.WebhookRetries = retries
			c := newTestClient(t, Options{App: cfg})

			if res := c.Call(t.Context(), testRequest()); !isUnavailable(res) {
				t.Fatalf("Result = %T, want Unavailable", res)
			}
			if got, want := app.count(), retries+1; got != want {
				t.Errorf("the application saw %d requests, want %d (one attempt plus %d retries)", got, want, retries)
			}
		})
	}
}

// TestCall_RetrySucceeds: a 500 followed by a 200 authorizes. This is the deploy case —
// the whole reason 5xx is retryable.
func TestCall_RetrySucceeds(t *testing.T) {
	app := newStubApp(t, func(w http.ResponseWriter, n int) {
		if n == 1 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		okBody(w, n)
	})
	c := newTestClient(t, Options{App: testApp(app.server.URL)})

	auth := mustAuthorized(t, c.Call(t.Context(), testRequest()))
	if auth.User != "u-7" {
		t.Errorf("User = %q", auth.User)
	}
	if n := app.count(); n != 2 {
		t.Errorf("the application saw %d requests, want 2", n)
	}
}

// TestCall_RetryIsFreshlySigned. Each attempt carries its own timestamp, nonce and
// signature: an application caching seen nonces for exactly-once
// (docs/04-integration.md §1.1) would reject a replayed one, turning every retry into a
// refusal.
func TestCall_RetryIsFreshlySigned(t *testing.T) {
	app := newStubApp(t, func(w http.ResponseWriter, n int) {
		if n == 1 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		okBody(w, n)
	})
	c := newTestClient(t, Options{App: testApp(app.server.URL)})
	mustAuthorized(t, c.Call(t.Context(), testRequest()))

	first, second := app.request(0).header, app.request(1).header
	if first.Get("X-St-Nonce") == second.Get("X-St-Nonce") {
		t.Error("the retry reused the first attempt's nonce")
	}
	if first.Get("X-St-Signature") == second.Get("X-St-Signature") {
		t.Error("the retry reused the first attempt's signature")
	}
}

// TestCall_TransportErrorIsTransient: connection refused is "I could not tell you right
// now", not "you may not connect" (FR-6).
func TestCall_TransportErrorIsTransient(t *testing.T) {
	dead := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	url := dead.URL
	dead.Close()

	cfg := testApp(url)
	cfg.WebhookRetries = 0
	c := newTestClient(t, Options{App: cfg})

	res := c.Call(t.Context(), testRequest())
	u, ok := res.(Unavailable)
	if !ok {
		t.Fatalf("Result = %T, want Unavailable", res)
	}
	if u.Status != 0 {
		t.Errorf("Unavailable.Status = %d, want 0: nothing reached the network", u.Status)
	}
	if !strings.Contains(u.Error(), "connect webhook") {
		t.Errorf("Unavailable.Error() = %q, want it to name the operation", u.Error())
	}
}

// TestCall_UnbuildableRequestIsTransient covers a connect_url that http.NewRequest
// refuses. config.Validate rejects it at startup; this is the belt for a Client built by
// hand.
func TestCall_UnbuildableRequestIsTransient(t *testing.T) {
	cfg := testApp("http://127.0.0.1:1/\x7f")
	cfg.WebhookRetries = 0
	c := newTestClient(t, Options{App: cfg})

	if res := c.Call(t.Context(), testRequest()); !isUnavailable(res) {
		t.Fatalf("Result = %T, want Unavailable", res)
	}
}

// TestCall_TimeoutIsTransient. The handler never answers, so the only way this test can
// finish is app.webhook_timeout firing — there is no race to lose (FR-6).
func TestCall_TimeoutIsTransient(t *testing.T) {
	block := make(chan struct{})
	app := newStubApp(t, func(w http.ResponseWriter, _ int) {
		<-block
		okBody(w, 0)
	})
	// Registered after newStubApp, so it runs before the server's own cleanup: t.Cleanup
	// is LIFO, and httptest.Server.Close waits for outstanding handlers.
	t.Cleanup(func() { close(block) })

	cfg := testApp(app.server.URL)
	cfg.WebhookTimeout = config.Duration(50 * time.Millisecond)
	cfg.WebhookRetries = 0
	c := newTestClient(t, Options{App: cfg})

	res := c.Call(t.Context(), testRequest())
	u, ok := res.(Unavailable)
	if !ok {
		t.Fatalf("Result = %T, want Unavailable", res)
	}
	if !errors.Is(u, context.DeadlineExceeded) {
		t.Errorf("Unavailable does not unwrap to context.DeadlineExceeded: %v", u)
	}
}

// TestCall_TruncatedBodyIsTransient: a response that stops mid-body is a transport
// failure, not an application decision, so it is retryable.
func TestCall_TruncatedBodyIsTransient(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		conn, buf, err := http.NewResponseController(w).Hijack()
		if err != nil {
			return
		}
		defer conn.Close()
		_, _ = buf.WriteString("HTTP/1.1 200 OK\r\nContent-Length: 500\r\n\r\n{\"user\":\"u-7\"")
		_ = buf.Flush()
	}))
	t.Cleanup(server.Close)

	cfg := testApp(server.URL)
	cfg.WebhookRetries = 0
	c := newTestClient(t, Options{App: cfg})

	if res := c.Call(t.Context(), testRequest()); !isUnavailable(res) {
		t.Fatalf("Result = %T, want Unavailable: a truncated response is a transport failure", res)
	}
}

// TestCall_OversizeBodyIsRefused caps what one answer may cost. A body larger than the cap
// is truncated, so it will not parse, and the connection is refused rather than the
// gateway buffering whatever the application sends per connect.
func TestCall_OversizeBodyIsRefused(t *testing.T) {
	app := newStubApp(t, func(w http.ResponseWriter, _ int) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"user":"u-7","channels":["` + strings.Repeat("a", maxResponseBytes) + `"],"expires_in":60}`))
	})
	cfg := testApp(app.server.URL)
	cfg.WebhookRetries = 0
	c := newTestClient(t, Options{App: cfg})

	res := c.Call(t.Context(), testRequest())
	refused, ok := res.(Refused)
	if !ok {
		t.Fatalf("Result = %T, want Refused", res)
	}
	if !errors.Is(refused, ErrMalformedResponse) {
		t.Errorf("Refused does not unwrap to ErrMalformedResponse: %v", refused)
	}
}

// ---------------------------------------------------------------------------
// The response (§1.2)
// ---------------------------------------------------------------------------

// TestCall_GrantsAreCompiledHere. Compiling at authorization time is what makes a
// malformed grant refuse the connection instead of surfacing minutes later as one
// subscribe that mysteriously fails (docs/05-authorization.md §3).
func TestCall_GrantsAreCompiledHere(t *testing.T) {
	app := newStubApp(t, okBody) // channels: ["room-4410", "org-42-*"]
	c := newTestClient(t, Options{App: testApp(app.server.URL)})

	auth := mustAuthorized(t, c.Call(t.Context(), testRequest()))

	tests := []struct {
		channel string
		want    bool
	}{
		{"room-4410", true},
		{"room-44100", false},
		{"org-42-alerts", true},
		{"org-99-secret", false},
	}
	for _, tt := range tests {
		if got := auth.Grants.Match(tt.channel); got != tt.want {
			t.Errorf("Grants.Match(%q) = %v, want %v", tt.channel, got, tt.want)
		}
	}
}

// TestCall_EmptyGrantsAreLegal: a connection with no grants is legal and simply cannot
// subscribe to anything (docs/04-integration.md §1.2). Refusing it here would break
// applications that connect an anonymous visitor for presence alone.
func TestCall_EmptyGrantsAreLegal(t *testing.T) {
	app := newStubApp(t, func(w http.ResponseWriter, _ int) {
		writeJSON(w, 200, `{"user":"anon","channels":[],"expires_in":3600}`)
	})
	c := newTestClient(t, Options{App: testApp(app.server.URL)})

	auth := mustAuthorized(t, c.Call(t.Context(), testRequest()))
	if auth.User != "anon" {
		t.Errorf("User = %q, want anon", auth.User)
	}
	if auth.Grants.Match("room-4410") {
		t.Error("an empty grant list matched a channel")
	}
}

// TestCall_ExpiresInIsClamped_BothWays. The clamped value is what the caller reports to
// the client: a client told 24h by an application whose answer was clamped to 6h would
// schedule its own refresh two close frames too late (docs/04-integration.md §1.2).
func TestCall_ExpiresInIsClamped_BothWays(t *testing.T) {
	tests := []struct {
		name      string
		expiresIn string
		want      time.Duration
	}{
		{"inside the range is untouched", "3600", time.Hour},
		{"below min_expiry clamps up", "5", 60 * time.Second},
		{"exactly min_expiry", "60", 60 * time.Second},
		{"above max_expiry clamps down", "86400", 6 * time.Hour},
		{"exactly max_expiry", "21600", 6 * time.Hour},
		{"zero clamps up", "0", 60 * time.Second},
		{"negative clamps up", "-30", 60 * time.Second},
		{"absurd value cannot overflow", "9223372036854775807", 6 * time.Hour},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			app := newStubApp(t, func(w http.ResponseWriter, _ int) {
				writeJSON(w, 200, `{"user":"u-7","channels":[],"expires_in":`+tt.expiresIn+`}`)
			})
			c := newTestClient(t, Options{App: testApp(app.server.URL)})

			auth := mustAuthorized(t, c.Call(t.Context(), testRequest()))
			if auth.ExpiresIn != tt.want {
				t.Errorf("ExpiresIn = %s, want %s", auth.ExpiresIn, tt.want)
			}
		})
	}
}

// TestCall_InfoIsIgnored: info is opaque and reserved for presence in a later milestone.
// An application sending one today must not be refused for it
// (docs/04-integration.md §1.2).
func TestCall_InfoIsIgnored(t *testing.T) {
	app := newStubApp(t, func(w http.ResponseWriter, _ int) {
		writeJSON(w, 200, `{"user":"u-7","channels":["a"],"expires_in":60,"info":{"name":"Ada"},"future":[1,2]}`)
	})
	c := newTestClient(t, Options{App: testApp(app.server.URL)})

	auth := mustAuthorized(t, c.Call(t.Context(), testRequest()))
	if auth.User != "u-7" {
		t.Errorf("User = %q", auth.User)
	}
}

// ---------------------------------------------------------------------------
// FR-22 / C3 — no cookie retention
// ---------------------------------------------------------------------------

// TestCall_RetainsNoCookie_FR22. The gateway MUST NOT retain the client's cookie beyond
// the connect call. An earlier design cached it for revalidation, which made the process
// a session-hijacking toolkit and broke every application that rotates its session —
// Django SESSION_SAVE_EVERY_REQUEST, Rails, any app calling cycle_key()
// (docs/13-review-findings.md C3, docs/05-authorization.md §6).
func TestCall_RetainsNoCookie_FR22(t *testing.T) {
	app := newStubApp(t, okBody)
	cfg := testApp(app.server.URL)
	cfg.CacheTTL = config.Duration(time.Minute) // the cache is where a cookie would hide
	cfg.CookieNames = []string{"sessionid"}
	c := newTestClient(t, Options{App: cfg, Clock: newFakeClock(baseTime)})

	auth := mustAuthorized(t, c.Call(t.Context(), testRequest()))

	// Nothing returned to the caller carries it.
	if strings.Contains(auth.User, "s3cr3t-session-value") {
		t.Error("the Authorized result carries the cookie")
	}
	// Nothing stored carries it: the key is a digest and the value is an answer.
	for key, entry := range c.cache.entries {
		if strings.Contains(key, "s3cr3t-session-value") || strings.Contains(key, "sessionid") {
			t.Errorf("cache key %q contains the cookie", key)
		}
		if strings.Contains(entry.value.User, "s3cr3t-session-value") {
			t.Error("a cached value contains the cookie")
		}
	}
	if c.cache.size() != 1 {
		t.Fatalf("cache size = %d, want 1", c.cache.size())
	}
}

// ---------------------------------------------------------------------------
// The cache (C4)
// ---------------------------------------------------------------------------

// TestCall_CacheOffByDefault. app.cache_ttl defaults to 0 and 0 means off, because a
// cached entry survives a revocation (docs/04-integration.md §1.5).
func TestCall_CacheOffByDefault(t *testing.T) {
	app := newStubApp(t, okBody)
	c := newTestClient(t, Options{App: testApp(app.server.URL)})

	mustAuthorized(t, c.Call(t.Context(), testRequest()))
	mustAuthorized(t, c.Call(t.Context(), testRequest()))

	if n := app.count(); n != 2 {
		t.Errorf("the application saw %d requests, want 2: the cache is on by default", n)
	}
	if c.cache != nil {
		t.Error("a cache was constructed for cache_ttl 0")
	}
	c.Flush() // must be safe with no cache at all
}

// TestCall_CacheHitMissFlushExpiry_C4 drives the whole cache contract through Call,
// including the row the finding turns on: two tabs of one user differ only in _ga and
// must hit the same entry (docs/13-review-findings.md C4).
func TestCall_CacheHitMissFlushExpiry_C4(t *testing.T) {
	app := newStubApp(t, okBody)
	clock := newFakeClock(baseTime)
	cfg := testApp(app.server.URL)
	cfg.CacheTTL = config.Duration(30 * time.Second)
	cfg.CookieNames = []string{"sessionid"}
	c := newTestClient(t, Options{App: cfg, Clock: clock})

	tabOne := testRequest()
	tabOne.Cookie = "_ga=GA1.1.111; sessionid=abc123"
	tabTwo := testRequest()
	tabTwo.Cookie = "_ga=GA1.1.222; sessionid=abc123" // same session, different _ga

	mustAuthorized(t, c.Call(t.Context(), tabOne))
	if n := app.count(); n != 1 {
		t.Fatalf("the application saw %d requests after the first call, want 1", n)
	}

	second := mustAuthorized(t, c.Call(t.Context(), tabTwo))
	if n := app.count(); n != 1 {
		t.Errorf("the application saw %d requests, want 1: two tabs of one user missed each other (C4)", n)
	}
	if second.User != "u-7" || second.ExpiresIn != time.Hour {
		t.Errorf("cached answer = %+v, want the stored one", second)
	}

	// A different session is a different key.
	other := testRequest()
	other.Cookie = "sessionid=zzz999"
	mustAuthorized(t, c.Call(t.Context(), other))
	if n := app.count(); n != 2 {
		t.Errorf("the application saw %d requests, want 2: a different session hit another user's entry", n)
	}

	// Flush is what the control channel calls on disconnect, because a cached entry
	// otherwise survives a revocation (C4).
	c.Flush()
	mustAuthorized(t, c.Call(t.Context(), tabOne))
	if n := app.count(); n != 3 {
		t.Errorf("the application saw %d requests, want 3: Flush did not clear the entry", n)
	}

	// And the TTL bounds it even without a flush.
	clock.advance(31 * time.Second)
	mustAuthorized(t, c.Call(t.Context(), tabOne))
	if n := app.count(); n != 4 {
		t.Errorf("the application saw %d requests, want 4: the entry outlived cache_ttl", n)
	}
}

// TestCall_OnlyAuthorizedIsCached. Caching a refusal would keep a user out for a TTL after
// the application let them back in; caching a failure would keep answering with a failure
// after the application recovered.
func TestCall_OnlyAuthorizedIsCached(t *testing.T) {
	tests := []struct {
		name    string
		respond func(w http.ResponseWriter, n int)
	}{
		{"refusal", func(w http.ResponseWriter, _ int) { w.WriteHeader(http.StatusUnauthorized) }},
		{"failure", func(w http.ResponseWriter, _ int) { w.WriteHeader(http.StatusInternalServerError) }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			app := newStubApp(t, tt.respond)
			cfg := testApp(app.server.URL)
			cfg.CacheTTL = config.Duration(time.Minute)
			cfg.WebhookRetries = 0
			c := newTestClient(t, Options{App: cfg, Clock: newFakeClock(baseTime)})

			c.Call(t.Context(), testRequest())
			c.Call(t.Context(), testRequest())

			if n := app.count(); n != 2 {
				t.Errorf("the application saw %d requests, want 2: a non-authorized answer was cached", n)
			}
		})
	}
}

// TestCall_UncacheableCookieIsNotCached: with cookie_names configured and none of them
// present, the request has no key. Sharing one key across every such request would serve
// one user's grants to another.
func TestCall_UncacheableCookieIsNotCached(t *testing.T) {
	app := newStubApp(t, okBody)
	cfg := testApp(app.server.URL)
	cfg.CacheTTL = config.Duration(time.Minute)
	cfg.CookieNames = []string{"sessionid"}
	c := newTestClient(t, Options{App: cfg, Clock: newFakeClock(baseTime)})

	req := testRequest()
	req.Cookie = "_ga=GA1.1.111"
	mustAuthorized(t, c.Call(t.Context(), req))
	mustAuthorized(t, c.Call(t.Context(), req))

	if n := app.count(); n != 2 {
		t.Errorf("the application saw %d requests, want 2", n)
	}
	if c.cache.size() != 0 {
		t.Errorf("cache size = %d, want 0", c.cache.size())
	}
}

// ---------------------------------------------------------------------------
// Grant compilation errors surface as refusals
// ---------------------------------------------------------------------------

// TestCall_ReservedGrantUnwrapsToGlob keeps the wrap chain intact, so an operator reading
// the log sees which rule the application broke (docs/14-coding-standards.md §6).
func TestCall_ReservedGrantUnwrapsToGlob(t *testing.T) {
	app := newStubApp(t, func(w http.ResponseWriter, _ int) {
		writeJSON(w, 200, `{"user":"u-7","channels":["room-1","_control"],"expires_in":60}`)
	})
	c := newTestClient(t, Options{App: testApp(app.server.URL)})

	res := c.Call(t.Context(), testRequest())
	refused, ok := res.(Refused)
	if !ok {
		t.Fatalf("Result = %T, want Refused", res)
	}
	if !errors.Is(refused, glob.ErrReservedPrefix) {
		t.Errorf("Refused does not unwrap to glob.ErrReservedPrefix: %v", refused)
	}
	if !errors.Is(refused, ErrMalformedResponse) {
		t.Errorf("Refused does not unwrap to ErrMalformedResponse: %v", refused)
	}
}

// TestCall_OmitsEmptyHeaders. A non-browser client sends no Cookie and no Origin, and
// server.allow_missing_origin lets it connect. The gateway sends no empty headers for
// them: an application reading a present-but-empty Cookie has to special-case a value the
// browser never produced. The signature still covers the digest of "", which is what the
// application computes for a header it did not receive.
func TestCall_OmitsEmptyHeaders(t *testing.T) {
	app := newStubApp(t, okBody)
	c := newTestClient(t, Options{App: testApp(app.server.URL)})

	req := testRequest()
	req.Cookie = ""
	req.Origin = ""
	req.UserAgent = ""
	req.RemoteAddr = "@/tmp/unix-socket" // yields no forwardable address
	mustAuthorized(t, c.Call(t.Context(), req))

	got := app.request(0).header
	for _, key := range []string{"Cookie", "X-St-Origin", "X-St-User-Agent", "X-St-Forwarded-For"} {
		if _, present := got[http.CanonicalHeaderKey(key)]; present {
			t.Errorf("%s is present with an empty value", key)
		}
	}
	// The signature is still sent, and still verifies: the stub application rejects a bad
	// one with a 403, and this call authorized.
	if got.Get("X-St-Signature") == "" {
		t.Error("no signature was sent for a cookieless request")
	}
}

// TestCall_DrainErrorIsHarmless: the gateway drains a body it will not parse so the
// connection returns to the idle pool. A drain that fails costs one pooled connection and
// changes nothing about the answer, which was already decided by the status line.
func TestCall_DrainErrorIsHarmless(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		conn, buf, err := http.NewResponseController(w).Hijack()
		if err != nil {
			return
		}
		defer conn.Close()
		// A Content-Length that promises far more than is written, then a close.
		_, _ = buf.WriteString("HTTP/1.1 500 Internal Server Error\r\nContent-Length: 500\r\n\r\nboom")
		_ = buf.Flush()
	}))
	t.Cleanup(server.Close)

	cfg := testApp(server.URL)
	cfg.WebhookRetries = 0
	c := newTestClient(t, Options{App: cfg})

	res := c.Call(t.Context(), testRequest())
	u, ok := res.(Unavailable)
	if !ok {
		t.Fatalf("Result = %T, want Unavailable", res)
	}
	if u.Status != 500 {
		t.Errorf("Unavailable.Status = %d, want 500: the status line decided the answer", u.Status)
	}
}

// TestCall_401And403AreDifferentTypes_FR6 is the split stated directly.
//
// 401 is a statement about the user: they may not connect, and the client must stop
// asking. 403 is a statement about the request — a bad signature, a timestamp outside the
// ±300s window, an unknown key during a rotation — which is a gateway-side fault. Merged,
// a replica whose clock drifts past 300s locks out every user it serves with
// reconnect: false, and they stay locked out until a human notices. Split, they retry,
// the fleet degrades to the healthy replicas, and the operator gets an alarm instead of
// an outage (FR-6, docs/04-integration.md §1.3).
func TestCall_401And403AreDifferentTypes_FR6(t *testing.T) {
	call := func(status int) Result {
		t.Helper()
		app := newStubApp(t, func(w http.ResponseWriter, _ int) { w.WriteHeader(status) })
		cfg := testApp(app.server.URL)
		cfg.WebhookRetries = 2
		c := newTestClient(t, Options{App: cfg})
		res := c.Call(t.Context(), testRequest())
		if n := app.count(); n != 1 {
			t.Errorf("a %d produced %d requests, want 1: neither is retried in-process", status, n)
		}
		return res
	}

	unauthorized := call(http.StatusUnauthorized)
	forbidden := call(http.StatusForbidden)

	refused, ok := unauthorized.(Refused)
	if !ok {
		t.Fatalf("401 gave %T, want Refused", unauthorized)
	}
	unavailable, ok := forbidden.(Unavailable)
	if !ok {
		t.Fatalf("403 gave %T, want Unavailable: a gateway fault must not be a permanent refusal", forbidden)
	}

	// Distinguishable by type, which is the whole point of Result being an interface.
	if _, alsoRefused := forbidden.(Refused); alsoRefused {
		t.Error("403 is also a Refused")
	}
	if _, alsoUnavailable := unauthorized.(Unavailable); alsoUnavailable {
		t.Error("401 is also an Unavailable")
	}

	if refused.CloseCode() != 3003 || refused.Reconnect() {
		t.Errorf("401 closes %d reconnect=%v, want 3003 false", refused.CloseCode(), refused.Reconnect())
	}
	if unavailable.CloseCode() != 3008 || !unavailable.Reconnect() {
		t.Errorf("403 closes %d reconnect=%v, want 3008 true", unavailable.CloseCode(), unavailable.Reconnect())
	}
	if !errors.Is(unavailable, ErrRequestRejected) {
		t.Errorf("403 does not unwrap to ErrRequestRejected: %v", unavailable)
	}
	if errors.Is(refused, ErrRequestRejected) {
		t.Error("401 unwraps to ErrRequestRejected")
	}
}

// TestCall_RejectionIsCountedSeparately_FR6: "my application is down" and "my gateway
// cannot authenticate to my application" must be distinguishable at a glance, so a 403 is
// counted apart from 5xx (docs/04-integration.md §1.3).
func TestCall_RejectionIsCountedSeparately_FR6(t *testing.T) {
	app := newStubApp(t, func(w http.ResponseWriter, n int) {
		switch n {
		case 1:
			w.WriteHeader(http.StatusForbidden)
		case 2:
			w.WriteHeader(http.StatusUnauthorized)
		case 3:
			w.WriteHeader(http.StatusInternalServerError)
		default:
			okBody(w, n)
		}
	})
	cfg := testApp(app.server.URL)
	cfg.WebhookRetries = 0
	c := newTestClient(t, Options{App: cfg, Clock: newFakeClock(baseTime)})

	for range 4 {
		c.Call(t.Context(), testRequest())
	}

	got := c.Stats()
	want := Stats{Authorized: 1, Refused: 1, Rejected: 1, Failed: 1}
	if got != want {
		t.Errorf("Stats() = %+v, want %+v", got, want)
	}
}

// TestCall_RejectionIsLoggedOncePerInterval_FR6. Retrying cannot fix a bad secret or a
// skewed clock, so the 403 is loud — but at 25,000 reconnecting connections it must be
// loud once per interval and not once per connection, or the signal that tells the
// operator what is wrong is buried under the incident it is describing.
func TestCall_RejectionIsLoggedOncePerInterval_FR6(t *testing.T) {
	var out syncBuffer
	logger := slog.New(slog.NewJSONHandler(&out, &slog.HandlerOptions{Level: slog.LevelDebug}))

	app := newStubApp(t, func(w http.ResponseWriter, _ int) { w.WriteHeader(http.StatusForbidden) })
	clock := newFakeClock(baseTime)
	cfg := testApp(app.server.URL)
	cfg.WebhookRetries = 0
	c := newTestClient(t, Options{
		App:               cfg,
		Clock:             clock,
		Logger:            logger,
		RejectLogInterval: time.Minute,
	})

	for range 5 {
		c.Call(t.Context(), testRequest())
	}
	if got := strings.Count(out.String(), `"level":"ERROR"`); got != 1 {
		t.Errorf("five rejections logged %d ERROR lines, want 1:\n%s", got, out.String())
	}

	// Still inside the interval: still one line.
	clock.advance(59 * time.Second)
	c.Call(t.Context(), testRequest())
	if got := strings.Count(out.String(), `"level":"ERROR"`); got != 1 {
		t.Errorf("a rejection inside the interval logged again: %d ERROR lines", got)
	}

	// Past it: the operator hears about it again, because it is still broken.
	clock.advance(2 * time.Second)
	c.Call(t.Context(), testRequest())
	if got := strings.Count(out.String(), `"level":"ERROR"`); got != 2 {
		t.Errorf("a rejection past the interval logged %d ERROR lines, want 2", got)
	}

	logged := out.String()
	for _, want := range []string{"403", "8f2c1e04a7b3d915"} {
		if !strings.Contains(logged, want) {
			t.Errorf("the ERROR line does not contain %q:\n%s", want, logged)
		}
	}
	// Suppressed rejections are still visible at debug: the count of connections
	// affected is what an operator asks for next.
	if got := strings.Count(logged, `"level":"DEBUG"`); got != 5 {
		t.Errorf("suppressed rejections logged %d DEBUG lines, want 5", got)
	}
	// And nothing here carries a secret (NFR-7).
	for _, banned := range []string{testSecret, "s3cr3t-session-value"} {
		if strings.Contains(logged, banned) {
			t.Errorf("the rejection log contains a secret (NFR-7):\n%s", logged)
		}
	}
}

// TestCall_RejectLogIntervalDefaults: an operator who configures nothing still gets one
// line per minute rather than one per connection.
func TestCall_RejectLogIntervalDefaults(t *testing.T) {
	app := newStubApp(t, func(w http.ResponseWriter, _ int) { w.WriteHeader(http.StatusForbidden) })
	c := newTestClient(t, Options{App: testApp(app.server.URL)})
	if c.rejectLogInterval != defaultRejectLogInterval {
		t.Errorf("rejectLogInterval = %s, want %s", c.rejectLogInterval, defaultRejectLogInterval)
	}
}
