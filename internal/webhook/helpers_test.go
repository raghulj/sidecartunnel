package webhook

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/raghulj/sidecartunnel/internal/config"
)

// testSecret is 32 bytes, the minimum config.Validate accepts.
const testSecret = "0123456789abcdef0123456789abcdef"

// testCookie is the value every test forwards. It is deliberately distinctive so the
// NFR-7 log test can search for it and find nothing.
const testCookie = "sessionid=s3cr3t-session-value; _ga=GA1.1.111"

// fakeClock is the injected time source. Tests move time rather than passing it, because
// a sleep is either a flake or a wasted second and usually both
// (docs/14-coding-standards.md §2).
type fakeClock struct {
	mu         sync.Mutex
	now        time.Time
	seq        int
	pending    map[int]chan time.Time
	registered chan struct{}
}

func newFakeClock(at time.Time) *fakeClock {
	return &fakeClock{now: at, pending: map[int]chan time.Time{}, registered: make(chan struct{}, 256)}
}

// Now returns the current fake instant.
func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// NewTimer hands back a channel the test fires explicitly with fireTimers, and a stop
// function that forgets it. It also signals registration, so a test can wait for the code
// under test to start waiting instead of guessing that it has.
//
// A stopped timer leaves nothing behind, which is what outstanding counts: a production
// timer that is never stopped is one the runtime holds to its deadline.
func (c *fakeClock) NewTimer(time.Duration) (<-chan time.Time, func()) {
	ch := make(chan time.Time, 1)
	c.mu.Lock()
	c.seq++
	id := c.seq
	c.pending[id] = ch
	c.mu.Unlock()
	select {
	case c.registered <- struct{}{}:
	default:
	}
	return ch, func() {
		c.mu.Lock()
		defer c.mu.Unlock()
		delete(c.pending, id)
	}
}

// outstanding reports how many timers the code under test is still holding. A timer that
// is never released is a timer the runtime keeps until it fires, which at
// connect_queue: 4096 and connect_timeout: 10s is 4096 of them held through exactly the
// reconnect storm the queue exists to survive.
func (c *fakeClock) outstanding() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.pending)
}

// advance moves the clock without firing anything.
func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// fireTimers fires every timer still armed.
func (c *fakeClock) fireTimers() {
	c.mu.Lock()
	pending := c.pending
	c.pending = map[int]chan time.Time{}
	now := c.now
	c.mu.Unlock()
	for _, ch := range pending {
		ch <- now
	}
}

// awaitTimer blocks until the code under test has asked for a timer. The timeout is a
// failure detector, not a delay: the happy path takes microseconds.
func (c *fakeClock) awaitTimer(t *testing.T) {
	t.Helper()
	select {
	case <-c.registered:
	case <-time.After(2 * time.Second):
		t.Fatal("nothing asked for a timer within 2s")
	}
}

// capturedRequest is what the stub application saw. It never stores anything this package
// is forbidden to log; the tests need the cookie precisely to assert it was forwarded
// byte for byte.
type capturedRequest struct {
	header http.Header
	body   []byte
}

// stubApp is the application side of the connect webhook: an httptest server plus the
// verification docs/04-integration.md §1.4 tells an application to perform.
type stubApp struct {
	t      *testing.T
	server *httptest.Server

	mu       sync.Mutex
	requests []capturedRequest

	// appNow is the application's clock, used for the ±300s skew check. Tests that do
	// not care leave it nil and no skew check happens.
	appNow func() time.Time

	// respond writes the reply. It is called after verification.
	respond func(w http.ResponseWriter, n int)
}

// newStubApp starts a stub application. respond receives the 1-based request number so a
// test can answer the first call differently from the second.
func newStubApp(t *testing.T, respond func(w http.ResponseWriter, n int)) *stubApp {
	t.Helper()
	app := &stubApp{t: t, respond: respond}
	app.server = httptest.NewServer(http.HandlerFunc(app.serve))
	t.Cleanup(app.server.Close)
	return app
}

func (a *stubApp) serve(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		a.t.Errorf("reading the request body: %v", err)
		return
	}

	a.mu.Lock()
	a.requests = append(a.requests, capturedRequest{header: r.Header.Clone(), body: body})
	n := len(a.requests)
	appNow := a.appNow
	a.mu.Unlock()

	// Verify the signature FIRST, exactly as docs/04-integration.md §1.4 instructs an
	// application to: parsing attacker-controlled input before authenticating it turns a
	// malformed header into an unauthenticated 500, which the gateway then treats as
	// transient and retries.
	ts := r.Header.Get("X-St-Timestamp")
	nonce := r.Header.Get("X-St-Nonce")
	cookieDigest := sha256.Sum256([]byte(r.Header.Get("Cookie")))
	bodyDigest := sha256.Sum256(body)
	signed := ts + "." + nonce + "." + hex.EncodeToString(cookieDigest[:]) + "." + hex.EncodeToString(bodyDigest[:])
	mac := hmac.New(sha256.New, []byte(testSecret))
	mac.Write([]byte(signed))
	expected := hex.EncodeToString(mac.Sum(nil))
	if !hmac.Equal([]byte(expected), []byte(r.Header.Get("X-St-Signature"))) {
		http.Error(w, "bad signature", http.StatusForbidden)
		return
	}

	if appNow != nil {
		seconds, err := strconv.ParseInt(ts, 10, 64)
		if err != nil {
			http.Error(w, "bad timestamp", http.StatusForbidden)
			return
		}
		skew := appNow().Sub(time.Unix(seconds, 0))
		if skew < 0 {
			skew = -skew
		}
		if skew > 300*time.Second {
			http.Error(w, "stale timestamp", http.StatusForbidden)
			return
		}
	}

	a.respond(w, n)
}

// count is the number of requests the application received.
func (a *stubApp) count() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.requests)
}

// request returns the i-th captured request, 0-based.
func (a *stubApp) request(i int) capturedRequest {
	a.mu.Lock()
	defer a.mu.Unlock()
	if i >= len(a.requests) {
		a.t.Fatalf("request %d was never made; the application saw %d", i, len(a.requests))
	}
	return a.requests[i]
}

// setAppNow installs the application's clock for the skew check.
func (a *stubApp) setAppNow(now func() time.Time) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.appNow = now
}

// okBody is the canonical 200 from docs/04-integration.md §1.2.
func okBody(w http.ResponseWriter, _ int) {
	writeJSON(w, http.StatusOK, `{"user":"u-7","channels":["room-4410","org-42-*"],"expires_in":3600,"info":{"name":"Ada"}}`)
}

func writeJSON(w http.ResponseWriter, status int, body string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = io.WriteString(w, body)
}

// testApp is the app configuration every test starts from: the documented defaults from
// docs/08-config.md §3, pointed at the stub.
func testApp(url string) config.App {
	return config.App{
		Name:               "app",
		ConnectURL:         url,
		WebhookSecrets:     []string{testSecret},
		ConnectTimeout:     config.Duration(10 * time.Second),
		ConnectQueue:       4096,
		WebhookTimeout:     config.Duration(3 * time.Second),
		WebhookConcurrency: 32,
		WebhookRetries:     1,
		CacheTTL:           0,
		MinExpiry:          config.Duration(60 * time.Second),
		MaxExpiry:          config.Duration(6 * time.Hour),
	}
}

// testRequest is the per-connection input every test starts from.
func testRequest() Request {
	return Request{
		Client:            "8f2c1e04a7b3d915",
		Cookie:            testCookie,
		Origin:            "https://app.example.com",
		UserAgent:         "Mozilla/5.0",
		RemoteAddr:        "203.0.113.9:51234",
		ChannelsRequested: []string{"room-4410"},
	}
}

// newTestClient builds a Client from Options, failing the test on a construction error.
//
// It installs a discarding logger unless the test supplies one, so a suite that drives
// hundreds of failing webhooks does not bury its own output. TestNew_DefaultsAreUsable
// calls New directly, so the default slog.Default() path is still exercised.
func newTestClient(t *testing.T, opts Options) *Client {
	t.Helper()
	if opts.Logger == nil {
		opts.Logger = slog.New(slog.DiscardHandler)
	}
	c, err := New(opts)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return c
}

// mustAuthorized asserts the Result is an Authorized and returns it.
func mustAuthorized(t *testing.T, res Result) Authorized {
	t.Helper()
	a, ok := res.(Authorized)
	if !ok {
		t.Fatalf("Result = %T (%v), want Authorized", res, res)
	}
	return a
}

// decodeBody unmarshals a captured request body.
func decodeBody(t *testing.T, raw []byte) map[string]any {
	t.Helper()
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("request body is not JSON: %v", err)
	}
	return out
}

// waitFor spins until cond holds. It is not a sleep: the timeout is a failure detector,
// and the loop yields rather than blocking a P (docs/14-coding-standards.md §2).
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after 5s waiting for %s", what)
		}
		runtime.Gosched()
	}
}

// isUnavailable reports whether the Result is an Unavailable. Result is not an error —
// Authorized is not a failure — so tests assert with a type switch rather than errors.As.
func isUnavailable(res Result) bool {
	_, ok := res.(Unavailable)
	return ok
}
