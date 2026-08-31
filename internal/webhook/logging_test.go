package webhook

import (
	"bytes"
	"context"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"testing"
)

// syncBuffer is a bytes.Buffer safe for the logger to write to from any goroutine.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
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

// TestCall_LogsNoSecrets_NFR7 is the required test from docs/11-testing.md §5 and
// docs/14-coding-standards.md §9: drive a full call at debug, capture every log line, and
// assert it contains neither the cookie value nor the body.
//
// This process sees every connected user's session cookie. Its logs must not become a
// credential store, and a debug line added in a hurry during an incident is exactly how
// that happens. This test is cheap and it is the only thing that will catch that line.
func TestCall_LogsNoSecrets_NFR7(t *testing.T) {
	var out syncBuffer
	logger := slog.New(slog.NewJSONHandler(&out, &slog.HandlerOptions{Level: slog.LevelDebug}))

	responses := []func(w http.ResponseWriter, n int){
		okBody, // a 200 whose info block carries a name
		func(w http.ResponseWriter, _ int) {
			writeJSON(w, 200, `{"user":"u-7","channels":["_control"],"expires_in":60}`)
		},
		func(w http.ResponseWriter, _ int) { writeJSON(w, 200, `<html>login page for Ada</html>`) },
		func(w http.ResponseWriter, _ int) {
			writeJSON(w, 401, `{"detail":"session s3cr3t-session-value expired"}`)
		},
		func(w http.ResponseWriter, _ int) {
			writeJSON(w, 500, `{"traceback":"0123456789abcdef0123456789abcdef"}`)
		},
	}

	for _, respond := range responses {
		app := newStubApp(t, respond)
		cfg := testApp(app.server.URL)
		cfg.WebhookRetries = 0
		c := newTestClient(t, Options{App: cfg, Logger: logger})
		c.Call(t.Context(), testRequest())
	}

	logged := out.String()
	if logged == "" {
		t.Fatal("nothing was logged at debug; the NFR-7 assertion below would be vacuous")
	}

	banned := []struct {
		what  string
		value string
	}{
		{"the cookie value", "s3cr3t-session-value"},
		{"the whole Cookie header", testCookie},
		{"the webhook secret", testSecret},
		{"the response body's info block", "Ada"},
		{"a grant from the response body", "org-42-*"},
		{"the application's error body", "traceback"},
	}
	for _, b := range banned {
		if strings.Contains(logged, b.value) {
			t.Errorf("the log contains %s (NFR-7):\n%s", b.what, logged)
		}
	}

	// What it must contain, per docs/14-coding-standards.md §9: the client id, so an
	// operator can correlate, and enough to tell a refusal from a failure.
	for _, want := range []string{"8f2c1e04a7b3d915", "401", "500", "duration_ms"} {
		if !strings.Contains(logged, want) {
			t.Errorf("the log does not contain %q; it is not usable for an incident:\n%s", want, logged)
		}
	}
}

// TestCall_LogsNoSignature_NFR7: the signature is a MAC over the cookie digest. It is not
// the secret, but it is a credential for a 300-second replay window, so it stays out of
// the log with everything else (docs/04-integration.md §1.1).
func TestCall_LogsNoSignature_NFR7(t *testing.T) {
	var out syncBuffer
	logger := slog.New(slog.NewJSONHandler(&out, &slog.HandlerOptions{Level: slog.LevelDebug}))

	app := newStubApp(t, okBody)
	c := newTestClient(t, Options{App: testApp(app.server.URL), Logger: logger})
	mustAuthorized(t, c.Call(t.Context(), testRequest()))

	sig := app.request(0).header.Get("X-St-Signature")
	if sig == "" {
		t.Fatal("no signature was sent")
	}
	if strings.Contains(out.String(), sig) {
		t.Errorf("the log contains the signature (NFR-7):\n%s", out.String())
	}
	if strings.Contains(out.String(), app.request(0).header.Get("X-St-Nonce")) {
		t.Logf("the nonce is logged; harmless, but noted")
	}
}

// TestCall_LogsNoConnectURLCredentials_NFR7: `app.connect_url:
// "https://gw:hunter2@webapp.internal/_st/connect"` is an ordinary shape for an internal
// endpoint, and this package formats the configured URL into four of its own error
// strings. Those errors reach c.log.Warn("connect webhook unavailable", …, "err", err),
// so when the application restarts, every timed-out connect logs the password.
//
// The trap is that net/http strips userinfo from its own *url.Error — so the leak looks
// exactly like Go's output and is not. config.Validate now refuses userinfo outright;
// this asserts the second half, because a Client is constructible without it.
func TestCall_LogsNoConnectURLCredentials_NFR7(t *testing.T) {
	const password = "hunter2"

	withCredentials := func(raw string) string {
		parsed, err := url.Parse(raw)
		if err != nil {
			t.Fatalf("parsing the stub app's URL: %v", err)
		}
		parsed.User = url.UserPassword("gw", password)
		return parsed.String()
	}

	var out syncBuffer
	logger := slog.New(slog.NewJSONHandler(&out, &slog.HandlerOptions{Level: slog.LevelDebug}))

	// classify's default arm: an unlisted status formats the URL into the error.
	app := newStubApp(t, func(w http.ResponseWriter, _ int) { writeJSON(w, 500, `{}`) })
	cfg := testApp(withCredentials(app.server.URL))
	cfg.WebhookRetries = 0
	newTestClient(t, Options{App: cfg, Logger: logger}).Call(t.Context(), testRequest())

	// attempt's transport arm: the application is down, which is the case that produces
	// one of these lines per reconnecting connection.
	down := testApp(withCredentials("http://127.0.0.1:1/_st/connect"))
	down.WebhookRetries = 0
	newTestClient(t, Options{App: down, Logger: logger}).Call(t.Context(), testRequest())

	// attempts' cancelled-context arm: the browser went away mid-authorization.
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	newTestClient(t, Options{App: cfg, Logger: logger}).Call(ctx, testRequest())

	logged := out.String()
	if !strings.Contains(logged, "redacted@") {
		t.Fatalf("no redacted URL was logged, so the assertion below is vacuous:\n%s", logged)
	}
	if strings.Contains(logged, password) {
		t.Errorf("the log contains the app.connect_url password (NFR-7):\n%s", logged)
	}
	// The inner *url.Error from net/http still reads `Post "http://gw:***@…"`. That is
	// Go's own redaction and it is the shape this leak was hiding behind: the username
	// survives, the password does not. What must never appear is the pair.
	if strings.Contains(logged, "gw:"+password) {
		t.Errorf("the log contains the app.connect_url userinfo verbatim (NFR-7):\n%s", logged)
	}
}
