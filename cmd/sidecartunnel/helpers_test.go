package main

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/raghulj/sidecartunnel/internal/bus"
	"github.com/raghulj/sidecartunnel/internal/config"
	"github.com/raghulj/sidecartunnel/internal/proto"
)

// waitFor is the generous failure detector docs/14-coding-standards.md §2 allows in place
// of a sleep: the happy path takes microseconds and the timeout only fires when the test
// was going to fail anyway.
const waitFor = 5 * time.Second

// Fixture secrets. Both are over the 32-byte floor docs/08-config.md §3 sets, and neither
// may appear in any log line the tests capture (NFR-7).
const (
	testWebhookSecret = "webhook-secret-that-is-long-enough-for-the-32-byte-floor"
	testControlSecret = "control-secret-that-is-long-enough-for-the-32-byte-floor"
)

// slogDebug is slog.LevelDebug as a level value, so a capturing logger records
// everything the NFR-7 assertions need to inspect.
const slogDebug = slog.LevelDebug

// discardLogger is a logger for the paths whose output no test asserts on.
func discardLogger() *slog.Logger {
	return slog.New(slog.NewJSONHandler(io.Discard, nil))
}

// capturingLogger returns a logger and the buffer it writes to, for the NFR-7 assertions
// that no secret reaches a log line.
func capturingLogger(t *testing.T, level slog.Level) (*slog.Logger, *syncBuffer) {
	t.Helper()
	buf := &syncBuffer{}
	return slog.New(slog.NewJSONHandler(buf, &slog.HandlerOptions{Level: level})), buf
}

// syncBuffer is a bytes.Buffer safe to read while a goroutine is logging into it.
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

// grant is one stub-webhook answer.
type grant struct {
	user      string
	channels  []string
	expiresIn int64
	status    int
}

// stubWebhook is the consuming application's connect endpoint (docs/04-integration.md §1).
//
// It verifies the signature exactly as the document specifies — HMAC-SHA256 over
// timestamp + "." + nonce + "." + sha256(Cookie) + "." + sha256(body) — because a stub
// that accepted anything would let a signing regression through, and FR-3's whole
// contract is that the application can prove the request came from the gateway.
type stubWebhook struct {
	*httptest.Server

	mu      sync.Mutex
	grant   grant
	calls   int
	cookie  string
	gate    chan struct{}
	entered chan struct{}
}

func newStubWebhook(t *testing.T, g grant) *stubWebhook {
	t.Helper()
	s := &stubWebhook{grant: g, entered: make(chan struct{}, 8)}
	s.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			// coverage: test helper.
			t.Errorf("stub webhook: read body: %v", err)
			return
		}
		ts := r.Header.Get("X-St-Timestamp")
		nonce := r.Header.Get("X-St-Nonce")
		cookie := r.Header.Get("Cookie")

		cookieDigest := sha256.Sum256([]byte(cookie))
		bodyDigest := sha256.Sum256(body)
		mac := hmac.New(sha256.New, []byte(testWebhookSecret))
		_, _ = mac.Write([]byte(ts + "." + nonce + "." +
			hex.EncodeToString(cookieDigest[:]) + "." + hex.EncodeToString(bodyDigest[:])))
		want := hex.EncodeToString(mac.Sum(nil))
		if got := r.Header.Get("X-St-Signature"); got != want {
			t.Errorf("stub webhook: signature %q, want %q (FR-3)", got, want)
			w.WriteHeader(http.StatusForbidden)
			return
		}

		s.mu.Lock()
		s.calls++
		s.cookie = cookie
		answer := s.grant
		gate := s.gate
		s.mu.Unlock()

		// A blocked call is how a test holds a connection open past a drain budget: the
		// handler goroutine is tracked before authorization and cannot finish until the
		// application answers.
		if gate != nil {
			select {
			case s.entered <- struct{}{}:
			default:
			}
			<-gate
		}

		if answer.status != 0 && answer.status != http.StatusOK {
			w.WriteHeader(answer.status)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"user":       answer.user,
			"channels":   answer.channels,
			"expires_in": answer.expiresIn,
		})
	}))
	t.Cleanup(s.Close)
	return s
}

// block makes every subsequent call park until the returned function is called. The
// release is idempotent, so a test may both defer it and call it explicitly.
func (s *stubWebhook) block() func() {
	gate := make(chan struct{})
	s.mu.Lock()
	s.gate = gate
	s.mu.Unlock()

	var once sync.Once
	return func() { once.Do(func() { close(gate) }) }
}

func (s *stubWebhook) callCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

// testEnv is the minimal environment-only deployment of docs/08-config.md §4, plus the
// overrides a test needs. Both listeners take port 0, which validateListeners
// deliberately permits: two ephemeral ports are never the same socket.
func testEnv(connectURL string) map[string]string {
	return map[string]string{
		"ST_SERVER__ALLOWED_ORIGINS": "https://app.example.com",
		"ST_SERVER__LISTEN":          "127.0.0.1:0",
		"ST_APP__CONNECT_URL":        connectURL,
		"ST_APP__WEBHOOK_SECRETS":    testWebhookSecret,
		"ST_CONTROL__SECRET":         testControlSecret,
		"ST_BUS__KIND":               "memory",
		"ST_LOG__LEVEL":              "debug",
	}
}

// loadConfig applies env to the process environment and loads a validated configuration
// from it. t.Setenv restores every key when the test ends, which is why no test here runs
// in parallel.
func loadConfig(t *testing.T, env map[string]string) *config.Config {
	t.Helper()
	for k, v := range env {
		t.Setenv(k, v)
	}
	cfg, err := config.Load("")
	if err != nil {
		t.Fatalf("config.Load: %v", err)
	}
	return cfg
}

// signControl builds the signed control envelope of docs/04-integration.md §3: the action
// as an opaque JSON string, and a MAC over the exact bytes carried.
func signControl(secret string, ts time.Time, body string) []byte {
	stamp := strconv.FormatInt(ts.Unix(), 10)
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write([]byte(stamp + "." + "nonce-1" + "." + body))
	payload, _ := json.Marshal(controlEnvelope{
		TS:    ts.Unix(),
		Nonce: "nonce-1",
		Body:  body,
		Sig:   hex.EncodeToString(mac.Sum(nil)),
	})
	return payload
}

// fakeSignal is an os.Signal a test can send without touching the process.
type fakeSignal string

func (s fakeSignal) String() string { return string(s) }
func (s fakeSignal) Signal()        {}

// sigterm is the signal the drain tests send. It is not the real SIGTERM, deliberately:
// delivering a real one to the test binary would end the test run.
const sigterm = fakeSignal("terminated")

// httpGet performs one GET and returns the status and body, failing the test on a
// transport error.
func httpGet(t *testing.T, url, token string) (int, string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), waitFor)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		t.Fatalf("new request %s: %v", url, err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read %s: %v", url, err)
	}
	return resp.StatusCode, string(body)
}

// waitSubscribed blocks until the bus holds at least n subscriptions.
//
// The hub reconciles asynchronously, on purpose: a subscribe marks a dirty flag and
// returns, and the reconciler calls Bus.Sync off the request path (docs/09-internals.md
// §4.1). A publish issued before that Sync lands reaches nobody, which is correct
// behaviour and a race in a test that publishes immediately. This is the synchronisation
// point, not a delay: it spins on the bus's own health and only the timeout is a wait.
func waitSubscribed(t *testing.T, b interface{ Health() bus.Health }, n int) {
	t.Helper()
	deadline := time.Now().Add(waitFor)
	for time.Now().Before(deadline) {
		if b.Health().Subscriptions >= n {
			return
		}
		runtime.Gosched()
	}
	t.Fatalf("the bus reached %d subscriptions within %s, want %d",
		b.Health().Subscriptions, waitFor, n)
}

// signals returns a channel a test can push fake signals onto.
func signalChan(n int) chan os.Signal { return make(chan os.Signal, n) }

// testSink is a hub.Sink for the consumer tests. The hub keys maps by Sink, so it must be
// a pointer: an interface holding an uncomparable value panics the moment it is inserted.
type testSink struct {
	id   string
	user string

	mu     sync.Mutex
	frames []*proto.Frame
	closes []proto.CloseCode

	delivered chan *proto.Frame
	closed    chan proto.CloseCode
}

func newTestSink(id, user string) *testSink {
	return &testSink{
		id:        id,
		user:      user,
		delivered: make(chan *proto.Frame, 64),
		closed:    make(chan proto.CloseCode, 8),
	}
}

func (s *testSink) ID() string   { return s.id }
func (s *testSink) User() string { return s.user }

func (s *testSink) Send(f *proto.Frame) bool {
	s.mu.Lock()
	s.frames = append(s.frames, f)
	s.mu.Unlock()
	select {
	case s.delivered <- f:
	default:
	}
	return true
}

func (s *testSink) Close(code proto.CloseCode, _ string) {
	s.mu.Lock()
	s.closes = append(s.closes, code)
	s.mu.Unlock()
	select {
	case s.closed <- code:
	default:
	}
}

// closeCount reports how many times Close has been called.
func (s *testSink) closeCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.closes)
}

// waitFrame blocks until the sink accepts a frame, failing the test rather than hanging.
func (s *testSink) waitFrame(t *testing.T) *proto.Frame {
	t.Helper()
	select {
	case f := <-s.delivered:
		return f
	case <-time.After(waitFor):
		t.Fatalf("sink %s: no frame within %s", s.id, waitFor)
		return nil
	}
}

// waitClose blocks until the sink is closed, failing the test rather than hanging.
func (s *testSink) waitClose(t *testing.T) proto.CloseCode {
	t.Helper()
	select {
	case c := <-s.closed:
		return c
	case <-time.After(waitFor):
		t.Fatalf("sink %s: no close within %s", s.id, waitFor)
		return 0
	}
}
