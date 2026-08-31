package admin

import (
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/raghulj/sidecartunnel/internal/bus"
)

// testToken is the bearer token every authenticated test uses.
const testToken = "s3cret-admin-token"

// fakeBus is a BusHealth whose snapshot the test sets. It stands in for a clock: /ready
// compares bus.Health.DisconnectedFor against bus.ready_grace, so a test moves the
// boundary by setting a duration rather than by sleeping
// (docs/14-coding-standards.md §2).
type fakeBus struct {
	mu     sync.Mutex
	health bus.Health
	calls  int
}

// Health returns the snapshot the test set and counts the call, so a test can assert that
// /health never made one.
func (f *fakeBus) Health() bus.Health {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	return f.health
}

// set replaces the snapshot.
func (f *fakeBus) set(h bus.Health) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.health = h
}

// observed reports how many times Health has been called.
func (f *fakeBus) observed() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

// fakeRegistry is a Registry over a fixed set of channels.
type fakeRegistry struct {
	mu         sync.Mutex
	channels   []Channel
	targets    []Target
	closed     int
	disconnErr error
}

// Channels returns this replica's occupied channels, deliberately out of order so the
// admin listener's own sort is exercised.
func (f *fakeRegistry) Channels() []Channel {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]Channel, len(f.channels))
	copy(out, f.channels)
	return out
}

// Channel returns one channel's subscribers and user ids.
func (f *fakeRegistry) Channel(name string) (Channel, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, c := range f.channels {
		if c.Name == name {
			return c, true
		}
	}
	return Channel{}, false
}

// Disconnect records the target and returns the configured outcome.
func (f *fakeRegistry) Disconnect(t Target) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.targets = append(f.targets, t)
	if f.disconnErr != nil {
		return 0, f.disconnErr
	}
	return f.closed, nil
}

// seen returns the targets Disconnect was called with.
func (f *fakeRegistry) seen() []Target {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]Target, len(f.targets))
	copy(out, f.targets)
	return out
}

// harness is a Server under test with the doubles it was built from.
type harness struct {
	*Server
	bus      *fakeBus
	registry *fakeRegistry
	reg      *prometheus.Registry
	logs     *strings.Builder
	refresh  *int
}

// newHarness builds a Server with working doubles, applying mutate to the options first.
func newHarness(t *testing.T, mutate func(*Options)) *harness {
	t.Helper()

	fb := &fakeBus{health: bus.Health{Connected: true}}
	fr := &fakeRegistry{
		channels: []Channel{
			{Name: "user-7", Subscribers: 1, Users: []string{"u-7"}},
			{Name: "room-4410", Subscribers: 2, Users: []string{"u-7", "u-9"}},
			{Name: "room-1", Subscribers: 1, Users: []string{"u-1"}},
		},
		closed: 2,
	}
	reg := prometheus.NewRegistry()
	reg.MustRegister(prometheus.NewCounter(prometheus.CounterOpts{
		Name: "st_origin_rejected_total",
		Help: "Handshakes refused because the Origin was not in server.allowed_origins.",
	}))

	var logs strings.Builder
	refreshed := 0
	opts := Options{
		Token:    testToken,
		Bus:      fb,
		Registry: fr,
		Gatherer: reg,
		Refresh:  func() { refreshed++ },
		Logger:   slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})),
	}
	if mutate != nil {
		mutate(&opts)
	}
	s, err := New(opts)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return &harness{Server: s, bus: fb, registry: fr, reg: reg, logs: &logs, refresh: &refreshed}
}

// do runs one request through the handler and returns the recorder.
func (h *harness) do(t *testing.T, method, target string, body string, headers ...string) *httptest.ResponseRecorder {
	t.Helper()
	var r *http.Request
	if body == "" {
		r = httptest.NewRequest(method, target, nil)
	} else {
		r = httptest.NewRequest(method, target, strings.NewReader(body))
	}
	for i := 0; i+1 < len(headers); i += 2 {
		r.Header.Set(headers[i], headers[i+1])
	}
	w := httptest.NewRecorder()
	h.Handler().ServeHTTP(w, r)
	return w
}

// authed runs one authenticated request through the handler.
func (h *harness) authed(t *testing.T, method, target, body string) *httptest.ResponseRecorder {
	t.Helper()
	return h.do(t, method, target, body, "Authorization", "Bearer "+testToken)
}

// errWriter is an http.ResponseWriter whose body write fails, which is what a client that
// hangs up mid-response looks like from inside a handler.
type errWriter struct {
	header http.Header
	status int
}

// Header returns the response header map.
func (e *errWriter) Header() http.Header {
	if e.header == nil {
		e.header = make(http.Header)
	}
	return e.header
}

// Write always fails.
func (e *errWriter) Write([]byte) (int, error) { return 0, errors.New("connection reset by peer") }

// WriteHeader records the status.
func (e *errWriter) WriteHeader(status int) { e.status = status }

// seconds is a small readability helper for the grace-boundary table.
func seconds(n int) time.Duration { return time.Duration(n) * time.Second }
