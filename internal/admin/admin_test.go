package admin

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/raghulj/sidecartunnel/internal/bus"
)

// TestNew_RequiredDependencies refuses a half-wired listener at construction rather than
// at the first request. A nil dependency here is a wiring bug, and NFR-5's rule that the
// process must not start in a partially-configured state applies to the object graph as
// much as to the config file.
func TestNew_RequiredDependencies(t *testing.T) {
	full := func() Options {
		return Options{Bus: &fakeBus{}, Registry: &fakeRegistry{}, Gatherer: prometheus.NewRegistry()}
	}
	tests := []struct {
		name string
		opts func() Options
		want string
	}{
		{"no bus", func() Options { o := full(); o.Bus = nil; return o }, "bus"},
		{"no registry", func() Options { o := full(); o.Registry = nil; return o }, "registry"},
		{"no gatherer", func() Options { o := full(); o.Gatherer = nil; return o }, "gatherer"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := New(tt.opts())
			if err == nil {
				t.Fatal("New: want an error, got nil")
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Errorf("error %q does not name %q", err, tt.want)
			}
		})
	}
}

// TestNew_Defaults applies the documented defaults: bus.ready_grace 30s and a
// ReadHeaderTimeout on the server, which is what stops a slowloris from holding an admin
// worker open (docs/08-config.md §3).
func TestNew_Defaults(t *testing.T) {
	h := newHarness(t, func(o *Options) {
		o.ReadyGrace = 0
		o.ReadHeaderTimeout = 0
	})
	if h.readyGrace != defaultReadyGrace {
		t.Errorf("ready grace = %v, want %v", h.readyGrace, defaultReadyGrace)
	}
	if h.srv.ReadHeaderTimeout != defaultReadHeaderTimeout {
		t.Errorf("ReadHeaderTimeout = %v, want %v", h.srv.ReadHeaderTimeout, defaultReadHeaderTimeout)
	}
	if h.srv.Handler == nil {
		t.Error("the http.Server has no handler")
	}
}

// TestNew_ExplicitTimeouts honours the values it is given.
func TestNew_ExplicitTimeouts(t *testing.T) {
	h := newHarness(t, func(o *Options) {
		o.ReadyGrace = 5 * time.Second
		o.ReadHeaderTimeout = time.Second
	})
	if h.readyGrace != 5*time.Second || h.srv.ReadHeaderTimeout != time.Second {
		t.Errorf("grace = %v, ReadHeaderTimeout = %v", h.readyGrace, h.srv.ReadHeaderTimeout)
	}
}

// TestNew_NilLoggerIsUsable falls back to slog.Default rather than panicking on the first
// unauthorized request.
func TestNew_NilLoggerIsUsable(t *testing.T) {
	h := newHarness(t, func(o *Options) { o.Logger = nil })
	if w := h.do(t, http.MethodGet, "/health", ""); w.Code != http.StatusOK {
		t.Fatalf("/health = %d, want 200", w.Code)
	}
	if w := h.do(t, http.MethodGet, "/channels", ""); w.Code != http.StatusUnauthorized {
		t.Fatalf("/channels unauthenticated = %d, want 401", w.Code)
	}
}

// TestHealth_NeverConsultsTheBus is FR-20 and docs/13-review-findings.md M20.
//
// A Redis restart makes every replica unready at once. Wired to a liveness probe that
// kills the entire fleet, drops every connection, and turns an eight-second blip into a
// full application outage as everyone re-authorizes together. /health exists so there is
// something correct to point a liveness probe at, and it is only correct while it stays
// 200 with the bus on the floor.
func TestHealth_NeverConsultsTheBus_FR20(t *testing.T) {
	h := newHarness(t, nil)
	h.bus.set(bus.Health{Connected: false, DisconnectedFor: time.Hour})

	for range 3 {
		w := h.do(t, http.MethodGet, "/health", "")
		if w.Code != http.StatusOK {
			t.Fatalf("/health with the bus down = %d, want 200", w.Code)
		}
	}
	if got := h.bus.observed(); got != 0 {
		t.Errorf("/health consulted the bus %d times; it must never consult it", got)
	}

	// And /ready, on the same server and the same snapshot, does report it.
	if w := h.do(t, http.MethodGet, "/ready", ""); w.Code != http.StatusServiceUnavailable {
		t.Errorf("/ready with the bus down for an hour = %d, want 503", w.Code)
	}
}

// TestHealth_Body is a small, stable document: a liveness probe reads the status code,
// and an operator reads the body.
func TestHealth_Body(t *testing.T) {
	h := newHarness(t, nil)
	w := h.do(t, http.MethodGet, "/health", "")
	if got := w.Header().Get("Content-Type"); got != "application/json" {
		t.Errorf("Content-Type = %q", got)
	}
	var body struct {
		Status string `json:"status"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body.Status != "ok" {
		t.Errorf("status = %q, want ok", body.Status)
	}
}

// TestReady_GraceBoundary walks bus.ready_grace in both directions.
//
// The grace is the whole point: a Redis restart makes every replica unready at once, and
// without it a short blip pulls the entire fleet out of the load balancer together
// (docs/04-integration.md §4). The boundary is inclusive — "503 once the bus has been
// down *longer than* ready_grace".
func TestReady_GraceBoundary(t *testing.T) {
	h := newHarness(t, func(o *Options) { o.ReadyGrace = 30 * time.Second })

	tests := []struct {
		name   string
		health bus.Health
		want   int
	}{
		{"connected", bus.Health{Connected: true}, http.StatusOK},
		{"down inside the grace", bus.Health{DisconnectedFor: seconds(8)}, http.StatusOK},
		{"down exactly at the grace", bus.Health{DisconnectedFor: seconds(30)}, http.StatusOK},
		{"down past the grace", bus.Health{DisconnectedFor: seconds(31)}, http.StatusServiceUnavailable},
		{"down far past the grace", bus.Health{DisconnectedFor: time.Hour}, http.StatusServiceUnavailable},
		// Back up again: readiness must recover without a restart (NFR-8).
		{"reconnected", bus.Health{Connected: true, Reconnects: 1}, http.StatusOK},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h.bus.set(tt.health)
			w := h.do(t, http.MethodGet, "/ready", "")
			if w.Code != tt.want {
				t.Errorf("/ready = %d, want %d", w.Code, tt.want)
			}
			var body struct {
				Ready        bool    `json:"ready"`
				BusConnected bool    `json:"bus_connected"`
				DownFor      float64 `json:"bus_down_for_seconds"`
			}
			if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
				t.Fatalf("decode: %v", err)
			}
			if body.Ready != (tt.want == http.StatusOK) {
				t.Errorf("body ready = %v, status = %d", body.Ready, w.Code)
			}
			if body.BusConnected != tt.health.Connected {
				t.Errorf("bus_connected = %v, want %v", body.BusConnected, tt.health.Connected)
			}
			if want := tt.health.DisconnectedFor.Seconds(); body.DownFor != want {
				t.Errorf("bus_down_for_seconds = %v, want %v", body.DownFor, want)
			}
		})
	}
}

// TestReady_NeedsNoToken: readiness is consumed by a load balancer that has no token, so
// it stays unauthenticated even when admin.token is set (docs/04-integration.md §4).
func TestReady_NeedsNoToken(t *testing.T) {
	h := newHarness(t, nil)
	for _, path := range []string{"/health", "/ready", "/metrics"} {
		if w := h.do(t, http.MethodGet, path, ""); w.Code != http.StatusOK {
			t.Errorf("%s unauthenticated = %d, want 200", path, w.Code)
		}
	}
}

// TestMetrics_Exposition serves the registry it was handed, and refreshes first so the
// gauges a snapshot owns are exact at scrape time rather than as of the last tick.
func TestMetrics_Exposition(t *testing.T) {
	h := newHarness(t, nil)
	w := h.do(t, http.MethodGet, "/metrics", "")
	if w.Code != http.StatusOK {
		t.Fatalf("/metrics = %d, want 200", w.Code)
	}
	if !strings.Contains(w.Body.String(), "st_origin_rejected_total") {
		t.Errorf("/metrics body does not carry the registered metric:\n%s", w.Body.String())
	}
	if *h.refresh != 1 {
		t.Errorf("Refresh called %d times, want 1", *h.refresh)
	}
}

// TestMetrics_NoRefreshHook works without one; the hook is optional.
func TestMetrics_NoRefreshHook(t *testing.T) {
	h := newHarness(t, func(o *Options) { o.Refresh = nil })
	if w := h.do(t, http.MethodGet, "/metrics", ""); w.Code != http.StatusOK {
		t.Fatalf("/metrics = %d, want 200", w.Code)
	}
}

// TestChannels_ListAndFilter covers GET /channels and its prefix filter. The list is this
// replica's view only (docs/04-integration.md §4), and it is sorted here so that an
// operator diffing two scrapes sees a stable document rather than Go's map order.
func TestChannels_ListAndFilter(t *testing.T) {
	h := newHarness(t, nil)

	tests := []struct {
		name   string
		target string
		want   []string
	}{
		{"no filter lists everything, sorted", "/channels", []string{"room-1", "room-4410", "user-7"}},
		{"prefix filter", "/channels?prefix=room-", []string{"room-1", "room-4410"}},
		{"prefix is a byte prefix, not a glob", "/channels?prefix=room-4", []string{"room-4410"}},
		{"empty prefix is no filter", "/channels?prefix=", []string{"room-1", "room-4410", "user-7"}},
		{"prefix matching nothing", "/channels?prefix=nope", nil},
		{"a glob is not expanded", "/channels?prefix=room-*", nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := h.authed(t, http.MethodGet, tt.target, "")
			if w.Code != http.StatusOK {
				t.Fatalf("%s = %d, want 200", tt.target, w.Code)
			}
			var body struct {
				Channels []Channel `json:"channels"`
			}
			if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
				t.Fatalf("decode: %v", err)
			}
			got := make([]string, 0, len(body.Channels))
			for _, c := range body.Channels {
				got = append(got, c.Name)
			}
			if strings.Join(got, ",") != strings.Join(tt.want, ",") {
				t.Errorf("channels = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestChannels_ListOmitsUsers: the list is a count per channel. User ids belong to the
// single-channel view, which is the one an operator asks for deliberately.
func TestChannels_ListOmitsUsers(t *testing.T) {
	h := newHarness(t, nil)
	w := h.authed(t, http.MethodGet, "/channels", "")
	if strings.Contains(w.Body.String(), "u-7") {
		t.Errorf("the channel list carries user ids:\n%s", w.Body.String())
	}
	if !strings.Contains(w.Body.String(), `"subscribers":2`) {
		t.Errorf("the channel list carries no subscriber count:\n%s", w.Body.String())
	}
}

// TestChannel_One covers GET /channels/{channel}: subscriber count and user ids for one
// channel, and a 404 for a channel this replica does not hold.
func TestChannel_One(t *testing.T) {
	h := newHarness(t, nil)

	w := h.authed(t, http.MethodGet, "/channels/room-4410", "")
	if w.Code != http.StatusOK {
		t.Fatalf("/channels/room-4410 = %d, want 200", w.Code)
	}
	var got Channel
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.Name != "room-4410" || got.Subscribers != 2 || strings.Join(got.Users, ",") != "u-7,u-9" {
		t.Errorf("channel = %+v", got)
	}

	tests := []struct{ name, target string }{
		{"unheld channel", "/channels/room-9999"},
		{"empty channel", "/channels/"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if w := h.authed(t, http.MethodGet, tt.target, ""); w.Code != http.StatusNotFound {
				t.Errorf("%s = %d, want 404", tt.target, w.Code)
			}
		})
	}
}

// TestChannel_NameWithSlash: channel names are opaque printable ASCII and may contain a
// slash, so the route captures the whole remainder rather than one path segment
// (docs/06-channels.md §1).
func TestChannel_NameWithSlash(t *testing.T) {
	h := newHarness(t, nil)
	h.registry.channels = append(h.registry.channels, Channel{Name: "org-42/alerts", Subscribers: 1, Users: []string{"u-1"}})

	w := h.authed(t, http.MethodGet, "/channels/org-42/alerts", "")
	if w.Code != http.StatusOK {
		t.Fatalf("/channels/org-42/alerts = %d, want 200:\n%s", w.Code, w.Body.String())
	}
}

// TestDisconnect covers POST /disconnect in every shape docs/04-integration.md §4 allows
// and the two it does not.
func TestDisconnect(t *testing.T) {
	tests := []struct {
		name       string
		body       string
		wantStatus int
		wantTarget *Target
	}{
		{"by user", `{"user":"u-7"}`, http.StatusOK, &Target{User: "u-7"}},
		{"by client", `{"client":"8f2c1e04a7b3d915"}`, http.StatusOK, &Target{Client: "8f2c1e04a7b3d915"}},
		// C8: an omitted target is a validation error, not "everyone". Treating it as
		// everyone means one request forces every connection on the replica to
		// re-authorize at once — the application outage docs/10-operations.md §4 models,
		// available on demand.
		{"neither target", `{}`, http.StatusBadRequest, nil},
		{"empty strings are no target", `{"user":"","client":""}`, http.StatusBadRequest, nil},
		{"both targets", `{"user":"u-7","client":"8f2c"}`, http.StatusBadRequest, nil},
		{"malformed json", `{"user":`, http.StatusBadRequest, nil},
		{"not an object", `["u-7"]`, http.StatusBadRequest, nil},
		{"empty body", ``, http.StatusBadRequest, nil},
		{"unknown field", `{"users":"u-7"}`, http.StatusBadRequest, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newHarness(t, nil)
			w := h.authed(t, http.MethodPost, "/disconnect", tt.body)
			if w.Code != tt.wantStatus {
				t.Fatalf("POST /disconnect %s = %d, want %d (%s)", tt.body, w.Code, tt.wantStatus, w.Body.String())
			}
			seen := h.registry.seen()
			if tt.wantTarget == nil {
				if len(seen) != 0 {
					t.Fatalf("a rejected request reached the registry: %+v", seen)
				}
				return
			}
			if len(seen) != 1 || seen[0] != *tt.wantTarget {
				t.Fatalf("registry saw %+v, want %+v", seen, *tt.wantTarget)
			}
			var body struct {
				Disconnected int `json:"disconnected"`
			}
			if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
				t.Fatalf("decode: %v", err)
			}
			if body.Disconnected != 2 {
				t.Errorf("disconnected = %d, want 2", body.Disconnected)
			}
		})
	}
}

// TestDisconnect_RegistryError reports a failure rather than claiming success, and does
// not put the underlying error in the response.
func TestDisconnect_RegistryError(t *testing.T) {
	h := newHarness(t, nil)
	h.registry.disconnErr = errors.New("hub: control message names no target")

	w := h.authed(t, http.MethodPost, "/disconnect", `{"user":"u-7"}`)
	if w.Code != http.StatusInternalServerError {
		t.Fatalf("POST /disconnect = %d, want 500", w.Code)
	}
	if !strings.Contains(h.logs.String(), "admin.disconnect") {
		t.Errorf("the failure was not logged:\n%s", h.logs.String())
	}
}

// TestDisconnect_BodyLimit refuses a body big enough to be an attack rather than reading
// it into memory.
func TestDisconnect_BodyLimit(t *testing.T) {
	h := newHarness(t, nil)
	body := `{"user":"` + strings.Repeat("u", maxBodyBytes+1) + `"}`
	if w := h.authed(t, http.MethodPost, "/disconnect", body); w.Code != http.StatusBadRequest {
		t.Errorf("oversize body = %d, want 400", w.Code)
	}
}

// TestMethodNotAllowed: the routes are method-specific, so a POST to a GET route is a 405
// rather than a silent success.
func TestMethodNotAllowed(t *testing.T) {
	h := newHarness(t, nil)
	tests := []struct{ method, target string }{
		{http.MethodPost, "/health"},
		{http.MethodPost, "/ready"},
		{http.MethodGet, "/disconnect"},
	}
	for _, tt := range tests {
		t.Run(tt.method+" "+tt.target, func(t *testing.T) {
			w := h.do(t, tt.method, tt.target, "", "Authorization", "Bearer "+testToken)
			if w.Code != http.StatusMethodNotAllowed {
				t.Errorf("%s %s = %d, want 405", tt.method, tt.target, w.Code)
			}
		})
	}
}

// TestUnknownPath is a 404, and in particular it is not a redirect to something else.
func TestUnknownPath(t *testing.T) {
	h := newHarness(t, nil)
	if w := h.authed(t, http.MethodGet, "/", ""); w.Code != http.StatusNotFound {
		t.Errorf("GET / = %d, want 404", w.Code)
	}
}
