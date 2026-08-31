package server

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/raghulj/sidecartunnel/internal/bus"
	"github.com/raghulj/sidecartunnel/internal/config"
)

// probeBus is a BusHealth whose snapshot the test sets.
//
// It stands in for a clock: /ready compares bus.Health.DisconnectedFor against
// bus.ready_grace, so a test moves the boundary by setting a duration rather than by
// sleeping through one (docs/14-coding-standards.md §2). It also counts calls, so a test
// can assert that /health made none.
type probeBus struct {
	health bus.Health
	calls  int
}

func (p *probeBus) Health() bus.Health {
	p.calls++
	return p.health
}

// newTestListener puts the rig's handler behind an httptest server. newRigWithOptions
// deliberately builds no listener, because most of its callers drive Serve themselves;
// these tests want an address to curl.
func newTestListener(t *testing.T, r *rig) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(r.srv.Handler())
	t.Cleanup(srv.Close)
	return srv
}

// probe issues one GET against the rig's listener and returns the status and body.
func probe(t *testing.T, r *rig, path string) (int, string) {
	t.Helper()
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, r.http.URL+path, nil)
	if err != nil {
		t.Fatalf("request %s: %v", path, err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET %s: %v", path, err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return resp.StatusCode, string(body)
}

// TestHealth_IsLivenessAndNeverTouchesTheBus_FR20 is the rule the endpoint exists to keep.
//
// A Redis restart makes every replica unready at once. A /health that consulted the bus,
// wired to a liveness probe, would kill every replica simultaneously and turn an
// eight-second blip into a full application outage (docs/13-review-findings.md M20). The
// assertion is the absence of a call, which is why the double counts them.
func TestHealth_IsLivenessAndNeverTouchesTheBus_FR20(t *testing.T) {
	t.Parallel()
	fb := &probeBus{health: bus.Health{DisconnectedFor: time.Hour}}
	r := newRigWithOptions(t, func(o *Options) { o.Bus = fb })
	r.http = newTestListener(t, r)

	code, body := probe(t, r, "/health")
	if code != http.StatusOK {
		t.Fatalf("GET /health = %d %s, want 200 with the bus down for an hour", code, body)
	}
	if fb.calls != 0 {
		t.Fatalf("/health consulted the bus %d times; it must never consult it (FR-20)", fb.calls)
	}
}

// TestHealth_StaysUpThroughTheDrain: a draining replica is alive and finishing its work.
// Liveness must not fail, or the container runtime restarts a process that is shutting
// down correctly. Readiness is the signal that changes, and TestReady_Draining covers it.
func TestHealth_StaysUpThroughTheDrain(t *testing.T) {
	t.Parallel()
	r := newRig(t)
	r.srv.StopAccepting()

	if code, body := probe(t, r, "/health"); code != http.StatusOK {
		t.Fatalf("GET /health = %d %s while draining, want 200: liveness is not readiness", code, body)
	}
}

// TestReady_GraceBoundary walks bus.ready_grace in both directions.
//
// The grace is the whole point: a Redis restart makes every replica unready at once, and
// without it a short blip pulls the entire fleet out of the load balancer together
// (docs/04-integration.md §4). The boundary is inclusive — "503 once the bus has been
// down *longer than* ready_grace".
func TestReady_GraceBoundary(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		health bus.Health
		want   int
	}{
		{"connected", bus.Health{Connected: true}, http.StatusOK},
		{"down inside the grace", bus.Health{DisconnectedFor: 8 * time.Second}, http.StatusOK},
		{"down exactly at the grace", bus.Health{DisconnectedFor: 30 * time.Second}, http.StatusOK},
		{"down past the grace", bus.Health{DisconnectedFor: 31 * time.Second}, http.StatusServiceUnavailable},
		{"down far past the grace", bus.Health{DisconnectedFor: time.Hour}, http.StatusServiceUnavailable},
		// Back up again: readiness must recover without a restart (NFR-8).
		{"reconnected", bus.Health{Connected: true, Reconnects: 1}, http.StatusOK},
		// Connected and reconnecting repeatedly. This is the M8 shape — output-buffer
		// eviction against a healthy Redis — and it is ready, which is why the count has
		// to be in the body: the status code cannot say it (docs/10-operations.md §7).
		{"connected but oscillating", bus.Health{Connected: true, Reconnects: 41}, http.StatusOK},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			fb := &probeBus{health: tt.health}
			r := newRigWithOptions(t, func(o *Options) { o.Bus = fb },
				func(c *config.Config) { c.Bus.ReadyGrace = config.Duration(30 * time.Second) })
			r.http = newTestListener(t, r)

			code, raw := probe(t, r, "/ready")
			if code != tt.want {
				t.Errorf("GET /ready = %d, want %d", code, tt.want)
			}

			var body struct {
				Ready        bool    `json:"ready"`
				BusConnected bool    `json:"bus_connected"`
				DownFor      float64 `json:"bus_down_for_seconds"`
				Reconnects   uint64  `json:"bus_reconnects"`
				Draining     bool    `json:"draining"`
			}
			if err := json.Unmarshal([]byte(raw), &body); err != nil {
				t.Fatalf("decode %q: %v", raw, err)
			}
			if body.Ready != (tt.want == http.StatusOK) {
				t.Errorf("body ready = %v, status = %d", body.Ready, code)
			}
			if body.BusConnected != tt.health.Connected {
				t.Errorf("bus_connected = %v, want %v", body.BusConnected, tt.health.Connected)
			}
			if want := tt.health.DisconnectedFor.Seconds(); body.DownFor != want {
				t.Errorf("bus_down_for_seconds = %v, want %v", body.DownFor, want)
			}
			if body.Reconnects != tt.health.Reconnects {
				t.Errorf("bus_reconnects = %d, want %d", body.Reconnects, tt.health.Reconnects)
			}
			if body.Draining {
				t.Error("draining = true on a replica that is not draining")
			}
		})
	}
}

// TestReady_DefaultsTheGrace: bus.ready_grace left at zero is the documented 30s, not zero
// seconds of tolerance. A grace of zero would pull every replica out of the load balancer
// on the first blip, which is exactly what the key exists to prevent
// (docs/08-config.md §3).
func TestReady_DefaultsTheGrace(t *testing.T) {
	t.Parallel()
	fb := &probeBus{health: bus.Health{DisconnectedFor: 29 * time.Second}}
	r := newRigWithOptions(t, func(o *Options) { o.Bus = fb })
	r.http = newTestListener(t, r)

	if code, body := probe(t, r, "/ready"); code != http.StatusOK {
		t.Fatalf("GET /ready = %d %s at 29s down, want 200 under the default 30s grace", code, body)
	}
}

// TestReady_Draining is step 1 of docs/09-internals.md §8: the replica reports itself out
// of rotation the moment the drain begins, and no grace applies.
//
// The grace exists to absorb a Redis blip. A deliberate shutdown is not a blip, and
// applying the grace to it would delay by up to bus.ready_grace the one signal a rolling
// deploy depends on.
func TestReady_Draining(t *testing.T) {
	t.Parallel()
	fb := &probeBus{health: bus.Health{Connected: true}}
	r := newRigWithOptions(t, func(o *Options) { o.Bus = fb })
	r.http = newTestListener(t, r)

	if code, _ := probe(t, r, "/ready"); code != http.StatusOK {
		t.Fatalf("GET /ready = %d before the drain, want 200", code)
	}

	r.srv.StopAccepting()

	code, raw := probe(t, r, "/ready")
	if code != http.StatusServiceUnavailable {
		t.Fatalf("GET /ready = %d after StopAccepting, want 503 on a connected bus (§8 step 1)", code)
	}
	var body struct {
		Ready        bool `json:"ready"`
		BusConnected bool `json:"bus_connected"`
		Draining     bool `json:"draining"`
	}
	if err := json.Unmarshal([]byte(raw), &body); err != nil {
		t.Fatalf("decode %q: %v", raw, err)
	}
	if body.Ready || !body.Draining || !body.BusConnected {
		// bus_connected stays true, and that is the point of reporting both: "going away"
		// and "cannot reach Redis" are the same status code and different incidents.
		t.Fatalf("body = %+v, want ready false, draining true, bus_connected true", body)
	}
}

// TestProbes_AreUnauthenticatedAndOnTheClientListener pins where these live. They are on
// server.listen, alongside the websocket endpoint, because the loopback listener they used
// to share with the operator API went with it (docs/12-roadmap.md §2). Neither carries a
// token: they say "this process is up" and "this process can reach Redis", which is what a
// load balancer needs and what every health endpoint on the internet already says.
func TestProbes_AreUnauthenticatedAndOnTheClientListener(t *testing.T) {
	t.Parallel()
	r := newRig(t)

	for _, path := range []string{"/health", "/ready"} {
		if code, body := probe(t, r, path); code != http.StatusOK {
			t.Errorf("GET %s with no credential = %d %s, want 200", path, code, body)
		}
	}
	// Everything that is not the websocket path or a probe is still 404 (FR-1). The
	// operator API used to answer here on its own listener; it does not answer anywhere.
	for _, path := range []string{"/metrics", "/channels", "/channels/room-4410", "/disconnect"} {
		if code, _ := probe(t, r, path); code != http.StatusNotFound {
			t.Errorf("GET %s = %d, want 404: that surface was removed", path, code)
		}
	}
}
