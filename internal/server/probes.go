package server

import (
	"encoding/json"
	"net/http"
	"time"
)

// healthBody is the GET /health response.
type healthBody struct {
	// Status is always "ok". A liveness probe reads the status code; this is for whoever
	// curls it during an incident.
	Status string `json:"status"`
}

// readyBody is the GET /ready response.
type readyBody struct {
	// Ready mirrors the status code: true with 200, false with 503.
	Ready bool `json:"ready"`

	// BusConnected is the transport's state right now.
	BusConnected bool `json:"bus_connected"`

	// DownForSeconds is how long the bus has been down, and zero while connected. It is
	// in the body because the difference between "down for 2 seconds" and "down for 20
	// minutes" is the difference between waiting and paging someone, and the status code
	// cannot say which it is.
	DownForSeconds float64 `json:"bus_down_for_seconds"`

	// Reconnects is the number of bus transports this process has established after the
	// first. It is cumulative for the life of the process, so it is read by curling twice
	// and comparing.
	//
	// It is here because it is the only way to see the M8 signature. A reader evicted by
	// Redis's pubsub output buffer reconnects, resubscribes, falls behind again and is
	// evicted again — a stable oscillation, against a Redis that is itself perfectly
	// healthy. Every other symptom of it points on-call at Redis, which is the wrong
	// system (docs/10-operations.md §7). A count climbing while BusConnected is true is
	// that oscillation and nothing else.
	Reconnects uint64 `json:"bus_reconnects"`

	// Draining is true once the replica has begun shutting down. It is reported so that
	// an operator watching a rolling deploy can tell "this replica is going away" from
	// "this replica cannot reach Redis", which are the same status code and very
	// different incidents.
	Draining bool `json:"draining"`
}

// health is liveness. It returns 200 while the process runs, and it never consults the
// bus.
//
// That last clause is the whole point of the endpoint (FR-20,
// docs/13-review-findings.md M20). A Redis restart makes every replica unready at once. A
// /health that consulted the bus, wired to a liveness probe, would kill every replica
// simultaneously, drop every connection, and convert an eight-second blip into a full
// application outage as the whole fleet re-authorizes together against one connect
// webhook. Anything added to this handler that can fail turns liveness into readiness and
// rebuilds that outage.
//
// It stays 200 through the drain, deliberately. A draining replica is alive and is
// finishing its work; the thing that should change is readiness, and it does.
//
// It is unauthenticated because it says only that this process is running, which is what
// a load balancer needs and what every health endpoint on the internet already says.
func (s *Server) health(w http.ResponseWriter, _ *http.Request) {
	s.writeJSON(w, http.StatusOK, healthBody{Status: "ok"})
}

// ready is readiness: 503 once the replica is draining, or once the bus has been down
// longer than bus.ready_grace.
//
// The grace is what stops a short blip from pulling the whole fleet out of the load
// balancer at once (docs/04-integration.md §4). It is compared against the bus's own
// DisconnectedFor rather than a timestamp kept here, because the bus is the only thing
// that knows when its transport went away and a second clock would drift from it.
//
// Draining is checked first and takes no grace at all. Step 1 of docs/09-internals.md §8
// requires /ready to answer 503 from the next probe, so the load balancer stops steering
// new connections here well before the sockets close; a grace applied to a deliberate
// shutdown would delay exactly the signal the shutdown depends on.
//
// Connections stay open and silent while the bus is down; readiness reports it, and
// nothing closes (NFR-8).
func (s *Server) ready(w http.ResponseWriter, _ *http.Request) {
	health := s.bus.Health()

	s.mu.Lock()
	draining := s.draining
	s.mu.Unlock()

	ready := !draining && (health.Connected || health.DisconnectedFor <= s.readyGrace())

	status := http.StatusOK
	if !ready {
		status = http.StatusServiceUnavailable
	}
	s.writeJSON(w, status, readyBody{
		Ready:          ready,
		BusConnected:   health.Connected,
		DownForSeconds: health.DisconnectedFor.Seconds(),
		Reconnects:     health.Reconnects,
		Draining:       draining,
	})
}

// StopAccepting begins the drain's first step without closing anything: new upgrades get
// 503 and GET /ready answers 503 from the next probe.
//
// It is separate from Drain because the two have to happen in that order and with a gap
// between them. Drain shuts the listener down, and a shut-down listener cannot answer
// /ready at all — the load balancer would see a refused connection rather than an honest
// 503, at the exact moment the point of the exercise is to be told honestly. Calling this
// first means the replica reports itself out of rotation while it is still reachable
// (docs/09-internals.md §8 step 1).
//
// It is idempotent and safe to call concurrently. Drain does the same thing itself, so a
// caller that only calls Drain still drains correctly — it just skips the announcement.
func (s *Server) StopAccepting() {
	s.mu.Lock()
	s.draining = true
	s.mu.Unlock()
}

// readyGrace is bus.ready_grace, defaulted.
func (s *Server) readyGrace() time.Duration {
	if d := s.cfg.Bus.ReadyGrace.Duration(); d > 0 {
		return d
	}
	return defaultReadyGrace
}

// writeJSON writes v as JSON with the given status.
//
// It marshals to a buffer before writing the status line, so that a marshalling failure is
// a 500 rather than a 200 with a truncated body — a response a load balancer would read as
// success.
func (s *Server) writeJSON(w http.ResponseWriter, status int, v any) {
	body, err := json.Marshal(v)
	if err != nil {
		// coverage: healthBody and readyBody are strings, bools and numbers, none of
		// which encoding/json can fail on. The branch stays so that a field added later
		// cannot turn an unmarshalable value into a 200 with an empty body, which is the
		// one failure a probe reads as success.
		s.log.Error("probe.encode", "err", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if _, err := w.Write(body); err != nil {
		// coverage: the client hung up mid-response. There is nothing to do and nothing
		// to alert on; a probe that times out is already reporting it from the other side.
		s.log.Debug("probe.write", "err", err)
	}
}
