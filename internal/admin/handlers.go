package admin

import (
	"encoding/json"
	"net/http"
	"sort"
	"strings"
)

// healthBody is the /health response.
type healthBody struct {
	// Status is always "ok". A liveness probe reads the status code; this is for whoever
	// curls it during an incident.
	Status string `json:"status"`
}

// readyBody is the /ready response.
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
}

// channelsBody is the GET /channels response.
type channelsBody struct {
	// Channels are this replica's occupied channels, sorted by name.
	Channels []Channel `json:"channels"`
}

// disconnectRequest is the POST /disconnect body: {"user":"u-7"} or {"client":"…"}.
type disconnectRequest struct {
	// User targets every connection for one opaque user id.
	User string `json:"user"`

	// Client targets one connection by client id.
	Client string `json:"client"`
}

// disconnectBody is the POST /disconnect response.
type disconnectBody struct {
	// Disconnected is how many connections on this replica were closed. Zero is a normal
	// answer: the target may be held by another replica, or by none.
	Disconnected int `json:"disconnected"`
}

// health is liveness. It returns 200 while the process runs and never consults the bus.
//
// That last clause is the whole point of the endpoint, and it is FR-20 and
// docs/13-review-findings.md M20. A Redis restart makes every replica unready at once. A
// /health that consulted the bus, wired to a liveness probe, would kill every replica
// simultaneously, drop every connection, and convert an eight-second blip into a full
// application outage as the whole fleet re-authorizes together. Anything added to this
// handler that can fail turns liveness into readiness and rebuilds that outage.
func (s *Server) health(w http.ResponseWriter, _ *http.Request) {
	s.writeJSON(w, http.StatusOK, healthBody{Status: "ok"})
}

// ready is readiness: 503 once the bus has been down longer than bus.ready_grace.
//
// The grace is what stops a short blip from pulling the whole fleet out of the load
// balancer at once (docs/04-integration.md §4). It is compared against the bus's own
// DisconnectedFor rather than a timestamp kept here, because the bus is the only thing
// that knows when its transport went away, and a second clock would drift from it.
//
// Connections stay open and silent while the bus is down; readiness reports it, and
// nothing closes (NFR-8).
func (s *Server) ready(w http.ResponseWriter, _ *http.Request) {
	health := s.bus.Health()
	ready := health.Connected || health.DisconnectedFor <= s.readyGrace

	status := http.StatusOK
	if !ready {
		status = http.StatusServiceUnavailable
	}
	s.writeJSON(w, status, readyBody{
		Ready:          ready,
		BusConnected:   health.Connected,
		DownForSeconds: health.DisconnectedFor.Seconds(),
	})
}

// serveMetrics is the Prometheus exposition, unauthenticated: FR-20's acceptance
// criterion says /metrics does not require the token, and a scrape configuration that
// needs a secret is a scrape configuration that eventually does not happen.
//
// The refresh hook runs first, so gauges sampled from somewhere else are exact as of this
// scrape.
func (s *Server) serveMetrics(w http.ResponseWriter, r *http.Request) {
	if s.refresh != nil {
		s.refresh()
	}
	s.metrics.ServeHTTP(w, r)
}

// channels answers GET /channels?prefix=, this replica's occupied channels and their
// local subscriber counts.
//
// prefix is a byte prefix on the channel name, not a glob: "room-*" matches the channel
// literally named that and nothing else. Grants are the only place patterns are
// interpreted, and an operator endpoint that quietly expanded one would report a set that
// no authorization decision agrees with.
//
// The result is sorted so that two scrapes of an unchanged replica produce the same
// document, rather than Go's map order.
func (s *Server) channels(w http.ResponseWriter, r *http.Request) {
	prefix := r.URL.Query().Get("prefix")

	held := s.registry.Channels()
	out := make([]Channel, 0, len(held))
	for _, c := range held {
		if !strings.HasPrefix(c.Name, prefix) {
			continue
		}
		// User ids are dropped from the list. On a replica holding 10,000 channels the
		// full membership is a document nobody asked for, and the single-channel route
		// below is where an operator asks for it deliberately.
		out = append(out, Channel{Name: c.Name, Subscribers: c.Subscribers})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })

	s.writeJSON(w, http.StatusOK, channelsBody{Channels: out})
}

// channel answers GET /channels/{channel}: the subscriber count and user ids for one
// channel on this replica.
//
// A channel this replica does not hold is 404. That is the answer docs/10-operations.md
// §7 turns on — "nobody receives anything": the endpoint says what the gateway thinks is
// subscribed, and a channel that is missing here when the publisher believes it is not is
// the bug.
func (s *Server) channel(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("channel")
	held, ok := s.registry.Channel(name)
	if !ok {
		s.writeJSON(w, http.StatusNotFound, errorBody{Error: "no such channel on this replica"})
		return
	}
	s.writeJSON(w, http.StatusOK, held)
}

// disconnect answers POST /disconnect, closing the connections a target names. Same
// effect as the control-channel disconnect action (docs/04-integration.md §4).
//
// Exactly one of user and client must be named. An omitted target is a validation error,
// not "everyone": treating it as everyone means one request forces every connection on
// the replica to re-authorize at once, which is the application outage
// docs/10-operations.md §4 models, available on demand to anyone holding the token
// (docs/13-review-findings.md C8).
func (s *Server) disconnect(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
	decoder := json.NewDecoder(r.Body)
	// A body naming "users" instead of "user" is a mistake that must not be read as "no
	// target"; rejecting unknown fields turns a typo into a 400 rather than a 400 whose
	// message is about something else.
	decoder.DisallowUnknownFields()

	var req disconnectRequest
	if err := decoder.Decode(&req); err != nil {
		s.writeJSON(w, http.StatusBadRequest, errorBody{Error: "body must be a JSON object with one of user or client"})
		return
	}

	// A conversion rather than a field-by-field copy: the two structs are the same shape
	// on purpose, and if one grows a field the other does not, this stops compiling
	// instead of silently dropping it.
	target := Target(req)
	switch {
	case target.User == "" && target.Client == "":
		s.writeJSON(w, http.StatusBadRequest, errorBody{Error: "name exactly one of user or client; an omitted target is not everyone"})
		return
	case target.User != "" && target.Client != "":
		s.writeJSON(w, http.StatusBadRequest, errorBody{Error: "name exactly one of user or client, not both"})
		return
	}

	closed, err := s.registry.Disconnect(target)
	if err != nil {
		s.log.Error("admin.disconnect", "user", target.User, "client", target.Client, "err", err)
		s.writeJSON(w, http.StatusInternalServerError, errorBody{Error: "disconnect failed"})
		return
	}
	s.log.Info("admin.disconnect", "user", target.User, "client", target.Client, "closed", closed)
	s.writeJSON(w, http.StatusOK, disconnectBody{Disconnected: closed})
}
