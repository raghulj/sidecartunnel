package admin

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/raghulj/sidecartunnel/internal/bus"
)

// Defaults applied to a zero-valued Options. Each mirrors a documented configuration
// default in docs/08-config.md §3.
const (
	// defaultReadyGrace is bus.ready_grace: the bus may be down this long before /ready
	// reports 503. It is not zero for a reason that has taken services down — see the
	// ready handler.
	defaultReadyGrace = 30 * time.Second

	// defaultReadHeaderTimeout is server.read_header_timeout, applied to the admin
	// listener too: a connection that opens and never finishes its request headers
	// otherwise holds a server goroutine open indefinitely.
	defaultReadHeaderTimeout = 5 * time.Second

	// maxBodyBytes caps POST /disconnect. The largest legitimate body is two short
	// identifiers; anything beyond this is a mistake or an attempt to make the process
	// allocate.
	maxBodyBytes = 4 << 10
)

// Channel is one channel's occupancy on this replica.
//
// It is this replica's view only. Cluster-wide counts would need a scatter-gather across
// replicas, which is not built (docs/04-integration.md §4, docs/12-roadmap.md).
type Channel struct {
	// Name is the bare channel name, without bus.prefix.
	Name string `json:"channel"`

	// Subscribers is the number of connections on this replica holding the channel.
	Subscribers int `json:"subscribers"`

	// Users are the opaque user ids of those connections. It is populated for
	// GET /channels/{channel} and omitted from the list, where it would be a large
	// document nobody asked for.
	Users []string `json:"users,omitempty"`
}

// Target names the connections a disconnect applies to. Exactly one field is set.
//
// Both are matched exactly, never as a glob: a target of "u-*" reaches the connection
// literally named that and nothing else (docs/13-review-findings.md C8).
type Target struct {
	// User targets every connection for one opaque user id.
	User string

	// Client targets one connection by client id.
	Client string
}

// Registry is the admin listener's entire view of the hub.
//
// It is three methods on purpose. The admin package needs to list what this replica
// holds, describe one channel, and end connections; anything more would let an operator
// endpoint reach into hub internals, and would stop the two packages from being built and
// tested in parallel. It is the same seam hub.Sink is, in the other direction.
//
// Every method must be safe to call concurrently with live traffic, and none may block on
// network I/O: a /channels call that queues behind a 30,000-channel resubscribe is an
// incident tool that stops working during an incident.
type Registry interface {
	// Channels returns every channel this replica currently holds, with the number of
	// local subscribers on each. Users may be left empty; the list endpoint drops it.
	//
	// The prefix filter is applied here rather than passed down, so the query semantics
	// — a byte prefix on the channel name, never a glob — live with the HTTP contract
	// that defines them.
	Channels() []Channel

	// Channel returns one channel's subscriber count and the user ids holding it, and
	// reports whether this replica holds it at all.
	Channel(name string) (Channel, bool)

	// Disconnect closes every connection matching target and returns how many it closed.
	// It has the same effect as the control-channel disconnect action
	// (docs/04-integration.md §4): proto.CloseRevoked, reconnect false.
	//
	// A target held by no connection on this replica is not an error; it returns zero.
	Disconnect(target Target) (int, error)
}

// BusHealth is the admin listener's entire view of the bus.
//
// /ready is the only route that consults it. /health must never call it, and the
// interface is this narrow so that reading the handler makes that obvious
// (FR-20, docs/13-review-findings.md M20).
//
// Both bus implementations satisfy it through bus.HealthReporter.
type BusHealth interface {
	// Health returns a snapshot of the transport. It never blocks.
	Health() bus.Health
}

// Options configure a Server. Bus, Registry and Gatherer are required; everything else
// has a documented default.
type Options struct {
	// Token is admin.token, the bearer token for /channels and /disconnect.
	//
	// When it is empty those routes are not registered at all and return 404. That is
	// the rule docs/04-integration.md §4 states: an accidentally unconfigured admin API
	// should look absent, not permissive.
	Token string

	// ReadyGrace is bus.ready_grace. Default 30s.
	ReadyGrace time.Duration

	// ReadHeaderTimeout is the http.Server's header deadline. Default 5s.
	ReadHeaderTimeout time.Duration

	// Bus is consulted by /ready and by nothing else. Required.
	Bus BusHealth

	// Registry serves /channels and /disconnect. Required even when Token is empty, so
	// that turning the admin API on is a configuration change rather than a rewiring.
	Registry Registry

	// Gatherer is exposed on /metrics. It is the registry internal/metrics was
	// constructed against, never prometheus.DefaultGatherer. Required.
	Gatherer prometheus.Gatherer

	// Refresh, when set, runs immediately before /metrics gathers.
	//
	// It exists for the gauges whose value is owned elsewhere and only sampled here —
	// bus subscriptions, intake depth, webhook inflight. Sampling them at scrape time
	// makes them exact and saves the process a ticker whose period would silently
	// become the resolution of every one of those gauges.
	//
	// It must not block: it runs on the scrape's goroutine.
	Refresh func()

	// Logger defaults to slog.Default(). Nothing logged here carries a cookie, an
	// Authorization header, or a token (NFR-7).
	Logger *slog.Logger
}

// Server is the operator HTTP listener: /health, /ready, /metrics, /channels and
// POST /disconnect on their own http.Server, separate from the client listener and bound
// to loopback by default (docs/04-integration.md §4).
//
// It is safe for concurrent use; the routes are stateless apart from the dependencies
// they were built with.
type Server struct {
	token      string
	readyGrace time.Duration
	bus        BusHealth
	registry   Registry
	refresh    func()
	metrics    http.Handler
	log        *slog.Logger
	srv        *http.Server
}

// New builds the listener and its routes.
//
// It returns an error rather than panicking on a missing dependency: a half-wired admin
// listener that starts and fails at the first probe is worse than one that refuses to
// start, because the probe is what was supposed to notice (NFR-5).
//
// The returned Server is not listening. The caller binds admin.listen itself and passes
// the listener to Serve, so a bind failure is a startup error rather than something
// discovered later — silently failing to bind is how an operator loses /metrics without
// noticing.
func New(opts Options) (*Server, error) {
	switch {
	case opts.Bus == nil:
		return nil, errors.New("admin: no bus health source; /ready cannot be answered")
	case opts.Registry == nil:
		return nil, errors.New("admin: no registry; /channels and /disconnect cannot be answered")
	case opts.Gatherer == nil:
		return nil, errors.New("admin: no gatherer; /metrics cannot be answered")
	}

	s := &Server{
		token:      opts.Token,
		readyGrace: opts.ReadyGrace,
		bus:        opts.Bus,
		registry:   opts.Registry,
		refresh:    opts.Refresh,
		log:        opts.Logger,
	}
	if s.readyGrace == 0 {
		s.readyGrace = defaultReadyGrace
	}
	if s.log == nil {
		s.log = slog.Default()
	}
	s.metrics = promhttp.HandlerFor(opts.Gatherer, promhttp.HandlerOpts{})

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", s.health)
	mux.HandleFunc("GET /ready", s.ready)
	mux.HandleFunc("GET /metrics", s.serveMetrics)

	// FR-20: with no admin.token the authenticated routes are not registered, so the mux
	// answers 404 — the surface is absent rather than merely closed. Registering them and
	// returning 404 from the handler would work too; not registering them means no
	// request ever reaches the Registry, which is the property worth having.
	if opts.Token != "" {
		mux.Handle("GET /channels", s.authorize(http.HandlerFunc(s.channels)))
		// {channel...} rather than {channel}: a channel name is an opaque printable-ASCII
		// string and may contain a slash (docs/06-channels.md §1), which a single-segment
		// wildcard would refuse to match.
		mux.Handle("GET /channels/{channel...}", s.authorize(http.HandlerFunc(s.channel)))
		mux.Handle("POST /disconnect", s.authorize(http.HandlerFunc(s.disconnect)))
	}

	readHeaderTimeout := opts.ReadHeaderTimeout
	if readHeaderTimeout == 0 {
		readHeaderTimeout = defaultReadHeaderTimeout
	}
	s.srv = &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: readHeaderTimeout,
	}
	return s, nil
}

// Handler returns the route multiplexer. It exists for tests and for a caller that wants
// to mount the routes elsewhere; the routes carry their own authorization.
func (s *Server) Handler() http.Handler { return s.srv.Handler }

// Serve accepts connections on l until Shutdown is called, and returns
// http.ErrServerClosed when it was stopped cleanly. It blocks.
func (s *Server) Serve(l net.Listener) error { return s.srv.Serve(l) }

// Shutdown stops accepting and waits for in-flight requests, or for ctx to expire.
func (s *Server) Shutdown(ctx context.Context) error { return s.srv.Shutdown(ctx) }

// errorBody is the shape of every error response. It carries a fixed human-readable
// string and never the underlying error: an admin API answers on the same network as the
// thing it describes, and an error string is a place details leak from (NFR-7).
type errorBody struct {
	Error string `json:"error"`
}

// writeJSON writes v as JSON with the given status.
//
// It marshals to a buffer before writing the status line, so that a marshalling failure
// is a 500 rather than a 200 with a truncated body — a response that a monitoring system
// would read as success.
func (s *Server) writeJSON(w http.ResponseWriter, status int, v any) {
	body, err := json.Marshal(v)
	if err != nil {
		s.log.Error("admin.encode", "err", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if _, err := w.Write(body); err != nil {
		// The client hung up mid-response. There is nothing to do and nothing to alert
		// on; a probe that times out is already reporting it from the other side.
		s.log.Debug("admin.write", "err", err)
	}
}
