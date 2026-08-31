package server

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"

	"github.com/raghulj/sidecartunnel/internal/bus"
	"github.com/raghulj/sidecartunnel/internal/config"
	"github.com/raghulj/sidecartunnel/internal/conn"
	"github.com/raghulj/sidecartunnel/internal/hub"
	"github.com/raghulj/sidecartunnel/internal/proto"
	"github.com/raghulj/sidecartunnel/internal/webhook"
)

// Defaults applied to a Config field left at zero. Each mirrors the documented default in
// docs/08-config.md §3, so a Server built from a hand-written Config in a test behaves
// like one built from a validated file.
const (
	defaultPath              = "/ws"
	defaultReadHeaderTimeout = 5 * time.Second
	defaultDrainTimeout      = 20 * time.Second
	defaultDrainSpread       = 60 * time.Second
	defaultReadyGrace        = 30 * time.Second
)

// The probe routes, on the same listener as the websocket endpoint.
//
// They used to live on a second listener bound to loopback, so that a proxy
// misconfiguration could not expose the operator API to the internet. That listener is
// gone along with the operator API it protected (docs/12-roadmap.md §2): these two routes
// leak "this process is up" and "this process can reach Redis", which is what a load
// balancer needs and what every health endpoint on the internet already says. In the
// documented deployment the proxy forwards only server.path, so neither is publicly
// reachable anyway (docs/10-operations.md §2) — but that is defence in depth and not the
// reason these are safe.
const (
	healthPath = "/health"
	readyPath  = "/ready"
)

// drainReason is the close reason on a graceful shutdown. It names the condition and
// nothing else: a reason must never carry a cookie, a header value or a payload (NFR-7).
const drainReason = "draining"

// BusHealth is the Server's entire view of the bus, and GET /ready is the only thing that
// consults it.
//
// GET /health must never call it. The distinction is load-bearing: a Redis restart makes
// every replica unready at once, so /ready wired to a liveness probe kills every replica
// simultaneously, drops every connection, and converts an eight-second blip into a full
// application outage as the whole fleet re-authorizes together
// (docs/13-review-findings.md M20). The interface is this narrow so that reading the two
// handlers makes it obvious which one is which.
//
// bus.HealthReporter satisfies it, so both bus implementations do.
type BusHealth interface {
	// Health returns a snapshot of the transport. It never blocks.
	Health() bus.Health
}

// Connector is the connect webhook as this package uses it. *webhook.Client implements
// it, and New builds one when the caller supplies none.
//
// It is an interface for one reason worth stating: FR-2's acceptance criterion is that a
// handshake with a foreign Origin makes **no application call at all**, and the only way
// to assert the absence of a call is to hold something that would have recorded one.
//
// Implementations must be safe for concurrent use — one is shared by every connection on
// the replica — and must never return nil (docs/13-review-findings.md FR-6).
type Connector interface {
	// Call turns one connection's request into a Result: Authorized, Refused or
	// Unavailable. It honours ctx, which carries app.connect_timeout.
	Call(ctx context.Context, req webhook.Request) webhook.Result
}

// Options are the dependencies of a Server. Config and Hub are required; everything else
// has a working default, and every default is stated on the field.
//
// There is no package-level configuration and no singleton: two tests configure a Server
// differently and run at the same time, which a global would make impossible
// (docs/14-coding-standards.md §7).
type Options struct {
	// Config is the whole configuration tree. Required. It is read, never retained past
	// New except for the values copied onto the Server.
	Config *config.Config

	// Hub is the channel registry every connection on this replica shares. Required.
	Hub *hub.Hub

	// Bus is what GET /ready reports on, and nothing else reads it. Required: a gateway
	// that cannot answer readiness is a gateway a load balancer cannot manage, and
	// discovering that at the first probe rather than at startup is the failure NFR-5 is
	// about.
	Bus BusHealth

	// Webhook is the connect client. Nil builds one from Config, which is what puts
	// server.trusted_proxies in the hands of the package that owns the X-Forwarded-For
	// walk (FR-24).
	Webhook Connector

	// Clock is the time source handed to every connection. Defaults to conn.SystemClock.
	Clock conn.Clock

	// Log receives connection events. Defaults to slog.Default. Nothing logged here
	// carries a cookie, an Authorization header or a payload (NFR-7).
	Log *slog.Logger

	// ClientID overrides the client id generator. Empty — the default — leaves it to
	// internal/conn, which takes 16 hex characters from crypto/rand.
	//
	// It is a seam rather than a convenience: the hub refuses a client id already held by
	// another connection, and crypto/rand makes that refusal unreachable, so without a
	// way to force a collision the branch that protects control targeting (FR-18) could
	// never be exercised.
	ClientID func() string
}

// Stats are a Server's cumulative counters, read by the drain log and by tests.
//
// OriginRejected is kept apart from OverCapacity because they are different incidents: a
// rejected Origin is somebody trying to hijack a session, and a 503 is this replica being
// full (FR-2, NFR-1).
type Stats struct {
	// OriginRejected counts handshakes refused by the allowlist, before any application
	// call. Each one is logged at warn with the offending Origin.
	OriginRejected uint64

	// OverCapacity counts handshakes refused with 503, by limits.max_connections or
	// because the replica is draining.
	OverCapacity uint64

	// Accepted counts authorizations the application granted and this replica admitted.
	Accepted uint64

	// Refused counts connections closed with proto.CloseUnauthorized (3003).
	Refused uint64

	// Unavailable counts connections closed with proto.CloseAuthUnavailable (3008).
	Unavailable uint64

	// UserLimited counts authorizations refused by limits.max_connections_per_user.
	UserLimited uint64

	// Current is the number of connections held right now.
	Current int
}

// counters are the atomic halves of Stats.
type counters struct {
	originRejected atomic.Uint64
	overCapacity   atomic.Uint64
	accepted       atomic.Uint64
	refused        atomic.Uint64
	unavailable    atomic.Uint64
	userLimited    atomic.Uint64
}

// Server is the client-facing listener: the Origin check, the connection-count check, the
// websocket upgrade, and the assembly of a connection from a successful handshake.
//
// One Server is shared by every connection on the replica. Every exported method is safe
// to call concurrently.
//
// The order at the upgrade is normative and it matters (docs/03-client-protocol.md §2):
// the Origin allowlist first, answering 403 with no application call, then the connection
// count, answering 503. Only then is the upgrade completed and a connection built.
type Server struct {
	cfg     *config.Config
	hub     *hub.Hub
	bus     BusHealth
	webhook Connector
	clock   conn.Clock
	log     *slog.Logger
	newID   func() string

	// origins is server.allowed_origins as a set. A map lookup is an exact string
	// comparison and cannot accidentally become a suffix match, which is the point
	// (FR-2, docs/05-authorization.md §5).
	origins            map[string]struct{}
	allowMissingOrigin bool

	// rates is the parsed namespaces[].rate_limit, by namespace name. Parsing at
	// construction rather than per publish is what makes an unparseable rate a startup
	// error naming the key rather than a client event that mysteriously fails (NFR-5).
	rates map[string]rate

	upgrader websocket.Upgrader
	mux      *http.ServeMux

	// ctx bounds every connection this server accepts. Drain cancels it as its last act,
	// so a connection that outlived the drain budget still unwinds.
	ctx    context.Context
	cancel context.CancelFunc

	// mu guards the connection bookkeeping below. It is never held across a network
	// operation, and never while closing a connection: Close deregisters from the hub,
	// and holding two locks in two orders is the deadlock docs/09-internals.md §4.4 is
	// about.
	mu sync.Mutex

	// conns maps every live connection to the user it authorized as, or "" before the
	// application has answered. It is what the drain closes and what the per-user cap
	// counts.
	conns map[*conn.Conn]string

	// users counts live connections per user id, for limits.max_connections_per_user.
	// One looping client must not consume the global cap
	// (docs/13-review-findings.md m8).
	users map[string]int

	// current is the admitted-connection count, reserved before the upgrade so that two
	// simultaneous handshakes cannot both pass limits.max_connections.
	current int

	// draining is set by StopAccepting and by Drain. It refuses new upgrades with 503 and
	// makes GET /ready answer 503.
	draining bool

	// stopped is set by Drain alone, and is what Serve refuses to start on.
	//
	// It is a second flag rather than a reuse of draining, and the difference is the whole
	// point of StopAccepting: announcing that this replica is going away must not tear
	// down the listener that has to answer /ready while it goes. Sharing one flag meant a
	// StopAccepting that raced ahead of Serve closed the listener before it ever accepted
	// anything.
	stopped bool

	http  *http.Server
	bound atomic.Bool

	// wg tracks one goroutine per live connection — the handler goroutine that becomes
	// the reader. Drain waits on it, which is what makes FR-19's bound assertable.
	wg sync.WaitGroup

	stats counters
}

// New builds a Server. It returns an error rather than a Server that fails at the first
// connection: an unparseable rate limit or an unusable trusted-proxy CIDR is a startup
// failure naming the key (NFR-5).
//
// config.Validate already enforces most of what is checked here. The checks stay anyway,
// because a Server is also built from a hand-written Config in tests and in main's
// wiring, and a silently defaulted value is a gateway that runs and does the wrong thing.
func New(opts Options) (*Server, error) {
	if opts.Config == nil {
		return nil, fmt.Errorf("server: Options.Config is required")
	}
	if opts.Hub == nil {
		return nil, fmt.Errorf("server: Options.Hub is required")
	}
	if opts.Bus == nil {
		return nil, fmt.Errorf("server: Options.Bus is required; GET /ready cannot be answered")
	}
	cfg := opts.Config

	rates, err := parseRates(cfg.Namespaces)
	if err != nil {
		return nil, err
	}

	log := opts.Log
	if log == nil {
		log = slog.Default()
	}

	connector := opts.Webhook
	if connector == nil {
		// FR-24: server.trusted_proxies goes to the webhook client, which owns the walk
		// over X-Forwarded-For. This package must never do that walk itself — the same
		// rule in two places is one refactor away from forwarding a client-supplied
		// 127.0.0.1 to an application's localhost trust path.
		client, err := webhook.New(webhook.Options{
			App:            cfg.App,
			TrustedProxies: cfg.Server.TrustedProxies,
			Logger:         log,
		})
		if err != nil {
			return nil, fmt.Errorf("server: connect webhook: %w", err)
		}
		connector = client
	}

	origins := make(map[string]struct{}, len(cfg.Server.AllowedOrigins))
	for _, origin := range cfg.Server.AllowedOrigins {
		origins[origin] = struct{}{}
	}

	ctx, cancel := context.WithCancel(context.Background())
	s := &Server{
		cfg:                cfg,
		hub:                opts.Hub,
		bus:                opts.Bus,
		webhook:            connector,
		clock:              opts.Clock,
		log:                log,
		newID:              opts.ClientID,
		origins:            origins,
		allowMissingOrigin: cfg.Server.AllowMissingOrigin,
		rates:              rates,
		conns:              make(map[*conn.Conn]string),
		users:              make(map[string]int),
		ctx:                ctx,
		cancel:             cancel,
	}
	if s.clock == nil {
		s.clock = conn.SystemClock()
	}
	if s.newID == nil {
		s.newID = func() string { return "" }
	}
	s.upgrader = websocket.Upgrader{
		ReadBufferSize:    cfg.Limits.ReadBuffer,
		WriteBufferSize:   cfg.Limits.WriteBuffer,
		EnableCompression: cfg.Limits.Compression,

		// The Origin check has already run, against the configured allowlist, and
		// answered 403 if it failed. gorilla's default CheckOrigin compares the Origin's
		// host to the request's Host, which is neither FR-2's rule nor a subset of it: it
		// would refuse an allowlisted cross-origin deployment and admit any page served
		// from the gateway's own host. Deciding here would also put the check after the
		// upgrade has begun, where a 403 is no longer the answer §2 requires.
		CheckOrigin: func(*http.Request) bool { return true },
	}
	s.mux = http.NewServeMux()
	s.mux.Handle(s.path(), s)
	s.mux.HandleFunc("GET "+healthPath, s.health)
	s.mux.HandleFunc("GET "+readyPath, s.ready)
	return s, nil
}

// Handler returns the HTTP handler for this server: the websocket endpoint mounted at
// server.path, GET /health, GET /ready, and 404 everywhere else (FR-1, FR-20).
func (s *Server) Handler() http.Handler { return s.mux }

// Serve accepts connections on l until Drain shuts it down, and returns
// http.ErrServerClosed when it does.
//
// ReadHeaderTimeout is set, because the upgrade is an HTTP request like any other and a
// slowloris that dribbles headers holds a connection with no timeout otherwise.
func (s *Server) Serve(l net.Listener) error {
	srv := &http.Server{
		Handler:           s.Handler(),
		ReadHeaderTimeout: s.readHeaderTimeout(),
		BaseContext:       func(net.Listener) context.Context { return s.ctx },
	}

	s.mu.Lock()
	stopped := s.stopped
	if !stopped {
		s.http = srv
	}
	s.mu.Unlock()
	if stopped {
		if err := l.Close(); err != nil {
			// coverage: closing a listener the caller just opened fails only on an
			// already-closed one; the drain answer below is the same either way.
			s.log.Debug("close listener", "err", err)
		}
		return http.ErrServerClosed
	}

	s.bound.Store(true)
	if err := srv.Serve(l); err != nil {
		return fmt.Errorf("server: serve: %w", err)
	}
	// coverage: http.Server.Serve never returns nil; it returns ErrServerClosed or the
	// accept error. The branch exists so a future change cannot lose one silently.
	return nil
}

// ListenAndServe binds server.listen and serves it. It is the path main takes, and it
// reports a bind failure rather than running and answering nothing (NFR-5).
func (s *Server) ListenAndServe() error {
	l, err := net.Listen("tcp", s.cfg.Server.Listen)
	if err != nil {
		return fmt.Errorf("server: listen on server.listen %q: %w", s.cfg.Server.Listen, err)
	}
	return s.Serve(l)
}

// Drain performs the graceful shutdown of docs/09-internals.md §8 and FR-19: stop
// accepting, close every connection with proto.CloseDraining and reconnect true, and
// return within server.drain_timeout.
//
// The retry_after each client is given is spread across server.drain_spread by the
// connection layer, deterministically per client id. That spread is not an optimization:
// the gateway knows how many connections it is dropping and the client does not, and
// without it a replica's whole population re-authorizes inside the one-second window
// docs/10-operations.md §4 models as an application outage (S5,
// docs/03-client-protocol.md §7.1).
//
// It is idempotent — SIGTERM and a cancelled context can both arrive — and safe to call
// concurrently with Serve. An error means connections were still open when the budget ran
// out; the caller logs it and exits anyway, because a rolling deploy that never completes
// is worse than one that drops a socket.
func (s *Server) Drain(ctx context.Context) error {
	s.mu.Lock()
	s.draining = true
	s.stopped = true
	srv := s.http
	live := make([]*conn.Conn, 0, len(s.conns))
	for c := range s.conns {
		live = append(live, c)
	}
	s.mu.Unlock()

	ctx, cancel := context.WithTimeout(ctx, s.drainTimeout())
	defer cancel()

	var shutdownErr error
	if srv != nil {
		if err := srv.Shutdown(ctx); err != nil {
			shutdownErr = fmt.Errorf("server: shut down the listener: %w", err)
		}
	}

	// Step 2 of §8, and the reason drain is deliberate rather than a process exit:
	// clients that see 3000 apply their backoff instead of treating an abrupt TCP reset
	// as a network blip and retrying immediately.
	for _, c := range live {
		c.Close(proto.CloseDraining, drainReason)
	}

	err := s.wait(ctx)
	s.cancel()
	if shutdownErr != nil {
		return shutdownErr
	}
	return err
}

// wait blocks until every connection goroutine has returned, or the drain budget expires.
func (s *Server) wait(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("server: %d connection(s) still open after server.drain_timeout %s: %w",
			s.Stats().Current, s.drainTimeout(), ctx.Err())
	}
}

// Stats returns the Server's cumulative counters. It never blocks for longer than the
// bookkeeping lock and is safe to call concurrently.
func (s *Server) Stats() Stats {
	s.mu.Lock()
	current := s.current
	s.mu.Unlock()
	return Stats{
		OriginRejected: s.stats.originRejected.Load(),
		OverCapacity:   s.stats.overCapacity.Load(),
		Accepted:       s.stats.accepted.Load(),
		Refused:        s.stats.refused.Load(),
		Unavailable:    s.stats.unavailable.Load(),
		UserLimited:    s.stats.userLimited.Load(),
		Current:        current,
	}
}

// path is server.path, defaulted.
func (s *Server) path() string {
	if s.cfg.Server.Path == "" {
		return defaultPath
	}
	return s.cfg.Server.Path
}

// readHeaderTimeout is server.read_header_timeout, defaulted. It is the slowloris guard
// on the upgrade request.
func (s *Server) readHeaderTimeout() time.Duration {
	return positive(s.cfg.Server.ReadHeaderTimeout.Duration(), defaultReadHeaderTimeout)
}

// drainTimeout is server.drain_timeout, defaulted (FR-19).
func (s *Server) drainTimeout() time.Duration {
	return positive(s.cfg.Server.DrainTimeout.Duration(), defaultDrainTimeout)
}

// drainSpread is server.drain_spread, defaulted. It is handed to every connection as the
// window its retry_after is spread across (docs/03-client-protocol.md §7.1).
func (s *Server) drainSpread() time.Duration {
	return positive(s.cfg.Server.DrainSpread.Duration(), defaultDrainSpread)
}

// listening reports whether a listener is bound. It exists for the tests that start
// ListenAndServe on its own goroutine and need to know it is up before shutting it down.
func (s *Server) listening() bool { return s.bound.Load() }

// positive returns v when it is above zero, and fallback otherwise.
func positive(v, fallback time.Duration) time.Duration {
	if v > 0 {
		return v
	}
	return fallback
}
