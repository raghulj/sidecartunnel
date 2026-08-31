package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/raghulj/sidecartunnel/internal/admin"
	"github.com/raghulj/sidecartunnel/internal/bus"
	"github.com/raghulj/sidecartunnel/internal/config"
	"github.com/raghulj/sidecartunnel/internal/hub"
	"github.com/raghulj/sidecartunnel/internal/metrics"
	"github.com/raghulj/sidecartunnel/internal/server"
	"github.com/raghulj/sidecartunnel/internal/webhook"
)

// adminShutdownTimeout bounds the wait for in-flight admin requests once the client
// drain has finished. It is short on purpose: the only requests it can be holding are a
// scrape and an operator's curl, and a rolling deploy that waits on either is a rolling
// deploy that stalls.
const adminShutdownTimeout = 2 * time.Second

// memoryBusWarning is logged, at warn, on every start with bus.kind: memory.
//
// It is loud because the failure it describes is invisible. A memory bus delivers a
// publish to this process's own subscribers and to nobody else, so two replicas behind
// one load balancer produce messages that arrive for some users and not others, with
// nothing in any metric or log to distinguish it from an application that did not publish
// (docs/08-config.md §3).
const memoryBusWarning = "bus.kind is memory: this replica shares no messages with any other. " +
	"Running more than one replica on the memory bus is undetectable by the gateway and delivers " +
	"messages to some users and not others. Use bus.kind: redis for anything but a single-node " +
	"development instance."

// gateway is the whole object graph, built once by build and torn down once by close.
//
// It exists so that main holds no state and no package-level variable: every dependency
// below is passed explicitly to the thing that uses it, which is what makes each of those
// things constructible in a test (docs/14-coding-standards.md §7).
type gateway struct {
	cfg *config.Config
	log *slog.Logger

	metrics   *metrics.Metrics
	bus       bus.HealthReporter
	hub       *hub.Hub
	webhook   *webhook.Client
	server    *server.Server
	admin     *admin.Server
	consumer  *consumer
	readiness *readiness

	// clientLn and adminLn are bound by build, before anything starts. A bind failure is
	// a startup error and never a listener that silently is not there: an operator who
	// loses /metrics to a port clash finds out at the next incident (NFR-5,
	// docs/04-integration.md §4).
	clientLn net.Listener
	adminLn  net.Listener
}

// build wires the graph: metrics registry, bus, hub, connect webhook client, client
// listener, admin listener, and the bus consumer that feeds the hub.
//
// reg is the caller's Prometheus registry, never prometheus.DefaultRegisterer: a global
// registry makes two gateways in one process collide on duplicate registration, which is
// the exact trap docs/14-coding-standards.md §7 names.
//
// ctx bounds the hub's background goroutines. Every error closes whatever was already
// built, so a failed build leaks neither a goroutine nor a bound port (NFR-3).
func build(ctx context.Context, cfg *config.Config, reg *prometheus.Registry, log *slog.Logger) (*gateway, error) {
	g := &gateway{cfg: cfg, log: log}
	built := false
	defer func() {
		if !built {
			g.close()
		}
	}()

	var err error
	names := make([]string, 0, len(cfg.Namespaces))
	for _, ns := range cfg.Namespaces {
		names = append(names, ns.Name)
	}
	g.metrics, err = metrics.New(reg, metrics.Options{
		App:        cfg.App.Name,
		Separator:  cfg.Channels.Separator,
		Namespaces: names,
	})
	if err != nil {
		return nil, fmt.Errorf("metrics: %w", err)
	}

	if g.bus, err = newBus(cfg, log); err != nil {
		return nil, err
	}

	g.hub = hub.New(ctx, g.bus, hub.Options{
		Prefix:                  cfg.Bus.Prefix,
		Separator:               cfg.Channels.Separator,
		Namespaces:              cfg.Namespaces,
		MaxSubscriptionsPerConn: cfg.Limits.MaxSubscriptionsPerConn,
		RetryMin:                cfg.Bus.ReconnectMin.Duration(),
		RetryMax:                cfg.Bus.ReconnectMax.Duration(),
	})

	// FR-24: the webhook client owns the X-Forwarded-For walk, so server.trusted_proxies
	// goes to it and to nothing else. It is built here rather than left to server.New so
	// that this process can read its in-flight count for st_webhook_inflight and flush
	// its cache on a control disconnect.
	if g.webhook, err = webhook.New(webhook.Options{
		App:            cfg.App,
		TrustedProxies: cfg.Server.TrustedProxies,
		Logger:         log,
	}); err != nil {
		return nil, fmt.Errorf("connect webhook: %w", err)
	}

	if g.server, err = server.New(server.Options{
		Config:  cfg,
		Hub:     g.hub,
		Webhook: g.webhook,
		Log:     log,
	}); err != nil {
		return nil, err
	}

	if g.clientLn, err = net.Listen("tcp", cfg.Server.Listen); err != nil {
		return nil, fmt.Errorf("listen on server.listen %q: %w", cfg.Server.Listen, err)
	}
	if g.adminLn, err = net.Listen("tcp", cfg.Admin.Listen); err != nil {
		return nil, fmt.Errorf("listen on admin.listen %q: %w", cfg.Admin.Listen, err)
	}

	g.readiness = &readiness{bus: g.bus}
	sample := &sampler{metrics: g.metrics, bus: g.readiness, webhook: g.webhook, server: g.server}
	if g.admin, err = admin.New(admin.Options{
		Token:             cfg.Admin.Token,
		ReadyGrace:        cfg.Bus.ReadyGrace.Duration(),
		ReadHeaderTimeout: cfg.Server.ReadHeaderTimeout.Duration(),
		Bus:               g.readiness,
		Registry:          adminRegistry{hub: g.hub, flush: g.webhook.Flush},
		Gatherer:          reg,
		Refresh:           sample.refresh,
		Logger:            log,
	}); err != nil {
		// coverage: admin.New fails only on a nil Bus, Registry or Gatherer, and all
		// three are constructed non-nil in the lines above. It is checked rather than
		// discarded so that a fourth required dependency cannot be added without this
		// stopping at startup, and there is no seam to force it that would exist for any
		// reason but this test.
		return nil, fmt.Errorf("admin listener: %w", err)
	}

	g.consumer = newConsumer(g.bus, g.hub, g.metrics, log,
		[]byte(cfg.Control.Secret), cfg.Bus.Prefix, cfg.Bus.DispatchWorkers, g.webhook.Flush, nil)

	built = true
	return g, nil
}

// newBus builds the transport docs/08-config.md §3's bus block names.
//
// The memory bus is warned about on every single start, not once per deployment and not
// only when a second replica is detected, because a second replica cannot be detected:
// nothing in the memory bus can know another process exists.
func newBus(cfg *config.Config, log *slog.Logger) (bus.HealthReporter, error) {
	if cfg.Bus.Kind == "memory" {
		log.Warn(memoryBusWarning, "bus.kind", "memory")
		return bus.NewMemory(cfg.Bus.IntakeQueue), nil
	}
	b, err := bus.NewRedis(bus.RedisOptions{
		URL:          cfg.Bus.URL,
		DialTimeout:  cfg.Bus.DialTimeout.Duration(),
		ReconnectMin: cfg.Bus.ReconnectMin.Duration(),
		ReconnectMax: cfg.Bus.ReconnectMax.Duration(),
		IntakeQueue:  cfg.Bus.IntakeQueue,
	})
	if err != nil {
		// The message names the key and never the value: bus.url can carry a password
		// (docs/14-coding-standards.md §9).
		return nil, fmt.Errorf("bus: %w", err)
	}
	return b, nil
}

// serve runs the gateway until ctx ends, a signal arrives, or a listener fails, then
// performs the FR-19 drain. It returns the process exit code.
//
// The shutdown sequence is docs/09-internals.md §8, in order: stop accepting and report
// /ready 503, close every connection with 3000 and reconnect true, wait up to
// server.drain_timeout, stop the bus consumer, stop the admin listener, and let close
// unsubscribe and release the transport.
//
// A second signal abandons the drain and returns immediately. That is deliberate: an
// operator who sends SIGTERM twice has decided the drain is not going to finish, and the
// only thing left to respect is the decision.
func (g *gateway) serve(ctx context.Context, signals <-chan os.Signal) int {
	// WithoutCancel, not Background: the consumer inherits ctx's values but not its
	// cancellation, because the drain has to keep delivering while it closes connections.
	// A consumer that stopped with ctx would go silent at the exact moment every
	// connected client is being told to reconnect.
	consumerCtx, stopConsumer := context.WithCancel(context.WithoutCancel(ctx))
	defer stopConsumer()
	consumerDone := make(chan struct{})
	go func() {
		defer close(consumerDone)
		g.consumer.run(consumerCtx)
	}()

	fatal := make(chan error, 2)
	go func() {
		if err := g.admin.Serve(g.adminLn); err != nil && !errors.Is(err, http.ErrServerClosed) {
			fatal <- fmt.Errorf("admin listener on admin.listen %q: %w", g.cfg.Admin.Listen, err)
		}
	}()
	go func() {
		if err := g.server.Serve(g.clientLn); err != nil && !errors.Is(err, http.ErrServerClosed) {
			fatal <- fmt.Errorf("client listener on server.listen %q: %w", g.cfg.Server.Listen, err)
		}
	}()

	g.log.Info("sidecartunnel started",
		"version", version, "commit", commit, "built", date,
		"server.listen", g.clientLn.Addr().String(), "server.path", g.cfg.Server.Path,
		"admin.listen", g.adminLn.Addr().String(),
		"bus.kind", g.cfg.Bus.Kind, "bus.prefix", g.cfg.Bus.Prefix,
		"app.name", g.cfg.App.Name, "namespaces", len(g.cfg.Namespaces),
		"dispatch_workers", g.cfg.Bus.DispatchWorkers)

	code := exitOK
	if err := waitForStop(ctx, signals, fatal, g.log); err != nil {
		g.log.Error("a listener failed; draining", "err", err)
		code = exitFailure
	}

	// Step 1 of §8: /ready answers 503 from the next probe, so the load balancer stops
	// steering new connections here well before the sockets close.
	g.readiness.drain()

	drained := make(chan error, 1)
	// The drain outlives ctx for the same reason: FR-19 gives it server.drain_timeout,
	// and a cancelled context would cut that budget to nothing exactly when the signal
	// that started the drain is the thing that cancelled it.
	drainCtx := context.WithoutCancel(ctx)
	go func() { drained <- g.server.Drain(drainCtx) }()
	completed, err := awaitDrain(drained, signals, g.log)

	// The consumer stops either way, and before close touches the hub: Hub.Close must not
	// race a Dispatch (docs/09-internals.md §8). It is immediate — the workers select on
	// the context — so it costs an abandoned drain nothing.
	stopConsumer()
	<-consumerDone

	if !completed {
		return exitFailure
	}
	if err != nil {
		g.log.Error("drain did not complete within server.drain_timeout", "err", err)
		code = exitFailure
	}

	shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), adminShutdownTimeout)
	defer cancel()
	if err := g.admin.Shutdown(shutdownCtx); err != nil {
		// coverage: Shutdown fails only when an admin request outlives
		// adminShutdownTimeout. It is logged and the exit proceeds, because a rolling
		// deploy held up by somebody's curl is worse than a truncated response.
		g.log.Warn("admin listener did not shut down cleanly", "err", err)
	}
	g.log.Info("sidecartunnel stopped", "exit", code)
	return code
}

// waitForStop blocks until the gateway should begin draining, and reports a listener
// failure as an error. A cancelled context and a signal are both clean stops.
func waitForStop(ctx context.Context, signals <-chan os.Signal, fatal <-chan error, log *slog.Logger) error {
	select {
	case <-ctx.Done():
		log.Info("context cancelled; draining", "err", ctx.Err())
		return nil
	case sig := <-signals:
		// FR-19: SIGTERM stops accepting and closes every connection with 3000 and
		// reconnect true, so clients apply their backoff instead of treating an abrupt
		// TCP reset as a blip and retrying immediately.
		log.Info("signal received; draining", "signal", sig.String())
		return nil
	case err := <-fatal:
		return err
	}
}

// awaitDrain waits for the drain to finish or for a second signal to abandon it. It
// reports whether the drain completed at all, and the drain's own error.
//
// It is a function rather than an inline select so that both outcomes are assertable
// without a live connection that has to be made to outlast a timeout
// (docs/14-coding-standards.md §2).
func awaitDrain(drained <-chan error, signals <-chan os.Signal, log *slog.Logger) (bool, error) {
	select {
	case err := <-drained:
		return true, err
	case sig := <-signals:
		log.Warn("second signal; exiting without completing the drain", "signal", sig.String())
		return false, nil
	}
}

// close releases everything build acquired, in the reverse order it was acquired, and
// tolerates a partially built graph so that a failed build cleans up after itself.
//
// The hub is closed before the bus: the hub's reconciler calls Bus.Sync, and a bus closed
// underneath it would turn a clean shutdown into a logged error on the way out.
func (g *gateway) close() {
	if g.adminLn != nil {
		if err := g.adminLn.Close(); err != nil {
			// coverage: the listener has already been closed by admin.Shutdown on every
			// path but a failed build. Closing twice is the expected error and there is
			// nothing to do about either.
			g.log.Debug("close admin listener", "err", err)
		}
	}
	if g.clientLn != nil {
		if err := g.clientLn.Close(); err != nil {
			// coverage: as above — Server.Drain has already closed it.
			g.log.Debug("close client listener", "err", err)
		}
	}
	if g.hub != nil {
		g.hub.Close()
	}
	if g.bus != nil {
		if err := g.bus.Close(); err != nil {
			// coverage: reached only when the transport is already gone, which is the
			// case this shutdown is indifferent to.
			g.log.Warn("close bus", "err", err)
		}
	}
}
