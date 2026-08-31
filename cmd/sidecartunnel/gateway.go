package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"

	"github.com/raghulj/sidecartunnel/internal/bus"
	"github.com/raghulj/sidecartunnel/internal/config"
	"github.com/raghulj/sidecartunnel/internal/consumer"
	"github.com/raghulj/sidecartunnel/internal/hub"
	"github.com/raghulj/sidecartunnel/internal/server"
	"github.com/raghulj/sidecartunnel/internal/webhook"
)

// memoryBusWarning is logged, at warn, on every start with bus.kind: memory.
//
// It is loud because the failure it describes is invisible. A memory bus delivers a
// publish to this process's own subscribers and to nobody else, so two replicas behind
// one load balancer produce messages that arrive for some users and not others, with
// nothing in any log to distinguish it from an application that did not publish
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

	bus     bus.HealthReporter
	hub     *hub.Hub
	webhook *webhook.Client
	server  *server.Server

	// cons is the bus consumer: the loop joining bus.Receive to hub.Dispatch, the
	// control-channel routing, and the FR-23 signature check. It lives in
	// internal/consumer rather than here, so that the integration suite runs the same
	// code this binary does rather than an equivalent of its own.
	cons *consumer.Consumer

	// clientLn is bound by build, before anything starts. A bind failure is a startup
	// error and never a listener that silently is not there: a gateway that runs and
	// accepts nothing is the failure NFR-5 exists to prevent.
	//
	// There is one listener. GET /health and GET /ready are served from it alongside the
	// websocket endpoint; the separate loopback listener they used to have is gone with
	// the operator API it was protecting (docs/12-roadmap.md §2).
	clientLn net.Listener
}

// build wires the graph: bus, hub, connect webhook client, the listener, and the bus
// consumer that feeds the hub.
//
// ctx bounds the hub's background goroutines. Every error closes whatever was already
// built, so a failed build leaks neither a goroutine nor a bound port (NFR-3).
func build(ctx context.Context, cfg *config.Config, log *slog.Logger) (*gateway, error) {
	g := &gateway{cfg: cfg, log: log}
	built := false
	defer func() {
		if !built {
			g.close()
		}
	}()

	var err error
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
	// that this process can flush its cache on a control disconnect.
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
		Bus:     g.bus,
		Webhook: g.webhook,
		Log:     log,
	}); err != nil {
		return nil, err
	}

	if g.clientLn, err = net.Listen("tcp", cfg.Server.Listen); err != nil {
		return nil, fmt.Errorf("listen on server.listen %q: %w", cfg.Server.Listen, err)
	}

	g.cons = consumer.New(consumer.Options{
		Bus:            g.bus,
		Hub:            g.hub,
		Log:            log,
		Secret:         []byte(cfg.Control.Secret),
		Workers:        cfg.Bus.DispatchWorkers,
		MaxMessageSize: cfg.Limits.MaxMessageSize,
		Flush:          g.webhook.Flush,
	})

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
// server.drain_timeout, stop the bus consumer, and let close unsubscribe and release the
// transport.
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
		g.cons.Run(consumerCtx)
	}()

	fatal := make(chan error, 1)
	go func() {
		if err := g.server.Serve(g.clientLn); err != nil && !errors.Is(err, http.ErrServerClosed) {
			fatal <- fmt.Errorf("client listener on server.listen %q: %w", g.cfg.Server.Listen, err)
		}
	}()

	g.log.Info("sidecartunnel started",
		"version", version, "commit", commit, "built", date,
		"server.listen", g.clientLn.Addr().String(), "server.path", g.cfg.Server.Path,
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
	//
	// It is a separate call from Drain, and it has to be. /health and /ready are served
	// from the same listener the websockets are, and Drain shuts that listener down — a
	// load balancer probing it after that gets a refused connection rather than an honest
	// 503, at the exact moment the point of the exercise is to be told honestly.
	g.server.StopAccepting()

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
	if g.clientLn != nil {
		if err := g.clientLn.Close(); err != nil {
			// coverage: Server.Drain has already closed it on every path but a failed
			// build. Closing twice is the expected error and there is nothing to do
			// about either.
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
