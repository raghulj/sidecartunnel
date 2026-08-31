package main

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/raghulj/sidecartunnel/internal/bus"
	"github.com/raghulj/sidecartunnel/internal/hub"
	"github.com/raghulj/sidecartunnel/internal/metrics"
)

// controlQueue is the depth of the queue between the dispatch workers and the control
// goroutine. Control messages are rare — a revocation, a refresh, an unsubscribe — so the
// depth exists only so that a worker never waits on the control goroutine finishing one.
const controlQueue = 64

// consumer is the fan-out path's top half: it drains the bus's intake channel on
// bus.dispatch_workers goroutines and hands each message to the hub
// (docs/09-internals.md §5).
//
// The split between the bus's own reader and these workers is the whole of M8. The reader
// does nothing but drain the socket into the bounded intake channel; a single goroutine
// that decoded and fanned out to 10,000 connections between socket reads would fall
// behind during a broadcast burst, be evicted by Redis's client-output-buffer-limit,
// reconnect, resubscribe and be immediately behind again — a stable oscillation that
// presents as st_bus_reconnects_total climbing against a perfectly healthy Redis.
//
// The control channel is consumed on its own goroutine, so a revocation cannot queue
// behind the firehose it may exist to stop.
type consumer struct {
	bus     bus.Bus
	hub     *hub.Hub
	metrics *metrics.Metrics
	log     *slog.Logger

	// secret is control.secret. It is the only secret this process holds outside the
	// webhook client, and it never reaches a log line (NFR-7).
	secret []byte

	// prefix is bus.prefix, used to recover the bare channel name for the namespace
	// metric label. The hub's own keys stay prefixed (FR-21).
	prefix string

	// flush discards every cached webhook answer. It runs on a control disconnect,
	// because a cached entry otherwise survives a revocation and a suspended user
	// reconnecting inside app.cache_ttl gets their pre-revocation grants back
	// (docs/08-config.md §3, docs/13-review-findings.md C4).
	flush func()

	workers int

	// now is the clock the control window is measured against. It is a field so a test
	// can assert the ±300s boundary exactly instead of sleeping through it
	// (docs/14-coding-standards.md §7).
	now func() time.Time

	controlq chan bus.Message
}

// newConsumer builds the fan-out top half. workers below one is raised to one: a consumer
// with no workers is a gateway that accepts connections, reports healthy and delivers
// nothing.
func newConsumer(b bus.Bus, h *hub.Hub, m *metrics.Metrics, log *slog.Logger, secret []byte, prefix string, workers int, flush func(), now func() time.Time) *consumer {
	if workers < 1 {
		workers = 1
	}
	if now == nil {
		now = time.Now
	}
	if flush == nil {
		flush = func() {}
	}
	return &consumer{
		bus:      b,
		hub:      h,
		metrics:  m,
		log:      log,
		secret:   secret,
		prefix:   prefix,
		flush:    flush,
		workers:  workers,
		now:      now,
		controlq: make(chan bus.Message, controlQueue),
	}
}

// run starts the dispatch workers and the control goroutine and blocks until ctx is
// cancelled or the bus closes its intake channel.
//
// It returns only once every goroutine it started has exited, so the caller may close the
// hub straight afterwards: Hub.Close must not race a Dispatch, exactly as accepting stops
// before draining (docs/09-internals.md §8).
func (c *consumer) run(ctx context.Context) {
	in := c.bus.Receive()

	var wg sync.WaitGroup
	wg.Add(c.workers + 1)
	go func() {
		defer wg.Done()
		c.control(ctx)
	}()
	for range c.workers {
		go func() {
			defer wg.Done()
			c.dispatch(ctx, in)
		}()
	}
	wg.Wait()
}

// dispatch is one worker: decode, fan out, count. It returns when ctx ends or the bus
// closes in.
//
// A message for the control key is handed to the control goroutine rather than applied
// here. The send blocks if that goroutine is busy, and it is allowed to: dropping a
// revocation because a queue was full is the one message loss that cannot be tolerated,
// and the alternative — a non-blocking send — would silently discard it.
func (c *consumer) dispatch(ctx context.Context, in <-chan bus.Message) {
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-in:
			if !ok {
				return
			}
			if msg.Channel == c.hub.ControlKey() {
				select {
				case c.controlq <- msg:
				case <-ctx.Done():
					return
				}
				continue
			}
			c.deliver(msg)
		}
	}
}

// deliver hands one message to the hub and records it.
//
// A malformed envelope is dropped and counted, never logged with its payload: the whole
// point of the drop counter is that the payload must not reach the log
// (docs/04-integration.md §2.2, NFR-7).
func (c *consumer) deliver(msg bus.Message) {
	ns := c.metrics.Namespace(strings.TrimPrefix(msg.Channel, c.prefix))
	c.metrics.MessagePublished(ns)
	if err := c.hub.Dispatch(msg); err != nil {
		c.metrics.MessageDropped(metrics.DropMalformed)
		c.log.Debug("bus.dispatch dropped a message", "channel", msg.Channel, "err", err)
	}
}

// control verifies and applies control messages on its own goroutine (FR-23).
//
// Every rejection is counted by reason and logged at warn without the payload. An
// unsigned message is somebody publishing to the control channel who should not be; a
// stale one is usually this replica's clock. Neither is applied partially.
func (c *consumer) control(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-c.controlq:
			if !ok {
				// coverage: controlq is never closed — only the process exits — and the
				// branch exists so a future change cannot turn a closed queue into a
				// goroutine spinning on a receive that always succeeds.
				return
			}
			c.apply(msg)
		}
	}
}

// apply authenticates one control message and hands it to the hub.
func (c *consumer) apply(msg bus.Message) {
	cmd, reason, err := verifyControl(c.secret, c.now(), msg.Payload)
	if err != nil {
		c.metrics.ControlRejected(reason)
		c.log.Warn("control message rejected", "reason", string(reason), "err", err)
		return
	}
	if err := c.hub.Control(cmd); err != nil {
		// coverage: verifyControl has already run hub.ParseControl over the same bytes,
		// which is the same validation Hub.Control repeats on a message built by hand.
		// It is checked rather than discarded so that a future action added to one and
		// not the other is counted instead of silently ignored.
		c.metrics.ControlRejected(metrics.ControlMalformed)
		c.log.Warn("control message refused by the hub", "err", err)
		return
	}
	if cmd.Action == hub.ActionDisconnect {
		c.flush()
	}
	c.log.Info("control applied", "action", cmd.Action, "user", cmd.User, "client", cmd.Client)
}
