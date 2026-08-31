package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/raghulj/sidecartunnel/internal/bus"
	"github.com/raghulj/sidecartunnel/internal/hub"
)

// DefaultControlQueue is the depth of the queue between the dispatch workers and the
// control goroutine. Control messages are rare — a revocation, a refresh, an unsubscribe
// — so the depth exists only so that a worker never waits on the control goroutine
// finishing one.
const DefaultControlQueue = 64

// Options configure a Consumer. Every field has a usable default except Bus and Hub,
// which are what there is to consume and what to consume it into.
type Options struct {
	// Bus is the transport to drain. Receive's channel is read by Workers goroutines.
	Bus bus.Bus

	// Hub is the local registry every message is delivered through.
	Hub *hub.Hub

	// Log receives one line per drop and per applied control message. Default: a logger
	// that discards, so a caller that forgot one gets silence rather than a panic.
	Log *slog.Logger

	// Secret is control.secret. It is the only secret this package holds, and it never
	// reaches a log line (FR-23, NFR-7).
	Secret []byte

	// Workers is bus.dispatch_workers. Below one it is raised to one: a consumer with no
	// workers is a gateway that accepts connections, reports healthy and delivers
	// nothing.
	Workers int

	// MaxMessageSize is limits.max_message_size in bytes. A larger published envelope is
	// dropped and logged once with its channel and reason "oversize" (FR-14). Zero or
	// below leaves the cap off.
	MaxMessageSize int

	// Flush discards every cached webhook answer. It runs on a control disconnect,
	// because a cached entry otherwise survives a revocation and a suspended user
	// reconnecting inside app.cache_ttl gets their pre-revocation grants back
	// (docs/08-config.md §3, docs/13-review-findings.md C4). Default: a no-op.
	Flush func()

	// Now is the clock the ±Skew control window is measured against. It is an option so a
	// test can assert the boundary exactly instead of sleeping through it
	// (docs/14-coding-standards.md §2). Default time.Now.
	Now func() time.Time
}

// Stats is a snapshot of the drop accounting, taken without stopping anything.
//
// It is what separates "the bus never sent it" from "the gateway never delivered it",
// which is the whole diagnosis during a broadcast burst (docs/13-review-findings.md M8).
type Stats struct {
	// Dispatched is the number of messages the hub accepted for fan-out.
	Dispatched int64

	// DroppedMalformed is the number of published envelopes the hub refused: not an
	// object, no event, no data (docs/04-integration.md §2.2).
	DroppedMalformed int64

	// DroppedOversize is the number of published envelopes larger than
	// limits.max_message_size (FR-14).
	DroppedOversize int64

	// ControlApplied is the number of control messages that verified and were applied.
	ControlApplied int64

	// ControlRejected is the total of the three counters below — every control message
	// that was dropped rather than applied (FR-23).
	ControlRejected int64

	// ControlUnsigned is the number of control messages whose signature was absent or
	// did not verify.
	ControlUnsigned int64

	// ControlStale is the number of control messages outside the ±Skew window.
	ControlStale int64

	// ControlMalformed is the number of control messages that verified but named no
	// target, named an action that does not exist, or did not decode.
	ControlMalformed int64
}

// Consumer is the fan-out path's top half: it drains the bus's intake channel on
// bus.dispatch_workers goroutines and hands each message to the hub
// (docs/09-internals.md §5).
//
// The split between the bus's own reader and these workers is the whole of M8. The reader
// does nothing but drain the socket into the bounded intake channel; a single goroutine
// that decoded and fanned out to 10,000 connections between socket reads would fall
// behind during a broadcast burst, be evicted by Redis's client-output-buffer-limit,
// reconnect, resubscribe and be immediately behind again — a stable oscillation that
// presents as /ready's bus_reconnects climbing against a perfectly healthy Redis.
//
// The control channel is consumed on its own goroutine, so a revocation cannot queue
// behind the firehose it may exist to stop.
type Consumer struct {
	bus bus.Bus
	hub *hub.Hub
	log *slog.Logger

	secret     []byte
	flush      func()
	workers    int
	maxMessage int
	now        func() time.Time

	controlq chan bus.Message

	dispatched atomic.Int64
	malformed  atomic.Int64
	oversize   atomic.Int64

	applied          atomic.Int64
	unsigned         atomic.Int64
	stale            atomic.Int64
	controlMalformed atomic.Int64
}

// New builds the fan-out top half from opts, applying the documented default for every
// field that has one. It starts nothing; call Run.
func New(opts Options) *Consumer {
	if opts.Workers < 1 {
		opts.Workers = 1
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	if opts.Flush == nil {
		opts.Flush = func() {}
	}
	if opts.Log == nil {
		opts.Log = slog.New(slog.DiscardHandler)
	}
	return &Consumer{
		bus:        opts.Bus,
		hub:        opts.Hub,
		log:        opts.Log,
		secret:     opts.Secret,
		flush:      opts.Flush,
		workers:    opts.Workers,
		maxMessage: opts.MaxMessageSize,
		now:        opts.Now,
		controlq:   make(chan bus.Message, DefaultControlQueue),
	}
}

// Stats returns a snapshot of the drop accounting. It is safe to call at any time from
// any goroutine; the counters are read independently, so a snapshot taken mid-message is
// consistent with itself only to the message.
func (c *Consumer) Stats() Stats {
	unsigned, stale, malformed := c.unsigned.Load(), c.stale.Load(), c.controlMalformed.Load()
	return Stats{
		Dispatched:       c.dispatched.Load(),
		DroppedMalformed: c.malformed.Load(),
		DroppedOversize:  c.oversize.Load(),
		ControlApplied:   c.applied.Load(),
		ControlRejected:  unsigned + stale + malformed,
		ControlUnsigned:  unsigned,
		ControlStale:     stale,
		ControlMalformed: malformed,
	}
}

// Run starts the dispatch workers and the control goroutine and blocks until ctx is
// cancelled or the bus closes its intake channel.
//
// It returns only once every goroutine it started has exited, so the caller may close the
// hub straight afterwards: Hub.Close must not race a Dispatch, exactly as accepting stops
// before draining (docs/09-internals.md §8).
//
// It may be called once per Consumer. The workers own the control queue's lifetime and
// close it on the way out, which is what lets Run return when the bus closes without
// waiting for a context that a caller may never cancel.
func (c *Consumer) Run(ctx context.Context) {
	in := c.bus.Receive()

	var control sync.WaitGroup
	control.Add(1)
	go func() {
		defer control.Done()
		c.control(ctx)
	}()

	var workers sync.WaitGroup
	workers.Add(c.workers)
	for range c.workers {
		go func() {
			defer workers.Done()
			c.dispatch(ctx, in)
		}()
	}

	workers.Wait()
	close(c.controlq)
	control.Wait()
}

// dispatch is one worker: decode, fan out, count. It returns when ctx ends or the bus
// closes in.
//
// A message for the control key is handed to the control goroutine rather than applied
// here. The send blocks if that goroutine is busy, and it is allowed to: dropping a
// revocation because a queue was full is the one message loss that cannot be tolerated,
// and the alternative — a non-blocking send — would silently discard it.
func (c *Consumer) dispatch(ctx context.Context, in <-chan bus.Message) {
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

// deliver hands one message to the hub, and counts it either way.
//
// A dropped envelope is logged with its channel and the reason, never with its payload:
// an envelope that failed to decode is still an application's data, and the log is the
// wrong place for it (docs/04-integration.md §2.2, NFR-7). The reason goes through
// safeError, because encoding/json's own message quotes the byte it choked on and that
// byte belongs to the publisher — the claim in this comment was false until it did.
//
// The oversize check is FR-14 and it is here rather than in the hub because the hub is
// handed a message that has already been accepted: this is the one place that knows
// limits.max_message_size and the last one that can decline the whole envelope.
func (c *Consumer) deliver(msg bus.Message) {
	if c.maxMessage > 0 && len(msg.Payload) > c.maxMessage {
		c.oversize.Add(1)
		c.log.Warn("bus message dropped", "channel", msg.Channel, "reason", string(ReasonOversize),
			"bytes", len(msg.Payload), "limit", c.maxMessage)
		return
	}
	if err := c.hub.Dispatch(msg); err != nil {
		c.malformed.Add(1)
		c.log.Debug("bus message dropped", "channel", msg.Channel, "reason", string(ReasonMalformed),
			"err", safeError(err))
		return
	}
	c.dispatched.Add(1)
}

// safeError renders an error for a log line with no byte of the message in it (NFR-7).
//
// encoding/json's *json.SyntaxError reads `invalid character 'Z' looking for beginning of
// value`, where Z is one byte of whatever was published: a publisher's payload on the
// fan-out path, and anything at all on the control channel, which is unauthenticated
// until its signature is checked. The offset says where without saying what, which is the
// part an operator needs — it separates "not JSON at all" from "no event field", and
// those have different causes.
//
// It replaces the whole chain rather than the leaf, because by the time the error reaches
// here the leaf's text has already been formatted into every wrapper above it. Nothing
// useful is lost: the channel is a separate field on the same line.
//
// Only *json.SyntaxError carries a byte of the document. *json.UnmarshalTypeError names a
// JSON type and one of our own field names, and is left alone.
func safeError(err error) string {
	var syntax *json.SyntaxError
	if errors.As(err, &syntax) {
		return fmt.Sprintf("not valid JSON: syntax error at byte offset %d", syntax.Offset)
	}
	return err.Error()
}

// control verifies and applies control messages on its own goroutine (FR-23).
//
// It ends on a cancelled context, or when the workers close the queue on their way out.
// Both are shutdowns, and neither leaves a goroutine spinning on a receive that always
// succeeds.
func (c *Consumer) control(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-c.controlq:
			if !ok {
				return
			}
			c.apply(msg)
		}
	}
}

// apply authenticates one control message and hands it to the hub.
//
// Every rejection is logged at warn, with its reason and without the payload. An unsigned
// message is somebody publishing to the control channel who should not be; a stale one is
// usually this replica's clock. Neither is applied partially.
func (c *Consumer) apply(msg bus.Message) {
	cmd, reason, err := Verify(c.secret, c.now(), msg.Payload)
	if err != nil {
		c.count(reason)
		c.log.Warn("control message rejected", "reason", string(reason), "err", safeError(err))
		return
	}
	if err := c.hub.Control(cmd); err != nil {
		// coverage: Verify has already run hub.ParseControl over the same bytes, which is
		// the same validation Hub.Control repeats on a message built by hand. It is
		// checked rather than discarded so that a future action added to one and not the
		// other is logged instead of silently ignored.
		c.count(ReasonMalformed)
		c.log.Warn("control message refused by the hub", "reason", string(ReasonMalformed), "err", err)
		return
	}
	c.applied.Add(1)
	if cmd.Action == hub.ActionDisconnect {
		c.flush()
	}
	c.log.Info("control applied", "action", cmd.Action, "user", cmd.User, "client", cmd.Client)
}

// count accounts one control rejection to the counter its reason names, so an operator
// alerting on unsigned messages is not woken by a replica whose clock has drifted.
func (c *Consumer) count(reason Reason) {
	switch reason {
	case ReasonUnsigned:
		c.unsigned.Add(1)
	case ReasonStale:
		c.stale.Add(1)
	default:
		c.controlMalformed.Add(1)
	}
}
