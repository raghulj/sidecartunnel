package conn

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"

	"github.com/raghulj/sidecartunnel/internal/glob"
	"github.com/raghulj/sidecartunnel/internal/proto"
)

// Defaults for the Options a caller leaves zero. Each mirrors the documented default of
// the configuration key it comes from (docs/08-config.md §3), so a Conn built with only
// its four required seams behaves like a configured one.
const (
	defaultHandshakeTimeout = 5 * time.Second
	defaultConnectTimeout   = 5 * time.Second
	defaultPingInterval     = 25 * time.Second
	defaultPongTimeout      = 10 * time.Second
	defaultWriteTimeout     = 10 * time.Second
	defaultRetrySpread      = 60 * time.Second
	defaultOutboundQueue    = 256
	defaultMaxFrameSize     = 16384
	defaultMaxChannelLength = 255
	defaultMaxSubscriptions = 500
)

// reservedPrefix marks a control channel. It is refused before grants are consulted, so
// that a grant of "*" still cannot reach one (docs/06-channels.md §4).
const reservedPrefix = "_"

// maxCloseReason is the longest reason that fits a websocket close frame: RFC 6455 caps a
// control frame payload at 125 bytes and the close code takes the first two.
const maxCloseReason = 123

// Options configures one connection. Socket, Registry and Authorizer are required;
// everything else has a documented default.
type Options struct {
	// ID is the client id. Left empty, one is generated: 16 hex characters from
	// crypto/rand, per hub.Sink's contract.
	ID string

	// Socket is the websocket connection to drive.
	Socket Socket

	// Registry is the subscription bookkeeping — internal/hub in production.
	Registry Registry

	// Authorizer answers the connect webhook for this connection. It closes over the
	// request's cookie, so a Conn holds the reference only until the connect frame has
	// used it and then drops it: nothing reachable from a live connection may hold a
	// cookie (FR-22). It is called at most once.
	Authorizer Authorizer

	// Clock is the time source. Defaults to SystemClock.
	Clock Clock

	// Log receives connection events. Defaults to a discard logger rather than
	// slog.Default, because docs/14-coding-standards.md §7 forbids reaching for a global.
	Log *slog.Logger

	// HandshakeTimeout bounds receipt of the connect frame, and nothing else (FR-4).
	// Default 5s.
	HandshakeTimeout time.Duration

	// ConnectTimeout is the authorization budget, app.connect_timeout. It is deliberately
	// separate from HandshakeTimeout: conflating them turns a slow application into a
	// permanent, non-retryable lockout of every reconnecting user. Default 5s.
	ConnectTimeout time.Duration

	// PingInterval is the gap between protocol-level pings (FR-7). Default 25s.
	PingInterval time.Duration

	// PongTimeout is how long a pong may take before the connection closes with
	// proto.ClosePingTimeout (FR-7). Default 10s.
	PongTimeout time.Duration

	// WriteTimeout bounds one socket write. Default 10s. Without it a peer that stops
	// reading parks the writer goroutine forever and the connection leaks (NFR-3).
	WriteTimeout time.Duration

	// RetrySpread is the window retry_after is spread across for the close codes
	// docs/03-client-protocol.md §7 marks "spread". Default 60s, matching
	// server.drain_spread.
	RetrySpread time.Duration

	// RetryAfter overrides the retry_after for a close code. The default spreads each
	// connection deterministically across RetrySpread by hashing its client id; a server
	// that knows how many connections it is dropping can supply a better one (§7.1).
	RetryAfter func(code proto.CloseCode) time.Duration

	// OutboundQueue is the outbound queue depth, limits.outbound_queue. Default 256. It
	// holds pointers to one shared immutable buffer, so the depth costs 256 × 16 bytes
	// ≈ 4 KiB per connection rather than 256 × 32 KiB, which was a factor-of-200 error
	// against a 1 GiB budget (docs/13-review-findings.md M10).
	OutboundQueue int

	// MaxFrameSize caps an inbound frame, limits.max_frame_size. Default 16384. An
	// oversize frame closes with proto.CloseProtocolError.
	MaxFrameSize int

	// MaxChannelLength caps a channel name, limits.max_channel_length. Default 255.
	MaxChannelLength int

	// MaxSubscriptions caps the channels a connect frame may ask for,
	// limits.max_subscriptions_per_conn. Default 500.
	MaxSubscriptions int
}

// Conn is one client connection: its two goroutines, its outbound queue, its grants and
// its lifecycle. It implements Sink, and therefore hub.Sink.
//
// # Goroutines
//
// Exactly two, and Run owns both (docs/09-internals.md §3). The reader is the caller's
// own goroutine: it blocks in ReadMessage, parses one frame and handles the command
// inline, because grant matching is in-memory and a queue would only add latency. The
// writer is spawned by Run: it drains the outbound queue, sends protocol pings on a
// ticker, and owns every timer. **The writer is the only goroutine that ever writes to or
// closes the socket**, which is what makes concurrent publishes safe without a write
// mutex — a mutex would mean two writers exist and one of them would eventually
// interleave a partial frame.
//
// # Locking
//
// A Conn holds **no mutex at all**, which is the strongest available answer to the lock
// ordering rule in docs/09-internals.md §4.4 and docs/13-review-findings.md M3: hub
// before conn, never the reverse. There is no conn lock to invert. Its mutable state is
// four atomics — grants, user, the closed flag and the single-use Authorizer — and its
// channels; the subscription set that §2 sketches on the connection lives in the
// Registry, whose lock is the one place it moves (docs/09-internals.md §4.2). A Conn never holds anything while sending
// on a channel, and the only lock it is ever under is the Registry's, taken inside the
// Registry methods it calls and never held across one of its own.
//
// # Grants
//
// grants is an atomic.Pointer to an immutable glob.Set. Matching is a lock-free load;
// changing the grants swaps the pointer. A mutex here would violate FR-9, and a plainly
// assigned slice tears under -race the first time a revalidation swaps it, which was the
// actual defect in docs/13-review-findings.md M2.
type Conn struct {
	id       string
	log      *slog.Logger
	sock     Socket
	registry Registry
	clock    Clock

	// auth is the Authorizer, held only until the connect frame has used it.
	//
	// It is a pointer that is swapped for nil rather than a plain field, because the
	// Authorizer closes over the request's Cookie header and a Conn outlives the connect
	// call by app.expires_in — 6h by default. FR-22 says the gateway must not retain the
	// cookie beyond the connect call, and the only way to mean that structurally is for
	// the last reference to it to be gone before doConnect returns. At 20,000
	// connections holding a 1–4 KB session cookie each, the difference is whether a core
	// dump of this process yields 20,000 replayable sessions
	// (docs/13-review-findings.md S3).
	//
	// The swap is also the once-guard: a connect frame that finds it already taken is
	// "already connected", so the drop cannot be removed without failing that rule's own
	// test (docs/03-client-protocol.md §4.1).
	auth atomic.Pointer[Authorizer]

	handshakeTimeout time.Duration
	connectTimeout   time.Duration
	pingInterval     time.Duration
	pongTimeout      time.Duration
	writeTimeout     time.Duration
	retrySpread      time.Duration
	retryAfterFn     func(proto.CloseCode) time.Duration
	maxFrameSize     int
	maxChannelLength int
	maxSubscriptions int

	// out is the bounded outbound queue. It holds pointers into one shared immutable
	// buffer per message, never copies (M10). Sends into it are always non-blocking.
	out chan *proto.Frame

	// expires carries the grant lifetime from the reader to the writer, which owns every
	// timer. Capacity 1 and written at most once, so the send cannot block (FR-22).
	expires chan time.Duration

	// closing is closed by Close and is the writer's signal to shut the socket down. It
	// is a channel rather than a flag because the writer selects on it.
	closing chan struct{}

	// writerDone is closed by the writer goroutine as it exits. Run waits on it, which is
	// what makes "no goroutine leaks" observable rather than hoped for (NFR-3).
	writerDone chan struct{}

	// pongs counts protocol-level pongs. The reader increments it from the pong handler
	// and the writer compares it against the value it snapshotted when it sent the ping;
	// a counter rather than a channel means neither goroutine can miss the other's
	// signal, and the pong deadline needs no handshake to disarm (FR-7).
	pongs atomic.Uint64

	// connectSeen is set by the reader the instant a connect frame arrives, before
	// authorization begins. The writer reads it when the handshake alarm fires. FR-4:
	// the timer covers receipt of the frame and nothing after it.
	connectSeen atomic.Bool

	user        atomic.Pointer[string]
	grants      atomic.Pointer[glob.Set]
	closed      atomic.Bool
	closeCode   atomic.Int64
	closeReason atomic.Pointer[string]
	closeOnce   sync.Once
}

// New builds a connection. It does not start any goroutine; Run does.
//
// It returns an error naming the missing field when Socket, Registry or Authorizer is
// nil, and when a client id cannot be generated. Every other option is defaulted.
func New(opts Options) (*Conn, error) {
	switch {
	case opts.Socket == nil:
		return nil, fmt.Errorf("conn: Options.Socket is required")
	case opts.Registry == nil:
		return nil, fmt.Errorf("conn: Options.Registry is required")
	case opts.Authorizer == nil:
		return nil, fmt.Errorf("conn: Options.Authorizer is required")
	}

	id := opts.ID
	if id == "" {
		generated, err := newID()
		if err != nil {
			// coverage: unreachable while crypto/rand.Read cannot fail; see newID.
			return nil, err
		}
		id = generated
	}

	c := &Conn{
		id:               id,
		log:              orDefault(opts.Log, slog.New(slog.DiscardHandler)),
		sock:             opts.Socket,
		registry:         opts.Registry,
		clock:            orDefault(opts.Clock, SystemClock()),
		handshakeTimeout: positive(opts.HandshakeTimeout, defaultHandshakeTimeout),
		connectTimeout:   positive(opts.ConnectTimeout, defaultConnectTimeout),
		pingInterval:     positive(opts.PingInterval, defaultPingInterval),
		pongTimeout:      positive(opts.PongTimeout, defaultPongTimeout),
		writeTimeout:     positive(opts.WriteTimeout, defaultWriteTimeout),
		retrySpread:      positive(opts.RetrySpread, defaultRetrySpread),
		retryAfterFn:     opts.RetryAfter,
		maxFrameSize:     positive(opts.MaxFrameSize, defaultMaxFrameSize),
		maxChannelLength: positive(opts.MaxChannelLength, defaultMaxChannelLength),
		maxSubscriptions: positive(opts.MaxSubscriptions, defaultMaxSubscriptions),
		out:              make(chan *proto.Frame, positive(opts.OutboundQueue, defaultOutboundQueue)),
		expires:          make(chan time.Duration, 1),
		closing:          make(chan struct{}),
		writerDone:       make(chan struct{}),
	}
	c.auth.Store(&opts.Authorizer)
	return c, nil
}

// takeAuthorizer returns the Authorizer and drops the connection's reference to it, or
// nil if it has already been taken.
//
// FR-22 in one line: the Authorizer closes over the cookie, so the connection holds it
// for exactly as long as it takes to make one call. The returned value goes out of scope
// when doConnect returns, and nothing that outlives the connect call can reach it.
func (c *Conn) takeAuthorizer() Authorizer {
	if a := c.auth.Swap(nil); a != nil {
		return *a
	}
	return nil
}

// newID returns 16 hex characters from crypto/rand.
func newID() (string, error) {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		// coverage: crypto/rand.Read is documented never to fail — it fills b entirely or
		// the process cannot continue — so there is no fault to inject. The branch stays
		// because ignoring the error would be a blank assignment on a security primitive.
		return "", fmt.Errorf("conn: generate client id: %w", err)
	}
	return hex.EncodeToString(b[:]), nil
}

// orDefault returns v unless it is the zero pointer or interface, in which case it
// returns fallback.
func orDefault[T comparable](v, fallback T) T {
	var zero T
	if v == zero {
		return fallback
	}
	return v
}

// positive returns v when it is above zero, and fallback otherwise.
func positive[T int | time.Duration](v, fallback T) T {
	if v > 0 {
		return v
	}
	return fallback
}

// ID returns the client id: 16 hex characters, stable for the connection's life and safe
// to read after close.
func (c *Conn) ID() string { return c.id }

// User returns the opaque user id the connect webhook supplied, or "" before the webhook
// has answered. It is fixed once set, including after close, because a connection whose
// user could change underneath a revocation sweep is one that can dodge one (FR-18).
func (c *Conn) User() string {
	if u := c.user.Load(); u != nil {
		return *u
	}
	return ""
}

// SetGrants replaces the connection's grant set.
//
// The set is immutable and the pointer is swapped, never mutated, so a reader matching a
// channel at the same instant sees either the old set or the new one and never a torn
// one. Safe to call from any goroutine (FR-9, docs/13-review-findings.md M2).
func (c *Conn) SetGrants(set glob.Set) { c.grants.Store(&set) }

// Allows reports whether channel matches this connection's grants.
//
// It is a lock-free atomic load and an in-memory match: no I/O, no mutex, nothing that
// can block the reader goroutine (FR-9). A connection with no grants yet matches nothing,
// which is the safe direction to fail in.
func (c *Conn) Allows(channel string) bool {
	set := c.grants.Load()
	if set == nil {
		return false
	}
	return set.Match(channel)
}

// Send queues one encoded frame for the writer goroutine and reports whether it was
// accepted. It never blocks, and it never panics on a nil frame or a closed connection.
//
// FR-15: false means the outbound queue is full, and nothing else. The caller must not
// retry and must not wait — it collects the refusing sinks, releases its lock, and hands
// them to the closer goroutine, which closes them with proto.CloseSlowConsumer. A
// blocking send here would stall the fan-out goroutine and with it every other subscriber
// on the channel, which is the failure the whole backpressure design exists to prevent
// (docs/07-delivery.md §4).
//
// A closed connection reports true and drops the frame. It is not a slow consumer, it
// needs no closing, and a fan-out that raced a close must not be sent down an error path
// it has nothing to do (hub.Sink).
//
// f is shared with every other recipient of the same message and must not be modified by
// anything downstream (docs/09-internals.md §5).
func (c *Conn) Send(f *proto.Frame) bool {
	if c.closed.Load() {
		// True, not false. False means one thing to every caller — the outbound queue is
		// full, close this connection — and a connection that is already closed is not
		// backpressure. Reporting it as such turns a drain of N connections into N
		// pointless slow-consumer closes of connections that are already gone, and
		// hub.Sink says in as many words that a race between fan-out and close must not
		// be an error path (internal/hub/sink.go, docs/07-delivery.md §4). The frame is
		// dropped, which is what at-most-once delivery to a closed socket means.
		return true
	}
	return c.offer(f)
}

// offer is the non-blocking append, without the closed check. Close uses it so that a
// disconnect frame can still be queued for a connection it has just marked closed.
func (c *Conn) offer(f *proto.Frame) bool {
	if f == nil {
		return false
	}
	select {
	case c.out <- f:
		return true
	default:
		return false
	}
}

// Close ends the connection: it queues a disconnect frame carrying code, its reconnect
// flag and its retry_after if the outbound queue has room, deregisters from the Registry,
// and hands the socket to the writer goroutine to close.
//
// It is guarded by sync.Once and is safe to call from any goroutine at any time —
// expiry, revocation, drain, a ping timeout and a slow-consumer overflow can all decide
// to close the same connection at once. It never blocks and it must never be called
// while holding the Registry's lock, because it deregisters (docs/09-internals.md §4.5).
//
// The disconnect frame is best-effort by design. A slow-consumer close has a full queue
// by definition, so that client sees only the websocket close code — which
// docs/03-client-protocol.md §5.2 covers, since a client with no frame falls back to the
// §7 table.
func (c *Conn) Close(code proto.CloseCode, reason string) { c.doClose(code, reason, true) }

// abort ends the connection without a disconnect frame or a websocket close frame. It is
// for the case where the peer is already gone — a read error, a failed write — and there
// is nobody left to tell.
func (c *Conn) abort() { c.doClose(0, "", false) }

func (c *Conn) doClose(code proto.CloseCode, reason string, notify bool) {
	c.closeOnce.Do(func() {
		c.closed.Store(true)
		if notify {
			c.closeCode.Store(int64(code))
			c.closeReason.Store(&reason)
			c.offer(c.encode(&proto.DisconnectFrame{Disconnect: &proto.Disconnect{
				Code:       code,
				Reason:     reason,
				Reconnect:  reconnectable(code),
				RetryAfter: c.retryAfter(code).Milliseconds(),
			}}))
		}
		// Closing before deregistering, so that a fan-out already inside the Registry's
		// read lock gets false from Send rather than queueing into a connection that is
		// about to stop draining.
		close(c.closing)
		c.registry.Remove(c)
		c.log.Info("connection closed", "client", c.id, "code", int(code), "reason", reason)
	})
}

// reconnectable reports whether a close code is transient, per the table in
// docs/03-client-protocol.md §7. A client MUST honour false: retrying through a
// revocation turns it into a denial-of-service against the connect webhook.
func reconnectable(code proto.CloseCode) bool {
	switch code {
	case proto.CloseHandshakeTimeout, proto.CloseUnauthorized, proto.CloseProtocolError, proto.CloseRevoked:
		return false
	default:
		return true
	}
}

// retryAfter returns the retry_after for a close code (docs/03-client-protocol.md §7.1).
//
// The spread is derived from the client id rather than from a random source: it is
// uniform across connections, it needs no package-level state, and it is deterministic
// per connection, so the behaviour is testable rather than merely plausible. A server
// that knows how many connections it is dropping can override the whole function.
func (c *Conn) retryAfter(code proto.CloseCode) time.Duration {
	if c.retryAfterFn != nil {
		return c.retryAfterFn(code)
	}
	switch code {
	case proto.CloseDraining, proto.CloseExpired, proto.CloseAuthUnavailable:
		// The window is measured in milliseconds because retry_after is, and it is
		// floored at one: positive() guarantees a positive *duration*, not a positive
		// millisecond count, and 999µs.Milliseconds() is 0. Dividing by that panicked on
		// every close of every connection, so a server.drain_spread under a millisecond
		// turned the first SIGTERM into a crashed replica. The floor is here rather than
		// in the validator because the arithmetic must be safe on its own: a value that
		// reaches this line by another route is still a panic on a connection goroutine.
		spread := max(c.retrySpread.Milliseconds(), 1)
		// The +1 keeps the value positive for the same reason the floor exists: a
		// retry_after of 0 is omitted on the wire and reads to a client as "no guidance",
		// which is the opposite of the instruction.
		//
		// #nosec G115 -- spread is at least 1 by the line above, so uint64(spread) is a
		// positive int64 widened, and the remainder is strictly below it and therefore
		// well inside int64. Neither conversion can overflow.
		slot := int64(fnv1a(c.id) % uint64(spread))
		return time.Duration(slot+1) * time.Millisecond
	case proto.CloseRateLimited:
		// Fixed by docs/03-client-protocol.md §4.4. Without the delay the anti-abuse
		// control amplifies load onto the connect webhook, which is the component least
		// able to absorb it (docs/13-review-findings.md m13).
		return 60 * time.Second
	default:
		return 0
	}
}

// Run drives the connection until it ends, and returns only once both of its goroutines
// have. The caller's goroutine becomes the reader; the writer is spawned here.
//
// Cancelling ctx closes the connection with proto.CloseDraining, so clients apply their
// backoff instead of treating an abrupt reset as a network blip and retrying immediately
// (docs/09-internals.md §8).
//
// Returning only after the writer has exited is what makes NFR-3 assertable: a test that
// waits for Run has waited for every goroutine this connection created.
func (c *Conn) Run(ctx context.Context) {
	go c.write(ctx)
	c.read(ctx)
	<-c.writerDone
}

// write is the writer goroutine: the only goroutine that touches the socket for writing,
// and the owner of every timer on the connection.
func (c *Conn) write(ctx context.Context) {
	defer close(c.writerDone)

	handshake := c.clock.NewTimer(c.handshakeTimeout)
	defer handshake.Stop()
	ping := c.clock.NewTicker(c.pingInterval)
	defer ping.Stop()

	handshakeC := handshake.C()
	pingC := ping.C()
	ctxDone := ctx.Done()
	var expiry, pong Alarm
	var expiryC, pongC <-chan time.Time
	var pongMark uint64
	writable := true

loop:
	for {
		// Closing wins over every other ready branch. Without this the select would pick
		// uniformly among whatever is ready, so which frames the main loop writes and
		// which shutdown drains would differ run to run — and a shutdown path that is
		// only sometimes exercised is a shutdown path that is not tested.
		select {
		case <-c.closing:
			break loop
		default:
		}

		select {
		case <-c.closing:
			break loop

		case <-ctxDone:
			// Nilled so the branch cannot win the select again: Done stays ready once
			// cancelled, and the loop would otherwise spin until closing happened to be
			// chosen instead.
			ctxDone = nil
			c.Close(proto.CloseDraining, "draining")

		case f := <-c.out:
			if !c.writeFrame(f) {
				writable = false
				c.abort()
				break loop
			}

		case <-handshakeC:
			handshakeC = nil
			// FR-4: the timer covers receipt of the connect frame only. A connection
			// waiting on a slow authorization has already set this and closes with
			// proto.CloseAuthUnavailable if the webhook never answers, never with 3001.
			if !c.connectSeen.Load() {
				c.Close(proto.CloseHandshakeTimeout, "handshake timeout")
			}

		case d := <-c.expires:
			expiry = c.clock.NewTimer(d)
			expiryC = expiry.C()

		case <-expiryC:
			expiryC = nil
			c.Close(proto.CloseExpired, "grants expired")

		case <-pingC:
			// Snapshotted before the write, so a pong that arrives while the ping is in
			// flight still counts against this deadline.
			mark := c.pongs.Load()
			if !c.writePing() {
				writable = false
				c.abort()
				break loop
			}
			if pong != nil {
				pong.Stop()
			}
			pongMark = mark
			pong = c.clock.NewTimer(c.pongTimeout)
			pongC = pong.C()

		case <-pongC:
			pongC = nil
			// FR-7: a missed pong within pong_timeout closes with 3004. Comparing counters
			// rather than draining a channel means the two goroutines cannot miss each
			// other's signal.
			if c.pongs.Load() == pongMark {
				c.Close(proto.ClosePingTimeout, "ping timeout")
			}
		}
	}

	if expiry != nil {
		expiry.Stop()
	}
	if pong != nil {
		pong.Stop()
	}
	c.shutdown(writable)
}

// shutdown is the writer's last act: drain whatever is queued, send the websocket close
// frame if there is a code to send, and close the socket. Nothing else in the package
// closes the socket, which is what guarantees the reader unblocks exactly once.
func (c *Conn) shutdown(writable bool) {
	if writable && c.drain() {
		c.writeClose()
	}
	if err := c.sock.Close(); err != nil {
		c.log.Debug("socket close failed", "client", c.id, "err", err)
	}
}

// drain writes everything already queued and reports whether the socket survived. The
// disconnect frame is the last thing Close queues, so draining is what puts it on the
// wire immediately before the close frame (docs/03-client-protocol.md §5.2).
func (c *Conn) drain() bool {
	for {
		select {
		case f := <-c.out:
			if !c.writeFrame(f) {
				return false
			}
		default:
			return true
		}
	}
}

// writeFrame writes one text frame under a deadline, and reports whether it succeeded.
func (c *Conn) writeFrame(f *proto.Frame) bool {
	if err := c.sock.SetWriteDeadline(c.clock.Now().Add(c.writeTimeout)); err != nil {
		c.log.Debug("set write deadline failed", "client", c.id, "err", err)
		return false
	}
	if err := c.sock.WriteMessage(websocket.TextMessage, f.Data); err != nil {
		c.log.Debug("write failed", "client", c.id, "err", err)
		return false
	}
	return true
}

// writePing sends one protocol-level ping (FR-7).
func (c *Conn) writePing() bool {
	if err := c.sock.WriteControl(websocket.PingMessage, nil, c.clock.Now().Add(c.writeTimeout)); err != nil {
		c.log.Debug("ping failed", "client", c.id, "err", err)
		return false
	}
	return true
}

// writeClose sends the websocket close frame carrying the close code, for clients that
// only see that and never the disconnect frame (docs/03-client-protocol.md §5.2).
func (c *Conn) writeClose() {
	code := proto.CloseCode(c.closeCode.Load())
	if code == 0 {
		return
	}
	reason := ""
	if r := c.closeReason.Load(); r != nil {
		reason = *r
	}
	if len(reason) > maxCloseReason {
		reason = reason[:maxCloseReason]
	}
	msg := websocket.FormatCloseMessage(int(code), reason)
	if err := c.sock.WriteControl(websocket.CloseMessage, msg, c.clock.Now().Add(c.writeTimeout)); err != nil {
		c.log.Debug("close frame failed", "client", c.id, "err", err)
	}
}

// encode serializes one outbound frame. It returns nil on failure, which offer and Send
// both refuse rather than dereference.
func (c *Conn) encode(v any) *proto.Frame {
	data, err := proto.Encode(v)
	if err != nil {
		// coverage: proto.Encode fails only on a json.RawMessage holding bytes that are
		// not valid JSON, and every frame built here is strings, ints and empty structs.
		// The one client-supplied value, a publish payload, is never echoed back.
		c.log.Error("encode frame failed", "client", c.id, "err", err)
		return nil
	}
	return &proto.Frame{Data: data}
}

// fnv1a is the 64-bit FNV-1a hash of s.
//
// It is written out rather than taken from hash/fnv because hash.Hash.Write returns an
// error that cannot occur, and the standard forbids both discarding it with a blank
// assignment and inventing a branch that no test can reach
// (docs/14-coding-standards.md §3, §6).
func fnv1a(s string) uint64 {
	const offset64 = 14695981039346656037
	const prime64 = 1099511628211
	h := uint64(offset64)
	for i := range len(s) {
		h ^= uint64(s[i])
		h *= prime64
	}
	return h
}
