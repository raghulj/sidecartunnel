package bus

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

// Defaults for RedisOptions, from docs/08-config.md §3.
const (
	// DefaultDialTimeout is bus.dial_timeout.
	DefaultDialTimeout = 3 * time.Second

	// DefaultReconnectMin is bus.reconnect_min, the backoff floor after bus loss (NFR-8).
	DefaultReconnectMin = 200 * time.Millisecond

	// DefaultReconnectMax is bus.reconnect_max, the backoff ceiling after bus loss.
	DefaultReconnectMax = 10 * time.Second

	// DefaultChunkSize is how many channels one SUBSCRIBE or UNSUBSCRIBE carries.
	//
	// 256 is chosen against two numbers. A 30,000-channel resubscribe — the M7 case — is
	// 118 round trips instead of 30,000, which is the difference between milliseconds and
	// seconds of blackout. And at a 64-byte channel name a chunk is ~16 KB, one socket
	// write and one command for Redis's single event loop to execute, so no single call
	// from this gateway can hold that loop away from every other client for long. Larger
	// chunks buy almost nothing on the first number and lose on the second: going from
	// 256 to 4096 turns 118 round trips into 8, while making one command 16 times more
	// expensive for every other client on the server.
	DefaultChunkSize = 256
)

// subscriber is the part of a Redis pub/sub connection this package uses. *redis.PubSub
// satisfies it.
//
// It exists as an interface so the failure branches — a refused SUBSCRIBE, a socket that
// dies between a command and its confirmation, a reply of an unexpected type — can be
// tested deterministically instead of being provoked through a real socket and asserted
// on with a timeout.
type subscriber interface {
	Subscribe(ctx context.Context, channels ...string) error
	Unsubscribe(ctx context.Context, channels ...string) error
	Ping(ctx context.Context, payload ...string) error
	Receive(ctx context.Context) (any, error)
	Close() error
}

// transport is everything the bus needs from a Redis client: a way to open a subscriber
// connection, and a way to publish.
type transport interface {
	// Open returns a new subscriber connection. Connectivity is not established here —
	// the caller proves it with Ping, so that a dead server is one failed attempt rather
	// than a connection that looks alive until the first message never arrives.
	Open(ctx context.Context) subscriber
	Publish(ctx context.Context, channel string, payload []byte) error
	Close() error
}

// redisTransport is the production transport: one go-redis client.
type redisTransport struct {
	client *redis.Client
}

func (t *redisTransport) Open(ctx context.Context) subscriber {
	return t.client.Subscribe(ctx)
}

func (t *redisTransport) Publish(ctx context.Context, channel string, payload []byte) error {
	return t.client.Publish(ctx, channel, payload).Err()
}

func (t *redisTransport) Close() error {
	return t.client.Close()
}

// RedisOptions configures a RedisBus. Every field maps to a key in the bus block of
// docs/08-config.md §3, and a zero field takes that key's documented default.
type RedisOptions struct {
	// URL is bus.url, e.g. "redis://localhost:6379/0".
	URL string

	// DialTimeout is bus.dial_timeout. Default DefaultDialTimeout.
	DialTimeout time.Duration

	// ReconnectMin and ReconnectMax bound the reconnection backoff (NFR-8). Defaults
	// DefaultReconnectMin and DefaultReconnectMax.
	ReconnectMin time.Duration
	ReconnectMax time.Duration

	// IntakeQueue is bus.intake_queue, the depth of the channel Receive returns. Default
	// DefaultIntakeQueue.
	IntakeQueue int

	// ChunkSize is how many channels one SUBSCRIBE or UNSUBSCRIBE carries. Default
	// DefaultChunkSize. It is not a configuration key; it is exposed so a test can assert
	// the batching, and so an operator with pathological channel names has a lever.
	ChunkSize int

	// Clock supplies the current time for Health.DisconnectedFor. Default time.Now.
	//
	// It is a field rather than a package-level variable because two tests in one process
	// must be able to hold different times (docs/14-coding-standards.md §7).
	Clock func() time.Time
}

// RedisBus is the Bus the product runs on: one Redis pub/sub connection for delivery, one
// client for publishing, and a supervisor that reconnects with backoff.
//
// Its shape follows two findings.
//
// Subscriptions are state, not events (S2). There is no command queue, so nothing a
// caller does can stall on Redis being slow, a failed Sync loses no intent, and a
// reconnect is a resync of the whole desired set rather than a sweep racing a live
// command stream.
//
// The reader goroutine does nothing but drain the socket into the bounded intake channel
// (M8). Redis enforces client-output-buffer-limit pubsub and disconnects a subscriber
// that falls behind; a goroutine that decoded and fanned out to 10,000 connections
// between socket reads would fall behind during a broadcast burst, be evicted, reconnect,
// resubscribe and be immediately behind again — a stable oscillation that reads to an
// operator as an unstable Redis.
//
// Every method is safe for concurrent use.
type RedisBus struct {
	tr    transport
	out   chan Message
	chunk int
	clock func() time.Time

	reconnectMin time.Duration
	reconnectMax time.Duration

	// ctx bounds every background goroutine and is cancelled by Close. It is held on the
	// struct because this type owns a lifecycle rather than serving a request, and the
	// constructor takes no context.
	ctx    context.Context
	cancel context.CancelFunc

	done      chan struct{}
	wg        sync.WaitGroup
	closeOnce sync.Once
	closeErr  error

	first     chan struct{}
	firstOnce sync.Once

	// syncMu serializes Sync against itself and against the reconnect resync, and guards
	// current and desired.
	//
	// It is deliberately held across network I/O: a chunked Sync is one ordered
	// conversation with one connection, and interleaving two of them would leave a
	// subscription set nobody can reason about. That is safe here only because nothing on
	// a delivery path ever acquires it — not Publish, not Receive, not the reader, not
	// Health — so a slow Redis cannot stall fan-out, which is exactly the C7 failure the
	// old command queue produced (docs/09-internals.md §4.1).
	syncMu  sync.Mutex
	current map[string]struct{}
	desired map[string]struct{}

	// connMu guards the current connection and nothing else. It is never held across I/O,
	// so Close never queues behind a 30,000-channel resubscribe.
	connMu  sync.Mutex
	sub     subscriber
	lost    chan struct{}
	closing bool

	// acks counts subscription confirmations for the chunk currently in flight, and
	// ackWake pokes the waiter. The reader increments and pokes without ever blocking:
	// a reader that could block on bookkeeping is a reader that stops draining the socket.
	acks    atomic.Int64
	ackWake chan struct{}

	connected   atomic.Bool
	downSince   atomic.Int64
	connections atomic.Uint64
	subs        atomic.Int64
	dropped     atomic.Uint64
}

// NewRedis returns a RedisBus for opts.URL and starts its connection supervisor.
//
// It returns after the first connection attempt has resolved, so a caller can log and
// report readiness honestly from the moment it returns. A failed first attempt is not an
// error: losing Redis must not take the gateway down (NFR-8), so the bus comes back
// disconnected and keeps retrying with backoff. Only a URL that cannot be parsed is an
// error, because that one will never succeed.
func NewRedis(opts RedisOptions) (*RedisBus, error) {
	o, err := redis.ParseURL(opts.URL)
	if err != nil {
		// The message names the key, never the value: a Redis URL can carry a password
		// (docs/14-coding-standards.md §9).
		return nil, fmt.Errorf("bus: bus.url is not a valid redis url: %w", err)
	}
	if opts.DialTimeout <= 0 {
		opts.DialTimeout = DefaultDialTimeout
	}
	o.DialTimeout = opts.DialTimeout
	return newRedisBus(&redisTransport{client: redis.NewClient(o)}, opts), nil
}

// newRedisBus is the constructor the tests use to substitute a transport. NewRedis is the
// only production caller.
func newRedisBus(tr transport, opts RedisOptions) *RedisBus {
	if opts.IntakeQueue <= 0 {
		opts.IntakeQueue = DefaultIntakeQueue
	}
	if opts.ChunkSize <= 0 {
		opts.ChunkSize = DefaultChunkSize
	}
	if opts.ReconnectMin <= 0 {
		opts.ReconnectMin = DefaultReconnectMin
	}
	if opts.ReconnectMax <= 0 {
		opts.ReconnectMax = DefaultReconnectMax
	}
	if opts.Clock == nil {
		opts.Clock = time.Now
	}

	ctx, cancel := context.WithCancel(context.Background())
	b := &RedisBus{
		tr:           tr,
		out:          make(chan Message, opts.IntakeQueue),
		chunk:        opts.ChunkSize,
		clock:        opts.Clock,
		reconnectMin: opts.ReconnectMin,
		reconnectMax: opts.ReconnectMax,
		ctx:          ctx,
		cancel:       cancel,
		done:         make(chan struct{}),
		first:        make(chan struct{}),
		current:      map[string]struct{}{},
		desired:      map[string]struct{}{},
		ackWake:      make(chan struct{}, 1),
	}
	b.downSince.Store(opts.Clock().UnixNano())

	b.wg.Add(1)
	go b.supervise()
	<-b.first
	return b
}

// Sync makes the upstream subscription set exactly desired, in batched SUBSCRIBE and
// UNSUBSCRIBE commands, and returns once Redis has confirmed every one of them.
//
// It is idempotent: while connected, a desired set equal to the one already applied issues
// no commands at all, which matters because the hub's reconciler calls it on every dirty
// pass. desired is not retained.
//
// It returns ErrDisconnected while the bus is down — the desired set is still recorded,
// and applied in full when the connection comes back — ErrClosed after Close, the
// context's error if ctx ends, and the transport's error wrapped if Redis refuses a
// command. It never returns nil after a failure: a Sync that failed silently is a channel
// that is locally held and upstream dead, invisible until someone asks why it is quiet
// (M5).
func (b *RedisBus) Sync(ctx context.Context, desired []string) error {
	if err := checkState(ctx, b.done); err != nil {
		return fmt.Errorf("bus: sync %d channels: %w", len(desired), err)
	}

	// Copied, never retained: the reconciler reuses its backing array on the next pass
	// (docs/09-internals.md §4.2).
	next := make(map[string]struct{}, len(desired))
	for _, ch := range desired {
		next[ch] = struct{}{}
	}

	b.syncMu.Lock()
	defer b.syncMu.Unlock()
	b.desired = next
	return b.apply(ctx)
}

// Publish sends payload on channel. channel is a full bus key.
//
// On a disconnected bus it returns ErrDisconnected immediately rather than blocking:
// messages published during an outage are lost by design, and a publish that waited would
// hold its caller — including the control channel — for the length of the outage
// (docs/09-internals.md §7).
func (b *RedisBus) Publish(ctx context.Context, channel string, payload []byte) error {
	if err := checkState(ctx, b.done); err != nil {
		return fmt.Errorf("bus: publish %s: %w", channel, err)
	}
	if !b.connected.Load() {
		return fmt.Errorf("bus: publish %s: %w", channel, ErrDisconnected)
	}
	if err := b.tr.Publish(ctx, channel, payload); err != nil {
		// The channel is named, the payload never is (NFR-7).
		return fmt.Errorf("bus: publish %s: %w", channel, err)
	}
	return nil
}

// Receive returns the intake channel. It is the same channel on every call and is closed
// only by Close — never by a reconnect, because losing the bus must not close client
// connections (NFR-8).
//
// When the intake is full the reader drops the message and counts it in Health.Dropped.
// It cannot block: a reader that stops draining the socket is a reader Redis evicts for
// exceeding client-output-buffer-limit pubsub, and the eviction-reconnect-fall-behind
// cycle that follows is stable rather than transient (M8, docs/09-internals.md §5). A drop
// is at-most-once behaving as documented, and the client's reconciliation covers it
// (docs/07-delivery.md §2).
func (b *RedisBus) Receive() <-chan Message {
	return b.out
}

// Close stops the supervisor, closes the transport and closes the channel Receive
// returns. It is idempotent, and every subsequent or in-flight Sync and Publish returns
// rather than blocking.
func (b *RedisBus) Close() error {
	b.closeOnce.Do(func() {
		// closing is set before done is closed, so that anything which observes the bus
		// closing also observes the flag adopt reads. The reverse order leaves a window
		// in which a connection opened concurrently is adopted after Close has stopped
		// looking for one, and then nothing ever closes it: Close waits for a reader that
		// is parked on a live socket, forever.
		b.connMu.Lock()
		b.closing = true
		sub := b.sub
		b.connMu.Unlock()
		close(b.done)
		if sub != nil {
			// Closing the connection is what unblocks a reader parked in a socket read.
			// Cancelling the context does not: there is no deadline on that read, by
			// design, because a deadline would turn a quiet channel into a reconnect.
			_ = sub.Close()
		}

		b.cancel()
		b.wg.Wait()
		close(b.out)
		b.connected.Store(false)
		if err := b.tr.Close(); err != nil {
			b.closeErr = fmt.Errorf("bus: close: %w", err)
		}
	})
	return b.closeErr
}

// Health reports the transport's state for /ready and for metrics. It takes no lock and
// never blocks, so a readiness probe cannot queue behind a large resubscribe.
func (b *RedisBus) Health() Health {
	h := Health{
		Connected:     b.connected.Load(),
		Subscriptions: int(b.subs.Load()),
		IntakeDepth:   len(b.out),
		Dropped:       b.dropped.Load(),
	}
	if n := b.connections.Load(); n > 1 {
		h.Reconnects = n - 1
	}
	if !h.Connected {
		h.DisconnectedFor = b.clock().Sub(time.Unix(0, b.downSince.Load()))
	}
	return h
}

// supervise owns the connection: it opens one, proves it with a ping, hands it to a reader
// goroutine, resubscribes the whole desired set, and waits for it to die.
//
// Reconnect is a forced resync and nothing else. There is no sweep of the hub to race a
// live command stream, because S2 removed the command stream: the desired set is the only
// truth and Sync is idempotent (M6).
func (b *RedisBus) supervise() {
	defer b.wg.Done()

	var backoff time.Duration
	for {
		sub := b.tr.Open(b.ctx)
		if err := sub.Ping(b.ctx); err != nil {
			_ = sub.Close()
			b.firstAttemptDone()
			backoff = nextBackoff(backoff, b.reconnectMin, b.reconnectMax)
			if !b.wait(backoff) {
				return
			}
			continue
		}
		backoff = 0

		// The reader starts before the connection is adopted so that the confirmations
		// for the resubscribe below have somebody counting them.
		lost := make(chan struct{})
		b.wg.Add(1)
		go b.read(sub, lost)

		if !b.adopt(sub, lost) {
			_ = sub.Close()
			b.firstAttemptDone()
			<-lost
			return
		}
		b.firstAttemptDone()

		if err := b.resync(); err != nil {
			// There is nothing to report the error to — Sync's caller is the reconciler
			// and it is not here — so the response is to drop the connection and try
			// again from a known state. The failure is visible as Health.Reconnects
			// climbing, which is st_bus_reconnects_total.
			b.dropConnection()
		}

		<-lost
		b.release(sub)
		if b.ctx.Err() != nil {
			return
		}
		backoff = nextBackoff(backoff, b.reconnectMin, b.reconnectMax)
		if !b.wait(backoff) {
			return
		}
	}
}

// read drains the connection into the intake channel and does nothing else. Every
// statement in this loop is on the path Redis measures against
// client-output-buffer-limit pubsub (M8).
func (b *RedisBus) read(sub subscriber, lost chan struct{}) {
	defer b.wg.Done()
	defer close(lost)

	for {
		msg, err := sub.Receive(b.ctx)
		if err != nil {
			return
		}
		switch m := msg.(type) {
		case *redis.Message:
			select {
			case b.out <- Message{Channel: m.Channel, Payload: []byte(m.Payload)}:
			default:
				b.dropped.Add(1)
			}
		case *redis.Subscription:
			b.acks.Add(1)
			select {
			case b.ackWake <- struct{}{}:
			default:
			}
		}
		// Anything else — a pong, or a reply a future server invents — is not this
		// goroutine's business.
	}
}

// adopt makes sub the live connection. It returns false once Close has run, so a
// connection opened in the gap cannot be adopted and then leaked.
func (b *RedisBus) adopt(sub subscriber, lost chan struct{}) bool {
	// The subscription set of a fresh connection is empty, and that has to be recorded
	// before anything can be told the bus is connected: a Sync that ran against a stale
	// current would diff to "nothing to do" and leave every channel unsubscribed
	// upstream while the hub believed otherwise.
	b.syncMu.Lock()
	defer b.syncMu.Unlock()

	b.connMu.Lock()
	defer b.connMu.Unlock()
	if b.closing {
		return false
	}
	b.sub, b.lost = sub, lost
	b.current = map[string]struct{}{}
	b.subs.Store(0)
	b.acks.Store(0)
	b.connections.Add(1)
	b.connected.Store(true)
	return true
}

// release records that the connection is gone. Client connections are untouched: they
// stay open and silent until the bus returns (NFR-8).
func (b *RedisBus) release(sub subscriber) {
	b.connMu.Lock()
	if b.sub == sub {
		b.sub, b.lost = nil, nil
	}
	b.connMu.Unlock()

	if b.connected.CompareAndSwap(true, false) {
		b.downSince.Store(b.clock().UnixNano())
		// Nothing is subscribed upstream once the connection is gone, so the gauge must
		// say zero. It used to keep reporting the pre-outage count until a new connection
		// was adopted, which made st_bus_subscriptions_current read healthy during
		// exactly the incident it is watched in - and made any test that waited on the
		// count alone proceed against a gateway that had not resubscribed yet.
		b.subs.Store(0)
	}
	_ = sub.Close()
}

// dropConnection ends the live connection, which makes the supervisor reconnect and
// resync. It is the response to any failure that leaves the subscription set unknowable.
func (b *RedisBus) dropConnection() {
	b.connMu.Lock()
	sub := b.sub
	b.connMu.Unlock()
	if sub != nil {
		_ = sub.Close()
	}
}

// resync reapplies the whole desired set to a new connection.
func (b *RedisBus) resync() error {
	b.syncMu.Lock()
	defer b.syncMu.Unlock()
	return b.apply(b.ctx)
}

// connection returns the live connection, or false if there is none.
func (b *RedisBus) connection() (subscriber, <-chan struct{}, bool) {
	b.connMu.Lock()
	defer b.connMu.Unlock()
	if b.sub == nil || !b.connected.Load() {
		return nil, nil, false
	}
	return b.sub, b.lost, true
}

// command is one direction of the diff: a name for the error message, the variadic call
// that carries it, and whether the channels end up held.
type command struct {
	name string
	send func(ctx context.Context, channels ...string) error
	hold bool
}

// apply issues whatever commands make current equal desired. syncMu must be held.
func (b *RedisBus) apply(ctx context.Context) error {
	// The connection is checked before the diff, and deliberately: while the bus is down
	// its subscription set upstream is empty whatever current says, so a Sync that
	// happened to match current would otherwise report success for a set that exists
	// nowhere. Nil from Sync means the set is applied upstream, and nothing else.
	sub, lost, ok := b.connection()
	if !ok {
		return fmt.Errorf("bus: sync %d channels: %w", len(b.desired), ErrDisconnected)
	}

	add, remove := diff(b.current, b.desired)
	if len(add) == 0 && len(remove) == 0 {
		// Idempotence is not an optimization here. The reconciler calls Sync on every
		// dirty pass, so a no-op that still issued commands would put Redis back on the
		// subscribe path it was taken off (S2).
		return nil
	}

	// Removals first: a replica that has just dropped 20,000 channels should stop
	// receiving them before it asks for anything new.
	for _, cmd := range []command{
		{name: "unsubscribe", send: sub.Unsubscribe, hold: false},
		{name: "subscribe", send: sub.Subscribe, hold: true},
	} {
		channels := remove
		if cmd.hold {
			channels = add
		}
		if err := b.issue(ctx, lost, cmd, channels); err != nil {
			return err
		}
	}
	return nil
}

// issue sends one direction of the diff in chunks, waiting for each chunk's confirmations
// before recording it and moving on. syncMu must be held.
//
// Waiting for the confirmations is what makes Sync's contract true rather than hopeful:
// when it returns, Redis has processed the subscription, so a message published
// afterwards is delivered. Applying a chunk only once it is confirmed is what makes a
// failure retryable — the caller's next Sync re-diffs and reissues exactly what did not
// land.
func (b *RedisBus) issue(ctx context.Context, lost <-chan struct{}, cmd command, channels []string) error {
	for _, part := range chunk(channels, b.chunk) {
		b.acks.Store(0)
		if err := cmd.send(ctx, part...); err != nil {
			b.dropConnection()
			return fmt.Errorf("bus: %s %d channels: %w", cmd.name, len(part), err)
		}
		if err := b.waitAcks(ctx, lost, len(part)); err != nil {
			// A chunk that was sent but not confirmed leaves a subscription set nobody
			// can reason about, so the connection goes and the next one starts from
			// empty.
			b.dropConnection()
			return fmt.Errorf("bus: %s %d channels: %w", cmd.name, len(part), err)
		}
		for _, ch := range part {
			if cmd.hold {
				b.current[ch] = struct{}{}
			} else {
				delete(b.current, ch)
			}
		}
		b.subs.Store(int64(len(b.current)))
	}
	return nil
}

// waitAcks blocks until want confirmations have arrived for the chunk in flight, or until
// the connection, the context or the bus ends. It is bounded on every axis; nothing here
// can park a caller indefinitely.
func (b *RedisBus) waitAcks(ctx context.Context, lost <-chan struct{}, want int) error {
	for b.acks.Load() < int64(want) {
		select {
		case <-b.ackWake:
		case <-lost:
			return ErrDisconnected
		case <-b.done:
			return ErrClosed
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

// wait sleeps for d, or returns false if the bus is closing first.
func (b *RedisBus) wait(d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-b.ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

// firstAttemptDone releases the constructor once the first connection attempt has
// resolved, either way.
func (b *RedisBus) firstAttemptDone() {
	b.firstOnce.Do(func() { close(b.first) })
}

// diff returns what must be subscribed and what must be unsubscribed to turn current into
// desired.
//
// Both are sorted. Map iteration order is deliberately random in Go, and an unsorted diff
// would make chunk boundaries differ run to run — which makes a resubscribe impossible to
// reproduce from a packet capture and impossible to assert in a test. The sort costs
// O(n log n) on a path that is about to do network round trips.
func diff(current, desired map[string]struct{}) (add, remove []string) {
	for ch := range desired {
		if _, held := current[ch]; !held {
			add = append(add, ch)
		}
	}
	for ch := range current {
		if _, wanted := desired[ch]; !wanted {
			remove = append(remove, ch)
		}
	}
	slices.Sort(add)
	slices.Sort(remove)
	return add, remove
}

// chunk splits channels into slices of at most size. The slices share the input's backing
// array; nothing here retains them past the command they are sent in.
func chunk(channels []string, size int) [][]string {
	var out [][]string
	for i := 0; i < len(channels); i += size {
		out = append(out, channels[i:min(i+size, len(channels))])
	}
	return out
}

// nextBackoff doubles cur between floor and ceiling, starting at floor.
//
// Plain exponential backoff with no jitter is right here and jitter is not: there is one
// bus connection per replica and two replicas, so there is no herd to spread. The herd
// that matters is clients reconnecting, and that is spread by server-directed retry_after
// (docs/13-review-findings.md S5).
func nextBackoff(cur, floor, ceiling time.Duration) time.Duration {
	if cur <= 0 {
		cur = floor
	} else {
		cur *= 2
	}
	if cur > ceiling {
		cur = ceiling
	}
	return cur
}
