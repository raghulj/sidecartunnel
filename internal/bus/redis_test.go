package bus

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// ---------------------------------------------------------------------------
// Wire-level tests: miniredis, or ST_TEST_REDIS_URL, through a counting proxy.
// ---------------------------------------------------------------------------

// proxiedBus returns a bus whose every byte passes through a proxy the test can count and
// break.
func proxiedBus(t *testing.T, opts RedisOptions) (*RedisBus, *tcpProxy, func(string) string) {
	t.Helper()
	raw := redisURL(t)
	p := newProxy(t, hostOf(t, raw))
	opts.URL = proxiedURL(t, raw, p.addr())
	return mustRedis(t, opts), p, keyspace(t)
}

func channelSet(key func(string) string, n int) []string {
	out := make([]string, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, key(fmt.Sprintf("ch-%04d", i)))
	}
	return out
}

// TestRedisSyncBatchesIntoChunks is M7. One channel per call made a 30,000-channel
// resubscribe 30,000 serial round trips — seconds of blackout at any real scale.
// SUBSCRIBE is variadic, so this asserts what went onto the wire rather than what the bus
// believes it did.
func TestRedisSyncBatchesIntoChunks(t *testing.T) {
	b, p, key := proxiedBus(t, RedisOptions{IntakeQueue: 64})

	const channels = 1000
	mustSync(t, b, channelSet(key, channels)...)

	got := argsOf(only(p.commands(), "subscribe"))
	want := []int{DefaultChunkSize, DefaultChunkSize, DefaultChunkSize, channels - 3*DefaultChunkSize}
	if !slices.Equal(got, want) {
		t.Fatalf("subscribe round trips carried %v channels, want %v", got, want)
	}
	if h := b.Health(); h.Subscriptions != channels {
		t.Errorf("Subscriptions = %d, want %d", h.Subscriptions, channels)
	}
}

// TestRedisSyncIsIdempotentOnTheWire is S2. Sync is state, not events: repeating the same
// desired set must cost nothing, because the reconciler calls it on every dirty pass and a
// per-call round trip would put Redis back on the subscribe path.
func TestRedisSyncIsIdempotentOnTheWire(t *testing.T) {
	b, p, key := proxiedBus(t, RedisOptions{IntakeQueue: 64})
	desired := channelSet(key, 3)

	mustSync(t, b, desired...)
	first := len(only(p.commands(), "subscribe"))
	if first != 1 {
		t.Fatalf("first sync issued %d subscribe commands, want 1", first)
	}

	mustSync(t, b, desired...)
	mustSync(t, b, desired[2], desired[0], desired[1])
	cmds := p.commands()
	if got := len(only(cmds, "subscribe")); got != first {
		t.Errorf("repeat syncs issued %d subscribe commands in total, want %d", got, first)
	}
	if got := len(only(cmds, "unsubscribe")); got != 0 {
		t.Errorf("repeat syncs issued %d unsubscribe commands, want 0", got)
	}
}

// TestRedisSyncUnsubscribesInChunks covers the removal half of the diff, which is the half
// that runs when a replica drains and 30,000 channels go away at once.
func TestRedisSyncUnsubscribesInChunks(t *testing.T) {
	b, p, key := proxiedBus(t, RedisOptions{IntakeQueue: 64})

	const channels = 600
	desired := channelSet(key, channels)
	mustSync(t, b, desired...)
	mustSync(t, b, desired[:100]...)

	got := argsOf(only(p.commands(), "unsubscribe"))
	if total(got) != channels-100 {
		t.Fatalf("unsubscribed %v channels (%d total), want %d", got, total(got), channels-100)
	}
	for _, n := range got {
		if n > DefaultChunkSize {
			t.Fatalf("one unsubscribe carried %d channels, want at most %d", n, DefaultChunkSize)
		}
	}
	if h := b.Health(); h.Subscriptions != 100 {
		t.Errorf("Subscriptions = %d, want 100", h.Subscriptions)
	}
}

// TestRedisReconnectKeepsClientsAndRestoresDelivery is NFR-8: losing Redis must not close
// client connections, the whole desired set must come back, and messages must flow again.
// There is no sweep to race a command stream, because S2 removed the command stream.
func TestRedisReconnectKeepsClientsAndRestoresDelivery(t *testing.T) {
	b, p, key := proxiedBus(t, RedisOptions{IntakeQueue: 64})

	const channels = 600
	desired := channelSet(key, channels)
	mustSync(t, b, desired...)
	before := connIDs(p.commands())

	// A consumer holding Receive stands in for the dispatch workers. If the bus closed
	// its intake on connection loss, every client on the replica would go silent for good.
	got := make(chan Message, 16)
	closed := make(chan struct{})
	stop := make(chan struct{})
	go func() {
		for {
			select {
			case msg, ok := <-b.Receive():
				if !ok {
					close(closed)
					return
				}
				select {
				case got <- msg:
				default:
				}
			case <-stop:
				return
			}
		}
	}()
	defer close(stop)

	p.Break()

	// The whole desired set comes back on the bus's next connection, still batched.
	//
	// The assertion is per connection because go-redis re-dials and resubscribes its own
	// channel set in one unbatched command inside the read that is failing, before it
	// hands the error back. That connection is discarded a moment later — this bus starts
	// each generation from an empty set on a connection it opened itself, which is what
	// keeps a 30,000-channel resubscribe 118 round trips instead of one enormous command
	// (M7) and what keeps `current` a fact rather than a guess.
	cmds := p.waitFor(t, "the desired set to be resubscribed in chunks", func(cmds []busCommand) bool {
		return resubscribedConn(cmds, before, channels) > 0
	})
	if got := resubscribedConn(cmds, before, channels); got <= 0 {
		t.Fatalf("no connection carried the whole desired set in chunks; commands: %s", summarise(cmds))
	}

	select {
	case <-closed:
		t.Fatal("Receive was closed by a bus reconnect: NFR-8 says client connections survive")
	default:
	}

	// A Sync fences the publish below: it returns only once this connection has confirmed
	// the subscription, so the message cannot race the resubscribe.
	mustSync(t, b, append(slices.Clone(desired), key("fence"))...)
	mustPublish(t, b, desired[channels/2], "after the outage")
	select {
	case msg := <-got:
		if msg.Channel != desired[channels/2] || string(msg.Payload) != "after the outage" {
			t.Fatalf("got %s %q after the outage", msg.Channel, msg.Payload)
		}
	case <-time.After(receiveTimeout):
		t.Fatalf("no delivery within %s of the bus reconnecting", receiveTimeout)
	}

	if h := b.Health(); !h.Connected || h.Reconnects < 1 {
		t.Errorf("Health = %+v, want connected with at least one reconnect", h)
	}
}

// resubscribedConn returns the id of a connection opened after those in seen that carried
// the whole desired set in chunks of at most DefaultChunkSize, or 0 if there is none.
func resubscribedConn(cmds []busCommand, seen []int, want int) int {
	for _, id := range connIDs(cmds) {
		if slices.Contains(seen, id) {
			continue
		}
		sizes := argsOf(only(onConn(cmds, id), "subscribe"))
		if total(sizes) != want {
			continue
		}
		oversize := false
		for _, n := range sizes {
			if n > DefaultChunkSize {
				oversize = true
			}
		}
		if !oversize {
			return id
		}
	}
	return 0
}

// TestRedisDisconnectedPublishAndSync asserts the two calls a caller makes during an
// outage fail fast rather than blocking, and that the desired set survives the outage and
// is applied on reconnect.
func TestRedisDisconnectedPublishAndSync(t *testing.T) {
	raw := redisURL(t)
	p := newProxy(t, hostOf(t, raw))
	p.Refuse(true)
	b := mustRedis(t, RedisOptions{
		URL:          proxiedURL(t, raw, p.addr()),
		IntakeQueue:  64,
		ReconnectMin: 20 * time.Millisecond,
		ReconnectMax: 20 * time.Millisecond,
	})
	key := keyspace(t)

	if h := b.Health(); h.Connected {
		t.Fatalf("Health = %+v, want disconnected: nothing is listening", h)
	}
	if err := b.Publish(t.Context(), key("a"), []byte("x")); !errors.Is(err, ErrDisconnected) {
		t.Errorf("Publish while disconnected = %v, want ErrDisconnected", err)
	}
	// Sync fails, but records the intent: there is no command queue to lose it in, and
	// reconnect is a forced resync rather than a sweep (S2, docs/09-internals.md §7).
	if err := b.Sync(t.Context(), []string{key("a")}); !errors.Is(err, ErrDisconnected) {
		t.Errorf("Sync while disconnected = %v, want ErrDisconnected", err)
	}

	p.Refuse(false)
	p.waitFor(t, "the desired set to be subscribed once the bus comes back", func(cmds []busCommand) bool {
		return total(argsOf(only(cmds, "subscribe"))) >= 1
	})
	mustSync(t, b, key("a"), key("fence"))
	mustPublish(t, b, key("a"), "recovered")
	mustReceive(t, b.Receive(), key("a"), "recovered")
}

// ---------------------------------------------------------------------------
// Transport-level tests: a fake transport, for the branches a socket cannot be
// made to take on demand.
// ---------------------------------------------------------------------------

var (
	errPing      = errors.New("ping refused")
	errSubscribe = errors.New("subscribe refused")
	errPublish   = errors.New("publish refused")
	errClose     = errors.New("close refused")
	errSocket    = errors.New("connection reset")
)

// fakeSub is one subscriber connection under test control.
type fakeSub struct {
	tr *fakeTransport

	mu       sync.Mutex
	pingErr  error
	subErr   error
	unsubErr error
	pingGate chan struct{}

	in     chan any
	closed chan struct{}
	once   sync.Once
}

func (f *fakeSub) Ping(_ context.Context, _ ...string) error {
	f.mu.Lock()
	gate, err := f.pingGate, f.pingErr
	f.mu.Unlock()
	if gate != nil {
		select {
		case f.tr.pinging <- struct{}{}:
		default:
		}
		<-gate
	}
	return err
}

func (f *fakeSub) Subscribe(_ context.Context, channels ...string) error {
	return f.command("subscribe", channels)
}

func (f *fakeSub) Unsubscribe(_ context.Context, channels ...string) error {
	return f.command("unsubscribe", channels)
}

func (f *fakeSub) command(kind string, channels []string) error {
	f.mu.Lock()
	err := f.subErr
	if kind == "unsubscribe" {
		err = f.unsubErr
	}
	f.mu.Unlock()

	select {
	case f.tr.issued <- busCommand{name: kind, args: len(channels)}:
	default:
	}
	if err != nil {
		return err
	}
	if f.tr.autoAck {
		for _, ch := range channels {
			f.deliver(&redis.Subscription{Kind: kind, Channel: ch})
		}
	}
	return nil
}

// deliver hands the reader one thing to read: a message, a subscription confirmation, or
// an error standing in for a broken socket.
func (f *fakeSub) deliver(v any) {
	select {
	case f.in <- v:
	case <-f.closed:
	}
}

func (f *fakeSub) Receive(ctx context.Context) (any, error) {
	select {
	case v := <-f.in:
		if err, ok := v.(error); ok {
			return nil, err
		}
		return v, nil
	case <-f.closed:
		return nil, errSocket
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (f *fakeSub) Close() error {
	f.once.Do(func() { close(f.closed) })
	return nil
}

// fakeTransport hands out fakeSubs and records publishes.
type fakeTransport struct {
	autoAck      bool
	pingFailures int
	publishErr   error
	closeErr     error

	opened  chan *fakeSub
	issued  chan busCommand
	pinging chan struct{}

	mu         sync.Mutex
	attempts   int
	published  []Message
	nextSubErr error
	nextGate   chan struct{}
}

// failNextSubscribe makes the next connection's SUBSCRIBE fail, which is how the resync
// on a reconnect is made to fail on demand.
func (f *fakeTransport) failNextSubscribe(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.nextSubErr = err
}

// holdDown makes every further connection attempt fail, so a test can keep the bus
// disconnected for as long as it needs to.
func (f *fakeTransport) holdDown() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.pingFailures = 1 << 30
}

// gateNextPing holds the next connection inside Ping until the returned channel is
// closed, which is the only way to stand a goroutine still in the window between a
// connection being proved and being adopted.
func (f *fakeTransport) gateNextPing() chan struct{} {
	gate := make(chan struct{})
	f.mu.Lock()
	defer f.mu.Unlock()
	f.nextGate = gate
	return gate
}

func newFakeTransport(autoAck bool) *fakeTransport {
	return &fakeTransport{
		autoAck: autoAck,
		opened:  make(chan *fakeSub, 64),
		issued:  make(chan busCommand, 64),
		pinging: make(chan struct{}, 1),
	}
}

func (f *fakeTransport) Open(_ context.Context) subscriber {
	f.mu.Lock()
	f.attempts++
	fail := f.attempts <= f.pingFailures
	subErr, gate := f.nextSubErr, f.nextGate
	f.nextSubErr, f.nextGate = nil, nil
	f.mu.Unlock()

	s := &fakeSub{tr: f, in: make(chan any, 4096), closed: make(chan struct{}), subErr: subErr, pingGate: gate}
	if fail {
		s.pingErr = errPing
	}
	select {
	case f.opened <- s:
	default:
	}
	return s
}

func (f *fakeTransport) Publish(_ context.Context, channel string, payload []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.publishErr != nil {
		return f.publishErr
	}
	f.published = append(f.published, Message{Channel: channel, Payload: payload})
	return nil
}

func (f *fakeTransport) Close() error { return f.closeErr }

func (f *fakeTransport) nextSub(t *testing.T) *fakeSub {
	t.Helper()
	select {
	case s := <-f.opened:
		return s
	case <-time.After(receiveTimeout):
		t.Fatalf("no subscriber connection opened within %s", receiveTimeout)
		return nil
	}
}

func (f *fakeTransport) nextCommand(t *testing.T) busCommand {
	t.Helper()
	select {
	case c := <-f.issued:
		return c
	case <-time.After(receiveTimeout):
		t.Fatalf("no command issued within %s", receiveTimeout)
		return busCommand{}
	}
}

func fakeBus(t *testing.T, tr *fakeTransport, opts RedisOptions) *RedisBus {
	t.Helper()
	if opts.ReconnectMin == 0 {
		opts.ReconnectMin = time.Millisecond
	}
	if opts.ReconnectMax == 0 {
		opts.ReconnectMax = 2 * time.Millisecond
	}
	b := newRedisBus(tr, opts)
	t.Cleanup(func() { _ = b.Close() })
	return b
}

// TestRedisRetriesUntilTheTransportAnswers covers the backoff loop and, with it, the rule
// that a bus which cannot reach Redis at startup is a bus that keeps trying rather than a
// process that refuses to start (NFR-8).
func TestRedisRetriesUntilTheTransportAnswers(t *testing.T) {
	tr := newFakeTransport(true)
	tr.pingFailures = 2
	b := fakeBus(t, tr, RedisOptions{})

	if h := b.Health(); h.Connected {
		t.Fatalf("Health = %+v, want disconnected after a refused ping", h)
	}
	if err := b.Sync(t.Context(), []string{"b", "a"}); !errors.Is(err, ErrDisconnected) {
		t.Fatalf("Sync while disconnected = %v, want ErrDisconnected", err)
	}

	// The third attempt succeeds, and the desired set recorded during the outage is what
	// gets subscribed. Nothing had to be queued to make that happen.
	if got := tr.nextCommand(t); got.name != "subscribe" || got.args != 2 {
		t.Fatalf("resubscribe issued %+v, want subscribe with 2 channels", got)
	}
	// A Sync blocks until the reconnect's own sync has finished with the lock, so this
	// reads the settled state rather than racing it.
	mustSync(t, b, "b", "a")
	if h := b.Health(); !h.Connected || h.Subscriptions != 2 {
		t.Errorf("Health = %+v, want connected with 2 subscriptions", h)
	}
}

// TestRedisDisconnectedForUsesTheClock is what /ready compares against bus.ready_grace, so
// it has to be a duration and not just a flag (docs/13-review-findings.md M20).
func TestRedisDisconnectedForUsesTheClock(t *testing.T) {
	now := time.Unix(1756612800, 0)
	var mu sync.Mutex
	clock := func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		return now
	}

	tr := newFakeTransport(true)
	tr.pingFailures = 1 << 30
	b := fakeBus(t, tr, RedisOptions{
		Clock:        clock,
		ReconnectMin: time.Hour,
		ReconnectMax: time.Hour,
	})

	mu.Lock()
	now = now.Add(45 * time.Second)
	mu.Unlock()

	h := b.Health()
	if h.Connected {
		t.Fatalf("Health = %+v, want disconnected", h)
	}
	if h.DisconnectedFor != 45*time.Second {
		t.Errorf("DisconnectedFor = %s, want 45s", h.DisconnectedFor)
	}
}

// TestRedisSyncSubscribeFailureIsReportedAndRetried is M5: a Sync that fails must say so.
// The earlier design's Subscribe returned an error nobody consumed, and one transient
// failure left a channel locally subscribed and upstream dead forever, with no metric.
func TestRedisSyncSubscribeFailureIsReportedAndRetried(t *testing.T) {
	tr := newFakeTransport(true)
	b := fakeBus(t, tr, RedisOptions{})

	first := tr.nextSub(t)
	first.mu.Lock()
	first.subErr = errSubscribe
	first.mu.Unlock()

	err := b.Sync(t.Context(), []string{"a"})
	if !errors.Is(err, errSubscribe) {
		t.Fatalf("Sync = %v, want the transport's error wrapped", err)
	}
	if got := tr.nextCommand(t); got.name != "subscribe" {
		t.Fatalf("the failing sync issued %+v, want a subscribe", got)
	}
	if h := b.Health(); h.Subscriptions != 0 {
		t.Errorf("Subscriptions = %d after a failed sync, want 0: a failed chunk is not applied", h.Subscriptions)
	}

	// A failed sync drops the connection, because a half-applied subscription set is a
	// set nobody can reason about. The supervisor reconnects and reapplies the whole
	// desired set — batched, idempotent, no sweep.
	if got := tr.nextCommand(t); got.name != "subscribe" {
		t.Fatalf("after a failed sync the bus issued %+v, want a resubscribe", got)
	}
	mustSync(t, b, "a")
	if h := b.Health(); h.Subscriptions != 1 {
		t.Errorf("Subscriptions = %d after the retry, want 1", h.Subscriptions)
	}
}

// TestRedisSyncUnsubscribeFailureIsReported covers the removal half of the same rule.
func TestRedisSyncUnsubscribeFailureIsReported(t *testing.T) {
	tr := newFakeTransport(true)
	b := fakeBus(t, tr, RedisOptions{})

	sub := tr.nextSub(t)
	if err := b.Sync(t.Context(), []string{"a", "b"}); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	sub.mu.Lock()
	sub.unsubErr = errSubscribe
	sub.mu.Unlock()

	if err := b.Sync(t.Context(), []string{"a"}); !errors.Is(err, errSubscribe) {
		t.Fatalf("Sync = %v, want the transport's error wrapped", err)
	}
}

// TestRedisSyncAbandonsTheWaitWhenTheConnectionDies asserts a Sync waiting for
// confirmations does not hang past the connection it is waiting on.
func TestRedisSyncAbandonsTheWaitWhenTheConnectionDies(t *testing.T) {
	tr := newFakeTransport(false) // no confirmations: the wait has to be interrupted
	b := fakeBus(t, tr, RedisOptions{})
	sub := tr.nextSub(t)

	result := make(chan error, 1)
	go func() { result <- b.Sync(context.Background(), []string{"a"}) }()

	tr.nextCommand(t)
	_ = sub.Close()

	select {
	case err := <-result:
		if !errors.Is(err, ErrDisconnected) {
			t.Fatalf("Sync = %v, want ErrDisconnected", err)
		}
	case <-time.After(receiveTimeout):
		t.Fatalf("Sync still waiting %s after the connection died", receiveTimeout)
	}
}

// TestRedisSyncHonoursContextCancellationMidFlight is the other half of the same wait: the
// caller's context bounds it, not the transport.
func TestRedisSyncHonoursContextCancellationMidFlight(t *testing.T) {
	tr := newFakeTransport(false)
	b := fakeBus(t, tr, RedisOptions{})
	tr.nextSub(t)

	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- b.Sync(ctx, []string{"a"}) }()

	tr.nextCommand(t)
	cancel()

	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Sync = %v, want context.Canceled", err)
		}
	case <-time.After(receiveTimeout):
		t.Fatalf("Sync still waiting %s after its context was cancelled", receiveTimeout)
	}
}

// TestRedisSyncStopsWaitingOnClose is the third: Close must not leave a caller parked.
func TestRedisSyncStopsWaitingOnClose(t *testing.T) {
	tr := newFakeTransport(false)
	b := fakeBus(t, tr, RedisOptions{})
	tr.nextSub(t)

	result := make(chan error, 1)
	go func() { result <- b.Sync(context.Background(), []string{"a"}) }()

	tr.nextCommand(t)
	if err := b.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	select {
	case err := <-result:
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("Sync = %v, want ErrClosed", err)
		}
	case <-time.After(receiveTimeout):
		t.Fatalf("Sync still waiting %s after Close", receiveTimeout)
	}
}

// TestRedisReaderIgnoresEverythingButMessages: the reader does nothing but drain, and
// pongs, confirmations and anything a future server invents must not disturb it.
func TestRedisReaderIgnoresEverythingButMessages(t *testing.T) {
	tr := newFakeTransport(true)
	b := fakeBus(t, tr, RedisOptions{IntakeQueue: 8})
	sub := tr.nextSub(t)

	sub.deliver(&redis.Pong{Payload: "pong"})
	sub.deliver("something a future server invented")
	sub.deliver(&redis.Message{Channel: "a", Payload: "real"})

	mustReceive(t, b.Receive(), "a", "real")
}

// TestRedisPublishReportsTransportFailures: publish errors reach the caller, wrapped, and
// without the payload in the message (NFR-7).
func TestRedisPublishReportsTransportFailures(t *testing.T) {
	tr := newFakeTransport(true)
	tr.publishErr = errPublish
	b := fakeBus(t, tr, RedisOptions{})
	tr.nextSub(t)

	err := b.Publish(t.Context(), "a", []byte("secret payload"))
	if !errors.Is(err, errPublish) {
		t.Fatalf("Publish = %v, want the transport's error wrapped", err)
	}
	if got := err.Error(); strings.Contains(got, "secret payload") {
		t.Errorf("Publish error %q quotes the payload (NFR-7)", got)
	}
}

// TestRedisCloseReportsTransportFailures: Close reports what the transport said, and stays
// idempotent.
func TestRedisCloseReportsTransportFailures(t *testing.T) {
	tr := newFakeTransport(true)
	tr.closeErr = errClose
	b := fakeBus(t, tr, RedisOptions{})
	tr.nextSub(t)

	if err := b.Close(); !errors.Is(err, errClose) {
		t.Fatalf("Close = %v, want the transport's error wrapped", err)
	}
	if err := b.Close(); !errors.Is(err, errClose) {
		t.Fatalf("second Close = %v, want the same error", err)
	}
}

// TestRedisResyncFailureRetriesOnAFreshConnection covers the reconnect path's own sync
// failing. A half-applied subscription set is a set nobody can reason about, so the answer
// is another connection and another full resync rather than a partial state to patch up
// — the sweep-versus-command-stream race of M6 has no analogue here because there is
// nothing to race.
func TestRedisResyncFailureRetriesOnAFreshConnection(t *testing.T) {
	tr := newFakeTransport(true)
	b := fakeBus(t, tr, RedisOptions{})
	first := tr.nextSub(t)

	mustSync(t, b, "a")
	tr.nextCommand(t)

	tr.failNextSubscribe(errSubscribe)
	_ = first.Close()

	// The reconnect resubscribes, is refused, and drops that connection too.
	if got := tr.nextCommand(t); got.name != "subscribe" {
		t.Fatalf("the reconnect issued %+v, want a subscribe", got)
	}
	// The one after it succeeds, and the desired set recorded before the outage is what
	// lands. Nothing had to be re-derived from the hub.
	if got := tr.nextCommand(t); got.name != "subscribe" || got.args != 1 {
		t.Fatalf("the retry issued %+v, want subscribe with 1 channel", got)
	}
	mustSync(t, b, "a")
	if h := b.Health(); !h.Connected || h.Subscriptions != 1 {
		t.Errorf("Health = %+v, want connected with 1 subscription", h)
	}
}

// TestRedisSyncWhileDisconnectedNeverReportsSuccess is the case a diff alone would get
// wrong. While the bus is down its subscription set upstream is empty whatever the bus
// last confirmed, so a Sync that happens to match the last confirmed set must still fail:
// nil from Sync means the set is applied upstream, and the reconciler's dirty flag is the
// only thing standing between a transient outage and a channel that is locally held and
// upstream dead (M5).
func TestRedisSyncWhileDisconnectedNeverReportsSuccess(t *testing.T) {
	tr := newFakeTransport(true)
	b := fakeBus(t, tr, RedisOptions{})
	first := tr.nextSub(t)

	mustSync(t, b, "a")
	tr.nextCommand(t)

	tr.holdDown()
	_ = first.Close()
	tr.nextSub(t) // the next attempt has begun, so the old connection is already released

	if err := b.Sync(t.Context(), []string{"a"}); !errors.Is(err, ErrDisconnected) {
		t.Fatalf("Sync of an unchanged set while disconnected = %v, want ErrDisconnected", err)
	}
}

// TestRedisCloseDuringConnect covers the window between a connection being proved and
// being adopted. A connection adopted after Close has stopped looking for one is a
// connection nothing will ever close, and Close then waits on its reader forever.
func TestRedisCloseDuringConnect(t *testing.T) {
	tr := newFakeTransport(true)
	b := fakeBus(t, tr, RedisOptions{})
	first := tr.nextSub(t)

	gate := tr.gateNextPing()
	_ = first.Close()
	select {
	case <-tr.pinging:
	case <-time.After(receiveTimeout):
		t.Fatalf("no reconnection attempt within %s", receiveTimeout)
	}

	closed := make(chan error, 1)
	go func() { closed <- b.Close() }()

	// Close sets its flag before closing done, so this is a fence rather than a guess:
	// the supervisor released below is guaranteed to see a bus that is closing.
	<-b.done
	close(gate)

	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
	case <-time.After(receiveTimeout):
		t.Fatalf("Close still waiting %s after a connection was proved during it", receiveTimeout)
	}
	if h := b.Health(); h.Connected {
		t.Errorf("Health = %+v, want disconnected after Close", h)
	}
}

// ---------------------------------------------------------------------------
// Pure helpers.
// ---------------------------------------------------------------------------

func TestChunk(t *testing.T) {
	tests := []struct {
		name string
		in   int
		size int
		want []int
	}{
		{"empty", 0, 4, nil},
		{"one short chunk", 3, 4, []int{3}},
		{"exactly one chunk", 4, 4, []int{4}},
		{"one over", 5, 4, []int{4, 1}},
		{"several whole chunks", 8, 4, []int{4, 4}},
		// M7's number: 30,000 channels is 118 round trips at the default chunk, not
		// 30,000.
		{"a full resubscribe", 30000, DefaultChunkSize, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			in := make([]string, tt.in)
			got := chunk(in, tt.size)
			if tt.want == nil && tt.name == "a full resubscribe" {
				if len(got) != 118 {
					t.Fatalf("30000 channels became %d round trips, want 118", len(got))
				}
				return
			}
			sizes := make([]int, 0, len(got))
			for _, c := range got {
				sizes = append(sizes, len(c))
			}
			if !slices.Equal(sizes, tt.want) {
				t.Fatalf("chunk sizes %v, want %v", sizes, tt.want)
			}
		})
	}
}

func TestNextBackoff(t *testing.T) {
	const (
		floor   = 200 * time.Millisecond
		ceiling = 10 * time.Second
	)
	tests := []struct {
		name string
		cur  time.Duration
		want time.Duration
	}{
		{"first failure starts at the floor", 0, floor},
		{"doubles", floor, 400 * time.Millisecond},
		{"keeps doubling", 4 * time.Second, 8 * time.Second},
		{"clamps to the ceiling", 8 * time.Second, ceiling},
		{"stays at the ceiling", ceiling, ceiling},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := nextBackoff(tt.cur, floor, ceiling); got != tt.want {
				t.Errorf("nextBackoff(%s) = %s, want %s", tt.cur, got, tt.want)
			}
		})
	}
}

func TestNewRedisRejectsABadURL(t *testing.T) {
	if _, err := NewRedis(RedisOptions{URL: "not-a-redis-url"}); err == nil {
		t.Fatal("NewRedis accepted a URL that is not a Redis URL")
	}
}

// TestRedisDefaults asserts the documented defaults from docs/08-config.md §3 are what an
// unconfigured bus gets, rather than a zero value that would make the intake unbuffered
// and every chunk empty.
func TestRedisDefaults(t *testing.T) {
	tr := newFakeTransport(true)
	b := newRedisBus(tr, RedisOptions{})
	t.Cleanup(func() { _ = b.Close() })

	if got := cap(b.Receive()); got != DefaultIntakeQueue {
		t.Errorf("intake capacity = %d, want %d", got, DefaultIntakeQueue)
	}
	if b.chunk != DefaultChunkSize {
		t.Errorf("chunk size = %d, want %d", b.chunk, DefaultChunkSize)
	}
	if b.reconnectMin != DefaultReconnectMin || b.reconnectMax != DefaultReconnectMax {
		t.Errorf("backoff = [%s,%s], want [%s,%s]", b.reconnectMin, b.reconnectMax, DefaultReconnectMin, DefaultReconnectMax)
	}
	if b.clock == nil {
		t.Error("clock = nil, want time.Now")
	}
}
