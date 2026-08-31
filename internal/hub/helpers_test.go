package hub

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/raghulj/sidecartunnel/internal/bus"
	"github.com/raghulj/sidecartunnel/internal/config"
	"github.com/raghulj/sidecartunnel/internal/proto"
)

// waitFor is the generous failure detector docs/14-coding-standards.md §2 allows in place
// of a sleep: the happy path takes microseconds and the timeout only fires when the test
// was going to fail anyway.
const waitFor = 2 * time.Second

// errBus is the transient failure a fakeBus returns while it is configured to fail. M5:
// a Sync that fails must leave the desired set dirty and be retried, never swallowed.
var errBus = errors.New("bus unavailable")

// closeCall records one Sink.Close, so a test can assert the close code without reaching
// into the connection layer the hub deliberately cannot see.
type closeCall struct {
	code   proto.CloseCode
	reason string
}

// fakeSink is the test double for a connection. It is a pointer type because the hub
// keys maps by Sink and an interface holding an uncomparable value panics on insert.
type fakeSink struct {
	id   string
	user string

	// full makes the next Send refuse, which is the only way a connection tells the hub
	// its outbound queue overflowed (FR-15).
	full atomic.Bool

	// blockClose, when non-nil, parks Close until the test releases it. That is what
	// makes the closer-queue overflow path (docs/09-internals.md §4.3) deterministic:
	// a closer stuck inside Close cannot drain the queue.
	blockClose chan struct{}

	mu     sync.Mutex
	frames []*proto.Frame
	closes []closeCall

	delivered chan *proto.Frame
	closed    chan closeCall
}

func newSink(id, user string) *fakeSink {
	return &fakeSink{
		id:        id,
		user:      user,
		delivered: make(chan *proto.Frame, 4096),
		closed:    make(chan closeCall, 8),
	}
}

func (s *fakeSink) ID() string   { return s.id }
func (s *fakeSink) User() string { return s.user }

func (s *fakeSink) Send(f *proto.Frame) bool {
	if s.full.Load() {
		return false
	}
	s.mu.Lock()
	s.frames = append(s.frames, f)
	s.mu.Unlock()
	select {
	case s.delivered <- f:
	default:
	}
	return true
}

func (s *fakeSink) Close(code proto.CloseCode, reason string) {
	s.mu.Lock()
	s.closes = append(s.closes, closeCall{code: code, reason: reason})
	s.mu.Unlock()
	select {
	case s.closed <- closeCall{code: code, reason: reason}:
	default:
	}
	if s.blockClose != nil {
		<-s.blockClose
	}
}

// got returns every frame this sink accepted, in order.
func (s *fakeSink) got() []*proto.Frame {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]*proto.Frame(nil), s.frames...)
}

// closeCount reports how many times Close has been called.
func (s *fakeSink) closeCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.closes)
}

// waitFrame blocks until the sink accepts a frame, failing the test rather than hanging.
func (s *fakeSink) waitFrame(t *testing.T) *proto.Frame {
	t.Helper()
	select {
	case f := <-s.delivered:
		return f
	case <-time.After(waitFor):
		t.Fatalf("sink %s: no frame within %s", s.id, waitFor)
		return nil
	}
}

// waitClose blocks until the sink is closed, failing the test rather than hanging.
func (s *fakeSink) waitClose(t *testing.T) closeCall {
	t.Helper()
	select {
	case c := <-s.closed:
		return c
	case <-time.After(waitFor):
		t.Fatalf("sink %s: no close within %s", s.id, waitFor)
		return closeCall{}
	}
}

// fakeBus is the frozen bus.Bus contract, instrumented and blockable.
type fakeBus struct {
	mu    sync.Mutex
	calls [][]string
	raw   [][]string
	err   error
	gate  chan struct{}

	synced    chan []string
	published chan busPublish
	msgs      chan bus.Message
}

type busPublish struct {
	channel string
	payload []byte
}

func newBus() *fakeBus {
	return &fakeBus{
		synced:    make(chan []string, 4096),
		published: make(chan busPublish, 64),
		msgs:      make(chan bus.Message),
	}
}

// wedge makes every Sync block until release is called or the hub's context ends. This is
// "Redis merely slow" — the exact condition that stalled all fan-out on the replica (C7).
func (b *fakeBus) wedge() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.gate = make(chan struct{})
}

func (b *fakeBus) release() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.gate != nil {
		close(b.gate)
		b.gate = nil
	}
}

func (b *fakeBus) fail(err error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.err = err
}

func (b *fakeBus) Sync(ctx context.Context, desired []string) error {
	got := append([]string(nil), desired...)
	sort.Strings(got)

	b.mu.Lock()
	b.calls = append(b.calls, got)
	// The raw slice is kept alongside the copy purely so a test can prove the hub hands
	// out a fresh backing array per pass. A real bus must not retain it.
	b.raw = append(b.raw, desired)
	gate, err := b.gate, b.err
	b.mu.Unlock()

	select {
	case b.synced <- got:
	default:
	}

	if gate != nil {
		select {
		case <-gate:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return err
}

func (b *fakeBus) Publish(_ context.Context, channel string, payload []byte) error {
	b.mu.Lock()
	err := b.err
	b.mu.Unlock()
	if err != nil {
		return err
	}
	select {
	case b.published <- busPublish{channel: channel, payload: payload}:
	default:
	}
	return nil
}

func (b *fakeBus) Receive() <-chan bus.Message { return b.msgs }

func (b *fakeBus) Close() error {
	close(b.msgs)
	return nil
}

// syncCalls returns every desired set the hub has pushed, sorted within each call.
func (b *fakeBus) syncCalls() [][]string {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([][]string, len(b.calls))
	copy(out, b.calls)
	return out
}

// drainSynced discards notifications for reconciliations that have already happened, so
// a following waitSync can only be satisfied by a new one. Safe only while the reconciler
// is parked, which is exactly when a test holds an unfired backoff request.
func (b *fakeBus) drainSynced() {
	for {
		select {
		case <-b.synced:
		default:
			return
		}
	}
}

// rawCalls returns the exact slices the hub passed to Sync, unsorted and uncopied.
func (b *fakeBus) rawCalls() [][]string {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([][]string, len(b.raw))
	copy(out, b.raw)
	return out
}

// waitSync blocks until the hub reconciles a desired set equal to want, failing rather
// than hanging. Reconciliation coalesces, so a test asserts convergence, not call counts.
func (b *fakeBus) waitSync(t *testing.T, want ...string) {
	t.Helper()
	sort.Strings(want)
	deadline := time.After(waitFor)
	for {
		select {
		case got := <-b.synced:
			if equalStrings(got, want) {
				return
			}
		case <-deadline:
			t.Fatalf("no Sync with %v within %s; calls: %v", want, waitFor, b.syncCalls())
		}
	}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// waitRequest is one pending backoff sleep, handed to the test so a retry schedule can be
// asserted and advanced without a clock.
type waitRequest struct {
	d  time.Duration
	ch chan time.Time
}

// fakeClock replaces time.After in the reconciler. Its After blocks until the test takes
// the request, which is what lets a retry test be exact instead of timed.
type fakeClock struct {
	requests chan waitRequest
}

func newClock() *fakeClock {
	return &fakeClock{requests: make(chan waitRequest, 16)}
}

func (c *fakeClock) After(d time.Duration) <-chan time.Time {
	ch := make(chan time.Time, 1)
	c.requests <- waitRequest{d: d, ch: ch}
	return ch
}

// next takes the next pending backoff sleep without releasing it. Taking it
// happens-after the Sync call that scheduled it, which is what lets a test change the
// bus's behaviour between two attempts without racing the reconciler.
func (c *fakeClock) next(t *testing.T) waitRequest {
	t.Helper()
	select {
	case req := <-c.requests:
		return req
	case <-time.After(waitFor):
		t.Fatalf("no backoff sleep requested within %s", waitFor)
		return waitRequest{}
	}
}

// fire releases one pending backoff sleep, letting the next attempt run.
func fire(req waitRequest) { req.ch <- time.Time{} }

// newTestHub builds a hub with the reserved empty-name namespace block, the default
// prefix, and a cleanup that proves the goroutines exit (NFR-3).
func newTestHub(t *testing.T, b bus.Bus, mutate ...func(*Options)) *Hub {
	t.Helper()
	opts := Options{
		Prefix:     "st:",
		Separator:  "-",
		Namespaces: []config.Namespace{{Name: ""}},
	}
	for _, m := range mutate {
		m(&opts)
	}
	h := New(context.Background(), b, opts)
	t.Cleanup(h.Close)
	return h
}

// mustAdd registers a sink, failing the test if the hub refuses.
func mustAdd(t *testing.T, h *Hub, sinks ...*fakeSink) {
	t.Helper()
	for _, s := range sinks {
		if err := h.Add(s); err != nil {
			t.Fatalf("Add(%s): %v", s.id, err)
		}
	}
}

// mustSubscribe subscribes and fails the test on any error.
func mustSubscribe(t *testing.T, h *Hub, s Sink, channels ...string) {
	t.Helper()
	for _, ch := range channels {
		if err := h.Subscribe(s, ch, nil); err != nil {
			t.Fatalf("Subscribe(%s): %v", ch, err)
		}
	}
}

// insertUnderLock and dropUnderLock take the write lock the hub's own callers hold
// around insertLocked and dropLocked, so a test can assert the FR-10 refcount edges
// directly. The locking moved out of those two when the ack had to be queued in the same
// critical section as the mutation (M15).
func insertUnderLock(h *Hub, s Sink, key string) (bool, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.insertLocked(s, key)
}

func dropUnderLock(h *Hub, s Sink, key string) (bool, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.dropLocked(s, key)
}

// desiredSnapshot reads the reconciler's target set under the hub lock.
func desiredSnapshot(h *Hub) []string {
	got := h.snapshot()
	sort.Strings(got)
	return got
}

// channelKeys lists the hub's map keys, for failure messages only.
func (h *Hub) channelKeys() []string {
	h.mu.RLock()
	defer h.mu.RUnlock()
	out := make([]string, 0, len(h.channels))
	for k := range h.channels {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// timeoutAfter is the shared failure detector for a select that must not hang.
func timeoutAfter() <-chan time.Time { return time.After(waitFor) }

// decodeFrame unmarshals an encoded outbound frame, so an assertion reads the shape a
// client would see rather than a substring of JSON.
func decodeFrame(t *testing.T, f *proto.Frame, v any) {
	t.Helper()
	if err := json.Unmarshal(f.Data, v); err != nil {
		t.Fatalf("decode frame %s: %v", f.Data, err)
	}
}
