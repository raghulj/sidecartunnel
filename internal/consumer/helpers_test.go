package consumer

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"log/slog"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/raghulj/sidecartunnel/internal/bus"
	"github.com/raghulj/sidecartunnel/internal/hub"
	"github.com/raghulj/sidecartunnel/internal/proto"
)

// waitFor is the generous failure detector docs/14-coding-standards.md §2 allows in place
// of a sleep: the happy path takes microseconds and the timeout only fires when the test
// was going to fail anyway.
const waitFor = 5 * time.Second

// testSecret is control.secret for these tests. It is over the 32-byte floor
// docs/08-config.md §3 sets, and it may not appear in any captured log line (NFR-7).
const testSecret = "control-secret-that-is-long-enough-for-the-32-byte-floor"

// testNow is the fixed clock the ±Skew window is measured against. A fake clock is what
// makes the boundary assertable exactly rather than by sleeping through it.
var testNow = time.Unix(1756612800, 0)

// sign builds the signed envelope of docs/04-integration.md §3: the action as an opaque
// JSON string, and a MAC over the exact bytes carried.
func sign(secret string, ts time.Time, body string) []byte {
	stamp := strconv.FormatInt(ts.Unix(), 10)
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write([]byte(stamp + "." + "nonce-1" + "." + body))
	payload, err := json.Marshal(Envelope{
		TS:    ts.Unix(),
		Nonce: "nonce-1",
		Body:  body,
		Sig:   hex.EncodeToString(mac.Sum(nil)),
	})
	if err != nil {
		panic("control envelope must marshal: " + err.Error())
	}
	return payload
}

// fixture is a memory bus, a hub on it, and a Consumer draining it — the fan-out path's
// top half exactly as the binary wires it (docs/09-internals.md §5).
type fixture struct {
	bus      *bus.MemoryBus
	hub      *hub.Hub
	consumer *Consumer
	logs     *syncBuffer

	flushed chan struct{}
	done    chan struct{}
	cancel  context.CancelFunc
}

// newFixture starts a Consumer with workers dispatch workers and maxMessage as
// limits.max_message_size; a non-positive maxMessage leaves the cap off.
func newFixture(t *testing.T, workers, maxMessage int) *fixture {
	t.Helper()
	b := bus.NewMemory(64)
	h := hub.New(context.Background(), b, hub.Options{Prefix: "st:", Separator: "-"})
	log, logs := capturingLogger()

	f := &fixture{
		bus:     b,
		hub:     h,
		logs:    logs,
		flushed: make(chan struct{}, 8),
		done:    make(chan struct{}),
	}
	f.consumer = New(Options{
		Bus:            b,
		Hub:            h,
		Log:            log,
		Secret:         []byte(testSecret),
		Workers:        workers,
		MaxMessageSize: maxMessage,
		Flush:          func() { f.flushed <- struct{}{} },
		Now:            func() time.Time { return testNow },
	})

	ctx, cancel := context.WithCancel(context.Background())
	f.cancel = cancel
	go func() {
		defer close(f.done)
		f.consumer.Run(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-f.done:
		case <-time.After(waitFor):
			t.Fatal("Run did not return within the budget after its context was cancelled")
		}
		h.Close()
		if err := b.Close(); err != nil {
			t.Errorf("close bus: %v", err)
		}
	})
	return f
}

// publish puts one raw payload on a bus key, the way an application's Redis PUBLISH does.
func (f *fixture) publish(t *testing.T, key string, payload []byte) {
	t.Helper()
	if err := f.bus.Publish(context.Background(), key, payload); err != nil {
		t.Fatalf("publish %s: %v", key, err)
	}
}

// subscribe registers a sink, gives it the named channels, and waits until the bus holds
// the resulting subscriptions.
//
// The wait is a synchronisation point rather than a delay: the hub reconciles off the
// request path, so a publish issued before Bus.Sync lands reaches nobody — correct
// behaviour, and a race in a test that publishes immediately.
func (f *fixture) subscribe(t *testing.T, s hub.Sink, channels ...string) {
	t.Helper()
	if err := f.hub.Add(s); err != nil {
		t.Fatalf("hub.Add: %v", err)
	}
	for _, ch := range channels {
		if err := f.hub.Subscribe(s, ch, nil); err != nil {
			t.Fatalf("hub.Subscribe(%s): %v", ch, err)
		}
	}
	// The control key plus one per channel.
	waitSubscribed(t, f.bus, 1+len(channels))
}

// waitSubscribed blocks until the bus holds at least n subscriptions.
func waitSubscribed(t *testing.T, b interface{ Health() bus.Health }, n int) {
	t.Helper()
	deadline := time.Now().Add(waitFor)
	for time.Now().Before(deadline) {
		if b.Health().Subscriptions >= n {
			return
		}
		runtime.Gosched()
	}
	t.Fatalf("the bus reached %d subscriptions within %s, want %d",
		b.Health().Subscriptions, waitFor, n)
}

// capturingLogger returns a debug-level logger and the buffer it writes to, for the
// assertions that a drop is logged with its channel and that no secret or payload is
// (NFR-7).
func capturingLogger() (*slog.Logger, *syncBuffer) {
	buf := &syncBuffer{}
	return slog.New(slog.NewJSONHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug})), buf
}

// syncBuffer is a bytes.Buffer safe to read while a goroutine is logging into it.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// testSink is a hub.Sink for these tests. The hub keys maps by Sink, so it must be a
// pointer: an interface holding an uncomparable value panics the moment it is inserted.
type testSink struct {
	id   string
	user string

	mu     sync.Mutex
	closes []proto.CloseCode

	delivered chan *proto.Frame
	closed    chan proto.CloseCode
}

func newTestSink(id, user string) *testSink {
	return &testSink{
		id:        id,
		user:      user,
		delivered: make(chan *proto.Frame, 64),
		closed:    make(chan proto.CloseCode, 8),
	}
}

func (s *testSink) ID() string   { return s.id }
func (s *testSink) User() string { return s.user }

func (s *testSink) Send(f *proto.Frame) bool {
	select {
	case s.delivered <- f:
	default:
	}
	return true
}

func (s *testSink) Close(code proto.CloseCode, _ string) {
	s.mu.Lock()
	s.closes = append(s.closes, code)
	s.mu.Unlock()
	select {
	case s.closed <- code:
	default:
	}
}

// closeCount reports how many times Close has been called.
func (s *testSink) closeCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.closes)
}

// waitFrame blocks until the sink accepts a frame, failing the test rather than hanging.
func (s *testSink) waitFrame(t *testing.T) *proto.Frame {
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
func (s *testSink) waitClose(t *testing.T) proto.CloseCode {
	t.Helper()
	select {
	case c := <-s.closed:
		return c
	case <-time.After(waitFor):
		t.Fatalf("sink %s: no close within %s", s.id, waitFor)
		return 0
	}
}
