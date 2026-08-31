package main

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/raghulj/sidecartunnel/internal/bus"
	"github.com/raghulj/sidecartunnel/internal/hub"
	"github.com/raghulj/sidecartunnel/internal/metrics"
	"github.com/raghulj/sidecartunnel/internal/proto"
)

// consumerFixture is a memory bus, a hub on it, and a consumer draining it — the top half
// of the fan-out path exactly as main wires it (docs/09-internals.md §5).
type consumerFixture struct {
	bus      *bus.MemoryBus
	hub      *hub.Hub
	reg      *prometheus.Registry
	consumer *consumer
	logs     *syncBuffer

	flushed chan struct{}
}

func newConsumerFixture(t *testing.T, workers int) *consumerFixture {
	t.Helper()
	reg := prometheus.NewRegistry()
	m, err := metrics.New(reg, metrics.Options{App: "app", Separator: "-", Namespaces: []string{"", "room"}})
	if err != nil {
		t.Fatalf("metrics.New: %v", err)
	}
	b := bus.NewMemory(64)
	h := hub.New(context.Background(), b, hub.Options{Prefix: "st:", Separator: "-"})
	log, logs := capturingLogger(t, slogDebug)

	f := &consumerFixture{bus: b, hub: h, reg: reg, logs: logs, flushed: make(chan struct{}, 8)}
	f.consumer = newConsumer(b, h, m, log, []byte(testControlSecret), "st:", workers,
		func() { f.flushed <- struct{}{} }, func() time.Time { return controlNow })

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		f.consumer.run(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
		h.Close()
		if err := b.Close(); err != nil {
			t.Errorf("close bus: %v", err)
		}
	})
	return f
}

// publish puts one raw payload on a bus key, the way an application's Redis PUBLISH does.
func (f *consumerFixture) publish(t *testing.T, key string, payload []byte) {
	t.Helper()
	if err := f.bus.Publish(context.Background(), key, payload); err != nil {
		t.Fatalf("publish %s: %v", key, err)
	}
}

// subscribe registers a sink and gives it one channel.
func (f *consumerFixture) subscribe(t *testing.T, s hub.Sink, channels ...string) {
	t.Helper()
	if err := f.hub.Add(s); err != nil {
		t.Fatalf("hub.Add: %v", err)
	}
	for _, ch := range channels {
		if err := f.hub.Subscribe(s, ch, nil); err != nil {
			t.Fatalf("hub.Subscribe(%s): %v", ch, err)
		}
	}
	// The control key plus one per channel: a publish before Bus.Sync has landed reaches
	// nobody, which is correct and is a race in a test that publishes immediately.
	waitSubscribed(t, f.bus, 1+len(channels))
}

// TestConsumer_FansOutABusMessage_FR12 is the delivery path below the socket: a publish on
// a bus key reaches every subscriber this replica holds, and the message is counted under
// its namespace rather than under its channel name (docs/06-channels.md §2).
func TestConsumer_FansOutABusMessage_FR12(t *testing.T) {
	f := newConsumerFixture(t, 2)
	s := newTestSink("c1", "u-7")
	f.subscribe(t, s, "room-4410")

	f.publish(t, "st:room-4410", []byte(`{"event":"x","data":{"id":1}}`))

	frame := s.waitFrame(t)
	if !strings.Contains(string(frame.Data), `"channel":"room-4410"`) {
		t.Fatalf("frame = %s, want a push for room-4410", frame.Data)
	}
	if got := metricValue(t, f.reg, "st_messages_published_total", map[string]string{"namespace": "room"}); got != 1 {
		t.Fatalf("st_messages_published_total{namespace=room} = %v, want 1", got)
	}
}

// TestConsumer_MalformedEnvelopeIsDroppedAndCounted covers docs/04-integration.md §2.2: an
// envelope missing event or data is dropped and counted in
// st_messages_dropped_total{reason="malformed"}, and its payload never reaches the log.
//
// A single worker makes the ordering a synchronisation point rather than a sleep: the
// good message that follows cannot be delivered until the bad one has been handled.
func TestConsumer_MalformedEnvelopeIsDroppedAndCounted(t *testing.T) {
	f := newConsumerFixture(t, 1)
	s := newTestSink("c1", "u-7")
	f.subscribe(t, s, "room-4410")

	f.publish(t, "st:room-4410", []byte(`{"data":{"secret":"hunter2"}}`))
	f.publish(t, "st:room-4410", []byte(`{"event":"x","data":{}}`))
	s.waitFrame(t)

	if got := metricValue(t, f.reg, "st_messages_dropped_total", map[string]string{"reason": "malformed"}); got != 1 {
		t.Fatalf("st_messages_dropped_total{reason=malformed} = %v, want 1", got)
	}
	if strings.Contains(f.logs.String(), "hunter2") {
		t.Fatalf("a dropped payload reached the log (NFR-7): %q", f.logs.String())
	}
}

// TestConsumer_AppliesASignedControlMessage_FR23 is the control path: a signed disconnect
// closes the targeted connection with 3501 and flushes the webhook cache, because a cached
// entry otherwise survives the revocation (docs/13-review-findings.md C4).
func TestConsumer_AppliesASignedControlMessage_FR23(t *testing.T) {
	f := newConsumerFixture(t, 2)
	s := newTestSink("c1", "u-7")
	f.subscribe(t, s)

	body := `{"action":"disconnect","user":"u-7","reason":"suspended"}`
	f.publish(t, f.hub.ControlKey(), signControl(testControlSecret, controlNow, body))

	if got := s.waitClose(t); got != proto.CloseRevoked {
		t.Fatalf("close code = %d, want %d", got, proto.CloseRevoked)
	}
	select {
	case <-f.flushed:
	case <-time.After(waitFor):
		t.Fatal("a control disconnect did not flush the webhook cache (C4)")
	}
}

// TestConsumer_ControlRejections asserts the counter an operator alerts on and that
// nothing is applied. Every row is followed by a valid disconnect, which the single
// control goroutine cannot reach until the rejected one has been counted — a
// synchronisation point rather than a sleep.
func TestConsumer_ControlRejections(t *testing.T) {
	tests := []struct {
		name    string
		payload []byte
		reason  string
	}{
		{
			name:    "unsigned",
			payload: []byte(`{"ts":1,"nonce":"n","body":"{}","sig":"00"}`),
			reason:  "unsigned",
		},
		{
			name:    "stale",
			payload: signControl(testControlSecret, controlNow.Add(-controlSkew-time.Second), `{"action":"refresh","user":"u-7"}`),
			reason:  "stale",
		},
		{
			name:    "unknown action",
			payload: signControl(testControlSecret, controlNow, `{"action":"vaporize","user":"u-7"}`),
			reason:  "malformed",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newConsumerFixture(t, 1)
			rejected := newTestSink("c1", "u-7")
			f.subscribe(t, rejected)

			f.publish(t, f.hub.ControlKey(), tt.payload)
			f.publish(t, f.hub.ControlKey(),
				signControl(testControlSecret, controlNow, `{"action":"disconnect","user":"u-7"}`))
			rejected.waitClose(t)

			if got := metricValue(t, f.reg, "st_control_rejected_total", map[string]string{"reason": tt.reason}); got != 1 {
				t.Fatalf("st_control_rejected_total{reason=%s} = %v, want 1", tt.reason, got)
			}
			if got := rejected.closeCount(); got != 1 {
				t.Fatalf("connection closed %d times; the rejected message was applied too (FR-23)", got)
			}
			if strings.Contains(f.logs.String(), testControlSecret) {
				t.Fatal("control.secret reached a log line (NFR-7)")
			}
		})
	}
}

// TestDispatch_ReturnsWhenTheBusCloses covers the other way a worker ends. The intake
// channel is closed only when the Bus is closed, and a worker that spun on a closed
// channel would burn a core for the life of the process.
func TestDispatch_ReturnsWhenTheBusCloses(t *testing.T) {
	c := bareConsumer(t, 1)
	in := make(chan bus.Message)
	close(in)

	done := make(chan struct{})
	go func() {
		defer close(done)
		c.dispatch(context.Background(), in)
	}()
	select {
	case <-done:
	case <-time.After(waitFor):
		t.Fatal("dispatch did not return on a closed intake channel")
	}
}

// TestDispatch_ControlBackpressureRespectsTheContext covers the one place a worker can
// wait: handing a control message to the control goroutine. It waits rather than dropping
// — losing a revocation is the one message loss that cannot be tolerated — and it stops
// waiting when the process is shutting down.
func TestDispatch_ControlBackpressureRespectsTheContext(t *testing.T) {
	c := bareConsumer(t, 1)
	for len(c.controlq) < cap(c.controlq) {
		c.controlq <- bus.Message{Channel: c.hub.ControlKey()}
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	in := make(chan bus.Message)
	go func() {
		defer close(done)
		c.dispatch(ctx, in)
	}()

	// The channel is unbuffered, so this send returns only once the worker has taken the
	// message and is inside the inner select. Cancelling before that would let the outer
	// select win, and the test would pass without ever reaching the branch it is about.
	in <- bus.Message{Channel: c.hub.ControlKey(), Payload: []byte("{}")}
	cancel()
	select {
	case <-done:
	case <-time.After(waitFor):
		t.Fatal("dispatch did not return on a cancelled context with a full control queue")
	}
}

// TestNewConsumer_Defaults keeps a zero-valued wiring usable: a consumer with no workers
// is a gateway that accepts connections, reports healthy and delivers nothing.
func TestNewConsumer_Defaults(t *testing.T) {
	c := bareConsumer(t, 0)
	if c.workers != 1 {
		t.Fatalf("workers = %d, want 1", c.workers)
	}
	if c.now == nil || c.flush == nil {
		t.Fatal("newConsumer left the clock or the flush hook nil")
	}
	c.flush()
	c.now()
}

// TestConsumer_DispatchIsParallel proves the workers are separate goroutines rather than
// one loop: two messages are in flight at once. docs/09-internals.md §5 splits the reader
// from the workers precisely so this is true, and a single worker would be a fan-out that
// serializes behind its slowest recipient.
func TestConsumer_DispatchIsParallel(t *testing.T) {
	f := newConsumerFixture(t, 2)

	var wg sync.WaitGroup
	wg.Add(2)
	blocker := &blockingSink{
		id: "c1", user: "u-7",
		entered: make(chan struct{}, 2),
		release: make(chan struct{}),
		wg:      &wg,
	}
	f.subscribe(t, blocker, "room-1")

	f.publish(t, "st:room-1", []byte(`{"event":"a","data":{}}`))
	f.publish(t, "st:room-1", []byte(`{"event":"b","data":{}}`))

	for range 2 {
		select {
		case <-blocker.entered:
		case <-time.After(waitFor):
			t.Fatal("only one message was in flight; bus.dispatch_workers is not honoured")
		}
	}
	close(blocker.release)
	wg.Wait()
}

// bareConsumer builds a consumer whose goroutines the test starts itself, for the paths
// that are about one loop rather than about the whole path.
func bareConsumer(t *testing.T, workers int) *consumer {
	t.Helper()
	m, err := metrics.New(prometheus.NewRegistry(), metrics.Options{})
	if err != nil {
		t.Fatalf("metrics.New: %v", err)
	}
	b := bus.NewMemory(4)
	h := hub.New(context.Background(), b, hub.Options{})
	t.Cleanup(func() {
		h.Close()
		_ = b.Close()
	})
	return newConsumer(b, h, m, discardLogger(), []byte(testControlSecret), "st:", workers, nil, nil)
}

// blockingSink parks inside Send until it is released, so a test can observe how many
// messages are in flight at once. It deliberately breaks hub.Sink's "never blocks"
// contract, which is the only way to see the concurrency from outside.
type blockingSink struct {
	id      string
	user    string
	entered chan struct{}
	release chan struct{}
	wg      *sync.WaitGroup
}

func (s *blockingSink) ID() string   { return s.id }
func (s *blockingSink) User() string { return s.user }

func (s *blockingSink) Send(*proto.Frame) bool {
	s.entered <- struct{}{}
	<-s.release
	s.wg.Done()
	return true
}

func (s *blockingSink) Close(proto.CloseCode, string) {}
