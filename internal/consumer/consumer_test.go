package consumer

import (
	"context"
	"log/slog"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/raghulj/sidecartunnel/internal/bus"
	"github.com/raghulj/sidecartunnel/internal/hub"
	"github.com/raghulj/sidecartunnel/internal/proto"
)

// TestConsumer_FansOutABusMessage_FR12 is the delivery path below the socket: a publish
// on a bus key reaches every subscriber this replica holds, and the frame that arrives
// names the bare channel rather than the prefixed bus key (FR-21).
func TestConsumer_FansOutABusMessage_FR12(t *testing.T) {
	f := newFixture(t, 2, 0)
	s := newTestSink("c1", "u-7")
	f.subscribe(t, s, "room-4410")

	f.publish(t, "st:room-4410", []byte(`{"event":"x","data":{"id":1}}`))

	frame := s.waitFrame(t)
	if !strings.Contains(string(frame.Data), `"channel":"room-4410"`) {
		t.Fatalf("frame = %s, want a push for room-4410", frame.Data)
	}
	if got := f.consumer.Stats().Dispatched; got != 1 {
		t.Fatalf("Stats().Dispatched = %d, want 1", got)
	}
}

// TestConsumer_MalformedEnvelopeIsDroppedAndCounted covers docs/04-integration.md §2.2: an
// envelope missing event or data is dropped, counted, and logged with the channel it was
// published to — never with its payload, which is still an application's data (NFR-7).
//
// A single worker makes the ordering a synchronisation point rather than a sleep: the good
// message that follows cannot be delivered until the bad one has been handled. That the
// good one arrives at all is the assertion that the drop did not stop the worker.
func TestConsumer_MalformedEnvelopeIsDroppedAndCounted(t *testing.T) {
	f := newFixture(t, 1, 0)
	s := newTestSink("c1", "u-7")
	f.subscribe(t, s, "room-4410")

	f.publish(t, "st:room-4410", []byte(`{"data":{"secret":"hunter2"}}`))
	f.publish(t, "st:room-4410", []byte(`{"event":"x","data":{}}`))
	s.waitFrame(t)

	stats := f.consumer.Stats()
	if stats.DroppedMalformed != 1 || stats.Dispatched != 1 {
		t.Fatalf("Stats() = %+v, want one malformed drop and one dispatch", stats)
	}
	logs := f.logs.String()
	if !strings.Contains(logs, `"channel":"st:room-4410"`) || !strings.Contains(logs, `"reason":"malformed"`) {
		t.Fatalf("the drop was not logged with its channel and reason:\n%s", logs)
	}
	if strings.Contains(logs, "hunter2") {
		t.Fatalf("a dropped payload reached the log (NFR-7): %q", logs)
	}
}

// TestConsumer_MalformedJSONNeverLogsAPayloadByte_NFR7: the comment above deliver claims
// the payload is never logged, and it was not true.
//
// hub.Dispatch wraps json.Unmarshal's error, and a *json.SyntaxError reads
// `invalid character 'Z' looking for beginning of value` — where Z is one byte of the
// publisher's payload, chosen by whoever published it. NFR-7 is absolute: no level, not
// even debug, and not one byte. The reason still has to be logged, so the offset is kept
// and the character is not.
func TestConsumer_MalformedJSONNeverLogsAPayloadByte_NFR7(t *testing.T) {
	f := newFixture(t, 1, 0)
	s := newTestSink("c1", "u-7")
	f.subscribe(t, s, "room-4410")

	f.publish(t, "st:room-4410", []byte(`{"event":"x","data":Z}`))
	f.publish(t, "st:room-4410", []byte(`{"event":"x","data":{}}`))
	s.waitFrame(t)

	logs := f.logs.String()
	if !strings.Contains(logs, `"reason":"malformed"`) {
		t.Fatalf("the drop was not logged, so the assertion below is vacuous:\n%s", logs)
	}
	if strings.Contains(logs, "'Z'") {
		t.Fatalf("a byte of the payload reached the log (NFR-7):\n%s", logs)
	}
	// Still diagnosable: an operator has to be able to tell "not JSON" from "no event".
	if !strings.Contains(logs, "offset") {
		t.Fatalf("the drop reason says nothing about why the envelope did not decode:\n%s", logs)
	}
}

// TestConsumer_MalformedControlNeverLogsAPayloadByte_NFR7 is the same leak on the control
// channel, where it is logged at warn rather than debug. Anyone who can publish to Redis
// can publish to the control key, so the byte in the message is attacker-chosen.
func TestConsumer_MalformedControlNeverLogsAPayloadByte_NFR7(t *testing.T) {
	f := newFixture(t, 1, 0)
	s := newTestSink("c1", "u-7")
	f.subscribe(t, s)

	// The valid disconnect behind it is the synchronisation point: the single control
	// goroutine cannot reach it until the rejected message has been logged and counted.
	f.publish(t, f.hub.ControlKey(), []byte(`{"ts":1,"body":Z}`))
	f.publish(t, f.hub.ControlKey(), sign(testSecret, testNow, `{"action":"disconnect","user":"u-7"}`))
	s.waitClose(t)

	logs := f.logs.String()
	if !strings.Contains(logs, `"reason":"malformed"`) {
		t.Fatalf("the rejection was not logged, so the assertion below is vacuous:\n%s", logs)
	}
	if strings.Contains(logs, "'Z'") {
		t.Fatalf("a byte of the control payload reached the log (NFR-7):\n%s", logs)
	}
}

// TestConsumer_OversizeEnvelopeIsDroppedAndCounted_FR14 is FR-14: an envelope larger than
// limits.max_message_size is dropped and logged once with the channel name and reason
// "oversize". FR-14 is the one requirement whose acceptance criterion is the log line
// itself, so this asserts on it exactly (docs/11-testing.md §8).
func TestConsumer_OversizeEnvelopeIsDroppedAndCounted_FR14(t *testing.T) {
	f := newFixture(t, 1, 64)
	s := newTestSink("c1", "u-7")
	f.subscribe(t, s, "room-4410")

	big := `{"event":"x","data":{"filler":"` + strings.Repeat("z", 200) + `"}}`
	f.publish(t, "st:room-4410", []byte(big))
	f.publish(t, "st:room-4410", []byte(`{"event":"x","data":{}}`))
	s.waitFrame(t)

	stats := f.consumer.Stats()
	if stats.DroppedOversize != 1 || stats.Dispatched != 1 {
		t.Fatalf("Stats() = %+v, want one oversize drop and one dispatch", stats)
	}
	logs := f.logs.String()
	if !strings.Contains(logs, `"channel":"st:room-4410"`) || !strings.Contains(logs, `"reason":"oversize"`) {
		t.Fatalf("the oversize drop was not logged with its channel and reason (FR-14):\n%s", logs)
	}
	if strings.Contains(logs, "zzz") {
		t.Fatalf("an oversize payload reached the log (NFR-7): %q", logs)
	}
}

// TestConsumer_AppliesASignedControlMessage_FR23 is the control path: a signed disconnect
// closes the targeted connection with 3501 and flushes the webhook cache, because a cached
// entry otherwise survives the revocation (docs/13-review-findings.md C4).
func TestConsumer_AppliesASignedControlMessage_FR23(t *testing.T) {
	f := newFixture(t, 2, 0)
	s := newTestSink("c1", "u-7")
	f.subscribe(t, s)

	body := `{"action":"disconnect","user":"u-7","reason":"suspended"}`
	f.publish(t, f.hub.ControlKey(), sign(testSecret, testNow, body))

	if got := s.waitClose(t); got != proto.CloseRevoked {
		t.Fatalf("close code = %d, want %d", got, proto.CloseRevoked)
	}
	select {
	case <-f.flushed:
	case <-time.After(waitFor):
		t.Fatal("a control disconnect did not flush the webhook cache (C4)")
	}
	if got := f.consumer.Stats().ControlApplied; got != 1 {
		t.Fatalf("Stats().ControlApplied = %d, want 1", got)
	}
}

// TestConsumer_ControlRejections asserts the counters an operator alerts on, that the
// reason is distinguishable per kind, and that nothing was applied.
//
// Every row is followed by a valid disconnect, which the single control goroutine cannot
// reach until the rejected one has been counted — a synchronisation point rather than a
// sleep.
func TestConsumer_ControlRejections(t *testing.T) {
	tests := []struct {
		name    string
		payload []byte
		reason  Reason
		count   func(Stats) int64
	}{
		{
			name:    "unsigned",
			payload: []byte(`{"ts":1,"nonce":"n","body":"{}","sig":"00"}`),
			reason:  ReasonUnsigned,
			count:   func(s Stats) int64 { return s.ControlUnsigned },
		},
		{
			name:    "wrong secret",
			payload: sign("a-different-secret-that-is-also-long-enough", testNow, `{"action":"refresh","user":"u-7"}`),
			reason:  ReasonUnsigned,
			count:   func(s Stats) int64 { return s.ControlUnsigned },
		},
		{
			name:    "stale",
			payload: sign(testSecret, testNow.Add(-Skew-time.Second), `{"action":"refresh","user":"u-7"}`),
			reason:  ReasonStale,
			count:   func(s Stats) int64 { return s.ControlStale },
		},
		{
			name:    "unknown action",
			payload: sign(testSecret, testNow, `{"action":"vaporize","user":"u-7"}`),
			reason:  ReasonMalformed,
			count:   func(s Stats) int64 { return s.ControlMalformed },
		},
		{
			// The body verified, and is valid JSON that is not an object. It must be
			// refused rather than panic anything downstream of the signature check.
			name:    "body is not an object",
			payload: sign(testSecret, testNow, `[1,2,3]`),
			reason:  ReasonMalformed,
			count:   func(s Stats) int64 { return s.ControlMalformed },
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFixture(t, 1, 0)
			s := newTestSink("c1", "u-7")
			f.subscribe(t, s)

			f.publish(t, f.hub.ControlKey(), tt.payload)
			f.publish(t, f.hub.ControlKey(), sign(testSecret, testNow, `{"action":"disconnect","user":"u-7"}`))
			s.waitClose(t)

			stats := f.consumer.Stats()
			if got := tt.count(stats); got != 1 {
				t.Fatalf("the %s counter = %d, want 1: %+v", tt.reason, got, stats)
			}
			if stats.ControlRejected != 1 {
				t.Fatalf("Stats().ControlRejected = %d, want 1", stats.ControlRejected)
			}
			if stats.ControlApplied != 1 {
				t.Fatalf("Stats().ControlApplied = %d, want exactly the one valid message", stats.ControlApplied)
			}
			if want := `"reason":"` + string(tt.reason) + `"`; !strings.Contains(f.logs.String(), want) {
				t.Fatalf("the rejection was not logged with %s:\n%s", want, f.logs.String())
			}
			if got := s.closeCount(); got != 1 {
				t.Fatalf("connection closed %d times; the rejected message was applied too (FR-23)", got)
			}
			if strings.Contains(f.logs.String(), testSecret) {
				t.Fatal("control.secret reached a log line (NFR-7)")
			}
		})
	}
}

// TestConsumer_ControlChannelNeverReachesDispatch is half of the routing rule: a message
// on {bus.prefix}_control is applied as control and is never fanned out.
func TestConsumer_ControlChannelNeverReachesDispatch(t *testing.T) {
	f := newFixture(t, 2, 0)
	s := newTestSink("c1", "u-7")
	f.subscribe(t, s)

	f.publish(t, f.hub.ControlKey(), sign(testSecret, testNow, `{"action":"disconnect","user":"u-7"}`))
	s.waitClose(t)

	stats := f.consumer.Stats()
	if stats.Dispatched != 0 || stats.DroppedMalformed != 0 {
		t.Fatalf("Stats() = %+v, want a control message to reach the hub's fan-out not at all", stats)
	}
}

// TestConsumer_NormalChannelNeverReachesControl is the other half: a signed control
// envelope published to an ordinary channel is an ordinary message, and an ordinary
// message that happens to be a valid disconnect must not disconnect anybody. Otherwise
// control.secret would be enough to revoke through any channel an application publishes
// to, rather than through the one reserved channel FR-23 names.
func TestConsumer_NormalChannelNeverReachesControl(t *testing.T) {
	f := newFixture(t, 1, 0)
	s := newTestSink("c1", "u-7")
	f.subscribe(t, s, "room-4410")

	f.publish(t, "st:room-4410", sign(testSecret, testNow, `{"action":"disconnect","user":"u-7"}`))
	f.publish(t, "st:room-4410", []byte(`{"event":"x","data":{}}`))
	s.waitFrame(t)

	stats := f.consumer.Stats()
	if stats.ControlApplied != 0 || stats.ControlRejected != 0 {
		t.Fatalf("Stats() = %+v, want an ordinary channel to reach Hub.Control not at all", stats)
	}
	if stats.DroppedMalformed != 1 {
		t.Fatalf("Stats() = %+v, want the control envelope dropped as a malformed push envelope", stats)
	}
	if got := s.closeCount(); got != 0 {
		t.Fatalf("the connection was closed %d times by a control message on an ordinary channel (FR-23)", got)
	}
}

// TestConsumer_DispatchIsParallel proves the workers are separate goroutines rather than
// one loop: two messages are in flight at once. docs/09-internals.md §5 splits the reader
// from the workers precisely so this is true, and a single worker would be a fan-out that
// serializes behind its slowest recipient.
func TestConsumer_DispatchIsParallel(t *testing.T) {
	f := newFixture(t, 2, 0)

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

// TestRun_ReturnsWhenTheBusCloses covers the other way every goroutine ends. The intake
// channel is closed only when the Bus is closed, and a worker that spun on a closed
// channel would burn a core for the life of the process. The control goroutine ends with
// them, without waiting for a context that may never be cancelled.
func TestRun_ReturnsWhenTheBusCloses(t *testing.T) {
	c, b, h := bareConsumer(t, 2)
	done := make(chan struct{})
	go func() {
		defer close(done)
		c.Run(context.Background())
	}()

	if err := b.Close(); err != nil {
		t.Fatalf("close bus: %v", err)
	}
	select {
	case <-done:
	case <-time.After(waitFor):
		t.Fatal("Run did not return when the bus closed its intake channel")
	}
	h.Close()
}

// TestRun_StopsOnContextCancelWithNoLeak is NFR-3 for this package: Run returns only once
// every goroutine it started has exited, so the caller may close the hub straight
// afterwards — Hub.Close must not race a Dispatch (docs/09-internals.md §8).
//
// Many lifetimes rather than one, because a leak of one goroutine per consumer is exactly
// what a single-lifetime count cannot see.
func TestRun_StopsOnContextCancelWithNoLeak(t *testing.T) {
	runtime.GC()
	baseline := runtime.NumGoroutine()

	const lifetimes = 40
	for range lifetimes {
		c, b, h := bareConsumer(t, 4)
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() {
			defer close(done)
			c.Run(ctx)
		}()
		cancel()
		select {
		case <-done:
		case <-time.After(waitFor):
			t.Fatal("Run did not return within the budget after its context was cancelled")
		}
		h.Close()
		if err := b.Close(); err != nil {
			t.Fatalf("close bus: %v", err)
		}
	}

	runtime.GC()
	if got, allowed := runtime.NumGoroutine(), baseline+baseline/20+2; got > allowed {
		t.Fatalf("goroutines: %d after %d consumer lifetimes, baseline %d, allowed %d (NFR-3)",
			got, lifetimes, baseline, allowed)
	}
}

// TestDispatch_ControlBackpressureRespectsTheContext covers the one place a worker can
// wait: handing a control message to the control goroutine. It waits rather than dropping
// — losing a revocation is the one message loss that cannot be tolerated — and it stops
// waiting when the process is shutting down.
func TestDispatch_ControlBackpressureRespectsTheContext(t *testing.T) {
	c, _, _ := bareConsumer(t, 1)
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

// TestNew_Defaults keeps a zero-valued wiring usable. A consumer with no workers is a
// gateway that accepts connections, reports healthy and delivers nothing, and a nil clock
// or flush hook is a nil dereference on the control path rather than a missing feature.
func TestNew_Defaults(t *testing.T) {
	c := New(Options{})
	if c.workers != 1 {
		t.Fatalf("workers = %d, want 1", c.workers)
	}
	if cap(c.controlq) != DefaultControlQueue {
		t.Fatalf("control queue depth = %d, want %d", cap(c.controlq), DefaultControlQueue)
	}
	if c.now == nil || c.flush == nil || c.log == nil {
		t.Fatal("New left the clock, the flush hook or the logger nil")
	}
	c.flush()
	c.now()
	c.log.Debug("the default logger must be usable and silent")
}

// TestNew_HonoursTheWorkerCount is bus.dispatch_workers reaching the pool it names.
func TestNew_HonoursTheWorkerCount(t *testing.T) {
	if got := New(Options{Workers: 7}).workers; got != 7 {
		t.Fatalf("workers = %d, want 7", got)
	}
}

// bareConsumer builds a Consumer whose goroutines the test starts itself, for the paths
// that are about one loop rather than about the whole fan-out path.
func bareConsumer(t *testing.T, workers int) (*Consumer, *bus.MemoryBus, *hub.Hub) {
	t.Helper()
	b := bus.NewMemory(4)
	h := hub.New(context.Background(), b, hub.Options{})
	c := New(Options{
		Bus:     b,
		Hub:     h,
		Log:     slog.New(slog.DiscardHandler),
		Secret:  []byte(testSecret),
		Workers: workers,
	})
	return c, b, h
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
