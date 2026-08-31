package hub

import (
	"bytes"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/raghulj/sidecartunnel/internal/bus"
	"github.com/raghulj/sidecartunnel/internal/proto"
)

// envelope is the wire shape docs/04-integration.md §2.2 defines, built by tests.
func envelope(t *testing.T, fields map[string]any) []byte {
	t.Helper()
	payload, err := json.Marshal(fields)
	if err != nil {
		t.Fatalf("marshal envelope: %v", err)
	}
	return payload
}

// TestDispatch_FanOut_FR12 delivers one bus message to every connection subscribed to the
// channel, and to no connection that is not.
func TestDispatch_FanOut_FR12(t *testing.T) {
	t.Parallel()
	h := newTestHub(t, newBus())

	const subscribers = 100
	subs := make([]*fakeSink, subscribers)
	for i := range subs {
		subs[i] = newSink(fmt.Sprintf("c%03d", i), "u1")
		mustAdd(t, h, subs[i])
		mustSubscribe(t, h, subs[i], "room-1")
	}
	other := newSink("other", "u2")
	mustAdd(t, h, other)
	mustSubscribe(t, h, other, "room-2")

	msg := bus.Message{
		Channel: "st:room-1",
		Payload: envelope(t, map[string]any{"event": "order.created", "data": map[string]any{"id": 88123}}),
	}
	if err := h.Dispatch(msg); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}

	want := `{"push":{"channel":"room-1","pub":{"event":"order.created","data":{"id":88123}}}}`
	for _, s := range subs {
		got := s.waitFrame(t)
		if string(got.Data) != want {
			t.Fatalf("sink %s got %s, want %s", s.id, got.Data, want)
		}
	}
	if n := len(other.got()); n != 0 {
		t.Fatalf("a connection on another channel received %d frames", n)
	}
}

// TestDispatch_Exclude_FR13 skips exactly the connection whose client id the envelope
// names, and nobody else.
func TestDispatch_Exclude_FR13(t *testing.T) {
	t.Parallel()
	h := newTestHub(t, newBus())
	a, b, c := newSink("aaa", "u1"), newSink("bbb", "u2"), newSink("ccc", "u3")
	mustAdd(t, h, a, b, c)
	for _, s := range []*fakeSink{a, b, c} {
		mustSubscribe(t, h, s, "room-1")
	}

	msg := bus.Message{
		Channel: "st:room-1",
		Payload: envelope(t, map[string]any{"event": "e", "data": 1, "exclude": "bbb"}),
	}
	if err := h.Dispatch(msg); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	a.waitFrame(t)
	c.waitFrame(t)
	if n := len(b.got()); n != 0 {
		t.Fatalf("excluded connection received %d frames", n)
	}
}

// TestDispatch_OneSharedImmutableFrame_M10 is the memory arithmetic made a test: one
// encode per message, one pointer handed to every recipient, and bytes that never change
// afterwards. A per-connection copy is 160 GiB at 20,000 connections against a 1 GiB
// budget; sharing is ~4 KiB per connection.
func TestDispatch_OneSharedImmutableFrame_M10(t *testing.T) {
	t.Parallel()
	h := newTestHub(t, newBus())

	const subscribers = 100
	subs := make([]*fakeSink, subscribers)
	for i := range subs {
		subs[i] = newSink(fmt.Sprintf("c%03d", i), "u1")
		mustAdd(t, h, subs[i])
		mustSubscribe(t, h, subs[i], "room-1")
	}

	first := bus.Message{Channel: "st:room-1", Payload: envelope(t, map[string]any{"event": "one", "data": "x"})}
	if err := h.Dispatch(first); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}

	shared := subs[0].waitFrame(t)
	snapshot := append([]byte(nil), shared.Data...)
	for _, s := range subs[1:] {
		got := s.waitFrame(t)
		if got != shared {
			t.Fatalf("sink %s got a different *proto.Frame: the frame is encoded once and shared", s.id)
		}
	}

	second := bus.Message{Channel: "st:room-1", Payload: envelope(t, map[string]any{"event": "two", "data": "yyyy"})}
	if err := h.Dispatch(second); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	for _, s := range subs {
		if next := s.waitFrame(t); next == shared {
			t.Fatal("the second message reused the first message's Frame")
		}
	}
	if !bytes.Equal(shared.Data, snapshot) {
		t.Fatalf("the shared buffer was mutated after being handed out: %s != %s", shared.Data, snapshot)
	}
}

// TestDispatch_FromIsPassedThrough covers the client-event field the gateway stamps and a
// client may never set (docs/03-client-protocol.md §4.4).
func TestDispatch_FromIsPassedThrough(t *testing.T) {
	t.Parallel()
	h := newTestHub(t, newBus())
	s := newSink("c1", "u1")
	mustAdd(t, h, s)
	mustSubscribe(t, h, s, "desk-1")

	msg := bus.Message{
		Channel: "st:desk-1",
		Payload: envelope(t, map[string]any{"event": "typing", "data": true, "from": "u-7", "id": "01J8"}),
	}
	if err := h.Dispatch(msg); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	want := `{"push":{"channel":"desk-1","pub":{"event":"typing","data":true,"from":"u-7"}}}`
	if got := string(s.waitFrame(t).Data); got != want {
		t.Fatalf("frame = %s, want %s", got, want)
	}
}

// TestDispatch_UnprefixedKeyReachesNobody_FR21 is the accept test for bus-key isolation:
// the map holds bus keys, so a publish to the bare name matches nothing. It is
// structural, not a filter.
func TestDispatch_UnprefixedKeyReachesNobody_FR21(t *testing.T) {
	t.Parallel()
	h := newTestHub(t, newBus())
	s := newSink("c1", "u1")
	mustAdd(t, h, s)
	mustSubscribe(t, h, s, "room-1")

	msg := bus.Message{Channel: "room-1", Payload: envelope(t, map[string]any{"event": "e", "data": 1})}
	if err := h.Dispatch(msg); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	if n := len(s.got()); n != 0 {
		t.Fatalf("an unprefixed publish reached %d connections, want 0", n)
	}
}

// TestParseEnvelope_Malformed drops what docs/04-integration.md §2.2 says to drop. The
// caller drops the message and logs the channel, never the payload.
func TestParseEnvelope_Malformed(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		payload string
	}{
		{"not json", `{`},
		{"not an object", `[1,2]`},
		{"json null", `null`},
		{"missing event", `{"data":1}`},
		{"empty event", `{"event":"","data":1}`},
		{"missing data", `{"event":"e"}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if _, err := ParseEnvelope([]byte(tt.payload)); err == nil {
				t.Fatalf("ParseEnvelope(%s) = nil error, want a drop", tt.payload)
			}
		})
	}
}

// TestDispatch_MalformedEnvelopeDeliversNothing keeps a bad publish from reaching a
// socket, and names the channel in the error so the drop is actionable.
func TestDispatch_MalformedEnvelopeDeliversNothing(t *testing.T) {
	t.Parallel()
	h := newTestHub(t, newBus())
	s := newSink("c1", "u1")
	mustAdd(t, h, s)
	mustSubscribe(t, h, s, "room-1")

	err := h.Dispatch(bus.Message{Channel: "st:room-1", Payload: []byte(`{"event":"e"}`)})
	if err == nil {
		t.Fatal("Dispatch(malformed) = nil, want an error the caller can count")
	}
	if n := len(s.got()); n != 0 {
		t.Fatalf("a malformed envelope reached %d connections", n)
	}
}

// TestDispatch_SlowConsumerClosedOthersUnaffected_FR15 is the backpressure policy: the
// connection whose queue is full is closed with 3005 and removed, while every other
// connection on the same channel receives everything throughout.
func TestDispatch_SlowConsumerClosedOthersUnaffected_FR15(t *testing.T) {
	t.Parallel()
	b := newBus()
	h := newTestHub(t, b)
	slow, healthy := newSink("slow", "u1"), newSink("good", "u2")
	mustAdd(t, h, slow, healthy)
	mustSubscribe(t, h, slow, "room-1")
	mustSubscribe(t, h, healthy, "room-1")
	slow.full.Store(true)

	msg := bus.Message{Channel: "st:room-1", Payload: envelope(t, map[string]any{"event": "e", "data": 1})}
	if err := h.Dispatch(msg); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	healthy.waitFrame(t)

	if got := slow.waitClose(t); got.code != proto.CloseSlowConsumer {
		t.Fatalf("close code = %d, want %d", got.code, proto.CloseSlowConsumer)
	}
	// The closer deregisters before it closes, so the connection is gone by the time the
	// close is observable and a later publish cannot select it again.
	h.mu.RLock()
	_, resident := h.channels["st:room-1"][slow]
	_, mirror := h.subs[slow]
	h.mu.RUnlock()
	if resident || mirror {
		t.Fatalf("slow connection survived the close: resident=%v mirror=%v (M4)", resident, mirror)
	}

	if err := h.Dispatch(msg); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	healthy.waitFrame(t)
	if n := slow.closeCount(); n != 1 {
		t.Fatalf("slow connection was closed %d times, want 1", n)
	}
}

// TestDispatch_CloserQueueOverflow covers the last resort in docs/09-internals.md §4.3:
// when the closer queue is full the close is spawned rather than waited on, because the
// fan-out goroutine holds the read lock and blocking here stops delivery for everyone.
func TestDispatch_CloserQueueOverflow(t *testing.T) {
	t.Parallel()
	h := newTestHub(t, newBus(), func(o *Options) { o.CloserQueue = 1 })
	blocked := make(chan struct{})
	t.Cleanup(func() { close(blocked) })

	sinks := make([]*fakeSink, 3)
	closes := make(chan closeCall, 3)
	for i := range sinks {
		s := newSink(fmt.Sprintf("c%d", i), "u1")
		s.blockClose = blocked
		s.closed = closes
		s.full.Store(true)
		sinks[i] = s
		mustAdd(t, h, s)
		mustSubscribe(t, h, s, "room-1")
	}

	msg := bus.Message{Channel: "st:room-1", Payload: envelope(t, map[string]any{"event": "e", "data": 1})}
	if err := h.Dispatch(msg); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}

	// One connection is being closed by the closer goroutine, which is parked inside
	// Close; one is sitting in the capacity-1 queue behind it. The third can only have
	// been closed by a goroutine spawned for it.
	for i := 0; i < 2; i++ {
		select {
		case <-closes:
		case <-timeoutAfter():
			t.Fatalf("only %d closes with the closer parked; the overflow path did not spawn", i)
		}
	}
}

// TestDispatch_NoSubscribers is the ordinary case of a channel this replica does not
// hold: nothing to do, and no error.
func TestDispatch_NoSubscribers(t *testing.T) {
	t.Parallel()
	h := newTestHub(t, newBus())
	if err := h.Dispatch(bus.Message{Channel: "st:room-9", Payload: envelope(t, map[string]any{"event": "e", "data": 1})}); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
}

// TestEncodeFrame_Rejects covers the encoder's failure path directly. Neither caller in
// this package can reach it — both build their frames from validated input — but the
// error is returned rather than assumed away, and an assumption nobody tests is how a
// "null" text frame no client can parse reaches a socket.
func TestEncodeFrame_Rejects(t *testing.T) {
	t.Parallel()
	if _, err := encodeFrame(42); err == nil {
		t.Fatal("encodeFrame(42) = nil error, want a refusal")
	}
	if _, err := encodeFrame((*proto.PushFrame)(nil)); err == nil {
		t.Fatal("encodeFrame(nil frame) = nil error, want a refusal")
	}
}
