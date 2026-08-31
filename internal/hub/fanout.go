package hub

import (
	"encoding/json"
	"fmt"

	"github.com/raghulj/sidecartunnel/internal/bus"
	"github.com/raghulj/sidecartunnel/internal/proto"
)

// slowConsumerReason is the reason string on a backpressure close. It names the condition
// and nothing else: a reason must never carry a cookie, a header or a payload (NFR-7).
const slowConsumerReason = "outbound queue full"

// Envelope is one published message as it arrives on the bus
// (docs/04-integration.md §2.2).
//
// There was an only_users field. It is gone: a delivery-time authorization filter in a
// design whose authorization happens at subscribe time, with no bound and no requirement,
// costing an O(subscribers × users) scan under the read lock. Do not add it back
// (docs/13-review-findings.md M9).
type Envelope struct {
	// Event is the publisher's opaque event name. Required.
	Event string `json:"event"`

	// Data is the publisher's opaque payload, any JSON value. Required, and passed to
	// subscribers untouched.
	Data json.RawMessage `json:"data"`

	// Exclude is the client id that must not receive this message. Empty means everyone
	// receives it (FR-13).
	Exclude string `json:"exclude"`

	// From is the publishing connection's user id, set by the gateway on client events
	// and never by an application.
	From string `json:"from"`

	// ID is echoed in logs and metrics exemplars for tracing. The hub does not use it.
	ID string `json:"id"`
}

// ParseEnvelope decodes one published payload.
//
// An envelope that is not a JSON object, or is missing event or data, is an error: the
// caller drops the message and counts st_messages_dropped_total{reason="malformed"}. Note
// that "data": null is a value and therefore valid, while an absent data is not — the two
// are different things and only the second is a publisher bug.
//
// It holds no state and is safe to call concurrently.
func ParseEnvelope(payload []byte) (Envelope, error) {
	var env Envelope
	if err := json.Unmarshal(payload, &env); err != nil {
		return Envelope{}, fmt.Errorf("hub: envelope is not a JSON object: %w", err)
	}
	if env.Event == "" {
		return Envelope{}, fmt.Errorf("hub: envelope has no event")
	}
	if env.Data == nil {
		return Envelope{}, fmt.Errorf("hub: envelope has no data")
	}
	return env, nil
}

// Dispatch decodes one bus message and delivers it to every local subscriber of the
// channel it arrived on.
//
// msg.Channel is a full bus key and is used as the map key directly, so a publish to an
// unprefixed channel name reaches nobody — structurally, rather than by a filter someone
// has to remember (FR-21). An error means the envelope was dropped and nothing was
// delivered; the caller counts it.
//
// The frame is encoded exactly once, before the lock is taken, and every recipient is
// handed the same pointer. Its bytes are never modified afterwards: they are visible to
// every recipient's writer goroutine at once, so a single append is a data race against
// the whole channel (M10, docs/09-internals.md §5).
//
// Dispatch is safe to call from several dispatch workers at once. It must not be called
// concurrently with Close.
func (h *Hub) Dispatch(msg bus.Message) error {
	env, err := ParseEnvelope(msg.Payload)
	if err != nil {
		return fmt.Errorf("hub: dispatch %s: %w", msg.Channel, err)
	}
	push := &proto.PushFrame{Push: &proto.Push{
		Channel: h.channelName(msg.Channel),
		Pub: &proto.Pub{
			Event: env.Event,
			Data:  env.Data,
			From:  env.From,
		},
	}}
	frame, err := encodeFrame(push)
	if err != nil {
		// coverage: unreachable — env.Data came out of a successful json.Unmarshal in
		// ParseEnvelope, which validates the whole document, and that is proto.Encode's
		// only failure mode. Reported rather than assumed away so that the drop is
		// counted if a future caller hands Dispatch an unvalidated payload.
		return fmt.Errorf("hub: dispatch %s: %w", msg.Channel, err)
	}
	h.deliver(msg.Channel, frame, env.Exclude)
	return nil
}

// deliver hands one frame to every connection holding key, except the excluded one.
//
// The read lock is held for a bounded, short time — one map iteration and one
// non-blocking send per recipient, roughly 0.2 ms at 10,000 connections — because every
// send is non-blocking by contract (FR-15, docs/09-internals.md §4.3).
//
// Connections that refuse are collected and closed only after the lock is released.
// Closing inline would deadlock: Close deregisters, which needs the write lock this
// goroutine is holding for read (C7, docs/09-internals.md §4.5).
func (h *Hub) deliver(key string, frame *proto.Frame, exclude string) {
	var slow []Sink
	h.mu.RLock()
	for s := range h.channels[key] {
		// FR-13. The empty-exclude guard is not cosmetic: an envelope with no exclude
		// must not match a Sink whose id is somehow empty.
		if exclude != "" && s.ID() == exclude {
			continue
		}
		if !s.Send(frame) {
			slow = append(slow, s)
		}
	}
	h.mu.RUnlock()

	for _, s := range slow {
		h.enqueueClose(s)
	}
}

// enqueueClose hands one slow connection to the closer goroutine.
//
// The send is non-blocking, and overflow spawns a goroutine rather than waiting. C7: the
// fan-out goroutine may never block to schedule work for another goroutine, whatever the
// queue depth — a bounded queue that fills is a bounded queue that stops all delivery on
// the replica while every socket stays open and /ready stays 200.
func (h *Hub) enqueueClose(s Sink) {
	select {
	case h.closeq <- s:
	default:
		h.wg.Add(1)
		go func() {
			defer h.wg.Done()
			h.closeSlow(s)
		}()
	}
}

// closeLoop is the dedicated closer goroutine. Closing lives here and nowhere near the
// fan-out path (docs/09-internals.md §4.3).
func (h *Hub) closeLoop() {
	defer h.wg.Done()
	for {
		select {
		case <-h.ctx.Done():
			return
		case s := <-h.closeq:
			h.closeSlow(s)
		}
	}
}

// closeSlow deregisters a connection and then ends it with proto.CloseSlowConsumer.
//
// Deregistering first is deliberate: from that moment no further fan-out can select the
// connection, so a burst in flight does not queue more frames onto a socket that is
// already going away. Sink.Close deregisters again on its own and is idempotent, and
// Remove is too — expiry, revocation, drain and this path all race for the same
// connection (FR-15, docs/07-delivery.md §4).
func (h *Hub) closeSlow(s Sink) {
	h.Remove(s)
	s.Close(proto.CloseSlowConsumer, slowConsumerReason)
}

// encodeFrame encodes one outbound frame into the shared immutable buffer every recipient
// of a fan-out receives a pointer to (docs/09-internals.md §5).
func encodeFrame(v any) (*proto.Frame, error) {
	data, err := proto.Encode(v)
	if err != nil {
		return nil, fmt.Errorf("hub: encode frame: %w", err)
	}
	return &proto.Frame{Data: data}, nil
}
