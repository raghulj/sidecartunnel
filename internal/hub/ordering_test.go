package hub

import (
	"encoding/json"
	"fmt"
	"sync"
	"testing"

	"github.com/raghulj/sidecartunnel/internal/bus"
	"github.com/raghulj/sidecartunnel/internal/proto"
)

// The ordering rule these tests exist for is normative
// (docs/03-client-protocol.md §5.1, docs/13-review-findings.md M15):
//
//	The gateway MUST NOT send a push for a channel BEFORE that channel's subscribe
//	reply, nor AFTER its unsubscribe reply.
//
// It is free only when the reply is queued inside the same critical section that mutates
// the subscription. Split that into two sections — mutate, unlock, then queue the reply —
// and a fan-out that acquires the read lock in the gap sees a subscriber the client has
// not been told about yet, so its push overtakes the reply that announces the channel.
// The window is small, silent, and produces two conforming clients that disagree: one
// drops the message, the other closes the connection.
//
// Both halves of the rule are asserted twice over: once structurally, by observing that
// the write lock is held at the instant the ack is queued, and once behaviourally, by
// driving concurrent publishes at the channel being subscribed.

// recordedFrame is one frame handed to a sink, with the lock state at the hand-off.
type recordedFrame struct {
	frame *proto.Frame

	// locked records that the hub's write lock was held when this frame was queued,
	// which is the mechanism M15 requires of an ack and forbids of a push — fan-out
	// holds the READ lock, so a push can never observe this as true.
	locked bool
}

// orderSink is a Sink that records the order it was handed frames in, and whether the
// hub's write lock was held at each hand-off.
//
// It is a white-box double on purpose: M15's guarantee is a statement about which lock is
// held when the reply is queued, and asserting it directly is deterministic where racing
// a publisher against a subscribe is only probabilistic.
type orderSink struct {
	h    *Hub
	id   string
	user string

	mu     sync.Mutex
	frames []recordedFrame
}

func newOrderSink(h *Hub, id string) *orderSink {
	return &orderSink{h: h, id: id, user: "u-1"}
}

func (s *orderSink) ID() string   { return s.id }
func (s *orderSink) User() string { return s.user }

func (s *orderSink) Send(f *proto.Frame) bool {
	// TryRLock never blocks, so this honours Sink.Send's contract. It fails only while a
	// writer holds the lock or is waiting for it, which is exactly the condition an ack
	// queued in the mutating critical section is under. A push cannot fake it: fan-out
	// holds the read lock, and a read lock is shared, so TryRLock succeeds there.
	held := !s.h.mu.TryRLock()
	if !held {
		s.h.mu.RUnlock()
	}
	s.mu.Lock()
	s.frames = append(s.frames, recordedFrame{frame: f, locked: held})
	s.mu.Unlock()
	return true
}

func (s *orderSink) Close(proto.CloseCode, string) {}

// recorded returns the frames this sink accepted, in order.
func (s *orderSink) recorded() []recordedFrame {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]recordedFrame(nil), s.frames...)
}

// isPush reports whether a recorded frame is a push rather than a reply.
func isPush(t *testing.T, f *proto.Frame) bool {
	t.Helper()
	var probe struct {
		Push *proto.Push `json:"push"`
	}
	if err := json.Unmarshal(f.Data, &probe); err != nil {
		t.Fatalf("decode frame %s: %v", f.Data, err)
	}
	return probe.Push != nil
}

// ackFrame is a stand-in for the pre-encoded reply internal/conn hands the registry.
func ackFrame(t *testing.T, id int64) *proto.Frame {
	t.Helper()
	f, err := encodeFrame(&proto.Reply{ID: id, Subscribe: &proto.SubscribeReply{}})
	if err != nil {
		t.Fatalf("encode ack: %v", err)
	}
	return f
}

// TestSubscribe_QueuesTheAckInTheMutatingCriticalSection_M15 is the structural half: the
// subscribe reply is handed to the connection while the write lock that inserted it is
// still held. Queue it after the unlock and this fails on the first run.
func TestSubscribe_QueuesTheAckInTheMutatingCriticalSection_M15(t *testing.T) {
	h := newTestHub(t, newBus())
	s := newOrderSink(h, "c1")
	if err := h.Add(s); err != nil {
		t.Fatalf("Add: %v", err)
	}

	if err := h.Subscribe(s, "room-1", ackFrame(t, 2)); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	got := s.recorded()
	if len(got) != 1 {
		t.Fatalf("frames = %d, want 1", len(got))
	}
	if !got[0].locked {
		t.Fatal("the subscribe reply was queued outside the critical section that inserted the subscription; a push for the channel can overtake it (M15, docs/03-client-protocol.md §5.1)")
	}
}

// TestUnsubscribe_QueuesTheAckInTheMutatingCriticalSection_M15 is the same assertion for
// the other end of the rule: nothing may be delivered for a channel after its unsubscribe
// reply, which holds only if the reply is queued where the subscription is dropped.
func TestUnsubscribe_QueuesTheAckInTheMutatingCriticalSection_M15(t *testing.T) {
	h := newTestHub(t, newBus())
	s := newOrderSink(h, "c1")
	if err := h.Add(s); err != nil {
		t.Fatalf("Add: %v", err)
	}
	if err := h.Subscribe(s, "room-1", nil); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	if err := h.Unsubscribe(s, "room-1", ackFrame(t, 3)); err != nil {
		t.Fatalf("Unsubscribe: %v", err)
	}

	got := s.recorded()
	if len(got) != 1 {
		t.Fatalf("frames = %d, want 1", len(got))
	}
	if !got[0].locked {
		t.Fatal("the unsubscribe reply was queued outside the critical section that dropped the subscription; a push can still follow it (M15)")
	}
}

// TestAttach_QueuesTheConnectReplyInTheMutatingCriticalSection_M15 covers the connect
// path, where the window is widest: a connect frame may take hundreds of channels, and
// every one of them is live from the moment it is inserted.
func TestAttach_QueuesTheConnectReplyInTheMutatingCriticalSection_M15(t *testing.T) {
	h := newTestHub(t, newBus())
	s := newOrderSink(h, "c1")
	if err := h.Add(s); err != nil {
		t.Fatalf("Add: %v", err)
	}

	var granted []string
	h.Attach(s, []string{"room-1", "room-2"}, func(g []string) *proto.Frame {
		granted = append([]string(nil), g...)
		return ackFrame(t, 1)
	})

	if len(granted) != 2 {
		t.Fatalf("granted = %v, want both channels", granted)
	}
	got := s.recorded()
	if len(got) != 1 {
		t.Fatalf("frames = %d, want 1", len(got))
	}
	if !got[0].locked {
		t.Fatal("the connect reply was queued outside the critical section that took the subscriptions; a push for a just-granted channel can overtake it (M15)")
	}
}

// TestPush_NeverOvertakesItsSubscribeReply_M15 is the behavioural half, driven under
// -race with publishers hammering the channel throughout the subscribe.
//
// A two-section implementation loses this: between releasing the lock and queueing the
// reply, a dispatch worker takes the read lock, finds the connection already in the
// channel's set, and queues a push in front of the reply.
func TestPush_NeverOvertakesItsSubscribeReply_M15(t *testing.T) {
	const rounds, publishers = 400, 8

	h := newTestHub(t, newBus())
	msg := bus.Message{Channel: "st:room-1", Payload: []byte(`{"event":"e","data":1}`)}

	for round := range rounds {
		s := newOrderSink(h, fmt.Sprintf("c%d", round))
		if err := h.Add(s); err != nil {
			t.Fatalf("Add: %v", err)
		}

		stop := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(publishers)
		for range publishers {
			go func() {
				defer wg.Done()
				for {
					select {
					case <-stop:
						return
					default:
					}
					if err := h.Dispatch(msg); err != nil {
						return
					}
				}
			}()
		}

		if err := h.Subscribe(s, "room-1", ackFrame(t, 2)); err != nil {
			t.Fatalf("Subscribe: %v", err)
		}
		close(stop)
		wg.Wait()
		h.Remove(s)

		got := s.recorded()
		if len(got) == 0 {
			t.Fatal("the sink received nothing, not even its own subscribe reply")
		}
		if isPush(t, got[0].frame) {
			t.Fatalf("round %d: a push for room-1 arrived before the subscribe reply (M15, docs/03-client-protocol.md §5.1)", round)
		}
	}
}

// TestPush_NeverFollowsItsUnsubscribeReply_M15 is the mirror image: with publishers
// running throughout, the unsubscribe reply must be the last thing the connection is
// handed for that channel.
func TestPush_NeverFollowsItsUnsubscribeReply_M15(t *testing.T) {
	const rounds, publishers = 400, 8

	h := newTestHub(t, newBus())
	msg := bus.Message{Channel: "st:room-1", Payload: []byte(`{"event":"e","data":1}`)}

	for round := range rounds {
		s := newOrderSink(h, fmt.Sprintf("c%d", round))
		if err := h.Add(s); err != nil {
			t.Fatalf("Add: %v", err)
		}
		if err := h.Subscribe(s, "room-1", nil); err != nil {
			t.Fatalf("Subscribe: %v", err)
		}

		stop := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(publishers)
		for range publishers {
			go func() {
				defer wg.Done()
				for {
					select {
					case <-stop:
						return
					default:
					}
					if err := h.Dispatch(msg); err != nil {
						return
					}
				}
			}()
		}

		if err := h.Unsubscribe(s, "room-1", ackFrame(t, 3)); err != nil {
			t.Fatalf("Unsubscribe: %v", err)
		}
		close(stop)
		wg.Wait()
		h.Remove(s)

		got := s.recorded()
		if len(got) == 0 {
			t.Fatal("the sink received nothing, not even its own unsubscribe reply")
		}
		if isPush(t, got[len(got)-1].frame) {
			t.Fatalf("round %d: a push for room-1 arrived after the unsubscribe reply (M15, docs/03-client-protocol.md §5.1)", round)
		}
	}
}
