package hub

import (
	"reflect"
	"testing"

	"github.com/raghulj/sidecartunnel/internal/config"
	"github.com/raghulj/sidecartunnel/internal/proto"
)

// TestAttach_TakesTheGrantedChannelsAndReports covers the connect path's filtering
// (docs/03-client-protocol.md §4.1): a channel the registry cannot take is omitted from
// the reply rather than failing the whole connect, and the caller learns which ones it
// got so it can build the subs map from the answer instead of from the request.
//
// The table is one Attach per row rather than one per channel, because the whole point of
// Attach is that a connect frame's channels move in a single critical section.
func TestAttach_TakesTheGrantedChannelsAndReports(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		channels []string
		opts     func(*Options)
		setup    func(t *testing.T, h *Hub, s Sink)
		want     []string
	}{
		{
			name:     "every channel granted",
			channels: []string{"room-1", "room-2"},
			want:     []string{"room-1", "room-2"},
		},
		{
			name:     "no channels at all",
			channels: nil,
			want:     []string{},
		},
		{
			// docs/06-channels.md §4: refused before grants are consulted, so a grant of
			// "*" still cannot reach a control channel.
			name:     "a reserved channel is omitted",
			channels: []string{"room-1", "_control"},
			want:     []string{"room-1"},
		},
		{
			// FR-11: an unresolvable namespace fails closed. It is omitted here rather
			// than refusing the connect, which is what §4.1 requires.
			name:     "an unknown namespace is omitted",
			channels: []string{"room-1", "nope-1"},
			opts: func(o *Options) {
				o.Namespaces = []config.Namespace{{Name: "room"}}
			},
			want: []string{"room-1"},
		},
		{
			name:     "a duplicate is omitted",
			channels: []string{"room-1", "room-1"},
			want:     []string{"room-1"},
		},
		{
			// FR-8's cap. The connection keeps what fits and is told what it kept.
			name:     "the subscription cap truncates",
			channels: []string{"room-1", "room-2", "room-3"},
			opts:     func(o *Options) { o.MaxSubscriptionsPerConn = 2 },
			want:     []string{"room-1", "room-2"},
		},
		{
			// A client id held by another connection registers nothing: overwriting the
			// index entry would leave a live connection unreachable by a control
			// disconnect (FR-18).
			name:     "a duplicate client id grants nothing",
			channels: []string{"room-1"},
			setup: func(t *testing.T, h *Hub, _ Sink) {
				t.Helper()
				if err := h.Add(newSink("c1", "u-other")); err != nil {
					t.Fatalf("Add: %v", err)
				}
			},
			want: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var mutate []func(*Options)
			if tt.opts != nil {
				mutate = append(mutate, tt.opts)
			}
			h := newTestHub(t, newBus(), mutate...)
			s := newSink("c1", "u1")
			if tt.setup != nil {
				tt.setup(t, h, s)
			}

			var granted []string
			called := 0
			h.Attach(s, tt.channels, func(g []string) *proto.Frame {
				called++
				granted = append(make([]string, 0, len(g)), g...)
				return nil
			})

			if called != 1 {
				t.Fatalf("ack called %d times, want exactly 1: an empty subs map is the important case (docs/03-client-protocol.md §4.1)", called)
			}
			if !reflect.DeepEqual(granted, tt.want) {
				t.Fatalf("granted = %v, want %v", granted, tt.want)
			}
			if got := h.Subscriptions(s); !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("Subscriptions = %v, want %v: the reply and the registry must agree", got, tt.want)
			}
		})
	}
}

// TestAttach_SubscribesUpstreamOnce_FR10 proves Attach drives the same refcount as
// Subscribe: the channels it takes reach the desired set and the reconciler syncs them.
func TestAttach_SubscribesUpstreamOnce_FR10(t *testing.T) {
	t.Parallel()
	b := newBus()
	h := newTestHub(t, b)
	s := newSink("c1", "u1")

	h.Attach(s, []string{"room-1", "room-2"}, func([]string) *proto.Frame { return nil })

	b.waitSync(t, "st:_control", "st:room-1", "st:room-2")
}

// TestAttach_ANilAckQueuesNothing: the callback may fail to encode, and a connection with
// a healthy outbound queue must not be closed because the gateway had nothing to say.
func TestAttach_ANilAckQueuesNothing(t *testing.T) {
	t.Parallel()
	h := newTestHub(t, newBus())
	s := newSink("c1", "u1")

	h.Attach(s, []string{"room-1"}, func([]string) *proto.Frame { return nil })

	if got := len(s.got()); got != 0 {
		t.Fatalf("frames = %d, want 0", got)
	}
	if got := s.closeCount(); got != 0 {
		t.Fatalf("closes = %d, want 0: a nil reply is not a slow consumer", got)
	}
}

// TestAckRefused_ClosesOffTheLock_FR15 covers the backpressure path on all three ack
// carriers. A connection whose outbound queue is full when its own reply is queued is a
// slow consumer like any other: it is collected, and closed by the closer goroutine after
// the lock is released, never inline (docs/09-internals.md §4.3, §4.5).
func TestAckRefused_ClosesOffTheLock_FR15(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		act  func(t *testing.T, h *Hub, s *fakeSink)
	}{
		{
			name: "attach",
			act: func(t *testing.T, h *Hub, s *fakeSink) {
				t.Helper()
				s.full.Store(true)
				h.Attach(s, []string{"room-1"}, func([]string) *proto.Frame {
					return mustEncode(t, &proto.Reply{ID: 1, Connect: &proto.ConnectReply{}})
				})
			},
		},
		{
			name: "subscribe",
			act: func(t *testing.T, h *Hub, s *fakeSink) {
				t.Helper()
				mustAdd(t, h, s)
				s.full.Store(true)
				if err := h.Subscribe(s, "room-1", mustEncode(t, &proto.Reply{ID: 2, Subscribe: &proto.SubscribeReply{}})); err != nil {
					t.Fatalf("Subscribe: %v", err)
				}
			},
		},
		{
			name: "unsubscribe",
			act: func(t *testing.T, h *Hub, s *fakeSink) {
				t.Helper()
				mustAdd(t, h, s)
				mustSubscribe(t, h, s, "room-1")
				s.full.Store(true)
				if err := h.Unsubscribe(s, "room-1", mustEncode(t, &proto.Reply{ID: 3, Unsubscribe: &proto.UnsubscribeReply{}})); err != nil {
					t.Fatalf("Unsubscribe: %v", err)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHub(t, newBus())
			s := newSink("c1", "u1")

			tt.act(t, h, s)

			if got := s.waitClose(t); got.code != proto.CloseSlowConsumer {
				t.Fatalf("close code = %d, want %d (FR-15)", got.code, proto.CloseSlowConsumer)
			}
		})
	}
}

// mustEncode builds one outbound frame for a test, standing in for the pre-encoded reply
// internal/conn hands the registry.
func mustEncode(t *testing.T, v any) *proto.Frame {
	t.Helper()
	f, err := encodeFrame(v)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	return f
}
