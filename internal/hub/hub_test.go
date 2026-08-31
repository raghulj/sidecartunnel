package hub

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"

	"github.com/raghulj/sidecartunnel/internal/config"
)

// TestSubscribe_KeysByBusKey_FR21 pins the one property that makes cross-delivery
// structurally impossible rather than a filter someone must remember to apply.
func TestSubscribe_KeysByBusKey_FR21(t *testing.T) {
	t.Parallel()
	b := newBus()
	h := newTestHub(t, b)
	s := newSink("c1", "u1")
	mustAdd(t, h, s)
	mustSubscribe(t, h, s, "room-1")

	h.mu.RLock()
	_, prefixed := h.channels["st:room-1"]
	_, bare := h.channels["room-1"]
	h.mu.RUnlock()

	if !prefixed {
		t.Fatalf("hub map is not keyed by the bus key; keys: %v", h.channelKeys())
	}
	if bare {
		t.Fatal("hub map holds the bare channel name (FR-21)")
	}
	if got := desiredSnapshot(h); !reflect.DeepEqual(got, []string{"st:_control", "st:room-1"}) {
		t.Fatalf("desired = %v, want the control key and the bus key", got)
	}
}

// TestNew_SeedsControlKey_C8 covers the trap in a desired-state reconciler: Sync sets the
// subscription set exactly, so a desired set that omitted the control key would
// unsubscribe the replica from control on the very first reconciliation and silently
// disable revocation (docs/04-integration.md §3, C8).
func TestNew_SeedsControlKey_C8(t *testing.T) {
	t.Parallel()
	b := newBus()
	h := newTestHub(t, b)
	if got := h.ControlKey(); got != "st:_control" {
		t.Fatalf("ControlKey() = %q, want %q", got, "st:_control")
	}
	b.waitSync(t, "st:_control")
}

// TestRefcount_TransitionsDecidedUnderTheLock_FR10 asserts the 0→1 and 1→0 edges
// directly, because those booleans are the whole of FR-10 and recomputing either after
// the lock is released is how a concurrent unsubscribe produces a double subscribe or a
// dropped live subscription.
func TestRefcount_TransitionsDecidedUnderTheLock_FR10(t *testing.T) {
	t.Parallel()
	h := newTestHub(t, newBus())
	s1, s2 := newSink("c1", "u1"), newSink("c2", "u2")
	mustAdd(t, h, s1, s2)

	first, err := h.insert(s1, "st:room-1")
	if err != nil || !first {
		t.Fatalf("insert(s1) = (%v, %v), want (true, nil): 0→1 must be the first edge", first, err)
	}
	first, err = h.insert(s2, "st:room-1")
	if err != nil || first {
		t.Fatalf("insert(s2) = (%v, %v), want (false, nil): a second holder is not a new subscription", first, err)
	}
	last, err := h.drop(s1, "st:room-1")
	if err != nil || last {
		t.Fatalf("drop(s1) = (%v, %v), want (false, nil): one holder remains", last, err)
	}
	last, err = h.drop(s2, "st:room-1")
	if err != nil || !last {
		t.Fatalf("drop(s2) = (%v, %v), want (true, nil): 1→0 must be the last edge", last, err)
	}
}

// TestSubscribe_TwoHoldersOneUpstreamEntry_FR10 is the same requirement seen from the
// bus: two local subscribers are one upstream subscription, not two.
func TestSubscribe_TwoHoldersOneUpstreamEntry_FR10(t *testing.T) {
	t.Parallel()
	b := newBus()
	h := newTestHub(t, b)
	s1, s2 := newSink("c1", "u1"), newSink("c2", "u2")
	mustAdd(t, h, s1, s2)
	mustSubscribe(t, h, s1, "room-1")
	mustSubscribe(t, h, s2, "room-1")

	b.waitSync(t, "st:_control", "st:room-1")

	if err := h.Unsubscribe(s1, "room-1"); err != nil {
		t.Fatalf("Unsubscribe(s1): %v", err)
	}
	if got := desiredSnapshot(h); !reflect.DeepEqual(got, []string{"st:_control", "st:room-1"}) {
		t.Fatalf("desired = %v: one holder remains, the subscription must stay", got)
	}
	if err := h.Unsubscribe(s2, "room-1"); err != nil {
		t.Fatalf("Unsubscribe(s2): %v", err)
	}
	if got := desiredSnapshot(h); !reflect.DeepEqual(got, []string{"st:_control"}) {
		t.Fatalf("desired = %v, want only the control key after the last holder left", got)
	}
	b.waitSync(t, "st:_control")
}

// TestSubscribe_Errors covers every structural refusal the hub owns. Grants are not
// among them: the hub has none and never re-checks authorization at delivery time.
func TestSubscribe_Errors(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		channel string
		setup   func(t *testing.T, h *Hub, s Sink)
		opts    func(*Options)
		want    error
	}{
		{
			name:    "reserved prefix is never subscribable",
			channel: "_control",
			want:    ErrReservedChannel,
		},
		{
			name:    "unknown namespace fails closed",
			channel: "nope-1",
			opts:    func(o *Options) { o.Namespaces = []config.Namespace{{Name: "room"}} },
			want:    ErrUnknownNamespace,
		},
		{
			name:    "duplicate subscribe is not idempotent",
			channel: "room-1",
			setup:   func(t *testing.T, h *Hub, s Sink) { mustSubscribe(t, h, s, "room-1") },
			want:    ErrAlreadySubscribed,
		},
		{
			name:    "past the per-connection cap",
			channel: "room-2",
			opts:    func(o *Options) { o.MaxSubscriptionsPerConn = 1 },
			setup:   func(t *testing.T, h *Hub, s Sink) { mustSubscribe(t, h, s, "room-1") },
			want:    ErrSubscriptionLimit,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			mutate := []func(*Options){}
			if tt.opts != nil {
				mutate = append(mutate, tt.opts)
			}
			h := newTestHub(t, newBus(), mutate...)
			s := newSink("c1", "u1")
			mustAdd(t, h, s)
			if tt.setup != nil {
				tt.setup(t, h, s)
			}
			if err := h.Subscribe(s, tt.channel); !errors.Is(err, tt.want) {
				t.Fatalf("Subscribe(%q) = %v, want %v", tt.channel, err, tt.want)
			}
		})
	}
}

// TestSubscribe_UnregisteredSinkCannotResurrect is the close-versus-subscribe race: a
// reader goroutine subscribing while the connection is being removed must not put the
// connection back into the map, where fan-out would write to it forever (M4).
func TestSubscribe_UnregisteredSinkCannotResurrect(t *testing.T) {
	t.Parallel()
	h := newTestHub(t, newBus())
	s := newSink("c1", "u1")
	mustAdd(t, h, s)
	mustSubscribe(t, h, s, "room-1")
	h.Remove(s)

	if err := h.Subscribe(s, "room-2"); !errors.Is(err, ErrNotRegistered) {
		t.Fatalf("Subscribe after Remove = %v, want ErrNotRegistered", err)
	}
	if err := h.Unsubscribe(s, "room-1"); !errors.Is(err, ErrNotRegistered) {
		t.Fatalf("Unsubscribe after Remove = %v, want ErrNotRegistered", err)
	}
	if got := h.Subscriptions(s); len(got) != 0 {
		t.Fatalf("Subscriptions after Remove = %v, want none", got)
	}
}

// TestUnsubscribe_NotSubscribed keeps the command non-idempotent: a client unsubscribing
// from a channel it does not hold has a drifted registry, and hiding that makes reconnect
// bugs very hard to find.
func TestUnsubscribe_NotSubscribed(t *testing.T) {
	t.Parallel()
	h := newTestHub(t, newBus())
	s := newSink("c1", "u1")
	mustAdd(t, h, s)
	if err := h.Unsubscribe(s, "room-1"); !errors.Is(err, ErrNotSubscribed) {
		t.Fatalf("Unsubscribe = %v, want ErrNotSubscribed", err)
	}
}

// TestAdd_DuplicateClientID refuses a second registration for one client id rather than
// overwriting the index entry, which would leave a live connection unreachable by a
// control disconnect (FR-18).
func TestAdd_DuplicateClientID(t *testing.T) {
	t.Parallel()
	h := newTestHub(t, newBus())
	s1, s2 := newSink("c1", "u1"), newSink("c1", "u2")
	mustAdd(t, h, s1)
	if err := h.Add(s1); err != nil {
		t.Fatalf("Add is idempotent for the same sink: %v", err)
	}
	if err := h.Add(s2); !errors.Is(err, ErrDuplicateID) {
		t.Fatalf("Add(duplicate id) = %v, want ErrDuplicateID", err)
	}
}

// TestRemove_LeavesNoResidue_M4 is the drift check: after removal the connection is in
// neither the channel map nor its own subscription mirror, and the refcount reached zero.
func TestRemove_LeavesNoResidue_M4(t *testing.T) {
	t.Parallel()
	b := newBus()
	h := newTestHub(t, b)
	s1, s2 := newSink("c1", "u1"), newSink("c2", "u1")
	mustAdd(t, h, s1, s2)
	mustSubscribe(t, h, s1, "room-1", "room-2")
	mustSubscribe(t, h, s2, "room-1")
	b.waitSync(t, "st:_control", "st:room-1", "st:room-2")

	h.Remove(s1)
	h.Remove(s1) // idempotent: expiry, revocation and a slow-consumer close all race here.

	h.mu.RLock()
	_, mirror := h.subs[s1]
	_, inRoom1 := h.channels["st:room-1"][s1]
	_, room2Present := h.channels["st:room-2"]
	users := len(h.users["u1"])
	_, client := h.clients["c1"]
	h.mu.RUnlock()

	switch {
	case mirror:
		t.Error("subscription mirror survived Remove (M4)")
	case inRoom1:
		t.Error("connection is still resident in the channel map (M4)")
	case room2Present:
		t.Error("room-2 had one holder; its entry must be gone with it")
	case users != 1:
		t.Errorf("user index holds %d sinks, want the one remaining", users)
	case client:
		t.Error("client index survived Remove")
	}
	if got := desiredSnapshot(h); !reflect.DeepEqual(got, []string{"st:_control", "st:room-1"}) {
		t.Fatalf("desired = %v, want room-2 dropped and room-1 kept", got)
	}
	b.waitSync(t, "st:_control", "st:room-1")
}

// TestSubscriptions_ReturnsBareChannelsSorted feeds the sync reply (M16). The client is
// told channels, never bus keys — the prefix is the gateway's business.
func TestSubscriptions_ReturnsBareChannelsSorted(t *testing.T) {
	t.Parallel()
	h := newTestHub(t, newBus())
	s := newSink("c1", "u1")
	mustAdd(t, h, s)
	mustSubscribe(t, h, s, "room-2", "room-1")
	if got := h.Subscriptions(s); !reflect.DeepEqual(got, []string{"room-1", "room-2"}) {
		t.Fatalf("Subscriptions = %v, want [room-1 room-2]", got)
	}
}

// TestNamespace_Resolution_FR11 covers the substring-before-the-first-separator rule and
// the reserved empty-name block that catches separator-less channels and, when it is
// present, everything else (M11).
func TestNamespace_Resolution_FR11(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		namespaces []config.Namespace
		channel    string
		wantName   string
		wantOK     bool
	}{
		{"named namespace", []config.Namespace{{Name: "room"}}, "room-4410", "room", true},
		{"first separator wins", []config.Namespace{{Name: "org"}}, "org-42-desk-1", "org", true},
		{"no separator resolves to the reserved empty name", []config.Namespace{{Name: ""}}, "status", "", true},
		{"unmatched falls back to the empty-name block", []config.Namespace{{Name: "room"}, {Name: ""}}, "other-1", "", true},
		{"unmatched with no empty-name block fails closed", []config.Namespace{{Name: "room"}}, "other-1", "", false},
		{"empty list installs the built-in block", nil, "anything-1", "", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHub(t, newBus(), func(o *Options) { o.Namespaces = tt.namespaces })
			ns, ok := h.Namespace(tt.channel)
			if ok != tt.wantOK {
				t.Fatalf("Namespace(%q) ok = %v, want %v", tt.channel, ok, tt.wantOK)
			}
			if ok && ns.Name != tt.wantName {
				t.Fatalf("Namespace(%q).Name = %q, want %q", tt.channel, ns.Name, tt.wantName)
			}
		})
	}
}

// TestPublish_PrefixesTheChannel_FR21 keeps bus-key construction in exactly one place.
func TestPublish_PrefixesTheChannel_FR21(t *testing.T) {
	t.Parallel()
	b := newBus()
	h := newTestHub(t, b)
	if err := h.Publish(context.Background(), "room-1", []byte(`{"event":"e","data":1}`)); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	got := <-b.published
	if got.channel != "st:room-1" {
		t.Fatalf("published to %q, want the bus key", got.channel)
	}

	b.fail(errBus)
	if err := h.Publish(context.Background(), "room-1", nil); !errors.Is(err, errBus) {
		t.Fatalf("Publish error = %v, want the bus error wrapped", err)
	}
}

// TestNew_Defaults checks that a zero Options is usable rather than a hub that refuses
// every channel or panics on an empty separator.
func TestNew_Defaults(t *testing.T) {
	t.Parallel()
	h := New(context.Background(), newBus(), Options{})
	t.Cleanup(h.Close)
	if h.ControlKey() != "st:_control" {
		t.Fatalf("ControlKey() = %q, want the default prefix applied", h.ControlKey())
	}
	if _, ok := h.Namespace("room-1"); !ok {
		t.Fatal("a hub with no namespaces must install the built-in block (M11)")
	}
	s := newSink("c1", "u1")
	mustAdd(t, h, s)
	mustSubscribe(t, h, s, "room-1")
}

// TestConcurrentSubscribeUnsubscribe_SameChannel_FR10 is the interleaving M6 and FR-10
// are about. Many goroutines churn one channel; afterwards the desired set must agree
// with the map exactly — a key with holders and no desired entry is a channel that is
// locally held and upstream dead, and a desired entry with no holders is a leak.
func TestConcurrentSubscribeUnsubscribe_SameChannel_FR10(t *testing.T) {
	t.Parallel()
	b := newBus()
	// A wedged bus keeps the reconciler out of the way: this test is about the map and
	// the desired set moving together, and nothing here may depend on Sync completing.
	b.wedge()
	h := newTestHub(t, b)
	t.Cleanup(b.release)

	const sinks, rounds = 16, 200
	all := make([]*fakeSink, sinks)
	for i := range all {
		all[i] = newSink(fmt.Sprintf("c%02d", i), "u1")
		mustAdd(t, h, all[i])
	}

	var wg sync.WaitGroup
	for _, s := range all {
		wg.Add(1)
		go func(s *fakeSink) {
			defer wg.Done()
			for i := 0; i < rounds; i++ {
				if err := h.Subscribe(s, "room-1"); err != nil {
					t.Errorf("Subscribe: %v", err)
					return
				}
				if err := h.Unsubscribe(s, "room-1"); err != nil {
					t.Errorf("Unsubscribe: %v", err)
					return
				}
			}
		}(s)
	}
	wg.Wait()

	h.mu.RLock()
	holders := len(h.channels["st:room-1"])
	_, wanted := h.desired["st:room-1"]
	h.mu.RUnlock()
	if holders != 0 || wanted {
		t.Fatalf("after the churn: holders=%d desired=%v, want 0 and false", holders, wanted)
	}

	// And the same channel, once held again, is desired again.
	mustSubscribe(t, h, all[0], "room-1")
	if got := desiredSnapshot(h); !reflect.DeepEqual(got, []string{"st:_control", "st:room-1"}) {
		t.Fatalf("desired = %v, want the channel back", got)
	}
}
