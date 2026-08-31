package hub

import (
	"errors"
	"reflect"
	"testing"

	"github.com/raghulj/sidecartunnel/internal/proto"
)

// TestChannels_ReportsBareNamesAndCounts_FR20 pins the shape the admin listener publishes:
// bare channel names, never bus keys, and this replica's own subscriber count.
func TestChannels_ReportsBareNamesAndCounts_FR20(t *testing.T) {
	t.Parallel()
	h := newTestHub(t, newBus())

	a := newSink("c1", "u1")
	b := newSink("c2", "u2")
	mustAdd(t, h, a)
	mustAdd(t, h, b)
	mustSubscribe(t, h, a, "room-1")
	mustSubscribe(t, h, b, "room-1")
	mustSubscribe(t, h, b, "user-2")

	got := h.Channels()
	want := []Occupancy{
		{Channel: "room-1", Subscribers: 2},
		{Channel: "user-2", Subscribers: 1},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Channels() = %+v, want %+v", got, want)
	}
	for _, occ := range got {
		if occ.Users != nil {
			t.Fatalf("Channels() populated Users for %q; the list endpoint drops them and a "+
				"10,000-channel membership document is one nobody asked for", occ.Channel)
		}
	}
}

// TestChannels_ExcludesTheControlChannel_C8 keeps the reserved control channel out of the
// operator listing. It is in the desired set permanently (C8) and has no subscribers, so
// reporting it would be a channel an operator cannot act on.
func TestChannels_ExcludesTheControlChannel_C8(t *testing.T) {
	t.Parallel()
	h := newTestHub(t, newBus())
	if got := h.Channels(); len(got) != 0 {
		t.Fatalf("Channels() = %+v on an idle hub, want none", got)
	}
}

// TestChannel_OneChannelCarriesItsUsers_FR20 is the single-channel route: the count plus
// the opaque user ids holding it, sorted so two scrapes of an unchanged replica agree.
func TestChannel_OneChannelCarriesItsUsers_FR20(t *testing.T) {
	t.Parallel()
	h := newTestHub(t, newBus())

	for _, s := range []*fakeSink{newSink("c1", "u-9"), newSink("c2", "u-1"), newSink("c3", "u-1")} {
		mustAdd(t, h, s)
		mustSubscribe(t, h, s, "room-1")
	}

	got, ok := h.Channel("room-1")
	if !ok {
		t.Fatal("Channel(room-1) reported the channel as unheld")
	}
	want := Occupancy{Channel: "room-1", Subscribers: 3, Users: []string{"u-1", "u-1", "u-9"}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Channel(room-1) = %+v, want %+v", got, want)
	}
}

// TestChannel_UnheldIsNotAnError is the 404 the admin listener answers with: a channel
// this replica does not hold is a normal answer, not a failure.
func TestChannel_UnheldIsNotAnError(t *testing.T) {
	t.Parallel()
	h := newTestHub(t, newBus())
	if got, ok := h.Channel("room-1"); ok {
		t.Fatalf("Channel(room-1) = %+v, true on an idle hub; want false", got)
	}
}

// TestChannel_KeysByBusKey_FR21 asserts the lookup goes through the same prefixing every
// other path uses. A lookup on the bare name would report every channel as unheld.
func TestChannel_KeysByBusKey_FR21(t *testing.T) {
	t.Parallel()
	h := newTestHub(t, newBus(), func(o *Options) { o.Prefix = "other:" })
	s := newSink("c1", "u1")
	mustAdd(t, h, s)
	mustSubscribe(t, h, s, "room-1")

	if _, ok := h.Channel("room-1"); !ok {
		t.Fatal("Channel(room-1) missed the channel under a non-default bus.prefix (FR-21)")
	}
	if _, ok := h.Channel("other:room-1"); ok {
		t.Fatal("Channel accepted a bus key as a channel name; the prefix is the gateway's business")
	}
}

// TestDisconnect_ByUserAndByClient_FR18 is the admin surface's POST /disconnect: the same
// effect as the control action, closing with 3501 and reconnect false.
func TestDisconnect_ByUserAndByClient_FR18(t *testing.T) {
	t.Parallel()
	h := newTestHub(t, newBus())

	one := newSink("c1", "u-7")
	two := newSink("c2", "u-7")
	other := newSink("c3", "u-8")
	for _, s := range []*fakeSink{one, two, other} {
		mustAdd(t, h, s)
		mustSubscribe(t, h, s, "room-1")
	}

	closed, err := h.Disconnect("u-7", "")
	if err != nil {
		t.Fatalf("Disconnect(user) = %v", err)
	}
	if closed != 2 {
		t.Fatalf("Disconnect(u-7) closed %d, want 2", closed)
	}
	for _, s := range []*fakeSink{one, two} {
		if got := s.waitClose(t); got.code != proto.CloseRevoked {
			t.Fatalf("sink %s closed with %d, want %d", s.id, got.code, proto.CloseRevoked)
		}
	}
	if other.closeCount() != 0 {
		t.Fatal("Disconnect(u-7) closed a connection belonging to another user")
	}

	closed, err = h.Disconnect("", "c3")
	if err != nil {
		t.Fatalf("Disconnect(client) = %v", err)
	}
	if closed != 1 {
		t.Fatalf("Disconnect(c3) closed %d, want 1", closed)
	}
	other.waitClose(t)
}

// TestDisconnect_TargetsAreExactNeverGlobs_C8 is the row that matters: a target of "u-*"
// reaches the connection literally named that and nothing else.
func TestDisconnect_TargetsAreExactNeverGlobs_C8(t *testing.T) {
	t.Parallel()
	h := newTestHub(t, newBus())
	s := newSink("c1", "u-7")
	mustAdd(t, h, s)

	closed, err := h.Disconnect("u-*", "")
	if err != nil {
		t.Fatalf("Disconnect = %v", err)
	}
	if closed != 0 {
		t.Fatalf("Disconnect(u-*) closed %d connections; targets are exact, never globs (C8)", closed)
	}
	if s.closeCount() != 0 {
		t.Fatal("a glob target reached a connection it does not name (C8)")
	}
}

// TestDisconnect_Validation refuses the same targeting mistakes the control channel does.
// An omitted target is a validation error and not "everyone" (C8).
func TestDisconnect_Validation(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		user   string
		client string
		want   error
	}{
		{"no target", "", "", ErrNoTarget},
		{"both targets", "u-7", "c1", ErrAmbiguousTarget},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHub(t, newBus())
			closed, err := h.Disconnect(tt.user, tt.client)
			if !errors.Is(err, tt.want) {
				t.Fatalf("Disconnect(%q, %q) = %v, want %v", tt.user, tt.client, err, tt.want)
			}
			if closed != 0 {
				t.Fatalf("Disconnect closed %d connections on a refused target", closed)
			}
		})
	}
}

// TestRegister_UserIndexFollowsAuthorization_FR18 is the case the whole control surface
// turns on and the one no other test here covers: a connection is registered *before* the
// application has answered, so Sink.User is empty at Add and only becomes the real user id
// by the time Attach runs (docs/09-internals.md §3, internal/server's handler).
//
// Indexing it once at Add leaves it filed under "" forever. A control disconnect naming
// its real user then reaches nothing — revocation silently does nothing, which is the
// worst possible way for a security control to fail — and Remove, which deletes under the
// current user id, leaves the connection in the "" bucket for the life of the process.
func TestRegister_UserIndexFollowsAuthorization_FR18(t *testing.T) {
	t.Parallel()
	h := newTestHub(t, newBus())

	s := newSink("c1", "")
	mustAdd(t, h, s)

	// The application answers, and the connection learns who it is.
	s.user = "u-7"
	h.Attach(s, []string{"room-1"}, func([]string) *proto.Frame { return nil })

	closed, err := h.Disconnect("u-7", "")
	if err != nil {
		t.Fatalf("Disconnect: %v", err)
	}
	if closed != 1 {
		t.Fatalf("Disconnect(u-7) closed %d; the connection is still filed under the empty "+
			"user it was registered with, so revocation reaches nothing (FR-18)", closed)
	}
	if got := s.waitClose(t); got.code != proto.CloseRevoked {
		t.Fatalf("close code = %d, want %d", got.code, proto.CloseRevoked)
	}
}

// TestRemove_LeavesNoUserIndexEntry_NFR3 is the other half of the same defect: Remove
// deletes under the connection's current user id, so a connection filed under a different
// one stays in the map after it is gone. That is an unbounded leak of one map entry and
// one dead Sink per connection, on a process meant to run for weeks.
func TestRemove_LeavesNoUserIndexEntry_NFR3(t *testing.T) {
	t.Parallel()
	h := newTestHub(t, newBus())

	s := newSink("c1", "")
	mustAdd(t, h, s)
	s.user = "u-7"
	h.Attach(s, nil, func([]string) *proto.Frame { return nil })
	h.Remove(s)

	h.mu.RLock()
	users := len(h.users)
	h.mu.RUnlock()
	if users != 0 {
		t.Fatalf("the user index holds %d entries after Remove, want 0 (NFR-3)", users)
	}
}
