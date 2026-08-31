package hub

import (
	"errors"
	"reflect"
	"testing"

	"github.com/raghulj/sidecartunnel/internal/proto"
)

// TestParseControl_Validation is C8 as a table. The two rows that matter most are the
// ones with no target and with both: an omitted target is a validation error, not
// "everyone", because otherwise one publish forces every connection on every replica to
// re-authorize at once — the outage docs/10-operations.md §4 models, available on demand.
func TestParseControl_Validation(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		payload string
		wantErr bool
	}{
		{"disconnect by user", `{"action":"disconnect","user":"u-7","reason":"suspended"}`, false},
		{"disconnect by client", `{"action":"disconnect","client":"8f2c1e04a7b3d915"}`, false},
		{"refresh by user", `{"action":"refresh","user":"u-7"}`, false},
		{"unsubscribe with a channel glob", `{"action":"unsubscribe","user":"u-7","channel":"org-42-*"}`, false},
		{"not json", `{`, true},
		{"missing action", `{"user":"u-7"}`, true},
		{"unknown action", `{"action":"explode","user":"u-7"}`, true},
		{"no target is never everyone", `{"action":"disconnect"}`, true},
		{"both targets", `{"action":"disconnect","user":"u-7","client":"c1"}`, true},
		{"unsubscribe without a channel", `{"action":"unsubscribe","user":"u-7"}`, true},
		{"unsubscribe of a reserved channel", `{"action":"unsubscribe","user":"u-7","channel":"_control"}`, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := ParseControl([]byte(tt.payload))
			if (err != nil) != tt.wantErr {
				t.Fatalf("ParseControl(%s) error = %v, wantErr %v", tt.payload, err, tt.wantErr)
			}
		})
	}
}

// TestControl_RejectsAnUnvalidatedCommand keeps the validation on the operation rather
// than only on the parser: a caller that builds a Control by hand gets the same refusal.
func TestControl_RejectsAnUnvalidatedCommand(t *testing.T) {
	t.Parallel()
	h := newTestHub(t, newBus())
	s := newSink("c1", "u1")
	mustAdd(t, h, s)
	if err := h.Control(Control{Action: ActionDisconnect}); !errors.Is(err, ErrNoTarget) {
		t.Fatalf("Control with no target = %v, want ErrNoTarget", err)
	}
	if n := s.closeCount(); n != 0 {
		t.Fatal("a targetless control message closed a connection")
	}
}

// TestControl_TargetsAreExactNeverGlobs_C8 is the other half of C8. "u-7" must not reach
// "u-70", and a target that looks like a glob matches the connection literally named that
// and nothing else.
func TestControl_TargetsAreExactNeverGlobs_C8(t *testing.T) {
	t.Parallel()
	h := newTestHub(t, newBus())
	target := newSink("c1", "u-7")
	neighbour := newSink("c2", "u-70")
	mustAdd(t, h, target, neighbour)

	if err := h.Control(Control{Action: ActionDisconnect, User: "u-7"}); err != nil {
		t.Fatalf("Control: %v", err)
	}
	if got := target.waitClose(t); got.code != proto.CloseRevoked {
		t.Fatalf("close code = %d, want %d (FR-18)", got.code, proto.CloseRevoked)
	}
	if n := neighbour.closeCount(); n != 0 {
		t.Fatal("a prefix-sharing user id was matched: targets are exact, never globs")
	}

	if err := h.Control(Control{Action: ActionDisconnect, User: "u-*"}); err != nil {
		t.Fatalf("Control: %v", err)
	}
	if n := neighbour.closeCount(); n != 0 {
		t.Fatal("a glob-looking user id matched a connection")
	}
}

// TestControl_DisconnectByClientDeregisters closes exactly one connection and leaves the
// registry with no trace of it (FR-18, M4).
func TestControl_DisconnectByClientDeregisters(t *testing.T) {
	t.Parallel()
	b := newBus()
	h := newTestHub(t, b)
	a, keep := newSink("aaa", "u1"), newSink("bbb", "u1")
	mustAdd(t, h, a, keep)
	mustSubscribe(t, h, a, "room-1")
	mustSubscribe(t, h, keep, "room-2")
	b.waitSync(t, "st:_control", "st:room-1", "st:room-2")

	if err := h.Control(Control{Action: ActionDisconnect, Client: "aaa", Reason: "account suspended"}); err != nil {
		t.Fatalf("Control: %v", err)
	}
	got := a.waitClose(t)
	if got.code != proto.CloseRevoked || got.reason != "account suspended" {
		t.Fatalf("close = %+v, want 3501 with the control message's reason", got)
	}
	if n := keep.closeCount(); n != 0 {
		t.Fatal("an untargeted connection was closed")
	}
	b.waitSync(t, "st:_control", "st:room-2")
}

// TestControl_RefreshClosesForReauthorization uses 3503, which is retryable: the client
// reconnects and re-handshakes with whatever cookie the browser currently holds (S3).
func TestControl_RefreshClosesForReauthorization(t *testing.T) {
	t.Parallel()
	h := newTestHub(t, newBus())
	s := newSink("c1", "u-7")
	mustAdd(t, h, s)
	if err := h.Control(Control{Action: ActionRefresh, User: "u-7"}); err != nil {
		t.Fatalf("Control: %v", err)
	}
	if got := s.waitClose(t); got.code != proto.CloseExpired {
		t.Fatalf("close code = %d, want %d", got.code, proto.CloseExpired)
	}
}

// TestControl_UnsubscribeDropsAndNotifies_FR17 is the requirement's whole point: dropping
// a subscription silently leaves the client's registry claiming a channel it will never
// hear from again, indistinguishable from a quiet channel, forever.
func TestControl_UnsubscribeDropsAndNotifies_FR17(t *testing.T) {
	t.Parallel()
	b := newBus()
	h := newTestHub(t, b)
	s := newSink("c1", "u-7")
	// A second tab of the same user, holding one of the same channels: each tab has its
	// own client id and both must be told.
	tab := newSink("c3", "u-7")
	other := newSink("c2", "u-8")
	mustAdd(t, h, s, tab, other)
	mustSubscribe(t, h, s, "org-42-a", "org-42-b", "room-1")
	mustSubscribe(t, h, tab, "org-42-a")
	mustSubscribe(t, h, other, "org-42-a")
	b.waitSync(t, "st:_control", "st:org-42-a", "st:org-42-b", "st:room-1")

	if err := h.Control(Control{Action: ActionUnsubscribe, User: "u-7", Channel: "org-42-*", Reason: "grant revoked"}); err != nil {
		t.Fatalf("Control: %v", err)
	}

	pushes := map[string]bool{}
	for i := 0; i < 2; i++ {
		frame := s.waitFrame(t)
		var got proto.PushFrame
		decodeFrame(t, frame, &got)
		if got.Push.Unsubscribed == nil || got.Push.Unsubscribed.Reason != "grant revoked" {
			t.Fatalf("frame = %s, want an unsubscribed push carrying the reason", frame.Data)
		}
		pushes[got.Push.Channel] = true
	}
	if !pushes["org-42-a"] || !pushes["org-42-b"] {
		t.Fatalf("pushes = %v, want one per dropped channel, named without the bus prefix", pushes)
	}

	if got := h.Subscriptions(s); !reflect.DeepEqual(got, []string{"room-1"}) {
		t.Fatalf("Subscriptions = %v, want only the unmatched channel (M16)", got)
	}
	if got := h.Subscriptions(other); !reflect.DeepEqual(got, []string{"org-42-a"}) {
		t.Fatalf("an untargeted user lost %v", got)
	}
	tabPush := tab.waitFrame(t)
	var tabFrame proto.PushFrame
	decodeFrame(t, tabPush, &tabFrame)
	if tabFrame.Push.Channel != "org-42-a" || tabFrame.Push.Unsubscribed == nil {
		t.Fatalf("second tab got %s, want its own unsubscribed push", tabPush.Data)
	}
	if got := h.Subscriptions(tab); len(got) != 0 {
		t.Fatalf("second tab kept %v", got)
	}
	// org-42-a still has a holder; org-42-b does not, so only that one leaves the
	// desired set (FR-10).
	b.waitSync(t, "st:_control", "st:org-42-a", "st:room-1")
	if n := len(other.got()); n != 0 {
		t.Fatal("an untargeted connection received an unsubscribed push")
	}
}

// TestControl_UnsubscribeNoMatchIsQuiet: a control unsubscribe naming a channel the
// target does not hold changes nothing and pushes nothing.
func TestControl_UnsubscribeNoMatchIsQuiet(t *testing.T) {
	t.Parallel()
	h := newTestHub(t, newBus())
	s := newSink("c1", "u-7")
	mustAdd(t, h, s)
	mustSubscribe(t, h, s, "room-1")

	if err := h.Control(Control{Action: ActionUnsubscribe, User: "u-7", Channel: "org-*"}); err != nil {
		t.Fatalf("Control: %v", err)
	}
	if n := len(s.got()); n != 0 {
		t.Fatalf("%d pushes for a channel the connection does not hold", n)
	}
	if got := h.Subscriptions(s); !reflect.DeepEqual(got, []string{"room-1"}) {
		t.Fatalf("Subscriptions = %v, want unchanged", got)
	}
}

// TestControl_UnsubscribeOnAFullQueueClosesTheConnection: the unsubscribed push obeys the
// same backpressure rule as a fan-out frame. It is queued under the hub lock so it cannot
// overtake the subscription change, and the close happens after the lock is released
// (FR-15, docs/09-internals.md §4.5).
func TestControl_UnsubscribeOnAFullQueueClosesTheConnection(t *testing.T) {
	t.Parallel()
	h := newTestHub(t, newBus())
	s := newSink("c1", "u-7")
	mustAdd(t, h, s)
	mustSubscribe(t, h, s, "room-1", "room-2")
	s.full.Store(true)

	if err := h.Control(Control{Action: ActionUnsubscribe, User: "u-7", Channel: "room-*"}); err != nil {
		t.Fatalf("Control: %v", err)
	}
	if got := s.waitClose(t); got.code != proto.CloseSlowConsumer {
		t.Fatalf("close code = %d, want %d", got.code, proto.CloseSlowConsumer)
	}
	if n := s.closeCount(); n != 1 {
		t.Fatalf("connection closed %d times for two undeliverable pushes, want 1", n)
	}
}

// TestControl_TargetMissingIsANoOp: a control message for a user or client held by
// another replica is not an error here. Every replica consumes every control message.
func TestControl_TargetMissingIsANoOp(t *testing.T) {
	t.Parallel()
	h := newTestHub(t, newBus())
	for _, c := range []Control{
		{Action: ActionDisconnect, Client: "nobody"},
		{Action: ActionDisconnect, User: "nobody"},
		{Action: ActionUnsubscribe, User: "nobody", Channel: "room-*"},
	} {
		if err := h.Control(c); err != nil {
			t.Fatalf("Control(%+v) = %v, want nil", c, err)
		}
	}
}

// TestControl_ParsedMessageApplies drives the two halves together, as the control
// consumer does.
func TestControl_ParsedMessageApplies(t *testing.T) {
	t.Parallel()
	h := newTestHub(t, newBus())
	s := newSink("8f2c1e04a7b3d915", "u-7")
	mustAdd(t, h, s)

	c, err := ParseControl([]byte(`{"action":"disconnect","client":"8f2c1e04a7b3d915"}`))
	if err != nil {
		t.Fatalf("ParseControl: %v", err)
	}
	if err := h.Control(c); err != nil {
		t.Fatalf("Control: %v", err)
	}
	if got := s.waitClose(t); got.code != proto.CloseRevoked {
		t.Fatalf("close code = %d, want %d", got.code, proto.CloseRevoked)
	}
}

// TestDisconnect_ByUserAndByClient_FR18 is Hub.Disconnect reached directly: the same
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
