package server

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/raghulj/sidecartunnel/internal/config"
	"github.com/raghulj/sidecartunnel/internal/conn"
	"github.com/raghulj/sidecartunnel/internal/hub"
	"github.com/raghulj/sidecartunnel/internal/proto"
)

// TestCommandError_MapsHubRefusalsToProtocolCodes is the whole of the adapter's error
// contract, and it is a table because the mapping is the kind of thing a refactor
// silently collapses: every one of these codes means something different to a client, and
// two of them mean "your registry has drifted" in opposite directions.
func TestCommandError_MapsHubRefusalsToProtocolCodes(t *testing.T) {
	t.Parallel()
	stray := errors.New("something else entirely")
	tests := []struct {
		name string
		err  error
		want proto.ErrCode
	}{
		{name: "nil stays nil", err: nil},
		{name: "already subscribed", err: wrapped(hub.ErrAlreadySubscribed), want: proto.ErrAlreadySubscribed},
		{name: "not subscribed", err: wrapped(hub.ErrNotSubscribed), want: proto.ErrNotSubscribed},
		{name: "subscription limit", err: wrapped(hub.ErrSubscriptionLimit), want: proto.ErrSubscriptionLimit},
		{name: "unknown namespace", err: wrapped(hub.ErrUnknownNamespace), want: proto.ErrUnknownNamespace},
		// The same code as an ungranted channel: answering differently would make the
		// existence of a control channel detectable by trying to subscribe to one.
		{name: "reserved channel", err: wrapped(hub.ErrReservedChannel), want: proto.ErrPermissionDenied},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := commandError(tt.err)
			if tt.err == nil {
				if got != nil {
					t.Fatalf("commandError(nil) = %v, want nil", got)
				}
				return
			}
			var cmdErr *conn.CommandError
			if !errors.As(got, &cmdErr) {
				t.Fatalf("commandError(%v) = %v, want a *conn.CommandError", tt.err, got)
			}
			if cmdErr.Code != tt.want {
				t.Fatalf("code = %d, want %d", cmdErr.Code, tt.want)
			}
		})
	}

	// Anything the hub did not name is a gateway fault, and the connection answers 100.
	// Reporting a specific code for it would be inventing an answer the client could act
	// on when there is none.
	if got := commandError(stray); !errors.Is(got, stray) {
		t.Fatalf("commandError(stray) = %v, want the error unchanged", got)
	}
}

// wrapped mimics how the hub returns a sentinel: wrapped with the channel that caused it.
// The mapping must survive that, which is why it uses errors.Is and never a comparison.
func wrapped(err error) error {
	return errorsJoinChannel("room-1", err)
}

// TestSubscribeUnsubscribeSync walks the three commands that move subscription state, over
// a real socket, and finishes on sync — the only way a client can discover that the
// gateway dropped something it did not ask to drop (docs/03-client-protocol.md §4.5, M16).
func TestSubscribeUnsubscribeSync(t *testing.T) {
	t.Parallel()
	r := newRig(t)
	c := r.dial()
	c.connect("room-1")

	c.send(map[string]any{"id": 2, "subscribe": map[string]any{"channel": "room-2"}})
	if got := c.read(); got.Subscribe == nil || got.ID != 2 {
		t.Fatalf("frame = %+v, want a subscribe reply against id 2", got)
	}

	c.send(map[string]any{"id": 3, "sync": map[string]any{}})
	got := c.read()
	if got.Sync == nil {
		t.Fatalf("frame = %+v, want a sync reply", got)
	}
	if want := []string{"room-1", "room-2"}; !reflect.DeepEqual(got.Sync.Channels, want) {
		t.Fatalf("sync channels = %v, want %v", got.Sync.Channels, want)
	}

	c.send(map[string]any{"id": 4, "unsubscribe": map[string]any{"channel": "room-1"}})
	if got := c.read(); got.Unsubscribe == nil {
		t.Fatalf("frame = %+v, want an unsubscribe reply", got)
	}

	c.send(map[string]any{"id": 5, "sync": map[string]any{}})
	if got := c.read(); !reflect.DeepEqual(got.Sync.Channels, []string{"room-2"}) {
		t.Fatalf("sync channels = %v, want only room-2", got.Sync.Channels)
	}
}

// TestSubscribe_Refusals covers every code a subscribe can come back with, over the wire,
// because the code is the whole of what a client acts on.
func TestSubscribe_Refusals(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		channel string
		connect []string
		opts    func(*config.Config)
		want    proto.ErrCode
	}{
		{
			// Not silently idempotent: a duplicate subscribe means the client's own
			// registry has drifted, and hiding that makes reconnect bugs hard to find.
			name: "already subscribed", channel: "room-1", connect: []string{"room-1"},
			want: proto.ErrAlreadySubscribed,
		},
		{
			// FR-5: a channel matching no grant never reaches the registry.
			name: "not granted", channel: "secret-1", want: proto.ErrPermissionDenied,
		},
		{
			// docs/06-channels.md §4: refused before grants are consulted, so a grant of
			// "*" still cannot reach a control channel.
			name: "reserved", channel: "_control", want: proto.ErrPermissionDenied,
		},
		{
			// FR-11: a channel whose namespace has no block fails closed.
			name: "unknown namespace", channel: "nope-1", want: proto.ErrUnknownNamespace,
		},
		{
			name: "subscription limit", channel: "room-2", connect: []string{"room-1"},
			opts: func(c *config.Config) { c.Limits.MaxSubscriptionsPerConn = 1 },
			want: proto.ErrSubscriptionLimit,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var mutate []func(*config.Config)
			if tt.opts != nil {
				mutate = append(mutate, tt.opts)
			}
			r := newRig(t, mutate...)
			r.web.answer(authorized("u-1", "room-*", "desk-*", "nope-*"))

			c := r.dial()
			c.connect(tt.connect...)
			c.send(map[string]any{"id": 9, "subscribe": map[string]any{"channel": tt.channel}})

			got := c.read()
			if got.Error == nil {
				t.Fatalf("frame = %+v, want an error reply", got)
			}
			if got.Error.Code != tt.want {
				t.Fatalf("error code = %d, want %d", got.Error.Code, tt.want)
			}
			// docs/03-client-protocol.md §6: an error code says the command failed, never
			// that the session is over. The connection stays open.
			c.send(map[string]any{"id": 10, "ping": map[string]any{}})
			if next := c.read(); next.ID != 10 {
				t.Fatalf("frame after an error = %+v, want the pong: the connection must stay open", next)
			}
		})
	}
}

// TestUnsubscribe_NotSubscribed_105: not idempotent, for the same reason as a duplicate
// subscribe.
func TestUnsubscribe_NotSubscribed_105(t *testing.T) {
	t.Parallel()
	r := newRig(t)
	c := r.dial()
	c.connect()

	c.send(map[string]any{"id": 2, "unsubscribe": map[string]any{"channel": "room-1"}})
	got := c.read()
	if got.Error == nil || got.Error.Code != proto.ErrNotSubscribed {
		t.Fatalf("frame = %+v, want error %d", got, proto.ErrNotSubscribed)
	}
}

// TestClientEvent_StampedAndExcluded_M19 is the client-event path end to end: the
// publisher's own event reaches the other subscriber stamped with the publisher's user id,
// and never comes back to the publisher.
//
// from is the gateway's to set and the client's never to set: a client that could name
// itself could name anybody (docs/03-client-protocol.md §4.4).
func TestClientEvent_StampedAndExcluded_M19(t *testing.T) {
	t.Parallel()
	r := newRig(t)
	r.pump()

	publisher := r.dial()
	publisher.connect("desk-1")
	listener := r.dial()
	listener.connect("desk-1")

	publisher.send(map[string]any{"id": 2, "publish": map[string]any{
		"channel": "desk-1", "event": "typing", "data": map[string]any{"typing": true},
	}})
	if got := publisher.read(); got.Publish == nil {
		t.Fatalf("frame = %+v, want a publish reply", got)
	}

	got := listener.read()
	if got.Push == nil || got.Push.Pub == nil {
		t.Fatalf("frame = %+v, want a push", got)
	}
	if got.Push.Channel != "desk-1" || got.Push.Pub.Event != "typing" {
		t.Fatalf("push = %+v, want the client event on desk-1", got.Push)
	}
	if got.Push.Pub.From != "u-1" {
		t.Fatalf("from = %q, want the publisher's user id stamped by the gateway", got.Push.Pub.From)
	}
	if string(got.Push.Pub.Data) != `{"typing":true}` {
		t.Fatalf("data = %s, want the payload passed through untouched", got.Push.Pub.Data)
	}

	// FR-13: the publisher is excluded from its own event. Asserting the absence of a
	// frame without a sleep: a ping sent afterwards must be answered by the pong and not
	// by a push that was queued ahead of it.
	publisher.send(map[string]any{"id": 3, "ping": map[string]any{}})
	if next := publisher.read(); next.Push != nil {
		t.Fatalf("the publisher received its own event: %+v (FR-13)", next.Push)
	} else if next.ID != 3 {
		t.Fatalf("frame = %+v, want the pong", next)
	}
}

// TestClientEvent_RequiresClientEvents_M19: a grant matching the channel is necessary and
// not sufficient. Without namespaces[].client_events the event is refused, or any
// connected client could inject fabricated events into a channel it can read.
func TestClientEvent_RequiresClientEvents_M19(t *testing.T) {
	t.Parallel()
	r := newRig(t)
	c := r.dial()
	c.connect("room-1")

	c.send(map[string]any{"id": 2, "publish": map[string]any{"channel": "room-1", "event": "typing", "data": 1}})
	got := c.read()
	if got.Error == nil || got.Error.Code != proto.ErrPermissionDenied {
		t.Fatalf("frame = %+v, want error %d", got, proto.ErrPermissionDenied)
	}
}

// TestClientEvent_UnknownNamespace_102: the namespace check happens on the publish path
// too, and fails closed the same way.
func TestClientEvent_UnknownNamespace_102(t *testing.T) {
	t.Parallel()
	r := newRig(t)
	r.web.answer(authorized("u-1", "nope-*"))

	c := r.dial()
	c.connect()
	c.send(map[string]any{"id": 2, "publish": map[string]any{"channel": "nope-1", "event": "e", "data": 1}})

	got := c.read()
	if got.Error == nil || got.Error.Code != proto.ErrUnknownNamespace {
		t.Fatalf("frame = %+v, want error %d", got, proto.ErrUnknownNamespace)
	}
}

// TestClientEvent_RateLimited_106_And_3007 covers both halves of
// docs/03-client-protocol.md §4.4: one event over the namespace's rate is error 106 with
// the connection left open, and ten violations within 60 seconds close it with 3007 and a
// retry_after of 60s.
//
// The delay is not optional. Without it the anti-abuse control amplifies load onto the
// connect webhook, which is the component least able to absorb it (m13).
func TestClientEvent_RateLimited_106_And_3007(t *testing.T) {
	t.Parallel()
	r := newRig(t, func(c *config.Config) {
		c.Namespaces = []config.Namespace{{Name: "desk", ClientEvents: true, RateLimit: "1/s"}}
	})

	c := r.dial()
	c.connect("desk-1")

	publish := func(id int) {
		c.send(map[string]any{"id": id, "publish": map[string]any{"channel": "desk-1", "event": "e", "data": 1}})
	}

	publish(2)
	if got := c.read(); got.Publish == nil {
		t.Fatalf("frame = %+v, want the first event inside the rate", got)
	}

	// The clock never moves, so every following event falls in the same window.
	for i := range maxViolations - 1 {
		publish(3 + i)
		got := c.read()
		if got.Error == nil || got.Error.Code != proto.ErrRateLimited {
			t.Fatalf("violation %d: frame = %+v, want error %d", i+1, got, proto.ErrRateLimited)
		}
	}

	publish(100)
	disc := c.wantDisconnect(proto.CloseRateLimited)
	if !disc.Reconnect {
		t.Fatal("3007 is reconnect true: the client may come back after the delay")
	}
	if disc.RetryAfter != (60 * time.Second).Milliseconds() {
		t.Fatalf("retry_after = %dms, want 60s (docs/03-client-protocol.md §4.4)", disc.RetryAfter)
	}
}

// TestRateLimit_WindowResets: the limit is a rate, not a quota. A connection that waits
// out its window may publish again, or the first burst of a long-lived page would silence
// it forever.
func TestRateLimit_WindowResets(t *testing.T) {
	t.Parallel()
	clk := newFakeClock()
	l := newLimiter(clk)
	limit := rate{count: 1, per: time.Second}

	if allowed, _ := l.allow("desk", limit); !allowed {
		t.Fatal("the first event was refused")
	}
	if allowed, abusive := l.allow("desk", limit); allowed || abusive {
		t.Fatal("the second event in the same window must be refused, and one violation is not abuse")
	}
	clk.Advance(time.Second)
	if allowed, _ := l.allow("desk", limit); !allowed {
		t.Fatal("the window did not reset")
	}
}

// TestRateLimit_ViolationWindowResets: ten violations spread over hours are a badly
// written client, not an abusive one, and closing it would be a reconnect loop the
// gateway caused.
func TestRateLimit_ViolationWindowResets(t *testing.T) {
	t.Parallel()
	clk := newFakeClock()
	l := newLimiter(clk)
	limit := rate{count: 1, per: time.Second}

	for range maxViolations {
		if _, abusive := l.allow("desk", limit); abusive {
			t.Fatal("closed a connection whose violations were spread across windows")
		}
		clk.Advance(violationWindow)
	}
}

// TestParseRate covers the "<int>/<s|m>" form of docs/08-config.md §3. An unparseable
// value is a startup error naming the key, never a silent default: a gateway that claims
// a rate limit it is not applying is a lie an operator will act on (NFR-5).
func TestParseRate(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		value string
		want  rate
		bad   bool
	}{
		{name: "per second", value: "10/s", want: rate{count: 10, per: time.Second}},
		{name: "per minute", value: "5/m", want: rate{count: 5, per: time.Minute}},
		{name: "no unit", value: "10", bad: true},
		{name: "unknown unit", value: "10/h", bad: true},
		{name: "not an integer", value: "ten/s", bad: true},
		{name: "zero", value: "0/s", bad: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := parseRate(tt.value)
			if tt.bad {
				if err == nil {
					t.Fatalf("parseRate(%q) succeeded; want an error", tt.value)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseRate(%q): %v", tt.value, err)
			}
			if got != tt.want {
				t.Fatalf("parseRate(%q) = %+v, want %+v", tt.value, got, tt.want)
			}
		})
	}
}

// TestParseRates_DefaultsAnEmptyValue: a namespace block that names no rate gets the
// documented default rather than a zero that would refuse every event.
func TestParseRates_DefaultsAnEmptyValue(t *testing.T) {
	t.Parallel()
	rates, err := parseRates([]config.Namespace{{Name: "desk"}})
	if err != nil {
		t.Fatalf("parseRates: %v", err)
	}
	if got := rates["desk"]; got != defaultRate {
		t.Fatalf("rate = %+v, want the documented default %+v", got, defaultRate)
	}
	// FR-11: the reserved empty name is the block a channel with no separator resolves
	// to, and internal/hub installs it when the list is empty.
	if got := rates[""]; got != defaultRate {
		t.Fatalf("reserved empty-name rate = %+v, want %+v", got, defaultRate)
	}
}
