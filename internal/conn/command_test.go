package conn

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/raghulj/sidecartunnel/internal/glob"
	"github.com/raghulj/sidecartunnel/internal/proto"
)

// TestCommandBeforeConnect_101 — docs/03-client-protocol.md §4.1: connect MUST be the
// first frame. Anything before it is error 101 and the connection stays open.
func TestCommandBeforeConnect_101(t *testing.T) {
	tests := []struct {
		name  string
		frame map[string]any
	}{
		{"subscribe", map[string]any{"id": 1, "subscribe": map[string]any{"channel": "room-1"}}},
		{"unsubscribe", map[string]any{"id": 1, "unsubscribe": map[string]any{"channel": "room-1"}}},
		{"publish", map[string]any{"id": 1, "publish": map[string]any{"channel": "room-1", "event": "e"}}},
		{"sync", map[string]any{"id": 1, "sync": map[string]any{}}},
		{"ping", map[string]any{"id": 1, "ping": map[string]any{}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := newRig(t)
			r.sock.feed(tt.frame)
			r.wantError(proto.ErrBadRequest)

			// The connection is still open: connect still works afterwards.
			if got := r.connect().Client; got != r.conn.ID() {
				t.Fatalf("client = %q, want %q", got, r.conn.ID())
			}
		})
	}
}

// TestDuplicateConnect_101 — docs/03-client-protocol.md §4.1: sending connect twice on one
// connection is error 101, not a second handshake.
func TestDuplicateConnect_101(t *testing.T) {
	r := newRig(t)
	r.connect()
	r.sock.feed(map[string]any{"id": 2, "connect": map[string]any{}})
	got := r.wantError(proto.ErrBadRequest)
	if got.Message == "" {
		t.Fatal("the error reply carries no message")
	}

	// Still open, and still the same connection: a duplicate connect must not
	// re-authorize, which would let a client refresh grants without a new socket (FR-22).
	r.sock.feed(map[string]any{"id": 3, "ping": map[string]any{}})
	if _, ok := r.sock.nextWrite(t)["pong"]; !ok {
		t.Fatal("the connection closed on a duplicate connect")
	}
}

// TestMalformedFrames_101 — docs/03-client-protocol.md §3: exactly one command key per
// frame; zero or several is 101 with the connection left open. A client that sends a
// malformed frame is usually a client with a bug, and disconnecting it turns a
// recoverable bug into a reconnect loop.
func TestMalformedFrames_101(t *testing.T) {
	tests := []struct {
		name string
		raw  string
	}{
		{"not an object", `["connect"]`},
		{"null", `null`},
		{"no command key", `{"id":1}`},
		{"two command keys", `{"id":1,"ping":{},"sync":{}}`},
		{"unknown command", `{"id":1,"resubscribe":{}}`},
		{"wrong case", `{"id":1,"Ping":{}}`},
		{"non-positive id", `{"id":0,"ping":{}}`},
		{"null body", `{"id":1,"subscribe":null}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := newRig(t)
			r.connect()
			r.sock.feedRaw(tt.raw)
			r.wantError(proto.ErrBadRequest)
		})
	}
}

// TestConnect_SubsAreFilteredNotFailed — docs/03-client-protocol.md §4.1: a channel that
// fails authorization is omitted from the reply map rather than failing the whole
// connect, and the client compares what it asked for against what it got.
func TestConnect_SubsAreFilteredNotFailed(t *testing.T) {
	r := newRig(t, func(o *Options) { o.Authorizer = okAuth(t, "u", "room-*", "desk-*") })
	r.reg.refuse["desk-9"] = true // an unknown namespace, refused by the registry

	reply := r.connect("room-1", "room-1", "org-99-secret", "_control", "desk-9", "room-2")

	want := map[string]bool{"room-1": true, "room-2": true}
	if len(reply.Subs) != len(want) {
		t.Fatalf("subs = %v, want %v", reply.Subs, want)
	}
	for channel := range want {
		if _, ok := reply.Subs[channel]; !ok {
			t.Fatalf("subs = %v, want %s present", reply.Subs, channel)
		}
	}
	if reply.Ping != 25 {
		t.Fatalf("ping = %d, want the ping interval in seconds", reply.Ping)
	}
}

// TestConnect_SubsCappedAtTheLimit — FR-8's cap, limits.max_subscriptions_per_conn.
func TestConnect_SubsCappedAtTheLimit(t *testing.T) {
	r := newRig(t, func(o *Options) { o.MaxSubscriptions = 2 })
	reply := r.connect("room-1", "room-2", "room-3", "room-4")
	if len(reply.Subs) != 2 {
		t.Fatalf("subs = %v, want 2 channels", reply.Subs)
	}
}

// TestSubscribe covers docs/03-client-protocol.md §4.2 and the codes in §6.
func TestSubscribe(t *testing.T) {
	tests := []struct {
		name     string
		frame    map[string]any
		setup    func(*rig)
		wantCode proto.ErrCode // 0 means a successful subscribe reply
	}{
		{
			name:  "granted",
			frame: map[string]any{"id": 2, "subscribe": map[string]any{"channel": "room-1"}},
		},
		{
			name:     "not granted",
			frame:    map[string]any{"id": 2, "subscribe": map[string]any{"channel": "org-99-secret"}},
			wantCode: proto.ErrPermissionDenied,
		},
		{
			// docs/06-channels.md §4: refused before the grants are consulted, so a grant
			// of "*" still cannot reach a control channel.
			name:     "reserved prefix",
			frame:    map[string]any{"id": 2, "subscribe": map[string]any{"channel": "_control"}},
			setup:    func(r *rig) { r.conn.SetGrants(mustGrants(t, "*")) },
			wantCode: proto.ErrPermissionDenied,
		},
		{
			name:     "channel too long",
			frame:    map[string]any{"id": 2, "subscribe": map[string]any{"channel": strings.Repeat("x", 300)}},
			wantCode: proto.ErrBadRequest,
		},
		{
			name:     "empty channel",
			frame:    map[string]any{"id": 2, "subscribe": map[string]any{"channel": ""}},
			wantCode: proto.ErrBadRequest,
		},
		{
			name:     "no id",
			frame:    map[string]any{"subscribe": map[string]any{"channel": "room-1"}},
			wantCode: proto.ErrBadRequest,
		},
		{
			name:  "registry refuses",
			frame: map[string]any{"id": 2, "subscribe": map[string]any{"channel": "room-1"}},
			setup: func(r *rig) {
				r.reg.subscribeErr = &CommandError{Code: proto.ErrAlreadySubscribed, Message: "already subscribed"}
			},
			wantCode: proto.ErrAlreadySubscribed,
		},
		{
			name:  "registry limit",
			frame: map[string]any{"id": 2, "subscribe": map[string]any{"channel": "room-1"}},
			setup: func(r *rig) {
				r.reg.subscribeErr = &CommandError{Code: proto.ErrSubscriptionLimit, Message: "subscription limit"}
			},
			wantCode: proto.ErrSubscriptionLimit,
		},
		{
			// A registry failure that is not a client-visible refusal becomes 100, which
			// carries no detail on the wire: the client can do nothing with it but retry.
			name:     "registry fault",
			frame:    map[string]any{"id": 2, "subscribe": map[string]any{"channel": "room-1"}},
			setup:    func(r *rig) { r.reg.subscribeErr = errors.New("bus unavailable") },
			wantCode: proto.ErrInternal,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := newRig(t)
			r.connect()
			if tt.setup != nil {
				tt.setup(r)
			}
			r.sock.feed(tt.frame)

			if tt.wantCode != 0 {
				r.wantError(tt.wantCode)
				return
			}
			frame := r.sock.nextWrite(t)
			if _, ok := frame["subscribe"]; !ok {
				t.Fatalf("frame = %v, want a subscribe reply", frame)
			}
			if got := r.reg.Subscriptions(r.conn); len(got) != 1 || got[0] != "room-1" {
				t.Fatalf("registry holds %v, want [room-1]", got)
			}
		})
	}
}

// TestUnsubscribe covers docs/03-client-protocol.md §4.3.
func TestUnsubscribe(t *testing.T) {
	t.Run("held", func(t *testing.T) {
		r := newRig(t)
		r.connect("room-1")
		r.sock.feed(map[string]any{"id": 2, "unsubscribe": map[string]any{"channel": "room-1"}})
		frame := r.sock.nextWrite(t)
		if _, ok := frame["unsubscribe"]; !ok {
			t.Fatalf("frame = %v, want an unsubscribe reply", frame)
		}
		if got := r.reg.Subscriptions(r.conn); len(got) != 0 {
			t.Fatalf("registry still holds %v", got)
		}
	})

	t.Run("not held is 105", func(t *testing.T) {
		r := newRig(t)
		r.connect()
		r.sock.feed(map[string]any{"id": 2, "unsubscribe": map[string]any{"channel": "room-1"}})
		r.wantError(proto.ErrNotSubscribed)
	})

	// A grant can be narrowed while a subscription is held. Dropping such a channel must
	// succeed: telling a client it lacks permission for something it is giving up leaves
	// the channel in the registry forever (FR-17).
	t.Run("no longer granted", func(t *testing.T) {
		r := newRig(t)
		r.connect("room-1")
		r.conn.SetGrants(mustGrants(t, "desk-*"))
		r.sock.feed(map[string]any{"id": 2, "unsubscribe": map[string]any{"channel": "room-1"}})
		if _, ok := r.sock.nextWrite(t)["unsubscribe"]; !ok {
			t.Fatal("unsubscribing a narrowed channel was refused")
		}
	})

	t.Run("no id", func(t *testing.T) {
		r := newRig(t)
		r.connect()
		r.sock.feed(map[string]any{"unsubscribe": map[string]any{"channel": "room-1"}})
		r.wantError(proto.ErrBadRequest)
	})

	t.Run("channel too long", func(t *testing.T) {
		r := newRig(t)
		r.connect()
		r.sock.feed(map[string]any{"id": 2, "unsubscribe": map[string]any{"channel": strings.Repeat("x", 300)}})
		r.wantError(proto.ErrBadRequest)
	})
}

// TestPublish covers docs/03-client-protocol.md §4.4. The grant is checked here and
// client_events in the registry, because a client event requires both: the namespace flag
// alone would let any connected client inject fabricated events into a channel it cannot
// even read (docs/13-review-findings.md M19).
func TestPublish(t *testing.T) {
	t.Run("granted", func(t *testing.T) {
		r := newRig(t)
		r.connect()
		r.sock.feed(map[string]any{"id": 2, "publish": map[string]any{
			"channel": "room-1", "event": "typing", "data": map[string]any{"typing": true},
		}})
		if _, ok := r.sock.nextWrite(t)["publish"]; !ok {
			t.Fatal("no publish reply")
		}
		r.reg.mu.Lock()
		defer r.reg.mu.Unlock()
		if len(r.reg.published) != 1 {
			t.Fatalf("registry saw %d publishes, want 1", len(r.reg.published))
		}
		got := r.reg.published[0]
		if got.channel != "room-1" || got.event != "typing" || got.data != `{"typing":true}` {
			t.Fatalf("published %+v, want the payload passed through untouched", got)
		}
	})

	tests := []struct {
		name     string
		frame    map[string]any
		setup    func(*rig)
		wantCode proto.ErrCode
	}{
		{
			name:     "no event",
			frame:    map[string]any{"id": 2, "publish": map[string]any{"channel": "room-1"}},
			wantCode: proto.ErrBadRequest,
		},
		{
			name:     "no id",
			frame:    map[string]any{"publish": map[string]any{"channel": "room-1", "event": "e"}},
			wantCode: proto.ErrBadRequest,
		},
		{
			name:     "not granted",
			frame:    map[string]any{"id": 2, "publish": map[string]any{"channel": "org-99-secret", "event": "e"}},
			wantCode: proto.ErrPermissionDenied,
		},
		{
			name:  "namespace forbids client events",
			frame: map[string]any{"id": 2, "publish": map[string]any{"channel": "room-1", "event": "e"}},
			setup: func(r *rig) {
				r.reg.publishErr = &CommandError{Code: proto.ErrPermissionDenied, Message: "client events disabled"}
			},
			wantCode: proto.ErrPermissionDenied,
		},
		{
			name:     "rate limited",
			frame:    map[string]any{"id": 2, "publish": map[string]any{"channel": "room-1", "event": "e"}},
			setup:    func(r *rig) { r.reg.publishErr = &CommandError{Code: proto.ErrRateLimited, Message: "rate limited"} },
			wantCode: proto.ErrRateLimited,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := newRig(t)
			r.connect()
			if tt.setup != nil {
				tt.setup(r)
			}
			r.sock.feed(tt.frame)
			r.wantError(tt.wantCode)
		})
	}
}

// TestSync covers docs/03-client-protocol.md §4.5: the gateway's authoritative set, which
// is the first thing to call when debugging "nobody receives anything".
func TestSync(t *testing.T) {
	t.Run("returns the registry's set", func(t *testing.T) {
		r := newRig(t)
		r.connect("room-1", "room-2")
		r.sock.feed(map[string]any{"id": 5, "sync": map[string]any{}})

		var reply proto.SyncReply
		if err := json.Unmarshal(r.sock.nextWrite(t)["sync"], &reply); err != nil {
			t.Fatalf("sync reply: %v", err)
		}
		if len(reply.Channels) != 2 {
			t.Fatalf("channels = %v, want two", reply.Channels)
		}
	})

	t.Run("empty is an array", func(t *testing.T) {
		r := newRig(t)
		r.connect()
		r.sock.feed(map[string]any{"id": 5, "sync": map[string]any{}})
		if got := string(r.sock.nextWrite(t)["sync"]); got != `{"channels":[]}` {
			t.Fatalf("sync reply = %s, want an empty array, never null", got)
		}
	})

	t.Run("no id", func(t *testing.T) {
		r := newRig(t)
		r.connect()
		r.sock.feed(map[string]any{"sync": map[string]any{}})
		r.wantError(proto.ErrBadRequest)
	})
}

// TestPing_EchoesID — docs/03-client-protocol.md §4.6: without the echo a client with two
// pings in flight cannot correlate replies or measure round-trip time.
func TestPing_EchoesID(t *testing.T) {
	tests := []struct {
		name  string
		frame map[string]any
		want  string
	}{
		{"with id", map[string]any{"id": 9, "ping": map[string]any{}}, `{"id":9,"pong":{}}`},
		{"without id", map[string]any{"ping": map[string]any{}}, `{"pong":{}}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := newRig(t)
			r.connect()
			r.sock.feed(tt.frame)
			select {
			case data := <-r.sock.writes:
				if string(data) != tt.want {
					t.Fatalf("pong = %s, want %s", data, tt.want)
				}
			case <-r.done:
				t.Fatal("the connection closed instead of answering")
			}
		})
	}
}

// TestAllows_NoGrantsMatchesNothing — a connection whose webhook has not answered yet
// matches nothing. Failing closed is the only safe direction (FR-5).
func TestAllows_NoGrantsMatchesNothing(t *testing.T) {
	c, err := New(Options{Socket: newFakeSocket(), Registry: newFakeRegistry(), Authorizer: okAuth(t, "u")})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if c.Allows("room-1") {
		t.Fatal("a connection with no grant set matched a channel")
	}
}

// TestCommandError_Error keeps the code in the message, so a wrapped registry error is
// readable in a log without the reader guessing which refusal it was.
func TestCommandError_Error(t *testing.T) {
	err := error(&CommandError{Code: proto.ErrRateLimited, Message: "rate limited"})
	if got := err.Error(); !strings.Contains(got, "106") || !strings.Contains(got, "rate limited") {
		t.Fatalf("Error() = %q, want the code and the message", got)
	}
	wrapped := fmt.Errorf("registry: %w", err)
	var target *CommandError
	if !errors.As(wrapped, &target) || target.Code != proto.ErrRateLimited {
		t.Fatalf("errors.As failed on %v", wrapped)
	}
}

// mustGrants compiles a grant list or fails the test. Compiling is the Authorizer's job
// in production — Authorization.Grants arrives as a glob.Set — so a test that wants a
// connection with grants compiles them the same way internal/webhook does.
func mustGrants(t *testing.T, patterns ...string) glob.Set {
	t.Helper()
	set, err := glob.NewSet(patterns)
	if err != nil {
		t.Fatalf("glob.NewSet(%v): %v", patterns, err)
	}
	return set
}

// TestConnect_RequestedChannelsReachTheAuthorizer_FR3 — the connect frame's subs are
// handed to the Authorizer, which is the only way they can reach the connect webhook's
// channels_requested (docs/04-integration.md §1.1).
//
// The connection authorizes after the frame has been read, so the Authorizer is the seam
// that carries them: a signature taking only a context leaves the value with nowhere to
// go, and the normative request example then shows a field the gateway never sends.
func TestConnect_RequestedChannelsReachTheAuthorizer_FR3(t *testing.T) {
	seen := make(chan []string, 1)
	r := newRig(t, func(o *Options) {
		set := mustGrants(t, "room-*")
		o.Authorizer = AuthorizerFunc(func(_ context.Context, requested []string) (Authorization, error) {
			seen <- requested
			return Authorization{User: "u", Grants: set, ExpiresIn: time.Hour}, nil
		})
	})

	r.connect("room-1", "room-2")

	select {
	case got := <-seen:
		if !slices.Equal(got, []string{"room-1", "room-2"}) {
			t.Fatalf("requested = %v, want the channels the connect frame asked for", got)
		}
	case <-time.After(failAfter):
		t.Fatal("the Authorizer was never called")
	}
}

// TestConnect_RequestedChannelsAreBounded_FR3 — the list is untrusted client input on its
// way into a webhook body, so it is bounded before it leaves the connection:
// limits.max_subscriptions_per_conn entries, each at most limits.max_channel_length
// bytes, deduplicated.
//
// Unbounded, one connect frame is an amplification vector into the application: the
// gateway would sign and POST whatever a client typed, at the connection rate, to the
// component least able to absorb it (NFR-4). Names that cannot become subscriptions
// anyway — empty, or over the length cap — are dropped rather than forwarded, because
// forwarding them costs the application bytes and tells it nothing.
func TestConnect_RequestedChannelsAreBounded_FR3(t *testing.T) {
	tests := []struct {
		name string

		// max is limits.max_subscriptions_per_conn, maxLen limits.max_channel_length.
		max    int
		maxLen int

		subs []string
		want []string
	}{
		{
			name: "forwarded in the order the client asked",
			max:  500, maxLen: 255,
			subs: []string{"room-2", "room-1"}, want: []string{"room-2", "room-1"},
		},
		{
			name: "capped at limits.max_subscriptions_per_conn",
			max:  2, maxLen: 255,
			subs: []string{"room-1", "room-2", "room-3", "room-4"}, want: []string{"room-1", "room-2"},
		},
		{
			name: "a name over limits.max_channel_length is dropped",
			max:  500, maxLen: 6,
			subs: []string{"room-1", "room-1000"}, want: []string{"room-1"},
		},
		{
			name: "an empty name is dropped",
			max:  500, maxLen: 255,
			subs: []string{"", "room-1"}, want: []string{"room-1"},
		},
		{
			name: "duplicates collapse, so a repeated name cannot inflate the body",
			max:  500, maxLen: 255,
			subs: []string{"room-1", "room-1", "room-1"}, want: []string{"room-1"},
		},
		{
			name: "a connect that asked for nothing forwards nothing",
			max:  500, maxLen: 255,
			subs: nil, want: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			seen := make(chan []string, 1)
			r := newRig(t, func(o *Options) {
				o.MaxSubscriptions = tt.max
				o.MaxChannelLength = tt.maxLen
				set := mustGrants(t, "room-*")
				o.Authorizer = AuthorizerFunc(func(_ context.Context, requested []string) (Authorization, error) {
					seen <- requested
					return Authorization{User: "u", Grants: set, ExpiresIn: time.Hour}, nil
				})
			})

			r.connect(tt.subs...)

			select {
			case got := <-seen:
				if !slices.Equal(got, tt.want) {
					t.Fatalf("requested = %v, want %v", got, tt.want)
				}
			case <-time.After(failAfter):
				t.Fatal("the Authorizer was never called")
			}
		})
	}
}

// TestConnect_RequestedChannelsConferNoGrant_FR5 is the invariant the whole gateway turns
// on: the application decides, the gateway enforces. The requested list is a hint carried
// to the application and nothing else — a channel that appears in it and not in the
// grants the application answered with must be refused exactly as if it had never been
// asked for.
//
// It fails the moment anything downstream reads the requested list as authority: a
// connection that subscribes to what it asked for, or an Authorizer whose answer is
// merged with the request rather than replacing it.
func TestConnect_RequestedChannelsConferNoGrant_FR5(t *testing.T) {
	r := newRig(t, func(o *Options) {
		set := mustGrants(t, "room-*")
		o.Authorizer = AuthorizerFunc(func(_ context.Context, requested []string) (Authorization, error) {
			// The application ignored the request entirely, which is its right: the
			// requested list is informational and the grants are the answer.
			_ = requested
			return Authorization{User: "u", Grants: set, ExpiresIn: time.Hour}, nil
		})
	})

	reply := r.connect("room-1", "vault-1")

	if _, ok := reply.Subs["vault-1"]; ok {
		t.Fatalf("subs = %v: asking for a channel granted it (FR-5)", reply.Subs)
	}
	if _, ok := reply.Subs["room-1"]; !ok {
		t.Fatalf("subs = %v, want the granted channel present", reply.Subs)
	}
	if r.conn.Allows("vault-1") {
		t.Fatal("Allows(\"vault-1\") is true: the requested list widened the grant set (FR-5)")
	}

	// And it is still refused as a later command, so nothing further along the connection
	// remembers the request either.
	r.sock.feed(map[string]any{"id": 2, "subscribe": map[string]any{"channel": "vault-1"}})
	r.wantError(proto.ErrPermissionDenied)
}
