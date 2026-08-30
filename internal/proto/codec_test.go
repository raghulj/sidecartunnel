package proto

import (
	"encoding/json"
	"strings"
	"testing"
)

// TestDecode_SpecEnvelopes decodes every client frame literal that
// docs/03-client-protocol.md shows, and re-encodes it. A third-party client is written
// from those literals, so they are the contract: if one of them stops decoding, or
// decodes to something other than the obvious struct, the gateway has silently forked
// the protocol.
func TestDecode_SpecEnvelopes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		frame string
		want  Command
		// wantJSON is the canonical re-encoding. It differs from frame only where the
		// wire format has a required field the example omits.
		wantJSON string
	}{
		{
			// docs/03-client-protocol.md §3, §9.
			name:     "connect with subs",
			frame:    `{"id":1,"connect":{"subs":["room-4410"]}}`,
			want:     Command{ID: 1, Connect: &ConnectRequest{Subs: []string{"room-4410"}}},
			wantJSON: `{"id":1,"connect":{"subs":["room-4410"]}}`,
		},
		{
			name:     "connect with no body fields",
			frame:    `{"id":1,"connect":{}}`,
			want:     Command{ID: 1, Connect: &ConnectRequest{}},
			wantJSON: `{"id":1,"connect":{}}`,
		},
		{
			// docs/03-client-protocol.md §3.
			name:     "subscribe",
			frame:    `{"id":2,"subscribe":{"channel":"room-4410"}}`,
			want:     Command{ID: 2, Subscribe: &SubscribeRequest{Channel: "room-4410"}},
			wantJSON: `{"id":2,"subscribe":{"channel":"room-4410"}}`,
		},
		{
			// docs/03-client-protocol.md §3.
			name:     "unsubscribe",
			frame:    `{"id":3,"unsubscribe":{"channel":"room-4410"}}`,
			want:     Command{ID: 3, Unsubscribe: &UnsubscribeRequest{Channel: "room-4410"}},
			wantJSON: `{"id":3,"unsubscribe":{"channel":"room-4410"}}`,
		},
		{
			// docs/03-client-protocol.md §3's example omits event; §4.4 says event is
			// required. Decode accepts the example as written and leaves the
			// requiredness check to internal/conn, which is where an ErrBadRequest can
			// actually be sent. Decoding it here would make the spec's own §3 literal
			// undecodable.
			name:  "publish, event omitted as in the §3 example",
			frame: `{"id":4,"publish":{"channel":"desk-42","data":{"typing":true}}}`,
			want: Command{ID: 4, Publish: &PublishRequest{
				Channel: "desk-42",
				Data:    json.RawMessage(`{"typing":true}`),
			}},
			wantJSON: `{"id":4,"publish":{"channel":"desk-42","event":"","data":{"typing":true}}}`,
		},
		{
			// docs/03-client-protocol.md §4.4.
			name:  "publish with event",
			frame: `{"id":4,"publish":{"channel":"desk-42","event":"typing","data":{"typing":true}}}`,
			want: Command{ID: 4, Publish: &PublishRequest{
				Channel: "desk-42",
				Event:   "typing",
				Data:    json.RawMessage(`{"typing":true}`),
			}},
			wantJSON: `{"id":4,"publish":{"channel":"desk-42","event":"typing","data":{"typing":true}}}`,
		},
		{
			// docs/03-client-protocol.md §4.5.
			name:     "sync",
			frame:    `{"id":5,"sync":{}}`,
			want:     Command{ID: 5, Sync: &SyncRequest{}},
			wantJSON: `{"id":5,"sync":{}}`,
		},
		{
			// docs/03-client-protocol.md §3: ping is the one command routinely sent
			// with no id, because a client that does not need to correlate need not.
			name:     "ping without id",
			frame:    `{"ping":{}}`,
			want:     Command{Ping: &PingRequest{}},
			wantJSON: `{"ping":{}}`,
		},
		{
			// docs/03-client-protocol.md §4.6: the id is echoed so a client with two
			// pings in flight can correlate replies.
			name:     "ping with id",
			frame:    `{"id":7,"ping":{}}`,
			want:     Command{ID: 7, Ping: &PingRequest{}},
			wantJSON: `{"id":7,"ping":{}}`,
		},
		{
			name:     "surrounding whitespace is not significant",
			frame:    "  \n\t{\"ping\":{}}\n ",
			want:     Command{Ping: &PingRequest{}},
			wantJSON: `{"ping":{}}`,
		},
		{
			// Unknown fields inside a command body are ignored. See TestDecode_UnknownBodyFieldIsIgnored.
			name:     "unknown field inside a body is ignored",
			frame:    `{"id":2,"subscribe":{"channel":"room-4410","presence":true}}`,
			want:     Command{ID: 2, Subscribe: &SubscribeRequest{Channel: "room-4410"}},
			wantJSON: `{"id":2,"subscribe":{"channel":"room-4410"}}`,
		},
		{
			// A JSON object with a repeated key is legal input; encoding/json takes the
			// last occurrence, so the frame still carries exactly one command key. It is
			// not worth a scanner of our own to reject.
			name:     "duplicate command key is one key",
			frame:    `{"ping":{},"ping":{}}`,
			want:     Command{Ping: &PingRequest{}},
			wantJSON: `{"ping":{}}`,
		},
		{
			name:     "large positive id",
			frame:    `{"id":9007199254740993,"ping":{}}`,
			want:     Command{ID: 9007199254740993, Ping: &PingRequest{}},
			wantJSON: `{"id":9007199254740993,"ping":{}}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := Decode([]byte(tt.frame))
			if err != nil {
				t.Fatalf("Decode(%s) error = %v, want nil", tt.frame, err)
			}
			assertCommandEqual(t, got, tt.want)

			back, err := json.Marshal(got)
			if err != nil {
				t.Fatalf("json.Marshal(%#v) error = %v", got, err)
			}
			if string(back) != tt.wantJSON {
				t.Errorf("re-encoded\n got %s\nwant %s", back, tt.wantJSON)
			}
		})
	}
}

// TestDecode_Rejects covers every way a frame can be malformed. Each of these is
// ErrBadRequest (101) at the call site with the connection left open
// (docs/03-client-protocol.md §3, §6).
func TestDecode_Rejects(t *testing.T) {
	t.Parallel()

	deep := strings.Repeat(`{"a":`, 200000) + "1" + strings.Repeat("}", 200000)

	tests := []struct {
		name  string
		frame string
	}{
		// Not a JSON object at all.
		{"empty input", ""},
		{"whitespace only", "   \n\t "},
		{"json null", "null"},
		{"json array", `["ping"]`},
		{"json number", "1"},
		{"json string", `"ping"`},
		{"json true", "true"},
		{"truncated object", `{"subscribe":{"channel":"a"`},
		{"trailing garbage", `{"ping":{}}x`},
		{"two objects", `{"ping":{}}{"ping":{}}`},
		{"deeply nested", deep},

		// docs/03-client-protocol.md §3: exactly one command key per frame. A frame
		// with zero or several MUST be answered with error 101.
		{"zero keys", `{}`},
		{"id only, no command", `{"id":1}`},
		{"two commands", `{"subscribe":{"channel":"a"},"ping":{}}`},
		{"two commands with id", `{"id":1,"connect":{},"ping":{}}`},
		{"three commands", `{"ping":{},"sync":{},"connect":{}}`},
		{"command plus unknown key", `{"subscribe":{"channel":"a"},"presence":{}}`},

		// Unknown commands are 101, never ignored: silently ignoring one makes a
		// client's typo look like a server that never answers.
		{"unknown command", `{"presence":{}}`},
		{"unknown command is case sensitive", `{"Subscribe":{"channel":"a"}}`},
		{"empty key", `{"":{}}`},

		// A command key whose value is not an object.
		{"null command body", `{"subscribe":null}`},
		{"array command body", `{"subscribe":[]}`},
		{"string command body", `{"subscribe":"room-4410"}`},
		{"number command body", `{"ping":1}`},
		{"bool command body", `{"sync":true}`},

		// Wrong types inside a body.
		{"channel is a number", `{"subscribe":{"channel":123}}`},
		{"channel is an object", `{"unsubscribe":{"channel":{}}}`},
		{"subs is a string", `{"connect":{"subs":"room-4410"}}`},
		{"subs element is a number", `{"connect":{"subs":[1]}}`},
		{"publish event is a number", `{"publish":{"channel":"a","event":1}}`},

		// docs/03-client-protocol.md §3: id is a positive integer. Zero, negative and
		// non-integer are all distinct from absent, and all wrong.
		{"id zero", `{"id":0,"ping":{}}`},
		{"id negative", `{"id":-1,"ping":{}}`},
		{"id fractional", `{"id":1.5,"ping":{}}`},
		{"id string", `{"id":"1","ping":{}}`},
		{"id null", `{"id":null,"ping":{}}`},
		{"id bool", `{"id":true,"ping":{}}`},
		{"id object", `{"id":{},"ping":{}}`},
		{"id overflows int64", `{"id":9223372036854775808,"ping":{}}`},
		{"id in exponent form", `{"id":1e2,"ping":{}}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := Decode([]byte(tt.frame))
			if err == nil {
				t.Fatalf("Decode(%.80q) = %#v, want an error", tt.frame, got)
			}
			if got != (Command{}) {
				t.Errorf("Decode(%.80q) returned %#v alongside an error, want the zero Command", tt.frame, got)
			}
			if !strings.HasPrefix(err.Error(), "proto: ") {
				t.Errorf("error %q does not name the package", err)
			}
		})
	}
}

// TestDecode_UnknownBodyFieldIsIgnored pins the forward-compatibility decision: an
// unrecognised field *inside* a command body is ignored, while an unrecognised
// *top-level* key is ErrBadRequest.
//
// The asymmetry is the point. The top-level key is the command name, and a wrong one is
// a client that will otherwise wait forever for a reply that is never coming. A body
// field is a parameter, and rejecting unknown ones would make every future additive
// field (§4.1's subs was one) a breaking change against every already-deployed gateway,
// which is the thing a version-skewed browser fleet cannot survive.
//
// The cost, stated so nobody has to rediscover it: a client that misspells an optional
// field gets the default rather than an error.
func TestDecode_UnknownBodyFieldIsIgnored(t *testing.T) {
	t.Parallel()

	got, err := Decode([]byte(`{"id":1,"connect":{"subs":["a"],"token":"x","v":2}}`))
	if err != nil {
		t.Fatalf("Decode error = %v, want nil", err)
	}
	if got.Connect == nil || len(got.Connect.Subs) != 1 || got.Connect.Subs[0] != "a" {
		t.Fatalf("Decode = %#v, want connect with subs [a]", got)
	}
}

// TestEncode_SpecFrames asserts the exact bytes of every gateway frame literal in
// docs/03-client-protocol.md §3, §5, §7.1 and §9.
//
// Byte-for-byte, not "equivalent JSON": these literals are what a third-party client
// implementer reads, and a renamed field or a dropped omitempty is a silent break for
// every client already deployed against them.
func TestEncode_SpecFrames(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   any
		want string
	}{
		{
			// docs/03-client-protocol.md §3, §9.
			name: "connect reply",
			in: &Reply{ID: 1, Connect: &ConnectReply{
				Client:    "8f2c1e04a7b3d915",
				Ping:      25,
				ExpiresIn: 3600,
				Subs:      map[string]SubDetail{"room-4410": {}},
			}},
			want: `{"id":1,"connect":{"client":"8f2c1e04a7b3d915","ping":25,"expires_in":3600,"subs":{"room-4410":{}}}}`,
		},
		{
			// docs/03-client-protocol.md §4.1: a channel that fails authorization is
			// omitted from the map, not reported. With none granted the map is empty —
			// and it must serialize as {} rather than null, or a client doing
			// Object.keys(subs) on the reply throws.
			name: "connect reply with no granted subs is an empty object, never null",
			in: &Reply{ID: 1, Connect: &ConnectReply{
				Client: "8f2c1e04a7b3d915", Ping: 25, ExpiresIn: 3600,
			}},
			want: `{"id":1,"connect":{"client":"8f2c1e04a7b3d915","ping":25,"expires_in":3600,"subs":{}}}`,
		},
		{
			// docs/03-client-protocol.md §3.
			name: "subscribe reply",
			in:   &Reply{ID: 2, Subscribe: &SubscribeReply{}},
			want: `{"id":2,"subscribe":{}}`,
		},
		{
			// docs/03-client-protocol.md §4.3: success replies {}.
			name: "unsubscribe reply",
			in:   &Reply{ID: 3, Unsubscribe: &UnsubscribeReply{}},
			want: `{"id":3,"unsubscribe":{}}`,
		},
		{
			name: "publish reply",
			in:   &Reply{ID: 4, Publish: &PublishReply{}},
			want: `{"id":4,"publish":{}}`,
		},
		{
			// docs/03-client-protocol.md §4.5, §9.
			name: "sync reply",
			in:   &Reply{ID: 5, Sync: &SyncReply{Channels: []string{"room-4410"}}},
			want: `{"id":5,"sync":{"channels":["room-4410"]}}`,
		},
		{
			// An empty subscription set is the interesting case for sync: it is exactly
			// what "nobody receives anything" looks like, so it must be an empty array a
			// client can iterate, not null.
			name: "sync reply with no channels is an empty array, never null",
			in:   &Reply{ID: 9, Sync: &SyncReply{}},
			want: `{"id":9,"sync":{"channels":[]}}`,
		},
		{
			// docs/03-client-protocol.md §3, §9.
			name: "error reply",
			in:   &Reply{ID: 2, Error: &Error{Code: ErrPermissionDenied, Message: "permission denied"}},
			want: `{"id":2,"error":{"code":103,"message":"permission denied"}}`,
		},
		{
			// docs/03-client-protocol.md §3: pong carries no id when the ping carried none.
			name: "pong without id",
			in:   &Reply{Pong: &Pong{}},
			want: `{"pong":{}}`,
		},
		{
			// docs/03-client-protocol.md §4.6: the id is echoed when one was supplied.
			name: "pong with id",
			in:   &Reply{ID: 7, Pong: &Pong{}},
			want: `{"id":7,"pong":{}}`,
		},
		{
			// docs/03-client-protocol.md §3, §5.1, §9.
			name: "push pub",
			in: &PushFrame{Push: &Push{Channel: "room-4410", Pub: &Pub{
				Event: "order.created", Data: json.RawMessage(`{"id":88123}`),
			}}},
			want: `{"push":{"channel":"room-4410","pub":{"event":"order.created","data":{"id":88123}}}}`,
		},
		{
			// docs/03-client-protocol.md §4.4: from is stamped by the gateway and is
			// present only for client events.
			name: "push pub from a client event carries from",
			in: &PushFrame{Push: &Push{Channel: "desk-42", Pub: &Pub{
				Event: "typing", Data: json.RawMessage(`{"typing":true}`), From: "user-7",
			}}},
			want: `{"push":{"channel":"desk-42","pub":{"event":"typing","data":{"typing":true},"from":"user-7"}}}`,
		},
		{
			// docs/03-client-protocol.md §3, §5.1, §9.
			name: "push unsubscribed",
			in: &PushFrame{Push: &Push{Channel: "org-42-alerts", Unsubscribed: &Unsubscribed{
				Reason: "grant revoked",
			}}},
			want: `{"push":{"channel":"org-42-alerts","unsubscribed":{"reason":"grant revoked"}}}`,
		},
		{
			// §5.1's shapes are distinguished by which key sits alongside channel, so a
			// reasonless unsubscribed must still carry the unsubscribed key.
			name: "push unsubscribed with no reason",
			in:   &PushFrame{Push: &Push{Channel: "org-42-alerts", Unsubscribed: &Unsubscribed{}}},
			want: `{"push":{"channel":"org-42-alerts","unsubscribed":{}}}`,
		},
		{
			// docs/03-client-protocol.md §3, §5.2, §9. reconnect: false is serialized
			// rather than omitted: a client that has to infer it from absence infers it
			// wrongly, and §5.2 makes honouring it mandatory.
			name: "disconnect, not retryable, omits retry_after",
			in:   &DisconnectFrame{Disconnect: &Disconnect{Code: CloseRevoked, Reason: "revoked"}},
			want: `{"disconnect":{"code":3501,"reason":"revoked","reconnect":false}}`,
		},
		{
			// docs/03-client-protocol.md §7.1: retry_after is in milliseconds.
			name: "disconnect with retry_after",
			in: &DisconnectFrame{Disconnect: &Disconnect{
				Code: CloseDraining, Reason: "draining", Reconnect: true, RetryAfter: 18400,
			}},
			want: `{"disconnect":{"code":3000,"reason":"draining","reconnect":true,"retry_after":18400}}`,
		},
		{
			// docs/03-client-protocol.md §9.
			name: "disconnect on expiry",
			in: &DisconnectFrame{Disconnect: &Disconnect{
				Code: CloseExpired, Reason: "expired", Reconnect: true, RetryAfter: 21700,
			}},
			want: `{"disconnect":{"code":3503,"reason":"expired","reconnect":true,"retry_after":21700}}`,
		},
		{
			// A retryable close with no gateway-computed delay: the client falls back to
			// its own full jitter (§8.2), so the key must be absent rather than 0, which
			// would read as "retry immediately".
			name: "disconnect retryable without retry_after",
			in: &DisconnectFrame{Disconnect: &Disconnect{
				Code: ClosePingTimeout, Reason: "ping timeout", Reconnect: true,
			}},
			want: `{"disconnect":{"code":3004,"reason":"ping timeout","reconnect":true}}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := Encode(tt.in)
			if err != nil {
				t.Fatalf("Encode error = %v, want nil", err)
			}
			if string(got) != tt.want {
				t.Errorf("Encode\n got %s\nwant %s", got, tt.want)
			}
			assertStableRoundTrip(t, tt.in, tt.want)
		})
	}
}

// assertStableRoundTrip parses the encoded frame back into its own struct and encodes it
// again, asserting the bytes are identical.
//
// Encode is only half of the wire contract — a client parses what it produces — and this
// catches the class of bug where a field serializes under one name and deserializes under
// another, which no amount of staring at a struct tag reliably finds. It also proves the
// nil-normalizing MarshalJSON methods are idempotent rather than oscillating between two
// shapes across a decode.
func assertStableRoundTrip(t *testing.T, in any, encoded string) {
	t.Helper()

	var target any
	switch in.(type) {
	case *Reply:
		target = &Reply{}
	case *PushFrame:
		target = &PushFrame{}
	case *DisconnectFrame:
		target = &DisconnectFrame{}
	default:
		t.Fatalf("no round-trip target for %T", in)
	}

	if err := json.Unmarshal([]byte(encoded), target); err != nil {
		t.Fatalf("json.Unmarshal(%s) error = %v", encoded, err)
	}
	got, err := Encode(target)
	if err != nil {
		t.Fatalf("re-encode error = %v", err)
	}
	if string(got) != encoded {
		t.Errorf("round trip\n got %s\nwant %s", got, encoded)
	}
}

// TestEncode_RoundTrip re-decodes every gateway frame into its own struct. Encode is
// only half of the wire contract; a client parses what it produces.
func TestEncode_RoundTrip(t *testing.T) {
	t.Parallel()

	t.Run("reply", func(t *testing.T) {
		t.Parallel()

		want := &Reply{ID: 1, Connect: &ConnectReply{
			Client: "8f2c1e04a7b3d915", Ping: 25, ExpiresIn: 3600,
			Subs: map[string]SubDetail{"room-4410": {}},
		}}
		b, err := Encode(want)
		if err != nil {
			t.Fatalf("Encode error = %v", err)
		}
		var got Reply
		if err := json.Unmarshal(b, &got); err != nil {
			t.Fatalf("json.Unmarshal(%s) error = %v", b, err)
		}
		if got.ID != want.ID || got.Connect == nil ||
			got.Connect.Client != want.Connect.Client ||
			got.Connect.Ping != want.Connect.Ping ||
			got.Connect.ExpiresIn != want.Connect.ExpiresIn ||
			len(got.Connect.Subs) != 1 {
			t.Errorf("round trip = %#v, want %#v", got.Connect, want.Connect)
		}
		if _, ok := got.Connect.Subs["room-4410"]; !ok {
			t.Errorf("subs = %#v, want the granted channel present", got.Connect.Subs)
		}
	})

	t.Run("push", func(t *testing.T) {
		t.Parallel()

		want := &PushFrame{Push: &Push{Channel: "room-4410", Pub: &Pub{
			Event: "order.created", Data: json.RawMessage(`{"id":88123}`),
		}}}
		b, err := Encode(want)
		if err != nil {
			t.Fatalf("Encode error = %v", err)
		}
		var got PushFrame
		if err := json.Unmarshal(b, &got); err != nil {
			t.Fatalf("json.Unmarshal(%s) error = %v", b, err)
		}
		if got.Push == nil || got.Push.Channel != "room-4410" || got.Push.Pub == nil ||
			got.Push.Pub.Event != "order.created" || string(got.Push.Pub.Data) != `{"id":88123}` ||
			got.Push.Unsubscribed != nil {
			t.Errorf("round trip = %#v", got.Push)
		}
	})

	t.Run("disconnect", func(t *testing.T) {
		t.Parallel()

		want := &DisconnectFrame{Disconnect: &Disconnect{
			Code: CloseDraining, Reason: "draining", Reconnect: true, RetryAfter: 18400,
		}}
		b, err := Encode(want)
		if err != nil {
			t.Fatalf("Encode error = %v", err)
		}
		var got DisconnectFrame
		if err := json.Unmarshal(b, &got); err != nil {
			t.Fatalf("json.Unmarshal(%s) error = %v", b, err)
		}
		if got.Disconnect == nil || *got.Disconnect != *want.Disconnect {
			t.Errorf("round trip = %#v, want %#v", got.Disconnect, want.Disconnect)
		}
	})

	t.Run("disconnect without retry_after decodes to zero", func(t *testing.T) {
		t.Parallel()

		b, err := Encode(&DisconnectFrame{Disconnect: &Disconnect{
			Code: CloseRevoked, Reason: "revoked",
		}})
		if err != nil {
			t.Fatalf("Encode error = %v", err)
		}
		var got DisconnectFrame
		if err := json.Unmarshal(b, &got); err != nil {
			t.Fatalf("json.Unmarshal(%s) error = %v", b, err)
		}
		if got.Disconnect.RetryAfter != 0 || got.Disconnect.Reconnect {
			t.Errorf("round trip = %#v, want zero retry_after and reconnect false", got.Disconnect)
		}
	})
}

// TestEncode_Rejects covers the inputs Encode refuses. v is typed any because the three
// frame types share no method, so the type switch is the only thing standing between a
// caller's mistake and a websocket text frame the client cannot parse.
func TestEncode_Rejects(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   any
	}{
		{"untyped nil", nil},
		{"nil reply", (*Reply)(nil)},
		{"nil push frame", (*PushFrame)(nil)},
		{"nil disconnect frame", (*DisconnectFrame)(nil)},
		{"a command is not a gateway frame", &Command{Ping: &PingRequest{}}},
		{"a body is not a frame", &Push{Channel: "a"}},
		{"a value, not a pointer", Reply{Pong: &Pong{}}},
		{"an unrelated type", "hello"},
		// An invalid RawMessage is the one way a well-typed frame fails to marshal: the
		// bus hands through publisher-supplied bytes, so this is reachable from outside.
		{"push with an invalid raw payload", &PushFrame{Push: &Push{
			Channel: "a", Pub: &Pub{Event: "e", Data: json.RawMessage(`{oops`)},
		}}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := Encode(tt.in)
			if err == nil {
				t.Fatalf("Encode(%#v) = %s, want an error", tt.in, got)
			}
			if got != nil {
				t.Errorf("Encode returned %s alongside an error, want nil", got)
			}
			if !strings.HasPrefix(err.Error(), "proto: ") {
				t.Errorf("error %q does not name the package", err)
			}
		})
	}
}

// TestEncode_DoesNotEscapeHTML asserts that a payload reaches the wire byte-for-byte as
// the publisher wrote it.
//
// encoding/json escapes <, > and & by default, for embedding in a <script> block.
// Nothing here is embedded in HTML; docs/03-client-protocol.md §4.4 says a payload is
// passed through untouched; and the escaping costs six bytes per character on a buffer
// shared by every subscriber of the channel. A client author diffing a delivered frame
// against what their application published must see the same bytes.
func TestEncode_DoesNotEscapeHTML(t *testing.T) {
	t.Parallel()

	got, err := Encode(&PushFrame{Push: &Push{Channel: "c", Pub: &Pub{
		Event: "e", Data: json.RawMessage(`{"html":"<b>&</b>"}`),
	}}})
	if err != nil {
		t.Fatalf("Encode error = %v", err)
	}
	const want = `{"push":{"channel":"c","pub":{"event":"e","data":{"html":"<b>&</b>"}}}}`
	if string(got) != want {
		t.Errorf("Encode\n got %s\nwant %s", got, want)
	}
}

// assertCommandEqual compares two commands field by field. reflect.DeepEqual would do,
// but a failure that names the command key is worth the lines when the thing under test
// is a wire format.
func assertCommandEqual(t *testing.T, got, want Command) {
	t.Helper()

	if got.ID != want.ID {
		t.Errorf("id = %d, want %d", got.ID, want.ID)
	}
	switch {
	case want.Connect != nil:
		if got.Connect == nil || strings.Join(got.Connect.Subs, ",") != strings.Join(want.Connect.Subs, ",") {
			t.Errorf("connect = %#v, want %#v", got.Connect, want.Connect)
		}
	case want.Subscribe != nil:
		if got.Subscribe == nil || *got.Subscribe != *want.Subscribe {
			t.Errorf("subscribe = %#v, want %#v", got.Subscribe, want.Subscribe)
		}
	case want.Unsubscribe != nil:
		if got.Unsubscribe == nil || *got.Unsubscribe != *want.Unsubscribe {
			t.Errorf("unsubscribe = %#v, want %#v", got.Unsubscribe, want.Unsubscribe)
		}
	case want.Publish != nil:
		if got.Publish == nil || got.Publish.Channel != want.Publish.Channel ||
			got.Publish.Event != want.Publish.Event ||
			string(got.Publish.Data) != string(want.Publish.Data) {
			t.Errorf("publish = %#v, want %#v", got.Publish, want.Publish)
		}
	case want.Sync != nil:
		if got.Sync == nil {
			t.Error("sync = nil, want a body")
		}
	case want.Ping != nil:
		if got.Ping == nil {
			t.Error("ping = nil, want a body")
		}
	}
}
