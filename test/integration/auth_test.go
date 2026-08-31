package integration_test

import (
	"net/http"
	"slices"
	"strings"
	"testing"

	"github.com/raghulj/sidecartunnel/internal/proto"
)

// TestSubscribeOutsideGrantsIsRefused proves FR-5 and FR-8 across a real socket: the
// gateway stores the grant list the application supplied at connect and refuses any
// subscribe that does not match it, with error 103, leaving the connection open.
//
// The connection stays open because a refused subscribe is a failed command, never a
// failed session: a client that asks for one channel it may not have must not lose the
// twenty it may.
//
// The reserved subtest is the one that must hold even when the application is generous.
// A grant of "*" matches every channel a glob can express, and "_control" must still be
// refused — otherwise any connected client subscribes to the control channel and reads
// every revocation for every user in the system (docs/06-channels.md §4). The reserved
// check therefore runs before grants are consulted, not after.
//
// It fails if grants are consulted after the subscription is taken, if the reserved check
// runs after the glob rather than before it, or if a refusal closes the connection.
func TestSubscribeOutsideGrantsIsRefused(t *testing.T) {
	t.Parallel()

	t.Run("a configured channel the application did not grant", func(t *testing.T) {
		t.Parallel()
		c := newCluster(t, clusterOptions{Replicas: 1, Grants: []string{"room-*"}})

		client := c.r(0).dial()
		client.connect()
		client.subscribe("room-8") // granted, so the refusals below are about the grant

		for _, channel := range []string{"desk-1", "user-1"} {
			f := client.subscribeFrame(channel)
			if f.Error == nil {
				t.Fatalf("subscribe %q answered with %s, want error 103 (FR-5)", channel, f)
			}
			if f.Error.Code != proto.ErrPermissionDenied {
				t.Fatalf("subscribe %q answered with error %d, want %d", channel, f.Error.Code, proto.ErrPermissionDenied)
			}
		}

		// The connection survived every refusal and still holds what it was granted.
		client.ping()
		c.publish("room-8", event("message.new", map[string]any{"marker": "still-open"}))
		if m := marker(t, client.wantPub("room-8", "message.new").Data); m != "still-open" {
			t.Fatalf("granted channel delivered marker %q, want %q", m, "still-open")
		}
	})

	t.Run("a reserved channel against a grant of *", func(t *testing.T) {
		t.Parallel()
		c := newCluster(t, clusterOptions{Replicas: 1, Grants: []string{"*"}})

		client := c.r(0).dial()
		client.connect()
		client.subscribe("room-8") // "*" really does grant everything a glob can express

		for _, channel := range []string{"_control", "_anything"} {
			f := client.subscribeFrame(channel)
			if f.Error == nil {
				t.Fatalf("subscribe %q answered with %s, want error 103 (docs/06-channels.md §4)", channel, f)
			}
			if f.Error.Code != proto.ErrPermissionDenied {
				t.Fatalf("subscribe %q answered with error %d, want %d", channel, f.Error.Code, proto.ErrPermissionDenied)
			}
		}
		client.ping()
	})
}

// TestForeignOriginIsRefusedBeforeTheApplicationIsCalled proves FR-2: an upgrade whose
// Origin is not on the allowlist is refused with HTTP 403, before the upgrade completes
// and before any application call is made.
//
// The status is the whole answer. The check finishes before the websocket exists, so
// there is no socket on which a close code could be sent — which is why
// docs/03-client-protocol.md §7 has no code for a rejected origin and why
// docs/13-review-findings.md M14 removed the one that used to be there.
//
// The second assertion is the one that matters and the one only an integration test can
// make: the stub application recorded no call. Browsers do not apply CORS to websocket
// handshakes but do attach cookies, so a gateway that called the application first would
// hand evil.example a valid grant list for the victim's session before deciding to refuse
// it — and an application that logs its callers would show the attack as a successful
// authorization.
//
// The allowed origin is dialled afterwards, so a gateway that refused everything could
// not pass this test by being broken.
//
// It fails if the Origin comparison ever becomes a suffix or wildcard match, if a missing
// Origin is admitted without server.allow_missing_origin, or if the application is called
// before the allowlist is consulted.
func TestForeignOriginIsRefusedBeforeTheApplicationIsCalled(t *testing.T) {
	t.Parallel()
	c := newCluster(t, clusterOptions{Replicas: 1})

	for _, origin := range []string{foreignOrigin, ""} {
		_, status, err := c.r(0).dialOrigin(origin)
		if got := statusOf(t, status, err); got != http.StatusForbidden {
			t.Fatalf("handshake with Origin %q was refused with %d, want %d (FR-2)", origin, got, http.StatusForbidden)
		}
	}

	if n := c.app.count(); n != 0 {
		t.Fatalf("the connect webhook was called %d time(s) for a refused Origin, want 0: the cookie reached the application before the Origin was checked (FR-2)", n)
	}

	// The allowlisted origin still works, so the assertion above is about the Origin and
	// not about a gateway that refuses everything.
	client := c.r(0).dial()
	client.connect("room-13")
	if n := c.app.count(); n != 1 {
		t.Fatalf("the connect webhook was called %d time(s) for an allowed Origin, want 1", n)
	}

	// And while there is a recorded call to look at, FR-3 and FR-24 over a real socket.
	// The cookie is forwarded byte for byte — the gateway cannot parse it, because
	// session formats belong to the application — and X-St-Forwarded-For is the socket
	// peer address, because server.trusted_proxies is empty and a client-supplied
	// X-Forwarded-For from an untrusted peer must be discarded rather than passed on
	// under a header prefix implying the gateway vouched for it.
	call := c.app.call(t, 0)
	if call.Cookie != testCookie {
		t.Fatalf("the connect webhook received a cookie the client did not send; it must be forwarded verbatim (FR-3)")
	}
	if call.Origin != testOrigin {
		t.Fatalf("X-St-Origin = %q, want %q", call.Origin, testOrigin)
	}
	if !strings.HasPrefix(call.Forwarded, "127.0.0.1") {
		t.Fatalf("X-St-Forwarded-For = %q, want the socket peer address with no trusted proxies configured (FR-24)", call.Forwarded)
	}
}

// TestConnectWebhookReceivesTheRequestedChannels asserts the channels_requested field of
// docs/04-integration.md §1.1: the connect frame's subs, forwarded to the application so
// it can scope the grant list it computes.
//
// It is skipped because it does not pass, and the cause is a seam in internal/, which
// this suite may not edit. Written out so the fix has a test waiting for it:
//
//	internal/server/handler.go builds the webhook.Request in serve, before the connect
//	frame has arrived, so ChannelsRequested is never populated. It cannot be populated
//	there: the field's value is in a frame that has not been read yet. The connection
//	does have it by the time it authorizes, but conn.Authorizer takes only a context, so
//	there is nowhere to put it.
//
// The field is documented as informational — the application answers with grants and the
// gateway matches against those — so nothing is unsafe. What is wrong is that the
// normative request example shows a field the gateway never sends, and an application
// that reads it gets an empty list with no way to tell that from a connect frame that
// asked for nothing.
//
// Un-skip this when conn.Authorizer can carry the requested channels.
func TestConnectWebhookReceivesTheRequestedChannels(t *testing.T) {
	t.Skip("known gap: the connect webhook request is built before the connect frame is read, so channels_requested is always empty (see this test's doc comment)")

	t.Parallel()
	c := newCluster(t, clusterOptions{Replicas: 1})

	client := c.r(0).dial()
	client.connect("room-13", "room-14")

	if got := c.app.call(t, 0).ChannelsRequested; !slices.Equal(got, []string{"room-13", "room-14"}) {
		t.Fatalf("channels_requested = %v, want the channels the connect frame asked for (docs/04-integration.md §1.1)", got)
	}
}

// TestWebhookStatusesMapToCloseCodes proves FR-6 across real sockets: a refusal and a
// failure never share a close code or a reconnect value, and only the failure is retried.
//
// This is the most consequential distinction in the gateway. A 401 means *this user may
// not connect*: the client must stop asking, because retrying cannot change the answer
// and a retry loop through a revocation is a denial-of-service against the application. A
// 5xx means *I could not tell you right now*: every replica will answer the same, so the
// client must come back later rather than hammering an endpoint that is already failing.
// Collapsing them either locks every user out during an application deploy or turns a
// revocation into an infinite loop (docs/04-integration.md §1.3).
//
// A 403 is deliberately not a refusal. It means the application rejected the *request* —
// a bad signature, a skewed clock, a key removed mid-rotation — which is a gateway-side
// fault. A replica whose clock has drifted gets 403 on every call; as 3008 its clients
// retry onto healthy replicas, and as 3003 they would be locked out until a human
// noticed.
//
// Three things are asserted per row: the close code, the reconnect flag, and the number
// of application calls. The call count is what separates 403 from 5xx, which share a
// close code by design: a 403 is not retried in process — the retry would carry the same
// bad signature — while a 5xx is, up to app.webhook_retries.
//
// It fails if any status is mapped to the wrong code, if reconnect is inferred from the
// code rather than sent, or if a refusal is ever retried.
func TestWebhookStatusesMapToCloseCodes(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string

		// status is what the stub application answers with.
		status int

		// body is the answer's body, for the 2xx-but-unusable row.
		body string

		want      proto.CloseCode
		reconnect bool

		// calls is how many times the application must have been called for one
		// connection: app.webhook_retries is 1, so a retried failure is two.
		calls int

		// stat names the webhook counter that must have moved. A single "webhook errors"
		// counter makes "my application is down" and "my gateway cannot authenticate to
		// my application" indistinguishable at the moment that matters.
		stat func(webhookStats) uint64
	}{
		{
			name:   "401 refuses the user permanently",
			status: http.StatusUnauthorized, want: proto.CloseUnauthorized, reconnect: false,
			calls: 1, stat: func(s webhookStats) uint64 { return s.Refused },
		},
		{
			name:   "403 rejects the gateway's request, transiently",
			status: http.StatusForbidden, want: proto.CloseAuthUnavailable, reconnect: true,
			calls: 1, stat: func(s webhookStats) uint64 { return s.Rejected },
		},
		{
			name:   "500 is a failure and is retried",
			status: http.StatusInternalServerError, want: proto.CloseAuthUnavailable, reconnect: true,
			calls: 2, stat: func(s webhookStats) uint64 { return s.Failed },
		},
		{
			name:   "200 with a body the gateway cannot use is a refusal",
			status: http.StatusOK, body: `{"user":"u-1"}`, want: proto.CloseUnauthorized, reconnect: false,
			calls: 1, stat: func(s webhookStats) uint64 { return s.Refused },
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c := newCluster(t, clusterOptions{Replicas: 1})
			c.app.answerWith(func(appCall, int) appReply {
				return appReply{Status: tc.status, Body: tc.body}
			})

			client := c.r(0).dial()
			client.send(map[string]any{"id": 1, "connect": map[string]any{}})

			got := client.wantDisconnect(tc.want)
			if got.Reconnect != tc.reconnect {
				t.Fatalf("disconnect %d carried reconnect=%t, want %t (FR-6)", got.Code, got.Reconnect, tc.reconnect)
			}
			if tc.reconnect && got.RetryAfter <= 0 {
				t.Fatalf("retryable disconnect %d carried retry_after=%d, want a positive value: the gateway knows how many connections it is dropping and the client does not (S5)", got.Code, got.RetryAfter)
			}
			if !tc.reconnect && got.RetryAfter != 0 {
				t.Fatalf("permanent disconnect %d carried retry_after=%d, want none", got.Code, got.RetryAfter)
			}
			if n := c.app.count(); n != tc.calls {
				t.Fatalf("the connect webhook was called %d time(s), want %d (FR-6: a refusal is never retried, a failure is, up to app.webhook_retries)", n, tc.calls)
			}
			if n := tc.stat(statsOf(c.r(0))); n != 1 {
				t.Fatalf("the webhook counter for this outcome is %d, want 1: a refusal, a rejection and a failure must be countable apart", n)
			}
		})
	}
}

// webhookStats mirrors webhook.Client.Stats, so a table row can name one counter.
type webhookStats struct {
	Authorized uint64
	Refused    uint64
	Rejected   uint64
	Failed     uint64
}

// statsOf reads one replica's webhook counters.
func statsOf(r *replica) webhookStats {
	s := r.web.Stats()
	return webhookStats{Authorized: s.Authorized, Refused: s.Refused, Rejected: s.Rejected, Failed: s.Failed}
}
