package integration_test

import (
	"slices"
	"testing"

	"github.com/raghulj/sidecartunnel/internal/proto"
)

// TestControlDisconnectReachesAnotherReplica proves FR-18: a signed disconnect on the
// control channel closes the connection it names, on whatever replica holds it, within a
// second, with reconnect false.
//
// The publisher is neither replica. It is a third Redis client, which is exactly how an
// application revokes a session (docs/04-integration.md §3) — no service discovery, no
// HTTP call to a specific gateway, one PUBLISH.
//
// The deadline is asserted, not assumed: the read that expects the disconnect frame is
// given one second, so a revocation that arrives late fails rather than passing slowly.
// reconnect: false is asserted because a client that retries through a revocation turns
// it into a denial-of-service against the connect webhook.
//
// The untouched client on the other replica is the other half. Every replica consumes
// every control message, so a targeting bug that matched loosely — a glob where an exact
// match is required — would disconnect connections nobody named. Its survival is proved
// by a command round trip and a delivered publish, not by silence.
func TestControlDisconnectReachesAnotherReplica(t *testing.T) {
	t.Parallel()
	c := newCluster(t, clusterOptions{})

	bystander := c.r(0).dial()
	bystander.connect()
	bystander.subscribe("room-7")
	waitUpstream(t, c.r(0), 2)

	target := c.r(1).dial()
	target.connect()
	target.subscribe("room-7")
	waitUpstream(t, c.r(1), 2)

	c.publishControl(map[string]any{
		"action": "disconnect",
		"client": target.id,
		"reason": "account suspended",
	})

	got := target.wantDisconnectWithin(proto.CloseRevoked, revocationBudget)
	if got.Reconnect {
		t.Fatalf("disconnect carried reconnect=true; a revocation is a decision and must not be retried (FR-18)")
	}
	if got.Reason != "account suspended" {
		t.Fatalf("disconnect reason = %q, want the reason the control message carried", got.Reason)
	}

	// The connection nobody named is still there, still serving commands, still being
	// delivered to.
	bystander.ping()
	c.publish("room-7", event("message.new", map[string]any{"marker": "bystander"}))
	if m := marker(t, bystander.wantPub("room-7", "message.new").Data); m != "bystander" {
		t.Fatalf("bystander got marker %q, want %q", m, "bystander")
	}
}

// TestControlDisconnectByUserSpansReplicas is the user-targeted form of FR-18: every
// connection for one opaque user id closes, on every replica, and connections for other
// users do not. One user with a tab on each replica is the case that makes revocation
// worth doing on the bus rather than over HTTP — the application does not know, and must
// not have to know, which replica holds which tab.
//
// It is skipped because it does not pass, and it does not pass because of a defect in
// internal/, which this suite may not edit. Written out so the fix has a test waiting for
// it rather than a paragraph in a report:
//
//	internal/server/handler.go registers the connection with hub.Add before the connect
//	frame arrives, which is correct and deliberate — FR-18 wants a connection targetable
//	from the moment it exists. At that instant Conn.User() is "", so hub.registerLocked
//	files it under h.users[""]. internal/conn/command.go then stores the real user id
//	after the webhook answers, and nothing re-files it: hub.Attach calls registerLocked,
//	which returns early for a Sink it already holds.
//
// Two consequences, both live:
//
//  1. h.users[<real user>] is always empty, so a control disconnect, refresh or
//     unsubscribe naming a user matches nothing on any replica. Client-id targeting is
//     unaffected and is proved by the test above.
//  2. Hub.Remove deletes from h.users[s.User()] — the real id — so the entry left in
//     h.users[""] is never removed. That map grows by one entry per connection for the
//     life of the process.
//
// Un-skip this the moment the hub learns to re-file a connection when its user id is
// set.
func TestControlDisconnectByUserSpansReplicas(t *testing.T) {
	// Was skipped as a known defect: the hub indexed a connection under user "" at
	// hub.Add time (the upgrade, before the application has named anyone) and was
	// thought never to re-file it, which would make every user-targeted control message
	// match nothing on every replica - FR-18's primary form, silently dead.
	//
	// It re-files. registerLocked's re-registration branch calls indexUserLocked when
	// Attach runs after the connect webhook answers, and conn stores the user id before
	// calling Attach. The diagnosis was made against a tree that changed underneath it.
	// The test stays, un-skipped, because nothing else covers the re-filing and the
	// failure mode is invisible: revocation returns success and does nothing.

	t.Parallel()
	c := newCluster(t, clusterOptions{})

	c.app.setUser("u-7")
	first := c.r(0).dial()
	first.connect()
	second := c.r(1).dial()
	second.connect()

	c.app.setUser("u-8")
	other := c.r(0).dial()
	other.connect()

	c.publishControl(map[string]any{"action": "disconnect", "user": "u-7"})

	for name, client := range map[string]*wsClient{"replica 1 tab": first, "replica 2 tab": second} {
		got := client.wantDisconnectWithin(proto.CloseRevoked, revocationBudget)
		if got.Reconnect {
			t.Fatalf("%s: disconnect carried reconnect=true, want false (FR-18)", name)
		}
	}

	// A different user is untouched. Exact matching is the rule: "matched exactly, never
	// as a glob" (docs/13-review-findings.md C8).
	other.ping()
}

// TestControlUnsubscribeEmitsPush proves FR-17: a control-channel unsubscribe drops the
// matching subscriptions and tells the client it did.
//
// The push is the whole requirement. Dropping a subscription silently leaves the client's
// registry claiming a channel it will never hear from again, which is indistinguishable
// from a quiet channel, forever (docs/13-review-findings.md M16).
//
// Three things are asserted, and the second and third are what stop a partial
// implementation passing: the unsubscribed push arrives for the channel that matched; the
// gateway's own authoritative set — from sync, not from a client-side copy — no longer
// holds it; and the upstream subscription count falls, so the refcount moved with it.
// The channel that did not match the glob is still held and still delivered to.
//
// It fails if the subscription is dropped without a push, if the push and the drop happen
// in two critical sections so the client can be told about a channel it still receives on,
// or if the glob is applied to the bus key rather than to the bare channel name.
func TestControlUnsubscribeEmitsPush(t *testing.T) {
	t.Parallel()
	c := newCluster(t, clusterOptions{Replicas: 1})
	r := c.r(0)

	client := r.dial()
	client.connect()
	client.subscribe("room-5")
	client.subscribe("desk-5")
	waitUpstream(t, r, 3) // control, room-5, desk-5

	c.publishControl(map[string]any{
		"action":  "unsubscribe",
		"client":  client.id,
		"channel": "room-*",
		"reason":  "grant revoked",
	})

	push := client.nextPush()
	if push.Unsubscribed == nil {
		t.Fatalf("first push after the control unsubscribe is %v, want an unsubscribed push (FR-17)", push)
	}
	if push.Channel != "room-5" {
		t.Fatalf("unsubscribed push names channel %q, want %q", push.Channel, "room-5")
	}
	if push.Unsubscribed.Reason != "grant revoked" {
		t.Fatalf("unsubscribed push reason = %q, want the reason the control message carried", push.Unsubscribed.Reason)
	}

	if got := client.sync(); !slices.Equal(got, []string{"desk-5"}) {
		t.Fatalf("sync returns %v, want only the channel the glob did not match (M16)", got)
	}
	waitUpstream(t, r, 2)

	// The channel that did not match is untouched and still delivering.
	c.publish("desk-5", event("message.new", map[string]any{"marker": "kept"}))
	if m := marker(t, client.wantPub("desk-5", "message.new").Data); m != "kept" {
		t.Fatalf("the surviving channel delivered marker %q, want %q", m, "kept")
	}
}

// TestUnsignedControlMessageHasNoEffect proves FR-23: an unsigned or tampered control
// envelope is dropped and counted, never applied.
//
// The gateway's entire revocation surface is a Redis PUBLISH. Without signature
// verification, anything that can reach Redis can disconnect every user on every replica.
//
// The absence is proved positively: the forged disconnect goes first, a legitimate signed
// one follows, and the connection must survive long enough to be closed by the second.
// Waiting for silence after the forgery would prove only that the test is patient.
//
// It fails if the signature is not checked, if it is checked over a re-serialized body
// rather than the literal signed bytes, or if a rejected envelope is applied anyway.
func TestUnsignedControlMessageHasNoEffect(t *testing.T) {
	t.Parallel()
	c := newCluster(t, clusterOptions{Replicas: 1})

	client := c.r(0).dial()
	client.connect()

	forged := signControl("not-the-control-secret-0123456789abcdef", `{"action":"disconnect","client":"`+client.id+`"}`, timeNow())
	if err := c.pub.Publish(t.Context(), c.prefix+"_control", []byte(forged)).Err(); err != nil {
		t.Fatalf("publish forged control: %v", err)
	}

	// The connection is still there and still serving commands.
	client.ping()

	// And the properly signed message closes it, which proves the forgery was rejected
	// for its signature and not merely lost.
	c.publishControl(map[string]any{"action": "disconnect", "client": client.id})
	client.wantDisconnectWithin(proto.CloseRevoked, revocationBudget)

	if n := c.r(0).cons.controlRejected.Load(); n != 1 {
		t.Fatalf("the consumer rejected %d control envelope(s), want exactly 1 (FR-23)", n)
	}
}
