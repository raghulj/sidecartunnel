package integration_test

import (
	"encoding/json"
	"testing"
)

// TestCrossReplicaFanOut proves FR-12: a message published to {bus.prefix}{channel}
// reaches every subscriber on every replica, not just the one the publisher happens to be
// near.
//
// Client A is on replica 1 and client B is on replica 2. They share nothing but Redis:
// two hubs, two bus connections, two listeners. The publish comes from a third Redis
// client that is neither replica, which is how an application publishes
// (docs/04-integration.md §2).
//
// It fails if the bus subscription is never established, if the hub keys its map by
// something other than the bus key, or if a replica delivers only to connections that
// published through it. The unit layer cannot catch any of those: with a memory bus there
// is only ever one replica, so a cross-replica hop has never once been executed.
func TestCrossReplicaFanOut(t *testing.T) {
	t.Parallel()
	c := newCluster(t, clusterOptions{})

	a := c.r(0).dial()
	a.connect()
	a.subscribe("room-1")
	waitUpstream(t, c.r(0), 2) // the control channel, plus room-1

	b := c.r(1).dial()
	b.connect()
	b.subscribe("room-1")
	waitUpstream(t, c.r(1), 2)

	c.publish("room-1", event("message.new", map[string]any{"n": 1}))

	for name, client := range map[string]*wsClient{"replica 1": a, "replica 2": b} {
		pub := client.wantPub("room-1", "message.new")
		var got struct {
			N int `json:"n"`
		}
		if err := json.Unmarshal(pub.Data, &got); err != nil {
			t.Fatalf("%s: decode payload: %v", name, err)
		}
		if got.N != 1 {
			t.Fatalf("%s: payload n = %d, want 1", name, got.N)
		}
	}
}

// TestUnprefixedPublishReachesNobody proves FR-21: the hub keys by bus key, never by the
// bare channel name.
//
// A publish to "room-1" rather than "{bus.prefix}room-1" must reach nobody. It is
// asserted positively rather than by waiting for silence: the unprefixed publish goes
// first, the prefixed one second, and the client's *first* push must be the second
// message. A test that merely waited for nothing to arrive would pass on a gateway that
// was simply slow.
//
// It fails the moment somebody keys the channel map by the name the client used, which is
// the exact defect that made two applications cross-deliver (docs/13-review-findings.md
// C1, S1).
func TestUnprefixedPublishReachesNobody(t *testing.T) {
	t.Parallel()
	c := newCluster(t, clusterOptions{Replicas: 1})

	client := c.r(0).dial()
	client.connect()
	client.subscribe("room-9")
	waitUpstream(t, c.r(0), 2)

	// Straight to Redis, with no prefix: the name the client used, which is not the key
	// anything is subscribed to.
	payload, err := json.Marshal(event("message.new", map[string]any{"marker": "unprefixed"}))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if err := c.pub.Publish(t.Context(), "room-9", payload).Err(); err != nil {
		t.Fatalf("publish unprefixed: %v", err)
	}

	c.publish("room-9", event("message.new", map[string]any{"marker": "prefixed"}))

	pub := client.wantPub("room-9", "message.new")
	var got struct {
		Marker string `json:"marker"`
	}
	if err := json.Unmarshal(pub.Data, &got); err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	if got.Marker != "prefixed" {
		t.Fatalf("the first push carried marker %q; the unprefixed publish was delivered (FR-21)", got.Marker)
	}
}

// TestExcludeSuppressesExactlyOneClient proves FR-13: an envelope carrying exclude is
// withheld from the connection with that client id, and from nobody else.
//
// A is on replica 1, B is on replica 2, and both hold room-2. The publish excludes A.
//
// The absence is proved positively rather than by waiting for silence, which would only
// prove that the test is patient. A second publish follows with no exclude, and A's first
// push must be that second message: had A received the excluded one, its first push would
// carry marker "excluded".
//
// The two publishes are separated by an observed fact rather than by hope. The second one
// is sent only after A's own replica has handed the first to the hub, so if A were ever
// going to be given the excluded message it would already be queued on A's socket when
// the second is published. Publishing both at once would not do: bus.dispatch_workers is
// 2, so two messages on one channel are fanned out concurrently and can reach a socket in
// either order — which is not what this test is about, and is measurable enough to make a
// naive version of it fail about two runs in three.
//
// It fails if exclude is compared against the wrong identity, if it is applied only on
// the replica the publisher is nearest, or if an empty exclude ever matches a connection.
func TestExcludeSuppressesExactlyOneClient(t *testing.T) {
	t.Parallel()
	c := newCluster(t, clusterOptions{})

	a := c.r(0).dial()
	a.connect()
	a.subscribe("room-2")
	waitUpstream(t, c.r(0), 2)

	b := c.r(1).dial()
	b.connect()
	b.subscribe("room-2")
	waitUpstream(t, c.r(1), 2)

	if a.id == "" || a.id == b.id {
		t.Fatalf("client ids are %q and %q; each connection must have its own", a.id, b.id)
	}

	before := c.r(0).cons.dispatched.Load()
	c.publish("room-2", map[string]any{
		"event":   "message.new",
		"data":    map[string]any{"marker": "excluded"},
		"exclude": a.id,
	})

	// B is not excluded and receives it.
	if m := marker(t, b.wantPub("room-2", "message.new").Data); m != "excluded" {
		t.Fatalf("the client that was not excluded got marker %q, want %q (FR-13)", m, "excluded")
	}

	// A's replica has the message too — the exclusion is applied at fan-out, on every
	// replica, not by withholding the publish.
	waitFor(t, "the excluded client's replica to receive the message", func() bool {
		return c.r(0).cons.dispatched.Load() > before
	})

	c.publish("room-2", event("message.new", map[string]any{"marker": "everyone"}))

	if m := marker(t, a.wantPub("room-2", "message.new").Data); m != "everyone" {
		t.Fatalf("the excluded client received marker %q; exclude did not suppress delivery to it (FR-13)", m)
	}
	if m := marker(t, b.wantPub("room-2", "message.new").Data); m != "everyone" {
		t.Fatalf("the client that was not excluded got marker %q for the second publish, want %q", m, "everyone")
	}
}

// marker decodes the {"marker": …} payload these tests publish.
func marker(t *testing.T, data json.RawMessage) string {
	t.Helper()
	var got struct {
		Marker string `json:"marker"`
	}
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	return got.Marker
}
