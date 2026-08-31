package integration_test

import "testing"

// TestUpstreamRefcounting proves FR-10: a replica holds exactly one upstream subscription
// per channel however many local connections hold it, and drops it when the last one
// goes.
//
// The count asserted is the bus's own confirmed subscription set, so it is what Redis
// agrees to, not what the gateway believes. It always includes the reserved
// control channel, which is seeded into the desired set at startup and never removed: a
// desired set computed only from client subscriptions would unsubscribe the replica from
// control on the first reconciliation and silently disable every revocation
// (docs/04-integration.md §3).
//
// The "one subscription, not two" half is proved by arithmetic rather than by waiting.
// After the second client subscribes to the same channel and then to a different one, the
// count must be exactly 3: control, room-1, room-1b. A refcount that subscribed twice
// would show 4, and a test that only asserted "still 2" after the duplicate subscribe
// would pass on a gateway whose reconciler had simply not run yet.
//
// It fails if the 0→1 transition is computed outside the lock that inserts, if the
// desired set is keyed by anything but the bus key, or if Remove drops the connection
// from the map without dropping it from the desired set.
func TestUpstreamRefcounting(t *testing.T) {
	t.Parallel()
	c := newCluster(t, clusterOptions{Replicas: 1})
	r := c.r(0)

	waitUpstream(t, r, 1) // the control channel alone
	baseline := r.upstream()

	a := r.dial()
	a.connect()
	a.subscribe("room-1")
	waitUpstream(t, r, baseline+1)

	// A second connection on the same replica and the same channel. It must add no
	// upstream subscription of its own; the third channel is what forces a reconciliation
	// so the assertion is on a set the reconciler has actually applied.
	b := r.dial()
	b.connect()
	b.subscribe("room-1")
	b.subscribe("room-1b")
	waitUpstream(t, r, baseline+2)

	if got := r.upstream(); got != baseline+2 {
		t.Fatalf("upstream subscriptions = %d, want %d: two connections on one channel produced more than one subscription (FR-10)", got, baseline+2)
	}

	// Dropping the second connection returns the count to what it was before it: room-1b
	// goes, room-1 stays, because A still holds it.
	b.close()
	waitUpstream(t, r, baseline+1)

	// Dropping the last holder returns the count to the prior value exactly.
	a.close()
	waitUpstream(t, r, baseline)
	if got := r.upstream(); got != baseline {
		t.Fatalf("upstream subscriptions = %d after every client disconnected, want the prior value %d (FR-10)", got, baseline)
	}
}

// TestUnsubscribeReturnsUpstreamCount proves the other half of FR-10: an explicit
// unsubscribe drops the upstream subscription when it was the last holder, and does not
// when it was not.
//
// It is separate from the disconnect path on purpose. Both must reach the same desired
// set, and they take different routes through the hub — Unsubscribe drops one key,
// Remove sweeps the whole mirror — so a defect in one is invisible from the other
// (docs/13-review-findings.md M4).
//
// It fails if the 1→0 transition is decided outside the lock that removes, which drops a
// subscription another connection still holds, or if the desired set keeps a key whose
// last holder has gone, which leaves the replica receiving a channel nobody wants.
func TestUnsubscribeReturnsUpstreamCount(t *testing.T) {
	t.Parallel()
	c := newCluster(t, clusterOptions{Replicas: 1})
	r := c.r(0)

	waitUpstream(t, r, 1)
	baseline := r.upstream()

	a := r.dial()
	a.connect()
	b := r.dial()
	b.connect()

	a.subscribe("room-3")
	b.subscribe("room-3")
	waitUpstream(t, r, baseline+1)

	// One of two holders leaves: the subscription stays, and the channel keeps working.
	a.unsubscribe("room-3")
	c.publish("room-3", event("message.new", map[string]any{"marker": "still-here"}))
	if got := marker(t, b.wantPub("room-3", "message.new").Data); got != "still-here" {
		t.Fatalf("remaining subscriber got marker %q, want %q", got, "still-here")
	}
	if got := r.upstream(); got != baseline+1 {
		t.Fatalf("upstream subscriptions = %d after one of two holders unsubscribed, want %d (FR-10)", got, baseline+1)
	}

	// The last holder leaves: the subscription goes.
	b.unsubscribe("room-3")
	waitUpstream(t, r, baseline)
}

// TestReplicasSubscribeIndependently proves that refcounting is per replica: two
// gateways holding the same channel each hold their own upstream subscription, and one
// releasing it does not stop the other from receiving.
//
// It is the failure mode a shared counter would produce, and it is invisible with a
// memory bus because there is only ever one replica to share it with.
func TestReplicasSubscribeIndependently(t *testing.T) {
	t.Parallel()
	c := newCluster(t, clusterOptions{})

	a := c.r(0).dial()
	a.connect()
	a.subscribe("room-4")
	waitUpstream(t, c.r(0), 2)

	b := c.r(1).dial()
	b.connect()
	b.subscribe("room-4")
	waitUpstream(t, c.r(1), 2)

	// Replica 1 releases the channel entirely.
	a.close()
	waitUpstream(t, c.r(0), 1)

	// Replica 2 is untouched.
	if got := c.r(1).upstream(); got != 2 {
		t.Fatalf("replica 2 upstream subscriptions = %d, want 2 after replica 1 released the channel", got)
	}
	c.publish("room-4", event("message.new", map[string]any{"marker": "second-replica"}))
	if got := marker(t, b.wantPub("room-4", "message.new").Data); got != "second-replica" {
		t.Fatalf("replica 2 client got marker %q, want %q", got, "second-replica")
	}
}
