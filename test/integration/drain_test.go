package integration_test

import (
	"testing"
	"time"

	"github.com/raghulj/sidecartunnel/internal/proto"
)

// TestDrainClosesOneReplicaOnly proves FR-19: draining a replica stops it accepting,
// closes every connection it holds with 3000, reconnect true and a retry_after, and
// leaves the other replica's connections entirely alone.
//
// The retry_after is the part worth being careful about. The gateway knows how many
// connections it is dropping and the client does not, so the spread belongs on the
// server: at 10,000 connections per replica, 60 seconds of spread is ~7 concurrent
// authorizations at the application and no spread at all is ~400 in a one-second window,
// which is a full application outage during what was supposed to be a routine deploy
// (docs/13-review-findings.md S5, docs/10-operations.md §4). It is asserted as positive
// and inside server.drain_spread, not as an exact value: the value is a deterministic
// hash of the client id and asserting it exactly would couple this test to that choice.
//
// The other replica is the reason this test is here rather than in the unit layer. A
// drain is a per-process event, and "it did not take the fleet with it" is a claim that
// needs a second process to be true or false. Its survival is proved by a delivered
// publish after the drain has returned, not by silence.
//
// It fails if drain closes with a code the client treats as permanent, if it omits the
// retry_after, if it returns before the connections are actually gone, or if anything it
// does reaches across the bus.
func TestDrainClosesOneReplicaOnly(t *testing.T) {
	t.Parallel()
	c := newCluster(t, clusterOptions{})

	leaving := c.r(0).dial()
	leaving.connect()
	leaving.subscribe("room-10")
	waitUpstream(t, c.r(0), 2)

	staying := c.r(1).dial()
	staying.connect()
	staying.subscribe("room-10")
	waitUpstream(t, c.r(1), 2)

	c.r(0).drain()

	got := leaving.wantDisconnect(proto.CloseDraining)
	if !got.Reconnect {
		t.Fatalf("drain closed with reconnect=false; a rolling deploy is not a decision about the user (FR-19)")
	}
	spread := c.r(0).cfg.Server.DrainSpread.Duration()
	if got.RetryAfter <= 0 || time.Duration(got.RetryAfter)*time.Millisecond > spread {
		t.Fatalf("drain retry_after = %dms, want a positive value no greater than server.drain_spread %s (S5)", got.RetryAfter, spread)
	}

	// Drain returned only once every connection had unwound, so the count is exact
	// rather than eventual.
	if n := c.r(0).srv.Stats().Current; n != 0 {
		t.Fatalf("replica 1 still holds %d connection(s) after Drain returned, want 0 (FR-19)", n)
	}

	// The other replica never noticed.
	staying.ping()
	c.publish("room-10", event("message.new", map[string]any{"marker": "unaffected"}))
	if m := marker(t, staying.wantPub("room-10", "message.new").Data); m != "unaffected" {
		t.Fatalf("the surviving replica's client got marker %q, want %q", m, "unaffected")
	}
}

// TestDrainingReplicaRefusesNewConnections proves the first step of FR-19: a draining
// replica stops accepting, and says so with 503 rather than by accepting a connection it
// is about to close.
//
// 503 rather than a websocket close code, because the refusal happens before the upgrade:
// the connection count and the drain flag are checked in the same critical section, and a
// client that is told 503 by a load balancer's backend simply goes to another one
// (docs/03-client-protocol.md §2).
//
// It fails if the drain flag is checked after the upgrade, which admits a connection the
// replica is already shutting down, or not at all, which makes a rolling deploy accept
// traffic it is about to drop.
func TestDrainingReplicaRefusesNewConnections(t *testing.T) {
	t.Parallel()
	c := newCluster(t, clusterOptions{Replicas: 1})

	client := c.r(0).dial()
	client.connect()

	c.r(0).drain()
	client.wantDisconnect(proto.CloseDraining)

	_, status, err := c.r(0).dialOrigin(testOrigin)
	if got := statusOf(t, status, err); got != 503 {
		t.Fatalf("handshake against a draining replica was refused with %d, want 503 (FR-19)", got)
	}
}
