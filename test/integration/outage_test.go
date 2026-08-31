package integration_test

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/raghulj/sidecartunnel/internal/config"
)

// TestBusLossKeepsConnectionsOpenAndRestoresDelivery proves NFR-8, and it is the highest
// value test in this suite: losing Redis must not close a single client connection, must
// be visible on /ready and nowhere else, and must repair itself completely when Redis
// comes back — with the clients none the wiser.
//
// The alternative behaviour is what makes this worth a container of its own. A gateway
// that closed connections on bus loss turns an eight-second Redis restart into every
// connected user re-authorizing at once, which is an outage of the *application* rather
// than of the gateway — the failure docs/10-operations.md §4 models and the one this
// architecture is otherwise most exposed to. Nothing short of stopping a real Redis
// exercises it: a fake bus that reports itself disconnected does not sever the sockets,
// does not lose the subscription set, and does not have to re-establish anything.
//
// Redis is stopped with `docker stop` on a container this test owns. It cannot use the
// shared one — every other test in the run is on it.
//
// Five things are asserted, in order, and the middle one is the requirement:
//
//  1. Delivery works before the outage, so what follows is about the outage.
//  2. The client sockets stay open and keep serving commands while the bus is gone.
//     Proved by an application-level ping round trip on each, which also proves no
//     disconnect frame arrived: the ping helper fails on any frame that is not a pong.
//  3. /ready turns 503 once the bus has been down longer than bus.ready_grace, while
//     /health is untouched — readiness reports the outage, liveness must not, or a
//     liveness probe kills the whole fleet during a Redis restart.
//  4. The subscription set is restored upstream when Redis returns, in full, without
//     anyone resubscribing: reconnect is a forced resync of the desired set, not a sweep.
//  5. Delivery resumes on the same sockets, which is what "without clients reconnecting"
//     means in an assertion — these are the same *websocket.Conn values, and a gateway
//     that had closed them could not answer on them.
func TestBusLossKeepsConnectionsOpenAndRestoresDelivery(t *testing.T) {
	t.Parallel()
	rc := startRedis(t)
	c := newCluster(t, clusterOptions{
		RedisURL: rc.url,
		Config: func(cfg *config.Config) {
			// The grace exists so a short blip does not pull the whole fleet from the
			// load balancer at once. Here it is set as short as it goes, because the
			// behaviour under test is the transition and not the length of the grace,
			// which the unit layer asserts.
			cfg.Bus.ReadyGrace = config.Duration(time.Millisecond)
		},
	})

	first := c.r(0).dial()
	first.connect()
	first.subscribe("room-out")
	waitUpstream(t, c.r(0), 2)

	second := c.r(1).dial()
	second.connect()
	second.subscribe("room-out")
	waitUpstream(t, c.r(1), 2)

	// 1. Delivery works, and both replicas are ready.
	c.publish("room-out", event("message.new", map[string]any{"marker": "before"}))
	for _, client := range []*wsClient{first, second} {
		if m := marker(t, client.wantPub("room-out", "message.new").Data); m != "before" {
			t.Fatalf("before the outage a client got marker %q, want %q", m, "before")
		}
	}
	for i, r := range c.replicas {
		if got := r.ready(); got != http.StatusOK {
			t.Fatalf("replica %d: /ready = %d before the outage, want 200", i, got)
		}
	}

	rc.stop(t)
	for i, r := range c.replicas {
		waitFor(t, "replica to notice the bus is gone", func() bool { return !r.bus.Health().Connected })
		if got := r.srv.Stats().Current; got != 1 {
			t.Fatalf("replica %d holds %d connection(s) after the bus went away, want 1: losing the bus must not close client connections (NFR-8)", i, got)
		}
	}

	// 2. The connections are open and still serving commands. A closed one would fail
	// the read; a disconnected one would answer with a disconnect frame instead of a
	// pong.
	first.ping()
	second.ping()

	// 3. Readiness reports the outage. Liveness does not.
	for i, r := range c.replicas {
		waitFor(t, "/ready to report the bus outage", func() bool { return r.ready() == http.StatusServiceUnavailable })
		if got := r.health(); got != http.StatusOK {
			t.Fatalf("replica %d: /health = %d during a bus outage, want 200: /health must never consult the bus, or a liveness probe kills the fleet during a Redis restart (FR-20, M20)", i, got)
		}
	}

	rc.start(t)

	// 4. The subscription set comes back on its own, and readiness with it.
	for i, r := range c.replicas {
		waitUpstream(t, r, 2)
		waitFor(t, "/ready to recover", func() bool { return r.ready() == http.StatusOK })
		if got := r.reconnects(); got < 1 {
			t.Fatalf("replica %d: st_bus_reconnects_total = %d after a bus outage, want at least 1", i, got)
		}
	}

	// 5. Delivery resumes on the very same sockets. Nobody reconnected, nobody
	// resubscribed, and neither client can tell that anything happened.
	c.publishEventually("room-out", event("message.new", map[string]any{"marker": "after"}))
	for _, client := range []*wsClient{first, second} {
		if m := marker(t, client.wantPub("room-out", "message.new").Data); m != "after" {
			t.Fatalf("after the bus returned a client got marker %q, want %q (NFR-8)", m, "after")
		}
	}
}

// TestReconcilerConvergesAcrossAnOutage proves the desired-state reconciler of
// docs/13-review-findings.md S2, and M6 in particular: subscriptions churn while the bus
// is unreachable, and when it comes back the upstream set is exactly what the replica
// wants — no more and no less.
//
// This is the test the old design could not pass. Modelling subscriptions as *events* on
// a queue meant a failed subscribe was lost with no metric (M5), a reconnect swept the hub
// while commands were still flowing so a subscribe and an unsubscribe for one channel
// could land in either order (M6), and a full queue blocked the fan-out goroutine and
// stopped all delivery on the replica while every socket stayed open and /ready stayed
// 200 (C7). Subscriptions are state; this asserts that they are treated as state.
//
// The churn covers all three shapes that move the desired set, because they take
// different routes through the hub and a defect in one is invisible from the others: an
// explicit unsubscribe, a fresh subscribe, and a connection going away entirely.
//
// Convergence is asserted twice over, and the second assertion is the one that catches a
// set that is merely plausible. The upstream count must be exactly the size of the
// desired set — a channel that was unsubscribed but never removed upstream would make it
// larger — and delivery must be correct in both directions: the channels that should be
// subscribed receive, and the one that was dropped does not. The negative is proved
// positively, by publishing to the dropped channel first and requiring the live channel's
// message to be the one that arrives.
func TestReconcilerConvergesAcrossAnOutage(t *testing.T) {
	t.Parallel()
	rc := startRedis(t)
	c := newCluster(t, clusterOptions{Replicas: 1, RedisURL: rc.url})
	r := c.r(0)

	// Before: control, room-a, room-b, and room-c held by a second connection.
	keeper := r.dial()
	keeper.connect()
	keeper.subscribe("room-a")
	keeper.subscribe("room-b")

	leaver := r.dial()
	leaver.connect()
	leaver.subscribe("room-c")
	waitUpstream(t, r, 4)

	rc.stop(t)
	waitFor(t, "the replica to notice the bus is gone", func() bool { return !r.bus.Health().Connected })

	// Churn, with nothing upstream to hear about it. Every one of these is local
	// bookkeeping: the hub never waits for the bus, which is what makes this possible at
	// all rather than a pile of blocked connections.
	keeper.unsubscribe("room-b")
	keeper.subscribe("room-d")
	keeper.subscribe("room-e")
	leaver.close()

	// Desired afterwards: control, room-a, room-d, room-e. room-b was unsubscribed and
	// room-c lost its only holder.
	rc.start(t)
	waitUpstream(t, r, 4)

	if got := r.upstream(); got != 4 {
		t.Fatalf("upstream subscriptions = %d after the bus returned, want exactly 4 (control, room-a, room-d, room-e): the reconciler did not converge on the desired set (S2, M6)", got)
	}

	// The exact count above is the proof that room-b and room-c are gone: the set has
	// four members, and the three publishes below show that room-a, room-d and room-e are
	// three of them, with the control channel the fourth by construction — it is seeded
	// into the desired set and never removed. The publish to room-b is corroboration
	// rather than the argument, which is why nothing here depends on the order two
	// messages arrive in.
	c.publishEventually("room-b", event("message.new", map[string]any{"marker": "dropped"}))
	c.publishEventually("room-a", event("message.new", map[string]any{"marker": "kept-a"}))
	if m := marker(t, keeper.wantPub("room-a", "message.new").Data); m != "kept-a" {
		t.Fatalf("the first push after convergence carried marker %q, want %q: a channel the replica unsubscribed is still delivering (M6)", m, "kept-a")
	}

	for _, channel := range []string{"room-d", "room-e"} {
		c.publishEventually(channel, event("message.new", map[string]any{"marker": channel}))
		if m := marker(t, keeper.wantPub(channel, "message.new").Data); m != channel {
			t.Fatalf("%s carried marker %q after convergence, want %q: a channel subscribed during the outage was never applied upstream (M5)", channel, m, channel)
		}
	}
}

// publishEventually publishes, retrying while the publishing client is still repairing
// its own connection to a Redis that has just come back.
//
// It is about the test's Redis client and not about the gateway: go-redis reconnects
// lazily, so the first command after a restart can fail on a connection that was severed
// while it was idle. Retrying here keeps that from reading as a gateway defect.
func (c *cluster) publishEventually(channel string, env map[string]any) {
	c.t.Helper()
	payload, err := json.Marshal(env)
	if err != nil {
		c.t.Fatalf("marshal envelope: %v", err)
	}
	key := c.prefix + channel
	waitFor(c.t, "the publishing client to reach Redis", func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		return c.pub.Publish(ctx, key, payload).Err() == nil
	})
}
