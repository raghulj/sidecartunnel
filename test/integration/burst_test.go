package integration_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/raghulj/sidecartunnel/internal/config"
	"github.com/redis/go-redis/v9"
)

const (
	// burstMessages and burstPayload size the burst. 2,000 × 8 KiB is 16 MiB arriving at
	// one subscriber socket as fast as Redis will write it, which straddles the stock
	// client-output-buffer-limit pubsub hard limit of 32mb and its 8mb/60s soft limit.
	// The Redis docs/10-operations.md §3 requires — and the one `make redis` starts —
	// raises both, so a correct deployment absorbs this and an unconfigured one may not.
	burstMessages = 2000
	burstPayload  = 8 << 10

	// burstPipeline is how many publishes go out per round trip. Publishing one at a
	// time would pace the burst at the round-trip time and stop it being a burst.
	burstPipeline = 250

	// reorderSlack is how many messages of the burst's tail a subscriber may legitimately
	// not have counted when the sentinel arrives.
	//
	// It is not slop. bus.dispatch_workers is 2 by default, so two messages from the same
	// channel are decoded and fanned out concurrently and can reach a connection's queue
	// in either order — which means the sentinel can overtake the last message or two of
	// the burst, and a reader that stops at the sentinel has not seen them yet. This is
	// measurable: at this rate the last message arrives after the sentinel in most runs.
	//
	// docs/07-delivery.md §5 says per-channel order "is preserved in practice" while
	// declining to guarantee it. With more than one dispatch worker the first half of
	// that sentence is not true, and this constant is where that shows up.
	reorderSlack = 8

	// burstSubscribers is the width of the channel. The subject is the bus socket rather
	// than the fan-out, so this is wide enough to be a broadcast and narrow enough that
	// the test stays a thing people run.
	burstSubscribers = 6
)

// TestBroadcastBurstDoesNotEvictTheGateway proves docs/13-review-findings.md M8: a large
// burst on a wide channel does not get the gateway evicted by Redis's pubsub output
// buffer — and if it does, the gateway recovers completely rather than oscillating.
//
// The failure this exists to catch is stable rather than transient, which is what makes
// it nasty. Redis enforces client-output-buffer-limit pubsub and disconnects a subscriber
// that falls behind. A gateway whose reader goroutine decoded and fanned out to thousands
// of connections between socket reads falls behind during a broadcast, is dropped,
// reconnects, resubscribes and is immediately behind again. It presents to an operator as
// st_bus_reconnects_total climbing against a perfectly healthy Redis, which points
// on-call at the wrong system entirely.
//
// The design answer is that the reader does nothing but drain the socket into a bounded
// intake, with decode and fan-out on workers behind it, and that a full intake drops
// rather than blocks. Dropping is at-most-once behaving as documented; blocking would be
// the eviction.
//
// Both halves of the requirement are asserted, and which one applies is decided by what
// happened rather than by hope. If the gateway was not evicted — the outcome a correctly
// configured Redis must produce — st_bus_reconnects_total is unchanged. If it was, the
// test insists on full recovery: the subscription set is restored and delivery resumes to
// every client. Either way no client connection may be lost, because a burst on one
// channel must never cost a subscriber its session.
func TestBroadcastBurstDoesNotEvictTheGateway(t *testing.T) {
	t.Parallel()
	c := newCluster(t, clusterOptions{
		Replicas: 1,
		Config: func(cfg *config.Config) {
			// The subject is Redis's buffer, not the per-connection one. A deep outbound
			// queue takes client backpressure out of the experiment so a failure here
			// means what the test says it means.
			cfg.Limits.OutboundQueue = 8192
		},
	})
	r := c.r(0)

	before := r.reconnects()

	readers := make([]*burstReader, 0, burstSubscribers)
	for range burstSubscribers {
		client := r.dial()
		client.connect()
		client.subscribe("room-burst")
		readers = append(readers, startBurstReader(client))
	}
	waitUpstream(t, r, 2)

	burst(t, c, "room-burst")

	// Whatever happened to the bus, the desired set is what the replica ends on. This is
	// a no-op when there was no eviction and it is the resubscribe when there was.
	waitUpstream(t, r, 2)

	c.publishEventually("room-burst", event("message.new", map[string]any{"marker": "sentinel"}))

	delivered := 0
	for i, reader := range readers {
		got, err := reader.wait()
		if err != nil {
			t.Fatalf("subscriber %d: %v: a broadcast burst must never cost a subscriber its connection (M8)", i, err)
		}
		delivered += got
	}

	health := r.bus.Health()
	dispatched := r.cons.dispatched.Load()
	reconnects := health.Reconnects - before
	t.Logf("burst of %d messages: %d reached the hub, %d/%d delivered across %d subscribers, %d dropped at the intake, %d bus reconnect(s)",
		burstMessages, dispatched, delivered, burstMessages*burstSubscribers, burstSubscribers, health.Dropped, reconnects)

	if reconnects == 0 {
		// Not evicted, so nothing may have been lost between Redis and the hub. This is
		// the assertion that separates "the gateway kept up" from "the gateway was
		// dropped and quietly re-established": every published message, plus the
		// sentinel, arrived.
		if want := int64(burstMessages) + 1; dispatched != want {
			t.Fatalf("%d of %d messages reached the hub with no bus reconnect and %d dropped at the intake, want all of them (M8)", dispatched, want, health.Dropped)
		}
		// Fan-out reordering costs a subscriber the tail of the burst rather than the
		// middle of it: see reorderSlack.
		if floor := burstMessages*burstSubscribers - reorderSlack*burstSubscribers; delivered < floor {
			t.Fatalf("%d of %d fan-out deliveries arrived, want at least %d: the gateway kept up with Redis but lost messages on the way to the clients",
				delivered, burstMessages*burstSubscribers, floor)
		}
	}

	if reconnects > 0 {
		// The recovery half of M8. It is a pass, but a loud one: a Redis without the
		// required setting evicts the gateway under a burst it should absorb, and the
		// operator needs to know which system to look at.
		t.Logf("the gateway was evicted %d time(s) during the burst and recovered: the delivery above happened after the resubscribe. "+
			"This Redis does not have the setting docs/10-operations.md §3 requires — "+
			"client-output-buffer-limit pubsub 256mb 64mb 60 — which is what `make redis` sets.", reconnects)
	}
}

// burst publishes burstMessages envelopes to one channel as fast as Redis will accept
// them, pipelined so the gateway's subscriber socket sees a wall of data rather than a
// stream paced by round trips.
func burst(t *testing.T, c *cluster, channel string) {
	t.Helper()
	payload, err := json.Marshal(event("message.new", map[string]any{
		"marker": "burst",
		"filler": strings.Repeat("x", burstPayload),
	}))
	if err != nil {
		t.Fatalf("marshal burst envelope: %v", err)
	}

	key := c.prefix + channel
	ctx, cancel := context.WithTimeout(context.Background(), waitBudget)
	defer cancel()
	for sent := 0; sent < burstMessages; sent += burstPipeline {
		n := min(burstPipeline, burstMessages-sent)
		if _, err := c.pub.Pipelined(ctx, func(p redis.Pipeliner) error {
			for range n {
				p.Publish(ctx, key, payload)
			}
			return nil
		}); err != nil {
			t.Fatalf("publish burst: %v", err)
		}
	}
}

// burstReader drains one client until the sentinel arrives, counting burst messages.
//
// It reads on its own goroutine so nothing buffers behind a test that is busy publishing,
// and it reports through a channel rather than calling t.Fatalf, which is not safe from a
// goroutine that may outlive the test.
type burstReader struct {
	done  chan struct{}
	count int
	err   error
}

// startBurstReader begins draining a client.
func startBurstReader(client *wsClient) *burstReader {
	b := &burstReader{done: make(chan struct{})}
	go func() {
		defer close(b.done)
		for {
			data, err := client.raw(waitBudget)
			if err != nil {
				b.err = err
				return
			}
			var f frame
			if err := json.Unmarshal(data, &f); err != nil {
				b.err = err
				return
			}
			if f.Disconnect != nil {
				b.err = errDisconnected(f)
				return
			}
			if f.Push == nil || f.Push.Pub == nil {
				continue
			}
			var got struct {
				Marker string `json:"marker"`
			}
			if err := json.Unmarshal(f.Push.Pub.Data, &got); err != nil {
				b.err = err
				return
			}
			if got.Marker == "sentinel" {
				return
			}
			b.count++
		}
	}()
	return b
}

// wait blocks until the reader has seen the sentinel or failed.
//
// The time.After is a failure detector and not a pause: the select returns the moment the
// reader is done, and the deadline is reached only when the frame is never going to
// arrive. It is here rather than as a read deadline because the thing being waited on is
// a goroutine finishing, not a socket.
func (b *burstReader) wait() (int, error) {
	select {
	case <-b.done:
		return b.count, b.err
	case <-time.After(waitBudget):
		return b.count, errTimeout
	}
}
