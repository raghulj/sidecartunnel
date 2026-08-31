package integration_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

// orderingIterations is how many times a connection subscribes to a channel that is
// already carrying a firehose. Each iteration is a fresh chance for a push to overtake
// the reply that announces the channel.
const orderingIterations = 25

// quietWindow is how long a socket is watched for a frame that must not arrive. It is a
// read deadline, not a pause: the test spends it only in the case where nothing is wrong.
const quietWindow = 250 * time.Millisecond

// TestPushNeverPrecedesItsSubscribeReply proves the normative ordering rule of
// docs/03-client-protocol.md §5.1 over a real socket, under concurrent publishes: the
// gateway MUST NOT send a push for a channel before that channel's subscribe reply, nor
// after its unsubscribe reply.
//
// The channel is already hot when each connection subscribes. A second client holds it
// and a publisher is running flat out, so the upstream subscription exists, the hub's
// dispatch workers are already delivering on it, and the window between "this connection
// is in the channel's set" and "this connection has been told so" is a window a real
// message can land in. Subscribing to a cold channel would prove nothing: nothing can
// arrive before the bus has been told to subscribe.
//
// Without the rule a conforming client legitimately receives a push for a channel it has
// not been told it holds, and two conforming implementations diverge silently — one drops
// the message, the other closes the connection (docs/13-review-findings.md M15). The
// guarantee is free given the single writer goroutine, but only if the reply is queued
// under the same lock that mutates the subscription; queue it after releasing the lock
// and a dispatch worker slips into the gap.
//
// It fails if the subscribe reply is encoded or queued outside the hub's write lock, and
// it fails under -race if the two are only accidentally ordered.
func TestPushNeverPrecedesItsSubscribeReply(t *testing.T) {
	t.Parallel()
	c := newCluster(t, clusterOptions{Replicas: 1})

	// A holder, so the channel is subscribed upstream before anything else happens, and
	// a drain so the holder never becomes a slow consumer and takes the channel with it.
	holder := c.r(0).dial()
	holder.connect()
	holder.subscribe("room-ord")
	waitUpstream(t, c.r(0), 2)
	go func() {
		for {
			if _, err := holder.raw(waitBudget); err != nil {
				return
			}
		}
	}()

	stop := startFirehose(t, c, "room-ord")
	defer stop()

	for i := range orderingIterations {
		client := c.r(0).dial()
		client.connect()

		id := client.next()
		client.send(map[string]any{"id": id, "subscribe": map[string]any{"channel": "room-ord"}})

		var f frame
		for {
			f = client.read()
			if f.Push != nil {
				t.Fatalf("iteration %d: a %s arrived before the subscribe reply that announces the channel (docs/03-client-protocol.md §5.1)", i, f)
			}
			if f.Subscribe == nil {
				t.Fatalf("iteration %d: subscribe answered with %s", i, f)
			}
			break
		}
		if f.ID != id {
			t.Fatalf("iteration %d: subscribe reply id = %d, want %d", i, f.ID, id)
		}
		client.close()
	}
}

// TestPushNeverFollowsItsUnsubscribeReply is the other half of the §5.1 rule: once the
// gateway has answered an unsubscribe, no further push for that channel may arrive.
//
// The firehose is still running while the socket is watched, so a violation would be
// immediate rather than theoretical. Anything published before the unsubscribe was
// processed was queued before the reply was — both happen under the same hub lock — so a
// push seen after the reply is a real ordering violation and not a message in flight.
//
// It fails if the subscription is dropped and the reply queued in two separate critical
// sections, which is the shape that leaves a client holding a channel the gateway has
// already stopped acknowledging.
func TestPushNeverFollowsItsUnsubscribeReply(t *testing.T) {
	t.Parallel()
	c := newCluster(t, clusterOptions{Replicas: 1})

	client := c.r(0).dial()
	client.connect()
	client.subscribe("room-ord2")
	waitUpstream(t, c.r(0), 2)

	stop := startFirehose(t, c, "room-ord2")
	defer stop()

	// Drain whatever the firehose has already delivered, then unsubscribe. The reply is
	// the boundary: everything before it is legitimate, everything after it is not.
	id := client.next()
	client.send(map[string]any{"id": id, "unsubscribe": map[string]any{"channel": "room-ord2"}})
	for {
		f := client.read()
		if f.Unsubscribe != nil {
			break
		}
		if f.Push == nil {
			t.Fatalf("unsubscribe answered with %s", f)
		}
	}

	for {
		data, err := client.raw(quietWindow)
		if err != nil {
			return // the read deadline expired with nothing arriving, which is the pass
		}
		var f frame
		if err := json.Unmarshal(data, &f); err != nil {
			t.Fatalf("decode frame %s: %v", data, err)
		}
		if f.Push != nil && f.Push.Channel == "room-ord2" {
			t.Fatalf("a %s arrived after the unsubscribe reply for that channel (docs/03-client-protocol.md §5.1)", f)
		}
	}
}

// startFirehose publishes to one channel as fast as Redis will take it, until the
// returned function is called.
//
// It exists so the ordering tests race against something real. A publisher that ran
// slowly would leave the window these tests are about unexercised, and the test would
// pass on a gateway that had it wide open.
func startFirehose(t *testing.T, c *cluster, channel string) func() {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	payload, err := json.Marshal(event("message.new", map[string]any{"marker": "firehose"}))
	if err != nil {
		t.Fatalf("marshal firehose envelope: %v", err)
	}
	go func() {
		defer close(done)
		for ctx.Err() == nil {
			if err := c.pub.Publish(ctx, c.prefix+channel, payload).Err(); err != nil {
				return
			}
		}
	}()
	return func() {
		cancel()
		<-done
	}
}
