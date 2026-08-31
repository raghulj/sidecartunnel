package integration_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/raghulj/sidecartunnel/internal/config"
	"github.com/raghulj/sidecartunnel/internal/proto"
)

// slowConsumerMessages is how many messages the burst carries. It has to exceed the
// socket buffers plus the outbound queue of the connection that stops reading, and it is
// paced by the healthy client's receipts, so a larger number costs round trips rather
// than risk.
const slowConsumerMessages = 200

// TestSlowConsumerIsClosedAndTheChannelKeepsWorking proves FR-15: a client that stops
// reading is disconnected with 3005, and — the part that matters — every other subscriber
// on the same channel receives every message throughout.
//
// docs/11-testing.md §5 is explicit that without the second assertion this test passes
// while the bug it exists to catch is present. A gateway that blocked the fan-out on the
// stalled socket would still close it eventually, and still pass a test that only
// asserted the close; what it would not do is deliver anything to anybody else while it
// waited. Closing is not the requirement. Not waiting is the requirement.
//
// The burst is paced by the healthy client's own receipts — publish one, read one — so
// the healthy connection never has more than one message outstanding and cannot be
// closed as a slow consumer itself. That is what makes "received every message" a claim
// about the gateway rather than about how fast this machine happens to be. The stalled
// client, meanwhile, reads nothing at all: its socket buffers fill, its writer parks, its
// outbound queue fills behind that, and the next fan-out finds it full.
//
// It fails if a full outbound queue blocks the publisher, if the slow connection is
// closed on the fan-out goroutine (which needs the write lock the fan-out is holding for
// read, and deadlocks), or if messages are dropped for healthy subscribers to spare the
// unhealthy one.
func TestSlowConsumerIsClosedAndTheChannelKeepsWorking(t *testing.T) {
	t.Parallel()
	c := newCluster(t, clusterOptions{
		Replicas: 1,
		Config: func(cfg *config.Config) {
			// A shallow queue so the overflow happens in a bounded number of messages.
			// The depth is not what is under test; the behaviour at the bottom of it is.
			cfg.Limits.OutboundQueue = 16
		},
	})

	stalled := c.r(0).dial()
	stalled.connect()
	stalled.subscribe("room-slow")

	healthy := c.r(0).dial()
	healthy.connect()
	healthy.subscribe("room-slow")
	waitUpstream(t, c.r(0), 2)

	// From here the stalled client reads nothing. Every publish waits for the healthy
	// client to acknowledge the previous one by receiving it.
	payload := strings.Repeat("x", 8<<10)
	for i := range slowConsumerMessages {
		c.publish("room-slow", event("message.new", map[string]any{
			"marker": fmt.Sprintf("%d", i),
			"filler": payload,
		}))
		if got := marker(t, healthy.wantPub("room-slow", "message.new").Data); got != fmt.Sprintf("%d", i) {
			t.Fatalf("the healthy subscriber received marker %q as message %d: a message was dropped or reordered while another client was stalled (FR-15)", got, i)
		}
	}

	// The stalled connection is closed with 3005, and it is retryable: the client
	// reconnects, resubscribes and reconciles from the application, so it ends up
	// consistent having noticed. Blocking would have stalled the channel for everyone;
	// dropping would have left this client silently wrong (docs/07-delivery.md §4).
	if code := stalled.closeCode(); code != proto.CloseSlowConsumer {
		t.Fatalf("the stalled connection closed with %d, want %d (FR-15)", code, proto.CloseSlowConsumer)
	}

	// And the channel is still working for the client that kept up.
	c.publish("room-slow", event("message.new", map[string]any{"marker": "after"}))
	if got := marker(t, healthy.wantPub("room-slow", "message.new").Data); got != "after" {
		t.Fatalf("after the slow consumer was closed the channel delivered marker %q, want %q", got, "after")
	}
}
