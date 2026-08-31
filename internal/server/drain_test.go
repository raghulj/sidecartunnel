package server

import (
	"context"
	"net/http"
	"runtime"
	"testing"
	"time"

	"github.com/raghulj/sidecartunnel/internal/config"
	"github.com/raghulj/sidecartunnel/internal/proto"
)

// TestDrain_ClosesEveryConnectionWith3000_FR19 is the rolling-deploy path.
//
// Every connection is told 3000 with reconnect true, and every one is given a
// retry_after spread across server.drain_spread. The spread is not an optimization: the
// gateway knows how many connections it is dropping and the client does not, and without
// it a replica's whole population re-authorizes inside the one-second window
// docs/10-operations.md §4 models as an application outage
// (docs/03-client-protocol.md §7.1, S5).
func TestDrain_ClosesEveryConnectionWith3000_FR19(t *testing.T) {
	t.Parallel()
	const connections = 8
	spread := 60 * time.Second

	r := newRig(t, func(c *config.Config) { c.Server.DrainSpread = config.Duration(spread) })

	clients := make([]*client, 0, connections)
	for range connections {
		c := r.dial()
		c.connect("room-1")
		clients = append(clients, c)
	}

	ctx, cancel := context.WithTimeout(context.Background(), failAfter)
	defer cancel()
	start := time.Now()
	if err := r.srv.Drain(ctx); err != nil {
		t.Fatalf("Drain: %v", err)
	}
	// FR-19: the whole drain finishes inside server.drain_timeout. Returning after it is
	// a rolling deploy that stalls on one replica.
	if elapsed := time.Since(start); elapsed > r.cfg.Server.DrainTimeout.Duration() {
		t.Fatalf("Drain took %s, over server.drain_timeout %s", elapsed, r.cfg.Server.DrainTimeout)
	}

	seen := map[int64]bool{}
	for _, c := range clients {
		got := c.wantDisconnect(proto.CloseDraining)
		if !got.Reconnect {
			t.Fatal("3000 is reconnect true: the fleet is healthy and the client should come back")
		}
		if got.RetryAfter <= 0 || got.RetryAfter > spread.Milliseconds() {
			t.Fatalf("retry_after = %dms, want it inside (0, %d] — spread across server.drain_spread", got.RetryAfter, spread.Milliseconds())
		}
		seen[got.RetryAfter] = true
	}
	if len(seen) < 2 {
		t.Fatalf("every connection got the same retry_after (%v); that is a stampede with extra steps (§7.1)", seen)
	}
}

// TestDrain_StopsAcceptingConnections_FR19: step one of docs/09-internals.md §8. A
// draining replica answers 503 rather than accepting a connection it is about to close.
func TestDrain_StopsAcceptingConnections_FR19(t *testing.T) {
	t.Parallel()
	r := newRig(t)
	r.dial().connect()

	ctx, cancel := context.WithTimeout(context.Background(), failAfter)
	defer cancel()
	if err := r.srv.Drain(ctx); err != nil {
		t.Fatalf("Drain: %v", err)
	}

	_, status, err := r.dialOrigin(testOrigin)
	if got := statusOf(t, status, err); got != http.StatusServiceUnavailable {
		t.Fatalf("status while draining = %d, want 503", got)
	}
	if got := r.web.count(); got != 1 {
		t.Fatalf("webhook calls = %d, want 1: a draining replica asks the application nothing", got)
	}
}

// TestDrain_IsIdempotent: SIGTERM and a context cancellation can both arrive, and a
// second drain must not panic on a closed listener or wait a second timeout.
func TestDrain_IsIdempotent(t *testing.T) {
	t.Parallel()
	r := newRig(t)
	r.dial().connect()

	ctx, cancel := context.WithTimeout(context.Background(), failAfter)
	defer cancel()
	if err := r.srv.Drain(ctx); err != nil {
		t.Fatalf("first Drain: %v", err)
	}
	if err := r.srv.Drain(ctx); err != nil {
		t.Fatalf("second Drain: %v", err)
	}
}

// TestDrain_ReportsConnectionsThatOutlastTheDeadline_FR19: the drain is bounded. A
// connection that has not finished when the budget runs out is reported rather than
// waited on forever, because a rolling deploy that never completes is worse than one that
// drops a socket.
func TestDrain_ReportsConnectionsThatOutlastTheDeadline_FR19(t *testing.T) {
	t.Parallel()
	r := newRig(t)
	r.dial().connect()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := r.srv.Drain(ctx); err == nil {
		t.Fatal("Drain reported success although its budget was already spent")
	}
}

// TestNoGoroutineLeaks_NFR3 runs many connect/close cycles and asserts the goroutine
// count comes back. A leak here is two goroutines and a socket per connection, which at
// this design's target scale is the difference between a replica that runs for weeks and
// one that is restarted nightly by an operator who thinks that is normal.
func TestNoGoroutineLeaks_NFR3(t *testing.T) {
	r := newRig(t)

	// One cycle first, so the lazily-created machinery — the transport's idle
	// connections, the listener's per-connection goroutines — is already up before the
	// baseline is taken.
	first := r.dial()
	first.connect("room-1")
	first.close()
	waitFor(t, func() bool { return r.srv.Stats().Current == 0 })
	runtime.GC()
	baseline := runtime.NumGoroutine()

	const cycles = 60
	for range cycles {
		c := r.dial()
		c.connect("room-1")
		c.close()
	}
	waitFor(t, func() bool { return r.srv.Stats().Current == 0 })

	runtime.GC()
	got := runtime.NumGoroutine()
	if allowed := baseline + baseline/20 + 2; got > allowed {
		t.Fatalf("goroutines: %d after %d connect/close cycles, baseline %d, allowed %d (NFR-3)", got, cycles, baseline, allowed)
	}
}
