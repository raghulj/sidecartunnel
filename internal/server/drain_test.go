package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"runtime"
	"sync"
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

// TestDrain_ConnectionAdmittedAfterTheSnapshot_FR19 drives the window between reserve()
// and track() deterministically, with a seam rather than a sleep.
//
// ServeHTTP reserves a slot before the upgrade; the connection is only put in s.conns —
// the set Drain snapshots — once it has been built. A handshake that lands in between is
// invisible to the drain, and two things follow, both on every deploy:
//
//   - The connection is never told to close, so Drain waits out the whole of
//     server.drain_timeout, serve returns exit 1, and that client gets a bare 1006
//     instead of a 3000 carrying a spread retry_after — the stampede FR-19 exists to
//     prevent, aimed at the application by the pod being rolled.
//   - The tracking used to be a sync.WaitGroup, and Add on a counter at zero concurrently
//     with Wait is "sync: WaitGroup misuse", which is a panic: the process dies during
//     SIGTERM.
//
// The window is tens of microseconds against a handshake rate of a couple of hundred a
// second, so it is rare per connection and routine per fleet.
func TestDrain_ConnectionAdmittedAfterTheSnapshot_FR19(t *testing.T) {
	reserved := make(chan struct{})
	proceed := make(chan struct{})
	snapshotted := make(chan struct{})
	var snapshotOnce sync.Once

	r := newRigWithOptions(t, func(o *Options) {
		o.seams.afterReserve = func() {
			select {
			case <-reserved:
			default:
				close(reserved)
				<-proceed
			}
		}
		// The rig drains again on cleanup, so both hooks fire at most once.
		o.seams.afterDrainSnapshot = func() { snapshotOnce.Do(func() { close(snapshotted) }) }
	}, func(c *config.Config) {
		// A drain that is going to fail should fail inside the test's own budget rather
		// than the documented 20s.
		c.Server.DrainTimeout = config.Duration(failAfter)
	})
	r.http = httptest.NewServer(r.srv.Handler())
	t.Cleanup(r.http.Close)

	dialed := make(chan *client, 1)
	go func() {
		c, _, err := r.dialOrigin(testOrigin)
		if err != nil {
			t.Errorf("dial: %v", err)
			close(dialed)
			return
		}
		dialed <- c
	}()

	// The handshake is now past reserve() and has not been tracked.
	select {
	case <-reserved:
	case <-time.After(failAfter):
		t.Fatal("no handshake reached the window between reserve and track")
	}

	drained := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*failAfter)
		defer cancel()
		drained <- r.srv.Drain(ctx)
	}()

	// Drain has set draining and taken its snapshot, which this connection is not in.
	select {
	case <-snapshotted:
	case <-time.After(failAfter):
		t.Fatal("Drain never took its snapshot")
	}
	close(proceed)

	select {
	case err := <-drained:
		if err != nil {
			t.Fatalf("Drain: %v — the connection admitted after the snapshot was never closed (FR-19)", err)
		}
	case <-time.After(2 * failAfter):
		t.Fatal("Drain did not return")
	}

	c := <-dialed
	if c == nil {
		t.Fatal("the handshake never completed")
	}
	got := c.wantDisconnect(proto.CloseDraining)
	if !got.Reconnect || got.RetryAfter <= 0 {
		t.Fatalf("disconnect = %+v, want reconnect true with a spread retry_after (FR-19, §7.1)", got)
	}
}

// TestDrain_ConcurrentDrainsBothReturn_FR19: SIGTERM and a cancelled context can both
// arrive, so two drains can be in flight at once and each has to see the replica empty.
func TestDrain_ConcurrentDrainsBothReturn_FR19(t *testing.T) {
	t.Parallel()
	r := newRig(t)
	c := r.dial()
	c.connect("room-1")

	const drains = 3
	errs := make(chan error, drains)
	for range drains {
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), failAfter)
			defer cancel()
			errs <- r.srv.Drain(ctx)
		}()
	}
	for range drains {
		select {
		case err := <-errs:
			if err != nil {
				t.Errorf("Drain: %v", err)
			}
		case <-time.After(2 * failAfter):
			t.Fatal("a concurrent Drain never returned")
		}
	}
}
