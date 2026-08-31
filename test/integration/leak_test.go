package integration_test

import (
	"runtime"
	"testing"
)

// leakCycles is how many connect/subscribe/disconnect cycles the leak test runs.
//
// docs/11-testing.md §6 asks for 10,000 as a load test, run before a release. This is the
// per-commit version: enough cycles that a per-connection leak is unmistakable — a
// goroutine per connection would be several hundred of them — and few enough that the
// suite stays a thing people run.
const leakCycles = 300

// TestConnectDisconnectCyclesLeakNoGoroutines proves NFR-3 against a real Redis: after
// many complete connection lifecycles the process holds no more goroutines than it did
// before.
//
// This is the leak class that only shows up after a week of uptime, and it is exactly the
// class that a unit test with a fake socket misses: the goroutines that leak are the ones
// created per connection — the reader, the writer, and whatever the websocket library and
// the HTTP server keep behind them — and a fake never creates them.
//
// Each cycle is a complete lifecycle, not a half one: dial, connect, subscribe, and close
// from the client side, which is how a browser tab actually goes away. The count is taken
// after a warm-up cycle, so the pools and background goroutines that are created once are
// not counted as growth.
//
// The assertion allows a small tolerance because the process is not otherwise quiet: the
// Redis client keeps a connection pool, and the HTTP server may hold a goroutine for a
// socket it has not yet noticed is closed. A leak of the kind this test exists to catch
// is proportional to leakCycles, so it is hundreds of goroutines over the line rather
// than a handful.
//
// It fails if a connection's reader or writer can outlive its socket, if Conn.Run returns
// before its writer has, or if the hub's closer goroutine can be left holding a reference
// to a connection nobody else does.
func TestConnectDisconnectCyclesLeakNoGoroutines(t *testing.T) {
	t.Parallel()
	c := newCluster(t, clusterOptions{Replicas: 1})
	r := c.r(0)

	// One warm-up lifecycle, so everything created once has been created.
	cycle(t, r)
	waitFor(t, "the warm-up connection to unwind", func() bool { return r.srv.Stats().Current == 0 })
	runtime.GC()
	baseline := runtime.NumGoroutine()

	for range leakCycles {
		cycle(t, r)
	}

	waitFor(t, "every connection to unwind", func() bool { return r.srv.Stats().Current == 0 })

	// The tolerance is 5% of the baseline or 8 goroutines, whichever is larger:
	// docs/01-requirements.md NFR-3 states 5%, and on a small process 5% is under one
	// goroutine, which would make the assertion about scheduler timing rather than about
	// leaks.
	tolerance := max(baseline/20, 8)
	waitFor(t, "the goroutine count to return to its baseline", func() bool {
		runtime.GC()
		return runtime.NumGoroutine() <= baseline+tolerance
	})

	if got := runtime.NumGoroutine(); got > baseline+tolerance {
		t.Fatalf("goroutines = %d after %d connect/disconnect cycles, want no more than %d (baseline %d + tolerance %d) (NFR-3)",
			got, leakCycles, baseline+tolerance, baseline, tolerance)
	}
}

// cycle is one complete connection lifecycle: dial, connect, subscribe, close.
//
// The close is explicit rather than left to the cleanup the dial registers. A cycle that
// relied on cleanup would hold every socket open until the test ended, and would measure
// three hundred concurrent connections rather than three hundred sequential lifetimes —
// which is the opposite of what NFR-3 is about.
func cycle(t *testing.T, r *replica) {
	t.Helper()
	ws, status, err := r.dialOrigin(testOrigin)
	if err != nil {
		t.Fatalf("dial: %v (handshake status %d)", err, status)
	}
	ws.connect("room-11")
	ws.subscribe("room-12")
	ws.close()
}
