package hub

import (
	"fmt"
	"testing"
	"time"

	"github.com/raghulj/sidecartunnel/internal/bus"
)

// TestReconcile_WedgedSyncNeverStallsFanOut_C7 is the most important test in this
// package.
//
// C7: with Redis merely slow rather than down, the old bounded command queue filled, the
// fan-out goroutine blocked pushing an unsubscribe for a slow-consumer close, and all
// delivery on the replica stopped while every socket stayed open and /ready stayed 200.
// Here bus.Sync is wedged for the whole test — every reconciliation is stuck — and the
// assertions are that subscribe, unsubscribe, a slow-consumer close taken on the fan-out
// path, and delivery itself all continue to make progress regardless.
func TestReconcile_WedgedSyncNeverStallsFanOut_C7(t *testing.T) {
	t.Parallel()
	b := newBus()
	b.wedge()
	h := newTestHub(t, b)
	// Registered after the hub, so it runs before Close: the reconciler must be let out
	// of the wedge before anything waits for it.
	t.Cleanup(b.release)

	// The first reconciliation — the control-channel subscribe — is now stuck inside
	// Sync and stays stuck.
	select {
	case <-b.synced:
	case <-timeoutAfter():
		t.Fatal("the reconciler never reached Sync")
	}

	one, two := newSink("one", "u1"), newSink("two", "u2")
	mustAdd(t, h, one, two)
	mustSubscribe(t, h, one, "room-1")
	mustSubscribe(t, h, two, "room-1")

	msg := bus.Message{Channel: "st:room-1", Payload: envelope(t, map[string]any{"event": "e", "data": 1})}
	if err := h.Dispatch(msg); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	one.waitFrame(t)
	two.waitFrame(t)

	// The C7 sequence exactly: a slow consumer discovered on the fan-out path, whose
	// close deregisters it and therefore schedules bus work, while the bus is wedged.
	slow := newSink("slow", "u3")
	mustAdd(t, h, slow)
	mustSubscribe(t, h, slow, "room-1")
	slow.full.Store(true)
	if err := h.Dispatch(msg); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	slow.waitClose(t)
	one.waitFrame(t)
	two.waitFrame(t)

	// And churn: thousands of desired-set changes with the reconciler stuck. markDirty is
	// an atomic store and a select-with-default, so none of these can wait for anything.
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 2000; i++ {
			channel := fmt.Sprintf("churn-%d", i)
			if err := h.Subscribe(one, channel, nil); err != nil {
				t.Errorf("Subscribe(%s): %v", channel, err)
				return
			}
			if err := h.Unsubscribe(one, channel, nil); err != nil {
				t.Errorf("Unsubscribe(%s): %v", channel, err)
				return
			}
		}
	}()
	select {
	case <-done:
	case <-timeoutAfter():
		t.Fatal("subscribe/unsubscribe blocked behind a wedged bus (C7)")
	}

	// Delivery is still healthy after all of it.
	if err := h.Dispatch(msg); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	one.waitFrame(t)
	two.waitFrame(t)

	if n := len(b.syncCalls()); n != 1 {
		t.Fatalf("bus.Sync was called %d times while wedged, want 1: the reconciler is the only caller and nobody queues behind it", n)
	}

	// Once the bus recovers, the accumulated state converges in one batched call (M7).
	b.release()
	b.waitSync(t, "st:_control", "st:room-1")
}

// TestReconcile_FailedSyncIsRetriedAndConverges_M5 pins the replacement for an error
// nobody consumed: a failed Sync leaves the set dirty and is retried with backoff, so a
// transient failure cannot leave a channel locally held and upstream dead forever.
func TestReconcile_FailedSyncIsRetriedAndConverges_M5(t *testing.T) {
	t.Parallel()
	b := newBus()
	b.fail(errBus)
	clock := newClock()
	h := newTestHub(t, b, func(o *Options) {
		o.After = clock.After
		o.RetryMin = 10 * time.Millisecond
		o.RetryMax = 40 * time.Millisecond
	})

	s := newSink("c1", "u1")
	mustAdd(t, h, s)
	mustSubscribe(t, h, s, "room-1")

	// Each sleep is taken, then released, which lets the next attempt run. The delay
	// doubles up to its ceiling rather than spinning on a dead bus.
	want := []time.Duration{10 * time.Millisecond, 20 * time.Millisecond, 40 * time.Millisecond}
	var pending waitRequest
	for i, d := range want {
		pending = clock.next(t)
		if pending.d != d {
			t.Fatalf("retry %d slept %s, want %s", i+1, pending.d, d)
		}
		if i < len(want)-1 {
			fire(pending)
		}
	}

	// The bus recovers while the last backoff is still pending. Nothing had to remember
	// the outstanding work: the desired set is the whole of the intent (M5).
	before := len(b.syncCalls())
	b.drainSynced()
	b.fail(nil)
	fire(pending)
	b.waitSync(t, "st:_control", "st:room-1")

	if n := len(b.syncCalls()); n <= before {
		t.Fatalf("bus.Sync was called %d times, want a further attempt after the failures", n)
	}
}

// TestReconcile_CloseDuringBackoff proves shutdown does not wait out a retry sleep.
func TestReconcile_CloseDuringBackoff(t *testing.T) {
	t.Parallel()
	b := newBus()
	b.fail(errBus)
	clock := newClock()
	h := newTestHub(t, b, func(o *Options) { o.After = clock.After })

	clock.next(t)

	done := make(chan struct{})
	go func() {
		defer close(done)
		h.Close()
	}()
	select {
	case <-done:
	case <-timeoutAfter():
		t.Fatal("Close waited for a backoff sleep to expire")
	}
}

// TestBackoff_Schedule is the table for the retry delays.
func TestBackoff_Schedule(t *testing.T) {
	t.Parallel()
	const floor, ceiling = 200 * time.Millisecond, time.Second
	tests := []struct {
		attempt int
		want    time.Duration
	}{
		{1, 200 * time.Millisecond},
		{2, 400 * time.Millisecond},
		{3, 800 * time.Millisecond},
		{4, time.Second},
		{10, time.Second},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("attempt %d", tt.attempt), func(t *testing.T) {
			t.Parallel()
			if got := backoff(tt.attempt, floor, ceiling); got != tt.want {
				t.Fatalf("backoff(%d) = %s, want %s", tt.attempt, got, tt.want)
			}
		})
	}
}

// TestReconcile_SyncGetsAFreshSlice covers the bus contract's "must not retain the
// slice": every pass allocates its own snapshot, so an implementation that keeps the
// slice it was handed cannot have it rewritten underneath it by the next reconciliation.
func TestReconcile_SyncGetsAFreshSlice(t *testing.T) {
	t.Parallel()
	b := newBus()
	h := newTestHub(t, b)
	s := newSink("c1", "u1")
	mustAdd(t, h, s)

	mustSubscribe(t, h, s, "room-1")
	b.waitSync(t, "st:_control", "st:room-1")
	if err := h.Unsubscribe(s, "room-1", nil); err != nil {
		t.Fatalf("Unsubscribe: %v", err)
	}
	b.waitSync(t, "st:_control")
	mustSubscribe(t, h, s, "room-2")
	b.waitSync(t, "st:_control", "st:room-2")

	var pairs [][]string
	for _, call := range b.rawCalls() {
		if len(call) == 2 {
			pairs = append(pairs, call)
		}
	}
	if len(pairs) < 2 {
		t.Fatalf("want two two-element reconciliations, got %d", len(pairs))
	}
	if &pairs[0][0] == &pairs[len(pairs)-1][0] {
		t.Fatal("two Sync calls shared a backing array")
	}
}
