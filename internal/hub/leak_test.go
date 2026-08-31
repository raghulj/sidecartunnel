package hub

import (
	"context"
	"fmt"
	"runtime"
	"testing"

	"github.com/raghulj/sidecartunnel/internal/bus"
)

// TestHub_NoGoroutineLeaks_NFR3 runs many hub lifetimes and many connect/disconnect
// cycles inside each, and asserts the goroutine count comes back.
//
// It needs no sleep and no polling because Close waits on the WaitGroup that every
// goroutine the hub starts — the reconciler, the closer, and each spawned overflow
// close — is registered with. A hub that has returned from Close has none left.
func TestHub_NoGoroutineLeaks_NFR3(t *testing.T) {
	runtime.GC()
	baseline := runtime.NumGoroutine()

	const hubs, conns = 50, 20
	for i := 0; i < hubs; i++ {
		b := newBus()
		h := New(context.Background(), b, Options{})
		for j := 0; j < conns; j++ {
			s := newSink(fmt.Sprintf("c%d-%d", i, j), "u1")
			if err := h.Add(s); err != nil {
				t.Fatalf("Add: %v", err)
			}
			if err := h.Subscribe(s, "room-1"); err != nil {
				t.Fatalf("Subscribe: %v", err)
			}
			if err := h.Dispatch(bus.Message{Channel: "st:room-1", Payload: []byte(`{"event":"e","data":1}`)}); err != nil {
				t.Fatalf("Dispatch: %v", err)
			}
			h.Remove(s)
		}
		h.Close()
		h.Close() // idempotent: drain and a context cancellation both arrive here.
	}

	runtime.GC()
	got := runtime.NumGoroutine()
	if allowed := baseline + baseline/20 + 2; got > allowed {
		t.Fatalf("goroutines: %d after %d hub lifetimes, baseline %d, allowed %d (NFR-3)", got, hubs, baseline, allowed)
	}
}

// TestHub_ParentContextCancellationStopsTheGoroutines: the hub's goroutines are bounded
// by the context it was given, not only by Close, so a wiring that cancels on SIGTERM
// needs no second teardown call.
func TestHub_ParentContextCancellationStopsTheGoroutines(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	h := New(ctx, newBus(), Options{})
	cancel()

	done := make(chan struct{})
	go func() {
		defer close(done)
		h.wg.Wait()
	}()
	select {
	case <-done:
	case <-timeoutAfter():
		t.Fatal("cancelling the parent context did not stop the hub's goroutines")
	}
	h.Close()
}
