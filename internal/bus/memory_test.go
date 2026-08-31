package bus

import (
	"testing"
)

// TestMemoryIntakeOverflowIsCountedAndDropped pins the exact overflow arithmetic, which
// the conformance table cannot: memory delivers synchronously inside Publish, so the drop
// count is deterministic here and only bounded there.
//
// Dropping is the documented behaviour (see MemoryBus.Publish and RedisBus.Receive). The
// alternative — blocking the producer — is C7 in a different costume on memory, and M8's
// eviction oscillation on redis.
func TestMemoryIntakeOverflowIsCountedAndDropped(t *testing.T) {
	const intake = 4
	b := NewMemory(intake)
	t.Cleanup(func() { _ = b.Close() })

	mustSync(t, b, "ch")
	const published = 10
	for i := 0; i < published; i++ {
		mustPublish(t, b, "ch", "x")
	}

	h := b.Health()
	if h.IntakeDepth != intake {
		t.Errorf("IntakeDepth = %d, want %d", h.IntakeDepth, intake)
	}
	if want := uint64(published - intake); h.Dropped != want {
		t.Errorf("Dropped = %d, want %d", h.Dropped, want)
	}
}

// TestMemoryHealth asserts the memory bus is always connected. It has no transport to
// lose, so /ready must never go 503 on its account (docs/04-integration.md §4).
func TestMemoryHealth(t *testing.T) {
	b := NewMemory(0)
	t.Cleanup(func() { _ = b.Close() })

	h := b.Health()
	if !h.Connected {
		t.Error("Connected = false, want true: the memory bus has no transport to lose")
	}
	if h.DisconnectedFor != 0 {
		t.Errorf("DisconnectedFor = %s, want 0", h.DisconnectedFor)
	}
	if h.Reconnects != 0 {
		t.Errorf("Reconnects = %d, want 0", h.Reconnects)
	}
}

// TestMemoryDefaultIntake asserts a non-positive intake size falls back to the documented
// default rather than producing an unbuffered channel, where every publish would be
// dropped unless a consumer happened to be blocked on Receive at that instant.
func TestMemoryDefaultIntake(t *testing.T) {
	b := NewMemory(0)
	t.Cleanup(func() { _ = b.Close() })

	if got := cap(b.Receive()); got != DefaultIntakeQueue {
		t.Errorf("intake capacity = %d, want %d", got, DefaultIntakeQueue)
	}
}
