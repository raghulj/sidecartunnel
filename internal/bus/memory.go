package bus

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
)

// MemoryBus is a single-process Bus: a publish is delivered to this process's own
// subscribers and to nobody else.
//
// It exists for tests and single-node development (docs/08-config.md §3). Running more
// than one replica on it is undetectable by the gateway and produces the worst kind of
// bug — messages that arrive for some users and not others — so whoever constructs it
// from configuration logs a prominent warning at startup every time.
//
// It is safe for concurrent use by any number of goroutines, and it matches the redis
// implementation's observable behaviour case for case (see conformance_test.go), because
// a memory bus that behaves differently is a memory bus that hides bugs until production.
type MemoryBus struct {
	out chan Message

	// mu guards subs and closed. It is never held across anything that can block: the
	// send on out below is non-blocking, so a publisher cannot stall a Sync and no
	// sequence of calls can deadlock.
	mu     sync.RWMutex
	subs   map[string]struct{}
	closed bool

	dropped atomic.Uint64
}

// NewMemory returns a MemoryBus whose intake channel holds intake messages. A
// non-positive intake uses DefaultIntakeQueue.
//
// The returned bus is connected immediately and starts no goroutines. Close it when
// finished; Receive's channel is closed only by Close.
func NewMemory(intake int) *MemoryBus {
	if intake <= 0 {
		intake = DefaultIntakeQueue
	}
	return &MemoryBus{
		out:  make(chan Message, intake),
		subs: map[string]struct{}{},
	}
}

// Sync makes the subscription set exactly desired. It is idempotent, it never blocks, and
// it does not retain desired.
//
// There is no batching to do — there is no transport — but the whole-set signature is the
// point of S2 and is what keeps this backend and the redis one interchangeable.
//
// It returns ErrClosed after Close and the context's error if ctx is already done.
func (m *MemoryBus) Sync(ctx context.Context, desired []string) error {
	// closed is checked under the lock below rather than here, because the lock is what
	// makes the answer true rather than merely recent.
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("bus: sync %d channels: %w", len(desired), err)
	}

	// Copied, never retained: the reconciler reuses its backing array on the next pass
	// (docs/09-internals.md §4.2).
	next := make(map[string]struct{}, len(desired))
	for _, ch := range desired {
		next[ch] = struct{}{}
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return fmt.Errorf("bus: sync %d channels: %w", len(desired), ErrClosed)
	}
	m.subs = next
	return nil
}

// Publish delivers payload to this process's subscribers of channel, and copies it first.
//
// The copy is not an optimization to remove. Redis round-trips a publish through a socket,
// so the receiver's bytes are always distinct from the publisher's; without the copy a
// caller that reuses its buffer would race on memory and be correct on redis, which is the
// worst possible way to discover the two are not equivalent.
//
// Publishing to a channel nobody holds is not an error — Redis says the same thing with a
// subscriber count of zero (docs/04-integration.md §2.2).
//
// If the intake is full the message is dropped and counted in Health.Dropped. Blocking
// here would let one slow dispatch worker stall every publisher, which is C7 with a
// different queue in front of it.
func (m *MemoryBus) Publish(ctx context.Context, channel string, payload []byte) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("bus: publish %s: %w", channel, err)
	}

	// The read lock is what makes the send safe against a concurrent Close: Close takes
	// the write lock before closing out, so out cannot close under a send in progress.
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.closed {
		return fmt.Errorf("bus: publish %s: %w", channel, ErrClosed)
	}
	if _, held := m.subs[channel]; !held {
		return nil
	}

	msg := Message{Channel: channel, Payload: make([]byte, len(payload))}
	copy(msg.Payload, payload)
	select {
	case m.out <- msg:
	default:
		m.dropped.Add(1)
	}
	return nil
}

// Receive returns the intake channel. It is the same channel on every call and is closed
// only by Close.
func (m *MemoryBus) Receive() <-chan Message {
	return m.out
}

// Close releases the bus and closes the channel Receive returns. It is idempotent and
// safe to call concurrently with Sync and Publish, both of which then return ErrClosed.
func (m *MemoryBus) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil
	}
	m.closed = true
	close(m.out)
	return nil
}

// Health reports a memory bus as permanently connected: it has no transport to lose, so
// nothing here may ever make /ready return 503.
func (m *MemoryBus) Health() Health {
	m.mu.RLock()
	subs := len(m.subs)
	m.mu.RUnlock()
	return Health{
		Connected:     true,
		Subscriptions: subs,
		IntakeDepth:   len(m.out),
		Dropped:       m.dropped.Load(),
	}
}
