package hub

import "time"

// markDirty records that the desired set has changed and nudges the reconciler.
//
// C7: this is the function that must never block, and the reason the whole package is
// built as a desired-state reconciler rather than a command queue. The earlier design
// pushed subscribe and unsubscribe onto a bounded channel drained by one goroutine; with
// Redis merely slow the queue filled, the fan-out goroutine blocked pushing an
// unsubscribe for a slow-consumer close, and all delivery on the replica stopped while
// every socket stayed open and /ready stayed 200 (S2, docs/09-internals.md §4.1).
//
// An atomic store plus a select-with-default on a capacity-1 channel cannot block, so no
// producer can stall — least of all the fan-out goroutine. A dropped wake token loses
// nothing: the dirty flag is the truth, and the reconciler drains it in a loop.
func (h *Hub) markDirty() {
	h.dirty.Store(true)
	select {
	case h.wake <- struct{}{}:
	default:
	}
}

// snapshot copies the desired set for one Sync call.
//
// A fresh slice per pass, because bus.Sync must not retain the one it is given and the
// hub must not hand out a view of a map it is about to mutate.
func (h *Hub) snapshot() []string {
	h.mu.RLock()
	defer h.mu.RUnlock()
	out := make([]string, 0, len(h.desired))
	for key := range h.desired {
		out = append(out, key)
	}
	return out
}

// reconcile drives the bus towards the desired set, forever.
//
// It is the only goroutine that calls bus.Sync, and Sync is idempotent and takes the
// whole set, which is what kills three defects at once: a failed Sync leaves the set
// dirty and is retried with backoff rather than leaving a channel locally held and
// upstream dead (M5); a bus reconnect is a forced dirty rather than a sweep racing a live
// command stream (M6); and a 30,000-channel resubscribe is one call rather than 30,000
// serial round trips (M7).
//
// The dirty flag is cleared before the snapshot is taken, so a subscribe that lands
// during a Sync sets it again and is picked up by the next turn of the inner loop. The
// worst case is one redundant Sync, which is a no-op by contract.
func (h *Hub) reconcile() {
	defer h.wg.Done()
	attempt := 0
	for {
		select {
		case <-h.ctx.Done():
			return
		case <-h.wake:
		}
		for h.dirty.Swap(false) {
			if err := h.bus.Sync(h.ctx, h.snapshot()); err != nil {
				// M5: never lose the intent. The error is not swallowed — it is
				// re-armed as state, which is the only form that survives a process
				// that cannot report it anywhere useful yet.
				h.dirty.Store(true)
				attempt++
				if !h.wait(backoff(attempt, h.retryMin, h.retryMax)) {
					return
				}
				continue
			}
			attempt = 0
		}
	}
}

// wait sleeps for d, or returns false if the hub is closing.
func (h *Hub) wait(d time.Duration) bool {
	timer := h.after(d)
	select {
	case <-h.ctx.Done():
		return false
	case <-timer:
		return true
	}
}

// backoff returns the delay before retry number attempt, doubling from floor and clamped
// at ceiling. attempt is 1 for the first retry.
//
// It is deliberately deterministic and unjittered. The jitter in docs/13-review-findings.md
// M1 is about a fleet of browsers retrying one application at once; this is one goroutine
// per replica retrying its own Redis connection, where the retries are already spread by
// whenever each replica's Sync happened to fail, and a deterministic schedule is one a
// test can assert exactly.
func backoff(attempt int, floor, ceiling time.Duration) time.Duration {
	d := floor
	for i := 1; i < attempt; i++ {
		d *= 2
		if d >= ceiling {
			return ceiling
		}
	}
	return d
}
