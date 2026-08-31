package hub

import (
	"context"
	"testing"
)

// TestClose_LosesNoCloseEnqueuedWhileItShutsDown covers both halves of the shutdown
// defect, with a seam rather than a sleep.
//
// Close documented only that it must not race Dispatch. But Attach, Subscribe,
// Unsubscribe and controlUnsubscribe all reach enqueueClose, so a close can be enqueued
// at any moment during a shutdown, and two things went wrong when one was:
//
//   - The closer goroutine returned on ctx.Done and abandoned whatever was still in the
//     queue — up to CloserQueue connections left open with nothing left to end them.
//   - The overflow path did h.wg.Add(1) on the very WaitGroup Close was inside Wait on,
//     which is "sync: WaitGroup misuse: Add called concurrently with Wait": a panic,
//     during shutdown, on a path that only runs when a connection is already misbehaving.
//
// The seam parks the closer at the moment it has committed to returning, which is exactly
// the window, and makes the ordering exact instead of probable.
func TestClose_LosesNoCloseEnqueuedWhileItShutsDown(t *testing.T) {
	exiting := make(chan struct{})
	release := make(chan struct{})
	h := newTestHub(t, newBus(), func(o *Options) {
		// One slot, so the third enqueue below is forced down the overflow path.
		o.CloserQueue = 1
		o.seams.closerExiting = func() {
			close(exiting)
			<-release
		}
	})

	closed := make(chan struct{})
	go func() {
		defer close(closed)
		h.Close()
	}()

	select {
	case <-exiting:
	case <-timeoutAfter():
		t.Fatal("the closer goroutine never reached its exit")
	}

	// queued goes into the capacity-1 queue nothing is draining any more; spilled and
	// spilledAgain overflow it and are spawned.
	queued := newSink("c1", "u1")
	spilled := newSink("c2", "u1")
	spilledAgain := newSink("c3", "u1")
	for _, s := range []*fakeSink{queued, spilled, spilledAgain} {
		h.enqueueClose(s)
	}

	close(release)
	select {
	case <-closed:
	case <-timeoutAfter():
		t.Fatal("Close did not return")
	}

	for _, s := range []*fakeSink{queued, spilled, spilledAgain} {
		if got := s.closeCount(); got != 1 {
			t.Fatalf("sink %s was closed %d times, want 1: a close abandoned by the shutdown "+
				"is a connection left open with nothing left to end it", s.id, got)
		}
	}
}

// TestEnqueueClose_AfterCloseRunsInline is the other side of the same ordering. Once
// Close has stopped waiting there is no goroutine to hand a close to, and handing it to a
// new one would be the WaitGroup misuse again — so the caller does it, before
// enqueueClose returns.
func TestEnqueueClose_AfterCloseRunsInline(t *testing.T) {
	h := New(context.Background(), newBus(), Options{})
	h.Close()

	s := newSink("c1", "u1")
	h.enqueueClose(s)

	if got := s.closeCount(); got != 1 {
		t.Fatalf("closes = %d, want 1 by the time enqueueClose returned: after Close there is "+
			"no closer goroutine, and a close handed to a new one is registered on a "+
			"WaitGroup that is already being waited on", got)
	}
	h.Close() // idempotent, and must not wait a second time on anything.
}
