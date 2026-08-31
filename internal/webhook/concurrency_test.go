package webhook

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/raghulj/sidecartunnel/internal/config"
)

// TestCall_ConcurrencyCap_NFR4 is NFR-4's acceptance criterion: with the cap at 8, a mass
// reconnect produces at most 8 concurrent requests at the application.
//
// Excess connections wait inside the gateway, where waiting is cheap, rather than being
// issued at an application with a fixed worker pool. A reconnect after a replica restart
// is N simultaneous authentications and N is every connected user
// (docs/04-integration.md §1.5).
func TestCall_ConcurrencyCap_NFR4(t *testing.T) {
	const (
		capacity = 8
		calls    = 200
	)

	var (
		inFlight    atomic.Int64
		observedMax atomic.Int64
		gate        = make(chan struct{})
		gateOnce    sync.Once
	)

	app := newStubApp(t, func(w http.ResponseWriter, _ int) {
		cur := inFlight.Add(1)
		for {
			old := observedMax.Load()
			if cur <= old || observedMax.CompareAndSwap(old, cur) {
				break
			}
		}
		// Hold every request until the cap is actually reached, so the test proves the
		// gateway reaches 8 as well as never exceeding it. With 200 callers and a cap of
		// 8 this always fires: the eighth admitted request closes the gate for everyone.
		if cur >= capacity {
			gateOnce.Do(func() { close(gate) })
		}
		<-gate
		inFlight.Add(-1)
		okBody(w, 0)
	})

	cfg := testApp(app.server.URL)
	cfg.WebhookConcurrency = capacity
	c := newTestClient(t, Options{App: cfg})

	var wg sync.WaitGroup
	results := make([]Result, calls)
	for i := range calls {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results[i] = c.Call(t.Context(), testRequest())
		}()
	}
	wg.Wait()

	for i, res := range results {
		mustAuthorized(t, res)
		_ = i
	}
	if got := observedMax.Load(); got > capacity {
		t.Errorf("the application observed %d concurrent requests, want at most %d (NFR-4)", got, capacity)
	} else if got != capacity {
		t.Errorf("the application observed %d concurrent requests, want exactly %d: the cap is not being filled", got, capacity)
	}
	if n := c.InFlight(); n != 0 {
		t.Errorf("InFlight = %d after every call returned, want 0: a slot was leaked", n)
	}
	if n := c.Waiting(); n != 0 {
		t.Errorf("Waiting = %d after every call returned, want 0: a queue slot was leaked", n)
	}
}

// TestCall_QueueOverflowIsTransient_C2 is the other axis. app.connect_queue caps how many
// may wait; overflow is a failure, never a refusal.
//
// The earlier design let the handshake timeout fire on queued connections and close them
// 3001 reconnect:false, so the mechanism advertised as protecting the application against
// a reconnect storm would have permanently locked out every user caught in one
// (docs/13-review-findings.md C2, NFR-4).
func TestCall_QueueOverflowIsTransient_C2(t *testing.T) {
	block := make(chan struct{})
	started := make(chan struct{}, 1)
	release := sync.OnceFunc(func() { close(block) })
	app := newStubApp(t, func(w http.ResponseWriter, _ int) {
		select {
		case started <- struct{}{}:
		default:
		}
		<-block
		okBody(w, 0)
	})
	// Registered after newStubApp so it runs before the server's cleanup: t.Cleanup is
	// LIFO, and httptest.Server.Close waits for outstanding handlers.
	t.Cleanup(release)

	cfg := testApp(app.server.URL)
	cfg.WebhookConcurrency = 1
	cfg.ConnectQueue = 1
	c := newTestClient(t, Options{App: cfg})

	var wg sync.WaitGroup
	wg.Add(2)
	// One call occupies the single in-flight slot.
	go func() { defer wg.Done(); mustCallOK(t, c) }()
	<-started
	// A second occupies the single queue slot.
	go func() { defer wg.Done(); mustCallOK(t, c) }()
	waitFor(t, "the second call to enter the queue", func() bool { return c.Waiting() == 1 })

	// The third has nowhere to go. It must fail fast and retryably.
	res := c.Call(t.Context(), testRequest())
	u, ok := res.(Unavailable)
	if !ok {
		t.Fatalf("Result = %T, want Unavailable: a full queue is transient, never a refusal (C2)", res)
	}
	if !errors.Is(u, ErrQueueOverflow) {
		t.Errorf("Unavailable does not unwrap to ErrQueueOverflow: %v", u)
	}
	if u.CloseCode() != 3008 || !u.Reconnect() {
		t.Errorf("queue overflow closes %d reconnect=%v, want 3008 true (FR-6)", u.CloseCode(), u.Reconnect())
	}
	if n := app.count(); n != 1 {
		t.Errorf("the application saw %d requests, want 1: the overflow reached the network", n)
	}

	release()
	wg.Wait()
}

// mustCallOK runs a call that is expected to succeed, from a goroutine.
func mustCallOK(t *testing.T, c *Client) {
	t.Helper()
	if _, ok := c.Call(context.Background(), testRequest()).(Authorized); !ok {
		t.Error("a queued call did not authorize")
	}
}

// TestCall_ConnectTimeoutWhileWaitingIsTransient_C2 is the second bound: any one
// connection may wait at most app.connect_timeout. Exceeding it closes 3008 with a
// retry_after — never 3001, which is permanent (FR-4, FR-6, NFR-4).
//
// The wait is bounded by the injected clock, so this test moves time rather than
// spending it.
func TestCall_ConnectTimeoutWhileWaitingIsTransient_C2(t *testing.T) {
	block := make(chan struct{})
	started := make(chan struct{}, 1)
	release := sync.OnceFunc(func() { close(block) })
	app := newStubApp(t, func(w http.ResponseWriter, _ int) {
		select {
		case started <- struct{}{}:
		default:
		}
		<-block
		okBody(w, 0)
	})
	// Registered after newStubApp so it runs before the server's cleanup: t.Cleanup is
	// LIFO, and httptest.Server.Close waits for outstanding handlers.
	t.Cleanup(release)

	clock := newFakeClock(baseTime)
	cfg := testApp(app.server.URL)
	cfg.WebhookConcurrency = 1
	cfg.ConnectTimeout = config.Duration(10 * time.Second)
	c := newTestClient(t, Options{App: cfg, Clock: clock})

	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); mustCallOK(t, c) }()
	<-started

	done := make(chan Result, 1)
	go func() { done <- c.Call(t.Context(), testRequest()) }()

	clock.awaitTimer(t) // the second call is now waiting on connect_timeout
	clock.advance(10 * time.Second)
	clock.fireTimers()

	select {
	case res := <-done:
		u, ok := res.(Unavailable)
		if !ok {
			t.Fatalf("Result = %T, want Unavailable: a queue wait that times out is retryable (C2)", res)
		}
		if !errors.Is(u, context.DeadlineExceeded) {
			t.Errorf("Unavailable does not unwrap to context.DeadlineExceeded: %v", u)
		}
		if u.CloseCode() != 3008 || !u.Reconnect() {
			t.Errorf("closes %d reconnect=%v, want 3008 true", u.CloseCode(), u.Reconnect())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the waiting call did not give up within 5s of its budget expiring")
	}

	if n := app.count(); n != 1 {
		t.Errorf("the application saw %d requests, want 1", n)
	}
	release()
	wg.Wait()
}

// TestCall_CallerCancellationWhileWaitingIsTransient: the browser went away mid-queue.
// The caller has nothing left to close, but the Result must still be the retryable one —
// there is no decision here to make permanent.
func TestCall_CallerCancellationWhileWaitingIsTransient(t *testing.T) {
	block := make(chan struct{})
	started := make(chan struct{}, 1)
	release := sync.OnceFunc(func() { close(block) })
	app := newStubApp(t, func(w http.ResponseWriter, _ int) {
		select {
		case started <- struct{}{}:
		default:
		}
		<-block
		okBody(w, 0)
	})
	// Registered after newStubApp so it runs before the server's cleanup: t.Cleanup is
	// LIFO, and httptest.Server.Close waits for outstanding handlers.
	t.Cleanup(release)

	cfg := testApp(app.server.URL)
	cfg.WebhookConcurrency = 1
	c := newTestClient(t, Options{App: cfg})

	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); mustCallOK(t, c) }()
	<-started

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan Result, 1)
	go func() { done <- c.Call(ctx, testRequest()) }()
	waitFor(t, "the second call to enter the queue", func() bool { return c.Waiting() == 1 })
	cancel()

	select {
	case res := <-done:
		u, ok := res.(Unavailable)
		if !ok {
			t.Fatalf("Result = %T, want Unavailable", res)
		}
		if !errors.Is(u, context.Canceled) {
			t.Errorf("Unavailable does not unwrap to context.Canceled: %v", u)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the cancelled call did not return within 5s")
	}
	release()
	wg.Wait()
}

// TestCall_BudgetExhaustedBeforeARetry: the whole authorization budget covers queue wait
// plus the call, so a retry that would start after it has run out is not made. The
// application must not receive a request from a connection the gateway has already given
// up on (docs/08-config.md §3, app.connect_timeout).
func TestCall_BudgetExhaustedBeforeARetry(t *testing.T) {
	clock := newFakeClock(baseTime)
	app := newStubApp(t, func(w http.ResponseWriter, _ int) {
		// The application is slow enough that the budget is gone by the time it answers.
		clock.advance(11 * time.Second)
		w.WriteHeader(http.StatusInternalServerError)
	})

	cfg := testApp(app.server.URL)
	cfg.ConnectTimeout = config.Duration(10 * time.Second)
	cfg.WebhookRetries = 3
	c := newTestClient(t, Options{App: cfg, Clock: clock})

	res := c.Call(t.Context(), testRequest())
	u, ok := res.(Unavailable)
	if !ok {
		t.Fatalf("Result = %T, want Unavailable", res)
	}
	if !errors.Is(u, context.DeadlineExceeded) {
		t.Errorf("Unavailable does not unwrap to context.DeadlineExceeded: %v", u)
	}
	if n := app.count(); n != 1 {
		t.Errorf("the application saw %d requests, want 1: a retry was issued past the budget", n)
	}
}

// TestCall_CallerCancellationBeforeTheCall: a context already dead on arrival must not
// reach the network at all.
func TestCall_CallerCancellationBeforeTheCall(t *testing.T) {
	app := newStubApp(t, okBody)
	c := newTestClient(t, Options{App: testApp(app.server.URL)})

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	res := c.Call(ctx, testRequest())
	if _, ok := res.(Unavailable); !ok {
		t.Fatalf("Result = %T, want Unavailable", res)
	}
	if n := app.count(); n != 0 {
		t.Errorf("the application saw %d requests, want 0", n)
	}
}
