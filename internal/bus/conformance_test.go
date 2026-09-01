package bus

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
)

// receiveTimeout bounds every wait for an expected message.
//
// It is a failure detector, not a sleep (docs/14-coding-standards.md §2): the happy path
// takes microseconds and this only fires when the test has already failed, at which point
// a clear message beats a hung test.
const receiveTimeout = 2 * time.Second

// harness is one live bus plus a channel-name namespace private to the test.
//
// The namespace matters only when ST_TEST_REDIS_URL points the suite at a shared Redis:
// pub/sub is global there, so two tests using "a" would deliver into each other.
type harness struct {
	bus Bus
	key func(name string) string
}

// backend is one implementation under the conformance table.
type backend struct {
	name string
	open func(t *testing.T, intake int) harness
}

// backends returns every implementation the conformance table runs against.
//
// Both must pass the identical table. The memory bus exists so protocol tests are fast
// and deterministic, and the moment the two drift it stops standing in for the redis one
// (docs/11-testing.md §1).
func backends() []backend {
	return []backend{
		{name: "memory", open: openMemory},
		{name: "redis", open: openRedis},
	}
}

func openMemory(t *testing.T, intake int) harness {
	t.Helper()
	b := NewMemory(intake)
	t.Cleanup(func() { _ = b.Close() })
	return harness{bus: b, key: keyspace(t)}
}

func openRedis(t *testing.T, intake int) harness {
	t.Helper()
	b := mustRedis(t, RedisOptions{URL: redisURL(t), IntakeQueue: intake})
	if h := b.Health(); !h.Connected {
		t.Fatalf("NewRedis returned disconnected: %+v", h)
	}
	return harness{bus: b, key: keyspace(t)}
}

// mustRedis builds a redis bus with test-scale backoff and fails the test on a
// construction error. Cleanup closes it, which also exercises Close being idempotent
// against the tests that close it themselves.
func mustRedis(t *testing.T, opts RedisOptions) *RedisBus {
	t.Helper()
	if opts.ReconnectMin == 0 {
		opts.ReconnectMin = time.Millisecond
	}
	if opts.ReconnectMax == 0 {
		opts.ReconnectMax = 5 * time.Millisecond
	}
	b, err := NewRedis(opts)
	if err != nil {
		t.Fatalf("NewRedis: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	return b
}

// redisURL returns ST_TEST_REDIS_URL when CI has set it, and otherwise a fresh miniredis.
// The test bodies are identical either way, which is the point: miniredis keeps the redis
// backend unit-testable without Docker, and the same suite proves it against a real
// server when one is available.
func redisURL(t *testing.T) string {
	t.Helper()
	if u := os.Getenv("ST_TEST_REDIS_URL"); u != "" {
		return u
	}
	return "redis://" + miniredis.RunT(t).Addr() + "/0"
}

func keyspace(t *testing.T) func(string) string {
	t.Helper()
	var seed [8]byte
	if _, err := rand.Read(seed[:]); err != nil {
		t.Fatalf("rand: %v", err)
	}
	prefix := "sttest:" + hex.EncodeToString(seed[:]) + ":"
	return func(name string) string { return prefix + name }
}

func mustSync(t *testing.T, b Bus, desired ...string) {
	t.Helper()
	if err := b.Sync(t.Context(), desired); err != nil {
		t.Fatalf("Sync(%v): %v", desired, err)
	}
}

func mustPublish(t *testing.T, b Bus, channel, payload string) {
	t.Helper()
	if err := b.Publish(t.Context(), channel, []byte(payload)); err != nil {
		t.Fatalf("Publish(%s): %v", channel, err)
	}
}

// mustReceive asserts the next message off the intake is exactly the one expected.
//
// Asserting on the *next* message is how the negative cases are made deterministic
// without a sleep: a message that must not be delivered is published before one that
// must, and both backends deliver in publish order, so a wrongly-delivered message shows
// up here as the wrong channel rather than as a timeout that only fails sometimes.
func mustReceive(t *testing.T, in <-chan Message, wantChannel, wantPayload string) {
	t.Helper()
	select {
	case msg, ok := <-in:
		if !ok {
			t.Fatalf("Receive closed, want a message on %s", wantChannel)
		}
		if msg.Channel != wantChannel || string(msg.Payload) != wantPayload {
			t.Fatalf("got %s %q, want %s %q", msg.Channel, msg.Payload, wantChannel, wantPayload)
		}
	case <-time.After(receiveTimeout):
		t.Fatalf("no message on %s within %s", wantChannel, receiveTimeout)
	}
}

type conformanceCase struct {
	name   string
	intake int
	run    func(t *testing.T, h harness)
}

// conformanceCases is the contract every Bus implementation must satisfy.
//
// It is one table run against every backend so that memory and redis cannot drift: a case
// added here is a case both must pass, and there is no way to satisfy one and quietly
// skip the other.
var conformanceCases = []conformanceCase{
	{
		name: "publish reaches a subscribed channel FR-12",
		run: func(t *testing.T, h harness) {
			ch := h.key("a")
			mustSync(t, h.bus, ch)
			mustPublish(t, h.bus, ch, "one")
			mustReceive(t, h.bus.Receive(), ch, "one")
		},
	},
	{
		name: "publish to an unsubscribed channel delivers nothing",
		run: func(t *testing.T, h harness) {
			held, loose := h.key("held"), h.key("loose")
			mustSync(t, h.bus, held)
			mustPublish(t, h.bus, loose, "must not arrive")
			mustPublish(t, h.bus, held, "must arrive")
			mustReceive(t, h.bus.Receive(), held, "must arrive")
		},
	},
	{
		name: "receive returns the same channel every call",
		run: func(t *testing.T, h harness) {
			// A caller that took a different channel on each call would have every
			// dispatch worker draining a different queue, and only one of them would
			// ever see a message.
			first, second := h.bus.Receive(), h.bus.Receive()
			if first != second {
				t.Fatal("Receive returned two different channels")
			}
		},
	},
	{
		name: "sync is idempotent S2",
		run: func(t *testing.T, h harness) {
			a, b := h.key("a"), h.key("b")
			mustSync(t, h.bus, a, b)
			mustSync(t, h.bus, a, b)
			mustSync(t, h.bus, b, a)
			if got := health(t, h.bus).Subscriptions; got != 2 {
				t.Fatalf("Subscriptions = %d after three identical syncs, want 2", got)
			}
			mustPublish(t, h.bus, a, "still here")
			mustReceive(t, h.bus.Receive(), a, "still here")
		},
	},
	{
		name: "sync deduplicates the desired set",
		run: func(t *testing.T, h harness) {
			a := h.key("a")
			mustSync(t, h.bus, a, a, a)
			if got := health(t, h.bus).Subscriptions; got != 1 {
				t.Fatalf("Subscriptions = %d, want 1", got)
			}
			mustPublish(t, h.bus, a, "once")
			mustReceive(t, h.bus.Receive(), a, "once")
		},
	},
	{
		name: "sync from A to B and back to A",
		run: func(t *testing.T, h harness) {
			a, b := h.key("a"), h.key("b")
			mustSync(t, h.bus, a)
			mustSync(t, h.bus, b)
			mustPublish(t, h.bus, a, "a is gone")
			mustPublish(t, h.bus, b, "b is held")
			mustReceive(t, h.bus.Receive(), b, "b is held")

			mustSync(t, h.bus, a)
			mustPublish(t, h.bus, b, "b is gone")
			mustPublish(t, h.bus, a, "a is back")
			mustReceive(t, h.bus.Receive(), a, "a is back")
		},
	},
	{
		name: "empty desired removes every subscription",
		run: func(t *testing.T, h harness) {
			a := h.key("a")
			mustSync(t, h.bus, a)
			mustSync(t, h.bus)
			if got := health(t, h.bus).Subscriptions; got != 0 {
				t.Fatalf("Subscriptions = %d after an empty sync, want 0", got)
			}
			mustPublish(t, h.bus, a, "dropped")
			mustSync(t, h.bus, a)
			mustPublish(t, h.bus, a, "delivered")
			mustReceive(t, h.bus.Receive(), a, "delivered")
		},
	},
	{
		name: "sync does not retain the caller's slice",
		run: func(t *testing.T, h harness) {
			held, loose := h.key("held"), h.key("loose")
			desired := []string{held}
			mustSync(t, h.bus, desired...)
			// The reconciler reuses its backing array on the next pass
			// (docs/09-internals.md §4.2), so retaining it silently changes the
			// subscription set behind the bus's back.
			desired[0] = loose
			mustPublish(t, h.bus, loose, "must not arrive")
			mustPublish(t, h.bus, held, "must arrive")
			mustReceive(t, h.bus.Receive(), held, "must arrive")
		},
	},
	{
		name: "payload ownership passes to the receiver",
		run: func(t *testing.T, h harness) {
			ch := h.key("a")
			mustSync(t, h.bus, ch)
			payload := []byte("original")
			if err := h.bus.Publish(t.Context(), ch, payload); err != nil {
				t.Fatalf("Publish: %v", err)
			}
			// Redis copies through the socket whatever the caller does next. Memory must
			// copy too, or a caller reusing its buffer races on one backend and not the
			// other — the worst possible way to find out the two are not equivalent.
			copy(payload, "mutated!")
			mustReceive(t, h.bus.Receive(), ch, "original")
		},
	},
	{
		name: "publish to nobody is not an error",
		run: func(t *testing.T, h harness) {
			if err := h.bus.Publish(t.Context(), h.key("nobody"), []byte("x")); err != nil {
				t.Fatalf("Publish to an unsubscribed channel: %v", err)
			}
		},
	},
	{
		name: "close is idempotent and closes receive",
		run: func(t *testing.T, h harness) {
			if err := h.bus.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			if err := h.bus.Close(); err != nil {
				t.Fatalf("second Close: %v", err)
			}
			select {
			case _, ok := <-h.bus.Receive():
				if ok {
					t.Fatal("Receive delivered a message after Close")
				}
			case <-time.After(receiveTimeout):
				t.Fatalf("Receive still open %s after Close", receiveTimeout)
			}
		},
	},
	{
		name: "sync and publish after close return ErrClosed",
		run: func(t *testing.T, h harness) {
			if err := h.bus.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			if err := h.bus.Sync(t.Context(), []string{h.key("a")}); !errors.Is(err, ErrClosed) {
				t.Fatalf("Sync after Close = %v, want ErrClosed", err)
			}
			if err := h.bus.Publish(t.Context(), h.key("a"), []byte("x")); !errors.Is(err, ErrClosed) {
				t.Fatalf("Publish after Close = %v, want ErrClosed", err)
			}
		},
	},
	{
		name: "sync and publish honour a cancelled context",
		run: func(t *testing.T, h harness) {
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			if err := h.bus.Sync(ctx, []string{h.key("a")}); !errors.Is(err, context.Canceled) {
				t.Fatalf("Sync with a cancelled context = %v, want context.Canceled", err)
			}
			if err := h.bus.Publish(ctx, h.key("a"), []byte("x")); !errors.Is(err, context.Canceled) {
				t.Fatalf("Publish with a cancelled context = %v, want context.Canceled", err)
			}
		},
	},
	{
		name:   "a full intake drops rather than blocking M8",
		intake: 1,
		run: func(t *testing.T, h harness) {
			ch := h.key("a")
			mustSync(t, h.bus, ch)

			// Nothing consumes during the burst, so the intake is full after one message.
			// A reader that blocked here would stop draining the socket, Redis would
			// evict it for exceeding client-output-buffer-limit pubsub, and the
			// reconnect-resubscribe-fall-behind oscillation of M8 would follow. The
			// overflow is dropped and counted instead (docs/09-internals.md §5).
			const burst = 200
			for i := 0; i < burst; i++ {
				mustPublish(t, h.bus, ch, "burst")
			}

			// A Sync is a fence: it completes only once the transport has confirmed the
			// subscription. Against a real Redis that confirmation genuinely arrives
			// behind every message published before it -- Redis is single-threaded, so a
			// PUBLISH that has returned is already queued to this subscriber's output
			// buffer, ahead of a SUBSCRIBE sent afterwards.
			mustSync(t, h.bus, ch, h.key("fence"))

			// miniredis offers no such ordering. It is a concurrent Go implementation on
			// its own locks, and a PUBLISH can return before the message reaches the
			// subscriber connection, which lets the fence overtake the tail of the burst.
			// The drain below then runs while the reader is still working and finds a
			// message that should have been counted as dropped: "Dropped = 198, want 199"
			// and a leftover "burst" where "after the burst" was expected. Rare under Go
			// 1.26 and about two runs in three under 1.27, purely on scheduling.
			//
			// So wait for the count rather than trusting the fence. This is a wait for a
			// known final value, not a sleep: the intake holds one message and nothing is
			// draining it, so of a burst of 200 exactly 199 must end up dropped. When the
			// counter reaches that, every message has been through read() and the
			// assertions below are exact again.
			deadline := time.Now().Add(receiveTimeout)
			for health(t, h.bus).Dropped < uint64(burst-1) && time.Now().Before(deadline) {
				time.Sleep(time.Millisecond)
			}

			drained := 0
			for draining := true; draining; {
				select {
				case _, ok := <-h.bus.Receive():
					if !ok {
						t.Fatal("Receive closed during a burst")
					}
					drained++
				default:
					draining = false
				}
			}
			if drained != 1 {
				t.Errorf("drained %d messages, want the intake size 1", drained)
			}
			if got, want := health(t, h.bus).Dropped, uint64(burst-1); got != want {
				t.Errorf("Dropped = %d, want %d", got, want)
			}

			// The intake is empty and nothing is in flight, so this is the assertion that
			// matters: the reader never deadlocked and delivery continues.
			mustPublish(t, h.bus, ch, "after the burst")
			mustReceive(t, h.bus.Receive(), ch, "after the burst")
		},
	},
	{
		name: "concurrent sync publish and receive",
		run: func(t *testing.T, h harness) {
			const workers, rounds = 8, 25
			channels := []string{h.key("a"), h.key("b"), h.key("c"), h.key("d")}

			stop := make(chan struct{})
			var drained sync.WaitGroup
			drained.Add(1)
			go func() {
				defer drained.Done()
				for {
					select {
					case _, ok := <-h.bus.Receive():
						if !ok {
							return
						}
					case <-stop:
						return
					}
				}
			}()

			errs := make(chan error, workers*rounds*2)
			var wg sync.WaitGroup
			for w := 0; w < workers; w++ {
				wg.Add(1)
				go func(w int) {
					defer wg.Done()
					for r := 0; r < rounds; r++ {
						desired := channels[:1+(w+r)%len(channels)]
						if err := h.bus.Sync(t.Context(), desired); err != nil {
							errs <- err
						}
						if err := h.bus.Publish(t.Context(), channels[r%len(channels)], []byte("x")); err != nil {
							errs <- err
						}
					}
				}(w)
			}
			wg.Wait()
			close(stop)
			drained.Wait()
			close(errs)
			for err := range errs {
				t.Fatalf("concurrent operation: %v", err)
			}
		},
	},
}

// health reads the health of a bus that must implement HealthReporter. Every
// implementation in this package does; the conformance table depends on it, which is what
// stops one of them shipping without the readiness signal /ready needs.
func health(t *testing.T, b Bus) Health {
	t.Helper()
	r, ok := b.(HealthReporter)
	if !ok {
		t.Fatalf("%T does not implement HealthReporter", b)
	}
	return r.Health()
}

func TestConformance(t *testing.T) {
	for _, be := range backends() {
		t.Run(be.name, func(t *testing.T) {
			for _, tc := range conformanceCases {
				t.Run(tc.name, func(t *testing.T) {
					intake := tc.intake
					if intake == 0 {
						intake = 64
					}
					tc.run(t, be.open(t, intake))
				})
			}
		})
	}
}
