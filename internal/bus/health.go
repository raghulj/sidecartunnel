package bus

import "time"

// DefaultIntakeQueue is the depth of the channel between a bus's reader and its dispatch
// workers when none is configured: bus.intake_queue in docs/08-config.md §3.
const DefaultIntakeQueue = 4096

// Health is a point-in-time view of a bus's transport, for /ready and for metrics.
//
// Every implementation in this package reports it, and reading it must never block: it is
// read by the admin listener's /ready handler, and a readiness probe that queues behind a
// 30,000-channel resubscribe reports 503 for the duration of the thing it exists to
// observe (docs/04-integration.md §4).
type Health struct {
	// Connected reports whether the transport is currently usable. The memory bus is
	// always connected; it has no transport to lose.
	Connected bool

	// DisconnectedFor is how long the transport has been down, and zero while connected.
	//
	// The caller compares it against bus.ready_grace rather than failing readiness on the
	// first blip: a Redis restart makes every replica unready at once, and an eight-second
	// restart should be invisible (docs/13-review-findings.md M20).
	DisconnectedFor time.Duration

	// Reconnects counts transports established after the first one, for
	// st_bus_reconnects_total. Climbing against a healthy Redis is the M8 signature —
	// eviction for a slow subscriber, not an unstable server (docs/09-internals.md §5).
	Reconnects uint64

	// Subscriptions is the number of channels currently subscribed upstream, for
	// st_bus_subscriptions_current, whose value FR-10's acceptance criterion names.
	Subscriptions int

	// IntakeDepth is the number of messages queued between the reader and the dispatch
	// workers, for st_bus_intake_depth. Sustained non-zero means the workers are behind
	// the reader.
	IntakeDepth int

	// Dropped counts messages discarded because the intake was full, for
	// st_messages_dropped_total{reason="intake"}. See Receive on each implementation for
	// why a full intake drops rather than blocks.
	Dropped uint64
}

// HealthReporter is a Bus that reports the state of its transport.
//
// It is deliberately not part of Bus: Bus is the frozen delivery surface, and readiness
// is a separate concern that only the admin listener and the metrics collector need. Both
// implementations here satisfy it, so a caller may type-assert without a fallback.
type HealthReporter interface {
	Bus

	// Health returns a snapshot of the transport. It never blocks.
	Health() Health
}
