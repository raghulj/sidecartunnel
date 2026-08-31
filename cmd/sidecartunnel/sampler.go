package main

import (
	"sync"

	"github.com/raghulj/sidecartunnel/internal/admin"
	"github.com/raghulj/sidecartunnel/internal/metrics"
	"github.com/raghulj/sidecartunnel/internal/server"
)

// sampler folds the counters and gauges that live outside internal/metrics into it, at
// scrape time.
//
// It is admin.Options.Refresh. Sampling at the scrape rather than on a ticker makes every
// gauge exact as of the exposition and saves the process a ticker whose period would
// silently become the resolution of all of them. It must not block — it runs on the
// scrape's goroutine — and nothing here does: every source is a lock-free read or one
// uncontended mutex.
//
// The counters are folded as deltas, because the sources own cumulative totals and a
// Prometheus counter only goes up. A source that went backwards is treated as a restarted
// counter and contributes its whole new value, the same rule Metrics.ObserveBus applies to
// the bus's own totals.
type sampler struct {
	metrics *metrics.Metrics
	bus     admin.BusHealth
	webhook interface{ InFlight() int }
	server  interface{ Stats() server.Stats }

	// mu guards last. Two scrapes can arrive at once, and folding a delta twice is a
	// counter that reports double the traffic.
	mu   sync.Mutex
	last server.Stats
}

// refresh samples every source once. Safe to call concurrently.
func (s *sampler) refresh() {
	s.metrics.ObserveBus(s.bus.Health())
	s.metrics.SetWebhookInflight(s.webhook.InFlight())

	now := s.server.Stats()

	s.mu.Lock()
	defer s.mu.Unlock()
	s.fold(metrics.ResultAccepted, now.Accepted, s.last.Accepted)
	s.fold(metrics.ResultUnauthorized, now.Refused, s.last.Refused)
	s.fold(metrics.ResultUnavailable, now.Unavailable, s.last.Unavailable)
	s.fold(metrics.ResultOverLimit, now.OverCapacity+now.UserLimited, s.last.OverCapacity+s.last.UserLimited)

	// st_origin_rejected_total is its own family as well as a result label, because a
	// rejected Origin is somebody trying to hijack a session and a 503 is this replica
	// being full — different incidents, and docs/10-operations.md §5 alerts on them
	// separately (FR-2, NFR-1).
	rejected := delta(now.OriginRejected, s.last.OriginRejected)
	s.fold(metrics.ResultOriginRejected, now.OriginRejected, s.last.OriginRejected)
	for range rejected {
		s.metrics.OriginRejected()
	}

	s.last = now
}

// fold advances one st_connections_total series by the increase since the last sample.
func (s *sampler) fold(result metrics.Result, now, last uint64) {
	for range delta(now, last) {
		s.metrics.ConnectionResult(result)
	}
}

// delta returns the increase from last to now, treating a decrease as a reset and
// contributing the whole new value.
func delta(now, last uint64) uint64 {
	if now < last {
		return now
	}
	return now - last
}
