package main

import (
	"sync/atomic"
	"time"

	"github.com/raghulj/sidecartunnel/internal/admin"
	"github.com/raghulj/sidecartunnel/internal/bus"
	"github.com/raghulj/sidecartunnel/internal/hub"
)

// adminRegistry adapts *hub.Hub to admin.Registry.
//
// The adapter lives here, in the process that already built both, and not in either of
// them: internal/admin must not import internal/hub, or the operator listener and the
// registry stop being buildable and testable in parallel, which is the whole reason
// admin.Registry is three methods (docs/09-internals.md §1).
//
// It is stateless and safe for concurrent use; every method delegates straight to the
// hub, which holds the lock.
type adminRegistry struct {
	hub *hub.Hub

	// flush discards every cached webhook answer after a disconnect. POST /disconnect has
	// the same effect as the control action (docs/04-integration.md §4), and that
	// includes the cache flush: a cached entry otherwise survives a revocation
	// (docs/13-review-findings.md C4).
	flush func()
}

// Channels returns every channel this replica holds, with its local subscriber count.
// User ids are left out; the list endpoint drops them anyway.
func (r adminRegistry) Channels() []admin.Channel {
	held := r.hub.Channels()
	out := make([]admin.Channel, 0, len(held))
	for _, occ := range held {
		out = append(out, admin.Channel{Name: occ.Channel, Subscribers: occ.Subscribers})
	}
	return out
}

// Channel returns one channel's subscriber count and the user ids holding it, and reports
// whether this replica holds it at all.
func (r adminRegistry) Channel(name string) (admin.Channel, bool) {
	occ, ok := r.hub.Channel(name)
	if !ok {
		return admin.Channel{}, false
	}
	return admin.Channel{Name: occ.Channel, Subscribers: occ.Subscribers, Users: occ.Users}, true
}

// Disconnect closes every connection matching target and returns how many it closed. A
// target held by no connection on this replica is not an error; it returns zero.
func (r adminRegistry) Disconnect(target admin.Target) (int, error) {
	closed, err := r.hub.Disconnect(target.User, target.Client)
	if err != nil {
		return 0, err
	}
	r.flush()
	return closed, nil
}

// readiness is the bus health admin.Options.Bus sees, with one addition: once the drain
// has begun it reports the transport as gone, so /ready answers 503 immediately.
//
// docs/09-internals.md §8 step 1 requires exactly that — stop accepting upgrades and have
// /ready report 503 at once — and the load balancer needs it well before the sockets
// close, or it keeps steering new connections at a replica that is going away. /health is
// untouched and stays 200 for the life of the process, which is the difference between a
// readiness signal and a liveness one (FR-20, docs/13-review-findings.md M20).
//
// It is safe for concurrent use: the flag is atomic and the underlying Health never
// blocks.
type readiness struct {
	bus      bus.HealthReporter
	draining atomic.Bool
}

// drain makes every subsequent Health report the replica as not ready. It is idempotent.
func (r *readiness) drain() { r.draining.Store(true) }

// Health returns the bus's own snapshot, or a not-ready one once draining has begun.
//
// The synthetic snapshot keeps the bus's real counters and reports the transport as down
// for longer than any configured bus.ready_grace can be — the grace is capped at a
// duration, and a value this large is past every one of them — so the grace cannot delay
// a drain the way it deliberately delays a Redis blip.
func (r *readiness) Health() bus.Health {
	h := r.bus.Health()
	if r.draining.Load() {
		h.Connected = false
		h.DisconnectedFor = drainingFor
	}
	return h
}

// drainingFor is the DisconnectedFor a draining replica reports. It is beyond any
// bus.ready_grace an operator can configure, so /ready is 503 from the first probe after
// the drain begins.
const drainingFor = 100 * 365 * 24 * time.Hour
