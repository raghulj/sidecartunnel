// Package admin owns the operator HTTP listener on admin.listen: /health, /ready,
// /metrics, /channels and POST /disconnect. docs/04-integration.md §4.
//
// It is a separate listener from the client one, bound to loopback by default, and never
// exposed publicly.
//
// /health is liveness and returns 200 while the process runs. It never consults the bus.
// /ready is readiness and returns 503 once the bus has been down longer than
// bus.ready_grace.
//
// The distinction is load-bearing: a Redis restart makes every replica unready at once,
// and /ready wired to a liveness probe kills every replica simultaneously, drops every
// connection, and converts an eight-second Redis blip into a full application outage as
// the whole fleet re-authorizes together. /health exists precisely so there is something
// correct to point a liveness probe at (docs/13-review-findings.md M20).
//
// What this package must never do:
//
//   - Consult the bus from /health.
//   - Serve /channels or /disconnect when admin.token is empty. Those routes return 404,
//     not 200 and not 401: an accidentally unconfigured admin API should look absent, not
//     permissive (FR-20).
//   - Compare the bearer token with ==. crypto/subtle.ConstantTimeCompare, always.
//   - Invert the lock order. The /channels handler reads connection state out of the hub
//     map, so it takes the hub lock first and the connection lock second, like every
//     other path. Acquiring them in opposite orders in two paths is a deadlock that -race
//     does not detect and that only appears under contention
//     (docs/09-internals.md §4.4).
//   - Claim a cluster-wide view. /channels reports this replica only; scatter-gather
//     across replicas is not built.
package admin
