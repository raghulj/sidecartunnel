// Package hub owns the channel registry and local fan-out: which connections hold which
// channels, the refcount that drives upstream subscription, and the loop that writes one
// encoded frame to every subscriber.
//
// It does not import internal/conn. It holds Sink, an interface small enough that the hub
// and the connection can be built and tested independently, and that is deliberate — the
// dependency direction in docs/09-internals.md §1 is downward only.
//
// The concurrency rules in docs/09-internals.md §4 are binding here more than anywhere
// else in the tree, because their opposites produce bugs that pass code review:
//
//   - One sync.RWMutex, not shards, until a profile shows lock contention above 5% of
//     fan-out time (NFR-9). Fanning out to 10,000 connections is ~10,000 map iterations,
//     roughly 0.2 ms under the read lock, against subscribes that are rare by comparison.
//   - The map is keyed by the bus key ({bus.prefix}{channel}), never the bare channel
//     name (FR-21).
//   - Never hold the lock across network I/O, and never block to schedule bus work.
//     Subscribe and unsubscribe update a desired set and set a dirty flag; a reconciler
//     goroutine calls Bus.Sync.
//   - A connection's own subscription set is mutated only under the hub lock, in the same
//     critical section as the hub map. Any path that touches one without the other leaves
//     a connection resident in the map after close, so fan-out writes to a dead
//     connection forever and the refcount never reaches zero.
//   - The reply to a subscription change is queued in that same critical section. Attach,
//     Subscribe and Unsubscribe take the caller's pre-encoded frame and hand it to the
//     connection before releasing the lock, which is what makes the ordering rule in
//     docs/03-client-protocol.md §5.1 free rather than something to re-check: no push for
//     a channel before its subscribe reply, none after its unsubscribe reply (M15).
//   - Lock order is hub, then conn. Never the reverse, and neither is ever held while
//     sending on a channel.
//   - Fan-out takes the read lock, sends non-blockingly, collects the connections whose
//     queues were full, releases the lock, and only then hands them to the closer
//     goroutine. Closing needs the write lock, so closing inline under the read lock
//     deadlocks.
//
// What this package must never do:
//
//   - Block on a connection's outbound queue. A slow client is closed, never waited on
//     (FR-15).
//   - Close a connection on the dispatch goroutine.
//   - Re-check authorization at delivery time. A subscription that exists is delivered
//     to; the decision was made when it was created (docs/05-authorization.md §4).
//   - Grow a second lock for the user index. Revocation by user needs an index, but it is
//     a second map under the same lock — a separate one is a contention point on every
//     connect and disconnect (docs/09-internals.md §9).
package hub
