// Package bus is the message transport between replicas. It knows about strings and
// bytes, and nothing else.
//
// There are two implementations: RedisBus, which is the product, and MemoryBus, which
// exists for tests and single-node development. Starting a multi-replica deployment on
// memory is undetectable by the gateway and produces the worst kind of bug — messages
// that arrive for some users and not others — so whoever constructs it from configuration
// logs a prominent warning at startup every time (docs/08-config.md §3).
//
// Both implementations pass one shared conformance table (conformance_test.go). That is
// deliberate: the memory bus stands in for the redis one in every protocol test, and a
// memory bus that behaves differently is a memory bus that hides bugs until production.
//
// What this package must never do:
//
//   - Know what a channel is. It receives fully-formed bus keys. Prefixing, namespace
//     resolution and the "_" reservation all happen above it. This is what makes FR-21
//     structural rather than a filter someone has to remember to apply.
//   - Parse a payload. Envelopes are decoded by the dispatch workers, not here.
//   - Model subscriptions as events. Sync takes the whole desired set; there is no
//     Subscribe and no Unsubscribe, deliberately. See the Bus interface comment.
//   - Close a client connection when it loses its upstream. Losing the bus MUST NOT close
//     client connections (NFR-8); they stay open and silent until it returns.
package bus
