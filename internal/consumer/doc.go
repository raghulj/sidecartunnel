// Package consumer is the fan-out path's top half: the loop that joins the bus to the
// hub. It drains bus.Receive on bus.dispatch_workers goroutines, hands each message to
// Hub.Dispatch, and routes the reserved control channel to Hub.Control behind an FR-23
// signature check.
//
// It lives under internal/ rather than in cmd/sidecartunnel because it is the only part
// of the delivery path that is not reachable from a test outside package main. It was in
// package main once, and the integration suite had to write its own equivalent of this
// loop to have anything to test — two implementations of one routing rule, of which the
// copy under test was not the copy that shipped (docs/12-roadmap.md §4). There is one
// now, and both the binary and test/integration construct it.
//
// The shape is docs/09-internals.md §5 and nothing else:
//
//   - The bus's own reader goroutine does nothing but drain the transport into the
//     bounded intake channel. Decode and fan-out happen here, on N workers. A single
//     goroutine doing both falls behind during a broadcast burst, is evicted by Redis's
//     client-output-buffer-limit, reconnects, resubscribes and is immediately behind
//     again — a stable oscillation that presents as /ready's bus_reconnects climbing
//     against a perfectly healthy Redis (docs/13-review-findings.md M8).
//   - The control channel is consumed on its own goroutine, so a revocation cannot queue
//     behind the firehose it may exist to stop.
//
// What this package must never do:
//
//   - Apply a control message it has not authenticated. control.secret lives here and in
//     the webhook client, and nowhere else in the process (FR-23, NFR-7).
//   - Log a payload. A dropped envelope is still an application's data; the drop is
//     logged with its channel and its reason, never with its bytes (NFR-7).
//   - Block the bus reader. Every drop is counted and the worker continues; a message the
//     hub refuses ends here.
package consumer
