// Package conn owns one client connection: its reader goroutine, its writer goroutine,
// its grant set, its subscription set, and its lifecycle.
//
// Exactly two goroutines per connection, plus whatever the runtime uses for the socket
// (docs/09-internals.md §3):
//
//   - reader — a blocking read loop. It parses one frame and handles the command inline,
//     because grant matching is in-memory and cheap enough that a queue would only add
//     latency, then writes any reply to the outbound queue. It exits on read error or
//     close.
//   - writer — drains the outbound queue, writes to the socket, and sends protocol pings
//     on a ticker. It is the only goroutine that ever writes to the socket, which is what
//     makes concurrent publishes safe without a write mutex.
//
// Everything else — fan-out, control commands, expiry, drain — reaches a connection by
// appending to its outbound queue. Nothing outside the writer writes to the socket. Ever.
//
// This package declares Sink, which is structurally identical to hub.Sink, and Registry,
// which is what it needs from the hub. It imports internal/hub for nothing at all: the
// two packages are built and tested apart, which is what docs/09-internals.md §1 means by
// a downward-only dependency direction, and *Conn satisfies both interfaces without
// either package naming the other.
//
// A Conn holds no mutex. Its mutable state is three atomics — the grant set, the user id
// and the closed flag — plus its channels, and the subscription set lives in the Registry
// rather than here. The lock-ordering rule in docs/09-internals.md §4.4 and
// docs/13-review-findings.md M3 is therefore satisfied by construction: there is no
// connection lock to acquire out of order, and the only lock a Conn is ever under is the
// Registry's, taken inside the Registry methods it calls.
//
// What this package must never do:
//
//   - Take a mutex on the grant-match path. Grants are an atomic pointer to an immutable
//     glob.Set, swapped and never mutated (FR-9, docs/13-review-findings.md M2).
//   - Mutate its subscription set outside the hub lock. The set and the hub map are two
//     views of one fact and must move together, or a connection stays resident in the hub
//     map after close and fan-out writes to a dead connection forever
//     (docs/09-internals.md §4.2).
//   - Block when the outbound queue is full. The send is non-blocking; a full queue means
//     the connection is closed with proto.CloseSlowConsumer, by the closer goroutine,
//     never inline on the fan-out path (FR-15, docs/09-internals.md §4.3).
//   - Add a write mutex on the socket. The single writer goroutine is the design; a mutex
//     means two writers exist and one of them will eventually interleave a partial frame
//     (docs/09-internals.md §9).
//   - Retain the cookie past the connect call. The gateway holds no cookie once the
//     webhook has answered, which is what keeps a memory dump of the process from
//     yielding a set of live sessions and what makes re-handshake survive session
//     rotation (FR-22, docs/13-review-findings.md S3).
//   - Log a cookie, an Authorization header, or a payload, at any level (NFR-7).
package conn
