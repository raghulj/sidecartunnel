// Package proto owns the websocket wire format: the frame structs, the JSON codec,
// and the error and close code registries defined in docs/03-client-protocol.md.
//
// It is the lowest layer in the tree. It imports nothing from this module and knows
// nothing about connections, channels, grants, or configuration — it turns bytes into
// frames and frames into bytes, and that is the whole of its job.
//
// What this package must never do:
//
//   - Decide anything. A frame that decodes cleanly is not an authorized frame. Deciding
//     which command a connection may issue belongs to internal/conn; deciding which
//     channel it may reach belongs to internal/glob and the grant set.
//   - Perform I/O. No sockets, no logging, no metrics. Callers own all three.
//   - Grow a code that docs/03-client-protocol.md does not list. Protocol changes are
//     documentation changes first (docs/AGENTS.md §4); a code that exists here and not
//     there is a bug, in this file.
//   - Mutate a Frame after it has been handed to a Sink. One encoded buffer is shared by
//     every recipient of a fan-out (docs/09-internals.md §5), so a write to it is a data
//     race against every subscriber's writer goroutine at once.
package proto
