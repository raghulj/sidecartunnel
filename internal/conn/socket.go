package conn

import (
	"io"
	"time"
)

// Socket is the websocket connection a Conn drives.
//
// It is an interface rather than *websocket.Conn for two reasons. The first is testing:
// every failure the writer has to survive — a write that fails, a deadline that cannot be
// set, a peer that stops reading — is a switch on a fake here instead of a socket
// contorted into failing. The second is that it states the whole surface this package
// touches, which is what makes the single-writer rule checkable by reading rather than by
// hoping (docs/09-internals.md §3).
//
// *websocket.Conn from gorilla/websocket satisfies it as it stands; no adapter is needed.
//
// The concurrency contract is the one gorilla documents and the one the design depends
// on: ReadMessage, SetReadLimit and SetPongHandler are used by the reader goroutine only,
// and SetWriteDeadline, WriteMessage and WriteControl by the writer goroutine only. There
// is no write mutex, because there is only ever one writer (docs/09-internals.md §9).
type Socket interface {
	// NextReader blocks until the next data frame arrives and returns a reader over its
	// payload, along with its websocket message type.
	//
	// It is NextReader rather than ReadMessage, and the read limit is enforced by the
	// caller rather than by Socket.SetReadLimit, for a reason worth recording:
	// gorilla/websocket answers a frame over its read limit by writing a close frame
	// itself, with code 1009, from inside the read call. That is a second goroutine
	// writing to the socket — the one thing docs/09-internals.md §9 forbids — and it puts
	// 1009 on the wire where docs/03-client-protocol.md §7 requires
	// proto.CloseProtocolError. Reading through a bounded reader keeps the memory bound
	// and leaves both the close code and the socket to this package.
	NextReader() (messageType int, r io.Reader, err error)

	// SetPongHandler registers the function called, from inside ReadMessage, when a
	// protocol-level pong arrives (FR-7).
	SetPongHandler(h func(appData string) error)

	// SetWriteDeadline bounds the next write. Without it a peer that stops reading parks
	// the writer goroutine forever, and the connection leaks both goroutines (NFR-3).
	SetWriteDeadline(t time.Time) error

	// WriteMessage writes one data frame.
	WriteMessage(messageType int, data []byte) error

	// WriteControl writes one control frame — a ping or a close. Its payload is capped at
	// 125 bytes by RFC 6455.
	WriteControl(messageType int, data []byte, deadline time.Time) error

	// Close closes the underlying network connection. It is called exactly once, by the
	// writer goroutine, as its last act.
	Close() error
}
