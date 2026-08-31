package conn

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/raghulj/sidecartunnel/internal/proto"
)

// ---------------------------------------------------------------------------
// fake clock
// ---------------------------------------------------------------------------

// fakeClock is a deterministic Clock. Tests move time with Advance and never sleep
// (docs/14-coding-standards.md §2).
type fakeClock struct {
	mu      sync.Mutex
	now     time.Time
	alarms  []*fakeAlarm
	created chan struct{}
}

func newFakeClock() *fakeClock {
	return &fakeClock{
		now:     time.Date(2026, 8, 31, 12, 0, 0, 0, time.UTC),
		created: make(chan struct{}, 256),
	}
}

func (f *fakeClock) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.now
}

func (f *fakeClock) NewTimer(d time.Duration) Alarm  { return f.newAlarm(d, 0) }
func (f *fakeClock) NewTicker(d time.Duration) Alarm { return f.newAlarm(d, d) }

func (f *fakeClock) newAlarm(d, period time.Duration) Alarm {
	f.mu.Lock()
	a := &fakeAlarm{clk: f, c: make(chan time.Time, 1), at: f.now.Add(d), period: period}
	f.alarms = append(f.alarms, a)
	f.mu.Unlock()
	select {
	case f.created <- struct{}{}:
	default:
	}
	return a
}

// Advance moves the clock forward and fires every alarm that is now due.
func (f *fakeClock) Advance(d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.now = f.now.Add(d)
	for _, a := range f.alarms {
		if a.stopped || a.at.After(f.now) {
			continue
		}
		select {
		case a.c <- f.now:
		default:
		}
		if a.period > 0 {
			a.at = f.now.Add(a.period)
			continue
		}
		a.stopped = true
	}
}

// waitAlarms blocks until n alarms have been created, so a test never advances the clock
// past an alarm the code under test has not armed yet. That is the synchronisation a
// sleep would otherwise be standing in for.
func (f *fakeClock) waitAlarms(t *testing.T, n int) {
	t.Helper()
	for i := range n {
		select {
		case <-f.created:
		case <-time.After(failAfter):
			t.Fatalf("only %d of %d alarms were armed", i, n)
		}
	}
}

type fakeAlarm struct {
	clk     *fakeClock
	c       chan time.Time
	at      time.Time
	period  time.Duration
	stopped bool
}

func (a *fakeAlarm) C() <-chan time.Time { return a.c }

func (a *fakeAlarm) Stop() {
	a.clk.mu.Lock()
	defer a.clk.mu.Unlock()
	a.stopped = true
}

// ---------------------------------------------------------------------------
// fake socket
// ---------------------------------------------------------------------------

// socketRead is one item the test feeds to the reader goroutine. A pong item invokes the
// registered pong handler and is not returned from ReadMessage, exactly as gorilla
// dispatches control frames from inside ReadMessage.
type socketRead struct {
	kind    int
	data    []byte
	err     error
	bodyErr error
	pong    bool
}

// body returns the reader for this frame: its bytes, or one that fails partway through,
// which is what a connection reset in the middle of a message looks like to the reader.
func (r socketRead) body() io.Reader {
	if r.bodyErr != nil {
		return errReader{err: r.bodyErr}
	}
	return bytes.NewReader(r.data)
}

// errReader fails on every read.
type errReader struct{ err error }

func (r errReader) Read([]byte) (int, error) { return 0, r.err }

// socketControl is one control frame the writer sent.
type socketControl struct {
	kind int
	data []byte
}

// fakeSocket is an in-memory Socket. Every failure mode the writer has to handle is
// switchable, because docs/14-coding-standards.md §3 requires the error paths to be
// covered rather than skipped.
type fakeSocket struct {
	reads   chan socketRead
	writes  chan []byte
	control chan socketControl
	gate    chan struct{}
	parked  chan struct{}
	closed  chan struct{}

	gated        atomic.Bool
	gateOnce     sync.Once
	closeOnce    sync.Once
	closeCalls   atomic.Int32
	failWrite    atomic.Bool
	failDeadline atomic.Bool
	failControl  atomic.Bool
	failClose    atomic.Bool

	pong atomic.Pointer[func(string) error]
}

func newFakeSocket() *fakeSocket {
	return &fakeSocket{
		reads:   make(chan socketRead, 64),
		writes:  make(chan []byte, 4096),
		control: make(chan socketControl, 64),
		gate:    make(chan struct{}),
		parked:  make(chan struct{}, 64),
		closed:  make(chan struct{}),
	}
}

var errSocketClosed = errors.New("fake socket closed")
var errSocketWrite = errors.New("fake socket write failed")

func (s *fakeSocket) NextReader() (int, io.Reader, error) {
	for {
		select {
		case r := <-s.reads:
			if r.pong {
				if h := s.pong.Load(); h != nil {
					if err := (*h)(""); err != nil {
						return 0, nil, err
					}
				}
				continue
			}
			if r.err != nil {
				return 0, nil, r.err
			}
			return r.kind, r.body(), nil
		case <-s.closed:
			return 0, nil, errSocketClosed
		}
	}
}

func (s *fakeSocket) SetPongHandler(h func(string) error) { s.pong.Store(&h) }

func (s *fakeSocket) SetWriteDeadline(time.Time) error {
	if s.failDeadline.Load() {
		return errSocketWrite
	}
	return nil
}

func (s *fakeSocket) WriteMessage(_ int, data []byte) error {
	// The failure is decided before the gate, so a write already parked in the gate when
	// a test flips the switch still succeeds. That is what lets a test put the failure on
	// a specific later write.
	if s.failWrite.Load() {
		return errSocketWrite
	}
	if s.gated.Load() {
		select {
		case s.parked <- struct{}{}:
		default:
		}
		select {
		case <-s.gate:
		case <-s.closed:
		}
	}
	s.writes <- data
	return nil
}

func (s *fakeSocket) WriteControl(kind int, data []byte, _ time.Time) error {
	if s.failControl.Load() {
		return errSocketWrite
	}
	s.control <- socketControl{kind: kind, data: bytes.Clone(data)}
	return nil
}

func (s *fakeSocket) Close() error {
	s.closeCalls.Add(1)
	s.closeOnce.Do(func() { close(s.closed) })
	if s.failClose.Load() {
		return errSocketWrite
	}
	return nil
}

// feed queues one text frame for the reader.
func (s *fakeSocket) feed(v any) {
	data, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	s.reads <- socketRead{kind: websocket.TextMessage, data: data}
}

// feedRaw queues one text frame verbatim, for the malformed-frame cases.
func (s *fakeSocket) feedRaw(text string) {
	s.reads <- socketRead{kind: websocket.TextMessage, data: []byte(text)}
}

// nextWrite returns the next text frame the writer produced, decoded into a map.
func (s *fakeSocket) nextWrite(t *testing.T) map[string]json.RawMessage {
	t.Helper()
	select {
	case data := <-s.writes:
		var out map[string]json.RawMessage
		if err := json.Unmarshal(data, &out); err != nil {
			t.Fatalf("frame %q is not a JSON object: %v", data, err)
		}
		return out
	case <-time.After(failAfter):
		t.Fatal("no frame written within the deadline")
		return nil
	}
}

// nextControl returns the next control frame the writer produced.
func (s *fakeSocket) nextControl(t *testing.T) socketControl {
	t.Helper()
	select {
	case c := <-s.control:
		return c
	case <-time.After(failAfter):
		t.Fatal("no control frame within the deadline")
		return socketControl{}
	}
}

// closeCode returns the websocket close code of the next close control frame, skipping
// pings. It is what a client that never received a disconnect frame would fall back to
// (docs/03-client-protocol.md §5.2).
func (s *fakeSocket) closeCode(t *testing.T) proto.CloseCode {
	t.Helper()
	for {
		c := s.nextControl(t)
		if c.kind != websocket.CloseMessage {
			continue
		}
		if len(c.data) < 2 {
			t.Fatalf("close frame payload %q is shorter than a close code", c.data)
		}
		return proto.CloseCode(binary.BigEndian.Uint16(c.data[:2]))
	}
}

// ---------------------------------------------------------------------------
// fake registry
// ---------------------------------------------------------------------------

// fakeRegistry stands in for internal/hub. It keeps the subscription bookkeeping the real
// hub owns, and its fanout method reproduces docs/09-internals.md §4.3 and §4.5: a
// non-blocking send under the lock, slow sinks collected, and only closed after the lock
// is released.
type fakeRegistry struct {
	mu      sync.Mutex
	subs    map[Sink][]string
	members map[Sink]struct{}
	removed []string

	refuse       map[string]bool // channels Attach silently omits (unknown namespace)
	subscribeErr error
	publishErr   error
	published    []publishedEvent
}

type publishedEvent struct {
	channel string
	event   string
	data    string
}

func newFakeRegistry() *fakeRegistry {
	return &fakeRegistry{
		subs:    map[Sink][]string{},
		members: map[Sink]struct{}{},
		refuse:  map[string]bool{},
	}
}

func (r *fakeRegistry) Attach(s Sink, channels []string, ack func([]string) *proto.Frame) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.members[s] = struct{}{}
	granted := []string{}
	for _, ch := range channels {
		if r.refuse[ch] {
			continue
		}
		r.subs[s] = append(r.subs[s], ch)
		granted = append(granted, ch)
	}
	if f := ack(granted); f != nil {
		s.Send(f)
	}
}

func (r *fakeRegistry) Subscribe(s Sink, channel string, ack *proto.Frame) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.subscribeErr != nil {
		return r.subscribeErr
	}
	r.subs[s] = append(r.subs[s], channel)
	s.Send(ack)
	return nil
}

func (r *fakeRegistry) Unsubscribe(s Sink, channel string, ack *proto.Frame) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	held := r.subs[s]
	for i, ch := range held {
		if ch == channel {
			r.subs[s] = append(held[:i:i], held[i+1:]...)
			s.Send(ack)
			return nil
		}
	}
	return &CommandError{Code: proto.ErrNotSubscribed, Message: "not subscribed"}
}

func (r *fakeRegistry) Subscriptions(s Sink) []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string{}, r.subs[s]...)
}

func (r *fakeRegistry) Publish(_ Sink, channel, event string, data json.RawMessage) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.publishErr != nil {
		return r.publishErr
	}
	r.published = append(r.published, publishedEvent{channel: channel, event: event, data: string(data)})
	return nil
}

func (r *fakeRegistry) Remove(s Sink) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.members, s)
	delete(r.subs, s)
	r.removed = append(r.removed, s.ID())
}

// snapshot returns the current members. Callers close them after it returns, never while
// the lock is held: Close deregisters, and deregistering needs the same lock
// (docs/09-internals.md §4.5).
func (r *fakeRegistry) snapshot() []Sink {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]Sink, 0, len(r.members))
	for s := range r.members {
		out = append(out, s)
	}
	return out
}

// fanout delivers one shared frame to every member, exactly as docs/09-internals.md §4.5
// requires: non-blocking sends under the read lock, refusing sinks collected, and closed
// only after the lock is released.
func (r *fakeRegistry) fanout(f *proto.Frame) {
	r.mu.Lock()
	var slow []Sink
	for s := range r.members {
		if !s.Send(f) {
			slow = append(slow, s)
		}
	}
	r.mu.Unlock()
	for _, s := range slow {
		s.Close(proto.CloseSlowConsumer, "slow consumer")
	}
}

// ---------------------------------------------------------------------------
// fake authorizer
// ---------------------------------------------------------------------------

func okAuth(t *testing.T, user string, grants ...string) Authorizer {
	t.Helper()
	set := mustGrants(t, grants...)
	return AuthorizerFunc(func(context.Context) (Authorization, error) {
		return Authorization{User: user, Grants: set, ExpiresIn: time.Hour}, nil
	})
}

// ---------------------------------------------------------------------------
// log capture
// ---------------------------------------------------------------------------

// syncBuffer is a concurrency-safe log sink: both connection goroutines log.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// ---------------------------------------------------------------------------
// rig
// ---------------------------------------------------------------------------

// rig is one connection under test with all four seams faked.
type rig struct {
	t      *testing.T
	conn   *Conn
	sock   *fakeSocket
	reg    *fakeRegistry
	clk    *fakeClock
	logs   *syncBuffer
	done   chan struct{}
	cancel context.CancelFunc
}

// newRig builds a running connection. The returned rig is torn down by t.Cleanup, which
// waits for Run to return — that wait is what makes the goroutine-leak assertions honest.
func newRig(t *testing.T, tweak ...func(*Options)) *rig {
	t.Helper()
	sock := newFakeSocket()
	reg := newFakeRegistry()
	clk := newFakeClock()
	logs := &syncBuffer{}

	opts := Options{
		Socket:     sock,
		Registry:   reg,
		Authorizer: okAuth(t, "u-1", "room-*"),
		Clock:      clk,
		Log:        slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug})),
	}
	for _, fn := range tweak {
		fn(&opts)
	}

	c, err := New(opts)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	r := &rig{t: t, conn: c, sock: sock, reg: reg, clk: clk, logs: logs, done: make(chan struct{}), cancel: cancel}
	go func() {
		defer close(r.done)
		c.Run(ctx)
	}()
	// The writer arms the handshake timer and the ping ticker before it can service
	// anything else; waiting for them removes every startup race from the tests.
	clk.waitAlarms(t, 2)

	t.Cleanup(func() {
		cancel()
		sock.openGate()
		c.Close(proto.CloseDraining, "test over")
		select {
		case <-r.done:
		case <-time.After(failAfter):
			t.Fatal("Run did not return within the deadline")
		}
	})
	return r
}

// blockWrites parks the writer inside WriteMessage until openGate is called. It is how
// the slow-consumer test (FR-15) stops a client reading without a sleep.
func (s *fakeSocket) blockWrites() { s.gated.Store(true) }

// waitParked blocks until the writer has entered a gated WriteMessage, so a test can put
// frames behind it knowing they will queue rather than be written.
func (s *fakeSocket) waitParked(t *testing.T) {
	t.Helper()
	select {
	case <-s.parked:
	case <-time.After(failAfter):
		t.Fatal("the writer never parked in WriteMessage")
	}
}

// openGate releases a writer parked in WriteMessage.
func (s *fakeSocket) openGate() { s.gateOnce.Do(func() { close(s.gate) }) }

// connect performs the handshake and returns the connect reply body.
func (r *rig) connect(subs ...string) proto.ConnectReply {
	r.t.Helper()
	body := map[string]any{}
	if len(subs) > 0 {
		body["subs"] = subs
	}
	r.sock.feed(map[string]any{"id": 1, "connect": body})
	frame := r.sock.nextWrite(r.t)
	raw, ok := frame["connect"]
	if !ok {
		r.t.Fatalf("first frame is not a connect reply: %v", frame)
	}
	var reply proto.ConnectReply
	if err := json.Unmarshal(raw, &reply); err != nil {
		r.t.Fatalf("connect reply: %v", err)
	}
	return reply
}

// wantError asserts the next frame is an error reply with the given code.
func (r *rig) wantError(want proto.ErrCode) proto.Error {
	r.t.Helper()
	frame := r.sock.nextWrite(r.t)
	raw, ok := frame["error"]
	if !ok {
		r.t.Fatalf("frame %v is not an error reply", frame)
	}
	var got proto.Error
	if err := json.Unmarshal(raw, &got); err != nil {
		r.t.Fatalf("error reply: %v", err)
	}
	if got.Code != want {
		r.t.Fatalf("error code = %d, want %d", got.Code, want)
	}
	return got
}

// wantDisconnect asserts the next frame is a disconnect with the given close code and
// returns it, so a test can check reconnect and retry_after (docs/03-client-protocol.md
// §7.1).
func (r *rig) wantDisconnect(want proto.CloseCode) proto.Disconnect {
	r.t.Helper()
	frame := r.sock.nextWrite(r.t)
	raw, ok := frame["disconnect"]
	if !ok {
		r.t.Fatalf("frame %v is not a disconnect", frame)
	}
	var got proto.Disconnect
	if err := json.Unmarshal(raw, &got); err != nil {
		r.t.Fatalf("disconnect: %v", err)
	}
	if got.Code != want {
		r.t.Fatalf("close code = %d, want %d", got.Code, want)
	}
	return got
}
