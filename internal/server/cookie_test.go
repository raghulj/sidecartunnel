package server

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"unsafe"

	"github.com/raghulj/sidecartunnel/internal/conn"
	"github.com/raghulj/sidecartunnel/internal/webhook"
)

// testCookie is the value every rig sends. It is distinctive on purpose: the assertion
// below is a substring search over every string the gateway can still reach.
const testCookie = "session=secret-cookie-value"

// TestConnect_RetainsNoCookie_FR22 is the retention test where the retention was.
//
// internal/webhook has had TestCall_RetainsNoCookie_FR22 since the beginning and it never
// failed, because the cookie was not being retained there: Server.serve built a
// webhook.Request holding the raw Cookie header and captured it in the closure it handed
// conn.New as the Authorizer. A Conn keeps that closure, and a Conn lives until
// app.expires_in — 6h by default. At 20,000 connections holding a 1–4 KB session cookie
// each, a core dump of the process yielded 20,000 live, replayable sessions. That is
// exactly what S3 restructured expiry to prevent, and FR-22 says it in one line: the
// gateway MUST NOT retain the client's cookie beyond the connect call.
//
// So this asserts the property rather than the code: after a completed connect, with the
// connection still live, nothing reachable from it is an Authorizer and no string it can
// reach is the cookie. Reading the source cannot establish that, and neither can a test
// in the package that never held the value.
func TestConnect_RetainsNoCookie_FR22(t *testing.T) {
	r := newRig(t)
	c := r.dial()
	reply := c.connect("room-1")
	if reply.Client == "" {
		t.Fatal("the connect reply carries no client id")
	}

	// The webhook was called with the cookie: without this the test could pass on a
	// gateway that never captured one at all, which is a different program.
	if got := r.web.request(t, 0).Cookie; got != testCookie {
		t.Fatalf("the connect webhook received cookie %q, want %q", got, testCookie)
	}

	live := r.live(t)
	found := walkLiveObjects(t, live)

	for _, s := range found.strings {
		if strings.Contains(s, "secret-cookie-value") {
			t.Fatalf("FR-22: the live connection can still reach the cookie, in %q", s)
		}
	}
	if found.authorizers > 0 {
		t.Fatalf("FR-22: the live connection still holds %d conn.Authorizer(s); each one closes over the Cookie header",
			found.authorizers)
	}
	if found.objects < 10 {
		t.Fatalf("the walk reached only %d objects; it is not inspecting the connection", found.objects)
	}
}

// TestConnectAuthorizer_IsSingleUse_FR22 is the other half of the guarantee, and the
// reason the Authorizer is a type rather than a closure: the request is swapped out
// before the call, so a second call has nothing to send and the cookie is unreachable
// even from an Authorizer somebody kept.
func TestConnectAuthorizer_IsSingleUse_FR22(t *testing.T) {
	r := newRig(t)
	r.dial().connect()
	before := r.web.count()

	a := &connectAuthorizer{srv: r.srv, conn: r.live(t)[0]}
	a.req.Store(&webhook.Request{Cookie: testCookie})

	if _, err := a.Authorize(context.Background(), nil); err != nil {
		t.Fatalf("first Authorize: %v", err)
	}
	if got := a.req.Load(); got != nil {
		t.Fatalf("the authorizer still holds the request after the call: %+v (FR-22)", got)
	}

	_, err := a.Authorize(context.Background(), nil)
	if err == nil {
		t.Fatal("the second Authorize succeeded; the connect authorizer is single use")
	}
	if got := r.web.count() - before; got != 1 {
		t.Fatalf("webhook calls = %d, want 1: a second authorization has no request to send", got)
	}
	if strings.Contains(err.Error(), "secret") {
		t.Fatalf("the error quotes the request: %v (NFR-7)", err)
	}
}

// live returns the connections the server is holding right now, under the same lock the
// drain takes.
func (r *rig) live(t *testing.T) []*conn.Conn {
	t.Helper()
	r.srv.mu.Lock()
	defer r.srv.mu.Unlock()
	if len(r.srv.conns) == 0 {
		t.Fatal("the server holds no connection; there is nothing to inspect")
	}
	out := make([]*conn.Conn, 0, len(r.srv.conns))
	for c := range r.srv.conns {
		out = append(out, c)
	}
	return out
}

// findings is what one walk of the object graph saw.
type findings struct {
	strings     []string
	authorizers int
	objects     int
}

// authorizerType is the interface a retained cookie hides behind. An Authorizer is
// per-connection and closes over that request's Cookie header
// (internal/conn/registry.go), so one reachable from a live connection is a retained
// cookie whether or not any string in it survived the walk.
var authorizerType = reflect.TypeOf((*conn.Authorizer)(nil)).Elem()

// walkLiveObjects walks everything reachable from the live connections and reports every
// string it found, how many Authorizers it saw, and how many objects it visited.
//
// It reads unexported fields through reflect.NewAt, which is what makes this an
// inspection of the object graph rather than a reading of the source: a value the code
// keeps is a value this finds, wherever the code chose to put it.
//
// Two rules bound the walk, and both are deliberate:
//
//   - Named types this repository does not own are not entered. The websocket connection
//     and the standard library's buffers are the client's own bytes on their way in and
//     out, and their internals are being written by the reader goroutine while this runs;
//     walking them would race rather than prove anything. Only code in this repository
//     can decide to retain a cookie, so only this repository's types are walked.
//   - The hub is a boundary for the same two reasons at once: it is shared by every
//     connection on the replica, its entire view of one is hub.Sink's four methods — an
//     id, a user, a frame and a close code, none of which can carry a cookie — and its
//     reconciler goroutine is writing the desired set while this runs.
//   - Anything behind a Load method — sync/atomic's wrappers — is read through it, both
//     because reflection cannot see through an unsafe.Pointer and because Load is the
//     race-free way to read a value another goroutine may be writing.
//
// It must run against a quiescent connection: one connection, idle, after its connect
// reply. That is the state the test sets up.
func walkLiveObjects(t *testing.T, conns []*conn.Conn) findings {
	t.Helper()
	w := &walker{seen: map[visited]bool{}}
	for _, c := range conns {
		w.walk(reflect.ValueOf(c))
	}
	return w.found
}

// visited is one already-walked object: a pointer and the type it was reached as.
type visited struct {
	ptr uintptr
	typ reflect.Type
}

type walker struct {
	seen  map[visited]bool
	found findings
}

// modulePrefix is this repository's import path. A named type from anywhere else is a
// boundary the walk stops at.
const modulePrefix = "github.com/raghulj/sidecartunnel/"

// boundaries are types inside this repository that the walk still stops at, each with the
// reason it cannot be hiding a cookie. A list of exemptions in a test is the same bargain
// as a "// coverage:" comment: it lives next to the assertion, where it can be argued
// with (docs/14-coding-standards.md §3).
var boundaries = map[string]string{
	"*hub.Hub": "shared by every connection; its whole view of one is hub.Sink's four " +
		"methods, and its reconciler is writing the desired set while this runs",
	"*server.fakeClock": "this test's own clock, whose alarm list the writer goroutine " +
		"is appending to under its own mutex; the production clock is stateless",
	"*server.stubWebhook": "the application's side of the wire. This double keeps every " +
		"request it was called with, cookie and all, so that FR-2 can assert a call was " +
		"never made — finding a cookie in it would be finding the test's own copy",
}

func (w *walker) walk(v reflect.Value) {
	if !v.IsValid() {
		return
	}
	w.found.objects++

	// atomic.Pointer, atomic.Value, atomic.Bool and friends keep their state in an
	// unsafe.Pointer or an interface reflection must not read directly.
	if v.CanAddr() {
		if load := v.Addr().MethodByName("Load"); load.IsValid() && load.Type().NumIn() == 0 && load.Type().NumOut() == 1 {
			w.walk(load.Call(nil)[0])
			return
		}
	}

	if _, stop := boundaries[v.Type().String()]; stop {
		return
	}

	switch v.Kind() {
	case reflect.String:
		w.found.strings = append(w.found.strings, v.String())

	case reflect.Interface:
		if v.IsNil() {
			return
		}
		w.count(v)
		w.walk(v.Elem())

	case reflect.Pointer:
		if v.IsNil() || w.repeat(v) {
			return
		}
		w.count(v)
		w.walk(v.Elem())

	case reflect.Struct:
		if !ours(v.Type()) {
			return
		}
		for i := range v.NumField() {
			w.walk(readable(v.Field(i)))
		}

	case reflect.Slice:
		if v.IsNil() || w.repeat(v) {
			return
		}
		for i := range v.Len() {
			w.walk(v.Index(i))
		}

	case reflect.Array:
		for i := range v.Len() {
			w.walk(v.Index(i))
		}

	case reflect.Map:
		if v.IsNil() || w.repeat(v) {
			return
		}
		iter := v.MapRange()
		for iter.Next() {
			w.walk(iter.Key())
			w.walk(iter.Value())
		}

	case reflect.Func:
		w.count(v)

	default:
		// Numbers, channels and unsafe pointers hold no string this test can read.
	}
}

// count records a value that satisfies conn.Authorizer.
func (w *walker) count(v reflect.Value) {
	if v.Type().Implements(authorizerType) && !v.IsNil() {
		w.found.authorizers++
	}
}

// repeat reports whether this pointer has been walked already, and records it otherwise.
// Without it the hub's maps walk back into every connection, forever.
func (w *walker) repeat(v reflect.Value) bool {
	key := visited{ptr: v.Pointer(), typ: v.Type()}
	if w.seen[key] {
		return true
	}
	w.seen[key] = true
	return false
}

// readable returns a field that can be read and called through, including an unexported
// one. Reflection refuses both on a value obtained from an unexported field; NewAt on its
// address returns the same memory without that flag, which is the whole technique this
// test is built on.
func readable(v reflect.Value) reflect.Value {
	if v.CanInterface() || !v.CanAddr() {
		return v
	}
	return reflect.NewAt(v.Type(), unsafe.Pointer(v.UnsafeAddr())).Elem()
}

// ours reports whether a named type belongs to this repository. Anonymous types — a map,
// a slice, a plain struct literal — are walked wherever they came from.
func ours(t reflect.Type) bool {
	path := t.PkgPath()
	return path == "" || strings.HasPrefix(path, modulePrefix)
}
