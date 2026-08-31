package webhook

import (
	"errors"
	"fmt"
	"time"

	"github.com/raghulj/sidecartunnel/internal/glob"
	"github.com/raghulj/sidecartunnel/internal/proto"
)

// Result is the outcome of one connect-webhook call. It is one of exactly three concrete
// types — Authorized, Refused, Unavailable — and callers switch on it.
//
// It is an interface with an unexported marker method rather than a struct with a boolean
// or a status code, because FR-6's distinction is the most consequential decision in this
// package and it must not be expressible as a mistake. A 401 means *this user may not
// connect* and the client must stop asking. A 500 means *I could not tell you right now*
// and the client must come back. Collapsing them either locks every user out during an
// application deploy, or turns a revocation into an infinite retry loop against an
// endpoint that is already failing (docs/04-integration.md §1.3).
//
// A bool cannot carry that. A type switch that has forgotten a case does not compile into
// silence: it falls into the caller's default, which is a visible bug rather than a user
// permanently locked out. Client.Call never returns a nil Result, and never returns an
// error alongside one — a caller that has to check two things can get the distinction
// wrong in two places.
type Result interface {
	// result is unexported so that Result is sealed: no package outside this one can add
	// a fourth case to a caller's type switch.
	result()
}

// Authorized is a 2xx whose body parsed and whose grants compiled. It is the only Result
// that lets a connection proceed.
//
// It holds no cookie. FR-22 and docs/13-review-findings.md C3: the gateway must not
// retain the client's cookie beyond the connect call, so a memory dump of the process
// does not yield a set of live sessions, and so a browser that rotates its session
// (Django SESSION_SAVE_EVERY_REQUEST, Rails, any app calling cycle_key()) is not
// re-validated against a cookie it has already replaced. Grants expire by re-handshake.
type Authorized struct {
	// User is the application's opaque identifier, used for revocation targeting and
	// stamped on client events. The gateway never parses it
	// (docs/04-integration.md §1.2).
	User string

	// Grants is the compiled grant set. Compiling here rather than at first subscribe is
	// deliberate: a malformed grant — one beginning "_", say — is then caught at
	// authorization time and refuses the connection, instead of surfacing minutes later
	// as one subscribe that mysteriously fails (docs/05-authorization.md §3).
	//
	// A Set is immutable and safe to share across goroutines, including from the cache.
	Grants glob.Set

	// ExpiresIn is the clamped lifetime: the application's expires_in bounded to
	// [app.min_expiry, app.max_expiry]. The clamped value is what the client is told,
	// because a client told 24h by an application whose answer the gateway clamped to 6h
	// would schedule its own refresh two close frames too late
	// (docs/04-integration.md §1.2).
	ExpiresIn time.Duration
}

func (Authorized) result() {}

// Refused is a permanent refusal: the application said no, or said something the gateway
// cannot use. The caller closes with proto.CloseUnauthorized (3003) and reconnect false,
// and MUST NOT retry — retrying a refusal turns a revocation into a denial-of-service
// against the application (FR-6).
type Refused struct {
	// Status is the HTTP status that refused: 401 or 403, or the 2xx status of a
	// response whose body could not be used.
	Status int

	// Err is the reason, for logs and for errors.Is. It never contains the cookie, the
	// signature, the secret or the response body (NFR-7).
	Err error
}

func (Refused) result() {}

// CloseCode returns proto.CloseUnauthorized (3003). FR-6.
func (Refused) CloseCode() proto.CloseCode { return proto.CloseUnauthorized }

// Reconnect returns false: a decision was made and retrying cannot change it. FR-6.
func (Refused) Reconnect() bool { return false }

// Error makes a Refused usable as an error, so a caller can log it directly. It names the
// status and the cause and nothing else.
func (r Refused) Error() string {
	if r.Err == nil {
		return fmt.Sprintf("connect webhook refused the connection with status %d", r.Status)
	}
	return fmt.Sprintf("connect webhook refused the connection with status %d: %v", r.Status, r.Err)
}

// Unwrap exposes the cause, so errors.Is reaches sentinels such as ErrMalformedResponse
// and glob.ErrReservedPrefix through the wrap chain (docs/14-coding-standards.md §6).
func (r Refused) Unwrap() error { return r.Err }

// Unavailable is a transient failure: a 5xx, a timeout, a transport error, an exhausted
// authorization budget, or a full connect queue. The caller closes with
// proto.CloseAuthUnavailable (3008), reconnect true, and a spread retry_after.
//
// Overflow of the concurrency queue is deliberately here and not in Refused. The earlier
// design let the handshake timeout fire on queued connections and close them 3001,
// reconnect false, so the mechanism advertised as protecting the application against a
// reconnect storm would have permanently locked out every user caught in one
// (docs/13-review-findings.md C2, NFR-4).
type Unavailable struct {
	// Status is the HTTP status when there was one, and zero for a timeout, a transport
	// error, or an overflow that never reached the network.
	Status int

	// Err is the reason, for logs and for errors.Is. It never contains the cookie, the
	// signature, the secret or the response body (NFR-7).
	Err error
}

func (Unavailable) result() {}

// CloseCode returns proto.CloseAuthUnavailable (3008). FR-6, NFR-4.
func (Unavailable) CloseCode() proto.CloseCode { return proto.CloseAuthUnavailable }

// Reconnect returns true, and the caller attaches a spread retry_after. The spread is the
// caller's to compute: it knows how many connections it is dropping and this package does
// not (docs/03-client-protocol.md §7.1).
func (Unavailable) Reconnect() bool { return true }

// Error makes an Unavailable usable as an error.
func (u Unavailable) Error() string {
	if u.Err == nil {
		return fmt.Sprintf("connect webhook unavailable, status %d", u.Status)
	}
	return fmt.Sprintf("connect webhook unavailable: %v", u.Err)
}

// Unwrap exposes the cause, so errors.Is reaches context.DeadlineExceeded,
// ErrQueueOverflow and the net package's own errors.
func (u Unavailable) Unwrap() error { return u.Err }

// ErrQueueOverflow is the cause on an Unavailable produced because app.connect_queue was
// already full. It is transient by construction: an unbounded queue is 25,000 half-open
// sockets each holding a captured cookie, and a permanent close would lock out exactly
// the users a reconnect storm caught (NFR-4, docs/13-review-findings.md C2).
var ErrQueueOverflow = errors.New("app.connect_queue is full")

// ErrMalformedResponse is the cause on a Refused produced by a 2xx whose body the gateway
// could not use: not JSON, missing user, missing channels, missing expires_in, or a grant
// that does not compile. Permanent, and logged once (docs/04-integration.md §1.3).
//
// It never carries the body. A json.SyntaxError names an offending character and an
// offset, never the document, which is what keeps this wrappable under NFR-7.
var ErrMalformedResponse = errors.New("connect webhook returned an unusable body")
