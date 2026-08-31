package server

import (
	"context"
	"fmt"
	"net/http"
	"sync/atomic"

	"github.com/gorilla/websocket"

	"github.com/raghulj/sidecartunnel/internal/conn"
	"github.com/raghulj/sidecartunnel/internal/proto"
	"github.com/raghulj/sidecartunnel/internal/webhook"
)

// ServeHTTP performs the handshake in the order docs/03-client-protocol.md §2 makes
// normative, and the order is the security model:
//
//  1. Origin against server.allowed_origins. On mismatch, 403 and stop — no application
//     call is made, and there is no websocket on which a close code could be sent.
//  2. The connection count against limits.max_connections, and whether this replica is
//     draining. Over either, 503.
//  3. The upgrade, and only then a connection.
//
// Nothing before step 3 reads the cookie for any purpose other than capturing it for
// forwarding (FR-3).
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	origin := r.Header.Get("Origin")
	if !s.originAllowed(origin) {
		s.stats.originRejected.Add(1)
		// NFR-7: the origin and the peer address, never the cookie. The origin is
		// client-supplied text and is what an operator needs to see to recognise an
		// attack from a misconfiguration.
		s.log.Warn("origin rejected", "origin", origin, "remote", r.RemoteAddr)
		http.Error(w, "origin not allowed", http.StatusForbidden)
		return
	}

	if !s.reserve() {
		s.stats.overCapacity.Add(1)
		http.Error(w, "too many connections", http.StatusServiceUnavailable)
		return
	}
	defer s.release()

	sock, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		// Upgrade has already written its own 400. There is nothing to close and nothing
		// to tell the client that it does not already know.
		s.log.Debug("upgrade failed", "remote", r.RemoteAddr, "err", err)
		return
	}

	s.serve(r, sock)
}

// originAllowed is the whole of FR-2, and docs/05-authorization.md §5 calls it the most
// important twenty lines in the codebase.
//
// Exact string comparison against the configured list. No suffix matching, no wildcards,
// no "ends with .example.com": subdomain wildcards are how an attacker who controls one
// forgotten subdomain gets everything. A map lookup is used precisely because it cannot
// drift into a prefix test the way a hand-written comparison can.
//
// Browsers do not apply CORS to websocket handshakes — there is no preflight and no
// same-origin policy — but they do attach cookies. Without this check a page on
// evil.example opens a socket to the gateway, the browser attaches the victim's session
// cookie, the application answers correctly, and the attacker's page receives that user's
// entire realtime stream.
//
// A missing Origin is a mismatch unless server.allow_missing_origin is set. That key
// exists for non-browser clients, which send none; turning it on removes the defense for
// browsers too, so it belongs with a source-address allowlist at the proxy.
func (s *Server) originAllowed(origin string) bool {
	if origin == "" {
		return s.allowMissingOrigin
	}
	_, ok := s.origins[origin]
	return ok
}

// reserve admits one connection against limits.max_connections, or reports that the
// replica is full or draining.
//
// The count is taken before the upgrade and under the lock, in the same critical section
// as the check: two simultaneous handshakes that both read the count and then both
// increment it are two connections past a cap that exists to keep the replica inside its
// memory budget (NFR-1).
func (s *Server) reserve() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.draining {
		return false
	}
	// 0 is unlimited (docs/08-config.md §3).
	if s.cfg.Limits.MaxConnections > 0 && s.current >= s.cfg.Limits.MaxConnections {
		return false
	}
	s.current++
	return true
}

// release returns the reservation. It runs on every exit path from an admitted handshake,
// including a failed upgrade, or the replica leaks capacity until it refuses everything.
//
// It is also what wakes a waiting Drain: the reservation count is what the drain waits
// on, because it is the only number that covers a handshake from before the upgrade to
// after the connection has unwound (FR-19).
func (s *Server) release() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.current--
	if s.current == 0 && s.drained != nil {
		close(s.drained)
		s.drained = nil
	}
}

// serve builds one connection from an upgraded socket and runs it until it ends. The
// caller's goroutine becomes the connection's reader (docs/09-internals.md §3).
func (s *Server) serve(r *http.Request, sock *websocket.Conn) {
	// FR-3: the cookie is captured here, forwarded verbatim, and never parsed, validated
	// or decrypted. It lives in the single-use connectAuthorizer below and in nothing
	// that outlives the connect call, which is what keeps a memory dump of the process
	// from yielding a set of live sessions (FR-22, docs/13-review-findings.md C3).
	req := webhook.Request{
		Cookie:     r.Header.Get("Cookie"),
		Origin:     r.Header.Get("Origin"),
		UserAgent:  r.Header.Get("User-Agent"),
		RemoteAddr: r.RemoteAddr,
		// FR-24: handed over unwalked. internal/webhook decides what to believe from
		// server.trusted_proxies; doing it here as well would be the same rule in two
		// places, and the one that drifts is the one that forwards a client-supplied
		// 127.0.0.1 into an application's localhost trust path.
		ForwardedFor: r.Header.Get("X-Forwarded-For"),
	}

	// The request goes behind a single-use Authorizer rather than into a closure. A
	// closure keeps what it captured for as long as it is reachable, and the Conn that
	// holds it is reachable for app.expires_in — 6h by default (FR-22).
	authorizer := &connectAuthorizer{srv: s}
	authorizer.req.Store(&req)

	c, err := conn.New(conn.Options{
		ID:         s.newID(),
		Socket:     sock,
		Registry:   newRegistry(s.ctx, s.hub, s.rates, s.clock),
		Authorizer: authorizer,
		Clock:      s.clock,
		Log:        s.log,

		// FR-4 and C2 in two adjacent lines: the handshake budget covers receipt of the
		// connect frame, and the authorization that follows has its own, longer one.
		// Conflating them turns a slow application into a permanent, non-retryable
		// lockout of every reconnecting user.
		HandshakeTimeout: s.cfg.Server.HandshakeTimeout.Duration(),
		ConnectTimeout:   s.cfg.App.ConnectTimeout.Duration(),

		PingInterval:     s.cfg.Server.PingInterval.Duration(),
		PongTimeout:      s.cfg.Server.PongTimeout.Duration(),
		RetrySpread:      s.drainSpread(),
		OutboundQueue:    s.cfg.Limits.OutboundQueue,
		MaxFrameSize:     s.cfg.Limits.MaxFrameSize,
		MaxChannelLength: s.cfg.Limits.MaxChannelLength,
		MaxSubscriptions: s.cfg.Limits.MaxSubscriptionsPerConn,
	})
	if err != nil {
		// coverage: conn.New fails only on a nil Socket, Registry or Authorizer, all of
		// which are set unconditionally three lines above, or on a crypto/rand read that
		// cannot fail. The branch stays because building a connection and ignoring the
		// error would leave a socket nobody closes.
		s.log.Error("build connection", "remote", r.RemoteAddr, "err", err)
		s.closeSocket(sock)
		return
	}
	// The connection is not known when the Authorizer is built, and cannot be: the
	// authorizer is an option of conn.New. It is written here and read on the reader
	// goroutine, which is this one (docs/09-internals.md §3).
	authorizer.conn = c

	// Registered before the connect frame arrives, so a control disconnect can reach this
	// connection from the moment it exists rather than from the moment it subscribes
	// (FR-18). It is also where the duplicate-id refusal is answerable: Attach cannot
	// report one, because there is no close code for "the gateway assigned an id twice".
	if err := s.hub.Add(c); err != nil {
		s.log.Error("register connection", "client", c.ID(), "err", err)
		s.closeSocket(sock)
		return
	}

	if s.seams.afterReserve != nil {
		s.seams.afterReserve()
	}

	accepted := s.track(c)
	defer s.untrack(c)

	if !accepted {
		// Admitted between reserve() and here, which is after Drain fixed its snapshot:
		// this connection is in no set the drain will ever look at again, so it closes
		// itself with exactly what the drain would have sent. Without this the client
		// gets a bare 1006 when the drain budget runs out and reconnects with no
		// retry_after, which is the stampede FR-19 exists to prevent
		// (docs/03-client-protocol.md §7.1).
		c.Close(proto.CloseDraining, drainReason)
	}

	// Run returns once both of the connection's goroutines have, so the deferred release
	// below — and with it the drain's wait — happens after this connection is gone.
	c.Run(s.ctx)
}

// closeSocket ends a socket that never became a connection. There is no close code: the
// client falls back to the docs/03-client-protocol.md §7 table, where a bare 1006 is
// retryable — which is correct, because both paths that reach here are gateway-side and
// a second attempt gets a fresh client id.
func (s *Server) closeSocket(sock *websocket.Conn) {
	if err := sock.Close(); err != nil {
		// coverage: closing an already-broken socket; there is nobody left to tell.
		s.log.Debug("close socket", "err", err)
	}
}

// track records a live connection for the drain and for the per-user cap, and reports
// whether this replica is still accepting.
//
// The report is not advisory. Drain snapshots s.conns once and never looks again, so a
// connection tracked after that snapshot would never be told to close: the drain would
// wait out the whole of server.drain_timeout and the client would get a bare 1006. The
// check is in the same critical section as the insert because a check outside it is the
// same race one lock later (FR-19).
func (s *Server) track(c *conn.Conn) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.conns[c] = ""
	return !s.draining
}

// untrack forgets a connection and returns its slot in the per-user count.
//
// The count must follow the connections, or a user who reconnects
// limits.max_connections_per_user times is locked out of their own account for the life
// of the process.
func (s *Server) untrack(c *conn.Conn) {
	s.mu.Lock()
	if user, ok := s.conns[c]; ok {
		delete(s.conns, c)
		if user != "" {
			s.users[user]--
			if s.users[user] <= 0 {
				delete(s.users, user)
			}
		}
	}
	s.mu.Unlock()
}

// admitUser applies limits.max_connections_per_user and binds the connection to its user.
//
// The check happens here rather than at the upgrade because the gateway does not know who
// is connecting until the application has said so, and it is under the same lock as the
// increment because a check outside it is a race two simultaneous connects both pass.
func (s *Server) admitUser(c *conn.Conn, user string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if limit := s.cfg.Limits.MaxConnectionsPerUser; limit > 0 && s.users[user] >= limit {
		return false
	}
	s.conns[c] = user
	s.users[user]++
	return true
}

// connectAuthorizer is the conn.Authorizer one connection is built with: it holds that
// handshake's webhook.Request until the connect frame uses it, and not one instant
// longer.
//
// It is a type rather than the closure it used to be, and that is the whole of FR-22 on
// this side of the seam. A closure holds what it captured for as long as it is reachable,
// and the Conn holding it is reachable until app.expires_in — 6h by default — so 20,000
// connections meant 20,000 session cookies sitting in the heap, replayable by anyone who
// obtained a core dump. Here Authorize swaps the request out before it makes the call, so
// the value is unreachable from any live object the moment authorization returns, whoever
// still holds the Authorizer (docs/13-review-findings.md S3).
//
// It is used exactly once. A Conn drops its reference to the Authorizer as it takes it,
// so the refusal below is not the connection's ordinary duplicate-connect path — it is
// the guarantee holding for a caller that kept one.
type connectAuthorizer struct {
	srv *Server

	// conn is the connection being authorized. It is written by the handler goroutine
	// before the connection runs and read by the reader goroutine, which is that same
	// goroutine (docs/09-internals.md §3).
	conn *conn.Conn

	// req is the handshake's request, cookie and all. Swapped for nil by the first
	// Authorize, which is what makes the retention structural rather than a convention
	// somebody has to keep (FR-22).
	req atomic.Pointer[webhook.Request]
}

// Authorize calls the application with this handshake's request and the channels the
// connect frame asked for, and releases the request before the call.
//
// requested is a hint, bounded by the connection and never authority
// (docs/04-integration.md §1.1). A second call returns a failure rather than a refusal:
// nothing about the client has been decided, so it closes with proto.CloseAuthUnavailable
// and reconnect true (FR-6).
func (a *connectAuthorizer) Authorize(ctx context.Context, requested []string) (conn.Authorization, error) {
	req := a.req.Swap(nil)
	if req == nil {
		return conn.Authorization{}, fmt.Errorf("server: the connect authorizer is single use and has already answered (FR-22)")
	}
	return a.srv.authorize(ctx, a.conn, *req, requested)
}

// authorize turns one connection's cookie and its requested channels into the gateway's
// answer, by asking the application and switching on the sealed Result type.
//
// FR-6 is the whole of it, and the two outcomes must never share a close code. A refusal
// is a decision: the client stops, and retrying cannot change the answer. A failure is
// the gateway saying it could not tell right now: every replica will answer the same, so
// the client must come back later with a retry_after rather than hammering an application
// that is already unwell.
func (s *Server) authorize(ctx context.Context, c *conn.Conn, req webhook.Request, requested []string) (conn.Authorization, error) {
	// req is a copy: the closure in serve holds its own, and writing here would otherwise
	// carry one connection's requested channels into the next call on the same closure.
	req.Client = c.ID()

	// docs/04-integration.md §1.1: a hint, and only that. The application answers with
	// the grants and everything downstream matches against those, so a channel named here
	// confers nothing. It arrives bounded by the connection —
	// limits.max_subscriptions_per_conn entries of at most limits.max_channel_length
	// bytes — because it is untrusted client input entering an outbound request (NFR-4).
	req.ChannelsRequested = requested

	switch res := s.webhook.Call(ctx, req).(type) {
	case webhook.Authorized:
		if !s.admitUser(c, res.User) {
			s.stats.userLimited.Add(1)
			s.log.Warn("connection refused: too many connections for this user",
				"client", c.ID(), "limit", s.cfg.Limits.MaxConnectionsPerUser)
			// A decision rather than a failure: the cap exists so that one looping client
			// cannot consume the global limit, and a retryable close is an invitation to
			// exactly that loop (docs/13-review-findings.md m8).
			return conn.Authorization{}, fmt.Errorf("server: limits.max_connections_per_user reached: %w", conn.ErrUnauthorized)
		}
		s.stats.accepted.Add(1)
		return conn.Authorization{
			User: res.User,
			// Already compiled by the webhook client, which is where a grant that does
			// not compile becomes a refusal (docs/05-authorization.md §3).
			Grants:    res.Grants,
			ExpiresIn: res.ExpiresIn,
		}, nil

	case webhook.Refused:
		s.stats.refused.Add(1)
		// Wrapping conn.ErrUnauthorized is what selects proto.CloseUnauthorized (3003)
		// with reconnect false. res carries the status and the cause and never the body
		// (NFR-7).
		return conn.Authorization{}, fmt.Errorf("server: connect refused: %w: %w", conn.ErrUnauthorized, res)

	case webhook.Unavailable:
		s.stats.unavailable.Add(1)
		return conn.Authorization{}, fmt.Errorf("server: connect webhook unavailable: %w", res)

	default:
		// coverage: webhook.Result is sealed by an unexported marker method, so no
		// package outside internal/webhook can add a fourth case and reach this. It stays
		// because falling out of the switch would return a nil error on the connect path,
		// which is a connection admitted with no grants and no user.
		s.stats.unavailable.Add(1)
		return conn.Authorization{}, fmt.Errorf("server: the connect webhook returned an unusable result")
	}
}
