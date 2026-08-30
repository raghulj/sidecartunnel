package proto

// ErrCode is an application-level error code, returned in an {"error":{…}} reply against
// a command's id. The connection stays open: an ErrCode says the command failed, never
// that the session is over. Registry: docs/03-client-protocol.md §6.
type ErrCode int

// Error codes. This list is exhaustive and closed. Adding one means editing
// docs/03-client-protocol.md §6 in the same commit (docs/AGENTS.md §4).
//
// Two gaps are deliberate and must stay:
//
//   - 107 was "frame too large". It was unreachable — an oversize frame closes the
//     connection with CloseProtocolError, so there is no open connection left to answer
//     on. Removed by docs/13-review-findings.md m2. Do not re-add it.
//   - There is no code for a rejected Origin. That check completes before the upgrade
//     and answers HTTP 403, so no websocket exists on which to send anything.
const (
	// ErrInternal (100) is an unexpected failure inside the gateway. It carries no detail
	// on the wire on purpose: the client can do nothing with it but retry the command.
	ErrInternal ErrCode = 100

	// ErrBadRequest (101) is a malformed frame, an unknown command, a frame carrying
	// zero or several command keys, a second connect on one connection, any command sent
	// before connect, or a channel longer than limits.max_channel_length
	// (docs/07-delivery.md §7).
	ErrBadRequest ErrCode = 101

	// ErrUnknownNamespace (102) is a channel whose namespace has no configured block and
	// for which no default block exists. Failing closed is deliberate: a typo'd namespace
	// must be an error, not a silently permissive channel (docs/06-channels.md §3).
	ErrUnknownNamespace ErrCode = 102

	// ErrPermissionDenied (103) is a channel matching no grant, a channel beginning "_"
	// (reserved, refused before grants are consulted so that a grant of "*" still cannot
	// reach a control channel), or a client publish to a namespace without client_events.
	// FR-5, docs/06-channels.md §4.
	ErrPermissionDenied ErrCode = 103

	// ErrAlreadySubscribed (104) is a subscribe to a channel this connection already
	// holds. Deliberately not idempotent: in practice a duplicate subscribe means the
	// client's own registry has drifted, and hiding that makes reconnect bugs very hard
	// to find (docs/03-client-protocol.md §4.2).
	ErrAlreadySubscribed ErrCode = 104

	// ErrNotSubscribed (105) is an unsubscribe from a channel this connection does not
	// hold. Not idempotent, for the same reason as ErrAlreadySubscribed.
	ErrNotSubscribed ErrCode = 105

	// ErrRateLimited (106) is a client publish over the namespace's rate_limit. Ten of
	// these within 60 seconds closes the connection with CloseRateLimited and a
	// retry_after of 60s (docs/03-client-protocol.md §4.4).
	ErrRateLimited ErrCode = 106

	// ErrSubscriptionLimit (108) is a subscribe past limits.max_subscriptions_per_conn.
	// FR-8's cap needed a code and did not have one until
	// docs/13-review-findings.md M17.
	ErrSubscriptionLimit ErrCode = 108
)

// CloseCode is a websocket close code. The gateway sends it twice: once in a disconnect
// frame, so the client can read a reason and a retry_after, and once as the websocket
// close code itself, for clients that only see the latter. Registry:
// docs/03-client-protocol.md §7.
//
// Codes 3000–3099 are transport-level; 3500+ are authorization decisions. A new code
// stays inside those bands.
type CloseCode int

// Close codes. Exhaustive and closed, like ErrCode.
//
// Three gaps are deliberate:
//
//   - 3002 was "origin rejected". Unreachable: the Origin check completes before the
//     upgrade, so there is no websocket to close. Removed by
//     docs/13-review-findings.md M14.
//   - 3500 was "killed". Unreachable by definition: an ungraceful kill sends nothing.
//     Removed by the same finding.
//   - 3502 never existed in a shipped registry and must not be introduced.
//
// Do not re-add any of them.
const (
	// CloseDraining (3000) means this replica is shutting down. reconnect: true, with a
	// retry_after spread uniformly across server.drain_spread (FR-19).
	//
	// It is distinct from CloseAuthUnavailable on purpose. A client seeing 3000 knows the
	// fleet is healthy and reconnecting promptly is correct. Sharing one code for both
	// makes every client hammer a failing application during the exact incident where
	// that is most harmful (docs/03-client-protocol.md §7).
	CloseDraining CloseCode = 3000

	// CloseHandshakeTimeout (3001) means no connect frame arrived within
	// server.handshake_timeout. reconnect: false.
	//
	// This covers only the part the client controls: receipt of the frame. A connection
	// waiting on a slow authorization closes with CloseAuthUnavailable instead. Conflating
	// the two turns a slow application into a permanent, non-retryable lockout of every
	// reconnecting user — FR-4 asserts both paths for exactly that reason.
	CloseHandshakeTimeout CloseCode = 3001

	// CloseUnauthorized (3003) means the connect webhook returned 401 or 403, or returned
	// 2xx with an unparseable body. reconnect: false — a decision was made and retrying
	// cannot change it (FR-6).
	CloseUnauthorized CloseCode = 3003

	// ClosePingTimeout (3004) means no pong arrived within server.pong_timeout of a
	// protocol-level ping. reconnect: true (FR-7).
	ClosePingTimeout CloseCode = 3004

	// CloseSlowConsumer (3005) means the connection's outbound queue overflowed.
	// reconnect: true.
	//
	// Closing is the only option that leaves the client correct: it reconnects,
	// resubscribes and reconciles from the application, so it ends up consistent having
	// noticed. Blocking stalls the channel for everyone; dropping leaves the client
	// silently wrong (FR-15, docs/07-delivery.md §4).
	CloseSlowConsumer CloseCode = 3005

	// CloseProtocolError (3006) means a binary frame or a frame over
	// limits.max_frame_size. reconnect: false — a client that speaks the protocol wrongly
	// will speak it wrongly again.
	CloseProtocolError CloseCode = 3006

	// CloseRateLimited (3007) means repeated client-publish rate-limit violations: ten
	// within 60 seconds. reconnect: true with a retry_after of 60s.
	//
	// The delay is not optional. Without it the anti-abuse control amplifies load onto the
	// connect webhook, which is the component least able to absorb it
	// (docs/13-review-findings.md m13).
	CloseRateLimited CloseCode = 3007

	// CloseAuthUnavailable (3008) means the gateway could not obtain an authorization
	// answer: a webhook 5xx, a webhook timeout, app.connect_timeout expiring, or
	// app.connect_queue overflowing. reconnect: true, with a retry_after (FR-6, NFR-4).
	//
	// Note that docs/04-integration.md §1.3 still shows 3000 in its status table. That is
	// a leftover from before this code existed; FR-6, docs/03-client-protocol.md §7 and
	// docs/13-review-findings.md M14 all say 3008, and they win.
	CloseAuthUnavailable CloseCode = 3008

	// CloseRevoked (3501) means the application published a control-channel disconnect
	// naming this user or client. reconnect: false (FR-18).
	CloseRevoked CloseCode = 3501

	// CloseExpired (3503) means the grant set reached expires_in, or a control-channel
	// refresh named this connection. reconnect: true, with a spread retry_after.
	//
	// The client reconnects and re-handshakes with whatever cookie the browser currently
	// holds. The gateway retains no cookie past the connect call, which is what makes this
	// survive session rotation — Django SESSION_SAVE_EVERY_REQUEST, Rails, any app calling
	// cycle_key() on privilege change — and what keeps a memory dump of the process from
	// yielding a set of live sessions (FR-22, docs/13-review-findings.md S3).
	CloseExpired CloseCode = 3503
)
