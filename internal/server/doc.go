// Package server owns the client-facing listener: the websocket upgrade, the Origin
// check, the connection-count check, and the assembly of a connection from a successful
// handshake.
//
// The order at the upgrade is normative and it matters
// (docs/03-client-protocol.md §2). Before completing the upgrade:
//
//  1. Check Origin against server.allowed_origins. On mismatch, respond 403 and stop —
//     no webhook call is made. A missing Origin counts as a mismatch unless
//     server.allow_missing_origin is set.
//  2. Check the connection count against limits.max_connections. Over the limit, 503.
//
// There is no close code for a rejected Origin, and there must never be one: the check
// completes before the upgrade, so no websocket exists on which to send a close frame.
//
// After the upgrade the package assembles a connection and gets out of the way. Three
// things it owns are worth naming, because each is a requirement that was got wrong first:
//
//   - The two timeouts are separate keys and separate outcomes.
//     server.handshake_timeout covers receipt of the connect frame and closes 3001,
//     reconnect false; app.connect_timeout is the whole authorization budget and closes
//     3008, reconnect true, with a retry_after. Conflating them turns a slow application
//     into a permanent, non-retryable lockout of every reconnecting user, which is what
//     docs/13-review-findings.md C2 was (FR-4, NFR-4).
//   - Authorization switches on the sealed webhook.Result. Authorized builds the
//     connection; Refused closes 3003 and must never be retried; Unavailable closes 3008
//     and must be. The two are never collapsed (FR-6).
//   - The drain closes every connection with 3000 and a retry_after spread across
//     server.drain_spread, and returns within server.drain_timeout. The spread is the
//     thing that makes a rolling deploy survivable on a request/response application: the
//     gateway knows how many connections it is dropping and the client does not
//     (FR-19, docs/03-client-protocol.md §7.1).
//
// The adapter that lets internal/conn talk to internal/hub lives here too, because
// neither of those packages may import the other. It maps the hub's sentinel errors onto
// the protocol error codes docs/03-client-protocol.md §6 assigns them, and it applies the
// client-event policy — namespaces[].client_events and the per-namespace rate limit —
// which is configuration the hub does not read.
//
// What this package must never do:
//
//   - Accept an upgrade without checking Origin against the allowlist. Browsers do not
//     apply CORS to websocket handshakes but do attach cookies; this check is the only
//     thing standing between a logged-in user and cross-site websocket hijacking
//     (FR-2, docs/05-authorization.md §5).
//   - Match an Origin by suffix, by wildcard, or by "ends with .example.com". Exact
//     string comparison against the configured list. Subdomain wildcards are how an
//     attacker who controls one forgotten subdomain gets everything.
//   - Call the webhook before the Origin check. FR-2's acceptance criterion asserts that
//     no webhook call is made on a foreign Origin.
//   - Forward a client-supplied X-Forwarded-For from an untrusted peer. X-St-Forwarded-For
//     is the socket peer address unless the peer is inside server.trusted_proxies, in
//     which case it is the leftmost untrusted hop (FR-24).
//   - Log a cookie or an Authorization header. Log the client id and the origin (NFR-7).
//   - Answer a rejected Origin or a full replica with a websocket close code. Both
//     complete before the upgrade and answer an HTTP status: 403 and 503.
//   - Keep accepting connections once a drain has begun. A replica that admits a
//     connection it is about to close has spent an authorization for nothing.
package server
