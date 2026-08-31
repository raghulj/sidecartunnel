// Package webhook owns the connect-webhook client: request signing, timeouts, the
// concurrency cap and its bounded queue, and the optional answer cache.
// docs/04-integration.md §1.
//
// The signature covers a digest of the Cookie header as well as the body:
//
//	X-St-Signature = HMAC-SHA256(secret,
//	    timestamp + "." + nonce + "." + sha256(Cookie header) + "." + sha256(body))
//
// Binding the cookie digest is not decoration. An earlier draft signed only
// timestamp + "." + body, and since the body contains nothing but a random client id, an
// attacker who observed one signed request — from a proxy log, a packet capture, or the
// application's own request log — could replay those exact signature headers with their
// own stolen cookie and receive that victim's user id and full grant list. The signature
// was defending nothing it claimed to (docs/13-review-findings.md C5).
//
// What this package must never do:
//
//   - Parse, validate, decrypt or shorten a cookie. Session formats belong to the
//     application; the header is forwarded byte for byte (FR-3).
//   - Retain a cookie after the call returns. FR-22.
//   - Treat a 401 or 403 as transient, or a 5xx as final. A 401 means this user may not
//     connect and the client must stop asking; a 500 means I could not tell you right now
//     and the client must come back. Collapsing them either locks users out during an
//     application deploy or turns a revocation into an infinite retry loop against the
//     endpoint (FR-6).
//   - Retry a 401 or 403. Retries apply to 5xx and timeout only, up to
//     app.webhook_retries.
//   - Let the wait be unbounded. Waiting is bounded on both axes: app.connect_queue how
//     many may wait, app.connect_timeout how long any one may. Overflow of either closes
//     with proto.CloseAuthUnavailable and a retry_after — never proto.CloseHandshakeTimeout,
//     which is permanent and would turn the mechanism that protects the application into
//     a permanent lockout of every user caught in a reconnect storm (NFR-4,
//     docs/13-review-findings.md C2).
//   - Log the request body, the response body, or the Cookie header (NFR-7).
package webhook
