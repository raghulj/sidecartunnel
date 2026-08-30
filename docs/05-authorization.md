# 05 — Authorization

## 1. What the gateway knows

Per connection, after a successful connect:

```
client   "8f2c1e04a7b3d915"
user     "u-7"
grants   ["room-4410", "user-7", "org-42-*"]
expires  2026-08-31T11:30:00Z
```

That is the whole of it. There is no user record, no role, no permission, no tenant, and
no way to look any of those up. Everything the gateway will ever decide about this
connection is decided from those four fields.

## 2. Why grants and not claims

An earlier draft had the application send structured claims — a user id, an organisation
id, a role list — and the gateway match channels against them using rules in its
configuration. It reads well and it is wrong.

The moment the gateway maps `org-42-alerts` to "requires `org_id == 42`", it has an
opinion about what an organisation is, and that opinion is a copy of one that already
exists in the application. Copies drift. This one drifts silently: no error, no exception,
just a message delivered to someone who should not have it, discovered weeks later or
never.

Grants avoid that by inverting who computes what. The application runs its own
authorization code — the same code guarding its HTTP routes — and emits the *result* as
strings. The gateway matches strings. It cannot disagree with the application because it
holds no rule that could.

The cost is real and worth naming: a grant list assumes the set of channels a user may
reach is enumerable. Where it is not — thousands of conversations, say — the list becomes
impractical and the answer is a per-subscribe callback, which is not built. See
`12-roadmap.md`.

## 3. Glob matching

`*` matches any run of characters, including none. It is the only metacharacter. `?`,
`[...]`, `{...}` and escaping are not supported and MUST NOT be added: every addition is a
new way for a grant to match more than its author intended.

Matching is case-sensitive. A channel matches if **any** grant matches it.

| Grant | Channel | Result |
|---|---|---|
| `room-4410` | `room-4410` | match |
| `room-4410` | `room-44100` | no match |
| `room-4410` | `Room-4410` | no match — case-sensitive |
| `org-42-*` | `org-42-alerts` | match |
| `org-42-*` | `org-42-` | match — `*` matches empty |
| `org-42-*` | `org-42` | no match |
| `org-42-*` | `org-421-alerts` | no match |
| `org-42-*` | `org-99-secret` | no match |
| `*` | anything | match |
| `user-*` | `user-7` | match |
| `user-*` | `user-7-private` | match — **see below** |
| (empty list) | anything | no match |

That last row is the trap. `user-*` grants everything beneath `user-`, including
`user-7-private` and `user-8`. An application meaning "only this user's own channel" must
emit `user-7`, not `user-*`. Grants are exactly as tight as the application writes them,
and the gateway will not second-guess a loose one.

A grant beginning `_` is rejected at connect time: the underscore prefix is reserved for
control channels and must never be grantable.

Implementation: precompile each grant once at connect into either a literal comparison
(no `*`) or a prefix/suffix/segment check. Do not build a regular expression per match,
and do not allocate on the match path (FR-9).

## 4. When authorization happens

| Moment | What is checked |
|---|---|
| Handshake | `Origin` against the allowlist. Nothing else. |
| Connect | The webhook decides. Grants are stored. |
| Subscribe | Channel against grants, in memory. |
| Publish (client event) | Channel against grants, plus the namespace's `client_events`, plus rate limit. |
| Delivery | Nothing. A subscription that exists is delivered to. |
| Expiry | The connection is closed retryably; the browser re-handshakes with a current cookie. |

Delivery is deliberately unchecked. Re-testing on every message would put the
authorization decision on the hot path for no benefit — the subscription was authorized
when it was created, and the two mechanisms that can invalidate it (revalidation, control
disconnect) both act on the subscription itself rather than waiting for the next message.

## 5. Origin checking

The most important twenty lines in the codebase.

Browsers do **not** apply CORS to websocket handshakes. There is no preflight and no
same-origin policy. But they *do* attach cookies. So without an `Origin` allowlist, this
happens:

1. A logged-in user visits `evil.example`.
2. That page opens a websocket to `wss://app.example.com/ws`.
3. The browser attaches the victim's session cookie.
4. The gateway asks the application, which answers correctly: this is user 7.
5. The attacker's page now receives user 7's entire realtime stream.

That is cross-site websocket hijacking, and it is the classic failure of exactly this
design. The check is:

```go
origin := r.Header.Get("Origin")
if origin == "" && !cfg.Server.AllowMissingOrigin { reject(403) }
if !cfg.Server.AllowedOrigins.Contains(origin) { reject(403) }
```

Exact string comparison against a configured list. No suffix matching, no wildcards, no
"ends with `.example.com`" — subdomain wildcards are how an attacker who controls one
forgotten subdomain gets everything.

Modern `SameSite=Lax` cookie defaults also happen to block this, since a websocket
handshake is not a top-level navigation. That is worth knowing and worth not relying on:
it evaporates the moment a cookie is set `SameSite=None` for an unrelated reason, such as
an embedded widget or a third-party checkout flow.

`allow_missing_origin` exists for non-browser clients, which send no `Origin`. Turning it
on removes the defense for browsers too, so it should be paired with an allowlist of
source addresses at the proxy.

## 6. Expiry and revalidation

Grants are a snapshot, so they need an end. A socket open for eight hours must not still
be acting on a decision made eight hours ago.

At `expires_in` the gateway closes the connection with **3503, `reconnect: true`**, and a
`retry_after` spread across the fleet. The browser reconnects and the whole connect flow
runs again with whatever cookie the browser currently holds.

An earlier design instead cached the cookie at handshake and replayed it to the webhook.
That failed on a case I had not considered and which is the *default* for several
mainstream session backends — Django `SESSION_SAVE_EVERY_REQUEST`, Rails, any app calling
`cycle_key()` on privilege change. The browser rotates to a new session; the gateway still
holds the old one; revalidation returns a correct 401; and the gateway closed with
`reconnect: false`, permanently cutting off a user who was sitting in the application fully
logged in. There was no channel by which a rotated cookie could ever reach the gateway.

Re-handshake has none of that. It also removes cookie retention entirely, which shrinks
what a compromised process is worth (§8).

The cost is one reconnect per `expires_in` per connection, paid for two ways: `max_expiry`
now defaults to **6h** rather than an hour, and revocation no longer depends on expiry at
all. Long expiry plus an immediate control channel is strictly better than short expiry,
which was quietly being used as a revocation mechanism it was bad at.

## 7. Revocation

Revalidation bounds exposure to at most one `expires_in`. When that is too slow, the
application publishes to the control channel and every replica acts within a second
(FR-18). See `04-integration.md` §3.

The two mechanisms are complementary: revalidation is the routine floor, the control
channel is the emergency. Neither requires the gateway to know why.

## 8. Threat model

What this design defends against, and what it does not.

| Threat | Defense |
|---|---|
| Cross-site websocket hijack | `Origin` allowlist (§5) |
| Subscribing to another user's channel | Grant matching (§3) |
| Guessing channel names | Nothing — and nothing is needed. Names are not secrets; grants are the control. |
| Swapping a cookie into a captured webhook call | The signature covers a digest of the `Cookie` header |
| Replaying a captured webhook call verbatim | **Limited, not prevented** — ±300s window. The endpoint is read-only and idempotent; a nonce is emitted for applications that want exactly-once |
| Forged control commands (disconnect, mass refresh) | HMAC signature on the control envelope (FR-23) |
| Compromised gateway process | **Partly mitigated.** Cookies are no longer retained past the connect call (FR-22), so a memory dump no longer yields a set of live sessions. Still a tier-1 service. |
| Anything with Redis access publishing to any channel | **Not defended.** Redis is a trust boundary; see below. |
| A client flooding client events | Per-connection rate limit, then close 3007 |
| A client flooding connections | `limits.max_connections`, plus limits at the proxy |

Two accepted risks, stated plainly rather than buried:

**Redis is a trust boundary.** Anything that can reach Redis can publish to any channel.
For one application on a private network that is fine, and it is the price of dropping the
HMAC-signed HTTP publish API. It does mean one Redis plus one gateway cannot safely serve
mutually-untrusting applications, so "multi-app" here means several applications under the
same operator.

**The gateway sees every user's cookie** as it passes through to the webhook. It no longer
*retains* them — an earlier design cached the cookie for revalidation, which both made the
process a session-hijacking toolkit and broke every application that rotates its session
(`13-review-findings.md` S3) — but it still handles them, so NFR-7's logging ban stands and
the image gets no shell and no publicly-bound debug endpoint.
