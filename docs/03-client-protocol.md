# 03 — Client protocol

**Normative.** An implementation that disagrees with this document is wrong. Behaviour
changes here must land in the same commit as the code.

`MUST` / `SHOULD` / `MAY` as in RFC 2119.

## 1. Transport

One websocket (RFC 6455), no subprotocol negotiated. All application frames are **text**
frames containing exactly one JSON object. Binary frames MUST be rejected with close code
3006.

The endpoint is `server.path` (default `/ws`). The whole protocol is nine message types,
which is deliberately the entire surface a client implementation has to cover.

## 2. Handshake

```
GET /ws HTTP/1.1
Host: app.example.com
Upgrade: websocket
Connection: Upgrade
Sec-WebSocket-Key: …
Sec-WebSocket-Version: 13
Origin: https://app.example.com
Cookie: session=…
```

The gateway MUST, in this order, before completing the upgrade:

1. Check `Origin` against `server.allowed_origins`. On mismatch, respond **403** and stop.
   No webhook call is made. A missing `Origin` counts as a mismatch unless
   `server.allow_missing_origin` is set (see `08-config.md`; it exists only for non-browser
   clients and defaults to `false`).
2. Check the connection count against `limits.max_connections`. Over the limit → **503**.

Cookies are not read by the gateway at this point beyond being captured for forwarding.

After a successful upgrade, the client MUST send the `connect` **frame** within
`server.handshake_timeout`. Otherwise the gateway closes with 3001.

`handshake_timeout` covers only receipt of that frame — the part the client controls. The
authorization that follows has its own, longer budget (`app.connect_timeout`), and
exceeding **that** closes with 3008, which is retryable. Conflating the two turns a slow
application into a permanent, non-retryable lockout of every reconnecting user.

## 3. Envelope

Client → gateway. `id` is a positive integer, unique per connection, present on any
command the client wants a reply to:

```json
{"id": 1, "connect":     {"subs": ["room-4410"]}}
{"id": 2, "subscribe":   {"channel": "room-4410"}}
{"id": 3, "unsubscribe": {"channel": "room-4410"}}
{"id": 4, "publish":     {"channel": "desk-42", "event": "typing", "data": {"typing": true}}}
{"ping": {}}
```

Exactly one command key per frame. A frame with zero or several MUST be answered with
error 101 and the connection left open.

Gateway → client:

```json
{"id": 1, "connect":     {"client": "8f2c1e04a7b3d915", "ping": 25,
                          "expires_in": 3600, "subs": {"room-4410": {}}}}
{"id": 2, "subscribe":   {}}
{"id": 2, "error":       {"code": 103, "message": "permission denied"}}
{"push": {"channel": "room-4410",
          "pub": {"event": "order.created", "data": {"id": 88123}}}}
{"push": {"channel": "org-42-alerts", "unsubscribed": {"reason": "grant revoked"}}}
{"id": 5, "sync":  {"channels": ["room-4410"]}}
{"disconnect": {"code": 3501, "reason": "revoked", "reconnect": false}}
{"pong": {}}
```

A reply carrying `id` corresponds to the command with that `id`. Frames without `id`
(`push`, `disconnect`, `pong`) are server-initiated and MAY arrive at any time, including
between a command and its reply.

## 4. Commands

### 4.1 `connect`

MUST be the first frame. Sending it twice on one connection is error 101.

Request fields, all optional:

| Field | Type | Meaning |
|---|---|---|
| `subs` | `[]string` | Channels to subscribe to atomically as part of connect. Saves a round trip on page load. |

Reply fields:

| Field | Type | Meaning |
|---|---|---|
| `client` | string | 16 hex chars. Stable for the connection's life. Used in `exclude`. |
| `ping` | int | Seconds between server pings. Informational; the client does not act on it. |
| `expires_in` | int | Seconds until the gateway closes with 3503 for re-authorization. Already clamped to `[app.min_expiry, app.max_expiry]` — the client is told the effective value, not the application's raw one. |
| `subs` | object | Map of channel → `{}` for those in the request that succeeded. Always an object: when nothing was granted it is `{}`, never `null`. |

An empty `subs` is the important case — it means every requested channel was refused — and
a client doing `Object.keys(subs)` must not have to guard against `null`. The same applies
to `sync`'s `channels`, which is `[]` when empty.

Channels in `subs` that fail authorization are **omitted from the reply map** rather than
failing the whole connect. The client compares what it asked for against what it got.

### 4.2 `subscribe`

The channel is matched against the connection's grants (see `05-authorization.md` §3).
Subscribing to a channel already held is error 104 — not silently idempotent, because in
practice a duplicate subscribe means the client's own registry has drifted and hiding that
makes reconnect bugs very hard to find.

### 4.3 `unsubscribe`

Unsubscribing from a channel not held is error 105. Success replies `{}`.

### 4.4 `publish` (client event)

Permitted **only** where the channel's namespace sets `client_events: true`. Otherwise
error 103. Rate limited per connection per `namespaces[].rate_limit`; exceeding it is error 106.
Ten violations within 60 seconds closes the connection with 3007 and a `retry_after` of
60s — without the delay, the anti-abuse control amplifies load onto the connect webhook,
which is the component least able to absorb it.

A client event requires **both** a grant matching the channel **and** `client_events: true`
on its namespace. Requiring only the namespace flag would let any connected client inject
fabricated events into a channel it cannot even read.

`event` is required and client-supplied. The gateway MUST stamp `from` with the
connection's user id, MUST NOT allow the client to set it, and MUST exclude the publisher
from its own event unless the namespace sets `echo: true`.

Client events are ephemeral. They are not persisted, not replayed, and not seen by the
application. Anything the application needs to know about goes over HTTP instead.

### 4.5 `sync`

`{"id":5,"sync":{}}` returns the gateway's authoritative subscription set for this
connection: `{"id":5,"sync":{"channels":["room-4410"]}}`.

The gateway can drop a subscription the client did not ask to drop — grant narrowing, or a
control-channel `unsubscribe`. Both send an `unsubscribed` push, but a client that missed
one has no other way to discover the divergence, and the symptom is indistinguishable from
a quiet channel. This is also the first thing to call when debugging "nobody receives
anything".

### 4.6 `ping`

Answered with `{"pong":{}}`, echoing `id` when one was supplied — without it a client with
two pings in flight cannot correlate replies or measure round-trip time. This exists
because browsers answer websocket-level pings
automatically and give JavaScript no way to observe them, so a client cannot use them for
its own liveness detection. The gateway uses protocol-level pings for *its* liveness
detection (FR-7); this application-level pair is for the client's.

## 5. Server-initiated frames

### 5.1 `push`

Three shapes, distinguished by which key is present alongside `channel`:

- `pub` — a message. `event` and `data` come from the publisher's envelope; `from` is
  present only for client events.
- `unsubscribed` — the gateway removed a subscription the client did not ask to drop,
  because the grant behind it disappeared (FR-17).
**Ordering, normative.** The gateway MUST NOT send a `push` for a channel **before** that
channel's `subscribe` reply, nor **after** its `unsubscribe` reply. This is free given the
single writer goroutine: the reply is queued to the outbound queue under the same shard
lock that mutates the subscription, so queue order guarantees it. Without this rule a
client legitimately receives a push for a channel it has not been told it holds, and two
conforming implementations diverge silently — one drops the message, the other closes.

### 5.2 `disconnect`

Sent immediately before the websocket close frame. `reconnect` tells the client whether
retrying could ever succeed:

- `true` — transient. Reconnect with jittered backoff.
- `false` — a decision was made. Stop, and surface it to the user.

Clients MUST honour `reconnect: false`. A client that retries through it turns a
revocation into a denial-of-service against the connect webhook.

## 6. Error codes

Returned in `{"error":{…}}` against a command's `id`. The connection stays open.

| Code | Meaning |
|---|---|
| 100 | Internal error |
| 101 | Bad request — malformed frame, unknown command, duplicate `connect` |
| 102 | Unknown namespace |
| 103 | Permission denied |
| 104 | Already subscribed |
| 105 | Not subscribed |
| 106 | Rate limited |
| 108 | Subscription limit reached (`limits.max_subscriptions_per_conn`) |

## 7. Close codes

Sent as the websocket close code, and mirrored in the `disconnect` frame.

| Code | Reason | `reconnect` | `retry_after` |
|---|---|---|---|
| 3000 | Draining — replica shutting down | true | yes, spread |
| 3001 | Handshake timeout — no `connect` frame in time | false | — |
| 3003 | Unauthorized — webhook returned 401/403 at connect | false | — |
| 3004 | Ping timeout | true | — |
| 3005 | Slow consumer — outbound queue overflowed | true | — |
| 3006 | Protocol error — binary frame, oversize frame | false | — |
| 3007 | Rate limit exceeded repeatedly | true | yes, 60s |
| 3008 | Authorization unavailable — webhook 5xx, timeout, or queue overflow | true | yes |
| 3501 | Revoked by the application | false | — |
| 3503 | Grants expired — reconnect to re-authorize | true | yes, spread |

Codes 3000–3099 are transport-level; 3500+ are authorization decisions. New codes stay
inside those bands.

There is deliberately **no code for a rejected `Origin`**: that check completes before the
upgrade and answers HTTP 403, so no websocket exists on which to send a close frame. Nor
is there one for an ungraceful kill, which by definition sends nothing.

3008 is separate from 3000 on purpose. A client that sees 3000 knows this replica is
draining and the fleet is healthy, so reconnecting immediately is correct. A client that
sees 3008 knows the *application* could not answer, so every replica will do the same
thing and reconnecting hard makes it worse. Reusing one code for both means every client
hammers a failing application during the exact incident where that is most harmful.

### 7.1 `retry_after`

Every retryable `disconnect` MAY carry `retry_after` in **milliseconds**:

```json
{"disconnect": {"code": 3000, "reason": "draining", "reconnect": true, "retry_after": 18400}}
```

Clients MUST honour it in place of their own backoff for that attempt. The gateway spreads
it — on drain, uniformly across `server.drain_spread` — because **the gateway knows how
many connections it is dropping and the client does not.** Client-side jitter alone cannot
solve this: at the first retry a formula like `min(30s, 2^n) × rand(0.5, 1.5)` yields
0.5–1.5s, which is precisely the one-second window that `10-operations.md` §4 models as an
application outage. Backoff widens only after the damage is done.

## 8. Client obligations

A conforming client MUST:

1. Send `connect` first, and wait for its reply before sending anything else.
2. Honour `retry_after` when present. When it is absent, reconnect with **full jitter**:
   `random(0, min(30s, 2^n))` — not the multiplicative form, which never produces a small
   enough spread on the first attempt (§7.1).
3. Maintain its own subscription registry and replay it after reconnect. The gateway
   remembers nothing across connections.
4. Honour `reconnect: false`.
5. Remove a channel from its registry on an `unsubscribed` push, and not resubscribe it.
   A client that replays a revoked channel gets 103 on every reconnect forever.
6. Reconcile from the application after any reconnect. The bus is at-most-once and a
   reconnect implies a gap. See `07-delivery.md` §2.

A conforming client SHOULD send an application-level `ping` if it needs to detect a
half-open connection faster than the browser will.

## 9. Worked exchange

```
→ GET /ws  (Origin, Cookie)
← 101 Switching Protocols

→ {"id":1,"connect":{"subs":["room-4410","org-99-secret"]}}
← {"id":1,"connect":{"client":"8f2c1e04a7b3d915","ping":25,
                     "expires_in":3600,"subs":{"room-4410":{}}}}
                                     ↑ org-99-secret omitted: not granted

← {"push":{"channel":"room-4410","pub":{"event":"order.created","data":{"id":88123}}}}

→ {"id":2,"subscribe":{"channel":"org-99-secret"}}
← {"id":2,"error":{"code":103,"message":"permission denied"}}

… application publishes a control unsubscribe …
← {"push":{"channel":"org-42-alerts","unsubscribed":{"reason":"grant revoked"}}}

→ {"id":9,"sync":{}}
← {"id":9,"sync":{"channels":["room-4410"]}}

… six hours later, grants expire …
← {"disconnect":{"code":3503,"reason":"expired","reconnect":true,"retry_after":21700}}
   [websocket close 3503]   → client reconnects with its CURRENT cookie

… or, application publishes a control disconnect …
← {"disconnect":{"code":3501,"reason":"revoked","reconnect":false}}
   [websocket close 3501]   → client must not retry
```
