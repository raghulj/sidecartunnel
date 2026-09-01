# 16 — Integration Guide

How to adopt sidecartunnel in an application that already exists. The normative contracts
are [`04-integration.md`](04-integration.md), [`03-client-protocol.md`](03-client-protocol.md)
and [`08-config.md`](08-config.md); this document is the working order in which to build
against them.

A runnable version of everything below is in [`examples/flask/`](../examples/flask/).

## 1. What The Application Must Provide

Four things. Nothing else changes: no event loop, no websocket library, no new process
type, no change to how the application is deployed.

| # | Surface | Shape | Effort |
|---|---|---|---|
| 1 | Connect webhook | One `POST` route returning `{user, channels, expires_in}` | One view function |
| 2 | Publish | `redis.publish("st:<channel>", envelope)` | One line per existing notification site |
| 3 | Reconciliation | `GET …?since=<cursor>` on every endpoint whose data arrives by push | One query per pushed resource |
| 4 | Channel scheme | A naming convention that is an authorization boundary | Design, not code |

Items 1 and 4 are security-critical. Item 3 is the one that appears optional and is not.

### Prerequisites

| Requirement | Value |
|---|---|
| Redis reachable from every process that publishes | Any Redis 6+ |
| A session the application can resolve from a raw `Cookie` header | Already true of Flask, Django, Rails |
| A shared secret with the gateway | Min **32 bytes**, `app.webhook_secrets` |
| A second shared secret for control messages | Min **32 bytes**, `control.secret` |
| `/ws` routed to the gateway on the **same origin** as the application | Proxy rule |

Same origin is not a nicety. It is what makes the browser attach the session cookie to the
websocket upgrade with no frontend authentication code at all.

## 2. The Connect Webhook

The gateway turns a browser's cookie into an identity and a grant list by asking the
application. This is the only HTTP call the gateway ever makes.

### 2.1 What The Gateway Sends

```
POST {app.connect_url}
Content-Type: application/json
Cookie:             <verbatim from the client's upgrade request>
X-St-Origin:        https://app.example.com
X-St-Forwarded-For: 203.0.113.9
X-St-User-Agent:    Mozilla/5.0 …
X-St-Timestamp:     1756612800
X-St-Nonce:         01J8XYZ...
X-St-Signature:     <hex HMAC-SHA256>

{"client": "8f2c1e04a7b3d915", "channels_requested": ["room-4410"]}
```

The `Cookie` header is forwarded byte for byte. The gateway does not parse, validate,
decrypt or shorten it — session formats belong to the application. Any framework's own
session middleware therefore resolves it unchanged.

`channels_requested` is what the client's `connect` frame asked for. It is a hint you may
scope your answer by, log, or ignore: the grants you return are the only thing the gateway
enforces, and it arrives bounded by `limits.max_subscriptions_per_conn` and
`limits.max_channel_length`. `04-integration.md` §1.1 is normative on both points.

`X-St-Forwarded-For` is the client address as the gateway determined it. It is the socket
peer unless the proxy's CIDR is listed in `server.trusted_proxies`.

### 2.2 Verification Order Is Part Of The Contract

Verify the signature **before parsing anything else**. Header values, the JSON body and the
timestamp are all attacker-controlled until the HMAC has been checked.

| Step | Input | On failure |
|---|---|---|
| 1 | Read `X-St-Timestamp`, `X-St-Nonce`, `X-St-Signature`, `Cookie`, raw body as bytes | — |
| 2 | Recompute the HMAC and compare in constant time | **403** |
| 3 | Parse the timestamp as an integer | **403** |
| 4 | Reject skew above **±300s** | **403** |
| 5 | Optionally reject a replayed nonce | **403** |
| 6 | Parse the JSON body | **403** |
| 7 | Resolve the session | **401** if absent or inactive |
| 8 | Compute grants and return **200** | **5xx** on any internal failure |

Step 3 before step 2 is the common mistake, and it is not cosmetic. `int(ts)` on a header
an attacker controls raises before any authentication has happened. The resulting
unhandled exception is a **500**, which the gateway classifies as *transient*, retries up
to `app.webhook_retries`, and closes with **3008, `reconnect: true`** — so the client
retries too. A single malformed header becomes an unauthenticated retry loop against the
application's worker pool. The correct answer to a malformed timestamp is a flat 403.

The `examples/flask/test_app.py` case `test_malformed_timestamp_is_403_not_500` is the
executable form of this paragraph.

Parse the timestamp with integer arithmetic. `abs(time.time() - int(ts))` — the form in
`04-integration.md` §1.4 — raises `OverflowError`, **not** `ValueError`, on a 400-digit
timestamp, so a `except ValueError` around it does not catch it and the result is the
unauthenticated 500 this whole section exists to prevent. Python integers do not overflow;
floats do.

```python
try:
    skew = abs(int(time.time()) - int(ts))
except (TypeError, ValueError):
    abort(403)
if skew > 300:
    abort(403)
```

Two more inputs in the same class. `hmac.compare_digest` raises `TypeError` on a
non-ASCII `str`, and header values reach the application as latin-1, so a signature header
of `"ü" * 64` is a 500 unless it is caught. And a body that is not a JSON object at all
must be refused rather than indexed into.

### 2.3 The Signature

```
X-St-Signature = HMAC-SHA256(secret,
      timestamp + "." + nonce + "." + sha256(Cookie header) + "." + sha256(body))
```

Both inner digests are lowercase hex. The result is lowercase hex. Compare with a
constant-time comparison, never `==`.

The cookie digest is load-bearing. Signing only `timestamp + "." + body` would leave the
signature defending nothing: the body is a random client id, so an attacker who observed
one signed request — from a proxy log, a packet capture, or the application's own request
log — could replay those headers with their **own stolen cookie** and be handed the
victim's user id and full grant list.

`app.webhook_secrets` is a **list** so a secret can be rotated without a simultaneous
restart of the gateway and the application. The gateway signs with the first entry. The
application must accept any entry in its own list, and must iterate all of them with
constant-time comparison rather than short-circuiting on the first mismatch.

### 2.4 The Timestamp Window

Reject any request whose timestamp is more than **300 seconds** from now, in either
direction. Both directions: a clock ahead of the application is as much a signal as one
behind.

The window is a replay window, not a replay defence. A captured request can be replayed
verbatim inside it. That is accepted because the endpoint is read-only and idempotent — a
replay returns the same answer to someone who already held the bytes.

### 2.5 The Nonce

`X-St-Nonce` is emitted so an application that wants exactly-once can have it. Nothing
requires its use.

A correct implementation stores the nonce with a **300 second** expiry in a store shared by
every worker process, and rejects a repeat:

```python
if not redis.set(f"st:nonce:{nonce}", "1", nx=True, ex=300):
    abort(403)
```

A process-local dictionary is not an implementation of this. Under Gunicorn with four
workers it catches one replay in four, which is worse than not claiming the property.

### 2.6 The Response

```json
{"user": "u-7", "channels": ["room-4410", "user-7", "user-7-*", "org-42-*"], "expires_in": 21600}
```

| Field | Type | Required | Notes |
|---|---|---|---|
| `user` | string | yes | Opaque. The revocation target, and stamped on client events. |
| `channels` | `[]string` | yes | Glob grants. May be empty; an empty list is legal and subscribes to nothing. |
| `expires_in` | int | yes | Seconds. Clamped to `[app.min_expiry, app.max_expiry]`, default `[60s, 6h]`. |
| `info` | object | no | Ignored today. Reserved for presence. |

`expires_in` is a reconnect interval, not a session lifetime. Every expiry is one full
re-handshake per connection: origin check, webhook call, grant computation. At **20,000**
connections and `expires_in: 21600` that is roughly **1 request/second** at the
application. At `expires_in: 300` it is **67 requests/second**, permanently. Long expiry is
safe because revocation is immediate over the control channel (§8); short expiry is not a
revocation mechanism.

A grant beginning `_` is rejected at connect time. The underscore prefix is reserved.

## 3. 401 Versus 5xx

The single most consequential distinction in the integration, and the one most often
collapsed.

| Response | Means | Close code | `reconnect` | Client behaviour |
|---|---|---|---|---|
| 401, 403 | This user may not connect | 3003 | **false** | Stops. Surfaces to the user. |
| 5xx, timeout, connection error | The application could not answer | 3008 | **true** | Retries with `retry_after`. |
| 2xx, unparseable body | Treated as a refusal | 3003 | false | Stops. |

The gateway retries a 5xx up to `app.webhook_retries` and never retries a 401 or 403.

The two failure modes, stated plainly:

**Returning 5xx where 401 is meant.** A suspended user's browser retries forever. Every
retry is a webhook call. A revocation becomes a self-inflicted denial of service against
the endpoint least able to absorb one, and it never resolves, because the answer will
never change.

**Returning 401 where 5xx is meant.** A session backend that is not up yet during a deploy
answers 401 for every user. Every client receives `reconnect: false` and stops
permanently. Realtime does not come back when the deploy finishes — it comes back when
each user reloads the page, one at a time, over the following hours. `10-operations.md` §7
lists "401s from the webhook after a deploy" as a runbook entry for this reason.

The rule that produces correct code:

> 401 is a **decision about the user**. Anything else — an exception, a timeout, an
> unavailable dependency, an unhandled branch — is a 5xx.

In practice that means the session lookup is the only thing permitted to produce a 401,
and it must be able to distinguish "no valid session" from "the session store did not
answer". A framework's `get_or_404`-style helper is usually wrong here.

Verify the split before shipping:

```sh
# Refusal: no cookie. Expect 401.
curl -si -X POST localhost:5000/_st/connect -H "$SIGNED_HEADERS" | head -1

# Failure: stop the session backend, repeat. Expect 5xx, never 401.
```

## 4. Designing The Channel Namespace

A channel is an opaque string to the gateway. It parses exactly one thing out of it — the
namespace prefix, up to the first separator — and matches the rest as bytes.

That means **the naming scheme is the application's access-control surface**. A grant of
`org-42-*` is only as tight as the names beneath it. The gateway will not second-guess a
loose grant, because it holds no rule that could.

### 4.1 The Property That Matters

> A prefix must be a meaningful authorization boundary.

Which follows from one mechanical rule: **put the separator immediately after every
identifier.**

| Scheme | Grant | Also matches | Verdict |
|---|---|---|---|
| `org-{id}-{feature}` | `org-42-*` | `org-42-alerts`, `org-42-billing` | Correct |
| `org-{id}-{feature}` | `org-42-*` | not `org-421-alerts` | Correct — the separator stops it |
| `org{id}{feature}` | `org42*` | `org421secret` | **Broken** |
| `{feature}-org-{id}` | — | ungrantable as a prefix | **Broken** |

Working shapes:

```
user-{user_id}                 one user, all their devices
user-{user_id}-{feature}       one user's private feeds
room-{room_id}                 one room
org-{org_id}-{feature}         one org, grantable as org-{org_id}-*
status                         global broadcast, no separator
```

### 4.2 The `user-*` Trap, Worked

Glob matching has one metacharacter. `*` matches **any run of characters, including
none** — it does not stop at a separator.

An application means "this user's own channels" and writes:

```python
def grants_for(user):
    return ["user-*"]          # WRONG
```

The channels in existence:

| Channel | Contents |
|---|---|
| `user-7` | User 7's notification stream |
| `user-7-billing` | User 7's invoices and card details |
| `user-8` | User 8's notification stream |
| `user-8-billing` | User 8's invoices and card details |

`user-*` matches every row. User 7 subscribes to `user-8-billing` and the gateway allows
it, correctly: it was granted. There is no error, no exception and no log line, because
nothing went wrong from the gateway's point of view. The application said yes.

The fix is to enumerate the boundary rather than the namespace:

```python
def grants_for(user):
    return [f"user-{user.id}", f"user-{user.id}-*"]     # correct
```

Both entries are needed. `user-7-*` matches `user-7-billing` but **not** `user-7` — a
trailing `*` after a separator does not match the bare prefix. It also does not match
`user-71-billing`, which is the separator doing its job.

The same trap in other clothing:

| Intent | Wrong | Right |
|---|---|---|
| One user | `user-*` | `user-7`, `user-7-*` |
| One org | `org-*` | `org-42-*` |
| One room | `room-*` | `room-4410` |
| Every public banner | `*` | `status` |

`*` as a grant matches every channel including every other tenant's. It is never the
answer to "this user sees everything" — enumerate instead, or the first channel added by a
future feature is granted to everyone by default.

### 4.3 Cardinality

Channel names appear only in logs. Nothing aggregates by namespace — the log lines operate
on individual channels — so a per-user namespace at 200,000 users costs the gateway nothing
beyond the connection state it already holds.

Nothing secret belongs in a name. Names are not a security control; grants are. An encoded
or hashed name buys nothing and costs the ability to read a log during an incident.

### 4.4 Public Broadcasts

There is no way to turn authorization off for a namespace. A genuinely public broadcast is
expressed by putting one extra string — `status`, say — into every connection's grant
list. One string, no new concept, and no config key that can accidentally open a namespace.

## 5. Computing Grants

`grants_for()` is the only place the application's authorization reaches the gateway. It
returns strings. The gateway matches them and holds no rule that could disagree.

### 5.1 Reuse, Do Not Rewrite

The rule: **every grant must be produced by the same predicate that guards the
corresponding HTTP route.** A second rule set written specially for realtime is a copy of
an authorization policy, and copies drift silently — no error, no exception, just a message
delivered to someone who should not have it, discovered weeks later or never.

Wrong, because it is a second implementation:

```python
def grants_for(user):
    rows = db.execute("SELECT room_id FROM room_members WHERE user_id = ?", user.id)
    return [f"room-{r.room_id}" for r in rows]
```

That query will be correct on the day it is written. It will not know about the
`archived_at` column added next quarter, or the org-level override, or the suspension flag
that `can_read_room()` learned about and it did not.

Right, because there is one predicate:

```python
def can_read_room(user, room_id):        # already guards GET /rooms/<id>
    ...

def readable_room_ids(user):             # the enumeration behind the same predicate
    ...

def grants_for(user):
    grants = [f"user-{user.id}", f"user-{user.id}-*", "status"]
    grants += [f"room-{rid}" for rid in readable_room_ids(user)]
    grants += [f"org-{oid}-*" for oid in user.org_ids]
    return grants
```

Where the framework has a policy object — a Django permission backend, a Pundit policy, a
Flask-Principal need — call it. The test that keeps this honest asserts the two agree:

```python
for room_id in every_room_id():
    assert can_read_room(user, room_id) == (f"room-{room_id}" in grants_for(user))
```

That test is in `examples/flask/test_app.py` as `test_grants_agree_with_route_guard`.

### 5.2 The Enumerability Limit

The grant model assumes the set of channels a user may reach is **enumerable**. For a few
dozen channels this is free. For a user in ten thousand conversations the connect response
becomes absurd, and `limits.max_subscriptions_per_conn` (default **500**) is a second wall
behind the first.

There is no per-subscribe callback today; see `12-roadmap.md` §3.1. Until there is, an
application in that position subscribes to a coarser channel — `org-42-*` rather than ten
thousand `conv-*` entries — and filters client-side, accepting that filtering client-side
is not authorization. Where the finer boundary is a real one, the coarse channel must not
carry the finer data: publish an id and let the client fetch it over HTTP, where the route
guard still applies.

## 6. Publishing

### 6.1 The Envelope

A gateway channel `room-4410` is the Redis channel `{bus.prefix}room-4410`, prefix
defaulting to `st:`.

```json
{"event": "message.created", "data": {"id": 88123, "body": "…"}, "exclude": "8f2c1e04a7b3d915", "id": "01J8XYZ"}
```

| Field | Required | Meaning |
|---|---|---|
| `event` | yes | Opaque name, passed through to the client. |
| `data` | yes | Opaque payload. Any JSON value. |
| `exclude` | no | Client id that must not receive this. |
| `from` | no | Set by the gateway on client events. Never set by an application. |
| `id` | no | Echoed in logs for tracing. |

An envelope that is not valid JSON, or is missing `event` or `data`, is dropped. The
gateway logs the drop with the channel name; the publisher is not told.

```python
import json, redis
r = redis.from_url(REDIS_URL)

def publish(channel, event, data, exclude=None, message_id=None):
    envelope = {"event": event, "data": data}
    if exclude:
        envelope["exclude"] = exclude
    if message_id:
        envelope["id"] = message_id
    r.publish(f"st:{channel}", json.dumps(envelope))
```

No HTTP client, no signing, no dependency on a gateway being up. Safe to call inline in a
request handler: a local Redis publish is well under a millisecond, unlike the outbound
HTTPS call a hosted service requires.

### 6.2 Publish After Commit

Publish after the transaction commits, never inside it. A client that receives
`message.created` and immediately reconciles with `?since=` will read from a connection
that cannot see an uncommitted row, and will conclude the message does not exist.

```python
db.session.add(message)
db.session.commit()          # first
publish(f"room-{room_id}", "message.created", message.as_dict())   # then
```

In Django the equivalent is `transaction.on_commit(lambda: publish(...))`. In Rails it is
`after_commit`.

### 6.3 Size

The published envelope limit is **32 KiB** and it is more generous than it looks. A
realtime message should be a notification — an id and enough to render a row — not a
document. A payload approaching the limit should be replaced by an id the client fetches,
which also makes it survivable by reconciliation.

### 6.4 `exclude` And `X-St-Client`

`exclude` suppresses the echo back to the tab that caused the write. It needs the
originating connection's client id, which the browser has from its `connect` reply and the
application does not.

The convention is the **`X-St-Client`** header on the write request:

```
POST /api/rooms/4410/messages
X-St-Client: 8f2c1e04a7b3d915
```

```python
publish(f"room-{room_id}", "message.created", payload,
        exclude=request.headers.get("X-St-Client"))
```

Each tab has its own client id, which is the point. The other tabs of the same user
receive the event; only the originating tab does not. Treat the header as advisory: it is
client-supplied, and its worst case is a client suppressing its own message.

### 6.5 There Is No Error Channel

A typo'd channel name is silent forever. Redis `PUBLISH` returns a subscriber count that
reflects gateway replicas, not end clients, so it distinguishes "definitely nobody" from
"maybe someone" and nothing more.

This is the cost of choosing Redis over an HTTP publish API. There is no introspection
route. The gateway's own log is the whole of its introspection: subscribe and unsubscribe
are logged at INFO with `client` and `channel`, including the subscriptions that arrive on
the connect frame, so "I published and nobody received anything" is answered by grepping
what the gateway already wrote:

```sh
grep '"msg":"subscribe"' gateway.log | grep room-4410
```

Nothing there is almost always the prefix or the separator (§4.1), not a delivery failure —
that is reconciliation's job (§7).

Build the channel name in exactly one function. A format string repeated at eleven publish
sites will be wrong at one of them.

## 7. Reconciliation

### 7.1 Why It Is Mandatory

Delivery is **at-most-once**. A replica restart, a network partition, a laptop asleep in a
tunnel — in each case the message is published, nobody is there, and nothing keeps it.

There is no redelivery, no history buffer and no offset. The gateway holds no state that
survives a connection.

So the client closes its own gaps:

```
on reconnect:
    resubscribe every channel in the local registry
    GET /api/rooms/4410/messages?since={last_seen_id}
    merge
```

This is the single most important thing an integrating application implements, and the
easiest to skip, because **everything appears to work without it until the first deploy**.
A rolling restart of two replicas drops every connection twice. Without `?since=` every
connected user silently loses whatever was published during their reconnect, and there is
nothing to alert on: the loss happens on the client, and nothing server-side failed.

### 7.2 The Endpoint

For every resource whose updates arrive by push, expose an ordered read with a cursor.

| Property | Requirement |
|---|---|
| Cursor | Monotonic and total: an autoincrement id, or a sequence. **Not** a timestamp. |
| Semantics | Strictly greater than `since`, in ascending order. |
| Bound | A hard `limit`, plus a `has_more` flag. A client asleep for four hours must not ask for 400,000 rows. |
| Authorization | The same route guard as the non-realtime read. |
| Cold start | `since` absent means "the last page", not "everything since the beginning of time". |

```python
@app.get("/api/rooms/<int:room_id>/messages")
def list_messages(room_id):
    user = current_user()
    if not can_read_room(user, room_id):
        abort(403)
    since = request.args.get("since", type=int)
    rows = query(room_id, since=since, limit=200)
    return jsonify(messages=rows, latest_id=rows[-1]["id"] if rows else since,
                   has_more=len(rows) == 200)
```

A timestamp cursor loses rows. Two inserts in the same millisecond, or a clock adjustment,
and `since=<ts>` skips one silently — the exact failure the endpoint exists to prevent.

### 7.3 Ordering And Duplicates

Per-channel order is preserved in practice and is **not guaranteed**. Two publishes from
two processes have no defined order between them, and there is no sequence number on the
wire to detect a reorder.

The gateway generates no duplicates. The application might — a retried Celery task
publishing twice. Both problems have the same answer: put the id in the payload and make
the client's merge idempotent and order-independent on it. It is the same field `?since=`
already needs.

## 8. Revocation

Grants are a snapshot taken at connect. Two mechanisms end one.

| Mechanism | Latency | Use |
|---|---|---|
| Expiry (`expires_in`) | Up to `expires_in`, default max **6h** | The routine floor. Automatic. |
| Control channel `disconnect` | Under **1 second**, every replica | The emergency. Explicit. |

Publish a control message wherever the application already revokes access: suspension,
logout-everywhere, role removal, deleting a membership.

### 8.1 The Control Channel

Reserved channel `{bus.prefix}_control`, consumed by every replica. Clients can never
subscribe to it — any channel beginning `_` is refused with error 103 before grants are
consulted.

Control messages are **signed** with `control.secret` and carry a timestamp. Unsigned or
stale messages are dropped and logged; there is no other record of the rejection.

The action travels as an opaque JSON **string** in `body`, not as sibling fields of the
envelope, and the signature covers those exact bytes:

```json
{"ts": 1756612800,
 "nonce": "01J8XYZ...",
 "body": "{\"action\":\"disconnect\",\"user\":\"u-7\",\"reason\":\"account suspended\"}",
 "sig": "<hex HMAC-SHA256(control_secret, ts + \".\" + nonce + \".\" + body)>"}
```

The receiver verifies over the literal `body` string and only then parses it. The string is
what makes the signature verifiable at all: JSON object serialization is not canonical, so
a receiver handed the action as sibling fields cannot recover the bytes the sender signed,
and two libraries ordering keys differently produce different signatures for the same
message. Nothing is re-serialized, so no canonicalization rule is needed.

Building the envelope with the action fields merged in fails silently on both sides. The
gateway reads `body` as the empty string, verifies the signature against those bytes, drops
the message with `reason=unsigned`, and the publish still succeeds. Nothing disconnects and
nothing errors. `examples/flask/app.py` had exactly this bug until it was run against the
gateway; `test_suspend_publishes_a_signed_control_disconnect` now pins the envelope shape.

```python
action = {"action": "disconnect", "user": user, "reason": reason}
body = json.dumps(action, sort_keys=True, separators=(",", ":"))
ts, nonce = int(time.time()), uuid.uuid4().hex
sig = hmac.new(CONTROL_SECRET, f"{ts}.{nonce}.{body}".encode(), hashlib.sha256).hexdigest()

redis.publish("st:_control",
              json.dumps({"ts": ts, "nonce": nonce, "body": body, "sig": sig}))
```

`sort_keys` and `separators` are the sender's own choice, not a contract. The receiver
never re-serializes, so any serialization of the action works as long as the same bytes are
both signed and published.

| Action | Effect |
|---|---|
| `disconnect` | Close matching connections with 3501, `reconnect: false`. |
| `refresh` | Close with 3503, `reconnect: true`, with a `retry_after` spread over `server.drain_spread`. Clients reconnect and re-authorize with a current cookie. |
| `unsubscribe` | Drop matching subscriptions and send each client an `unsubscribed` push. `channel` may be a glob. |

`user` and `client` are matched **exactly, never as globs**, and every action must name
exactly one of them. An omitted target is a validation error, not "everyone" — otherwise a
single publish forces every connected user to re-authorize at once, which is the outage
modelled in `10-operations.md` §4.

The signature is checked before the timestamp. The signature covers the timestamp, so a
forgery can never be logged as stale, and a `reason=stale` line therefore always means an
authentic message outside the ±300s window — usually this replica's clock.

### 8.2 Which Action

| Situation | Action |
|---|---|
| Account suspended, session invalidated, user deleted | `disconnect` by `user` |
| One tab misbehaving | `disconnect` by `client` |
| Role or membership changed, user still valid | `refresh` by `user` — grants are recomputed on reconnect |
| One channel's access removed | `unsubscribe` by `user` with the channel |

`refresh` on a large population is a stampede. Each closed connection carries a
`retry_after` spread over `server.drain_spread`, and it should still be issued per user
rather than in a loop over every user of a tenant: the spread bounds how fast they come
back, not how many.

### 8.3 There Is No Second Door

There used to be a `POST /disconnect` on a separate admin listener, doing the same thing
over HTTP with a bearer token. It is gone. The control channel above (§8.1) is now the only
way to revoke a connection, application or operator alike — a second door onto the same
action was a second thing to keep signed and current for no capability the first did not
already have. An operator with Redis access publishes the same `disconnect` message §8.1
shows; there is no result returned, and the confirmation is the same one the application
gets — the connection closes with **3501**, logged.

## 9. Frontend Wiring

The socket is receive-only for anything durable. Writes go over ordinary HTTP to the
application, where CSRF, rate limiting, validation and request logging apply unchanged.

```
                 durable write
  Browser ─── POST /api/… (HTTP, X-St-Client) ───▶ Application ── commit ── publish
     ▲                                                                        │
     └──────────── push, over the websocket ◀───── Gateway ◀───── Redis ──────┘
```

### 9.1 The Client Library

[`client/js/sidecartunnel.js`](../client/js/) is the browser client: one file, no
dependencies, no build step. It carries the obligations in `03-client-protocol.md` §8 so
the application does not have to:

| Obligation | Why the library owns it |
|---|---|
| `connect` first, wait for the reply | Anything else is error 101. Commands issued earlier are queued, and a `subscribe` issued before the socket opens rides along in the connect frame's `subs`. |
| Honour `retry_after` over its own backoff | The gateway knows how many connections it dropped; the client does not. |
| Full jitter when `retry_after` is absent: `random(0, min(30s, 2^n))` | The multiplicative form fires inside the one-second window that takes the application down. |
| Maintain a subscription registry and replay it after reconnect | The gateway remembers nothing across connections. |
| Honour `reconnect: false` | Retrying through a revocation is a denial of service against the webhook. |
| Drop a channel on an `unsubscribed` push and not resubscribe it | Replaying a revoked channel earns error 103 on every reconnect forever. |
| Fire `onReconnect` so the application can reconcile | The bus is at-most-once; a reconnect implies a gap. |

The application supplies three things: what to render, where to reconcile from, and what
to show when the connection stops permanently.

```html
<script type="module">
import { connect } from "/static/sidecartunnel.js";

let lastSeen = window.__BOOTSTRAP__.latest_id;

async function reconcile() {
  const r = await fetch(`/api/rooms/4410/messages?since=${lastSeen}`);
  const { messages, latest_id } = await r.json();
  messages.forEach(render);
  lastSeen = latest_id;
}

const st = connect({
  url: "/ws",
  onReconnect: reconcile,                    // after every reconnect, not only the first
  onStateChange(state, info) {
    if (state === "closed" && info.permanent) showBanner(info.reason);
  },
});

st.subscribe("room-4410", (pub) => {
  if (pub.event === "message.created") render(pub.data);
}).catch((err) => console.warn("not subscribed:", err.code));

await reconcile();
</script>
```

Three rules for the application code around it:

1. **Render from the reconciliation endpoint, not only from pushes.** A push is an
   accelerator in front of a database that was always the source of truth.
2. **Make the merge idempotent on the payload id.** Reconciliation and a live push will
   deliver the same row.
3. **Send `X-St-Client` on writes.** `st.clientId` after the socket is open.

Two members worth knowing during an incident. `st.sync()` returns the gateway's
authoritative subscription set, and comparing it with `st.channels()` is the way to find a
divergence whose symptom is otherwise indistinguishable from a quiet channel. `st.stats`
carries `{connections, reconnects, orphanPushes, malformed}`.

The full API is in [`client/js/README.md`](../client/js/README.md). A worked page is
`examples/flask/templates/index.html`.

### 9.2 Proxy Routing

Route by path so the socket is same-origin and the cookie flows with no frontend
authentication code:

```
example.com/       → application
example.com/ws     → sidecartunnel:8000
```

The proxy must pass `Upgrade` and `Connection` through, forward `Origin` unmodified, set
`X-Forwarded-For` with its own CIDR in `server.trusted_proxies`, and have an **idle timeout
above `server.ping_interval`** (default **25s**). A lower idle timeout reaps healthy
sockets on a fixed cadence and presents as "it disconnects every 60 seconds".

## 10. Django And Rails

The structure is identical. Only the session lookup and the after-commit hook change.

### 10.1 Django

| Piece | Django form |
|---|---|
| Webhook route | A `@csrf_exempt` view. The gateway is not a browser and sends no CSRF token. |
| Session lookup | `SessionMiddleware` populates `request.session` from the forwarded cookie. `request.user` works if `AuthenticationMiddleware` is installed. |
| Refusal | `HttpResponse(status=401)`. Never `raise Http404`, which is a 404 and not in the table. |
| Failure | Let the exception propagate to Django's 500 handler. |
| Publish | `transaction.on_commit(lambda: publish(...))` |
| Reconciliation | `Model.objects.filter(room_id=…, id__gt=since).order_by("id")[:200]` |
| Grants | Reuse the permission backend or the same queryset the list view uses. |

```python
@csrf_exempt
@require_POST
def st_connect(request):
    if not verify_signature(request):          # first
        return HttpResponse(status=403)
    if not request.user.is_authenticated or not request.user.is_active:
        return HttpResponse(status=401)
    return JsonResponse({"user": f"u-{request.user.pk}",
                         "channels": grants_for(request.user),
                         "expires_in": 21600})
```

Two Django-specific notes. `SESSION_SAVE_EVERY_REQUEST` rotates the cookie on every
request; this is safe here because the gateway does not retain the cookie past the connect
call and re-handshakes with whatever the browser currently holds. And the view must be
excluded from any middleware that redirects unauthenticated requests to a login page — a
302 is not in the response table and will be treated as unparseable.

### 10.2 Rails

| Piece | Rails form |
|---|---|
| Webhook route | `post "/_st/connect"`, with `skip_before_action :verify_authenticity_token`. |
| Session lookup | The cookie jar resolves the forwarded header unchanged. |
| Refusal | `head :unauthorized` |
| Failure | Raise; let the 500 handler answer. |
| Publish | `after_commit { publish(...) }` |
| Reconciliation | `Message.where(room_id:).where("id > ?", since).order(:id).limit(200)` |
| Grants | Call the same Pundit or CanCanCan policy the controller uses. |

Rails rotates the session on `reset_session` and on privilege change. Same answer as
Django: re-handshake carries the current cookie.

## 11. Migration

### 11.1 From A Hosted Service — Pusher, Ably, PubNub

| Concern | Hosted | sidecartunnel |
|---|---|---|
| Publish | Outbound HTTPS, signed, rate limited, billed | `redis.publish`, sub-millisecond, local |
| Authorization | Per-subscribe signed auth endpoint | One connect webhook returning all grants |
| Private channel naming | `private-` prefix is a protocol convention | No convention; grants are the control |
| Presence | Built in | Not built (M4) |
| Client events | Built in | Not built (M4) |
| History / replay | Built in, bounded | Not built. `?since=` replaces it. |
| Webhooks to the app | Connection and presence events | Not built |
| Failure of the service | Publishes fail; app can retry | Publishes succeed into Redis; nobody is listening |

What changes:

- The auth endpoint moves from **per subscribe** to **per connect**. It is called once per
  connection rather than once per channel, and it must return the whole grant set at once.
  This is the change that surfaces §5.2: an application relying on per-subscribe signing
  for an unbounded channel set has no direct equivalent yet.
- Publishing becomes fire-and-forget with no error channel (§6.5). Any code that inspected
  a publish result, retried a failed publish, or alerted on publish errors loses its signal
  outright, with no automatic replacement — see §6.5.
- History and presence must be removed from the frontend or replaced. History becomes
  `?since=`. Presence has no replacement today.
- The `private-` / `presence-` prefixes carry no meaning. Redesign the names against §4
  rather than transliterating them; a straight rename preserves a scheme built for a
  different authorization model.

What does not change: the shape of the application. Persist, then notify. Every existing
publish site becomes a `redis.publish` on the same event name with the same payload.

### 11.2 From An In-Process Solution — Flask-SocketIO, Django Channels, ActionCable

| Concern | In-process | sidecartunnel |
|---|---|---|
| Where connections live | Inside the application's workers | A separate process |
| Deployment | Async worker class, sticky sessions, a channel layer | An ordinary WSGI/Rack app plus one container |
| Authorization | Ambient — the connection has `request.user` | Computed once, emitted as strings |
| Server-side handlers | `@socketio.on("message")` in the application | Not available. Writes are HTTP. |
| Rooms | `join_room` / `stream_from` at any time | Grants at connect, subscribe from the client |
| Broadcasting from a worker | A channel layer, or the same process | `redis.publish` from any process |
| A restart | Drops connections and often blocks on them | Drops connections; clients reconnect and reconcile |

What changes:

- **Server-side socket handlers disappear.** Every `@socketio.on(...)`, every
  `ActionCable` `receive`, every `AsyncJsonWebsocketConsumer.receive_json` that performed a
  durable action becomes an HTTP route. This is the bulk of the work and the bulk of the
  benefit: those handlers ran outside CSRF, outside the rate limiter, and outside request
  logging. Moving them into ordinary routes puts them back under controls that already
  exist and are already reviewed.
- **Dynamic room joins become grants.** `join_room(f"room-{id}")` inside a handler has no
  equivalent, because the gateway consults grants computed at connect. A room the user may
  join must be in `channels`. A room whose membership changes mid-connection needs a
  control `refresh` (§8.2).
- **Ambient identity disappears.** Anything that read `request.user` inside a socket
  handler must now be passed explicitly or resolved from the HTTP request that replaced the
  handler.
- **The worker class reverts.** `gevent`, `eventlet` and ASGI-for-websockets can go. This
  usually removes a monkey-patch and a class of production bugs with it.

What does not change: templates, HTTP routes, session handling, the database, and every
existing notification payload.

### 11.3 Running Both In Parallel

Neither migration needs a flag day. Both systems can deliver the same event to the same
page at once, because the merge is already idempotent on the payload id (§7.3).

| Phase | Duration | State |
|---|---|---|
| 1. Dual publish | 1 week | Every publish site sends to both the old system and Redis. Nothing consumes Redis. |
| 2. Shadow connect | 1 week | Gateway deployed. Frontend opens the websocket but ignores every push. Watch the webhook's own request logs for latency and error rate, `GET /ready` for bus health, and the gateway's logs for subscribe denials. |
| 3. Cohort cutover | 1–2 weeks | A percentage of sessions render from the gateway and stop rendering from the old system. Both connections stay open. |
| 4. Full cutover | — | All sessions on the gateway. The old client is dead code. |
| 5. Removal | — | Remove the old publish call and the old dependency. |

What each phase proves:

- **Phase 1** proves the envelope and the channel names. Redis with no gateway is a null
  sink, so mistakes cost nothing. Verify names against the gateway's subscribe log in phase
  2, not here.
- **Phase 2** proves the webhook under real load, including reconnect behaviour on a
  deploy. This is where a collapsed 401/5xx (§3) shows up, and where it is cheap. A
  subscribe-denied line in the gateway's logs is a grant bug, and finding it here is the
  point of the phase.
- **Phase 3** proves reconciliation. Any user reporting a missing message in this phase has
  found a gap in `?since=`, not in the gateway.
- Roll back by reverting the frontend cohort. The application-side changes — the webhook,
  the Redis publish, `?since=` — are all additive and can stay deployed through a rollback.

Two rules for the parallel period:

1. **Render idempotently from the first day of phase 1.** If a duplicate render is
   visible, phase 3 is not safe.
2. **Do not dual-publish client events or presence.** They have no equivalent, so there is
   nothing to compare, and the parallel run will not tell anyone whether the replacement
   works.

## 12. Pre-Production Checklist

Every line is a thing to verify, not a thing to intend. `17-production-readiness.md` has the
full table with thresholds.

### Infrastructure

- [ ] `server.allowed_origins` is populated with every exact origin, scheme included. No
      wildcards, no suffix matching. The gateway refuses to start when it is empty; it will
      start happily with the wrong value.
- [ ] Proxy idle timeout is **above `server.ping_interval`** (default **25s**). Verified by
      a connection held open for **10 minutes**, not by reading the config.
- [ ] Proxy passes `Upgrade` and `Connection`, and forwards `Origin` unmodified.
- [ ] `server.trusted_proxies` lists the proxy's CIDR, or every user is logged with the
      proxy's address.
- [ ] Redis `client-output-buffer-limit pubsub` raised from the default `32mb 8mb 60` to
      **`256mb 64mb 60`**. At the default, Redis evicts the gateway during a broadcast burst
      and the resubscribe leaves it behind again — a stable oscillation, not a transient.
- [ ] Redis is a **dedicated database index**, not shared with a cache.
- [ ] `bus.kind` is `redis`, not `memory`, for any deployment with more than one replica.

### Application

- [ ] The connect webhook verifies the signature **before parsing anything else**.
- [ ] **401 and 5xx are distinguished**, and the 5xx path is tested by stopping the session
      backend rather than by reading the code.
- [ ] A malformed `X-St-Timestamp` returns **403**, not 500.
- [ ] `?since=` exists on **every** endpoint whose data arrives by push, with a monotonic
      non-timestamp cursor and a bounded page.
- [ ] The frontend reconciles on **every** reconnect, not only on the first load.
- [ ] The channel scheme puts a separator immediately after every identifier, and no grant
      is `*` or a bare namespace wildcard (§4).
- [ ] `grants_for()` calls the same predicate as the HTTP route guard, with a test
      asserting the two agree.
- [ ] `X-St-Client` is sent on every write whose event should not echo to its own tab.
- [ ] Publishes happen **after commit**.
- [ ] A control `disconnect` is published wherever the application already revokes access.

### Security

- [ ] `app.webhook_secrets` and `control.secret` are each **at least 32 bytes** from a CSPRNG,
      and neither is in an image, a Dockerfile or the repository.
- [ ] Both are supplied by `_FILE` or a secret manager, and **rotation is rehearsed**:
      append the new secret to the gateway's list, deploy the application accepting both,
      make the new one first, remove the old one.
- [ ] TLS terminates in front of the gateway. `wss://`, never `ws://`, outside localhost.
- [ ] No log line, at any level, contains a cookie, an `Authorization` header, a webhook
      body or a message payload.
- [ ] No secret is in a channel name.

### Verify Before Real Users

| Check | How | Expected |
|---|---|---|
| Origin enforcement | Open a socket from a foreign origin | HTTP **403**, logged as a rejected origin, no webhook call |
| Grant enforcement | Subscribe to another tenant's channel | Error **103** |
| Revocation | Suspend an account | Every connection closes with **3501** in under **1 second** |
| Reconnect storm | Kill a replica in staging at realistic connection counts | Application request queue stays flat; reconnects spread over `server.drain_spread` |
| Reconciliation | Kill a replica mid-publish | Reconnected clients hold every message |
| Deploy | Roll the application | Connections recover; no 401 during the window |

## 13. Running The Reference Stack

[`examples/flask/`](../examples/flask/) is the whole arrangement: the application, the
gateway, Redis, and a proxy that puts them on one origin. Every command and every line of
output below was run against it.

### 13.1 Build The Gateway Image

From the repository root:

```sh
docker build -t sidecartunnel:dev .
```

`make image` runs the same build with `VERSION`, `COMMIT` and `DATE` derived from git, so
the image reports something other than `dev` when asked. Either produces the same layers
and the same nine OCI labels; only the values of the three generated ones differ.

| Measure | Value |
|---|---|
| Binary | 8.65 MB |
| Layer over `distroless/static-debian12:nonroot` | 8.66 MB |
| Image on disk | 18.3 MB |
| Compressed content | 4.15 MB |

NFR-6 targets under 20 MB. No release has been published, so nothing pulls a tag —
`examples/flask/docker-compose.yml` builds the gateway from this same `Dockerfile`, and
`examples/compose/docker-compose.yml` takes the tag from `ST_IMAGE`.

### 13.2 Start The Stack

```sh
cd examples/flask
cp .env.example .env
docker compose up -d --build
docker compose ps --format 'table {{.Service}}\t{{.Status}}'
```

```
SERVICE         STATUS
caddy           Up 8 seconds
redis           Up 8 seconds
sidecartunnel   Up 8 seconds (healthy)
webapp          Up 8 seconds
```

| Service | What it is |
|---|---|
| `caddy` | The only published port, 8080. Routes `/ws*` to the gateway and everything else to the application. This is what makes them one origin. |
| `webapp` | Flask under gunicorn, four sync workers. No websocket library, no async worker class. |
| `sidecartunnel` | The gateway. No published port — reachable only through the proxy. |
| `redis` | pub/sub on database index 3, with `client-output-buffer-limit pubsub` raised. |

The `.env` copied above carries placeholder secrets. They are long enough to start and are
obvious placeholders; replace them with `openssl rand -hex 32` before this is reachable from
anywhere.

### 13.3 Settings That Had To Be Right

Four, and three of them fail silently rather than loudly.

| Setting | Value | What the wrong value does |
|---|---|---|
| `ST_NAMESPACES_JSON` | must include `{"name":""}` | The empty name is the namespace separator-less channels resolve to. The demo grants `status`, which has no separator. Without the entry it resolves to no block, fails closed, and is **omitted from the connect reply with no error frame anywhere** — the subscribe simply never happens. Once a list is written the built-in catch-all is not installed, so every namespace in use must be in it. |
| `client-output-buffer-limit pubsub` | `256mb 64mb 60` | At the default `32mb 8mb 60` Redis evicts the gateway mid-burst and the resubscribe leaves it behind again. Verify with `docker compose exec redis redis-cli config get client-output-buffer-limit`. |
| Proxy upgrade passthrough | see `examples/flask/Caddyfile` | Caddy's `reverse_proxy` passes `Upgrade` and `Connection`, forwards `Origin` unmodified, and defaults to a 5m idle timeout, already above `ping_interval` (25s). nginx needs `proxy_http_version 1.1`, both `proxy_set_header` lines, and `proxy_read_timeout` above 25s stated explicitly. |
| Control envelope | `{ts, nonce, body, sig}` | The action goes in `body` as a string (§8.1). Merged into the envelope instead, the gateway verifies against an empty `body`, drops the message as unsigned, and the publish still returns success. Nothing disconnects and nothing errors. |

Only `/ws*` is routed to the gateway. `GET /health` and `GET /ready` share that listener, so
a proxy rule forwarding `/` would publish both.

### 13.4 The Health Checks

`sidecartunnel healthcheck` is a loopback `GET /health` and needs no shell in the image:

```sh
docker compose exec sidecartunnel /sidecartunnel healthcheck; echo $?
```

```
0
```

`/ready` is not routed through the proxy, so read it from inside the network:

```sh
docker run --rm --network flask_default curlimages/curl -s http://sidecartunnel:8000/ready
```

```json
{"ready":true,"bus_connected":true,"bus_down_for_seconds":0,"bus_reconnects":0,"draining":false}
```

### 13.5 The Connect Webhook, Directly

The webhook is an ordinary HTTP route and can be called with the gateway out of the picture.
Sign as §2.3 says, then vary one input at a time. Against the placeholder secret in
`.env.example`:

| Request | Status |
|---|---|
| Valid signature | **200** `{"channels":["user-7","user-7-*","status","room-4410","room-4411","org-42-*"],"expires_in":21600,"user":"u-7"}` |
| Bad signature | **403** |
| Valid signature, but over a **different** `Cookie` | **403** — this is the oracle §2.3 closes |
| Timestamp 400s in the past | **403** |
| Signature computed with `control.secret` | **403** — the two secrets are not interchangeable |
| `X-St-Signature` of `"ü" * 64` | **403**, never 500 |
| `X-St-Timestamp` of 400 digits | **403**, never 500 |

The last two are the ones that become an unauthenticated retry loop when the verification
order is wrong (§2.2).

### 13.6 The Whole Flow

The socket parts need a websocket client. This is the browser's behaviour written out:
the cookie and the `Origin` on the upgrade, `connect` as the first frame, subscriptions
riding it, `X-St-Client` on the write, and `?since=` after the reconnect.

```sh
python3 -m pip install websockets requests
```

```python
"""Drive the examples/flask stack the way a browser does. python -m pip install websockets requests"""
import asyncio, json, requests, websockets

ORIGIN, WS = "http://localhost:8080", "ws://localhost:8080/ws"

def login(user_id):
    s = requests.Session()
    s.get(f"{ORIGIN}/login/{user_id}", allow_redirects=False)
    return s, "session=" + s.cookies.get("session")

class Tab:
    def __init__(self, name, cookie):
        self.name, self.cookie, self.n, self.pending, self.pushes = name, cookie, 1, {}, []
    async def open(self):
        # The cookie and the Origin are what the browser sends on the upgrade by itself.
        self.ws = await websockets.connect(
            WS, additional_headers={"Cookie": self.cookie, "Origin": ORIGIN})
        asyncio.create_task(self._read())
        return self
    async def _read(self):
        try:
            async for raw in self.ws:
                print(f"  [{self.name}] <- {raw}", flush=True)
                f = json.loads(raw)
                if f.get("push"): self.pushes.append(f["push"])
                if f.get("id") in self.pending: self.pending.pop(f["id"]).set_result(f)
        except websockets.ConnectionClosed as e:
            print(f"  [{self.name}] CLOSED {e.rcvd.code} {e.rcvd.reason!r}", flush=True)
    async def cmd(self, name, payload):
        i, self.n = self.n, self.n + 1
        self.pending[i] = asyncio.get_running_loop().create_future()
        print(f"  [{self.name}] -> " + json.dumps({"id": i, name: payload}), flush=True)
        await self.ws.send(json.dumps({"id": i, name: payload}))
        return await asyncio.wait_for(self.pending[i], 6)

async def main():
    ada, ada_cookie = login(7)
    grace, _ = login(8)

    print("\n-- connect: subscriptions ride the first frame --")
    a = await Tab("A", ada_cookie).open()
    r = await a.cmd("connect", {"subs": ["room-4410", "user-7", "status"]})
    client_a = r["connect"]["client"]

    print("\n-- subscribe: allowed, then denied --")
    await a.cmd("subscribe", {"channel": "room-4411"})      # granted
    await a.cmd("subscribe", {"channel": "org-99-secret"})  # org-42-* cannot reach it
    await a.cmd("subscribe", {"channel": "room-4412"})      # archived, so not granted
    await a.cmd("subscribe", {"channel": "_control"})       # reserved

    print("\n-- a server-side event: Flask commits, publishes, the frame arrives --")
    print("  POST", grace.post(f"{ORIGIN}/api/rooms/4410/messages",
                               json={"body": "server-side event"}).text.strip())
    await asyncio.sleep(0.5)

    print("\n-- a durable write from tab A, with a second tab open --")
    b = await Tab("B", ada_cookie).open()
    await b.cmd("connect", {"subs": ["room-4410"]})
    w = ada.post(f"{ORIGIN}/api/rooms/4410/messages", json={"body": "from tab A"},
                 headers={"X-St-Client": client_a})
    print("  POST", w.text.strip())
    await asyncio.sleep(0.5)
    print("  echo to the originating tab A:", a.pushes[1:] or "none — suppressed by exclude")
    print("  delivered to tab B:          ", len(b.pushes), "push")

    print("\n-- reconnect: ?since= closes the gap --")
    cursor = w.json()["id"]
    await b.ws.close()
    grace.post(f"{ORIGIN}/api/rooms/4410/messages", json={"body": "published while B was gone"})
    b2 = await Tab("B'", ada_cookie).open()
    await b2.cmd("connect", {"subs": ["room-4410"]})
    print("  GET ?since=%d ->" % cursor,
          ada.get(f"{ORIGIN}/api/rooms/4410/messages?since={cursor}").text.strip())

    print("\n-- revocation: a signed control message closes every connection --")
    print("  POST", ada.post(f"{ORIGIN}/admin/users/7/suspend").text.strip())
    await asyncio.sleep(1)

asyncio.run(main())
```

```sh
python3 drive.py
```

```
-- connect: subscriptions ride the first frame --
  [A] -> {"id": 1, "connect": {"subs": ["room-4410", "user-7", "status"]}}
  [A] <- {"id":1,"connect":{"client":"1042187c649c01bd","ping":25,"expires_in":21600,"subs":{"room-4410":{},"status":{},"user-7":{}}}}

-- subscribe: allowed, then denied --
  [A] -> {"id": 2, "subscribe": {"channel": "room-4411"}}
  [A] <- {"id":2,"subscribe":{}}
  [A] -> {"id": 3, "subscribe": {"channel": "org-99-secret"}}
  [A] <- {"id":3,"error":{"code":103,"message":"permission denied"}}
  [A] -> {"id": 4, "subscribe": {"channel": "room-4412"}}
  [A] <- {"id":4,"error":{"code":103,"message":"permission denied"}}
  [A] -> {"id": 5, "subscribe": {"channel": "_control"}}
  [A] <- {"id":5,"error":{"code":103,"message":"permission denied"}}

-- a server-side event: Flask commits, publishes, the frame arrives --
  POST {"body":"server-side event","id":1,"room_id":4410,"user_id":8,"user_name":"Grace"}
  [A] <- {"push":{"channel":"room-4410","pub":{"event":"message.created","data":{"id":1,"room_id":4410,"user_id":8,"user_name":"Grace","body":"server-side event"}}}}

-- a durable write from tab A, with a second tab open --
  [B] -> {"id": 1, "connect": {"subs": ["room-4410"]}}
  [B] <- {"id":1,"connect":{"client":"2b07283cd718a47a","ping":25,"expires_in":21600,"subs":{"room-4410":{}}}}
  POST {"body":"from tab A","id":2,"room_id":4410,"user_id":7,"user_name":"Ada"}
  [B] <- {"push":{"channel":"room-4410","pub":{"event":"message.created","data":{"id":2,"room_id":4410,"user_id":7,"user_name":"Ada","body":"from tab A"}}}}
  echo to the originating tab A: none — suppressed by exclude
  delivered to tab B:           1 push

-- reconnect: ?since= closes the gap --
  [A] <- {"push":{"channel":"room-4410","pub":{"event":"message.created","data":{"id":3,"room_id":4410,"user_id":8,"user_name":"Grace","body":"published while B was gone"}}}}
  [B'] -> {"id": 1, "connect": {"subs": ["room-4410"]}}
  [B'] <- {"id":1,"connect":{"client":"3ca56352693ebaff","ping":25,"expires_in":21600,"subs":{"room-4410":{}}}}
  GET ?since=2 -> {"has_more":false,"latest_id":3,"messages":[{"body":"published while B was gone","id":3,"room_id":4410,"user_id":8,"user_name":"Grace"}]}

-- revocation: a signed control message closes every connection --
  POST {"active":false,"user":"u-7"}
  [B'] <- {"disconnect":{"code":3501,"reason":"account suspended","reconnect":false}}
  [A] <- {"disconnect":{"code":3501,"reason":"account suspended","reconnect":false}}
  [B'] CLOSED 3501 'account suspended'
  [A] CLOSED 3501 'account suspended'
```

Line by line:

| Line | What it shows |
|---|---|
| `connect` reply carries `client`, `ping`, `expires_in`, `subs` | The webhook resolved the cookie and returned grants. Nothing in the page sent a token. |
| `room-4411` allowed | Granted by `readable_room_ids`, the same predicate that guards the HTTP route. |
| `org-99-secret`, `room-4412`, `_control` all **103** | `org-42-*` does not reach `org-99-`; 4412 is archived so it was never granted; `_` is reserved before grants are consulted. |
| The `push` after Grace's POST | Flask committed, called `redis.publish`, and the frame reached a client that never spoke to Redis. |
| `echo to the originating tab A: none` | `X-St-Client` became `exclude` in the envelope. Tab B, the same user, still received it. |
| `GET ?since=2` returns the message published while B was gone | The gap a reconnect always implies, closed from the database rather than from the bus. |
| Both tabs close **3501** after the suspend | One signed publish to `st:_control`, every replica, under a second. |

A connect attempt after the suspend is refused **3003 `reconnect: false`** rather than
retried — the webhook answered 401, which is a statement about the user (§3). Recover the
demo database with `docker compose up -d --force-recreate webapp`.

### 13.7 The Client Library, Against The Live Gateway

The driver above is hand-rolled framing. `client/js/sidecartunnel.js` is what a page
actually loads, and it can be pointed at the same gateway from Node by injecting a
`WebSocket` that carries the cookie and the `Origin` the browser would send by itself:

```js
import WS from 'ws';
import { connect } from './client/js/sidecartunnel.js';

class Sock extends WS {
  constructor(url) { super(url, { headers: { Cookie: cookie, Origin: ORIGIN } }); }
}

const st = connect({ url: 'ws://localhost:8080/ws', WebSocket: Sock,
                     onReconnect: reconcile });
st.subscribe('room-4410', render).catch((e) => console.warn(e.code));
```

A gateway restart, with a `?since=` reconcile wired to `onReconnect`:

```
  state: open {"clientId":"b388c6c4df4f3a50","channels":["room-4410"]}
  reconcile -> +0 rows, cursor=0

-- restart the gateway --
  state: connecting {"attempt":1,"delay":45134,"retryAfter":45134,"code":3000,"reason":"draining"}

-- publish while the client is disconnected --
  POST -> 201 {"body":"sent during the gap","id":1,"room_id":4410,"user_id":7,"user_name":"Ada"}
  rendered so far: []

  t=10s state=connecting channels=["room-4410"]
  t=40s state=connecting channels=["room-4410"]
  state: open {"clientId":"9f83b6e5c5ad2a51","channels":["room-4410"]}
  onReconnect: {"attempt":1,"connections":2,"channels":["room-4410"],"denied":[]}
  reconcile -> +1 rows, cursor=1

final: state= open stats= {"connections":2,"reconnects":1,"orphanPushes":0,"malformed":0}
rendered: ["sent during the gap"]
```

Five obligations discharged by the library rather than by the page. It waited **45 seconds**
because the gateway told it to — `retry_after` from `drain_spread`, not its own backoff. It
kept `room-4410` in its registry across a connection that no longer existed and replayed it
in the next `connect` frame's `subs`. It fired `onReconnect` once the reply landed, which is
where the `?since=` fetch belongs. And the message published while the socket was down was
rendered from the database, not from the bus.

A denied channel is reported to the caller and dropped rather than replayed forever:

```
  onReconnect: {"connections":1,"channels":["room-4410"],"denied":["org-99-secret"]}
  refused org-99-secret: 103 permission denied
```

The library's own suite is 52 tests with no gateway:

```sh
cd client/js && node --test
```

```
# tests 52
# pass 52
# fail 0
```

### 13.8 The Failure Modes, Deliberately

| Command | Observed |
|---|---|
| `docker compose stop redis` | Sockets stay **open and silent** for the whole outage. `/sidecartunnel healthcheck` still exits 0 — liveness never consults the bus. After `docker compose start redis` the **same** socket resumes receiving pushes; it was never closed. |
| `docker compose restart sidecartunnel` | `{"disconnect":{"code":3000,"reason":"draining","reconnect":true,"retry_after":12324}}` — milliseconds, drawn per connection from `drain_spread` (**60s**), so 10,000 clients do not return at once. Across the runs here it landed between 12s and 45s. Reconnected clients hold every message sent during the gap, through `?since=` (§13.7). |
| A socket held idle for 70s | Still open through the proxy, and an application-level `ping` answers `{"pong":{}}`. Proves the proxy's idle timeout is above `ping_interval`. |
| `Origin: https://evil.example.com` on the upgrade | HTTP **403**, before any webhook call. A missing `Origin` is also 403. |
| `/login/9` (a suspended seed user) | **3003 `reconnect: false`**. `docker compose logs webapp` shows `"POST /_st/connect HTTP/1.1" 401`. |

### 13.9 What Is Subscribed

Publishing has no error channel, so the gateway's own log is the answer:

```sh
docker compose logs sidecartunnel | grep '"msg":"subscribe"' | grep room-4410
```

```
sidecartunnel-1  | {"time":"...","level":"INFO","msg":"subscribe","client":"1042187c649c01bd","channel":"room-4410"}
sidecartunnel-1  | {"time":"...","level":"INFO","msg":"subscribe","client":"2b07283cd718a47a","channel":"room-4410"}
```

Two clients, two ids. A channel nobody holds produces no line at all, which is the
difference between a typo and a quiet room.

### 13.10 The Application Test Suite

The application side needs neither Redis nor a gateway:

```sh
cd examples/flask
python3 -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
pytest -q
```

```
..................................................                       [100%]
50 passed in 0.45s
```

