# 04 — Integration

**Normative.** Everything an application must implement to adopt sidecartunnel, and
everything the gateway promises in return. Two surfaces on the application side — one
HTTP endpoint it exposes, one Redis contract it publishes on — plus the HTTP surface the
gateway itself exposes to infrastructure (§4).

Total work on the application side is one view function and one line at each publish site.

## 1. The connect webhook

The single integration point. The gateway calls it to turn a browser's cookie into an
identity and a set of grants.

### 1.1 Request

```
POST {app.connect_url}
Content-Type: application/json
Cookie:            <verbatim from the client's upgrade request>
X-St-Origin:       https://app.example.com
X-St-Forwarded-For: 203.0.113.9
X-St-User-Agent:   Mozilla/5.0 …
X-St-Timestamp:    1756612800
X-St-Nonce:        01J8XYZ...
X-St-Signature:    <see below>

{"client": "8f2c1e04a7b3d915", "channels_requested": ["room-4410"]}
```

The `Cookie` header is forwarded **byte for byte**. The gateway does not parse, validate,
decrypt, or shorten it. It cannot: session formats belong to the application.

The signature covers a **digest of the cookie** as well as the body:

```
X-St-Signature = HMAC-SHA256(secret,
      timestamp + "." + nonce + "." + sha256(Cookie header) + "." + sha256(body))
```

Binding the cookie digest is not decoration. An earlier draft signed only
`timestamp + "." + body`, and since the body contains nothing but a random client id, an
attacker who observed **one** signed request — from a proxy log, a packet capture, or the
application's own request log — could replay those exact signature headers with **their
own stolen cookie** and receive that victim's user id and full grant list. The signature
was defending nothing it claimed to.

The application MUST verify the signature and reject a timestamp outside ±300s.

Be clear about what this does not do: **the ±300s window is a replay window.** A signed
request can be replayed verbatim within it. That is acceptable because the endpoint is
read-only and idempotent, so a replay returns the same answer to someone who already had
the bytes. `X-St-Nonce` is emitted so an application that wants exactly-once can cache seen
nonces for 300s; nothing here requires it.

### 1.2 Response — 200

```json
{
  "user": "u-7",
  "channels": ["room-4410", "user-7", "org-42-*"],
  "expires_in": 3600,
  "info": {"name": "Ada"}
}
```

| Field | Type | Required | Meaning |
|---|---|---|---|
| `user` | string | yes | Opaque identifier. Used for revocation targeting and stamped on client events. The gateway never parses it. |
| `channels` | `[]string` | yes | Glob grants. May be empty — a connection with no grants is legal and simply cannot subscribe to anything. |
| `expires_in` | int | yes | Seconds until the gateway closes the connection for re-authorization (FR-22). Clamped to `[app.min_expiry, app.max_expiry]`; the clamped value is what the client is told. |
| `info` | object | no | Opaque. Reserved for presence in a later milestone; ignored today. |

### 1.3 Response — anything else

| Status | Gateway behaviour | Close code | `reconnect` |
|---|---|---|---|
| **401** — the application refused this *user* | Refuse the connection | 3003 | **false** |
| **403** — the application rejected the *request* | Refuse the connection | **3008** | **true** |
| queue overflow or `connect_timeout` | Refuse the connection | **3008** | **true** |
| 5xx, timeout, connection error | Refuse the connection | **3008** | **true** |
| 404, 400, 3xx, 429, any unlisted status | Refuse the connection | **3008** | **true** |
| 2xx with an unparseable body | Refuse, log once | 3003 | false |

**401 and 403 mean different things and must not be merged.** 401 is a statement about the
user: they may not connect, and the client must stop asking. 403 is a statement about the
*request* — a bad signature, a timestamp outside the ±300s window, an unknown key during a
rotation. That is a gateway-side fault, and a gateway fault must never be expressed to
users as a permanent refusal.

The case that forces this: a replica whose clock drifts past 300s gets a 403 on every
call. Merged into 401 it locks out every user it serves with `reconnect: false`, and they
stay locked out until a human notices. As 3008 they retry, the fleet degrades to the
healthy replicas, and the operator gets an alarm instead of an outage. Because retrying
cannot fix a bad secret or a skewed clock, a 403 is logged at ERROR and counted separately
from 5xx, so "my app is down" and "my gateway cannot authenticate to my app" are
distinguishable at a glance.

Unlisted statuses are transient for the same reason. A 404 means `connect_url` is wrong;
the choice is between every user locked out until someone notices and every user retrying
until someone notices, and only the second is recoverable.

The 401-vs-5xx distinction is the important one and it is easy to get wrong. A 401 means
*this user may not connect* and the client must stop asking. A 500 means *I could not tell
you right now* and the client must come back. Collapsing them either locks users out
during an app deploy, or turns a revocation into an infinite retry loop against the
endpoint.

### 1.4 Reference implementation (Flask)

Illustrative only — nothing in this repository depends on Flask.

```python
import hmac, hashlib, time
from flask import Blueprint, request, jsonify, abort

bp = Blueprint("sidecartunnel", __name__)

@bp.post("/_st/connect")
def st_connect():
    ts    = request.headers.get("X-St-Timestamp", "")
    nonce = request.headers.get("X-St-Nonce", "")
    sig   = request.headers.get("X-St-Signature", "")
    body  = request.get_data()

    # Verify the signature FIRST. Parsing attacker-controlled input before
    # authenticating it turns a malformed header into an unauthenticated 500,
    # which the gateway then treats as transient and retries forever.
    #
    # Three traps here, all of which produce that 500 if you get them wrong:
    #   - `int(ts)` on a 400-digit header raises OverflowError, NOT ValueError.
    #     Bound the length before converting, or compare as integers only after
    #     an isdigit() check.
    #   - hmac.compare_digest raises TypeError on a non-ASCII str, and header
    #     values arrive latin-1 decoded. Encode before comparing.
    #   - A signed body that is valid JSON but not an object (e.g. `[]` or `7`)
    #     blows up when indexed. Check the type.
    if not sig.isascii() or len(ts) > 20 or not ts.isdigit():
        abort(403)

    cookie_digest = hashlib.sha256(request.headers.get("Cookie", "").encode()).hexdigest()
    signed = f"{ts}.{nonce}.{cookie_digest}.{hashlib.sha256(body).hexdigest()}".encode()
    expected = hmac.new(WEBHOOK_SECRET, signed, hashlib.sha256).hexdigest()
    if not hmac.compare_digest(expected.encode(), sig.encode()):
        abort(403)

    if abs(int(time.time()) - int(ts)) > 300:
        abort(403)

    user = resolve_session(request.cookies)      # your existing code
    if user is None or not user.active:
        abort(401)                               # note: 401, not 500

    return jsonify(
        user=f"u-{user.id}",
        channels=grants_for(user),               # your existing authorization
        expires_in=3600,
    )
```

`grants_for()` is where the application's own authorization lives. It returns strings. The
gateway will match them and nothing else.

### 1.5 Concurrency

The gateway caps concurrent webhook calls at `app.webhook_concurrency`. Excess connections
wait inside the gateway, where waiting is cheap, rather than being issued at an application
with a fixed worker pool (NFR-4).

Waiting is bounded on both axes. `app.connect_queue` (default 4096) caps how many may wait
— an unbounded queue is 25,000 half-open sockets each holding a captured cookie — and
`app.connect_timeout` (default 10s) caps how long any one may wait. Exceeding either closes
with **3008 and a `retry_after`**, which is retryable. This is deliberate: the earlier
design let the handshake timeout fire on queued connections and close them with 3001,
`reconnect: false`, so the mechanism advertised as protecting the application against a
reconnect storm would have permanently locked out every user caught in one.

Set this deliberately. A reconnect after a replica restart is N simultaneous
authentications, and N is every connected user. The cap, plus the server-directed
`retry_after` spread in `03-client-protocol.md` §7.1, are what stand between a rolling
deploy and an outage.

`app.cache_ttl` defaults to **0, off**. It is worth less than it sounds — N reconnecting
users are N distinct cookies and therefore N cache misses — and it has a cost: a cached
entry survives a revocation, so a suspended user who reconnects within the TTL gets their
pre-revocation grants back. When enabled, any control-channel `disconnect` flushes the
whole cache, and `app.cookie_names` restricts the key to the session cookie, without which
`_ga`, `_fbp` and CSRF tokens make two tabs of one user miss each other anyway.

## 2. Publishing over Redis

### 2.1 Channel keys

A gateway channel `room-4410` is the Redis channel `{bus.prefix}room-4410`, prefix
defaulting to `st:`. One gateway serves one application (see `13-review-findings.md` S1),
so there is exactly one prefix, configured as `bus.prefix`.

### 2.2 Envelope

```json
{
  "event":      "order.created",
  "data":       {"id": 88123, "total": "42.00"},
  "exclude":    "8f2c1e04a7b3d915",
  "id":         "01J8XYZ…"
}
```

| Field | Required | Meaning |
|---|---|---|
| `event` | yes | Opaque event name, passed through to the client. |
| `data` | yes | Opaque payload. Any JSON value. |
| `exclude` | no | Client id that must not receive this. |
| `from` | no | Set by the gateway on client events; never set by an application. |
| `id` | no | Echoed in logs for tracing. |

There was an `only_users` field. It is gone: it was a delivery-time authorization filter in
a design that states delivery-time authorization does not exist, it had no bound, and
evaluating it meant an O(subscribers × users) scan while holding a shard read lock — 16
million string comparisons for one plausible digest, blocking every subscribe and close on
1/32 of all channels. Publish to per-user channels instead.

`exclude` needs the originating connection's client id, which the browser has (from its
`connect` reply) and the application does not. Send it as **`X-St-Client`** on the write
request. Each tab has its own client id, which is the point — the other tabs should receive
the event.

An envelope that is not valid JSON, or is missing `event` or `data`, is dropped and
logged once with reason `malformed`.

**Publishing gives you no error channel.** A typo'd channel name is silent forever: Redis
`PUBLISH` returns a subscriber count that reflects gateway replicas, not end clients, so
it distinguishes "definitely nobody" from "maybe someone" and nothing more. This is the
real cost of choosing Redis over an HTTP publish API. During development, the way to find
out whether anyone is listening to what you think they are is a grep of the gateway's own
log — subscribe and unsubscribe are logged at INFO with `client` and `channel`:

    grep '"msg":"subscribe"' gateway.log | grep room-4410

### 2.3 Publishing (Python)

```python
import json, redis
r = redis.from_url(REDIS_URL)

def push(channel, event, data, exclude=None):
    envelope = {"event": event, "data": data}
    if exclude:
        envelope["exclude"] = exclude
    r.publish(f"st:{channel}", json.dumps(envelope))
```

No HTTP client, no signing, no dependency on a gateway being up. Safe to call inline in a
request handler — a local Redis publish is well under a millisecond, unlike the outbound
HTTPS call a hosted service requires.

## 3. The control channel

Reserved channel `{bus.prefix}_control`, consumed by every replica. Clients can never
subscribe to it: any channel beginning `_` is refused with error 103.

**The control channel is a permanent member of the reconciler's desired set**, seeded at
startup and never removed. `Bus.Sync` sets the subscription set *exactly*, so a desired set
computed only from live client subscriptions unsubscribes the replica from control on the
very first reconciliation — after which every revocation is silently ignored. The gateway
keeps accepting connections and delivering messages, so nothing looks wrong until someone
tries to cut off a user and nothing happens.

Control messages are **signed**, and unsigned or stale ones are dropped and counted.

The action travels as a **JSON string**, not as sibling fields, and the signature covers
those exact bytes:

```json
{"ts": 1756612800,
 "nonce": "01J8XYZ...",
 "body": "{\"action\":\"disconnect\",\"user\":\"u-7\",\"reason\":\"suspended\"}",
 "sig": "<hex HMAC-SHA256(control_secret, ts + \".\" + nonce + \".\" + body)>"}
```

The receiver verifies over the literal `body` string, then parses it. This is deliberate:
an earlier draft put the action fields flat in the envelope and signed "the body", which
is **not implementable** — JSON object serialization is not canonical, so a receiver
cannot recover the bytes the sender signed. Two libraries ordering keys differently
produce different signatures for the same message. Carrying the body as an opaque string
removes the question entirely; no canonicalization rule is needed because nothing is
re-serialized.

`nonce` is echoed for receivers that want to reject replays within the ±300s window.

The read-only connect webhook was signed while the operation that can disconnect every
user on every replica — and, via `refresh`, stampede the application — was not. That
asymmetry was indefensible. Redis remains a trust boundary for message *publishing*
(`05-authorization.md` §8); it is no longer one for control.

```json
{"action": "disconnect",  "user": "u-7", "reason": "account suspended"}
{"action": "disconnect",  "client": "8f2c1e04a7b3d915"}
{"action": "refresh",     "user": "u-7"}
{"action": "unsubscribe", "user": "u-7", "channel": "org-42-*"}
```

| Action | Effect |
|---|---|
| `disconnect` | Close matching connections with 3501, `reconnect: false`. |
| `refresh` | Close matching connections with 3503, `reconnect: true`, spread over `control.refresh_spread`. They reconnect and re-authorize with a current cookie. |
| `unsubscribe` | Drop matching subscriptions and send each client an `unsubscribed` push. `channel` may be a glob. |

`user` and `client` are matched **exactly, never as globs**, and every action MUST name
exactly one of them. An omitted target is a validation error, not "everyone" — otherwise a
single publish forces 50,000 simultaneous re-authorizations, which is the outage modelled
in `10-operations.md` §4, available on demand.

`unsubscribe` must notify. Dropping a subscription silently leaves the client's registry
claiming a channel it will never hear from again, indistinguishable from a quiet channel,
forever.

Revocation on the bus rather than over HTTP keeps it a one-line call for the application
and reaches every replica without service discovery. It is also the only revocation
route there is: an HTTP `POST /disconnect` duplicated exactly this action and has been
removed for that reason.

## 4. HTTP surface

Everything the gateway answers over HTTP, all on one listener, `server.listen`. There is
no second listener and no credential on any route.

| Route | Auth | Answers |
|---|---|---|
| `<server.path>` (default `/ws`) | Origin allowlist + connect webhook | The websocket |
| `GET /health` | none | 200 while the process runs. **Never consults the bus.** |
| `GET /ready` | none | 503 once draining, or once the bus has been down longer than `bus.ready_grace`. |

Everything else is 404.

`/ready`'s body, verbatim:

    {"ready":true,"bus_connected":true,"bus_down_for_seconds":0,"bus_reconnects":0,"draining":false}

`bus_reconnects` is cumulative for the life of the process — read it by curling twice and
comparing. A count that climbs while `bus_connected` stays `true` is Redis pub/sub
output-buffer eviction, not an unstable Redis; it is the only remaining way to observe
that condition.

Neither probe carries a credential. They disclose that the process is up and that it can
reach Redis, which is what a load balancer needs and what every health endpoint on the
internet already says. In the documented deployment (`10-operations.md` §2) the proxy
forwards only `server.path`, so neither is publicly reachable anyway — that is defence in
depth, not the reason they are safe to leave unauthenticated.

**Never wire `/ready` to a liveness probe.** A Redis restart makes every replica
unready at once; on a liveness probe that kills every replica simultaneously, drops every
connection, and converts an eight-second Redis blip into a full application outage as
50,000 clients re-authorize together. `/health` exists precisely so there is something
correct to point a liveness probe at. `bus.ready_grace` (default 30s) covers the same case
for readiness, so a short blip does not pull the whole fleet from the load balancer.

## 5. Integration checklist

For an application adopting this:

- [ ] Route `/ws` on the same origin to the gateway, with upgrade passthrough and an idle
      timeout above `server.ping_interval`.
- [ ] Add the connect webhook. Verify the signature **before parsing anything else**.
      Return 401 for refusal and let 5xx mean failure — they are not interchangeable.
- [ ] Send `X-St-Client` on any write whose event should not echo to its own tab.
- [ ] Decide the grant vocabulary. Channel names are opaque to the gateway but they are
      your access-control surface — see `06-channels.md` §2.
- [ ] Replace each realtime publish site with a `redis.publish`.
- [ ] Add `?since=` reconciliation to any endpoint whose data arrives by push
      (`07-delivery.md` §2). Without it, every reconnect leaves a silent gap.
- [ ] Publish a control `disconnect` wherever the application already revokes access.
- [ ] Put the gateway's `Origin` allowlist in configuration management, not in a Dockerfile.
