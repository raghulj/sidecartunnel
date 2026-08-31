# 12 — Roadmap and open decisions

## 1. Milestones

Each is shippable on its own. A milestone is done when its requirements pass and the test
layers in `11-testing.md` §7 are green.

### M0 — Specification

This document set. Done when the open decisions in §3 are either resolved or explicitly
deferred with a reason.

### M1 — The tunnel works

The smallest thing that replaces a hosted service for server-to-client notifications.

- FR-1 … FR-5, FR-8 … FR-15, FR-21
- Websocket endpoint, Origin check, connect webhook, grants, glob matching
- Subscribe/unsubscribe with upstream refcounting
- Redis and memory buses, fan-out across replicas, `exclude`
- Bounded outbound queue with slow-consumer disconnect
- Config load and validation
- Unit, protocol and integration test layers

Not in M1: expiry, revocation, client library. A connection lives until it drops.

### M2 — Safe to run

Everything that makes M1 operable by someone who was not there when it was written.

- FR-6, FR-7, FR-16 … FR-20; NFR-4 … NFR-8
- Revalidation, grant narrowing, control channel, graceful drain
- `GET /health` and `GET /ready`, structured logs
- Webhook concurrency cap and cache
- Failure test layer

M2 shipped with a Prometheus endpoint and an operator API as well. Both have since been
removed; §2 records why.

M2 is the first version I would put in front of real users.

### M3 — The client

- Browser client: connect, jittered backoff, subscription registry replay, event emitter
- Dependency-free ES module, no build step required to use it
- A worked integration example against a stub application
- NFR-1 … NFR-3 measured and recorded

Around 200 lines. The size matters: a second implementation in another language should be
an afternoon, and that stays true only if the client stays thin (see §3.3).

### M4 — Chat-shaped features

Only after M2 has run in production long enough to be boring.

- Client events (`publish`), per-namespace, rate limited, requiring both a grant and the
  namespace flag
- Presence: membership tracking, join/leave, and whatever surface reports it

Bounded history was listed here and has been removed. `07-delivery.md` §2 argues that
reconciliation from the application beats any replay buffer, and §2 of this document lists
persistence as not planned — planning it one screen later was a straight contradiction.

Presence is deliberately last. It is the one feature that makes the gateway stateful, and
statefulness is what the rest of the design spends its budget avoiding.

## 2. Explicitly not planned

| Not building | Why |
|---|---|
| Pusher, Centrifugo or Socket.IO protocol compatibility | Each brings an authorization model shaped by constraints this design does not have |
| Message persistence | `07-delivery.md` §1 |
| A user, session or permission store | The invariant in `00-overview.md` |
| Client-to-client durable messaging | Application in the path, always |
| Redis Cluster or sharded pub/sub | One Redis until a measurement says otherwise |
| An admin UI | There is no admin API to put a UI on, and the log is the interface |
| Encrypted channels | End-to-end encryption with a server that fans out is theatre; if payloads need encrypting, encrypt them in the application |
| Prometheus metrics | Below |
| An operator HTTP API | Below |

### 2.1 Prometheus Metrics — Built, Then Removed

Eighteen metric families were specified before a line of code existed. Nine of them ended
up with no producer at all and sat permanently at zero.

That is worse than having none. Somebody writes an alert on
`st_slow_consumer_disconnects_total`, it never fires, and they conclude that clients are
never dropped. A gauge reading zero because nothing increments it is indistinguishable from
one reading zero because nothing is wrong, and the difference is exactly what the alert was
supposed to tell them.

For a gateway serving one application at 20,000 connections, the whole surface was
ceremony. The structured logs already carry the client id as a join key across every line a
connection produces, which answers the questions that actually get asked during an
incident. The cost was a package, a direct dependency and its transitive tail, and a
scrape-time sampler.

`GET /metrics` returns 404. It is not coming back, and re-adding it in six months without
re-reading this section would be re-adding the same nine permanently-zero families.

### 2.2 An Operator HTTP API — Built, Then Removed

`GET /channels`, `GET /channels/{channel}` and `POST /disconnect` lived on a second HTTP
listener bound to loopback, behind a bearer token.

The second listener existed so that a proxy misconfiguration could not expose `/channels`
publicly. That is a defence against a configuration mistake rather than against an
attacker, and it cost a package, an HTTP server, two configuration keys and a credential to
rotate.

`POST /disconnect` was a second door onto the hub call the control channel already makes,
and the worse of the two: the control channel is signed, it reaches every replica instead
of the one being curled, and it flushes the connect-webhook cache so a revoked user cannot
reconnect on a cached grant. `GET /channels` was a convenience over grepping a log this
process already writes — subscribe and unsubscribe are logged at info with the client id
and the channel, so `grep '"msg":"subscribe"' | grep room-4410` answers the same question
without a listener, a token or a package.

What is left on `server.listen` is `GET /health` and `GET /ready`, unauthenticated. They
report that the process is up and that it can reach Redis. Putting a credential on those
would be protecting the two facts every health endpoint on the internet already publishes.

## 3. Open decisions

Open on purpose. Do not resolve one by writing code that assumes an answer — change this
document first, with the reasoning.

### 3.1 Unbounded channel sets

A grant list assumes the set of channels a user may reach is enumerable. For a few dozen,
fine. For a user in ten thousand conversations it is not, and the connect response becomes
absurd.

Pusher solves this by signing per subscribe, on demand. The equivalent here is a
per-subscribe callback: the gateway asks the application "may this user subscribe to this
channel?", caches the answer for the connection's life, and the namespace opts in.

I have not built it because I do not yet have the case, and because it puts an HTTP call
back on the subscribe path that the grant model exists to remove. But the grant model
genuinely does not stretch to this, so the callback is the likely answer when the case
arrives rather than a hypothetical.

*(Not convinced about caching lifetime. For the connection's life is simple and wrong in
the same way an over-long `expires_in` is wrong.)*

### 3.2 Client events: M4, or never?

Ephemeral client events — typing indicators, cursors — are the only place the gateway
would forward client data without the application in the path. That is a real widening of
the trust surface for a genuine need: a keystroke must not become a request against a
worker pool.

The design is settled (namespace-gated, rate limited, `from` stamped server-side). What is
not settled is whether the need is real enough to justify it, or whether a debounced HTTP
call every few seconds is good enough. The config key is reserved and rejected at startup
until then, so nobody can half-enable it.

### 3.3 How much does the client library carry?

Reconnect and subscription replay are non-negotiable. Presence state, history replay and
offset tracking could live in the library or be left to the application.

Every addition makes the library more useful and makes a second implementation in another
language less likely to happen. I am inclined to keep it thin and let applications build
on top — but I have not yet written an application against it, which is the only way to
find out.

### 3.4 Control channel, or admin API, for revocation? — resolved, cut

Both existed and they overlapped: the bus version a one-liner for the application needing
no credential, the HTTP version what an operator reached for, returning a count.

"Keeping both may be one too many" was the answer here for a while. It was one too many.
The control channel won on the merits — it is signed, it reaches every replica rather than
the one being curled, and it flushes the connect-webhook cache so a revoked user cannot
reconnect on a cached grant, which the HTTP route did only because main's adapter
remembered to. §2.2 has the rest.

### 3.5 Multi-app — resolved, cut

Answered by the review. It was not merely unfinished; it was unsound (`13-review-findings.md`
C1). Cut. One gateway, one application; a second application is a second container.

If it ever returns, two rules must come with it: the hub keys by bus key (already true,
FR-21), and prefix validation must be "no prefix may be a prefix of another", not "unique"
— `a:` and `a:b:` are unique and still collide.

## 4. Things I expect to get wrong

Written down now so they are recognisable later.

- **`outbound_queue` default.** 256 is a guess. The right number depends on message size
  and fan-out width, neither of which I have measured.
- **`outbound_queue` and `drain_spread` defaults.** Both are now derived from arithmetic
  rather than guessed, which is better but still not measured. `drain_spread: 60s` assumes
  40 ms auth latency and a 16-worker pool; a slower application needs a wider window, and
  nothing currently tells an operator that except this line.
- **The single hub lock.** Right for 20,000 connections by the arithmetic in
  `09-internals.md` §3. NFR-9 gates sharding behind a profile precisely because I expect to
  be tempted to build it before the profile says so.
- **`webhook.Request.ChannelsRequested` was always empty — fixed.** The connect frame's
  `subs` could not reach the connect webhook, because `conn.Authorizer.Authorize` took
  only a context and was called from a connection built at the upgrade, before any frame
  existed. It now takes the requested channels as an argument, bounded by
  `limits.max_subscriptions_per_conn` and `limits.max_channel_length` before they leave
  the connection. The field stays a hint: the application answers with the grants and the
  gateway matches against those (`04-integration.md` §1.1).
- **Namespace-as-prefix.** Splitting on the first separator is simple and might be too
  simple for a naming scheme I have not thought of yet.
- **`max_subscriptions_per_conn` and §3.1 are the same problem.** A user in ten thousand
  conversations cannot enumerate grants *and* cannot subscribe past the cap. I had written
  down only one half. Raising the cap without solving the grant set just moves the wall.
