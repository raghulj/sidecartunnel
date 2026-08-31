# 01 — Requirements

Numbered so commits and tests can cite them. A requirement without an acceptance criterion
is a wish, so every one here has a way to be shown done.

`MUST` / `SHOULD` / `MAY` are used as in RFC 2119.

## Functional requirements

### Connection lifecycle

**FR-1 — Websocket endpoint.** The gateway MUST accept websocket upgrades on a configured
path (default `/ws`).
*Accept:* a client connects and receives a `connect` reply.

**FR-2 — Origin allowlist.** The gateway MUST reject any upgrade whose `Origin` header is
absent from `server.allowed_origins`, with HTTP 403, before any application call.
*Accept:* a handshake with a foreign `Origin` returns 403, no webhook call is made, and
`st_origin_rejected_total` increments.

**FR-3 — Cookie-forward authentication.** On accepting an upgrade, the gateway MUST call
the application's connect webhook, forwarding the client's `Cookie` header verbatim, and
MUST NOT attempt to parse, validate, or decrypt a session itself.
*Accept:* the application receives the exact cookie bytes the browser sent.

**FR-4 — Handshake timeout.** A connection that has not sent the `connect` **frame**
within `server.handshake_timeout` (default 5s) MUST be closed with code 3001,
`reconnect: false`. This timer MUST stop once the frame arrives; the authorization that
follows is governed by `app.connect_timeout` and its overflow closes with 3008,
`reconnect: true` (FR-6).
*Accept:* a socket opened and left silent closes 3001; a socket whose `connect` is queued
behind a slow webhook closes 3008 with a `retry_after`, never 3001. Both asserted, because
conflating them locks every reconnecting user out permanently.

**FR-5 — Grants applied at connect.** The gateway MUST store the grant list from the
webhook response against the connection and MUST NOT subscribe that connection to any
channel not matching a grant.
*Accept:* a subscribe to an ungranted channel returns error 103.

**FR-6 — Distinguish refusal from failure.** A webhook **401** MUST close with 3003,
`reconnect: false`. A **403**, 5xx, timeout, queue overflow, `connect_timeout`, or any
status not listed in `04-integration.md` §1.3 MUST close with 3008, `reconnect: true`,
carrying `retry_after`. The two MUST NOT share a code.
*Accept:* both paths tested and distinguishable by type; 401 is not retried while 5xx is,
up to `webhook_retries`; a 403 yields the transient result and is logged at ERROR and
counted separately, because a clock-skewed replica returning 403 forever must degrade to
its peers rather than permanently lock out every user it serves.

**FR-7 — Heartbeat.** The gateway MUST send websocket-level pings every
`server.ping_interval` (default 25s) and MUST close a connection whose pong does not
arrive within `server.pong_timeout` (default 10s), code 3004.
*Accept:* a client that stops responding to pings is closed within the window.

### Subscriptions

**FR-8 — Glob matching.** Channel authorization MUST be a glob match of the requested
channel against the connection's grants, where `*` matches any run of characters. No other
metacharacter is supported.
*Accept:* the table in `05-authorization.md` §3 passes as a unit test.

**FR-9 — In-memory matching.** Grant matching MUST NOT perform I/O — no network call, no
disk, and no mutex acquisition. A lock-free atomic load of an immutable grant set is
permitted and is how this is intended to be built (`09-internals.md` §2).
*Accept:* a benchmark shows zero allocations per match, **and** a `-race` test swaps the
grant set from one goroutine while another matches continuously, with no report. The
earlier design declared `grants` as guarded by `Conn.mu`, which meant matching either took
a mutex (violating this requirement) or read a slice header while it was being written —
a torn read that yields a new pointer with an old length, i.e. a subscribe authorized
against grants that were revoked seconds earlier. Code review alone does not catch it.

**FR-10 — Upstream subscription refcounting.** A replica MUST subscribe to a bus channel
when its local subscriber count for that channel goes 0 → 1, and unsubscribe when it goes
1 → 0.
*Accept:* with one client subscribed and then disconnected, `st_bus_subscriptions_current`
returns to its prior value.

**FR-11 — Namespace resolution.** A channel MUST resolve to the namespace named by the
substring before the first separator character. A channel whose namespace is not
configured, and for which no `default` namespace exists, MUST be refused with error 102.
*Accept:* `unknownns-1` is refused when no `default` block is configured.

### Publishing and delivery

**FR-12 — Bus publish.** A message published to `{bus.prefix}{channel}` MUST be delivered
to every connection subscribed to `channel` on every replica.
*Accept:* an integration test with two gateway processes and one Redis, publishing to a
channel held on both, delivers to both clients.

**FR-13 — Exclusion.** When a published envelope carries `exclude`, the connection with
that client id MUST NOT receive it.
*Accept:* a two-client test where the excluded client receives nothing.

**FR-14 — Oversize messages.** A published envelope larger than
`limits.max_message_size` (default 32 KiB) MUST be dropped, logged once with the channel
name, and counted in `st_messages_dropped_total{reason="oversize"}`.
*Accept:* an oversize publish delivers nothing and increments the counter.

**FR-15 — Bounded outbound queue.** Each connection MUST have an outbound queue of
`limits.outbound_queue` messages (default 256). On overflow, the connection MUST be closed
with code 3005 rather than blocking the publisher.
*Accept:* a client that stops reading is disconnected, and other clients on the same
channel continue to receive messages throughout.

### Authorization lifetime

**FR-16 — Bounded authorization lifetime.** No connection may act on a grant set older
than `expires_in`. See FR-22 for the mechanism.
*Accept:* covered by FR-22.

**FR-17 — Subscription withdrawal.** When a control-channel `unsubscribe` matches a live
subscription, the gateway MUST drop it **and** send the client an `unsubscribed` push.
*Accept:* the subscription is gone from a subsequent `sync` reply and the client received
the push. Dropping it silently leaves the client's registry claiming a channel it will
never hear from again, indistinguishable from a quiet channel.

**FR-18 — Revocation.** A `disconnect` command on the control channel MUST close every
connection for the named user or client, on every replica, within one second.
*Accept:* a control publish closes a connection held on a different replica from the
publisher.

### Operations

**FR-19 — Graceful drain.** On `SIGTERM` the gateway MUST stop accepting connections,
close open sockets with code 3000 and `reconnect: true`, and exit within
`server.drain_timeout` (default 20s).
*Accept:* clients reconnect after a rolling restart with no manual intervention.

**FR-24 — Forwarded address.** `X-St-Forwarded-For` MUST be the socket peer address unless
the peer is inside `server.trusted_proxies`. For a trusted peer, walk `X-Forwarded-For`
**from the rightmost entry leftwards while each hop is trusted, and take the first
untrusted address**; if every entry is trusted, use the socket peer. Taking the *leftmost*
untrusted entry instead lets a client behind a trusted proxy prepend a fake hop and have
the gateway forward it — the spoofing this requirement exists to prevent, surviving one
layer in. A client-supplied
`X-Forwarded-For` from an untrusted peer MUST be discarded, never forwarded.
*Accept:* a handshake carrying `X-Forwarded-For: 127.0.0.1` from an untrusted peer reaches
the webhook with the real peer address. Passing it through would let an attacker trigger
an application's localhost trust path from the public internet — an auth bypass in the
app, delivered by the gateway, under a header prefix implying the gateway vouched for it.

**FR-20 — Admin surface.** The gateway MUST expose, on a separate listener,
`/health`, `/ready`, `/metrics`, `/channels`, and `POST /disconnect`, with `/channels` and
`/disconnect` requiring a bearer token compared in constant time.
*Accept:* with `admin.token` configured, `/channels` without a token returns 401; with
`admin.token` unset the authenticated routes are not registered at all and return **404**,
so an accidentally unconfigured admin API looks absent rather than merely closed.
`/metrics` requires no token in either case.

**FR-21 — Bus-key isolation.** The hub MUST key subscriptions by the full bus key
(`{bus.prefix}{channel}`), never the bare channel name.
*Accept:* code review, plus a test that a publish to an unprefixed channel name reaches
nobody. (Multi-app support was cut — `13-review-findings.md` S1. This requirement is what
keeps restoring it cheap.)

**FR-22 — Expiry by re-handshake.** At `expires_in` the gateway MUST close the connection
with 3503, `reconnect: true`, and a spread `retry_after`. It MUST NOT retain the client's
cookie beyond the connect call.
*Accept:* a short `expires_in` produces a close and a successful reconnect; a memory
inspection after connect finds no retained cookie.

**FR-23 — Control authentication.** Control-channel messages MUST be rejected unless
signed with `control.secret` and timestamped within ±300s.
*Accept:* an unsigned control publish has no effect and increments a drop counter.

## Non-functional requirements

**NFR-1 — Connection density.** A single replica SHOULD hold **15,000** idle connections
within **1 GiB** RSS on a 2-core container, with `limits.read_buffer` and
`limits.write_buffer` at 2 KiB and `limits.compression` off.
*Accept:* a load test reports RSS and connection count.

The target deployment is 20,000 concurrent connections across two replicas — 10,000 each,
with 15,000 as headroom so either replica can absorb the whole fleet during a rolling
deploy. At ~35 KB per connection that is ~350 MiB in steady state and ~525 MiB while one
replica carries everything. An earlier draft required 50,000 in 4 GiB, which was designing
for a scale nobody asked for and paid for it in the hub (NFR-9).

**NFR-2 — Fan-out latency.** With 10,000 connections subscribed to one channel, p99 from
bus receipt to socket write SHOULD be under 20 ms. (One replica's full complement on one
channel is the worst realistic case; the measured cost is a map iteration, roughly 0.2 ms.)
*Accept:* a load test reports the histogram.

**NFR-9 — Simplicity ceiling.** The hub MUST use a single `sync.RWMutex` until a profile
shows lock contention above 5% of fan-out time. Sharding is a documented later step
(`09-internals.md` §4), not a starting position.
*Accept:* a profile at target load, recorded in the release notes. If contention is under
the threshold, the sharded version MUST NOT be built.

**NFR-3 — No goroutine leaks.** After 10,000 connect/disconnect cycles, goroutine count
MUST return to within 5% of baseline.
*Accept:* a soak test asserts it.

**NFR-4 — Webhook backpressure.** Outbound connect-webhook calls MUST be capped at
`app.webhook_concurrency`. Excess connections wait in the gateway, bounded on both axes:
at most `app.connect_queue` may wait, for at most `app.connect_timeout` each. Overflow of
either MUST close with 3008 and a `retry_after`.
*Accept:* with the cap at 8, a 1,000-client reconnect produces at most 8 concurrent
requests at the application; with `connect_queue` at 100, the 101st closes 3008 rather
than waiting. An unbounded queue is thousands of half-open sockets holding captured
cookies.

**NFR-5 — Startup validation.** Invalid configuration MUST fail startup with a message
naming the offending key. The process MUST NOT start in a partially-configured state.
*Accept:* each validation rule in `08-config.md` has a test asserting the message.

**NFR-6 — Binary size and base image.** The release image SHOULD be under 20 MB, built
`FROM scratch` or distroless, running as a non-root user.
*Accept:* `docker images` output in CI.

**NFR-7 — Secret hygiene.** No cookie, `Authorization` header, webhook body, or message
payload may appear in logs at any level, including debug.
*Accept:* a test drives a full connect and asserts the captured log output contains
neither the cookie value nor the payload.

**NFR-8 — Bus reconnection.** Loss of the bus MUST NOT close client connections. The
gateway MUST reconnect with backoff, resubscribe every held channel, and report
`/ready` as not-ready while disconnected.
*Accept:* killing Redis and restarting it restores delivery without clients reconnecting.

## Explicit non-requirements

Listed so nobody implements them by accident:

- Message durability, redelivery, or acknowledgement.
- Ordering guarantees beyond what a single Redis channel already provides.
- Any user, session, or permission store inside the gateway.
- Horizontal scaling of the bus (Redis Cluster, sharded pub/sub). One Redis is assumed;
  revisit only with a measurement showing it is the bottleneck.
- Protocol compatibility with any existing service.
