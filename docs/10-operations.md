# 10 — Operations

## 1. The image

Single static binary, `FROM scratch` or distroless, non-root, no shell. Target under 20 MB
(NFR-6).

```dockerfile
FROM golang:1.23-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /sidecartunnel ./cmd/sidecartunnel

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /sidecartunnel /sidecartunnel
USER nonroot:nonroot
EXPOSE 8000
ENTRYPOINT ["/sidecartunnel"]
```

`EXPOSE 9001` is deliberately absent: `docker run -P` publishes every exposed port, and
the admin listener defaults to loopback precisely so it cannot be reached from outside.

No shell is deliberate. This process holds every connected user's session cookie in memory
(`05-authorization.md` §8); it should be as hard to introspect from inside as it is easy to
observe from outside.

## 2. Routing

Route by path so the socket is same-origin and the cookie flows with no frontend work:

```
example.com/       → application
example.com/ws     → sidecartunnel:8000
```

Whatever terminates TLS must:

- pass `Upgrade` and `Connection` through
- have an **idle timeout above `server.ping_interval`**, or it will reap healthy sockets
  on that timeout and you will chase it for an afternoon
- forward `Origin` unmodified — the allowlist depends on it
- set `X-Forwarded-For`, **and** have its own CIDR listed in `server.trusted_proxies`.
  Without that entry the gateway discards the header and reports the proxy's address to
  your application for every user — which looks like it works, and silently flattens any
  app-side rate limiting, geo-blocking or audit logging keyed on it (FR-24).

Caddy:

```
example.com {
    handle /ws* { reverse_proxy sidecartunnel:8000 }
    handle     { reverse_proxy webapp:5000 }
}
```

Nginx needs the upgrade headers set explicitly and `proxy_read_timeout` raised above the
ping interval; that omission is the single most common cause of "it disconnects every 60
seconds".

The admin listener on `:9001` is never published. It is reachable from the internal
network only.

## 3. Compose and Swarm

```yaml
services:
  sidecartunnel:
    image: ghcr.io/…/sidecartunnel:1.0.0
    environment:
      ST_SERVER__ALLOWED_ORIGINS: "https://app.example.com"
      ST_APP__CONNECT_URL: "http://webapp:5000/_st/connect"
      ST_APP__WEBHOOK_SECRETS_FILE: /run/secrets/st_webhook
      ST_CONTROL__SECRET_FILE: /run/secrets/st_control
      ST_BUS__URL: "redis://redis:6379/3"
    secrets: [st_webhook, st_control]
    healthcheck:
      test: ["CMD", "/sidecartunnel", "healthcheck"]
      interval: 10s
    deploy:
      replicas: 2
      update_config: { order: start-first, delay: 30s }
```

`order: start-first` with a delay above `drain_timeout` means a rolling update never has
zero replicas, so reconnecting clients always find somewhere to land.

The healthcheck runs the binary in a subcommand rather than shelling out to curl, because
the image has no shell and no curl.

**Raise Redis's pub/sub output buffer limit.** The default —
`client-output-buffer-limit pubsub 32mb 8mb 60` — will disconnect the gateway during a
broadcast burst, and the resulting resubscribe leaves it immediately behind again. The
oscillation is stable, not transient, and presents as `st_bus_reconnects_total` climbing
against a Redis that looks perfectly healthy. Set it generously (`256mb 64mb 60`) and watch
`st_bus_intake_depth`.

Use a dedicated Redis database index. Sharing one with a cache means someone's `FLUSHDB`
takes realtime down with it — pub/sub is unaffected by `FLUSHDB`, but the surrounding
confusion is not worth it.

## 4. The reconnect storm

The one operational risk this architecture creates that a hosted service does not: every
new connection is a synchronous HTTP call into an app with a fixed worker pool. Restart a
replica and every client on it comes back at once.

Rough model: with N clients returning over a window of J seconds and an auth latency of L,
the app sees `N/J × L/1000` concurrent requests. 5,000 clients, a 1-second window and 40 ms
latency is 200 concurrent authentications against — commonly — 16 workers. That is an
outage of the whole application, not just realtime.

Three controls, in order of effect:

1. **Server-directed `retry_after`.** The gateway spreads reconnects across
   `server.drain_spread` because it knows how many connections it is dropping and the
   client does not. Relying on client-side backoff alone does not work: at the first
   attempt any sensible formula still fires within a second or two, which *is* the window
   above. Backoff widens only after the application has already fallen over.
2. **`app.webhook_concurrency`, `connect_queue`, `connect_timeout`.** Cap what the gateway
   will issue, how many may wait, and for how long. Overflow closes 3008 — retryable, with
   a `retry_after`. Never 3001, which is permanent.
3. **`app.cache_ttl`.** Off by default and helps least: N reconnecting users are N distinct
   cookies and therefore N cache misses. It earns its place against multiple tabs and rapid
   flapping, not against a storm — and it costs revocation latency.

Rehearse it: kill a replica in staging with realistic connection counts and watch the
application's request queue. The failure mode is not subtle once you have seen it.

## 5. Metrics

Prometheus on `/metrics`.

| Metric | Type | Labels | Watch for |
|---|---|---|---|
| `st_connections_current` | gauge | app | Capacity, and a cliff during deploys |
| `st_connections_total` | counter | app, result | `result="origin_rejected"` climbing = probing or a misconfigured origin |
| `st_connection_duration_seconds` | histogram | app | A drop in median = something reaping sockets, usually a proxy timeout |
| `st_subscriptions_current` | gauge | app, namespace | |
| `st_messages_published_total` | counter | app, namespace | |
| `st_messages_delivered_total` | counter | app, namespace | Ratio to published = average fan-out |
| `st_messages_dropped_total` | counter | reason | `oversize`, `malformed`, `no_subscriber`, `intake` — the last means the bus intake channel filled and the reader dropped rather than blocking, which is the M8 behaviour and a sign the dispatch workers are behind |
| `st_webhook_duration_seconds` | histogram | app, status | The app's auth latency, which drives §4 |
| `st_webhook_inflight` | gauge | app | Sitting at the cap = storm in progress |
| `st_webhook_requests_total` | counter | app, status | 401 rate = revocations or a broken session |
| `st_bus_subscriptions_current` | gauge | | Should track distinct active channels |
| `st_bus_reconnects_total` | counter | | Climbing with a healthy Redis = output-buffer eviction, §3 |
| `st_bus_intake_depth` | gauge | | Sustained non-zero = dispatch workers behind the bus reader |
| `st_bus_sync_failures_total` | counter | | Reconciler cannot reach Redis; channels may be locally held but not subscribed |
| `st_origin_rejected_total` | counter | | Probing, or a misconfigured allowlist |
| `st_control_rejected_total` | counter | reason | Unsigned or stale control messages |
| `st_slow_consumer_disconnects_total` | counter | app | Sustained non-zero = `outbound_queue` too small, or genuinely bad clients |
| `st_subscribe_denied_total` | counter | app, namespace | A spike = a client bug, or someone probing |

The `app` label is `app.name` and is constant for the process; it is attached once, at
construction, not passed at each call site. The `namespace` label is the namespace block
that governs the channel, never the channel itself — `room-4410` is labelled
`namespace="room"`. A channel whose namespace has no block and for which no reserved `""`
block is configured is labelled `namespace="_other"` rather than by its own name. That
fold matters on `st_subscribe_denied_total`, which is the one family a client can drive:
a client subscribing to `probe1-x`, `probe2-x`, … would otherwise mint a time series per
attempt, which is the cardinality failure `06-channels.md` §2 describes, available on
demand to anyone who can open a socket. `_other` cannot collide with a real namespace
because a channel beginning `_` is reserved and refused (`06-channels.md` §4).

Alert on: `st_bus_reconnects_total` increasing, `st_webhook_inflight` at cap for more than
a minute, `st_slow_consumer_disconnects_total` rate above baseline,
`st_connections_current` dropping sharply outside a deploy.

## 6. Logs

JSON, one line per event. Never a cookie, an `Authorization` header, a webhook body, or a
message payload (NFR-7).

```json
{"level":"info","event":"conn.open","app":"main","client":"8f2c1e04a7b3d915","user":"u-7","origin":"https://app.example.com"}
{"level":"warn","event":"conn.slow","client":"8f2c1e04a7b3d915","queue":256,"channel":"room-4410"}
{"level":"warn","event":"origin.rejected","origin":"https://evil.example","ip":"203.0.113.9"}
```

The client id is the join key across every line for a connection, and the thing to ask
for when someone reports a problem.

## 7. Runbook

**Clients disconnect every N seconds, N ≈ 60.** A proxy idle timeout below
`ping_interval`. Check §2. `st_connection_duration_seconds` will show a suspiciously tight
median.

**Nobody receives anything, connections are fine.** Check `/ready` — if the bus is down,
connections stay open and silent by design (NFR-8). Then check the channel name: the
publisher's key must be `{bus.prefix}{channel}` exactly. `GET /channels` on the admin API
tells you what the gateway thinks is subscribed. This is the failure mode Redis publishing
buys us, and it is why that endpoint exists.

**Some users receive, others do not.** Almost always `bus.kind: memory` with more than one
replica. The startup warning will be in the logs.

**`st_bus_reconnects_total` climbing, Redis healthy.** Pub/sub output buffer eviction.
Raise `client-output-buffer-limit pubsub` (§3) and check `st_bus_intake_depth`. The obvious
reading — "unstable Redis" — points at the wrong system.

**A channel is locally subscribed but receives nothing, and `/ready` is 200.** Check
`st_bus_sync_failures_total`. The reconciler retries, so this should clear; if it does not,
Redis is refusing subscriptions.

**Application falls over on deploy.** §4.

**`st_slow_consumer_disconnects_total` climbing.** Either `outbound_queue` is too small for
the message rate, or a genuine population of bad connections. Check whether the same
clients recur; if the rate scales with publish volume rather than client count, raise the
queue.

**401s from the webhook after a deploy.** The application is returning 401 where it means
500 — most often a session backend that is not up yet. That combination locks users out
with `reconnect: false`, so it is worth a specific check (`04-integration.md` §1.3).

## 8. Capacity

Rough sizing to start from, to be replaced with measurements:

**Target deployment: 20,000 concurrent connections, two replicas, 10,000 each.**

- ~35 KB per idle connection **with `read_buffer`/`write_buffer` at 2 KiB** and compression
  off. Left at a library's 4 KiB defaults this roughly doubles.
- Outbound queues cost ~4 KiB per connection, not `depth × message size`, because the queue
  holds pointers to one shared buffer (`09-internals.md` §5).
- So: **~350 MiB per replica in steady state, ~525 MiB while one replica carries the whole
  fleet** during a rolling deploy. A 1 GiB container is comfortable, and NFR-1's 15,000 is
  the headroom that makes single-replica operation survivable rather than the target.
- Redis at this scale is doing nothing interesting: one message per publish per replica
  holding the channel, over two subscriber connections.
- Steady-state authorization load on the application, at `max_expiry: 6h`: **about one
  request per second.** Not a factor.

The number that actually constrains this deployment is not the gateway — it is the
application's worker pool during a reconnect. Losing one replica returns 10,000 clients at
once. Spread over `server.drain_spread: 60s` at 40 ms auth latency that is ~7 concurrent
requests against a typical 16-worker pool, which is fine. At 30s it is ~13, which is tight.
**Without the spread, in a one-second window, it is 400 concurrent — a full application
outage.** That is why `retry_after` (`03-client-protocol.md` §7.1) is not an optimization
here; it is the thing that makes this scale work on a request/response application.
