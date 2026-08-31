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

One port, and one listener behind it: the websocket endpoint, `GET /health` and
`GET /ready`. There used to be a second listener on `:9001` carrying an operator API, kept
off the network by binding loopback; it is gone along with the API it was protecting
(`12-roadmap.md` §2).

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

Forward the websocket path and nothing else. `GET /health` and `GET /ready` share the
listener, and a proxy that forwards `/` to the gateway publishes both. They say only "this
process is up" and "this process can reach Redis" — which is what a load balancer needs,
and what every health endpoint on the internet already says — so publishing them is not a
disclosure. It is still not the intent, and a `handle /ws*` rule keeps it from happening by
accident.

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
the image has no shell and no curl. It performs a loopback `GET /health` against
`server.listen` and exits 0 or 1. Liveness only, and never the bus: a bus-dependent
healthcheck restarts every container during a Redis restart, which turns an eight-second
blip into a full outage. The same rule is why `/ready` must never be a liveness probe — see
`README.md`'s Health Checks section for the Kubernetes pair.

**Raise Redis's pub/sub output buffer limit.** The default —
`client-output-buffer-limit pubsub 32mb 8mb 60` — will disconnect the gateway during a
broadcast burst, and the resulting resubscribe leaves it immediately behind again. The
oscillation is stable, not transient, and presents as `bus_reconnects` on `/ready` climbing
against a Redis that looks perfectly healthy. Set it generously (`256mb 64mb 60`), and raise
`bus.dispatch_workers` if the workers are falling behind the reader.

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

## 5. Observability

There is no metrics endpoint. Prometheus was cut from this project deliberately, and
`12-roadmap.md` §2 records why: eighteen families were specified before any code existed,
nine of them ended up with no producer at all and sat permanently at zero, and a gauge
reading zero because nothing increments it is indistinguishable from one reading zero
because nothing is wrong. An alert written against a family in that second group never
fires, and the conclusion drawn from its silence is the opposite of the truth.

There is no operator API either. It went in the same change and for a related reason: the
separate loopback listener existed so that a proxy misconfiguration could not expose
`GET /channels` publicly, which defends against a configuration mistake rather than an
attacker, and it cost a package, an HTTP server, two configuration keys and a credential.
`POST /disconnect` was a second door onto the control channel's disconnect action.
`GET /channels` was a convenience over grepping a log this process already writes.

What is left is smaller and honest: the structured logs of §6, and two probes.

### 5.1 The Probes

Two routes, on `server.listen`, alongside the websocket endpoint. Everything else is 404.

| Route | Auth | Answers |
|---|---|---|
| `GET /health` | none | Is the process alive. Never consults the bus |
| `GET /ready` | none | Is this replica taking traffic: 503 while draining, and 503 once the bus has been down longer than `bus.ready_grace` |

Neither carries a credential, and neither needs one. They report that the process is up and
that it can reach Redis — the two facts a load balancer is asking for, and the two facts
every health endpoint on the internet already publishes. §2's routing rule keeps them off
the public listener anyway, but that is defence in depth and not the reason they are safe.

`GET /ready` carries four fields beyond the status code:

```json
{"ready":true,"bus_connected":true,"bus_down_for_seconds":0,"bus_reconnects":0,"draining":false}
```

`bus_reconnects` is cumulative for the life of the process, so it is read by curling twice
and comparing. Climbing while `bus_connected` is `true` is pub/sub output-buffer eviction
and nothing else — see §7. `draining` separates "this replica is going away" from "this
replica cannot reach Redis", which are the same status code and very different incidents.

### 5.2 What To Watch

| Question | Where to look |
|---|---|
| Is the fleet losing connections? | Rate of `connection closed` in the log, grouped by `code` |
| Is a deploy about to take the application down? | Rate of `connected`, against §4's model |
| Is the application refusing everyone? | `connect webhook refused the connection` with `status` 401. The `connect refused by the application` line beside it carries only `client` |
| Is Redis evicting this replica? | `bus_reconnects` on `/ready`, sampled a minute apart |
| Is this replica out of the load balancer? | The `/ready` status code |
| Is anybody subscribed to the channel being published? | `grep '"msg":"subscribe"'` for the channel name |
| Where is this user, and on which replica? | `client` in the logs, joined across the connection's lines |

## 6. Logs

JSON, one line per event, on stderr. Never a cookie, an `Authorization` header, a webhook
body, or a message payload (NFR-7).

```json
{"time":"...","level":"INFO","msg":"connected","client":"1042187c649c01bd","user":"u-7","subs":3}
{"time":"...","level":"INFO","msg":"subscribe","client":"1042187c649c01bd","channel":"room-4410"}
{"time":"...","level":"INFO","msg":"connection closed","client":"1042187c649c01bd","code":3501,"reason":"account suspended"}
{"time":"...","level":"INFO","msg":"control applied","action":"disconnect","user":"u-7","client":""}
{"time":"...","level":"WARN","msg":"origin rejected","origin":"https://evil.example","remote":"172.23.0.5:33218"}
{"time":"...","level":"WARN","msg":"control message rejected","reason":"stale","err":"control message rejected: ts is 1m40s outside the ±5m0s window; check this replica's clock"}
```

**`client` is the join key.** Every line a connection produces carries it, from the
`connected` that opens it to the `connection closed` that ends it. It is the thing to ask
for when someone reports a problem, and it is what the control channel's disconnect action
targets (`04-integration.md` §3), so a line in the log is directly actionable.

The lines worth building a query on:

| Message | Level | Fields | Means |
|---|---|---|---|
| `connected` | info | `client`, `user`, `subs` | A handshake completed. The rate is the connect rate; a spike outside a deploy is §4 |
| `subscribe` | info | `client`, `channel` | One channel is now held by one connection, whether it arrived on the connect frame or as its own command. This is what replaced `GET /channels` |
| `unsubscribe` | info | `client`, `channel` | The client gave a channel up |
| `connection closed` | info | `client`, `code`, `reason` | Group by `code`: `3005` slow consumer, `3004` ping timeout, `3000` drain, `3501` revocation, `3503` expiry |
| `subscription withdrawn` | info | `client`, `channel`, `reason` | A grant stopped covering a channel the connection held |
| `origin rejected` | warn | `origin`, `remote` | Probing, or a missing entry in `server.allowed_origins` |
| `connect webhook refused the connection` | info | `app`, `client`, `duration_ms`, `cached`, `status`, `reconnect` | The application said no. A 401 rate that jumps after a deploy is §7. `connect refused by the application` follows it carrying only `client` |
| `connect webhook unavailable` | error | `status`, `err` | The application could not answer. Closes 3008, retryable |
| `connect webhook rejected the gateway's request; check app.webhook_secrets and this replica's clock` | error | `status` | A 403: wrong `app.webhook_secrets`, or a skewed clock. Rate-limited, so one line stands for many |
| `bus message dropped` | debug | `channel`, `reason`, `err` | A published envelope did not decode, or carried no `event` or no `data`. The channel is there; the payload deliberately is not |
| `control message rejected` | warn | `reason`, `err` | `unsigned`, `stale` or `malformed` — three different people to talk to (`04-integration.md` §3). `control applied` (`action`, `user`, `client`) is the line for one that was accepted |
| `sidecartunnel started` | info | `server.listen`, `server.path`, `bus.kind`, `namespaces` | The configuration that actually took effect, once per process |

`bus message dropped` needs `log.level: debug`, as do `connect webhook authorized` (`app`,
`client`, `duration_ms`, `cached`, `user`, `expires_in_s`) and `read loop ended`. Everything
else above is at info or higher.

## 7. Runbook

**Clients disconnect every N seconds, N ≈ 60.** A proxy idle timeout below
`ping_interval`. Check §2. The signature in the log is a population of `connection closed`
lines whose gap from their matching `connected` line clusters tightly around one value —
real disconnects are spread out, a proxy timeout is not.

**Nobody receives anything, connections are fine.** Check `/ready` — if the bus is down,
connections stay open and silent by design (NFR-8). Then check the channel name: the
publisher's key must be `{bus.prefix}{channel}` exactly.

Then ask the gateway what it thinks is subscribed. Every subscribe is a log line carrying
the channel:

```
grep '"msg":"subscribe"' gateway.log | grep room-4410
```

Nothing back, on any replica, while the publisher believes otherwise, is the bug — and it
is almost always the prefix or the separator. Subtract the `unsubscribe` and
`connection closed` lines for the same `client` values to see who still holds it.

This is the failure mode Redis publishing buys us: the application publishes straight to
Redis and gets no acknowledgement, so a message that reached nobody is indistinguishable
from one that reached ten thousand sockets. The log is the only place that difference is
visible.

**Some users receive, others do not.** Almost always `bus.kind: memory` with more than one
replica. The startup warning is in the logs: grep for `bus.kind is memory`.

**`/ready` shows `bus_reconnects` climbing while `bus_connected` is true.** Pub/sub
output-buffer eviction, not an unstable Redis. Raise `client-output-buffer-limit pubsub`
(§3), and raise `bus.dispatch_workers` if the workers are behind the reader. The obvious
reading — "Redis is flapping" — points at the wrong system, which is what makes this one
worth naming.

**A channel is locally subscribed but receives nothing, and `/ready` is 200.** A
reconciliation is failing, leaving the channel held locally and dead upstream. The
reconciler retries, so it should clear on its own; if it does not, Redis is refusing
subscriptions. The diagnosis is a disagreement between two sources: the replica's log has a
`subscribe` line for the channel with no matching `unsubscribe`, while
`redis-cli PUBSUB CHANNELS '{bus.prefix}*'` does not list it.

**Application falls over on deploy.** §4.

**Connections closing with code 3005.** Slow consumers. Grep for
`"msg":"connection closed"` with `"code":3005` and group by `client`. If a handful of
clients recur, it is those clients. If the set is broad and the rate scales with publish
volume rather than with connection count, `limits.outbound_queue` is too small — raise it,
remembering that the queue holds pointers into one shared buffer, so the cost is about
4 KiB per connection rather than depth times message size (§8).

**Revoking one user's access, right now.** Publish a signed disconnect on the control
channel (`04-integration.md` §3). That is the only route: the admin API's `POST /disconnect`
was a second door onto the same hub call and went with the rest of the listener. The control
channel is the better one anyway — it is signed, it reaches every replica rather than the
one being curled, and it flushes the connect-webhook cache so a revoked user cannot
reconnect on a cached grant.

**401s from the webhook after a deploy.** Grep for `connect refused by the application`
with `"status":401`. The application is returning 401 where it means 500 — most often a
session backend that is not up yet. That combination locks users out with
`reconnect: false`, so it is worth a specific check (`04-integration.md` §1.3).

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
