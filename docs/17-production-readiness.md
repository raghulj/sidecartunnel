# 17 — Production Readiness

What is required once everything is complete. Every row is a state to verify before real
users, not an intention.

Scope is the target deployment in `10-operations.md` §8: **20,000 concurrent connections
across two replicas**, either able to carry the fleet alone. Thresholds below are derived
from that arithmetic and are starting points to be replaced with measurements.

Reading order: this table, then [`16-integration-guide.md`](16-integration-guide.md) for
the application side, then [`10-operations.md`](10-operations.md) §7 for the runbook.

## 1. Infrastructure

### 1.1 Redis

| Item | Required value | Why | Verify |
|---|---|---|---|
| `client-output-buffer-limit pubsub` | **`256mb 64mb 60`** | The default `32mb 8mb 60` evicts the gateway during a broadcast burst. The resubscribe leaves it behind again, so the oscillation is stable, not transient. | `CONFIG GET client-output-buffer-limit` |
| Database index | Dedicated, not shared with a cache | Pub/sub survives `FLUSHDB`; the confusion around it does not | `bus.url` ends in a distinct index |
| Version | **6.0+** | — | `INFO server` |
| Reachability | From every process that publishes, including workers and cron | A publish from a process that cannot reach Redis is silent | Publish from each process type |
| `maxmemory-policy` | Any. Pub/sub holds no keys. | Documented so nobody tunes it for this | — |
| Persistence | Not required | Redis holds nothing but in-flight delivery | — |
| Failover | Single instance is supported. Cluster and sharded pub/sub are **not built**. | `12-roadmap.md` §2 | — |
| Reconnect churn after **1 hour** of load | `bus_reconnects` flat | A `bus_reconnects` count that climbs while `bus_connected` stays true is output-buffer eviction, not an unstable Redis | Curl `GET /ready` twice and compare `bus_reconnects` (§4.1) |

### 1.2 Proxy And TLS

| Item | Required value | Why | Verify |
|---|---|---|---|
| Idle timeout | **Above `server.ping_interval`**, default 25s. **120s** is a safe value. | A lower timeout reaps healthy sockets on a fixed cadence | Hold one connection open **10 minutes** |
| `Upgrade` and `Connection` | Passed through | No upgrade otherwise | A successful handshake |
| `Origin` | Forwarded unmodified | The allowlist depends on it | Foreign origin returns **403** |
| `X-Forwarded-For` | Set by the proxy, proxy CIDR in `server.trusted_proxies` | Without the CIDR the gateway reports the proxy's address for every user, flattening app-side rate limiting and audit logging | `X-St-Forwarded-For` at the webhook is the real client |
| Buffering | Disabled on the websocket route | Buffering a websocket stalls delivery | — |
| Routing | `example.com/ws` → gateway, `example.com/` → application | Same origin is what attaches the cookie | Cookie present at the webhook |
| TLS | Terminated in front of the gateway. **`wss://` only** outside localhost. | A cookie crosses this connection | Browser console shows `wss:` |
| Certificate | Covers the **application's** hostname | There is no separate websocket hostname | — |
| HTTP version | HTTP/1.1 on the upgrade hop | HTTP/2 does not carry RFC 6455 upgrades | — |

### 1.3 DNS And Placement

| Item | Required value | Why |
|---|---|---|
| Hostname | **The same hostname as the application** | A separate `ws.example.com` makes the socket cross-origin, drops the cookie, and needs `SameSite=None` — which removes the browser's own defence against cross-site websocket hijacking |
| Load balancing | No sticky sessions | Replicas share nothing; a publish anywhere reaches subscribers everywhere |
| Replicas | **2 minimum** | One replica means every deploy is a full disconnect |
| Placement | Gateway and application on the same private network as Redis | Redis is a trust boundary; anything that reaches it can publish to any channel |

### 1.4 Host Limits

| Item | Required value | Why |
|---|---|---|
| File descriptors per replica | **≥ 32,768** | `limits.max_connections` defaults to **25,000**, each a socket, plus Redis connections and headroom |
| Container memory | **1 GiB** per replica | ~350 MiB steady state, **~525 MiB** while one replica carries the whole fleet during a rolling deploy |
| Ephemeral ports at the proxy | Sized for 20,000 upstream connections | The proxy holds one connection per client to the gateway |
| Container runtime | Non-root, no shell — the shipped image is distroless | The process handles every user's cookie in transit |

## 2. Application

| Item | Required state | Failure if missing |
|---|---|---|
| Connect webhook | Deployed, signature verified **before parsing anything else** | A malformed header becomes an unauthenticated 500, retried by the gateway and by every client |
| Malformed `X-St-Timestamp` | **403**, not 500 | As above |
| 401 versus 5xx | Distinguished, and the 5xx path tested by stopping the session backend | 401-for-5xx locks every user out for the life of their page. 5xx-for-401 turns a revocation into an infinite retry loop. |
| `expires_in` | **21600** (6h) unless measured otherwise | At 300s, 20,000 connections cost a permanent **67 req/s** of authorization |
| Webhook latency | **p99 under 200 ms** | It drives the reconnect-storm arithmetic; `server.drain_spread: 60s` assumes 40 ms |
| Webhook route exclusions | No CSRF check, no login redirect, no rate limiter on this route | A 302 or 429 is not in the response table and is treated as unparseable |
| `?since=` reconciliation | On **every** endpoint whose data arrives by push | Delivery is at-most-once. Every deploy silently loses messages, invisibly. |
| Cursor | Monotonic and total — an id or a sequence, **never a timestamp** | Two inserts in one millisecond and a row is skipped |
| Page bound | Hard `limit` plus `has_more` | A client asleep four hours asks for everything |
| Frontend reconciliation | On **every** reconnect, not only first load | The gap is per reconnect |
| Merge | Idempotent and order-independent on the payload id | The gateway generates no duplicates; a retried task does |
| Channel scheme | Separator immediately after every identifier | `org-42-*` accidentally granting `org-421-*` |
| Grants | No `*`, no bare namespace wildcard such as `user-*` | `user-*` grants `user-8-billing` to user 7 |
| `grants_for()` | Calls the same predicate as the HTTP route guard, with a test asserting agreement | A second rule set drifts silently |
| Grant set size | Enumerable, and under `limits.max_subscriptions_per_conn` (**500**) | No per-subscribe callback exists (`12-roadmap.md` §3.1) |
| Publishes | **After commit** | A client reconciles against a transaction it cannot see |
| Channel names | Built in exactly one function | A format string at eleven sites is wrong at one |
| `X-St-Client` | Sent on every write whose event should not echo to its own tab | The originating tab renders twice |
| Envelope size | Under **32 KiB** | Dropped, logged once, counted; the publisher is not told |
| Control `disconnect` | Published wherever the application already revokes access | Revocation waits up to `expires_in`, default **6h** |

## 3. Security

| Item | Required state | Verify |
|---|---|---|
| `server.allowed_origins` | Every exact origin, scheme included. No wildcards, no suffix matching. | A foreign origin returns **403** and logs `origin rejected` with the origin and remote address |
| `server.allow_missing_origin` | **`false`** | Enabling it removes the defence for browsers too |
| `app.webhook_secrets` | **≥ 32 bytes** each, from a CSPRNG | Length check in config validation; the process refuses to start otherwise |
| `control.secret` | **≥ 32 bytes**, and **different** from the webhook secret | Distinct values in the secret store |
| Secret delivery | `_FILE` suffix or a secret manager. Never an image layer, a Dockerfile, a compose file or the repository. | `docker history` shows no secret |
| Secret rotation | **Rehearsed**, not merely possible | See §3.1 |
| `/ready` | Wired to readiness only, **never to a liveness probe** | A Redis restart makes every replica unready at once; on a liveness probe that kills the fleet and converts an 8-second blip into a full outage |
| `/health` | Wired to liveness. Never consults the bus. | The container healthcheck runs `sidecartunnel healthcheck` |
| Proxy | Forwards only `server.path` to the gateway | `/health` and `/ready` carry no credential; they are unreachable from outside only because nothing routes to them, which is defence in depth, not the reason they are safe to expose |
| Logs | No cookie, no `Authorization` header, no webhook body, no message payload, at any level | Grep a day of logs for a session cookie name |
| Channel names | Contain nothing secret | Names appear only in logs, by design |
| Session cookies | `Secure`, `HttpOnly`, `SameSite=Lax` or stricter | `SameSite=None` removes the browser's own cross-site websocket defence, leaving only the Origin allowlist |
| Redis | Reachable only from the private network. Password or ACL set if it crosses any boundary. | Anything that reaches Redis can publish to any channel — **not defended** |

### 3.1 Secret Rotation

Rotation must never require the gateway and the application to restart at the same instant.
`app.webhook_secrets` is a list for that reason, and the application must accept every
secret it is configured with.

| Step | Gateway | Application |
|---|---|---|
| 1 | `["old"]` | accepts `["old"]` |
| 2 | `["old"]` | accepts `["old", "new"]` — deploy |
| 3 | `["new", "old"]` — deploy, signs with `new` | accepts `["old", "new"]` |
| 4 | `["new"]` — deploy | accepts `["new"]` — deploy |

Every step is independently deployable and independently reversible. Verifying against a
list must iterate all entries with a constant-time comparison rather than short-circuiting.

Rehearse the whole sequence in staging. A rotation procedure that has never been run is a
procedure that does not exist, and the first time it is needed will be the time it is
needed quickly.

## 4. Operations

### 4.1 Observability

There is no metrics endpoint and no admin listener. `internal/metrics` and `internal/admin`
do not exist; the gateway serves exactly one HTTP listener, `server.listen`, carrying the
websocket route plus `/health` and `/ready` — everything else is a 404. The admin listener
existed to keep `/channels` off the public internet by construction, which defends against a
proxy misconfiguration rather than an attacker; it cost a package, a second server, two
config keys and a credential for a convenience over grepping a log the gateway already
writes. What remains is smaller and has to be read, not scraped:

| Surface | Carries | Use |
|---|---|---|
| Logs, one JSON line per event | `level`, an event message, and the **client id** on every connection-scoped line | The client id ties every line for one connection together (§4.3) |
| `GET /health` | 200 while the process runs, no body, no credential. **Never consults the bus** | The only correct liveness target. Wiring liveness to `/ready` instead kills the fleet during a Redis restart (`README.md`, Health Checks) |
| `GET /ready` | `{"ready":bool,"bus_connected":bool,"bus_down_for_seconds":float,"bus_reconnects":int,"draining":bool}`, no credential | This replica's bus state, right now. `bus_reconnects` is cumulative for the process's life — read it by curling twice and comparing |
| `sidecartunnel healthcheck` | Exit 0 or 1 | The container healthcheck. A loopback `GET /health`, and a subcommand rather than a `curl` line because the release image is distroless: no shell, no curl |
| Subscribe/unsubscribe log lines | `client`, `channel` | What this replica believes is subscribed. Publishing goes over the bus with no error channel back to the caller, so a publish that reaches nobody and one that reaches ten thousand subscribers look identical without a grep of this log |

That is the entire HTTP surface. Against a running gateway, `/health` and `/ready` answer
200 and **every other path answers 404** — `/metrics`, `/channels`, `/disconnect` and
`/admin` included — and there is nothing listening on the port the admin listener used to
hold. Verified in `16-integration-guide.md` §13.

Neither `/health` nor `/ready` carries a credential. Each leaks only "this process is up"
and "this process can reach Redis" — what a load balancer needs, and what every health
endpoint on the internet already says. In the documented deployment the proxy forwards only
`server.path`, so neither is publicly reachable anyway; that is defence in depth, not the
reason they are safe to leave open.

Ship stdout wherever the platform already collects it — the container runtime's log
driver, journald, a shipper. There is no scrape target to stand up and no dashboard to
build; `log.level` and `log.format` (`08-config.md` §3) are the only knobs.

Application-side, the webhook client logs `duration_ms`, `cached` and the outcome on every
call. That is the webhook's request rate, error rate and p99 latency, read off the same
lines rather than scraped separately.

### 4.2 Alerts

Every alert here is keyed on a log line, on `/ready`, or on a platform signal already
available (restart count, memory, CPU, the Docker `HEALTHCHECK` result). Thresholds sized
for **20,000 connections across two replicas**; replace with measured baselines after the
first month.

| Alert | Condition | Severity | Means |
|---|---|---|---|
| Not ready | Load balancer's health check sees `/ready` return 503 past `bus.ready_grace` (**30s**) | **page** | Bus down longer than the grace window (§1.1) |
| Webhook refused / unavailable / rejected the gateway | Rate of the matching log line above baseline (§4.1) | **page** | 401-for-500, an app that cannot authorize, or a bad `app.webhook_secrets`/clock (§3.1) |
| Replica unhealthy | Restart count > 0 outside a deploy, or the Docker `HEALTHCHECK` failing | **page** | Crash loop, or the process is up but not answering |
| Connection cliff | Load balancer's active-connection count for an upstream drops **>30%** in 5m outside a deploy | **page** | Proxy, gateway or application failure |
| Origin / control rejected | Rate of `origin rejected` / any `control message rejected` line | ticket | Probing or a missing allowlist entry; secret mismatch, clock skew, or a forged publish |
| Slow consumers | Rate of `connection closed` with `reason":"outbound queue full"` above 3× baseline | ticket | `outbound_queue` too small, or genuinely bad clients |
| Capacity | Open file descriptors per replica, or the load balancer's per-upstream count, approaching `limits.max_connections` headroom (§1.4) | ticket | Add a replica |

Four of the old alerts cannot be rebuilt — the resync and a failed `Sync` retry were always
silent, so bus eviction and bus sync failures have no signal beyond polling `/ready` for a
`bus_connected:false` blip (or a climbing `bus_reconnects` while `bus_connected` stays true)
and comparing the subscribe log to what the publisher expects. A grant denial goes to the
client and nowhere else; verify by hand (§4.3, §5.3). Webhook saturation has no gauge or log
line; rising `duration_ms` is the only indirect warning.

Two alerts must not exist: anything wired to `/ready` as a **liveness** probe, and any
alert that restarts replicas automatically on bus loss. Both convert a short Redis blip
into a full application outage as every client re-authorizes at once.

### 4.3 Runbook

`10-operations.md` §7 is the runbook. On call needs these entries reachable in under a
minute:

| Symptom | First check |
|---|---|
| Disconnects every ~60s | Proxy idle timeout below `ping_interval` (§1.2) |
| Nobody receives anything, connections fine | `/ready`, then `grep '"msg":"subscribe"'` for the channel name against `client` |
| Some users receive, others do not | `bus.kind: memory` with more than one replica |
| Connections keep dropping and resubscribing, Redis is healthy | Pub/sub output buffer (§1.1). No counter distinguishes it from an unstable Redis directly — poll `/ready` and watch `bus_reconnects` climb while `bus_connected` stays true |
| Application falls over on deploy | Reconnect storm, `10-operations.md` §4 |
| 401s from the webhook after a deploy | The application is returning 401 where it means 500 |
| A channel is subscribed locally but silent, `/ready` is 200 | `Sync` failed and is retrying silently — it is never logged. Compare the subscribe log against what the publisher believes it holds; that comparison is the whole signal |
| Connections closing with `"reason":"outbound queue full"` in the logs | `outbound_queue` too small for the message rate, or genuinely bad clients |

The **client id** is the join key across every log line for a connection. It is the value
to ask a reporting user for, and the value to grep.

Rehearse before going live, in staging, at realistic connection counts:

| Drill | Expected |
|---|---|
| Kill one replica | Reconnects spread over `server.drain_spread` (**60s**); the application's request queue stays flat |
| Stop Redis for **10 seconds** | Connections stay open and silent. `/ready` stays 200 for `bus.ready_grace` (**30s**). No replica restarts. |
| Roll the application | Connections recover. **No 401 during the window.** |
| Suspend an account | Every connection for that user closes with **3501** in under **1 second** |
| Rotate the webhook secret | Four steps, §3.1, no connection lost |

The reconnect storm is the one operational risk this architecture creates that a hosted
service does not, and the failure mode is not subtle once it has been seen.

### 4.4 Capacity

Target: **20,000 concurrent connections, two replicas, 10,000 each.**

| Dimension | Value | Note |
|---|---|---|
| Memory per connection | **~35 KB** | With `read_buffer`/`write_buffer` at **2 KiB** and compression off. Library defaults of 4 KiB roughly double it. |
| Memory per replica, steady | **~350 MiB** | |
| Memory per replica, one carrying the fleet | **~525 MiB** | The rolling-deploy case. Size the container for this. |
| Container memory | **1 GiB** | Comfortable at the above |
| `limits.max_connections` | **25,000** | Per replica, so either can carry the fleet alone |
| `limits.compression` | **off** | Each `permessage-deflate` context is ~256 KiB — **5 GiB** at 20,000 connections |
| `limits.outbound_queue` | **256** | ~4 KiB per connection: the queue holds pointers to one shared buffer, not copies |
| Redis connections | **2** subscriber connections per replica | Redis is doing nothing interesting at this scale |
| Authorization load, steady | **~1 request/second** | At `expires_in: 6h` across 20,000 connections |
| Reconnect after a replica dies | 10,000 clients over **60s** at 40 ms latency → **~7 concurrent** authorizations | Fine against a 16-worker pool |
| The same, at `drain_spread: 30s` | **~13 concurrent** | Tight |
| The same, without the spread | **~400 concurrent** | A full application outage, not a realtime one |

The number that constrains this deployment is not the gateway. It is the application's
worker pool during a reconnect. `retry_after` is not an optimization here; it is what makes
the design work on a request/response application.

Scale by replica count. Nothing is sticky and nothing is shared, so adding a replica adds
`max_connections` and two Redis subscriber connections. Adding replicas does **not** reduce
the reconnect load on the application — it increases the number of independent events that
can cause one.

## 5. Rollout

### 5.1 Parallel Run

Full detail in `16-integration-guide.md` §11.3. The shape:

| Phase | Duration | Frontend renders from | Rollback |
|---|---|---|---|
| 1. Dual publish | 1 week | Old system | Remove the Redis publish |
| 2. Shadow connect | 1 week | Old system; socket open and ignored | Stop opening the socket |
| 3. Cohort cutover | 1–2 weeks | Gateway for a percentage of sessions | Set the percentage to 0 |
| 4. Full cutover | — | Gateway | Set the percentage to 0 |
| 5. Removal | — | Gateway | Redeploy the previous release |

Cohort by session, not by request. A user whose page renders from the gateway on one
request and from the old system on the next will see duplicates, which proves nothing about
either system.

### 5.2 Rollback

| Fault | Rollback | Time |
|---|---|---|
| Gateway misbehaving | Cohort percentage to **0** | Seconds. No deploy. |
| Webhook wrong | Application deploy | One deploy cycle |
| Channel scheme wrong | Application deploy, then a control `refresh` per affected user | One deploy cycle |
| Gateway config wrong | Redeploy the gateway | Every connection drops and reconnects — expect a storm |
| Everything | Cohort to **0**, leave the gateway running | Seconds |

The application-side changes — the webhook, the Redis publish, `?since=` — are **additive**.
They are inert without a gateway and can stay deployed through any rollback. Only the
frontend cohort flag needs to move quickly, so it must be a runtime value, not a build-time
one.

Publishing to Redis with no gateway running is a null sink. That is the property that makes
phase 1 free.

### 5.3 The First Hour

Watch, in this order:

| # | Signal | Healthy | Act if |
|---|---|---|---|
| 1 | Rate of `connect webhook refused the connection` in the logs | Matches the known revocation rate | Any spike. **Roll back.** A wrong 401 closes with `reconnect: false` and users do not come back until they reload. |
| 2 | Rate of `connect webhook unavailable` / `rejected the gateway's request` | Zero | Non-zero. The application cannot authorize, or the gateway's own secret or clock is wrong. |
| 3 | `duration_ms` on the webhook log lines | Under **200 ms** and flat | Rising — it drives the storm arithmetic |
| 4 | Application request queue depth | Flat | Any correlation with the rollout |
| 5 | Active connections at the load balancer | Rises to the cohort size and holds | A sawtooth means something is reaping sockets, usually the proxy |
| 6 | `connection closed` reasons in the logs | Few, and not clustered | A cluster near one duration is a proxy idle timeout (`ping_interval` is **25s**) |
| 7 | Rate of `origin rejected` | 0, or a low probing floor | Correlated with the rollout — an origin is missing from the allowlist |
| 8 | `/ready`, polled every few seconds | Always 200, `bus_connected: true` | A `bus_connected:false` blip against a healthy Redis is output-buffer eviction (§1.1) |
| 9 | Rate of `connection closed` with `"reason":"outbound queue full"` | Near zero | Scaling with publish volume means `outbound_queue` is too small |
| 10 | The subscribe log against the application's write rate | Every channel the application writes to appears, with a `client` | A channel absent while writes continue is a wrong channel name |

Two failure modes leave no log line and no signal on `/health` or `/ready`, and must still
be checked by hand in the first hour:

- **Missing reconciliation.** Nothing reports it. Restart one replica and confirm a client
  ends up holding every message published during the gap.
- **A too-loose grant.** Nothing reports it either — the gateway allowed what it was told to
  allow. Subscribe to another tenant's channel from a test account and confirm error **103**.

The first deploy after cutover is the real test. It is the first time every connection
reconnects at once against a live webhook, and it is where a collapsed 401/5xx and a missing
`?since=` both surface at the same moment.
