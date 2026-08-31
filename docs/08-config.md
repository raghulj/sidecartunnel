# 08 — Configuration

**Normative.** Every key, its default, and its validation rule. A key that exists in code
and not here is a bug.

## 1. Sources and precedence

YAML file, overridden by environment variables. Later wins:

1. Built-in defaults
2. `--config /etc/sidecartunnel/config.yaml`
3. Environment: `ST_` prefix, `__` for nesting, e.g. `ST_SERVER__PATH=/socket`

Scalar lists are comma-separated. Any key may be supplied from a file by appending
`_FILE`, e.g. `ST_APP__WEBHOOK_SECRETS_FILE=/run/secrets/st` — the convention for Docker
and Swarm secrets. When both `X` and `X_FILE` are set, **`_FILE` wins**: a mounted secret
should outrank a stale value left in a compose file.

Three things this overlay deliberately does not do:

- **No `${VAR}` expansion.** The `${ST_WEBHOOK_SECRET}` placeholders in §4's worked example
  are shell substitution performed by whatever renders the file, not by the gateway. Supply
  the value through the environment overlay instead.
- **Unknown `ST_` variables are ignored**, unlike unknown YAML keys which are a hard error.
  The asymmetry is forced: §4's own deployment expects `ST_WEBHOOK_SECRET` to exist in the
  environment for substitution, and rejecting unrecognised `ST_` names would break it.
- **A YAML type error names the line, not the key.** `max_connections: lots` reports
  `cannot unmarshal !!str 'lots' into int` with a line number. Unknown keys, durations and
  every rule in §3 name the key as NFR-5 requires; a raw type mismatch is the one gap, and
  closing it would need a shadow struct per config block.

**Lists of objects cannot be expressed in environment variables.** `namespaces` therefore
needs either the YAML file or `ST_NAMESPACES_JSON` containing the whole list as JSON. An
earlier version of this section claimed everything was configurable by environment alone;
it was not, and the minimal env-only example it offered produced a gateway that started
cleanly, reported healthy, and refused every subscribe because it configured no namespaces.
That is now impossible: an empty `namespaces` gets a built-in `default` block.

### Subcommands

`sidecartunnel healthcheck` performs a loopback `GET /health` against `server.listen` and
exits 0 or 1. It exists so a distroless image with no shell and no curl can still declare a
container healthcheck. It checks liveness only, never the bus — see `04-integration.md` §4
on why a bus-dependent healthcheck kills the fleet during a Redis restart.

## 2. Validation

The process MUST fail to start, with a message naming the key, when any rule below is
broken (NFR-5). There is no partially-configured start and no silent default for anything
security-relevant.

```
FATAL config: server.allowed_origins is empty — refusing to start.
  Every accepted Origin must be listed explicitly. See docs/05-authorization.md §5.
```

## 3. Full reference

### `server`

| Key | Type | Default | Rule |
|---|---|---|---|
| `listen` | string | `:8000` | Valid listen address |
| `path` | string | `/ws` | Must begin `/` |
| `allowed_origins` | []string | — | **Required, non-empty.** Exact origins including scheme. No wildcards, no suffix matching. |
| `allow_missing_origin` | bool | `false` | See `05-authorization.md` §5 before enabling |
| `handshake_timeout` | duration | `5s` | 1s–60s |
| `ping_interval` | duration | `25s` | 5s–300s. Must be below any proxy idle timeout. |
| `pong_timeout` | duration | `10s` | 1s–60s, and less than `ping_interval` |
| `drain_timeout` | duration | `20s` | 1s–300s |
| `drain_spread` | duration | `60s` | 1s–300s. Window across which a retryable `disconnect`'s `retry_after` is spread — drain (3000), grant expiry and control `refresh` (3503), and an unavailable webhook (3008). At 10,000 connections per replica and 40 ms auth latency, 60s yields ~7 concurrent requests at the application; 30s yields ~13, which is tight against a 16-worker pool. The 1s floor is not cosmetic: `retry_after` is milliseconds on the wire, so a sub-millisecond window is arithmetically no window at all, and it validated cleanly before the range existed. |
| `read_header_timeout` | duration | `5s` | Guards against slowloris on the upgrade |
| `trusted_proxies` | []string | `[]` | CIDRs whose `X-Forwarded-For` is believed. Default trusts nothing, so `X-St-Forwarded-For` is the socket peer — see FR-24. |

**List proxy addresses here, not the network your clients sit on.** Trusting a CIDR means
trusting every address inside it, and the FR-24 walk steps left past every trusted hop. Set
`trusted_proxies: ["10.0.0.0/8"]` when the proxy is `10.0.0.7` and clients are also inside
10/8, and a client's own prepended hop is walked off along with the real proxy — the header
becomes attacker-controlled again. `["10.0.0.7/32"]`, the proxy itself, is the correct
form. This is the same reasoning as `allowed_origins` taking exact origins rather than a
suffix: a broad match is a trust decision about everything it covers.

`allowed_origins` has no default on purpose. A default of `["*"]` would be a security hole
shipped as a convenience, and a default of `[]` that silently accepts everything would be
worse. Refusing to start is the only honest option.

### `app`

A **single** block. Multi-app was cut (`13-review-findings.md` S1) — a second application
is a second container, which is free with a 12 MB static binary and avoids an app-routing
rule, per-app namespaces, per-app limits, and a per-app cache key.

| Key | Type | Default | Rule |
|---|---|---|---|
| `name` | string | `app` | Used in logs. |
| `connect_url` | string | — | **Required.** Absolute http/https URL, **with no credentials in it.** |
| `webhook_secrets` | []string | — | **Required**, min 32 bytes each. Signs with the first; a list exists so a secret can be rotated without simultaneous restarts. |
| `connect_timeout` | duration | `10s` | Whole authorization budget: queue wait plus the call. Exceeding it closes 3008, retryable. |
| `connect_queue` | int | `4096` | Max connections awaiting authorization. Overflow closes 3008. |
| `cookie_names` | []string | `[]` | Cookies forming the cache key. Empty = whole header. |
| `webhook_timeout` | duration | `3s` | 100ms–30s |
| `webhook_concurrency` | int | `32` | 1–4096. See NFR-4. |
| `webhook_retries` | int | `1` | 0–5. Applies to 5xx and timeout only, never to 401/403. |
| `cache_ttl` | duration | `0` | **Off by default.** Keyed on a hash of `cookie_names`. Flushed entirely by any control `disconnect`, because a cached entry otherwise survives a revocation. |
| `min_expiry` | duration | `60s` | Clamps a webhook's `expires_in` from below |
| `max_expiry` | duration | `6h` | Clamps from above. The clamped value is what the client is told. |

`connect_url` refuses userinfo — `https://gw:hunter2@webapp.internal/_st/connect` fails
at startup, naming the key and quoting nothing. That shape is ordinary for an internal
endpoint, and the gateway formats the configured URL into its own error strings, which
reach a `warn` line on every timed-out connect: one logged password per reconnecting
connection, every time the application restarts. It buys nothing either, because the
webhook is already authenticated to the application by the HMAC over `webhook_secrets`. Put
credentials in a header the application checks, or in the secret. Any URL the gateway does
name in an error or a log line has its userinfo replaced first, `bus.url` included —
`redis://:password@host` is the documented way to give the bus a password, so there the
userinfo is accepted and only the echo is refused (NFR-7).

`refresh_at`, `refresh_retries` and `bus_prefix` are gone. Grants now expire by
re-handshake rather than revalidation (`13-review-findings.md` S3), so there is no refresh
schedule to tune, and with one app there is one `bus.prefix`.

`max_expiry` defaults to **6h**, not 1h. Long expiry is now safe because revocation no
longer depends on it — the control channel is immediate — and each expiry costs a full
reconnect.

### `bus`

| Key | Type | Default | Rule |
|---|---|---|---|
| `kind` | string | `redis` | `redis` or `memory`. `memory` is single-process only. |
| `url` | string | `redis://localhost:6379/0` | Required when `kind: redis` |
| `dial_timeout` | duration | `3s` | |
| `reconnect_min` | duration | `200ms` | Backoff floor after bus loss |
| `reconnect_max` | duration | `10s` | Backoff ceiling |
| `intake_queue` | int | `4096` | Depth between the bus reader and the dispatch workers |
| `dispatch_workers` | int | `2` | 1–64. Fan-out workers, kept separate from the bus reader so Redis never evicts us for a slow subscriber. |
| `ready_grace` | duration | `30s` | Bus may be down this long before `/ready` reports 503 |
| `prefix` | string | `st:` | Redis channel prefix. Also the hub's internal key prefix. |

`command_queue` is gone with the reconciler (`09-internals.md` §4.2).

`kind: memory` exists for tests and single-node development. Starting with `memory` and
more than one replica is undetectable by the gateway and produces the worst kind of bug —
messages that arrive for some users and not others — so it logs a prominent warning at
startup every time.

### `channels`

| Key | Type | Default | Rule |
|---|---|---|---|
| `separator` | string | `-` | Exactly one printable ASCII character |

### `namespaces`

List. See `06-channels.md` §3 for semantics.

| Key | Type | Default | Rule |
|---|---|---|---|
| `name` | string | — | **Required**, unique. The reserved empty name `""` owns separator-less channels; a namespace may not be named `default`. |
| `client_events` | bool | `false` | M4 |
| `rate_limit` | string | `10/s` | `<int>/<s\|m>` |
| `max_message_size` | bytes | inherits | 1–1 MiB |
| `presence` | bool | `false` | M4, rejected as unimplemented if set |
| `history_size` | int | `0` | M4, rejected as unimplemented if non-zero |

There is no `auth_required` key. It was cut (`13-review-findings.md` S4) because it
contradicted FR-5 and reintroduced the public-channel hole. A config that still sets it
fails to start with a message naming the key rather than being silently ignored.

Setting an unimplemented key is a startup error rather than a warning. A config that
claims presence is on, while presence does nothing, is a lie an operator will act on.

### `limits`

| Key | Type | Default | Rule |
|---|---|---|---|
| `max_connections` | int | `25000` | 0 = unlimited. Sized for 20,000 concurrent across two replicas, either able to carry the fleet alone. |
| `max_subscriptions_per_conn` | int | `500` | 1–10000. Exceeding it is error 108. |
| `max_connections_per_user` | int | `20` | 0 = unlimited. One looping client must not consume the global cap. Exceeding it closes 3003, `reconnect: false` (FR-25) — a retryable close would invite the loop the cap exists to stop. |
| `read_buffer` | bytes | `2048` | Socket read buffer. Library defaults of 4 KiB each are the difference between fitting the memory budget and not. |
| `write_buffer` | bytes | `2048` | Socket write buffer. |
| `compression` | bool | `false` | `permessage-deflate`. Each context is ~256 KiB; at 20,000 connections that is 5 GiB against a 1 GiB budget. Leave off unless measured. |
| `outbound_queue` | int | `256` | 16–65536. See `07-delivery.md` §4. |
| `max_message_size` | bytes | `32768` | 1–1 MiB. Published envelopes. |
| `max_frame_size` | bytes | `16384` | 1–1 MiB. Client frames. |
| `max_channel_length` | int | `255` | 16–1024 |

### `control`

| Key | Type | Default | Rule |
|---|---|---|---|
| `secret` | string | — | **Required.** Min 32 bytes. Signs control messages (FR-23). |

There was a `refresh_spread` here. It is gone: a control `refresh` closes with 3503, and
every retryable close takes its `retry_after` from `server.drain_spread` — there is one
spread window in the connection layer and there was only ever one. The key was defaulted,
validated and documented, and read by no code at all, so an operator who widened it got
`drain_spread` anyway and five times the concurrent authorization load they had asked to
avoid. A second window would also have had nothing to say that the first does not: a
`refresh` must name exactly one `user` or one `client` (§8.1 of `04-integration.md`), so
one message reaches at most `limits.max_connections_per_user` connections, and a loop over
users is paced by the loop. This is the third key in this repository to be specified and
wired to nothing; see `13-review-findings.md`.

### `log`

| Key | Type | Default | Rule |
|---|---|---|---|
| `level` | string | `info` | `debug`, `info`, `warn`, `error` |
| `format` | string | `json` | `json` or `text` |

No log level ever emits a cookie, an `Authorization` header, a webhook body, or a message
payload (NFR-7). `debug` adds frame *types* and channel names, never contents.

## 4. Worked example

```yaml
server:
  listen: ":8000"
  path: "/ws"
  allowed_origins:
    - "https://app.example.com"
  ping_interval: 25s

app:
  name: main
  connect_url: "http://webapp:5000/_st/connect"
  webhook_secrets: ["${ST_WEBHOOK_SECRET}"]
  webhook_concurrency: 32
  connect_timeout: 10s
  max_expiry: 6h

bus:
  kind: redis
  url: "redis://redis:6379/3"
  prefix: "st:"

control:
  secret: "${ST_CONTROL_SECRET}"

namespaces:
  - name: room
  - name: user

limits:
  max_connections: 25000
  outbound_queue: 256
```

Equivalent minimum by environment alone:

```
ST_SERVER__ALLOWED_ORIGINS=https://app.example.com
ST_APP__CONNECT_URL=http://webapp:5000/_st/connect
ST_APP__WEBHOOK_SECRETS=…
ST_CONTROL__SECRET=…
ST_BUS__URL=redis://redis:6379/3
```

This one genuinely works: with no `namespaces`, the built-in `default` block applies to
every channel, so subscribes succeed.
