# sidecartunnel

A Go websocket gateway for applications that cannot hold long-lived connections — Flask, Django, Rails, PHP-FPM. Terminates websockets, authorizes each connection against the application's own session cookie, and fans messages out across replicas through Redis pub/sub.

The application publishes with one `redis.publish()` call and authorizes with one HTTP endpoint. It does not import a websocket library, run an event loop, or change how it is deployed.

**Not implemented yet.** The specification is complete and lives in [`docs/`](docs/). Implementation is in progress.

## How It Works

### 1. Connect

The browser opens a websocket to the **same origin** as the application, so the session cookie is attached to the HTTP upgrade automatically. No authentication code in the frontend.

```
GET /ws HTTP/1.1
Origin: https://app.example.com
Cookie: session=eyJ1c2VyX2lkIjo3fQ...
```

The gateway checks `Origin` against an allowlist and rejects with **403** on mismatch. Browsers do not apply CORS to websocket handshakes but do attach cookies, so this check is what prevents cross-site websocket hijacking.

### 2. Authorize

The gateway forwards the cookie to one endpoint on the application, signed with HMAC-SHA256 over the timestamp, a nonce, a digest of the cookie, and a digest of the body.

```json
POST /_st/connect
{"client": "8f2c1e04a7b3d915"}
```

The application resolves its own session and answers with a user id and a list of channel glob patterns:

```json
{"user": "u-7", "channels": ["room-4410", "user-7", "org-42-*"], "expires_in": 21600}
```

The gateway matches subscription requests against those patterns as plain strings. It never learns what `org-42` means.

| Grant | Channel | Result |
|---|---|---|
| `room-4410` | `room-4410` | allow |
| `org-42-*` | `org-42-alerts` | allow |
| `org-42-*` | `org-99-secret` | **deny**, error 103 |
| `user-*` | `user-7-private` | allow — `*` matches everything after the prefix |

A **401** from this endpoint refuses the connection permanently. A **5xx** or timeout refuses it retryably. The two are not interchangeable.

### 3. Publish

```python
redis.publish("st:room-4410", json.dumps({
    "event": "order.created",
    "data": {"id": 88123},
}))
```

Every replica holding that channel delivers to its own sockets. Any process can publish — a web worker, a background task, a cron job, a shell script.

### 4. Client Writes

Durable writes go over ordinary HTTP to the application, which persists and then publishes. CSRF, rate limiting and validation apply unchanged. The socket is receive-only for anything that matters.

Send `X-St-Client` on the write so the event does not echo back to the tab that caused it.

### Delivery

At-most-once. A replica restart or a sleeping laptop drops messages. The client closes the gap on reconnect:

```
GET /api/messages?room=4410&since=88124
```

Postgres is the source of truth. Realtime is an accelerator in front of it.

## Channels

A channel is an opaque string. The substring before the first `-` selects a namespace, which selects a block of configuration. Names must be human-readable — they appear in logs, metrics labels and the admin API.

```
room-4410              namespace "room"
org-42-alerts          namespace "org"
user-7                 namespace "user"
_control               reserved, never subscribable
```

## Environment Variables

Everything is configurable by environment except `namespaces`, which needs the YAML file or `ST_NAMESPACES_JSON`. Any key accepts a `_FILE` suffix for Docker and Swarm secrets.

| Variable | Required | Default | Description |
|---|---|---|---|
| `ST_SERVER__ALLOWED_ORIGINS` | Yes | — | Exact origins, comma separated. No wildcards. Startup fails if empty. |
| `ST_APP__CONNECT_URL` | Yes | — | The application's authorization endpoint |
| `ST_APP__WEBHOOK_SECRETS` | Yes | — | Min 32 bytes. A list, so secrets can be rotated without a simultaneous restart. |
| `ST_CONTROL__SECRET` | Yes | — | Min 32 bytes. Signs control-channel messages. |
| `ST_BUS__URL` | Yes | — | Redis connection string |
| `ST_SERVER__LISTEN` | No | `:8000` | Websocket listener |
| `ST_SERVER__PATH` | No | `/ws` | Websocket path |
| `ST_SERVER__DRAIN_SPREAD` | No | `60s` | Window across which reconnects are spread on shutdown |
| `ST_BUS__PREFIX` | No | `st:` | Redis channel prefix |
| `ST_APP__MAX_EXPIRY` | No | `6h` | Connection lifetime before re-authorization |
| `ST_ADMIN__LISTEN` | No | `127.0.0.1:9001` | Admin listener. Loopback by default. |
| `ST_ADMIN__TOKEN` | No | — | Bearer token. Authenticated routes return 404 when unset. |
| `ST_LIMITS__MAX_CONNECTIONS` | No | `25000` | Per replica |

Full reference with every key and validation rule: [`docs/08-config.md`](docs/08-config.md).

## Deployment

Route by path so the socket is same-origin and the cookie flows without frontend changes.

```
example.com/       → application
example.com/ws     → sidecartunnel:8000
```

The proxy must pass `Upgrade` and `Connection` through, forward `Origin` unmodified, and have an **idle timeout above `ping_interval`** (default 25s). A lower idle timeout reaps healthy sockets every 60 seconds.

Redis needs `client-output-buffer-limit pubsub` raised from its default of `32mb 8mb 60`. At the default, Redis disconnects the gateway during a broadcast burst and the resubscribe leaves it immediately behind again.

### Sizing

Target is **20,000 concurrent connections across two replicas**.

| | |
|---|---|
| Memory | ~35 KB per connection → **~350 MiB per replica**, ~525 MiB if one carries the whole fleet |
| Authorization load | **~1 request/second** at the application in steady state, at `max_expiry: 6h` |
| Redis | Two subscriber connections |
| Reconnect after a replica dies | 10,000 clients over a 60s spread → **~7 concurrent** authorizations against a 16-worker pool |

Without the reconnect spread, those 10,000 clients arrive in about a second: **~400 concurrent authorizations**, which takes the application down. The gateway is nowhere near its own limits at this scale — the application's worker pool is the binding constraint.

## Local Development

### Prerequisites

- Go 1.26+
- Docker (for the Redis integration tests)
- golangci-lint

### Build and Run

```sh
make check          # lint + test -race + 100% coverage gate
make test           # go test -race -cover ./...
make redis          # throwaway Redis for integration tests
make build
```

Tests are written before implementations and coverage is gated at **100%** in CI. Any uncovered line needs a `// coverage: <reason>` comment. See [`docs/14-coding-standards.md`](docs/14-coding-standards.md).

## Documentation

| Document | Covers |
|---|---|
| [00-overview](docs/00-overview.md) | Definition, non-goals, glossary |
| [01-requirements](docs/01-requirements.md) | FR-1..24, NFR-1..9 with acceptance criteria |
| [02-architecture](docs/02-architecture.md) | Topology and end-to-end flows |
| [03-client-protocol](docs/03-client-protocol.md) | **Normative.** Frames, error codes, close codes |
| [04-integration](docs/04-integration.md) | **Normative.** Webhook, Redis envelope, control channel, admin API |
| [05-authorization](docs/05-authorization.md) | Grants, Origin, expiry, revocation, threat model |
| [06-channels](docs/06-channels.md) | Naming and namespace configuration |
| [07-delivery](docs/07-delivery.md) | Delivery semantics and backpressure |
| [08-config](docs/08-config.md) | **Normative.** Every key, default and validation rule |
| [09-internals](docs/09-internals.md) | Package layout and concurrency model |
| [10-operations](docs/10-operations.md) | Deployment, metrics, runbook |
| [11-testing](docs/11-testing.md) | Required coverage per milestone |
| [12-roadmap](docs/12-roadmap.md) | Milestones and open decisions |
| [13-review-findings](docs/13-review-findings.md) | Adversarial review of the spec and what changed |
| [14-coding-standards](docs/14-coding-standards.md) | **Normative.** TDD, coverage, comments, concurrency rules |

## Contributing

[CONTRIBUTING.md](CONTRIBUTING.md) has the setup, the test-first requirement and the
coverage gate. [docs/14-coding-standards.md](docs/14-coding-standards.md) is the normative
version of the same rules.

## Prior Art

[Centrifugo](https://centrifugal.dev) does everything here and more, including the same cookie-forwarding [connect proxy](https://centrifugal.dev/docs/server/proxy). If it fits, it is the cheaper answer. sidecartunnel exists to be small enough to read in an afternoon.

[soketi](https://github.com/soketi/soketi) is Pusher-protocol-compatible and self-hosted, but is Node and unmaintained since 2024.

## License

MIT. See [LICENSE](LICENSE).
