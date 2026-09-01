# sidecartunnel

[![ci](https://github.com/raghulj/sidecartunnel/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/raghulj/sidecartunnel/actions/workflows/ci.yml)
[![codeql](https://github.com/raghulj/sidecartunnel/actions/workflows/codeql.yml/badge.svg?branch=main)](https://github.com/raghulj/sidecartunnel/actions/workflows/codeql.yml)
[![coverage](https://img.shields.io/badge/coverage-100%25-brightgreen)](docs/14-coding-standards.md)
[![go report](https://goreportcard.com/badge/github.com/raghulj/sidecartunnel)](https://goreportcard.com/report/github.com/raghulj/sidecartunnel)
[![release](https://img.shields.io/github/v/release/raghulj/sidecartunnel?sort=semver&color=blue)](https://github.com/raghulj/sidecartunnel/releases/latest)
[![ghcr.io](https://img.shields.io/badge/ghcr.io-multi--arch-blue?logo=docker&logoColor=white)](https://github.com/raghulj/sidecartunnel/pkgs/container/sidecartunnel)
[![go](https://img.shields.io/github/go-mod/go-version/raghulj/sidecartunnel)](go.mod)
[![license](https://img.shields.io/badge/license-MIT-green)](LICENSE)

A Go websocket gateway for applications that cannot hold long-lived connections — Flask, Django, Rails, PHP-FPM. Terminates websockets, authorizes each connection against the application's own session cookie, and fans messages out across replicas through Redis pub/sub.

The application publishes with one `redis.publish()` call and authorizes with one HTTP endpoint. It does not import a websocket library, run an event loop, or change how it is deployed.

The gateway is implemented and runs. A worked end-to-end stack — application, gateway, Redis and a same-origin proxy — is in [`examples/flask/`](examples/flask/), and [`docs/16-integration-guide.md`](docs/16-integration-guide.md) §13 walks the whole flow with commands that work. The specification lives in [`docs/`](docs/).

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

A channel is an opaque string. The substring before the first `-` selects a namespace, which selects a block of configuration. Names must be human-readable — they appear in the logs, which is where an operator goes to find out who is subscribed to what.

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
| `ST_LIMITS__MAX_CONNECTIONS` | No | `25000` | Per replica |

Full reference with every key and validation rule: [`docs/08-config.md`](docs/08-config.md).

## Container Images

Published to GHCR on every tag, as a multi-arch manifest covering `linux/amd64` and
`linux/arm64`. The base is `gcr.io/distroless/static-debian12:nonroot` — no shell, no
package manager, non-root by default.

```
docker pull ghcr.io/raghulj/sidecartunnel@sha256:90c14aba99da3053c1cc14e827414dfee9ea804cf57d49a95e21f0fe07167343
```

That digest is `v0.1.1`. Every release prints its own in the release notes.

| Reference | Moves | Use it for |
|---|---|---|
| `@sha256:…` | Never. It is the bytes, not a pointer to them | Production |
| `v0.1.1` | Only if someone moves it | Reading a release note |
| `0.1` | Patch releases within the minor | Picking up fixes without a redeploy decision |
| `latest` | Every release | Local development only |

`refs/tags/v*` is protected against update and deletion, so a released tag will not move by
accident. That is a repository setting rather than a property of the reference — the
`v0.1.0` tag was pushed twice during two release attempts, at two different commits,
before the rule existed. Only the digest cannot move.

### Verifying A Release

Cosign signatures, build provenance and an image SBOM start at **v0.1.1**. `v0.1.0`
predates all three and carries only the archives, `checksums.txt` and one SBOM per
archive. A `cosign verify` against `v0.1.0` fails for that reason — nothing signed it —
and not because anything was tampered with.

Images and `checksums.txt` are signed with
[cosign](https://docs.sigstore.dev) keyless, so there is no public key to distribute; the
identity being attested is the release workflow itself.

```
cosign verify ghcr.io/raghulj/sidecartunnel:v0.1.2 \
  --certificate-identity-regexp 'https://github.com/raghulj/sidecartunnel/.*' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com
```

The blob signature moved from two files to one at **v0.1.2**. cosign 3 replaced
`--output-signature` and `--output-certificate` with a Sigstore bundle carrying the
signature, the certificate and the Rekor inclusion proof together, so releases from
v0.1.2 publish `checksums.txt.bundle` and v0.1.1 published `checksums.txt.sig` and
`checksums.txt.pem`. Verify whichever the release you have actually carries:

```
# v0.1.2 and later
cosign verify-blob \
  --bundle checksums.txt.bundle \
  --certificate-identity-regexp 'https://github.com/raghulj/sidecartunnel/.*' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  checksums.txt

# v0.1.1
cosign verify-blob \
  --certificate checksums.txt.pem --signature checksums.txt.sig \
  --certificate-identity-regexp 'https://github.com/raghulj/sidecartunnel/.*' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  checksums.txt
```

Image signatures are unaffected: they stay in cosign's legacy `.sig`-tag layout rather
than moving to OCI 1.1 referring artifacts, so the `cosign verify` above is the same
command for every signed release.

Every archive, `checksums.txt` and the image also carry a signed build provenance
attestation — which commit and which workflow produced the bytes, rather than only that
they were signed. It needs no cosign install to check:

```
gh attestation verify oci://ghcr.io/raghulj/sidecartunnel:v0.1.2 --repo raghulj/sidecartunnel
```

| Artifact | v0.1.0 | v0.1.1 | v0.1.2 on |
|---|---|---|---|
| Archives, `checksums.txt` | Yes | Yes | Yes |
| SBOM per archive | Yes | Yes | Yes |
| SBOM for the image | No | Yes | Yes |
| Image cosign signature | No | Yes | Yes |
| Build provenance attestation | No | Yes | Yes |
| `checksums.txt.sig` + `.pem` | No | Yes | No |
| `checksums.txt.bundle` | No | No | Yes |

The full verification procedure is in [`docs/15-releasing.md`](docs/15-releasing.md).

## Deployment

Route by path so the socket is same-origin and the cookie flows without frontend changes.

```
example.com/       → application
example.com/ws     → sidecartunnel:8000
```

The proxy must pass `Upgrade` and `Connection` through, forward `Origin` unmodified, and have an **idle timeout above `ping_interval`** (default 25s). A lower idle timeout reaps healthy sockets on that timeout.

Redis needs `client-output-buffer-limit pubsub` raised from its default of `32mb 8mb 60`. At the default, Redis disconnects the gateway during a broadcast burst and the resubscribe leaves it immediately behind again.

Forward the websocket path and nothing else. `GET /health` and `GET /ready` share the listener with it, and a proxy that forwards `/` publishes both. Neither carries a credential and neither needs one — they report that the process is up and that it can reach Redis, which is what a load balancer is asking and what every health endpoint on the internet already says. Routing only `/ws*` keeps them internal anyway; that is defence in depth, not the reason they are safe.

### Sizing

Target is **20,000 concurrent connections across two replicas**.

| | |
|---|---|
| Memory | ~35 KB per connection → **~350 MiB per replica**, ~525 MiB if one carries the whole fleet |
| Authorization load | **~1 request/second** at the application in steady state, at `max_expiry: 6h` |
| Redis | Two subscriber connections |
| Reconnect after a replica dies | 10,000 clients over a 60s spread → **~7 concurrent** authorizations against a 16-worker pool |

Without the reconnect spread, those 10,000 clients arrive in about a second: **~400 concurrent authorizations**, which takes the application down. The gateway is nowhere near its own limits at this scale — the application's worker pool is the binding constraint.

## Health Checks

Two routes on `server.listen`, unauthenticated, alongside the websocket endpoint. There is no second listener.

| Route | Answers | Consults the bus |
|---|---|---|
| `GET /health` | 200 while the process runs | Never |
| `GET /ready` | 503 while draining, and 503 once the bus has been down longer than `bus.ready_grace` (default 30s) | Yes |

`GET /ready` carries the detail the status code cannot:

```json
{"ready":true,"bus_connected":true,"bus_down_for_seconds":0,"bus_reconnects":0,"draining":false}
```

`bus_reconnects` is cumulative for the life of the process — read it by curling twice and comparing. Climbing while `bus_connected` is `true` is Redis pub/sub output-buffer eviction, not an unstable Redis. `draining` separates "this replica is going away" from "this replica cannot reach Redis", which share a status code and are very different incidents.

### Docker

```dockerfile
HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
  CMD ["/sidecartunnel", "healthcheck"]
```

`sidecartunnel healthcheck` performs a loopback `GET /health` against `server.listen` and exits 0 or 1. It is a subcommand rather than a `curl` line because the release image is distroless: no shell, no curl, so the only executable available to probe the process is the process. Exec form matters for the same reason — there is no `/bin/sh` to parse a string form.

### Compose

```yaml
services:
  sidecartunnel:
    # Tag and digest both. The tag says which release this is; the digest is what
    # actually gets pulled, and it cannot be moved.
    image: ghcr.io/raghulj/sidecartunnel:v0.1.1@sha256:90c14aba99da3053c1cc14e827414dfee9ea804cf57d49a95e21f0fe07167343
    healthcheck:
      test: ["CMD", "/sidecartunnel", "healthcheck"]
      interval: 10s
      timeout: 3s
      start_period: 5s
      retries: 3
```

### Kubernetes

```yaml
livenessProbe:
  httpGet: { path: /health, port: 8000 }
  periodSeconds: 10
  failureThreshold: 3

readinessProbe:
  httpGet: { path: /ready, port: 8000 }
  periodSeconds: 5
  failureThreshold: 2
```

> **`/ready` must NEVER be wired to a liveness probe.**
>
> A Redis restart makes every replica unready at the same instant, because every replica lost the same transport. A liveness probe on `/ready` therefore kills the entire fleet simultaneously, drops every connection, and turns an eight-second Redis blip into a full application outage as 20,000 clients re-authorize together against one connect webhook. `/health` exists precisely so there is something correct to point a liveness probe at, and it never consults the bus for exactly this reason.

The same rule is why `sidecartunnel healthcheck` probes `/health` and only `/health`: a container healthcheck is a liveness signal, and pointing it at the bus rebuilds the outage one container at a time.

`bus.ready_grace` is the other half. It is why a short blip does not pull every replica out of the load balancer at once: readiness tolerates the bus being gone for 30 seconds before reporting 503. Connections stay open and silent throughout — nothing is closed because Redis went away.

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
| [04-integration](docs/04-integration.md) | **Normative.** Webhook, Redis envelope, control channel, health checks |
| [05-authorization](docs/05-authorization.md) | Grants, Origin, expiry, revocation, threat model |
| [06-channels](docs/06-channels.md) | Naming and namespace configuration |
| [07-delivery](docs/07-delivery.md) | Delivery semantics and backpressure |
| [08-config](docs/08-config.md) | **Normative.** Every key, default and validation rule |
| [09-internals](docs/09-internals.md) | Package layout and concurrency model |
| [10-operations](docs/10-operations.md) | Deployment, observability, runbook |
| [11-testing](docs/11-testing.md) | Required coverage per milestone |
| [12-roadmap](docs/12-roadmap.md) | Milestones and open decisions |
| [13-review-findings](docs/13-review-findings.md) | Adversarial review of the spec and what changed |
| [14-coding-standards](docs/14-coding-standards.md) | **Normative.** TDD, coverage, comments, concurrency rules |
| [15-releasing](docs/15-releasing.md) | Versioning policy, cutting a release, verifying artifacts |
| [16-integration-guide](docs/16-integration-guide.md) | Adopting this in an existing app, with a worked Flask example |
| [17-production-readiness](docs/17-production-readiness.md) | Everything required before real users |

## Contributing

[CONTRIBUTING.md](CONTRIBUTING.md) has the setup, the test-first requirement and the
coverage gate. [docs/14-coding-standards.md](docs/14-coding-standards.md) is the normative
version of the same rules.

## Prior Art

[Centrifugo](https://centrifugal.dev) does everything here and more, including the same cookie-forwarding [connect proxy](https://centrifugal.dev/docs/server/proxy). If it fits, it is the cheaper answer. sidecartunnel exists to be small enough to read in an afternoon.

[soketi](https://github.com/soketi/soketi) is Pusher-protocol-compatible and self-hosted, but is Node and unmaintained since 2024.

## License

MIT. See [LICENSE](LICENSE).
