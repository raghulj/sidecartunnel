# Changelog

All notable changes to this project are documented here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this
project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). What counts
as a breaking change for this project — the wire protocol and the configuration keys are
public contracts, `internal/` is not — is in
[`docs/15-releasing.md`](docs/15-releasing.md) §1.

**No release has been cut.** Everything below is unreleased work: the specification and the
scaffolding around it. The gateway does not run yet.

## [Unreleased]

### Added

- **The specification.** The document set under [`docs/`](docs/), written before any
  implementation. Four are normative and an implementation that disagrees with one of them
  is wrong: [`03-client-protocol.md`](docs/03-client-protocol.md) (frames, error codes,
  close codes), [`04-integration.md`](docs/04-integration.md) (connect webhook, Redis
  envelope, control channel), [`08-config.md`](docs/08-config.md) (every key,
  default and validation rule), and
  [`14-coding-standards.md`](docs/14-coding-standards.md) (TDD, the coverage gate,
  concurrency rules).
- **An adversarial review of the specification**, recorded in
  [`13-review-findings.md`](docs/13-review-findings.md): 8 critical and 22 major defects,
  with five structural changes (S1–S5) made in response. Multi-app was cut, grants now
  expire by re-handshake rather than revalidation, and the reconciler was removed.
- **Package skeletons** under `internal/` — `proto`, `config`, `bus`, `glob`, `hub`,
  `conn`, `server`, `webhook` — carrying package documentation and the frame, code and
  configuration types the specification defines. No wiring; `main` panics.
- **The build and check pipeline.** `Makefile` with `check` as the default goal,
  `scripts/cover.sh` enforcing 100% statement coverage per package with `// coverage:`
  justifications read out of the source, `.golangci.yml`, and
  [`ci.yml`](.github/workflows/ci.yml) running lint, `go test -race` against a real Redis,
  and the coverage gate.
- **Release engineering.** A multi-stage [`Dockerfile`](Dockerfile) producing a non-root
  distroless image that exposes port 8000 only, [`.goreleaser.yaml`](.goreleaser.yaml) for
  archives, checksums, SBOMs and multi-arch images on `ghcr.io`, a tag-triggered
  [`release.yml`](.github/workflows/release.yml) that gates on the test suite and the
  coverage gate before it publishes anything, weekly CodeQL analysis, Dependabot, and the
  runbook in [`15-releasing.md`](docs/15-releasing.md).
- **A Compose example** at [`examples/compose/`](examples/compose/) with the settings
  [`10-operations.md`](docs/10-operations.md) §3 requires — `order: start-first`, an update
  delay above `drain_timeout`, a dedicated Redis database index, and
  `client-output-buffer-limit pubsub 256mb 64mb 60`, which is the one Redis setting this
  system will not run correctly without.
- **Contributor documentation.** [`CONTRIBUTING.md`](CONTRIBUTING.md),
  [`CLAUDE.md`](CLAUDE.md) and [`docs/AGENTS.md`](docs/AGENTS.md), stating the test-first
  requirement and the coverage gate.
- MIT [`LICENSE`](LICENSE).

### Fixed

- **`channels_requested` is now sent.** The connect webhook's request body carries the
  `subs` list from the client's `connect` frame, which
  [`04-integration.md`](docs/04-integration.md) §1.1 has specified since the first draft
  and which no code populated: the webhook request was built at the HTTP upgrade, before
  any frame had been read, so an application reading the field could not tell "this client
  asked for nothing" from "this gateway never tells me". `conn.Authorizer.Authorize` now
  takes the requested channels alongside the context. They remain a hint — the application
  answers with the grants and the gateway matches against those — and they are bounded by
  `limits.max_subscriptions_per_conn` and `limits.max_channel_length` before they enter an
  outbound request, because an unbounded list from an untrusted client is an amplification
  vector into the application.

- **`server.drain_spread` has a documented range.** It was checked only for being
  non-negative while every neighbouring duration in
  [`08-config.md`](docs/08-config.md) §3 had a range, so `drain_spread: 500us` validated,
  started cleanly, and reached arithmetic that works in whole milliseconds — the spread
  silently gone at the one moment it matters. Now 1s–300s.
- **A password in `app.connect_url` no longer reaches the logs.** `validateApp` accepted
  userinfo where `validateOrigin` refused it, and `internal/webhook` formatted the
  configured URL into four of its own error strings, which reach a `warn` line on every
  timed-out connect. `https://gw:hunter2@webapp.internal/_st/connect` is an ordinary shape
  for an internal endpoint, so an application restart logged the password once per
  reconnecting connection. `app.connect_url` now refuses userinfo at startup, naming the
  key and quoting nothing, and every URL this gateway does name in an error is redacted
  first — `bus.url` included, where `redis://:password@host` stays legal and only the echo
  is refused. The leak was hard to see because `net/http` redacts userinfo from its own
  `*url.Error`, so ours looked like Go's output and was not (NFR-7).
- **A publisher's payload byte no longer reaches a DEBUG log.** `internal/consumer` logged
  the error from `hub.Dispatch`, which wraps `encoding/json`'s — and a `*json.SyntaxError`
  reads `invalid character 'X' …`, where `X` is one byte of the published payload. The byte
  offset is kept, the byte is not. Same fix on the control channel, where anyone who can
  publish to Redis chooses the byte and it was logged at `warn` (NFR-7).
- **`webhook.New` refuses an unset `app.max_expiry`.** `clampExpiry` compares against the
  maximum before clamping up to the minimum, so a zero maximum gave every connection
  `expires_in: 0`, the expiry timer was never armed, and FR-22 was silently off.
- **The connect queue no longer pins a timer per waiting connection.** `acquire` waited on
  a `time.After` in a select whose other arms usually win, and that timer is held by the
  runtime until it fires. At `connect_queue: 4096` and `connect_timeout: 10s` that is up to
  4096 live timers for ten seconds, during exactly the reconnect storm the queue exists to
  survive. The clock seam is now `NewTimer`, which returns a stop function that every path
  calls.
- **The browser client no longer leaves promises unsettled on `close()`.** Called during
  backoff there is no socket, so no `close` event arrives and `_onClose` never runs:
  anything queued behind the connect reply, and every registry entry waiting to ride the
  next `connect` frame, hung for the lifetime of the page. `close()` now settles them all.
- **The browser client bounds `retry_after`.** §7.1 makes honouring it a MUST, and a client
  with no floor and no ceiling cannot survive a gateway that gets it wrong: a negative value
  defeats the spread, and `{"retry_after": 1e999}` parses to `Infinity`. Values are clamped
  to `[0, 300000]` — 300000 being the top of `server.drain_spread`'s range — and anything
  negative or non-finite is treated as absent, falling back to full jitter (§8.2).
- **`unsubscribe()` before the connect reply no longer rejects the `subscribe()` promise.**
  Cancelling a pending subscribe is ordinary control flow, and rejecting it produced an
  unhandled rejection whenever the caller had not attached a `.catch` it had no reason to
  attach. It resolves.
- **The gateway no longer holds a session cookie for the life of a connection (FR-22).**
  `Server.serve` built the `webhook.Request` at the upgrade and captured it in the closure
  it handed `conn.New` as the `Authorizer`; a `Conn` keeps that closure and a `Conn` lives
  until `app.expires_in`, 6h by default. At 20,000 connections that is 20,000 live,
  replayable sessions in a core dump — the thing S3 restructured expiry to prevent, and
  the reason `internal/conn`'s own documentation claimed the value "never enters the type
  that outlives the call". It did. Two changes make the claim structural: the request now
  lives behind a single-use `connectAuthorizer` that swaps it out before it calls the
  application, and a `Conn` takes its `Authorizer` exactly once, dropping its reference in
  the same act — which is also what makes a second `connect` frame "already connected", so
  the drop cannot be quietly removed. The retention test now lives in `internal/server`,
  where the retention was, and walks the live object graph rather than reading the source;
  the one in `internal/webhook` had been passing for months about a value that package
  never held.
- **A connection admitted while a drain is starting is drained (FR-19).** `ServeHTTP`
  reserves a slot before the upgrade, and a connection only enters the set `Drain`
  snapshots once it has been built. A handshake landing in that window was never told to
  close: the drain waited out the whole of `server.drain_timeout`, `serve` returned exit 1,
  and that client got a bare 1006 instead of a 3000 with a spread `retry_after` — the
  stampede FR-19 exists to prevent, aimed at the application by the pod being rolled. It is
  now refused-and-closed under the same lock that sets `draining`. The drain also waits on
  the reservation count rather than a `sync.WaitGroup`: `Add` on a group another goroutine
  is inside `Wait` on is a fatal misuse, which was a process death during SIGTERM.
- **`server.drain_spread` under a millisecond no longer panics every close.**
  `retryAfter` computed `fnv1a(id) % spread.Milliseconds()`, and `999µs.Milliseconds()` is
  0. The `#nosec` justification asserted that `positive()` prevented it; `positive()`
  guarantees a positive *duration*, not a positive millisecond count. The count is floored
  at 1 in the arithmetic itself rather than trusted to the validator in another package,
  because a panic on a connection goroutine takes the process with it.
- **The documented channel character rule is enforced.** `06-channels.md` §1 is normative —
  printable ASCII, no whitespace, no control characters — and nothing checked it. A grant
  is a prefix, so a client granted `room-*` could subscribe to `room- \n admin`, and the
  name reached the hub map, the desired set, a Redis `SUBSCRIBE`, the subscribe line the
  runbook greps, and every `sync` reply. §2 requires names be human-readable **because**
  they appear in logs. A malformed name is now error 101 on `subscribe` and `unsubscribe`,
  omitted from `subs` on `connect`, and never forwarded as `channels_requested`.
- **`Hub.Attach` can no longer resurrect a deregistered connection.** `ErrNotRegistered`
  exists to stop a reader goroutine's in-flight subscribe from bringing back a connection
  that close has just deregistered, because a resurrected connection is resident in the
  channel map forever — fan-out writes to a dead socket and the refcount never reaches
  zero. `insertLocked` honoured it and `Attach` did not: it re-created the maps `Remove`
  had just deleted. Reachable whenever SIGTERM closes a connection whose reader is blocked
  in the connect webhook and the application then answers 200. `Attach` now registers
  nothing; registration is `Add`'s, at the upgrade, which is where it already happened.
- **`Hub.Close` no longer races the closer queue.** `Close` documented only that it must
  not race `Dispatch`, but `Attach`, `Subscribe`, `Unsubscribe` and `controlUnsubscribe`
  all reach `enqueueClose`, whose overflow path did `wg.Add(1)` on the very WaitGroup
  `Close` was waiting on — the same fatal misuse as the drain's, on a path that only runs
  when a connection is already misbehaving. The closer goroutine also abandoned up to
  `CloserQueue` queued closes on `ctx.Done`, each one a connection left open with nothing
  left to end it. Close now announces under a mutex that nothing more may be queued,
  waits for the closers spawned before that, and performs the remainder itself; a close
  enqueued afterwards runs inline.
- **A closed connection reports no backpressure.** `Conn.Send` returned false both when the
  outbound queue was full and when the connection was closed, and false has exactly one
  meaning to a caller: hand this sink to the closer goroutine. A drain therefore turned N
  closed connections into N slow-consumer closes of connections that had already gone.
  `hub.Sink` says in as many words that a fan-out/close race must not be an error path; it
  now returns true and drops the frame. `Conn.Unsubscribed`, which no production path has
  called since S3 removed revalidation, is deleted rather than left inflating FR-17's
  evidence at 100% coverage.

### Removed

- **Breaking: `control.refresh_spread`.** Defined, defaulted, validated, documented, and
  read by no code at all — the third key in this repository with that shape, after FR-14's
  `limits.max_message_size`. A comment in `internal/hub` claimed the connection layer
  applied it; the connection layer spreads `retry_after` over `server.drain_spread` and
  always has, so an operator setting `refresh_spread: 300s` got 60s and five times the
  concurrent authorization load they had asked to avoid. It is deleted rather than wired:
  there is one spread window in the connection layer, `refresh` must name exactly one
  `user` or `client` so one message reaches at most `limits.max_connections_per_user`
  connections, and `drain_spread`'s value is derived from exactly the arithmetic a refresh
  window would be. A key that lies is worse than a key that is absent.
  [`04-integration.md`](docs/04-integration.md) §3 and
  [`08-config.md`](docs/08-config.md) §3 now say `server.drain_spread`.
- **Breaking: the admin API.** `internal/admin`, the second HTTP listener, and the
  `admin.listen` / `admin.token` config keys (`ST_ADMIN__LISTEN`, `ST_ADMIN__TOKEN`) are
  gone, along with `GET /channels`, `GET /channels/{channel}` and `POST /disconnect`.
  Every route on the gateway's one remaining listener now answers with no credential. The
  separate listener defended against a proxy misconfiguration exposing `/channels`
  publicly — a config mistake, not an attacker. `POST /disconnect` duplicated the signed
  control channel, which is now the only way to revoke. `GET /channels` is replaced by
  grepping the `subscribe`/`unsubscribe` lines the gateway logs at INFO.
- **Breaking: Prometheus metrics.** `internal/metrics`, `cmd/sidecartunnel/sampler.go` and
  the `client_golang` dependency are gone. `GET /metrics` now answers 404. Eighteen metric
  families were specified before any code existed to produce them; nine never got a
  producer and sat permanently at zero, which reads identically to "nothing is wrong."
  Runtime dependencies are now three: `gorilla/websocket`, a Redis client, and a YAML
  parser.

### Security

- The `Origin` allowlist has no default and startup fails when it is empty. A default of
  `["*"]` would be a security hole shipped as a convenience, and browsers do not apply CORS
  to websocket handshakes, so this check is the only thing between a logged-in user and
  cross-site websocket hijacking ([`05-authorization.md`](docs/05-authorization.md) §5).
- The release image has no shell and no curl, and `EXPOSE 8000` is the only port it
  declares, so `docker run -P` cannot publish anything else
  ([`10-operations.md`](docs/10-operations.md) §1).
- CodeQL runs on every push and pull request and again weekly, so a rule added to the pack
  after a commit lands still sees it.

[Unreleased]: https://github.com/raghulj/sidecartunnel/commits/main
