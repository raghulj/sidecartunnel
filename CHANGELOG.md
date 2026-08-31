# Changelog

All notable changes to this project are documented here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this
project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html). What counts
as a breaking change for this project — the wire protocol and the configuration keys are
public contracts, `internal/` is not — is in
[`docs/15-releasing.md`](docs/15-releasing.md) §1.

The gateway runs. See [`docs/16-integration-guide.md`](docs/16-integration-guide.md) §13
for a worked end-to-end run against a real application.

## [Unreleased]

Nothing yet.

## [0.1.0] — 2026-08-31

First release. The gateway is feature-complete for its stated scope and has been run end
to end against a real application, but it has never carried production traffic.

### Added

- **Client protocol.** Websocket, JSON frames, one multiplexed socket per browser tab.
  `connect`, `subscribe`, `unsubscribe`, `publish`, `sync`, `ping`; nine message types
  total. Server-directed `retry_after` on every retryable close.
- **Cookie-forward authorization.** The gateway forwards the browser's cookie to one
  endpoint on the application, signed with HMAC-SHA256 over the timestamp, a nonce and a
  digest of the cookie. The application answers with a user id and a list of channel glob
  patterns. No frontend authentication code, no token minting.
- **Publishing over Redis.** `redis.publish("st:room-4410", …)` from any process — a web
  worker, a background task, a cron job.
- **Cross-replica fan-out** through Redis pub/sub, with subscriptions modelled as state
  and reconciled rather than issued as commands.
- **Signed control channel** for revocation, forced re-authorization and forced
  unsubscribe, reaching every replica.
- **`/health` and `/ready`** on the main listener, plus a `sidecartunnel healthcheck`
  subcommand so a distroless image with no shell can declare a Docker `HEALTHCHECK`.
- **Browser client** (`client/js`) — dependency-free ES module plus a UMD build, with
  reconnect, full-jitter backoff, subscription replay and a reconciliation hook.
- **Reference application** (`examples/flask`) — a runnable Flask integration with 50
  tests, and a compose stack.
- Multi-arch container images, checksums and SBOMs, published from a tag.

### Known limitations

- **Not load tested.** NFR-1 (15,000 connections per replica in 1 GiB) and NFR-2 (p99
  fan-out under 20 ms) are derivations, not measurements. The harness exists; it has never
  been run at scale.
- **Delivery is at-most-once.** A replica restart or a sleeping client drops messages. The
  application is expected to expose a `?since=` endpoint; the browser client calls it on
  every reconnect. This is deliberate — see `docs/07-delivery.md`.
- **No ordering guarantee, not even per channel.** Measured, not assumed: at the default
  `dispatch_workers: 2` two messages on one channel arrive in either order.
- **The per-namespace `max_message_size` override is not wired.** The global limit is
  enforced.
- **No per-IP connection cap.** The per-user cap applies only after the application has
  named the user, so an unauthenticated client that sets a correct `Origin` can queue
  authorizations.
- Presence, message history and client events are not built. See `docs/12-roadmap.md`.

### Notes

Two adversarial reviews were run and their findings fixed: one against the specification
(44 findings) and one against the implementation (20). Both logs are in
`docs/13-review-findings.md`.

Three configuration keys were found fully specified, validated, documented and read by no
code at all — `limits.max_message_size`, the per-namespace override, and
`control.refresh_spread` — while every package reported 100% statement coverage.
`scripts/trace.sh` was added in response and runs in CI.

