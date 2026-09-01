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

### Added

- Every GitHub Actions dependency moved to its current major, still SHA-pinned, which also
  clears the Node 20 runtime deprecation the `v0.1.1` release run warned about.
- The cosign **binary** is pinned to `v2.5.2` in `release.yml`, separately from the action
  that installs it. `cosign-installer` v4 installs cosign 3, which removed
  `--output-certificate` and `--output-signature` from `sign-blob` — both passed by the
  `signs` block in `.goreleaser.yaml` — and changed `sign` to store container signatures as
  OCI 1.1 referring artifacts rather than a `.sig` tag. Taken silently, the next tag would
  have failed at the signing step or stopped producing the `checksums.txt.sig` and
  `checksums.txt.pem` that `README.md` tells people to verify. `docs/15-releasing.md` §4
  records what migrating actually involves.

- **`make image` and `make image-check`.** One documented way to build the image outside a
  release, and a gate that asserts it carries what a released image carries — the nine OCI
  labels, the version metadata, `8000/tcp` and nothing else exposed, `nonroot:nonroot`, and
  a binary that answers `--version`. CI runs both on every pull request.

### Changed

- **Signing moved to cosign 3, and the blob signature is now one file.** cosign 3 removed
  `--output-signature` and `--output-certificate` from `sign-blob` in favour of a Sigstore
  bundle carrying the signature, the certificate and the Rekor inclusion proof together, so
  releases from here publish `checksums.txt.bundle` where `v0.1.1` published
  `checksums.txt.sig` and `checksums.txt.pem`. Verification takes `--bundle` in place of
  `--certificate` and `--signature`, and no longer re-fetches the inclusion proof from
  Rekor. `README.md` and `docs/15-releasing.md` §4 carry both shapes, since `v0.1.1` is
  still published and still verifiable.
- **Container signatures are held at cosign's legacy layout on purpose**, with
  `--registry-referrers-mode=legacy` written explicitly in `docker_signs`. cosign 3 can
  store them as OCI 1.1 referring artifacts instead, which changes where every verifier has
  to look — including verifiers running older cosign — and would have silently invalidated
  the `cosign verify` command already published for `v0.1.1`. That move is a decision with
  its own compatibility question, not a side effect of a version bump.
- The cosign **binary** version stays pinned in `release.yml`, now at `v3.0.6` rather than
  `v2.5.2`. The tool that produces the signatures decides what a release publishes, so the
  version is recorded in the workflow rather than inherited from the installer's default.
- **The Go toolchain moved to 1.27 everywhere it is named** — the Dockerfile's build stage
  and every `setup-go` step — so the archive binaries and the image binary in one release
  come from the same compiler. Two toolchains build a release: the archives through
  GoReleaser on the runner, the image binary inside the Dockerfile. Bumping only the
  Dockerfile, which is what Dependabot proposed, would have shipped a release split across
  1.26 and 1.27, and compiled the artifact production runs with a Go version that failed
  this repository's own lint and coverage gates. The test matrix now covers 1.26 and 1.27:
  `go.mod` still declares `go 1.26` as the minimum a consumer needs, which is a claim worth
  testing, and 1.27 is what ships. `go.mod` itself is unchanged — building with a newer
  toolchain than the directive does not raise the requirement on anyone downstream.

- The compose examples and the deployment snippets pin `0.1.1` by digest
  (`sha256:90c14aba…`) rather than `0.1.0`. Verified against the published manifest: the
  cosign signature, the SLSA provenance attestation and `checksums.txt.sig` all resolve to
  the release workflow at `refs/tags/v0.1.1`.

### Fixed

- **`TestConsumer_ControlRejections` raced the consumer's own bookkeeping.** It read
  `Stats()` the instant `waitClose` returned, but the counters are incremented by the
  consumer goroutine *after* it closes the sink, so the read raced that goroutine. It held
  under Go 1.26 and failed about three batches in five of 200 runs under 1.27 —
  `ControlApplied = 0, want exactly the one valid message`. It now waits for the two
  counters to reach the values it is about to assert, which returns on the first read on
  the happy path. Clean at 1000 runs on each of 1.26 and 1.27.

- **A prerelease tag repointed `:latest` and `:X.Y` at the release candidate.** The moving
  image tags were listed unconditionally in both architecture blocks and in
  `docker_manifests`, so `release.prerelease: auto` kept an `-rc` out of the "latest
  release" slot on GitHub while the registry tags production pulls moved to it anyway.
  They are now guarded on `{{ if not .Prerelease }}`. Verified both ways with a local
  GoReleaser run: `v0.1.2-rc.1` builds only `:v0.1.2-rc.1-{amd64,arm64}`, and `v0.1.2`
  still builds all six per-architecture tags. `docs/15-releasing.md` §3 gains the
  release-candidate rehearsal this makes safe — the thing that was missing when `v0.1.1`
  had to be cut with four never-executed steps in the release job.

- **The coverage gate honoured only the first three lines above a block.**
  `scripts/cover.sh` looked for a `// coverage:` justification in a fixed `start - 3`
  window, so any justification longer than three lines was found only by accident — and a
  justification is prose, so several here run to four. It worked under Go 1.26 because 1.26
  reports a block as starting at the function's opening brace, which puts the whole comment
  inside the block's own span. Go 1.27 reports the block starting at the first statement
  instead, the window stopped reaching the marker, and ten justified exemptions were
  reported as unjustified. The scan now walks the contiguous comment above the block
  however far it runs. Reproduced by feeding the gate a profile carrying 1.27's block
  boundaries: the old gate prints `main.go:48 (3 stmt)` and fails, the new one passes, and
  an unmodified 1.26 profile is unaffected under both.
- **The M8 intake test raced its own fence under miniredis.** It drained the intake as
  soon as `Sync` returned, on the reasoning that a subscribe confirmation arrives behind
  every message published before it. That holds against a real Redis, which is
  single-threaded, so a `PUBLISH` that has returned is already queued to the subscriber
  ahead of a later `SUBSCRIBE`. miniredis is concurrent Go and gives no such ordering, so
  the fence could overtake the tail of the burst and the drain would find a message that
  should have been counted as dropped — `Dropped = 198, want 199`. Rare under Go 1.26 and
  about two runs in three under 1.27, purely on scheduling. It now waits for the drop
  counter to reach the only value it can finally have, which is a wait for a known number
  rather than a sleep, and the assertions are unchanged and still exact.
- **Go 1.27 could not be linted.** golangci-lint v2.12.2 links `go/types` from Go 1.25 and
  panics on 1.27 source rather than reporting anything. Moved to v2.13.2.

- **Only the release workflow produced a complete image.** All nine OCI labels were
  `--label` flags in `.goreleaser.yaml`, duplicated across the amd64 and arm64 blocks, so
  the published image carried them and a `docker build .` — the command the compose example
  and `docs/16-integration-guide.md` §13.1 both tell you to run — produced an image whose
  `Config.Labels` was `null`. `org.opencontainers.image.source` is the label that links a
  GHCR package back to its repository. The labels are now `LABEL` instructions in the
  Dockerfile, which is the one file every build path goes through.
- **`make build` stamped no version.** Its comment claimed identical flags to the release
  build while it passed no `-X` at all, so every locally built binary reported
  `dev / none / unknown`. It now derives the version from `git describe`, the commit and
  the commit's timestamp, and CI's build job calls it rather than keeping a second copy of
  the flags.
- The illustrative Dockerfile in `docs/10-operations.md` §1 had drifted to `golang:1.23`
  and omitted every label the real one sets. It is a pointer to the actual file now.

## [0.1.1] — 2026-09-02

Supply chain. No change to the gateway itself — the binary, the protocol and the
configuration keys are identical to `0.1.0`. What changed is what can be verified about the
artifacts, and which of them can be moved after the fact.

### Added

- **Release artifacts are signed.** Images and `checksums.txt` are signed with cosign
  keyless, against the release workflow's OIDC identity rather than a stored key. Images
  are signed by digest, not by tag, because `:latest` and `:X.Y` both move. Verification
  commands are in [`docs/15-releasing.md`](docs/15-releasing.md) §4. Signing starts here:
  `v0.1.0` was tagged before the `signs` and `docker_signs` blocks existed and carries no
  signatures at all.
- **Build provenance attestations** over every archive, `checksums.txt`, and the image.
  A cosign signature says this workflow signed these bytes; provenance says which commit
  and which workflow produced them. `gh attestation verify` checks it with no cosign
  install, which is the check other people will actually run. The image's attestation is
  also pushed to GHCR, so a puller who has the digest and no access to this repository can
  still verify it.
- **An SBOM for the container image**, not only for the archives. The image is what
  production runs and it had none, which is the one that matters when an advisory lands
  against something in the base layer. It is attached to the release as
  `sidecartunnel_<version>_image.sbom.json`.
- **The manifest list digest is appended to every release's notes**, under *Pin This
  Digest*, so pinning correctly does not require running `imagetools inspect`.
- `SECURITY.md` — private disclosure through GitHub advisories, plus what is in scope and
  what is not. The gateway enforcing an application's decision incorrectly is in scope; the
  application making a bad decision is not.
- Issue templates for bugs and features, and a pull request template carrying the gates
  from `docs/14-coding-standards.md`.

### Changed

- **Every GitHub Actions `uses:` is pinned to a commit SHA** rather than a tag, with the
  version in a trailing comment. All twelve floated, four of them inside the release job,
  which holds `contents: write`, `packages: write` and an OIDC token that signs releases
  under my identity. A tag on someone else's repository is a pointer they can move;
  `anchore/sbom-action/download-syft@v0` was a floating major, the loosest form there is.
  Dependabot updates the SHAs and rewrites the comments.
- **`refs/tags/v*` is protected against update and deletion**, with no bypass for
  administrators. The `v0.1.0` tag was pushed at two different commits during two release
  attempts and nothing stopped it. `docs/15-releasing.md` §6 has the procedure for the rare
  case where a tag genuinely has to be removed.
- **The compose examples and the deployment snippets pin the image by digest**, keeping the
  tag alongside it for readability. `examples/compose` previously defaulted to `:latest`
  with a comment saying no release existed; one does.
- The compose example in the README and `docs/10-operations.md` §1 pointed at
  `ghcr.io/…/sidecartunnel:1.0.0` — an ellipsis and a version that does not exist. It now
  names the image that is actually published.

### Fixed

- The release workflow requested `id-token: write` and its comment claimed keyless signing
  and provenance attestation, but nothing in the pipeline signed anything. The permission
  was real and the signing was not. It signs now.
- **The README claimed `v0.1.0` was signed and gave a `cosign verify` command for it.**
  Signing was added after that tag shipped, so the command fails — and a failed `cosign
  verify` reads as tampering rather than as an absent signature, which is worse than
  documenting nothing. The README and `docs/15-releasing.md` §4 now say which release each
  guarantee starts at.

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

