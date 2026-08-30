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
  envelope, control channel, admin API), [`08-config.md`](docs/08-config.md) (every key,
  default and validation rule), and
  [`14-coding-standards.md`](docs/14-coding-standards.md) (TDD, the coverage gate,
  concurrency rules).
- **An adversarial review of the specification**, recorded in
  [`13-review-findings.md`](docs/13-review-findings.md): 8 critical and 22 major defects,
  with five structural changes (S1–S5) made in response. Multi-app was cut, grants now
  expire by re-handshake rather than revalidation, and the reconciler was removed.
- **Package skeletons** under `internal/` — `proto`, `config`, `bus`, `glob`, `hub`,
  `conn`, `server`, `webhook`, `admin`, `metrics` — carrying package documentation and the
  frame, code and configuration types the specification defines. No wiring; `main` panics.
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

### Security

- The `Origin` allowlist has no default and startup fails when it is empty. A default of
  `["*"]` would be a security hole shipped as a convenience, and browsers do not apply CORS
  to websocket handshakes, so this check is the only thing between a logged-in user and
  cross-site websocket hijacking ([`05-authorization.md`](docs/05-authorization.md) §5).
- The release image has no shell and no curl, and `EXPOSE 9001` is deliberately absent:
  `docker run -P` publishes every exposed port, and the admin listener defaults to loopback
  precisely so it cannot be reached from outside
  ([`10-operations.md`](docs/10-operations.md) §1).
- CodeQL runs on every push and pull request and again weekly, so a rule added to the pack
  after a commit lands still sees it.

[Unreleased]: https://github.com/raghulj/sidecartunnel/commits/main
