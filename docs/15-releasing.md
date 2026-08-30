# 15 — Releasing

A release is a git tag. Everything else — archives for six platforms, `checksums.txt`,
SBOMs, GitHub release notes, and a multi-arch image on `ghcr.io` — is derived from it by
[`.github/workflows/release.yml`](../.github/workflows/release.yml) and
[`.goreleaser.yaml`](../.goreleaser.yaml). Nothing is built by hand and nothing is uploaded
by hand.

The workflow runs the full test suite and the 100% coverage gate **before** GoReleaser. A
tag that fails either produces no artifacts at all. A released version is the one people
pin, and shipping it untested is worse than not shipping.

No release has been cut yet.

## 1. Versioning

[Semantic versioning](https://semver.org). The version is the tag, `vMAJOR.MINOR.PATCH`,
and the major number answers one question: **can an existing deployment upgrade without
changing anything outside this repository?**

Two surfaces are public contracts. Both are normative documents, and both are consumed by
code that is not in this repository:

| Contract | Document | Consumed by |
|---|---|---|
| The websocket wire protocol — frames, error codes, close codes | [`03-client-protocol.md`](03-client-protocol.md) | Every browser client, including third-party implementations |
| Configuration keys, defaults and validation rules | [`08-config.md`](08-config.md) | Every deployment's environment and YAML |
| Webhook, Redis envelope, control channel, admin API | [`04-integration.md`](04-integration.md) | The consuming application's own code |

A change to any of them is a major bump. Everything under `internal/` is free: Go's
`internal` rule means no external package can import it, so it can be rewritten, split or
deleted without a version consequence. Package layout is not a contract; observable
behaviour is.

### What Each Number Means Here

| Change | Bump |
|---|---|
| A frame field renamed or removed; an error or close code given a new meaning | major |
| A configuration key renamed or removed | major |
| A default changed in a way that alters behaviour on an unchanged config | major |
| Validation tightened so a previously-accepted configuration now fails startup | major |
| The webhook signature scheme, the Redis envelope, or an admin route changed | major |
| A metric renamed or removed — alerts are keyed on those names ([`10-operations.md`](10-operations.md) §5) | major |
| The module path changed | major |
| A new optional frame field, or a new command a client may ignore | minor |
| A new configuration key with a default that preserves current behaviour | minor |
| A new metric, a new admin route, a new log event | minor |
| The minimum Go version raised | minor |
| A bug fixed with no observable contract change | patch |
| Anything under `internal/` — refactor, split, concurrency rewrite | patch |
| A base image or dependency bump with no behaviour change | patch |

### Before 1.0

While the major is `0`, a breaking change bumps the **minor** and a compatible one bumps
the patch — the standard `0.y.z` reading. The table above still decides which is which; it
shifts one column left. `1.0.0` is cut when M2 is complete
([`12-roadmap.md`](12-roadmap.md) §1), because M2 is the first version worth putting in
front of real users, and a `1.0.0` that is not is a promise that cannot be kept.

Pre-releases use `-rc.N`: `v1.0.0-rc.1`. GoReleaser marks any tag with a hyphen as a GitHub
pre-release automatically, so `:latest` does not move to it.

### Deprecating Rather Than Breaking

A key or a field that has to go leaves in two steps: a minor release that keeps it working
and logs a warning naming the replacement, then the next major that removes it. The
warning is what makes the major bump survivable — an operator who never reads a changelog
still sees it in their logs before the upgrade that breaks them.

## 2. Before The Tag

| Check | Command |
|---|---|
| `main` is green — lint, race tests, coverage gate | `make check` |
| The working tree is clean and pushed | `git status --porcelain` prints nothing |
| The GoReleaser configuration is valid | `goreleaser check` |
| The whole pipeline runs end to end | `goreleaser release --snapshot --clean` |
| `CHANGELOG.md` has the release section | `[Unreleased]` moved under the new version and dated |
| Normative documents match the code | Protocol and config changes landed in the same commits as their implementations |

The snapshot run is the rehearsal. It builds every archive, every checksum and both
architecture images against the working tree without a tag, without a push and without
touching the registry:

```sh
goreleaser release --snapshot --clean
ls dist/
```

`dist/` is gitignored. `--clean` removes it first, so a stale artifact from a previous run
cannot be mistaken for a current one.

## 3. Cutting It

```sh
git switch main
git pull --ff-only
make check

git tag -a v0.2.0 -m "v0.2.0"
git push origin v0.2.0
```

Annotated tags, always: `git tag -a` records the tagger and the date, and GoReleaser reads
that date. A lightweight tag leaves the release metadata pointing at the commit's
timestamp instead.

The tag triggers [`release.yml`](../.github/workflows/release.yml), which:

| Step | What it does |
|---|---|
| `verify` | `go test -race -cover ./...` against a real Redis, then `scripts/cover.sh` |
| `release` | QEMU and buildx, `ghcr.io` login, syft, then `goreleaser release --clean` |

Watch it with `gh run watch`. Nothing is published until `verify` passes.

### What A Tag Produces

| Artifact | Name |
|---|---|
| Archives | `sidecartunnel_<version>_<os>_<arch>.tar.gz`, `.zip` on Windows — each containing the binary, `README.md` and `LICENSE` |
| Platforms | linux, darwin, windows × amd64, arm64 |
| Checksums | `checksums.txt`, SHA256 |
| SBOM | One per archive, generated by syft |
| Image | `ghcr.io/raghulj/sidecartunnel`, tagged `:vX.Y.Z`, `:X.Y` and `:latest`, `linux/amd64` and `linux/arm64` |
| Release notes | Grouped by commit prefix; `docs:`, `chore:`, `test:`, `ci:` and `build(deps):` commits are excluded |

## 4. Verifying The Artifacts

Verification is not optional for the person cutting the release. The point of a checksum
nobody checks is zero.

```sh
gh release download v0.2.0 --pattern 'checksums.txt' --pattern '*linux_amd64.tar.gz'
sha256sum -c checksums.txt --ignore-missing

tar -tzf sidecartunnel_0.2.0_linux_amd64.tar.gz    # binary, README.md, LICENSE
```

The image is where the mistakes with consequences live:

| Check | Command | Expected |
|---|---|---|
| Both architectures present | `docker buildx imagetools inspect ghcr.io/raghulj/sidecartunnel:v0.2.0` | `linux/amd64` and `linux/arm64` in one manifest list |
| Size under 20 MB (NFR-6) | `docker images ghcr.io/raghulj/sidecartunnel:v0.2.0` | Well under; the binary is nearly all of it |
| Port 8000 only | `docker image inspect --format '{{json .Config.ExposedPorts}}' ghcr.io/raghulj/sidecartunnel:v0.2.0` | `{"8000/tcp":{}}` and nothing else |
| Non-root | `docker image inspect --format '{{.Config.User}}' ghcr.io/raghulj/sidecartunnel:v0.2.0` | `nonroot:nonroot` |
| OCI labels | `docker image inspect --format '{{json .Config.Labels}}' ghcr.io/raghulj/sidecartunnel:v0.2.0` | `source`, `revision`, `version`, `licenses`, `description` |
| Version metadata | `docker run --rm ghcr.io/raghulj/sidecartunnel:v0.2.0 --version` | The tag, the full commit, and the build date |

`ExposedPorts` is the one worth checking every time. `9001` must never appear: `docker run
-P` publishes every exposed port, and `admin.listen` defaults to loopback precisely so the
admin API cannot be reached from outside ([`10-operations.md`](10-operations.md) §1).

## 5. Version Metadata

The build injects three symbols with `-X`:

| Symbol | Value |
|---|---|
| `main.version` | The tag, without the leading `v` |
| `main.commit` | The full commit SHA |
| `main.date` | The build timestamp, RFC 3339 |

They must exist as package-level `var` declarations in `cmd/sidecartunnel/main.go`. The Go
linker **silently ignores** an `-X` for a symbol that is not there: the build succeeds, the
release ships, and the binary reports an empty version. Until `main` declares them the
flags are inert, and the `--version` check in §4 is the thing that catches it.

## 6. Yanking A Bad Release

Three artifacts, three different degrees of permanence. Deal with them in this order.

| Artifact | Mutable? | Action |
|---|---|---|
| Container tags | Yes | Repoint `:latest` and `:X.Y` at the last good version |
| GitHub release | Yes | Mark it as a pre-release, or delete it |
| Git tag and module version | **No, in practice** | The module proxy has already cached it |

```sh
# 1. Stop new pulls landing on it. :latest and :X.Y are mutable; repoint them first.
docker buildx imagetools create \
  --tag ghcr.io/raghulj/sidecartunnel:latest \
  --tag ghcr.io/raghulj/sidecartunnel:0.2 \
  ghcr.io/raghulj/sidecartunnel:v0.1.9

# 2. Take the release out of the "latest release" slot on the repository page.
gh release edit v0.2.0 --prerelease --notes "Withdrawn: <one line on what was wrong>."

# 3. Only if it is minutes old and demonstrably unused.
gh release delete v0.2.0 --yes
git push --delete origin v0.2.0
```

Deleting the git tag does **not** unpublish the version. `proxy.golang.org` caches a module
version permanently and serves it from the cache after the tag is gone, so anyone who ran
`go install` before the deletion keeps it and anyone who runs it after may still get it.
The immutable-looking artifact is the one that cannot be recalled.

The honest fix is forward. Cut `vX.Y.Z+1` with the correction, mark the bad version
`[YANKED]` in `CHANGELOG.md` with one line on what was wrong, and — for a version that is
genuinely dangerous to build against — add a `retract` block to `go.mod` in the follow-up
release:

```
retract v0.2.0 // Grants were matched case-insensitively; see #NN.
```

`retract` reaches `go list -m -versions` and `go get`, which is the only mechanism that
reaches someone who has already downloaded it.

### Whether To Yank At All

| Situation | Do |
|---|---|
| Authorization or Origin handling is wrong | Yank, repoint tags, cut the fix immediately |
| The binary does not start, or the image is broken | Yank and repoint |
| A bug with a workaround, and the release is hours old | Do not yank. Cut a patch and note it in the changelog. |
| The changelog is wrong, the notes have a typo | Edit the release notes. That is not a new version. |

A yank is disruptive to everyone who pinned correctly, which is exactly the population that
did the right thing. Reserve it for cases where continuing to run the version is worse than
the interruption.
