<!--
CONTRIBUTING.md has the setup and the gates. docs/14-coding-standards.md is the normative
version of the same rules. The short version is below.
-->

## What changed

<!-- The change first, then what made it necessary. Name the failure specifically:
     "the cursor went stale over a long weekend and the run aborted" rather than
     "improved error handling". -->

## Requirement

<!-- The FR/NFR number from docs/01-requirements.md this satisfies, or "none" for a
     refactor. `./scripts/trace.sh` checks the traceability and runs in CI. -->

## Checklist

- [ ] Tests written before the implementation, and they failed first
- [ ] `make check` passes — gofmt, vet, staticcheck, `go test -race`, the 100% coverage
      gate and the traceability check
- [ ] Any uncovered line carries a `// coverage: <reason>` comment in the diff, not an
      entry in an allowlist
- [ ] No lock held across network I/O, and nothing new blocking the fan-out path
      (`docs/09-internals.md` §4)
- [ ] No cookie, `Authorization` header, or webhook body reaches a log line
- [ ] A new config key has a documented default in `docs/08-config.md` and a validation
      rule that fails startup loudly
- [ ] A protocol change updates `docs/03-client-protocol.md` in this same commit

## Left undone

<!-- Anything knowingly deferred. A parenthetical here is worth more than a TODO nobody
     reads. -->
