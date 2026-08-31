# Contributing

sidecartunnel is an application-agnostic websocket gateway, specified in full before any
of it was implemented. The specification lives in [`docs/`](docs/) and it is the contract:
three of those documents are normative, and code that disagrees with one of them is a bug
in the code.

None of this is a formality. The specification was reviewed adversarially before a line of
Go existed, and the review found 8 critical and 22 major defects
([`docs/13-review-findings.md`](docs/13-review-findings.md)). Several things in this
codebase look like needless complication and are not — they are the shape that survived
that review. Reading the findings first will save you re-proposing something already
rejected for a stated reason.

## Before you write anything

Read, in this order:

1. [`CLAUDE.md`](CLAUDE.md) — the short version of the working rules.
2. [`docs/AGENTS.md`](docs/AGENTS.md) — the same rules with the reasoning behind them.
3. [`docs/01-requirements.md`](docs/01-requirements.md) — find the FR or NFR number you are
   satisfying. You will cite it in the commit and in the code.
4. The normative document for the surface you are touching:
   [`03-client-protocol.md`](docs/03-client-protocol.md),
   [`04-integration.md`](docs/04-integration.md), or
   [`08-config.md`](docs/08-config.md).
5. [`docs/09-internals.md`](docs/09-internals.md) §4 — the concurrency model. This is where
   mistakes turn into intermittent failures that are miserable to reproduce, so it is worth
   the ten minutes.
6. [`docs/14-coding-standards.md`](docs/14-coding-standards.md) — normative, and the rest
   of this file assumes it.

If what you are about to build is not covered by a requirement, stop and add the
requirement first. Scope that arrives as code rather than as a requirement is how a small
product stops being small.

## Getting set up

Go 1.26 and `golangci-lint` v2.12.2. Docker for the integration tests.

```
git clone https://github.com/raghulj/sidecartunnel
cd sidecartunnel
go mod download
make check
```

`make` with no argument runs `check`, which is lint, tests and the coverage gate — the same
three things CI runs.

| Target | What it does |
|---|---|
| `make check` | Lint, test, coverage gate. The default. |
| `make test` | `go test -race -cover ./...` |
| `make cover` | 100% coverage gate, with a per-package table |
| `make lint` | `golangci-lint run ./...` |
| `make build` | The binary, with the release flags |
| `make redis` | A throwaway Redis on `:6379` for the integration tests |
| `make redis-stop` | Remove it |
| `make clean` | Remove build and coverage artifacts |

The integration layer needs a real Redis. `make redis` starts one with the pub/sub output
buffer raised — the default `client-output-buffer-limit pubsub` disconnects a subscriber
that falls behind during a broadcast burst, which shows up as a flaky test blaming the
wrong component ([`docs/10-operations.md`](docs/10-operations.md) §3).

## Tests first. Actually first.

Write the test, watch it fail, then write the code.

**Every pull request shows its tests first** — either the test commit precedes the
implementation commit, or the description says which test was failing and what it asserted
before the fix landed. "Tests included" is not the claim being made; "this test failed with
this output, then it passed" is.

A test written after the implementation is written against the implementation's behaviour,
including its bugs. It passes the first time it runs, which is the moment it has told you
nothing.

The full rules are in [`docs/14-coding-standards.md`](docs/14-coding-standards.md) §1–3.
The short version:

- Table-driven, with the interesting case sitting next to the boring ones.
- `-race` on every run. Not a nightly job.
- No `time.Sleep`. Synchronise with a channel, a `WaitGroup`, or an injected clock. A
  generous timeout as a *failure* detector is fine; a sleep as a *synchroniser* is not.
- Name the requirement the test satisfies, so that when a requirement changes the affected
  tests are findable with grep.

## The coverage gate

100% of statements, per package, checked by `./scripts/cover.sh` in `make cover` and in CI.
There is no soft target and no ratchet.

80% and 90% gates do not work here, and the reason is that the uncovered fraction is never
randomly distributed. It is the error paths — Redis down, webhook 500, queue full — and
those are the branches this whole design turns on.

If a line genuinely cannot be covered, justify it in place:

```go
// coverage: only reachable on a full disk; not worth a fault-injection harness for a
// path that logs and continues.
```

`scripts/cover.sh` reads those comments and exempts the block. The exemption lives in the
diff where a reviewer can disagree with it, which is the point — an allowlist in a config
file is a place to hide things. A `// coverage:` with no reason after the colon is not an
exemption, and neither is "hard to test", which is a description of a design problem.

## Commit messages

Per [`docs/AGENTS.md`](docs/AGENTS.md) §5: **what you changed and what made it necessary,
in that order, naming the failure specifically.**

```
hub: close slow consumers off the dispatch goroutine

Closing inline took the write lock while fan-out held the read lock, so a single
slow client deadlocked delivery for the whole replica under load. Slow connections
are now collected under the read lock and handed to a closer goroutine after it is
released.

FR-15, docs/09-internals.md §4.5.

(The closer channel is unbounded for now; a bounded one with a `go c.Close()`
fallback is the documented shape and is not built yet.)
```

The parts that matter:

- A subject line of `package: what changed`, imperative, under ~72 characters.
- The body names the failure. "Cursor went stale over a long weekend and the run aborted"
  beats "improved error handling."
- Cite the FR or NFR number, and the document section if one mandates the shape.
- Anything knowingly left undone goes in as a parenthetical, not a `TODO` comment nobody
  will read.

No `Co-authored-by` boilerplate, no emoji prefixes, no `feat:`/`fix:` conventional-commit
tags. The subject line has room for a sentence; use it for a sentence.

## Pull requests

- A claim is not a fact. If the description says fan-out works across replicas, the
  evidence is a test running two gateway processes against one Redis, not a reading of the
  code.
- `make check` passes. CI runs the same steps, so a red CI is a local step that was
  skipped.
- Protocol, integration or configuration changes update the normative document **in the
  same commit**. A spec that lags the code is worse than no spec, because people trust it.
- Say what is left undone. The uncomfortable sentence in a description is usually the most
  useful one in it.

## Proposing a change to the specification

The specification will be wrong in places — it was written before there was code to argue
with. When you find a contradiction or something unimplementable, **fix the document
first**, in its own pull request, and say what you found.

Do not implement around it and leave the document standing. A wrong spec that nobody
corrects is how the next person gets confidently misled, and it is a slower failure than a
bug.

For a change that adds behaviour rather than correcting a mistake:

1. Add or amend the requirement in `docs/01-requirements.md`, with an acceptance criterion.
   A requirement without one is a wish.
2. Update the normative document for the surface — `03`, `04` or `08`.
3. Then open the implementation pull request, citing the requirement number.

Two things to know before proposing:

- **The invariant is not negotiable.** The gateway enforces; the application decides. Any
  change that makes the gateway *derive* an authorization answer rather than *enforce* one
  is wrong, however convenient. See [`docs/AGENTS.md`](docs/AGENTS.md) §2.
- **No domain nouns.** No tenants, merchants, orgs, workspaces, accounts or rooms as
  concepts. Channels are opaque strings. The test: if a type, function, config key or
  comment would need to change when a second, unrelated application adopts this, it is
  wrong.

Adding a dependency needs a reason written into the commit message. The budget is three —
`gorilla/websocket`, a Redis client, and a YAML parser — and a fourth is a decision, not a
detail.

## Licence

MIT, in [`LICENSE`](LICENSE). By contributing you agree your contribution is licensed
under it.
