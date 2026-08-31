# Documentation map

Specification for sidecartunnel, written before implementation. Nothing here is
implemented yet.

## Reading order

If you are new to the project, read in this order. It is arranged so each document only
depends on the ones above it.

1. **[00-overview.md](00-overview.md)** — what this is, the one principle, non-goals,
   glossary. Ten minutes, and everything else assumes it.
2. **[02-architecture.md](02-architecture.md)** — the shape of the running system and the
   four end-to-end flows.
3. **[01-requirements.md](01-requirements.md)** — numbered requirements with acceptance
   criteria. This is the checklist a milestone is measured against.
4. **[13-review-findings.md](13-review-findings.md)** — an adversarial review found 8
   critical and 22 major defects in the first draft. Its five structural changes (S1–S5)
   are the reason several documents look the way they do, and reading them will save you
   re-proposing something already rejected for a stated reason.
5. Then whichever surface you are working on.

## Normative vs. explanatory

Four documents are **normative**. An implementation that disagrees with them is wrong,
and a change to behaviour they describe must change them in the same commit:

- [03-client-protocol.md](03-client-protocol.md) — the websocket wire protocol
- [04-integration.md](04-integration.md) — webhook, Redis contract, control channel
- [08-config.md](08-config.md) — configuration keys, defaults, validation
- [14-coding-standards.md](14-coding-standards.md) — how the code is written and what CI enforces

The first three describe behaviour the outside world can observe. The fourth describes how
the inside is built, and it is normative for the same reason the others are: people
implementing packages in parallel against one contract need a single answer to "is this
test good enough", the same way they need a single answer to "what does close code 3008
mean".

Everything else explains, justifies, or plans. Where an explanatory document disagrees
with a normative one, the normative one wins and the other is a bug.

The two HTML files here — `sidecartunnel-spec.html` and `sidecartunnel-simulator.html` —
are companions for reading and demonstration. They are **not** normative and they lag the
Markdown.

## Full index

| # | Document | Kind | Covers |
|---|---|---|---|
| 00 | [overview](00-overview.md) | explanatory | Definition, principle, non-goals, glossary |
| 01 | [requirements](01-requirements.md) | normative-ish | FR/NFR with acceptance criteria |
| 02 | [architecture](02-architecture.md) | explanatory | Topology, components, flows |
| 03 | [client-protocol](03-client-protocol.md) | **normative** | Websocket frames, codes, lifecycle |
| 04 | [integration](04-integration.md) | **normative** | Connect webhook, Redis publish, control |
| 05 | [authorization](05-authorization.md) | explanatory | Grants, Origin, expiry, revocation, threat model |
| 06 | [channels](06-channels.md) | explanatory | Naming rules and namespace config |
| 07 | [delivery](07-delivery.md) | explanatory | At-most-once, backpressure, reconciliation |
| 08 | [config](08-config.md) | **normative** | Every key, default, and validation rule |
| 09 | [internals](09-internals.md) | explanatory | Package layout, concurrency, data structures |
| 10 | [operations](10-operations.md) | explanatory | Deploy, observability, logs, runbook, capacity |
| 11 | [testing](11-testing.md) | normative-ish | Required test coverage per milestone |
| 12 | [roadmap](12-roadmap.md) | explanatory | Milestones, scope ladder, open decisions |
| 13 | [review-findings](13-review-findings.md) | explanatory | Adversarial review of M0 and what changed because of it |
| 14 | [coding-standards](14-coding-standards.md) | **normative** | TDD, coverage gate, comments, concurrency rules |
| 15 | [releasing](15-releasing.md) | explanatory | Versioning policy, cutting a release, verifying and yanking artifacts |
| 16 | [integration-guide](16-integration-guide.md) | explanatory | Adopting this in an existing application, with a worked Flask example |
| 17 | [production-readiness](17-production-readiness.md) | explanatory | Everything required before real users |
| 14 | [coding-standards](14-coding-standards.md) | **normative** | TDD, coverage gate, comments, concurrency, security |
| — | [AGENTS](AGENTS.md) | normative-ish | How to work in this repository |

## Status

| Milestone | State |
|---|---|
| M0 — specification | reviewed 2026-08-31; 44 findings, see [13](13-review-findings.md) |
| M1 — connect, subscribe, publish, fan-out | not started |
| M2 — expiry, revocation, drain | not started |
| M3 — client library | not started |
| M4 — client events, presence, history | not started, may not happen |

Open decisions live at the bottom of [12-roadmap.md](12-roadmap.md). They are open on
purpose; do not resolve one by writing code that assumes an answer.
