# CLAUDE.md — working rules for this repository

Read [docs/AGENTS.md](docs/AGENTS.md) in full before writing any code. This file is the
short version; that one has the reasoning.

## What this is

sidecartunnel is an application-agnostic websocket gateway. It is a **product**, not an
adapter for any one application. Nothing in this repository may name a domain concept
belonging to a consuming application — no tenants, merchants, orgs, accounts, or
workspaces as first-class ideas. Channels are opaque strings. If a design sentence names
a domain noun, it belongs in that application's integration notes, not here.

## The invariant everything else serves

**The gateway enforces; the application decides.** Every authorization answer originates
in the consuming application and reaches the gateway as an HTTP response. The gateway's
entire security model is: verify an Origin, then match a string against a list the
application supplied. It holds no policy, no user store, and no rule that could ever
disagree with the application's.

Any change that makes the gateway *derive* an authorization decision rather than *enforce*
one is wrong, regardless of how much convenience it buys.

## Before you write code

1. `docs/01-requirements.md` — the requirement you are satisfying, by its FR/NFR number.
2. The normative document for the surface you are touching: `03-client-protocol.md`,
   `04-integration.md`, or `08-config.md`.
3. `docs/09-internals.md` — the concurrency model. Get this wrong and the failures are
   intermittent and awful to reproduce.

## Non-negotiables

- **Never block the fan-out path.** A slow client is disconnected, never waited on.
- **Never hold a lock across network I/O.** Bus subscribe/unsubscribe is dispatched to a
  serialized command loop, not called under a shard lock.
- **Never log a cookie, an `Authorization` header, or a webhook body.** Log the client id.
- **Never trust the `Origin` header without checking it against the allowlist.** Browsers
  do not apply CORS to websocket handshakes; this check is the only thing standing
  between a logged-in user and cross-site websocket hijacking.
- **Every new config key gets a documented default in `docs/08-config.md`** and a
  validation rule that fails startup loudly rather than defaulting silently.
- **Protocol changes are documentation changes first.** Update `03-client-protocol.md` in
  the same commit, or the spec stops being trustworthy and everything downstream rots.

## Style

Standard Go. `gofmt`, `go vet`, `staticcheck` clean. Table-driven tests. No framework, no
DI container, no code generation. Dependencies are a cost — the target is `gorilla/websocket`
or `coder/websocket`, a Redis client, a YAML parser, and the Prometheus client. Adding a
fifth needs a reason written into the commit message.
