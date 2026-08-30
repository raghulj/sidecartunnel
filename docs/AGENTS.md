# Rules for agents working in this repository

The short version is in the root `CLAUDE.md`. This is the reasoning behind it, plus the
mistakes I expect and want to head off.

## 1. This is a product, not an adapter

The strongest pull on this project is toward the application I will integrate first. Every
time I have described the design in terms of that application's concepts, the design got
worse — it acquired a tenant model, then a permission model, and at that point the gateway
was a second copy of an authorization system that already existed somewhere better.

So: **no domain nouns.** No tenants, merchants, orgs, workspaces, accounts, teams, rooms
as *concepts*. `room-4410` appears in examples as an opaque string and nothing in the code
may parse it for meaning beyond splitting off the namespace prefix.

The test: if a type, function, config key, or comment would need to change when a second,
unrelated application adopts this, it is wrong.

## 2. The invariant

**The gateway enforces; the application decides.**

Every authorization answer originates in the consuming application. The gateway verifies
an `Origin`, then matches strings against a list the application supplied. It holds no
policy of its own.

The failure this prevents is specific and I have watched it happen: a realtime layer that
re-derives "who may see this" becomes a second implementation of the application's
authorization rules, written in another language, deployed on another cadence, by someone
reading the schema from outside. The two drift. The drift is silent — it raises no error,
it just delivers a message to the wrong person.

Any change that makes the gateway *derive* rather than *enforce* is wrong, however
convenient.

## 3. Before writing code

Read, in order:

1. `01-requirements.md` — find the FR/NFR number you are satisfying. Cite it in the commit.
2. The normative doc for your surface — `03-client-protocol.md`, `04-integration.md`, or
   `08-config.md`.
3. `09-internals.md` — the concurrency model. This is the part where mistakes produce
   intermittent failures that are miserable to reproduce, so it is worth the ten minutes.

If what you are about to build is not covered by a requirement, stop and add the
requirement first. Scope that arrives as code rather than as a requirement is how a small
product stops being small.

## 4. Non-negotiables

These are not style preferences. Each one exists because the alternative fails in
production in a way that is hard to see from a test suite.

- **Never block the fan-out path.** A connection with a full outbound queue is closed, not
  waited on. One client on a failing connection must not stall delivery for the rest of
  the channel. See `07-delivery.md` §4.
- **Never hold a lock across network I/O.** Bus subscribe/unsubscribe is dispatched to a
  serialized command loop; it is never called while a shard mutex is held. See
  `09-internals.md` §4.
- **Never log a cookie, an `Authorization` header, a webhook request body, or a message
  payload.** Log the client id and the channel. The gateway sees every user's session
  cookie; its logs must not become a credential store.
- **Never accept a websocket handshake without checking `Origin` against the allowlist.**
  Browsers do not apply CORS to websocket upgrades but do attach cookies. This check is
  the only thing between a logged-in user and cross-site websocket hijacking. See
  `05-authorization.md` §5.
- **Never let a config key default silently.** Every key has a documented default in
  `08-config.md` or fails startup with a message naming the key.
- **Never change protocol behaviour without changing `03-client-protocol.md` in the same
  commit.** A spec that lags the code is worse than no spec, because people trust it.

## 5. Reporting work

State what you changed and what made it necessary, in that order, naming the failure
specifically. "Cursor went stale over a long weekend and the run aborted" beats "improved
error handling." Anything knowingly left undone goes in the commit message as a
parenthetical, not a TODO comment nobody will read.

A claim is not a fact. If you say the fan-out works across replicas, the evidence is a
test that runs two gateway instances against one Redis, not a reading of the code.

## 6. Dependencies

The target dependency set is four: a websocket library (`gorilla/websocket` or
`coder/websocket`), a Redis client, a YAML parser, and the Prometheus client. A fifth
needs a reason written into the commit message. No framework, no DI container, no code
generation, no ORM.

## 7. What to do when the spec is wrong

It will be, in places. It was written before there was code to argue with. When you find
a contradiction or something unimplementable, fix the document and say so — do not
implement around it and leave the document standing. A wrong spec that nobody corrects is
how the next agent gets confidently misled.
