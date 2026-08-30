# sidecartunnel

A websocket gateway that sits beside an application which cannot hold long-lived
connections itself, and gives that application realtime without changing how it is
deployed. The app publishes by writing to Redis and authorizes by answering one HTTP
call. It never imports a websocket library, never runs an event loop, and never learns
anything new about its own users.

**None of this is implemented yet.** Everything under `docs/` is specification written
before the first line of Go exists. I expect parts of it to be wrong once there is code
to argue with, and the open questions in [docs/12-roadmap.md](docs/12-roadmap.md) are the
parts I already know are unsettled.

## Why not one of the ones that already exist

**[Pusher](https://pusher.com/channels)** works, and I ran it in production for years.
The model has one property I could not keep living with: the connection itself is
anonymous, because Pusher's edge cannot reach into my network to check a session.
Authorization is per-channel, signed by me and verified by them — so *public* channels
have no authorization step at all, and every push is a blocking HTTPS call to someone
else's infrastructure from inside a request handler.

**[Centrifugo](https://centrifugal.dev)** is Go, mature, and — I have to be straight about
this — already does the thing I thought was novel here. Its
[connect proxy](https://centrifugal.dev/docs/server/proxy) forwards configured headers
including `Cookie` to an endpoint on my backend, which returns a user id and a `subs` map.
Its subscribe proxy is the per-channel callback I have written down as a future
possibility. The docs put it plainly: *"you don't need to generate JWT and pass it to a
client-side and can rely on a cookie."*

An earlier version of this README claimed Centrifugo was JWT-first. That was wrong — JWT is
one of its two modes, not the product — and an adversarial review of the spec caught it
(`docs/13-review-findings.md` M22).

So the honest position: **there is no capability here that Centrifugo lacks.** What is left
is preference — a surface small enough to read in an afternoon, and owning the code end to
end. Those are real reasons to build something, and they are not the same as a reason it
needs to exist. If the choice is being made on capability, take Centrifugo.

**[soketi](https://github.com/soketi/soketi)** is Pusher-protocol-compatible and
self-hosted, which is close to right, but it is Node and has been effectively
unmaintained since 2024.

## How it works

1. The browser opens a websocket to the **same origin** as the app, so the session cookie
   rides along on the HTTP upgrade. There is no authentication code in the frontend.
2. The gateway checks `Origin` against an allowlist, then forwards the cookie verbatim to
   one endpoint on the app.
3. The app resolves its own session however it already does, and answers with a user id
   and a list of channel glob patterns that connection may subscribe to.
4. To push, the app calls `redis.publish("st:room-4410", …)`. Every gateway replica
   holding that channel delivers to its own local sockets.
5. Durable writes from the client go over ordinary HTTP to the app, which persists them
   and then publishes. The socket is receive-only for anything that matters.

**Target scale: 20,000 concurrent connections across two replicas.** That is deliberately
modest, and it is what lets the hub be one map behind one mutex rather than a sharded
structure. Sizing, and the one number that actually constrains it, are in
[docs/10-operations.md](docs/10-operations.md) §8.

The one idea the whole design rests on: **the gateway enforces, the application decides.**
Grants are opaque strings the gateway matches. It cannot tell you what `org-42` means.

## Documentation

Read [docs/README.md](docs/README.md) first — it has the reading order and says which
documents are normative.

| Document | Covers |
|---|---|
| [00-overview](docs/00-overview.md) | Product definition, the principle, non-goals, glossary |
| [01-requirements](docs/01-requirements.md) | Numbered functional and non-functional requirements |
| [02-architecture](docs/02-architecture.md) | Topology, components, end-to-end flows |
| [03-client-protocol](docs/03-client-protocol.md) | **Normative.** Websocket wire protocol |
| [04-integration](docs/04-integration.md) | **Normative.** Webhook, Redis publish, control channel, admin API |
| [05-authorization](docs/05-authorization.md) | Grants, Origin, expiry, revocation, threat model |
| [06-channels](docs/06-channels.md) | Channel naming and namespace configuration |
| [07-delivery](docs/07-delivery.md) | Delivery semantics, backpressure, reconnect |
| [08-config](docs/08-config.md) | **Normative.** Full configuration reference |
| [09-internals](docs/09-internals.md) | Go package layout, concurrency model, data structures |
| [10-operations](docs/10-operations.md) | Deployment, metrics, runbook |
| [11-testing](docs/11-testing.md) | What must be tested before anything ships |
| [12-roadmap](docs/12-roadmap.md) | Milestones, scope ladder, open decisions |
| [AGENTS](docs/AGENTS.md) | Rules for agents working in this repository |

Two interactive companions live in `docs/` as HTML: `sidecartunnel-spec.html` (the
design argument) and `sidecartunnel-simulator.html` (nine scenarios stepped one frame at
a time). Neither is normative — where they disagree with the Markdown, the Markdown wins.
