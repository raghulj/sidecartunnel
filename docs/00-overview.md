# 00 — Overview

## What this is

sidecartunnel is a standalone process that holds websocket connections on behalf of an
application that cannot hold them itself. It terminates the sockets, decides which
connection may receive which channel, and fans messages out across its own replicas. The
application talks to it over Redis and one HTTP endpoint.

## Why

Request/response runtimes — WSGI, Rack, PHP-FPM, CGI — cannot hold thousands of open
sockets. A Gunicorn worker handling a websocket is a worker not handling anything else,
and sixteen workers is sixteen concurrent users.

The two usual escapes both cost something I did not want to pay.

**Bolt an async server onto the side.** Flask-SocketIO, ASGI alongside WSGI, a separate
Tornado process sharing the session store. This couples the realtime layer to one
framework's event loop and one language's deployment story, and every dependency upgrade
becomes a negotiation. I ran Flask-SocketIO for a while and removed it.

**Rent a hosted service.** Pusher, Ably, PubNub. These work. What they cost is a vendor in
the path of every notification, an outbound HTTPS call from inside a request handler, and
— the part that actually decided it — an authorization model shaped by the fact that the
vendor cannot reach into my network. See *How it differs from Pusher's model* below.

sidecartunnel is the third option: a process on my own network that owns the sockets and
speaks ordinary HTTP and Redis to the application.

## The principle

> **The gateway enforces. The application decides.**

Every authorization answer originates in the application and reaches the gateway as an
HTTP response. The gateway's entire security model is: verify an `Origin`, then match a
string against a list the application supplied.

This is not tidiness. A realtime layer that re-derives *who may see this* becomes a second
copy of the application's authorization rules — different language, different deploy
cadence, written by someone reading the schema from outside. The two copies drift, and the
drift is silent. It raises no error; it delivers a message to the wrong person.

So the gateway is given **grants**, never **facts**. It is told "this connection may
subscribe to `org-42-*`", never "this user belongs to organisation 42". It cannot tell you
what an organisation is, and that is exactly what makes it reusable.

## Non-goals

Each of these is a feature. Together they are why the thing fits in a few thousand lines.

**Not a message broker.** No durable queues, no delivery receipts, no dead-letter
handling, no redelivery. The application's database is the source of truth and this is an
accelerator in front of it. See `07-delivery.md`.

**Not an identity provider.** No user table, no login, no password, no session store, no
token issuance. It cannot authenticate anyone; it can only ask something that can.

**Not a policy engine.** It holds no rules about who may see what, because it holds no
concept of what anything *is*.

**Not a peer-to-peer relay.** Clients never exchange durable messages without the
application in the path. The one narrow exception under consideration is ephemeral client
events (typing indicators, cursors), gated per namespace and rate limited — see
`12-roadmap.md`.

**Not protocol-compatible with anything.** Not Pusher, not Centrifugo, not Socket.IO.
Pusher compatibility was considered and rejected: its per-subscribe HMAC callback costs a
round trip per channel, and that exists only because Pusher's edge cannot call into my
network. Mine can.

## How it differs from Pusher's model

Worth stating plainly, because the difference explains most of the design.

| | Pusher | sidecartunnel |
|---|---|---|
| Connection | Anonymous — public app key, anyone may connect | Authenticated via the app's own session cookie |
| Authorization | Per subscribe; the app signs an HMAC, Pusher verifies it | Once at connect; the app returns a grant list |
| Gateway → app | Never (webhooks optional) | Once per connection, and on revalidation |
| Publish | Outbound HTTPS + HMAC to the vendor | `redis.publish()` on the local network |
| Public channels | No authorization step at all | No such thing; every channel is matched against grants |
| Latency per push | Internet round trip | Sub-millisecond |

Pusher's design falls out of one constraint: it is a multi-tenant service on the public
internet that cannot reach into a customer's network, so authorization has to be a bearer
capability the client carries. Every awkward part follows from that. A sidecar on the same
Docker network does not have the constraint, so it does not need the workaround.

What Pusher's model does better, and which is worth stealing eventually: signing on demand
scales to an unbounded channel set. A grant list assumes the set is enumerable. See the
open question in `12-roadmap.md`.

## Glossary

| Term | Meaning |
|---|---|
| **gateway** | One sidecartunnel process. Also "replica" when there are several. |
| **application** | The consuming app — Flask, Django, Rails. Never named more specifically anywhere in this repository. |
| **connection** | One websocket, one browser tab. Identified by a client id. |
| **client id** | Random 16-hex-character identifier for one connection. Appears in `exclude` and in logs. |
| **channel** | An opaque string clients subscribe to. Never parsed for meaning beyond its namespace prefix. |
| **namespace** | The substring of a channel before the first separator. Selects a block of configuration. |
| **grant** | A glob pattern the application issued, naming channels a connection may subscribe to. |
| **connect webhook** | The single HTTP endpoint the application exposes so the gateway can ask "who is this, and what may they see?" |
| **bus** | The transport carrying messages between replicas. Redis pub/sub by default. |
| **control channel** | A reserved bus channel carrying operational commands (disconnect, refresh) to every replica. |
| **push** | A message travelling gateway → client. |
| **client event** | An ephemeral message travelling client → gateway → other clients, never persisted. |
