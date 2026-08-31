# 02 — Architecture

## The running system

```
                     TRUST BOUNDARY — all of this is one private network
                                    │
  ┌──────────┐                      │
  │ Browser  │══ wss ══╗            │
  └──────────┘         ║            │
  ┌──────────┐    ┌────╨──────────┐ │   POST /_st/connect   ┌─────────────────┐
  │ Browser  │════│ sidecartunnel │─┼──────────────────────▶│                 │
  └──────────┘    │   replica 1   │ │                       │  Application    │
  ┌──────────┐    └────┬──────────┘ │                       │  (WSGI/Rack/…)  │
  │ Browser  │════┌────┴──────────┐ │                       │                 │
  └──────────┘    │ sidecartunnel │ │                       │  + Postgres     │
                  │   replica N   │ │                       └────────┬────────┘
                  └────┬──────────┘ │                                │
                       │            │                    redis.publish()
                  SUBSCRIBE         │                                │
                       │            │                                │
                  ┌────┴────────────┴────────────────────────────────┴────┐
                  │                    Redis pub/sub                      │
                  │              fan-out only — holds no state            │
                  └───────────────────────────────────────────────────────┘
```

Replicas share nothing. A publish arriving anywhere reaches subscribers everywhere,
which is why the load balancer needs no sticky sessions and scaling is a replica count.

## Components

| Component | Holds | Survives restart? |
|---|---|---|
| Gateway replica | Live sockets, their grants and subscriptions | No — clients reconnect |
| Redis | Nothing but in-flight pub/sub delivery | N/A |
| Application | Everything that matters | Yes |

The gateway is **stateless in the sense that matters**: losing a replica loses no data,
only connections, and a connection is a thing clients already know how to re-establish.
It is not stateless in the trivial sense — it obviously holds sockets in memory — and the
distinction matters when reasoning about restarts. See `07-delivery.md` §3.

## Flow 1 — connect

1. Browser opens `wss://app.example.com/ws`. Same origin as the app, so the browser
   attaches the session cookie to the upgrade request without any frontend code.
2. Gateway checks `Origin` against the allowlist. Mismatch → 403, nothing else happens.
3. Gateway `POST`s the connect webhook, forwarding `Cookie` verbatim plus context headers.
4. Application resolves its own session and returns `{user, channels[], expires_in}`.
5. Gateway stores the grants against the connection and replies `{"id":1,"connect":{…}}`.

The application's involvement is one endpoint. It does not learn that websockets exist.

## Flow 2 — server push

1. Application does whatever it does, commits its transaction.
2. Application calls `redis.publish("st:room-4410", envelope)`.
3. Every replica with a local subscriber to `room-4410` receives it — precisely those, not
   all of them, because subscriptions upstream are per-channel and refcounted (FR-10).
4. Each replica writes to its own sockets.

No HTTP, no signing, no dependency on the gateway being reachable. Any process can
publish: a web worker, a background task, a cron job, a shell script.

## Flow 3 — durable client write

1. Browser `POST`s to the application over ordinary HTTP. CSRF, rate limiting, WAF,
   request logging and validation all apply unchanged.
2. Application persists.
3. Application publishes, with `exclude` naming the originating connection.
4. Everyone except the sender receives the push.

The socket is receive-only for anything durable. This costs one extra HTTP request per
send and buys keeping every write path that is already reviewed and trusted. The
alternative — forwarding client frames to the app from the gateway — needs an RPC layer
for results and a rate limiter to stop a socket becoming an unmetered firehose into a
fixed worker pool. See the open question in `12-roadmap.md`.

## Flow 4 — revocation

1. Application decides a user must be cut off now.
2. Application publishes `{"action":"disconnect","user":"u-7"}` to the control channel.
3. Every replica closes that user's connections with `reconnect: false`.

Using the bus rather than an HTTP call keeps this a one-liner for the application and
reaches every replica without service discovery. It is also the only revocation route —
there is no second, HTTP one for an operator who is not the application. See
`04-integration.md` §3.

## What runs where

A gateway replica has exactly three network relationships:

- **inbound** websockets and HTTP (`/health`, `/ready`), on one listener
- **outbound** HTTP to the application's connect webhook
- **bidirectional** to Redis

It never connects to the application's database, never reads its session store, and never
holds a credential belonging to a user.

## Deployment shape

Behind whatever already terminates TLS, routed by path so the socket is same-origin:

```
example.com/       → application
example.com/ws     → sidecartunnel  (upgrade passthrough, idle timeout > ping_interval)
```

Same-origin routing is a recommendation, not a requirement — a separate `ws.example.com`
works too, being same-site — but it costs cookie-domain configuration for nothing.
See `10-operations.md` §2.
