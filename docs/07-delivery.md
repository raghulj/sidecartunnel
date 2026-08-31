# 07 — Delivery semantics

## 1. At-most-once, and why that is the answer

Messages can be lost. A replica restart, a network partition, a client asleep in a tunnel
— in each case the message is published, nobody is there, and nothing keeps it.

The alternative is at-least-once, and the cost of it is not subtle: durable per-subscriber
cursors, acknowledgement frames, redelivery windows, deduplication on the client, and a
storage tier that has to be operated and backed up. That is a broker. Building one inside
a gateway contradicts every non-goal in `00-overview.md`, and there are better brokers.

So the gateway does not try. What makes that acceptable is §2.

## 2. Reconciliation, not redelivery

The application already persists whatever it publishes — that is what makes the publish
worth sending. So the client closes its own gaps:

```
on reconnect:
    resubscribe every channel in the local registry
    GET /api/messages?room=4410&since={last_seen_id}
    merge
```

Realtime becomes an accelerator in front of a database that was always the source of
truth. This handles a replica restart, and it also handles laptop sleep, a tunnel drop, a
throttled background tab, and a four-hour disconnection — none of which any redelivery
scheme handles well, because all of them exceed whatever window the buffer was sized for.

This is the single most important thing for an integrating application to implement, and
the easiest to skip because everything appears to work without it until the first deploy.
It is on the checklist in `04-integration.md` §5 for that reason.

## 3. What "stateless" means here

The gateway is stateless in the sense that losing a replica loses **no data** — only
connections, which clients already know how to re-establish. It is obviously not stateless
in the trivial sense; it holds sockets, grants and subscriptions in memory.

The distinction matters when reasoning about restarts. Nothing needs draining to disk,
nothing needs to be handed over to another replica, and no state needs replicating. A
replica can be killed at any moment and the worst outcome is a reconnect storm, which is
an operational problem (`10-operations.md` §4) rather than a correctness one.

## 4. Backpressure

Every connection has an outbound queue of `limits.outbound_queue` messages, default 256.

When the queue is full, the connection is **closed** with code 3005. It is not blocked on,
and messages are not dropped silently.

The reasoning is worth spelling out because both alternatives look reasonable:

- **Block until there is room.** The fan-out path stalls behind the slowest subscriber in
  the channel. One phone in a tunnel delays delivery for everyone else. Unacceptable.
- **Drop the message, keep the connection.** The client is now silently inconsistent, with
  no signal that it missed anything and no way to find out.
- **Close the connection.** The client reconnects, resubscribes, and reconciles from the
  database (§2) — ending up *consistent*, having noticed.

Closing is the only option that leaves the client correct. This is also where naive
implementations of this pattern fail, usually in production, usually during the incident
that caused the slow consumer in the first place.

Queue depth is a tuning knob, but a cheaper one than it looks. **The queue holds pointers
to a single shared, immutable buffer** encoded once per message (`09-internals.md` §5), so
depth costs 256 × 16 bytes ≈ 4 KiB per connection, not 256 × message size. Getting this
wrong on paper produced a 160 GiB memory estimate against a 1 GiB budget. Watch
`st_slow_consumer_disconnects_total`; sustained non-zero means the depth is too small for
the message rate, or the clients are genuinely bad.

## 5. Ordering

**There is no ordering guarantee, not even within one channel.** Do not rely on one.

An earlier version of this section said per-channel order "is preserved in practice" while
declining to guarantee it. That was measured and it is false: with `bus.dispatch_workers`
at its default of 2, two messages published to one channel are fanned out by different
workers concurrently and reach a socket in either order. At burst rate the integration
suite saw the last message before a marker arrive *after* it in roughly two runs in three.

Saying "not guaranteed but true in practice" is the worst of both worlds. Nobody writes the
reconciliation code, and the reordering shows up later as a bug that reproduces once a week.

Where order matters, put an id or a version in the payload and let the client sort. That is
the same field `?since=` reconciliation already needs (§2), so it costs nothing extra.
Setting `dispatch_workers: 1` makes reordering unlikely rather than impossible, and gives
up the M8 protection the workers exist to provide — it is not a fix.

Across channels there is no ordering at all, and never was.

## 6. Duplicates

The gateway does not generate duplicates. The application might — a retried Celery task
publishing twice — and the client's merge should be idempotent on the payload's id. Again,
the same field.

## 7. Size limits

| Limit | Default | On breach |
|---|---|---|
| Published envelope | 32 KiB | Dropped, logged once, counted |
| Client frame | 16 KiB | Connection closed, 3006 |
| Channel name | 255 bytes | Subscribe refused, 101 |

The publish limit is lower than it looks generous. A realtime message should be a
notification — an id and enough to render a row — not a document. If a payload is
approaching 32 KiB, the right shape is to publish an id and let the client fetch, which
also makes the message survivable by reconciliation.
