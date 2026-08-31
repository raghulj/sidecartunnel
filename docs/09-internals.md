# 09 — Internals

How the Go side is put together. Read §4 before touching anything concurrent; that is
where mistakes turn into intermittent failures that are miserable to reproduce.

## 1. Package layout

```
cmd/sidecartunnel/     main, wiring, healthcheck
internal/bus/          redis and memory transports
internal/config/       load, defaults, env overlay, validate
internal/conn/         one connection: two goroutines, grants, commands
internal/consumer/     the bus consumer: dispatch workers, control routing, FR-23 verification
internal/glob/         grant pattern matching
internal/hub/          channel registry, fan-out, reconciler, control
internal/proto/        frame codec and close codes
internal/server/       listener, upgrade, origin check, GET /health, GET /ready
internal/webhook/      the connect webhook client
```

`internal/metrics/` and `internal/admin/` are gone. `internal/server` gained `probes.go`
(`GET /health`, `GET /ready`, `StopAccepting`).

Dependency direction is downward only. `hub` does not import `conn`; it holds an interface
with `Send([]byte)` and `Close(code, reason)` so the two can be tested apart. `bus` knows
nothing about channels-as-a-concept, only strings and bytes.

## 2. Core types

```go
type Conn struct {
    id      string            // 16 hex chars, crypto/rand
    app     string            // which configured app
    user    string            // opaque, from the webhook
    grants  atomic.Pointer[grantSet]   // immutable; swapped, never mutated
    expires time.Time

    out     chan *Frame       // buffered, limits.outbound_queue — POINTERS, see §5
    subs    map[string]struct{} // guarded by the hub lock, not by mu
    mu      sync.Mutex        // guards nothing on the match path
    closed  atomic.Bool
    once    sync.Once
}

type Hub struct {
    mu      sync.RWMutex       // ONE lock — see §4.2 before sharding this
    channels map[string]map[*Conn]struct{}   // keyed by BUS KEY
    bus     bus.Bus
    desired sync.Map           // busKey -> struct{}; the reconciler's target state
    dirty   atomic.Bool
    wake    chan struct{}      // capacity 1, non-blocking send
}

type Bus interface {
    // Sync makes the bus subscription set exactly `desired`. Idempotent, batched.
    Sync(ctx context.Context, desired []string) error
    Publish(ctx context.Context, channel string, payload []byte) error
    Receive() <-chan Message
}

`grants` is an atomic pointer to an **immutable** set. Matching is a lock-free load, which
is what makes FR-9 satisfiable — the earlier design declared `grants` as guarded by
`Conn.mu`, which either violated FR-9 or produced a torn slice header under `-race` the
first time a revalidation swapped it.

The hub map is keyed by the **bus key** (`{bus.prefix}{channel}`), never the bare channel
name. That is what makes cross-delivery structurally impossible rather than a filter
someone has to remember to apply.
```

## 3. Goroutines per connection

Exactly two, plus whatever the runtime uses for the socket:

- **reader** — blocking read loop. Parses one frame, handles the command inline (grant
  matching is in-memory and cheap enough that a queue would only add latency), writes any
  reply to `out`. Exits on read error or close.
- **writer** — drains `out`, writes to the socket, sends protocol pings on a ticker. The
  only goroutine that ever touches the socket for writing, which is what makes concurrent
  publishes safe without a write mutex.

Everything else — fan-out, control commands, expiry — reaches a connection by appending to
`out`. Nothing outside the writer writes to the socket. Ever.

**The hub is one map behind one `RWMutex`.** An earlier draft specified 32 shards. At the
target scale that is unjustified: fanning out to 10,000 connections is ~10,000 map
iterations — roughly 0.2 ms holding the read lock — and subscribes are rare by comparison.
Sharding buys nothing measurable and costs a shard-index concept threaded through every
call site. NFR-9 forbids building it until a profile shows contention above 5% of fan-out
time, and §4's lock rules are written so that sharding later is a mechanical change rather
than a redesign.

`Close` is `sync.Once`-guarded, sets `closed`, sends the `disconnect` frame if there is
room, closes the socket, and removes the connection from the hub map.

## 4. Concurrency rules

These four exist because their opposites produce bugs that pass code review.

**4.1 Never hold a hub lock across network I/O, and never block to schedule bus work.**

```go
func (h *Hub) Subscribe(c *Conn, key string) {
    h.mu.Lock()
    set, ok := h.channels[key]
    if !ok { set = map[*Conn]struct{}{}; h.channels[key] = set }
    set[c] = struct{}{}
    c.subs[key] = struct{}{}          // same lock — see 4.2
    if len(set) == 1 {
        h.desired[key] = struct{}{}   // ALSO under the lock — see below
    }
    h.mu.Unlock()

    h.markDirty()                     // the only thing that happens after unlock
}

func (h *Hub) markDirty() {
    h.dirty.Store(true)
    select { case h.wake <- struct{}{}: default: }   // never blocks
}
```

An earlier version of this sketch mutated `desired` *after* releasing the lock. That is
wrong, and subtly so: a concurrent subscribe and unsubscribe of the same channel can land
the two `desired` writes in the opposite order to the two map mutations, leaving the
desired set disagreeing with the map in either direction — a channel held with no
subscription upstream, or subscribed upstream with no holders. Everything that must agree
with the map is mutated in the same critical section as the map. Only `markDirty`, which
cannot block, happens after unlock.

**4.2 Subscriptions are state, not events — and `Conn.subs` moves under the hub lock.**

An earlier design pushed `subscribe`/`unsubscribe` onto a bounded `cmds` channel drained by
one goroutine. That single choice produced four separate defects, and they are worth naming
because the shape recurs:

- With Redis merely **slow**, the queue filled. The fan-out goroutine then blocked pushing
  an unsubscribe for a slow-consumer close, and **all delivery on the replica stopped**
  while every socket stayed open and `/ready` stayed 200. Nothing reconnected, nothing
  reconciled, and the runbook had no entry for it.
- `Bus.Subscribe` returned an error nobody consumed, so one transient failure left a
  channel locally subscribed and upstream dead, permanently, with no log line.
- On bus reconnect, the resubscribe sweep raced the live command stream in both directions:
  a channel could end up subscribed with no local holders, or held with no subscription.
- One channel per call meant a 30,000-channel resubscribe was 30,000 serial round trips —
  seconds of blackout at any real scale.

All four are symptoms of modelling state as events. The reconciler:

```go
func (h *Hub) reconcile(ctx context.Context) {
    for range h.wake {
        for {
            if !h.dirty.Swap(false) { break }
            want := h.snapshotDesired()            // one entry per active channel
            if err := h.bus.Sync(ctx, want); err != nil {
                h.dirty.Store(true)                // retry; never lose the intent
                sleepBackoff()
            }
        }
    }
}
```

`markDirty` never blocks, so no producer can stall — including the fan-out goroutine. A
failed `Sync` simply stays dirty. Reconnect is a forced dirty. Batching is inherent,
because `Sync` takes the whole set and Redis `SUBSCRIBE` is variadic. Memory is bounded by
the number of distinct channels rather than by command churn.

`c.subs` is mutated **only under the hub lock**, in the same critical section as
the hub map. §7 rejects a separately-tracked list on drift grounds and then §2 defines
one; the two are reconciled by updating them atomically together. Any path that touches one
without the other — grant narrowing, control `unsubscribe`, slow-consumer close — leaves a
connection resident in the hub map after close, so fan-out writes to a dead connection
forever and the refcount never reaches zero.

**4.3 Never block on `out`.**

```go
select {
case c.out <- frame:
default:
    h.closer <- c   // non-blocking; falls back to `go c.CloseSlow()` if full
}
```

Note that closing happens on a **dedicated closer goroutine**, never inline on the fan-out
path. Closing deregisters from the hub, which needs write locks the fan-out goroutine is
currently holding for read.

The `default` branch is the entire slow-consumer policy (`07-delivery.md` §4). A blocking
send here would stall the fan-out goroutine and therefore the whole channel.

**4.4 Lock order is hub, then conn — never the reverse.**

Two locks exist. Every path that needs both MUST take `Hub.mu` first. `Close` reads
`c.subs` and deregisters from the hub; `Hub.Disconnect` (`internal/hub/control.go`) reads
the user index under the hub lock before closing. Acquire them in opposite orders in two
paths and you have a deadlock that `-race` does not detect and that only appears under
contention. Neither lock is ever held while sending on a channel.

**4.5 Fan-out takes a read lock and copies.**

Delivery iterates a channel's connection set under `RLock`, appending to each `out`. Since
`out` sends never block (4.3), the read lock is held for a bounded, short time. Do not
call `Close` while holding it — `Close` needs the write lock to deregister, so that
deadlocks. Collect the slow connections into a slice, release the lock, then close them.

## 5. Fan-out path

```
bus reader goroutine     — does NOTHING but drain the socket into `intake` (bounded)
dispatch workers (N=4)   — decode, encode once, fan out
   frame := encodeOnce(envelope)        // ONE []byte, shared by every recipient
   hub.RLock()
   for conn in set:
       skip if conn.id == envelope.exclude
       non-blocking send of *frame pointer* to conn.out   (collect if full)
   hub.RUnlock()
   hand collected slow connections to the closer goroutine
```

**The outbound queue holds pointers to one shared immutable buffer**, encoded once per
message. This was never written down and the omission was a factor-of-200 error: at
`outbound_queue: 256`, `max_message_size: 32 KiB` and 20,000 connections, a per-connection
copy is 160 GiB against a 1 GiB budget. Sharing makes it 256 × 16 bytes ≈ 4 KiB per
connection. The buffer MUST NOT be mutated after encoding — it is visible to every
recipient's goroutine at once.

Splitting the reader from the workers is not premature. Redis enforces
`client-output-buffer-limit pubsub` (32 MB hard / 8 MB for 60s soft by default) and
**disconnects a subscriber that falls behind**. A single goroutine that decodes and fans
out to 10,000 connections between socket reads will fall behind during a broadcast burst;
Redis then drops it, the gateway reconnects, resubscribes, and is immediately behind again.
That oscillation is stable, not transient, and it presents to an operator as `bus_reconnects`
in the `/ready` body climbing against a perfectly healthy Redis — pointing on-call at the
wrong system entirely.

The control channel is consumed on its own goroutine, so a revocation cannot queue behind
the firehose it may exist to stop.

## 6. Grant matching

Compiled once at connect, never at match time (FR-9):

```go
type Pattern struct {
    kind   patKind   // literal | prefix | suffix | contains | segments
    a, b   string
    parts  []string  // for multi-star patterns
}
```

`room-4410` compiles to a literal comparison. `org-42-*` compiles to a prefix check.
Multi-star patterns fall back to a segment scan, still without allocation. No
`regexp.Compile`, no `filepath.Match` — the former allocates and the latter supports
metacharacters we deliberately do not (`05-authorization.md` §3).

## 7. Bus reconnection

Losing Redis MUST NOT close client connections (NFR-8). The redis implementation:

- reconnects with backoff between `bus.reconnect_min` and `bus.reconnect_max`
- on reconnect, sets the dirty flag; the reconciler re-`Sync`s the whole desired set in
  batches. There is no sweep to race the live command stream, because there is no command
  stream — the desired set is the only truth and `Sync` is idempotent
- reports not-ready on `/ready` while disconnected, so a load balancer stops sending new
  connections to a replica that cannot deliver
- keeps existing connections open throughout; they simply receive nothing until it returns

Messages published while disconnected are lost. That is at-most-once behaving as
documented, and the client's reconciliation covers it (`07-delivery.md` §2).

## 8. Shutdown

On `SIGTERM`:

1. `Server.StopAccepting()`: new upgrades get 503, `/ready` returns 503, and the listener
   stays open so the load balancer gets an honest answer instead of a refused connection.
2. Send `{"disconnect":{"code":3000,"reconnect":true}}` to every connection.
3. Close sockets, allowing up to `server.drain_timeout`.
4. Stop the bus consumer.
5. Close the hub.
6. Release the bus, exit 0.

Step 2 exists so clients apply their backoff-with-jitter instead of treating an abrupt TCP
reset as a network blip and retrying immediately. Draining deliberately is what turns a
rolling deploy from a stampede into a spread.

## 9. What not to build

- **A worker pool for frame handling.** Handling is cheap and a pool adds latency and a
  queue to reason about.
- **A write mutex on the socket.** The single writer goroutine is the design; a mutex
  means two writers exist and one of them will eventually interleave a partial frame.
- **A global connection registry.** Revocation by user needs an index, but build it as a
  second map keyed by user, maintained under the same hub lock — a separate lock for it
  contention point on every connect and disconnect.
- **Caching parsed channel namespaces in a global map.** It grows without bound with
  attacker-chosen keys. Parse per use; it is a single `IndexByte`.
