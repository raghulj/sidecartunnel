# 13 — Adversarial review, 2026-08-31

An adversarial review of the M0 specification found 8 critical, 22 major and 14 minor
findings. This document records every one and what I did about it. It is a log, not a
plan: where a finding changed the design, the normative document has already been changed
and this entry says how.

Dispositions: **FIXED** (design changed), **CUT** (feature removed rather than fixed),
**ACCEPTED** (real, documented, not changing), **DISPUTED** (I think the finding is wrong).

Five architectural changes account for most of the criticals. They are listed first
because reading them makes the individual entries make sense.

---

## The five structural changes

### S1. Multi-app support is cut

**Kills C1, C4-B, M12, M13.** One gateway process now serves exactly one application.

The critic showed that multi-app was not merely unfinished but actively unsound: the hub
map was keyed by bare channel name, so two apps sharing a channel name cross-delivered;
`namespaces`, `limits` and `allowed_origins` were global while `app` was a list; nothing
specified how an inbound socket picks an app; and the refcount `first := len(set) == 1`
was computed over the merged set, so the second app's bus subscription was never issued.

Fixing all that costs an app-routing rule, a composite hub key, per-app limits, per-app
namespaces, and a per-app cache key. Cutting it costs nothing: the binary is 12 MB and
static, so a second application is a second container.

`12-roadmap.md` §3.5 already asked whether multi-app earned its keep. It did not.

Door left open: the hub is now keyed by the **bus key** (`{bus.prefix}{channel}`), not the
bare channel. That is already unique per prefix, so restoring multi-app later is a routing
rule and a per-app prefix, not a data-structure rewrite.

### S2. The bus command queue becomes a desired-state reconciler

**Kills C7, M5, M6, M7.** This is the most valuable thing the review produced.

The old design pushed `subscribe`/`unsubscribe` commands onto a bounded channel drained by
one goroutine. Four separate defects followed from that single choice:

- When Redis was *slow* rather than down, the queue filled; the fan-out goroutine blocked
  pushing an unsubscribe for a slow-consumer close, and **all delivery on the replica
  stopped** while every socket stayed open and `/ready` stayed 200 (C7).
- `Bus.Subscribe` returned an error nobody consumed, so a transient failure left a channel
  locally subscribed and upstream dead, forever, with no metric (M5).
- Bus reconnect swept the hub map while commands were still flowing, so a subscribe and an
  unsubscribe for the same channel could land in either order (M6).
- One channel per call meant a 30,000-channel resubscribe was 30,000 serial round trips
  (M7).

All four are symptoms of modelling subscriptions as *events*. They are not events; they
are **state**. The replacement:

```go
type Bus interface {
    // Sync makes the bus subscription set exactly `desired`. Idempotent.
    Sync(ctx context.Context, desired []string) error
    Publish(ctx context.Context, channel string, payload []byte) error
    Receive() <-chan Message
}
```

The hub owns a `desired` set and a dirty flag. A reconciler goroutine wakes on dirty,
snapshots the desired set, and calls `Sync`, which batches (Redis `SUBSCRIBE` is variadic).
Marking dirty is a non-blocking atomic store, so **no producer ever blocks** — killing C7.
A failed `Sync` leaves the set dirty and is retried with backoff — killing M5. Reconnect is
just a forced dirty — killing M6. Batching is inherent — killing M7.

### S3. Grants expire by re-handshake; the gateway no longer retains cookies

**Kills C3, shrinks C5 and the §8 accepted risk.**

The old design cached the cookie at handshake and replayed it at revalidation. The critic
found that any app rotating its session — Django `SESSION_SAVE_EVERY_REQUEST`, Rails,
`cycle_key()` on privilege change — makes that cached cookie stale, so revalidation returns
401 and the gateway closed with `reconnect: false`, permanently ejecting a user who is
sitting in the app fully logged in. That is default behaviour for several mainstream
session backends.

Now: at expiry the gateway closes with **3503, `reconnect: true`, carrying a `retry_after`**.
The browser reconnects and sends its *current* cookie. No cookie is retained anywhere, the
refresh path disappears, and a memory dump of the process no longer yields a set of live
session cookies.

The cost is a reconnect per `expires_in` per connection. Two things pay for it: `expires_in`
now defaults to **6h** rather than 1h, and revocation no longer depends on expiry at all —
the control channel is immediate. Long expiry plus immediate revocation is strictly better
than short expiry as a revocation mechanism, which is what the old design was quietly using
it as.

### S4. `auth_required: false` is cut

**Kills C6.** `01-requirements.md` FR-5 said a grant is required for every subscribe;
`06-channels.md` §3 said a namespace could turn that off. Both were citable, and the
normative config document defined only the key's type, so the precedence rule did not
resolve it.

I removed the key. Every channel requires a matching grant, with no exception. A genuinely
public broadcast is expressed by the application putting `status` (or whatever) in every
grant list — one extra string, no new concept, no way to accidentally disable
authorization for a namespace.

### S5. Disconnects carry `retry_after`, and the client obeys it

**Kills M1, most of C2, helps C8 and M20.**

The old design leaned on client-side jitter to spread reconnects, and the critic did the
arithmetic: the reference formula `min(30s, 2^n) × rand(0.5,1.5)` gives **0.5–1.5 s at
n=0**, which is exactly the one-second window `10-operations.md` §4 models as an outage.
Backoff only widens after the application has already fallen over.

Server-directed backoff fixes this properly. Every retryable `disconnect` now carries
`retry_after` in milliseconds, and the gateway spreads it: on drain, uniformly across
`server.drain_spread` (default 60s — raised from 30s when the target scale was set; see
the addendum below). The client MUST honour it, and falls back to full
jitter — `rand(0, min(30s, 2^n))`, not the multiplicative form — when it is absent.

The gateway knows how many connections it is dropping. The client does not. Putting the
spread where the information is was the obvious fix and I missed it.

---

## CRITICAL

### C1 — Multi-app isolation unimplementable; hub cross-delivers between apps
**FIXED by S1.** Also: hub key is now the bus key, so even a future multi-app cannot
collide. `01-requirements.md` FR-21 removed; `08-config.md` `app` is now a single block,
`bus.prefix` is a single key.

### C2 — Webhook queue + handshake timeout = permanent mass lockout
**FIXED.** Three changes:
- `server.handshake_timeout` now covers **only receipt of the `connect` frame** — the part
  the client controls. Stated explicitly in `03-client-protocol.md` §2.
- Authorization has its own budget, `app.connect_timeout` (default 10s), covering queue
  wait plus the webhook call. Exceeding it closes with **3008, `reconnect: true`, with
  `retry_after`** — never 3001, which stays `reconnect: false` and now applies only to a
  client that genuinely never sent `connect`.
- The queue is bounded by `app.connect_queue` (default 4096) and overflow closes retryable
  rather than queueing without limit. The critic correctly noted that the unbounded reading
  was also a defect: 25,000 half-open sockets holding captured cookies.

### C3 — Cookie rotation ejects logged-in users permanently
**FIXED by S3.**

### C4 — `cache_ttl` defeats revocation; leaks across apps; does not help tabs
**FIXED.** Scenario B dies with S1. For A and C:
- `app.cache_ttl` now defaults to **0 (off)**. It is an optimization with a stated
  revocation-latency cost, not a default.
- When enabled, any control-channel `disconnect` **flushes the entire cache**. Coarse and
  correct; the cache is small and revocations are rare.
- New `app.cookie_names` names which cookies form the cache key. Hashing the whole `Cookie`
  header was the reason it did not deduplicate tabs — `_ga`, `_fbp` and CSRF tokens differ
  per tab. The critic was right that the claimed benefit largely did not exist.

### C5 — HMAC does not sign the Cookie; no replay defense
**FIXED, partially ACCEPTED.** The signed input is now:

```
HMAC-SHA256(secret, timestamp + "." + nonce + "." + sha256(cookie_header) + "." + sha256(body))
```

Binding the cookie digest kills the swap-the-cookie oracle, which was the real finding.

On replay I accept and restate rather than fix: the ±300s window **is** a replay window.
The endpoint is idempotent and read-only, so a replay returns the same answer to whoever
already had the bytes. `05-authorization.md` §8's row now says "limits replay to a 300s
window" instead of claiming prevention, and a `nonce` is emitted so an application that
wants exactly-once can cache seen nonces. The old row was false as written.

### C6 — FR-5 vs `auth_required: false`
**FIXED by S4.**

### C7 — Slow Redis stalls all fan-out on the replica
**FIXED by S2.** Additionally: closing a connection may never happen on the dispatch
goroutine. Slow-consumer closes are handed to a dedicated closer goroutine over a channel
whose send is non-blocking, falling back to `go conn.Close()` on overflow.

### C8 — Control channel unauthenticated; `refresh` amplifies into the application
**FIXED.**
- Control envelopes are now **signed** with `control_secret` and carry a timestamp;
  unsigned or stale messages are dropped and counted. The asymmetry the critic identified
  — HMAC on the read-only webhook, nothing on the operation that can disconnect every user
  — was indefensible.
- `refresh` and `disconnect` MUST name exactly one of `user` or `client`, matched
  **exactly, never as a glob**. An omitted target is a validation error, not "all".
- Forced revalidation is spread over `control.refresh_spread` (default 60s) with
  `retry_after`, so a legitimate mass refresh cannot stampede the application either.

---

## MAJOR

### M1 — Reference backoff's first retry lands in the outage window
**FIXED by S5.** Formula changed to full jitter, `retry_after` takes precedence.

### M2 — FR-9 forbids the lock that §2 requires; torn grant slice
**FIXED.** `Conn.grants` is now an `atomic.Pointer[grantSet]` holding an **immutable** set.
Revalidation (now: re-handshake, S3) and control `unsubscribe` swap the pointer rather than
mutating. Matching is a lock-free atomic load. FR-9's wording changed from "no lock
acquisition" to "no mutex acquisition; a lock-free atomic load is permitted".

### M3 — No lock-ordering rule among the "four concurrency rules"
**FIXED.** Added as rule 4.5: **`shard.mu` is always acquired before `Conn.mu`, never the
reverse, and neither is ever held while sending on a channel.** The admin `/channels`
handler, which the critic identified as the likely inversion, is explicitly bound by it.

### M4 — `Conn.subs` and the shard map are the two drifting lists §7 warns against
**FIXED.** The duplicate is necessary — `Close` must know where to deregister — so the rule
is now that **`Conn.subs` is mutated only under the corresponding shard's lock**, making
the two updates atomic together. Every mutation path (subscribe, unsubscribe, FR-17
narrowing, control unsubscribe, slow-consumer close) is named as bound by it.

### M5, M6, M7 — bus errors unhandled; reconnect race; no batching
**FIXED by S2.**

### M8 — Redis `client-output-buffer-limit pubsub` disconnect oscillation
**FIXED.** Real, and I had not thought about it. Three changes:
- The bus reader goroutine now does nothing but drain the socket into a bounded intake
  channel. Decode and fan-out happen on a small worker pool (`bus.dispatch_workers`,
  default 4). The socket is drained fast enough that Redis's buffer does not fill.
- `10-operations.md` gains a required Redis setting: raise
  `client-output-buffer-limit pubsub`, with the default's inadequacy explained.
- New metric `st_bus_intake_depth` and a runbook entry for "reconnects climbing with a
  healthy Redis", which previously pointed on-call at the wrong system.

Also: the control channel no longer shares the fan-out path, so a revocation cannot queue
behind a firehose.

### M9 — `only_users` is unbounded O(n×m) under a read lock, and is undocumented authorization
**CUT.** The field is removed. It was a delivery-time authorization filter in a design
whose §4 states delivery-time authorization does not exist, it had no requirement, no
validation spec, no test, and no bound, and its effect is available by publishing to
per-user channels instead. Cutting beats bounding.

### M10 — Memory arithmetic is 200× over the stated budget
**FIXED.** Two errors, both mine:
- The queue holds a **pointer to one shared immutable `[]byte`**, encoded once per message
  and shared by every recipient. The real cost is 256 × 16 bytes ≈ 4 KiB per connection,
  not 256 × 32 KiB. This was never written down; now it is, in `09-internals.md` §5, along
  with the rule that the buffer is never mutated after encoding.
- The per-connection estimate was optimistic by ~3×. `10-operations.md` §8 now says 30–40 KB
  with explicitly tuned socket buffers, and `08-config.md` gains `limits.read_buffer` /
  `limits.write_buffer` (default 2 KiB) because leaving them at a library default was the
  difference between fitting the budget and not. NFR-1 is restated as **50,000 connections
  in 4 GiB**, flagged as unverified until measured.

### M11 — The env-only config example produces a gateway where every subscribe fails
**FIXED.** When `namespaces` is empty, a built-in `default` namespace applies to all
channels with `auth_required` semantics. The env-only example now works. `08-config.md` §1
also documents the `__N__` indexed form and states plainly that per-namespace configuration
requires YAML or `ST_NAMESPACES_JSON`, replacing the false claim that everything is
configurable by environment alone.

### M12 — `bus.prefix` vs `app.bus_prefix`; control channel scope undefined
**FIXED by S1.** One `bus.prefix`. Control channel is `{bus.prefix}_control`. Target
matching is specified as exact.

### M13 — Prefix uniqueness does not prevent prefix collision
**MOOT** with S1. Recorded in `12-roadmap.md` so that a future multi-app implements "no
prefix may be a prefix of another", which is the correct rule.

### M14 — Close code 3000 means two incompatible things; 3002 and 3500 unreachable
**FIXED.** Added **3008 `auth_unavailable`** for webhook 5xx/timeout, retryable with
`retry_after`. 3000 now means drain only. Removed 3002 and 3500 from the registry — the
critic is right that neither can be sent, since the Origin check completes before the
upgrade and a hard kill sends no frame.

### M15 — Pushes may arrive before a subscribe ack or after an unsubscribe ack
**FIXED.** New normative rule in `03-client-protocol.md` §5.1: the gateway MUST NOT send a
push for a channel before that channel's subscribe reply, nor after its unsubscribe reply.
This is free to implement given the single writer goroutine — the reply is queued to `out`
under the same shard lock that mutates the subscription, so queue order guarantees it.

### M16 — No way to learn the authoritative subscription set; control unsubscribe is silent
**FIXED.** Control-channel `unsubscribe` now MUST send the `unsubscribed` push, same as the
grant-narrowing path. Client obligations gain: on `unsubscribed`, remove from the registry.
Added a `sync` command returning the authoritative subscription set, which also gives
integrators something to call when debugging a silent channel.

### M17 — `max_subscriptions_per_conn` has no error code
**FIXED.** Added error **108 `subscription limit`**. Default raised 100 → 500. The
collision with `12-roadmap.md` §3.1 is now noted in that section: the subscription cap and
the unbounded-grant-set problem are the same problem, and I had only written down one.

### M18 — `X-Forwarded-For` handling with empty `trusted_proxies` unspecified
**FIXED.** Specified: when the peer is not in `trusted_proxies`, `X-St-Forwarded-For` is
the **socket peer address only**, never a client-supplied header. The pass-through reading
would have let an attacker send `X-Forwarded-For: 127.0.0.1` and hit an application's
internal-IP trust path — an auth bypass in the app, delivered by the gateway, under a
header prefix implying the gateway vouched for it.

### M19 — Client `publish` under-specified in three ways
**FIXED**, and it stays in M4. The spec now defines: `event` is required and
client-supplied; publishing requires **both** a matching grant and `client_events: true` on
the namespace (the normative document previously said only the latter, which would have let
a client inject events into a channel it cannot read); the bus envelope gains an optional
`from` field so client events survive the replica hop; the publisher is excluded by default.

### M20 — `/ready` on a liveness probe; undocumented `healthcheck` subcommand and `_FILE` env
**FIXED.**
- `/health` is liveness and returns 200 while the process runs — it never consults the bus.
  `/ready` is readiness only, and `04-integration.md` §4 now says in bold not to wire
  `/ready` to a liveness probe.
- `/ready` gains `bus.ready_grace` (default 30s): a short bus blip does not pull the whole
  fleet from the load balancer at once. An 8-second Redis restart is now invisible.
- The `healthcheck` subcommand and the `*_FILE` environment convention are documented in
  `08-config.md`, which claimed to be the complete surface and was not.

### M21 — Roadmap plans history one screen after listing it as not planned
**FIXED.** Bounded history is removed from M4 and appears only as an open question, which
is consistent with `07-delivery.md` §2's argument that reconciliation beats any buffer.

### M22 — The case against Centrifugo rests on a false claim
**ACCEPTED — the finding is correct.** I verified it against Centrifugo's proxy
documentation rather than taking the critic's word. Centrifugo's connect proxy forwards
configured headers including `Cookie`, the backend returns `user` and a `subs` map, and the
docs state directly: *"you don't need to generate JWT and pass it to a client-side and can
rely on a cookie."* Its subscribe proxy is the per-channel callback that
`12-roadmap.md` §3.1 defers as unbuilt.

So the "JWT-first" characterisation in `README.md` describes one of two modes, not the
product, and the single novel idea here already exists upstream. I have rewritten the
README's comparison to say that plainly. What remains as an honest reason to build is
preference — a surface small enough to read in an afternoon, and owning the code — not a
capability gap. That is a decision for whoever is paying for the time, and the README now
presents it as one.

---

## MINOR

| # | Finding | Disposition |
|---|---|---|
| m1 | `refresh` push has no `channel`, contradicting §5.1's own rule | **CUT.** The frame is gone with S3; expiry is now a retryable close. |
| m2 | Error 107 unreachable — oversize frames close with 3006 | **FIXED.** 107 removed from the registry. |
| m3 | §8 forbids the metric assertions FR-2/10/14 mandate; `st_origin_rejected_total` not in the metric table | **FIXED.** §8 narrowed to "do not assert exact counts except where a requirement names one". Metric added to the table. |
| m4 | `delay: 20s` is not "above" `drain_timeout: 20s` | **FIXED.** Example now 30s. |
| m5 | A channel named `default-1` and a separator-less channel both resolve to `default` | **FIXED.** Separator-less channels resolve to the reserved name `""`, which the built-in default block owns. A namespace may not be named `default`. |
| m6 | `exclude` needs the browser's client id on every HTTP write; no convention given | **FIXED.** `X-St-Client` header convention documented, plus a checklist item. Multiple tabs have distinct ids, which is the point. |
| m7 | Flask sample parses the timestamp before verifying the signature → unauthenticated 500 | **FIXED.** Reordered; malformed input now 403s. This is the code integrators paste, so it mattered. |
| m8 | No per-user or per-IP connection limit | **FIXED.** Added `limits.max_connections_per_user` (default 20). "Limits at the proxy" was hand-waving at a component the gateway does not configure. |
| m9 | `permessage-deflate` never mentioned; ~256 KB/conn if a library enables it | **FIXED.** Explicitly disabled, with the arithmetic in `08-config.md`. |
| m10 | `webhook_secret` has no rotation path | **FIXED.** Now a list; the gateway signs with the first and the app accepts any. |
| m11 | Unspecified whether the client sees clamped or raw `expires_in` | **FIXED.** Clamped. |
| m12 | Application-level `ping` carries no `id`, so RTT cannot be measured | **FIXED.** `id` permitted and echoed. |
| m13 | "Repeated violation" of the rate limit undefined; 3007 is retryable so abuse amplifies | **FIXED.** Defined as 10 violations in 60s; 3007 now carries a `retry_after` of 60s. |
| m14 | `admin.listen` defaults to all interfaces and the Dockerfile `EXPOSE`s it | **FIXED.** Default `127.0.0.1:9001`; `EXPOSE 9001` removed. |

---

## What the review confirmed as sound

Recorded so nobody churns it: the Origin check and its reasoning; refusing to start on an
empty `allowed_origins`; at-most-once plus client reconciliation; close-on-slow-consumer
and the non-blocking send; the single writer goroutine; the glob restricted to `*` with the
`user-*` trap documented as a trap; per-channel refcounted `SUBSCRIBE` rather than
`PSUBSCRIBE`; the 401-vs-5xx distinction as a concept; `09-internals.md` §9; the
no-domain-nouns discipline across all fifteen files; and naming Redis as a trust boundary
and the reconnect storm as this architecture's own cost.

## Verification

Every CRITICAL and MAJOR finding is fixed **in the specification documents**, not only
recorded here. Checked by grepping each fix's distinguishing artifact — a config key, a
close code, a normative sentence — against the file it belongs in, with a sweep confirming
no normative document still references a cut feature (`only_users`, `auth_required: false`,
`refresh_at`, `app.bus_prefix`, `command_queue`, `3502`, `X-St-Reason`, FR-21's old form).

Four fixes had been written up here but never applied to the specs on the first pass, and
were caught by that check rather than by re-reading:

| | Gap | Now |
|---|---|---|
| M10 | `01-requirements.md` NFR-1 still said 2 GiB while `10-operations.md` said 4 GiB | Restated as 4 GiB, flagged as a derivation rather than a measurement |
| M18 | The `X-Forwarded-For` rule existed nowhere in a normative document | **FR-24**, plus config and proxy-setup notes |
| C2 | FR-4's acceptance criterion still conflated the frame timeout with the authorization timeout — the exact conflation that causes the lockout | Split, with both paths asserted |
| S3 | FR-17 and NFR-4 still described revalidation, which no longer exists | Rewritten around control-channel withdrawal and the bounded connect queue |

FR-9's acceptance criterion was also strengthened: "code review confirms it" does not catch
a torn slice header, which was the actual defect M2 described. It now requires a `-race`
test that swaps the grant set under a continuous matcher.

The MINOR findings are dispositioned in the table above; those marked FIXED are applied,
and none of them gate M1.

## Addendum, same day — target scale set

The review was answered against an assumed 50,000 connections. The actual target is
**20,000 concurrent across two replicas**, and Centrifugo was judged too large for the job.
Right-sizing removed one thing the bigger number had forced:

**The 32-shard hub is now a single `RWMutex` (NFR-9).** Fanning out to 10,000 connections
is ~10,000 map iterations, roughly 0.2 ms under the read lock, against subscribes that are
rare by comparison. Sharding bought nothing measurable and cost a shard-index concept at
every call site. NFR-9 gates it behind a profile showing >5% contention, because the
temptation to build it early is the whole reason it was in the first draft.

What did **not** get cut, and why — all of these are correctness or security, not scale:
the Origin check, grants and glob matching, the cookie-bound webhook HMAC, the signed
control channel, close-on-slow-consumer, the reconciler (simpler than what it replaced),
and at-most-once with client reconciliation.

`retry_after` in particular earns its place *more* at this scale, not less. Losing one
replica returns 10,000 clients; spread over 60s that is ~7 concurrent authorizations
against a 16-worker pool, and in a one-second window it is ~400 — a full application
outage. The gateway is nowhere near its limits at 20,000 connections. **The application's
worker pool is the binding constraint, and it always was.**

## What I got wrong, in one place

The pattern across C1, C2, C7, M5, M6, M7 and M10 is the same: I specified **events where
I should have specified state**, and **wrote prose where I should have done arithmetic**.
The command queue, the cookie cache, the refcount and the reconnect sweep are all
event-shaped solutions to state-shaped problems, and every one of them produced a defect.
The memory budget and the backoff window were both wrong by a factor I would have caught
by multiplying two numbers in the document.
