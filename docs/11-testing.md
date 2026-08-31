# 11 — Testing

What has to be true before a milestone is done. A claim is not a fact: "fan-out works
across replicas" is evidenced by a test running two gateway processes against one Redis,
not by a reading of the code.

## 1. Layers

| Layer | Runs against | Speed | Covers |
|---|---|---|---|
| Unit | Nothing external | ms | Glob matching, frame codec, config validation, shard bookkeeping |
| Protocol conformance | In-process gateway, `bus: memory` | ms | Every frame and code in `03-client-protocol.md` |
| Integration | Real Redis, two gateway processes | seconds | Fan-out, refcounting, control channel, bus reconnection |
| Failure | Real Redis, fault injection | seconds | Slow consumer, webhook 401 vs 5xx, oversize, ping timeout, drain |
| Load | Real Redis, many connections | minutes | NFR-1, NFR-2, NFR-3 |

`bus: memory` makes protocol tests fast and deterministic. It is not a substitute for the
integration layer — the bugs that matter live in the Redis path.

## 2. Unit tests that must exist

**Glob matching.** Table-driven, and the table in `05-authorization.md` §3 is the minimum,
including `user-*` matching `user-7-private` — that row is a documented trap and the test
is what stops someone "fixing" it. Plus: empty grant list, `_`-prefixed channel, patterns
with several stars, and a benchmark asserting zero allocations (FR-9).

**Frame codec.** Round-trip every frame in `03-client-protocol.md` §3. Rejection cases:
zero command keys, two command keys, unknown command, `id` absent where required, `id`
non-positive, malformed JSON, a frame over `max_frame_size`.

**Config validation.** One test per rule in `08-config.md` §3, asserting the error names
the offending key (NFR-5). Empty `allowed_origins` gets its own test — it is the rule most
likely to be relaxed for convenience by someone in a hurry.

**Shard bookkeeping.** Add/remove across shards; refcount 0→1 and 1→0 emit exactly one bus
command each; concurrent add and remove of the same channel from many goroutines under
`-race` never produces a subscribe without a matching unsubscribe.

## 3. Protocol conformance

Given a script of client frames, assert the exact server frames. One case per row of the
error and close code tables, at minimum:

- `connect` sent twice → 101
- command before `connect` → 101
- subscribe granted → `{}`; subscribe ungranted → 103
- subscribe to `_control` with a grant of `*` → 103
- duplicate subscribe → 104; unsubscribe not held → 105
- `publish` on a namespace without `client_events` → 103
- `publish` over the rate limit → 106, then 3007 on repetition
- channel over `max_channel_length` → 101
- unknown namespace with no `default` block → 102
- binary frame → close 3006
- no `connect` within `handshake_timeout` → close 3001
- `connect` with a mix of granted and ungranted `subs` → only granted appear in the reply
- subscribe past `max_subscriptions_per_conn` → 108
- a push is never delivered before its subscribe reply or after its unsubscribe reply
  (`03-client-protocol.md` §5.1) — drive it under `-race` with concurrent publishes
- `sync` returns exactly the gateway's set after a control-channel `unsubscribe`
- unsigned and stale control messages have no effect (FR-23)

## 4. Integration

Two gateway processes, one Redis, real websockets.

- **FR-12.** Client A on replica 1, client B on replica 2, both subscribed; a publish
  reaches both.
- **FR-10.** Subscribe, assert one upstream subscription; disconnect, assert it is gone.
  Two clients on the same replica and channel produce one upstream subscription, not two.
- **FR-13.** `exclude` suppresses delivery to exactly that client id.
- **FR-18.** A control `disconnect` published by a third party closes a connection on a
  replica that did not publish it, within one second.
- **FR-21.** A publish to an unprefixed channel name reaches nobody — the hub keys by bus
  key, not bare channel.
- **FR-22.** A short `max_expiry` closes with 3503 and `retry_after`; the client reconnects
  and re-authorizes successfully with a rotated cookie.
- **NFR-8.** Kill Redis, assert connections stay open and `/ready` returns 503; restart,
  assert subscriptions are restored and delivery resumes without clients reconnecting.

## 5. Failure tests

The ones most likely to be skipped, and the ones that matter in production.

**Slow consumer (FR-15).** A client that completes the handshake then stops reading. Fill
its queue; assert it is closed with 3005, and — the part that matters — assert a second
client on the same channel received every message throughout. Without that second
assertion the test passes while the bug it exists to catch is present.

**Webhook 401 vs 5xx (FR-6).** Both paths, asserting the `reconnect` field differs. Also
that 401 is *not* retried and 5xx *is*, up to `webhook_retries`.

**Webhook concurrency (NFR-4).** Cap at 8, open 200 connections at once, assert the stub
application never observed more than 8 concurrent requests.

**Revalidation narrowing (FR-17).** Short `expires_in`; the stub returns a narrower list
on the second call. Assert the subscription is dropped and an `unsubscribed` push arrives.

**Drain (FR-19).** `SIGTERM` a replica with open connections; assert every client got 3000
with `reconnect: true`, and the process exited within `drain_timeout`.

**Secret hygiene (NFR-7).** Drive a full connect at `debug`, capture all log output, assert
it contains neither the cookie value nor a message payload. This is cheap and it is the
only thing that will catch a debug line added in a hurry.

## 6. Load

Not per-commit. Run before a release and record the numbers in the PR.

- **NFR-1.** 15,000 idle connections on a 2-core, 1 GiB container. Report RSS.
- **NFR-9.** Profile at 10,000 connections under a realistic publish rate; record hub
  lock contention as a percentage of fan-out time. Above 5% justifies sharding; below it,
  sharding must not be built.
- **NFR-2.** 10,000 connections on one channel; publish; report the p50/p95/p99 histogram
  from bus receipt to socket write.
- **NFR-3.** 10,000 connect/disconnect cycles; assert goroutine count returns to within 5%
  of baseline. This catches the leak class that only shows up after a week of uptime.
- **Storm.** 10,000 connections, kill the replica, measure concurrent requests at the stub
  application with `drain_spread` at 60s, at 30s, and disabled. The model in
  `10-operations.md` §4 predicts ~7, ~13 and ~400 respectively; if reality disagrees, the
  document is wrong and moves. This is the single most important load test — it is the only
  one that exercises the failure that takes the *application* down rather than the gateway.

## 7. Gates

Every commit: `go vet`, `staticcheck`, `go test -race ./...`, unit and protocol layers.

Every PR: the integration and failure layers against a real Redis.

Every release: the load layer, with numbers recorded.

`-race` is not optional on any layer. The concurrency rules in `09-internals.md` §4 are
exactly the kind of thing that passes review and fails under the detector.

## 8. What is not worth testing

- Redis itself, or the websocket library. Test the code that uses them.
- Exact wire bytes of a frame beyond one round-trip test per type. Assert on the decoded
  structure; brittle byte assertions make every field addition a test rewrite.
- Exact log line wording, **except where a requirement names it**. FR-2 and FR-10 are
  asserted through the observable behaviour they always described — a 403 response, one
  upstream subscription — never a log scrape. FR-14 is log-valued: the dropped-oversize
  log line, with the channel name, is the acceptance criterion itself, so it is the one
  line in this file worth asserting exactly. Beyond FR-14, assert that a log line fires and
  its level; asserting the rest of its wording couples a test to phrasing and it gets
  deleted the first time that phrasing changes.
