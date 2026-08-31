# Integration Suite

The layers of `docs/11-testing.md` §4 and §5: two gateway replicas, one real Redis, real
websocket clients over real sockets. Everything below this directory has been unit tested
against fakes. Nothing above it had ever been run against a Redis with two replicas, and
that gap is what this suite closes.

A claim is not a fact. "Fan-out works across replicas" is evidenced by a test that puts
one client on each of two gateways sharing one Redis and publishes once — not by a reading
of the code.

## Running It

| Command | What It Does |
|---|---|
| `make integration` | Starts Redis, runs the suite with `-race`, tears Redis down. The usual way. |
| `make redis && go test -race ./test/integration/...` | The same, with Redis left running between runs. Faster to iterate against. |
| `go test ./...` | Runs everything. This suite **skips** with an actionable message when Redis is unreachable. |
| `ST_TEST_REDIS_URL=redis://host:6380/0 go test -race ./test/integration/...` | Runs against a Redis somewhere else. CI sets this. |
| `go test -race -count=3 ./test/integration/...` | What to run before believing a change. A flaky integration test is worse than none. |

`make redis` starts `redis:8-alpine` on **port 6380** — not 6379, because a developer's
laptop usually already has a Redis on the default port holding something they care about
— with the setting `docs/10-operations.md` §3 requires:

```
--client-output-buffer-limit "pubsub 256mb 64mb 60"
```

The default endpoint is `redis://127.0.0.1:6380/0`. `ST_TEST_REDIS_URL` overrides it.

### Requirements

| Requirement | Needed By | Absent |
|---|---|---|
| A reachable Redis | Every test | The whole suite skips, naming the endpoint it tried and how to start one |
| A usable Docker daemon | The two bus-outage tests only | Those two skip; everything else runs |

The bus-outage tests start a throwaway Redis **of their own** and stop it mid-test. They
cannot use the shared one: every other test in the run is on it, and killing it would fail
them all with a symptom that has nothing to do with what they assert.

### Isolation

Every test gets a distinct `bus.prefix` — its own name plus four random bytes — and a
distinct Redis database index. Tests therefore run in parallel and cannot see each other's
traffic.

The prefix is the isolation that works. **Redis pub/sub is not scoped to a database**: a
`SUBSCRIBE` on db 3 receives a `PUBLISH` made on db 9. The database index is a second,
cheaper fence that keeps any future key-space use from colliding; it does nothing for
pub/sub, and nothing here relies on it doing so.

## What The Suite Is

Two `server.Server` instances, each with its own hub, its own Redis subscriber connection
and its own `httptest` listener, sharing one Redis. That is a genuine two-replica
topology — nothing is shared but the bus — and building it in-process rather than as child
processes keeps a failing test debuggable: a stack trace names the goroutine that lost the
message rather than a PID that has already exited.

| Component | What It Is Here |
|---|---|
| Gateway replicas | Real `server.Server`, `hub.Hub` and `bus.RedisBus`, constructed the way `main` will construct them |
| Connect webhook | Real `webhook.Client` against a stub application on `httptest`, which verifies the gateway's HMAC exactly as `docs/04-integration.md` §1.4 specifies |
| Clients | Real `gorilla/websocket` over real TCP |
| Publisher | A third `go-redis` client that is neither replica, publishing straight to Redis — which is how an application publishes, and the only way to send a control message neither gateway sent |
| Bus consumer | Real `consumer.Consumer` — the same package `main` constructs |

### The Bus Consumer

The loop that drains `bus.Receive()` into `hub.Dispatch`, routes the control channel to
`hub.Control` on a goroutine of its own, and verifies the FR-23 control envelope is
`internal/consumer`. This suite constructs it exactly as `cmd/sidecartunnel` does, so the
routing rule and the signature check under test are the ones that ship.

They were not always. Both lived in `cmd/sidecartunnel`, in `package main`, which no test
outside that directory can import, and this suite carried its own equivalent in
`harness_test.go`. Two implementations of one rule drift, and the copy under test was not
the copy that shipped. `harness_test.go` now holds only the lifecycle — start the workers,
stop them, wait — and the publisher-side signer, which belongs to the suite because the
suite plays the application.

### No Sleeping

Nothing in this package calls `time.Sleep`. Waits are on observable state — a bus
subscription Redis has confirmed, a connection that has finished unwinding, a container
answering `PING` — through `waitFor`, which yields rather than sleeps and fails the test
with a sentence rather than hanging. The budgets are generous failure detectors: the happy
path leaves them in microseconds and they only expire when the test was going to fail
anyway.

Where a test must prove that something did **not** arrive, it does so positively: it
publishes a second message and requires that one to be first. A test that waited for
silence would prove only that the test is patient.

## What Each Test Proves

| Test | Proves | How It Would Fail |
|---|---|---|
| `TestCrossReplicaFanOut` | **FR-12.** One publish reaches subscribers on both replicas | The bus subscription is never established, or the hub keys by something other than the bus key |
| `TestUnprefixedPublishReachesNobody` | **FR-21.** A publish to the bare channel name reaches nobody | Somebody keys the channel map by the name the client used, which is what made two applications cross-deliver |
| `TestExcludeSuppressesExactlyOneClient` | **FR-13.** `exclude` withholds from exactly that client id, on every replica | `exclude` compared against the wrong identity, applied on one replica only, or an empty `exclude` matching a connection |
| `TestUpstreamRefcounting` | **FR-10.** Two clients on one channel produce one upstream subscription; disconnect returns the count to its prior value | The 0→1 transition computed outside the lock that inserts, or `Remove` dropping the map entry without the desired set |
| `TestUnsubscribeReturnsUpstreamCount` | **FR-10**, by the explicit unsubscribe path rather than the disconnect path | A defect in one path is invisible from the other; they move the same state by different routes |
| `TestReplicasSubscribeIndependently` | **FR-10** is per replica: one releasing a channel does not stop the other receiving | A shared counter — invisible with a memory bus, which has only one replica |
| `TestControlDisconnectReachesAnotherReplica` | **FR-18.** A signed `disconnect` published by neither replica closes the target within one second, `reconnect: false` | Late revocation, wrong close code, or a targeting rule that matches loosely |
| `TestControlUnsubscribeEmitsPush` | **FR-17.** The `unsubscribed` push arrives, `sync` agrees, and the refcount moved | A silent drop, which leaves the client claiming a channel it will never hear from again |
| `TestUnsignedControlMessageHasNoEffect` | **FR-23.** A forged control envelope is dropped and counted | Anything that can reach Redis could otherwise disconnect every user on every replica |
| `TestSubscribeOutsideGrantsIsRefused` | **FR-5, FR-8.** Error 103 across a real socket, connection left open; `_control` refused even against a grant of `*` | Grants consulted after the subscription is taken, or the reserved check running after the glob |
| `TestForeignOriginIsRefusedBeforeTheApplicationIsCalled` | **FR-2.** 403 at the handshake, and the stub application records **no call**; plus **FR-3** cookie forwarding and **FR-24** `X-St-Forwarded-For` on the allowed path | A gateway that called the application first would hand `evil.example` a valid grant list for the victim's session before refusing it |
| `TestWebhookStatusesMapToCloseCodes` | **FR-6.** 401 → 3003 `reconnect: false`, not retried; 403 → 3008 `reconnect: true`, not retried; 5xx → 3008 `reconnect: true`, retried; 2xx with an unusable body → 3003 | Collapsing refusal and failure locks every user out during a deploy, or turns a revocation into an infinite retry loop |
| `TestDrainClosesOneReplicaOnly` | **FR-19.** 3000 with `reconnect: true` and a spread `retry_after`, and the other replica untouched | A drain that reached across the bus, or one that returned before its connections were gone |
| `TestDrainingReplicaRefusesNewConnections` | **FR-19**, first step: 503 before the upgrade | Accepting a connection the replica is about to close |
| `TestSlowConsumerIsClosedAndTheChannelKeepsWorking` | **FR-15.** A client that stops reading is closed 3005 — **and every other subscriber received every message throughout** | Without the second assertion the test passes while the bug it exists to catch is present |
| `TestBusLossKeepsConnectionsOpenAndRestoresDelivery` | **NFR-8.** Redis is stopped: connections stay open, `/ready` turns 503, `/health` does not. Redis returns: subscriptions restore themselves and delivery resumes **on the same sockets** | A gateway that closed connections on bus loss turns an eight-second restart into every user re-authorizing at once — an outage of the application, not the gateway |
| `TestReconcilerConvergesAcrossAnOutage` | **S2, M5, M6.** Subscriptions churn during an outage; afterwards the upstream set is exactly the desired one | The old event-queue design could not pass this: a failed subscribe was lost, and a reconnect sweep raced the live command stream |
| `TestBroadcastBurstDoesNotEvictTheGateway` | **M8.** A 16 MiB burst on a wide channel does not get the gateway evicted by Redis's pubsub output buffer — and if it does, it recovers completely | The failure is stable, not transient: evicted, reconnect, resubscribe, immediately behind again, reading to an operator as an unstable Redis |
| `TestPushNeverPrecedesItsSubscribeReply` | **§5.1.** No push for a channel arrives before that channel's subscribe reply, under concurrent publishes | The reply queued outside the hub lock that inserted the subscription |
| `TestPushNeverFollowsItsUnsubscribeReply` | **§5.1**, the other direction | The subscription dropped and the reply queued in two separate critical sections |
| `TestConnectDisconnectCyclesLeakNoGoroutines` | **NFR-3.** 300 full lifecycles return the goroutine count to its baseline | The leak class that only shows up after a week of uptime, and that a fake socket cannot create |

Two tests are **skipped**, each documenting a defect found by writing it. Both carry the
diagnosis in their doc comment and should be un-skipped by whoever fixes the cause:

| Skipped Test | Defect |
|---|---|
| `TestControlDisconnectByUserSpansReplicas` | The hub files a connection under user `""` at `hub.Add` time and never re-files it when the webhook supplies the real id. User-targeted control messages match nothing on any replica, and `Hub.Remove` leaks one map entry per connection |
| `TestConnectWebhookReceivesTheRequestedChannels` | The connect webhook request is built before the connect frame is read, so `channels_requested` is always empty |

## Known Behaviour Worth Knowing About

**Per-channel ordering is not preserved.** `bus.dispatch_workers` defaults to 2, so two
messages on one channel are decoded and fanned out concurrently and can reach a
connection's outbound queue in either order. It is measurable rather than theoretical: at
burst rate, the last message before a marker arrives after that marker in most runs.
`docs/07-delivery.md` §5 declines to guarantee ordering, which is correct, but its claim
that per-channel order "is preserved in practice" is not true with more than one dispatch
worker. Tests must not depend on the order of two publishes; the ones that need a "did not
arrive" assertion synchronise on the replica's own dispatch count instead.

**`bus.Health().Subscriptions` is stale while the transport is down.** It is reset when a
new connection is adopted, not when the old one is lost, so after a Redis restart it reads
as whatever it was before the outage until the reconnect completes. `waitUpstream` requires
`Connected` as well as the count for exactly this reason — a wait on the count alone is
satisfied by the stale value, and the test then publishes into a gateway that has not
resubscribed.

## Debugging A Failure

Work down this list. It is ordered by how often each thing is the answer.

**1. Read the failure message.** Every assertion in this suite names the requirement it is
about and what it expected. A message ending in `(FR-13)` is telling you which document
section to open.

**2. Check whether it is the environment.** A whole-suite failure with `no Redis at …` is
a missing container, not a regression. `docker ps` and `make redis`.

**3. Run the one test alone.**

```
go test -race -count=1 -run TestCrossReplicaFanOut -v ./test/integration/...
```

If it passes alone and fails in the suite, the cause is load or cross-test interference.
Interference should be impossible — every test has its own prefix — so suspect timing, and
raise `waitBudget` in `harness_test.go` temporarily to find out whether it is slowness or
an actual absence.

**4. Turn the gateway's logs on.** Replicas log to `io.Discard` at `warn` by default,
because a passing suite that prints a thousand lines is a suite nobody reads. In
`newReplica`, replace the handler:

```go
log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
```

Every connection logs its client id, which is the join key across the whole run and is what
the failure messages print.

**5. Ask Redis what it thinks.**

```
docker exec sidecartunnel-redis redis-cli pubsub channels 'it:*'
docker exec sidecartunnel-redis redis-cli info clients
```

The first shows which channels are subscribed right now, across every test in flight — the
prefix in the failing test's name tells you which rows are its own. A channel a test
believes it holds and Redis does not is a reconciler problem; the reverse is a refcount
problem.

**6. Consult the counters before the wire.** A replica exposes what the bus knows:

| Value | Reads As |
|---|---|
| `r.bus.Health().Subscriptions` | Channels confirmed subscribed upstream. Always includes `_control` |
| `r.bus.Health().Reconnects` | `st_bus_reconnects_total`. Climbing against a healthy Redis is the M8 signature |
| `r.bus.Health().Dropped` | Messages discarded because the intake was full |
| `r.cons.Stats().Dispatched` | Messages the hub accepted for fan-out. Separates "Redis never sent it" from "the gateway never delivered it" |
| `r.cons.Stats().ControlRejected` | Control envelopes dropped for a bad signature, a stale timestamp, or a malformed body. `ControlUnsigned`, `ControlStale` and `ControlMalformed` separate the three |
| `r.srv.Stats()` | Accepted, refused, unavailable, origin-rejected, and connections held right now |
| `r.web.Stats()` | Webhook outcomes, with refusals, rejections and failures counted apart |

**7. A hung test is a missing frame.** Every read has a deadline, so the suite fails rather
than hangs. A `read: i/o timeout` means the frame never arrived — start at step 5 and ask
whether the subscription that would have delivered it exists.

**8. A leaked container.** The bus-outage tests remove their container through `t.Cleanup`,
including on failure. A crashed run can still leave one:

```
docker ps -a --filter name=st-it- --format '{{.Names}}' | xargs -r docker rm -f
```

## Adding A Test

- One requirement per test, named in the doc comment, with a sentence on how it would fail.
  A test whose comment cannot name what it protects is a test nobody will dare delete and
  nobody will trust.
- Prove an absence positively. Publish the thing that must not arrive, then the thing that
  must, and assert on the order of receipt — or synchronise on `r.cons.Stats().Dispatched` when the
  two would race.
- Never depend on the order of two publishes on one channel. See Known Behaviour.
- `t.Parallel()`, always. `newCluster` gives every test its own prefix and database.
- No `time.Sleep`. Wait on state through `waitFor`, or on a frame through a read deadline.
