# 14 — Coding standards

**Normative.** This is how code gets written in this repository. A pull request that
disagrees with it is wrong in the same way a pull request that disagrees with
`03-client-protocol.md` is wrong, and it gets fixed rather than argued with.

Most of what follows is ordinary Go discipline. The parts that are not ordinary are here
because I have watched the alternative fail, and each one names the failure rather than
appealing to taste. Where a rule looks expensive, the failure it prevents is the reason,
and the failure is in the paragraph.

## 1. Tests come first, and the diff shows it

Write the test. Watch it fail. Then write the code.

This is not a preference about workflow. It is the only way I know to be sure a test
actually tests something. A test written after the implementation is written against the
implementation's behaviour, including its bugs — it passes the first time it runs, which
is precisely the moment it has told you nothing. I have had entire suites like that:
green, comprehensive-looking, and blind to the defect that took the service down.

So: **every pull request shows its tests first.** Either the test commit precedes the
implementation commit, or the description says which test was failing and what it asserted
before the fix landed. "Tests included" is not the claim; "this test failed, here is the
output, then it passed" is.

The exception is small and specific: a pure refactor that changes no behaviour needs no new
test, and its evidence is that the existing tests pass unchanged. If a refactor needs a
test changed, it is not a refactor.

## 2. What the tests look like

**Table-driven.** A table makes the interesting case obvious, because it sits next to
fifteen boring ones and looks different. It also makes adding a case cheap enough that
people actually do it when a bug turns up. The glob table in `05-authorization.md` §3 is
the canonical example — it is a required test, and it includes the row where `user-*`
matches `user-7-private`, which is a documented trap and not a bug to be helpfully fixed.

```go
tests := []struct {
    name    string
    grant   string
    channel string
    want    bool
}{
    {"literal match", "room-4410", "room-4410", true},
    {"literal is not a prefix", "room-4410", "room-44100", false},
    {"star matches empty", "org-42-*", "org-42-", true},
    // FR-8, docs/05-authorization.md §3: this row is a trap, not a defect.
    {"star crosses separators", "user-*", "user-7-private", true},
}
for _, tt := range tests {
    t.Run(tt.name, func(t *testing.T) {
        // …
    })
}
```

**`-race` on every run, every layer.** `go test -race ./...` is the only invocation. Not a
nightly job, not a separate CI stage that people learn to ignore — the default. The
concurrency rules in §8 below are exactly the kind of thing that passes code review and
fails under the detector, and `13-review-findings.md` M2 is a defect that no amount of
reading would have caught.

**No sleeps.** `time.Sleep` in a test is either a race waiting to be flaky on a loaded CI
box, or a second of wall clock spent doing nothing, and usually both. Synchronise properly:
a channel the code under test closes, a `sync.WaitGroup`, a fake clock passed in as a
dependency, a `t.Cleanup` that blocks until the goroutine is gone. If the code cannot be
tested without a sleep, the code needs a seam, not the test a delay.

The one thing a test may wait on is a channel with a generous timeout as a *failure*
detector:

```go
select {
case got := <-out:
    // assert
case <-time.After(2 * time.Second):
    t.Fatal("no frame within 2s")
}
```

That is not a sleep. The happy path takes microseconds; the timeout only fires when the
test is about to fail anyway, and a hung test with a clear message beats a hung test.

**Test what the requirement says.** Every test that exists to satisfy a requirement names
it, in the test name or a comment: `TestSubscribe_UngrantedChannel_FR5`. When a requirement
changes, the tests that have to change are then findable with grep instead of by reading.

## 3. Coverage is 100%, per package, and CI checks it

`make cover` runs `scripts/cover.sh`, which instruments every package and fails if any one
of them is below 100% of statements. CI runs the same script. There is no soft target and
no ratchet.

I have used 80% and 90% gates and they do not work, for a reason that took me a while to
see: the uncovered 10% is never randomly distributed. It is the error paths — the branch
that runs when Redis is down, when the webhook returns 500, when the queue is full. Those
are the branches this whole design turns on, and they are the ones a percentage target
lets you skip. Requiring all of it removes the negotiation.

**Anything genuinely uncoverable gets a comment saying why**, on or just above the line:

```go
if err := f.Sync(); err != nil {
    // coverage: only reachable on a full disk or a failing fsync; not worth a
    // fault-injection harness for a path that logs and continues.
    log.Warn("sync failed", "err", err)
}
```

`scripts/cover.sh` reads those comments and exempts the block. That is deliberate: the
exemption lives in the diff, next to the code, where a reviewer can disagree with the
reason. An allowlist in a config file is a place to hide things, and it grows.

A `// coverage:` comment with no reason after the colon is not an exemption. Neither is
"hard to test" — that is a description of a design problem, and the fix is a seam.

Coverage is measured across the whole module (`-coverpkg=./...`), so an integration test
that drives `internal/hub` through `internal/server` counts for both. That is intentional:
the alternative punishes exactly the end-to-end tests `11-testing.md` says matter most.

## 4. Doc comments on every exported identifier

Every exported type, function, method, constant and field has a doc comment, and it starts
with the identifier's own name. `revive`'s `exported` rule enforces it, so this is checked
rather than hoped for.

```go
// Sync makes the bus subscription set exactly desired. Idempotent and batched.
func (b *RedisBus) Sync(ctx context.Context, desired []string) error {
```

The comment says what the caller needs to know to use the thing without reading its body:
what it does, what it does with the arguments it is given, what it returns on failure,
whether it blocks, and whether it is safe to call concurrently. That last one is not
optional in this codebase. Half the types here are touched by three goroutines and a doc
comment that omits the concurrency contract is a doc comment that will be guessed at.

## 5. Comments explain why, and cite the requirement

The code says what it does. A comment repeating that in English is noise that goes stale
the first time someone edits the line above it.

What a comment is for is the reason — and specifically, the reason that is not visible from
the code. Every non-obvious decision cites the thing that mandates it: a requirement
number, or the section of the normative document it comes from.

```go
// FR-15: a full queue closes the connection rather than blocking. One phone in a
// tunnel must not delay delivery for everyone else on the channel.
select {
case c.out <- frame:
default:
    return false
}
```

versus what I do not want, which is:

```go
// Try to send the frame, otherwise return false.
select {
```

The citation matters more than it looks. When someone comes along in a year and sees a
non-blocking send that occasionally drops a connection, the difference between "FR-15" and
nothing is the difference between reading one paragraph of a document and deciding the code
looks like a bug. Every rule in this codebase that appears to be a needless complication is
a rule that was arrived at by getting it wrong first; the citation is the pointer to that
story.

Where a comment restates a piece of arithmetic — the 160 GiB shared-buffer calculation, the
400-concurrent-requests reconnect model — restate the numbers, not the conclusion. Numbers
can be re-checked; "for performance reasons" cannot.

## 6. Errors, returns, and panics

**Wrap with `%w` and add context.** An error that crosses a package boundary says what was
being attempted:

```go
return fmt.Errorf("connect webhook %s: %w", cfg.App.ConnectURL, err)
```

`errorlint` is on, so `%v` on an error is a lint failure. The wrap chain is not decoration:
FR-6's whole distinction between a webhook 401 and a webhook 500 is a decision made by
inspecting an error several frames above where it was created, and a chain broken by a `%v`
somewhere in the middle turns that into a guess. The failure looks like users being
permanently locked out during an application deploy.

**Never `return nil` after a non-nil error.** `nilerr` is on. This is how the earlier
design's `Bus.Subscribe` failure went unnoticed forever: one transient error, swallowed, and
a channel that was locally subscribed and upstream dead with no log line at all
(`13-review-findings.md` M5).

**No naked returns.** A bare `return` in a function with named results makes the reader
scroll up to find out what is being returned, and it is how a zero value gets returned from
an error path by accident. Name results only when they genuinely document something, and
return them explicitly either way.

**No panics outside `init` and contract stubs.** A panic in a connection's goroutine takes
down the process and with it every other connection on the replica, which turns one
malformed frame into a fleet-wide reconnect. Handle the error, or close the one connection.
The `panic("not implemented")` bodies in the current skeleton are the exception, and each
disappears the moment its package is implemented.

Every goroutine that could panic on data it did not construct gets a `recover` at its
boundary, logging the client id and closing that connection only.

## 7. No global mutable state

No package-level `var` that is written after `init`. No singletons. No `sync.Once` hiding a
lazily-constructed global. Dependencies are passed explicitly, as arguments to
constructors, and stored on the struct that uses them.

This one gets pushback because it is more typing, so here is what it buys, concretely: two
tests in the same package can configure the gateway differently and run at the same time.
With a global config, or a global logger, they cannot — they collide, someone adds
`t.Parallel()` removal or a mutex around the whole suite, and the suite gets slow enough
that people stop running it.

The other thing it buys is that the dependency graph is visible in the constructor
signatures. `server.New(Options{Config, Hub, Bus, Webhook, Log})` says what a server needs.
A server that reaches for `config.Default()` says nothing, and by the time it needs a
fourth global nobody can tell what it touches.

Constants are fine. An immutable package-level table is fine. Anything written at runtime
is not.

## 8. Concurrency

The rules in `09-internals.md` §4 are binding. Restated here so there is no version of this
document that lets someone claim they were guidance:

**8.1 Never hold a lock across network I/O, and never block to schedule bus work.** A
subscribe updates the hub map and the desired set, releases the lock, and sets a dirty flag
with a non-blocking store. A reconciler goroutine calls `Bus.Sync`. Nothing on a request
path waits for Redis. The earlier design pushed subscribe and unsubscribe onto a bounded
command channel, and when Redis was merely *slow* the queue filled, the fan-out goroutine
blocked pushing an unsubscribe, and all delivery on the replica stopped — with every socket
still open and `/ready` still returning 200. There was no runbook entry for it because
nothing in the design predicted it (`13-review-findings.md` S2).

**8.2 Subscriptions are state, not events.** `Bus` has `Sync`, not `Subscribe` and
`Unsubscribe`, and it takes the whole desired set. A failed `Sync` leaves the set dirty and
is retried. Reconnect is a forced dirty. Do not add a command queue back.

**8.3 A connection's subscription set moves under the hub lock**, in the same critical
section as the hub map. They are two views of one fact. Any path that touches one without
the other — grant narrowing, control unsubscribe, slow-consumer close — leaves a connection
resident in the hub map after close, so fan-out writes to a dead connection forever and the
refcount never reaches zero.

**8.4 Never block on a connection's outbound queue.** The send is
`select { case out <- f: default: }`. A full queue means the connection is closed with 3005,
by a dedicated closer goroutine, never inline on the fan-out path — closing needs the write
lock that fan-out is currently holding for read.

**8.5 Lock order is hub, then connection.** Never the reverse. Neither is ever held while
sending on a channel. Two paths acquiring them in opposite orders is a deadlock that
`-race` does not detect and that only appears under load.

**8.6 One `RWMutex`, not shards** (NFR-9). Sharding is forbidden until a profile shows
contention above 5% of fan-out time. The first draft had 32 shards for a scale nobody asked
for, and the shard index leaked into every call site.

**8.7 The shared frame buffer is immutable.** One encoded `[]byte` is handed to every
recipient of a fan-out. Appending to it, or reusing it for the next message, is a data race
against ten thousand writer goroutines at once. This is also the difference between 4 KiB
and 32 KiB × 256 of queue per connection (`09-internals.md` §5).

**8.8 One writer goroutine per socket.** No write mutex. A mutex means two writers exist,
and one of them will eventually interleave a partial frame.

## 9. Security

**Never log a cookie, an `Authorization` header, a webhook request or response body, or a
message payload — at any level, including debug** (NFR-7). Log the client id, the channel,
and the namespace. `debug` may add frame *types*; it may not add frame contents.

This process sees every connected user's session cookie. Its logs must not become a
credential store, and a debug line added in a hurry during an incident is exactly how that
happens. There is a required test for it: drive a full connect at `debug`, capture all log
output, assert it contains neither the cookie value nor the payload
(`11-testing.md` §5). It is cheap and it is the only thing that will catch the hurried line.

The same applies to error messages, which end up in logs: an error from `internal/config`
must name the offending key and must not quote the value of `app.webhook_secrets` or
`control.secret`.

Two more, both of which are one line of code and the whole security model:

- **The `Origin` check happens before anything else, and it is an exact string comparison
  against the configured list.** No suffix matching, no wildcards. Browsers do not apply
  CORS to websocket handshakes but do attach cookies (`05-authorization.md` §5).
- **Secrets are compared with `crypto/subtle.ConstantTimeCompare`.** The webhook signature,
  the control signature. `==` on a secret is a timing oracle, and it is free to avoid.

`gosec` is on. When it flags something that is genuinely fine, the suppression carries a
reason in the same comment, same rule as `// coverage:`.

## 10. Public API stability

Everything in this repository lives under `internal/` for now, and that is on purpose:
`internal/` is free to change. Rename a type, change a signature, delete a package — the
only cost is the callers in this repository, and the compiler finds all of them.

Anything exported outside `internal/` is a promise. Before the first tagged release that is
a promise to whoever vendors it; after, it is one I cannot take back without a major
version. So the bar for promoting a package out of `internal/` is that I am willing to keep
its signature for years, and the answer today is no for all of them.

The client library (M3) will be the first thing to face this. When it lands, it gets its own
module path and its own compatibility statement, and the gateway's internals stay internal.

The wire protocol is a separate promise and a stronger one, because a browser holding a
client library I do not control is talking to it. `03-client-protocol.md` §7's code bands
exist so a new code is additive rather than a break, and a protocol change is a
documentation change in the same commit (`AGENTS.md` §4).

## 11. What CI enforces

`make check` locally, the same steps in `.github/workflows/ci.yml`:

| Step | Command | Fails on |
|---|---|---|
| Format | `gofmt -l .` | Any output at all |
| Vet | `go vet ./...` | Any finding |
| Lint | `golangci-lint run ./...` | Any finding |
| Test | `go test -race -cover ./...` | Any failure, any race |
| Coverage | `./scripts/cover.sh` | Any package below 100% |
| Build | `go build` | Any failure |

Tidiness of `go.mod` is checked too — `go mod tidy` must produce no diff — because the
dependency budget is three, and an accidental fourth arriving as an indirect requirement is
how budgets stop being budgets (`AGENTS.md` §6).

Nothing here is skippable with a flag, and none of it should need to be. If a rule is
costing more than it is worth, the fix is to argue it out and change this document, not to
route around it in one pull request.
