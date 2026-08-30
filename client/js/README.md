# sidecartunnel — JavaScript Client

Browser client for the sidecartunnel websocket gateway. One file, no dependencies, no build step. It implements every obligation in [`docs/03-client-protocol.md`](../../docs/03-client-protocol.md) §8: `connect` first, `retry_after` over local backoff, full-jitter reconnect, `reconnect: false` honoured permanently, a local subscription registry replayed after every reconnect, and a hook for the `?since=` reconciliation that [`docs/07-delivery.md`](../../docs/07-delivery.md) §2 requires.

**Not published.** `package.json` is marked `private`. Vendor the file or install from the repository until the gateway itself ships.

## Install

Two forms of the same code. `sidecartunnel.umd.js` is generated from `sidecartunnel.js` by `node build-umd.mjs`; it is never edited by hand.

| Form | File | Use |
|---|---|---|
| ES module | `sidecartunnel.js` | `import` in a browser, a bundler, or Node |
| UMD / IIFE | `sidecartunnel.umd.js` | a plain `<script>` tag, exposing `window.sidecartunnel` |

```html
<script type="module">
  import { connect } from '/static/sidecartunnel.js';
</script>
```

```html
<script src="/static/sidecartunnel.umd.js"></script>
<script>
  const st = sidecartunnel.connect({ url: '/ws' });
</script>
```

## Quick Start

The socket is opened against the same origin as the application, so the session cookie is attached to the upgrade automatically. There is no authentication code in the frontend.

```js
import { connect } from './sidecartunnel.js';

const st = connect({
  url: '/ws',
  onStateChange(state, info) {
    document.body.dataset.realtime = state;
    if (state === 'closed' && info.permanent) showSignedOutBanner(info.reason);
  },
  onReconnect: reconcile,
});

await st.subscribe('room-4410', (pub) => {
  if (pub.event === 'message.created') renderMessage(pub.data);
});
```

`subscribe` calls made before the socket opens are folded into the `connect` frame's `subs`, which saves a round trip on page load ([`03-client-protocol.md`](../../docs/03-client-protocol.md) §4.1). No special call is needed for that.

## API

`connect(options)` returns a client. Every method that talks to the gateway returns a promise that rejects with an `StError` carrying the protocol code from §6.

| Member | Returns | Description |
|---|---|---|
| `st.subscribe(channel, handler)` | `Promise<true>` | Subscribe and register `handler(pub, channel)`. Rejects `103` when the grant does not cover the channel, `104` when the channel is already held, `108` at the per-connection subscription limit. |
| `st.unsubscribe(channel)` | `Promise<void>` | Drop the channel and remove it from the registry. Rejects `105` when the channel is not held. |
| `st.sync()` | `Promise<string[]>` | The gateway's authoritative subscription set for this connection (§4.5). The first thing to call when nothing appears to be arriving. |
| `st.publish(channel, event, data)` | `Promise<object>` | Client event. Permitted only where the namespace sets `client_events: true`; otherwise rejects `103`, or `106` when rate limited. |
| `st.ping()` | `Promise<object>` | Application-level ping (§4.6). Browsers hide websocket-level pongs from JavaScript. |
| `st.channels()` | `string[]` | The local registry — what will be replayed on the next reconnect. |
| `st.close()` | `void` | Stop permanently. Idempotent. |
| `st.state` | `'connecting' \| 'open' \| 'closed'` | Current state. `'connecting'` covers both backoff and an unanswered handshake. |
| `st.clientId` | `string \| null` | 16 hex chars, stable for the connection's life. Sent as `X-St-Client` on writes. `null` while not open. |
| `st.stats` | `object` | `{connections, reconnects, orphanPushes, malformed}`. Diagnostics. |
| `st.serverPing` | `number` | Server ping interval in seconds, from the connect reply. Informational. |
| `st.expiresIn` | `number` | Seconds until the gateway closes with 3503 for re-authorization. |

### Options

| Option | Default | Description |
|---|---|---|
| `url` | `'/ws'` | Gateway endpoint. A relative path resolves against `location` and the scheme is swapped to `ws:`/`wss:`. |
| `onEvent(evt)` | — | Called for **every** push: `{channel, pub, orphan}` or `{channel, unsubscribed, orphan}`. Fires whether or not a channel handler claimed it. |
| `onStateChange(state, info)` | — | Every transition. `info` carries `attempt`, `delay`, `retryAfter`, `code`, `reason`, `permanent` where they apply. |
| `onReconnect(info)` | — | After the connect reply of every reconnection, with `{connections, channels, denied}`. Run the `?since=` fetch here. |
| `onError(err)` | `console.error` | Transport errors and exceptions thrown by application callbacks. |
| `maxBackoff` | `30000` | Backoff ceiling in milliseconds. |
| `backoffBase` | `1000` | Base unit of `2^n` in milliseconds. |
| `WebSocket` | `globalThis.WebSocket` | Constructor override. Injected by the tests. |
| `setTimeout` / `clearTimeout` | globals | Timer override. Injected by the tests so backoff is asserted without sleeping. |
| `random` | `Math.random` | Jitter source. Injected by the tests. |

### Exports

| Export | Description |
|---|---|
| `connect(options)` | Opens a client. |
| `backoffDelay(attempt, opts)` | The full-jitter formula, exported so it can be asserted directly. |
| `resolveUrl(url)` | Relative endpoint to absolute websocket URL. |
| `StError` | `Error` subclass with a `code` field. |

## Reconnection and Backoff Contract

The gateway sends a `disconnect` frame immediately before the close frame. That frame is authoritative; the close code is a fallback for an abrupt drop.

| Signal | Client behaviour |
|---|---|
| `retry_after` present | That value, in milliseconds, is used **in place of** local backoff for the attempt. The gateway knows how many connections it is dropping and the client does not. |
| `retry_after` absent | Full jitter: `random(0, min(30000, 1000 × 2^n))`. |
| `reconnect: false` | Stop permanently. No timer is scheduled, no further socket is opened, and every later command rejects with code `'closed'`. |
| No `disconnect` frame | Close codes 3001, 3003, 3006 and 3501 are treated as permanent per the §7 table. Everything else, including 1006, is retried. |
| Successful connect reply | The attempt counter resets to zero. |

The jitter is **full jitter**, not the multiplicative `x × random(0.5, 1.5)` form. §7.1 is explicit about why: the multiplicative form yields 0.5–1.5 s at the first retry, which is the one-second window [`10-operations.md`](../../docs/10-operations.md) §4 models as an application outage. Full jitter reaches the whole range including zero, so the retries spread from the first attempt rather than after the damage.

`2^n` is expressed in seconds by §8.2 (`min(30s, 2^n)`), so the implementation uses `min(30000, 1000 × 2^n)` milliseconds. `backoffBase` overrides the base unit for anyone who reads it otherwise.

Every in-flight command rejects with `StError` code `'disconnected'` when the connection dies. A `subscribe` that was still awaiting its reply is the exception: it stays in the registry, is replayed in the next `connect` frame, and settles on that reply instead.

## The Subscription Registry

The gateway remembers nothing across connections (§8.3). The client keeps its own registry and replays it as the `connect` frame's `subs`, which authorizes the whole set in one round trip.

| Event | Effect on the registry |
|---|---|
| `subscribe` accepted | Added. |
| `subscribe` refused (103, 104, 108) | Not added. |
| `unsubscribe` accepted | Removed. |
| `unsubscribed` push | Removed, and never resubscribed. A client that replays a revoked channel gets 103 on every reconnect forever (§8.5). |
| Channel omitted from a replayed connect reply | Removed, and reported in `onReconnect().denied`. The grant behind it is gone, so replaying it has the same failure mode as the previous row. Re-subscribe explicitly if the denial is expected to be transient. |

`st.sync()` returns the gateway's view. Comparing it against `st.channels()` is the way to find a divergence, whose symptom is otherwise indistinguishable from a quiet channel.

## Reconciliation With `?since=`

Delivery is at-most-once and every reconnect implies a gap ([`07-delivery.md`](../../docs/07-delivery.md) §2). Realtime is an accelerator in front of a database that was always the source of truth, and the client closes its own gaps. Skipping this is the failure that looks like it works until the first deploy.

```js
import { connect } from './sidecartunnel.js';

const ROOM = 4410;
let lastSeenId = 0;

function render(message) {
  if (message.id <= lastSeenId) return;   // the merge is idempotent on the payload id
  lastSeenId = message.id;
  document.querySelector('#messages').append(row(message));
}

/** Fetch everything published while the socket was down, then merge. */
async function reconcile({ channels, denied }) {
  const res = await fetch(`/api/messages?room=${ROOM}&since=${lastSeenId}`);
  for (const message of await res.json()) render(message);
  for (const channel of denied) console.warn('grant lost, not replayed:', channel);
}

const st = connect({
  url: '/ws',
  onReconnect: reconcile,        // fires after every reconnect, not the first connect
  onStateChange(state, info) {
    if (state === 'closed' && info.permanent) location.assign('/login');
  },
});

await st.subscribe(`room-${ROOM}`, (pub) => {
  if (pub.event === 'message.created') render(pub.data);
});

await reconcile({ channels: st.channels(), denied: [] });   // initial page load
```

`onReconnect` deliberately does not fire for the first connection: a page load fetches its own initial state. The call above makes that explicit rather than implicit.

## Writes and the `X-St-Client` Header

The socket is receive-only for anything durable. Writes go over ordinary HTTP to the application, which persists and then publishes, so CSRF, rate limiting and validation apply unchanged.

`exclude` needs the originating connection's client id, which the browser has from its connect reply and the application does not. Send it as `X-St-Client` so the event does not echo back to the tab that caused it. Each tab has a distinct client id, which is the point — the other tabs should receive the event.

```js
async function postMessage(body) {
  const res = await fetch(`/api/messages`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'X-CSRF-Token': csrfToken,
      ...(st.clientId ? { 'X-St-Client': st.clientId } : {}),
    },
    body: JSON.stringify({ room: ROOM, body }),
  });
  render(await res.json());     // optimistic render; the push is excluded
}
```

The application forwards it into the envelope's `exclude`:

```python
@app.post("/api/messages")
def create_message():
    message = Message.create(room=request.json["room"], body=request.json["body"])
    envelope = {"event": "message.created", "data": message.to_dict()}
    if client := request.headers.get("X-St-Client"):
        envelope["exclude"] = client
    redis.publish(f"st:room-{message.room}", json.dumps(envelope))
    return message.to_dict()
```

`st.clientId` is `null` while the socket is not open. The spread above omits the header in that case rather than sending `null`, and the write still succeeds — the tab simply receives its own event back on the next connection, which the id-keyed merge discards.

## Out-of-Order Pushes

[`03-client-protocol.md`](../../docs/03-client-protocol.md) §5.1 makes ordering normative on the gateway: no `push` for a channel before that channel's `subscribe` reply, none after its `unsubscribe` reply. A defensive client still needs a defined answer for a gateway that breaks the rule, because the two obvious answers — drop silently, or close the connection — are exactly the silent divergence §5.1 exists to prevent.

| Situation | Behaviour |
|---|---|
| Push for a channel in the registry, `subscribe` reply not yet received | Delivered to the handler. The application asked for the channel and has not been told it is gone; dropping the message opens a gap only reconciliation can close. |
| Push for a channel whose `unsubscribe` is still in flight | Delivered. Not a violation — §5.1 forbids a push only after the reply. |
| Push for a channel with no registry entry at all | No handler exists, so none is called. The push is passed to `onEvent` with `orphan: true` and counted in `stats.orphanPushes`. Observable, never silent, never fatal. |

Malformed frames are counted in `stats.malformed` and ignored. Nothing a gateway sends can make the client throw.

## Tests

Node's built-in runner, no npm dependencies. The websocket and the clock are both fakes, so no test sleeps and backoff is asserted against the delay handed to the injected timer rather than against elapsed wall time.

```sh
cd client/js && node --test                    # 52 tests
node --test client/js/sidecartunnel.test.js    # or by file, from the repository root
node client/js/build-umd.mjs --check           # fails if the UMD bundle is stale
npm run check                                  # both
```

Node's `--test` recurses only from the working directory. A bare directory argument — `node --test client/js/` — is treated as a literal path and discovers nothing, on Node 22 through 26 alike. Pass the file, a glob, or run from inside the directory.

## Files

| File | Contents |
|---|---|
| `sidecartunnel.js` | The client. The only file an application needs. |
| `sidecartunnel.umd.js` | Generated UMD bundle for a `<script>` tag. Do not edit. |
| `build-umd.mjs` | Generates the bundle. Strips `export` keywords; no compilation. |
| `sidecartunnel.test.js` | The test suite. |
| `package.json` | Metadata. `private: true` — not published. |

## License

MIT, with the rest of the repository.
