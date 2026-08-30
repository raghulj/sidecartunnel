/**
 * sidecartunnel — browser client for the websocket gateway.
 *
 * Normative reference: docs/03-client-protocol.md. Every rule in §8 "Client
 * obligations" is implemented here and cited at the line that implements it.
 * Zero dependencies, no build step: `import { connect } from './sidecartunnel.js'`.
 */

/** Default websocket path (`server.path`, docs/08-config.md). */
const DEFAULT_URL = '/ws';

/** Backoff ceiling in milliseconds — the `30s` of docs/03-client-protocol.md §8.2. */
const MAX_BACKOFF_MS = 30000;

/**
 * Backoff base in milliseconds. §8.2 writes the ceiling as `min(30s, 2^n)`, so
 * the units of `2^n` are seconds; expressed in milliseconds that is
 * `min(30000, 1000 * 2^n)`. Overridable via the `backoffBase` option.
 */
const BACKOFF_BASE_MS = 1000;

/**
 * Close codes whose row in docs/03-client-protocol.md §7 says `reconnect: false`.
 * Used only when the socket closes without a preceding `disconnect` frame; a
 * frame that is present always wins.
 */
const NON_RETRYABLE_CLOSE = new Set([3001, 3003, 3006, 3501]);

/** Protocol error raised against a command, carrying the code from §6. */
export class StError extends Error {
  /**
   * @param {number|string} code Error code from docs/03-client-protocol.md §6,
   *   or a client-side reason string such as `'closed'`.
   * @param {string} message Human-readable message.
   */
  constructor(code, message) {
    super(message);
    this.name = 'StError';
    this.code = code;
  }
}

/**
 * Full-jitter backoff, docs/03-client-protocol.md §8.2: `random(0, min(30s, 2^n))`.
 * Deliberately not the multiplicative `x * random(0.5, 1.5)` form, which cannot
 * produce a small first-attempt spread (§7.1).
 *
 * @param {number} attempt Zero-based retry number `n`.
 * @param {{max?: number, base?: number, random?: () => number}} [opts] Ceiling,
 *   base unit and RNG. Injectable so backoff is testable without a clock.
 * @returns {number} Delay in milliseconds, uniform over `[0, ceiling)`.
 */
export function backoffDelay(attempt, opts = {}) {
  const max = opts.max === undefined ? MAX_BACKOFF_MS : opts.max;
  const base = opts.base === undefined ? BACKOFF_BASE_MS : opts.base;
  const random = opts.random || Math.random;
  return random() * Math.min(max, base * Math.pow(2, attempt));
}

/**
 * Resolve a possibly relative endpoint to a websocket URL. Left untouched when
 * there is no `location` (Node, tests) or the URL is already absolute.
 *
 * @param {string} url Endpoint, e.g. `/ws` or `wss://app.example.com/ws`.
 * @returns {string} Absolute `ws:`/`wss:` URL where one can be derived.
 */
export function resolveUrl(url) {
  if (/^wss?:\/\//i.test(url)) return url;
  const loc = globalThis.location;
  if (!loc || typeof URL !== 'function') return url;
  const abs = new URL(url, loc.href);
  abs.protocol = abs.protocol === 'https:' ? 'wss:' : 'ws:';
  return abs.toString();
}

/** Swallow an application callback's exception; one bad handler must not kill the socket. */
function safe(fn, onError, ...args) {
  if (typeof fn !== 'function') return;
  try {
    fn(...args);
  } catch (err) {
    if (typeof onError === 'function') onError(err);
  }
}

class SidecarTunnel {
  constructor(o) {
    this._url = o.url || DEFAULT_URL;
    this._WS = o.WebSocket || globalThis.WebSocket;
    this._setTimeout = o.setTimeout || globalThis.setTimeout.bind(globalThis);
    this._clearTimeout = o.clearTimeout || globalThis.clearTimeout.bind(globalThis);
    this._random = o.random || Math.random;
    this._backoff = { max: o.maxBackoff, base: o.backoffBase, random: this._random };
    this._onEvent = o.onEvent;
    this._onStateChange = o.onStateChange;
    this._onReconnect = o.onReconnect;
    this._onError = o.onError || ((e) => { if (globalThis.console) globalThis.console.error(e); });

    /** channel -> {state:'pending'|'active'|'unsubscribing', handler, resolve, reject} */
    this._registry = new Map();
    this._pending = new Map();  // command id -> {resolve, reject}
    this._queue = [];           // commands issued before the connect reply (§8.1)
    this._nextId = 1;           // §3 — positive, unique per connection
    this._sock = null;
    this._open = false;         // socket open
    this._ready = false;        // connect reply received (§8.1)
    this._attempt = 0;
    this._connections = 0;
    this._timer = null;
    this._disconnect = null;    // last `disconnect` frame, §5.2
    this._closed = false;       // stopped for good
    this.state = 'connecting';
    this.clientId = null;
    this.stats = { connections: 0, reconnects: 0, orphanPushes: 0, malformed: 0 };

    this._openSocket();
  }

  // ---- lifecycle ---------------------------------------------------------

  _setState(state, info) {
    if (this.state === state && !info) return;
    this.state = state;
    safe(this._onStateChange, this._onError, state, info || {});
  }

  _openSocket() {
    this._timer = null;
    this._setState('connecting', { attempt: this._attempt });
    let sock;
    try {
      sock = new this._WS(resolveUrl(this._url));
    } catch (err) {
      this._onError(err);
      this._scheduleReconnect();
      return;
    }
    this._sock = sock;
    sock.onopen = () => { this._open = true; this._sendConnect(); };
    sock.onmessage = (ev) => this._receive(ev && ev.data);
    sock.onerror = (ev) => safe(this._onError, null, new StError('transport', 'websocket error: ' + (ev && ev.type)));
    sock.onclose = (ev) => this._onClose(ev || {});
  }

  /** §8.1 / §4.1 — `connect` MUST be the first frame; §8.3 replays the registry. */
  _sendConnect() {
    const subs = [];
    for (const [channel, e] of this._registry) {
      if (e.state !== 'unsubscribing') subs.push(channel);
    }
    this._nextId = 1;
    const id = this._nextId++;
    this._pending.set(id, {
      resolve: (r) => this._onConnected(r),
      reject: (err) => this._onError(err),
    });
    this._write({ id, connect: subs.length ? { subs } : {} });
  }

  _onConnected(reply) {
    this.clientId = reply.client || null;
    this.serverPing = reply.ping;
    this.expiresIn = reply.expires_in;
    const granted = (reply && reply.subs) || {};
    const denied = [];
    // §4.1 — channels that failed authorization are omitted from the reply map.
    for (const [channel, e] of Array.from(this._registry)) {
      if (Object.prototype.hasOwnProperty.call(granted, channel)) {
        e.state = 'active';
        this._settle(e, true);
      } else if (e.state !== 'unsubscribing') {
        // A replayed channel the gateway no longer grants. Dropped rather than
        // retained, for the reason §8.5 gives about `unsubscribed`: replaying a
        // channel the grants no longer cover fails on every reconnect forever.
        // Surfaced through `onReconnect` so the application can re-subscribe if
        // the denial was transient.
        denied.push(channel);
        this._registry.delete(channel);
        this._settle(e, false, new StError(103, 'permission denied'));
      }
    }
    this._ready = true;
    const attempts = this._attempt;
    this._attempt = 0;
    this._connections++;
    this.stats.connections = this._connections;
    const q = this._queue;
    this._queue = [];
    for (const thunk of q) thunk();
    this._setState('open', { clientId: this.clientId, channels: Object.keys(granted) });
    if (this._connections > 1) {
      this.stats.reconnects++;
      // §8.6 — every reconnect implies a gap; the application reconciles with
      // `?since=` against its own API. See docs/07-delivery.md §2.
      safe(this._onReconnect, this._onError, {
        attempt: attempts,
        connections: this._connections,
        channels: Object.keys(granted),
        denied,
      });
    } else if (denied.length) {
      safe(this._onReconnect, this._onError, { connections: 1, channels: Object.keys(granted), denied });
    }
  }

  _onClose(ev) {
    this._open = false;
    this._ready = false;
    this._sock = null;
    this.clientId = null;
    const d = this._disconnect;
    this._disconnect = null;
    // In-flight commands cannot be answered on a connection that no longer exists.
    for (const p of this._pending.values()) p.reject(new StError('disconnected', 'connection closed'));
    this._pending.clear();
    for (const [channel, e] of Array.from(this._registry)) {
      // An unsubscribe that was never acknowledged still means the application
      // wants the channel gone: do not replay it.
      if (e.state === 'unsubscribing') { this._registry.delete(channel); this._settle(e, true); }
      else e.state = 'pending';
    }
    for (const thunk of this._queue) thunk.reject(new StError('disconnected', 'connection closed'));
    this._queue = [];
    if (this._closed) {
      if (this.state !== 'closed') this._setState('closed', { reason: 'client closed' });
      return;
    }

    const code = d ? d.code : ev.code;
    const reason = d ? d.reason : ev.reason;
    // §5.2 / §8.4 — `reconnect: false` is a decision. Stop, and surface it.
    const permanent = d ? d.reconnect === false : NON_RETRYABLE_CLOSE.has(code);
    if (permanent) {
      this._closed = true;
      this._setState('closed', { code, reason, permanent: true, reconnect: false });
      for (const e of this._registry.values()) this._settle(e, false, new StError(code, reason || 'closed'));
      this._registry.clear();
      return;
    }
    // §7.1 / §8.2 — `retry_after` (ms) replaces our own backoff for this attempt.
    // The gateway knows how many connections it is dropping and we do not.
    const retryAfter = d && typeof d.retry_after === 'number' ? d.retry_after : null;
    this._scheduleReconnect(retryAfter, { code, reason });
  }

  _scheduleReconnect(retryAfter, info) {
    if (this._closed || this._timer !== null) return;
    const delay = retryAfter !== null && retryAfter !== undefined
      ? retryAfter
      : backoffDelay(this._attempt, this._backoff);
    this._attempt++;
    this._setState('connecting', Object.assign({ attempt: this._attempt, delay, retryAfter: retryAfter ?? null }, info));
    this._timer = this._setTimeout(() => this._openSocket(), delay);
  }

  // ---- framing -----------------------------------------------------------

  _write(frame) {
    try {
      this._sock.send(JSON.stringify(frame));
    } catch (err) {
      safe(this._onError, null, err);
    }
  }

  _receive(raw) {
    let frame;
    try {
      frame = JSON.parse(raw);
    } catch {
      this.stats.malformed++;                    // a malformed frame must not throw
      return;
    }
    if (frame === null || typeof frame !== 'object' || Array.isArray(frame)) {
      this.stats.malformed++;
      return;
    }
    if (frame.disconnect && typeof frame.disconnect === 'object') {
      // §5.2 — sent immediately before the close frame; applied in _onClose.
      this._disconnect = frame.disconnect;
      return;
    }
    if (frame.push) { this._handlePush(frame.push); return; }
    // §3 — a reply carrying `id` corresponds to the command with that `id`.
    if (typeof frame.id === 'number') {
      const p = this._pending.get(frame.id);
      if (!p) { this.stats.malformed++; return; }
      this._pending.delete(frame.id);
      if (frame.error && typeof frame.error === 'object') {
        p.reject(new StError(frame.error.code, frame.error.message || 'error'));
      } else {
        p.resolve(frame.connect || frame.sync || frame.subscribe || frame.unsubscribe ||
                  frame.publish || frame.pong || {});
      }
      return;
    }
    if (frame.pong) return;                       // unsolicited pong, nothing to correlate
    this.stats.malformed++;                       // unknown server frame: counted, never fatal
  }

  /**
   * §5.1 makes ordering normative — no `push` before a channel's `subscribe`
   * reply, none after its `unsubscribe` reply. A defensive client still has to
   * decide what to do when a gateway breaks that rule, and the two obvious
   * answers (drop silently / close the connection) are exactly the divergence
   * §5.1 warns about. The choice here:
   *
   *   - Push for a channel in the registry but not yet confirmed ('pending') or
   *     awaiting its unsubscribe reply ('unsubscribing'): DELIVERED to the
   *     channel handler. The application asked for the channel and has not been
   *     told it is gone, so dropping the message would create a gap that only
   *     reconciliation can close — the expensive outcome — to enforce a rule the
   *     server broke. The 'unsubscribing' case is not even a violation: §5.1
   *     forbids a push only after the reply.
   *   - Push for a channel with no registry entry at all: NOT delivered (there
   *     is no handler to deliver it to) but always passed to `onEvent` with
   *     `orphan: true` and counted in `stats.orphanPushes`. Observable, never
   *     silent, and never fatal.
   */
  _handlePush(push) {
    if (typeof push !== 'object' || typeof push.channel !== 'string') { this.stats.malformed++; return; }
    const channel = push.channel;
    const entry = this._registry.get(channel);
    if (push.unsubscribed) {
      // §8.5 — remove from the registry and do not resubscribe. A client that
      // replays a revoked channel gets 103 on every reconnect forever.
      if (entry) {
        this._registry.delete(channel);
        this._settle(entry, false, new StError(103, (push.unsubscribed && push.unsubscribed.reason) || 'unsubscribed'));
      }
      safe(this._onEvent, this._onError, { channel, unsubscribed: push.unsubscribed, orphan: !entry });
      return;
    }
    if (!push.pub) { this.stats.malformed++; return; }
    if (entry) safe(entry.handler, this._onError, push.pub, channel);
    else this.stats.orphanPushes++;
    safe(this._onEvent, this._onError, { channel, pub: push.pub, orphan: !entry });
  }

  _settle(entry, ok, err) {
    const fn = ok ? entry.resolve : entry.reject;
    entry.resolve = null;
    entry.reject = null;
    if (fn) fn(ok ? true : err);
  }

  /** Issue a command, deferring it until the connect reply has landed (§8.1). */
  _request(command, payload) {
    return new Promise((resolve, reject) => {
      if (this._closed) { reject(new StError('closed', 'client is closed')); return; }
      const send = () => {
        const id = this._nextId++;
        this._pending.set(id, { resolve, reject });
        this._write({ id, [command]: payload });
      };
      if (this._ready) send();
      else { send.reject = reject; this._queue.push(send); }
    });
  }

  // ---- public API --------------------------------------------------------

  subscribe(channel, handler) {
    if (this._closed) return Promise.reject(new StError('closed', 'client is closed'));
    const existing = this._registry.get(channel);
    if (existing && existing.state !== 'unsubscribing') {
      // §4.2 — a duplicate subscribe means the local registry has drifted, and
      // hiding that makes reconnect bugs very hard to find. Reported locally
      // with the same code the gateway would return.
      return Promise.reject(new StError(104, 'already subscribed'));
    }
    return new Promise((resolve, reject) => {
      const entry = { state: 'pending', handler, resolve, reject };
      this._registry.set(channel, entry);
      if (!this._ready) return;   // rides along in the connect frame's `subs` (§4.1)
      this._request('subscribe', { channel }).then(
        () => { if (this._registry.get(channel) === entry) entry.state = 'active'; this._settle(entry, true); },
        (err) => {
          // The connection died before the reply. The entry stays in the
          // registry and rides the next connect frame's `subs` (§8.3); the
          // promise is settled by that reply, not by this one.
          if (err.code === 'disconnected') return;
          // 103/104/108: the gateway does not hold it, so neither do we.
          if (this._registry.get(channel) === entry) this._registry.delete(channel);
          this._settle(entry, false, err);
        },
      );
    });
  }

  unsubscribe(channel) {
    const entry = this._registry.get(channel);
    if (!entry) return Promise.reject(new StError(105, 'not subscribed'));
    // The handler stays attached until the reply lands: §5.1 only forbids a push
    // AFTER the `unsubscribe` reply, so anything arriving before it is ordinary
    // in-spec traffic for a channel the connection still holds.
    if (!this._ready) {
      // Nothing was sent on this connection; dropping it locally is enough to
      // keep it out of the next connect frame's `subs` (§8.3).
      this._registry.delete(channel);
      this._settle(entry, false, new StError('unsubscribed', 'unsubscribed before connect'));
      return Promise.resolve();
    }
    entry.state = 'unsubscribing';
    return this._request('unsubscribe', { channel }).then(
      () => { this._registry.delete(channel); },
      (err) => { if (err.code === 105) this._registry.delete(channel); throw err; },
    );
  }

  sync() {
    // §4.5 — the gateway's authoritative subscription set for this connection.
    return this._request('sync', {}).then((r) => (r && r.channels) || []);
  }

  publish(channel, event, data) {
    // §4.4 — permitted only where the namespace sets `client_events: true`.
    return this._request('publish', { channel, event, data });
  }

  ping() {
    // §4.6 — application-level liveness; browsers hide websocket-level pongs.
    return this._request('ping', {});
  }

  channels() {
    return Array.from(this._registry.keys());
  }

  close() {
    if (this._closed) return;   // idempotent
    this._closed = true;
    if (this._timer !== null) { this._clearTimeout(this._timer); this._timer = null; }
    const sock = this._sock;
    this._sock = null;
    this._open = false;
    this._ready = false;
    if (sock) { try { sock.close(1000, 'client closed'); } catch { /* already gone */ } }
    if (this.state !== 'closed') this._setState('closed', { reason: 'client closed' });
  }
}

/**
 * Open a connection to a sidecartunnel gateway. Connects immediately, sends
 * `connect` as its first frame (docs/03-client-protocol.md §8.1) and reconnects
 * on its own until told not to (§8.2, §8.4).
 *
 * @param {object} [options]
 * @param {string} [options.url='/ws'] Gateway endpoint. Relative paths resolve
 *   against `location` so the socket is same-origin and the session cookie is
 *   attached to the upgrade.
 * @param {(evt: {channel: string, pub?: object, unsubscribed?: object, orphan: boolean}) => void} [options.onEvent]
 *   Called for every `push`, including ones no channel handler claimed.
 * @param {(state: 'connecting'|'open'|'closed', info: object) => void} [options.onStateChange]
 *   Called on every state transition.
 * @param {(info: {connections: number, channels: string[], denied: string[]}) => void} [options.onReconnect]
 *   Called after the connect reply of every reconnection. Run the `?since=`
 *   reconciliation fetch here (§8.6, docs/07-delivery.md §2).
 * @param {(err: Error) => void} [options.onError] Transport errors and exceptions
 *   thrown by application callbacks.
 * @param {number} [options.maxBackoff=30000] Backoff ceiling in milliseconds.
 * @param {number} [options.backoffBase=1000] Base unit of `2^n` in milliseconds.
 * @param {typeof WebSocket} [options.WebSocket] Websocket constructor, injectable for tests.
 * @param {Function} [options.setTimeout] Timer, injectable for tests.
 * @param {Function} [options.clearTimeout] Timer, injectable for tests.
 * @param {() => number} [options.random] RNG for jitter, injectable for tests.
 * @returns {SidecarTunnel} A live client.
 */
export function connect(options = {}) {
  return new SidecarTunnel(options);
}

export default { connect, backoffDelay, resolveUrl, StError };
