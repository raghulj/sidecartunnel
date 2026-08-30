/**
 * Tests for the sidecartunnel browser client.
 *
 * Node's built-in runner only: `node --test client/js/`. No npm dependencies.
 * The websocket and the clock are both fakes, so nothing here sleeps and
 * nothing here is timing-dependent — backoff is asserted against the delay the
 * client hands the injected timer, never against elapsed wall time.
 */
import { test, describe } from 'node:test';
import assert from 'node:assert/strict';
import vm from 'node:vm';
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';

import { connect, backoffDelay, resolveUrl, StError } from './sidecartunnel.js';
import { toUmd } from './build-umd.mjs';

const HERE = dirname(fileURLToPath(import.meta.url));

// ---------------------------------------------------------------------------
// Fakes
// ---------------------------------------------------------------------------

/** Deterministic replacement for setTimeout/clearTimeout. Nothing waits. */
class FakeClock {
  constructor() {
    this.now = 0;
    this.timers = [];
    this.seq = 0;
    this.scheduled = [];               // every delay ever requested, in order
    this.setTimeout = (fn, delay) => {
      const t = { id: ++this.seq, at: this.now + delay, delay, fn };
      this.timers.push(t);
      this.scheduled.push(delay);
      return t.id;
    };
    this.clearTimeout = (id) => { this.timers = this.timers.filter((t) => t.id !== id); };
  }

  get pending() { return this.timers.length; }

  /** Fire every timer that is currently due, in scheduled order. */
  advance(ms) {
    this.now += ms;
    for (;;) {
      this.timers.sort((a, b) => a.at - b.at || a.id - b.id);
      const t = this.timers[0];
      if (!t || t.at > this.now) return;
      this.timers.shift();
      t.fn();
    }
  }

  /** Fire the next pending timer whatever its deadline. */
  fireNext() {
    this.timers.sort((a, b) => a.at - b.at || a.id - b.id);
    const t = this.timers.shift();
    assert.ok(t, 'expected a pending timer');
    this.now = t.at;
    t.fn();
    return t.delay;
  }
}

/** A websocket the test drives by hand. Callbacks fire synchronously. */
function makeTransport() {
  const sockets = [];
  class FakeWebSocket {
    constructor(url) {
      this.url = url;
      this.sent = [];
      this.closed = false;
      this.closeArgs = null;
      sockets.push(this);
    }
    send(raw) { this.sent.push(JSON.parse(raw)); }
    close(code, reason) {
      if (this.closed) return;
      this.closed = true;
      this.closeArgs = { code, reason };
      if (this.onclose) this.onclose({ code: code || 1000, reason: reason || '' });
    }
    // --- test-side drivers ---
    accept() { this.onopen({ type: 'open' }); }
    emit(frame) { this.onmessage({ data: typeof frame === 'string' ? frame : JSON.stringify(frame) }); }
    serverClose(code, reason) {
      if (this.closed) return;
      this.closed = true;
      this.onclose({ code, reason: reason || '' });
    }
    /** Last frame the client sent. */
    get last() { return this.sent[this.sent.length - 1]; }
  }
  return { FakeWebSocket, sockets, last: () => sockets[sockets.length - 1] };
}

/** Flush the microtask queue. A turn of the loop, not a sleep. */
const tick = () => new Promise((r) => setImmediate(r));

/** Build a client wired to fakes, with every callback recorded. */
function harness(overrides = {}) {
  const clock = new FakeClock();
  const t = makeTransport();
  const events = [];
  const states = [];
  const reconnects = [];
  const errors = [];
  const st = connect({
    url: 'ws://gateway.test/ws',
    WebSocket: t.FakeWebSocket,
    setTimeout: clock.setTimeout,
    clearTimeout: clock.clearTimeout,
    random: () => 0.5,
    onEvent: (e) => events.push(e),
    onStateChange: (s, i) => states.push([s, i]),
    onReconnect: (i) => reconnects.push(i),
    onError: (e) => errors.push(e),
    ...overrides,
  });
  return { clock, t, st, events, states, reconnects, errors, sock: () => t.last() };
}

/** Open the current socket and answer its `connect` with the given reply. */
async function handshake(h, reply = {}) {
  const sock = h.sock();
  sock.accept();
  const frame = sock.sent[0];
  sock.emit({ id: frame.id, connect: { client: '8f2c1e04a7b3d915', ping: 25, expires_in: 3600, subs: {}, ...reply } });
  await tick();
  return sock;
}

/** Attach a no-op catch so an expected rejection is not an unhandled one. */
const swallow = (p) => { p.catch(() => {}); return p; };

// ---------------------------------------------------------------------------

describe('handshake — docs/03-client-protocol.md §8.1', () => {
  test('sends connect as the very first frame and reports open only after the reply', async () => {
    const h = harness();
    assert.equal(h.st.state, 'connecting');
    const sock = h.sock();
    sock.accept();
    assert.equal(sock.sent.length, 1);
    assert.deepEqual(sock.sent[0], { id: 1, connect: {} });
    assert.equal(h.st.state, 'connecting', 'not open until the connect reply lands');

    sock.emit({ id: 1, connect: { client: '8f2c1e04a7b3d915', ping: 25, expires_in: 3600, subs: {} } });
    await tick();
    assert.equal(h.st.state, 'open');
    assert.equal(h.st.clientId, '8f2c1e04a7b3d915', 'clientId feeds the X-St-Client header');
    assert.equal(h.st.serverPing, 25);
    assert.equal(h.st.expiresIn, 3600);
  });

  test('commands issued before the reply are queued, not sent', async () => {
    const h = harness();
    const sock = h.sock();
    sock.accept();
    const syncing = h.st.sync();
    assert.equal(sock.sent.length, 1, 'only the connect frame is on the wire');

    sock.emit({ id: 1, connect: { client: 'c', subs: {} } });
    await tick();
    assert.equal(sock.sent.length, 2);
    assert.equal(Object.keys(sock.sent[1])[1], 'sync');
    sock.emit({ id: sock.sent[1].id, sync: { channels: ['room-4410'] } });
    assert.deepEqual(await syncing, ['room-4410']);
  });

  test('subscribes requested before the socket opens ride along in connect.subs', async () => {
    const h = harness();
    const sub = h.st.subscribe('room-4410', () => {});
    h.sock().accept();
    assert.deepEqual(h.sock().sent[0], { id: 1, connect: { subs: ['room-4410'] } });
    h.sock().emit({ id: 1, connect: { client: 'c', subs: { 'room-4410': {} } } });
    assert.equal(await sub, true);
  });

  test('a channel omitted from the connect reply map is a denial (§4.1)', async () => {
    const h = harness();
    const ok = h.st.subscribe('room-4410', () => {});
    const denied = swallow(h.st.subscribe('org-99-secret', () => {}));
    h.sock().accept();
    assert.deepEqual(h.sock().sent[0].connect.subs, ['room-4410', 'org-99-secret']);
    h.sock().emit({ id: 1, connect: { client: 'c', subs: { 'room-4410': {} } } });
    assert.equal(await ok, true);
    await assert.rejects(denied, (e) => e instanceof StError && e.code === 103);
    assert.deepEqual(h.st.channels(), ['room-4410'], 'the denied channel is not retained');
  });
});

describe('subscribe / unsubscribe', () => {
  test('allow', async () => {
    const h = harness();
    const sock = await handshake(h);
    const seen = [];
    const p = h.st.subscribe('room-4410', (pub, ch) => seen.push([ch, pub]));
    assert.deepEqual(sock.last, { id: 2, subscribe: { channel: 'room-4410' } });
    sock.emit({ id: 2, subscribe: {} });
    assert.equal(await p, true);

    sock.emit({ push: { channel: 'room-4410', pub: { event: 'order.created', data: { id: 88123 } } } });
    assert.deepEqual(seen, [['room-4410', { event: 'order.created', data: { id: 88123 } }]]);
    assert.equal(h.events[0].orphan, false);
  });

  test('deny — error 103 rejects and leaves the registry clean', async () => {
    const h = harness();
    const sock = await handshake(h);
    const p = swallow(h.st.subscribe('org-99-secret', () => {}));
    sock.emit({ id: 2, error: { code: 103, message: 'permission denied' } });
    await assert.rejects(p, (e) => e instanceof StError && e.code === 103 && e.message === 'permission denied');
    assert.deepEqual(h.st.channels(), [], 'a denied channel must not be replayed on reconnect');
  });

  test('duplicate subscribe is 104 locally and never reaches the wire (§4.2)', async () => {
    const h = harness();
    const sock = await handshake(h);
    const first = h.st.subscribe('room-4410', () => {});
    sock.emit({ id: 2, subscribe: {} });
    assert.equal(await first, true);

    const before = sock.sent.length;
    await assert.rejects(h.st.subscribe('room-4410', () => {}), (e) => e.code === 104);
    assert.equal(sock.sent.length, before, 'the drift is reported without a round trip');
  });

  test('a 104 from the gateway is surfaced verbatim', async () => {
    const h = harness();
    const sock = await handshake(h);
    const p = swallow(h.st.subscribe('room-4410', () => {}));
    sock.emit({ id: 2, error: { code: 104, message: 'already subscribed' } });
    await assert.rejects(p, (e) => e.code === 104);
  });

  test('unsubscribe removes the channel and stops delivery', async () => {
    const h = harness();
    const sock = await handshake(h);
    const seen = [];
    const s = h.st.subscribe('room-4410', () => seen.push(1));
    sock.emit({ id: 2, subscribe: {} });
    await s;

    const p = h.st.unsubscribe('room-4410');
    assert.deepEqual(sock.last, { id: 3, unsubscribe: { channel: 'room-4410' } });
    sock.emit({ id: 3, unsubscribe: {} });
    await p;
    assert.deepEqual(h.st.channels(), []);
    sock.emit({ push: { channel: 'room-4410', pub: { event: 'x', data: 1 } } });
    assert.deepEqual(seen, [], 'no delivery after the unsubscribe reply');
  });

  test('unsubscribe on a channel not held is 105 without a round trip', async () => {
    const h = harness();
    const sock = await handshake(h);
    const before = sock.sent.length;
    await assert.rejects(h.st.unsubscribe('nope'), (e) => e.code === 105);
    assert.equal(sock.sent.length, before);
  });

  test('sync returns the gateway authoritative set (§4.5)', async () => {
    const h = harness();
    const sock = await handshake(h);
    const p = h.st.sync();
    assert.deepEqual(sock.last, { id: 2, sync: {} });
    sock.emit({ id: 2, sync: { channels: ['room-4410', 'user-7'] } });
    assert.deepEqual(await p, ['room-4410', 'user-7']);
  });

  test('sync with no channels key yields an empty set', async () => {
    const h = harness();
    const sock = await handshake(h);
    const p = h.st.sync();
    sock.emit({ id: 2, sync: {} });
    assert.deepEqual(await p, []);
  });
});

describe('id correlation — §3', () => {
  test('replies are matched by id even when they arrive out of order among pushes', async () => {
    const h = harness();
    const sock = await handshake(h);
    const subbing = h.st.subscribe('desk-42', () => {});   // id 2
    const syncing = h.st.sync();                            // id 3
    const publishing = h.st.publish('desk-42', 'typing', { typing: true }); // id 4
    assert.deepEqual(sock.sent.map((f) => f.id), [1, 2, 3, 4]);

    // Server-initiated frames may arrive between a command and its reply (§3).
    sock.emit({ push: { channel: 'room-4410', pub: { event: 'a', data: 1 } } });
    sock.emit({ id: 4, publish: {} });
    sock.emit({ push: { channel: 'room-4410', pub: { event: 'b', data: 2 } } });
    sock.emit({ id: 3, sync: { channels: ['room-4410', 'desk-42'] } });
    sock.emit({ id: 2, subscribe: {} });

    assert.equal(await subbing, true);
    assert.deepEqual(await syncing, ['room-4410', 'desk-42']);
    await publishing;
    assert.deepEqual(h.events.map((e) => e.pub.event), ['a', 'b']);
  });

  test('ids are positive, unique, and restart at 1 on a new connection', async () => {
    const h = harness();
    let sock = await handshake(h);
    swallow(h.st.sync());
    swallow(h.st.sync());
    assert.deepEqual(sock.sent.map((f) => f.id), [1, 2, 3]);
    for (const f of sock.sent) assert.ok(Number.isInteger(f.id) && f.id > 0);

    sock.serverClose(3004, 'ping timeout');
    h.clock.fireNext();
    sock = h.sock();
    sock.accept();
    assert.equal(sock.sent[0].id, 1, 'ids are unique per connection, not per client');
  });
});

describe('reconnection — §7.1, §8.2', () => {
  test('retry_after replaces our own backoff for that attempt', async () => {
    const h = harness();
    const sock = await handshake(h);
    sock.emit({ disconnect: { code: 3000, reason: 'draining', reconnect: true, retry_after: 18400 } });
    sock.serverClose(3000, 'draining');

    assert.equal(h.clock.pending, 1);
    assert.deepEqual(h.clock.scheduled, [18400], 'the gateway spread wins over local jitter');
    const info = h.states.filter(([s]) => s === 'connecting').pop()[1];
    assert.equal(info.retryAfter, 18400);
    assert.equal(info.code, 3000);
  });

  test('retry_after of 0 is honoured, not treated as absent', async () => {
    const h = harness();
    const sock = await handshake(h);
    sock.emit({ disconnect: { code: 3000, reason: 'draining', reconnect: true, retry_after: 0 } });
    sock.serverClose(3000, 'draining');
    assert.deepEqual(h.clock.scheduled, [0]);
  });

  test('without retry_after the delay is full jitter over the attempt ceiling', async () => {
    const rnd = 0.25;
    const h = harness({ random: () => rnd });
    let sock = await handshake(h);
    for (let n = 0; n < 6; n++) {
      sock.serverClose(3004, 'ping timeout');            // no disconnect frame
      const ceiling = Math.min(30000, 1000 * Math.pow(2, n));
      assert.equal(h.clock.scheduled[n], rnd * ceiling, `attempt ${n}`);
      h.clock.fireNext();
      sock = h.sock();
      sock.accept();
      sock.emit({ id: 1, error: { code: 100, message: 'nope' } });   // fail the handshake
      await tick();
    }
  });

  test('a successful connect resets the attempt counter', async () => {
    const h = harness();
    let sock = await handshake(h);
    sock.serverClose(3004, '');
    assert.equal(h.clock.scheduled[0], 0.5 * 1000);
    h.clock.fireNext();
    sock = h.sock();
    await handshake(h);
    sock.serverClose(3004, '');
    assert.equal(h.clock.scheduled[1], 0.5 * 1000, 'back to n=0 after a good connection');
  });

  test('a close code the §7 table marks reconnect:false stops even with no disconnect frame', async () => {
    for (const code of [3001, 3003, 3006, 3501]) {
      const h = harness();
      const sock = await handshake(h);
      sock.serverClose(code, 'x');
      assert.equal(h.clock.pending, 0, `close ${code} must not schedule a retry`);
      assert.equal(h.st.state, 'closed');
    }
  });

  test('an unrecognised close code is treated as retryable', async () => {
    const h = harness();
    const sock = await handshake(h);
    sock.serverClose(1006, 'abnormal');
    assert.equal(h.clock.pending, 1);
  });
});

describe('full jitter distribution — §8.2', () => {
  test('samples stay inside [0, min(30000, 2**n)] for the literal spec form', () => {
    let seed = 12345;
    const rng = () => { seed = (seed * 1103515245 + 12345) % 2147483648; return seed / 2147483648; };
    for (let n = 0; n < 20; n++) {
      const ceiling = Math.min(30000, Math.pow(2, n));
      for (let i = 0; i < 500; i++) {
        const d = backoffDelay(n, { base: 1, max: 30000, random: rng });
        assert.ok(d >= 0 && d <= ceiling, `n=${n} produced ${d}, ceiling ${ceiling}`);
      }
    }
  });

  test('samples stay inside [0, min(30000, 1000 * 2**n)] with the default millisecond base', () => {
    let seed = 999;
    const rng = () => { seed = (seed * 1103515245 + 12345) % 2147483648; return seed / 2147483648; };
    for (let n = 0; n < 20; n++) {
      const ceiling = Math.min(30000, 1000 * Math.pow(2, n));
      for (let i = 0; i < 500; i++) {
        const d = backoffDelay(n, { random: rng });
        assert.ok(d >= 0 && d <= ceiling, `n=${n} produced ${d}, ceiling ${ceiling}`);
      }
    }
  });

  test('the ceiling is clamped at 30s however large n gets', () => {
    assert.equal(backoffDelay(99, { random: () => 1 }), 30000);
    assert.equal(backoffDelay(5, { random: () => 1 }), 30000);
    assert.equal(backoffDelay(4, { random: () => 1 }), 16000);
  });

  test('it is full jitter, not the multiplicative form §7.1 rejects', () => {
    // random() === 0 must yield 0. The x * random(0.5, 1.5) form yields half the
    // ceiling at its minimum and so can never spread the first retry.
    assert.equal(backoffDelay(0, { random: () => 0 }), 0);
    assert.equal(backoffDelay(3, { random: () => 0 }), 0);
    // The whole range is reachable, not just the top half.
    const lows = [];
    for (let i = 0; i < 1000; i++) lows.push(backoffDelay(4, { random: Math.random }));
    assert.ok(lows.some((d) => d < 16000 * 0.5), 'the sub-half range must be reachable');
    assert.ok(Math.max(...lows) <= 16000);
  });
});

describe('reconnect:false — §5.2, §8.4', () => {
  test('stops permanently and surfaces the reason', async () => {
    const h = harness();
    const sock = await handshake(h);
    sock.emit({ disconnect: { code: 3501, reason: 'revoked', reconnect: false } });
    sock.serverClose(3501, 'revoked');

    assert.equal(h.st.state, 'closed');
    assert.equal(h.clock.pending, 0, 'no retry may be scheduled');
    const [, info] = h.states.filter(([s]) => s === 'closed').pop();
    assert.equal(info.permanent, true);
    assert.equal(info.reconnect, false);
    assert.equal(info.code, 3501);
    assert.equal(info.reason, 'revoked');
    assert.equal(h.t.sockets.length, 1, 'no further sockets are ever opened');
  });

  test('stays stopped — later commands reject and no timer ever appears', async () => {
    const h = harness();
    const sock = await handshake(h);
    sock.emit({ disconnect: { code: 3003, reason: 'unauthorized', reconnect: false } });
    sock.serverClose(3003, 'unauthorized');
    await assert.rejects(h.st.subscribe('room-4410', () => {}), (e) => e.code === 'closed');
    await assert.rejects(h.st.sync(), (e) => e.code === 'closed');
    h.clock.advance(600000);
    assert.equal(h.t.sockets.length, 1);
  });

  test('reconnect:true on a normally non-retryable code wins — the frame is authoritative', async () => {
    const h = harness();
    const sock = await handshake(h);
    sock.emit({ disconnect: { code: 3501, reason: 'x', reconnect: true, retry_after: 500 } });
    sock.serverClose(3501, 'x');
    assert.deepEqual(h.clock.scheduled, [500]);
  });
});

describe('registry replay — §8.3', () => {
  test('every held channel is replayed in the next connect frame', async () => {
    const h = harness();
    let sock = await handshake(h);
    for (const [id, ch] of [[2, 'room-4410'], [3, 'user-7']]) {
      const p = h.st.subscribe(ch, () => {});
      sock.emit({ id, subscribe: {} });
      await p;
    }
    assert.deepEqual(h.st.channels(), ['room-4410', 'user-7']);

    sock.serverClose(3005, 'slow consumer');
    h.clock.fireNext();
    sock = h.sock();
    sock.accept();
    assert.deepEqual(sock.sent[0], { id: 1, connect: { subs: ['room-4410', 'user-7'] } });
  });

  test('the reconnect hook fires with the granted set so the app can run ?since= (§8.6)', async () => {
    const h = harness();
    let sock = await handshake(h, { subs: {} });
    const p = h.st.subscribe('room-4410', () => {});
    sock.emit({ id: 2, subscribe: {} });
    await p;
    assert.deepEqual(h.reconnects, [], 'not called for the first connection');

    sock.serverClose(3000, 'draining');
    h.clock.fireNext();
    sock = h.sock();
    sock.accept();
    sock.emit({ id: 1, connect: { client: 'c2', subs: { 'room-4410': {} } } });
    await tick();
    assert.equal(h.reconnects.length, 1);
    assert.deepEqual(h.reconnects[0].channels, ['room-4410']);
    assert.deepEqual(h.reconnects[0].denied, []);
    assert.equal(h.reconnects[0].connections, 2);
    assert.equal(h.st.stats.reconnects, 1);
  });

  test('a channel the gateway no longer grants is dropped and reported as denied', async () => {
    const h = harness();
    let sock = await handshake(h);
    const p = h.st.subscribe('org-42-alerts', () => {});
    sock.emit({ id: 2, subscribe: {} });
    await p;

    sock.serverClose(3503, 'expired');
    h.clock.fireNext();
    sock = h.sock();
    sock.accept();
    sock.emit({ id: 1, connect: { client: 'c2', subs: {} } });   // grant gone
    await tick();
    assert.deepEqual(h.reconnects[0].denied, ['org-42-alerts']);
    assert.deepEqual(h.st.channels(), [], 'not replayed forever');
  });

  test('in-flight commands reject when the connection dies', async () => {
    const h = harness();
    const sock = await handshake(h);
    const p = swallow(h.st.sync());
    sock.serverClose(3004, '');
    await assert.rejects(p, (e) => e.code === 'disconnected');
  });

  test('a subscribe still pending at the drop is replayed rather than rejected', async () => {
    const h = harness();
    let sock = await handshake(h);
    const p = h.st.subscribe('room-4410', () => {});
    sock.serverClose(3004, '');
    h.clock.fireNext();
    sock = h.sock();
    sock.accept();
    assert.deepEqual(sock.sent[0].connect.subs, ['room-4410']);
    sock.emit({ id: 1, connect: { client: 'c2', subs: { 'room-4410': {} } } });
    assert.equal(await p, true);
  });

  test('an unsubscribe left unacknowledged is not replayed', async () => {
    const h = harness();
    let sock = await handshake(h);
    const s = h.st.subscribe('room-4410', () => {});
    sock.emit({ id: 2, subscribe: {} });
    await s;
    const u = swallow(h.st.unsubscribe('room-4410'));
    sock.serverClose(3004, '');                 // reply never arrived
    await assert.rejects(u, (e) => e.code === 'disconnected');
    h.clock.fireNext();
    sock = h.sock();
    sock.accept();
    assert.deepEqual(sock.sent[0], { id: 1, connect: {} });
  });
});

describe('unsubscribed push — §8.5', () => {
  test('removes the channel from the registry and never resubscribes it', async () => {
    const h = harness();
    let sock = await handshake(h);
    const p = h.st.subscribe('org-42-alerts', () => {});
    sock.emit({ id: 2, subscribe: {} });
    await p;

    sock.emit({ push: { channel: 'org-42-alerts', unsubscribed: { reason: 'grant revoked' } } });
    assert.deepEqual(h.st.channels(), []);
    assert.deepEqual(h.events.pop(), {
      channel: 'org-42-alerts', unsubscribed: { reason: 'grant revoked' }, orphan: false,
    });

    sock.serverClose(3004, '');
    h.clock.fireNext();
    sock = h.sock();
    sock.accept();
    assert.deepEqual(sock.sent[0], { id: 1, connect: {} }, 'a revoked channel would 103 forever');
  });

  test('an unsubscribed push for a channel never held is reported, not fatal', async () => {
    const h = harness();
    const sock = await handshake(h);
    sock.emit({ push: { channel: 'ghost', unsubscribed: { reason: 'x' } } });
    assert.equal(h.events[0].orphan, true);
    assert.equal(h.st.state, 'open');
  });

  test('it settles a subscribe that was still in flight', async () => {
    const h = harness();
    const sock = await handshake(h);
    const p = swallow(h.st.subscribe('org-42-alerts', () => {}));
    sock.emit({ push: { channel: 'org-42-alerts', unsubscribed: { reason: 'grant revoked' } } });
    await assert.rejects(p, (e) => e.code === 103);
  });
});

describe('out-of-order pushes — §5.1', () => {
  // §5.1 makes the ordering normative on the server. The client is defensive
  // anyway: a push for a channel it holds but has not had confirmed is
  // DELIVERED (dropping it would open a gap only reconciliation could close),
  // and a push for a channel it does not hold at all is reported to onEvent
  // with orphan:true and counted, never dropped silently and never fatal.
  test('a push arriving before its subscribe reply is delivered', async () => {
    const h = harness();
    const sock = await handshake(h);
    const seen = [];
    const p = h.st.subscribe('room-4410', (pub) => seen.push(pub));
    sock.emit({ push: { channel: 'room-4410', pub: { event: 'early', data: 1 } } });
    assert.deepEqual(seen, [{ event: 'early', data: 1 }], 'the app asked for this channel');
    assert.equal(h.st.stats.orphanPushes, 0);
    sock.emit({ id: 2, subscribe: {} });
    assert.equal(await p, true);
  });

  test('a push arriving while an unsubscribe is in flight is still delivered', async () => {
    const h = harness();
    const sock = await handshake(h);
    const seen = [];
    const s = h.st.subscribe('room-4410', (pub) => seen.push(pub));
    sock.emit({ id: 2, subscribe: {} });
    await s;
    const u = h.st.unsubscribe('room-4410');
    sock.emit({ push: { channel: 'room-4410', pub: { event: 'late', data: 2 } } });
    assert.equal(seen.length, 1, 'handler detached at unsubscribe, message still surfaced');
    assert.equal(h.events.pop().pub.event, 'late');
    sock.emit({ id: 3, unsubscribe: {} });
    await u;
  });

  test('a push arriving after the unsubscribe reply is counted as orphan, never silent', async () => {
    const h = harness();
    const sock = await handshake(h);
    const s = h.st.subscribe('room-4410', () => {});
    sock.emit({ id: 2, subscribe: {} });
    await s;
    const u = h.st.unsubscribe('room-4410');
    sock.emit({ id: 3, unsubscribe: {} });
    await u;

    sock.emit({ push: { channel: 'room-4410', pub: { event: 'stale', data: 3 } } });
    assert.equal(h.st.stats.orphanPushes, 1);
    assert.deepEqual(h.events.pop(), { channel: 'room-4410', pub: { event: 'stale', data: 3 }, orphan: true });
    assert.equal(h.st.state, 'open', 'a spec violation must not close the connection');
  });
});

describe('robustness', () => {
  test('malformed frames do not throw and do not break the connection', async () => {
    const h = harness();
    const sock = await handshake(h);
    const junk = [
      'not json at all', '', '[]', 'null', '"string"', '42',
      '{}', '{"push":{}}', '{"push":{"channel":"a"}}', '{"push":"nope"}',
      '{"push":{"channel":123,"pub":{}}}', '{"id":"two","sync":{}}',
      '{"id":99,"sync":{"channels":[]}}', '{"nonsense":true}',
      '{"disconnect":"soon"}', '{"pong":{}}',
    ];
    for (const raw of junk) assert.doesNotThrow(() => sock.emit(raw), `frame: ${raw}`);
    assert.ok(h.st.stats.malformed > 0);
    assert.equal(h.st.state, 'open');

    const p = h.st.sync();                       // still fully functional
    sock.emit({ id: sock.last.id, sync: { channels: ['room-4410'] } });
    assert.deepEqual(await p, ['room-4410']);
  });

  test('an exception from an application handler is routed to onError, not thrown', async () => {
    const h = harness();
    const sock = await handshake(h);
    const p = h.st.subscribe('room-4410', () => { throw new Error('boom'); });
    sock.emit({ id: 2, subscribe: {} });
    await p;
    assert.doesNotThrow(() => sock.emit({ push: { channel: 'room-4410', pub: { event: 'e', data: 1 } } }));
    assert.equal(h.errors[0].message, 'boom');
    assert.equal(h.st.state, 'open');
  });

  test('a websocket constructor that throws schedules a retry instead of propagating', () => {
    const clock = new FakeClock();
    const errors = [];
    class Broken { constructor() { throw new Error('no transport'); } }
    assert.doesNotThrow(() => connect({
      url: 'ws://x/ws', WebSocket: Broken, setTimeout: clock.setTimeout,
      clearTimeout: clock.clearTimeout, random: () => 0.5, onError: (e) => errors.push(e),
    }));
    assert.equal(errors[0].message, 'no transport');
    assert.equal(clock.pending, 1);
  });

  test('an onerror event reaches onError without closing anything', async () => {
    const h = harness();
    const sock = await handshake(h);
    sock.onerror({ type: 'error' });
    assert.equal(h.errors.length, 1);
    assert.equal(h.st.state, 'open');
  });
});

describe('close', () => {
  test('is idempotent and emits exactly one closed transition', async () => {
    const h = harness();
    const sock = await handshake(h);
    h.st.close();
    h.st.close();
    h.st.close();
    assert.equal(h.st.state, 'closed');
    assert.equal(h.states.filter(([s]) => s === 'closed').length, 1);
    assert.equal(sock.closeArgs.code, 1000);
  });

  test('closing during backoff cancels the pending retry', async () => {
    const h = harness();
    const sock = await handshake(h);
    sock.serverClose(3004, '');
    assert.equal(h.clock.pending, 1);
    h.st.close();
    assert.equal(h.clock.pending, 0);
    h.clock.advance(600000);
    assert.equal(h.t.sockets.length, 1);
    assert.equal(h.st.state, 'closed');
  });

  test('closing before the socket opens is safe', () => {
    const h = harness();
    assert.doesNotThrow(() => h.st.close());
    assert.equal(h.st.state, 'closed');
  });

  test('a socket that throws on close does not propagate', async () => {
    const h = harness();
    const sock = await handshake(h);
    sock.close = () => { throw new Error('already gone'); };
    assert.doesNotThrow(() => h.st.close());
    assert.equal(h.st.state, 'closed');
  });
});

describe('url resolution', () => {
  test('absolute websocket urls pass through', () => {
    assert.equal(resolveUrl('wss://app.example.com/ws'), 'wss://app.example.com/ws');
    assert.equal(resolveUrl('ws://app.example.com/ws'), 'ws://app.example.com/ws');
  });

  test('with no location a relative path is left alone', () => {
    assert.equal(resolveUrl('/ws'), '/ws');
  });

  test('a relative path resolves against location and swaps the scheme', () => {
    const saved = Object.getOwnPropertyDescriptor(globalThis, 'location');
    globalThis.location = { href: 'https://app.example.com/dashboard' };
    try {
      assert.equal(resolveUrl('/ws'), 'wss://app.example.com/ws');
      globalThis.location = { href: 'http://localhost:5000/x' };
      assert.equal(resolveUrl('/ws'), 'ws://localhost:5000/ws');
    } finally {
      if (saved) Object.defineProperty(globalThis, 'location', saved);
      else delete globalThis.location;
    }
  });
});

describe('umd variant', () => {
  test('the committed bundle matches the module it is generated from', () => {
    const esm = readFileSync(join(HERE, 'sidecartunnel.js'), 'utf8');
    const committed = readFileSync(join(HERE, 'sidecartunnel.umd.js'), 'utf8');
    assert.equal(committed, toUmd(esm), 'run `node build-umd.mjs`');
  });

  test('a plain <script> tag gets window.sidecartunnel', () => {
    const src = readFileSync(join(HERE, 'sidecartunnel.umd.js'), 'utf8');
    const sandbox = {};
    sandbox.self = sandbox;
    vm.createContext(sandbox);
    vm.runInContext(src, sandbox);
    assert.equal(typeof sandbox.sidecartunnel.connect, 'function');
    assert.equal(typeof sandbox.sidecartunnel.backoffDelay, 'function');
    assert.equal(typeof sandbox.sidecartunnel.resolveUrl, 'function');
    assert.equal(typeof sandbox.sidecartunnel.StError, 'function');
    assert.equal(sandbox.sidecartunnel.backoffDelay(0, { random: () => 0.5 }), 500);
  });
});
