"""Reference application for sidecartunnel.

A minimal rooms-and-messages application carrying every application-side surface the
gateway requires, and nothing else:

    POST /_st/connect                       the connect webhook          docs/04 §1
    POST /api/rooms/<id>/messages           persist, then publish        docs/04 §2
    GET  /api/rooms/<id>/messages?since=    reconciliation               docs/07 §2
    POST /admin/users/<id>/suspend          revocation over the control channel

The normative contracts are docs/04-integration.md, docs/03-client-protocol.md and
docs/08-config.md. The guide that walks through this file is docs/16-integration-guide.md.
test_app.py is the executable form of the parts that are easy to get wrong.

Storage is stdlib sqlite3 so the example runs with three dependencies. Nothing here
depends on that choice.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import pathlib
import sqlite3
import time
import uuid

from flask import (
    Flask,
    abort,
    current_app,
    g,
    jsonify,
    redirect,
    render_template,
    request,
    send_from_directory,
    session,
    url_for,
)

REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]

# The browser client, served at the path templates/index.html imports it from. A real
# deployment copies it into the application's static directory instead; this route exists
# so the example runs from a checkout with nothing copied.
CLIENT_JS_DIR = REPO_ROOT / "client" / "js"

# docs/04-integration.md §1.1. Both directions: a clock ahead is as much a signal as one
# behind.
TIMESTAMP_WINDOW_SECONDS = 300

# docs/07-delivery.md §2. A client asleep for four hours must not be able to ask for every
# row ever written.
PAGE_LIMIT_DEFAULT = 200
PAGE_LIMIT_MAX = 500


# --------------------------------------------------------------------------------------
# Storage
# --------------------------------------------------------------------------------------

SCHEMA = """
CREATE TABLE IF NOT EXISTS users (
    id      INTEGER PRIMARY KEY,
    name    TEXT    NOT NULL,
    active  INTEGER NOT NULL DEFAULT 1
);
CREATE TABLE IF NOT EXISTS orgs (
    id      INTEGER PRIMARY KEY,
    name    TEXT    NOT NULL
);
CREATE TABLE IF NOT EXISTS org_members (
    user_id INTEGER NOT NULL,
    org_id  INTEGER NOT NULL,
    PRIMARY KEY (user_id, org_id)
);
CREATE TABLE IF NOT EXISTS rooms (
    id          INTEGER PRIMARY KEY,
    org_id      INTEGER NOT NULL,
    name        TEXT    NOT NULL,
    archived_at TEXT
);
CREATE TABLE IF NOT EXISTS room_members (
    user_id INTEGER NOT NULL,
    room_id INTEGER NOT NULL,
    PRIMARY KEY (user_id, room_id)
);
CREATE TABLE IF NOT EXISTS messages (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    room_id    INTEGER NOT NULL,
    user_id    INTEGER NOT NULL,
    body       TEXT    NOT NULL,
    created_at TEXT    NOT NULL
);
CREATE INDEX IF NOT EXISTS messages_room_id ON messages (room_id, id);
"""

# Room 4412 is archived and room 4413 belongs to an org nobody in the seed is a member of.
# Both exist so test_grants_agree_with_route_guard has something to fail against.
SEED = [
    ("INSERT OR IGNORE INTO users (id, name, active) VALUES (?, ?, ?)",
     [(7, "Ada", 1), (8, "Grace", 1), (9, "Suspended Sam", 0)]),
    ("INSERT OR IGNORE INTO orgs (id, name) VALUES (?, ?)",
     [(42, "Acme"), (421, "Acme Holdings")]),
    ("INSERT OR IGNORE INTO org_members (user_id, org_id) VALUES (?, ?)",
     [(7, 42), (8, 42)]),
    ("INSERT OR IGNORE INTO rooms (id, org_id, name, archived_at) VALUES (?, ?, ?, ?)",
     [(4410, 42, "General", None),
      (4411, 42, "Deploys", None),
      (4412, 42, "Archived", "2026-01-01T00:00:00Z"),
      (4413, 421, "Holdings Only", None)]),
    ("INSERT OR IGNORE INTO room_members (user_id, room_id) VALUES (?, ?)",
     [(7, 4410), (7, 4411), (7, 4412), (8, 4410), (8, 4413), (9, 4410)]),
]


def db() -> sqlite3.Connection:
    """The request-scoped connection.

    Any failure here raises, which becomes a 500. That is the point: a database that
    cannot answer is not a decision about the user, so it must never become a 401.
    See docs/16-integration-guide.md §3.
    """
    if "db" not in g:
        g.db = sqlite3.connect(current_app.config["DATABASE"])
        g.db.row_factory = sqlite3.Row
        g.db.execute("PRAGMA foreign_keys = ON")
    return g.db


def close_db(_exception=None) -> None:
    conn = g.pop("db", None)
    if conn is not None:
        conn.close()


def init_db(app: Flask) -> None:
    conn = sqlite3.connect(app.config["DATABASE"])
    try:
        conn.executescript(SCHEMA)
        for statement, rows in SEED:
            conn.executemany(statement, rows)
        conn.commit()
    finally:
        conn.close()


# --------------------------------------------------------------------------------------
# Authorization — one predicate per boundary, reused by routes and by grants
# --------------------------------------------------------------------------------------
#
# docs/16-integration-guide.md §5. Every grant is produced by the same function that
# guards the corresponding HTTP route. A second rule set written for realtime is a copy
# of an authorization policy, and copies drift silently.


def readable_room_ids(user_id: int) -> list[int]:
    """Rooms this user may read. The single source of truth for room access."""
    rows = db().execute(
        "SELECT r.id AS id "
        "  FROM rooms r "
        "  JOIN room_members m ON m.room_id = r.id "
        " WHERE m.user_id = ? AND r.archived_at IS NULL "
        " ORDER BY r.id",
        (user_id,),
    ).fetchall()
    return [row["id"] for row in rows]


def can_read_room(user_id: int, room_id: int) -> bool:
    """Guards GET and POST on a room. Defined in terms of the enumeration above, so the
    predicate and the grant list cannot disagree — there is only one of them."""
    return room_id in readable_room_ids(user_id)


def org_ids(user_id: int) -> list[int]:
    rows = db().execute(
        "SELECT org_id FROM org_members WHERE user_id = ? ORDER BY org_id", (user_id,)
    ).fetchall()
    return [row["org_id"] for row in rows]


def grants_for(user_id: int) -> list[str]:
    """The grant list for one user. Strings, and nothing else.

    Channel scheme (docs/06-channels.md §2, docs/16-integration-guide.md §4):

        user-{id}          the user's own notification stream
        user-{id}-*        the user's private feeds
        room-{id}          one room, enumerated — never room-*
        org-{id}-*         one org, everything under it
        status             the public banner, granted to everyone

    Note both user entries. A trailing * after a separator matches user-7-billing but
    NOT the bare user-7, so a single grant would be wrong either way. And note that the
    wildcard is anchored past the identifier: org-42-* cannot reach org-421-alerts,
    because the separator sits immediately after every identifier.

    What must never appear here: user-*, room-*, org-*, or *. Each of those grants every
    other user's channels, and the gateway will honour it exactly as written.
    """
    grants = [f"user-{user_id}", f"user-{user_id}-*", "status"]
    grants += [f"room-{room_id}" for room_id in readable_room_ids(user_id)]
    grants += [f"org-{org_id}-*" for org_id in org_ids(user_id)]
    return grants


def resolve_session():
    """The current user row, or None.

    None means refusal — no session, an unknown user, or a suspended one. That is the
    only path in this file permitted to produce a 401. Anything else that goes wrong
    raises and becomes a 5xx.
    """
    user_id = session.get("user_id")
    if user_id is None:
        return None
    row = db().execute(
        "SELECT id, name, active FROM users WHERE id = ?", (user_id,)
    ).fetchone()
    if row is None or not row["active"]:
        return None
    return row


def current_user():
    user = resolve_session()
    if user is None:
        abort(401)
    return user


# --------------------------------------------------------------------------------------
# Publishing
# --------------------------------------------------------------------------------------


class RedisPublisher:
    """Publishes to Redis. Constructed lazily so the app starts without Redis running."""

    def __init__(self, url: str) -> None:
        self._url = url
        self._client = None

    def _redis(self):
        if self._client is None:
            import redis  # imported here so the tests need no Redis at all

            self._client = redis.from_url(self._url)
        return self._client

    def publish(self, key: str, payload: str) -> None:
        self._redis().publish(key, payload)


class NullPublisher:
    """Records instead of publishing. Used by the tests and by `flask run` with no Redis."""

    def __init__(self) -> None:
        self.published: list[tuple[str, str]] = []

    def publish(self, key: str, payload: str) -> None:
        self.published.append((key, payload))


def publish(channel: str, event: str, data, exclude: str | None = None,
            message_id: str | None = None) -> None:
    """One publish. docs/04-integration.md §2.2.

    Envelope: event and data are required; exclude and id are optional; `from` is set by
    the gateway on client events and is never set here.

    Publishing gives no error channel. Redis PUBLISH returns a subscriber count that
    reflects gateway replicas, not end clients, so a typo'd channel name is silent
    forever. The gateway logs `subscribe`/`unsubscribe` at INFO with the client id and
    the channel, so whether anyone is listening is a grep of that log, not an API call:

        docker compose logs sidecartunnel | grep '"msg":"subscribe"' | grep room-4410

    A Redis failure is swallowed. The write it accompanies is already committed, so
    failing the request would report an error for something that succeeded, and the
    client's ?since= reconciliation closes the gap on its next reconnect anyway.
    """
    envelope = {"event": event, "data": data}
    if exclude:
        envelope["exclude"] = exclude
    if message_id:
        envelope["id"] = message_id

    key = current_app.config["ST_BUS_PREFIX"] + channel
    try:
        current_app.config["ST_PUBLISHER"].publish(key, json.dumps(envelope))
    except Exception:  # pragma: no cover - depends on a live Redis
        current_app.logger.exception("st publish failed key=%s event=%s", key, event)


def control_disconnect(user: str, reason: str) -> None:
    """Close every connection for one user, on every replica, within a second.

    docs/04-integration.md §3. Control messages are signed with control.secret and
    carry a timestamp; unsigned or stale ones are dropped. The target is matched
    exactly, never as a glob, and exactly one of user or client must be named.

    OPEN POINT — confirm against the gateway before relying on this.

        docs/04-integration.md §3 gives the signed input as `ts.nonce.body` and shows an
        envelope carrying ts, nonce and sig alongside the action fields in one flat
        object. It does not define `body` at the byte level, and JSON object
        serialization is not canonical, so a receiver cannot recover the signed bytes
        from the envelope without a stated rule.

        This implementation signs the canonical JSON of the action fields alone — sorted
        keys, no whitespace — and merges ts, nonce and sig into the published object. It
        is one defensible reading of the spec, not a settled one. The gateway is
        unimplemented (M2), so nothing verifies it yet.

        There is no fallback with less ambiguity: POST /disconnect on the admin API
        covered the same action over a bearer-token call, but the admin API is gone,
        so this control message is the only door onto revocation now.
    """
    action = {"action": "disconnect", "user": user, "reason": reason}
    body = json.dumps(action, sort_keys=True, separators=(",", ":"))
    ts = int(time.time())
    nonce = uuid.uuid4().hex
    sig = hmac.new(
        current_app.config["ST_CONTROL_SECRET"],
        f"{ts}.{nonce}.{body}".encode(),
        hashlib.sha256,
    ).hexdigest()

    envelope = dict(action, ts=ts, nonce=nonce, sig=sig)
    key = current_app.config["ST_BUS_PREFIX"] + "_control"
    try:
        current_app.config["ST_PUBLISHER"].publish(key, json.dumps(envelope))
    except Exception:  # pragma: no cover - depends on a live Redis
        current_app.logger.exception("st control publish failed user=%s", user)


# --------------------------------------------------------------------------------------
# The connect webhook
# --------------------------------------------------------------------------------------


def verify_webhook_signature(ts: str, nonce: str, cookie: str, body: bytes,
                             provided: str) -> bool:
    """docs/04-integration.md §1.1.

        HMAC-SHA256(secret, ts + "." + nonce + "." + sha256(cookie) + "." + sha256(body))

    Both inner digests are lowercase hex, and so is the result.

    Every configured secret is tried, with a constant-time comparison, and the loop does
    not short-circuit. app.webhook_secrets is a list so a secret can be rotated without
    restarting the gateway and the application at the same instant.

    A signature header containing non-ASCII bytes makes hmac.compare_digest raise. That
    is attacker-controlled input, so it must be a refusal, not an unhandled 500.
    """
    cookie_digest = hashlib.sha256(cookie.encode("utf-8", "surrogateescape")).hexdigest()
    body_digest = hashlib.sha256(body).hexdigest()
    signed = f"{ts}.{nonce}.{cookie_digest}.{body_digest}".encode()

    matched = False
    for secret in current_app.config["ST_WEBHOOK_SECRETS"]:
        expected = hmac.new(secret, signed, hashlib.sha256).hexdigest()
        try:
            matched |= hmac.compare_digest(expected, provided)
        except TypeError:
            return False
    return matched


def nonce_replayed(nonce: str) -> bool:
    """Optional exactly-once, per docs/04-integration.md §1.1.

    The ±300s window is a replay window: a captured request can be replayed verbatim
    inside it. That is accepted because the endpoint is read-only and idempotent. An
    application that wants exactly-once caches seen nonces for 300 seconds.

    The store must be shared by every worker process. A process-local dict under four
    Gunicorn workers catches one replay in four, which is worse than not claiming the
    property at all.
    """
    store = current_app.config.get("ST_NONCE_STORE")
    if store is None:
        return False
    if not nonce:
        return True
    ttl = current_app.config["ST_TIMESTAMP_WINDOW"]
    return not store.set(f"{current_app.config['ST_BUS_PREFIX']}nonce:{nonce}", "1",
                         nx=True, ex=ttl)


def register_routes(app: Flask) -> None:

    @app.post("/_st/connect")
    def st_connect():
        """The single integration point. docs/04-integration.md §1.

        Verification order is part of the contract. Everything below step 1 is
        attacker-controlled until the HMAC has been checked, and parsing it first turns
        a malformed header into an unauthenticated 500 — which the gateway classifies as
        transient, retries, and closes with 3008 reconnect:true, so the client retries
        too. One bad header becomes a retry loop against the worker pool.
        """
        # 1. Read. No parsing, no int(), no json.loads().
        ts = request.headers.get("X-St-Timestamp", "")
        nonce = request.headers.get("X-St-Nonce", "")
        sig = request.headers.get("X-St-Signature", "")
        cookie = request.headers.get("Cookie", "")
        body = request.get_data()

        # 2. Authenticate.
        if not verify_webhook_signature(ts, nonce, cookie, body, sig):
            abort(403)

        # 3. Now the timestamp may be parsed. A malformed one is a refusal, not a crash.
        #
        #    The comparison is integer arithmetic on purpose. The sample in
        #    docs/04-integration.md §1.4 writes `abs(time.time() - int(ts))`, which
        #    raises OverflowError — not ValueError — on a 400-digit timestamp, and an
        #    OverflowError here is exactly the unauthenticated 500 the ordering rule
        #    exists to prevent. Python integers do not overflow; floats do.
        try:
            skew = abs(int(time.time()) - int(ts))
        except (TypeError, ValueError):
            abort(403)
        if skew > current_app.config["ST_TIMESTAMP_WINDOW"]:
            abort(403)

        # 4. Optional replay rejection.
        if nonce_replayed(nonce):
            abort(403)

        # 5. The body carries {"client": "...", "channels_requested": [...]}. Both are
        #    informational; grants do not depend on what was asked for.
        payload = request.get_json(silent=True)
        if not isinstance(payload, dict):
            abort(403)

        # 6. Resolve the session. This is the only 401 in the file.
        #
        #    401 means "this user may not connect": the gateway closes 3003 with
        #    reconnect:false and the client stops asking. A 5xx means "could not answer":
        #    3008, reconnect:true, retried. Collapsing them either locks every user out
        #    during a deploy or turns a revocation into an infinite retry loop.
        #    docs/04-integration.md §1.3, docs/16-integration-guide.md §3.
        user = resolve_session()
        if user is None:
            abort(401)

        # 7. Answer. expires_in is a reconnect interval, not a session lifetime: every
        #    expiry costs a full re-handshake per connection. At 6h and 20,000
        #    connections that is roughly one webhook call per second.
        return jsonify(
            user=f"u-{user['id']}",
            channels=grants_for(user["id"]),
            expires_in=current_app.config["ST_EXPIRES_IN"],
        )

    # ----------------------------------------------------------------------------------
    # Durable writes. The socket is receive-only for anything that matters, so this is an
    # ordinary HTTP route and CSRF, rate limiting, validation and request logging apply
    # unchanged. docs/02-architecture.md flow 3.
    # ----------------------------------------------------------------------------------

    @app.post("/api/rooms/<int:room_id>/messages")
    def create_message(room_id: int):
        user = current_user()
        if not can_read_room(user["id"], room_id):   # the same predicate as the grant
            abort(403)

        payload = request.get_json(silent=True) or {}
        text = (payload.get("body") or "").strip()
        if not text:
            abort(400)

        conn = db()
        cursor = conn.execute(
            "INSERT INTO messages (room_id, user_id, body, created_at) VALUES (?, ?, ?, ?)",
            (room_id, user["id"], text,
             time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())),
        )
        conn.commit()           # commit BEFORE publishing

        message = {"id": cursor.lastrowid, "room_id": room_id, "user_id": user["id"],
                   "user_name": user["name"], "body": text}

        # Publish after commit. A client that receives this and immediately reconciles
        # with ?since= must be able to read the row it was told about.
        #
        # exclude suppresses the echo to the tab that caused the write. The client id
        # comes from the browser's connect reply, which the application never sees, so
        # the browser sends it as X-St-Client. Each tab has its own id, which is the
        # point: the user's other tabs still receive the event.
        publish(f"room-{room_id}", "message.created", message,
                exclude=request.headers.get("X-St-Client"),
                message_id=str(message["id"]))

        return jsonify(message), 201

    # ----------------------------------------------------------------------------------
    # Reconciliation. Mandatory, not optional. docs/07-delivery.md §2.
    # ----------------------------------------------------------------------------------

    @app.get("/api/rooms/<int:room_id>/messages")
    def list_messages(room_id: int):
        """Delivery is at-most-once. A replica restart, a partition or a sleeping laptop
        drops messages, and nothing keeps them. This endpoint is how a client closes its
        own gap after every reconnect.

        The cursor is the autoincrement id: monotonic and total. A timestamp cursor loses
        rows — two inserts in one millisecond, or a clock adjustment, and since=<ts>
        skips one silently, which is the exact failure this endpoint exists to prevent.

        Authorization is the same predicate as every other read of this room.
        """
        user = current_user()
        if not can_read_room(user["id"], room_id):
            abort(403)

        since = request.args.get("since", type=int)
        limit = request.args.get("limit", type=int) or PAGE_LIMIT_DEFAULT
        limit = max(1, min(limit, PAGE_LIMIT_MAX))

        if since is None:
            # Cold start is "the last page", never "everything since the beginning".
            rows = db().execute(
                "SELECT m.id, m.room_id, m.user_id, m.body, u.name AS user_name "
                "  FROM messages m JOIN users u ON u.id = m.user_id "
                " WHERE m.room_id = ? ORDER BY m.id DESC LIMIT ?",
                (room_id, limit),
            ).fetchall()
            rows = list(reversed(rows))
            has_more = False
        else:
            rows = db().execute(
                "SELECT m.id, m.room_id, m.user_id, m.body, u.name AS user_name "
                "  FROM messages m JOIN users u ON u.id = m.user_id "
                " WHERE m.room_id = ? AND m.id > ? ORDER BY m.id ASC LIMIT ?",
                (room_id, since, limit + 1),
            ).fetchall()
            has_more = len(rows) > limit
            rows = rows[:limit]

        messages = [dict(row) for row in rows]
        latest_id = messages[-1]["id"] if messages else (since or 0)
        return jsonify(messages=messages, latest_id=latest_id, has_more=has_more)

    # ----------------------------------------------------------------------------------
    # Revocation
    # ----------------------------------------------------------------------------------

    @app.post("/admin/users/<int:user_id>/suspend")
    def suspend_user(user_id: int):
        """Revoke, then tell every replica.

        Without the control publish, an open connection keeps its grants until
        expires_in, which defaults to a maximum of six hours. With it, every connection
        for the user closes with 3501 reconnect:false in under a second, on every
        replica, with no service discovery and no credential.

        The demo has no admin role. A real application guards this route with whatever
        already guards its other destructive admin actions.
        """
        conn = db()
        conn.execute("UPDATE users SET active = 0 WHERE id = ?", (user_id,))
        conn.commit()
        control_disconnect(f"u-{user_id}", "account suspended")
        return jsonify(user=f"u-{user_id}", active=False)

    # ----------------------------------------------------------------------------------
    # Demo scaffolding. None of this is part of the integration.
    # ----------------------------------------------------------------------------------

    @app.get("/")
    def index():
        user = resolve_session()
        if user is None:
            return redirect(url_for("login", user_id=7))

        rooms = []
        for rid in readable_room_ids(user["id"]):
            row = db().execute(
                "SELECT id, name FROM rooms WHERE id = ?", (rid,)
            ).fetchone()
            rooms.append(dict(row))
        room_id = request.args.get("room", type=int) or (rooms[0]["id"] if rooms else 0)

        return render_template(
            "index.html",
            user=dict(user),
            rooms=rooms,
            room_id=room_id,
            grants=grants_for(user["id"]),
            ws_url=current_app.config["ST_WS_URL"],
        )

    @app.get("/login/<int:user_id>")
    def login(user_id: int):
        session["user_id"] = user_id
        return redirect(url_for("index"))

    @app.get("/logout")
    def logout():
        session.clear()
        return redirect(url_for("login", user_id=7))

    @app.get("/client/js/<path:filename>")
    def client_js(filename: str):
        """Serves client/js/ from the checkout so the page can import the library.

        Demo scaffolding. A real application copies sidecartunnel.js into its own static
        directory and serves it the way it serves any other asset.
        """
        if not (CLIENT_JS_DIR / filename).is_file():
            abort(404)
        return send_from_directory(CLIENT_JS_DIR, filename,
                                   mimetype="text/javascript")


# --------------------------------------------------------------------------------------
# Application factory
# --------------------------------------------------------------------------------------


def _secrets_from_env(name: str) -> list[bytes]:
    raw = os.environ.get(name, "")
    return [s.strip().encode() for s in raw.split(",") if s.strip()]


def create_app(overrides: dict | None = None) -> Flask:
    app = Flask(__name__)

    webhook_secrets = _secrets_from_env("ST_WEBHOOK_SECRETS")
    control_secret = os.environ.get("ST_CONTROL_SECRET", "").encode()

    app.config.update(
        SECRET_KEY=os.environ.get("FLASK_SECRET_KEY", "dev-only-not-a-real-secret"),
        DATABASE=os.environ.get("DEMO_DATABASE", "/tmp/sidecartunnel-demo.db"),
        ST_WEBHOOK_SECRETS=webhook_secrets,
        ST_CONTROL_SECRET=control_secret,
        ST_BUS_PREFIX=os.environ.get("ST_BUS_PREFIX", "st:"),
        ST_TIMESTAMP_WINDOW=TIMESTAMP_WINDOW_SECONDS,
        ST_EXPIRES_IN=int(os.environ.get("ST_EXPIRES_IN", "21600")),
        ST_WS_URL=os.environ.get("ST_WS_URL", "/ws"),
        ST_PUBLISHER=None,
        ST_NONCE_STORE=None,
    )
    app.config.update(overrides or {})

    if app.config["ST_PUBLISHER"] is None:
        url = os.environ.get("REDIS_URL", "")
        app.config["ST_PUBLISHER"] = RedisPublisher(url) if url else NullPublisher()

    # Both secrets are required and both have a minimum length, matching the gateway's
    # own validation. Refusing to start is the only honest option: a shorter secret is a
    # security hole shipped as a convenience (docs/08-config.md §2).
    if not app.config["ST_WEBHOOK_SECRETS"]:
        raise RuntimeError("ST_WEBHOOK_SECRETS is empty — refusing to start")
    for secret in app.config["ST_WEBHOOK_SECRETS"]:
        if len(secret) < 32:
            raise RuntimeError("ST_WEBHOOK_SECRETS entries must be at least 32 bytes")
    if len(app.config["ST_CONTROL_SECRET"]) < 32:
        raise RuntimeError("ST_CONTROL_SECRET must be at least 32 bytes")

    app.teardown_appcontext(close_db)
    init_db(app)
    register_routes(app)
    return app


if __name__ == "__main__":  # pragma: no cover
    create_app().run(host="0.0.0.0", port=5000)
