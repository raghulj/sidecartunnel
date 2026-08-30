"""Executable form of the guidance in docs/16-integration-guide.md.

The webhook cases are the ones an integrator gets wrong, and each is a specific,
security-relevant failure rather than a style preference:

    bad signature        -> 403          an unsigned caller must learn nothing
    expired timestamp    -> 403          the +/-300s window is enforced in both directions
    malformed timestamp  -> 403, NOT 500 a 500 is retried by the gateway and by the client
    missing session      -> 401, NOT 500 a 5xx here is an infinite retry loop
    backend failure      -> 5xx, NOT 401 a 401 here locks every user out permanently

Run with: pytest -q
"""

from __future__ import annotations

import hashlib
import hmac
import json
import re
import sqlite3
import time

import pytest
from flask.sessions import SecureCookieSessionInterface

from app import (
    can_read_room,
    control_disconnect,
    create_app,
    grants_for,
    NullPublisher,
    readable_room_ids,
)

# Obvious placeholders. Real secrets are >= 32 bytes from a CSPRNG and never live in a
# repository; see docs/17-production-readiness.md §3.
WEBHOOK_SECRET = b"placeholder-webhook-secret-not-real-0001"
WEBHOOK_SECRET_ROTATED = b"placeholder-webhook-secret-not-real-0002"
CONTROL_SECRET = b"placeholder-control-secret-not-real-0001"


# --------------------------------------------------------------------------------------
# Fixtures and helpers
# --------------------------------------------------------------------------------------


@pytest.fixture
def publisher():
    return NullPublisher()


@pytest.fixture
def app(tmp_path, publisher):
    return create_app({
        "DATABASE": str(tmp_path / "demo.db"),
        "SECRET_KEY": "placeholder-flask-session-key-not-real",
        "ST_WEBHOOK_SECRETS": [WEBHOOK_SECRET],
        "ST_CONTROL_SECRET": CONTROL_SECRET,
        "ST_PUBLISHER": publisher,
        "ST_BUS_PREFIX": "st:",
    })


@pytest.fixture
def client(app):
    """For ordinary browser traffic. Keeps a cookie jar, so session_transaction works."""
    return app.test_client()


@pytest.fixture
def hook(app):
    """For the webhook.

    The gateway is not a browser: it forwards a Cookie header it was handed and keeps no
    jar of its own. Werkzeug's test client strips an explicit Cookie header when it is
    managing cookies itself, which would leave every signature computed over a header the
    application never sees, so cookie handling is turned off here.
    """
    return app.test_client(use_cookies=False)


def session_cookie(app, **data) -> str:
    """A real Flask session cookie, built the way the browser would receive one.

    The gateway forwards the Cookie header byte for byte, so the application's own
    session middleware resolves it with no special handling.
    """
    serializer = SecureCookieSessionInterface().get_signing_serializer(app)
    name = app.config["SESSION_COOKIE_NAME"]
    return f"{name}={serializer.dumps(dict(data))}"


def sign(secret: bytes, ts: str, nonce: str, cookie: str, body: bytes) -> str:
    cookie_digest = hashlib.sha256(cookie.encode()).hexdigest()
    body_digest = hashlib.sha256(body).hexdigest()
    return hmac.new(secret, f"{ts}.{nonce}.{cookie_digest}.{body_digest}".encode(),
                    hashlib.sha256).hexdigest()


def connect_headers(cookie: str, body: bytes, *, secret: bytes = WEBHOOK_SECRET,
                    ts: str | None = None, nonce: str = "01JTESTNONCE0000",
                    signature: str | None = None) -> dict:
    ts = str(int(time.time())) if ts is None else ts
    return {
        "Cookie": cookie,
        "Content-Type": "application/json",
        "X-St-Origin": "http://localhost",
        "X-St-Forwarded-For": "203.0.113.9",
        "X-St-User-Agent": "sidecartunnel-test",
        "X-St-Timestamp": ts,
        "X-St-Nonce": nonce,
        "X-St-Signature": signature or sign(secret, ts, nonce, cookie, body),
    }


BODY = json.dumps({"client": "8f2c1e04a7b3d915", "channels_requested": ["room-4410"]}).encode()


def glob_match(pattern: str, channel: str) -> bool:
    """The gateway's matcher, per docs/05-authorization.md §3.

    `*` matches any run of characters including none, and it is the only metacharacter.
    It does not stop at a separator, which is the whole of the user-* trap below.
    """
    return re.fullmatch(".*".join(re.escape(p) for p in pattern.split("*")),
                        channel) is not None


# --------------------------------------------------------------------------------------
# The webhook
# --------------------------------------------------------------------------------------


def test_valid_signature_returns_user_and_grants(app, hook):
    cookie = session_cookie(app, user_id=7)
    r = hook.post("/_st/connect", data=BODY,
                    headers=connect_headers(cookie, BODY))

    assert r.status_code == 200
    payload = r.get_json()
    assert payload["user"] == "u-7"
    assert payload["expires_in"] == 21600
    assert "room-4410" in payload["channels"]
    assert "user-7" in payload["channels"]


def test_bad_signature_is_403(app, hook):
    cookie = session_cookie(app, user_id=7)
    headers = connect_headers(cookie, BODY, signature="0" * 64)

    assert hook.post("/_st/connect", data=BODY, headers=headers).status_code == 403


def test_signature_over_a_different_cookie_is_403(app, hook):
    """The swap-the-cookie oracle, closed by binding sha256(Cookie) into the signature.

    Signing only timestamp + body would let anyone who captured one signed request replay
    those headers with a stolen cookie and be handed that victim's grant list.
    """
    victim = session_cookie(app, user_id=8)
    attacker_headers = connect_headers(session_cookie(app, user_id=7), BODY)
    attacker_headers["Cookie"] = victim

    assert hook.post("/_st/connect", data=BODY,
                       headers=attacker_headers).status_code == 403


def test_signature_over_a_different_body_is_403(app, hook):
    cookie = session_cookie(app, user_id=7)
    headers = connect_headers(cookie, BODY)

    assert hook.post("/_st/connect", data=b'{"client":"other"}',
                       headers=headers).status_code == 403


@pytest.mark.parametrize("offset", [-3600, -301, 301, 3600])
def test_timestamp_outside_the_window_is_403(app, hook, offset):
    """Both directions. A clock ahead is as much a signal as one behind."""
    cookie = session_cookie(app, user_id=7)
    ts = str(int(time.time()) + offset)

    assert hook.post("/_st/connect", data=BODY,
                       headers=connect_headers(cookie, BODY, ts=ts)).status_code == 403


@pytest.mark.parametrize("offset", [-299, 0, 299])
def test_timestamp_inside_the_window_is_accepted(app, hook, offset):
    cookie = session_cookie(app, user_id=7)
    ts = str(int(time.time()) + offset)

    assert hook.post("/_st/connect", data=BODY,
                       headers=connect_headers(cookie, BODY, ts=ts)).status_code == 200


@pytest.mark.parametrize("ts", ["", "not-a-number", "1756612800.5", "0x1", "1756612800abc",
                                "9" * 400, "-", "\u0669\u0669\u0669"])
def test_malformed_timestamp_is_403_not_500(app, hook, ts):
    """The whole reason the signature is verified first.

    These are all correctly signed: an attacker who has a valid signature still cannot
    reach int() with garbage, but a gateway bug or a truncated header can. int(ts) before
    the HMAC would raise here, and an unhandled exception is a 500 — which the gateway
    classifies as transient, retries up to app.webhook_retries, and closes with 3008
    reconnect:true, so the client retries too. One malformed header becomes an
    unauthenticated retry loop against the worker pool.
    """
    cookie = session_cookie(app, user_id=7)
    r = hook.post("/_st/connect", data=BODY,
                    headers=connect_headers(cookie, BODY, ts=ts))

    assert r.status_code == 403, f"expected 403 for {ts!r}, got {r.status_code}"


def test_non_ascii_signature_is_403_not_500(app, hook):
    """hmac.compare_digest raises TypeError on a non-ASCII str. That is attacker-supplied
    input, so it must be a refusal rather than an unhandled 500."""
    cookie = session_cookie(app, user_id=7)
    headers = connect_headers(cookie, BODY, signature="ü" * 64)

    assert hook.post("/_st/connect", data=BODY, headers=headers).status_code == 403


def test_missing_headers_entirely_is_403_not_500(app, hook):
    assert hook.post("/_st/connect", data=BODY,
                       headers={"Content-Type": "application/json"}).status_code == 403


def test_missing_session_is_401_not_500(app, hook):
    """401 means "this user may not connect": the gateway closes 3003 reconnect:false and
    the client stops. A 5xx here would mean "could not answer", closing 3008
    reconnect:true, and the client would retry a decision that will never change."""
    r = hook.post("/_st/connect", data=BODY,
                    headers=connect_headers("", BODY))

    assert r.status_code == 401


def test_unknown_user_is_401(app, hook):
    cookie = session_cookie(app, user_id=99999)
    assert hook.post("/_st/connect", data=BODY,
                       headers=connect_headers(cookie, BODY)).status_code == 401


def test_suspended_user_is_401(app, hook):
    cookie = session_cookie(app, user_id=9)          # seeded with active = 0
    assert hook.post("/_st/connect", data=BODY,
                       headers=connect_headers(cookie, BODY)).status_code == 401


def test_session_backend_failure_is_5xx_not_401(app, hook):
    """The mirror image, and the one that empties a product during a deploy.

    A session store that is not up yet must produce a 5xx. Returning 401 closes every
    connection with reconnect:false, and realtime does not come back when the deploy
    finishes — it comes back as each user reloads the page, over the following hours.
    docs/10-operations.md §7 lists this as a runbook entry.
    """
    cookie = session_cookie(app, user_id=7)
    app.config["DATABASE"] = "/nonexistent-directory-for-tests/demo.db"

    r = hook.post("/_st/connect", data=BODY, headers=connect_headers(cookie, BODY))

    assert r.status_code >= 500
    assert r.status_code != 401


def test_rotated_secret_is_accepted(app, hook):
    """app.webhook_secrets is a list so a secret can be rotated without restarting the
    gateway and the application at the same instant. docs/17-production-readiness.md §3.1.
    """
    app.config["ST_WEBHOOK_SECRETS"] = [WEBHOOK_SECRET, WEBHOOK_SECRET_ROTATED]
    cookie = session_cookie(app, user_id=7)

    for secret in (WEBHOOK_SECRET, WEBHOOK_SECRET_ROTATED):
        r = hook.post("/_st/connect", data=BODY,
                        headers=connect_headers(cookie, BODY, secret=secret))
        assert r.status_code == 200

    r = hook.post("/_st/connect", data=BODY,
                    headers=connect_headers(cookie, BODY, secret=b"z" * 40))
    assert r.status_code == 403


class _StubNonceStore:
    """Stands in for Redis SET NX EX. A real one must be shared by every worker."""

    def __init__(self):
        self.keys = set()

    def set(self, key, value, nx=False, ex=None):
        if nx and key in self.keys:
            return None
        self.keys.add(key)
        return True


def test_nonce_replay_is_rejected_when_the_store_is_enabled(app, hook):
    """Optional exactly-once. The +/-300s window is a replay window on its own."""
    app.config["ST_NONCE_STORE"] = _StubNonceStore()
    cookie = session_cookie(app, user_id=7)
    headers = connect_headers(cookie, BODY)

    assert hook.post("/_st/connect", data=BODY, headers=headers).status_code == 200
    assert hook.post("/_st/connect", data=BODY, headers=headers).status_code == 403


def test_replay_is_accepted_when_the_store_is_off(app, hook):
    """The default. Stated rather than hidden: the endpoint is read-only and idempotent,
    so a replay returns the same answer to someone who already had the bytes."""
    cookie = session_cookie(app, user_id=7)
    headers = connect_headers(cookie, BODY)

    assert hook.post("/_st/connect", data=BODY, headers=headers).status_code == 200
    assert hook.post("/_st/connect", data=BODY, headers=headers).status_code == 200


# --------------------------------------------------------------------------------------
# Grants
# --------------------------------------------------------------------------------------


def test_grants_agree_with_route_guard(app):
    """The test that keeps grants and HTTP authorization from drifting.

    A grant list computed by a query written specially for realtime is a second
    implementation of an authorization policy. It is correct the day it is written and
    silently wrong the first time the predicate learns something it did not.
    """
    conn = sqlite3.connect(app.config["DATABASE"])
    room_ids = [row[0] for row in conn.execute("SELECT id FROM rooms ORDER BY id")]
    conn.close()
    assert len(room_ids) > 2      # including one archived and one in a foreign org

    with app.test_request_context():
        for user_id in (7, 8):
            grants = grants_for(user_id)
            for room_id in room_ids:
                assert can_read_room(user_id, room_id) == (f"room-{room_id}" in grants), (
                    f"user {user_id} and room {room_id} disagree")


def test_archived_room_is_neither_readable_nor_granted(app):
    with app.test_request_context():
        assert 4412 not in readable_room_ids(7)
        assert "room-4412" not in grants_for(7)


def test_grants_never_contain_a_bare_wildcard(app):
    """No grant may be *, or a namespace wildcard with no identifier in it.

    Each of these matches every other user's channels, and the gateway honours a grant
    exactly as written. It holds no rule that could disagree.
    """
    forbidden = {"*", "user-*", "room-*", "org-*", "user*", "room*", "org*"}
    with app.test_request_context():
        for user_id in (7, 8):
            assert not (set(grants_for(user_id)) & forbidden)


def test_the_user_wildcard_trap(app):
    """docs/05-authorization.md §3, last row. `*` matches any run of characters including
    a separator, so `user-*` grants every user's channels, not one user's."""
    assert glob_match("user-*", "user-8-billing")     # the mistake
    assert glob_match("user-*", "user-8")

    with app.test_request_context():
        grants = grants_for(7)
    assert not any(glob_match(g, "user-8-billing") for g in grants)
    assert not any(glob_match(g, "user-8") for g in grants)


def test_both_user_grants_are_needed(app):
    """A trailing * after a separator matches user-7-billing but NOT the bare user-7."""
    assert glob_match("user-7-*", "user-7-billing")
    assert not glob_match("user-7-*", "user-7")
    assert not glob_match("user-7-*", "user-71-billing")

    with app.test_request_context():
        grants = grants_for(7)
    assert any(glob_match(g, "user-7") for g in grants)
    assert any(glob_match(g, "user-7-billing") for g in grants)


def test_org_prefix_is_an_authorization_boundary(app):
    """The separator immediately after the identifier is what stops org-42-* reaching
    org-421-*."""
    assert glob_match("org-42-*", "org-42-alerts")
    assert not glob_match("org-42-*", "org-421-alerts")
    assert not glob_match("org-42-*", "org-42")       # * matches empty, the prefix does not

    with app.test_request_context():
        grants = grants_for(7)                        # member of org 42, not 421
    assert any(glob_match(g, "org-42-alerts") for g in grants)
    assert not any(glob_match(g, "org-421-secret") for g in grants)


def test_public_broadcast_is_an_extra_string_not_a_config_key(app):
    """There is no way to disable authorization for a namespace. A public banner is one
    more grant in every connection's list. docs/06-channels.md §3."""
    with app.test_request_context():
        assert "status" in grants_for(7)
        assert "status" in grants_for(8)


# --------------------------------------------------------------------------------------
# Publishing and reconciliation
# --------------------------------------------------------------------------------------


def _login(client, user_id: int):
    with client.session_transaction() as sess:
        sess["user_id"] = user_id


def test_write_persists_then_publishes_with_exclude(app, client, publisher):
    _login(client, 7)
    r = client.post("/api/rooms/4410/messages", json={"body": "hello"},
                    headers={"X-St-Client": "8f2c1e04a7b3d915"})
    assert r.status_code == 201
    message_id = r.get_json()["id"]

    assert len(publisher.published) == 1
    key, payload = publisher.published[0]
    envelope = json.loads(payload)

    assert key == "st:room-4410"                      # {bus.prefix}{channel}
    assert envelope["event"] == "message.created"
    assert envelope["data"]["body"] == "hello"
    assert envelope["exclude"] == "8f2c1e04a7b3d915"  # do not echo to the writing tab
    assert envelope["id"] == str(message_id)
    assert "from" not in envelope                     # gateway-set only

    # Committed before the publish: a client that receives the push and immediately
    # reconciles must be able to read the row it was told about.
    body = client.get(f"/api/rooms/4410/messages?since={message_id - 1}").get_json()
    assert [m["id"] for m in body["messages"]] == [message_id]


def test_write_without_the_client_header_has_no_exclude(app, client, publisher):
    _login(client, 7)
    client.post("/api/rooms/4410/messages", json={"body": "hello"})

    envelope = json.loads(publisher.published[0][1])
    assert "exclude" not in envelope


def test_write_to_an_unreadable_room_is_403_and_publishes_nothing(app, client, publisher):
    _login(client, 7)
    assert client.post("/api/rooms/4413/messages",
                       json={"body": "hi"}).status_code == 403
    assert publisher.published == []


def test_since_returns_only_newer_rows_in_order(app, client):
    _login(client, 7)
    ids = [client.post("/api/rooms/4410/messages",
                       json={"body": f"m{n}"}).get_json()["id"] for n in range(5)]

    body = client.get(f"/api/rooms/4410/messages?since={ids[1]}").get_json()

    assert [m["id"] for m in body["messages"]] == ids[2:]
    assert body["latest_id"] == ids[-1]
    assert body["has_more"] is False


def test_since_at_the_head_returns_nothing_and_holds_the_cursor(app, client):
    _login(client, 7)
    last = client.post("/api/rooms/4410/messages", json={"body": "m"}).get_json()["id"]

    body = client.get(f"/api/rooms/4410/messages?since={last}").get_json()

    assert body["messages"] == []
    assert body["latest_id"] == last      # the cursor must not rewind to 0


def test_since_is_bounded_and_reports_more(app, client):
    _login(client, 7)
    for n in range(6):
        client.post("/api/rooms/4410/messages", json={"body": f"m{n}"})

    body = client.get("/api/rooms/4410/messages?since=0&limit=2").get_json()

    assert len(body["messages"]) == 2
    assert body["has_more"] is True       # a client asleep four hours pages, not floods


def test_cold_start_returns_the_last_page_not_everything(app, client):
    _login(client, 7)
    ids = [client.post("/api/rooms/4410/messages",
                       json={"body": f"m{n}"}).get_json()["id"] for n in range(6)]

    body = client.get("/api/rooms/4410/messages?limit=2").get_json()

    assert [m["id"] for m in body["messages"]] == ids[-2:]


def test_reconciliation_uses_the_same_guard_as_the_write(app, client):
    _login(client, 7)
    assert client.get("/api/rooms/4413/messages").status_code == 403
    assert client.get("/api/rooms/4412/messages").status_code == 403   # archived


def test_reconciliation_requires_a_session(app, client):
    assert client.get("/api/rooms/4410/messages").status_code == 401


# --------------------------------------------------------------------------------------
# Revocation
# --------------------------------------------------------------------------------------


def test_suspend_publishes_a_signed_control_disconnect(app, client, publisher):
    _login(client, 7)
    assert client.post("/admin/users/8/suspend").status_code == 200

    key, payload = publisher.published[-1]
    envelope = json.loads(payload)

    assert key == "st:_control"
    assert envelope["action"] == "disconnect"
    assert envelope["user"] == "u-8"           # exact, never a glob
    assert "client" not in envelope            # exactly one target must be named
    assert abs(time.time() - envelope["ts"]) < 5
    assert envelope["nonce"]

    body = json.dumps({"action": "disconnect", "user": "u-8",
                       "reason": envelope["reason"]},
                      sort_keys=True, separators=(",", ":"))
    expected = hmac.new(CONTROL_SECRET,
                        f"{envelope['ts']}.{envelope['nonce']}.{body}".encode(),
                        hashlib.sha256).hexdigest()
    assert hmac.compare_digest(expected, envelope["sig"])


def test_a_suspended_user_is_refused_on_the_next_connect(app, client, hook):
    _login(client, 7)
    client.post("/admin/users/8/suspend")

    cookie = session_cookie(app, user_id=8)
    r = hook.post("/_st/connect", data=BODY, headers=connect_headers(cookie, BODY))

    assert r.status_code == 401     # refusal, not failure: the client must stop asking


def test_control_signing_uses_the_control_secret_not_the_webhook_secret(app, publisher):
    """Two secrets, and they must not be the same value. A single secret means anyone who
    can read a webhook signature can forge a fleet-wide disconnect."""
    with app.test_request_context():
        control_disconnect("u-7", "test")

    envelope = json.loads(publisher.published[-1][1])
    body = json.dumps({"action": "disconnect", "user": "u-7", "reason": "test"},
                      sort_keys=True, separators=(",", ":"))
    with_webhook_secret = hmac.new(
        WEBHOOK_SECRET, f"{envelope['ts']}.{envelope['nonce']}.{body}".encode(),
        hashlib.sha256).hexdigest()

    assert envelope["sig"] != with_webhook_secret


# --------------------------------------------------------------------------------------
# Startup validation
# --------------------------------------------------------------------------------------


@pytest.mark.parametrize("overrides", [
    {"ST_WEBHOOK_SECRETS": []},
    {"ST_WEBHOOK_SECRETS": [b"too-short"]},
    {"ST_CONTROL_SECRET": b"too-short"},
])
def test_short_or_missing_secrets_refuse_to_start(tmp_path, overrides):
    """Matching the gateway's own validation. A shorter secret is a security hole shipped
    as a convenience, so refusing to start is the only honest option."""
    config = {
        "DATABASE": str(tmp_path / "demo.db"),
        "ST_WEBHOOK_SECRETS": [WEBHOOK_SECRET],
        "ST_CONTROL_SECRET": CONTROL_SECRET,
        "ST_PUBLISHER": NullPublisher(),
    }
    config.update(overrides)

    with pytest.raises(RuntimeError):
        create_app(config)
