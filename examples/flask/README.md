# Flask Reference Application

A runnable rooms-and-messages application carrying every application-side surface
sidecartunnel requires, and nothing else. The guide it accompanies is
[`docs/16-integration-guide.md`](../../docs/16-integration-guide.md).

Four dependencies: Flask, redis-py, gunicorn, pytest. No websocket library, no async
worker class, no monkey-patching. Storage is stdlib `sqlite3`.

## Run It

### Tests Only — 20 Seconds, No Docker

```sh
python3 -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
pytest -q
```

**50 tests.** They cover the whole webhook contract and need neither Redis nor a gateway.

### The Application — Under A Minute

```sh
cp .env.example .env        # placeholders only; replace them
docker compose up
open http://localhost:8080
```

Open the same URL in a **second tab**. A message sent in one appears in the other.

Or without Docker:

```sh
ST_WEBHOOK_SECRETS=REPLACE-ME-webhook-secret-at-least-32-bytes \
ST_CONTROL_SECRET=REPLACE-ME-control-secret-at-least-32-bytes \
python3 app.py
```

With no `REDIS_URL`, publishes go to a null publisher and are logged rather than sent.
Every HTTP surface still works.

### What Does Not Run Yet

| Piece | State |
|---|---|
| Application side — webhook, publish, `?since=`, revocation | **Runs.** Tested. |
| Browser client, `client/js/sidecartunnel.js` | **Present.** Imported by `templates/index.html`. |
| Gateway | **Not implemented.** `cmd/sidecartunnel` panics; M1 and M2 in `docs/12-roadmap.md`. |

Until the gateway exists, the page shows the connection retrying with full-jitter backoff,
and messages still appear through the HTTP write and the `?since=` reconciliation. That is
the correct behaviour for a gateway that is down, and worth seeing: connections retry, the
application keeps working, and nothing is lost.

`templates/index.html` imports the library from `/client/js/sidecartunnel.js`, which
`app.py` serves out of the checkout. A real application copies the file into its own static
directory and serves it the way it serves any other asset.

## What To Look At

In this order.

| # | File | Lines | What it demonstrates |
|---|---|---|---|
| 1 | `app.py` → `st_connect` | 7 numbered steps | Verification order. The signature is checked **before** anything is parsed. |
| 2 | `app.py` → step 6 | one `abort(401)` | The only 401 in the file. Everything else that fails becomes a 5xx. |
| 3 | `app.py` → `grants_for` | ~10 | The channel scheme, and why both `user-7` and `user-7-*` are needed. |
| 4 | `app.py` → `readable_room_ids` / `can_read_room` | ~12 | One predicate, used by the route guard and by the grants. They cannot drift because there is only one of them. |
| 5 | `app.py` → `create_message` | ~20 | Commit, **then** publish, with `exclude` from `X-St-Client`. |
| 6 | `app.py` → `list_messages` | ~25 | `?since=` on a monotonic id cursor, bounded, same guard. |
| 7 | `app.py` → `control_disconnect` | ~20 | Signed revocation, and an open point in the spec marked at the call site. |
| 8 | `test_app.py` | 50 tests | The guidance, executable. |
| 9 | `templates/index.html` | ~60 of script | Reconcile on every reconnect, `X-St-Client` on the write, and `catch` on a refused subscribe. |

### Things To Try Once It Is Running

| Try | Expected | Why it matters |
|---|---|---|
| Send from tab A | Appears in tab B highlighted; **not** re-added in tab A | `exclude` and `X-St-Client` |
| `docker compose restart sidecartunnel` | Tabs reconnect and hold every message sent during the gap | `?since=` is doing the work, not the socket |
| `docker compose stop redis` | Connections stay open and silent | The gateway holds sockets through a bus outage by design |
| `curl -X POST localhost:8080/admin/users/7/suspend` | Every connection for `u-7` closes with **3501** and does not retry | Revocation over the control channel |
| Visit `/login/9` | The webhook answers **401**, the page stops retrying | Suspended user, refusal not failure |
| `curl -H "Authorization: Bearer $ST_ADMIN_TOKEN" sidecartunnel:9001/channels` | The channels the gateway thinks are subscribed | The only way to catch a typo'd channel name |

## The Endpoints

| Method | Path | Purpose |
|---|---|---|
| POST | `/_st/connect` | The connect webhook. The only surface the gateway calls. |
| POST | `/api/rooms/<id>/messages` | Persist, commit, publish with `exclude`. |
| GET | `/api/rooms/<id>/messages?since=` | Reconciliation. Mandatory, not optional. |
| POST | `/admin/users/<id>/suspend` | Revoke, then publish a signed control `disconnect`. |
| GET | `/login/<id>`, `/logout`, `/` | Demo scaffolding. Not part of the integration. |

## The Channel Scheme

| Channel | Granted as | Note |
|---|---|---|
| `user-7` | `user-7` | The user's own stream. Never `user-*`. |
| `user-7-billing` | `user-7-*` | A trailing `*` after a separator does not match the bare `user-7`, so both grants are needed. |
| `room-4410` | `room-4410` | Enumerated from the same predicate that guards the HTTP route. |
| `org-42-alerts` | `org-42-*` | The separator after the identifier is what stops this reaching `org-421-*`. |
| `status` | `status` | A public broadcast is one extra string, not a config key. |

## The Seed Data

| User | State | Rooms | Orgs |
|---|---|---|---|
| 7 (Ada) | active | 4410, 4411 | 42 |
| 8 (Grace) | active | 4410, 4413 | 42 |
| 9 (Sam) | **suspended** | 4410 | — |

Room **4412** is archived and room **4413** belongs to org **421**. Both exist so
`test_grants_agree_with_route_guard` has something to fail against: a grant list built
from a second query rather than from the route's own predicate would include them.

## Not Production Code

The parts that are deliberately thin, so nobody copies them by accident:

| Shortcut | What production needs |
|---|---|
| `sqlite3`, one file | The application's real database |
| No CSRF on the write route | The framework's CSRF protection. The webhook route is the only one that must be exempt. |
| `/admin/...` guarded by nothing | Whatever already guards destructive admin actions |
| `/login/<id>` with no password | The application's real session |
| Nonce store off by default | Redis `SET NX EX`, shared by every worker |
| Placeholder secrets in `.env.example` | A secret manager or a Docker secret |

The webhook, the grant computation, the publish, the reconciliation endpoint and the
control message are the parts meant to be copied.
