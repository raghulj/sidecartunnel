# 06 — Channels and namespaces

## 1. What a channel is

An opaque string. The gateway parses exactly one thing out of it — the namespace prefix —
and treats the rest as bytes to be matched and compared.

Rules:

- 1 to `limits.max_channel_length` bytes (default 255).
- Printable ASCII, no whitespace, no control characters.
- MUST NOT begin with `_`. That prefix is reserved for control channels and is refused
  with error 103 even if a grant would match it.
- The separator character (`channels.separator`, default `-`) splits the namespace from
  the rest at its **first** occurrence.

```
room-4410              namespace "room"
org-42-alerts          namespace "org"
user-7                 namespace "user"
standalone             namespace ""  → the built-in default block
_control               refused, always
```

A channel with no separator resolves to the namespace named `""`, which the built-in
default block owns. A namespace may not be **named** `default`: otherwise one config block
would govern two disjoint populations — separator-less channels, and channels literally
beginning `default-`.

## 2. Naming, and why it is your problem not the gateway's

Channel names are opaque to the gateway but they are the application's access-control
surface. A grant of `org-42-*` is only as tight as the naming scheme beneath it, so the
scheme has to be designed rather than accreted.

What works:

```
user-{user_id}                       one user, all their devices
room-{room_id}                       one room
org-{org_id}-{feature}               everything for one org, grantable as org-{id}-*
```

The property that matters: **a prefix must be a meaningful authorization boundary**, so
that `org-42-*` can be granted without accidentally granting `org-421-*`. Keep the
separator immediately after every identifier and that holds.

What does not work, and why:

- **Encoded or hashed names.** An earlier version of a system I ran used
  `base64(path)` as the channel name. It bought nothing — a name is not a secret, grants
  are what protect a channel — and it cost every ability to read a log, group a metric, or
  answer "who is subscribed to what" during an incident. Human-readable, always.
- **Nesting identifiers without a separator**, e.g. `org42alerts`. Nothing can be granted
  as a prefix without ambiguity.
- **Putting anything secret in the name.** Names appear in logs, metrics labels, and the
  admin API by design.

Cardinality is worth a thought before shipping: channel names become metric labels, so a
namespace with one channel per user and 200,000 users will hurt Prometheus. Namespaces are
labelled; individual channels are not, except in the admin API. That is the reason for the
split.

## 3. Namespaces

The namespace selects a block of configuration. This is how per-feature behaviour gets
expressed without per-application code — a new feature is a config block, not a release.

```yaml
channels:
  separator: "-"

namespaces:
  - name: room

  - name: user

  - name: desk
    client_events: true          # M4
    rate_limit: "10/s"
```

| Key | Type | Default | Meaning |
|---|---|---|---|
| `name` | string | — | The namespace. The **reserved empty name `""`** catches channels with no separator; a namespace may not be *named* `default`. |
| `client_events` | bool | `false` | Permit `publish` from clients on these channels. M4. |
| `rate_limit` | string | `"10/s"` | Client-event rate per connection. Only meaningful with `client_events`. |
| `max_message_size` | bytes | inherits `limits.max_message_size` | Per-namespace override. |
| `presence` | bool | `false` | Track and broadcast membership. M4, not built. |
| `history_size` | int | `0` | Bounded replay buffer. M4, not built. |

A channel whose namespace has no block, and where no `default` block exists, is refused
with error 102. Failing closed is deliberate: a typo'd namespace should be an error, not a
silently permissive channel.

When `namespaces` is empty entirely, a built-in `default` block applies to every channel.
Without that, the minimal environment-only configuration in `08-config.md` §4 starts
cleanly, reports healthy, accepts connections, and refuses every single subscribe.

### There is no way to disable authorization

An earlier draft had `auth_required: false` on a namespace, for genuinely public
broadcasts — a status banner, a deploy notice. It is gone, for two reasons.

It contradicted `01-requirements.md` FR-5, which says a grant is required for every
subscribe, and both statements were citable from documents claiming authority. Worse, it
reintroduced exactly the hole that made hosted public channels unusable: a namespace where
knowing a name is the same as being allowed to read it, one config key away from any
channel.

A public broadcast is now expressed by the application putting `status` — or whatever it
is called — in every connection's grant list. One extra string, no new concept, and no way
to accidentally turn authorization off for a whole namespace.

## 4. Reserved names

| Pattern | Reserved for |
|---|---|
| `_control` | Operational commands to every replica (`04-integration.md` §3) |
| `_*` | Future internal use |

Clients can never subscribe to these; the gateway refuses before consulting grants, so a
misconfigured grant of `*` still cannot reach them.
