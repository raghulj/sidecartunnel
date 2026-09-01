# Security Policy

## Reporting a Vulnerability

Report privately through GitHub's
[private vulnerability reporting](https://github.com/raghulj/sidecartunnel/security/advisories/new)
on this repository. That opens a draft advisory only I can see. Do not open a public issue
for anything you believe is exploitable.

I am one person maintaining this alongside other work, so the honest expectation is a first
reply within a week rather than within a day. If a report is confirmed and I have a fix, I
will cut a release and credit you in the advisory unless you would rather I did not.

What helps most, in rough order: the version or image digest, a description of what an
attacker gets, and the smallest sequence of requests or frames that demonstrates it. A
working proof of concept is welcome but not required — a clear description of the
mechanism is usually enough for me to reproduce it.

## Supported Versions

The major is still `0`, so only the latest minor gets fixes. There are no backports to
earlier minors; upgrading is the remedy. Versioning policy is in
[`docs/15-releasing.md`](docs/15-releasing.md) §1.

| Version | Supported |
|---|---|
| `0.1.x` | Yes |
| `< 0.1` | No |

## What Is In Scope

The gateway's whole security model is: verify an `Origin`, then match a string against a
list the application supplied. It holds no policy, no user store, and no rule that could
disagree with the application's. So the things worth reporting are the ones that break that
model:

- A connection that subscribes to a channel outside the grants the application returned.
- An `Origin` that passes the allowlist check but should not, or a handshake that skips the
  check. Browsers do not apply CORS to websocket handshakes but do attach cookies, so this
  check is the only thing standing between a logged-in user and cross-site websocket
  hijacking.
- A cookie, `Authorization` header, or webhook body appearing in a log line or an error
  response.
- Forging or replaying the HMAC signature on the connect webhook.
- A revocation on the control channel that does not actually close the socket.
- Anything that lets one client's messages reach another client's socket.
- Remote crash or unbounded memory growth from frames a client controls.

The threat model this is drawn from is [`docs/05-authorization.md`](docs/05-authorization.md) §8.

## What Is Not

- **The application's authorization decisions.** The gateway enforces what the application
  returns from the connect webhook. If the application hands out a grant it should not
  have, that is a bug in the application. This boundary is deliberate and is not going to
  move.
- **Running without an `Origin` allowlist**, or with `*` in it. Configuration that disables
  a check is doing what it was asked to do.
- **Denial of service by opening many connections.** `limits.max_connections` is the answer
  and it has a documented default. A resource exhaustion that survives a correctly
  configured limit is in scope; one that only works because the limit was raised is not.
- **Redis reachable by untrusted parties.** Anything that can `PUBLISH` to the bus can
  publish to any channel, by design — that is the whole integration surface. Network
  isolation for Redis is the deployment's job.
- Missing hardening headers on `/health` and `/ready`, which carry no credential and report
  only that the process is up.
