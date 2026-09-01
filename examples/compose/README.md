# Compose Example

A gateway, a Redis, and a placeholder application on one network. It is the smallest
arrangement that is correct rather than the smallest that starts — every non-default
setting in [`docker-compose.yml`](docker-compose.yml) is there because the default is
wrong for this workload, and each one carries the reason in a comment.

Full operational detail: [`docs/10-operations.md`](../../docs/10-operations.md) §3.

## Before Starting It

| Step | Why |
|---|---|
| Replace `ST_APP__WEBHOOK_SECRETS` and `ST_CONTROL__SECRET` | Both are literal placeholders. Minimum 32 bytes each: `openssl rand -hex 32`. |
| Replace the `webapp` image | It is `nginx:1.27-alpine` standing in for the application. Nothing runs until it serves `POST /_st/connect`. |
| Set `ST_SERVER__ALLOWED_ORIGINS` to the real origin | Exact origins, no wildcards. Startup fails if it is empty. |
| Check the gateway image digest | It is pinned to the `v0.1.0` manifest list by digest. Newer releases print theirs in their own release notes; `:latest` and `:X.Y` both move and are not what a deployment should name. |

```sh
docker compose up -d
docker compose ps          # both replicas healthy
docker compose logs -f sidecartunnel
```

To run a local build instead of the released image:

```sh
docker build -t sidecartunnel:dev ../..
ST_IMAGE=sidecartunnel:dev docker compose up -d
```

```
NAME                                    STATUS
sidecartunnel-example-redis-1           Up 17 seconds (healthy)
sidecartunnel-example-sidecartunnel-1   Up 12 seconds (healthy)
sidecartunnel-example-sidecartunnel-2   Up 12 seconds (healthy)
sidecartunnel-example-webapp-1          Up 12 seconds
```

The `webapp` service is `nginx:1.27-alpine` standing in for the application. It listens on
80 while `ST_APP__CONNECT_URL` names 5000, so every connect closes **3008, retryable**. The
gateway starts, reports healthy and holds its bus subscription regardless, which is what
this file is here to show. For a stack that carries a real connection end to end, use
[`examples/flask`](../flask/).

The gateway publishes no ports. The websocket must be same-origin with the application, so
a front proxy routes `example.com/ws` to `sidecartunnel:8000` across this network. For a
single-replica local experiment, set `replicas: 1` and add
`ports: ["127.0.0.1:8000:8000"]`.

## The Settings That Matter

| Setting | Value | Consequence Of The Default |
|---|---|---|
| `client-output-buffer-limit pubsub` | `256mb 64mb 60` | At the default `32mb 8mb 60`, Redis disconnects the gateway during a broadcast burst and the resubscribe leaves it immediately behind again. The oscillation is stable, not transient, and reads as `bus_reconnects` in `GET /ready` climbing against a healthy Redis. |
| `update_config.order` | `start-first` | `stop-first` takes the fleet to zero replicas mid-update, and every client reconnects into nothing. |
| `update_config.delay` | `30s` | Must exceed `server.drain_timeout` (20s). Below it, the next replica starts while the previous one is still draining, and clients spread across `drain_spread` land on a container that is about to go away. |
| `stop_grace_period` | `30s` | Also above `drain_timeout`, so a stopping replica finishes its drain instead of being killed mid-sentence. |
| Redis database index | `3` | Sharing index 0 with a cache means someone's `FLUSHDB` arrives during an incident. Pub/sub is unaffected by it; the confusion is not. |
| `healthcheck.test` | `/sidecartunnel healthcheck` | The image has no shell and no curl. The subcommand checks liveness only — a bus-dependent healthcheck kills the whole fleet during a Redis restart. |

## Compose Versus Swarm

`deploy.replicas` is honoured by Compose v2. `deploy.update_config` is a Swarm key: Compose
parses it and ignores it, so a `docker compose up` rolling restart has neither the
`start-first` ordering nor the delay. It is kept here because this file is meant to be
copied to `docker stack deploy`, where both apply, and because the values document what a
correct rolling update looks like on any orchestrator.

| Key | Compose v2 | Swarm |
|---|---|---|
| `deploy.replicas` | applied | applied |
| `deploy.update_config` | ignored | applied |
| `deploy.resources.limits` | applied | applied |
| `stop_grace_period` | applied | applied |
| `healthcheck` | applied | applied |

## Secrets

The values in the file are placeholders and look like it. In production, any configuration
key accepts a `_FILE` suffix, which is the Docker and Swarm secret convention:

```yaml
environment:
  ST_APP__WEBHOOK_SECRETS_FILE: /run/secrets/st_webhook
  ST_CONTROL__SECRET_FILE: /run/secrets/st_control
secrets: [st_webhook, st_control]
```

That keeps both values out of `docker inspect` and out of the process environment of
anything that is not the gateway.

## Verifying It Works

| Check | Command |
|---|---|
| Gateway is alive | `docker compose exec sidecartunnel /sidecartunnel healthcheck` — exits `0` |
| Gateway is ready | `docker run --rm --network sidecartunnel-example_default curlimages/curl -s http://sidecartunnel:8000/ready` — `{"ready":true,"bus_connected":true,"bus_down_for_seconds":0,"bus_reconnects":0,"draining":false}`. No port is published, so the probe is read from inside the network |
| Redis buffer limit took effect | `docker compose exec redis redis-cli config get client-output-buffer-limit` |
| Publishing reaches the bus | `docker compose exec redis redis-cli -n 3 publish st:room-4410 '{"event":"ping","data":{}}'` — the returned count is **gateway replicas holding that channel**, not end clients. `0` with no client subscribed is correct, and is the only thing the publisher ever learns |
| Channel is actually subscribed | `docker compose logs sidecartunnel \| grep '"msg":"subscribe"' \| grep room-4410` |

The publish key is `{bus.prefix}{channel}` exactly — `st:room-4410` for channel
`room-4410`. A wrong prefix is silent: Redis accepts the publish, nobody is listening, and
the delivery counter never moves.
