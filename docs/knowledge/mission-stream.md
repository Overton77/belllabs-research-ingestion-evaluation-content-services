---
type: Concept
title: Mission stream (Socket.IO)
description: The /missions Socket.IO namespace served by bootstrap.realtime:create_asgi_app - authentication through the HTTP verifier, stream subscriptions with PostgreSQL replay, acknowledgements and bounded windows, commands and Human Task resolution over the same services, wake-up hints from PostgreSQL LISTEN or Redis, the typed stream error codes, and what is not proven.
tags: [mission-control, streams, socketio, realtime, subscriptions, implementation]
---

# Mission stream (Socket.IO)

The mission stream is a browser and agent channel over the same application services as HTTP,
SSE and MCP; PostgreSQL is the only replay ledger and Redis only coordinates fanout
([ADR-0036](../adr/0036-mission-stream-is-socket-io-over-shared-handlers-postgres-replay-redis-fanout.md),
`proposed`; SPEC-04). Built by MP-14, wired by the integrator 2026-10-09. A
[Stream Subscription](../../GLOSSARY.md) lives only as long as its socket and is not a durable
[Subscription](../../GLOSSARY.md) ([events and commands](events-and-commands.md)).

## Entrypoint and composition

`uvicorn mission_control.bootstrap.realtime:create_asgi_app --factory` wraps the one FastAPI
application of `bootstrap.api.create_application` once with `mount_mission_socketio`
(`interfaces/socketio/app.py`), so the API lifespan (pools, relays) runs exactly once;
`bootstrap.api:create_app` keeps working without the socket. The API composes one
`MissionStreamService(PostgresStreamSource(pool, scope))` per tenant as
`app.state.mission_control_stream_services`. Only the `/missions` namespace is accepted; origins
come from `SOCKETIO_CORS_ORIGINS` (`*` refused); `max_http_buffer_size` is 256 KiB.

Wake-up hints compose in `bootstrap/realtime.py`: with `MISSION_SOCKET_POSTGRES_HINTS` (default
true) one `PostgresStreamHints` listener per distinct application database on the
`mc_stream_hint` channel that the 0032 triggers notify ([persistence](persistence.md)); with
`MISSION_SOCKET_REDIS_FANOUT=true`, `RedisStreamHints` over `REDIS_URL` plus the Socket.IO Redis
manager for presence between API processes. Hints only wake pumps; each pump also polls its high
watermark every second, so a lost hint delays and never drops an event.

## Contract

- **Connect** with `auth={application_id, token}`; `interfaces/socketio/auth.py` uses the public
  API's own principal verifier and `ApplicationRegistry` check. The token is re-verified on every
  client event, its `exp` bounds the connection, `reauthenticate` accepts only the same
  installation, application, tenant and actor, and a revoked credential detaches every
  subscription and disconnects.
- **Client events**: `subscribe`, `ack`, `unsubscribe`, `command`, `resolve_human_task`,
  `reauthenticate`. **Server events**: `subscribed`, `snapshot`, `mission_event`,
  `provider_frame`, `presence`, `command_receipt`, `human_task_receipt`, `resync_required`,
  `stream_error` (`interfaces/socketio/server.py`).
- **Replay equals live.** Without a cursor a subscription starts at the high watermark behind a
  `snapshot`; with one it replays `(cursor, H]` in pages of 200 using the MP-13 per-stream
  cursors (`application/frames/streams.py`), then follows live. A relaunched execution replays the
  current generation with `gap=true`. Mission events pass the public alias projection
  (`aliases.select`); frame envelopes carry subordinate lineage.
- **Commands** forward to the same `MissionControlService.command` as HTTP, so an HTTP replay of
  a socket request returns `replay=true`. `resolve_human_task` forwards to the tenant's
  `HumanTaskService` ([Human Gates](human-gates.md)).
- **Bounds.** 16 subscriptions per connection, 20 messages per second (burst 40), a 1 MiB
  connection budget, per-subscription windows released by `ack`, delta coalescing of at least
  250 ms, and `resync_required{SLOW_CONSUMER, detached:true, replay_from}` after 10 s blocked.
  These are configurable defaults, not benchmarked capacity. Rooms carry best-effort presence
  only and never authorize.

## Error codes

`STREAM_ERROR_CODES` in `domain/subscriptions/streams.py` (`mc.stream_subscription.v1`):
`UNAUTHORIZED`, `SCOPE_MISMATCH`, `TARGET_NOT_FOUND`, `CURSOR_EXPIRED`, `CURSOR_AHEAD`,
`STALE_GENERATION`, `UNSUPPORTED_FILTER`, `RATE_LIMITED`, `SLOW_CONSUMER`, `COMMAND_CONFLICT`,
plus `UNAVAILABLE` (retryable: three consecutive store read failures or an unexpected handler
error) and `UNSUPPORTED_OPERATION` (not retryable: for example Human Tasks not composed), added by
the integrator. A foreign or absent target answers the same `TARGET_NOT_FOUND`.

## Not built or not proven (reported, not resolved)

- `chain` targets and provider frames for `run` or `mission` targets answer
  `UNSUPPORTED_FILTER` (one frame cursor per stream); `include_descendants` on mission events
  reports `mission_events.descendants = unavailable`.
- Multi-OS-process fanout behind a load balancer, a browser or Node `socket.io-client`, and load
  capacity were not run; the Redis proof runs two API servers in one OS process.
- Frame retention deletes non-closing frames in place, and frame replay returns what is retained
  without reporting `CURSOR_EXPIRED`.
- Clients must `ack`; one that never acknowledges is detached when its window fills.

# Citations

- Spec: [SPEC-04](../specs/multi-provider-2026-10/SPEC-04-realtime.md);
  [MP-14](../specs/multi-provider-2026-10/issues/MP-14-add-scoped-mission-socket-io-handlers-with-durable-replay.md).
- ADR: [0036](../adr/0036-mission-stream-is-socket-io-over-shared-handlers-postgres-replay-redis-fanout.md) (proposed).
- Code: [realtime bootstrap](../../src/mission_control/bootstrap/realtime.py),
  [socket mount](../../src/mission_control/interfaces/socketio/app.py),
  [namespace](../../src/mission_control/interfaces/socketio/server.py),
  [socket auth](../../src/mission_control/interfaces/socketio/auth.py),
  [command gateway](../../src/mission_control/interfaces/socketio/commands.py),
  [stream service](../../src/mission_control/application/streams/service.py),
  [pump](../../src/mission_control/application/streams/pump.py),
  [flow control](../../src/mission_control/application/streams/flow.py),
  [PostgreSQL source](../../src/mission_control/adapters/realtime/stream_source.py),
  [Redis hints](../../src/mission_control/adapters/realtime/stream_hints.py),
  [PostgreSQL hints](../../src/mission_control/adapters/realtime/stream_hints_postgres.py),
  [stream contracts](../../src/mission_control/domain/subscriptions/streams.py).
- Tests: [stream service](../../tests/unit/streams/test_stream_service.py),
  [pump](../../tests/unit/streams/test_stream_pump.py),
  [socket](../../tests/unit/socketio/test_mission_socket.py),
  [realtime bootstrap](../../tests/unit/socketio/test_realtime_bootstrap.py),
  [socket on PostgreSQL and Redis](../../tests/integration/postgres/test_mission_socket_postgres.py),
  [0032 stream hints](../../tests/integration/postgres/test_release_0032_cluster_binding_and_stream_hints.py).
