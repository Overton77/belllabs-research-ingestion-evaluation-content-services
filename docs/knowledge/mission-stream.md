---
type: Concept
title: Mission stream (Socket.IO)
description: The /missions Socket.IO namespace served by bootstrap.realtime:create_asgi_app - authentication through the HTTP verifier with an idle-connection watchdog, cursor domains per journal and per execution, chain targets, PostgreSQL replay with retention expiry, subordinate lineage and the common frame projection, durable command receipt progress, wake-up hints from PostgreSQL LISTEN or Redis, the typed error codes, and what is not proven.
tags: [mission-control, streams, socketio, realtime, subscriptions, lineage, implementation]
---

# Mission stream (Socket.IO)

The mission stream is a browser and agent channel over the same application services as HTTP,
SSE and MCP; PostgreSQL is the only replay ledger and Redis only coordinates fanout
([ADR-0036](../adr/0036-mission-stream-is-socket-io-over-shared-handlers-postgres-replay-redis-fanout.md),
[ADR-0040](../adr/0040-native-frames-normalized-lifecycle-lineage-graph-bounded-coordinator-subscriptions.md),
both `proposed`; SPEC-04). Built by MP-13/MP-14, completed in the 2026-10-09 recovery. A
[Stream Subscription](../../GLOSSARY.md) lives only as long as its socket and is not a durable
[Subscription](../../GLOSSARY.md) ([events and commands](events-and-commands.md)).

## Entrypoint and composition

`uvicorn mission_control.bootstrap.realtime:create_asgi_app --factory` wraps the one FastAPI
application of `bootstrap.api.create_application` once with `mount_mission_socketio`
(`interfaces/socketio/app.py`), so the API lifespan (pools, relays) runs exactly once;
`bootstrap.api:create_app` keeps working without the socket. The API composes one
`MissionStreamService(PostgresStreamSource(pool, scope))` per tenant as
`app.state.mission_control_stream_services`. Only the `/missions` namespace is accepted; origins
come from `SOCKETIO_CORS_ORIGINS` (`*` refused) and are checked independently of
authentication; `max_http_buffer_size` is 256 KiB.

Hints (`bootstrap/realtime.py`): one `LISTEN mc_stream_hint` connection per application database
(0032 triggers, [persistence](persistence.md); `MISSION_SOCKET_POSTGRES_HINTS`), and with
`MISSION_SOCKET_REDIS_FANOUT` Redis hints plus the Socket.IO Redis manager between processes.
Hints only wake pumps; each pump polls its high-watermark every second, so nothing is lost.

## Authentication, authorization and events

Connect with `auth={application_id, token}`; `interfaces/socketio/auth.py` uses the public API's
own principal verifier and `ApplicationRegistry` check. The token is re-verified on every client
event, and a per-connection **watchdog** re-verifies it every `reauthorize_seconds` (default 5 s)
and at its `exp`, even on an idle connection: a revoked grant or binding or an expired token
emits one `stream_error{UNAUTHORIZED}`, detaches every subscription and disconnects.
`reauthenticate` accepts only the same installation, application, tenant and actor. Scope comes
from the principal (a differing `application_id` is `SCOPE_MISMATCH`); a target of another tenant
or application answers the same `TARGET_NOT_FOUND` as an absent one. Rooms carry presence only.
Client events: `subscribe`, `ack`, `unsubscribe`, `command`, `resolve_human_task`,
`reauthenticate`; server events: `subscribed`, `snapshot`, `mission_event`, `provider_frame`,
`presence`, `lineage`, `command_receipt`, `human_task_receipt`, `resync_required`,
`stream_error` (versioned in `contracts/realtime.py`, `mc.realtime.v1`).

## Targets and cursor domains

`mc.stream_subscription.v1` keys a cursor domain by `(stream, key)`: the execution of a
provider-frame cursor (`<harness_execution_id>:<ordinal>` with its `generation`) or the mission
of a keyed mission-event cursor (`<mission_id>:<seq>`). The extension is additive: a request
with at most one cursor per stream validates and behaves as before. At most 64 cursors.

| Target | Mission events | Provider frames |
| --- | --- | --- |
| `execution` | the run's mission journal (`<seq>`) | that execution (`provider_frames`) |
| `run` | the run's events in its mission journal | every execution of the run, one domain each |
| `mission` | the mission journal | every execution of every run of the mission |
| `chain` | every member mission's journal, keyed `<mission>:<seq>` | refused (`UNSUPPORTED_FILTER`); subscribe a member |

- **Frames across executions.** A `run` or `mission` subscription follows up to 64 executions
  (`provider_frames.truncated` otherwise). Executions are discovered as they start, at most once
  per poll interval. A new execution, or one the resuming client has no cursor for, is replayed
  from its first frame. A cursor naming an execution outside the target is `SCOPE_MISMATCH`.
  `ack` returns acked positions per domain (`provider_frames:<execution>`).
- **Chains.** A chain is resolved by id or key under the caller's scope. Its members are the
  missions its links name, so a chain of another tenant or application is absent. A plain or
  non-member mission cursor is `SCOPE_MISMATCH`. The chain's lifecycle, phase, members and links
  arrive in the `snapshot` and in the full `lineage` listing. A changed chain version is sent
  with the next member event.

## Replay

- **Replay equals live.** Without a cursor a domain starts at its high-watermark behind a
  `snapshot`. With one it replays `(cursor, H]` in pages of 200, then follows live; positions
  only advance, so a subscription never emits an event twice and event ids are stable across
  reconnects.
- A relaunched execution replays its current generation with `gap=true` (live:
  `resync_required{STALE_GENERATION, detached:false}`).
- **Retention.** Retention deletes non-closing frames in place
  (`PostgresFrameRetention.expire`). A frame cursor is expired when its own frame was deleted,
  or when frames after it are missing in an execution older than the application's retention
  horizon. The domain then restarts at H behind `snapshot{reason: cursor_expired}` with
  `gap=true`. A subscription that falls behind while live gets
  `resync_required{CURSOR_EXPIRED, detached:false, replay_from:H}` and continues at H. The rule
  is conservative: an ordinal gap in an old execution may resync without a real deletion, but
  nothing is silently skipped. Mission events are immutable; a journal cursor below the oldest
  retained sequence is likewise `cursor_expired`.
- Mission events pass the public alias projection (`aliases.select`).

## Provider frames: common projection and lineage

- Every frame envelope keeps its native `raw_kind` and bounded excerpt and adds
  `payload.normalized`: `execution.started|ended` (parent frames only), `tool.*`,
  `approval.pending|resolved`, `compaction.observed`, `usage`, `subordinate.started|ended`
  (`application/frames/projections.py`). Unknown kinds stay visible with `known_kind:false` and
  no normalized fact.
- `usage` frames carry `usage_attribution`: a parent frame is counted in its turn; a subagent
  frame is `folded` (already in the parent turn) or `unattributable` (in no total; Claude
  reports it only inside provider-inclusive cost), and `add_to_parent` is always false, so a
  recursive view never double counts.
- **`include_descendants`.** Without it, subagent frames are withheld. With it, a
  `LineageTracker` (`application/frames/lineage_stream.py`) folds every frame scanned, delivered
  or filtered, from each execution's first frame. It gives the same answer as
  `lineage.provider_subordinates`, so each subagent frame's `subordinate_ref` reports the
  visibility actually persisted so far: `full` once a message or tool frame exists,
  `lifecycle_only` while only lifecycle frames do, `unavailable` for spawn evidence alone. Late
  children resolve by native refs or stay `resolved:false`.
- Mission-event `include_descendants` lists the linked missions this scope can see as
  `lifecycle_only` (`mission_events.descendants`). Their journals stay separate cursor domains;
  subscribe the chain to read them.
- **`lineage` event**: the full listing after `subscribed` (descendants or a chain), then only
  changed nodes. It carries refs, resolution, lifecycle, observed frame counts, visibility, the
  usage inclusion rule and, for chains, the chain body; never child content. Coverage is
  `partial` when seeding stopped at 50 000 frames or a retention resync skipped frames.

## Commands and Human Tasks

- `command` forwards to the same `MissionControlService.command` as HTTP, so an HTTP replay of a
  socket request returns `replay=true`. The first `command_receipt` is the admission or the
  boundary ledger's current state (`stage`, `final`). While not final, the namespace follows the
  command's durable receipt ledger through `MissionControlService.commands` (the handler of
  `GET /runs/{run_id}/commands`). It emits each later state once, in order (`queued`,
  `delivered`, `observed`, `applied` or `rejected`, `expired`, `failed`), up to 8 followers per
  connection and 120 s each; then `unfollowed`, with the ledger still readable over HTTP.
- `resolve_human_task` forwards a Human Gate body to `HumanTaskService.resolve` and an MP-11
  approval body (`reviewed_digest`, edited arguments, answers, elicitation content) to
  `resolve_approval`, the handler of `POST /human-tasks/{id}/approval-resolutions`.
  Rejections follow the HTTP statuses: 404 is `TARGET_NOT_FOUND`, 403 `UNAUTHORIZED`, every
  409/422 code is `COMMAND_CONFLICT` with the HTTP code as detail
  ([Human Gates](human-gates.md)).

## Bounds and error codes

16 subscriptions per connection, 20 messages per second (burst 40), a 1 MiB connection budget,
per-domain windows released by `ack`, delta coalescing of at least 250 ms, and
`resync_required{SLOW_CONSUMER, detached:true, replay_from}` after 10 s blocked: configurable
defaults, not benchmarked capacity. `STREAM_ERROR_CODES` (`domain/subscriptions/streams.py`):
the SPEC-04 ten plus `UNAVAILABLE` (retryable store or handler failure) and
`UNSUPPORTED_OPERATION` (for example Human Tasks not composed).

## Not built or not proven (reported, not resolved)

- Chain provider frames are not merged (subscribe a member). Retention expiry is inferred from what retention leaves (no durable expiry watermark). A
  replay from ordinal 0 of an execution that looks younger than the horizon returns only the
  retained frames.
- A browser or Node `socket.io-client` and load capacity were not run. Multi-process fanout is
  proven with two OS processes on one host (Redis 16379), not behind a load balancer.
- Clients must `ack` (else detached when the window fills). Redis hint publishers are not
  wired; with PostgreSQL LISTEN off, delivery relies on polling.

# Citations

- Spec: [SPEC-04](../specs/multi-provider-2026-10/SPEC-04-realtime.md), [MP-13](../specs/multi-provider-2026-10/issues/MP-13-extend-event-projections-native-frames-and-subordinate-lineage.md), [MP-14](../specs/multi-provider-2026-10/issues/MP-14-add-scoped-mission-socket-io-handlers-with-durable-replay.md); ADRs [0036](../adr/0036-mission-stream-is-socket-io-over-shared-handlers-postgres-replay-redis-fanout.md), [0040](../adr/0040-native-frames-normalized-lifecycle-lineage-graph-bounded-coordinator-subscriptions.md) (proposed).
- Code: [realtime bootstrap](../../src/mission_control/bootstrap/realtime.py), [socket mount](../../src/mission_control/interfaces/socketio/app.py), [namespace](../../src/mission_control/interfaces/socketio/server.py), [socket auth](../../src/mission_control/interfaces/socketio/auth.py), [command gateway](../../src/mission_control/interfaces/socketio/commands.py), [realtime contract](../../src/mission_control/contracts/realtime.py), [stream contracts](../../src/mission_control/domain/subscriptions/streams.py).
- Streams: [service](../../src/mission_control/application/streams/service.py), [pump](../../src/mission_control/application/streams/pump.py), [flow control](../../src/mission_control/application/streams/flow.py), [lineage tracker](../../src/mission_control/application/frames/lineage_stream.py), [frame projection](../../src/mission_control/application/frames/projections.py), [PostgreSQL source](../../src/mission_control/adapters/realtime/stream_source.py), [Redis hints](../../src/mission_control/adapters/realtime/stream_hints.py), [PostgreSQL hints](../../src/mission_control/adapters/realtime/stream_hints_postgres.py).
- Unit tests: [stream service](../../tests/unit/streams/test_stream_service.py), [cursor domains](../../tests/unit/streams/test_stream_channels.py), [stream lineage](../../tests/unit/streams/test_stream_lineage.py), [pump](../../tests/unit/streams/test_stream_pump.py), [socket](../../tests/unit/socketio/test_mission_socket.py), [socket controls](../../tests/unit/socketio/test_realtime_controls.py), [approval resolution](../../tests/unit/socketio/test_socket_approval_resolution.py), [realtime bootstrap](../../tests/unit/socketio/test_realtime_bootstrap.py).
- PostgreSQL/Redis tests: [socket](../../tests/integration/postgres/test_mission_socket_postgres.py), [realtime acceptance](../../tests/integration/postgres/test_realtime_acceptance_postgres.py), [cursor domains](../../tests/integration/postgres/test_realtime_channels_postgres.py), [0032 stream hints](../../tests/integration/postgres/test_release_0032_cluster_binding_and_stream_hints.py).
