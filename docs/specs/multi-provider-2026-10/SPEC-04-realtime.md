---
type: Specification
title: "Mission events, Socket.IO and coordinator subscriptions"
description: "Application-scoped realtime handlers, durable replay, provider-native streams, subordinate lineage and bounded callback delivery."
tags: [mission-control, socketio, events, subscriptions]
---

# Realtime and inspection

Use `python-socketio` already in the dependency set. Wrap the public FastAPI application with `socketio.ASGIApp`; preserve its startup/shutdown and existing HTTP/MCP/SSE mounts. The existing `bootstrap/technical_api.py` socket is not silently promoted into the mission API. First extract reusable authorizer/approval ports, then compose the new application-scoped handlers. [python-socketio ASGI integration](https://python-socketio.readthedocs.io/en/stable/server.html).

## Proposed organization

```text
contracts/realtime.py                    versioned client/server envelopes
interfaces/socketio/server.py            server factory and namespace registration
interfaces/socketio/auth.py              principal acquisition and expiry
interfaces/socketio/subscriptions.py     subscribe, unsubscribe, replay, ack
interfaces/socketio/commands.py          admitted command gateway
interfaces/socketio/human_tasks.py       resolution gateway
application/streams/                     snapshot/cursor/filter orchestration
adapters/realtime/                       outbox fanout and optional Redis manager
bootstrap/realtime.py                    shared API lifecycle composition
```

Names are proposed; use the closest existing module if it already owns a concern. No handlers call provider SDKs or write SQL. All command and resolution paths call the same application handlers as HTTP/MCP. HTTP remains the simplest durable mutation path; Socket.IO mutations are thin parity conveniences.

## Streams and envelopes

Keep three logical streams, each with its own cursor domain:

| Stream | Authority and ordering | Default audience |
| --- | --- | --- |
| Mission events | Application reducer journal; ordered `mission_seq` per mission | Dashboard and coordinator |
| Provider frames | Native Event Store; ordered persisted `frame_seq` per execution/generation | Explicitly subscribed run detail/transcript viewers |
| Presence/progress | Best-effort bounded telemetry, not state | Connected dashboard only |

Do not mix native offsets with mission sequences. There is no global order across missions or across streams. Envelope: `schema_version`, `stream`, `event_id`, authoritative scope, mission/run/execution/generation references, source sequence/cursor, `occurred_at`, `recorded_at`, `kind`, `payload` or authorized `payload_ref`, correlation/causation IDs and optional `subordinate_ref`. Require stream-specific fields through a discriminated union. Timestamps aid display; sequence and causal refs determine replay/meaning.

Normalize stable lifecycle facts: execution started/ended, tool started/completed/failed, approval pending/resolved, compaction observed, usage, subordinate started/ended. Preserve native kind/version and bounded original payload for provider-specific UI. Unknown native kinds remain visible as unknown, never silently promoted to closing facts. Redact before general fanout and apply stricter transcript grants to tool arguments/results. Expose only provider-supplied public reasoning summaries, not an inferred promise of access to hidden reasoning.

Raw deltas may be coalesced for display. Required terminal/control facts must be durable before delivery. Retention-expired raw detail does not erase authoritative mission events or effect receipts.

## Socket contract

Engine.IO path `/socket.io`; namespace `/missions`. Application identity belongs in authenticated scope, not a client-selectable namespace. Never trust requested room names or an actor ID in a payload.

| Direction / event | Request or result |
| --- | --- |
| connect | `auth` credential or scoped short-lived stream ticket; server validates issuer, audience, tenant and application grants |
| client `subscribe` | `{request_id, application_id, target:{kind,id}, streams, filters, cursors, include_descendants}` |
| server subscribe ack | `{subscription_id, snapshot_ref, snapshot_versions, high_watermarks, replay_from}` after authorization |
| server `mission_event` | Versioned journal event |
| server `provider_frame` | Authorized frame or coalesced delta with execution/generation cursor |
| server `snapshot` | Consistent scoped projection plus covered cursor range |
| client `ack` | `{subscription_id,cursors}` monotone, scoped and bounded by server-sent high-watermarks |
| client `unsubscribe` | Scoped subscription ID; remove socket membership |
| client `command` | Existing typed command envelope with idempotency/expected version; accepted receipt returned |
| client `resolve_human_task` | Same versioned resolution body as HTTP |
| server `command_receipt` | Accepted/delivered/applied outcome, not just network acknowledgement |
| server `resync_required` | Cursor expired or slow-consumer overflow; fresh snapshot/replay instructions |
| server `stream_error` | Typed code, retryability, request/subscription correlation; no secret exception text |

Proposed errors: `UNAUTHORIZED`, `SCOPE_MISMATCH`, `TARGET_NOT_FOUND`, `CURSOR_EXPIRED`, `CURSOR_AHEAD`, `STALE_GENERATION`, `UNSUPPORTED_FILTER`, `RATE_LIMITED`, `SLOW_CONSUMER`, `COMMAND_CONFLICT`. Agree casing with the existing public error convention in MP-01; no duplicate error vocabulary in SDK generation.

## Replay algorithm

1. Authenticate, authorize target and requested visibility; derive private room keys including installation/application/tenant and target identity.
2. Register a subscription in buffering mode, then read a consistent projection plus journal high-watermark H. Database replay is authoritative even if the Redis notification was missed.
3. Replay `(client_cursor, H]` from the store. If cursor is expired, return a consistent snapshot at H and mark the gap explicitly.
4. Drain buffered notifications beyond H in sequence, fetching missing rows from the store. Deduplicate overlaps by event ID/sequence. Transition to live delivery.
5. Persist callback cursors; browser reconnect supplies its acknowledged cursors. Periodically compare journal high-watermarks so a lost fanout notification cannot permanently suppress a committed event.

Socket.IO guarantees ordering on a connection but defaults to at-most-once arrival. Application persistence, replay and deduplication provide recoverable delivery; do not promise exactly-once delivery. [Socket.IO delivery guarantees](https://socket.io/docs/v4/delivery-guarantees/).

Start one API process locally. For multiple processes use an `AsyncRedisManager` and compatible load-balancer transport affinity when polling is enabled. Redis is fanout, never the replay ledger. Test ASGI lifespan delegation so socket composition does not skip database pools/outbox startup. Avoid accidental double startup by mounting two independently initialized API objects.

Bound each socket's subscriptions, queue bytes, replay page size and delta frequency. Coalesce/drop only explicitly best-effort deltas. If a durable stream cannot keep up, issue `resync_required` and detach the slow subscription; don't block workers or let memory grow without bound. Suggested qualification defaults: 250 ms delta coalescing and 1 MiB per socket queue, configurable and not claimed as benchmarked capacity.

Reauthorize on connection, subscribe, replay, mutation and credential renewal; revoke membership on expiry/revocation. Validate origins independently of authentication. Cross-tenant existence checks return the same outward behavior as absent targets.

## Event names and subordinate visibility

Close current alias gap through a versioned **read projection** over canonical events: terminalized runs yield the public `run.completed` notification, relevant lifecycle transitions yield `activation.completed`, and actual persisted task creation yields `human_task.opened`. Do not alias every `workflow_run.set_wait` into a human task. The derived notification references the canonical event and has a stable derived ID; no second authoritative transition is appended. Subscription filters operate on the documented public vocabulary with fixtures for old canonical names during migration.

Subordinate identity is a graph: parent execution, native parent/child refs, spawn/call correlation, kind (`provider_subagent`, `agent_server_child`, `linked_mission`), generation and visibility coverage. Provider subagents and linked missions stay distinct. Late child frames attach by stable native refs or remain explicitly unresolved; do not invent lineage by message text.

`include_descendants` expands only to authorized descendants. Display lifecycle/summary even where full child transcript is unavailable; report `full | lifecycle_only | unavailable`. A provider advertising subagents does not prove independent steering, full event forwarding or separately attributable usage. Recursive inspection and aggregate usage must avoid double counting provider-inclusive parent totals.

## Coordinator callbacks

Extend existing durable Subscriptions, webhook/SSE/MCP delivery and outbox. “Callback” means delivery to a registered transport endpoint or authorized coordinator inbox, not invoking a function pointer inside an arbitrary provider session.

Default coordinator filter: review required, blocked/failed, terminal result, significant accepted output, selected child lifecycle. Batch related progress, suppress token/tool delta notifications, dedupe by event ID, cap per-run rate and recursion depth, and carry causation IDs to prevent notification-triggered command loops. Main agent can inspect on demand and acknowledge notifications at a safe turn boundary.

An MCP notification cannot be assumed to wake a disconnected client or become model input. A qualified connected coordinator can subscribe; otherwise keep a durable inbox/poll cursor. A callback that should prompt a coordinator must use its own admitted mailbox and budget, not unsolicited nested provider sends.

Webhook delivery is at least once: signed payload, timestamp, delivery ID, retry schedule and dead-letter disposition; validate destination/egress to prevent arbitrary internal-network requests. Reuse the existing webhook adapter where it already satisfies these requirements. Never retry by rerunning the provider task.
