---
type: Concept
title: Mission events and commands
description: How sequenced mission events leave the ledger through the outbox, how subscriptions and SSE deliver them, the public alias projection that closed the event-name gap B7, the five-state command receipt vocabulary and what the command endpoints accept, separating spec from code. Frames, interventions and the Socket.IO mission stream have their own concepts.
tags: [mission-control, events, commands, streams, frames, subscriptions, implementation]
---

# Mission events and commands

Everything a dashboard, `missionctl`, an MCP tool or the skill shows comes from one stream of
[mission events](../../GLOSSARY.md), with provider detail one level below in
[Provider Frames](../../GLOSSARY.md); everything an operator does to a run is a
[Command](../../GLOSSARY.md) with recorded delivery semantics. This concept separates the
normative contract (workflow-types/09, RUNTIME-CONTRACTS, SPEC-03, SPEC-06) from the records
the code writes today. Admission and the reducer are in [lifecycle](lifecycle.md).

## Events as specified

Workflow 09 fixes the `mc.event.v1` envelope: scope (installation, application, tenant),
`mission_id`, per-mission monotonic `seq` assigned at durable write, `ledger_commit_id`,
`<aggregate>.<past_tense_verb>` types, execution and source references, and payloads that
carry references and digests, never bodies. Events are written to the ledger before any
fan-out (outbox), delivered at least once, deduplicated on `event_id` and gap-checked on
`seq`. Raw provider events go to a separate Native Event Store and never enter the mission
stream. Child mission events do not flow into the parent stream.

## Events as implemented

`adapters/postgres/run_control/canonical.py::append_events` appends one
`mission_control.ledger_commit` with contiguous events and their `mission_control.outbox`
rows in one transaction; sequences are allocated under the mission row lock from
`mission.next_event_seq`, and re-appending an identical envelope is a no-op while conflicting
content raises `IdempotencyConflict`. The envelope is `DomainEventEnvelope` in
`domain/policies/contracts.py` (aggregate `workflow_run`, `sequence`, actor, correlation and
causation). A post-append hook runs inside the same transaction (the chain reducer,
[mission chains](mission-chains.md)). The pending outbox is readable at
`GET /run-control/v1/outbox` for a principal holding `workflow_run.relay` (technical facade
only, see [interfaces](interfaces.md)). The kernel's event types are named after the reducer
action that wrote them (`workflow_run.start`, `workflow_run.set_wait`,
`workflow_run.satisfy_wait`, `workflow_run.terminalize`) plus the frame-derived and
command-derived types below; the full `mc.event.v1` vocabulary of the spec is not emitted
as such.

## Provider frames and the transcript

Lanes persist every provider event as a redacted, digested Provider Frame in the Native Event
Store before anything is derived; only Closing Frames move state, through a pure reducer that
writes mission events pointing back by `source.native_event_ref`; the Transcript is a read view
joining events, frames and artifacts. Run list and search are built on it. Detail, retention and
surfaces are in [provider frames and transcript](provider-frames-and-transcript.md).

## Streams and subscriptions

ADR-0016 requires SSE plus a ticket-based WebSocket adapter. Implemented now:
`GET /v1/applications/{app}/missions/{mission_id}/events` is an SSE stream (replay after
`after_seq` or `Last-Event-ID`, then live, heartbeats, `resync_required` on an expired
cursor; `missionctl events watch`), and durable Subscriptions (`mc.subscription.v1`) deliver
the reference-only `mc.event.v1` envelope by `webhook` (HMAC-SHA256 `X-MC-Signature` over the
raw body, the secret named by an `environment:<NAME>` reference and resolved at send time),
`stream_ticket` or `mcp_session` (`notifications/mission/event`). Endpoints: `POST|GET|DELETE
/subscriptions`, `POST /subscriptions/{id}/resume`, `missionctl subscribe create|list|close`,
MCP `mission_subscribe`. The relay (`application/subscriptions/relay.py`) is an outbox
consumer running in the API process only when `MISSION_CONTROL_SUBSCRIPTION_RELAY=1`
(`bootstrap/subscriptions.py`): at-least-once delivery in `seq` order, the cursor advances
only after a delivery receipt, exponential backoff with full jitter, and the twelfth
consecutive failure dead-letters the subscription and writes `subscription.dead_lettered`
into the mission stream. SSE needs no relay. The Socket.IO `/missions` namespace (MP-14) replays
the same ledger per connection ([mission stream](mission-stream.md)); `POST /stream-tickets` is not
built, and the Native Event Store has no public query beyond the transcript, tail and socket.

**Public aliases (B7 closed in code by MP-13).** Filters match exact names, `*`, or a trailing
`.*` family (`domain/subscriptions/contracts.py`). `application/subscriptions/aliases.py` derives
`run.completed` from `workflow_run.terminalize`, `activation.completed` from `attempt.completed`
and `human_task.opened` from `human_task.created` (written by the Human Gate repository,
[Human Gates](human-gates.md)); the relay, SSE and the socket select through it, at most one
envelope per canonical event, with a stable derived `event_id` and receipts on the canonical id.
Nothing is stored under an alias name. Retries emit one `activation.completed` per attempt.

## Command lifecycle

Specified (workflow 09 section 7, GLOSSARY): lifecycle `accepted, queued, delivered, observed,
completed`, outcome `applied | failed | rejected | expired`, a
[Delivery Report](../../GLOSSARY.md) naming one of `turn_boundary_guaranteed`,
`cooperative_inject`, `cancel_and_replace`, `wait_then_send`, `pause_at_tool_gate`,
`emulated`, `unsupported`, and, per ADR-0008, an urgent cancel that first persists a
[Stop Fence](../../GLOSSARY.md) rejecting new effect claims.

Implemented: `ReceiptState` (`domain/policies/contracts.py`) is now the five-state vocabulary
with terminal outcomes: `accepted`, `queued`, `delivered`, `observed`, then `applied`,
`rejected`, `expired` or `failed`. Boundary commands (`pause`, `resume`, `satisfy_wait`,
`cancel`, `reconcile_unit`, `BOUNDARY_COMMAND_KINDS`) keep the RRM-007 path: delivery is a
Temporal Update through the root, the family or the cancel path
(`adapters/temporal/boundary_commands.py`), and only the family's acknowledgement counts as
`delivered`. Three sequence spaces keep cancels from opening gaps (`execution`, `cancel`,
`boundary:<family>`). A `mission_control.delivery_report` row carries `delivery_semantics`.

Queued content, interrupt-and-inject, immediate cancel with a persisted Stop Fence and
`request_continuation` are described in [interventions](interventions.md): `queue_instruction` and
`add_context` write a durable per-generation mailbox entry claimed at the next boundary,
`interrupt_and_inject` acts by the lane's declared semantics (`cancel_and_replace` on every
first-wave lane), and `cancel` with `urgency: immediate` persists an insert-only fence before any
provider cancel so a fenced effect is denied `STOP_FENCED`.

## What the endpoints accept

`POST /v1/applications/{application_id}/runs/{run_id}/commands`
(`interfaces/http/mission_control.py`) takes `MissionCommandRequest` (`mc.command.v1`,
`contracts/contracts.py`), whose `kind` is `pause`, `resume`, `cancel`, `satisfy_wait`,
`queue_instruction`, `add_context`, `interrupt_and_inject` or `request_continuation`.
`application/missions/service.py` maps pause, resume, satisfy-wait and normal-urgency cancel
to reducer actions, and admits queue, add-context, inject, immediate cancel and continuation
only where the corresponding mailbox, stop fence or continuation store is composed
(otherwise `unsupported_control`). The response is 202 when admitted, 200 on exact replay, 409
when stale and 422 when rejected; a supplied `Idempotency-Key` must equal `request_id`.
`missionctl command send|list|queue|inject|cancel` and MCP `mission_command_send` call the same
service; `command queue --boundary`, `inject` and `cancel --urgency` were added by FT-F1,
F2 and the readiness pass. The technical `POST /run-control/v1/runs/{run_id}/commands`
accepts any `LifecycleCommand` action except `reconcile_unit`, which has its own privileged
route. Inspection (`mc.inspection.v1`) gains optional reference-only sections for lane,
sessions, frames cursor, mailbox, delivery reports, chain, subscriptions and stop fence
(`application/missions/inspection.py`).

## Open differences to report, not resolve

`satisfy_wait` exists in code but not in the spec vocabulary; spec scopes (`mission.command`)
versus code grants (`workflow_run.*`); the `mc.event.v1` vocabulary (aliases cover three names);
no stream tickets; `request_continuation` records but does not run (MP-12 is in flight, not
integrated). SPEC-07 names the
Cursor kernel hook script `mc_hook.py`, the code writes `.mission/hooks/kernel.py`; the code
name is current.

# Citations

- Spec: [SPEC-03](../specs/fast-track-2026-10/SPEC-03-mission-state-and-transcript.md),
  [SPEC-06](../specs/fast-track-2026-10/SPEC-06-interventions-inspection-subscriptions.md),
  [owner runbook](../specs/fast-track-2026-10/OWNER-FIXTURE-RUNBOOK.md) (B7);
  `../mission-control-general/workflow-types/09-EVENTS_COMMANDS_AND_STREAMS.md`;
  `../mission-control-general/general-mission-control/RUNTIME-CONTRACTS.md` (public operation
  additions and wire protocol);
  `../mission-control-general/general-mission-control/expansion/EXPERIENCE-AND-STREAMS.md`.
- ADRs: [0004](../adr/0004-temporal-sole-scheduler-transactional-outbox.md),
  [0008](../adr/0008-ordered-commands-with-urgent-stop-fence.md),
  [0016](../adr/0016-required-ui-surfaces-with-fallback.md),
  [0032](../adr/0032-interventions-real-queue-inject-cancel-fork-and-subscriptions.md).
- Code: [event append](../../src/mission_control/adapters/postgres/run_control/canonical.py), [policy contracts](../../src/mission_control/domain/policies/contracts.py),
  [boundary commands](../../src/mission_control/domain/policies/boundary_commands.py),
  [Temporal delivery](../../src/mission_control/adapters/temporal/boundary_commands.py),
  [subscription contracts](../../src/mission_control/domain/subscriptions/contracts.py),
  [subscription relay](../../src/mission_control/application/subscriptions/relay.py), [public aliases](../../src/mission_control/application/subscriptions/aliases.py),
  [subscription store](../../src/mission_control/adapters/postgres/subscriptions/store.py),
  [subscription routes](../../src/mission_control/interfaces/http/subscriptions.py),
  [inspection enrichment](../../src/mission_control/application/missions/inspection.py), [scoped router](../../src/mission_control/interfaces/http/mission_control.py),
  [public request contracts](../../src/mission_control/contracts/contracts.py), [mission service](../../src/mission_control/application/missions/service.py).
- Tests: [boundary commands](../../tests/unit/run_control/test_boundary_commands.py), [RRM-007 API](../../tests/acceptance/control_plane/test_rrm_007_api.py),
  [PostgreSQL lifecycle](../../tests/integration/postgres/test_mission_control_lifecycle_postgres.py),
  [subscriptions](../../tests/unit/subscriptions/test_subscriptions.py), [subscriptions in PostgreSQL](../../tests/integration/postgres/test_subscriptions.py),
  [aliases](../../tests/unit/subscriptions/test_aliases.py), [aliases in PostgreSQL](../../tests/integration/postgres/test_provider_lineage_postgres.py),
  [inspection](../../tests/unit/run_control/test_ft_f6_inspection.py).
