---
type: Concept
title: Mission events and commands
description: How sequenced mission events leave the ledger through the outbox and how admitted commands reach a run, separating what the spec requires from what the code records today.
tags: [mission-control, events, commands, streams, implementation]
---

# Mission events and commands

Everything a dashboard, `missionctl`, an MCP tool or the skill shows comes from one
stream of [mission events](../../GLOSSARY.md); everything an operator does to a run is a
[Command](../../GLOSSARY.md) with recorded delivery semantics. This concept separates the
normative contract (workflow-types/09, RUNTIME-CONTRACTS) from the records the code
writes today. Admission and the reducer are in [lifecycle](lifecycle.md).

## Events as specified

Workflow 09 fixes the `mc.event.v1` envelope: scope (installation, application, tenant),
`mission_id`, per-mission monotonic `seq` assigned at durable write, `ledger_commit_id`,
`<aggregate>.<past_tense_verb>` types, execution and source references, and payloads
that carry references and digests, never bodies. Events are written to the ledger before
any fan-out (outbox), delivered at least once, deduplicated on `event_id` and gap-checked
on `seq`. Raw provider events go to a separate Native Event Store and never enter the
mission stream. Child mission events do not flow into the parent stream.

## Events as implemented

`adapters/postgres/run_control/canonical.py::append_events` appends one
`mission_control.ledger_commit` with contiguous events and their `mission_control.outbox`
rows in one transaction; sequences are allocated under the mission row lock from
`mission.next_event_seq`, and re-appending an identical envelope is a no-op while
conflicting content raises `IdempotencyConflict`. The envelope is
`DomainEventEnvelope` in `domain/policies/contracts.py` (aggregate `workflow_run`,
`sequence`, actor, correlation and causation), with `OutboxRecord` and `ConsumerCursor`
for relay bookkeeping. The pending outbox is readable at `GET /run-control/v1/outbox`
for a principal holding `workflow_run.relay` (technical facade only, see
[interfaces](interfaces.md)). The `mc.event.v1` wire shape, `event_type` vocabulary and
Native Event Store are specified only.

## Streams

Specified: `GET .../missions/{id}/events?after_seq=` as SSE with replay then live,
heartbeats, gap detection and `CURSOR_EXPIRED` resync, plus a required ticket-based
WebSocket adapter (`POST /stream-tickets`, `WS /streams?ticket=`) that reads the same
durable tail (expansion/EXPERIENCE-AND-STREAMS.md; ADR-0016). Implemented: nothing in
`src/mission_control` serves SSE or WebSocket; the only `text/event-stream` mention is the
MCP client's accept header in `interfaces/mcp/coordinator_http_client.py`. Readers today
poll `GET /runs/{run_id}/commands` and inspection.

## Command lifecycle

Specified (workflow 09 section 7, GLOSSARY): lifecycle `accepted, queued, delivered,
observed, completed`, outcome `applied | failed | rejected | expired`, a
[Delivery Report](../../GLOSSARY.md) naming one of `turn_boundary_guaranteed`,
`cooperative_inject`, `cancel_and_replace`, `wait_then_send`, `pause_at_tool_gate`,
`emulated`, `unsupported`, and, per ADR-0008, an urgent cancel that first persists a
[Stop Fence](../../GLOSSARY.md) rejecting new effect claims. The initial endpoint is
required to support `pause`, `resume`, `cancel`, `queue_instruction` and
`interrupt_and_inject`; `retry`, `rerun`, `fork`, `request_continuation` and
`request_revision` use dedicated routes.

Implemented: `domain/policies/contracts.py` defines `BOUNDARY_COMMAND_KINDS` as
`pause`, `resume`, `satisfy_wait`, `cancel`, `reconcile_unit`; the family boundary applies
the first three, cancellation is applied by the terminal outcome and `reconcile_unit` by
the operation boundary. Receipts (`ReceiptState`) are `accepted`, `delivered`, `applied`
and `rejected`, four states rather than the spec's five, and three sequence spaces
(`execution`, `cancel`, `boundary:<family>`) keep cancels from opening gaps at the root.
`domain/policies/boundary_commands.py` decides the target boundary (`run_control`, `root`,
`family`, `unit`) from the projection alone and shapes the receipts run control writes in
one commit. Delivery is a Temporal Update through the root (`deliver_message`), the family
(`deliver_boundary_command`) or the cancel path (`deliver_cancel`) in
`adapters/temporal/boundary_commands.py`; only the family's acknowledgement counts as
`delivered`. A `mission_control.delivery_report` row with a `delivery_semantics` column
is written by `adapters/postgres/runtime/runtime_execution_repository.py`.

## What the current endpoints accept

The application-scoped `POST /v1/applications/{application_id}/runs/{run_id}/commands`
(`interfaces/http/mission_control.py`) takes `MissionCommandRequest` (`mc.command.v1`,
`contracts/contracts.py`) whose `kind` literal lists `pause`, `resume`, `cancel`,
`satisfy_wait`, `queue_instruction`, `interrupt_and_inject`. The handler in
`application/missions/service.py` maps pause, resume, satisfy-wait and normal-urgency
cancel to reducer actions and rejects `queue_instruction`, `interrupt_and_inject` and
`cancel` with `urgency=immediate` as `unsupported_control`. The response is 202 when
admitted, 200 on exact replay, 409 when stale and 422 when rejected; a supplied
`Idempotency-Key` must equal `request_id`. The technical
`POST /run-control/v1/runs/{run_id}/commands` (`interfaces/http/run_control.py`) accepts
any `LifecycleCommand` action except `reconcile_unit`, which has its own privileged route.

No persisted stop fence named as in ADR-0008 was found in the code opened here; the
cancel path records cancel delivery and relies on claim and generation fences
([context and continuation](context-and-continuation.md)).

## Open differences to report, not resolve

Five-state spec lifecycle versus four receipt states in code; `satisfy_wait` exists in
code but not in the spec vocabulary; `queue_instruction` and `interrupt_and_inject` are
declared in the request literal but rejected; spec scopes (`mission.command`) versus code
grants (`workflow_run.*`); SSE, WebSocket and the Native Event Store are absent.

# Citations

- Spec: `../mission-control-general/workflow-types/09-EVENTS_COMMANDS_AND_STREAMS.md`;
  `../mission-control-general/general-mission-control/RUNTIME-CONTRACTS.md` (public
  operation additions and wire protocol);
  `../mission-control-general/general-mission-control/expansion/EXPERIENCE-AND-STREAMS.md`;
  `../mission-control-general/general-mission-control/SPECIFICATION.md` (events paragraph).
- ADRs: [0008](../adr/0008-ordered-commands-with-urgent-stop-fence.md), [0004](../adr/0004-temporal-sole-scheduler-transactional-outbox.md), [0016](../adr/0016-required-ui-surfaces-with-fallback.md).
- Code: [event append](../../src/mission_control/adapters/postgres/run_control/canonical.py),
  [policy contracts](../../src/mission_control/domain/policies/contracts.py),
  [boundary commands](../../src/mission_control/domain/policies/boundary_commands.py),
  [Temporal delivery](../../src/mission_control/adapters/temporal/boundary_commands.py),
  [scoped router](../../src/mission_control/interfaces/http/mission_control.py),
  [technical run control](../../src/mission_control/interfaces/http/run_control.py),
  [public request contracts](../../src/mission_control/contracts/contracts.py),
  [mission service](../../src/mission_control/application/missions/service.py).
- Tests: [boundary commands](../../tests/unit/run_control/test_boundary_commands.py),
  [RRM-007 API](../../tests/acceptance/control_plane/test_rrm_007_api.py),
  [PostgreSQL lifecycle](../../tests/integration/postgres/test_mission_control_lifecycle_postgres.py).
