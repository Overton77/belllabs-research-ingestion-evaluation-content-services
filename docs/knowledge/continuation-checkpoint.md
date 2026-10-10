---
type: Concept
title: Continuation checkpoint and service
description: The Continuation Checkpoint contract, deterministic reduction, triggers, failed-compaction policy and governors, the service that seals and transfers it through a lane hydrator, and the MP-12 production composition - a persisted phase machine the operation workflow drives for Session Lanes (ADR-0041) - plus how compaction is observed or, on Codex, controlled.
tags: [mission-control, continuation, compaction, checkpoints, implementation]
---

# Continuation checkpoint and service

[Continuation](../../GLOSSARY.md) resumes work in a fresh session or workspace;
[Compaction](../../GLOSSARY.md) reduces carried context first. The packet both are built on,
and the lineage rules that keep a unit from being duplicated, are in
[context and continuation](context-and-continuation.md).

Specified (workflow-types/08, SPEC-02): agent sessions are disposable; continuation produces a
validated Continuation Checkpoint and transfers it into a fresh session; a raw transcript is
not a checkpoint; failed compaction parks, retries within a cap, falls back, asks a human or
fails explicitly; transfers are governed by count, cost and no-progress limits; a sealed
checkpoint is corrected only by a superseding one.

Implemented as code and tests:

- `domain/context/checkpoint.py` defines `mc.continuation_checkpoint.v1` (a sealed
  `purpose = continuation` packet plus typed state), `reduce_checkpoint` (the deterministic
  compactor; an admitted compactor's synthesis is merged only into rationale summaries and
  recommended next actions, so a corrupted summary cannot drop queued commands, budgets or
  open Human Tasks), `validate_checkpoint`, the trigger kinds (`context_health_soft`,
  `context_health_hard`, `provider_compaction`, `turn_count`, `workflow_boundary`,
  `request_continuation`), `CompactionFailurePolicy` (retry, fallback compactor, human
  review, fail) and `ContinuationGovernorPolicy` (defaults: 8 transfers, 3 failed
  compactions, 2 no-progress transfers, optional token, cost and wall-clock ceilings).
- `application/context/continuation.py::ContinuationService` records a trigger
  (idempotent by trigger reference), seals at the next safe boundary (governors, mailbox
  freeze, workspace snapshot, reduction, optional compactor, packet, validation,
  `session.checkpoint_sealed`) and transfers through a lane `SessionHydrator`
  (`adapters/deep_agents/compaction.py` for Deep Agents, `adapters/cursor/controls.py` for
  Cursor, see [cursor lane](cursor-lane.md)). State lives in a canonical
  `continuation_checkpoint` row plus its validation verdict
  (`adapters/postgres/context/continuation_repository.py`) and the `continuation_transfer`
  saga row (migration 0027). `GET .../runs/{id}/checkpoints[/{id}]` and
  `missionctl run checkpoint --list|--get` read them (bodies above 4 KiB are digests unless
  `full`).
- `request_continuation` is a command: the API composition (`ContinuationCommands`,
  `bootstrap/composition.py`) records it as `session.continuation_requested` with the lane's
  delivery semantics (`emulated` on every profile).
- Deep Agents compaction is observable: `ObservedSummarizationMiddleware` emits
  `before_compaction` and `after_compaction` frames, which the reducer turns into
  `session.compaction_observed` ([events and commands](events-and-commands.md)).

## Production composition (MP-12, ADR-0041)

Integrated 2026-10-09 and recorded as
[ADR-0041](../adr/0041-continuation-is-a-persisted-phase-machine-driven-by-the-operation-workflow.md)
(`proposed`). Each worker composes one stack (`adapters/temporal/continuation_composition.py`): the
phase service (`application/context/phases.py`, rules in `domain/context/phases.py`), a
`LaneHydratorRegistry` with hydrator and snapshot registrations for Cursor local and cloud,
Claude and Codex (`application/context/hydrators.py`), the context-pressure coordinator
(`domain/context/pressure.py`; soft 0.70, hard 0.85 and reserve 0.15 of the window by default,
`MISSION_CONTROL_CONTEXT_*`) and the mailbox hold oracle. `continuation.*` activities are served on
the cognitive queue beside `lane.turn` (`OperationExecutionActivities(continuation=)`).

At a Session Lane turn's terminal frame `lane.turn` measures occupancy (`ContextOccupancyLane`)
and, when a transfer is pending (a `request_continuation` command or a pressure trigger), ends the
segment without settling the unit. Behind patch `mp12-operation-continuation` the operation
workflow calls `continuation.pending` and `continuation.advance`, one idempotent phase per call:
requested, frozen, snapshotted, sealed, target prepared, hydrated, verified, activated. The phase
is persisted on the `continuation_transfer` row (in its `transfer` jsonb; typed columns are
deferred), so a retried activity resumes from it; one target generation activates per source
generation. From `frozen` until activation the source is fenced: `lane.turn` refuses dispatch
with the retryable `continuation_in_flight`, and held mailbox entries stay `queued`. A handover the
Stop Fence denies settles `cancelled`; an ambiguous one parks `in_doubt`. Delivery of the
continuation turn is `turn_boundary_guaranteed` (Claude) or `wait_then_send` (Codex)
(`domain/context/checkpoint.py`). Codex is the only lane with native compaction control
(`CompactingLane`, [provider lanes](provider-lanes.md)).

Limits: only registered lanes can be continued (the service-level snapshot port refuses others);
registration never marks continuation qualified; a `request_continuation` on a Deep Agents unit is
recorded only (no mid-activity safe boundary); the pressure policy is per worker until bindings
carry it. The canonical checkpoint manifest of RUNTIME-CONTRACTS.md remains a specification;
runtime (LangGraph) checkpoints are evidence only. `RedactedCheckpointSummary` exposes status,
pending interrupts and output references without bodies.

# Citations

- Spec: [SPEC-02](../specs/fast-track-2026-10/SPEC-02-context-packet.md);
  `../mission-control-general/workflow-types/08-CONTINUATION_COMPACTION_AND_TRANSFER.md`.
- ADRs: [0027](../adr/0027-context-packet-tiers-and-workspace-materialization.md),
  [0039](../adr/0039-four-independent-progress-mechanisms-compaction-native-first-continuation-sealed.md),
  [0041](../adr/0041-continuation-is-a-persisted-phase-machine-driven-by-the-operation-workflow.md) (proposed).
- Code: [checkpoint contract](../../src/mission_control/domain/context/checkpoint.py),
  [continuation service](../../src/mission_control/application/context/continuation.py),
  [continuation ledger](../../src/mission_control/adapters/postgres/context/continuation_repository.py),
  [activities](../../src/mission_control/adapters/temporal/activities/continuation.py),
  [checkpoint routes](../../src/mission_control/interfaces/http/continuation.py),
  [Deep Agents compaction and hydrator](../../src/mission_control/adapters/deep_agents/compaction.py),
  [Cursor snapshots and hydrator](../../src/mission_control/adapters/cursor/controls.py),
  [composition of the command side](../../src/mission_control/bootstrap/composition.py),
  [worker continuation composition](../../src/mission_control/adapters/temporal/continuation_composition.py),
  [phase service](../../src/mission_control/application/context/phases.py),
  [phase rules](../../src/mission_control/domain/context/phases.py),
  [pressure policy](../../src/mission_control/domain/context/pressure.py),
  [hydrator registry](../../src/mission_control/application/context/hydrators.py).
- Tests: [checkpoint](../../tests/unit/operations/test_continuation_checkpoint.py),
  [service](../../tests/unit/operations/test_continuation_service.py),
  [activities](../../tests/unit/operations/test_continuation_activities.py),
  [interfaces](../../tests/unit/operations/test_continuation_interfaces.py),
  [in PostgreSQL](../../tests/integration/postgres/test_continuation_postgres.py),
  [Deep Agents](../../tests/integration/deep_agents/test_continuation_deep_agents.py),
  [Cursor controls](../../tests/unit/harness/test_cursor_controls.py),
  [phase machine](../../tests/unit/continuation/test_phase_machine.py),
  [lane turn continuation](../../tests/unit/continuation/test_lane_turn_continuation.py),
  [continuation on Temporal](../../tests/integration/temporal/test_mp12_continuation.py),
  [Claude continuation on Temporal](../../tests/integration/claude/test_claude_continuation_temporal.py),
  [Codex continuation on Temporal](../../tests/integration/codex/test_codex_continuation_temporal.py).
