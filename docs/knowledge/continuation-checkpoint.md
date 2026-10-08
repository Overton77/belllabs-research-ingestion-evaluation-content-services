---
type: Concept
title: Continuation checkpoint and service
description: The Continuation Checkpoint contract, deterministic reduction, triggers, failed-compaction policy and governors, the service that seals and transfers it through a lane hydrator, how Deep Agents compaction is observed, and the limitation that the service is not composed or called by a workflow.
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

**Current limitation.** `ContinuationService` is not composed in the API or the worker
(`bootstrap/` builds only `ContinuationCommands`), the activities `continuation.seal`,
`continuation.transfer` and `continuation.release`
(`adapters/temporal/activities/continuation.py`) are registered on no worker queue, and no
family workflow calls them. A `request_continuation` command is therefore recorded, but no
worker seals a checkpoint or provisions the fresh session. The proofs exercise the service
directly (unit, `common_db`, Deep Agents with a local model, Cursor controls with a replaying
bridge). No context-health policy that raises the soft or hard triggers from live token
counts was found. The canonical checkpoint manifest of RUNTIME-CONTRACTS.md remains a
specification; runtime (LangGraph) checkpoints are evidence only. `RedactedCheckpointSummary`
exposes status, pending interrupts and output references without bodies, and
`ForkFromCheckpointIntervention` cannot cross request scope.

# Citations

- Spec: [SPEC-02](../specs/fast-track-2026-10/SPEC-02-context-packet.md);
  `../mission-control-general/workflow-types/08-CONTINUATION_COMPACTION_AND_TRANSFER.md`.
- ADR: [0027](../adr/0027-context-packet-tiers-and-workspace-materialization.md).
- Code: [checkpoint contract](../../src/mission_control/domain/context/checkpoint.py),
  [continuation service](../../src/mission_control/application/context/continuation.py),
  [continuation ledger](../../src/mission_control/adapters/postgres/context/continuation_repository.py),
  [activities](../../src/mission_control/adapters/temporal/activities/continuation.py),
  [checkpoint routes](../../src/mission_control/interfaces/http/continuation.py),
  [Deep Agents compaction and hydrator](../../src/mission_control/adapters/deep_agents/compaction.py),
  [Cursor snapshots and hydrator](../../src/mission_control/adapters/cursor/controls.py),
  [composition of the command side](../../src/mission_control/bootstrap/composition.py).
- Tests: [checkpoint](../../tests/unit/operations/test_continuation_checkpoint.py),
  [service](../../tests/unit/operations/test_continuation_service.py),
  [activities](../../tests/unit/operations/test_continuation_activities.py),
  [interfaces](../../tests/unit/operations/test_continuation_interfaces.py),
  [in PostgreSQL](../../tests/integration/postgres/test_continuation_postgres.py),
  [Deep Agents](../../tests/integration/deep_agents/test_continuation_deep_agents.py),
  [Cursor controls](../../tests/unit/harness/test_cursor_controls.py).
