# RRM-016 — Compose GoalDirected operations with the run-control journal and operation authority

**What to build:** GoalDirected executor and verifier operations settle through the same governed path as StageGraph operations: `RunControlOperationAuthority` verification, the journaled effect claim, the fenced result observation and an accepted run-control settlement, with usage recorded exactly once.

**Blocked by:** RRM-004 (journal and fence), RRM-007 (it edits `goal_directed.py`; land after it or coordinate the region).
**Blocks:** **RRM-010** (mission-blocking, coordinator decision 2026-10-02 after the RRM-006 review); RRM-009 production composition of GoalDirected cognition; governed GoalDirected fork evidence (RRM-006 snapshots see GoalDirected units as `excluded/not_accepted`, and the effect-quiescence check is vacuous for them).
**Status:** implemented; independent review pending ([evidence](../../../evidence_v2/research-runtime-mission/RRM-016/README.md)); **required before RRM-010**
**Branch:** `wp/rrm-016-goal-directed-journaled-operations`
**Authority:** REQ-CP-RUN-007 (claimed and reconciled effects), REQ-CP-RUN-006/009 (budgets settle once), REQ-CP-EXEC-014 (claim fence), REQ-BP-GD-011/012

## Diagnosis (RRM-006, 2026-10-02)

The RRM-006 GoalDirected demonstration had to run `OperationExecutionService` with `journal=None` and an accepting authority, because the two halves are not composable today:

- `GoalDirectedWorkflow` records each operation's usage itself (`goal:usage:{reservation_id}` `record_usage`) and carries its own `run_version` across the operation. `JournaledOperationExecutionCoordinator` also claims, observes and settles the same reservation in run control, so the family's next lifecycle command is stale and the usage would be recorded twice (`app/temporal/workflows/goal_directed.py` `_settle_operation`, `_lifecycle`; `app/application/operations/journaled_operation_execution.py` `settle`).
- `RunControlOperationAuthority._verify_bound_authority` rejects the `authored_instruction` goal-context segment the preparer appends (`goal-context:{run}:{iteration}:{role}` is not a configured prompt source) and requires `run.version == run_control_revision` at first verification (`app/application/orchestration/goal_directed.py` `_prompt_segments`; `app/application/operations/operation_execution.py`).

Consequence: GoalDirected settlements live in the operation binding store and the lineage result observation, not as run-control accepted operation settlements; RRM-006 snapshots list such units as `excluded` (`not_accepted`). GoalDirected units are not reusable in any case (their revision identity is run-bound), so forks are unaffected; production effect and budget accounting is.

Found during implementation (RRM-016): a third incompatibility. GoalDirected operations bound no compiled workspace slots (`slot_bindings=()`, writable `/goal/{i}/{role}/work`), which the real authority rejects (`operation writable paths do not exactly match its workspace contract`); RRM-006's accepting authority masked it. Resolved by binding the exact compiled slots under the role-scoped root, which the authority recomputes from the unit identity. See the evidence README.

## Acceptance

- [x] One GoalDirected composition in which every executor/verifier operation is verified by run-control authority, journaled, fenced and settled exactly once, and the family consumes that settlement (no second `record_usage`, no stale run version).
- [x] The goal-context segment is admitted under an explicit, non-privileged trust class or an accepted configuration source; no privileged prompt bypass.
- [x] Crash before and after settlement converges to one settlement (RRM-004 harness) for a GoalDirected unit.
- [x] Captured-history replay for GoalDirected (additive inputs, `workflow.patched` where commands change).
