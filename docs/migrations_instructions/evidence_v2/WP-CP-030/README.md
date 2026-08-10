# WP-CP-030 implementation evidence

Disposition: `accepted`  
Recorded: 2026-08-10  
Contracts: `CON-CP-TEMPORAL-IDENTITY-V1`, `CON-CP-WORKFLOW-MESSAGE-V1`,
`CON-CP-LINKED-RUN-V1`, `CON-CP-CONTINUATION-V1`  
Workflow schema versions: `belllabs.temporal-root.v1`,
`belllabs.operation-workflow.v1`, `belllabs.operation-result.v1`  
Qualifications: `QUAL-CP-TEMPORAL-REPLAY-RECOVERY`, `QUAL-CP-LINKED-RUN-SEMANTICS`

## Requirement-to-test/evidence map

| Requirement | Executable evidence |
|---|---|
| REQ-CP-EXEC-001 | `tests/test_coordinator_temporal_runtime.py::test_family_submitter_and_worker_set_run_and_replay_both_families` proves stable root-only submission and duplicate-start reuse. |
| REQ-CP-EXEC-002 | The same coordinator qualification executes StageGraph and GoalDirected through `BellLabsRunWorkflow`. |
| REQ-CP-EXEC-003 | `tests/test_operation_execution.py` covers stable semantic operation idempotency/recovery; root family tests prove operation-child registration. |
| REQ-CP-EXEC-004 | The coordinator qualification captures and replays both root histories with the root/family/operation registry. |
| REQ-CP-EXEC-005 | `tests/acceptance/control_plane/test_wp_cp_030.py` covers generation-bound messages and stable semantic operation identity. |
| REQ-CP-EXEC-006 | `test_ordered_message_receipts_reject_gaps_duplicates_and_late_generations` covers ordered receipt progression and durable rejection outcomes. |
| REQ-CP-EXEC-007 | Root Signal, Update, and Query handlers are registered; the acceptance test exercises their common deterministic receipt reducer. |
| REQ-CP-EXEC-008 | `tests/test_linked_runs.py` covers cancellation request/late-result disposition and `tests/test_run_control.py` covers authoritative effect settlement. |
| REQ-CP-EXEC-009 | `tests/test_linked_runs.py` proves independent child compile/admission, exact execution binding, all four dependency policies, and cancellation. |
| REQ-CP-EXEC-010 | Linked-run result admission, rejection, deferral, degradation, terminal-parent late-result rejection, and non-authoritative raw child output are covered in `tests/test_linked_runs.py`. |
| REQ-CP-EXEC-011 | `test_continue_as_new_advances_only_technical_segment_and_preserves_semantics` covers forced continuation state; the root calls Temporal Continue-As-New with that exact next-segment contract. |
| REQ-CP-EXEC-012 | `test_semantic_fork_starts_epoch_one_without_live_children_or_messages` proves new run/epoch isolation. |

## Runtime hierarchy and identities

```text
belllabs.run.v1                  workflow id belllabs-run/{run_id}
  belllabs.stagegraph | belllabs.goal-directed
                                  child id family/{run_id}/{execution_epoch}
    belllabs.operation.v1         child id operation/{semantic_attempt_id}
```

`TemporalWorkflowSubmitter` ignores caller-supplied workflow identity and derives the sole root
identity from the admitted `run_id`. It uses reject-duplicate plus use-existing policies. Root
inputs contain stable identities, digests, references, the family discriminator, compact
continuity state, and the exact family task queue. Production root submissions set
`durable_operation_children=True`; direct-family activity mode remains only for isolated legacy
interpreter tests and is not reachable through the active submitter.

The root exposes `signal_message`, the acknowledged `deliver_message` Update,
`message_receipts`/`continuity` Queries, and `request_cancel`. Receipt reduction accepts only the
next sequence for the current execution generation; duplicates, gaps, and stale generations are
recorded without advancing the accepted frontier.

Continue-As-New increments only `technical_segment`. Epoch, generation, family identity, active
operation identities, pending message identities, receipts/frontier, balances, and linked-run
identities remain stable. Semantic fork creates a new run at epoch/segment one and deliberately
copies no active child or pending message.

## Registration and logical queues

- `app/temporal/registration/workflows.py` is the single workflow registry.
- `app/temporal/registration/activities.py` is the single activity selection registry.
- `app/temporal/registration/task_queues.py` owns five logical isolation classes:
  `coordinator-family`, `agent-cognitive`, `ingestion-io`, `sandbox-external-job`, and
  `verification-reconciliation`.
- StageGraph and GoalDirected physical queues are declared subdivisions of the coordinator-family
  logical class. Both active worker factories consume the workflow and activity registries.

## Changed WP-CP-030 paths

- `app/domain/orchestration/contracts.py`
- `app/domain/operation_execution/contracts.py`
- `app/integrations/temporal_workflow_submission.py`
- `app/temporal/workflows/`
- `app/temporal/activities/`
- `app/temporal/registration/`
- `app/temporal/stagegraph_workflow.py`
- `app/temporal/goal_directed_workflow.py`
- `app/temporal/linked_run_workflow.py`
- `app/temporal/orchestration_activities.py`
- `app/temporal/goal_directed_activities.py`
- `app/temporal/coordinator_runtime.py`
- `app/temporal/worker.py`
- `app/temporal/run_probe.py`
- `tests/test_coordinator_temporal_runtime.py`
- `tests/test_linked_runs.py`
- `tests/acceptance/control_plane/test_wp_cp_030.py`
- package instruction, traceability, and this evidence file

## Verification

```text
.\.venv\Scripts\python.exe -m pytest -q \
  tests/acceptance/control_plane/test_wp_cp_030.py \
  tests/test_coordinator_temporal_runtime.py \
  tests/test_stagegraph_orchestration.py \
  tests/test_goal_directed_temporal.py \
  tests/test_linked_runs.py
30 passed

.\.venv\Scripts\python.exe -m pytest -q \
  tests/test_operation_execution.py \
  tests/test_generic_artifact_workflow.py \
  tests/test_web_research_temporal_smoke.py
14 passed

.\.venv\Scripts\python.exe -m ruff check app tests
All checks passed!

.\.venv\Scripts\python.exe -m mypy app
Success: no issues found in 329 source files

.\.venv\Scripts\python.exe -m pytest -q --tb=short
570 passed, 32 skipped, 46 warnings in 43.43s

git diff --check
exit 0; line-ending conversion warnings only
```

The default skips are service/credential gated. Temporal time-skipping and captured-history replay
ran locally. Drift searches found no direct family `start_workflow` submission and no forbidden
database/network/provider I/O in canonical workflow owners. Agent Server macro graph files remain
as pre-existing bounded/development assets and are not registered or submitted by this runtime.

## Replacement inventory and rollback

| Prior active responsibility | Disposition |
|---|---|
| Direct family Temporal submission | Replaced by root-only `TemporalWorkflowSubmitter`. |
| Direct family operation activity execution | Active root inputs select typed `OperationWorkflow` children; direct activity mode is isolated to family interpreter tests. |
| Scattered workflow/activity registration | Active coordinator workers consume the canonical registries. |
| Ad hoc coordinator queue naming | Physical family queues derive from the declared coordinator-family logical class. |
| Prototype workflow fallback | None retained in active submission; rollback is repository/deployment revert before production adoption. |

## Final disposition

Every WP-CP-030 requirement has executable evidence. Stable root and operation identities,
cross-family execution, captured-history replay, ordered messaging, generation rejection,
cancellation/late-result policy, linked-run admission, continuity, semantic fork, centralized
registration, drift checks, and shared repository gates pass. WP-CP-030 is accepted.
