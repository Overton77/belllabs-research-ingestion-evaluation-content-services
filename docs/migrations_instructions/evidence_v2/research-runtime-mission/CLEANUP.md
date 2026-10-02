# Research-runtime mission clean-code passes

## CR-1

Scope: checkpoint lineage and recovery (RRM-003 and RRM-004). Base `a9c3f1d`; the code and tests changed by `git diff --stat 57c99bd..a9c3f1d -- app tests`. Branch `cleanup/rrm-cr-1`.

### Findings
- Names in scope already use the `unit` / `attempt` / `generation` / `checkpoint` / `receipt` vocabulary. No `v2`/`new`/`tmp`/`helper`/`manager`/`util` names were added by the mission (`belllabs.operation.v2` is a persisted workflow name and stays).
- Placement is sound. Contracts and decisions are in `domain/operation_execution`, ports/services in `application/operations`, the Temporal nudge and DeepAgents adapter in `integrations`, and the workflow stays deterministic (`workflow.patched`, no I/O).
- Dead code: `_runtime_outbox_events` in `journaled_operation_execution.py` had no caller anywhere.
- Duplication: `adapter.py` and `checkpoint_verifier.py` each carried the root-namespace checkpoint config builder, the parent-id read and the lineage walk bound.

### Changes
1. `refactor:` deleted the unreferenced `_runtime_outbox_events` (and its now-unused `DomainEventEnvelope` import).
2. `refactor:` new `app/integrations/agents/deep_agents/checkpoint_reads.py` (`root_checkpoint_config`, `checkpoint_parent_id`, `MAX_LINEAGE_WALK`), used by `adapter.py` and `checkpoint_verifier.py`.

### Deliberate non-changes
- `SemanticOperationAttemptKey` and `LangGraphCheckpointKey` are kept. They are marked superseded but are not inert: `LangGraphCheckpointKey` is used by `runtime_interventions.py`, `runtime_reconciliation.py` and `graph_runtime/contracts.py`, and both feed `governance.py` schema exports and several unit/integration tests. They fall to RRM-006/007 (fork, snapshot, intervention paths) and CR-3.
- `_stable_id` (uuid5) exists in four modules (`operation_execution`, `run_control/service`, `run_control/reducer`, `conformance_operation_runtime`), and `_dump`/`_load` in both Postgres repositories. These predate the mission or are tiny; extracting churns files other tickets edit.
- `application/operations/checkpoint_lineage.py` (~1200 lines) holds the repository port, pure decision functions, the in-memory repository and the service. It is cohesive around one protocol; splitting is deferred (CR-3 could move the in-memory repository to a testing module).
- Unreferenced pre-mission symbols (`ArtifactPromotionPort`, `SnapshotPort`, `StageOperationExecutor`, `WorkflowEvaluator`, `UnsupportedWorkspaceRequirement`, `verify_digest`) were left. They are protocols or exceptions possibly used structurally, and not mission-owned.
- Test files named by ticket (`test_rrm_004_worker_restart_recovery.py`) were left; evidence READMEs reference them.
- No persisted or wire identity was touched. No file moves, so no path-safety sweep was needed. `async_subagents.py`, `application/async_subagents/` and `app/agent_server/` were not touched.

### Commands and results
- `ruff check app tests scripts`: pass. `mypy app`: pass (341 files).
- Hermetic pytest: 748 passed, 54 skipped, 2 xfailed (equals baseline).
- Pytest with Postgres and Mongo: 778 passed, 24 skipped, 2 xfailed (equals baseline).
- `git diff --check`: clean.

### Candidate generalization seams (not extracted)
- Runtime-unit identity plus lease/fence/generation (`RuntimeUnitIdentity`, claim lease, `decide_lease`): independent of family, company and provider.
- `CheckpointLineageRepository` port with in-memory and Postgres implementations: a recovery ledger usable for any resumable provider unit.
- `checkpoint_reads.py` and the classifier in `adapter.py`: a provider-neutral checkpoint-classification step behind a small port; only the LangGraph reads are provider-specific.
- `UnitReconciliationNudge` and the park-in-doubt workflow loop: a reusable operator-decision wake-up pattern.
- Shared `_stable_id` and JSON dump helpers: one `domain/control_plane` identity helper.

### CR-1 integration

The coordinator checked the diff independently: it extracts shared checkpoint-read helpers and deletes one unreferenced function, with no wire or persisted identity changes. It was merged `--no-ff` into integration at `08def14`. Merge gates:
- ruff: clean.
- mypy: 341 files, no issues.
- Hermetic pytest: 748 passed, 54 skipped, 2 xfailed.
- Pytest with the disposable Postgres/Mongo stack and `--env-file`: 778 passed, 24 skipped, 2 xfailed.

