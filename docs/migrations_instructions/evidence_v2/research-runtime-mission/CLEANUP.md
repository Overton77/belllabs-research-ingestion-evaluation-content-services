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


## CR-2

Scope: async-subagent deployment and graph files (RRM-013), runtime inspection API, projection and read models (RRM-005). Base `6e77850`; code changed by `git diff 08def14..6e77850 --stat -- app tests`. Branch `cleanup/rrm-cr-2`.

### Findings
- Names follow the snapshot / checkpoint / unit / attempt / generation / receipt vocabulary. No `v2`/`new`/`tmp`/`helper`/`manager`/`util` names were added.
- Placement is sound: contracts in `domain/run_control` and `domain/operation_execution`, services and ports in `application`, Temporal/DeepAgents/Agent Server adapters in `integrations`, `agent_server` and `temporal`, transport in `api`.
- `application/run_control/inspection.py` held the cursor codec, which has no dependency on the rest of the module.
- Three symbols had no reference anywhere (app, tests, scripts, docs): `RunListFilter`, `AsyncChildLineageReader`, `SearchAttributeValueType`.

### Changes
1. `refactor:` `InspectionCursorCodec`, `CURSOR_TTL` and `Clock` moved to `app/application/run_control/inspection_cursor.py`. `inspection.py` imports them, so all existing import paths (`api/runtime_inspection.py`, tests) still resolve. Cursor format and signing prefix are unchanged.
2. `refactor:` deleted `RunListFilter`, `AsyncChildLineageReader`, `SearchAttributeValueType`.

### Deliberate non-changes
- `InMemoryInspectionReadRepository` stays in `inspection.py`: it depends on `RunRecord`/`RunSnapshot`/`UnitRecord`, and moving it would need a re-export cycle or break tests on RRM-006's branch that import it from here.
- `InspectionReadRepository`, `classify_async_children_for_fork`, `AsyncSubagentService` untouched (RRM-006).
- `SEARCH_ATTRIBUTES_DISABLED` is unreferenced but mirrors `SEARCH_ATTRIBUTES_REQUIRED`; kept for symmetry.
- `service.py` (~1,600 lines) in `async_subagents` and `integrations/.../async_subagents.py` (~900) stay whole: each is cohesive around one service/adapter and RRM-006 and RRM-016 edit nearby.
- No wire or persisted identity touched (graph IDs, `/belllabs/async-subagents/served-graphs`, `/run-control/v1/inspection/...`, Temporal names, search-attribute names, stamps, migrations). No file moved except adding one new module; `langgraph.async_subagents.*` untouched, so no path sweep was needed.
- Ticket-named test files kept; evidence READMEs reference them.

### Commands and results
- `ruff check app tests scripts`: pass. `mypy app`: pass (362 files).
- Hermetic pytest: 913 passed, 65 skipped, 2 xfailed (equals baseline).
- Pytest with Postgres and Mongo (`uv run --env-file ...`): 948 passed, 30 skipped, 2 xfailed (equals baseline).
- `git diff --check`: clean.

### Candidate generalization seams (not extracted)
- `inspection_cursor.py`: a signed, scoped, expiring cursor codec usable for any paged read API.
- `RuntimeInspectionService` source ports (`TemporalVisibilityReader`, `CheckpointHistoryReader`, `AsyncChildDetailReader`): provider-neutral multi-source read composition with per-section freshness.
- Async-child submission fence and reconciliation decisions: a generic "provider-run adoption with in-doubt incident" pattern independent of Agent Server.

### CR-2 integration

The coordinator checked the diff independently: the cursor codec was extracted unchanged and three unreferenced symbols were deleted, with no wire or persisted identity changes. It was merged `--no-ff` at `c850526`. Merge gates:
- ruff: clean.
- mypy: 362 files, no issues.
- Hermetic pytest: 913 passed, 65 skipped, 2 xfailed.
- Pytest with the disposable Postgres/Mongo stack and `--env-file`: 948 passed, 30 skipped, 2 xfailed.



## CR-3

Scope: snapshot, fork and patch modules; command, receipt and intervention paths (RRM-006 and RRM-007). Base `2799e17`; code changed by `git diff ac7daf9..2799e17 --stat -- app tests` plus RRM-007's boundary-command modules. Branch `cleanup/rrm-cr-3`.

### Findings
- Names in scope use the snapshot / fork / unit / boundary-command / receipt vocabulary. No `v2`/`new`/`tmp`/`helper`/`manager`/`util` names were added.
- Placement is sound: contracts in `domain/run_control` (`forks`, `boundary_commands`), saga and ports in `application/runtime` and `application/run_control`, Temporal adapter in `integrations/temporal_boundary_commands.py`, activities in `temporal/boundary_activities.py`, transport in `api`.
- A symbol-level scan of every scope module (definitions with no reference elsewhere in app, tests, scripts) found four unreferenced items: `InMemoryForkSourceReader`, its only helper `InMemoryRunControlRepository.family_head_records`, `forks.changed_values`, and `CancellationSettlement`. Route handlers and Pydantic validators showed up as false positives.
- Decision on the CR-1 carry-over (`SemanticOperationAttemptKey`, `LangGraphCheckpointKey`, retired graph_runtime intervention commands): RRM-006 and RRM-007 did **not** supersede these uses. RRM-006 states it deleted no owners and left `ForkRequest`/`ForkReceipt`/`ForkFromCheckpointIntervention` inert because `/v2/graph-runtime/schemas` still exports them. RRM-006 and RRM-007 forks and commands use their own contracts (`RunForkRequest`/`RunForkReceipt`, `BoundaryCommandRecord`) and do not call the legacy ones.

### Changes
1. `refactor:` deleted the four unreferenced symbols above (and the imports they orphaned).

### Deliberate non-changes (grep proof)
- The retired graph_runtime intervention contracts (`InterventionBase` and subclasses, `InterventionReceipt`, `DurableInterrupt*`, `ForkRequest`, `ForkReceipt`, `RedactedCheckpointSummary`, `ProviderNeutralAttemptMetadata`) and the two keys stay. References: `app/api/graph_runtime_schemas.py` (export; `RuntimeIntervention` TypeAdapter), `graph_runtime/governance.py` model sets, `agent_server_actions.py`, `runtime_execution_bindings.py`, `postgres_runtime_execution_repository.py`, `runtime_interventions.py`, `runtime_repairs.py`, `integrations/langgraph_agent_server.py`, `runtime_recovery.build_cancellation_plan`, and about ten unit/integration tests including `test_digest_set_order_guard.py`. Deleting them means removing them from a published schema route, which RRM-001 §3 row 23 frames as a contract decision (remove from the export or label non-authoritative), not clean-up.
- The legacy Agent Server-shaped island (`runtime_interventions.py` service and router, `runtime_execution_bindings.py`, `graph_runtime_dispatch.py`, `agent_server_actions.py`, `runtime_decisions.py`, `integrations/langgraph_agent_server.py`) is reachable only from tests and itself. It stays: RRM-001 §3 says physical deletion is a later ticket, `postgres_runtime_execution_repository.py` has uncommitted user edits in the main checkout, and the export dependency above blocks the contract deletion.
- `build_cancellation_plan` and `apply_terminal_runtime_observation` (RRM-001 row 32) stay: only tests reference them, but the replacing cancellation saga is RRM-008 (CR-4).
- `postgres_run_forks._scope`/`_load` duplicate `postgres_stage3_kernel_repository._set_scope`/`_json`; `_dump` differs (stable-order dump). Left: each is a one-liner and cross-importing private names is worse.
- Route handlers in `api/run_forks.py` and `api/run_control.py` are referenced by FastAPI decorators only.
- No persisted or wire identity touched (Temporal names, `workflow.patched` IDs, payload fields, schema versions, migrations, API paths, receipt states, rejection reasons, sequence spaces, fork error codes). No file moved, so no path-safety sweep. GoalDirected files, `run_control/service.py` and `reducer.py` were not touched. Ticket-named test files kept.

### Commands and results
- `ruff check app tests scripts`: pass. `mypy app`: pass (366 files).
- Hermetic pytest (DSNs unset): 954 passed, 72 skipped, 2 xfailed (equals baseline; no tests deleted or renamed).
- Pytest with Postgres and Mongo DSNs plus `--env-file`: 996 passed, 30 skipped, 2 xfailed (equals baseline). Note: the `.env` file alone does not supply the DSNs; a run without them gave the hermetic counts, so the DSNs were exported explicitly.
- `git diff --check`: clean. Shared-stack lock acquired and released around each full run.

### Candidate generalization seams (not extracted)
- `RunSnapshot` manifest plus `ForkPatchPolicy` and reuse-frontier decision: a family-neutral "snapshot at safe boundary, patch, reuse what is unaffected" mechanism; only `default_patch_policy` and `_FAMILY_BY_HEAD_KIND` know the two families.
- Fork saga (`prepare`, claim, admit, materialize, record) over `ForkRepository`, `ForkAuthority` and `ForkMaterializer` ports: a generic reserve/claim/settle saga with receipts.
- Boundary-command ledger (`BoundaryCommandRecord`, per-space sequence, requested vs applied, delivery update) with `temporal_boundary_commands.py` as the sole Temporal-specific delivery: a provider-neutral operator-command receipt ledger.
- Shared Postgres helpers (`_set_scope`, JSON load/dump, advisory lock) repeated across the run-fork, stage3 kernel and lineage repositories: one `application` infrastructure helper.

### CR-3 integration

The coordinator checked the diff independently: it deletes four unreferenced symbols, with no wire or persisted identity changes. It was merged `--no-ff` at `1bdfd5c`. Merge gates:
- ruff: clean.
- mypy: 366 files, no issues.
- Hermetic pytest: 954 passed, 72 skipped, 2 xfailed.
- Pytest with the disposable Postgres/Mongo stack (DSNs exported explicitly) and `--env-file`: 996 passed, 30 skipped, 2 xfailed.

The service gate needs the DSNs exported explicitly. The developer `.env` does not supply them, so `--env-file` alone runs the hermetic set.

