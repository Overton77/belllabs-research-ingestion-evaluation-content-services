# RRM-003 implementation evidence

Disposition: accepted 2026-10-01 (independent review `approve_with_fixes`; blocking finding fixed in `07ac167`; merged into integration at `5b5cb55`)
Recorded date: 2026-10-01 (America/New_York)
Qualification identity: RRM-003 persist exact production checkpoint lineage — REQ-CP-EXEC-013/014 (observation part), REQ-CP-EXEC-005 (generation fence), REQ-CP-DA-016/017, REQ-CP-DA-018 (`not_submitted` only), REQ-BP-GD-012, REQ-CP-CS-007 (amended); `CON-CP-RUNTIME-UNIT-V1`, `CON-CP-CHECKPOINT-LINEAGE-V1` (AMD-RRM-001, accepted meta `main` `a50d833`)
Base revision and head revision: base `57c99bd3d47085cf29eb033a5411efcf74efd8ba` (integration `integration/research-runtime-mission`, RRM-001 merged). Tested code head `a3b27c5ed0cb4fd8ecf484fcc300700a4edc589d` on `wp/rrm-003-checkpoint-lineage`; the evidence/ticket commit follows it and changes documentation only. Review-fix commit follows `e294d48` (see Review disposition). Not merged (the coordinator owns review and merge).
Framework/package baseline: `uv sync --frozen` from the committed `uv.lock` (no dependency change); CPython 3.12.14 (Codex runtime), pytest 8.4.2, ruff 0.15.22, mypy 1.20.2, langgraph 1.2.10, langgraph-checkpoint 4.1.1, langgraph-checkpoint-postgres 3.1.1, deepagents 0.7.5, temporalio 1.30.0, asyncpg 0.31.0, psycopg 3.3.4

## Worktree provenance

- Worktree: `C:\Users\Pinda\Proyectos\Biotech\biotech-research-ingestion-evaluation-system-rrm-003`, branch `wp/rrm-003-checkpoint-lineage`, created by the coordinator from integration `57c99bd`. Clean at kickoff.
- The main checkout and the main `biotech-meta` checkout were not touched. No `.env` was copied or printed; the `.env` gate loads it with `--env-file ../biotech-research-ingestion-evaluation-system/.env`.
- Disposable services (coordinator-owned): `rrm-app-postgres` (`127.0.0.1:55432/belllabs`) and `rrm-app-mongodb` (`127.0.0.1:27017`, replica set). No container was created, stopped or removed by RRM-003. The persistent saver test creates and drops its own schema `rrm003_lineage_saver` inside that disposable database.

## Implemented contracts and seams

**Runtime unit (REQ-CP-EXEC-013).** `RuntimeUnitIdentity` (`belllabs.runtime-unit.v1`) in `app/domain/graph_runtime/identities.py`, with `StageGraphUnitLocation` (stage, mapped instance or `NO_MAPPED_INSTANCE`, workflow cycle, stage cycle, slot) and `GoalDirectedUnitLocation` (iteration, revision, role, agent run, session generation). `unit_key = "bl-unit-v1:" + sha256(canonical JSON)`. Temporal IDs, attempts, worker identity, timestamps and the execution generation are not identity fields (`extra="forbid"`). `app/domain/orchestration/runtime_units.py` maps the existing family identities. StageGraph materialization and GoalDirected preparation now put the unit on the `OperationExecutionRequest` and freeze it, with its cognitive namespace, into the Deep Agent binding. The `operation/{semantic_attempt_id}` wire ID is unchanged.

**Qualified checkpoint key (REQ-CP-DA-016).** `QualifiedCheckpointKey` (`belllabs.qualified-checkpoint-key.v1`): checkpointer ref digest, thread, `checkpoint_ns`, checkpoint and parent. It versions `LangGraphCheckpointKey`, which stays as inert prior art.

**Lineage contracts** (`app/domain/operation_execution/checkpoint_lineage.py`): namespace derivation (`belllabs/stage/{unit_key}/gen/{g}`; `belllabs/goal/{run}/epoch/{e}/session/{s}/role/{role}`; `…/unit/{unit_key}/gen/{g}` for GoalDirected generation ≥ 2), namespace owners, the five metadata stamps, `belllabs_invocation_id = sha256([unit_key, generation, "submit"])`, `OperationActivityAttempt`, `ActivityAttemptObservation`, `CheckpointInvocationPlan`, `CheckpointCapture`, `CheckpointTransitionObservation`, `LineageWriteRejection`, `CheckpointClassification`, and the fail-closed errors.

**Adapter (REQ-CP-DA-016/017/018).** `DeepAgentRuntimeAdapter.execute` requires a lineage plan that matches the exact binding (unit, generation, namespace, binding digest, state-schema digest, checkpointer digest). It runs the root graph with `thread_id = namespace`, `checkpoint_ns = ""`, pinned `checkpoint_id` of the expected source, `durability="sync"`, and the scalar stamps as config metadata. Before invoking, it admits only `not_submitted`: no root checkpoint in a head-less namespace, or a present, schema-compatible, parent-consistent source with no root descendant. Any other state raises `CheckpointLineageInDoubt` (or `IncompatibleCheckpointSchema`) and the model is never called. After invoking, it captures the result config from the post-invocation state snapshot, rejects pending tasks or interrupts, and walks parent links from the result to the source, requiring this invocation's stamps on every root checkpoint.

**Service and activity (REQ-CP-EXEC-014, REQ-CP-DA-017).** `operation.execute` builds `OperationActivityAttempt` from `activity.info()` (workflow, run, activity ID, real attempt) plus the worker identity. `OperationExecutionService.execute(request, attempt)` records the attempt observation, with the expected source (the namespace head), after acquiring the claim and before dispatch. Attempts that find the claim held are observed with `dispatching=false`. The service passes the plan to the runtime. The journal's `settle` gains `technical_attempt` (now the real Activity attempt, fixing RRM-001 §7 #13) and a `before_authority` hook. The hook records the transition by CAS, linked to the staged, digest-bound result manifest, before any authority settlement. The settlement manifest and the public result carry `result_checkpoint` and `checkpoint_transition_id`, and the result also carries `unit_key`. Lineage errors are re-raised, never settled `failed`. Deep Agent execution without lineage composition is rejected. The activity maps `CheckpointNamespaceBusy` to a retryable error and all other lineage errors to non-retryable `checkpoint_lineage_in_doubt` / `checkpoint_lineage_conflict`.

**Repository (REQ-CP-DA-017, REQ-BP-GD-012, REQ-CP-EXEC-005/014).** `CheckpointLineageRepository` protocol, with `InMemoryCheckpointLineageRepository` and `PostgresCheckpointLineageRepository`. Both apply one set of pure decision rules (`check_generation_admission`, `check_namespace_owner`, `reserve_in_flight`, `decide_transition`, `advance_namespace`):
- idempotent attempt observations;
- one in-flight invocation per namespace;
- namespace owner check;
- frozen binding and schema per unit generation;
- exact-duplicate transitions idempotent, different content rejected;
- in-flight ordering;
- CAS of the head against the transition's source;
- head-schema gate;
- claim fence and later-generation fence, with the rejected write recorded and never applied.

`CheckpointLineageService` turns a binding into a plan and a capture into a transition.

## Requirement-to-evidence map

| Requirement | Test → observed assertion |
|---|---|
| EXEC-013 golden digest | `tests/unit/orchestration/test_runtime_unit_identity.py::test_unit_key_is_the_prefixed_sha256_of_the_canonical_serialization[stage/goal]` → `unit_key` equals `"bl-unit-v1:" + hashlib.sha256(<literal canonical JSON>)` and the golden literal, for both families |
| EXEC-013 every location field distinguishes units | `…::test_every_stagegraph_location_field_changes_the_key` (10 cases: scope, run, epoch, operation, attempt, stage, mapped instance, workflow cycle, stage cycle, slot) and `…::test_every_goaldirected_location_field_changes_the_key` (6: iteration, revision, role, agent run, session generation, epoch) → key differs |
| EXEC-013 retries / restart / Continue-As-New keep the key | `…::test_payload_round_trip_across_retries_and_continue_as_new_keeps_the_key` → 3 JSON round trips give the golden key; `…::test_technical_and_generation_fields_are_not_unit_identity` (4) → `activity_attempt`, `temporal_workflow_id`, `worker_identity` and `execution_generation` are rejected as extra fields |
| StageGraph mapped instance/cycle/slot reach the unit | `tests/integration/temporal/test_wp_bp_010_temporal.py::test_real_materializer_persists_exact_operation_child_intent` → `runtime_unit.location` equals the candidate's stage, mapped instance, both cycle ordinals and slot, and the persisted binding carries it |
| GoalDirected iteration/revision/session/role reach the unit | `tests/integration/temporal/test_wp_bp_020_temporal.py::test_real_preparer_persists_revision_before_atomic_operation_admission` → `goal_executor` unit with iteration 1, revision, `executor`, agent run and session generation from the interpreter claim; preparation without agent run or session generation raises |
| EXEC-005/013/014: three Activity attempts, one unit, one dispatch | `tests/unit/operations/test_operation_execution.py::test_three_activity_attempts_share_one_unit_key_and_dispatch_once` (Temporal time-skipping) → observations `[(1, dispatching), (2, held), (3, held)]`, one `unit_key`, generation 1, fence 1, `runtime.invocations == 1` |
| EXEC-014 real attempt; RRM-001 §7 #13 | `…::test_settled_technical_attempt_is_the_real_activity_attempt` → the journal receives `technical_attempt == [2]` after a transient pre-claim failure on attempt 1; the observation records workflow ID and worker identity. Persistent: `operation_execution_attempts.technical_attempt == [2]` |
| EXEC-014 unit carried by the claim | persistent `test_operation_before_after_checkpoints_and_idempotent_duplicate_delivery` → `operation_effect_claims.unit_key == unit.unit_key` |
| DA-016 thread, root namespace, pin, `durability="sync"`, stamps | `tests/acceptance/control_plane/test_wp_cp_040.py::test_invocation_is_root_namespaced_pinned_sync_durable_and_fully_stamped` → `configurable == {thread_id: namespace, checkpoint_ns: ""}`, then the same plus the head's `checkpoint_id`; `durability == ["sync", "sync"]`; every root checkpoint carries the five stamps; stamped counts equal each capture's `stamped_checkpoint_count`; prompt text is absent from metadata |
| DA-016 on the persistent saver | persistent before/after test → every root checkpoint of the namespace carries the invocation ID, unit key and state-schema digest |
| DA-016 root `checkpoint_ns` decision (RRM-001 §8 #1) re-verified on Postgres | `tests/integration/deep_agents/test_checkpoint_lineage_postgres_saver.py::test_root_checkpoint_namespace_decision_reverified_on_postgres_saver` → `aget_state` with `checkpoint_ns="belllabs"` raises `Subgraph belllabs not found`, and the saver wrote only `""` |
| DA-018 / RRM-001 §7 #3: same unit never re-appends its prompt | `test_wp_cp_040.py::test_shared_session_reuse_is_ordered_and_same_unit_never_reappends_prompt` (rewritten from `…_governed_session_id_reuses_checkpoint_and_fresh_id_starts_empty`) → replaying the unit's own invocation raises `CheckpointLineageInDoubt` ("no namespace head") with no model call; a new attempt of the observed unit is refused (observed-unsettled); human counts stay `[1, 2, 1]` for next-iteration reuse and rollover |
| GD-012 reuse, rollover, verifier isolation | `tests/unit/orchestration/test_wp_bp_020_goal_directed.py::test_session_reuse_shares_and_rollover_isolates_cognitive_namespaces` → reused claim maps to the same namespace, verifier differs, rollover maps to `/session/2/role/executor`. `tests/acceptance/control_plane/test_wp_bp_020_sandbox_rollover.py` (Temporal, Docker sandbox) → 4 captures: session 1 executor/verifier, session 2 executor with `source_key is None`, session 2 verifier |
| GD-012 linear stamped lineage on the persistent saver | `…postgres_saver.py::test_goal_session_lineage_is_linear_rollover_is_empty_and_schema_gated` → iteration 2 source = iteration 1 result; `list_transitions == (one, two)`; the root chain is owned by unit 1, then unit 2; counts `[1, 2, 1, 1]` |
| Production role grants and RLS (review finding 1) | `test_checkpoint_lineage_postgres.py::test_runtime_role_grants_and_rls_admit_the_full_lineage_contract` → the repository pool runs `SET ROLE belllabs_control_runtime` (`current_user` is that role; not superuser, no `BYPASSRLS`). `SELECT … FOR UPDATE` on `runtime_units` raises `InsufficientPrivilegeError`. The full contract scenario then passes under the role: attempt, accepted transition, idempotent duplicate, conflict, compare-and-set, stale fence with recorded rejection, ownership, frozen binding, schema gate, rollover and generation fence. Another request scope sees zero transitions. Against the pre-fix repository the same test fails with `permission denied for table runtime_units` |
| GD-012 concurrent second session invocation rejected | `tests/integration/postgres/test_checkpoint_lineage_postgres.py::test_concurrent_session_invocations_serialize_on_the_namespace_row` → `asyncio.gather` of two dispatches gives exactly one admitted and one `CheckpointNamespaceBusy`; the contract scenario asserts the same sequentially |
| DA-017 capture and manifest link | persistent before/after test → head == transition result key == the saver's latest root checkpoint; journal settlement `result_manifest_ref`/`digest` == transition's; manifest `result_checkpoint`/`checkpoint_transition_id` == transition's; public result carries both. Offline: `test_wp_cp_040.py::test_operation_service_pins_records_and_links_the_result_checkpoint` |
| DA-017 CAS, idempotent duplicate, conflict, out-of-order, stale fence | `tests/fixtures/checkpoint_lineage.py::assert_checkpoint_lineage_repository_contract`, run by `tests/unit/operations/test_checkpoint_lineage.py` (memory) and `test_checkpoint_lineage_postgres.py::test_postgres_repository_satisfies_the_checkpoint_lineage_contract` → exact replay of attempt and transition is idempotent; different content conflicts; wrong source fails compare-and-set; after `advance_claim_fence` the fence-1 write raises `StaleClaimFence` and one `stale_claim_fence` rejection is recorded; the fence-2 write is accepted |
| Namespace ownership, frozen binding, schema (CS-007) | Contract → foreign owner digest raises `CheckpointNamespaceOwnershipError`; a changed binding digest raises "frozen"; a drifted schema raises `IncompatibleCheckpointSchema` at the head gate. Adapter: `test_wp_cp_040.py::test_source_state_schema_mismatch_fails_closed_before_model_invocation` → stamped source digest mismatch, model counts stay `[1]`. Persistent: the drifted unit raises, model counts unchanged, head unchanged |
| EXEC-005 generation fence | Contract → after generation 2 is observed, a generation 1 attempt and its late transition raise `StaleClaimFence`, with one `stale_execution_generation` rejection recorded |
| Plan required and exact | `test_wp_cp_040.py::test_unplanned_or_drifted_invocation_is_refused_before_materialization` → missing plan, unit-less binding and drifted checkpointer digest all raise before any model call |
| Persistent duplicate delivery | persistent before/after test → second delivery (attempt 3) returns the same settled result (output text excluded; see risks), adds 0 model calls, chain length unchanged, one transition, attempt observations `[(2, dispatching)]` |
| No secrets or bodies in BellLabs records | persistent before/after test → prompt text and secret value absent from every transition and attempt payload |
| Production composition calls the seam | `test_operation_service_pins_records_and_links_the_result_checkpoint` → service without lineage rejects Deep Agent execution; with lineage it pins, records and links. The activity passes the real attempt (Temporal tests above). Family preparation freezes unit and namespace: `test_wp_bp_010_temporal.py::test_real_materializer_freezes_the_stage_unit_cognitive_namespace` → `belllabs/stage/{unit_key}/gen/1` and a valid digest |
| Native operations and family regressions unchanged | Full offline suite: all pre-existing operation, StageGraph and GoalDirected suites pass. The native unit is observed without a namespace (`expected_source is None`) |
| Replay compatibility | Additive defaulted fields only (`GoalOperationPreparationRequest.execution_epoch/agent_run/session_generation`; `OperationExecutionRequest.runtime_unit`; result and settlement refs). Replayer suites pass (see Replay) |

## Changed paths and migrations

Shared seams edited under the coordinator's RRM-003 authorization:
- `app/domain/operation_execution/contracts.py`: additive. `DeepAgentExecutionBinding.runtime_unit` and `cognitive_session_namespace`, validated together and excluded from the digest when absent, plus `content_digest()`. `OperationExecutionRequest.runtime_unit`, validated against the attempt identity and the binding. `OperationExecutionBinding.runtime_unit`. `RuntimeInvocation.checkpoint_plan`. `RuntimeResult.checkpoint`. `OperationSettlement.checkpoint_transition_id` and `result_checkpoint`. `OperationExecutionResult.unit_key`, `checkpoint_transition_id` and `result_checkpoint`.
- `app/domain/graph_runtime/identities.py`: new `RuntimeUnitIdentity`, locations and `QualifiedCheckpointKey`; versioning docstrings on `SemanticOperationAttemptKey` and `LangGraphCheckpointKey`.
- `app/integrations/agents/deep_agents/adapter.py`: lineage protocol (above). `materializer.py` is unchanged.
- `app/migrations/0019_runtime_unit_checkpoint_lineage_v1.sql`: new.
- `app/temporal/registration/*`, run-control reducer, service and repositories, and `langgraph.json`: **unchanged**.

Other production paths:
- new `app/domain/operation_execution/checkpoint_lineage.py`, `app/domain/orchestration/runtime_units.py`, `app/application/operations/checkpoint_lineage.py` and `app/application/operations/postgres_checkpoint_lineage.py`;
- `app/application/operations/operation_execution.py`, `journaled_operation_execution.py` and `postgres_operation_journal.py`;
- `app/domain/operation_execution/journal.py` (`OperationEffectClaim.unit_key`);
- `app/application/orchestration/service.py` (StageGraph) and `goal_directed.py`;
- `app/domain/orchestration/goal_directed_runtime.py`;
- `app/temporal/workflows/goal_directed.py` (passes the claim's epoch, agent run and session generation into preparation; no command or name changes);
- `app/temporal/operation_activities.py`.

Tests:
- New: `tests/fixtures/checkpoint_lineage.py`, `tests/unit/orchestration/test_runtime_unit_identity.py`, `tests/unit/operations/test_checkpoint_lineage.py`, `tests/integration/postgres/test_checkpoint_lineage_postgres.py` and `tests/integration/deep_agents/test_checkpoint_lineage_postgres_saver.py`.
- Extended: `test_operation_execution.py`, `test_wp_cp_040.py`, `test_wp_bp_010_temporal.py`, `test_wp_bp_020_temporal.py`, `test_wp_bp_020_goal_directed.py` and `test_wp_bp_020_sandbox_rollover.py`.
- Harnesses that call the adapter directly now go through `execute_with_checkpoint_lineage`: `test_wp_bp_010_live.py`, `test_wp_bp_020_live.py`, `test_wp_cp_040_live.py` and `test_docker_sandbox.py`.
- `tests/conftest.py`: the persistent saver module joins the Psycopg Windows selector-loop list.

Migration `0019_runtime_unit_checkpoint_lineage_v1.sql` is forward-only and the next free number. Its explicit schema identities are `belllabs.runtime-unit.v1`, `belllabs.activity-attempt-observation.v1` and `belllabs.checkpoint-transition.v1`.

New tables:
- `runtime_units`, with a FK to `workflow_runs`;
- `runtime_unit_generations`, which holds the claim fence and the frozen binding, namespace and schema;
- `runtime_activity_attempt_observations`, unique per (unit, generation, Temporal workflow, run, activity, attempt);
- `runtime_cognitive_namespaces`, which holds the owner, the head and the in-flight holder;
- `runtime_checkpoint_transitions`, unique per unit generation and per (namespace, result), with a single-successor index on (namespace, source) as the CAS backstop;
- `runtime_lineage_write_rejections`.

All new tables use forced RLS by `belllabs.request_scope`. Grants are least privilege:
- `belllabs_control_runtime` has `SELECT, INSERT, UPDATE` only on `runtime_unit_generations` (claim fence) and `runtime_cognitive_namespaces` (head and in-flight holder).
- It has `SELECT, INSERT` only on the insert-only `runtime_units`, `runtime_activity_attempt_observations`, `runtime_checkpoint_transitions` and `runtime_lineage_write_rejections`.
- `belllabs_operations_readonly` has `SELECT` on all six tables.
- No role has `DELETE`.

Per-unit writes (`record_attempt`, `record_transition`, `advance_claim_fence`) serialize on `pg_advisory_xact_lock(hashtextextended('belllabs-runtime-unit:{scope}:{unit_key}', 0))`, following the journal and run-control convention. They do not take a row lock on `runtime_units`, so the runtime role needs no `UPDATE` there. Namespaces lock with `SELECT … FOR UPDATE`, which the `UPDATE` grant covers. `test_runtime_role_grants_and_rls_admit_the_full_lineage_contract` proves this under `SET ROLE belllabs_control_runtime` (see below).

Versioned tables:
- `operation_effect_claims.unit_key` (disposition row 48);
- `runtime_reconciliation_incidents.unit_key` (row 50);
- `runtime_fork_requests.source_binding_id` is now nullable (row 51);
- foreign keys from incidents and forks to the retired `runtime_execution_bindings` are dropped (rows 50–51).

The Agent Server-shaped `0012` runtime tables stay inert (row 47).

Deleted owners: none. The retired identities and `0012` runtime tables remain inert for later deletion.

## Deterministic verification

| Command | Result |
|---|---|
| Owning suites: `pytest tests/unit/orchestration/test_runtime_unit_identity.py tests/unit/operations tests/acceptance/control_plane/test_wp_cp_040.py tests/unit/orchestration tests/integration/temporal tests/acceptance/control_plane/test_wp_bp_020_sandbox_rollover.py tests/integration/deep_agents` | 123 passed, 4 skipped (3 persistent-saver tests without the DSN; 1 WSL-only BP-010 recovery) |
| `uv run --no-sync ruff check app tests scripts` | All checks passed! |
| `uv run --no-sync mypy app` | Success: no issues found in 336 source files |
| `BELLABS_RUN_WP_BP_010_LIVE=0 BELLABS_RUN_WP_BP_020_LIVE=0 BELLABS_RUN_WP_CP_040_LIVE=0 LANGSMITH_TRACING=false uv run --no-sync pytest -q` (hermetic) | 716 passed, 51 skipped, 2 xfailed, 0 failed |
| `uv run --no-sync --env-file ../biotech-research-ingestion-evaluation-system/.env pytest -q` | 716 passed, 51 skipped, 2 xfailed, 0 failed |
| `git diff --check 57c99bd HEAD` | clean |

Delta against the RRM-002 baseline (678 passed, 46 skipped, 2 xfailed). Collection grew from 726 to 769 node IDs: 44 new, 1 removed.
- The removed node ID is `test_wp_cp_040.py::test_governed_session_id_reuses_checkpoint_and_fresh_id_starts_empty`. It was rewritten as `test_shared_session_reuse_is_ordered_and_same_unit_never_reappends_prompt`; see the map.
- The 39 new offline tests are: 27 identity cases, 1 memory repository contract, 4 operation-execution, 4 new `test_wp_cp_040.py` cases, 1 StageGraph namespace, 1 GoalDirected namespace and the 1 rewritten session test. Net passed: +38.
- The 5 new skips are service-gated: 2 Postgres repository tests and 3 persistent saver tests. They skip without `TEST_APPLICATION_POSTGRES_DSN` and run in the service gate below.
- No test was skipped, xfailed, deselected or weakened by this ticket.

## Live runtime qualification

No live LLM call was made: the optional `BELLABS_RUN_*_LIVE` smoke was not run. **Spend: USD 0.** The live harnesses (`test_wp_bp_010_live.py`, `test_wp_bp_020_live.py`, `test_wp_cp_040_live.py`) were updated to the lineage seam and import cleanly, but stay skipped behind their flags. They are not re-qualified by this ticket.

Persistent technical integration against the disposable stack (`rrm-app-postgres`; LangGraph saver tables in a dedicated `rrm003_lineage_saver` schema; deterministic fake chat model inside a real `create_deep_agent` graph):

| Command | Result |
|---|---|
| `TEST_APPLICATION_POSTGRES_DSN=<disposable> TEST_MONGODB_URI=<disposable> BELLABS_RUN_*_LIVE=0 LANGSMITH_TRACING=false uv run --no-sync pytest -q -rs` (full suite with both services) | 743 passed, 24 skipped, 2 xfailed, 0 failed. Remaining skips: 19 Agent Server endpoint, 3 live-provider flags, 1 WSL, 1 pre-existing retirement. Every Postgres- and Mongo-gated suite ran, including all pre-existing ones against migration `0019` |
| `TEST_APPLICATION_POSTGRES_DSN=<disposable> uv run --no-sync pytest -q -s tests/integration/deep_agents/test_checkpoint_lineage_postgres_saver.py tests/integration/postgres/test_checkpoint_lineage_postgres.py` | 6 passed after the review fix (3 persistent saver, 3 PostgreSQL repository) |

Sanitized before/after record printed by the persistent test (one StageGraph operation, delivered as Activity attempt 2, then duplicate-delivered as attempt 3):

```text
namespace  belllabs/stage/bl-unit-v1:d6bba8c8…dd58ed/gen/1
before     root_checkpoint = none, namespace_head = none
after      result checkpoint 1f1bdf64-14b4-674c-8003-8ecb4d749063
           parent 1f1bdf64-1479-6504-8002-fce6879f7f47, checkpoint_ns ""
           checkpointer_ref_digest sha256:4bee4d60…5fc8
stamped root checkpoints 5; transition checkpoint-transition:f4177065-645a-5bb4-9af2-91a59d4d68ca
result manifest sha256:616b9f03…c5bb7 (equal to the journal settlement's)
technical_attempt 2; duplicate delivery added 0 model calls and 0 checkpoints
```

Checkpoint and transition IDs vary per run; the assertions, not the IDs, are the evidence.

## Replay and recovery artifacts

| Command | Result |
|---|---|
| `uv run --no-sync pytest -q tests/integration/temporal/test_wp_bp_010_temporal.py tests/integration/temporal/test_wp_bp_020_temporal.py tests/integration/temporal/test_linked_runs.py tests/unit/operations/test_operation_execution.py -k "replay or Replayer or replays or routes_bound"` | 4 passed, 54 deselected (the same replays also run inside every full gate above) |

The captured-history replays (StageGraph any-join, GoalDirected separate children and Continue-As-New, GoalDirected cancellation, OperationWorkflow cross-queue) record histories with the RRM-003 workflow code and replay them with `Replayer`. GoalDirected preparation inputs gained only defaulted fields; no workflow, activity, signal, query or payload field was renamed. No crash-window recovery is claimed (RRM-004).

## Replacement and deletion checks

- `SemanticOperationAttemptKey` → `RuntimeUnitIdentity`, and `LangGraphCheckpointKey` → `QualifiedCheckpointKey`. Both originals remain inert, with versioning docstrings; neither is on an active path.
- The adapter no longer uses `session_id or binding_id` as the thread; the frozen binding namespace is the only thread source.
- No competing lifecycle store: units, attempts and transitions are evidence records beside the existing journal, and settlement authority remains the journal plus run control.

## Unresolved risks and drift checks

Seams left for **RRM-004**:
1. **Classification.** Only `not_submitted` (and the existing settled return) is implemented. `observed_unsettled` raises `CheckpointLineageInDoubt` in `CheckpointLineageService.observe_attempt`. `interrupted` and `terminal_unobserved` hit the adapter's pre-dispatch guard (`_classify_not_submitted`), which then fails closed. `CheckpointClassification`, `CheckpointInvocationPlan.mode` (`"submit"` only today), and the transition `classification` CHECK (`interrupted`, `terminal_unobserved`) are ready to extend.
2. **Incidents.** No `in_doubt` incident is written. `runtime_reconciliation_incidents.unit_key` exists, the FK to the retired table is dropped, and the activity reports `checkpoint_lineage_in_doubt` as non-retryable.
3. **Claim lease and takeover (RRM-001 §7 #1).** The claim fence lives on `runtime_unit_generations.claim_fence`. `advance_claim_fence(expected_fence)` is the takeover seam, and stale writes are rejected and recorded. The journal claim itself still has no lease, so a held unsettled claim keeps returning `operation_execution_in_progress`.
4. **In-flight reservation.** A namespace reservation is released only by an accepted transition. After `in_doubt`, a post-dispatch failure, or a budget violation after capture, the namespace stays reserved, so later session units get `checkpoint_namespace_busy`. This is fail-closed by design; `reconcile_unit` must release or advance it.
5. **Post-dispatch failures (RRM-001 §7 #2).** Ordinary exceptions still settle `failed` without classification. Only lineage errors are excluded.
6. **Settled replay drops output.** The settled-replay result comes from the digest-bound manifest, which omits `output_text` and `structured_output` (pre-existing). A GoalDirected reconcile of such a replay would reject it. RRM-004's "existing settlement returns unchanged" must restore the output from the authoritative store.

Other notes:
- **Deviation (storage shape).** `operation_settlements` gained no checkpoint columns: adding fields to `OperationJournalSettlement` would change its `complete-v2` digest shape. The settlement references the result checkpoint through its digest-bound manifest (`result_checkpoint`, `checkpoint_transition_id`), and the transition row references the manifest ref and digest. The link is immutable in both directions.
- **Deviation (fence placement).** The claim fence is not yet on `operation_effect_claims`; see item 3.
- **Native operations** record attempt observations at generation 1: `OperationExecutionRequest` carries no generation, while Deep Agent bindings do. A native generation boundary needs a contract field if ever required.
- **Outside REQ-CP-EXEC-013 (RRM-001 §8 #10).** Callers that are not `OperationWorkflow`, such as the uncomposed `GovernedSchemaAgentRuntime`, cannot run Deep Agents without a runtime unit and a lineage composition; a contract revision is required first.
- **Not done here, by design:** Search Attributes and inspection reads (RRM-005), fork snapshot and patch columns (RRM-006), and async-child unit keys (RRM-013).
- **External gates still unrun:** Agent Server endpoint suites (19), live providers (3), and the WSL-only BP-010 recovery (1).
- New tickets: none.

### Reusable seams (mission-horizon lens)

- `RuntimeUnitIdentity` / `unit_key`: a generic structured, hashed unit identity. Families contribute only typed locations.
- `QualifiedCheckpointKey`: a framework-neutral checkpoint reference, keys only.
- `CheckpointInvocationPlan` / `CheckpointCapture`: the runtime-adapter contract. LangGraph specifics stay in the adapter.
- `CheckpointLineageRepository` with its shared pure decision rules: storage-agnostic CAS lineage authority (memory and PostgreSQL).
- `CheckpointLineageService`: binding to plan, and capture to transition.
- `OperationActivityAttempt`: a scheduler-neutral technical delivery observation.
- `ResultManifestObserver` (the journal `before_authority` hook): links evidence to a manifest before authority settlement.
- `assert_checkpoint_lineage_repository_contract`: a reusable conformance scenario for any repository implementation.

None of these carries company, fixture or provider specifics.

## Review disposition

Independent review verdict: `approve_with_fixes`.

| # | Finding | Disposition |
|---|---|---|
| 1 (blocking) | Migration 0019 granted `belllabs_control_runtime` only `SELECT, INSERT` on `runtime_units`, but `_lock_unit` used `SELECT … FOR UPDATE` on it. Under the production non-owner role this fails with `permission denied for table runtime_units`, and every Postgres test connected as the owner. | **Fixed** in the review-fix commit. `_lock_unit` now takes the per-unit advisory transaction lock (journal and run-control convention), so `runtime_units` stays insert-only. `advance_claim_fence` takes the same lock, closing a fence/transition race the audit found. `0019` was edited in place (unmerged): rationale comment, plus read-only `SELECT` on `runtime_lineage_write_rejections`. Audit of every `UPDATE` and `FOR UPDATE`: both target tables the runtime role may update; there are no `DELETE` statements. The new runtime-role test proves the grants and RLS and was shown red against the pre-fix code. |
| 2 | The new repositories are not wired into a production composition. | **Acknowledged** for RRM-004 and RRM-009: no deployment `WorkerActivityCompositionFactory` exists yet. The service refuses Deep Agent execution without lineage composition, so the factory must pass `lineage=CheckpointLineageService(PostgresCheckpointLineageRepository(pool))` with the persistent saver. |
| 3 | A namespace stays reserved after failure or `in_doubt`. | **Handed to RRM-004** (Unresolved risks, item 4). Reservation is released only by an accepted transition; `reconcile_unit` must release or advance it. |

Post-fix gates:

| Command | Result |
|---|---|
| `uv run --no-sync ruff check app tests scripts` | All checks passed |
| `uv run --no-sync mypy app` | Success: no issues found in 336 source files |
| Owning suites plus the Postgres lineage, journal and Stage 3 kernel suites, both service DSNs set | 141 passed, 1 skipped (WSL-only BP-010 recovery) |
| Full offline pytest, hermetic | 716 passed, 52 skipped, 2 xfailed, 0 failed (+1 skip: the new DSN-gated runtime-role test) |
| `git diff --check` | clean |

## Integration merge gates (coordinator, merge commit `5b5cb55`)

Run in the integrator worktree on `integration/research-runtime-mission` at `5b5cb55` (`git merge --no-ff wp/rrm-003-checkpoint-lineage`, tested head `07ac167`):

| Command | Result |
|---|---|
| `uv run --no-sync ruff check app tests scripts` | All checks passed |
| `uv run --no-sync mypy app` | no issues, 336 files |
| Hermetic full pytest (`BELLABS_RUN_*_LIVE=0 LANGSMITH_TRACING=false uv run --no-sync pytest -q`) | 716 passed, 52 skipped, 2 xfailed, 0 failed |
| Full pytest with `TEST_APPLICATION_POSTGRES_DSN` and `TEST_MONGODB_URI` (disposable stack) | first run 6 failed / 738 passed; five later runs 744 passed, 24 skipped, 2 xfailed, 0 failed (one plain service run plus four with `--env-file`) |
| Full pytest with `--env-file ../biotech-research-ingestion-evaluation-system/.env` | first run 3 failed / 713 passed; rerun 716 passed, 52 skipped, 2 xfailed, 0 failed |
| `git diff --check` on the merge | clean |

**Intermittent failure (unresolved, recorded honestly).** In the first post-merge batch, the service and `--env-file` runs failed. The visible failure was `tests/unit/operations/test_operation_execution.py::test_journaled_operation_settles_usage_effect_and_terminalizes[True-True]`, an in-memory unit test of the crash-after-authority path. Only the summary lines were captured, so the traceback was lost. The machine was under memory pressure at the time (about 1 GB of 15.3 GB free; a later background run was killed by the host for low memory). Six subsequent full runs were all green, along with six repeats of the module. Nothing in `operation_execution.py` or `journaled_operation_execution.py` reads a wall clock or timeout, so the root cause is not yet known. **Owner: RRM-004**, which reworks this exact crash-after-authority settlement path. It must reproduce the failure under load (repeated or stressed runs), capture the traceback, and fix the root cause instead of retrying.

## Final disposition

accepted
