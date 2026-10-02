# RRM-004 implementation evidence

Disposition: accepted 2026-10-01 (independent review `approve_with_fixes`, then re-reviews; findings fixed in `284ed20`, `89bbdf7` and `d296481`; docs in `b8f0d97` and `79ec2ee`; merged into integration at `fcefd54`)
Recorded date: 2026-10-01 (America/New_York)
Qualification identity: RRM-004 recover checkpoint and settlement crash windows. Requirements: REQ-CP-DA-018 (classification and crash windows); REQ-CP-EXEC-003/004/005/008(narrow)/014 (claim lease, takeover and fence); REQ-CP-RUN-007 (narrowed post-dispatch rule, `in_doubt`, run phase, `operator_reconciliation`); REQ-CP-DA-017 (transition linked to the fenced result). Contracts: `CON-CP-CHECKPOINT-LINEAGE-V1` (classification table, crash windows, operator decisions) and `CON-CP-LIFECYCLE-V1` `reconcile_unit` (AMD-RRM-001, accepted meta `main` `a50d833`).
Base revision and head revision: base `8762d3e` (integration `integration/research-runtime-mission`, RRM-001 and RRM-003 merged). Tested code head: ``a90022cddbd39348e8a3179a89685e2ceb0ac02b`` on `wp/rrm-004-checkpoint-recovery`. The evidence/ticket commit `404d253` follows it and changes documentation only. Review-fix code commit: `284ed20`; its documentation commit follows. Not merged (the coordinator owns review and merge).
Framework/package baseline: `uv sync --frozen` from the committed `uv.lock` (no dependency change); CPython 3.12.14 (Codex runtime), pytest 8.4.2, ruff 0.15.22, mypy 1.20.2, pydantic 2.13.4, langgraph 1.2.10, langgraph-checkpoint 4.1.1, langgraph-checkpoint-postgres 3.1.1, deepagents 0.7.5, temporalio 1.30.0 (its downloaded dev server serves `WorkflowEnvironment.start_local`), asyncpg 0.31.0, psycopg 3.3.4

## Worktree provenance

- Worktree: `C:\Users\Pinda\Proyectos\Biotech\biotech-research-ingestion-evaluation-system-rrm-004`, branch `wp/rrm-004-checkpoint-recovery`, created by the coordinator from integration `8762d3e`. It was clean at kickoff.
- The main checkout and the main `biotech-meta` checkout were not touched. No `.env` was copied or printed; the `.env` gate loads it with `--env-file ../biotech-research-ingestion-evaluation-system/.env`.
- Disposable services (coordinator-owned): `rrm-app-postgres` (`127.0.0.1:55432/belllabs`) and `rrm-app-mongodb` (`127.0.0.1:27017`). No container was created, stopped or removed. The worker-restart test creates and drops its own schema, `rrm004_restart_saver`, inside the disposable database. Temporal ran only as `WorkflowEnvironment.start_local()` (an ephemeral in-memory dev server) and the time-skipping test server; the user's docker compose Temporal stack was not used.

## Implemented contracts and seams

**Claim lease and takeover (REQ-CP-EXEC-014; RRM-001 §7 #1).** The claim of a unit generation now carries a lease (`runtime_unit_generations.lease_holder`, `lease_expires_at`; migration 0020). The holder is the exact Activity attempt observation.
- `CheckpointLineageService.admit_attempt` records the attempt. It then holds or renews the lease, or takes over an expired or released lease by advancing `claim_fence` (`decide_lease`). Otherwise the attempt stands down: it is observed with `dispatching=false` and raises the retryable `OperationExecutionInProgress`.
- The lease ends with the scheduler's deadline for the attempt. The activity sets `OperationActivityAttempt.lease_expires_at = info.started_time + start_to_close_timeout`, so the retry Temporal schedules after a lost worker finds the lease expired.
- A holder that stops on its own (an exception or cancellation) releases its lease, and the next attempt takes it over at once. A lost worker releases nothing.
- Concurrent recoveries of one unit serialize on the per-unit advisory lock.

**Fenced result observation (REQ-CP-EXEC-014; `observed_unsettled`).** `UnitResultObservation` (`belllabs.unit-result-observation.v1`) fixes the result manifest of a unit generation. It is written by compare-and-set on the claim fence, before authority settlement, for every unit (native or cognitive) and every status. For a completed cognitive unit, the checkpoint transition (CAS on the namespace head) and the result are one write.
- A superseded fence or generation is rejected and recorded in `runtime_lineage_write_rejections`, never applied. A different manifest conflicts.
- A later holder that finds a recorded result settles exactly that manifest (`load_result_manifest`), with no provider work.
- A result without a transition (failed or abandoned) releases the namespace's in-flight reservation instead of stranding it. A unit generation whose result is already fixed never reserves its namespace again.

**Classification before provider work (REQ-CP-DA-018).** `OperationExecutionService._recover_or_dispatch` acts in the contract's order:
1. `settled`: return the stored result.
2. `observed_unsettled`: settle the recorded manifest.
3. An open incident: act on the operator decision, or park.
4. A native unit whose earlier holder dispatched: `in_doubt` (an ambiguous effect is never re-run).
5. Otherwise, dispatch.

The adapter's `_classify` then classifies the checkpointer, replacing RRM-003's `not_submitted`-only guard. The descendants of the expected source are the root checkpoints whose parent chain reaches it; for a new namespace that is every root checkpoint.
- No descendants: `not_submitted`. Submit the input once, pinned to the source.
- Exactly one stamped lineage with no unstamped or foreign descendant, whose leaf has pending tasks: `interrupted`. Run `ainvoke(None)` pinned to the leaf; the input is never re-appended.
- The same, but the leaf has no pending tasks: `terminal_unobserved`. Reconstruct the result from the leaf's state with no invocation.
- Anything else raises `CheckpointLineageInDoubt` with a typed `reason` and the stamped candidate keys: a foreign or unstamped descendant, more than one stamped leaf, a pending LangGraph interrupt, a missing or ancestry-mismatched source, a schema-digest mismatch, or an unclassifiable lineage.

`CheckpointCapture.classification` records the act, so the transition's `classification` is the one applied. `CheckpointInvocationPlan.accepted_leaf` carries an operator's `accept_descendant`. The plan's `mode` stays `"submit"`: a resume or reconstruction keeps the unit generation's submission stamps, so every checkpoint stays attributable.

**Narrowed post-dispatch rule (REQ-CP-RUN-007; RRM-001 §7 #2).** The adapter wraps any exception after classification in `RuntimeInvocationFailure(error_type, terminal_result_observed)`, using a post-failure classification. The service settles `failed` only when no terminal result exists and every effect claim of the unit is settled (`unsettled_effect_ids` reads the run-control effect ledger). Otherwise the unit is `in_doubt` with reason `terminal_result_after_failure`, `unsettled_effect_ids` or `unclassifiable`. A takeover attempt that cannot classify an already-dispatched cognitive unit is `in_doubt` too. A budget violation stays an ordinary `failed` governance outcome.

**`in_doubt` incident and parking.** `UnitReconciliationIncident` (`belllabs.unit-reconciliation-incident.v1`) is written into `runtime_reconciliation_incidents` (`incident_type = runtime_unit_in_doubt`, keyed by unit generation, `unit_key` set). It records:
- the unit key, generation, binding and operation workflow;
- the typed reason, the namespace and the expected source;
- the candidate keys and the unsettled effect IDs.

`JournaledOperationExecutionCoordinator.record_in_doubt` then preserves the ambiguous disposition: the unit's effect claim gets an `ambiguous` observation and stays unsettled. It also declares an `operator_reconciliation` wait. The reducer never changes the run phase for this wait kind; it is accepted in `active`, `waiting`, `paused` and `cancelling`. The journal claim and its budget reservation stay open. The activity returns `status="in_doubt"` with `reconciliation_incident_id`. `OperationWorkflow` parks durably behind `workflow.patched("rrm-004-park-in-doubt-units")` until the new signal `unit_reconciliation_recorded`, then re-runs classification. The signal is a wake-up hint only: a hint without an accepted decision parks the unit again.

**`reconcile_unit` (`CON-CP-LIFECYCLE-V1`).**
- `ReconcileUnitAction` is a run-control lifecycle command with the privileged permission `workflow_run.reconcile_unit`. It targets one unit key, generation and incident, with one decision; `accept_descendant` carries an exact root-namespace `QualifiedCheckpointKey`.
- The reducer requires a matching pending `operator_reconciliation` wait. It rejects another incident or generation (`reconciliation_not_pending`) and a second decision (`unit_already_reconciled`); stale versions are `STALE`. It records `UnitReconciliationDecision` in `RunProjection.unit_reconciliations` (authority) and releases the wait. A run waiting only on that wait becomes active; `cancelling` and `paused` keep their phase. `satisfy_wait` cannot release this wait.
- `UnitReconciliationService.reconcile_unit` checks the incident, executes the command, applies the decision to lineage (`apply_reconciliation`) and nudges the parked workflow (`TemporalUnitReconciliationNudge`).

The three decisions:
- **`accept_descendant`** reclassifies as if the named key were the leaf: it resumes or reconstructs. Stamped descendants the key gains from a later resume are followed to their unique leaf.
- **`abandon_unit`** settles `failed` with `in_doubt_abandoned` and releases the namespace.
- **`start_new_generation`** marks the generation `superseded`, which fences all of its writes as `stale_execution_generation`, and releases the namespace. The old generation's workflow ends `in_doubt` with `generation_superseded`; re-admission at `g+1` is RRM-014.

**Settled replay restores output.** The journal coordinator stages a canonical output payload (`output_text`, `structured_output`) as a content-addressed object. The settlement and the digest-bound manifest carry its address (`OperationSettlement.output_payload_ref/digest/size_bytes`). `get_settlement` and `load_result_manifest` restore the payload, so a settled replay, a GoalDirected reconcile, or a settlement recovered from a manifest returns the identical result.

**Composition seam.** `compose_postgres_operation_recovery(pool, run_control=..., nudge=...)` (`app/application/operations/operation_recovery_composition.py`) returns the PostgreSQL-backed `CheckpointLineageService` and `UnitReconciliationService`. Both worker processes of the restart proof use it, and RRM-009's factory must use it too (see Unresolved risks).

**Intermittent RRM-003 merge-gate failure: root cause found and fixed.** See "Intermittent-failure diagnosis" below. The new `contract_fingerprint` (`app/domain/control_plane/canonical.py`) replaces the set-order-dependent JSON-mode fingerprints on the replay path.

## Requirement-to-evidence map

| Requirement | Test → observed assertion |
|---|---|
| DA-018 crash windows (7 windows, real Deep Agent) | `tests/unit/operations/test_checkpoint_recovery_classification.py::test_crash_window_converges_to_one_settlement_without_reappending_input[<window>]`. Per window: model calls before the crash and in total (table below); every call saw exactly 1 human message; the final transcript holds 1 `HumanMessage` and exactly 1 `ToolMessage` (`rrm004-write-todos`); the transition classification is as expected; every root checkpoint from the result to the empty source carries this unit generation's `belllabs_invocation_id`; one transition and one settlement; namespace head = result key; `result_digest` equals a crash-free run's; `structured_output == {"answer": "RRM004-OK", "tool_results": 1}` (output restored, not dropped); lease fences `[(1, 1), (2, 2)]`; `technical_attempt == [2]` |
| DA-018 / EXEC-003 real worker restart, persistent | `tests/integration/temporal/test_rrm_004_worker_restart_recovery.py::test_worker_restart_resumes_the_interrupted_unit_and_settles_once`. Worker 1 (a separate OS process, `PYTHONHASHSEED=7`) is killed after root checkpoint 6. Crash state: one dispatching attempt, 6 stamped checkpoints, no transition. Worker 2 (a fresh composition in another process, same dev server) completes the workflow: model calls `[worker-1, worker-2]` with human counts `[1, 1]`; attempts `[(1, True, fence 1), (2, True, fence 2)]`; classification `interrupted`; the result descends from worker 1's crash leaf; all checkpoints share one invocation ID and there is no branch; `operation_execution_attempts.technical_attempt == [2]`; one `operation_settlements` row; a duplicate delivery returns the identical result; one scheduled activity (Temporal retried it); the history replays with `Replayer` |
| EXEC-014 lease honored, stand-down observed | `…classification.py::test_lost_worker_lease_is_honored_until_it_expires` → before expiry the next attempt raises `OperationExecutionInProgress`; observations `[(1, True), (2, False)]`; no extra model call |
| EXEC-014 stale holder cannot apply a late observation | `…classification.py::test_zombie_holder_late_write_is_rejected_and_recorded` → zombie A stalls in model call 2. B takes over (fence 2), resumes and settles. A's late write raises `CheckpointLineageConflict`, and one rejection `("stale_claim_fence", 1, 2)` is recorded. Transition fence 2; replay equals B's result |
| EXEC-014 concurrent recovery serializes | `…classification.py::test_concurrent_recoveries_serialize_on_the_claim_lease` (memory) → the second concurrent attempt stands down; 2 model calls in total. `tests/integration/postgres/test_checkpoint_lineage_postgres.py::test_concurrent_recoveries_of_one_unit_serialize_on_the_claim_lease` (Postgres) → three concurrent takeovers: exactly one granted, fence 2, dispatching `[False, False, True]` |
| DA-018 multiple stamped leaves → `in_doubt`; `accept_descendant` | `…classification.py::test_multiple_stamped_leaves_park_in_doubt_until_operator_accepts_one`. While in doubt: no model call; both leaves are incident candidates; the run phase stays `active` with exactly one `operator_reconciliation` wait; the effect claim is `AMBIGUOUS` and unsettled; a repeat attempt stays in doubt; a checkpoint from a foreign namespace is refused. After the decision: the wait is released and recorded in `unit_reconciliations`; the run resumes from the accepted leaf (the sibling is not in the result chain); classification `interrupted`; the digest equals the baseline; the incident is `resolved`; the effect is `SUCCEEDED` |
| DA-018 foreign/unstamped descendant; `abandon_unit` | `…classification.py::test_foreign_descendant_is_in_doubt_and_abandon_settles_failed` → `in_doubt` / `foreign_descendant` with 0 model calls; the namespace is held until reconciliation, then released. The settlement is `failed` / `in_doubt_abandoned`, immutable on replay; the effect is `FAILED` |
| EXEC-005 `start_new_generation` fences the generation | `…classification.py::test_start_new_generation_fences_every_late_write_of_the_old_generation` → a stale-version decision is `STALE`; the namespace is released; the next attempt returns `in_doubt` / `generation_superseded` with no settlement; a late result write raises `StaleClaimFence`, and the recorded rejection is `stale_execution_generation` with current generation 2 |
| RUN-007 narrowed rule | `…classification.py::test_provider_failure_without_terminal_result_or_open_effects_settles_failed` → `failed` / `runtime_failed`, message `ValueError at governed operation boundary`, no incident. `…::test_provider_failure_with_an_unsettled_effect_claim_is_in_doubt` → a tool effect claimed during cognition, then a provider failure → `in_doubt` / `unsettled_effect_claims`, incident lists `tool-effect:send-email`, no settlement. `…::test_failure_after_a_terminal_checkpoint_is_in_doubt_then_reconstructed` → `in_doubt` / `terminal_result_after_failure`; `accept_descendant` of the terminal leaf reconstructs with 0 new model calls, `terminal_unobserved`, baseline digest |
| `reconcile_unit` reducer (RRM-001 §4 seam) | `tests/unit/run_control/test_run_control.py::test_in_doubt_unit_keeps_the_run_phase_until_reconcile_unit_decides` → the phase stays `active` even with `runnable_work_remains=False`; `satisfy_wait` → `operator_decision_required`; another incident or generation → `reconciliation_not_pending`; stale version → `STALE`; missing privilege → `CommandRejected`; acceptance records the decision and releases the wait; a second decision is rejected; an exact replay is idempotent. `…::test_operator_wait_preserves_cancelling_and_waiting_phases` → `cancelling` stays `cancelling`; `waiting` on another condition stays `waiting`. `…::test_reconcile_unit_decisions_are_typed` → the accepted key is required exactly for `accept_descendant` and must be root-namespace |
| Lineage storage rules (memory, Postgres owner, Postgres runtime role) | `tests/fixtures/checkpoint_lineage.py::assert_checkpoint_recovery_repository_contract`, run by `tests/unit/operations/test_checkpoint_lineage.py::test_in_memory_repository_satisfies_the_checkpoint_recovery_contract`, `test_checkpoint_lineage_postgres.py::test_postgres_repository_satisfies_the_checkpoint_lineage_contract` and `::test_runtime_role_grants_and_rls_admit_the_full_lineage_contract`. It covers: live-lease stand-down; takeover after expiry (fence 2, `prior_dispatch`); release then immediate takeover (fence 3); stale result rejected and recorded; result fixed once, exact duplicate idempotent, different manifest conflicts; a result without a transition frees the namespace and is found by a later attempt; result plus transition atomic (a mismatched pair writes nothing); incidents idempotent per unit generation; `abandon_unit` resolves once and releases the namespace, while a different second decision conflicts; `start_new_generation` refuses generation 1 attempts and results (recorded) and admits generation 2 |
| Least privilege (non-owner role) | `test_runtime_role_grants_and_rls_admit_the_full_lineage_contract` runs the recovery contract under `SET ROLE belllabs_control_runtime`. `UPDATE` and `DELETE` on `runtime_unit_result_observations` raise `InsufficientPrivilegeError`; 2 typed incidents are visible in scope; forced RLS hides other scopes' results |
| Native ambiguous effect; takeover; parking replay | `tests/unit/operations/test_operation_execution.py::test_lost_native_holder_is_taken_over_by_fence_and_parked_in_doubt` (Temporal time-skipping). Observations `[(1, True), (2, True)]` with fences `[1, 2]`; incident `ambiguous_native_effect`; one runtime invocation; a hint re-classifies (3rd observation) without dispatch; the workflow stays `RUNNING`; the parked history replays; 2 scheduled activities |
| Settled replay returns output unchanged | `test_checkpoint_lineage_postgres_saver.py::test_operation_before_after_checkpoints_and_idempotent_duplicate_delivery` → `duplicate == result` (strengthened from RRM-003's output-excluded comparison). `test_operation_execution.py::test_journaled_operation_settles_usage_effect_and_terminalizes[*]` → `coordinator.get_settlement(binding) == result`, with the output payload address set |
| DA-018 schema/digest mismatch → incident | `test_checkpoint_lineage_postgres_saver.py::test_goal_session_lineage_is_linear_rollover_is_empty_and_schema_gated` → the drifted unit returns `in_doubt` / `schema_mismatch` with an incident; model counts unchanged; the head is unmoved. Adapter level: `test_wp_cp_040.py::test_source_state_schema_mismatch_fails_closed_before_model_invocation` still raises `IncompatibleCheckpointSchema` |
| Same unit never re-appends its prompt | `test_wp_cp_040.py::test_shared_session_reuse_is_ordered_and_same_unit_never_reappends_prompt` → re-executing the first unit's plan now reconstructs (`terminal_unobserved`, same result key, output and usage), with human counts `[1]` before the cross-iteration reuse `[1, 2, 1]` |
| Fingerprints independent of set order (root cause) | `test_run_control.py::test_command_fingerprint_is_independent_of_set_iteration_order` → two equal commands whose permission frozensets iterate differently. JSON-mode digests differ (the defect); `contract_fingerprint` digests are equal; an exact run-control replay rebuilt with the other order returns the stored result |

### Crash-window matrix

| Window | Injection point | Recovering attempt classifies | Action | Model calls before crash / total | Human msgs per call | Tool executions | Test ID |
|---|---|---|---|---|---|---|---|
| Before the checkpoint | runtime port, after claim, lease and dispatching observation, before the adapter | `not_submitted` | single submission | 0 / 2 | 1 | 1 | `…[before_checkpoint]` |
| After the input checkpoint | saver: worker lost after root checkpoint 1 (`source=input`) | `interrupted` | resume `ainvoke(None)` pinned to the leaf | 0 / 2 | 1 | 1 | `…[after_input_checkpoint]` |
| After an intermediate checkpoint | saver: worker lost after root checkpoint 6 (tool result durable) | `interrupted` | resume; the tool is not re-run | 1 / 2 | 1 | 1 | `…[after_intermediate_checkpoint]` |
| After the terminal checkpoint | saver: worker lost after root checkpoint 7 (inside the graph) | `terminal_unobserved` | reconstruct; no invocation | 2 / 2 | 1 | 1 | `…[after_terminal_checkpoint]` |
| Before observation | runtime port: the adapter returned; lost before the result/transition write | `terminal_unobserved` | reconstruct; no invocation | 2 / 2 | 1 | 1 | `…[before_observation]` |
| Before settlement | run control: lost at the effect observation, after the fenced result and transition were recorded | `observed_unsettled` | settle the recorded manifest (transition fence 1; `technical_attempt` 2) | 2 / 2 | 1 | 1 | `…[before_settlement]` |
| After settlement | none (redelivery) | `settled` | return unchanged, output restored; no observation, lease or provider work | 2 / 2 | 1 | 1 | `…[after_settlement]` |
| Worker kill after an intermediate checkpoint (persistent, two processes) | `AsyncPostgresSaver` stall after root checkpoint 6, then `Popen.kill()` | `interrupted` | resume on worker 2 | 1 / 2 | 1 | 1 | `test_rrm_004_worker_restart_recovery.py::test_worker_restart_resumes_the_interrupted_unit_and_settles_once` |

Ambiguous cases: more than one stamped leaf, a foreign or unstamped descendant, a schema mismatch, a terminal result after a failure, an unsettled effect claim, and a native effect already dispatched. Each creates a typed incident with 0 model calls (rows above). Digest or ancestry mismatches and missing checkpoints raise the typed `CheckpointLineageInDoubt` reasons `schema_mismatch`, `ancestry_mismatch` and `missing_checkpoint`.

Worker crashes are injected at the checkpointer, not inside a model call: LangChain's chat-model plumbing converts a `BaseException` raised inside a call into an `AttributeError` (`'SimulatedWorkerCrash' object has no attribute 'generations'`). A checkpointer that loses its worker right after a durable write, and refuses every later write, models a lost process exactly. The persistent proof kills a real process.

## Changed paths and migrations

Shared seams edited under the coordinator's RRM-004 authorization:
- `app/domain/operation_execution/contracts.py`: additive. `OperationSettlement.output_payload_ref/digest/size_bytes` (an exact-address validator) and `OperationExecutionResult.reconciliation_incident_id`.
- `app/integrations/agents/deep_agents/adapter.py`: full classifier `_classify`, resume and reconstruct paths, `RuntimeInvocationFailure` wrapping, capture classification. Still the sole `create_deep_agent` site.
- `app/temporal/workflows/operation.py`: in_doubt parking behind `workflow.patched("rrm-004-park-in-doubt-units")` and the new signal `unit_reconciliation_recorded`. The activity call is byte-identical (moved into `_execute_operation`).
- `app/domain/run_control/contracts.py` and `reducer.py`: `operator_reconciliation` wait kind, `ReconcileUnitAction`, `UnitReconciliationDecision`, `RunProjection.unit_reconciliations` (defaulted), permission `workflow_run.reconcile_unit`, phase-preserving operator wait, and a `satisfy_wait` guard.
- `app/application/run_control/service.py`: `_fingerprint` now uses `contract_fingerprint`.
- `app/migrations/0020_runtime_unit_recovery_v1.sql`: new.
- `app/domain/graph_runtime/identities.py`, `materializer.py`, `app/temporal/registration/*`: **unchanged**.

Other production paths:
- `app/domain/operation_execution/checkpoint_lineage.py` (incident, result observation, typed in-doubt reasons, `accepted_leaf`, capture classification, attempt lease deadline) and `errors.py` (`RuntimeInvocationFailure`).
- `app/application/operations/checkpoint_lineage.py` and `postgres_checkpoint_lineage.py` (lease, result, incidents, reconciliation, generation boundary).
- `app/application/operations/operation_execution.py` (classification flow, narrowed rule, fenced settlement; `ResultManifestObserver` now also passes the manifest size) and `journaled_operation_execution.py` (output payload, `load_result_manifest`, `record_in_doubt`, `get_unit_reconciliation`, `unsettled_effect_ids`).
- `app/application/operations/operation_journal.py` (fingerprint).
- `app/domain/control_plane/canonical.py` (`contract_fingerprint`).
- New: `app/application/operations/unit_reconciliation.py`, `operation_recovery_composition.py` and `app/integrations/temporal_unit_reconciliation.py`.
- `app/temporal/operation_activities.py` (attempt lease deadline).

Tests:
- New: `tests/unit/operations/test_checkpoint_recovery_classification.py`, `tests/integration/temporal/test_rrm_004_worker_restart_recovery.py`, `tests/fixtures/checkpoint_recovery.py`, `tests/fixtures/rrm004_persistent_stack.py` and `tests/fixtures/rrm004_restart_worker.py`.
- Extended: `tests/fixtures/checkpoint_lineage.py` (recovery contract), `test_checkpoint_lineage.py`, `test_checkpoint_lineage_postgres.py`, `test_run_control.py`, `test_operation_execution.py`, `test_wp_cp_040.py`, `test_checkpoint_lineage_postgres_saver.py`, `test_operation_journal_stage1.py` (its fingerprint helper mirrors production) and `tests/conftest.py` (the selector loop for the restart module).

Rewritten assertions, each because it encoded a defect this ticket fixes:
- `test_three_activity_attempts_share_one_unit_key_and_dispatch_once` became `test_lost_native_holder_is_taken_over_by_fence_and_parked_in_doubt`. The old expectation `[(1, True), (2, False), (3, False)]` was the no-takeover defect of RRM-001 §7 #1.
- `test_shared_session_reuse…`: same-unit re-execution reconstructs instead of failing closed.
- The persistent drift case expects an `in_doubt` incident instead of a raised `IncompatibleCheckpointSchema` (DA-018: a digest mismatch is `in_doubt`).
- The persistent duplicate delivery and the journaled settlement now compare full results, output included.

**Migration `0020_runtime_unit_recovery_v1.sql`** is forward-only and the next free number. Its explicit schema identities are `belllabs.unit-result-observation.v1` and `belllabs.unit-reconciliation-incident.v1`. It:
- adds `lease_holder`, `lease_expires_at` and `superseded` to `runtime_unit_generations`, with a lease shape check;
- creates the insert-only `runtime_unit_result_observations`, unique per unit generation, with FKs to the generation and the transition;
- adds an index on `runtime_reconciliation_incidents (request_scope, unit_key)`;
- enables forced RLS by `belllabs.request_scope`;
- grants `belllabs_control_runtime` `SELECT, INSERT` on the new table and `belllabs_operations_readonly` `SELECT`. No role gets `UPDATE` or `DELETE`.

The lease columns use 0019's existing `UPDATE` grant on `runtime_unit_generations`; the incident table keeps its 0014 grants. All runtime writes serialize on 0019's per-unit advisory lock.

Deleted owners: none. `operation_effect_claims.lease_expires_at` (0012) stays unused. The claim row is digest-bound and immutable, so the lease lives on the unit generation, next to the RRM-003 fence.

## Deterministic verification

All commands ran from the worktree with `unset VIRTUAL_ENV`, one pytest process at a time.

| Command | Result |
|---|---|
| Owning suites, both service DSNs set: `pytest tests/unit/operations tests/unit/run_control tests/unit/orchestration tests/acceptance/control_plane/test_wp_cp_040.py tests/acceptance/control_plane/test_wp_bp_020_sandbox_rollover.py tests/integration/temporal tests/integration/deep_agents tests/integration/postgres/test_checkpoint_lineage_postgres.py tests/integration/postgres/test_operation_journal_stage1.py tests/integration/postgres/test_run_control_postgres_integration.py` | 294 passed, 1 skipped (WSL-only BP-010 recovery) |
| New crash-window module: `pytest tests/unit/operations/test_checkpoint_recovery_classification.py` | 16 passed |
| `uv run --no-sync ruff check app tests scripts` | All checks passed! |
| `uv run --no-sync mypy app` | Success: no issues found in 339 source files |
| `BELLABS_RUN_WP_BP_010_LIVE=0 BELLABS_RUN_WP_BP_020_LIVE=0 BELLABS_RUN_WP_CP_040_LIVE=0 LANGSMITH_TRACING=false uv run --no-sync pytest -q -rs` (hermetic) | 737 passed, 54 skipped, 2 xfailed, 0 failed |
| The same with `TEST_APPLICATION_POSTGRES_DSN=<disposable>` and `TEST_MONGODB_URI=<disposable>` | 767 passed, 24 skipped, 2 xfailed, 0 failed |
| `uv run --no-sync --env-file ../biotech-research-ingestion-evaluation-system/.env pytest -q -rs` | 737 passed, 54 skipped, 2 xfailed, 0 failed |
| `git diff --check 8762d3e HEAD` | clean |
| Intermittent-failure reproduction (before the fix): the single test repeated 40 times | 5 runs failed (`[False-True]` and `[True-True]`); with `PYTHONHASHSEED=1/7/10` it failed every time |
| After the fix: `PYTHONHASHSEED=<1, 7, 10, 2> pytest tests/unit/operations/test_operation_execution.py::test_journaled_operation_settles_usage_effect_and_terminalizes` | 4 passed for each seed |

Delta against the RRM-003 baseline (hermetic 716 passed, 52 skipped, 2 xfailed; services 744 passed, 24 skipped, 2 xfailed):
- **+21 passed (hermetic).** That is 16 crash-window tests, 3 `reconcile_unit` reducer tests, 1 fingerprint-order regression test and 1 in-memory recovery contract.
- **One node ID renamed, not added.** `test_three_activity_attempts_share_one_unit_key_and_dispatch_once` became `test_lost_native_holder_is_taken_over_by_fence_and_parked_in_doubt`; see Changed paths.
- **+2 skipped (hermetic).** Both are service-gated: the worker-restart proof and the Postgres concurrent-lease test. Both run in the service gate, which is +23 passed (21 + 2) with the same 24 skips. Those 24 skips are unchanged from RRM-003: 19 Agent Server endpoint, 3 live-provider flags, 1 WSL and 1 pre-existing retirement.
- **Nothing weakened.** No test was skipped, xfailed or deselected by this ticket, and no assertion was lowered. Four assertions were rewritten because they encoded defects RRM-004 fixes (see Changed paths).

## Live runtime qualification

No live LLM call was made. **Spend: USD 0.** The live harnesses stay behind their flags.

Persistent technical qualification:
- Real Temporal dev server through `WorkflowEnvironment.start_local()`, kept up across both workers.
- Real `AsyncPostgresSaver` in `rrm004_restart_saver`.
- Application PostgreSQL run control, operation journal and checkpoint lineage, composed through `compose_postgres_operation_recovery`.
- A real `create_deep_agent` graph with the deterministic scripted model.
- The production `RunControlOperationAuthority` and the production Mongo OEB binding store (`MongoOperationBindingRepository`, a dedicated database in the disposable Mongo), both added after review. Worker 2 therefore finds worker 1's binding, compares its fingerprint across processes with different hash seeds, and is admitted as a continuation.
- The worker processes share a content-addressed file payload store, a stand-in for the production S3 artifact store.

Sanitized record printed by the restart test (IDs and PIDs vary per run):

```text
RRM-004 EVIDENCE worker restart: {"attempts": [{"activity_attempt": 1, "claim_fence": 1,
  "dispatching": true}, {"activity_attempt": 2, "claim_fence": 2, "dispatching": true}],
  "classification": "interrupted", "crash_after_root_checkpoint": 6,
  "human_messages_per_call": [1, 1], "model_calls_by_worker": ["worker-1", "worker-2"],
  "namespace_digest": "bl-unit-v1:e538827f6bdca...",
  "result_checkpoint_descends_from_crash_leaf": true,
  "result_manifest_digest": "sha256:186b9982b4117f27574d443ce47d147893290779f112d9c0f6a1aa4e69306a30",
  "settlements": 1, "stamped_root_checkpoints": 8, "technical_attempt": [2],
  "worker_1_pid": 14300, "worker_2_pid": 46712}
```

## Replay and recovery artifacts

| Command | Result |
|---|---|
| `TEST_APPLICATION_POSTGRES_DSN=<disposable> pytest tests/integration/temporal/test_wp_bp_010_temporal.py tests/integration/temporal/test_wp_bp_020_temporal.py tests/integration/temporal/test_linked_runs.py tests/unit/operations/test_operation_execution.py tests/integration/temporal/test_rrm_004_worker_restart_recovery.py -k "replay or Replayer or replays or routes_bound or parked or worker_restart"` | 6 passed, 53 deselected |

- **RRM-003's captured-history replays still pass:** StageGraph any-join; GoalDirected separate children and Continue-As-New; GoalDirected cancellation; `OperationWorkflow` cross-queue.
- **Two new histories replay with `Replayer`:**
  - the parked `OperationWorkflow` (patch marker, wake-up signal, a second activity for re-classification);
  - the worker-restart history recorded on the real dev server across both workers (one scheduled activity, retried by Temporal on worker 2).
- **Semantic identity under retries.** Activity attempts, worker restarts and takeovers keep `unit_key`, generation and the `operation/{semantic_attempt_id}` workflow ID. The fence advances; the unit identity does not.

`OperationWorkflow` changed only additively: a new signal handler, and a parking loop reached only when the activity returns `in_doubt`, which no earlier history contains, under `workflow.patched`. The activity command is unchanged. The only Temporal payload change is the defaulted `OperationExecutionResult.reconciliation_incident_id`, carried in the activity result. The other contract changes are additive with defaults and are not Temporal payloads: `OperationActivityAttempt.lease_expires_at` is built inside the activity, and the settlement output-payload fields and `RunProjection.unit_reconciliations` are PostgreSQL authority. Nothing was renamed.

## Intermittent-failure diagnosis (RRM-003 merge gate)

- **Reproduced.** 40 isolated runs of `test_journaled_operation_settles_usage_effect_and_terminalizes`: 5 failed. Each failing run failed both crash parametrizations, `[False-True]` and `[True-True]`, never the crash-free ones.
- **Deterministic with a fixed hash seed.** `PYTHONHASHSEED=1`, `7` and `10` fail every time; `2` passes. A probe over 60 seeds found 7 that reorder the fingerprint (about 12%), consistent with the observed rate. Memory pressure was coincidental.
- **Traceback** (seed 1): `journaled_operation_execution.py:511 in _execute_replayable: RuntimeError: stored operation authority result conflicts with exact replay command: stored=sha256:04d4…fc14, rebuilt=sha256:dbb9…8616`. It is raised from `coordinator.acquire`, during the replay after the injected "crash after run-control claim authority".
- **Root cause.** Run control fingerprints a *revalidated copy* of each command (`LifecycleCommand.model_validate(model_dump(mode="python"))`). The journal coordinator fingerprints its *own instance*. Both hashed `command.model_dump(mode="json")`, which turns `ActorContext.permissions` and `authority_refs` (frozensets) into lists in iteration order. Iteration order depends on the per-process string-hash seed and on how the set was built: rebuilding a frozenset through validation can reorder colliding entries. `sha256_digest` sorts real sets, but it received lists. Only the crash cases replay a stored command, so only they failed.
- **Production impact.** Any crash-recovery replay of journal authority, and any cross-process re-binding (the OEB request fingerprint covers `CapabilityGrant.capabilities`), could spuriously fail in about 1 process in 8. It hit exactly the path this ticket makes live.
- **Fix.** `contract_fingerprint` dumps in Python mode, so `_normalize` sorts sets. It is used for run-control command, request and family fingerprints, the journal coordinator replay, `OperationJournalMutation.validate`, and the OEB request fingerprint.
- **Verification.** Seeds 1, 7 and 10 now pass (4/4 each). The deterministic regression test above covers it, and the restart proof runs worker 1 under `PYTHONHASHSEED=7` while worker 2 has a random seed. No retry was added. The remaining JSON-mode digest sites are latent and outside this path: RRM-015.
- **Compatibility note (review finding 6).** `contract_fingerprint` changes a stored digest only where the old digest was set-order (hash-seed) dependent. For a contract with no set-valued field, the Python-mode dump normalizes to the same canonical JSON. The independent reviewer measured 525 affected call sites, all differing only in set order. Fingerprints persisted before this change are therefore either unchanged or were already unstable across processes. Re-submitting a seed-ordered stored command fails closed (`IdempotencyConflict`); it is never silently accepted. Only disposable stacks hold such rows, so this is accepted pre-production. The remaining JSON-mode digest sites are RRM-015, which is **required before RRM-010**: `postgres_runtime_authority` hashes waits with a frozenset `scope` into `lifecycle_digest`.

## Replacement and deletion checks

- RRM-003's `_classify_not_submitted` guard is replaced by `_classify`, which still covers `not_submitted` and the fail-closed source checks.
- The service no longer settles every post-dispatch exception `failed` (RRM-001 §7 #2 resolved). A held unsettled claim no longer blocks forever (§7 #1 resolved through lease and takeover).
- No competing lifecycle store. Units, attempts, results, transitions and incidents are evidence beside the journal. Settlement authority remains the journal plus run control. The operator decision is held by the run projection.

## Unresolved risks and drift checks

What RRM-009 must plug in (production composition):
1. `compose_postgres_operation_recovery(pool, run_control=…, nudge=TemporalUnitReconciliationNudge(client))`. Pass `lineage` into `OperationExecutionService`, with the journaled coordinator over `PostgresAtomicOperationJournalRepository` and an S3 `ResultPayloadStore`.
2. The registered persistent `AsyncPostgresSaver` as the binding's checkpointer.
3. A deployment-stable `journal_claimed_by`. The claim payload digest includes it, so a per-worker value makes the replacement worker's claim replay conflict.
4. `RunControlOperationAuthority` as the operation authority. Its `verify_continuation` (review fix 2) admits retries and recoveries; the restart proof composes it.
5. `LangGraphCheckpointDescendantVerifier(registered checkpointers)` as the `verifier` of `compose_postgres_operation_recovery`.
6. A persistent OEB binding store (Mongo), so a replacement worker continues the bound attempt instead of re-binding.

The `operator` API role lacking `workflow_run.reconcile_unit`, and the missing route, are now recorded in the RRM-007 ticket (review finding 5).

What RRM-013 must know:
- Async children are not yet effect claims of the parent unit. Once they are registered as run-control effects with `operation_ref = binding_id`, the narrowed rule already turns an unsettled child into `in_doubt` (`unsettled_effect_ids`).
- Provider runs are not checkpoints in the parent namespace: classification sees only the parent's root namespace, and nested namespaces are evidence.
- `adopt_provider_run` and `orphan_child` decisions can reuse `UnitReconciliationIncident` and `reconcile_unit`'s authority pattern.

What RRM-005 must know:
- Inspection reads the lease (`runtime_unit_generations.lease_*`, `superseded`), `runtime_unit_result_observations`, the typed incidents (`incident_type = runtime_unit_in_doubt`), the run projection's `unit_reconciliations` and its `operator_reconciliation` waits (`reconciliation_state` `in_doubt` / `operator_required`), and the effect claim's `ambiguous` observation.
- All of these are written only by recovery and decision commands, never by reads.

What RRM-007 must know:
- `unit_reconciliation_recorded` is a hint transport only. The governed delivery and receipts (`accepted → delivered → applied`) for `reconcile_unit` are RRM-007's. A lost hint is recovered by re-sending, which is safe.
- The `operator_reconciliation` wait never changes phase on its own. RUN-007's "waiting if nothing else is admissible" requires family scheduling knowledge, so the family should derive it.

What RRM-008 must know:
- A parked `OperationWorkflow` ignores `request_cancel` (the wait condition watches only the hint).
- The cancellation saga must decide `cancelled` for an in_doubt unit: `abandon`-like settlement with the latest checkpoint, per EXEC-008's interrupted rule. The run stays `cancelling` with `operator_required`.
- A holder releases its lease when it stops on its own: on an exception, on `asyncio.CancelledError`, and at its lease deadline (`OperationLeaseExpired`, review fix 3). There is **no Activity heartbeat and no heartbeat timeout yet**, so a Temporal cancel does not reach a running cognitive attempt today; heartbeat-driven cancellation is RRM-008's. The lease-deadline timeout does not contradict it: a heartbeat cancel arrives as `CancelledError`, and the same release path hands over at once.
- Orphan lineages after a settlement without a transition (budget violation after a terminal leaf, provider `failed`, `abandon_unit`, cancellation of an `interrupted` unit) wedge a shared GoalDirected session namespace: the next unit classifies `foreign_descendant`. Review finding 4 was recorded as an RRM-008 acceptance item (see Review disposition).

**Continuation semantics (RRM-004 review).** `RunControlOperationAuthority.verify_continuation` admits a retry, takeover or recovery of a unit that already holds a claim when the run is `active` or `waiting`, with any wait kind. A declared wait (dependency, approval, operator reconciliation and so on) does not supersede already-claimed work. Terminal, pending, paused and cancelling runs, and foreign bindings, are refused (fail-closed).

**Residual limits of the lease deadline.**
- A blocking synchronous tool running in an executor thread cannot be cancelled by the lease timeout, because Python cannot cancel a thread. The asyncio side stops, but the thread finishes. This is owned by RRM-008's heartbeat and cancellation item.
- Each lease cut ends the Temporal Activity attempt with a retryable error, so it consumes one of the three attempts in `OperationWorkflow`'s retry policy.
- The safety margin (20% of the lease, 1–30 s) covers clock skew between the Temporal server and the worker only up to the margin. Larger skew is not covered: a worker clock running ahead ends the holder early (safe), and one running behind can let it overrun.

Other:
- **Deviation (lease placement).** The lease and fence live on `runtime_unit_generations`, not on `operation_effect_claims`; see Changed paths.
- **Deviation (settlement fencing).** The authority settlement itself is not fence-checked in the run-control transaction. Instead, the fenced `UnitResultObservation` fixes the only manifest that can settle, so a superseded holder can neither record a different result nor settle one. The identical manifest is idempotent.
- **Deviation (`start_new_generation`).** It fences and releases but does not re-run the unit at `g+1`: RRM-014.
- **Unobservable usage.** The usage of a provider call in flight when a worker dies is not observable to BellLabs. The windows above crash only at durable boundaries, so no call repeats. A mid-call loss would repeat that call (at-least-once cognition) and record only the completed call's usage.
- **Native units.** Native units dispatched by a lost holder always become `in_doubt` (`ambiguous_native_effect`); native runtimes have no checkpoint to classify. Native generations remain fixed at 1 (RRM-003 note).
- **Resume checkpoints.** A LangGraph resume with `None` input writes one extra stamped checkpoint (8 root checkpoints instead of 7 in the restart proof). The lineage stays linear and fully stamped.
- **External gates still unrun:** Agent Server endpoint suites, live providers, and the WSL-only BP-010 recovery (unchanged from RRM-003).
- **New tickets:** RRM-014 (re-admit a unit at a new generation), RRM-015 (set-order-stable contract digests outside the replay path).

### Reusable seams (mission-horizon lens)

- `decide_lease` / `UnitAttempt`: a generic claim lease with fence takeover for any at-least-once executor.
- `UnitResultObservation` + `decide_result`: a fenced, write-once result manifest for any unit kind.
- `CheckpointLineageService.admit_attempt/plan/record_result/open_incident`: scheduler- and framework-neutral recovery orchestration.
- `_classify` (adapter): the LangGraph-specific half, kept in the adapter.
- `UnitReconciliationIncident` + `ReconcileUnitAction` + `UnitReconciliationService`: a typed, operator-resolved ambiguity protocol.
- `RuntimeInvocationFailure`: a provider-neutral post-dispatch failure carrying terminal-result knowledge.
- `contract_fingerprint`: an order-stable identity digest for any contract.
- Output payload address on the settlement: digest-bound immutable outputs outside the manifest.
- `assert_checkpoint_recovery_repository_contract`: a conformance scenario for any repository.
- `CrashingSaver` / `HangAfterCheckpointSaver`: reusable crash injection at durable boundaries.

None of these carries company, fixture or provider specifics.

## Review disposition

Independent review verdict: `approve_with_fixes`. All findings are addressed on this branch with new commits; nothing was amended.

| # | Finding | Disposition |
|---|---|---|
| 1 (blocking) | A bad `accept_descendant` permanently strands the unit. A non-candidate or nonexistent key was accepted by run control, which released the wait. The next attempt re-parked `in_doubt`, but `open_incident` kept the resolved incident and `record_in_doubt` replayed the stored commands, so no wait was re-created and every further decision was `reconciliation_not_pending`. | **Fixed (`284ed20`).** (a) `UnitReconciliationService` validates the key *before* run control: it must be a recorded candidate or a verified stamped root-namespace descendant of the source (`LangGraphCheckpointDescendantVerifier`, a checkpointer read only; the root namespace, the unit generation's three stamps and the parent link are checked back to the source). A resolved incident cannot be decided again. Terminal-after-failure incidents now carry the terminal leaf as a candidate. (b) Chosen option: a superseding **incident revision**. An `in_doubt` classification after an accepted decision opens revision `n+1`: a new incident ID and identity digest, its own `operator_reconciliation` wait (`…:revision:n+1`), and its own ambiguous observation. Decisions are keyed by incident revision in the reducer, run projection and lookups. Only revision `n+1` on top of a resolved revision `n` may open (`decide_incident_opening`), so concurrent openers converge. This is spec-consistent: each revision is resolved only by a typed `reconcile_unit` command, and nothing is re-executed speculatively. (c) Tests: `test_unverifiable_accepted_descendant_is_rejected_before_run_control` (a nonexistent key and a wrong-parent key are rejected; the wait stays pending; a valid decision completes). `test_accepted_descendant_that_fails_at_dispatch_reopens_the_next_incident_revision` (accepting the verified common parent of two leaves re-parks at revision 2 with a new wait and 0 model calls; a second decision completes with the baseline digest; the projection holds both decisions). The recovery repository contract covers revision opening in memory and on Postgres, including under the runtime role. |
| 2 (blocking) | `RunControlOperationAuthority.verify` required the exact bound run version and an `active` phase on every unsettled attempt, while the first claim moves the version, so every retry, recovery or nudge re-run was rejected as non-retryable in a real composition. | **Fixed (`284ed20`).** New port method `verify_continuation(request, binding)`, called for a prior (bound) attempt; the exact check stays for the first binding. It relies on REQ-CP-EXEC-005 (retries, restarts and takeovers are not disruptive and continue the semantic attempt), REQ-CP-EXEC-014 (the bound claim continues by fence) and REQ-CP-RUN-007 (an `in_doubt` run keeps its phase). It admits a run in `active` or `waiting` whose version is at least the bound revision, with the bound configuration, capabilities, prompts, workspace and reservation. It stays fail-closed for terminal, pending, paused or cancelling runs (cancellation is EXEC-008's saga) and for a foreign binding. Tests with the **real** authority: `test_real_run_control_authority_admits_recovery_after_a_lost_worker[after_intermediate_checkpoint/after_terminal_checkpoint/before_settlement]`. In each, the first-binding check raises `Run Control revision` while the continuation passes, and recovery reaches the baseline digest with 2 model calls. `test_real_run_control_authority_admits_a_concurrent_retry_as_in_progress` (attempt 2 stands down retryably). `test_real_authority_continuation_fails_closed_for_paused_runs` (rejected while paused, with no model call; recovers after resume). `test_real_authority_continuation_rejects_terminal_or_foreign_bindings`. The **worker-restart proof** now composes the real authority with the Mongo binding store. Its first post-fix run was red until the replacement worker could find the persisted binding, which demonstrates the defect it guards. |
| 3 (non-blocking, fixed) | A live attempt could overrun its lease and keep calling the model and tools after a takeover; capture read the thread's latest checkpoint. | **Fixed (`284ed20`).** The service bounds the holder's work with `asyncio.timeout(work_budget)`. The lease deadline is the Temporal `started_time` plus start-to-close (a server timestamp) or the default lease. The margin is 20% of the lease, at least 1 s and at most 30 s; it absorbs server-to-worker clock skew and the release write. The budget is computed once from the wall clock and then enforced by the event loop's monotonic timer. On expiry the holder cancels its cognition, releases the lease and raises the retryable `OperationLeaseExpired`. Every invocation also stamps `belllabs_attempt_ref` (the lease holder), and capture selects this attempt's unique tip, which must descend from the checkpoint it pinned; otherwise it is `in_doubt`. Real heartbeat and cancel remain RRM-008's and are not contradicted. Tests: `test_slow_holder_stops_at_its_lease_deadline_before_a_takeover_proceeds` (A stops within its 3 s lease and writes nothing; B resumes, fence 2, no rejection, baseline digest). `test_an_attempt_captures_only_its_own_tip_descending_from_its_pin` (a newer tip from another attempt is ignored; a wrong pin or no tip is `in_doubt`). |
| 4 (plausible) | Orphan descendants after a settlement without a transition wedge a shared GoalDirected session. | **Recorded, not changed here.** Chosen: an explicit acceptance item in the RRM-008 ticket, which owns settling partial lineages (cancel of an `interrupted` unit, `abandon`, budget). It must decide between advancing the head over the settled branch by a recorded transition and requiring a session rollover, and prove that the next iteration proceeds. |
| 5 | The `operator` API role lacks `workflow_run.reconcile_unit`, and no route exposes `UnitReconciliationService`. | **Recorded** in the RRM-007 ticket body (governed delivery and receipts for `reconcile_unit`). |
| 6 | Fingerprint compatibility. | **Recorded** above (Intermittent-failure diagnosis, Compatibility note). RRM-015 is marked **required before RRM-010** in its body and in the index; the RRM-014 and RRM-015 index rows were added. RRM-014 stays non-blocking for RRM-010. |
| 7 | Evidence README. | This section, the updated gates below, and the corrected RRM-008 note on heartbeat cancellation (Unresolved risks). |

Post-review gates (tested code head `284ed20`):

| Command | Result |
|---|---|
| `uv run --no-sync ruff check app tests scripts` | All checks passed! |
| `uv run --no-sync mypy app` | Success: no issues found in 340 source files |
| Owning suites with both service DSNs (the list above plus `tests/integration/mongodb/test_operation_execution_mongodb_integration.py`) | 305 passed, 1 skipped (WSL-only BP-010 recovery) |
| Worker-restart proof, both DSNs: `pytest -s tests/integration/temporal/test_rrm_004_worker_restart_recovery.py` (real authority, Mongo bindings) | 1 passed in 40 s; worker 1 → worker 2, attempts fences `[1, 2]`, `interrupted`, human counts `[1, 1]`, one settlement, `technical_attempt [2]` |
| Full pytest, hermetic (`BELLABS_RUN_*_LIVE=0 LANGSMITH_TRACING=false`) | 747 passed, 54 skipped, 2 xfailed, 0 failed |
| Full pytest with `TEST_APPLICATION_POSTGRES_DSN` and `TEST_MONGODB_URI` | 777 passed, 24 skipped, 2 xfailed, 0 failed |
| `git diff --check 8762d3e HEAD` | clean |

The delta against the pre-review head is +10 passed in both full runs: the 10 new review-fix tests in `test_checkpoint_recovery_classification.py`. The skips are unchanged. No test was skipped, xfailed or deselected. Two existing assertions were adjusted to the new protocol: the role test's incident count is 3 (revision 2 added to the contract), and the terminal test asserts the recorded candidate.

### Re-review (findings 1-3 approved; one regression from the fix)

| Finding | Disposition |
|---|---|
| The fix for finding 1 rejected every non-`operator_required` incident. An exact resend of an accepted `reconcile_unit`, the documented recovery when the wake-up hint is lost (for example Temporal down), was therefore refused before run control could replay it, and the parked `OperationWorkflow` never woke. | **Fixed in `89bbdf7`.** For a resolved incident revision, the service accepts the resend only if run control holds a decision with the same `command_id`, `incident_id`, decision and accepted key, and the lineage incident records the same command. Validation is skipped; run control replays the stored result idempotently; `apply_reconciliation` is idempotent; and the hint is sent again. Any other decision for a resolved revision is rejected ("resolved by another decision"). Test: `test_checkpoint_recovery_classification.py::test_lost_wake_up_hint_is_recovered_by_resending_the_same_decision`. It runs a real parked `OperationWorkflow` (time-skipping) whose first hint fails ("Temporal unavailable"): the incident is resolved, the workflow stays `RUNNING`, and a different decision is rejected. The resend of the same command is `ACCEPTED`, the workflow wakes and completes `failed` / `in_doubt_abandoned`, with 2 hint attempts, 0 new model calls and one decision in the projection. |
| Docs: continuation semantics and residual lease limits. | Recorded in the RRM-004 ticket and in Unresolved risks above. |

Re-review gates (tested code heads `89bbdf7` and `d296481`):

| Command | Result |
|---|---|
| `uv run --no-sync ruff check app tests scripts` / `uv run --no-sync mypy app` | All checks passed / no issues in 340 source files |
| Owning suites with both service DSNs (`89bbdf7`) | 306 passed, 1 skipped (WSL-only) |
| Worker-restart proof with both DSNs (`89bbdf7`) | 1 passed in 39 s; worker 1 → worker 2, fences `[1, 2]`, `interrupted`, one settlement |
| Full pytest, hermetic, on `89bbdf7` | 747 passed, **1 failed**, 54 skipped, 2 xfailed. The failure was in this ticket's own regression test, `test_command_fingerprint_is_independent_of_set_iteration_order`: its helper needs two equal frozensets that iterate differently, and with only 23 permission names some hash seeds produce no such collision. This is a test-fixture seed dependence, not a product defect. It is fixed in `d296481` by padding the probe set to 400+ names, and verified passing for `PYTHONHASHSEED` 1-30. |
| Full pytest, hermetic, on `d296481` | 748 passed, 54 skipped, 2 xfailed, 0 failed |
| Full pytest with `TEST_APPLICATION_POSTGRES_DSN` and `TEST_MONGODB_URI`, on `d296481` | 778 passed, 24 skipped, 2 xfailed, 0 failed |
| `git diff --check 8762d3e HEAD` | clean |

The delta against the post-review head is +1 passed: the resend regression test. The skips are unchanged.

## Integration merge gates (coordinator, merge commit `fcefd54`)

| Command | Result |
|---|---|
| `uv run --no-sync ruff check app tests scripts` | All checks passed |
| `uv run --no-sync mypy app` | no issues, 340 files |
| Hermetic full pytest (`BELLABS_RUN_*_LIVE=0 LANGSMITH_TRACING=false uv run --no-sync pytest -q`) | 748 passed, 54 skipped, 2 xfailed, 0 failed |
| Full pytest, `--env-file ../biotech-research-ingestion-evaluation-system/.env`, with `TEST_APPLICATION_POSTGRES_DSN` and `TEST_MONGODB_URI` (disposable stack) | 778 passed, 24 skipped, 2 xfailed, 0 failed |
| `git diff --check` on the merge | clean |

An earlier attempt to run these gates was stopped by the host for low memory before any suite finished. No result was recorded from it. The runs above were made after memory was freed.

## Final disposition

accepted
