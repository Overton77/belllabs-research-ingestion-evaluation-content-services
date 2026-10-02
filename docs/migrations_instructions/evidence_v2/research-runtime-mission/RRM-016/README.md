# RRM-016 implementation evidence

Disposition: ready_for_review (independent review `approve_with_fixes`; fixes 1-5 applied in `0c8522f` and the following documentation commit; re-review pending)
Recorded date: 2026-10-02 (America/New_York)
Qualification identity: RRM-016 compose GoalDirected operations with the run-control journal and operation authority. Requirements: REQ-CP-RUN-006 (budgets reserve, consume, release and settle), REQ-CP-RUN-007 (consequential effects claimed and reconciled to exactly one settlement), REQ-CP-RUN-009 (usage settles exactly once, AMD-RRM-001), REQ-CP-EXEC-014 (claim-fenced attempts), REQ-CP-DA-013 (exact exclusive writable slots), REQ-CP-DA-018 (crash windows), REQ-BP-GD-004 (independent verifier), REQ-BP-GD-011 (durable pause), REQ-BP-GD-012 (shared-session ordering).
Base revision and head revision: base integration `6e77850`. Integration merged in three times (no rebase): `fd10c8e` (CR-2 at `ac7daf9`), `ddec4fa` (RRM-006 at `2799e17`), `54cd977` (CR-3 at `9d0ffbd`). Code commits: `5969f52` (implementation and tests), `aa87988` (captured post-change history, fork reuse outcome, RRM-018 reproduction), `731b770` (RRM-019 reproduction). Tested code head before review: `731b770` (its documentation commit `2c43b43` also corrected the spec identifiers in one comment). Review-fix code commit: `0c8522f`; **tested code head after review: `0c8522f`** (see Review disposition). The documentation commit recording the review follows it and changes no code. Not merged into integration (the coordinator owns review and merge).
Framework/package baseline: `uv sync --frozen` from the committed `uv.lock` (no dependency change); CPython 3.12 (Codex runtime); temporalio 1.30.0 (`WorkflowEnvironment.start_local` dev server), deepagents 0.7.5, langgraph 1.2.10, langgraph-checkpoint-postgres 3.1.1, pydantic 2.13.4, pytest 8.4.2, ruff 0.15.22, mypy 1.20.2.

## Worktree provenance

- Worktree `C:\Users\Pinda\Proyectos\Biotech\biotech-research-ingestion-evaluation-system-rrm-016`, branch `wp/rrm-016-goal-directed-journaled-operations`, created by the coordinator from integration `6e77850`; clean at kickoff.
- The main checkouts were not touched; `.env` was never copied or printed (the service gate loads it with `--env-file`).
- Disposable stack only (`127.0.0.1:55432/belllabs`, `127.0.0.1:27017`), always under the shared stack lock; no container was created, stopped or removed. Temporal ran only as `start_local` and the time-skipping test server.
- The ticket file existed only on `wp/rrm-006-forks` at kickoff; it arrived on this branch with the RRM-006 merge and was updated there.

## Implemented contracts and seams

**The diagnosis had three incompatibilities, not two.** RRM-006 masked the third with an accepting authority.

1. **Stale bound revision.** The preparer bound `run_control_revision = expected_run_version`, the version *before* its own admission. The journaled effect claim is made at exactly the bound revision (`JournaledOperationExecutionCoordinator.acquire`), so it was STALE. The first-binding check (`run.version == run_control_revision`) failed too.
   - **Fix:** the preparer binds `expected_run_version + 1`, the version its own atomic admission produces, as StageGraph does (`control_revision = projection.run_version + 1`). It then verifies the admission receipt produced exactly that version. The Deep Agent binding's `control_revision` matches.
2. **Privileged goal context.** The `goal-context:{run}:{iteration}:{role}` segment was `authored_instruction`, so it was not a configured prompt source.
   - **Fix:** it is admitted as `untrusted_content`; see Open decisions.
3. **Workspace slots (new).** GoalDirected operations bound no compiled slots (`slot_bindings=()`, writable `/goal/{i}/{role}/work`). The real authority rejects that: `operation writable paths do not exactly match its workspace contract`.
   - **Fix:** a GoalDirected unit binds the exact compiled slots under its role-scoped root `/goal/{iteration}/{role}` (`goal_unit_workspace_root`, `app/domain/orchestration/runtime_units.py`). Each slot is owned by the iteration (executor) or the evaluator (verifier).
   - `RunControlOperationAuthority._verify_bound_authority` recomputes that root from the digest-bound runtime unit and compares the exact rebased slot set. StageGraph units and units without a runtime unit have no root, so their behaviour is unchanged.
   - Executor and verifier writable paths stay disjoint, as the interpreter's REQ-BP-GD-004 isolation check requires.
   - A template without compiled slots fails closed at preparation (review fix 3).
   - Before it derives the root, the authority admits a GoalDirected unit only on a run whose execution target is GoalDirected, and only when the unit's location names the request's operation (`goal-iteration/{n}/{role}`) and run (review fix 1).

**One governed composition.** Every executor and verifier operation now runs through `OperationExecutionService`, using the real `RunControlOperationAuthority`, the `JournaledOperationExecutionCoordinator` and `CheckpointLineageService`. Each operation is claimed at its bound revision, its result manifest is fenced, and it is observed and settled once in one authority batch: usage, effect settlement and settlement evidence bound to the binding.

**The family consumes the settlement; it no longer records usage.** This mirrors StageGraph's `decide_result`, which reads the current version and requires the reservation to be settled.
- `RunControlGoalOperationSettlements` (`app/application/orchestration/goal_directed.py`) is the `GoalOperationSettlementPort`. It is called by `GoalDirectedOperationResultService.reconcile` before any family document is persisted. It verifies, from run control:
  - exactly one usage record under `operation_settlement_id(binding)`, with `authority_ref = binding` and the operation's own reservation;
  - that the reservation is released;
  - that the settled usage equals the operation result's usage;
  - the operation's own effect claim, settled by that settlement (unless the usage is pending external);
  - exactly one accepted settlement evidence with the binding as authority;
  - `run.version >= bound revision`.
- It returns `GoalOperationSettlement` (`belllabs.goal-operation-settlement.v1`), whose `settled_run_version` is the current version. Facts are read first and the run version last, so that version is at or after the settlement.
- `GoalOperationReconciliationResult.settlement` is an additive field, absent in old histories.
- `GoalDirectedWorkflow._consume_settlement` (behind `workflow.patched("rrm-016-journaled-goal-settlement")`) continues from `settled_run_version`. It fails closed, non-retryably, with `goal_operation_settlement_missing` when no settlement is present, and with `goal_operation_settlement_mismatch` on a reservation or usage mismatch or a version regression.
- Pre-patch histories take `_settle_operation` (`goal:usage:{reservation}` `record_usage`) unchanged.
- The run-level `baseline` reservation is still released by the family (`goal:usage:baseline`). It is not an operation.

**Production composition.** `compose_goal_directed_activities` wires `RunControlGoalOperationSettlements(run_control)`. Public helpers: `operation_settlement_id(binding_id)` (`operation_execution.py`) and `operation_effect_claim_id(request_scope, binding_id)` (`journaled_operation_execution.py`). Their derivations are unchanged.

## Requirement-to-evidence map

| Requirement | Test | Observed assertion |
|---|---|---|
| EXEC-014 bound revision; trust class; DA-013 slots | `tests/unit/orchestration/test_rrm_016_goal_directed_settlement.py::test_preparation_binds_the_admission_revision_role_slots_and_untrusted_context` | Admission at v2 → `resulting_run_version == 3 == run_control_revision == deep_agent_binding.control_revision == run.version`. `goal-context:{run}:1:executor` is `UNTRUSTED_CONTENT`; the only privileged source is `prompt:system@1`. The workspace writes `("/goal/1/executor/work",)`, slot `work`, owner `ITERATION` |
| Real authority admits and fails closed | `…::test_real_authority_admits_goal_operations_and_rejects_privileged_or_foreign_shapes` | `verify` (first binding) and `verify_continuation` both pass. Rejections: a privileged goal context → `privileged prompt segments`; the executor's slot under the verifier root → `workspace slots do not exactly match`; the pre-change revision (2) → `not bound to the accepted Run Control revision` |
| RUN-006/007/009 settled once; family consumes | `…::test_operations_settle_once_and_the_family_consumes_the_settlement` | Both settlement IDs are `operation_settlement_id(binding)`, with usage `{"tokens.total": 10}`. The executor's `settled_run_version` is its bound revision + 3 (claim, observation, settlement). `usage_records` holds exactly the two settlements with their bindings and reservations; consumed 20; the reservation is released. Both effects are `SUCCEEDED`; settlement evidence is accepted with the binding as authority. The pre-change family command `goal:usage:{reservation}` is now **REJECTED `reservation_required`**, and consumed stays 20. A repeated reconciliation returns the same settlement. The goal context never reached a system prompt |
| Fail closed before settlement | `…::test_settlement_consumption_fails_closed` | No operation run → `GoalOperationSettlementUnavailable: usage is not recorded`, and no iteration document is persisted. Drifted usage → `…differs`. Settled → consumed |
| Accounting before/after | `…::test_usage_accounting_before_and_after_the_governed_composition` | See "Usage accounting" |
| DA-018 crash before/after settlement (RRM-004 harness) | `tests/unit/operations/test_rrm_016_goal_directed_recovery.py::test_goal_directed_unit_crash_windows_converge_to_one_settlement[after_intermediate_checkpoint-6 / before_settlement-None / after_settlement-None]` | A GoalDirected executor unit prepared by the real preparer runs through the harness with the **real** authority. In every window: model calls `[(1, 0), (1, 1)]` (two calls, the input seen once); one usage record `[settlement_id]`; consumed 10; one `SUCCEEDED` effect; one accepted settlement evidence; one journal settlement. The family consumes the identical settlement twice. The result digest is identical in all windows (`sha256:77b220377553…`). Technical attempts `[[2]]` for both crash windows and `[[1]]` for redelivery |
| Crash injection is real | `…::test_crash_injection_is_real_for_the_before_settlement_window` | `SimulatedWorkerCrash` is raised after both model calls, with no journal settlement |
| GD-011 pause + journaled run; GD-012; isolation | `tests/integration/temporal/test_rrm_016_goal_directed_journaled.py::test_journaled_goal_directed_run_settles_each_operation_once_through_a_pause` (time-skipping; production `coordinator_activities("GoalDirected", …)` and `OperationExecutionActivities`) | The pause is `delivered` while the executor runs (phase `active`), then `applied` at the boundary (`paused`, next iteration 2, only `baseline` reserved, two usage records). After resume: `complete`, two iterations. Four distinct bindings; `usage_records == {4 settlements, "goal-usage:baseline"}`; each with `authority_ref = binding` and 10 tokens; consumed 40; nothing reserved. Four `SUCCEEDED` claims; four accepted evidences; `COMPLETED`. Model turns: `executor 1 (1 human), verifier 1 (1), executor 2 (2), verifier 2 (2)`. The executor session is reused across iterations; the verifier session is distinct; no goal context in a system prompt. The history's only usage command is `goal:usage:baseline`; patch `rrm-016-journaled-goal-settlement` present; replays |
| Fail closed in the workflow | `…::test_family_fails_closed_without_a_run_control_settlement` | The workflow fails `goal_operation_settlement_missing`; the family records no `record_usage` in its place |
| Replay: pre-change path kept | `…::test_pre_change_goal_directed_history_keeps_its_own_usage_commands` | `goal_directed_two_iterations.run1.json` (RRM-007 capture, base `d46f548`) has no RRM-016 patch and four `goal:usage:` commands, and replays |
| Replay: post-change capture | `…::test_post_change_goal_directed_history_replays_on_the_journaled_path` | New fixture `tests/fixtures/histories/rrm016_post_change/goal_directed_journaled_pause_resume.run1.json` (406 KB) carries the patch and only `goal:usage:baseline`, and prepares executor, executor, verifier, verifier |
| Temporal payload unchanged | `…::test_operation_result_contract_is_unchanged` | `OperationExecutionResult` gains no field |
| Real Temporal, PostgreSQL authority, worker restart | `tests/acceptance/control_plane/test_rrm_016_goal_directed_demo.py::test_journaled_goal_directed_run_on_real_temporal` | See "Live runtime qualification" |
| Reuse outcome (RRM-006) | `tests/acceptance/control_plane/test_rrm_006_semantic_forks.py::test_goal_directed_fork_starts_fresh_with_the_patched_goal` (moved to the governed composition) | `excluded_units == []` (no `not_accepted`); 2 reuse candidates; decisions exactly `{("not_reusable", "goal_revision_identity_is_run_bound")}`; `reused_unit_keys == []`; 2 accepted settlements on the source |

### Usage accounting (same iteration, old vs new composition)

`test_usage_accounting_before_and_after_the_governed_composition` runs iteration 1 (executor and verifier, real Deep Agent) twice:
- "before" is the composition RRM-006 had to use: no journal, an accepting authority, and the family recording `goal:usage:{reservation}` itself;
- "after" is the governed one.

Printed record:

| | before | after |
|---|---|---|
| usage records | `goal-usage`, `goal-usage` (family) | two operation settlements (binding as authority) |
| consumed `tokens.total` | 20 | 20 |
| usage records with operation authority | 0 | 2 |
| effect claims / settled | 0 / 0 | 2 / 2 |
| accepted operation settlements | 0 | 2 |
| settlements consumed by the family | 0 | 2 |

The same usage is consumed once in both. Only the governed composition claims, settles and accepts each operation in run control; RRM-006 saw the "before" units as `not_accepted`.

With the journal composed, the old family's own `record_usage` is rejected `reservation_required` (the settlement test above): the pre-change family could not run on the journaled boundary at all, which is why RRM-006 had to drop the journal.

## Changed paths and migrations

**Migrations: none.** The composition uses existing storage: run control, journal, lineage and the binding store.

Coordinator-owned shared seams (from the authorized list): **none edited.** `contracts.py`, `identities.py`, `adapter.py`, `materializer.py`, `app/temporal/registration/*`, the run-control reducer, service, repositories and API, and `app/migrations/*` are unchanged.

Shared modules outside the list, each a minimal edit:
- `app/application/operations/operation_execution.py`: the GoalDirected root in `_verify_bound_authority`'s slot set; a public `operation_settlement_id` (the existing derivation).
- `app/application/operations/journaled_operation_execution.py`: a public `operation_effect_claim_id` (the existing derivation).

GoalDirected-owned paths:
- `app/application/orchestration/goal_directed.py`: the settlement port, reader and `RunControlGoalOperationSettlements`; bound revision; slot rebasing; trust class.
- `app/domain/orchestration/goal_directed_runtime.py`: `GoalOperationSettlement` and `GoalOperationReconciliationResult.settlement`, both additive.
- `app/domain/orchestration/runtime_units.py`: `goal_unit_workspace_root`.
- `app/temporal/workflows/goal_directed.py`: the patch and `_consume_settlement`.
- `app/temporal/activities/goal_directed.py`: composition.

New tests:
- `tests/unit/orchestration/test_rrm_016_goal_directed_settlement.py` (5);
- `tests/unit/operations/test_rrm_016_goal_directed_recovery.py` (4);
- `tests/integration/temporal/test_rrm_016_goal_directed_journaled.py` (5 + RRM-019 strict xfail);
- `tests/acceptance/control_plane/test_rrm_016_goal_directed_demo.py` (1, DSN-gated);
- `tests/integration/mongodb/test_goal_directed_documents_mongodb.py` (RRM-018 strict xfail, Mongo-gated).

New fixtures:
- `tests/fixtures/goal_directed_journaled.py` (the composition);
- `tests/fixtures/temporal_history.py` (patch IDs, activity inputs);
- the captured post-change history.

Changed tests and fixtures, none weakened:
- `test_wp_bp_020_temporal.py`: the fake reconcile carries a `fake_settlement`.
- `test_wp_bp_020_sandbox_rollover.py`: fake run control, so it gets the explicit `FixtureGoalSettlements` port.
- `test_wp_bp_020_live.py`: moved to the governed composition (journal, real authority, compiled `/work` slot, settlement port), because the old harness could no longer terminalize. **Not run** (flag-gated).
- `test_rrm_006_semantic_forks.py`: the GoalDirected fork is governed, with stronger assertions; `rrm006_fork_stack.py` docstring updated.
- `test_rrm_007_replay_pre_change_histories.py`: the "no rrm-007 marker" assertion compared `marker_name`, which is always `core_patch` in the Python SDK, so it was **vacuous**. It now decodes the patch IDs (stronger).
- `checkpoint_recovery.py`: the harness accepts `run_control`.
- `conftest.py`: the demo uses the psycopg selector loop.

Merge commit `ddec4fa` also carries the RRM-006 demo update, needed for that merge to stay green; its message says so.

## Deterministic verification

All runs from the worktree with `unset VIRTUAL_ENV`, one pytest process at a time. Full runs and DSN runs were made under the stack lock.

| Gate | Command | Result |
|---|---|---|
| Owning suites (`54cd977`) | `env -u TEST_APPLICATION_POSTGRES_DSN -u TEST_MONGODB_URI LANGSMITH_TRACING=false uv run --no-sync pytest -q tests/unit/orchestration tests/unit/operations tests/unit/run_control tests/integration/temporal/test_rrm_016_goal_directed_journaled.py tests/integration/temporal/test_wp_bp_020_temporal.py tests/integration/temporal/test_rrm_007_boundary_interventions.py tests/integration/temporal/test_rrm_007_replay_pre_change_histories.py tests/integration/temporal/test_rrm_005_search_attributes.py tests/integration/temporal/test_coordinator_temporal_runtime.py tests/acceptance/control_plane/test_wp_bp_020_sandbox_rollover.py tests/acceptance/control_plane/test_wp_cp_040.py tests/unit/control_plane/test_digest_set_order_guard.py` | 390 passed |
| Ruff (`731b770`) | `uv run --no-sync ruff check app tests scripts` | All checks passed |
| Mypy (`731b770`) | `uv run --no-sync mypy app` | Success: no issues found in 366 source files |
| Full hermetic (`731b770`) | `env -u TEST_APPLICATION_POSTGRES_DSN -u TEST_MONGODB_URI BELLABS_RUN_WP_BP_010_LIVE=0 BELLABS_RUN_WP_BP_020_LIVE=0 BELLABS_RUN_WP_CP_040_LIVE=0 LANGSMITH_TRACING=false uv run --no-sync pytest -q` | **968 passed, 74 skipped, 3 xfailed**, 0 failed |
| Full with services and `.env` (`731b770`) | `TEST_APPLICATION_POSTGRES_DSN=<disposable> TEST_MONGODB_URI=<disposable> LANGSMITH_TRACING=false BELLABS_RUN_*_LIVE=0 uv run --no-sync --env-file ../biotech-research-ingestion-evaluation-system/.env pytest -q` (DSNs exported explicitly) | **1011 passed, 30 skipped, 4 xfailed**, 0 failed |
| `git diff --check 6e77850 731b770` | | clean |

The coordinator's baselines (913/65/2 hermetic, 948/30/2 services) predate RRM-006, CR-2 and CR-3. The deltas below are against the integration head this branch merged: `9d0ffbd`, recorded at 954/72/2 hermetic and 996/30/2 services.
- **Hermetic: +14 passed** (5 unit, 4 recovery, 5 Temporal) and **+2 skipped** (the DSN-gated demo and the Mongo-gated RRM-018 reproduction). **+1 xfailed**: the RRM-019 strict reproduction.
- **Services: +15 passed** (the 14 plus the demo) and **+2 xfailed** (RRM-018 and RRM-019).
- Before RRM-019's test was added: hermetic 968/74/2 on `aa87988` and `54cd977`; services 1011/30/3 on `54cd977`.
- No test was skipped, deselected or weakened. The only new markers are the two narrow `xfail(strict=True, raises=…)` citing RRM-018 and RRM-019.

## Live runtime qualification

No live model call. **Spend: USD 0.**

**Real Temporal demonstration** (`test_rrm_016_goal_directed_demo.py`, under the lock):
- **Runtime:** `WorkflowEnvironment.start_local`; the root is started by `TemporalWorkflowSubmitter`; commands go through `POST /run-control/v1/runs/{run}/commands` on the ASGI app and are delivered root-first.
- **Authority:** application PostgreSQL holds run control (with the family writer pool), the boundary ledger, the operation journal and checkpoint lineage (`compose_postgres_operation_recovery`).
- **Checkpointer and cognition:** a real `AsyncPostgresSaver` in schema `rrm016_goal_saver`, behind a real `create_deep_agent` graph with the deterministic `GoalScriptedModel`.
- **MongoDB:** OEB bindings (`MongoOperationBindingRepository`) and the GoalDirected templates (`MongoGoalDirectedDocumentRepository.get_template`).
- **In memory:** family detail documents (see RRM-018).
- **Results:** a file payload store.

Each worker is a fresh composition sharing only the durable stores.

Printed record (`RRM-016 EVIDENCE demonstration`):
- **Identity:** run `865ba49d-fd92-5559-b012-f16c53a1cca9`, root `belllabs-run/865ba49d-…`, family `family/865ba49d-…/1`.
- **Operations:** `goal-iteration/{1,2}/{executor,verifier}:attempt:1`, all `completed`, each with usage `{"tokens.total": 10}` in `operation_settlements` (revision 1).
- **Budget:** consumed `tokens.total` 40; nothing reserved. `budget_ledger` consumption entries: one per settlement plus one for `goal-usage:baseline` (`[1, 1, 1, 1, 1]`). Usage records: four `operation-settlement` (binding as authority) and `goal-usage:baseline`.
- **Effects:** four effect claims settled; four accepted settlement evidences.
- **Pause (REQ-BP-GD-011):** `pause` was accepted and delivered while the first executor ran (phase `active`), then applied at the iteration boundary. **Worker `rrm016-worker-1` was stopped while paused**; PostgreSQL then held iteration 1's two settlements and only the `baseline` reservation. Worker `rrm016-worker-2` resumed. Receipts: `pause 1 accepted/delivered/applied`, `resume 2 accepted/delivered/applied`.
- **Shared session (REQ-BP-GD-012) across the restart:** the executor's two transitions share one namespace. Iteration 2's `source_checkpoint_id` is iteration 1's `result_checkpoint_id`; all classifications are `not_submitted`. Model turns: worker 1 `[executor 1 ×2 (1 human), verifier 1 ×2 (1)]`, worker 2 `[executor 2 ×2 (2 humans), verifier 2 ×2 (2)]`. The verifier namespace is distinct.
- **Family history:** only `goal:usage:baseline`; patch `rrm-016-journaled-goal-settlement`. Root (1 run) and family (1 run) replayed.
- **Outcome:** terminal outcome `completed`.

**WP-BP-020 live harness:** moved to the governed composition, but not run (flag-gated, needs OpenAI spend). This is an unresolved gate.

## Replay and recovery artifacts

| Command | Result |
|---|---|
| `env -u … uv run --no-sync pytest -q tests/integration/temporal/test_rrm_016_goal_directed_journaled.py tests/integration/temporal/test_rrm_007_replay_pre_change_histories.py tests/integration/temporal/test_wp_bp_020_temporal.py tests/integration/temporal/test_wp_bp_010_temporal.py tests/integration/temporal/test_linked_runs.py tests/unit/operations/test_operation_execution.py -k "replay or Replayer or replays or history or histories"` (`54cd977`) | 11 passed |

These cover:
- the five RRM-007 pre-change histories, including two GoalDirected ones, whose patch IDs are now actually decoded;
- the RRM-016 pre-change usage-path check and the post-change capture;
- the WP-BP-010/020 in-run replays.

In-run replays also pass in the RRM-016 Temporal test, the RRM-007 GoalDirected scenarios (owning suites) and the demonstration.

**Patching.** The only command change is the removed per-operation `record_usage`. It sits behind `workflow.patched("rrm-016-journaled-goal-settlement")`, evaluated at the first settlement point after each operation's reconciliation.

**Additive inputs.** `GoalOperationReconciliationResult.settlement` defaults to `None`. Everything else changed inside activities, which are never re-executed on replay. No activity, signal, query, update or workflow type was renamed.

**Recovery.** GoalDirected crash windows run on the RRM-004 harness (rows above). The recovered attempt classifies `interrupted` or `observed_unsettled`, or returns `settled`, and in every window it converges to one settlement.

## Replacement and deletion checks

- **Replaced for new histories:** per-operation family usage recording (`goal:usage:{reservation}`). It is retained only for pre-patch histories.
- **Deleted:** nothing.
- **Unchanged:** the StageGraph path and the journal and authority semantics for other units.
- **Lifecycle store:** no second one was added. The family reads run-control facts.

## Unresolved risks and drift checks

**Open decisions (for the coordinator)**

1. **Goal-context trust class: `untrusted_content`.** No spec names a class for family-derived context, so I chose the most restrictive admissible one.
   - Spec text relied on:
     - SPEC-CP-DEFINITIONS, "Security, tenancy, redaction, and secrets": "Prompt text, catalog descriptions, retrieved content, … are untrusted inputs".
     - SPEC-CP-DEEP-AGENT-RUNTIME invariant 5: "Capability availability, prompts, Skills, MCP metadata, middleware, checkpoints, and model output never grant authority".
     - SPEC-CP-DEEP-AGENT-RUNTIME, Security: "Prompt/context segments and capability metadata are trust-classified".
     - SPEC-BP-GOAL-DIRECTED invariant 1: "Goal text cannot expand the objective, acceptance, inputs, authority, budget, or prohibited-work envelope".
     - SPEC-BP-GOAL-DIRECTED, Security: "The objective and context cannot grant capabilities".
   - Why: the segment carries the handoff and the executor's output refs, which derive from model output.
   - Alternative: `admitted_input`, the StageGraph precedent for a reducer-admitted cycle objective. The adapter renders both identically, as user content.
2. **Role-rooted slots in the generic authority.** The authority admits the compiled slot set rebased under `/goal/{iteration}/{role}`, which it recomputes from the unit identity. Spec text relied on: REQ-CP-DA-013 ("exact exclusive writable slots") and REQ-BP-GD-004 (independent verifier).
   - Alternative: declare per-role slots in a versioned Workflow Type workspace contract.
   - Production templates must carry compiled slots. RRM-009 must compose them; a template without them fails closed at preparation.
3. **Consumption reads the current run version**, as StageGraph's `decide_result` does, rather than the version the settlement batch produced. A concurrent version move, for example a cancel, makes the family's next command stale; since review fix 2 the family retries it once at the reported version, or enters its cancellation boundary when the run is `cancelling`.
4. **Test-only settlement port.** `FixtureGoalSettlements` is used only where run control is itself a fixture (sandbox rollover). The governed port is proved everywhere else.

**Unresolved gates**
- `test_wp_bp_020_live.py`: governed composition, not run (live provider, flag-gated).
- `test_wp_bp_010_recovery.py`: WSL-only, unchanged.
- The Agent Server endpoint suites: unchanged skips.

**New tickets**
- **RRM-018:** an unchanged Goal Revision re-persisted at the next iteration conflicts in MongoDB (`_insert_exact` compares `recorded_at`). Any GoalDirected run of two or more iterations on Mongo documents fails at the second executor preparation. Strict-xfail reproduction. The demonstration uses in-memory family documents for that reason.
- **RRM-019:** the family promotes the union of all iterations' output refs, but the terminalization proposal names only the last executor's, so the reducer rejects `terminal_output_mismatch` when iterations produce different refs. Strict-xfail reproduction. The RRM-016 fixtures use one stable output ref, as the existing fixtures did.

**Other risks**
- **Claim revision.** The journaled claim requires `run.version == bound revision` at claim time. A version move between the operation's admission and its first claim makes the claim STALE (`shadow_denied`, then `OperationExecutionInProgress`).
  - Review fix 2 covers the family's own commands (admissions and lifecycle facts), not this window inside the operation boundary.
  - GoalDirected operations are sequential and pending boundary commands do not move the version, so only an outside command landing in that short window (a late child effect, a cancel) can cause it.
  - It is the same for StageGraph with concurrent admissions. It was not changed here.
- **Re-admission and RRM-018.** A re-admitted executor (review fix 2) persists its Goal Revision again; on the MongoDB document repository that is the RRM-018 conflict, so RRM-018 matters for this path too.

- **Merge-commit content.** RRM-006's GoalDirected fork demonstration was updated inside merge commit `ddec4fa`, so that merge stays green.

**Deploy note (review finding 2).** Drain every in-flight GoalDirected run started before RRM-016 (let it finish or cancel it) before deploying this change. Replay of their histories stays deterministic, but once such a run executes past its recorded history on RRM-016 workers, `workflow.patched` returns true there, so the run mixes compositions:
- an operation the old preparer already admitted is bound to the pre-admission run version, so the journaled boundary's claim at that revision is stale and the operation never dispatches (`OperationExecutionInProgress`);
- an operation the old composition settled without the journal has no run-control settlement, so the patched family fails closed (`goal_operation_settlement_missing`).

Runs started after the deploy take the patched paths throughout.

**What RRM-008 must know (the new GoalDirected settlement path)**
- **The cancellation seam (review fix 2).** When an authority result shows the run already `cancelling` (a stale family admission or lifecycle fact, or a family `cancel` rejected because the run is cancelling), the family does not retry and does not issue another cancel: `_enter_cancellation` marks the run cancelling and calls `_stop_for_cancellation`, which today raises the existing `goal_cancelling` hand-off. Replace `_stop_for_cancellation`'s body with the saga; `_enter_cancellation` is the single entry for authority-observed cancellation. Proven by `test_outside_cancel_enters_the_cancellation_boundary_not_a_lifecycle_failure`.
- **Stale retries.** Behind `rrm-016-stale-version-retry`, a stale family command is retried once at the reported version under a new identity (`…:at-version:{v}` for lifecycle facts; `admission_attempt=2` and `…:attempt:2` for admissions). Keep `tests/fixtures/histories/rrm016_post_change/goal_directed_stale_version_retry.run1.json` replaying.
- A GoalDirected operation's cancelled or failed settlement is written by the journaled coordinator in the operation boundary (`settle`, status `cancelled` maps to `EffectDisposition.CANCELLED`), with usage and reservation release, exactly as for StageGraph.
- The family's `reconcile_operation` requires a `completed` result. A cancelled operation raises the child's `ChildWorkflowError`, and the family's existing `_stop_for_cancellation` issues `cancel`.
  - RRM-008 should add a cancellation consumption path that reads the cancelled settlement through `RunControlGoalOperationSettlements`. Its checks already accept any settled outcome: usage record, released reservation, settled claim, accepted evidence.
  - Do not re-add family `record_usage` for cancelled units.
  - Keep the `rrm-016-journaled-goal-settlement` patch in place, and replay `tests/fixtures/histories/rrm016_post_change/` plus the RRM-007 pre-change histories.
- `_consume_settlement` fails closed on a missing settlement; cancellation must not route through it.
- The family's `run_version` after a settlement is the run's current version: a `cancelling` transition is visible to the next lifecycle command.
- Shared-session namespaces: an orphan lineage after a cancelled `interrupted` executor wedges the next iteration's session (RRM-004 finding 4, already on RRM-008).

### Reusable seams (mission-horizon lens)

- `GoalOperationSettlementPort` / `RunControlGoalOperationSettlements`: a family-neutral "consume the operation's accepted run-control settlement" read, keyed only by binding, reservation and usage.
- `operation_settlement_id` and `operation_effect_claim_id`: public identities for any consumer of journaled settlements.
- `goal_unit_workspace_root` plus the authority's rebased slot comparison: a unit-identity-derived workspace root any family can adopt for role isolation.
- `tests/fixtures/goal_directed_journaled.py`: the governed GoalDirected composition over pluggable stores (in-memory or PostgreSQL and MongoDB) with a role-aware deterministic model.
- `tests/fixtures/temporal_history.py`: decodes `workflow.patched` IDs and activity inputs from captured histories.

None of these carries company, fixture or provider specifics.

## Review disposition

Independent review verdict: `approve_with_fixes`; nothing blocking. All fixes are new commits (nothing amended).

| # | Finding | Disposition |
|---|---|---|
| 1 | The slot root was recomputed from `request.runtime_unit` without cross-checks, so a goal unit could borrow a root on another family's run or for another iteration or role. | **Fixed (`0c8522f`).** `_verify_family_unit` in `RunControlOperationAuthority._verify_bound_authority`: a `goal_directed` unit requires a run whose execution target is `GoalDirected`, `unit.belllabs_run_id == identity.run_id`, and `goal_unit_operation_id(unit) == identity.operation_id`; the preparer uses the same `goal_operation_id`. Tests (`test_rrm_016_goal_directed_settlement.py`): `test_authority_rejects_a_goal_unit_on_a_run_that_is_not_goal_directed[StageGraph]` and `[None]` (no target) → `requires a GoalDirected Workflow Run`; `test_authority_rejects_a_goal_unit_whose_location_is_not_its_operation[2-executor]` and `[1-verifier]` → `location does not match its operation`, while the genuine operation is admitted. |
| 2 | The run version read in `observe` is consumed later; any outside command in between failed the run on the next command, with no retry. | **Fixed (`0c8522f`)**, behind `workflow.patched("rrm-016-stale-version-retry")` (the retry adds commands only on a stale result). A stale lifecycle fact is retried once as `{command_id}:at-version:{v}` at the version the stale result reported (the activity reads it; a terminalization proposal is rebound to it). A stale admission is reported by the preparation activity as `goal_admission_stale` (current version, phase) and re-admitted once (`admission_attempt=2`, a new command identity); the preparer now persists the binding only after its admission is accepted, so a stale admission leaves nothing bound and the re-admission binds the same semantic attempt at the new revision. A stale result showing `cancelling` is not retried and no second cancel is issued: the family enters its cancellation boundary (RRM-008's seam). Tests (`test_rrm_016_goal_directed_journaled.py`): `test_outside_commands_between_settlement_and_next_command_do_not_fail_the_run` (an outside reserve/release after every settlement read but one: two re-admissions and one lifecycle retry, the run completes `COMPLETED`, four operation usage records, consumed 40, the history replays); `test_outside_cancel_enters_the_cancellation_boundary_not_a_lifecycle_failure` (an API cancel after the executor's settlement: the family ends with `goal_cancelling`, not a lifecycle rejection; the run stays `cancelling`; no re-admission and no family cancel command); `test_stale_version_retry_history_replays` (captured fixture). |
| 3 | The no-slots branch in `_workspace_for` was unreachable and its comment misleading. | **Fixed (`0c8522f`).** Removed; a template without compiled slots fails closed. Two fixtures that still used slot-less templates (the WP-BP-020 real-preparer test and the docker sandbox rollover) now carry the compiled `/work` slot; their writable paths are unchanged (`/goal/{i}/{role}/work`). |
| 4 (optional) | Compare `observe`'s binding and reservation against the stored binding. | **Done (`0c8522f`).** With a binding reader composed (production passes the operation binding store), `observe` requires the stored binding to equal the admitted operation's binding and its reservation to be the operation's. |
| 5 | Deploy note and review disposition. | **Done.** "Deploy note" under Unresolved risks; this section. |

Post-review gates (code head `0c8522f`; DSN runs and full runs under the stack lock):

| Gate | Result |
|---|---|
| `uv run --no-sync ruff check app tests scripts` | All checks passed |
| `uv run --no-sync mypy app` | Success: no issues found in 366 source files |
| Owning suites (the list in Deterministic verification) | 397 passed, 1 xfailed (RRM-019); +7 over the pre-review run: 4 fix-1 cases, 2 fix-2 tests, 1 replay |
| Replay subset (same `-k` command as in Replay and recovery artifacts) | 12 passed (+1: the stale-retry history) |
| Demonstration and RRM-006 forks, with both DSNs: `pytest -q -s tests/acceptance/control_plane/test_rrm_016_goal_directed_demo.py tests/acceptance/control_plane/test_rrm_006_semantic_forks.py` | 3 passed; demonstration unchanged (4 accepted settlements, consumed `tokens.total` 40, pause and resume receipts `accepted/delivered/applied`, terminal `completed`); GoalDirected fork `excluded_units: []` |
| Full hermetic (DSNs unset, `BELLABS_RUN_*_LIVE=0`, `LANGSMITH_TRACING=false`) | **975 passed, 74 skipped, 3 xfailed**, 0 failed (+7 over `731b770`) |
| Full with both DSNs exported and `--env-file ../biotech-research-ingestion-evaluation-system/.env` | **1018 passed, 30 skipped, 4 xfailed**, 0 failed (+7 over `731b770`) |
| `git diff --check` | clean |

### Re-check (verdict `approve_with_fixes`, no blockers)

| Gap | Disposition |
|---|---|
| (a) `_verify_family_unit` returned early for an operation without a runtime unit, so on a GoalDirected run its slots would not be rebased. | **Fixed.** A run whose execution target is `GoalDirected` admits only `goal_directed` units: an operation with no unit, or with another family's unit, is rejected (`a GoalDirected Workflow Run admits only GoalDirected runtime units`). Test: `test_rrm_016_goal_directed_settlement.py::test_goal_directed_run_rejects_an_operation_without_a_goal_unit[none]` and `[stage_graph]`. A pre-existing authority test's fake run projection gained `execution_target=None` (the real projection's default), which the authority now reads for every operation. |
| (b) The stored-binding check in `observe` was skipped when `bindings` was `None` (the default). | **Fixed.** `RunControlGoalOperationSettlements(run_control, bindings)` now requires the binding reader; there is no bypass. Production (`compose_goal_directed_activities`) passes the operation binding store, and so does every test composition: `governed_result_service(documents, run_control, bindings)`, the RRM-004 recovery harness, RRM-006's GoalDirected fork demonstration (`stack.bindings`) and the WP-BP-020 live harness. Fixtures whose run control is itself a fake keep the explicit `FixtureGoalSettlements` port. |

Re-check gates (DSN runs and full runs under the stack lock):

| Gate | Result |
|---|---|
| `uv run --no-sync ruff check app tests scripts` / `uv run --no-sync mypy app` | All checks passed / no issues in 366 source files |
| Owning suites | 399 passed, 1 xfailed (+2: the re-check (a) cases) |
| Full hermetic (DSNs unset) | **977 passed, 74 skipped, 3 xfailed**, 0 failed |
| Full with both DSNs exported and `--env-file` | **1020 passed, 30 skipped, 4 xfailed**, 0 failed. A first run on the same tree took 380 s instead of about 240 s and timed out one RRM-007 time-skipping test (`test_goal_directed_policy_pause_is_durable_across_forced_continue_as_new`, a 120 s Temporal client RPC timeout under host load; the test uses fake activities and none of the changed code). It passed alone and in the hermetic run, and the full rerun above is clean. |

## Final disposition

ready_for_review
