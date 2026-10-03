# RRM-020 implementation evidence

Disposition: ready_for_review (implemented; independent review pending)
Recorded date: 2026-10-02 (America/New_York)
Qualification identity: RRM-020 materialize a `shared` GoalDirected workspace across iterations. Requirements: REQ-CP-DA-013 (exact exclusive writable slots), REQ-BP-GD-004 (independent verifier workspace), SPEC-BP-GOAL-DIRECTED workspace continuity (`GoalWorkspaceSnapshotPolicy`: "Workspace continuity is independent from model-session continuity"), REQ-CP-DA-014 (candidates registered against the slot that governs them).
Base revision and head revision: base integration `0d0c184` (everything through RRM-009). Code commit: `0da4911` (implementation and tests). **Tested code head: `0da4911`.** The documentation commit that adds this README and updates the ticket follows it and changes no code. Not merged (the coordinator owns review and merge).
Framework/package baseline: `uv.lock` unchanged (no dependency change); CPython 3.12.14 (Codex runtime venv); temporalio 1.30.0 (`WorkflowEnvironment.start_local`); uv 0.7.5.

## Worktree provenance

- Worktree `C:\Users\Pinda\Proyectos\Biotech\biotech-research-ingestion-evaluation-system-rrm-020`, branch `wp/rrm-020-shared-goal-workspace`, created by the coordinator from integration `0d0c184`; clean at kickoff.
- The main checkouts were not touched; `.env` was not read, copied or printed.
- Disposable stack only (`127.0.0.1:55432/belllabs`, `127.0.0.1:27017`), always under the shared stack lock (`stack_lock.py acquire/release RRM-020`; acquired once for the proof, the negative control, the RRM-009 GoalDirected regression, the Mongo case and both DSN chunks, then released). One pytest process at a time. No container was created, stopped or removed. Temporal ran only as `start_local` and the time-skipping test server.
- Concurrent tickets: RRM-021 (`app/temporal/workflows/stagegraph.py`) and CR-4 (cancellation, heartbeat, composition, launch path). No file of theirs was edited. `tests/conftest.py` gained one line in the psycopg selector-loop module list, which other tickets also append to (a trivial merge).

## Implemented contracts and seams

### Decision: option 1 (one workspace identity, a new manifest revision per iteration)

The ticket offered three options. Option 1 is the one the spec and code support:

1. **Option 1 (chosen).** Keep the workspace identity and add the next iteration's role-rooted slots to it as a new manifest revision; earlier roots keep their owners.
   - SPEC-BP-GOAL-DIRECTED's `GoalWorkspaceSnapshotPolicy` (`app/domain/control_plane/contracts.py`) says "Workspace continuity is independent from model-session continuity", and the interpreter keeps `workspace_generation` unchanged in `shared` mode (`app/domain/orchestration/goal_directed.py`). "Shared" therefore means one workspace, and the interpreter already names it so (`…/goal/workspace/{generation}`).
   - CON-CP-WORKSPACE-MANIFEST-V1 (SPEC-CP-DEEP-AGENT-RUNTIME) defines "logical slots and ownership", and the manifest already has revisions with lineage digests (`prior_manifest_digest`). A slot set that grows by one revision fits that contract without a new store.
   - REQ-CP-DA-013 ("exact exclusive writable slots") and RRM-016's authority rule are unchanged: each operation still binds exactly the compiled slots under its own role root `/goal/{iteration}/{role}`, which run control recomputes from the unit identity.
2. **Option 2 (rejected).** A per-iteration workspace with a snapshot/restore carrying files forward. REQ-CP-DA-015 says "restore MUST create a new workspace identity", so every `shared` iteration would become a clone with a new identity: `shared` would mean `fresh_from_snapshot`, a mode the policy already declares separately. It would also need snapshot plumbing in the GoalDirected path that `snapshot_mode` (`on_rollover` by default) does not ask for.
3. **Option 3 (rejected).** Role-only roots (`/goal/{role}`). This changes RRM-016's accepted authority rule and the RRM-009 ownership boundary (`slot_ownership_boundary`), and two iterations of one role would own the same writable root. REQ-CP-DA-013 then holds only per workspace, not per iteration owner, and every iteration's executor would have write access to every earlier iteration's output.

### The declared rule (`shared_goal_workspace_slots`, `app/domain/operation_execution/materialization.py`)

This is the only case where one workspace identity is materialized with another slot set:

- Every requested slot lies under one GoalDirected role root `/goal/{n}/{role}`, and every slot already in the workspace lies under a root of the same role. An executor workspace never takes a verifier root, and the reverse (REQ-BP-GD-004).
- If iteration `n` is already in the workspace, the request must carry exactly its recorded slots. This is a retry: the current manifest is returned and nothing is appended.
- Otherwise:
  - `n` is later than every iteration in the workspace;
  - its slots, without the root, are the same compiled slot set (name, relative path, access, owner kind, durable input) as every earlier iteration's;
  - its owners own nothing in the workspace yet.
  - The result is the current slots followed by the requested ones.
- Anything else returns `None`, and `materialize` raises `IdempotencyConflict("workspace identity was reused with different materialization")` as before. A workspace that is not role-rooted (StageGraph, generic artifact) keeps the exact-identity rule unchanged.

### Service changes (`WorkspaceMaterializationService`, `app/application/workspaces/workspace_materialization.py`)

- **`materialize`.** When the rule extends the workspace, it:
  1. reserves the new role root through the repository's `reserve_writable_slots(request)`, the same reservation path as a first materialization (Mongo unique `(namespace_id, logical_path)`);
  2. appends one revision with the extended slots and the current entries, plus any read-only inputs of the new root. Those input entry IDs are qualified by path, because the same input is mounted under each root. `created_at` is the binding's `bound_at`, so a retried append is byte-identical and the repository's idempotent append returns it;
  3. provisions from that revision.
  A retry, before or after the append, converges on one revision: before, the append is idempotent; after, the rule finds the iteration and appends nothing.
- **`register_candidate`.** The slot is now the slot *of that name whose path holds the candidate*. The workspace holds one `work` slot per iteration, and a first-match by name would have bound iteration 2's report to iteration 1's slot. Owner and path checks are unchanged, so iteration 2 cannot register into iteration 1's root.
- `_append_revision` takes an optional `slots`; `_durable_input_entries` is extracted from `_initial_manifest`, and first-revision entry IDs are unchanged.

**Temporal.** No workflow, signal, query, update, activity type or payload changed. Materialization runs inside the operation activity (`OperationExecutionService._invocation` → `BindingWorkspaceMaterializer`), and the GoalDirected workflow's commands are unchanged, so no `workflow.patched` marker is needed. All captured histories replay (below).

## Requirement-to-evidence map

| Requirement | Test | Observed assertion |
|---|---|---|
| RRM-020 reproduction passes without its marker | `tests/unit/workspaces/test_goal_role_slot_ownership.py::test_shared_goal_workspace_is_materialized_again_at_the_next_iteration` (the `xfail(strict=True, raises=IdempotencyConflict)` marker removed) | Same `workspace_id` at both iterations; manifest revisions `(1, 2)`; revision 2 verifies and links to revision 1's digest. Slots `[("/goal/1/executor/work", "goal-iteration/1/executor"), ("/goal/2/executor/work", "goal-iteration/2/executor")]`. Reservations exactly `{"/goal/1/executor": (ws, iteration-1 owner), "/goal/2/executor": (ws, iteration-2 owner)}` |
| Exactly-once revisions | same test | A retry of iteration 2 and of iteration 1 both return the identical revision-2 manifest; the repository holds exactly 2 revisions |
| Exactly-once on Mongo, across service instances | `tests/integration/mongodb/test_workspace_materialization_mongodb_integration.py::test_mongodb_shared_goal_workspace_gains_one_revision_per_iteration` | A fresh service per call over `MongoWorkspaceManifestRepository`: revisions `2 == 2` on retry, with identical manifests. Stored revisions `[1, 2]`; reservations exactly the two executor roots |
| Conflicts still conflict | `…::test_slot_sets_outside_the_declared_rule_still_conflict[6 cases]` | Six cases each raise `IdempotencyConflict("…different materialization")`: a recorded iteration with another path; an earlier never-held iteration (`/goal/0`); another compiled path (`/scratch`); another owner kind (`RUN`); an owner the workspace already has; an unrooted slot. In each, nothing is appended (still 2 revisions) and nothing is reserved (still the two roots) |
| Conflicts: two roots / unrooted workspaces | `…::test_two_roots_in_one_request_and_unrooted_workspaces_keep_exact_identity` | A request spanning `/goal/3` and `/goal/4` conflicts. A StageGraph-shaped workspace (`/workspace/output` → `/workspace/report`) still conflicts. The goal workspace stays at revision 1 |
| GD-004 / DA-013 disjoint, owned roots | `…::test_shared_executor_and_verifier_keep_disjoint_owned_roots_across_iterations` | Executor roots `{/goal/1/executor, /goal/2/executor}` (owner kind `ITERATION`); verifier roots `{/goal/1/verifier, /goal/2/verifier}` (`EVALUATOR`). A verifier root offered to the executor workspace and an executor root offered to the verifier workspace both raise `IdempotencyConflict`. Another workspace taking `/goal/2/executor` raises `WorkspaceSlotConflict` |
| DA-013/014 candidates per root (real filesystem provisioner) | `…::test_each_iteration_registers_candidates_in_its_own_root` | Revisions 1→4 (materialize, candidate, extend, candidate); candidates `/goal/{1,2}/executor/work/report.md`, each with its own iteration owner; both roots exist on disk. Iteration 2 registering into `/goal/1/…` raises `UndeclaredWorkspacePath` |
| Read-only inputs per root | `…::test_a_read_only_input_joins_each_iteration_under_its_own_root` | Two `durable_input` entries `/goal/{1,2}/executor/input` with distinct entry IDs; both inputs are loaded and digest-verified for provisioning. An iteration without the compiled input conflicts |
| Unchanged single-iteration ownership (RRM-009) | `…::test_role_root_is_the_ownership_boundary_and_other_paths_keep_two_components`, `…::test_executor_and_verifier_of_one_iteration_own_disjoint_roots` | Unchanged and passing |
| **Production composition: two-iteration `shared` run completes** | `tests/acceptance/control_plane/test_rrm_020_shared_goal_workspace.py::test_shared_goal_workspace_spans_two_iterations_on_the_production_composition` (DSN-gated) | See "Live runtime qualification" |
| RRM-009 `fresh` qualification still holds | `tests/acceptance/control_plane/test_rrm_009_production_composition.py::test_goal_directed_runs_two_iterations_through_the_production_composition` | Passed: accepted outputs `["artifact:rrm009-goal:2"]`, 4 settlements, `{"parent": 4, "child": 1}` per operation, 137 replayed events |
| Negative control | the RRM-020 proof on the base code (app changes reverted in the worktree, then restored) | Fails: the model log stops after iteration 1's verifier; the root's history ends `EVENT_TYPE_WORKFLOW_EXECUTION_FAILED` ("Child Workflow execution failed"), the ticket's diagnosis |

## Changed paths and migrations

**Migrations: none.** Manifests and reservations stay in the existing Mongo collections (`workspace_materialization_manifests`, slot reservations); no index or document schema changed.

**Shared-seam edits (authorized list):**
- `app/application/workspaces/workspace_materialization.py` (workspace materialization): the extension branch in `materialize`, the slot lookup in `register_candidate`, `_durable_input_entries`, and `slots` on `_append_revision`.
- `app/domain/operation_execution/materialization.py`: `shared_goal_workspace_slots` (the declared rule) and `_goal_role_root`. `slot_ownership_boundary` now uses `_goal_role_root`, with identical behaviour; its unit test is unchanged and passing.
- Mongo manifest repository (`app/application/workspaces/mongo_workspace_repository.py`): **not edited.** Its idempotent `append` and `reserve_writable_slots` already serve the extension.
- GoalDirected preparer (`app/application/orchestration/goal_directed.py`): **not edited.** It keeps binding exactly the role-rooted compiled slots.

**Tests and fixtures:**
- `tests/unit/workspaces/test_goal_role_slot_ownership.py`: xfail removed; five new tests (one parametrized ×6), so 10 new cases plus the former xfail.
- `tests/integration/mongodb/test_workspace_materialization_mongodb_integration.py`: one new Mongo-gated test.
- `tests/acceptance/control_plane/test_rrm_020_shared_goal_workspace.py` (new, DSN-gated): reuses RRM-009's `open_production_stack`, `mongo_database` and facade helpers.
- `tests/fixtures/rrm009_production_stack.py`:
  - `goal_blueprint(workspace_mode=…)` and `publish_technical_catalog(goal_workspace_mode=…)` both default to `fresh`, so RRM-009 is unchanged. Its comment now names both qualifications.
  - The technical model writes a GoalDirected operation's report into its role's `/work` slot (`_report_path`). StageGraph and the generic artifact keep `/workspace/output/report.md`. Before this, the GoalDirected write was denied (outside the binding's writable paths) and no candidate was captured, so the candidate path was never exercised. RRM-009's GoalDirected test still passes, with identical call counts.
- `tests/conftest.py`: the new acceptance module joins the psycopg selector-loop list.

## Deterministic verification

All runs from the worktree with `unset VIRTUAL_ENV`, one pytest process at a time, on `0da4911` (the full suites) or its working tree just before the commit (the targeted runs). `<DSNs>` = `TEST_APPLICATION_POSTGRES_DSN='postgresql://belllabs:belllabs-local@127.0.0.1:55432/belllabs' TEST_MONGODB_URI='mongodb://127.0.0.1:27017/?directConnection=true'`, exported explicitly; `LIVE=0` = `BELLABS_RUN_WP_BP_010_LIVE=0 BELLABS_RUN_WP_BP_020_LIVE=0 BELLABS_RUN_WP_CP_040_LIVE=0`.

| Gate | Command | Result |
|---|---|---|
| Ruff | `uv run --no-sync ruff check app tests scripts` | All checks passed |
| Mypy | `uv run --no-sync mypy app` | Success: no issues found in 383 source files |
| Owning suites (hermetic) | `env -u TEST_APPLICATION_POSTGRES_DSN -u TEST_MONGODB_URI LANGSMITH_TRACING=false uv run --no-sync pytest -q tests/unit/workspaces tests/unit/orchestration tests/unit/operations tests/integration/temporal/test_rrm_016_goal_directed_journaled.py tests/integration/temporal/test_wp_bp_020_temporal.py tests/acceptance/control_plane/test_wp_bp_020_sandbox_rollover.py tests/unit/control_plane/test_digest_set_order_guard.py` | 371 passed |
| Mongo workspace (lock) | `TEST_MONGODB_URI=… uv run --no-sync pytest -q tests/integration/mongodb/test_workspace_materialization_mongodb_integration.py` | 2 passed |
| Production proof (lock) | `<DSNs> LANGSMITH_TRACING=false uv run --no-sync pytest -q -s tests/acceptance/control_plane/test_rrm_020_shared_goal_workspace.py` | 1 passed (116 s) |
| RRM-009 GoalDirected regression (lock) | `<DSNs> … pytest -q -s tests/acceptance/control_plane/test_rrm_009_production_composition.py -k goal_directed` | 1 passed, 2 deselected (68 s) |
| Full hermetic | `env -u TEST_APPLICATION_POSTGRES_DSN -u TEST_MONGODB_URI LIVE=0 LANGSMITH_TRACING=false uv run --no-sync pytest -q -p no:warnings` | **1075 passed, 92 skipped, 2 xfailed** (266 s) |
| Full DSN, chunk 1 (lock) | `<DSNs> LIVE=0 LANGSMITH_TRACING=false uv run --no-sync pytest -q -p no:warnings --ignore=tests/acceptance` | 1068 passed, 27 skipped, 2 xfailed (373 s) |
| Full DSN, chunk 2 (lock) | `<DSNs> LIVE=0 LANGSMITH_TRACING=false uv run --no-sync pytest -q -p no:warnings tests/acceptance` | 63 passed, 9 skipped (413 s) |
| Full DSN total | chunk 1 + chunk 2 | **1131 passed, 36 skipped, 2 xfailed** |
| `git diff --check 0d0c184 0da4911` | | clean |

Deltas against the coordinator's baselines:
- **Hermetic** 1064/90/3 → 1075/92/2:
  - +11 passed: 10 new unit cases, plus the reproduction that is no longer xfailed;
  - +2 skipped: the DSN-gated proof and the Mongo-gated case;
  - −1 xfailed: RRM-020.
- **DSN** 1118/36/3 → 1131/36/2:
  - +13 passed: the 11, plus the Mongo case and the proof;
  - skipped unchanged;
  - −1 xfailed.
- The two remaining xfails are the other tickets' strict reproductions; they are untouched.
- No test was skipped, deselected or weakened, and no marker was added. The RRM-015 digest guard (`test_digest_set_order_guard.py`) passes. The new rule sorts slot shapes by `repr` for comparison only; it computes no digest.

## Live runtime qualification

No live model call. **Spend: USD 0.**

**Production composition proof** (`test_rrm_020_shared_goal_workspace.py`, under the lock):
- **Stack:** RRM-009's `open_production_stack`. The API is composed by `compose_runtime_control`, the workers by `ProductionWorkerActivityCompositionFactory`/`create_production_workers`. Temporal is a persistent `start_local`; PostgreSQL and MongoDB are disposable.
- **Materialization:** every operation is materialized by `BindingWorkspaceMaterializer` → `WorkspaceMaterializationService` over `MongoWorkspaceManifestRepository` and `FilesystemWorkspaceProvisioner`.
- **Cognition:** RRM-009's deterministic technical model through a real `create_deep_agent` graph, with one sync subagent and the exact MCP tool.
- **Blueprint:** the published blueprint's `workspace_mode` is `"shared"` (asserted).

Printed record (`RRM-020 EVIDENCE shared_goal_workspace`, sanitized):
- **Run and outcome:** run `bbed009a-ad06-5ceb-a13d-a1de89c4b8a8`, outcome `completed`, accepted outputs `["artifact:rrm009-goal:2"]`, 4 accepted operation settlements.
- **Model calls:** `goal-iteration/{1,2}/{executor,verifier}` each `{"parent": 4, "child": 1}`. Consumed `{"tokens.total": 100, "model.turns": 20, "goal.iterations": 0}`.
- **Executor workspace** `run/bbed009a-…/execution-epoch/1/goal/workspace/1`:
  - 4 revisions; the slot set changed at revisions `[1, 3]` only, which is one extension revision;
  - slots `["/goal/1/executor/work", "/goal/2/executor/work"]`;
  - candidates `["/goal/1/executor/work/report.md", "/goal/2/executor/work/report.md"]`, each owned by its iteration.
- **Verifier workspace** `…/goal/workspace/1:verifier`: the same shape over `/goal/{1,2}/verifier`.
- **Workspace count:** exactly these two workspace identities exist in the namespace `run/{run}`. A `fresh` run has four.
- **Revision chain:** unbroken (1…n), each linked by `prior_manifest_digest`, and every manifest digest verifies.
- **Reservations:** exactly `{"/goal/1/executor": iteration-1 executor, "/goal/2/executor": iteration-2 executor, "/goal/1/verifier": …, "/goal/2/verifier": …}` (4 documents), each bound to its own role's workspace.
- **Replay:** the root and family histories replay, 137 events.

Negative control: the same test on the base code (app changes reverted in the worktree, then restored) fails at iteration 2. See the map.

## Replay and recovery artifacts

| Command | Result |
|---|---|
| `env -u TEST_APPLICATION_POSTGRES_DSN -u TEST_MONGODB_URI LANGSMITH_TRACING=false uv run --no-sync pytest -q tests/integration/temporal/test_rrm_007_replay_pre_change_histories.py tests/integration/temporal/test_rrm_016_goal_directed_journaled.py -k "replay or Replayer or replays or history or histories" -rA` | 9 passed |

Histories replayed:
- `rrm007_pre_change/`: `goal_directed_two_iterations.run1.json`, `goal_directed_policy_pause_failure.run1.json`, and the three StageGraph histories;
- `rrm016_post_change/`: `goal_directed_journaled_pause_resume.run1.json` and `goal_directed_stale_version_retry.run1.json`, plus the pre-change usage-command check;
- `rrm019_post_change/`: `goal_directed_distinct_output_refs.run1.json`.

In-run replays also pass in the proof and the RRM-009 regression. No workflow code changed, so no new history was captured and no patch marker was added.

**Recovery.** A retried materialization converges to one revision in both crash windows:
- before the append: the reservation is re-reserved with the same token, and the append is the same manifest, accepted idempotently;
- after the append: the rule finds the recorded iteration.

The unit, Mongo and proof tests above show the post-append retry.

## Replacement and deletion checks

- **Replaced:** for role-rooted GoalDirected workspaces, the exact slot-equality idempotency check is replaced by "exact equality, or the declared extension rule". Every other workspace keeps exact equality.
- **Replaced:** the name-only slot lookup in `register_candidate`, by name plus path.
- **Deleted:** nothing. No second lifecycle store, manifest store or reservation scheme was added.
- **Superseded workaround:** RRM-009's technical GoalDirected qualification no longer *needs* `fresh`. It keeps `fresh` so that both modes stay qualified; the new proof covers `shared`.

## Unresolved risks and drift checks

**Open decisions (for the coordinator)**

1. **The rule lives in the generic materializer and is keyed on path shape.** The materializer cannot see the blueprint's `workspace_mode`, so the rule recognizes the GoalDirected role-root shape that RRM-016 binds and run control verifies for exactly that unit.
   - Spec text relied on: REQ-CP-DA-013; `GoalWorkspaceSnapshotPolicy` ("Workspace continuity is independent from model-session continuity"); CON-CP-WORKSPACE-MANIFEST-V1 ("logical slots and ownership").
   - Alternative: an explicit, digest-bound continuation declaration on `WorkspaceContract`. That changes binding digests and Temporal-carried contracts, which is why it was not taken here.
2. **Continuity is at the workspace and manifest level, not as an agent read grant.**
   - What is shared: one workspace root on the provisioner, which holds both iterations' slots, and one manifest that carries every iteration's candidates and promotions.
   - What is not: iteration 2's binding still grants writes only to its own root and reads only to its declared read mounts. Earlier roots are not readable to the next iteration's agent through the binding; content passes forward through the typed handoff (REQ-BP-GD-005).
   - Granting read access to earlier roots would be a separate decision: a read mount of the prior root in `_workspace_for`, which run control would then have to admit.

**Observations (not changed)**

- **A Mongo reservation token can conflict after a crash.** The token digests the whole request, including `created_at` (the binding's `bound_at`). Suppose a crash falls between the reservation and the first append, and the retry uses a *new* binding (a new `bound_at`); the role-root reservation would then conflict. This is pre-existing for first materializations too, and identical-binding retries are unaffected. The finding is from reading the code; no test exercises it.
- **`snapshot_mode`** (`on_rollover` by default) and `fresh_from_snapshot` are unaffected. No snapshot is taken or restored by this change.

**Drift checks.** No migration. No Temporal command change. The StageGraph and generic-artifact paths keep the exact-identity rule, which is asserted. The RRM-016 authority rule and `slot_ownership_boundary` behaviour are unchanged, which is asserted.

**Reusable seams (mission horizon)**

- `shared_goal_workspace_slots`: the declared continuation rule for role-rooted workspaces.
- `WorkspaceMaterializationService._append_revision(slots=…)`: append a slot-set revision with lineage.
- `publish_technical_catalog(goal_workspace_mode=…)` / `goal_blueprint(workspace_mode=…)`: qualify either GoalDirected workspace mode on the production stack.

## Final disposition

ready_for_review
