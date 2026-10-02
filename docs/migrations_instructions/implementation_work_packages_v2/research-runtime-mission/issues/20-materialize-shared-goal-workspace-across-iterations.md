# RRM-020 — Materialize a shared GoalDirected workspace across iterations

**What to build:** A GoalDirected run whose blueprint keeps one workspace across iterations (`workspace_policy.workspace_mode = "shared"`, the default) materializes that workspace again at its second iteration through the governed workspace materializer, instead of failing the executor's preparation with `IdempotencyConflict`.

**Blocked by:** None.
**Blocks:** Any GoalDirected run of two or more iterations with a `shared` workspace on the production composition (RRM-009's `WorkspaceMaterializationService` over Mongo manifests), including the GoalDirected company fixture (RRM-011) unless its blueprint uses `fresh` workspaces. Whether it blocks RRM-010 is the coordinator's decision.
**Status:** open (found by RRM-009, 2026-10-02)
**Branch:** `wp/rrm-020-shared-goal-workspace`
**Authority:** REQ-CP-DA-013 (exact exclusive writable slots), REQ-BP-GD-004 (independent verifier workspace), SPEC-BP-GOAL-DIRECTED workspace continuity (`GoalWorkspaceSnapshotPolicy`: workspace continuity is independent from model-session continuity)

## Diagnosis (RRM-009, 2026-10-02)

- The GoalDirected interpreter names the executor's workspace from the workspace generation only: `workspace_namespace = run/{run}/execution-epoch/{epoch}/goal/workspace/{generation}` (`app/domain/orchestration/goal_directed.py`, `claim_execution`). With `workspace_mode = "shared"` the generation does not advance, so every iteration's executor binds the same workspace identity.
- RRM-016 rebases the compiled slots under the unit's role root `/goal/{iteration}/{role}` (`_workspace_for` in `app/application/orchestration/goal_directed.py`; `goal_unit_workspace_root`), and the run-control authority recomputes that root from the unit identity. The second iteration's executor therefore requests the same workspace with different slots (`/goal/2/executor/work` instead of `/goal/1/executor/work`).
- `WorkspaceMaterializationService.materialize` treats a workspace identity as immutable: the same `(namespace, workspace)` with different slots raises `IdempotencyConflict("workspace identity was reused with different materialization")` (`app/application/workspaces/workspace_materialization.py`). The executor's operation settles `preparation_failed` and the family fails the run (`GoalDirected operation result does not match its durable request`).
- Every earlier multi-iteration GoalDirected proof used a fake sandbox port, so the governed materializer never saw a second iteration. RRM-009's production composition is the first to run it.
- Reproduction: `tests/unit/workspaces/test_goal_role_slot_ownership.py::test_shared_goal_workspace_is_materialized_again_at_the_next_iteration`, marked `xfail(strict=True, raises=IdempotencyConflict)` citing this ticket. Remove the marker when fixed. RRM-009's technical GoalDirected qualification uses `workspace_mode = "fresh"` meanwhile.

## Options (for the owner to decide)

1. Keep the workspace and add the next iteration's slots to it as a new manifest revision (the manifest already has revisions); ownership of earlier roots stays with their owners.
2. Name a shared workspace per iteration while carrying its files forward (a snapshot/restore between iterations), so "shared" means shared content, not one identity.
3. Root the slots of a shared workspace by role only (not by iteration), and have the run-control authority recompute that root; this changes RRM-016's rule.

## Acceptance

- [ ] A two-iteration GoalDirected run with `workspace_mode = "shared"` completes on the production composition (Mongo manifests, `BindingWorkspaceMaterializer`).
- [ ] The RRM-020 reproduction passes without its marker; a different slot set under one identity outside the declared rule still conflicts.
- [ ] Executor and verifier writable roots stay disjoint and owned (REQ-BP-GD-004).
