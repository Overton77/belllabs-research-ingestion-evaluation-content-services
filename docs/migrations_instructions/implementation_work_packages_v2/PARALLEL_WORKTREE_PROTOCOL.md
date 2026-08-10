# Parallel blueprint-runtime worktree protocol

Status: canonical implementation companion for `WP-BP-010` and `WP-BP-020`  
Applies when: the two blueprint runtimes are implemented concurrently

## 1. Entry gate and topology

Both worktrees must start from the same commit containing accepted `WP-CP-045` code and
[`evidence_v2/WP-CP-045/README.md`](../evidence_v2/WP-CP-045/README.md). Record that immutable
commit as `BP_BASE_REVISION` in both handoffs and evidence files. Completed but uncommitted
`WP-CP-045` work is not a valid base; do not create either worktree until the accepted code and
evidence are present in the same reachable commit. Do not start the BP branches from one another.

Use these logical branches:

```text
integration/bp-runtimes
  |-- wp/bp-010-stagegraph
  `-- wp/bp-020-goal-directed
```

Create `integration/bp-runtimes` at `BP_BASE_REVISION`; create both WP branches at that same
revision. Each WP branch merges into the integration branch. After the combined gate passes, merge
the integration branch into the target branch. Do not stack GoalDirected on StageGraph or merge
either WP directly to the target branch while this protocol is active.

`WP-CP-050` begins only after both blueprint packages are accepted. It is not a place to finish
missing family semantics.

## 2. Frozen foundation

The following accepted owners are read-only in both WP branches unless the active WP explicitly
requires a narrowly additive adapter call and the integrator approves it before editing:

- `app/domain/run_control/` and its lifecycle reducer;
- family-neutral portions of `app/domain/operation_execution/contracts.py`;
- `app/domain/operation_execution/delegation.py`, the sole async-delegation classifier;
- `app/temporal/workflows/belllabs_run.py`;
- `app/temporal/workflows/operation.py`;
- `app/integrations/agents/deep_agents/adapter.py` and `materializer.py`;
- `app/temporal/registration/`;
- accepted `WP-CP-030`, `WP-CP-040`, and `WP-CP-045` evidence.

No BP branch may redefine lifecycle authority, async-subagent semantics, Deep Agent binding
meaning, workflow identity, provider placement, or terminality.

## 3. Exclusive ownership

### WP-BP-010

Owns StageGraph-only behavior and tests:

- StageGraph types inside `app/domain/control_plane/contracts.py`;
- StageGraph compiler branches in `app/domain/control_plane/compiler.py`;
- StageGraph types inside `app/domain/orchestration/contracts.py`;
- `app/domain/orchestration/interpreter.py`;
- StageGraph methods inside `app/application/orchestration.py`;
- `app/temporal/workflows/stagegraph.py` and deletion of
  `app/temporal/stagegraph_workflow.py` at acceptance;
- StageGraph-specific activity handlers and removal of its direct-activity path;
- `app/agent_server/stagegraph/` deletion at acceptance;
- StageGraph unit, contract, integration, replay, acceptance, and evidence paths.

### WP-BP-020

Owns GoalDirected-only behavior and tests:

- GoalDirected types inside `app/domain/control_plane/contracts.py`;
- GoalDirected compiler branches in `app/domain/control_plane/compiler.py`;
- GoalDirected types inside `app/domain/orchestration/contracts.py`;
- `app/domain/orchestration/goal_directed.py`;
- GoalDirected methods inside `app/application/orchestration.py`;
- typed handoff, context-rollover, and verifier application services;
- `app/temporal/workflows/goal_directed.py` and deletion of
  `app/temporal/goal_directed_workflow.py` at acceptance;
- GoalDirected-specific activity handlers and removal of direct goal activities;
- `app/agent_server/goal_directed/` deletion at acceptance;
- GoalDirected unit, contract, integration, replay, acceptance, and evidence paths.

`WP-BP-020` consumes `classify_async_delegation` and may add GoalDirected fixtures. It must not
change classifier semantics or create another classifier.

## 4. Shared-file region locks

The files below are shared containers, not shared semantic ownership:

- `app/domain/control_plane/contracts.py`;
- `app/domain/control_plane/compiler.py`;
- `app/domain/orchestration/contracts.py`;
- `app/application/orchestration.py`.

Each branch edits only its named family region. Before moving, renaming, or reformatting a shared
region, record the intended range and reason in the handoff. Avoid whole-file formatting, import
sorting unrelated to the WP, mechanical model renames, and mixed-family fixture changes.

The integrator owns:

- family-neutral unions, exports, adapters, and dispatch;
- root and generic operation workflow changes;
- workflow/activity registries;
- resolution of shared-file imports and merge conflicts;
- combined API/runtime qualification and final drift searches.

Conflict resolution must preserve both family-owned changes. Never resolve a shared-file conflict by
accepting one side wholesale. If a family needs a foundation-contract change, stop both branches,
write an explicit amendment, commit it on a short-lived foundation-amendment branch from
`BP_BASE_REVISION`, and merge that exact commit into both WP branches and the integration branch.
Do not rewrite published/shared WP branch history.

## 5. Worktree kickoff record

Before the first edit, each worktree records:

- repository and absolute worktree path;
- WP ID and branch;
- `BP_BASE_REVISION` and current HEAD;
- clean/dirty status and all pre-existing changed paths;
- accepted dependency evidence paths;
- owned paths, shared-file regions, non-goals, and deletion gates;
- planned deterministic and live qualification commands.

Do not overwrite, revert, stage, or delete another actor's changes. A worktree blocked by a shared
change remains runnable and publishes a handoff rather than implementing the sibling WP's seam.

## 6. Verification gates

Each worktree must pass:

1. deterministic unit/property and contract suites for its interpreter and schemas;
2. API integration and Temporal time-skipping, replay, recovery, cancellation, and continuation
   suites applicable to that family;
3. accepted CP-030/040/045 regression suites;
4. `uv run ruff check app tests scripts`;
5. `uv run mypy app`;
6. the full offline test suite;
7. its credential-gated real-LLM API-to-Temporal acceptance vertical.

The StageGraph live vertical must prove incremental release in a branching graph with a controlled
synchronization gate around the slow sibling and Temporal-history event ordering; provider wall
clock latency alone is not evidence. The GoalDirected live vertical must prove separately bound
executor and independent-verifier operations plus convergence or revision behavior. Both must use
the real BellLabs API, root/family/operation workflow hierarchy, and accepted Deep Agents adapter.

Advanced capability combination is not required in these verticals. Use a minimal exact binding;
`WP-CP-050` later proves Skills, MCP, executable sandboxes, snapshots, sync/async subagents, both
families, and recovery together.

## 7. Evidence and handoff

Each branch creates its evidence directory only when real executable evidence exists. Follow
[`evidence_v2/README.md`](../evidence_v2/README.md). Include:

- requirement-to-test mapping and exact changed paths;
- base and head revisions;
- sanitized deterministic and live command outputs;
- API run identity, Temporal root/family/operation identities, binding/model identity, and semantic
  observations;
- captured replay histories and deletion/drift searches;
- unresolved risks and `ready_for_review`, `accepted`, or `rework_required`.

## 8. Integration sequence

1. Confirm both branch handoffs name the same `BP_BASE_REVISION`.
2. Merge one WP into `integration/bp-runtimes`; run its focused suites.
3. Merge the other WP and resolve only through the ownership matrix above.
4. Run both deterministic qualifications, CP regressions, lint, typecheck, and full offline suite.
5. Run both live API-to-Temporal verticals against the merged tree.
6. Verify legacy family modules, direct activity paths, and macro graphs are absent from active
   composition.
7. Publish combined sanitized outputs in both WP evidence files without setting final dispositions.
8. Record the current target revision, merge that exact revision into `integration/bp-runtimes`,
   resolve under the ownership matrix, and rerun every combined deterministic and live gate.
9. Set both package dispositions and commit the evidence-only acceptance update. Do not change
   executable code after the combined gates.
10. Rerun every combined deterministic and live gate on that final acceptance commit. Record its
    SHA and the tested target SHA in the external handoff/merge record without modifying the tested
    tree. The target must remain at the tested revision; if it advances, repeat step 8 onward.
11. Merge the exact final tested acceptance commit without adding changes, and only after both
    packages are accepted.
