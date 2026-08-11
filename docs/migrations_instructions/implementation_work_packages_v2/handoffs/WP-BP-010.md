# WP-BP-010 implementation handoff

Disposition: `blocked`

FOUNDATION_AMENDMENT_REQUIRED

## Kickoff and recovery context

- Repository/worktree:
  `C:\Users\Pinda\Proyectos\Biotech\biotech-bp-010-stagegraph`
- Work package/branch: `WP-BP-010` / `wp/bp-010-stagegraph`
- `BP_BASE_REVISION`: `20824742fcdc6f0d97189ceed7fc6cc2d2da2e9e`
- Recovery HEAD: `20824742fcdc6f0d97189ceed7fc6cc2d2da2e9e`
- Canonical metadata revision: `c48867a240d09a98db9cdfb4937f55176f30adf1`
- Accepted dependency evidence:
  `evidence_v2/WP-CP-030/README.md`, `evidence_v2/WP-CP-040/README.md`,
  `evidence_v2/WP-CP-045/README.md`
- The supervisor process crashed after the first focused gate. Recovery preserved all dirty
  worktree edits, did not rerun preflight, and did not reset, stage, commit, or run the
  credential-gated live vertical.

## Ownership and shared-file region locks

- `app/domain/control_plane/contracts.py`: StageGraph-only contracts from the StageGraph region;
  GoalDirected types and family-neutral unions remain locked.
- `app/domain/control_plane/compiler.py`: StageGraph branches and StageGraph-only normalization;
  generic dispatch and GoalDirected branches remain locked.
- `app/domain/orchestration/contracts.py`: StageGraph-only contracts; family-neutral and
  GoalDirected regions remain locked.
- `app/application/orchestration.py`: StageGraph launch/decision methods only.
- Exclusive owners:
  `app/domain/orchestration/interpreter.py`,
  `app/temporal/workflows/stagegraph.py`, StageGraph-specific activity handlers and tests.
- Frozen and unchanged: run control, generic operation execution, async delegation,
  `BellLabsRunWorkflow`, `OperationWorkflow`, Deep Agents adapter/materializer, Temporal
  registration, and accepted dependency evidence.

## Current changed paths

- `app/application/orchestration.py`
- `app/domain/control_plane/compiler.py`
- `app/domain/control_plane/contracts.py`
- `app/domain/control_plane/fixtures.py`
- `app/domain/orchestration/contracts.py`
- `app/domain/orchestration/interpreter.py`
- `app/temporal/orchestration_activities.py`
- `app/temporal/workflows/stagegraph.py`
- `tests/test_stagegraph_v2.py`
- this handoff

## Executable results

```text
uv run ruff check <changed StageGraph implementation and test files>
All checks passed.

uv run mypy <changed StageGraph implementation files>
Success: no issues found in 7 source files.

uv run pytest -q tests/test_stagegraph_v2.py --tb=short
44 passed in 0.36s.

uv run pytest -q tests/test_stagegraph_orchestration.py \
  tests/test_coordinator_temporal_runtime.py --tb=short
Collection failed with two ImportErrors: the still-active legacy
app/temporal/stagegraph_workflow.py imports StageGraphExecutionState, which the partial V2 contract
replacement removed. No tests executed.
```

No package evidence directory was created because the complete executable qualification does not
exist. The credential-gated real-LLM API-to-Temporal vertical remains explicitly pending and was
not run.

## Foundation blocker

Canonical result acceptance must atomically commit the StageGraph projection decision, authoritative
usage/effect settlement, accepted output and obligation evidence, resulting run/family versions,
and outbox records. The frozen `RunControlService.execute_family_admission` accepts one
`LifecycleCommand`/one reducer action plus one `AtomicFamilyMutation`. The current BP implementation
can atomically commit `RecordUsageAction` with the family mutation, but then records each accepted
output in separate commands. A crash or rejection between those commands leaves the StageGraph
projection and run-control evidence frontier inconsistent. Required obligation evidence also has no
atomic path into the accepted StageGraph projection.

The amendment must be authored from `BP_BASE_REVISION`, approved by the integrator, and merged as
the exact same commit into both BP branches and `integration/bp-runtimes`. It must provide a
family-neutral composite reducer/commit capability for one accepted family decision to settle usage
and effects and record output/obligation evidence in the same transaction. It must preserve exact
type registration, expected run/family version CAS, authority digests, idempotency, one outbox
sequence, and reducer-only terminality.

Integrator-owned composition changes are also required to inject the StageGraph decision service
and exact operation materializer, register the StageGraph mutation policy, retarget active
linked-run/smoke imports, and perform the coordinated legacy deletion gate. This branch must not
edit those frozen/shared owners.

## Additional verified rework after the amendment

- Restore an import-safe active tree during migration, then delete the legacy flat StageGraph
  workflow/direct-activity path and Agent Server macro graph only in the coordinated acceptance cut.
- Map failed/cancelled `OperationWorkflowResult` dispositions without inferring fulfilled evidence.
- Populate accepted obligation evidence and close producer liabilities only from authoritative
  child, reservation/usage, effect, cancellation, and exactly-one-result facts.
- Execute slow-sibling actions, late-result routing, wait/pause, cancellation reconciliation,
  stage/workflow cycles, minimal invalidation application, and Continue-As-New/recovery.
- Skip rejected candidates without advancing fairness cursors and continue scanning admissible
  candidates; account for declared concurrency slots and all authoritative capacity gates.
- Reject explicitly empty fairness groups and duplicate registered canonical keys.
- Add contract, application, API integration, Temporal time-skipping, replay, worker-loss,
  cancellation, continuation, controlled incremental-release, and credential-gated live tests.

## Deletion and drift status

- `app/temporal/stagegraph_workflow.py` remains active and currently import-broken.
- `stagegraph.execute_operation` direct activity remains present.
- `app/agent_server/stagegraph/` remains present and imported.
- The canonical replacement is not composed, so none of these deletion gates may be claimed.

Final package disposition remains `blocked`; never `accepted`.
