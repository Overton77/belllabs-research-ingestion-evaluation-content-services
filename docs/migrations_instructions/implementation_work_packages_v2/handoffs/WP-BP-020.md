# WP-BP-020 parallel-worktree handoff

Disposition: `rework_required`

## Kickoff record

- Repository: `C:\Users\Pinda\Proyectos\Biotech\biotech-bp-020-goal-directed`
- Absolute worktree path: `C:\Users\Pinda\Proyectos\Biotech\biotech-bp-020-goal-directed`
- Work package: `WP-BP-020` — Canonical GoalDirected runtime
- Branch: `wp/bp-020-goal-directed`
- BP_BASE_REVISION: `20824742fcdc6f0d97189ceed7fc6cc2d2da2e9e`
- Initial HEAD: `20824742fcdc6f0d97189ceed7fc6cc2d2da2e9e`
- Initial status before this record: clean
- Pre-existing changed paths: none
- Accepted dependency evidence:
  - `docs/migrations_instructions/evidence_v2/WP-CP-030/README.md`
  - `docs/migrations_instructions/evidence_v2/WP-CP-040/README.md`
  - `docs/migrations_instructions/evidence_v2/WP-CP-045/README.md`

## Ownership

- GoalDirected regions in app/domain/control_plane/contracts.py
- GoalDirected branches in app/domain/control_plane/compiler.py
- GoalDirected regions in app/domain/orchestration/contracts.py
- app/domain/orchestration/goal_directed.py
- GoalDirected methods in app/application/orchestration.py
- typed handoff, context-rollover, and verifier application services
- app/temporal/workflows/goal_directed.py
- GoalDirected-specific handlers, tests, replay fixtures, acceptance paths, and evidence

Shared-file edits are restricted to the named WP-BP-020 family regions. Family-neutral unions,
exports, adapters, dispatch, registries, root workflows, and generic operation workflows remain
integrator-owned.

## Non-goals and frozen foundation

- No sibling-family implementation.
- No lifecycle, terminality, async-delegation, provider-placement, or Deep Agent binding redefinition.
- No whole-file formatting or unrelated import sorting in shared containers.
- No WP-CP-050 combined-capability work.
- A required foundation change stops implementation and is recorded as an explicit amendment request.

## Deletion gates

- app/temporal/goal_directed_workflow.py
- direct goal activities and OpenAI Agents SDK goal/session runtime
- app/agent_server/goal_directed/

## Planned qualification

- Focused unit/property and contract suites for WP-BP-020.
- API integration and Temporal time-skipping, replay, recovery, cancellation, and continuation suites.
- Accepted CP-030/040/045 regression suites.
- `QUAL-BP-GOAL-DIRECTED-CONVERGENCE`.
- `uv run ruff check app tests scripts`.
- `uv run mypy app`.
- Full offline `uv run pytest -q --tb=short`.
- Credential-gated real-LLM API-to-Temporal vertical after deterministic gates pass.

## Progress, changed paths, commands, evidence, and risks

Update this section continuously. Do not create the package evidence directory until real executable
evidence exists. Do not set `accepted`; the integrator owns final combined acceptance.

### Shared-file region locks

- `app/domain/control_plane/contracts.py`: `GoalSessionRolloverPolicy` through
  `GoalDirectedBlueprint` only. Family-neutral blueprint/definition unions are integrator-owned.
- `app/domain/control_plane/compiler.py`: GoalDirected arms of `_declared_variants` and
  `_validate_implementation_binding`, plus adjacent GoalDirected-only validation helpers.
- `app/domain/orchestration/contracts.py`: `GoalVerifierAction` through
  `GoalDirectedRunResult` only.
- `app/application/orchestration.py`: `GoalDirectedLaunchService` and GoalDirected protocols only.
  Family-neutral dispatch and `RunControlLifecycleGateway` remain unchanged.

### Architecture decisions

- Reuse the frozen `OperationWorkflowRequest` V2 and `OperationWorkflow`; executor and verifier
  receive distinct semantic attempts, exact bindings, sessions, and writable workspaces.
- Reuse `AtomicFamilyMutation` and `RunControlService.execute_family_admission` through a
  GoalDirected-owned mutation subtype and application gateway. Do not modify run control.
- Persist immutable revision, iteration, handoff, and verifier detail in Mongo/Beanie; PostgreSQL
  atomic family receipts remain acceptance authority.
- Consume `classify_async_delegation` unchanged and add GoalDirected routing fixtures only.
- Family state emits continuation/stopping proposals and never owns `terminal`.

### Current paths and risks

- Changed owned/shared-region paths recovered after the supervisor crash:
  - `app/domain/control_plane/contracts.py`
  - `app/domain/control_plane/compiler.py`
  - `app/domain/orchestration/contracts.py`
  - `app/domain/orchestration/goal_directed.py`
  - `app/domain/orchestration/goal_directed_runtime.py`
  - `app/application/orchestration.py`
  - `app/application/goal_directed.py`
  - `app/application/mongo_goal_directed_repository.py`
  - `app/models/goal_directed.py`
  - `app/temporal/activities/goal_directed.py`
  - `app/temporal/workflows/goal_directed.py`
- GoalDirected fixture changes currently also exist in
  `app/domain/control_plane/fixtures.py` and `app/domain/schema_grounding/definitions.py`;
  ownership and necessity must be verified before review.
- Existing `tests/test_goal_directed_interpreter.py` and
  `tests/test_goal_directed_temporal.py` describe the superseded direct-activity contract and are
  currently module-skipped. Canonical replacement coverage is in
  `tests/test_wp_bp_020_goal_directed.py` and `tests/test_wp_bp_020_temporal.py`; the skipped files
  remain part of the deletion gate rather than acceptance evidence.
- Deletion gate remains the legacy flat GoalDirected workflow/direct activities and
  `app/agent_server/goal_directed/`.
- P0 integration blocker: this branch removed legacy symbols from the integrator-owned
  `app/application/orchestration_routing.py` and
  `app/application/schema_grounding_semantic_handlers.py` while active coordinator composition
  still imports them. The WP must not invent the replacement registry wiring. Restore/preserve the
  active integrator surface during integration, then inject the new GoalDirected activities and
  remove the legacy path atomically.
- Integrator-owned follow-up: register all new GoalDirected Beanie models, inject the Mongo
  repository and operation-template provider, wire API/worker family admission and Temporal
  registries, consume typed scope-expansion proposals, and remove legacy modules at the deletion
  gate.
- WP-owned rework still required: make authored context-compaction failure actions executable and
  qualify compaction failure. Cancellation deliberately enters the shared reconciliation saga with
  reservations retained; integration must prove that saga settles them before terminalization.
- `REQ-BP-GD-008` is implemented at the package-owned boundary required by the active WP: consume
  the frozen `classify_async_delegation` function and prove subordinate, operation, and linked-run
  routing fixtures. Actual subordinate execution remains inside the exact parent
  `OperationWorkflow`; generic operation/linked-run dispatch is not a second family classifier.
- Soft-budget continuation remains a typed deterministic interpreter decision. The canonical
  specification defines precedence and an authorized continuation action, but does not authorize
  this family package to rewrite generic reservations or operation bindings.
- No foundation blocker identified. If exact V2 requests or atomic family admission require a
  frozen-contract edit, stop and record `FOUNDATION_AMENDMENT_REQUIRED`.

### Recovery verification log

- `git rev-parse HEAD`: `20824742fcdc6f0d97189ceed7fc6cc2d2da2e9e`, exactly matching the
  recorded `BP_BASE_REVISION`.
- Accepted `WP-CP-045` evidence was rechecked; disposition remains `accepted`.
- Focused Ruff over all recovered GoalDirected implementation paths: `All checks passed!`.
- Focused mypy over the new GoalDirected domain/application/Temporal paths:
  `Success: no issues found in 8 source files`.
- Focused legacy GoalDirected pytest collection: failed with two import errors for removed
  `GoalHandoffCheckpoint`. No test executed; this is an expected incomplete-replacement failure,
  not passing evidence.
- Credential-gated live qualification was not run. It remains pending until deterministic gates
  are green and the required services/credentials are confirmed.
- Recovery-stage disposition was `in_progress`; the current final handoff disposition is
  `rework_required`.

### Post-recovery implementation and verification

- Canonical executor and verifier operations now use persisted strict structured-output templates,
  execute as separate `OperationWorkflow` children, reconcile into typed family results, and bind
  host-owned operation/binding/session/workspace/verifier authority before immutable persistence.
- Reconciliation now carries the generic operation effect frontier and active async children into
  family effect/liability facts; terminalization no longer infers `effects_settled` from a
  non-empty digest.
- Verifier reconciliation now receives the exact admitted executor result, freezes its output and
  evidence frontier into the verifier result, and rejects output contracts outside the frozen
  blueprint.
- Fresh-session operations now receive the complete typed immutable handoff in their exact authored
  context, not only a handoff identifier. Handoff digests, policy bindings, protected facts,
  continuation evidence/context, and instruction-size limits fail closed.
- Continue-As-New no longer reissues lifecycle `start`; it preserves cumulative iteration/agent
  counts and a compact chained lineage digest. A time-skipping test proves 25 semantic iterations,
  one lifecycle start, continuation, and replay.
- Scope expansion returns a typed governed family proposal without falsely terminalizing the run.
  The generic root/application consumer remains integrator-owned.
- `revision_required` without a bounded immutable revision now fails closed.
- Application reconciliation now replaces provider-chosen verifier and handoff identities with
  deterministic host-bound identities and canonical digests before immutable persistence.
- Handoff workspace/snapshot references must use blueprint-allowed reference classes and source
  document/binding references must be canonical SHA-256 digests. Repeated rollover exhaustion is
  now explicitly tested.
- Focused canonical GoalDirected suite:
  `uv run pytest -q tests/test_wp_bp_020_goal_directed.py tests/test_wp_bp_020_temporal.py --tb=short`
  — `35 passed`.
- Accepted CP-020/030/040/045 and Operation Execution regressions — `60 passed`.
- Final combined GoalDirected plus accepted-foundation regression command — `95 passed`.
- Repository Ruff (`app tests scripts`) — passed.
- Focused mypy over changed canonical GoalDirected domain/application/workflow paths — passed.
- `git diff --check` — passed.
- Repository-wide mypy — `23 errors in 4 files`; all are legacy/integrator composition surfaces
  that still reference removed direct GoalDirected contracts and handlers.
- Full offline pytest collection — interrupted with 12 collection errors: ten derive from the same
  active coordinator/legacy import break; two server/API modules additionally require undeclared
  environment credentials. No fake credentials were introduced.
- Credential-gated live qualification was not run because deterministic repository-wide gates are
  not green.
- Final handoff disposition is `rework_required`; no commit or package evidence directory was
  created.
