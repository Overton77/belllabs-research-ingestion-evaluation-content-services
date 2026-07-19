# BellLabs backend handoff: F3 StageGraph to F3 GoalDirected

## Handoff reference

- Created: `2026-07-19T16:35:00-04:00`
- Completed issue: `03 — Durable StageGraph orchestration`
- Recommended next issue: `04 — Durable GoalDirected orchestration`
- Branch: `issue-3-stagegraph-orchestration`
- Repository: `https://github.com/Overton77/belllabs-research-ingestion-evaluation-content-services`

## Delivered mechanism

- The immutable StageGraph blueprint now declares dependency classes, joins, completion and skip
  policy, fairness, concurrency slots, reservations, obligation bindings, bounded stage cycles, and
  bounded whole-workflow cycles while retaining an acyclic dependency graph.
- `app/domain/orchestration/` contains the pure deterministic scheduler, stable semantic
  identities, scoped wait/pause state, descendant invalidation, and immutable-output reuse logic.
- `app/temporal/stagegraph_workflow.py` coordinates only frozen inputs, deterministic decisions,
  timers/signals, and activities. It never reads a mutable definition or external store.
- `app/temporal/orchestration_activities.py` provides the nondeterministic activity boundary and a
  worker factory. Concrete OpenAI Agents SDK/runtime execution remains intentionally deferred to
  F4.
- `app/application/orchestration.py` translates lifecycle intent through the public
  `RunControlService`; it does not write PostgreSQL projections or budget ledgers directly. Its
  launch service resolves the exact admitted F1 configuration and blueprint before creating
  immutable Temporal input.

## Identity and retry invariants

- Temporal activity attempt, semantic operation attempt, stage cycle, workflow cycle, and
  execution epoch are separate fields.
- Activity retry reuses the operation identity, reservation identity, workspace namespace, and
  idempotency key.
- Stage cycles receive a new objective, reservation, workspace namespace, outputs, evaluation,
  and handoff lineage.
- Whole-workflow cycles invalidate only the accepted frontier and its descendants. Unaffected
  immutable outputs remain referenced and are not rerun.
- Final accepted output evidence is reported only after whole-workflow evaluation, preventing
  invalidated candidate outputs from entering the authoritative terminal evidence frontier.

## Executed acceptance fixture

The real Temporal time-skipping fixture contains:

```text
prepare -> extract (one evaluated stage cycle)
                    -> review_a --\
                    -> review_b ----> join -> publish
```

The first whole-workflow evaluation invalidates `review_a`; only `review_a`, `join`, and `publish`
rerun. `prepare`, `extract`, and `review_b` are reused by immutable reference. The fixture also
injects one retryable `review_b` activity failure and replays the completed Temporal history.

## Verification

- Offline suite: `38 passed, 2 skipped`
  - External MongoDB and PostgreSQL acceptance paths remain environment-gated.
- StageGraph tests: `5 passed`
- Ruff lint: passed
- Ruff format check: passed
- Mypy: passed
- IDE diagnostics: no errors

## Deferred work

- Issue 04 owns GoalDirected iteration, verifier, convergence, and Goal Revision semantics.
- Issue 05 owns linked-run admission, dependency classes, result admission, and continuity.
- F4 owns concrete OpenAI Agents SDK execution, operation bindings, provider-observed usage,
  workspaces, artifacts, and snapshots.
- Production worker composition must supply the F4 `StageOperationExecutor` and typed
  `WorkflowEvaluator`; tests intentionally use deterministic fakes only at those nondeterministic
  boundaries.
