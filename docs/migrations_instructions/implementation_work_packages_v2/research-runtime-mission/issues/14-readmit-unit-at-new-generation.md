# RRM-014 — Re-admit a runtime unit at a new execution generation after `start_new_generation`

**What to build:** after an accepted `reconcile_unit` `start_new_generation` decision, the owning family re-binds and dispatches the same unit (same `unit_key`) at generation `g + 1`, in its own unit-generation namespace, so the unit can reach exactly one settlement.

**Blocked by:** RRM-004 (generation boundary, fencing and decision authority).
**Blocks:** No mission ticket directly. Until it lands, `start_new_generation` fences the old generation and ends its `OperationWorkflow` with `in_doubt` / `generation_superseded`; the family treats that as a non-completed operation.
**Status:** ready-for-agent after RRM-004 is accepted
**Branch:** `wp/rrm-014-unit-generation-readmission`
**Authority:** REQ-CP-EXEC-005 (generation boundary, AMD-RRM-001), `CON-CP-LIFECYCLE-V1` `reconcile_unit`, REQ-BP-GD-012 (GoalDirected unit-generation namespace), REQ-CP-DA-016 (namespaces)

## Diagnosis (RRM-004, 2026-10-01)

RRM-004 implements the authority and lineage half of `start_new_generation`:

- run control records the decision in `RunProjection.unit_reconciliations`;
- the lineage repository marks generation `g` superseded (`runtime_unit_generations.superseded`), so every later attempt, transition or result write of `g` is refused and recorded as `stale_execution_generation`;
- the unit's namespace reservation is released.

The execution half is missing because the operation layer has no generation dimension:

- `OperationExecutionBinding.binding_id` derives from the request scope and semantic key only (`_binding_for` in `app/application/operations/operation_execution.py`). A generation-`g+1` request has a different fingerprint, so it is refused as `IdempotencyConflict`.
- The journal claim key is `(scope, operation contract digest, idempotency_key)`. The family supplies the idempotency key, and no family derives one per generation.
- No family preparer emits `execution_generation > 1`. StageGraph takes it from `proposal.identity`; GoalDirected hard-codes `1` in `app/temporal/workflows/goal_directed.py`.

## Acceptance

- [ ] Generation-qualified operation identity: generation 1 keeps today's IDs (replay compatibility), and generation `g > 1` derives a distinct binding ID and effect-claim key. The `unit_key` does not change.
- [ ] Both families observe `generation_superseded`, re-bind the unit at `g + 1` (GoalDirected: `belllabs/goal/{run}/epoch/{e}/unit/{unit_key}/gen/{g}`, fresh from the handoff), and dispatch it once. The next iteration moves to a new session generation.
- [ ] Late writes of generation `g` stay rejected and recorded. Generation `g + 1` settles the unit exactly once. Budget, effect and usage of generation `g` are reconciled, never dropped.
- [ ] Captured-history replay for both families. Evidence under `evidence_v2/research-runtime-mission/RRM-014/`.

Out of scope: forks and seeded cognition (RRM-006), and interventions other than `reconcile_unit` (RRM-007).
