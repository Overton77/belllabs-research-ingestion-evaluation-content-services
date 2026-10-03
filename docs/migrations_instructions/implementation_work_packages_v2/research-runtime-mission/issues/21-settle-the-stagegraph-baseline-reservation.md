# RRM-021 — Settle the admitted baseline reservation of a StageGraph run

**What to build:** A StageGraph run admitted with a baseline reservation (`BudgetEnvelope.baseline_reservations`) settles or releases that reservation before it terminalizes, so a run that completed its work reaches `terminal / completed` instead of failing `budget_not_settled`.

**Blocked by:** None.
**Blocks:** Every StageGraph run admitted with a non-empty baseline reservation, through any launch path (RRM-009's governed launch included). Today every StageGraph harness admits `baseline_reservations={}` (the WP-BP-010 live gate, RRM-009's technical qualification). Whether it blocks RRM-010 is the coordinator's decision.
**Status:** implemented; independent review pending (found by RRM-009, 2026-10-02; code `c30c9f8`; evidence `evidence_v2/research-runtime-mission/RRM-021/README.md`)
**Branch:** `wp/rrm-021-stagegraph-baseline-settlement`
**Authority:** REQ-CP-RUN-006 (budgets: reservations are released or settled before terminalization), the reducer's terminalization rule (`budget_not_settled`)

## Diagnosis (RRM-009, 2026-10-02)

- Admission records the baseline reservation (`RunControlService.admit`, `reservations["baseline"]`), and `StageGraphRunInput.baseline_reservation` carries it to the family.
- The StageGraph workflow never uses it: no activity settles or releases `baseline` (`app/temporal/workflows/stagegraph.py`). GoalDirected settles it at its closing boundary (`_settle_operation(..., "baseline", ...)` in `app/temporal/workflows/goal_directed.py`).
- The terminalization proposal then reports `budget_settled=False` (`StageGraphDecisionService.complete`), and the reducer rejects it with `budget_not_settled` ("budget reservations or charges remain"). The family fails with `StageGraph terminalization rejected: budget_not_settled`, and the root fails with it.
- Observed in RRM-009's technical qualification before its StageGraph admission was changed to an empty baseline: the run's budget held `reserved: {"tokens.total": 20}` under `reservations: {"baseline": {"tokens.total": 20}}` after both stages settled.

## Acceptance

- [x] A StageGraph run admitted with a baseline reservation releases (or settles against recorded usage) the baseline before it proposes terminalization, and completes.
- [x] The change is replay-safe for in-flight StageGraph histories (`workflow.patched`).
- [x] The governed launch keeps refusing a family input whose baseline differs from the admitted one (RRM-009).
