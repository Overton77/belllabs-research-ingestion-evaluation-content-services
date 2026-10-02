# RRM-007 — Apply governed boundary interventions to running workflows

**What to build:** an authorized application command pauses/resumes or releases a declared wait in a running family workflow, with durable proof of application rather than only message acceptance.

**Blocked by:** RRM-004.
**Status:** blocked
**Branch:** `wp/rrm-007-intervention`
**Authority:** EXEC-001/006/007/011, RUN-004, canonical family semantics and accepted command-routing details from RRM-001: receipt states in `CON-CP-WORKFLOW-MESSAGE-V1`, REQ-CP-RUN-004 and REQ-BP-SG-009 (clarified), REQ-BP-GD-011 (durable pause) — AMD-RRM-001 (accepted 2026-10-01 at meta `6c89143`, merged into meta main `a50d833`; see [RRM-001 contract authority](../RRM-001-contract-authority.md) §4)
**Evidence:** `docs/migrations_instructions/evidence_v2/research-runtime-mission/RRM-007/`

Connect the run-control facade/ledger/outbox to exact root/family operation boundaries. Existing root receipts do not forward generic messages into cognition. StageGraph signals need governed routing; GoalDirected's non-retryable goal_paused path must become a durable paused execution that awaits accepted resume.

- [ ] Commands are immutable, scope/target/version/generation bound, idempotent and authorized before Temporal transport.
- [ ] Acceptance, delivery, boundary application and rejection have distinct durable receipts; duplicates cannot apply twice.
- [ ] StageGraph declared wait can be inspected while running and released via the application facade.
- [ ] GoalDirected pause remains durably resumable with accepted continuation state, budgets and exact revision/session identity; it does not report failure merely for pausing.
- [ ] Scoped wait/pause does not block unrelated admissible work and aggregate lifecycle remains accurate.
- [ ] Worker restart, redelivery, stale target and Continue-As-New preserve command ordering and receipts.
- [ ] Accepted resume continues from the correct frontier without repeating settled work or mutating frozen bindings.
- [ ] Technical real-Temporal demonstrations cover both family boundaries with persistent command/application evidence.
- [ ] (From RRM-004 review.) `reconcile_unit` gets governed delivery: an API route over `UnitReconciliationService`, an operator role that holds `workflow_run.reconcile_unit` (the `operator` role in `app/api/run_control.py` does not today), and durable `accepted → delivered → applied` receipts. Today the `unit_reconciliation_recorded` signal is a hint transport only; the parked `OperationWorkflow` reads the accepted decision from run-control authority.

Out of scope: arbitrary state edits inside a currently executing Deep Agent. Optional source/clarification steering requires an accepted typed context/binding derivation contract; do not append model messages directly as a shortcut. Cancellation's active-provider reconciliation is RRM-008.
