# RRM-008 — Reconcile cancellation of active cognition and children

**What to build:** cancel through the application facade stops or safely quiesces a running technical operation, reconciles its child/provider/effect/usage state, and terminalizes only after authoritative settlement.

**Blocked by:** RRM-004, RRM-007 and RRM-013.
**Status:** blocked
**Branch:** `wp/rrm-008-cancellation`
**Authority:** EXEC-008/011, RUN-005/006/007/009/010, async subordinate DA requirements; REQ-CP-EXEC-008 seven-step saga and REQ-CP-DA-008/011 cancel hooks (clarified) — AMD-RRM-001 (accepted 2026-10-01 at meta `6c89143`, merged into meta main `a50d833`; see [RRM-001 contract authority](../RRM-001-contract-authority.md) §4)
**Evidence:** `docs/migrations_instructions/evidence_v2/research-runtime-mission/RRM-008/`

Complete cancellation propagation, heartbeat/progress evidence, provider acknowledgement or ambiguity handling, and late-result disposition. A cancel flag checked before an Activity is not a proof that an already running model/tool stopped.

- [ ] Journal authorized cancellation intent before sending subordinate/provider requests.
- [ ] Long cognitive activities heartbeat compact safe progress and use explicit timeout meanings; cancellation/recovery survives worker loss.
- [ ] StageGraph siblings and GoalDirected executor/verifier active work respect cancellation without bypassing family liability/terminality rules.
- [ ] Async children retain explicit propagation/orphan/late-result decisions; linked-run authority remains independent. Prove this against a real async child on the RRM-013 Agent Server, not the fake Agent Protocol client.
- [ ] Reconcile pending usage, reservations and effect claims; ambiguous effects create governed incidents, not speculative reexecution.
- [ ] Terminal cancellation is immutable and late/superseded-generation outputs cannot promote artifacts or mutate parent evidence.
- [ ] Inject cancellation before dispatch, during controlled model/tool work, during async work and after an ambiguous effect; verify invocation/settlement counts and receipts.
- [ ] Technical persistent Temporal qualification reaches accepted terminal cancellation with no unexplained liability; evidence/review/integration merge complete.
- [ ] (From RRM-004 review, finding 4.) Define how a shared GoalDirected session namespace continues after a unit is settled without a transition. Cases: a budget violation after a terminal leaf; a provider `failed` settlement that left a partial stamped lineage; `abandon_unit`; and cancellation of an `interrupted` unit (EXEC-008: settle `cancelled` with the latest checkpoint as the result checkpoint). Today the head does not move, so the orphan branch makes the next unit in that session classify `foreign_descendant` and park `in_doubt`. Decide either to advance the head over the settled branch by a recorded transition, or to require a session rollover. Prove that the next iteration proceeds.
- [ ] (From RRM-004.) A parked (`in_doubt`) `OperationWorkflow` currently waits only on the reconciliation hint and ignores `request_cancel`. The saga must reach it. A holder releases its claim lease on `asyncio.CancelledError` and at its lease deadline (`OperationLeaseExpired`), so the next attempt classifies the unit at once. There is still no Activity heartbeat or heartbeat timeout (this ticket's).

Out of scope: cancelling useful company missions in this session or promising immediate provider cancellation where the exact placement lacks it.
- [ ] (From RRM-007 review, F3.) Cancel delivery: run control records a `cancel` as a boundary command (`accepted` on the `cancelling` transition, `applied` when the terminal outcome is `cancelled`, `superseded` by any other outcome), but `pending_delivery()` deliberately excludes `cancel` and `TemporalBoundaryCommandTransport.family_delivery` cannot carry it. Deliver cancels root-first through the same ledger (record `delivered` after the root/family acknowledgement) and keep the per-run `execution` sequence space contiguous for the families, which refuse a non-contiguous sequence with a `gap` acknowledgement (either count cancels in the family's sequence or filter them before the contiguity check).
