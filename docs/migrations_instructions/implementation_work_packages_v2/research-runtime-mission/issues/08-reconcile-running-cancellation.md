# RRM-008 — Reconcile cancellation of active cognition and children

**What to build:** cancel through the application facade stops or safely quiesces a running technical operation, reconciles its child/provider/effect/usage state, and terminalizes only after authoritative settlement.

**Blocked by:** RRM-004 and RRM-007.
**Status:** blocked
**Branch:** `wp/rrm-008-cancellation`
**Authority:** EXEC-008/011, RUN-005/006/007/009/010, async subordinate DA requirements
**Evidence:** `docs/migrations_instructions/evidence_v2/research-runtime-mission/RRM-008/`

Complete cancellation propagation, heartbeat/progress evidence, provider acknowledgement or ambiguity handling, and late-result disposition. A cancel flag checked before an Activity is not a proof that an already running model/tool stopped.

- [ ] Journal authorized cancellation intent before sending subordinate/provider requests.
- [ ] Long cognitive activities heartbeat compact safe progress and use explicit timeout meanings; cancellation/recovery survives worker loss.
- [ ] StageGraph siblings and GoalDirected executor/verifier active work respect cancellation without bypassing family liability/terminality rules.
- [ ] Async children retain explicit propagation/orphan/late-result decisions; linked-run authority remains independent.
- [ ] Reconcile pending usage, reservations and effect claims; ambiguous effects create governed incidents, not speculative reexecution.
- [ ] Terminal cancellation is immutable and late/superseded-generation outputs cannot promote artifacts or mutate parent evidence.
- [ ] Inject cancellation before dispatch, during controlled model/tool work, during async work and after an ambiguous effect; verify invocation/settlement counts and receipts.
- [ ] Technical persistent Temporal qualification reaches accepted terminal cancellation with no unexplained liability; evidence/review/integration merge complete.

Out of scope: cancelling useful company missions in this session or promising immediate provider cancellation where the exact placement lacks it.
