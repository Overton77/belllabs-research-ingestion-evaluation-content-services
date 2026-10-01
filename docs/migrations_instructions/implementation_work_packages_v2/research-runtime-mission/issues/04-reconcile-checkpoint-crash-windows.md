# RRM-004 — Recover checkpoint and settlement crash windows

**What to build:** interrupted production operations converge to one accepted result/settlement without appending a completed prompt or repeating an already applied provider effect.

**Blocked by:** RRM-003.
**Status:** blocked
**Branch:** `wp/rrm-004-checkpoint-recovery`
**Authority:** EXEC-004/005/008, RUN effect/settlement requirements, accepted checkpoint protocol from RRM-001
**Evidence:** `docs/migrations_instructions/evidence_v2/research-runtime-mission/RRM-004/`

Integrate checkpoint observations with durable operation claims, attempt leases, result manifests and authoritative settlement. Reconcile claimed-but-unsettled work before provider invocation. Separate intermediate-checkpoint resume from terminal-result reconstruction. Recognize that durability of graph state does not make arbitrary tool side effects transactional.

- [ ] Existing authoritative settlement returns unchanged without provider work.
- [ ] Terminal checkpoint/result manifest without settlement completes settlement without reinvocation.
- [ ] Checkpoint committed without observation discovers and validates the unique qualifying descendant or creates an in_doubt incident.
- [ ] Interrupted intermediate checkpoints resume according to the accepted protocol without duplicating submitted input.
- [ ] Stale worker claims/generations cannot apply a late competing observation; concurrent recovery serializes.
- [ ] Ambiguous consequential effects are reconciled through their existing claim/settlement contract rather than repeated speculatively.
- [ ] Inject crashes before checkpoint, after intermediate/terminal checkpoint, before observation and before settlement; assert model/tool invocation counts, prompt counts, ancestry and final result digest.
- [ ] Prove real persistent saver/application-database worker-restart recovery; memory-only tests do not satisfy the integration gate.
- [ ] Digest/ancestry mismatch and multiple valid descendants produce typed failure/incidents; recorded results remain immutable.
- [ ] Captured operation histories replay and retries retain semantic identity; evidence/review/integration merge complete.

Out of scope: arbitrary checkpoint editing, company missions, model/tool fallback that changes exact placement.
