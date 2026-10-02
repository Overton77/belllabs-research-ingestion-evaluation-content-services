# RRM-004 — Recover checkpoint and settlement crash windows

**What to build:** interrupted production operations converge to one accepted result/settlement without appending a completed prompt or repeating an already applied provider effect.

**Blocked by:** RRM-003.
**Status:** accepted 2026-10-01 (tested head `d296481`, integration merge `fcefd54`; [evidence](../../../evidence_v2/research-runtime-mission/RRM-004/README.md))
**Branch:** `wp/rrm-004-checkpoint-recovery`
**Authority:** EXEC-004/005/008, RUN effect/settlement requirements, accepted checkpoint protocol from RRM-001: REQ-CP-DA-018 and `CON-CP-CHECKPOINT-LINEAGE-V1` crash windows, REQ-CP-EXEC-014 (claim fence), REQ-CP-EXEC-005 and REQ-CP-RUN-007 (clarified) — AMD-RRM-001 (accepted 2026-10-01 at meta `6c89143`, merged into meta main `a50d833`; see [RRM-001 contract authority](../RRM-001-contract-authority.md) §4)
**Evidence:** `docs/migrations_instructions/evidence_v2/research-runtime-mission/RRM-004/`

Integrate checkpoint observations with durable operation claims, attempt leases, result manifests and authoritative settlement. Reconcile claimed-but-unsettled work before provider invocation. Separate intermediate-checkpoint resume from terminal-result reconstruction. Recognize that durability of graph state does not make arbitrary tool side effects transactional.

- [x] Existing authoritative settlement returns unchanged without provider work.
- [x] Terminal checkpoint/result manifest without settlement completes settlement without reinvocation.
- [x] Checkpoint committed without observation discovers and validates the unique qualifying descendant or creates an in_doubt incident.
- [x] Interrupted intermediate checkpoints resume according to the accepted protocol without duplicating submitted input.
- [x] Stale worker claims/generations cannot apply a late competing observation; concurrent recovery serializes.
- [x] Ambiguous consequential effects are reconciled through their existing claim/settlement contract rather than repeated speculatively.
- [x] Inject crashes before checkpoint, after intermediate/terminal checkpoint, before observation and before settlement; assert model/tool invocation counts, prompt counts, ancestry and final result digest.
- [x] Prove real persistent saver/application-database worker-restart recovery; memory-only tests do not satisfy the integration gate.
- [x] Digest/ancestry mismatch and multiple valid descendants produce typed failure/incidents; recorded results remain immutable.
- [x] Captured operation histories replay and retries retain semantic identity; evidence recorded (review and integration merge pending, coordinator-owned).

Out of scope: arbitrary checkpoint editing, company missions, model/tool fallback that changes exact placement.

**Continuation semantics (RRM-004 review).** `RunControlOperationAuthority.verify_continuation` admits a retry, takeover or recovery of a unit that already holds a claim when the run is `active` or `waiting`, with any wait kind. A declared wait (dependency, approval, operator reconciliation and so on) does not supersede already-claimed work. Terminal, pending, paused and cancelling runs, and foreign bindings, are refused (fail-closed).

**Residual limits of the lease deadline.**
- A blocking synchronous tool running in an executor thread cannot be cancelled by the lease timeout, because Python cannot cancel a thread. The asyncio side stops, but the thread finishes. This is owned by RRM-008's heartbeat and cancellation item.
- Each lease cut ends the Temporal Activity attempt with a retryable error, so it consumes one of the three attempts in `OperationWorkflow`'s retry policy.
- The safety margin (20% of the lease, 1–30 s) covers clock skew between the Temporal server and the worker only up to the margin. Larger skew is not covered: a worker clock running ahead ends the holder early (safe), and one running behind can let it overrun.

Implementation evidence: [RRM-004 README](../../../evidence_v2/research-runtime-mission/RRM-004/README.md). Follow-ups split out: RRM-014 (re-admit a unit at a new execution generation) and RRM-015 (set-order-stable contract digests outside the replay path).
