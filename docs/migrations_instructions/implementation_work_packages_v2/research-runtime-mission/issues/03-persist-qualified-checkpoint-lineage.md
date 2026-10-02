# RRM-003 — Persist exact production checkpoint lineage

**What to build:** each production operation execution records a stable semantic unit and qualified checkpoint transition, queryable through existing repository/service seams for technical verification.

**Blocked by:** RRM-001 accepted contract coverage; RRM-002 baseline repair (both accepted).
**Status:** accepted 2026-10-01 (tested head `07ac167`, integration merge `5b5cb55`; [evidence](../../../evidence_v2/research-runtime-mission/RRM-003/README.md))
**Branch:** `wp/rrm-003-checkpoint-lineage`
**Authority:** EXEC-003–005, DA exact binding/placement requirements, accepted RRM-001 amendments: REQ-CP-EXEC-013/014, REQ-CP-DA-016/017, REQ-BP-GD-012, REQ-CP-CS-007 (amended) — AMD-RRM-001 (accepted, meta `main` `a50d833`; see [RRM-001 contract authority](../RRM-001-contract-authority.md) §4)
**Evidence:** `docs/migrations_instructions/evidence_v2/research-runtime-mission/RRM-003/`

Carry stable structured unit identity and its canonical key through the bound operation, Temporal attempt observation, exact Deep Agent invocation and PostgreSQL checkpoint observation. Capture source/result checkpoints with namespace, ancestry, binding/schema digests, generation and result manifest references. Use real persistent checkpointer and application repository technical integration, not just in-memory contract objects.

- [x] StageGraph mapped instance/cycle/slot and GoalDirected iteration/revision/session/role remain distinguishable; technical retries do not change semantic identity.
- [x] Record Activity identity/attempt and expected source checkpoint before provider dispatch; no secrets/raw checkpoint bodies in BellLabs observations.
- [x] Configure exact thread and namespace; preserve intentional GoalDirected session reuse and rollover isolation.
- [x] Capture the resulting checkpoint config and link it to the exact immutable operation result manifest.
- [x] Persist idempotent observations with expected-checkpoint CAS/serialized session ordering; conflicting observations fail closed.
- [x] Validate namespace ownership, ancestry and frozen binding/schema compatibility.
- [x] Version audited existing storage/contracts rather than creating a competing lifecycle system.
- [x] Production composition calls the new seam; native operations and both family regressions retain their accepted behavior.
- [x] Technical persistent integration demonstrates one operation's before/after checkpoint and one duplicate delivery; record actual migration/runtime/test revisions.

Out of scope: operational public history/fork routes, full ambiguous crash recovery (RRM-004), company fixtures. Successful lineage capture alone is not safe retry completion.

## Implementation record (2026-10-01)

Implemented on `wp/rrm-003-checkpoint-lineage` from integration `57c99bd`; migration `0019_runtime_unit_checkpoint_lineage_v1.sql`. Requirement-to-test map, gate results and the seams left for RRM-004 are in the [RRM-003 evidence](../../../evidence_v2/research-runtime-mission/RRM-003/README.md). Lineage capture is not safe-retry completion: a non-`not_submitted` state fails closed as `checkpoint_lineage_in_doubt` with no model call, and recovery (resume, reconstruction, `in_doubt` incidents, claim takeover) remains RRM-004.
