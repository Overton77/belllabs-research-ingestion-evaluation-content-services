# RRM-003 — Persist exact production checkpoint lineage

**What to build:** each production operation execution records a stable semantic unit and qualified checkpoint transition, queryable through existing repository/service seams for technical verification.

**Blocked by:** RRM-001 accepted contract coverage; RRM-002 baseline repair.
**Status:** blocked
**Branch:** `wp/rrm-003-checkpoint-lineage`
**Authority:** EXEC-003–005, DA exact binding/placement requirements, accepted RRM-001 amendments
**Evidence:** `docs/migrations_instructions/evidence_v2/research-runtime-mission/RRM-003/`

Carry stable structured unit identity and its canonical key through the bound operation, Temporal attempt observation, exact Deep Agent invocation and PostgreSQL checkpoint observation. Capture source/result checkpoints with namespace, ancestry, binding/schema digests, generation and result manifest references. Use real persistent checkpointer and application repository technical integration, not just in-memory contract objects.

- [ ] StageGraph mapped instance/cycle/slot and GoalDirected iteration/revision/session/role remain distinguishable; technical retries do not change semantic identity.
- [ ] Record Activity identity/attempt and expected source checkpoint before provider dispatch; no secrets/raw checkpoint bodies in BellLabs observations.
- [ ] Configure exact thread and namespace; preserve intentional GoalDirected session reuse and rollover isolation.
- [ ] Capture the resulting checkpoint config and link it to the exact immutable operation result manifest.
- [ ] Persist idempotent observations with expected-checkpoint CAS/serialized session ordering; conflicting observations fail closed.
- [ ] Validate namespace ownership, ancestry and frozen binding/schema compatibility.
- [ ] Version audited existing storage/contracts rather than creating a competing lifecycle system.
- [ ] Production composition calls the new seam; native operations and both family regressions retain their accepted behavior.
- [ ] Technical persistent integration demonstrates one operation's before/after checkpoint and one duplicate delivery; record actual migration/runtime/test revisions.

Out of scope: operational public history/fork routes, full ambiguous crash recovery (RRM-004), company fixtures. Successful lineage capture alone is not safe retry completion.
