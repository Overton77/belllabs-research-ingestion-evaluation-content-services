# RRM-001 — Establish accepted lifecycle contract coverage

**What to build:** a reviewable specification/contract inventory that makes the checkpoint, inspection, fork and intervention implementation issues executable under canonical authority.

**Blocked by:** None for authoring; implementation of new contracts remains gated on recorded specification acceptance.
**Status:** accepted 2026-10-01 (meta `6c89143`, merged into meta `main` at `a50d833`; AMD-RRM-001 accepted after independent review under the user's recorded pre-authorization; see [RRM-001 contract authority](../RRM-001-contract-authority.md))
**Branch:** `spec/research-runtime-lifecycle` in the meta repository; application traceability on an issue branch
**Authority:** Existing EXEC-003–008/011/012, RUN-003–010, DA-003/004/013–015, SG/GD canonical requirements; see the parent ticket index.
**Evidence:** reviewed meta commit, contract disposition table, requirement-to-ticket/test map; no executable evidence directory until execution exists

Publish the minimum canonical amendments needed for stable runtime-unit identities, exact checkpoint namespaces/ancestry, expected-checkpoint fencing and terminal result reconstruction, safe macro snapshots/reuse, scoped inspection, and boundary-command application. Distinguish already mandated behavior from newly specified storage/API/protocol details. Resolve whether the draft cognitive-schema spec/ADR must be accepted, narrowed, or excluded; do not silently promote them by implementation.

- [x] Classify existing identities, runtime storage, fork/intervention services, and snapshot contracts as reuse/version/retire with rationale. (authored: contract authority §3, mirrored in the meta index)
- [x] Define exact identity and schema/binding compatibility, source/result checkpoint linkage, CAS/claim semantics, generation boundaries and GoalDirected shared-session ordering. (authored: EXEC-005/013/014, DA-016/017, GD-012, CS-007)
- [x] Define terminal-result detection/reconstruction versus interrupted invocation resume; a completed checkpoint is not permission to append a prompt again. (authored: DA-018, `CON-CP-CHECKPOINT-LINEAGE-V1`)
- [x] Cover crash windows, ambiguous descendants, effect/result settlement, cancellation, operator reconciliation and immutable macro reuse frontier. (authored: DA-018, RUN-007, EXEC-008/012/016)
- [x] Define public read/projection freshness/redaction and command accepted/delivered/applied/rejected receipts; queries remain diagnostic. (authored: RUN-011/012, EXEC-006/007/015)
- [x] Specify GoalDirected durable pause/resume and safe continuation without turning a pause into workflow failure. (authored: GD-011, RUN-004)
- [x] Preserve independently admitted forks/new runs and parent-child ownership; prohibit implicit message/effect cloning. (authored: EXEC-012/016)
- [x] Record review disposition and accepted canonical revision before dependent implementation. Do not claim draft publication alone is acceptance.
- [x] Update the durable ticket index and owning acceptance tests/seams; no remote issue publication. (authored: contract authority §4; ticket Authority lines 03–09/13)

Out of scope: generalized framework extraction, company fixtures, arbitrary mid-invocation cognitive editing. If review requires a user decision, present the concrete spec diff and exact unresolved decision; complete all independent authoring first.
