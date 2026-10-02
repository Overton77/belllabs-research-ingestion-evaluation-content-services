# RRM-006 — Admit semantic forks from safe combined snapshots

**What to build:** an operator selects a qualified checkpoint and safe macro snapshot, applies a validated patch, and starts an independently admitted derived run without mutating its parent.

**Blocked by:** RRM-004 and RRM-005.
**Status:** implemented; independent review pending
**Branch:** `wp/rrm-006-forks`
**Authority:** EXEC-012, RUN admission/effect/budget requirements, DA-015 where sandbox cloning applies, accepted RRM-001 snapshot/patch contracts: REQ-CP-EXEC-012 (clarified), REQ-CP-EXEC-016, `CON-CP-CONTINUATION-V1` (`RunSnapshotManifest`, `RunForkPatch`), REQ-CP-DA-016 (seed keys) — AMD-RRM-001 (accepted 2026-10-01 at meta `6c89143`, merged into meta main `a50d833`; see [RRM-001 contract authority](../RRM-001-contract-authority.md) §4)
**Evidence:** `docs/migrations_instructions/evidence_v2/research-runtime-mission/RRM-006/`

Wire the existing fork admission/copy saga to local Deep Agent checkpoint lineage and a real immutable macro RunSnapshotManifest. Snapshot at declared semantic boundaries or classify/quiesce active work under the accepted protocol. Compute exact invalidation/reuse and independently compile/admit the target.

- [x] Snapshot binds authoritative projection/version, exact configuration/binding digests, cycles/revision, accepted results/evidence, budgets/effects, child status and cognitive/sandbox references.
- [x] Protected identity, authority, evidence, budgets and terminality fields cannot be patched.
- [x] New run starts at epoch 1 with its own admission, reservation, thread/namespace and explicit source checkpoint lineage.
  - Namespaces are not separate receipt fields: each derived unit gets its own cognitive namespace derived from its derived `unit_key` (a different run gives a different key). Source checkpoint lineage is explicit as the reuse candidates' result checkpoint keys in the snapshot plus the receipt lineage (seed checkpoint `null`; the seed is deferred to RRM-017).
- [x] Reuse only settled compatible immutable results; active parent children remain parent-owned and pending messages/effects are not blindly copied.
- [x] Fork idempotency and admission/provider-copy crash recovery use the audited existing saga and one durable receipt.
- [x] Real persistence/API-to-Temporal technical forks for both families produce distinct artifact/result refs; original state and artifacts remain unchanged.
  - **Caveat (review 2026-10-02):** for GoalDirected this is proven for snapshot, independent admission and a fresh derived run only. GoalDirected operations bypass the run-control journal and the demonstration uses an accepting operation authority, so the "no unresolved effect" quiescence check passes vacuously for GoalDirected and neither governed settlement nor reuse is proven for it. RRM-016 (mission-blocking for RRM-010) closes this.
- [x] Stale expected version/checkpoint, incompatible restore, unauthorized scope and ambiguous copy fail safely.
- [x] Prove the supported fork boundary precisely. Arbitrary executable graph-node forks remain deferred unless resume/patch compatibility and macro reuse are separately qualified.
- [x] Record safe-boundary/quiescence, patch, reuse and lineage manifests plus reviewed integration commit.

Out of scope: Temporal Reset as product branching, rollback of provider effects, company report forks in this session.
