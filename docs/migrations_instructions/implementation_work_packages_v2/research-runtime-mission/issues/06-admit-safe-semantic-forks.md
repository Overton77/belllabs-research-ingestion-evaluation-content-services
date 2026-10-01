# RRM-006 — Admit semantic forks from safe combined snapshots

**What to build:** an operator selects a qualified checkpoint and safe macro snapshot, applies a validated patch, and starts an independently admitted derived run without mutating its parent.

**Blocked by:** RRM-004 and RRM-005.
**Status:** blocked
**Branch:** `wp/rrm-006-forks`
**Authority:** EXEC-012, RUN admission/effect/budget requirements, DA-015 where sandbox cloning applies, accepted RRM-001 snapshot/patch contracts
**Evidence:** `docs/migrations_instructions/evidence_v2/research-runtime-mission/RRM-006/`

Wire the existing fork admission/copy saga to local Deep Agent checkpoint lineage and a real immutable macro RunSnapshotManifest. Snapshot at declared semantic boundaries or classify/quiesce active work under the accepted protocol. Compute exact invalidation/reuse and independently compile/admit the target.

- [ ] Snapshot binds authoritative projection/version, exact configuration/binding digests, cycles/revision, accepted results/evidence, budgets/effects, child status and cognitive/sandbox references.
- [ ] Protected identity, authority, evidence, budgets and terminality fields cannot be patched.
- [ ] New run starts at epoch 1 with its own admission, reservation, thread/namespace and explicit source checkpoint lineage.
- [ ] Reuse only settled compatible immutable results; active parent children remain parent-owned and pending messages/effects are not blindly copied.
- [ ] Fork idempotency and admission/provider-copy crash recovery use the audited existing saga and one durable receipt.
- [ ] Real persistence/API-to-Temporal technical forks for both families produce distinct artifact/result refs; original state and artifacts remain unchanged.
- [ ] Stale expected version/checkpoint, incompatible restore, unauthorized scope and ambiguous copy fail safely.
- [ ] Prove the supported fork boundary precisely. Arbitrary executable graph-node forks remain deferred unless resume/patch compatibility and macro reuse are separately qualified.
- [ ] Record safe-boundary/quiescence, patch, reuse and lineage manifests plus reviewed integration commit.

Out of scope: Temporal Reset as product branching, rollback of provider effects, company report forks in this session.
