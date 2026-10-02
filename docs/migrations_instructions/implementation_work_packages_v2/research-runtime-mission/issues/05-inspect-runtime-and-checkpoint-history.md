# RRM-005 — Inspect lifecycle and historical checkpoints

**What to build:** an authorized operator can inspect active/terminal runs and semantic units, join macro and cognitive lineage, and select an earlier qualified checkpoint for a redacted state summary.

**Blocked by:** RRM-004.
**Status:** blocked
**Branch:** `wp/rrm-005-inspection`
**Authority:** diagnostic EXEC-007, run-control authority, accepted inspection/projection contracts from RRM-001: REQ-CP-RUN-011/012 and `CON-CP-INSPECTION-READ-V1`, REQ-CP-EXEC-015 (Search Attributes), REQ-CP-EXEC-007 (clarified) — AMD-RRM-001 (accepted 2026-10-01 at meta `6c89143`, merged into meta main `a50d833`; see [RRM-001 contract authority](../RRM-001-contract-authority.md) §4)
**Evidence:** `docs/migrations_instructions/evidence_v2/research-runtime-mission/RRM-005/`

Deliver public scoped list/detail/unit/history reads through the application facade, including real Temporal Visibility/Search Attribute mapping where specified. PostgreSQL observations are primary; immutable family detail and live runtime/checkpointer reads are explicitly qualified sources. A schema-export route is not an inspection endpoint.

- [ ] Run/unit responses expose structured semantic identity, Temporal IDs/attempts, exact binding, thread/namespace/checkpoint ancestry, result/artifact refs, budget/effect status and reconciliation state.
- [ ] Scoped pagination and historical reads validate ownership, graph/schema/binding compatibility and checkpoint ancestry.
- [ ] Ordinary reads never mutate state or settle an operation; privileged reconciliation is a separate command.
- [ ] Projection freshness/source observation times and unavailable or stale runtime data are visible; live Temporal availability is not required for every persisted read.
- [ ] Redacted summaries are separately authorized; checkpoint bodies/secrets/transcripts are not exposed by default.
- [ ] Real namespace qualification verifies Search Attributes and runtime joins without querying Temporal's internal PostgreSQL database.
- [ ] API tests cover cross-scope access, bad/expired cursor, incompatible checkpoint, active/terminal state, missing live provider and redaction.
- [ ] Demonstrate a tiny technical run with at least two checkpoints, selected historical read and captured-history replay; no company research.

Out of scope: checkpoint mutation and semantic fork execution (RRM-006), UI redesign, general-purpose raw state access.
