# 05 — Implement linked-run composition and durable orchestration continuity

**What to build:** Make every Workflow Type boundary a distinct, idempotently admitted child Workflow Run with explicit parent-side dependency, authority, budget, cancellation, and exact result-admission semantics, while preserving logical execution across Temporal history compaction.

**Blocked by:**
- 01 — Implement versioned Workflow Definitions and Effective Run Configuration compilation
- 02 — Implement transactional Run admission, lifecycle, budgets, and domain events
- 03 — Implement durable StageGraph orchestration
- 04 — Implement durable GoalDirected orchestration

**Status:** ready-for-agent

**GitHub:** https://github.com/Overton77/belllabs-research-ingestion-evaluation-content-services/issues/5


**Read first:** [Local control-plane ticket index](INDEX.md)

## Implementation guidance

- Create application Run Composition Link and dependency decision records even when Temporal maps the child to a Child Workflow.
- Reuse ticket 01 compilation and ticket 02 admission independently for every child; the parent contributes ceilings and slot constraints, not executable child configuration.
- Carry only compact accepted state through Continue-As-New and preserve all semantic identities, links, revisions, waits, and reservations.

## Verification approach

Use minimal parent and child fixture Workflow Types to exercise retry, all dependency classes, cancellation, result admission, late delivery, and Continue-As-New under replay.

## Explicit non-goals

- Inlining child Workflow Types, credential propagation by process hierarchy, automatic result copying, and retroactive mutation of terminal parents.
- Mission-level scheduling above linked Workflow Runs.

## Acceptance criteria

- [ ] A parent-scoped linked identity returns one child, reservation, and Run Composition Link under retry/replay; conflicting fingerprint reuse is rejected.
- [ ] Each child independently passes its own admission and compilation under all child, parent, caller, permission, overlay, and environment ceilings.
- [ ] Required blocking, degradable blocking, degradable nonblocking, and detached advisory classes produce declared wait, timeout, degradation, and completion behavior.
- [ ] Dependency changes create immutable authorized revisions and assess affected obligations, artifacts, evaluations, and readiness.
- [ ] Cancellation follows class-specific parent policy; child termination never automatically reverse-cancels the parent.
- [ ] Every exact child output receives an admit, conditionally admit, reject, or defer decision against purpose, compatibility, readiness, provenance, permissions, and evaluation evidence.
- [ ] Late results cannot mutate a terminal parent and require a new admitted run for reuse.
- [ ] Continue-As-New preserves run identity, semantic counters, waits, links, accepted revisions, and budgets without creating a semantic cycle or resetting authority.

## Source basis

- Durable Blueprint Orchestration and Linked Runs
- Versioned Workflow Definitions and Effective Run Configuration
- Transactional Run Admission, Lifecycle, and Budgets

The source specifications and Human Upgrade System glossary were available locally when this ticket was authored. This ticket is intentionally self-contained for a fresh implementation agent.
