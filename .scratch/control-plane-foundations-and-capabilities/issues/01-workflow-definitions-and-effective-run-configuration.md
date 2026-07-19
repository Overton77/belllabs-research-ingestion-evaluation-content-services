# 01 — Implement versioned Workflow Definitions and Effective Run Configuration compilation

**What to build:** Create the application-owned definition registry and pure compiler that turn exact, immutable Workflow Type assets into the only executable configuration a Workflow Run may consume. Later workflow-specific stages, obligations, operations, policies, and outputs must plug into these contracts without allowing callers or runtime availability to invent semantics.

**Blocked by:**
None — can start immediately

**Status:** ready-for-agent

**GitHub:** https://github.com/Overton77/belllabs-research-ingestion-evaluation-content-services/issues/1


**Read first:** [Local control-plane ticket index](INDEX.md)

## Implementation guidance

- Begin with the repository packaging and test seam so `uv run pytest` can import the application without an external `PYTHONPATH` workaround.
- Expose publication, alias resolution, compilation, and immutable retrieval through application services or command/API contracts; keep canonicalization and validation pure beneath that seam.
- Use one deliberately generic StageGraph fixture and one generic GoalDirected fixture. They prove the two blueprint families without inventing a product Workflow Type.

## Verification approach

Exercise real strict models and disposable MongoDB persistence through publish → resolve → compile → retrieve. Add focused pure tests only for malformed structures and canonicalization.

## Explicit non-goals

- Run admission, lifecycle tables, Temporal scheduling, provider execution, and concrete workflow stages.
- Final storage thresholds, collection names, publication UX, or workflow-specific defaults.

## Acceptance criteria

- [ ] Strict, extra-forbidden versioned contracts cover Workflow Types, StageGraph and GoalDirected blueprints, control/runtime/workspace/evaluation profiles, and registered namespaced extensions.
- [ ] MongoDB/Beanie stores immutable published revisions separately from mutable authoring heads and aliases; alias movement and retirement affect future selection only.
- [ ] Publication rejects missing or ambiguous references, invalid structural variants, StageGraph cycles, undeclared output slots, and unsupported extensions.
- [ ] Compilation resolves aliases before a side-effect-free compiler binds exact revisions, digests, the Run Input Manifest, operation/workspace/evaluation policy, budgets, and linked-run slot constraints.
- [ ] Typed overlays record accepted, rejected, degraded, and omitted decisions and cannot weaken invariants, exceed caller or parent ceilings, or silently substitute assets.
- [ ] Canonical serialization is schema-versioned and deterministic; equivalent exact inputs under one compiler version produce byte-equivalent payloads and one digest.
- [ ] Compiled records contain secret references only and can be loaded and digest-verified without mutable catalog reads, network access, clocks, or runtime defaults.
- [ ] Large payload externalization remains content-addressed and exposes the same public configuration contract; generated schemas match server validation.

## Source basis

- Versioned Workflow Definitions and Effective Run Configuration
- Human Upgrade System Context

The source specifications and Human Upgrade System glossary were available locally when this ticket was authored. This ticket is intentionally self-contained for a fresh implementation agent.
