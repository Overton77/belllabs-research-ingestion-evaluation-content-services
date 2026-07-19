# 11 — Build deterministic Schema Catalogs and governed schema selections

**What to build:** Build reusable, content-addressed navigation resources from one authoritative Neo4j GraphQL directive SDL and govern semantic selection separately from deterministic expansion and purpose-specific projection.

**Blocked by:**
- 01 — Implement versioned Workflow Definitions and Effective Run Configuration compilation
- 08 — Implement transactional durable artifact promotion

**Status:** ready-for-agent

**GitHub:** https://github.com/Overton77/belllabs-research-ingestion-evaluation-content-services/issues/11


**Read first:** [Local control-plane ticket index](INDEX.md)

## Implementation guidance

- Consume one authoritative Neo4j GraphQL directive SDL identity. If modular authoring sources remain, make their generation/verification a deterministic pre-publication gate.
- Separate reusable deterministic catalog generation from semantic Schema Context Selection, independent review, deterministic closure, and purpose-specific projection.
- Persist queryable build/selection metadata separately from immutable large bundles in object storage.

## Verification approach

Use golden builds and malformed-schema fixtures to verify deterministic digests and lineage, then exercise selection review, closure, and cross-purpose admission through public services.

## Explicit non-goals

- Authoring the canonical graph schema, live introspection as identity, the full standalone Schema Context Selection Workflow, and retrieval-weight tuning.
- Granting graph access or mutation through schema resources.

## Acceptance criteria

- [ ] A deterministic build binds exact Schema Definition hash, generator version, governed module definitions, normalization, resource manifest, object digest, and decision.
- [ ] The catalog provides compact global/module/topology indexes, Compact Schema Overview, cards, drill-down resources, parsed artifacts, query patterns, and retrieval metadata.
- [ ] Every resource proves lineage to the exact directive SDL; malformed input, unresolved references, duplicate identities, or inconsistent closure fail publication.
- [ ] Identical canonical inputs produce one logical digest independent of time and storage; meaningful source/module/generator changes produce a successor.
- [ ] Agent-produced Schema Context Selections require structural validation and independent semantic coverage review before acceptance.
- [ ] Deterministic expansion adds endpoint, enum, union, directive, property, and relationship-property closure without adding semantic membership.
- [ ] Schema Operation Projections are purpose-bound and require admission before reuse for another purpose.
- [ ] Catalog resources provide context only and cannot grant graph access, mutation authority, approval, or semantic truth.

## Source basis

- Reusable Schema Catalog, Deployment Manifest, and Schema Workspace Materialization
- Human Upgrade System Context

The source specifications and Human Upgrade System glossary were available locally when this ticket was authored. This ticket is intentionally self-contained for a fresh implementation agent.
