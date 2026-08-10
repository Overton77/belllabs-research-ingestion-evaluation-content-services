---
id: WP-CP-010
title: Implement immutable definitions and deterministic effective run configuration
status: draft
implements: [REQ-CP-DEF-001, REQ-CP-DEF-002, REQ-CP-DEF-003, REQ-CP-DEF-004, REQ-CP-DEF-005, REQ-CP-DEF-006, REQ-CP-DEF-007, REQ-CP-DEF-008, REQ-CP-DEF-009, REQ-CP-DEF-010]
governed_by: [ADR-0003, SPEC-CP-DEFINITIONS]
contracts: [CON-CP-DEFINITION-REF-V1, CON-CP-ERC-V1]
blocked_by: [WP-CP-001]
github_issue: null
evidence: [docs/migrations_instructions/evidence_v2/WP-CP-010/]
---

# Implement immutable definitions and deterministic effective run configuration

## Outcome

An application service publishes immutable Workflow Type/blueprint/profile revisions and compiles a byte-stable ERC that includes flattened Deep Agent and capability attachment inputs without mutable runtime lookup.

## Current implementation baseline

Existing graph-runtime models contain content-addressed refs, capability requirements, operation assemblies, and execution bindings across multiple versions. Reuse them only through the WP-CP-001 migration map.

## Requirements implemented

All `REQ-CP-DEF-*` requirements in `SPEC-CP-DEFINITIONS`.

## Architectural seams affected

Domain definitions, compiler/application service, MongoDB/Beanie repositories, API schemas, canonical serialization, object-store externalization, fixtures, and schema registry.

## Compatibility and migrations

Use expand-contract for stored schemas. Preserve historical reads, introduce new versioned definitions/bindings, migrate fixtures, and retire old selection paths only after digest and consumer compatibility evidence.

## Acceptance criteria

- [ ] Immutable revision lifecycle and retirement work through the application/API seam.
- [ ] StageGraph and GoalDirected fixtures compile to exact ERCs.
- [ ] Deep Agent authoring composition flattens deterministically.
- [ ] Required MCP/Skill/sandbox refs resolve exactly; failures/degradations are typed.
- [ ] Alias movement cannot alter a compiled ERC.
- [ ] Child runs compile independently under frozen parent ceilings.
- [ ] Secrets cannot serialize into any executable document.

## Qualification and evidence

Run `QUAL-CP-DETERMINISTIC-COMPILATION`; record schema versions, canonical bytes/digests, migration rehearsal, generated schemas, and commands.

## Failure and rollback posture

Keep old readers and stored revisions available until all consumers accept the new ERC/definition versions. Rollback selects the prior compiler for new requests; published new revisions remain historical.

## Documentation and traceability updates

Publish exact contract artifact paths and tests in `TRACEABILITY.md`.

## Non-goals

Run admission, Temporal scheduling, or live Deep Agent invocation.

## Drift guards

Tests prohibit database/network/clock reads in the pure compiler and prohibit OpenAI Agents SDK fields in new contracts.

