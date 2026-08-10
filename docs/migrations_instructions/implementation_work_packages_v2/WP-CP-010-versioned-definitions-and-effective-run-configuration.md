---
id: WP-CP-010
title: Implement immutable definitions and deterministic effective run configuration
status: accepted
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

## Authorized implementation slice

- Replace canonical definition/ERC models in `app/domain/control_plane/contracts.py` and canonical
  bytes/digests in `app/domain/control_plane/canonical.py`.
- Replace the pure compiler in `app/domain/control_plane/compiler.py`.
- Replace publication/compilation use cases in `app/application/control_plane.py` and repository
  ports/adapters in `app/application/control_plane_repository.py`.
- Update `app/models/control_plane.py`, API DTOs, fixtures, and contract/unit/integration tests.
- Implement `CON-CP-DEFINITION-REF-V1` and `CON-CP-ERC-V1` exactly as described by the owning spec
  and `IMPLEMENTATION_READINESS.md`; no runtime adapter work belongs here.

## Replacement and persistence

Create the canonical versioned definition and ERC collections directly. Rebuild local fixtures
under the new schemas and remove old selection paths after the new compiler tests pass. No
historical reader, dual write, or prototype-data backfill is required.

## Acceptance criteria

- [x] Immutable revision lifecycle and retirement work through the application/API seam.
- [x] StageGraph and GoalDirected fixtures compile to exact ERCs.
- [x] Deep Agent authoring composition flattens deterministically.
- [x] Required MCP/Skill/sandbox refs resolve exactly; failures/degradations are typed.
- [x] Alias movement cannot alter a compiled ERC.
- [x] Child runs compile independently under frozen parent ceilings.
- [x] Secrets cannot serialize into any executable document.

## Qualification and evidence

Run `QUAL-CP-DETERMINISTIC-COMPILATION`; record schema versions, canonical bytes/digests, migration rehearsal, generated schemas, and commands.

## Failure and rollback posture

Rollback reverts this package before downstream admission is enabled. It does not restore the old
compiler as an alternate production path. Published test revisions may be discarded with the local
development database.

## Documentation and traceability updates

Publish exact contract artifact paths and tests in `TRACEABILITY.md`.

## Non-goals

Run admission, Temporal scheduling, or live Deep Agent invocation.

## Drift guards

Tests prohibit database/network/clock reads in the pure compiler and prohibit OpenAI Agents SDK fields in new contracts.
