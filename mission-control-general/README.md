# General Mission Control

Canonical documentation, consolidated 2026-10-02. Python + Temporal + **Deep Agents first**, required Agent Server async subagents for AI Engineer and Biotech, followed by Cursor SDK Cloud and frontier-provider execution. PostgreSQL owns Mission Control tables in each application's Supabase project; private Storage holds artifacts. Eve is excluded.

Read in order:

The [expanded implementation packet](general-mission-control/expansion/README.md) adds the current required architecture details, 26-requirement coverage, 44 repo-local issues, team workspace and validation. Read it with the canonical specification; it refines this single authority rather than creating a new service or alternate spec home.

1. [Canonical specification](general-mission-control/SPECIFICATION.md) — system authority, architecture and accepted application/storage responsibilities.
2. [New system proposal](general-mission-control/MISSION_CONTROL_SYSTEM_PROPOSAL.md) — naming, interfaces, event/review/navigation conventions and delivery decisions for review.
3. [Codebase and database organization](general-mission-control/CODEBASE-ORGANIZATION.md) — general Python runtime, common SQL component and `biotech-postgres-db-contract` installation/seeding package.
4. [Database contract](general-mission-control/DATABASE.md) and [runtime contracts](general-mission-control/RUNTIME-CONTRACTS.md).
5. [Workflow types](workflow-types/index.md) — required behavior annex, including all four workflow systems.
6. [Implementation and acceptance](general-mission-control/IMPLEMENTATION.md), [worked missions](general-mission-control/WORKED-MISSION.md), and [historical evidence](general-mission-control/EVIDENCE.md).
7. [Documentation cleanup plan](general-mission-control/DOCUMENTATION-CLEANUP.md) — what can be deleted after review and what still needs extraction.

This local pack is the canonical home. Older root documents and copies in other repositories are provenance, not parallel architecture authorities. No runtime rename, schema installation, data deletion or deployment is performed by this consolidation. Detailed organization/product choices remain proposals where labeled.
