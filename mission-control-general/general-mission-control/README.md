---
type: Specification
title: General Mission Control
description: "Canonical documentation, consolidated 2026-10-02. Python + Temporal + Deep Agents first, required Agent Server async subagents for AI Engineer and Biotech, followed by Cursor SDK Cloud and frontier-provider execution.…"
tags: [mission-control, spec, normative]
---
# General Mission Control

Canonical documentation, consolidated 2026-10-02. Python + Temporal + **Deep Agents first**, required Agent Server async subagents for AI Engineer and Biotech, followed by Cursor SDK Cloud and frontier-provider execution. PostgreSQL owns Mission Control tables in each application's Supabase project; private Storage holds artifacts. Eve is excluded.

Read in order:

Start with the new [expanded architecture/specification/issue packet](expansion/README.md) alongside the canonical specification. It includes the full local dependency DAG and user-requirement coverage, context/handoff and knowledge contracts, UI/stream/mobile requirements, compute guidance and release gates. The older list below remains the base reading map.

1. [Canonical specification](SPECIFICATION.md) — system authority, architecture and accepted application/storage responsibilities.
2. [New system proposal](MISSION_CONTROL_SYSTEM_PROPOSAL.md) — naming, interfaces, event/review/navigation conventions and delivery decisions for review.
3. [Codebase and database organization](CODEBASE-ORGANIZATION.md) — general Python runtime, common SQL component and `biotech-postgres-db-contract` installation/seeding package.
4. [Database contract](DATABASE.md) and [runtime contracts](RUNTIME-CONTRACTS.md).
5. [Workflow types](../workflow-types/index.md) — required behavior annex, including all four workflow systems.
6. [Implementation and acceptance](IMPLEMENTATION.md), [worked missions](WORKED-MISSION.md), and [historical evidence](EVIDENCE.md).
7. [Documentation cleanup plan](DOCUMENTATION-CLEANUP.md) — what can be deleted after review and what still needs extraction.

This local pack is the canonical home. Older root documents and copies in other repositories are provenance, not parallel architecture authorities. No runtime rename, schema installation, data deletion or deployment is performed by this consolidation. Detailed organization/product choices remain proposals where labeled.
