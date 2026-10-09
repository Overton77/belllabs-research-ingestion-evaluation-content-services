---
type: Specification Annex
title: Expanded implementation packet
description: "Normative design annex to SPECIFICATION, authored from the current local consolidation and the owner's expanded requirements. This packet is a specification and repo-local implementation backlog, not authorization for…"
tags: [mission-control, spec, expansion]
---
# Expanded implementation packet

Normative design annex to [SPECIFICATION](../SPECIFICATION.md), authored from the current local consolidation and the owner's expanded requirements. This packet is a specification and repo-local implementation backlog, not authorization for runtime changes, live probes or deployment. Requirements supersede older statements that these surfaces were deferred or optional. The existing clean-break, Python, Temporal, per-app Supabase decisions remain intact; common SQL ownership follows amendment D09 (`mission-control-db-contract`).

Read [architecture and ADRs](ARCHITECTURE-AND-ADRS.md), [context/state and intervention](CONTEXT-STATE-AND-CONTROL.md), [catalog/environment contracts](CATALOG-AND-ENVIRONMENTS.md), [knowledge services](KNOWLEDGE-SERVICES.md), [experience and streams](EXPERIENCE-AND-STREAMS.md), [deployment and release](DEPLOYMENT-AND-RELEASE.md), [worked scenarios](WORKED-SCENARIOS.md), [technology evidence](TECHNOLOGY-EVIDENCE.md), then [requirements and issues](REQUIREMENTS.md) and [team workspace](TEAM-WORKSPACE.md). [schemas.json](schemas.json) provides closed JSON Schema seeds for shared expansion contracts; issue contracts name the additional generated models required at their exit gate.

The workflow suite remains the owner of Stage Graph, Goal Loop, Parallel Swarm, Evaluator Optimizer, child missions and revision semantics. This annex makes context, services, interfaces and release requirements concrete without introducing another scheduler. The existing database/runtime specs remain authoritative for base tables and identity; this annex adds explicit records/fields and proof gates. In any conflict the current owner request and the scope/decisions recorded here govern; update the owning file rather than creating alternate interpretations.

Execution order is a dependency DAG, not calendar milestones. Foundational proofs precede enabling features. Deep Agents parity and Agent Server async qualification lead; Python Cursor Cloud and direct providers follow and are required for full release. Required release surfaces are skill + CLI, MCP, web, mobile, and secure generative UI/MCP Apps with tested fallback. Desktop is excluded from initial release. Knowledge-service integrations are required scope; product entity models remain app-owned.

Proof results are four distinct statuses: `existing_recorded`, `source_supported`, `proposed`, `qualified_new`. Only the last status can enable a newly generalized capability. There are no new runtime proof results from this documentation task. [Validation](VALIDATION.md) and [changed paths](CHANGED-PATHS.md) record the packet checks.
