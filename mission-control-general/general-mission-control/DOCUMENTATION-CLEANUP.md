---
type: Specification
title: Documentation consolidation and deletion plan
description: "2026-10-02. No documents were deleted in this pass. The canonical architecture is SPECIFICATION.md, with DATABASE, RUNTIME-CONTRACTS and the local workflow suite as its detailed companions. Old files are marked…"
tags: [mission-control, spec, normative]
---
# Documentation consolidation and deletion plan

2026-10-02. No documents were deleted in this pass. The canonical architecture is [SPECIFICATION.md](SPECIFICATION.md), with DATABASE, RUNTIME-CONTRACTS and the local workflow suite as its detailed companions. Old files are marked historical so they cannot silently govern new work.

## Keep as the active set

Keep root `README.md` (entry point), optional root `index.md` (navigation only), all of `general-mission-control/`, and `workflow-types/00`–`09` plus their index. Keep historical qualification evidence in its owning runtime repository; it is regression provenance, not an alternative architecture.

The new review proposal is `general-mission-control/MISSION_CONTROL_SYSTEM_PROPOSAL.md`. The root file with the same name is the old proposal. The different locations are deliberate during review; delete/archive the old one only after comparing the replacement.

## First deletion batch after this pack is reviewed

These are superseded architecture/delivery/navigation documents. Their operational role has moved to the listed canonical destination; their old instructions must not be used to implement the system. Archive before deleting if you want the interview or planning history.

| Root file | Replacement | Deletion condition |
| --- | --- | --- |
| `MISSION_CONTROL_ARCHITECTURE.md` | SPECIFICATION + workflow annex | Review the consolidated architecture and workflow runtime changes |
| `MISSION_CONTROL_IMPLEMENTATION_SEQUENCE.md` | IMPLEMENTATION | Accept the new parity-first sequence and acceptance matrix |
| `M0_M8_PROGRAM.md` | IMPLEMENTATION | Old milestone IDs are historical only; no active work should derive from this queue |
| `EXECUTION_DIRECTIVE.md` | SPECIFICATION clean-break policy + IMPLEMENTATION | Its applicable policy is incorporated; its instruction to immediately code is obsolete for this documentation task |
| `EDITED_PATHS.md` | Current pack navigation and VALIDATION | Retain only if the old change log is useful history |
| `SPEC_VALIDATION.md` | General pack VALIDATION, historical evidence attribution | Its old checks certify neither current docs nor runtime; preserve externally if needed |

`index.md` is also removable if nothing consumes it; it adds no specification content beyond README.

## Preserve until the next targeted extraction/review

These files contain more detail or decision provenance than this consolidation has promoted. Their architecture is superseded, but deleting them now could lose material you may want to integrate next. They are historical inputs only.

| File | What the new pack already covers | Remaining review before deletion |
| --- | --- | --- |
| `MISSION_CONTROL_SYSTEM_PROPOSAL.md` (root) | Python naming, contract families, event plane, interfaces, subagents, human review, navigation and dashboard responsibilities | Detailed WebSocket frames, event-handler catalog, trace digest, deliverable lifecycle and search payloads; the new proposal intentionally labels product choices rather than accepting every old proposal detail |
| `MISSION_STORAGE_AND_RETRIEVAL_PROJECTION.md` | App-local accepted bucket layout, custody, exact inputs/context, canonical events, historical frontier and replaceable projections | Detailed narrative/as-of/search tables, retention, improvement/evaluation protocols; old four-bucket/shared-registry assumptions must not be copied back |
| `MISSION_CONTROL_SUPPLEMENT.md` | Provider-neutral execution, continuation, application generality, source/memory boundary | Prompt caching, source intelligence, memory manager and inter-agent communication details |
| `2026-09-19_CONTROL_PLANE_EXPERIENCE_AND_SANDBOX_SEED.md` | Read-only mission seed, artifact materialization and dashboard lineage | Intra-mission conversation UX and staged/generative UI requirements |
| `MISSION_CONTROL_SPEC.md` | Architecture, core identities, workflows, revisions, harness, storage and control surfaces | Older goal-family/domain product details, research/ingestion specifics and composition/discovery payloads |
| `MISSION_CONTROL_PRESPEC.md` | General system foundation and implementation direction | Personal-intelligence/course/RSI and other product-specific ideas not promoted into the general contract |
| `HANDOFF.md` | Accepted workflow behavior and new canonical navigation | Interview/session log and decision provenance; compare any claimed acceptance before discarding |
| `ACCEPTED_APPLICATION_AND_STORAGE_ARCHITECTURE.md` | App separation, Auth, PostgreSQL, private buckets, runtime binding, checkpoints, domain operations, capabilities and deployment baseline | Keep as the owner-approval record until you are satisfied its decision provenance is preserved; the canonical pack now governs implementation |
| `runtime-facts/CURSOR_SDK_FACTS.md` | Adapter qualification requirements; historical limitations cited in EVIDENCE | Retain historical SDK evidence until the Python Cloud adapter has a replacement qualification record; it is not current runtime authority |

`runtime-facts/EVE_FACTS.md` is removable after review if Eve research history is no longer wanted. The canonical spec and workflow contracts have no dependency on it. Older historical documents may still reference it; archive/delete those references together. Eve is excluded, not an unimplemented future lane.

## Decision reconciliation map

| Old position | Canonical result |
| --- | --- |
| TypeScript/Fastify/Zod runtime and Eve | Python/FastAPI/Pydantic; Deep Agents first, Agent Server required, Cursor SDK Cloud/frontier providers |
| Deep Agents and Cursor both required initially | Deep Agents + Agent Server in both apps first; Cursor Cloud follows |
| Nested Biotech package and compatibility shell | General `mission-control` backend, clean break; no Mongo/Beanie or old engine migration |
| Mandatory separate checkpoint database | Qualified private app-local runtime persistence first; physical separation only if required by evidence |
| One Temporal namespace per environment | App/environment isolation; separate production namespaces initially recommended |
| Single default `mission-control-artifacts` or old four-bucket proposal | `mission-artifacts`, `knowledge-artifacts`, `capability-bundles` in each project |
| Separate editable Biotech mission SQL | Same common component from `mission-control-db-contract` (amendment D09); `biotech-postgres-db-contract` is a thin consumer of its installer |
| External meta copy is the canonical specification | Local `general-mission-control/SPECIFICATION.md` and its local annexes |
| Knowledge Services generalization excluded | Shared contracts/domain boundary included; detailed service packaging follows concrete workflows |

## Procedure when deleting

Review each replacement or explicitly retire the unpromoted requirement. Check inbound links across the repositories that actually consume these documents, update them to the local canonical pack, then remove the selected old file. This pass checked the active local pack; it did not claim a workspace-wide consumer audit. Historical cross-links among deletion candidates are not reasons to keep them as architecture authorities.

Do not delete `workflow-types/`, the new proposal, or runtime proof reports as part of removing the old architecture. A document with unpromoted product ideas can be archived outside the active spec tree until its next review; archiving preserves ideas without creating a second canonical architecture.
