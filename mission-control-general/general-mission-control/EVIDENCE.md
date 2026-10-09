---
type: Specification
title: "Sources, decisions and qualification gaps"
description: "Expansion evidence and new gaps are consolidated in TECHNOLOGY-EVIDENCE. Historical accepted tests below remain attributed results; they do not prove new two-app async, knowledge, UI/mobile or stream releases. This…"
tags: [mission-control, spec, normative]
---
# Sources, decisions and qualification gaps

Expansion evidence and new gaps are consolidated in [TECHNOLOGY-EVIDENCE](expansion/TECHNOLOGY-EVIDENCE.md). Historical accepted tests below remain attributed results; they do not prove new two-app async, knowledge, UI/mobile or stream releases. This packet records no new runtime/provider test result.

Canonical consolidation 3, 2026-10-02; retains the earlier inspection record. No runtime/proof tests were rerun for this spec task. Reported historical results below are attributed to their recorded evidence. General-system acceptance remains future work.

## Consolidated decisions

| ID | Decision and basis |
| --- | --- |
| D01 | Python `mission_control`; clean transformation/rename of the existing backend into `mission-control`, no nested permanent package or legacy compatibility engine |
| D02 | Same general service and contracts, app-bound workers/Agent Server pools and authenticated application installations |
| D03 | Identical common mission schema in each app's PostgreSQL; app-local private Storage and runtime persistence; entity stores remain domain-owned |
| D04 | Temporal + Deep Agents first, with required Agent Server async execution for both apps and a qualified frontier model route; Cursor SDK Cloud/direct-provider lanes follow; no Eve |
| D05 | Superseded by D09. Original: owner clarification retained common SQL in `ai-engineer-db-contract`; proposed `biotech-postgres-db-contract` installs/verifies and seeds Biotech's own installation |
| D06 | Retry, replay, fork, logical remediation and continuation remain distinct; new executions require durability, no old engine migration is required |
| D07 | Current task consolidates documents only; shared Knowledge Services boundary is in design scope; no runtime migration/provisioning or live probes performed |
| D08 | This local pack and workflow annex are canonical; external copies and older root proposals are historical inputs |
| D09 | Owner correction 2026-10-03: the independent `mission-control-db-contract` (in `mission-control/packages/`) owns common SQL, generated MC-only contract and the shared installer; `ai-engineer-db-contract` owns AI Engineer entity tables only; `biotech-postgres-db-contract` is a thin consumer; app manifests live in `mission-control/deployments/` |

## Historical evidence attribution

The source map and library observations below were inherited from revision 2. They are historical reports, not fresh source/vendor verification in this consolidation. Their test counts, installed versions and old paths must be rechecked at implementation time. Local link existence is checked separately; it does not certify the reported behavior.

## Inspected source map

Biotech paths resolve from [current backend](../../biotech-research-ingestion-evaluation-system/); AI Engineer paths from [workspace](../../../aiengineer/). Some accepted evidence uses old test locations; the actual current paths below account for test organization changes.

| Source | What it establishes / disposition |
| --- | --- |
| [CP-030 accepted evidence](../../biotech-research-ingestion-evaluation-system/docs/migrations_instructions/evidence_v2/WP-CP-030/README.md) | Recorded 2026-08-10: root/family/operation hierarchy, generation-bound message receipts, linked-run settlement, captured-history replay, Continue-As-New and semantic fork isolation. Historical report records 30-pass qualification and broader gates; not rerun here |
| [CP-040 accepted evidence](../../biotech-research-ingestion-evaluation-system/docs/migrations_instructions/evidence_v2/WP-CP-040/README.md) | Real Temporal + model + MCP + skill + sandbox materialization; bounded cognitive execution. Live checkpointer/store were in memory. Executable sandbox framework filesystem permissions limitation is explicit |
| [BP-010 accepted evidence](../../biotech-research-ingestion-evaluation-system/docs/migrations_instructions/evidence_v2/WP-BP-010/README.md) | Recorded 2026-08-11, audited head `8a05094509ad015422a62c0f9cd534fc21182447`, Temporal 1.30.0/Deep Agents 0.7.5. `all/any/minimum` joins, liabilities, cycles, retries, cancellation, replay and worker loss; records 126 passed/1 Windows skip, separate WSL 7 passed |
| BP-010 live evidence, same file | FastAPI -> Run Control -> root -> Stage Graph -> operation children -> real Deep Agents/model. Downstream child started at Temporal history index 48 before slow-child completion 63. Records 1 live test passed. This proves incremental release under a controlled gate, not a latency benchmark |
| [Runtime recovery service](../../biotech-research-ingestion-evaluation-system/app/application/runtime/runtime_recovery.py) | Recovery policy, fork admission/checkpoint-copy saga, cancellation settlement. Concrete implementation and unit seams; checkpoint-copy client is a protocol, not a fully wired new production API |
| [Runtime recovery tests](../../biotech-research-ingestion-evaluation-system/tests/unit/runtime/test_runtime_recovery_stage3.py) | Retry/fork/replay policy and uncertain admission/copy reconciliation fixtures; test presence inspected, not rerun |
| [StageGraph Temporal tests](../../biotech-research-ingestion-evaluation-system/tests/integration/temporal/test_wp_bp_010_temporal.py) and [recovery tests](../../biotech-research-ingestion-evaluation-system/tests/integration/temporal/test_wp_bp_010_recovery.py) | Current paths for replay/wait/worker-loss seams cited by accepted BP-010 evidence |
| [Runtime kernel persistence](../../biotech-research-ingestion-evaluation-system/app/application/runtime/postgres_stage3_kernel_repository.py) | Persisted fork/recovery claims; candidate for app-local common-schema repository transformation |
| [Deep Agents adapter](../../biotech-research-ingestion-evaluation-system/app/integrations/agents/deep_agents/adapter.py) and [materializer](../../biotech-research-ingestion-evaluation-system/app/integrations/agents/deep_agents/materializer.py) | Exact binding, skill/MCP and bounded harness reuse; not a GENERAL portability proof |
| [Older StageGraph experiment](../../biotech-research-ingestion-evaluation-system/app/experiments/langgraph_temporal_stagegraph/README.md) | 2026-08-08 report supplies checkpoint/outbox/wake provenance. Its limitations are not limitations of later accepted BP runtime; do not import experiment implementation into production |
| [AI Engineer workflow semantics](../workflow-types/index.md) | Four recursive systems, revisions, lifecycle/phase/outcome and acceptance target. Normative design semantics; not claims of current kernel implementation |
| [AI Engineer legacy contracts](../../../aiengineer/ai-engineer-mission-control/packages/mission-contracts/src/index.ts) | Current legacy states differ from target three-field contract; historical comparison only; no old runtime compatibility requirement |
| [AI Engineer compiler](../../../aiengineer/ai-engineer-mission-control/packages/mission-compiler/src/index.ts), [CLI](../../../aiengineer/ai-engineer-mission-control/packages/missionctl/src/index.ts), [verification workflow](../../../aiengineer/ai-engineer-mission-control/apps/worker/src/verification-workflow.ts) | Partial dependency compiler, status-only CLI, single verification activity workflow; no complete GENERAL four-system/lifecycle proof |
| [Cursor runtime facts](../runtime-facts/CURSOR_SDK_FACTS.md), Biotech `cursorloops/package.json` | Historical TypeScript SDK facts and local runner. These do not pin/qualify the new Python adapter |

## Library verification

Inspected Biotech `pyproject.toml`, `uv.lock` and installed `.venv/Lib/site-packages/*dist-info/METADATA`: Deep Agents 0.7.5, Temporal 1.30.0, LangGraph 1.2.10, PostgreSQL checkpoint package 3.1.1, Pydantic 2.13.4. Existing dependency ranges are not a standalone GENERAL lock; extraction must publish an exact compatible lock. Older experimental capture used Deep Agents 0.7.4; accepted BP-010 records 0.7.5.

Installed `temporalio/common.py` verifies maximum_attempts=0 means unlimited, and workflow reuse/conflict enums exist. `temporalio/workflow/_workflow_ops.py` verifies all_handlers_finished and continue_as_new. `deepagents/graph.py` verifies create_deep_agent accepts checkpointer/store. `langgraph/pregel/main.py` verifies async state/history/update methods. `langgraph/checkpoint/postgres/aio.py` verifies setup and the helper connection configuration. Checkpoint setup is deployment work, never an implicit model tool.

The inherited revision-2 record reports official documentation checks on 2026-10-02 (not repeated in this consolidation): [Temporal Python Continue-As-New](https://docs.temporal.io/develop/python/workflows/continue-as-new), [Deep Agents sandboxes](https://docs.langchain.com/oss/python/deepagents/sandboxes), [Cursor Python SDK](https://cursor.com/docs/sdk/python), [Cursor Cloud API](https://cursor.com/docs/cloud-agent/api/endpoints), [Supabase RLS](https://supabase.com/docs/guides/database/postgres/row-level-security), [MCP authorization](https://modelcontextprotocol.io/specification/2026-07-28/basic/authorization). These establish published library/protocol facilities; actual organization permissions, network access and pinned adapter behavior still require qualification. No SDK fork/pause method is invented in this pack.

## Proof gaps and implementation dependencies

- GENERAL API/CLI/skill, common neutral package and both-app deployment do not yet exist as qualified surfaces.
- Fork saga/provider copy wiring, durable materializer checkpoint store, canonical checkpoint hydration and real pause/resume require new end-to-end proofs. Existing semantic fork and worker recovery evidence must remain regression anchors.
- Cursor Python version/bridge packaging, lost-launch reconciliation, usage, cancellation, checkpoint hydration and workspace custody need conformance qualification. Native pause/fork are not assumed.
- Same schema/migration checksums/fingerprint in the two Supabase projects, scoped RLS roles, project identity checks and cross-app spoofing tests remain release gates.
- The four-system semantic target is broader than the inspected AI Engineer implementation; recursive Swarm/Optimizer/revision/child-mission completion must be built and proved, not inferred from documentation.
- Actual Supabase project refs, operator grants, domain endpoints, LangSmith/network profiles, finite probe budgets and deployment identities are installation inputs. Their absence does not block completing this specification and must not be filled with guessed credentials.
- Temporal transport TLS/history encryption, namespace access and queue/worker isolation are production prerequisites, not established by CP-040 local evidence.

## Historical review discipline (revision-2 record)

Root/destination AGENTS and relevant local skill/rules were inspected. The codebase-design skill informed interface seams; the to-spec publication workflow was inspected but not invoked because this task authorizes local spec edits only. No Codex memory summary directory was found at the inspected default location; session archives/private stores were not mined. Existing meta files are untracked in the inspected repository; retain their original content and apply bounded edits. Originals are hashed immediately before writes; a concurrent hash change aborts the batch. No generated AGENTS blocks, runtime source, credentials or memories are edited.

## This consolidation

Updated canonical architecture, storage/runtime companions, workflow runtime bindings, implementation sequence and local navigation; added the new proposal, code/database organization and deletion guidance. No qualification tests, vendor documentation refresh or deployment account inspection were performed. Documentation checks are recorded in VALIDATION.md.
