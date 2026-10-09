---
type: Specification
title: General Mission Control system proposal
description: "Review proposal, 2026-10-02. This replaces the older system proposal as the review document. SPECIFICATION.md is the canonical architecture; DATABASE.md owns persistence contracts; workflow-types owns detailed behavior.…"
tags: [mission-control, spec, normative]
---
# General Mission Control system proposal

Review proposal, 2026-10-02. This replaces the older system proposal as the review document. [SPECIFICATION.md](SPECIFICATION.md) is the canonical architecture; [DATABASE.md](DATABASE.md) owns persistence contracts; [workflow-types](../workflow-types/index.md) owns detailed behavior. Proposed product surfaces below do not become implemented or approved merely by appearing here.

**Current expansion authority:** [expansion/README.md](expansion/README.md) makes the owner's required catalog, knowledge integration, context/state controls, WebSocket, mobile, generative UI and standard MCP Apps/MCP-UI surfaces concrete. Earlier wording below that treats these as optional or detailed design for later is superseded for complete-release scope. Deep Agents-first sequencing, clean transformation and app-local storage remain current. Local issues/ADRs and acceptance gates govern planning; no runtime capability is certified.

## A. System shape and accepted decisions

Mission Control is one general Python system serving AI Engineer and Biotech through authenticated application installations. Temporal owns durable mission orchestration. Deep Agents is the first execution priority. A required Agent Server hosts bounded asynchronous subagent execution for both applications. Cursor SDK Cloud and frontier provider integrations extend the same admitted execution contracts. Eve is excluded.

The existing Python backend becomes `mission-control`, with the import identity `mission_control`. There is one kernel. The clean break requires no Mongo/Beanie dependency, legacy data migration, API aliases or old engine history support. Reuse the working Temporal/Deep Agents implementation and behavioral evidence, then prove the generalized contracts anew. This documentation task performs no physical rename or code migration.

Deep Agents uses a pinned frontier model route; model/provider support is therefore part of the first delivery, not something postponed until Cursor. A standalone bounded direct-provider executor is a subsequent lane. Provider selection, agent harness and workspace are different dimensions. Neither a model API nor Agent Server replaces Temporal.

| Responsibility | AI Engineer | Biotech |
| --- | --- | --- |
| Application ID | `ai-engineer` | `biotech` |
| Recorded Supabase project label | `supabase-blue-ocean` | `biotech-research-ingestion` |
| Users and application membership | Project-local Auth and membership tables | Project-local Auth and membership tables |
| Mission ledger | Project-local PostgreSQL `mission_control` schema | Identical common schema in its own PostgreSQL |
| Artifact bytes | Private project-local Storage | Private project-local Storage |
| Runtime recovery | Private app-local runtime persistence, qualified with the pinned Agent Server/checkpointer | Same ownership boundary |
| Knowledge entities | App-owned PostgreSQL domain schemas | App-owned Neo4j Aura |
| Deterministic knowledge operations | Governed SQL capability | Governed Cypher/graph capability |

Project labels are recorded configuration names, not verified account IDs or deployment evidence. An operator binding supplies actual identity and secrets server-side. AWS services/workers, Temporal Cloud, and qualified LangSmith tracing/sandboxes remain the deployment baseline. Per-app/environment production Temporal namespaces are the starting recommendation; queues are routing, not security boundaries.

## B. Naming and identity conventions

| Surface | Convention | Example |
| --- | --- | --- |
| Repository/distribution | kebab-case | `mission-control` |
| Python package, module, function, wire and SQL field | lower_snake_case | `mission_control`, `start_run`, `activation_id` |
| Python value/model type | PascalCase | `MissionRun`, `AgentExecutorBinding` |
| Versioned wire schema | `mc.<contract>.v<major>` | `mc.run_start.v1` |
| PostgreSQL | singular tables in `mission_control` | `mission_control.mission_run` |
| HTTP | app prefix plus resource/action grammar | `/v1/applications/biotech/runs/{run_id}/commands` |
| CLI | noun then verb | `missionctl --application biotech run inspect <id>` |
| MCP tool | `mission_<resource>_<verb>` | `mission_run_inspect`, `mission_command_send` |
| Event | subject plus recorded fact | `run.started`, `human_task.resolved` |
| Temporal registration | stable versioned name | `mc.mission_run.v1` |
| Temporal workflow ID | scoped semantic identity | `mc/{installation_id}/{application_id}/run/{run_id}` |
| Task queue | role plus application, within bound namespace | `mc-control-biotech`, `mc-agent-biotech` |
| Resource URI | app-scoped resource path | `mc://applications/biotech/missions/{mission_id}` |

Exported resource references carry installation and tenant scope in the typed envelope; a URI is resolved only against an authenticated installation. UUIDs alone grant no access. Native provider run/thread/request IDs remain strings in adapter mappings and never replace Mission Run or Attempt identities. UUIDv7 generation and canonical digest rules are specified in RUNTIME-CONTRACTS.

Mission, Mission Revision, Mission Run, Activation, Attempt, Harness Execution, Agent Session and Session Turn remain distinct. `stage_graph` and `goal_loop` are canonical behavior names; `StageGraph` and `GoalLoop` are Python types. Lifecycle, phase and terminal outcome are separate fields. A successful provider run supplies an execution result, not an accepted mission.

The operation catalog defines schemas, grants, idempotency, error codes and availability once. Generate OpenAPI, JSON Schema and TypeScript client artifacts from Python contracts. SQL constraints and golden fixtures must agree with the same vocabulary. Do not mechanically convert arbitrary camelCase vendor payloads; adapters translate them explicitly.

## C. Code and database organization

Use the complete layout and dependency rules in [CODEBASE-ORGANIZATION.md](CODEBASE-ORGANIZATION.md). The core division is contracts → domain rules → application handlers/ports, with PostgreSQL, Temporal, Deep Agents, Agent Server, Cursor Cloud, frontier provider and storage adapters wired at bootstrap. HTTP, MCP and CLI expose the same handlers.

Common Mission Control SQL is the separately released `mission-control-db-contract` component (`mission-control/packages/mission-control-db-contract/`, ownership amendment 2026-10-03). `biotech-postgres-db-contract` is a thin consumer of its installer; it does not author competing SQL. AI Engineer installs the same component; `ai-engineer-db-contract` keeps only AI Engineer entity tables.

Keep the general service independently buildable. No sibling checkout imports, Biotech-only bootstrap or domain database credentials in the kernel. Application templates, rubrics, grants, tools and endpoint bindings explain application differences. The general scheduler does not branch on SQL versus Neo4j.

## D. Contract and workflow catalog

| Contract family | Contents | Owner |
| --- | --- | --- |
| Authored intent | Mission definition, goals, objectives, criteria, typed inputs/outputs, program | SPECIFICATION and workflow 00 |
| Composition | Stage Graph, Goal Loop, Parallel Swarm, Evaluator Optimizer | Workflows 01–04 |
| Atomic work and waiting | Agent/Deterministic Executor, Human Gate, Proof Gate, Timer, Event Wait | Workflow 05 |
| Mission boundaries | Child Mission Invocation, invocation grants, relationships and projected outputs | Workflow 06 |
| Change | Draft, immutable revision, proposal, impact and carry-forward decisions | Workflow 07 |
| Recovery | Checkpoint, continuation, context health, technical retry and fork | Workflow 08 and RUNTIME-CONTRACTS |
| Activity and control | Events, commands, delivery reports, native observations, cursor recovery | Workflow 09 and RUNTIME-CONTRACTS |
| Execution binding | Runtime, provider, workspace, capabilities, skill bundles, policy and budgets | SPECIFICATION and RUNTIME-CONTRACTS |
| Persistence | Scoped records, constraints, receipts, outbox/inbox and transactions | DATABASE |

All four workflow systems will be implemented. Stage Graph and Goal Loop lead delivery; Swarm and Evaluator Optimizer are required subsequent work. Durable controls, child missions, revision evolution and continuation are also part of the complete system. An implementation advertises each behavior as available, unavailable or gated with reasons. A schema accepting a behavior does not justify executing it before the behavior is qualified.

## E. Execution lanes and Agent Server

| Lane | Execution responsibility | Qualification |
| --- | --- | --- |
| Deep Agents | Exact graph/model/tool/skill/middleware assembly; bounded cognitive execution with qualified workspace | First priority, in both applications |
| Agent Server | Host registered graphs and async children; retain native execution identity; observe, cancel and recover | Required with the first Deep Agents milestone, in both applications |
| Cursor SDK Cloud | Coding work in provider-owned isolated repository workspace | Subsequent harness conformance and coding vertical |
| Frontier providers | Model routes for Deep Agents; later bounded direct model calls behind common accounting/receipt contracts | One pinned route first; qualify each additional profile |

Async launch transactionally admits a `subordinate_execution`, stable launch identity, parent/generation, dependency policy, grants and budget reservation before dispatch. An Agent Server adapter submits the graph with an idempotency/recovery identity and persists native handles. If a launch response is lost, recover that identity; do not launch another paid child. If the selected server cannot meet this contract natively, an admission wrapper must provide it before enabling the lane.

Use app-bound Agent Server deployments/pools initially, pinned by application binding. Authenticate calls, isolate runtime persistence and enforce narrower child grants. Server-side credentials never enter agent-readable files. Parent cancellation requests child cancellation and reconciles effects/usage. Required children block completion until settled. A late child result cannot mutate a terminal or stale-generation parent. Agent Server completion is an observation that the kernel admits, never independent acceptance.

Native framework fan-out must be admitted and bounded before launch. Observation-only hooks cannot enforce a budget or permission ceiling retroactively. Disable unmanaged fan-out where it would bypass grants. Sync subagents, async subordinates, nested workflow nodes and independently governed Child Missions are distinct constructs.

Harness `describe` reports native/emulated/unsupported/unqualified controls. Do not assume Cursor or frontier APIs have pause, fork, checkpoint or idempotent create. Frontier direct calls without a durable conversation can support an explicit single-turn profile; unsupported session controls reject. Provider failover is a newly authorized action unless equivalence is explicitly pinned; uncertainty about a billed request requires reconciliation first.

## F. Storage, events and retrieval

PostgreSQL owns all authoritative structured Mission Control state: definitions, revisions, bindings, executions, acceptance, human decisions, commands, budgets, effects and receipts. Versioned JSONB carries typed structured detail. Temporal retains execution history; Agent Server/checkpointers retain runtime recovery; LangSmith retains diagnostics. None of the latter authorizes a mission transition.

Each application's private buckets use the accepted logical names:

| Bucket | Content |
| --- | --- |
| `mission-artifacts` | Inputs/outputs, intent/query files, reports, manifests, continuation packages and durable workspace snapshots |
| `knowledge-artifacts` | Source captures, extracted representations, ingestion inputs and evidence documents |
| `capability-bundles` | Full immutable skills, plugins/tools, hooks and supporting assets |

An artifact has one authoritative byte location and scoped immutable identity/digest. Cross-consumer reuse uses references. Upload, verification and database registration are a recoverable protocol, not one transaction. Signed URLs are temporary access. Backup must cover object bytes and database state together.

Canonical events describe committed business facts; native observations are noncanonical execution evidence. Store bounded native observations with source identity/cursor and generation; seal permitted transcript/tool content into authorized artifacts when retention requires it. Do not store credentials or require hidden model reasoning. A trace event cannot directly complete an activation.

Write aggregate transition, event sequence, receipt and outbox together. Relays deliver at least once; consumers deduplicate. SSE serves canonical events from the durable ledger with per-mission sequence cursors, retention floors and explicit resync. MCP uses bounded cursor reads. Notifications and optional database wakeups accelerate delivery but are never recovery authority. Multiplexed WebSockets remain an optional product extension, not an initial requirement.

Retrieval must distinguish available artifacts, selected inputs, materialized files, submitted model context and observed access. Freeze downstream selection by producer activation/output/digest rather than “latest.” Historical reconstruction ends at a complete ledger commit; later corrections remain later facts. Derived search indexes, narrative views and state snapshots are replaceable and retain their source frontier. A generated narrative cannot rewrite acceptance or imply causality from timestamp order.

## G. Coordinator, skill, CLI and MCP

The coordinator discovers the authenticated application catalog, discusses goals and constraints, authors a draft, validates without effects, proposes/commits a revision, starts an authorized run, observes work and intervenes through commands or attributed Human Task resolutions. The canonical skill teaches this same sequence for both applications.

| Need | CLI | MCP |
| --- | --- | --- |
| Discover system/capabilities | `system describe`, `capability list` | `mission_describe_system`, `mission_capabilities_list` |
| Select bounded context | `context select` | `mission_context_select` |
| Author and validate | `mission create`, `mission validate` | `mission_create`, `mission_validate` |
| Resolve revision | `proposal resolve`, `revision activate` | `mission_proposal_resolve`, `mission_revision_activate` |
| Start and inspect | `run start`, `run inspect` | `mission_run_start`, `mission_run_inspect` |
| Observe/intervene | `events watch`, `command send`, `command get` | `mission_events_read`, `mission_command_send`, `mission_command_get` |
| Human review | `human-task resolve` | `mission_human_task_resolve` |
| Recover an unknown response | `request get` | `mission_request_get` |

Exact paths/scopes/error contracts are in SPECIFICATION and RUNTIME-CONTRACTS. Agent Server reporting uses attempt/generation-scoped `execution.report`, never `mission.read` as write permission. Agents submit completion candidates; the kernel computes acceptance. Auth tokens are bound to their resource/application, not forwarded arbitrarily between services.

## H. Deliverable review and navigable context — proposed product conventions

Represent a deliverable initially as a typed artifact manifest plus completion contract. A review packet binds exact output digests, rubric, assessments, unresolved findings and requested decision. A Human Gate records reviewer, decision, version and policy. Changing the reviewed bytes invalidates reuse of the prior approval. Approval of a report does not authorize knowledge ingestion, merge or deployment; those are separately declared domain capabilities.

Use the existing Human Task mechanism for approval, questions, selections and review. Rejection can request a bounded authored remediation/revision. Do not add a second review state machine or automatically publish on approval. A separate deliverable aggregate/table is deferred until its additional lifecycle is justified.

MissionFS is a proposed read-only navigational projection over authorized schemas, templates, programs, runs, artifacts and Human Tasks. It is not a filesystem database or an alternate mutation API. The initial sandbox `.mission/` seed contains a small goal/Operating Contract summary, immutable input manifest and context references; writable outputs/scratch are distinct. Search and context packs enforce the same grants before returning content and record selection budgets, omissions and digests. Full search UI and MissionFS browsing can follow the working runtime.

## I. Dashboards and shared Knowledge Services boundary

Both dashboards consume the generated client. Required views show missions/revisions, program topology, activation/attempt lineage, blockers, accepted versus merely produced outputs, Human Tasks, commands/delivery reports, budgets and recovery state. Add canonical event tails and explicitly labeled native diagnostics. Connection loss shows a stale view until replay/resync completes. A unified operator view queries each authorized installation and keeps its label; it does not require a third mission database.

Shared Knowledge Services design covers source identity/version/snapshot/locator, artifact custody, provenance, evidence tracking, retrieval envelopes, governed intent execution, operation receipts and memory policy. Domain entity models, schema navigation, SQL/Cypher constraints, evidence interpretation and writes remain application-specific. Detailed service packaging follows concrete workflows; this proposal does not decide that all Knowledge Services must be one service.

Intent execution pins artifact/input digest, executor and target schema version, authority, preconditions and effect identity. Uncertain domain writes recover durable receipts before repeat. PostgreSQL mission state and Neo4j writes do not share a transaction. Runtime checkpoint, operational memory, continuation package and admitted domain knowledge have different retention and authority; recalled memory is advisory.

## J. Delivery and review decisions

1. Generalize the existing Python runtime and PostgreSQL persistence; preserve the demonstrated Stage Graph/Goal Loop/Deep Agents behaviors and required Agent Server async support for both apps.
2. Close public authoring/control interfaces and recovery/isolation proofs. First-stage provider support includes the model route used by Deep Agents.
3. Add qualified Cursor SDK Cloud and bounded direct frontier-provider profiles. Complete Parallel Swarm, Evaluator Optimizer, Child Mission Invocation and advanced revision/continuation behavior.
4. Qualify production bindings, both dashboards and domain integrations. Extend search/navigation/review presentation and Knowledge Services design from concrete use cases.

The exact acceptance matrix is in [IMPLEMENTATION.md](IMPLEMENTATION.md). Documentation, local proof and deployed qualification are separate statuses.

| Decision | Status |
| --- | --- |
| Python, Temporal, Deep Agents first; Agent Server for both apps; Cursor Cloud and frontier providers; no Eve | Owner-directed architecture, consolidated into SPECIFICATION |
| App-local PostgreSQL, Auth, buckets, runtime binding and domain separation | Accepted storage/application decisions, consolidated |
| Clean transformation into `mission-control`; no old engine compatibility or Mongo data migration | Retained owner-directed implementation policy |
| Concrete modules/process roles and `biotech-postgres-db-contract` installer name | Proposal for review; common SQL ownership moved to `mission-control-db-contract` by amendment D09 |
| SSE first; optional WebSocket multiplexing | Proposal consistent with canonical recoverable event interface |
| Deliverable as manifest + Human Gate; read-only MissionFS/search projection | Product proposal; not a new accepted persistence contract |
| Detailed shared Knowledge Services packaging | Subsequent design decision; common boundary is already specified |

Review the code/database organization and product conventions without reopening the established runtime/storage choices. The [documentation cleanup plan](DOCUMENTATION-CLEANUP.md) identifies superseded files and preserves unsynthesized detail before deletion.

Application seeding follows the owner-confirmed pattern in [CODEBASE-ORGANIZATION.md](CODEBASE-ORGANIZATION.md): identical common tables, separate versioned/idempotent application seed bundles, and no independent Biotech copy of common SQL.
