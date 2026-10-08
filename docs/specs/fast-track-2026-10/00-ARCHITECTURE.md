---
type: Specification
title: "Fast-track architecture: capabilities, context transfer, mission state, chains, manifest, interventions and the Cursor lane"
description: "The design synthesis for the 2026-10 fast track: what is added to the kernel, which contracts, tables, modules and interfaces change, how the lanes compare, the end-to-end flows for the three owner missions, and the ticket plan the specifications elaborate. Normative for this packet; the spec pack and docs/adr take precedence on conflict."
tags: [mission-control, spec, fast-track, architecture]
---

# Fast-track architecture

Owner request (interview 2026-10-07, accepted recommendations): make Mission Control ready to run three missions end to end, through a YAML Mission Manifest, with searchable and composable agent capabilities, real context transfer, persisted mission state, chained workflows, real interventions and a Cursor lane for local and cloud placement. Decisions are in ADR-0023 to ADR-0034; language is in `GLOSSARY.md`; the codebase facts are in [research/codebase-map.md](research/codebase-map.md). This document is the map the eight specifications ([SPEC-01](SPEC-01-capabilities-catalog.md) to [SPEC-08](SPEC-08-agent-skills.md)) elaborate, and the [team workspace](TEAM-WORKSPACE.md) is how the agent team executes it.

## 1. The three missions this packet must make runnable

| # | Mission | Lane profile | Workflow shape | Proves |
| --- | --- | --- | --- | --- |
| 1 | Research and ingestion for Biotech (literature sweep on a research question, then ingestion into the knowledge graph through the Biotech domain capability) | `deep_agents` | Stage Graph `collect → synthesize → review (human gate) → ingest`, where `collect` is a nested Goal Loop | packet handoff between stages and iterations, seeded MCP servers (PubMed, Tavily, Firecrawl), artifacts materialized into the sandbox, human gate, transcript |
| 2 | Research and ingestion with Cursor Cloud (same goals, the collecting and synthesizing work done by a Cursor Cloud agent in a repository workspace) | `cursor_cloud` | Mission Chain of two Goal Loops: `research` supplies `ingestion` | cursor cloud lane, chain link state transfer, SSE frame persistence, usage settlement, subscriptions |
| 3 | Codebase feature with local Cursor (a feature issue in a repository on the worker, delivered as a branch and a test report) | `cursor_local` | Goal Loop with `agent-browser` skill, repo hooks, a `verifier` subagent, `test_run` and `git_snapshot` deterministic executors | cursor local lane, workspace projections, kernel hooks fail-closed, queue instruction, interrupt, fork |

Each mission ships as a manifest under [missions/](missions/) and is the acceptance fixture of its final ticket.

## 2. What is added, in one picture

```text
                 Mission Manifest (mission.yml)  ──compile──▶  MissionDefinition@1 + Compiled Program + Validation Report
                        │ search/pin                                        │ submit (revision + run or chain admission)
                        ▼                                                   ▼
   ┌──────────── Catalog ────────────┐                     ┌──────────── Ledger (mission_control) ────────────┐
   │ skill_bundle  mcp_server        │   host projection    │ mission_chain / chain_link   command mailbox     │
   │ hook_script   subagent_profile  │ ───────────────────▶ │ provider_frame (Native Event Store)              │
   │ plugin        model/sandbox     │   per lane profile   │ mission_subscription   stop_fence   delivery_rpt │
   │ hybrid search projection        │                      └───────────┬──────────────────────────────────────┘
   └─────────────────────────────────┘                                  │ outbox
                                                                        ▼
                      Temporal: mission root ─ family (StageGraph | GoalDirected) ─ operation ─ lane.turn activity
                                                                        │
                       ┌────────────────────────── AgentHarness ────────┴──────────────────────────┐
                       │ deep_agents (LangGraph)      cursor_local (bridge)      cursor_cloud (API) │
                       │ prepare/start/reattach/send_turn/cancel_turn/observe/snapshot/usage/end   │
                       └───────────────────────────────────────────────────────────────────────────┘
                                 ▲ Context Packet in                      frames out ▼
                      .mission/ · /inputs · AGENTS.md · .cursor/* · hooks        provider_frame → reducer → mission events
```

Everything the agent reads enters through a Context Packet materialized into its workspace; everything the agent did leaves as provider frames the reducer turns into mission events. Commands, chains and subscriptions ride the ledger and the outbox. Temporal schedules; lanes execute; the catalog pins.

## 3. Contracts added or completed

All are strict versioned JSON objects with `lower_snake` keys and Pydantic models exported to JSON Schema beside the existing contracts (`src/mission_control/contracts/`, `domain/authoring/`).

| Contract | Owner spec | Purpose |
| --- | --- | --- |
| `mc.mission_manifest.v1` | SPEC-05 | The YAML authoring surface (`manifest: mission/v1`), one mission or `missions` plus `links` |
| `mc.manifest_resolution.v1` | SPEC-05 | Every `search` resolved to a pin, every inheritance decision, every blocker; stored with the revision |
| `mc.capability_host_support.v1` | SPEC-01 | Per-row matrix of lane profiles and overlays |
| `mc.hook_input.v1`, `mc.hook_result.v1` | SPEC-01 | stdin and stdout of a hook script on every lane |
| `mc.subagent_profile.v1`, `mc.plugin_manifest.v1` | SPEC-01 | Provider-neutral subagent and plugin cores |
| `mc.context_packet.v1` | SPEC-02 | The handoff bundle with tiers; sealed; the `mc.context_selection.v1` record references it |
| `mc.continuation_checkpoint.v1` | SPEC-02 | The checkpoint manifest of workflow-types/08, sealed from a packet plus typed state |
| `mc.provider_frame.v1` | SPEC-03 | One persisted frame: harness execution, generation, provider dedupe key, arrival ordinal, kind, digest, excerpt |
| `mc.transcript_entry.v1` | SPEC-03 | One line of the materialized transcript |
| `mc.chain.v1`, `mc.chain_link.v1` | SPEC-04 | Chain identity, links, acceptance conditions, cancel policy |
| `mc.command.v1` (completed) | SPEC-06 | `queue_instruction`, `add_context`, `interrupt_and_inject`, `cancel{urgency}`, `fork` payloads become accepted |
| `mc.subscription.v1` | SPEC-06 | Filter, channel, cursor, delivery receipts |
| `mc.lane_describe.v1` | SPEC-07 | Each lane profile's control matrix and native identity mapping |
| `mc.cursor_binding.v1` | SPEC-07 | `CursorExecutionBinding`: SDK and bridge pins, model, mode, workspace, projections, hook callback |

## 4. Persistence added (common component, `packages/mission-control-db-contract`)

Migration numbers are pre-assigned so teams never collide. Each is additive, RLS-forced, and seeded through `mission-db` like the existing chain.

| Migration | Team | Objects |
| --- | --- | --- |
| `0025_capability_kinds_and_host_support.sql` | T1 | extend `asset_version.kind` (`skill_bundle`, `mcp_server`, `mcp_tool`, `hook_script`, `subagent_profile`, `plugin`), `asset_version.host_support jsonb`, plugin membership table `capability_plugin_member` |
| `0026_search_projection_nullable_embedding.sql` | T1 | `search_document.embedding` nullable, `search_mode` function, host-support filter columns; bucket policy seed for `capability-bundles` as a `mission-db` seed bundle |
| `0027_provider_frames.sql` | T2 | `provider_frame` (keyed `harness_execution_id, generation, provider_key, arrival_ordinal`), writers for `harness_execution`, `agent_session`, `session_turn`, retention policy table |
| `0028_mission_chains.sql` | T3 | `mission_chain`, `chain_link`, link state and receipts; `mission_relationship.kind` gains `supplies`, `depends_on` |
| `0029_command_mailbox_stop_fence_subscriptions.sql` | T5 | `command_mailbox` (per run and generation), `stop_fence`, `mission_subscription`, `subscription_delivery` |
| `0030_lane_bindings.sql` | T4 | `lane_profile` registry rows, `execution_binding.lane` discriminator, Cursor native identity columns on `harness_execution` |

## 5. Modules added or changed (`src/mission_control`)

| Area | Add | Change | Team |
| --- | --- | --- | --- |
| Catalog | `domain/capabilities/{hooks,subagents,plugins}.py` definitions; `adapters/supabase_storage/bundles.py` upload and signed URLs; seed bundles under `packages/.../seeds/common/` | `domain/authoring/contracts.py` (`DefinitionKind`, `CapabilityKind`, `Definition` union), `adapters/postgres/control_plane/catalog_assets.py` (`ASSET_KIND`), `application/capabilities/capability_search.py` (lexical fallback, filters), `bootstrap/api.py` (embeddings), `application/agentic_components/projections.py` (all kinds, all lane profiles) | T1 |
| Context | `domain/context/packet.py` (packer, tiers, budget), `domain/context/render.py` (`.mission/context.md`, `inputs.json`, prompt segment), `domain/context/checkpoint.py` | `application/programs/service.py::StageGraphOperationPreparationService.materialize` (packet in, read mounts), `domain/programs/interpreter.py::_input_refs` (keep slot mapping), `application/programs/goal_directed.py` (`_prompt_segments`, `_workspace_for` use the packet), `adapters/deep_agents/adapter.py` (`output_refs`, frame sink, compaction wrapper) | T2 |
| Frames and transcript | `application/frames/{sink,reducer,transcript}.py`, `adapters/postgres/frames/`, `interfaces/http/transcript.py`, CLI `run transcript`, `run list` | `adapters/operations/runtime_ports.py` (`RecordedOperationEventSink` replaced), `domain/policies/reducer.py` (turn facts) | T2 |
| Authoring and chains | `domain/authoring/manifest.py`, `application/authoring/manifest_service.py`, `application/chains/{service,reducer}.py`, `domain/composition/chain.py`, CLI `mission compile|submit|start`, `chain inspect`, MCP tools | `interfaces/mcp/coordinator_server.py`, `application/authoring/service.py` (compile entry), outbox relay (chain release) | T3 |
| Harness and lanes | `application/execution/harness/{protocol,registry,describe}.py`, `adapters/cursor/{local,cloud,frames,projection,hooks_callback}.py`, `adapters/temporal/activities/lane_turn.py` (`lane.turn`, `lane.status`, `lane.cancel`), `interfaces/http/hook_callback.py` | `domain/execution/contracts.py` (`execution_runtime` gains `cursor`, `CursorExecutionBinding`), `application/execution/operations/operation_execution.py` (dispatch by lane), `adapters/temporal/workflows/operation.py` (segmented activity), `adapters/temporal/deployment_composition.py` (lane registry wiring), `pyproject.toml` (`temporalio` 1.34, `cursor-sdk` 1.0.37) | T4 |
| Control and subscriptions | `domain/policies/mailbox.py`, `domain/policies/stop_fence.py`, `application/subscriptions/{service,relay}.py`, `interfaces/http/subscriptions.py`, CLI `command queue|inject`, `subscribe`, `events watch` | `application/missions/service.py::_action` (rejection removed), `domain/policies/contracts.py` (`BOUNDARY_COMMAND_KINDS`), `adapters/temporal/boundary_commands.py`, family workflows (`deliver_boundary_command` next-turn and next-iteration delivery), `interfaces/http/mission_control.py`, `contracts/contracts.py` (`MissionInspection` enrichment) | T5 |
| Skills | `skills/mission-control-{author,catalog,observe,intervene,compose}/`, `scripts/skills_manifest.py`, `make skills-manifest` | `skills/mission-control/SKILL.md` (router), retire `.agents/skills/mission-control-coordinator` | T1 (docs) |

## 6. Lane matrix (what `describe` must report)

| Operation | deep_agents | cursor_local | cursor_cloud |
| --- | --- | --- | --- |
| prepare | native (materializer, backend `upload_files`) | native (workspace lease, projections, bridge launch) | native (repo, `starting_ref`, pre-created `mc/<run>` branch, env vars) |
| start / reattach | native / native by checkpoint | native / emulated (`Agent.resume`, run lost) | native (client `agentId`) / native (`get_run` + stream) |
| send_turn | native (`invoke` on thread) | `wait_then_send` | `wait_then_send` (`409 agent_busy`) |
| cancel_turn | native (activity cancel → graph cancel) | native (`run.cancel`) | native (`POST .../cancel`) |
| observe | native (`astream` v2, checkpoint id cursor) | native (`run.observe(after_offset)`) | native (SSE `Last-Event-ID`, `410` → `GET run`) |
| queue_instruction | `turn_boundary_guaranteed` (next invoke) | `wait_then_send` | `wait_then_send` |
| interrupt_and_inject | `cancel_and_replace` | `cancel_and_replace` | `cancel_and_replace` |
| pause | `pause_at_tool_gate` (HITL interrupt) or boundary | unsupported mid-run; boundary only | unsupported mid-run; boundary only |
| fork | emulated (new thread from packet + checkpoint values) | emulated (new agent, packet, git patch) | emulated (new agent, packet, branch) |
| snapshot | emulated (checkpoint + workspace files) | emulated (git patch + files) | emulated (branch + artifacts) |
| usage | settled tokens; cost estimated | settled tokens; cost estimated → settled (`get_usage`) | settled tokens; cost estimated → settled |
| hooks | `HookScriptMiddleware` (`wrap_tool_call`, before/after model and agent) plus compaction wrapper | `.cursor/hooks.json` command hooks, `fail_closed` kernel hooks, `sessionStart` available | command hooks, no `sessionStart`/`sessionEnd`/MCP hooks |
| instructions | `system_prompt` | `AGENTS.md` + `.cursor/rules/mc-mission.mdc` (`alwaysApply`) | same, committed to the branch |
| subagents | `SubAgent` dicts from profiles | `.cursor/agents/*.md` (full fields) or inline | `.cursor/agents/*.md` in repo or `customSubagents` (max 20) |

## 7. Flows

### 7.1 Stage handoff (Mission 1, `collect → synthesize`)

1. `collect` (Goal Loop) accepts; its outputs `sources` (artifact) are registered; `activation.completed{accepted}` commits.
2. StageGraph interpreter releases `synthesize`; `_input_refs` now returns `(producer_output_slot → consumer_input_slot, artifact_ref)` pairs.
3. Preparation calls the Context Packer with the consumer's bindings (`expand` per binding), the node's selected context, the model profile budget. Packet: `sources` as `materialize` (`/inputs/sources/source_manifest.json`), the collect Progress Review summary `inline`, the journal digest `reference`.
4. Packet is sealed, `mc.context_selection.v1` recorded, workspace manifest gains read-only entries; prompt gains one `admitted_input` segment rendering the packet index.
5. Lane `prepare` materializes `.mission/context.md`, `.mission/inputs.json`, `/inputs/...`; turn runs; frames persist; output artifacts registered; `output_refs` filled from promotions.

### 7.2 Goal Loop iteration handoff (all missions)

Iteration n seals its journal segment; the packer builds iteration n+1's packet from bounded Loop State (inline), the journal digest (reference), unresolved blockers and human answers (inline), accepted artifacts (reference or materialize). `GoalHandoffDraft` content from the model is data inside the packet, never the packet.

### 7.3 Continuation and compaction

Soft threshold (context health policy or provider summary frame) → `before_compaction` frame → deterministic reduction → `mc.continuation_checkpoint.v1` sealed from packet + typed state → fresh session hydrated from the checkpoint packet (`workspace` tier restores files) → `session.transferred` event. On Cursor the fresh session is a new agent; on Deep Agents a new thread seeded from checkpoint values.

### 7.4 Chain link (Mission 2, `research supplies ingestion`)

`goal.accepted` for `research.evidence_map` commits → chain reducer evaluates links → admits `ingestion` run with a packet built from supplied outputs + final checkpoint reference + journal digest, writes outbox intent in the same transaction → relay starts `mission-run:<run_id>` with `USE_EXISTING` → `chain_link.released` event.

### 7.5 Cursor Local turn (Mission 3)

`lane.turn` activity: lease workspace (git worktree of the target repo at base ref), write projections (`AGENTS.md`, `.cursor/rules/mc-mission.mdc`, `.cursor/skills/*`, `.cursor/agents/verifier.md`, `.cursor/mcp.json`, `.cursor/hooks.json` with kernel hooks first), `.mission/` from the packet, launch bridge (`setting_sources=["project"]`, pinned `state_root`), `Agent.create(local=...)`, `send(message)`; consume `run.observe(after_offset)`; persist each frame; heartbeat offset; kernel hooks call `POST /v1/internal/hook-callback` with the task token and receive allow or deny from the stop fence and intent ledger; on `RunResult` write closing facts, compute git patch, register artifacts, return.

### 7.6 Intervention

`command queue` → mailbox row → family boundary at `next_turn`/`next_iteration` consumes it into the next packet → delivery report `turn_boundary_guaranteed` or `wait_then_send`. `command inject` → `interrupt_and_inject` → lane describe → `cancel_and_replace`: activity cancel, idempotent `lane.cancel`, poll to final, uncertain effects settled, replacement turn with the injected packet item. `cancel --urgency immediate` → stop fence row first, then the same cancel path. `run fork --from-snapshot --instruction` → snapshot → new run admitted with packet `workspace` tier → optional mailbox entry.

## 8. Interfaces added

| Surface | Additions |
| --- | --- |
| CLI `missionctl` | `mission compile|submit|start FILE`, `chain inspect ID`, `catalog search --kind --host --json`, `catalog publish --dir`, `catalog pin`, `run list --query`, `run transcript ID --format --since`, `run search ID --query`, `run fork ID --from-snapshot --instruction-file`, `command queue ID --file`, `command inject ID --file`, `command cancel ID --urgency immediate`, `subscribe --webhook URL --events ...`, `events watch ID --after-seq` |
| HTTP below `/v1/applications/{app}` | `POST /missions:compile`, `POST /missions:submit`, `POST /missions/{id}/runs` (start), `GET /chains/{id}`, `GET /runs/{id}/transcript`, `GET /runs`, `POST /runs/{id}/forks` (completed), `POST /runs/{id}/commands` (all kinds accepted), `POST /subscriptions`, `GET /missions/{id}/events` (SSE), `POST /catalog/publish`, `POST /internal/hook-callback` (worker-scoped) |
| MCP (coordinator server) | `mission_manifest_compile`, `mission_manifest_submit`, `mission_run_start`, `mission_run_inspect`, `mission_run_transcript`, `mission_command_send`, `mission_run_fork`, `mission_subscribe`, `mission_chain_inspect`; resources `mc://.../runs/{id}/transcript`, `mc://.../chains/{id}` |
| Skills | router plus five bundles (SPEC-08) |

## 9. Ticket plan

Forty-one tracer-bullet tickets in nine epics; identifiers are the Linear issues created from [issues/](issues/). Blockers are the only ordering; waves are what `validate_workspace.py` prints from the edges.

| Id | Epic | Title | Blocked by |
| --- | --- | --- | --- |
| A1 | Capabilities | Capability kinds migration, definitions and host support | — |
| A2 | Capabilities | Bundle custody: upload, digest paths, signed download, bucket seed | A1 |
| A3 | Capabilities | Hybrid search with lexical fallback wired into the public API | A1 |
| A4 | Capabilities | Host projection for skills, MCP, hooks, subagents and plugins per lane profile | A1 |
| A5 | Capabilities | Hook script contract and Deep Agents HookScriptMiddleware with kernel hooks | A1 |
| A6 | Capabilities | Seed MCP servers: Tavily, Firecrawl, PubMed, EDGAR | A1 |
| A7 | Capabilities | Seed skill bundles: agent-browser, edgartools, mission-control bundles | A2, H1 |
| A8 | Capabilities | Catalog CLI and MCP parity for kinds, pins and plugin composition | A3, A4 |
| B1 | Context | Context Packet contract, packer and renderers | — |
| B2 | Context | Stage handoff delivers the packet and materializes inputs | B1 |
| B3 | Context | Goal Loop iteration packet replaces the stringified handoff | B1 |
| B4 | Context | Continuation checkpoint sealed from the packet; compaction frames; request_continuation | B1, C1 |
| C1 | Mission state | Provider frame store and Deep Agents frame writer | — |
| C2 | Mission state | Reducer derives turn facts from closing frames | C1 |
| C3 | Mission state | Transcript materialization on CLI, HTTP and MCP | C2 |
| C4 | Mission state | Run list and search through Temporal search attributes and transcript index | C2, G7 |
| D1 | Chains | Chain contracts, tables and compile from a missions list | — |
| D2 | Chains | Chain reducer releases the next run through the outbox with a packet | D1, B1 |
| D3 | Chains | Acceptance: two linked Goal Loops transfer state | D2, B3 |
| E1 | Manifest | Manifest schema, inheritance rules and JSON Schema export | — |
| E2 | Manifest | Compile resolves searches to pins and validates lanes | E1, A3 |
| E3 | Manifest | Submit and start on CLI and MCP; provenance stored with the revision | E2, D1 |
| F1 | Control | queue_instruction and add_context mailbox with boundary delivery | B1 |
| F2 | Control | interrupt_and_inject per lane with uncertain-effect settlement | F1, G1 |
| F3 | Control | Immediate cancel with persisted stop fence consulted by kernel hooks | G1 |
| F4 | Control | Fork from snapshot with queued instruction on CLI, HTTP and MCP | B1, F1 |
| F5 | Control | Subscriptions: webhook, SSE events watch, MCP notification | — |
| F6 | Control | Inspection enrichment: lane, sessions, mailbox, delivery reports | C2 |
| G1 | Lanes | AgentHarness protocol, lane registry and dispatch by lane | — |
| G2 | Lanes | lane.turn, lane.status and lane.cancel activities with segmented heartbeats | G1, C1 |
| G3 | Lanes | Cursor Local lane: prepare, projections, kernel hooks callback, turn, frames | G2, A4, A5 |
| G4 | Lanes | Cursor lane controls: cancel, wait_then_send, cancel_and_replace, hydrated fork and continuation | G3, B4, F2 |
| G5 | Lanes | Cursor Cloud lane: idempotent create, branch control, SSE resume, artifacts, usage settlement | G3 |
| G6 | Lanes | Cursor lane qualification fixtures and describe honesty tests | G4, G5 |
| G7 | Lanes | temporalio 1.34 upgrade, worker versioning and search attribute registration | — |
| H1 | Skills | Router skill and five bundles with manifests and digest check | — |
| H3 | Skills | Skill acceptance walkthrough with a headless coding agent | I1, F1, F4, C3 |
| I1 | Missions | Mission 1 manifest and acceptance run on Deep Agents | E3, B2, B3, A6, A7, C3 |
| I2 | Missions | Mission 2 manifest and acceptance run on Cursor Cloud as a chain | G5, D3, I1 |
| I3 | Missions | Mission 3 manifest and acceptance run on Cursor Local with interventions | G4, E3, F1, F4 |
| I4 | Missions | Knowledge bundle, implementation status, AGENTS.md index and glossary reconciliation | I1, I2, I3 |

## 10. Out of scope for this packet

Parallel Swarm and Evaluator Optimizer on any lane but Deep Agents; Claude Agent SDK and Codex lanes (ADR-0018 order keeps them after Cursor); manifest `templates` and `extends`; Nexus; Cursor v1 webhooks (not shipped); automatic context health policy learning; live production deployment of API and workers (the three missions run on the local real stack: disposable PostgreSQL 17 with pgvector, local Temporal dev server, Supabase bucket for bundles).

## 11. Precedence and conflicts to report

Spec pack, then ADRs, then this packet, then `docs/knowledge`, then implementation status. Known tensions this packet resolves by ADR: ADR-0013 (now extended by ADR-0025); RUNTIME-CONTRACTS "cloud placement first" (reversed by ADR-0019 and ADR-0030); workflow-types/05 "no Cursor local lane is required" (superseded by ADR-0030). Implementers report any further conflict in the ticket rather than choosing silently.

# Citations

- `docs/adr/0023` to `docs/adr/0034`; `GLOSSARY.md`.
- `research/codebase-map.md`, `research/cursor-platform.md`, `research/temporal-lifecycle.md`, `research/deepagents-middleware.md`, `research/seed-capabilities-and-formats.md`; `docs/research/2026-10-07-coding-lane-surfaces.md`.
- Spec pack: `../mission-control-general/general-mission-control/SPECIFICATION.md`, `expansion/CONTEXT-STATE-AND-CONTROL.md`, `expansion/CATALOG-AND-ENVIRONMENTS.md`, `workflow-types/05`, `06`, `08`, `09`.
