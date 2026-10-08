---
type: Agent Configuration
title: "Fast-track agent team workspace: ownership, protocol, waves and verification"
description: "How a team of implementation agents executes the 2026-10 fast-track packet end to end: reading order, five ownership regions with disjoint paths, pre-assigned migration numbers, the claim and handoff records, the dispatch waves computed from ticket blockers, verification commands, stop conditions, and the kickoff prompt to hand the team."
tags: [mission-control, agents, process, fast-track]
---

# Fast-track agent team workspace

This packet is executed by a team of coding agents working in worktrees of `mission-control`. The tickets are Linear issues in project Mission Control (team `OVE`), mirrored one file per ticket under [issues/](issues/). Published 2026-10-07: epics OVE-13 to OVE-21, tickets OVE-22 to OVE-62, 63 native blocking relations. Linear is canonical; each local file carries its `Linear: OVE-NNN` line.

## Reading order for every agent

1. `AGENTS.md` (repo root), then `GLOSSARY.md`, then `docs/adr/0023` to `0034`.
2. [00-ARCHITECTURE.md](00-ARCHITECTURE.md), then the specification your ticket names, then [research/codebase-map.md](research/codebase-map.md) for the exact insertion points.
3. The closest subsystem `AGENTS.md` under `src/mission_control/...` before editing there.
4. The ticket body and its blockers' handoff comments in Linear.

Retrieval beats recall: run `python docs/tools/okf_search.py "<terms>"` before asserting a Mission Control fact.

## Ownership regions

No two teams edit one path region concurrently. Shared files are integrator-only and changed through small, early tickets.

| Team | Tickets | Writable regions | Shared files touched (integrator-only) |
| --- | --- | --- | --- |
| T1 catalog and capabilities | A1 to A8, H1 | `src/mission_control/domain/capabilities/`, `application/capabilities/`, `application/agentic_components/`, `adapters/capabilities/`, `adapters/supabase_storage/`, `adapters/postgres/control_plane/catalog_assets.py`, `adapters/postgres/capability/`, `packages/mission-control-db-contract/component/migrations/0025_*`, `0026_*`, `packages/.../seeds/common/mc.catalog.agent-capabilities-*`, `skills/`, `scripts/skills_manifest.py` | `domain/authoring/contracts.py` (DefinitionKind, CapabilityKind, Definition union), `bootstrap/api.py` (embeddings wiring), `bootstrap/catalog.py`, `Makefile` |
| T2 context and mission state | B1 to B4, C1 to C4 | `domain/context/`, `application/frames/`, `adapters/postgres/frames/`, `interfaces/http/transcript.py`, `application/programs/service.py` (preparation only), `application/programs/goal_directed.py` (prompt and workspace only), `domain/programs/interpreter.py::_input_refs`, `adapters/deep_agents/adapter.py` (frames, output_refs, compaction wrapper), `migrations/0027_*` | `domain/policies/reducer.py` (turn facts), `adapters/operations/runtime_ports.py`, `interfaces/cli/main.py` (`run transcript`, `run list`) |
| T3 authoring and chains | D1 to D3, E1 to E3 | `domain/authoring/manifest.py`, `application/authoring/manifest_service.py`, `domain/composition/chain.py`, `application/chains/`, `interfaces/http/missions.py`, `interfaces/mcp/coordinator_server.py` (new tools only), `migrations/0028_*`, `docs/specs/fast-track-2026-10/missions/` | `interfaces/cli/main.py` (`mission`, `chain` groups), outbox relay (chain release hook), `application/authoring/service.py` (compile entry) |
| T4 harness and lanes | G1 to G7, F2, F3 | `application/execution/harness/`, `adapters/cursor/`, `adapters/temporal/activities/lane_turn.py`, `adapters/temporal/workflows/operation.py`, `interfaces/http/hook_callback.py`, `domain/policies/stop_fence.py`, `migrations/0030_*`, `pyproject.toml` dependency pins, `uv.lock` | `domain/execution/contracts.py` (`execution_runtime`, `CursorExecutionBinding`), `application/execution/operations/operation_execution.py` (dispatch), `adapters/temporal/deployment_composition.py`, `bootstrap/worker.py` |
| T5 control and interfaces | F1, F4, F5, F6 | `domain/policies/mailbox.py`, `application/subscriptions/`, `interfaces/http/subscriptions.py`, `interfaces/http/mission_control.py` (commands, fork, inspection), `adapters/temporal/boundary_commands.py`, `migrations/0029_*` | `application/missions/service.py::_action`, `domain/policies/contracts.py` (`BOUNDARY_COMMAND_KINDS`, mailbox records), `contracts/contracts.py` (`MissionInspection`), family workflows' `deliver_boundary_command` |
| Integrator and reviewer | I1 to I4, H3, all shared files | `docs/knowledge/`, `docs/MISSION_CONTROL_IMPLEMENTATION_STATUS.md`, `AGENTS.md` index, `GLOSSARY.md`, migration numbering, release of shared contracts | everything above when two teams need the same line |

Migration numbers are fixed in [00-ARCHITECTURE.md](00-ARCHITECTURE.md) section 4. A team that needs a table outside its number asks the integrator; it never renumbers.

## Dispatch waves

Computed from blockers; a ticket starts when every blocker is `Done` in Linear, not when its wave begins.

| Wave | Tickets |
| --- | --- |
| 0 | A1, B1, C1, D1, E1, F5, G1, G7, H1 |
| 1 | A2, A3, A4, A5, A6, B2, B3, B4, C2, D2, F1, F3, G2 |
| 2 | A7, A8, C3, C4, D3, E2, F2, F4, F6, G3 |
| 3 | E3, G4, G5 |
| 4 | G6, I1, I3 |
| 5 | H3, I2 |
| 6 | I4 |

Regenerate with `python docs/specs/fast-track-2026-10/validate_workspace.py`.

Default concurrency: five implementation agents (one per team) plus one integrator. Fewer slots shrink the wave, never the ownership rules.

## Claim and handoff

`issue_claim@1` (a Linear comment when you start): ticket, agent and team, worktree path and base commit, exact writable paths, blockers with their `Done` evidence, planned tests.

`issue_handoff@1` (a Linear comment when you finish): changed files, new or changed contracts and migrations, tests run with results (passed, failed, blocked, unrun, each listed), proof artifact paths under `.scratch/fast-track-2026-10-07/<ticket>/`, open assumptions, which tickets this unblocks. Move the issue to `In Review`; the integrator moves it to `Done` after `make check` and the ticket's acceptance criteria pass on the integration branch.

Branch and worktree: `git worktree add ../mission-control-<ticket> -b ft/<ticket>-<slug> main` from the repository root. Rebase on `main` before handoff. Never stash, reset or clean another worktree; the primary checkout carries uncommitted owner work. Commit on your branch; do not push or merge unless the ticket says so.

## Environment available to the team (owner statement 2026-10-07)

Credentials live in `mission-control/.env` and reach code only through `bootstrap/settings.py` or the host's secret mechanism; never print, commit, paste or copy them into manifests, seeds, frames, fixtures or transcripts.

| Resource | What is available | How to use it |
| --- | --- | --- |
| Cursor | `CURSOR_API_KEY` is set; both `cursor_local` (bridge on the worker) and `cursor_cloud` (Cloud Agents API) may run small real fixtures | G3 to G6, I2, I3; one agent and one run per recording; archive or delete agents you create |
| Temporal | Local stack is the development default: `make temporal-up` (server 1.31.0, UI, namespace job). Temporal Cloud is available through `TEMPORAL_CLOUD_API_KEY` with `TEMPORAL_ADDRESS` and `TEMPORAL_NAMESPACE` | G7 wires API-key TLS connection (`Client.connect(address, namespace=..., api_key=..., tls=True)`); use local whenever Cloud misbehaves or during iteration |
| Application databases | Supabase projects `biotech-research-ingestion` (app `biotech`) and `supabase-blue-ocean` (app `ai-engineer`) carry release `mission_control` 1.0.0; `SUPABASE_URL`, `SUPABASE_SECRET_KEY`, `APPLICATION_DATABASE_URL` and `APPLICATION_DATABASE_DIRECT` are set | Migrations 0025 to 0030 qualify on a disposable local PostgreSQL first (`MISSION_CONTROL_TEST_ADMIN_DSN`); applying a release to a Supabase project needs an owner approval comment on the ticket; bucket `capability-bundles` policies are seeded through `mission-db` the same way |
| Runtime persistence | LangGraph saver and store (`mission_control_runtime`) and the Agent Server use the local Docker PostgreSQL from `make infra-up` (`application-postgres`, pgvector); `LANGGRAPH_CHECKPOINT_DATABASE_DIRECT` points there; `mission-db runtime-apply` provisions it | Never point the checkpointer at a Supabase business database (ADR-0017) |
| Models | `OPENAI_API_KEY` (embeddings `text-embedding-3-small`, model routes) and `ANTHROPIC_API_KEY` are set | A3 embeddings, Deep Agents model routes, I1 |
| Retrieval providers | `TAVILY_API_KEY`, `FIRECRAWL_API_KEY`, `EXA_API_KEY` are set; `NCBI_API_KEY` and `EDGAR_IDENTITY` are not (PubMed works keyless at 3 requests per second; EDGAR needs an identity string before a live call) | A6 seeds and smoke tests; ask the owner for the two missing values through a Linear comment on A6 |
| Sandboxes and tracing | `DAYTONA_API_KEY`, `LANGSMITH_API_KEY` and the `LANGSMITH_*` tracing variables are set | Optional; LangSmith traces are evidence, not the ledger |
| Tracker | `LINEAR_API_KEY` is set | Claim and handoff comments; `publish_issues.py` and `sync_issues.py` |

Budget policy (replaces the earlier "zero until approved"): **small real fixture runs are permitted by default**. Bounded means one agent run, one search query, one embedding batch of a few hundred rows, or one mission iteration per recording. Record spent, reserved and unknown units in the handoff. A full mission run on Cursor Cloud, a bulk embedding of the catalog, or any repeated live drill still needs an owner-approved cap in a Linear comment on that ticket. Stop and record on any ambiguous paid effect.

## Verification

- `make check` (ruff, mypy fast, deptry, architecture tests, unit tests) must pass on every handoff.
- Persistence or Temporal claims need the real local stack: `make infra-up`, `make temporal-up`, then `uv run --group biotech pytest -m common_db <selection>` with `MISSION_CONTROL_TEST_ADMIN_DSN` set to a disposable PostgreSQL 17 with pgvector. Report passed, failed, blocked and unrun separately.
- `python docs/tools/validate_okf.py` and `python docs/tools/agents_docs_index.py` after any documentation change.
- Live provider calls follow the budget policy above: small real fixtures by default, recorded and replayed in CI afterwards; larger drills need an approved cap in Linear.
- Secrets come from `.env` and the host's secret mechanism; never print, commit or paste them, and never put them in a manifest, a seed, a frame or a transcript.

## Stop conditions

Stop the ticket and record the uncertainty in Linear when: an external effect is ambiguous (unknown whether a paid agent or run was created), a migration would conflict with a live installation, a contract change crosses into another team's region, a provider surface marked UNVERIFIED in the research behaves differently, or `make check` cannot pass without touching a shared file. Continue on other independent tickets.

## Kickoff prompt for the team orchestrator

```text
You are implementing the Mission Control fast-track packet in C:\Users\Pinda\Proyectos\Biotech\mission-control.
Read docs/specs/fast-track-2026-10/TEAM-WORKSPACE.md first and follow it exactly: reading order, ownership regions,
waves, claim and handoff comments in Linear (team OVE, project Mission Control; LINEAR_API_KEY is in .env, never print it).
Tickets are Linear issues OVE-22 .. OVE-62 (titles [FT-A1] .. [FT-I4]; epics OVE-13 .. OVE-21); the same text is under docs/specs/fast-track-2026-10/issues/, each with its Linear: line.
Work the frontier: any ticket whose blockers are Done. One worktree per ticket. Run make check before every handoff.
Specifications are docs/specs/fast-track-2026-10/SPEC-01 ... SPEC-08; decisions are docs/adr/0023 ... 0034; vocabulary is GLOSSARY.md.
Report progress as accepted tickets, open contracts, frontier-ready tickets, spent and unknown paid units, and blockers.
Finish when I1, I2 and I3 pass their acceptance runs and I4 has updated the knowledge bundle and status.
```

# Citations

- `docs/agents/issue-tracker.md`, `docs/agents/triage-labels.md`, `docs/agents/domain.md` (process the team follows).
- `../mission-control-general/general-mission-control/expansion/TEAM-WORKSPACE.md` (the earlier packet's protocol this one adapts).
- [00-ARCHITECTURE.md](00-ARCHITECTURE.md) section 9 (ticket plan) and section 4 (migration numbers).
