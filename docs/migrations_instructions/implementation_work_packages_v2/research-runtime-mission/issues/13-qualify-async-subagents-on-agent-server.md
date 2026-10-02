# RRM-013 — Qualify real async subagents on an Agent Server

**What to build:** governed async subagents that actually execute on a persistent Agent Server, started and controlled from a Deep Agent inside `operation.execute` while BellLabs keeps lifecycle authority. This replaces the fake Agent Protocol client that is the only qualification so far.

**Blocked by:** RRM-001 and RRM-004.
**Blocks:** RRM-008 (async-child cancellation against the real server), RRM-009 (production composition) and RRM-010.
**Status:** implemented; independent review pending (tested head `9c1d344` on `wp/rrm-013-async-subagent-agent-server`; evidence `docs/migrations_instructions/evidence_v2/research-runtime-mission/RRM-013/README.md`)
**Branch:** `wp/rrm-013-async-subagent-agent-server`
**Authority:** accepted CP-045 (`QUAL-CP-ASYNC-SUBAGENT-LIFECYCLE`, `CON-CP-ASYNC-SUBAGENT-V1`); DA async-subordinate requirements; ADR-0003 (the Agent Server is not a competing macro scheduler); REQ-CP-DA-008/011 and REQ-CP-RUN-009 (clarified), REQ-CP-DA-019 (exact non-scheduling hosting), REQ-CP-EXEC-016 (active children block snapshots), REQ-CP-RUN-011 (child lineage) — AMD-RRM-001 (accepted 2026-10-01 at meta `6c89143`, merged into meta main `a50d833`; see [RRM-001 contract authority](../RRM-001-contract-authority.md) §4)
**Evidence:** `docs/migrations_instructions/evidence_v2/research-runtime-mission/RRM-013/`
**Added:** 2026-10-01 at the user's request. Real async subagents on an Agent Server are a mission requirement, not an optional deferral.

## Current state

- Synchronous Deep Agents and sync subagents run in-process in the Temporal worker. No Agent Server is involved.
- `DeepAgentsAsyncSubagentAdapter` (`app/integrations/agents/deep_agents/async_subagents.py`) wraps the exact `AsyncSubAgentMiddleware` tools. It calls a remote Agent Protocol endpoint through `langgraph_sdk` and injects the BellLabs `child_execution_id` as the thread ID, so retries reconnect instead of duplicating.
- CP-045 qualified that adapter against a **deterministic fake** Agent Protocol client. No real Agent Server has hosted a BellLabs async subagent.
- `langgraph.json` registers no graphs. `ASYNC_SUBAGENT_SPAWNING_ENABLED` defaults to false.
- Prior art for a persistent self-hosted server: the Block C qualification topology (`langgraph.block_c*.json`, the `langgraph up --postgres-uri` recipe in `tests/fixtures/agent_server_block_c.py`, auth and restart drills). It is qualification topology only and must not be revived as a macro runtime.

## Placement decision (recorded 2026-10-01)

The user confirmed that their LangSmith Pro plan includes deployments. They chose to **prove the capability now on the local self-hosted Agent Server**, using `langgraph up` with dedicated durable Postgres and Redis and following the Block C recipe already in the codebase. A LangSmith-hosted deployment is an optional later step and does not gate this ticket. Before relying on a `langgraph up` license or API-key prerequisite, check it against the installed CLI and record it as a fact.

`langgraph dev` and in-memory servers do not count as qualification. Use a dedicated Agent Server config file for the async subagent graphs. The root `langgraph.json` keeps registering no BellLabs macro graphs; `test_root_langgraph_does_not_select_block_c_auth_or_graphs` guards that.

## Acceptance

- [x] **Deployment.** A persistent Agent Server with its own durable Postgres, separate from BellLabs application Postgres, is reproducible from documented commands. BellLabs authenticates to it with credential references and no secret values in the repo.
- [x] **Hosted graph.** The async subagent graph is built from an exact BellLabs binding through the canonical adapter and materializer. Its graph ID, revision and binding digest are frozen in the `AsyncSubagentContract`. No hand-written, unbound graph. Registering the graph does not make the Agent Server a scheduler of BellLabs work.
- [x] **Real spawn path.** A Deep Agent inside `operation.execute` calls `start_async_task` against the real server. The child is reserved and linked in BellLabs (Mongo detail and PostgreSQL authority) before the provider run starts. Check, update, cancel and list work through the stock middleware tools.
- [x] **Crash windows.** Kill the worker after BellLabs reservation but before provider submission, and after provider submission but before observation. Recovery reconnects to the same thread and run (one provider run). Ambiguity creates a governed incident, never a second spawn.
- [x] **Agent Server restart.** Restart the Agent Server during an active child. The child resumes or is reconciled from the server's durable state, and BellLabs records the outcome.
- [x] **Results.** Child completion is provider evidence until the parent operation admits it through the existing result-admission path. Late results after parent terminality are rejected and recorded.
- [x] **Cancellation hooks for RRM-008.** Parent-initiated cancel reaches the provider run; provider acknowledgement or ambiguity is recorded. RRM-008 owns the cross-family cancellation proof built on this.
- [x] **Usage and budgets.** Child usage and reservations settle against the parent run's budget ledger. Usage the provider cannot attribute is recorded as pending or ambiguous, never dropped.
- [x] **Lineage for RRM-005/006.** Inspection data includes the child's BellLabs identity, provider thread/run, binding digest and status. Fork admission classifies active async children explicitly. They are never copied implicitly.
- [x] **Tracing.** LangSmith traces for parent and child are correlated by BellLabs identities. They remain subordinate evidence.
- [x] **Gates.** Live qualification sits behind an explicit opt-in flag and an Agent Server endpoint variable, with real model calls bounded by small technical inputs. Offline/replay suites, Ruff and mypy stay green. Record evidence, get review and merge to integration.

## Implementation record (2026-10-02)

- Deployment: `langgraph.async_subagents.json` on `langchain/langgraph-api:0.12.0-py3.12`, Compose project `rrm013-agent-server` (API `127.0.0.1:8143`, Redis), own durable Postgres `rrm013-agent-server-postgres` (`127.0.0.1:55433`, volume `rrm013-agent-server-pgdata`). The user's `LANGSMITH_API_KEY` is accepted as the license (`api_variant=licensed`); no `LANGGRAPH_CLOUD_LICENSE_KEY` is needed. The stack is left running for RRM-008/009.
- Live gate: `BELLABS_RUN_RRM_013_LIVE=1` plus `AGENT_SERVER_ENDPOINT`, `BELLABS_ASYNC_SUBAGENT_SERVER_TOKEN` and the disposable DSNs; `tests/integration/agent_server/test_rrm_013_async_subagent_live.py` (real spawn path from `operation.execute`, cancel acknowledgement, duplicate run → `in_doubt` → `adopt_provider_run`, Agent Server restart, both worker-kill windows). Fake-client tests are offline regression only.
- Migration `0021_async_subagent_submission_fence_v1.sql`; contracts, adapter and materializer edits under the coordinator's authorization; see the evidence README for the requirement map, matrix, gates and spend.

Out of scope: company missions and their definitions; hosting BellLabs macro workflows on the Agent Server; generalizing the subagent mechanism into a reusable framework (see the mission horizon in the ticket index).
