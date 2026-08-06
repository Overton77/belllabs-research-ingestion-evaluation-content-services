# Stage 0 reconciled baseline

Captured: 2026-08-04  
Repository: `biotech-research-ingestion-evaluation-system`  
Base revision: `6e49ef1e49670c626956bfe0a9b1e65699dd279b`

## Worktree provenance

Before Stage 0 edits, the worktree was already dirty:

- four architecture documents were deleted from their former `docs/` paths;
- `docs/migrations_instructions/` was untracked and contained the relocated
  architecture documents and work packages.

Those owner changes were preserved. Stage 0 adds only the evidence/spike artifacts
listed in its handoff and one synthetic-PHI redaction assertion.

## Toolchain and dependency baseline

| Item | Observed value |
|---|---|
| Shell Python | `3.12.2` |
| Root `uv run` Python | `3.12.13` |
| Disposable spike Python | `3.12.7` |
| Project requirement | `>=3.12` |
| uv | `0.7.5` |
| Docker CLI | `29.2.0` |
| Docker daemon | unavailable: `dockerDesktopLinuxEngine` pipe absent |
| Root LangSmith lock | `langsmith==0.10.15` |
| Legacy execution | `temporalio>=1.30,<2`, `openai-agents>=0.17.8,<0.18` |

The root lock did not contain LangGraph, Deep Agents, Agent Server CLI, MCP adapters,
QuickJS, or PostgreSQL checkpointer packages. Stage 0 therefore uses a separate exact
lock under `spikes/stage0`; it does not change production dependencies or remove
legacy dependencies.

## Reproducible static and test baseline

| Command | Result |
|---|---|
| `uv run ruff check app tests` | pass |
| `uv run mypy app` | pass; 218 source files |
| `uv run pytest` | pass; 395 passed, 8 skipped, 11 warnings |

The eight skips are explicit service integration suites:

1. artifact promotion MongoDB;
2. artifact promotion PostgreSQL;
3. control-plane MongoDB;
4. external-candidate MongoDB;
5. operation-execution MongoDB;
6. run-control PostgreSQL;
7. sandbox-snapshot MongoDB;
8. workspace-materialization MongoDB.

They are environment/service skips, not unexplained test selection. They still prevent
Stage 0 from claiming live PostgreSQL/Mongo transaction, migration, and isolation
proof in this workstation state.

Warnings:

- a deprecated LangSmith OpenAI Agents wrapper import is still reached by pytest;
- Starlette reports its `httpx` TestClient path as deprecated in favor of `httpx2`;
- Temporal emits Pydantic v2 serialization deprecations.

These are baseline issues; Stage 0 did not broaden scope to clean them up.

## Current authority and execution map

```text
FastAPI / coordinator MCP
  -> coordinator facade / application services
  -> pure control-plane compilation and run-control reducers
  -> PostgreSQL run-control authority + outbox
  -> Temporal workflow submission
  -> StageGraphWorkflow or GoalDirectedWorkflow
  -> Temporal activities
  -> governed OperationExecutionService
  -> OpenAI Agents / MCP / Docker sandbox / artifact providers
  -> immutable settlement evidence
  -> PostgreSQL usage/lifecycle reconciliation
```

Observed authority boundaries:

- immutable definitions and exact compilation remain in BellLabs domain/application
  code;
- lifecycle, budgets, commands, results, audit/outbox, and runtime events are in
  application PostgreSQL;
- StageGraph and GoalDirected are both actually wired to Temporal workers and live
  coordinator paths;
- semantic operation binding, effect claim, and settlement are currently Beanie/Mongo
  documents;
- provider execution is guarded by stable semantic and side-effect identities;
- post-effect event publication and PostgreSQL budget reconciliation occur after the
  Mongo settlement and are independently idempotent, but are not one atomic database
  transaction.

The last two points justify amended D-13: the current implementation has good stable
identity and reconciliation behavior, but Mongo claim/settlement authority plus
PostgreSQL lifecycle authority cannot provide the planned single transaction. Stage 1
moves claim/attempt/usage/settlement authority into that PostgreSQL transaction while
retaining the immutable semantic Operation Execution Binding in MongoDB/Beanie and
referencing its stable identity/digest, as accepted `biotech-meta` requires.

## Runtime and API composition

- `app/server.py` constructs settings and configures LangSmith tracing at import time.
- FastAPI lifespan opens PostgreSQL/Redis/coordinator resources and closes them
  explicitly.
- Redis relay work is application-lifespan-owned and cancelled/awaited on shutdown.
- REST routers, Socket.IO runtime events, and the optional coordinator MCP endpoint
  coexist in one ASGI application.
- Socket.IO uses Supabase identity and request-scope authorization.
- Coordinator MCP uses a JWT verifier and request limits.
- Agent runtime approvals are typed and persisted before publication.
- `/health/live` is a true cheap liveness check.
- `/health/ready` currently always returns ready in
  `pre-emptive-bootstrap` mode; dependency/capability checks live in `app.preflight`.
  This does not yet satisfy the target split between dependency and capability
  readiness.

## Persistence and service topology

Local Compose defines:

- application PostgreSQL/pgvector on host port 55432;
- Redis on 56379;
- a separate Temporal PostgreSQL authority;
- Temporal server, namespace job, and UI.

MongoDB, Neo4j Aura, S3, Supabase, model providers, and web providers are external.
No destructive database operation was run. The Docker daemon was unavailable, so no
Compose service or production-like Agent Server container proof was possible.

## Tracing and data exposure review

Positive findings:

- settings use `SecretStr`;
- root runtime trace inputs replace resolved secrets with `[redacted]`;
- prompt bodies are represented by counts, not content;
- outputs expose usage/counts and omit full output text;
- current tests include secret and synthetic-PHI sentinels.

Risks and gaps:

- tracing configuration runs during `app.server` import rather than application
  lifespan;
- process-global tracing processor registration requires reload/multi-app scrutiny;
- request scope, provider IDs, workspace IDs, and configuration digests are metadata
  and need an accepted classification/retention policy;
- there is no accepted PHI/checkpoint encryption/deletion/retention policy;
- no live LangSmith trace shape or tenant-negative trace query was produced because
  Stage 0 was authorized as local-only.

## Build and Cloud compatibility drift

- the wheel includes only `app`;
- there is no production Dockerfile or `langgraph.json` at repository root;
- `.cursor/Dockerfile` is development-specific;
- `Settings` contains host-relative sibling `.tools` paths for stdio MCP/browser
  assets, which are not Cloud-deployable;
- the current runtime expects Docker-host access for sandbox behavior;
- the migration must publish a minimal build context and replace host stdio assets
  with remote Streamable HTTP or sandbox-contained adapters.

## Baseline issue register

| ID | Issue | Gate effect | Target |
|---|---|---|---|
| BL-01 | Effect claim/settlement authority is Mongo while lifecycle/budget/outbox is PostgreSQL; the immutable semantic binding is also Mongo-authoritative by design. | Blocks atomic claim journal proof; the fix must preserve semantic binding authority and reference it by digest. | Stage 1 |
| BL-02 | Docker daemon unavailable. | Blocks PostgreSQL injection, `langgraph build/up`, and Docker sandbox proofs. | Environment |
| BL-03 | Cloud entitlement/deployment inspection not authorized. | Blocks Serverless/Dedicated/region/revision/sandbox proof. | Owner/external |
| BL-04 | `/health/ready` is unconditional. | Contracted for later readiness work. | Stages 2/7 |
| BL-05 | Server import configures process-global tracing. | Double-registration/reload risk. | Stage 2 |
| BL-06 | Host `.tools` stdio/browser paths are in settings. | Not Cloud deployable. | Stages 2/6 |
| BL-07 | Root lock lacks migration dependencies. | Exact production lock not yet accepted. | Stage 2 |
| BL-08 | `langchain-mcp-adapters==0.3.1` resolves incompatible `mcp==2.0.0` unless constrained. | Architecture qualification failure; pin required. | Stage 0/2 |
| BL-09 | Local Deep Agents skill notes do not match the 0.7.4 default tool surface. | Never compile capability policy from stale skill prose. | Stage 5 |
| BL-10 | Checkpoint/trace/Store/sandbox retention and deletion thresholds are unsettled. | Blocks staging data policy. | Owner |
