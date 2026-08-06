# Stage 2 — standard Agent Server application foundation

Status: BLOCKED_ON_EXTERNAL_STATE — local implementation is complete; see the Stage 2 handoff for external acceptance blockers  
Mission type: pinned dependency integration, side-effect-free graph exports, custom auth, shared HTTP composition, local server conformance, and initial tracing  
Depends on: accepted Stages 0 and 1

## 1. Mission

Create the minimal production-shaped standard LangSmith Agent Server application that can host BellLabs custom LangGraphs and routes safely. Add the accepted dependency lock, `langgraph.json`, static or introspection-safe graph entry points, custom authentication/authorization, shared route composition, health/readiness, and native LangSmith trace correlation.

The stage ends with two minimal graph families importable and inspectable in Studio, authenticated native Agent Server resources, and coexistence-safe BellLabs routes. It does not yet implement full runtime parity.

## 2. Permission to clarify or interview

The agent may interview the owner before starting. Clarify:

- selected CLI or GitHub/UI deployment ownership and how much Cloud interaction is authorized now;
- dev/staging LangSmith workspace/project names and data policy;
- Supabase JWT/custom identity inputs and roles;
- whether a coordinator graph is registered now or added after facade convergence;
- final graph IDs and custom route prefixes;
- development Studio auth posture versus staging/production;
- Agent Server managed database versus any qualified custom Postgres experiment;
- trace project, masking defaults, and synthetic-only data requirements for this stage.

No production deployment or secret upload is implied by this mission.

## 3. Required inputs

- accepted package/version matrix and lock proposal;
- Stage 1 contracts, runtime-neutral ports, identity mapper, and schema exports;
- Stage 0 graph-factory/auth/platform spike evidence;
- current FastAPI router/lifespan/dependency composition;
- current settings and `.env.example` conventions;
- accepted build/package exclusion manifest.

## 4. Proposed source shape

Adapt to current code rather than duplicating packages:

```text
app/agent_server/
  graphs.py
  graph_factory.py
  http_app.py
  auth.py
  context.py
  health.py
  streams.py
  stagegraph/{graph.py,state.py,reducers.py,nodes.py}
  goal_directed/{graph.py,state.py,reducers.py,nodes.py}
app/api/dependencies.py
langgraph.json
langgraph.dev.json                 # if separate dev auth/env posture is accepted
tests/agent_server/
```

StageGraph and GoalDirected graph implementations are minimal conformance graphs here. Later stages fill the lifecycle nodes while preserving registered IDs and compatibility discipline.

## 5. Deliverables

### 5.1 Pinned dependencies and lock

Apply the accepted Stage 0 versions in `pyproject.toml` and regenerate `uv.lock`. Include only accepted provider packages. Retain Temporal/OpenAI Agents and the current trace bridge behind legacy paths.

Add conformance imports/minimal execution tests for:

- `create_agent` and async middleware hooks;
- `create_deep_agent` and default tool surface;
- Agent Server SDK client/thread/run/stream/state APIs;
- graph factory/current `ServerRuntime` API;
- MCP, QuickJS, async subagents, and Sandbox only according to enabled flags;
- CLI config/schema load.

Prevent overlapping global tracing hooks between legacy and LangChain runtimes.

### 5.2 `langgraph.json` and graph registry

Create a config validated by the pinned CLI. It must include:

- dependencies and Python version;
- stable `belllabs_stagegraph` and `belllabs_goal_directed` graph IDs, unless Stage 0 accepted alternatives;
- custom auth path;
- custom HTTP app path;
- current supported custom-route authentication/middleware configuration;
- narrow environment contract appropriate to local development;
- no embedded secrets.

Do not copy illustrative keys from the architecture plan without CLI validation. Add a config/schema conformance test.

### 5.3 Import-safe graph exports

Both graph entry points must:

- import with no network/DB/secret/tracing/sandbox/MCP/worker side effects;
- expose stable graph/state/node/reducer names;
- compile without explicit checkpointer/Store in managed Agent Server mode;
- use in-memory or explicit async persistence only in standalone test fixtures;
- provide minimal typed input/output schemas and safe placeholder behavior;
- carry provider-qualified BellLabs correlation metadata;
- avoid full ERC/transcript payloads in state.

### 5.4 Graph assembly factory boundary

Use a static compiled graph for families not requiring runtime assembly. Where a factory is necessary, implement the Stage 0-proven adapter:

- inspect `access_context` through a project adapter;
- return topology/schema-compatible introspection graphs for read/update/assistant inspection;
- create expensive resources only during execution;
- load one exact `GraphAssemblySpec` by digest, never mutable aliases;
- verify authoritative runtime binding in the first execution node;
- use async context management and cleanup;
- cache only immutable secret-free structure;
- never cache runtime context, Store memory, credentials, sessions, or sandbox handles globally.

Do not introduce per-run rebuilding merely because it is available.

### 5.5 Custom Agent Server auth and resource authorization

Implement current `langgraph_sdk.Auth` handlers using the shared principal mapper:

- authenticate external JWT/token and return safe subject/scope/role metadata;
- tenant-scope threads, runs, assistants, Store, crons, and other protected resources;
- default-deny unhandled protected resources;
- distinguish authenticated Studio behavior only by accepted policy;
- expose identity via non-serializable runtime context/config;
- never put raw token or user credentials in state, trace, or prompt;
- support authenticated tool actions through secure secret references when later required.

Test every resource action, not only thread search.

### 5.6 Shared custom HTTP application

Create a thin FastAPI/Starlette app that mounts shared router factories and dependencies without importing the legacy `app.server` lifespan or starting Temporal/OpenAI workers.

Requirements:

- reuse domain/application services and principal mapping;
- retain standalone FastAPI composition for coexistence;
- enable custom-route auth using exact pinned configuration;
- do not shadow Agent Server default routes;
- expose docs/OpenAPI as accepted;
- initially mount `/ok`, readiness, and a narrow representative v1/v2 route set;
- normalize BellLabs errors through the v2 envelope only on BellLabs routes;
- native Agent Server endpoints retain native schemas.

### 5.7 Health/readiness foundation

Implement distinct:

- liveness: process/event loop response;
- dependency readiness: BellLabs PostgreSQL, MongoDB, artifact service when required, and external Agent Server SDK only where it cannot self-call during startup;
- runtime capability readiness: graph registry/import/config metadata and non-mutating optional provider configuration checks;
- degraded capability details.

Readiness must not create threads, Store rows, MCP sessions, sandboxes, or recursive Agent Server calls during cold start. Persistence round-trip canaries belong to an isolated post-deploy monitor.

### 5.8 Initial tracing and masking

Enable native LangChain/LangGraph tracing under a distinct local project. Root metadata includes safe pseudonymous BellLabs/run/graph/assembly/deployment correlations. Add nested node/model/tool placeholders as applicable.

Prove masking of:

- auth headers and token values;
- secret refs versus values;
- signed URLs;
- environment dumps;
- sentinel secrets and synthetic PHI;
- raw large payloads.

Tracing failure must not become lifecycle failure unless explicitly accepted for a critical compliance posture.

### 5.9 Configuration and build hygiene

Update `.env.example` with names only for accepted runtime mode, environment, Agent Server endpoint/keys, graph IDs, expected deployment identity/revision, LangSmith trace settings, BellLabs data stores, sandbox, and MCP refs.

Rules:

- no broad local `.env` is uploaded;
- provider keys are conditional on model policy;
- no local AWS profile in Cloud;
- runtime DB credentials are non-owner;
- migrations are release-job work;
- package artifact excludes scratch/personal/experimental/legacy-worker-only assets not needed by Agent Server;
- tests run before image build because the existing build context may exclude them;
- no parent `.tools` dependency.

## 6. Required tests and proofs

### Import/config

- import modules in a clean process with network/DB/secret access traps;
- validate `langgraph.json` with pinned CLI;
- inspect input/output/state schemas for both graphs;
- Studio graph inspection triggers no expensive resources;
- `threads.read`, `threads.update`, and `assistants.read` factory paths are side-effect free;
- execution factory cleanup covers success/failure/cancel/interrupt.

### Auth and routes

- missing/invalid/expired tokens;
- cross-tenant thread/run/assistant/Store/cron denial;
- role-specific positive paths;
- custom route auth and shared principal identity;
- route inventory/collision snapshot against Agent Server defaults;
- REST error envelope and native endpoint schema separation;
- standalone FastAPI remains importable/runnable under coexistence.

### Local server E2E

Run the exact accepted equivalent of:

```powershell
uv sync --frozen
uv run langgraph dev --config langgraph.json --no-browser
```

Prove:

- both graphs appear in Studio/schema APIs;
- authenticated thread creation and run execution;
- streaming and reconnect on the minimal graph;
- state/history reads;
- a minimal interrupt/resume proof using non-production decisions;
- custom routes and health;
- traces arrive with masking/correlation.

### Static/full suite

- Ruff, mypy, tests, schema snapshots, and packaging manifest pass under the accepted baseline.

## 7. Gate

Stage 2 passes when:

- the pinned lock is reproducible;
- both stable graph IDs load and execute locally;
- graph import and introspection are side-effect free;
- managed persistence is not double-configured;
- custom auth tenant-scopes all required native resources;
- shared custom routes do not collide or duplicate domain services;
- liveness/readiness/degradation are truthful and non-mutating;
- trace correlation/redaction passes representative tests;
- standalone legacy API remains viable for coexistence;
- no production deployment or secret exposure occurred without authorization;
- outgoing handoff is accepted.

## 8. Explicit non-goals

- Do not implement complete StageGraph/GoalDirected behavior.
- Do not switch production runtime mode.
- Do not expose arbitrary state edits or full v2 API surface.
- Do not enable unqualified QuickJS/async subagents.
- Do not connect Agent Server to BellLabs application PostgreSQL as its own checkpointer unless separately accepted.
- Do not remove legacy server/worker composition.

## 9. Outgoing handoff additions

Include:

- exact dependency/lock and CLI/base-image compatibility;
- `langgraph.json` schema and graph registry;
- graph/factory access-context matrix;
- auth resource/filter coverage matrix;
- custom/default route collision inventory;
- local server commands and Studio/API evidence;
- trace project/masking evidence without secrets;
- environment/config names and deployment artifact manifest;
- managed versus standalone persistence instructions;
- stable node/state/reducer names that later stages must preserve or version.

