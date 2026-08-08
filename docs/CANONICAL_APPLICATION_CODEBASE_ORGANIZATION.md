# Canonical application codebase organization

Status: **accepted target organization (normative)**
Recorded: 2026-08-08
Scope: this repository through Stage 8
Implementation posture: evolve in place; do not perform speculative bulk moves

## 1. Current code versus accepted target

This document defines the best-case Stage 8 organization of the BellLabs application. It is
the authority for **where planned work should converge**, not a claim that every shown package
exists today.

- **Current / as-built:** executable code and
  [`CODEBASE_DOMAIN_WORKFLOW_GUIDE.md`](CODEBASE_DOMAIN_WORKFLOW_GUIDE.md) describe what runs
  now.
- **Target / end-state:** this document and the
  [implementation work-package index](migrations_instructions/implementation_work_packages/00_MAIN_GOAL_AND_INDEX.md)
  govern organization of new migration work.
- When target and current state differ, preserve current behavior and migrate incrementally
  behind tests and package gates. A target path does not authorize an immediate source move.

The application remains in this repository and in the canonical Python package **`app/`**.
Create no sibling application repository, no Agent Server application root, and no multi-package
split before measured ownership, deployment, or scaling evidence requires one and an accepted
architecture decision authorizes it.

## 2. Authority-first design

Dependency direction is:

```text
app/domain <- app/application <- app/api | app/temporal | app/integrations
```

`app/domain/` owns contracts, invariants, reducers, interpreters, acceptance semantics, and the
meaning of journal, effect, and settlement contracts. `app/application/` owns use-case
coordination and persists those contracts through ports and persistence adapters; it does not
redefine their meaning. API, Temporal, and integrations adapt those authorities to transports,
providers, and stores.

Dependencies may point inward, never outward:

- domain imports neither application nor runtime, provider, transport, or persistence SDKs.
  Domain may use approved framework-neutral modeling and validation libraries such as Pydantic;
  the boundary forbids infrastructure coupling, not every third-party package;
- application may import domain, but not FastAPI routers, Temporal workflows, provider SDKs,
  persistence implementations, or Agent Server graph composition;
- API, Temporal, and integrations may import application/domain contracts through explicit
  ports;
- integrations do not import API or Temporal workflow implementations;
- Temporal and providers execute decisions; they cannot own BellLabs semantics, lifecycle,
  readiness, convergence, admission, settlement, or terminality;
- cross-layer calls use typed contracts and ports, not imports between concrete adapters.

## 3. Accepted Stage 8 target tree

Names below are an **illustrative target projection**, not authorized creation paths. Existing
flat modules move only when touched by an authorized stage slice and protected by
import-compatibility tests. Packages `06B`/`06C`, or an accepted package amendment/spec-ticket,
must freeze exact filenames before any projected module is created.

```text
app/
  server.py                         # sole application composition root / ASGI assembly
  config.py
  preflight.py
  api/                              # BellLabs public facade only
    catalog_compile/
    run_control/
    evidence_artifacts/
    projections_events/
    callbacks/
    schema_grounding/
  domain/                           # existing bounded contexts remain authoritative
    control_plane/
    run_control/
    composition/
    orchestration/
    graph_runtime/
    coordinator/
    operation_execution/
    schema_catalog/
    schema_context/
    schema_grounding/
  application/                      # gradual grouping; no Stage 3 bulk move
    catalog/
    run_control/
    runtime/
    operations/
    orchestration/
    coordinator/
    schema/
    projections/
    capability/
    linked_runs/
    sandbox/
    bridge/
    callbacks/
    evaluation/
  models/                           # persistence-facing document/row shapes
  migrations/                       # application PostgreSQL migrations only
  integrations/
    persistence/                    # PostgreSQL, MongoDB, S3, Redis adapters
    temporal/                       # Temporal client/submit/query adapters, not workflows
    agents/
      langgraph/
      langsmith/
      deep_agents/
      openai/
    providers/
      sandbox/
      mcp/
      neo4j/
  temporal/
    workflows/
      belllabs_run.py
      family/
        stagegraph.py
        goal_directed.py
      operation.py
      linked_run.py
    activities/
    workers/
      coordinator_family.py
      agent_cognitive.py
      ingestion_io.py
      sandbox_external_job.py
      verification_reconciliation.py
    registration/
      workflows.py
      activities.py
      task_queues.py
  agent_server/                     # bounded operation/development assets only
    operations/
    qualification/
    shared/
  mcp/                              # MCP transport facet of BellLabs API/capabilities
tests/
  unit/                             # mirrors app/domain and app/application
  integration/                      # mirrors integrations, persistence, Temporal boundaries
  contract/                         # API, workflow/activity, event, provider contracts
  replay/                           # Temporal replay/versioning fixtures
  acceptance/                       # stage gates and production-shaped verticals
infra/                              # local/runtime infrastructure and initialization
deploy/                             # selected Stage 8 deployment definitions and runbooks
docs/
```

The projected worker filenames represent five logical isolation classes, not authorized module
creation or a premature commitment to queue names, process counts, or AWS services. Queue
selection is compiled from exact bindings. `06B`/`06C`, or an accepted package
amendment/spec-ticket, must freeze their exact paths before creation.

## 4. Path dispositions

| Current or target path | Status | Direction |
|---|---|---|
| `app/` | **KEEP** | Canonical application package in this repository |
| `app/server.py` | **KEEP** | Composition root and BellLabs API assembly |
| `app/domain/` and existing bounded contexts | **KEEP** | Preserve semantic ownership and layering |
| `app/application/*.py` | **REFACTOR LATER** | Group gradually into the target subpackages when stage work touches coherent seams |
| `app/api/` | **REFACTOR LATER** | Modularize into the six transport concerns in Stage 7 |
| `app/models/` | **KEEP** | Persistence-facing shapes; do not turn them into domain authority |
| `app/migrations/` | **KEEP** | Application PostgreSQL migrations only |
| `app/integrations/` | **REFACTOR LATER** | Group persistence, Temporal client, agents, and providers after ports stabilize |
| `app/temporal/` | **KEEP** | Sole macro-runtime implementation; introduce `workflows/`, `activities/`, `workers/`, and `registration/` incrementally |
| projected `app/temporal/workflows/belllabs_run.py` and `operation.py` | **PROPOSED TARGET** | Illustrative Stage 3 destination; create only after `06B`/`06C` or an accepted package amendment/spec-ticket freezes the exact filenames |
| projected `app/temporal/workers/` and `registration/` | **PROPOSED TARGET** | Illustrative five-pool composition and registry; exact modules require package authorization |
| projected `app/application/bridge/` and `app/integrations/agents/local.py` | **PROPOSED TARGET** | Illustrative ownership destinations only; default to extending current flat modules in place until an active package freezes these paths |
| `app/agent_server/` | **REPURPOSE** | Bounded operation graphs, qualification assets, and shared helpers only |
| `app/agent_server/stagegraph/` and `goal_directed/` macro scheduling | **RETIRE AFTER GATE** | Retire only after Temporal parity, replay, recovery, and evidence gates pass |
| `app/mcp/` | **REPURPOSE** | Governed MCP transport/capability facet; never an alternate control plane |
| `app/experiments/` | **RETIRE AFTER GATE** | Promote accepted behavior first; retain evidence until replacement gates pass |
| `tests/` | **KEEP** | Mirror target ownership gradually; preserve replay and acceptance evidence |
| `infra/` | **KEEP** | Local and shared infrastructure |
| `deploy/` | **PROPOSED TARGET** | Create only when Stage 8 selects and proves deployment topology |
| sibling app repository / Agent Server application root / package split | **DO NOT CREATE** | Rejected target; any future exception requires evidence and a new accepted decision |

Allowed statuses mean:

- **KEEP**: preserve the existing path and ownership;
- **REFACTOR LATER**: change only in an authorized package slice;
- **PROPOSED TARGET**: illustrative destination that is not a creation instruction;
- **REPURPOSE**: retain an existing path but narrow its accepted role;
- **RETIRE AFTER GATE**: preserve an existing path and its evidence until its named replacement
  passes semantic parity, failure/recovery, and decommission gates; it does not authorize deletion
  now;
- **DO NOT CREATE**: rejected nonexistent target; creation requires a new accepted decision.

## 5. Canonical module ownership

| Confusing names or alternatives | Canonical ownership and distinction |
|---|---|
| `domain/orchestration` | Pure workflow-family semantics: StageGraph/GoalDirected interpreters, readiness, convergence, and deterministic scheduling decisions. It does not own provider/runtime execution or operation journals/effects. |
| `domain/graph_runtime` | Runtime-neutral assembly definitions, binding and execution identities, provider/runtime facts, submissions, receipts, interventions, resource/wait facts, and lineage envelopes. It does not own the operation journal/effect lifecycle. |
| `domain/operation_execution` | Provider-neutral operation lifecycle, journal, effect, settlement, delegation, workspace, materialization, and execution contracts. This is the sole domain owner for journal/effect lifecycle contracts; application services persist and coordinate them rather than redefining them in `graph_runtime`. |
| Temporal StageGraph vs `agent_server/stagegraph` | `app/temporal/workflows/family/stagegraph.py` is the production durable family workflow and applies the pure interpreter. Agent Server StageGraph is a bounded qualification/operation graph or visualization and must not schedule macro work. |
| `OperationWorkflow` | `app/temporal/workflows/operation.py` owns one independently durable operation lifecycle. Operation meaning, exact assembly, claims, effects, evidence, and settlement remain domain/application authority. |
| coordinator vs orchestration vs dispatch | Coordinator selects/advises through BellLabs API/application commands. Orchestration applies family semantics. Dispatch is an application capability that resolves an exact binding/task queue; Temporal performs the durable dispatch. None bypass admission. |
| integration client vs graph definition | Provider clients live under `app/integrations/agents/` or `providers/`. LangGraph/Deep Agents graph definitions used for bounded cognition live with the bounded operation adapter or `app/agent_server/operations/`; they never become clients or macro schedulers. |
| PostgreSQL vs MongoDB | Application PostgreSQL owns run lifecycle, commands, runtime binding records, inbox/ledger/outbox, claims, effects, budgets, approvals, settlements, and durable product events according to current and Stage 1 contracts. MongoDB owns only immutable semantic definition/configuration records, immutable binding-definition records, document payloads, and snapshot/manifest metadata already assigned to it. Temporal PostgreSQL is separate runtime persistence and is never BellLabs authority. |
| API DTO vs domain contract | API DTOs shape transport under `app/api/`. Domain contracts express authoritative meaning under `app/domain/`. Translate explicitly; do not reuse a transport model merely to make it authoritative. |
| MCP vs REST | REST/streaming through the BellLabs API is the governed public facade. MCP is a bounded transport/capability facet under the same application authorization, compilation, journals, and evidence rules—not a parallel product API. |
| task queues | `app/temporal/registration/task_queues.py` is a projected target pending active-package or accepted spec-ticket authorization, not the current canonical source. Until an accepted package freezes or moves that path, queue names and routing remain in their existing modules/contracts. Exact bindings select a logical pool; workflows/providers/models cannot invent or select undeclared queues. |

## 6. Stage-by-stage organization

### Stage 3 — durable Temporal foundation

Do **not** begin by moving roughly 90 application modules. Unless the active package explicitly
authorizes a target path, extend current flat modules in place. Establish the new seams around
existing code:

1. freeze or extend contracts in `app/domain/orchestration/` and
   `app/domain/graph_runtime/`, and `app/domain/operation_execution/`, preserving the ownership
   boundaries in Section 5;
2. after `06B`/`06C` or an accepted package amendment/spec-ticket freezes exact filenames, add or
   relocate `BellLabsRunWorkflow`, `OperationWorkflow`, worker registration, and task-queue
   contracts under the authorized `app/temporal/` paths;
3. extend existing command/inbox/outbox application modules in place unless that authorization
   explicitly freezes `app/application/bridge/`;
4. extend an existing bounded agent adapter in place unless that authorization explicitly freezes
   `app/integrations/agents/local.py`;
5. prove replay, recovery, deduplication, intervention, settlement-before-readiness, and
   Continue-As-New before broader refactoring.

### Stage 4 — StageGraph family

Add the Temporal StageGraph family workflow around the existing pure interpreter. Use the
generic `OperationWorkflow`; do not duplicate operation lifecycle code. Pass the heterogeneous
`all`/`any`/`minimum(k)` vertical before retiring any Agent Server macro path.

### Stage 5 — GoalDirected and Deep Agents

Add the Temporal GoalDirected family workflow and bounded Deep Agents/LangGraph operation
adapter. Keep planning, tools, subagents, filesystem, and checkpoints operation-local and under
exact bindings. Preserve deterministic convergence and verifier authority outside provider state.

### Stage 6 — remote providers

Group and qualify remote LangSmith, sandbox, callback/poll, and provider-async adapters behind
application ports. Use start-bind-wait/reconcile, stable identities, and BellLabs settlement.
Long-running ambiguity and failure gates must pass before a provider is selectable.

### Stage 7 — API modularization

Refactor the API facade into catalog/compile, run control, evidence/artifacts,
projections/events, callbacks, and schema-grounding modules. Keep routes compatible or migrate
them explicitly. The coordinator, MCP clients, and external clients still enter through the
same application authorities.

### Stage 8 — deploy and decommission

Create `deploy/` only for the selected and tested topology. Prove replay/versioning, rollback,
worker loss, backlog recovery, shadow/canary, SLOs, and cutover. Then retire superseded Agent
Server macro graphs, experiments, compatibility shims, and old worker entry points with
preserved evidence.

## 7. Do not move yet

Until an active package names the move and supplies tests:

- do not bulk-relocate flat `app/application/*.py` modules;
- do not rename existing bounded contexts;
- do not move persistence models into domain packages;
- do not move Temporal workflows into Agent Server or provider packages;
- do not convert `app/agent_server/` into a second application/composition root;
- do not split `app/` into installable packages, services, or repositories;
- do not create duplicate “v2”, “new”, or provider-specific domain contracts;
- do not consolidate application PostgreSQL and Temporal PostgreSQL;
- do not delete experiment, qualification, or compatibility assets before their replacement
  gate records disposition and evidence;
- do not reorganize tests separately from the implementation slice they protect.

For incremental moves, preserve old import paths with narrow compatibility re-exports only
when callers require them, prohibit new imports through those shims, and remove them at an
explicit later gate. Avoid package `__init__` files that re-export broad concrete adapter
surfaces.

## 8. Illustrative target after package authorization

This is an illustrative target projection, not an implementation instruction. After Stage 3 entry
gates pass, `06B`/`06C` or an accepted package amendment/spec-ticket must freeze the exact paths.
Until then, implementation defaults to extending current flat modules in place. The smallest likely
production-shaped target after that authorization is:

```text
app/domain/orchestration/              # extend root/family semantic contracts
app/domain/graph_runtime/               # extend assembly/binding/runtime facts/receipts/lineage
app/domain/operation_execution/         # extend operation/journal/effect/settlement contracts
app/application/bridge/
  commands.py
  inbox.py
  outbox.py
app/integrations/agents/
  local.py
app/temporal/workflows/
  belllabs_run.py
  operation.py
app/temporal/workers/
  coordinator_family.py
  agent_cognitive.py
  ingestion_io.py
  sandbox_external_job.py
  verification_reconciliation.py
app/temporal/registration/
  workflows.py
  activities.py
  task_queues.py
tests/unit/domain/
tests/unit/application/bridge/
tests/integration/temporal/
tests/replay/
tests/acceptance/stage_3/
```

No shown filename is frozen by this document. Packages `06B`/`06C`, or an accepted package
amendment/spec-ticket, must authorize exact creation paths without inverting ownership.

## 9. Canonical context and document lifecycle

Read before implementing, in this order:

1. [Main goal and work-package index](migrations_instructions/implementation_work_packages/00_MAIN_GOAL_AND_INDEX.md)
   (`00`) — accepted target, dependencies, and active package selection.
2. [Global handoff and stage-gate rules](migrations_instructions/implementation_work_packages/01_GLOBAL_HANDOFF_AND_STAGE_GATE_RULES.md)
   (`01`) — evidence and handoff rules.
3. [Owner amendments for Stages 3–6](migrations_instructions/implementation_work_packages/02A_OWNER_AMENDMENTS_FOR_STAGES_3_TO_6.md)
   (`02A`) when applicable — accepted supersessions.
4. **This canonical organization document** for path and ownership decisions.
5. The complete active stage package and all declared dependencies; for the first implementation slice, start with
   [Stage 3 Temporal workflow foundation](migrations_instructions/implementation_work_packages/06B_STAGE_3_TEMPORAL_WORKFLOW_FOUNDATION.md)
   plus its declared dependencies. If a projected path differs, the active package's exact path
   authorization remains implementation authority.
6. The accepted [Temporal, LangSmith, and Deep Agents architecture proposal](TEMPORAL_LANGSMITH_DEEPAGENTS_BELLLABS_BACKEND_ARCHITECTURE_PROPOSAL.md),
   [workflow contract architecture](BELLLABS_AGENT_WORKFLOW_CONTRACT_ARCHITECTURE.md), and
   [contract atlas](BELLLABS_AGENT_WORKFLOW_CONTRACT_ATLAS.md).
7. [Current codebase and domain-workflow guide](CODEBASE_DOMAIN_WORKFLOW_GUIDE.md) for
   **as-built reference only**, followed by executable code and tests.

Document lifecycle:

- **Accepted / normative:** the accepted architecture proposal records the architectural decision;
  `00`, `01`, `02A`, and the active accepted package/dependencies govern implementation sequencing
  and gates; this organization document governs path and ownership decisions; accepted contract
  documents govern their named contracts. A later conflict requires an explicit indexed amendment,
  not an implied change in authority.
- **As-built / reference:** `CODEBASE_DOMAIN_WORKFLOW_GUIDE.md`, executable code, tests, and
  current operator notes. Executable code governs current behavior; it does not silently amend
  the accepted target.
- **Historical / superseded:** old migration recommendations, prior Agent Server macro-runtime
  plans, experiments, scratch notes, and qualification artifacts except for explicitly retained
  evidence.

Historical recommendations and experiments cannot override accepted documents. Conflicts must
be resolved by an explicit amendment in the normative index or active package, not by copying
an older structure into new code.
