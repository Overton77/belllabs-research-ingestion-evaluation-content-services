# Codebase organization implementation supplement

Status: **canonical implementation companion**

Applies to: every package in `implementation_work_packages_v2/`

Organization authority: [Canonical application codebase organization](../../interview_and_research_result_documentation/CANONICAL_APPLICATION_CODEBASE_ORGANIZATION.md)

Exact-path authority: [Implementation readiness contract](IMPLEMENTATION_READINESS.md) and the active `WP-*`

Supersedes as active guidance: the [Stage 0–8 organization compass](../implementation_work_packages/SUPPLEMENT_CODEBASE_ORGANIZATION.md)

## Purpose and precedence

This supplement makes the accepted codebase organization part of package implementation and
acceptance. It is general placement and ownership guidance, not permission for unrelated
reorganization.

Apply authority in this order:

1. canonical `ADR-*`, `SPEC-*`, `REQ-*`, and versioned `CON-*`;
2. the v2 [readiness contract](IMPLEMENTATION_READINESS.md) and complete active `WP-*`;
3. the canonical organization document;
4. this compact implementation test and recommended package tree;
5. as-built guides, code, and tests as current-state evidence only.

The canonical organization document owns the target package structure, semantic ownership, and
dependency direction. The readiness contract and active package own exact filenames, scope,
deletion gates, and acceptance evidence. Where this tree and a frozen readiness path differ, the
readiness contract and active `WP-*` win for that slice.

## Organization invariant

The application remains one repository and one canonical Python package, `app/`, with one
composition root at `app/server.py` and one governed public facade through the BellLabs API.

```text
app/domain <- app/application <- app/api | app/temporal | app/integrations
```

- `domain` owns framework-neutral meaning, invariants, reducers, and pure interpreters.
- `application` owns use cases, ports, transactions, and coordination of domain decisions.
- `api` owns transport shapes and explicit translation at the public boundary.
- `temporal` owns deterministic durable execution mechanics, never BellLabs semantics.
- `integrations` own database, agent, provider, sandbox, MCP, and other adapter implementations.
- `models` own persistence-facing shapes, not domain authority.
- `agent_server` is limited to bounded operation, qualification, development, and shared assets.
- `tests` mirror the ownership seam protected by each implementation slice.

Dependencies point inward. Concrete adapters do not import one another to bypass a port, and
framework, storage, transport, checkpoint, trace, or provider types do not become authoritative
domain contracts.

## Recommended Stage 8 package tree

Converge toward this ownership projection as each `WP-*` authorizes the seam it owns. Create or
group packages incrementally; do not bulk-relocate unrelated modules. Exact filenames below that
also appear in the readiness contract are frozen there; other projected leaf names remain
illustrative until an active package freezes them.

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
  application/                      # gradual grouping when a package touches a coherent seam
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
  acceptance/                       # package gates and production-shaped verticals
infra/                              # local/runtime infrastructure and initialization
deploy/                             # selected deployment definitions and runbooks only when authorized
docs/
```

### Domain package ownership

| Package | Owns | Does not own |
|---|---|---|
| `domain/control_plane/` | immutable definitions, blueprints, compilation contracts | provider or Temporal types |
| `domain/run_control/` | admitted run lifecycle, budgets, transitions, product events | operation journal/effect lifecycle |
| `domain/orchestration/` | pure StageGraph/GoalDirected interpreters, readiness, convergence | provider/runtime execution; journals |
| `domain/graph_runtime/` | assembly/binding identities, runtime facts, receipts, lineage envelopes | operation journal/effect lifecycle |
| `domain/operation_execution/` | operation lifecycle, journal, effect, settlement, workspace, delegation | family scheduling semantics |
| `domain/composition/` | linked-run relationships and result admission | child-run Temporal identity invention |
| `domain/coordinator/` | coordinator semantic contracts | API transport or Temporal mechanics |
| `domain/schema_*` | schema catalog/context/grounding meaning | persistence adapters or public DTOs |

### Application package ownership

Group flat `app/application/*.py` modules into these packages only when an active `WP-*` touches a
coherent seam. Until then, extend the existing flat module in place.

| Package | Owns |
|---|---|
| `application/catalog/` | definition publication and catalog use cases |
| `application/run_control/` | admission, lifecycle commands, budgets, outbox coordination |
| `application/runtime/` | runtime binding records and execution coordination |
| `application/operations/` | provider-neutral operation execution services |
| `application/orchestration/` | family launch preparation; no Temporal implementation |
| `application/coordinator/` | coordinator command coordination through BellLabs API |
| `application/schema/` | schema use cases that apply domain schema contracts |
| `application/projections/` | product projections and query models |
| `application/capability/` | capability resolution and materialization coordination |
| `application/linked_runs/` | linked-run admission and result coordination |
| `application/sandbox/` | sandbox use-case ports and reconciliation coordination |
| `application/bridge/` | command/inbox/outbox bridge surfaces when authorized |
| `application/callbacks/` | callback acceptance and poll/reconcile coordination |
| `application/evaluation/` | evaluation orchestration; not LangSmith authority |

### API, Temporal, integrations, and tests

| Package | Owns | Placement rule |
|---|---|---|
| `api/catalog_compile/` | catalog/compile transport | translate DTOs at the boundary |
| `api/run_control/` | run admission and lifecycle transport | no domain redefinition in routers |
| `api/evidence_artifacts/` | evidence and artifact transport | settle through application authority |
| `api/projections_events/` | projections and event streams | not a second product authority |
| `api/callbacks/` | provider callback ingress | accept only through application ports |
| `api/schema_grounding/` | schema-grounding transport | keep REST the public facade |
| `temporal/workflows/` | durable root, family, operation, linked-run mechanics | replay-safe; no provider I/O |
| `temporal/activities/` | idempotent application-service and adapter calls | all I/O and SDK work |
| `temporal/workers/` | five logical isolation classes | exact queues need package freeze |
| `temporal/registration/` | workflow/activity/queue registries | no undeclared queues |
| `integrations/persistence/` | PostgreSQL, MongoDB, S3, Redis adapters | implement ports only |
| `integrations/temporal/` | Temporal client/submit/query adapters | not workflow implementations |
| `integrations/agents/` | LangGraph, LangSmith, Deep Agents, OpenAI adapters | bounded cognition only |
| `integrations/providers/` | sandbox, MCP client, Neo4j adapters | never grant BellLabs authority |
| `agent_server/operations/` | bounded operation graphs | no production macro scheduler |
| `agent_server/qualification/` | qualification and development assets | retain evidence until gate |
| `mcp/` | governed MCP transport/capability facet | not a parallel control plane |
| `tests/unit/` | domain and application pure tests | mirror owning packages |
| `tests/integration/` | persistence, Temporal, provider boundaries | protect the seam under change |
| `tests/contract/` | API, workflow/activity, event, provider contracts | freeze against drift |
| `tests/replay/` | Temporal replay/versioning fixtures | required for workflow changes |
| `tests/acceptance/` | package gates and production-shaped verticals | record in package evidence |
| `infra/` | local/shared infrastructure | keep app and Temporal PostgreSQL separate |
| `deploy/` | selected deployment topology | create only when authorized |

Projected `temporal/workers/` filenames represent five logical isolation classes, not a premature
commitment to process counts, queue names, or cloud services. Queue selection is compiled from
exact bindings. Do not invent undeclared queues from workflows, providers, or models.

## Package implementation rule

Each `WP-*` must leave the codebase closer to the accepted organization within the seams it owns:

1. Place new behavior with its canonical semantic owner; use only paths authorized by the readiness
   contract or active package.
2. Move or split existing code only when required by that package's cohesive vertical. Do not run a
   parallel bulk reorganization.
3. Prefer the recommended package above when creating a new owner; keep flat modules only when the
   readiness contract still names them or the package has not yet authorized a group move.
4. Route active imports, composition, and worker registration through the canonical owner before
   package acceptance.
5. Remove the superseded executable owner at the package's deletion gate. Historical evidence may
   remain inert and clearly non-normative.
6. Put tests at the corresponding unit, contract, integration, replay, or acceptance boundary and
   record exact changed paths in package evidence.

Do not create sibling application roots, `v2`/`new` domain packages, provider-specific semantic
owners, direct public Temporal/provider surfaces, Agent Server macro schedulers, or a second
persistence authority. A new repository, service, installable-package split, or major target-tree
change requires an accepted architecture decision.

## Placement test

For every new type or behavior, ask in order:

1. **Does it define BellLabs meaning or an invariant?** Put it in the owning `app/domain/` package.
2. **Does it coordinate a use case or persistence port?** Put it in `app/application/`.
3. **Is it a public transport shape?** Put it in the matching `app/api/` package and translate
   explicitly to domain types.
4. **Is it deterministic durable orchestration?** Put it in `app/temporal/`; call activities for I/O.
5. **Is it an SDK, database, agent, sandbox, or provider implementation?** Put it in
   `app/integrations/`.
6. **Is it bounded agent cognition or qualification code?** Keep it in a bounded agent adapter or
   `app/agent_server/`; it may not schedule the BellLabs workflow.

If a module would need to import outward against `domain <- application <- adapters`, the ownership
is wrong or a port is missing.

## Acceptance check

A package is organization-complete only when its evidence shows:

- every changed module has one clear owner and respects inward dependency direction;
- domain and application layers are free of forbidden runtime/provider/transport coupling;
- the real application and worker composition use the canonical path;
- no duplicate active semantic owner or superseded launch path remains;
- tests and evidence protect the new seam; and
- any necessary departure is recorded as an explicit amendment, not hidden in code or an as-built
  guide.

When a placement is not frozen, choose the smallest extension consistent with the recommended
package owner above and record it in the active package evidence. Do not invent filename-level
authority here when the readiness contract or active `WP-*` already freezes the path.

## Source lineage

This document reconciles and represents:

- the accepted [canonical organization](../../interview_and_research_result_documentation/CANONICAL_APPLICATION_CODEBASE_ORGANIZATION.md),
  retained as the normative organization authority; and
- the older [organization compass](../implementation_work_packages/SUPPLEMENT_CODEBASE_ORGANIZATION.md),
  retained as frozen Stage 0–8 provenance and superseded for active implementation guidance.
