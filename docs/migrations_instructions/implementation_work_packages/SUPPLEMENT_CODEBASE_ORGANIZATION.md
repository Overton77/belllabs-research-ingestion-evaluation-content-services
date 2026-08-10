# Supplement — codebase organization compass

Status: **frozen historical implementation aid; superseded for active work**

Audience: implementation agents working sequentially from Stage 3 through Stage 8  
Last reconciled: 2026-08-08

> Active v2 work uses the
> [canonical implementation supplement](../implementation_work_packages_v2/SUPPLEMENT_CODEBASE_ORGANIZATION.md).
> This Stage 0–8 compass remains provenance and cannot authorize implementation.

## 1. Purpose and precedence

Use this document to answer two questions quickly: **where does a change belong?** and **which
layer may own the decision?** It summarizes the current transition state; it does not replace a
stage package or freeze a projected filename.

If guidance conflicts, use this order:

1. [`00_MAIN_GOAL_AND_INDEX.md`](00_MAIN_GOAL_AND_INDEX.md), global gates, and accepted owner amendments;
2. the complete active stage package and its declared dependencies;
3. [`CANONICAL_APPLICATION_CODEBASE_ORGANIZATION.md`](../../interview_and_research_result_documentation/CANONICAL_APPLICATION_CODEBASE_ORGANIZATION.md);
4. accepted architecture and contract documents;
5. this compass, the as-built guide, executable code, and tests.

The active package is implementation authority. A target tree in a planning document is not, by
itself, permission to move or create modules.

## 2. Architecture in one screen

```text
BellLabs API/control service                      sole governed public facade
  -> application services + PostgreSQL           admission and product authority
  -> BellLabsRunWorkflow                         stable Temporal root
       -> StageGraphWorkflow | GoalDirectedWorkflow
            -> OperationWorkflow                 independently durable operation
                 -> native | Deep Agents/LangGraph | MCP | sandbox | remote provider
```

The dependency direction is:

```text
app/domain <- app/application <- app/api | app/temporal | app/integrations
```

- **Temporal is the sole production macro-workflow executor.**
- **Deep Agents/LangGraph are bounded operation runtimes inside the Temporal hierarchy.**
- Pure `StageGraphInterpreter` and `GoalDirectedInterpreter` logic owns readiness, scheduling,
  convergence, and semantic transitions.
- BellLabs application services and PostgreSQL own admission, budgets, commands, claims, effects,
  evidence acceptance, settlement, and terminality.
- Temporal history is replay truth, not the public product query model.

The Q/D reference workflows from `00A` live in the normal definition/implementation, application,
persistence, workflow, adapter, and evaluation modules indicated below. Do not create `demo/`,
`examples/`, or one-off worker/provider code that bypasses production-shaped seams. Sanitized
fixtures and evidence may have dedicated test/evidence directories, but execution uses the same
ports and registrations.

## 3. Current codebase map

The repository is intentionally in transition. Extend current modules in place unless the active
package freezes a move.

| Path | Current responsibility | Rule while implementing |
|---|---|---|
| `app/domain/control_plane/` | immutable definitions, blueprints, compilation contracts | reusable semantic/catalog meaning only |
| `app/domain/run_control/` | admitted run lifecycle, budgets, transitions | no provider or Temporal types |
| `app/domain/orchestration/` | pure StageGraph and GoalDirected state/interpreters | deterministic; no I/O or SDK calls |
| `app/domain/graph_runtime/` | exact assemblies/bindings, runtime facts, identities, interventions, resource and lineage envelopes | runtime-neutral; not operation-journal authority |
| `app/domain/operation_execution/` | operation request/binding/result, workspace, delegation, journal/effect/settlement semantics | sole domain owner of operation/effect lifecycle |
| `app/domain/composition/` | linked-run relationships and result admission | use when work is a separately governed BellLabs run |
| `app/application/` | use cases, ports, repositories, admission, dispatch, reconciliation | coordinate domain decisions; do not redefine them |
| `app/models/` | database row/document representations | storage shape is not a domain contract |
| `app/api/` and `app/server.py` | BellLabs transport DTOs/routes and composition root | public entry remains BellLabs API only |
| `app/temporal/` | workflows, activities, worker composition, Temporal adapters currently in flat modules | sole macro runtime; move gradually when authorized |
| `app/integrations/` | PostgreSQL/Mongo/S3/Neo4j/agent/provider adapters | implement ports; never grant authority |
| `app/agent_server/` | bounded operation, qualification, development, and shared assets | no production StageGraph/GoalDirected scheduler |
| `app/mcp/` | governed transport/capability facet | not a parallel control plane |
| `app/experiments/` | prototypes and retained evidence | promote behind gates; do not treat as production authority |
| `app/migrations/` | BellLabs application PostgreSQL migrations | never mix with Temporal persistence migrations |
| `tests/` | current unit/integration/contract/replay/acceptance evidence | reorganize only with the protected implementation slice |
| `infra/` | local/shared infrastructure | keep application and Temporal PostgreSQL separate |
| `deploy/` | Stage 8 selected deployment topology | do not create before Stage 8 authorizes it |

## 4. Placement test

For every new type or behavior, ask in order:

1. **Does it define BellLabs meaning or an invariant?** Put it in the owning `app/domain/` bounded context.
2. **Does it coordinate a use case or persistence port?** Put it in `app/application/`.
3. **Is it a public transport shape?** Put it in `app/api/` and translate explicitly to domain types.
4. **Is it deterministic durable orchestration?** Put it in `app/temporal/`; call activities for I/O.
5. **Is it an SDK, database, agent, sandbox, or provider implementation?** Put it in `app/integrations/`.
6. **Is it bounded agent cognition or qualification code?** Keep it in a bounded agent adapter or
   `app/agent_server/`; it may not schedule the BellLabs workflow.

If a module would need to import outward against `domain <- application <- adapters`, the ownership
is wrong or a port is missing.

## 5. Stage-by-stage working surface

| Stage | Primary working surface | Do not broaden into |
|---|---|---|
| 3 | core runtime-neutral contracts; PostgreSQL authority; root/family/operation Temporal kernel; messaging, intervention, recovery, worker registration | bulk source moves, StageGraph feature expansion, remote-provider production paths |
| 4 | Temporal StageGraph family around the pure interpreter; generic `OperationWorkflow`; heterogeneous vertical | duplicate operation lifecycle or Agent Server macro scheduler |
| 5 | Temporal GoalDirected family; bounded Deep Agents harness; verifier, rollover, governed delegation | model-owned convergence or independently durable built-in subagents |
| 6 | exact local/remote provider variants; LangSmith; sandboxes; callback/poll reconciliation; long-run qualification | provider authority or provider-specific domain contracts |
| 7 | modular BellLabs API; coordinator integration; product projections/events; observability, evaluation, security | direct public Temporal/provider/Agent Server APIs |
| 8 | selected AWS topology; replay/versioning; shadow, canary, rollback, drain and decommission | premature service split or deletion before accepted evidence |

Every stage working surface also includes its Q/D blueprint increment, fixture/live runner,
comparison manifest, and capability delta. Placement follows the owning architectural plane, not
the fact that an asset belongs to a reference workflow.

## 6. Temporal and agent placement rules

| Concern | Correct owner |
|---|---|
| macro lifecycle, durable timers, retries, cancellation, Continue-As-New | Temporal workflows |
| stage readiness or goal convergence | pure domain interpreter |
| one durable semantic operation | `OperationWorkflow` |
| model loop, tool use, skills, bounded filesystem/context | Deep Agents/LangGraph operation adapter |
| operation-local synchronous specialist | built-in Deep Agents subagent |
| independently addressable/cancellable/capacity-accounted child | Temporal child via BellLabs delegation |
| exact binding, authority and budgets | BellLabs compilation/admission/application services |
| effects and exactly-once product settlement | BellLabs journal/claim/settlement authority |
| traces and evaluations | LangSmith, as non-authoritative evidence |

Workflow code must remain replay-safe: no network, filesystem, wall-clock, randomness, provider SDK,
or database I/O outside Temporal-safe APIs and activities. Histories carry compact identifiers,
digests, decisions, and bounded summaries—not secrets, PHI, corpora, transcripts, or large artifacts.

## 7. Drift protocol

Documentation drift is expected during prototyping. Handle it explicitly and cheaply:

1. Implement only within the active package's authority and preserve unrelated worktree changes.
2. When code and this compass differ, record the exact path and whether it is `current`, `target`,
   `compatibility`, `experiment`, or `retire-after-gate`.
3. Update this compass for navigation changes; update the canonical organization document only for
   an accepted ownership/path decision.
4. Amend the active package or index when architecture, sequencing, or a gate changes. Do not bury
   an architectural decision in an as-built guide.
5. Keep narrow compatibility re-exports when a move requires them; prohibit new callers and retire
   them only at a named later gate.
6. Every stage handoff lists exact changed paths, migrations, tests, evidence, compatibility impact,
   residual risks, and the next stage's first safe action.

Also compare Q/D blueprint and implementation digests. Never resolve code drift by mutating a
published blueprint, moving a call into a demo path, or weakening current-offer/current-ownership
semantics.

## 8. Agent kickoff checklist

- Identify the active package and verify all direct-dependency handoffs.
- Read this compass and [`SUPPLEMENT_APPLICATION_CONTRACT_ENHANCEMENTS.md`](SUPPLEMENT_APPLICATION_CONTRACT_ENHANCEMENTS.md).
- Inspect `git status`; existing changes belong to the user unless proven otherwise.
- Search current domain/application contracts before creating a new noun or `V2` type.
- State the smallest vertical slice and its requirements-to-evidence rows.
- Keep Temporal orchestration deterministic and provider work in activities/adapters.
- Verify authority, idempotency, recovery, cancellation, lineage, redaction, and settlement—not only
  the happy path.
- Update the outgoing handoff before authorizing the next stage.

## 9. Deeper references

- [`CANONICAL_APPLICATION_CODEBASE_ORGANIZATION.md`](../../interview_and_research_result_documentation/CANONICAL_APPLICATION_CODEBASE_ORGANIZATION.md)
- [`BELLLABS_AGENT_WORKFLOW_CONTRACT_ATLAS.md`](../../interview_and_research_result_documentation/BELLLABS_AGENT_WORKFLOW_CONTRACT_ATLAS.md)
- [`BELLLABS_AGENT_WORKFLOW_CONTRACT_ARCHITECTURE.md`](../../interview_and_research_result_documentation/BELLLABS_AGENT_WORKFLOW_CONTRACT_ARCHITECTURE.md)
- [`CODEBASE_DOMAIN_WORKFLOW_GUIDE.md`](../../interview_and_research_result_documentation/CODEBASE_DOMAIN_WORKFLOW_GUIDE.md) — as-built reference
- [`06A_STAGES_3_TO_6_OPERATION_ASSEMBLY_CONCURRENCY_AND_LINEAGE_CONTRACT.md`](06A_STAGES_3_TO_6_OPERATION_ASSEMBLY_CONCURRENCY_AND_LINEAGE_CONTRACT.md)
