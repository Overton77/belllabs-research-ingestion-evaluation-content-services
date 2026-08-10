# Agent Server contract implementation strategy

Status: proposed implementation guide  
Scope: where and how to write contracts while building the LangGraph + Deep Agents Agent Server  
Last reviewed: 2026-08-07

## Decision

Use the current BellLabs contracts as the semantic trunk. Add focused modules for genuinely new boundaries, and introduce versioned replacements only when meaning or persisted JSON changes.

Do not create a parallel `new_contracts.py`, a broad `Agent_Config`, or a second contract system for Agent Server.

```text
existing semantic contracts
  -> exact compilation and admission
  -> RunPlanV3
  -> Agent Server operation/scheduler modules
  -> typed evidence and BellLabs settlement
```

## Contract change rule

| Situation | Action |
|---|---|
| Existing contract has the correct meaning | Import and use it directly |
| It needs documentation or an additive invariant | Edit the current contract |
| A new concept has no current owner | Add a focused module under the owning domain |
| Field meaning, discriminator, or persisted shape changes | Add a versioned contract and migration |
| Shape is specific to LangGraph, Deep Agents, or Agent Server | Put it in `graph_runtime` or `agent_server`, not the semantic control plane |
| Shape is specific to Temporal/OpenAI Agents SDK | Leave it in the legacy integration path |
| Only the name is awkward | Defer renaming until the executable path works |

One concept must have one canonical class. Temporary compatibility modules may re-export a class; they must not define competing copies.

## Reuse these contracts

Use the current definitions rather than recreating them:

### Semantic and compilation

- `WorkflowTypeDefinition`
- `StageGraphBlueprint`
- `GoalDirectedBlueprint`
- `WorkflowImplementationBindingDefinition`
- `EffectiveRunConfiguration`
- exact refs, authority ceilings, workspace contracts, linked-run slots

Location: [`app/domain/control_plane/contracts.py`](../app/domain/control_plane/contracts.py)

### Runtime assembly

- `StageCapabilityRequirement`
- `OperationAssemblySpec`
- `StageExecutionBinding`
- `ExecutionResourceEnvelope`
- `GraphAssemblySpecV2`
- `RunPlanV3`
- harness, middleware, context, delegation, sandbox, and capability definitions

Location: [`app/domain/graph_runtime/definitions.py`](../app/domain/graph_runtime/definitions.py)

### Runtime identity and transport

- qualified BellLabs run/epoch, deployment, thread, run, checkpoint, and attempt identities
- `GraphExecutionSubmission`
- `RuntimeExecutionBinding`
- interventions, interrupts, async-task projections, and stream events

Locations: [`app/domain/graph_runtime/identities.py`](../app/domain/graph_runtime/identities.py) and [`contracts.py`](../app/domain/graph_runtime/contracts.py)

### Authority and durable domain state

- run admission, lifecycle, budgets, waits, evidence, output readiness, and terminality;
- linked-run composition and result admission;
- workspace ownership/materialization, artifact promotion, and sandbox snapshots.

Locations:

- [`app/domain/run_control/contracts.py`](../app/domain/run_control/contracts.py)
- [`app/domain/composition/contracts.py`](../app/domain/composition/contracts.py)
- [`app/domain/operation_execution/contracts.py`](../app/domain/operation_execution/contracts.py)

## Add these focused modules

```text
app/domain/validation/
  __init__.py
  contracts.py              validator metadata, bindings, reports, findings

app/application/
  validator_registry.py     approved callable registration and resolution

app/domain/graph_runtime/
  tooling.py                provider-neutral tool definitions/manifests
  filesystem.py             workspace access policy and backend bindings
  sandbox.py                materialization requests/results and compatibility
  execution_plans.py        future provider-qualified plan union

app/agent_server/operations/
  __init__.py
  contracts.py              operation outcomes used by the Agent Server
  compiler.py               RunPlan binding -> executable operation construction
  executor.py               OperationExecutor protocol and dispatch
  native.py                 trusted deterministic/native operations
  deepagents.py             Deep Agents harness adapter

app/agent_server/stagegraph/
  graph.py                  stable graph construction only
  state.py                  compact checkpointed runtime state
  scheduler.py              pure frontier/readiness decisions
  nodes.py                  orchestration nodes using application ports
  routing.py                conditional routing and Send construction
  settlement.py             result reconciliation/application

app/models/
  validation.py             only if validation reports need a Mongo query index
  sandbox_materialization.py only if live sandbox facts are not stored in Postgres
```

Create modules only as their first executable consumer is implemented. Do not scaffold empty abstraction layers far ahead of the vertical slice.

## Layering rules

### `app/domain/`

Contains frozen contracts and pure logic. It must not import LangGraph, Deep Agents, LangSmith, OpenAI Agents SDK, or Temporal.

Use it for:

- semantic definitions;
- exact runtime assembly descriptions;
- identities and immutable records;
- pure validation, scheduling, reduction, and canonical digests.

### `app/application/`

Contains ports and use-case orchestration.

Use it for:

- registries that bind exact references to installed implementations;
- compilation services;
- admission and runtime dispatch;
- resource leasing, reconciliation, and settlement coordination.

### `app/agent_server/`

Contains LangGraph/Deep Agents construction and provider mechanics.

Use it for:

- graph nodes, state, routing, and reducers;
- Deep Agent construction;
- Agent Server runtime context;
- mapping provider events/results into BellLabs application contracts.

It must not become the authority for permissions, budgets, evidence acceptance, or terminality.

### `app/integrations/`

Contains provider clients and storage adapters.

Use it for:

- Agent Server client transport;
- LangSmith sandbox and tracing adapters;
- Mongo/Postgres/object-store persistence;
- legacy OpenAI/Temporal adapters.

## Model and persistence strategy

“Model” must be qualified because the codebase has four different model shapes.

| Shape | Naming | Owner | Purpose |
|---|---|---|---|
| Domain/API Pydantic model | `...Definition`, `...Request`, `...Binding`, `...Result` | `app/domain/` | meaning and deterministic invariants |
| Mongo/Beanie persistence model | `...Document` | `app/models/` | collection shape and indexes |
| Authoritative SQL payload/row | domain contract validated by a Postgres repository | `app/application/postgres_*_repository.py` plus migrations | lifecycle, leases, commands, runtime bindings, settlement |
| LangGraph checkpoint state | `...Input`, `...State`, `...Output` `TypedDict` | `app/agent_server/<graph>/state.py` | compact recoverable runtime position |
| Read model | `...Projection` or `...View` | domain/application query surface | derived status for clients and agents |

Do not use a Beanie `Document` as an API contract, and do not place a domain class in `app/models/` merely because it inherits from Pydantic.

### Persistence authority by concern

| Concern | Recommended store/model |
|---|---|
| Published definitions and authoring heads | existing generic Mongo control-plane documents |
| Large definition/manifests | object store by digest plus a catalog payload reference |
| Capability search projections and external candidates | Mongo/Postgres search projection as currently designed |
| Run admission, lifecycle, budgets, decisions, leases, and outbox | authoritative Postgres records |
| Launch tickets | existing Postgres launch-ticket repository |
| Runtime submissions, bindings, attempts, interventions, and reconciliation | existing Postgres runtime-execution repository |
| LangGraph messages and node state | Agent Server checkpoint persistence; projection only |
| Workspace materialization manifests | existing Mongo workspace manifest documents |
| Immutable sandbox snapshots and clone history | existing Mongo snapshot documents plus object-store payload |
| Artifact/effect/usage settlement | authoritative Postgres journal plus immutable payload refs |
| Traces and evaluator telemetry | LangSmith; reference from BellLabs evidence where required |

### Reuse generic control-plane documents

New definition kinds such as `tool`, `validator`, or `workspace_access_policy` normally do **not** require new Mongo classes.

They should use:

- `DefinitionHeadDocument` for the current authoring head;
- `PublishedDefinitionDocument` for exact revisions;
- `DefinitionAliasDocument` and movement history for aliases;
- existing payload externalization for large content.

The `definition` payload is validated into the discriminated domain `Definition` union on repository read. Add a dedicated document only when the asset needs materially different lifecycle/index/query semantics that the catalog projection cannot provide.

### Agent configuration models

Do not add `AgentConfigDocument`.

Persist the layers that already express the agent:

```text
AgentProfileDefinition          published catalog definition
AgentHarnessDefinition          content-addressed runtime definition
OperationAssemblySpec           content-addressed per-operation recipe
StageExecutionBinding           frozen inside GraphAssemblySpecV2/RunPlanV3
RuntimeExecutionBinding         authoritative Postgres runtime record
```

A live Deep Agent object is constructed from these records and is never itself serialized as BellLabs authority.

### Validation models

Use `ValidationReport` as the canonical domain contract. Persistence depends on consequence:

```text
preview-only report
  -> return directly; optionally index for diagnostics

admission/result/artifact/terminality report
  -> store immutable report payload by digest
  -> record subject, validator set, report ref/digest, outcome, and decision lineage
     in the authoritative Postgres transaction or outbox
```

If search across diagnostic reports is required, `app/models/validation.py` may define a small `ValidationReportIndexDocument`. It should contain IDs, refs, digests, outcome, codes, timestamps, and scope—not duplicate the complete authoritative payload.

The callable `ValidatorImplementationRegistry` is deployment composition, not Mongo data. Published validator metadata may be stored in the generic control-plane definition documents; installed callable readiness is an observed runtime projection.

### Sandbox models

Keep these distinct:

| Model | Persistence |
|---|---|
| `SandboxProfileDefinition` | generic definition/content-addressed catalog |
| `SandboxMaterializationRequest` | command; persisted only through its idempotent application record |
| `SandboxMaterialization` | live operational binding; Postgres if it participates in leases/reconciliation |
| `WorkspaceMaterializationManifest` | existing Mongo manifest document |
| `SandboxSnapshot` | existing Mongo snapshot document plus object-store bytes |
| `SnapshotCloneRecord` | existing Mongo clone document |

Do not put live credentials, environment variables, sockets, leases, or MCP sessions in any of these persisted payloads. Store only safe references and reacquisition evidence.

### LangGraph state models

Checkpoint state must stay small and reconstructable. Store:

- BellLabs run/epoch and exact plan/assembly digests;
- compact scheduler state and semantic keys;
- authoritative projection references and versions;
- pending decision/wait/task references;
- result/evidence references;
- reducer-safe event references.

Do not store:

- ERC or RunPlan bodies that can be loaded by digest;
- secret values or credentials;
- full sandbox archives;
- raw private corpora;
- mutable database documents;
- lifecycle or budget authority that can drift from Postgres;
- unbounded tool results when an artifact/context reference is sufficient.

The existing runtime bootstrap must reconcile checkpoint projections against Postgres before any graph advancement.

### Persistence model rule

Before creating a new class under `app/models/`, answer all five questions:

1. What durable query or uniqueness requirement cannot be served by an existing generic document or SQL repository?
2. Is Mongo, Postgres, the object store, Agent Server persistence, or LangSmith the actual authority?
3. Which domain contract validates the stored payload on read?
4. Which fields are indexes versus authoritative content?
5. What is the migration and retention policy?

If those answers are unclear, create the domain contract and repository port first—not a database model.

## Versioning rule

Edit a contract in place only when old serialized payloads retain the same meaning and remain valid.

Add a new schema version when changing:

- field meaning or units;
- required fields;
- discriminators or definition kinds;
- canonicalization or digest inputs;
- persisted nesting;
- identity grammar;
- snapshot/checkpoint compatibility.

For the proposed StageGraph refinement:

```text
StageGraphBlueprint v1 ─┐
                        ├─> normalized internal StageGraph spec -> scheduler
StageGraphBlueprint v2 ─┘
```

V2 may introduce typed `StageDependency`, `StageJoinPolicy`, `stage_slot_weight`, and `max_parallel_instances`. Do not silently reinterpret the current `concurrency_slots` field.

Historical definitions must remain readable even after v1 authoring is retired.

## Agent Server operation boundary

The StageGraph scheduler should depend on one runtime-neutral operation port:

```python
class OperationExecutor(Protocol):
    async def execute(
        self,
        request: StageOperationRequest,
        binding: StageExecutionBinding,
        resources: ExecutionResourceLease,
        cancellation: CancellationContext,
    ) -> OperationExecutionOutcome: ...
```

Adapters implement this port:

```text
NativeOperationExecutor
DeepAgentOperationExecutor
CompiledGraphOperationExecutor
MCPBackedOperationExecutor
AsyncChildOperationExecutor
LinkedRunOperationExecutor
```

The scheduler decides **when** work may run. `OperationAssemblySpec` decides **how** it runs. Run control decides whether its result is accepted and whether the BellLabs run may advance.

## Validation implementation

Add these contracts before expanding agent-generated outputs:

```text
ValidatorDefinition
ValidatorImplementationBinding
ValidationBinding
ValidationFinding
ValidationReport
```

`validator_registry.py` maps an exact validator reference plus implementation digest to an approved callable. Registration must fail on duplicates, and resolution must fail closed on missing or mismatched implementations.

Published workflows reference validator definitions—not Python import paths. The compiled RunPlan freezes the resolved implementation digest.

## First vertical slice

Implement schema context selection first:

```text
materialize_selection_context       native operation
  -> semantic_selector              Deep Agent operation
  -> structural_validation          native validator
  -> independent_reviewer           Deep Agent or compiled graph
  -> accept_selection               native operation
```

Required sequence:

1. Load and verify the exact `RunPlanV3` during runtime bootstrap.
2. Implement a pure StageGraph frontier scheduler using the current blueprint.
3. Reserve authoritative resources before dispatch.
4. Implement `OperationExecutor` and native operation outcomes.
5. Port materialization, structural validation, and acceptance as native operations.
6. Compile and execute the selector through the Deep Agents adapter.
7. Add the separately bound reviewer.
8. Validate and settle typed stage results, evidence, and usage.
9. Connect waits, cancellation, budgets, output readiness, and terminalization.
10. Prove checkpoint resume and idempotent settlement.

After this works, add MCP, sandbox materialization, synchronous subagents, async children, and finally separately qualified QuickJS/dynamic delegation.

## Temporal/OpenAI transition

Select only `langgraph_agent_server` in active runtime composition while this path is built.

- Stop creating new Temporal submissions at the coordinator dispatch boundary.
- Keep Temporal/OpenAI code as prior art until parity evidence exists.
- Do not import Temporal or OpenAI SDK types into new domain modules.
- Do not delete historical contracts or persisted data solely to simplify Agent Server.
- Later implement an OpenAI/Temporal execution-plan variant against the same BellLabs ports.

Breaking the legacy execution API is acceptable. Breaking semantic authority, exact references, persisted definitions, or evidence lineage is not.

## Review checklist

Before adding or changing a contract:

- [ ] Is there already a canonical contract with the same meaning?
- [ ] Is the owner domain/application/runtime/integration layer clear?
- [ ] Does the name use `Definition`, `Policy`, `Requirement`, `Binding`, `Spec`, `Manifest`, `Projection`, `Snapshot`, or `Result` precisely?
- [ ] Are framework-specific fields outside the semantic control plane?
- [ ] Are permissions, budget, and resources references or compiled grants rather than runtime defaults?
- [ ] Does a shape change require a new schema version?
- [ ] Are exact refs/digests and compatibility keys preserved?
- [ ] Can the contract be rendered as JSON Schema with useful field descriptions?
- [ ] Does at least one executable consumer and test justify the new abstraction?
- [ ] Can runtime completion occur without bypassing BellLabs validation and settlement?
- [ ] Is its persistence authority explicitly Mongo, Postgres, object store, checkpoint store, LangSmith, or none?
- [ ] If a `...Document` is added, does it index rather than redefine the domain contract?

## Related documents

- [`BELLLABS_AGENT_WORKFLOW_CONTRACT_ARCHITECTURE.md`](BELLLABS_AGENT_WORKFLOW_CONTRACT_ARCHITECTURE.md)
- [`BELLLABS_AGENT_WORKFLOW_CONTRACT_ATLAS.md`](BELLLABS_AGENT_WORKFLOW_CONTRACT_ATLAS.md)
- [`APP_RUNTIME_PORTING_REFERENCE.md`](migrations_instructions/APP_RUNTIME_PORTING_REFERENCE.md)
- [`AGENT_FRAMEWORK_COEXISTENCE_STRATEGY.md`](AGENT_FRAMEWORK_COEXISTENCE_STRATEGY.md)
