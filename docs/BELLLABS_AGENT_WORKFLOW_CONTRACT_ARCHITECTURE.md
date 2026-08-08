# BellLabs agent workflow contract architecture

Status: accepted contract architecture grounded in current code  
Audience: BellLabs maintainers, coordinator agents, workflow authors, and runtime implementers  
Macro runtime: Temporal only; LangGraph + Deep Agents are bounded cognitive runtimes  
Last reviewed: 2026-08-08

## Executive decision

BellLabs should retain two canonical workflow families:

1. `StageGraph`: host-directed, dependency-aware work whose scheduling and acceptance are deterministic.
2. `GoalDirected`: bounded adaptive work whose objective and acceptance envelope are fixed, while an agent proposes the path.

Everything else discussed here is one of four things:

- a Workflow Type, such as schema context selection;
- a variation within a family, such as fan-out/fan-in, event waiting, or a review loop;
- an operation implementation, such as a Deep Agent, native validator, MCP call, or compiled subgraph;
- composition across runs, represented by a linked Workflow Run.

Do not add a third canonical family until it requires a genuinely different outer state machine that cannot be expressed without weakening either `StageGraph` or `GoalDirected` invariants.

The target contract spine is:

```text
WorkflowTypeDefinition                 semantic meaning
  -> WorkflowBlueprint                outer control pattern
  -> WorkflowImplementationDefinition approved exact realization
  -> EffectiveRunConfiguration        compiled semantic authority
  -> RunPlanV3                        compiled runtime mechanics
  -> BellLabsRunWorkflow binding      Temporal root + family + epoch/segment facts
  -> OperationExecutionBinding        Temporal operation child + exact adapter variant
  -> Optional agent binding           local or remote graph/thread/run/checkpoint facts
  -> Operation bindings               exact per-stage execution
  -> Evidence and result admission    BellLabs acceptance and terminality
```

The current repository already contains almost all of this spine. The recommended work is primarily clarification, versioned refinement, and completion of the runtime implementation—not a second contract system.

## 1. What exists today

### 1.1 Semantic control plane

[`app/domain/control_plane/contracts.py`](../app/domain/control_plane/contracts.py) currently defines:

- exact, revisioned, digest-addressed definition references;
- `WorkflowTypeDefinition`;
- `StageGraphBlueprint` and `GoalDirectedBlueprint`;
- `WorkflowImplementationBindingDefinition`;
- control, runtime, workspace, evaluation, workflow-specific, agent, prompt, skill, and MCP definitions;
- compilation inputs and the immutable `EffectiveRunConfiguration` (ERC).

[`app/domain/control_plane/compiler.py`](../app/domain/control_plane/compiler.py) performs pure exact-reference validation, implementation conformance, authority intersection, environment checks, overlay decisions, and ERC construction.

This is the correct semantic authority boundary.

### 1.2 Run lifecycle and linked runs

[`app/domain/run_control/contracts.py`](../app/domain/run_control/contracts.py) owns admission, lifecycle commands, waits, budgets, evidence recording, terminalization proposals, and projections.

[`app/domain/composition/contracts.py`](../app/domain/composition/contracts.py) correctly represents linked runs as a relationship between independently admitted runs. `RunCompositionLink`, dependency revisions, result admission decisions, and continuation state are composition facts—not a third workflow family.

### 1.3 Runtime-neutral graph assembly

[`app/domain/graph_runtime/definitions.py`](../app/domain/graph_runtime/definitions.py) now adds the layer that the earlier proof document identified as missing:

- `AgentHarnessDefinition`;
- `MiddlewareStackDefinition`;
- `ContextPolicyDefinition` and `ContextAssemblySpec`;
- `DelegationPolicyDefinition`;
- interpreter, sandbox, execution-environment, evaluation, and capability definitions;
- `GraphAssemblyDefinition`;
- `StageCapabilityRequirement`;
- `OperationAssemblySpec`;
- `StageExecutionBinding`;
- `ExecutionResourceEnvelope`;
- `GraphAssemblySpecV2` and `RunPlanV3`.

[`app/application/runtime_run_plan.py`](../app/application/runtime_run_plan.py) already proves exact stage/variant coverage, operation-assembly digest agreement, capability readiness checks, and delegation compatibility.

This v3 direction is preferable to the global-default shape in `RunPlan` v2 because each stage can bind a different model, harness, MCP surface, skill set, filesystem policy, sandbox, verifier, and resource envelope.

### 1.4 Agent Server implementation and superseded priority

[`app/agent_server/`](../app/agent_server/) currently provides:

- Agent Server authentication and tenant-scope checks;
- common state and safe reducers;
- runtime bootstrap/reconciliation as the first graph node;
- StageGraph and GoalDirected graph entry points;
- provider-neutral graph execution contracts, interventions, interrupts, async-task projections, streaming envelopes, and checkpoint summaries.

The graph bodies are intentionally skeletal. `StageGraph` presently admits a binding and emits a placeholder event for `next_stage_ref`; `GoalDirected` similarly uses bounded-agent and verifier placeholders. The contracts are ahead of the executable scheduler/harness, which is appropriate at this stage but should be explicit in all architecture discussions.

**Decision history and supersession.** Earlier versions of this document named LangGraph + Deep
Agents on Agent Server as the priority macro runtime. That direction remains recorded because it
explains the current package and contracts, but the 2026-08-08 architecture interview superseded it.
Agent Server may implement a bounded remote operation variant or an interactive development surface;
it must not schedule the authoritative StageGraph or GoalDirected macro lifecycle.

### 1.5 Workspaces and snapshots

[`app/domain/operation_execution/contracts.py`](../app/domain/operation_execution/contracts.py), [`app/application/workspace_materialization.py`](../app/application/workspace_materialization.py), and [`app/application/sandbox_snapshots.py`](../app/application/sandbox_snapshots.py) already establish strong rules:

- logical workspace slots are distinct from provider paths;
- writable slots have explicit owners;
- durable inputs are read-only and digest-bound;
- local outputs are candidates until promoted;
- sandbox snapshots are immutable historical artifacts;
- restore is clone-on-restore;
- credentials, leases, MCP connections, and sockets are reacquired rather than restored.

Preserve these rules when the LangSmith Sandbox provider is added.

### 1.6 Accepted runtime and lifecycle binding

Temporal is the sole macro runtime. Exactly one `BellLabsRunWorkflow` root owns commands, execution
epochs, cancellation, continuation, and the selected StageGraph or GoalDirected family-child
lifecycle. BellLabs pure interpreters and application services decide semantic transitions;
PostgreSQL owns run, command, effect, and settlement authority; Temporal durably executes those
decisions.

The binding hierarchy is:

```text
BellLabs Workflow Run                         PostgreSQL authority
  -> BellLabsRunWorkflow                     one Temporal root
       -> StageGraphWorkflow | GoalDirectedWorkflow
            -> OperationWorkflow             one semantic operation attempt
                 -> local cognitive variant  exact in-worker graph/agent binding
                  | remote cognitive variant exact deployed graph binding
                 -> optional agent thread/run/checkpoint lineage
```

Continue-As-New preserves the BellLabs run and epoch while creating a technical segment and new
Temporal Run ID. A product fork creates a new BellLabs run at epoch `1`. Any semantic work that is
independently messageable, cancellable, resumable, waiting, reusable, or effect-reconciling is a
Temporal child workflow. Each GoalDirected iteration is an operation child.

An agent thread defaults to one semantic operation attempt. Disruptive restart keeps the attempt,
advances an execution/intervention generation, and creates new provider thread/run lineage. Local and
remote cognition are separate exact variants; neither is an implicit fallback for the other.

#### 1.6.1 Communication and exact intervention

The minimal implementation is a BellLabs-authoritative command/message ledger plus inbox and
transactional outbox. Messages target a semantic operation attempt and carry a per-target monotonic
sequence. Delivery is an ordered bounded batch; stale messages are never silently retargeted.
Receipts distinguish `accepted`, `routed`, `runtime_observed`, `model_visible`, `applied`,
`rejected`, `expired`, and `superseded`. `applied` is valid only after the checkpoint containing the
injected messages commits.

The runtime lease/CAS-claims batches through an authorized BellLabs inbox service. Crash recovery
redelivers the same immutable IDs idempotently. Temporal carries IDs and status, not message
payloads. Typed peer input is candidate input only; agent-to-agent waiting requires an explicit
durable wait/dependency, and no message satisfies StageGraph dependencies without settlement. Only
privileged actors may alter user/system/developer prompt authority.

Certified precise injection occurs post-model and pre-tool: checkpoint the response, drain the inbox,
then revalidate or supersede proposed tool/effect calls. Any remote graph may run, but only a
BellLabs-certified graph may advertise this guarantee. Disruptive intervention is a saga:
best-effort cancel, reconcile effects/checkpoint, resume the same attempt in a new generation,
quarantine late output, and apply the orphan-overlap policy. There is no atomic
cancellation-plus-injection guarantee.

#### 1.6.2 Forks, children, sandboxes, and providers

A semantic snapshot is created only at a declared safe point or after quiescence classifies children,
effects, and messages. Active children remain parent-owned. Forks reuse settled compatible results
only and do not copy pending messages implicitly. Continue-As-New reattaches to or reconciles active
children.

A sandbox is owned by operation attempt and execution generation. A snapshot is immutable; live
authority, credentials, leases, and connections are reacquired. Built-in synchronous Deep Agents
subagents are operation-local and non-addressable. A custom BellLabs delegation tool starts a
Temporal child for independent lifecycle; provider asynchronous subagents are subordinate adapters.

## 2. The boundary model

The fastest way to understand the system is to ask which layer owns a fact.

| Layer | Owns | Example | Must not decide |
|---|---|---|---|
| Workflow Type | Why the workflow exists and what counts | purpose, invariants, obligations, output contracts | model, MCP endpoint, graph thread |
| Blueprint | Outer control topology | dependencies, joins, cycles, convergence | exact tool/model/server |
| Workflow Implementation | One approved realization | exact blueprint/control/runtime/workspace/evaluation refs | run-specific authority widening |
| ERC | Effective semantic authority for one launch | intersected capabilities, budgets, concurrency, variants | provider thread/run state |
| RunPlan | Exact executable mechanics | stage assemblies, schemas, manifests, resource envelopes | lifecycle truth or result acceptance |
| Temporal root/family binding | Where this run/epoch/segment executes | root, family child, Temporal workflow/run IDs | semantic success |
| Operation Binding | Exact capability and runtime surface for one semantic attempt | Temporal child, adapter variant, assembly, generation | parent workflow terminality |
| Agent Binding | Optional bounded cognition lineage | local/remote variant, graph, thread, run, checkpoint | lifecycle or settlement |
| Checkpoint | Recoverable runtime position | graph state after a super-step | BellLabs admission, budget, or authority |
| Evidence/Result | Claims produced by execution | typed output, artifact refs, usage, trace refs | self-admission |
| Run control | Authoritative decisions | waits, budget settlement, output readiness, terminality | agent message history |

The core invariant is:

> Runtime components produce typed claims and evidence. BellLabs domain services validate, admit, settle, and terminalize them.

## 3. Canonical workflow families

### 3.1 `StageGraph`

Choose `StageGraph` when the host can name the meaningful phases and enforce their dependencies, even if individual stages are agentic.

Canonical variations include:

| Variation | Representation |
|---|---|
| Linear pipeline | ordinary dependency chain |
| Parallel research lanes | sibling stages plus `all`, `any`, or `minimum` join |
| Map/reduce | mapped stage instances plus fan-in stage; not a family |
| Review/repair loop | stage or workflow cycle policy |
| Human approval | durable wait/interrupt node and decision binding |
| Event-driven continuation | stage enters an authoritative wait and resumes after reconciliation |
| Heterogeneous execution | per-stage native, Deep Agent, MCP, sandbox, compiled graph, async child, or linked-run assemblies |
| Schema selection | domain-specific Workflow Type implemented by a StageGraph |

The outer scheduler must remain capability-mechanics-free. It decides what is runnable; the exact operation assembly decides how a runnable stage executes.

### 3.2 `GoalDirected`

Choose `GoalDirected` when the sequence of steps cannot be usefully authored in advance, but the following can be frozen:

- objective;
- acceptance contract;
- invariants and prohibited work;
- admitted inputs;
- authority and budget;
- iteration and convergence limits;
- independent verifier;
- workspace and handoff behavior.

The agent may revise its plan. It may not revise the launch envelope. A model saying “done,” an empty todo list, or LangGraph reaching `END` is not BellLabs success.

### 3.3 What is not a canonical family

| Concept | Correct abstraction |
|---|---|
| Schema selection | Workflow Type with domain contracts and a StageGraph implementation |
| Deep Agent | operation harness or GoalDirected worker implementation |
| Async subagent | durable child task implementation plus wait/reconciliation |
| Linked run | composition edge between independent Workflow Runs |
| Human-in-the-loop | interrupt/wait/decision policy |
| Map/reduce | StageGraph scheduling variation |
| Evaluation workflow | StageGraph or GoalDirected Workflow Type depending on topology |
| One deterministic function | native operation, not a workflow |

## 4. Recommended StageGraph contract

The present `StageNode` combines topology with several execution hints. Version 2 should make the distinction more visible.

### 4.1 Proposed authored shape

```yaml
stage_graph_blueprint:
  family: stage_graph
  stages:
    - stage_id: semantic_selector
      dependencies:
        - stage_id: materialize_context
          dependency_class: required
      join:
        mode: all
      completion_class: required
      skip_policy: never
      scheduling:
        fairness_group: research
        fairness_priority: 20
        stage_slot_weight: 1
        max_parallel_instances: 1
      requirement_ref: catalog://stage_requirement/schema.semantic_selector@3
      obligation_refs:
        - obligation:semantic-selection:v1
      output_slots:
        - selection_draft
  scheduler_policy:
    max_parallel_stages: 4
    fairness_policy_ref: policy:stage-fairness:weighted-v1
  workflow_cycle_policy: null
```

This changes three things:

1. Replace parallel `depends_on` plus `dependency_classes` collections with typed `StageDependency` records.
2. Rename ambiguous `concurrency_slots` to the fact actually meant.
3. Move operation mechanics to an exact `StageCapabilityRequirement`/`StageExecutionBinding` pair.

### 4.2 Concurrency naming

`concurrency_slots` is too ambiguous because the system has multiple independent capacity dimensions.

Use these names:

| Name | Meaning |
|---|---|
| `max_parallel_stages` | Maximum concurrently running stages in this workflow frontier |
| `stage_slot_weight` | Number of frontier scheduler slots consumed by one stage instance |
| `max_parallel_instances` | Maximum mapped instances of this stage definition |
| `operation_worker_slots` | Workers required by the bound operation |
| `model_call_slots` | Concurrent model-call allowance inside the operation |
| `tool_call_slots` | Concurrent ordinary tool calls |
| `mcp_call_slots` | Concurrent MCP calls |
| `sync_subagent_slots` | Blocking specialist capacity |
| `async_child_slots` | Active durable async children |
| `linked_run_slots` | Independently admitted child runs |
| `resumption_reserve` | Capacity protected so supervisors and waiting work can resume |

`ExecutionResourceEnvelope` already contains most of these lower-level fields. Keep them out of `StageNode`.

### 4.3 Dependencies and joins

Recommended contract:

```python
class StageDependency(Contract):
    stage_id: StageId
    dependency_class: Literal["required", "degradable", "optional", "advisory"]

class StageJoinPolicy(Contract):
    mode: Literal["all", "any", "minimum"]
    minimum_satisfied: int | None = None
```

This is easier for humans and agents to search, diff, and validate than keeping the dependency IDs in one set and their classes in a second map.

### 4.4 Stage implementation selection

The blueprint should name the stage requirement, not the model or agent:

```text
StageNode
  -> StageCapabilityRequirement
       operation contract
       input/output contracts
       required/optional capabilities
       effect class
       allowed delegation modes
       verification/degradation/speculation policy
  -> StageExecutionBinding
       exact OperationAssemblySpec
       input/output projections
       resource envelope
       compatibility key
```

This preserves heterogeneous workflows without contaminating topology with framework-specific fields.

## 5. Agent, harness, and delegation configuration

Do not introduce a single broad `Agent_Config`. It would conflate author intent, exact runtime construction, child policies, and per-run authority.

Use three layers.

### 5.1 `AgentProfileDefinition`: authored reusable profile

Purpose: catalog-level identity and maximum request.

Recommended fields:

```yaml
agent_profile:
  logical_id: agent.schema_researcher
  role: schema_researcher
  prompt_refs: [exact_ref]
  skill_refs: [exact_ref]
  requested_tool_refs: [exact_ref]
  requested_mcp_server_refs: [exact_ref]
  model_selection_policy_ref: exact_ref
  output_contract_ref: exact_ref
  guardrail_refs: [exact_ref]
  maximum_capability_request: authority_ceiling
```

It describes what a reviewed agent profile may request. It does not prove that every requested capability is available or granted for a run.

### 5.2 `AgentHarnessDefinition`: exact reusable runtime construction

Purpose: construct one bounded model/tool loop.

The current runtime definition is close. Prefer exact references over duplicated embedded values:

```yaml
agent_harness:
  harness_kind: deep_agent       # langchain_agent | deep_agent | compiled_graph | pure_operation
  package_lock_digest: sha256:...
  model_binding_ref: content_ref
  prompt_context_ref: content_ref
  middleware_stack_ref: content_ref
  tool_manifest_ref: content_ref
  skill_manifest_ref: content_ref
  filesystem_backend_ref: content_ref
  output_validator_set_ref: content_ref
  framework_default_manifest_digest: sha256:...
```

`default_tools_digest` in the current contract is a useful safeguard. Generalize it to a framework-default manifest that covers all implicit model-visible tools and middleware. Runtime defaults must never appear silently.

### 5.3 `OperationAssemblySpec`: exact per-stage recipe

Purpose: bind the harness and all surrounding policies for one operation contract.

The current `OperationAssemblySpec` is the correct convergence point for:

- model;
- prompt and context;
- ordered middleware;
- tools and MCP;
- skills;
- synchronous and asynchronous children;
- workspace/filesystem and sandbox;
- verifier;
- resource, effect, fallback, tracing, capability, and compatibility policy.

This is the object that answers: “Exactly what can stage X do in this run?”

### 5.4 Subagent taxonomy

Keep these modes separate:

| Mode | Parent behavior | Child continuity | BellLabs representation |
|---|---|---|---|
| Synchronous dictionary subagent | blocks | fresh operation-local invocation | `SyncDictionarySubagent` |
| Synchronous compiled subgraph | blocks | graph-local invocation | `SyncCompiledGraphSubagent` |
| Dynamic interpreter dispatch | blocks inside QuickJS program | repeated sync child calls | interpreter policy plus sync catalog |
| Independent delegated child | continues or waits durably | Temporal child workflow | custom BellLabs delegation tool plus operation binding |
| Provider async subagent | subordinate provider execution | provider thread/run under Temporal child | adapter fact; never the BellLabs lifecycle |
| Linked Workflow Run | independent admission and lifecycle | independent BellLabs run | `LinkedRunSlotConstraint` plus `RunCompositionLink` |

Recommended default isolation:

| Child type | Default workspace/sandbox posture |
|---|---|
| Sync specialist doing read-only research | isolated owned folder in the parent operation workspace; explicit read mounts |
| Sync specialist writing candidates | exclusive child output slot; no shared write |
| Sync child executing untrusted code or unique dependencies | its own sandbox materialization |
| Independent delegated child | its own Temporal child, workspace owner, and usually its own sandbox |
| Linked run | independently compiled workspace and sandbox policy |

Never inherit a parent’s full messages, Store namespace, skills, MCP servers, secrets, writable filesystem, or authority by convenience. Compile a bounded `SubagentContextSlice` and explicit grants.

## 6. Tools, MCP, middleware, and filesystem

### 6.1 Tools need a provider-neutral definition

The control plane currently defines MCP tools specifically, while runtime assemblies refer to generic tool manifests. Add a provider-neutral catalog kind in a versioned contract:

```python
class ToolDefinition(DefinitionBase):
    kind: Literal[DefinitionKind.TOOL]
    tool_name: str
    source: NativeToolSource | LangChainToolSource | DeepAgentsBuiltinToolSource | MCPToolSource
    input_schema_ref: ContentAddressedRef
    output_schema_ref: ContentAddressedRef | None
    effect_class: Literal["pure", "read_only", "idempotent_effect", "consequential_effect"]
    approval_policy_ref: ExactDefinitionRef
    required_capabilities: frozenset[str]
    implementation_digest: str
```

Why a union instead of separate top-level configs:

- workflow authors search for one `ToolDefinition` concept;
- provenance remains explicit through `source.kind`;
- model-visible tool names and schemas can be collision-checked uniformly;
- framework coexistence remains possible without a least-common-denominator adapter.

An `MCPServerDefinition` selects no tool by itself. The compiled tool manifest must list the exact allowed MCP tools.

### 6.2 Middleware is not a tool list

Keep middleware in `MiddlewareStackDefinition`, even when a middleware contributes tools.

Each `MiddlewareBinding` should expose both:

```yaml
middleware_binding:
  middleware_id: summarization.primary
  implementation_ref: content_ref
  hook_phases: [before_model, after_model]
  contributes_tool_refs: []
  contributes_state_channels: [summary_state]
  configuration_digest: sha256:...
  conflicts_with: [summarization.secondary]
```

The compiler should derive and freeze:

- ordered hook execution;
- contributed model-visible tools;
- state-channel additions and reducers;
- prompt additions;
- conflicts and duplicates;
- configuration digests.

This is especially important for filesystem, subagent, summarization, planning, caching, human-in-the-loop, and tool-policy middleware.

### 6.3 A filesystem policy is warranted

The filesystem has three distinct concerns:

1. logical workspace structure;
2. model-visible filesystem operations;
3. provider backend and isolation.

Do not combine them into `Sandbox_Config`.

Recommended split:

```text
WorkspaceTemplateDefinition
  logical slots, paths, access modes, purposes

WorkspaceAccessPolicyDefinition
  read/write/list/search/delete/execute permissions by slot/path and owner

FilesystemBackendBinding
  state | store | local | sandbox | composite routing

SandboxProfileDefinition
  provider, image, packages, network, credentials, resources, snapshot policy
```

Deep Agents filesystem permissions cover its built-in filesystem tools, but they do not automatically govern custom tools, MCP filesystem tools, or arbitrary shell execution. BellLabs policy wrappers remain authoritative.

### 6.4 Stable BellLabs capability names

Use BellLabs capability IDs in semantic contracts and map them to framework tools during assembly:

```text
filesystem.list
filesystem.read
filesystem.search.glob
filesystem.search.grep
filesystem.write
filesystem.edit
filesystem.delete
sandbox.execute
mcp.invoke
agent.delegate.sync
agent.delegate.async
agent.interpreter.quickjs
```

Do not put vendor tool names such as `read_file`, `task`, or `start_async_task` into Workflow Type authority ceilings.

## 7. Sandbox materialization and snapshots

### 7.1 `SandboxProfileDefinition` is a recipe, not a sandbox

The definition should describe the allowed construction:

```yaml
sandbox_profile:
  provider_kind: langsmith_sandbox  # langsmith_sandbox | daytona | custom_container
  base_image_digest: sha256:...
  package_lock_digest: sha256:...
  resource_policy_ref: exact_ref
  network_policy_ref: exact_ref
  mount_policy_ref: exact_ref
  credential_policy_ref: exact_ref
  lifecycle_policy_ref: exact_ref
  snapshot_policy_ref: exact_ref
```

Avoid a mutable `enabled` field in a published definition. Enablement is an environment/capability fact evaluated during compilation and dispatch. A definition may exist even when an environment cannot run it.

### 7.2 Materialize from metadata or from a snapshot?

Both are valid, but they are different operations.

#### Fresh materialization from an exact recipe

```text
SandboxProfileDefinition
+ exact image/package manifests
+ WorkspaceMaterializationManifest
+ admitted durable inputs
+ current environment readiness
+ current authority and secrets-by-reference
-> new SandboxMaterialization
```

Metadata alone is sufficient only when it is a complete, content-addressed recipe and every referenced byte is retrievable. Call this a materialization recipe, not a snapshot.

#### Clone materialization from a snapshot

```text
SandboxSnapshot
+ target workspace contract
+ current compatibility decision
+ current authority
+ reacquired credentials/leases/connections
-> new cloned SandboxMaterialization
```

The snapshot supplies historical filesystem/runtime bytes. It never supplies current authority.

### 7.3 Proposed materialization contract

```yaml
sandbox_materialization_request:
  materialization_id: sbxmat_...
  request_scope: tenant:...
  owner:
    kind: stage
    owner_id: stage:semantic_selector:cycle:1
  sandbox_profile_ref: content_ref
  workspace_manifest_ref: content_ref
  source:
    kind: fresh                 # fresh | snapshot_clone
    snapshot_ref: null
  expected_compatibility_key: belllabs-sandbox-v3:...
  authority_binding_ref: authority-binding:...
  resource_lease_ref: resource-lease:...
  idempotency_key: ...
```

`SandboxMaterialization` is live and ephemeral. `SandboxSnapshot` is immutable and durable. `WorkspaceMaterializationManifest` is the logical file inventory. `LangGraphCheckpoint` is graph state. Never call all four “snapshot.”

All providers are reached through a BellLabs provider-neutral sandbox gateway; provider APIs are not
public BellLabs control surfaces.

### 7.4 Stage-level sandbox configuration

A stage should not embed a second complete sandbox configuration.

Use this flow:

```text
StageCapabilityRequirement
  says sandbox.execute and/or filesystem capabilities are required

StageExecutionBinding -> OperationAssemblySpec
  selects exact workspace policy and SandboxProfileDefinition

Runtime dispatch
  materializes or clones the sandbox for the stage owner
```

This allows different stages to use different sandboxes without duplicating policy inside the blueprint.

### 7.5 Snapshot rules to preserve

- capture files and compatible runtime bytes, never raw secret values;
- record the historical capability shape only as evidence;
- clone to a new workspace identity on restore;
- verify image, package, environment, workspace, and mount compatibility;
- reauthorize network and tools;
- reacquire credentials, leases, MCP sessions, and sockets;
- require artifact promotion for restored/generated outputs;
- never overwrite the source workspace during restore.

## 8. Budget, concurrency, and runtime control

These concepts must remain distinct.

| Contract noun | Meaning | Example |
|---|---|---|
| `BudgetCeiling` | Authored maximum authority | workflow permits at most 100k tokens |
| `BudgetEnvelope` | Admitted hard/soft limits and applicability | this run receives 60k tokens |
| `BudgetReservation` | Runtime claim against the envelope | selector reserves 12k tokens |
| `UsageSettlement` | Accepted actual/pending usage | 9,438 tokens charged |
| `ExecutionResourceEnvelope` | Capacity and quota needs | 1 worker, 2 model slots, 3 child slots |
| `RuntimePolicy` | Timeout, retry, cancellation, wait, and fallback behavior | retry transient provider failure twice |
| `AuthorityCeiling` | Capabilities, maximum budget, and top-level concurrency | caller may use search and 4 stages |

Do not put retry counts into budgets, concurrency into model policy, or sandbox CPU limits into `AuthorityCeiling`.

The effective resource surface is an intersection:

```text
workflow ceiling
∩ implementation policy
∩ caller and parent authority
∩ admitted run envelope
∩ environment readiness
∩ provider quota
∩ current resource leases
```

Revalidation may narrow or block. It may not silently substitute a model/tool/server or widen the frozen surface.

## 9. Deterministic validation architecture

### 9.1 Current state

The repository already contains the seed of the proposed mechanism:

- Pydantic field/model validators for local structural invariants;
- `ExtensionRegistry`, keyed by namespace/schema/discriminator;
- `AdmissionPolicyRegistry`, a deny-by-default map from contract reference to Python callable;
- domain-specific deterministic functions such as `validate_selection(...)`;
- compiler checks that compare exact references, digests, authority, compatibility, and coverage.

The concern is valid: opaque strings such as `admission:...`, `invariant:...`, `verification:...`, and `evaluation:...` need an inspectable, versioned bridge to executable server code.

### 9.2 Recommended split

Use three contract types.

#### `ValidatorDefinition`

Published catalog metadata describing what the validator means:

```yaml
validator_definition:
  validator_ref: validator:schema-selection-membership@2
  purpose: Verify canonical membership and topology closure
  phase: result_validation
  determinism_class: pure
  input_schema_ref: content_ref
  output_schema_ref: content_ref
  finding_code_namespace: schema.selection
  required_context_refs: [schema_catalog]
  implementation_compatibility_key: belllabs-validator-python-v1
```

#### `ValidatorImplementationBinding`

Server/deployment fact binding metadata to installed code:

```yaml
validator_implementation_binding:
  validator_ref: validator:schema-selection-membership@2
  implementation_id: belllabs.schema_context.validate_selection.v2
  implementation_digest: sha256:...
  input_schema_digest: sha256:...
  output_schema_digest: sha256:...
  package_lock_digest: sha256:...
```

The published definition should not contain an import path that the runtime blindly imports. Only startup composition code registers approved callables.

#### `ValidationBinding`

Frozen use of one or more validators in a RunPlan or operation assembly:

```yaml
validation_binding:
  validation_set_id: schema-selection-result-v2
  mode: all
  validators:
    - validator_ref: exact_ref
      implementation_digest: sha256:...
      severity_on_failure: error
  report_schema_ref: content_ref
```

### 9.3 Server-side registry

A server-side map is the right implementation, with stronger keys and startup validation:

```python
class ValidatorImplementationRegistry:
    def register(
        self,
        binding: ValidatorImplementationBinding,
        validator: DeterministicValidator,
    ) -> None: ...

    def resolve(
        self,
        validator_ref: ExactDefinitionRef,
        implementation_digest: str,
    ) -> DeterministicValidator: ...
```

Required behavior:

- duplicate registration fails startup;
- missing enabled validator fails readiness and compilation/admission;
- schema or implementation digest mismatch fails closed;
- registry inventory is exposed as a read-only capability/readiness projection;
- workflow contracts reference exact validator definitions, not function names;
- RunPlan freezes the resolved implementation digest;
- validation reports are content-addressed and preserve the validator set used.

### 9.4 Deterministic means deterministic

A `pure` validator may use only its supplied, content-addressed inputs. It must not read:

- wall-clock time;
- environment variables or secrets;
- mutable database state not represented by an exact snapshot/ref;
- network services;
- random values;
- a model or agent;
- mutable aliases.

If current external state is needed, split the operation:

```text
AttestationProvider / ReadinessCheck performs I/O
  -> immutable observation with timestamp, source, and digest
Pure validator evaluates the observation against the contract
```

This makes replay and disagreement diagnosis possible.

### 9.5 Validation phases

| Phase | Typical validators | Authority |
|---|---|---|
| Schema validation | JSON/Pydantic shape, discriminator, canonical collection rules | local and server |
| Definition validation | cross-ref family, uniqueness, DAG/cycle rules | control plane |
| Compilation validation | publication, exact digests, authority intersection, compatibility | compiler |
| Admission validation | input contract, invariants, evidence availability | run control |
| Pre-execution validation | runtime readiness, resource/secret ref availability, drift | dispatcher |
| Operation result validation | structured output, lineage, citations, effect claims | operation settlement |
| Stage/workflow evaluation | obligation and output evidence | evaluator/host logic |
| Artifact validation | media/schema/checks/provenance | artifact admission |
| Terminalization validation | accepted evidence frontier and readiness | run control |

Do not call every one of these a guardrail. A guardrail is runtime input/output interception around an agent. A deterministic validator is host-owned contract evaluation. An evaluator scores or judges evidence. An admission policy decides whether a run may begin.

### 9.6 Finding shape

All validators should return a shared report shape rather than free-form strings:

```yaml
validation_report:
  report_id: valrep_...
  validation_set_ref: content_ref
  subject_ref: content_ref
  subject_digest: sha256:...
  outcome: passed             # passed | failed | indeterminate
  findings:
    - code: schema.selection.unknown_node_type
      severity: error
      path: /selected_node_types/3
      message: Selected node type is not present in the exact catalog
      validator_ref: exact_ref
      evidence_refs: [schema-catalog:...]
      repair_hint: Choose a node type from /allowed_node_types
  report_digest: sha256:...
```

Stable codes and JSON paths make reports understandable to people, coordinator agents, IDEs, MCP clients, and repair loops.

## 10. Naming conventions

### 10.1 Suffix vocabulary

| Suffix | Use it for | Do not use it for |
|---|---|---|
| `Definition` | Published reusable catalog object | live runtime state |
| `Profile` | Reusable selection/preferences bundle | exact resolved binding |
| `Policy` | Rules, limits, decisions, fallback behavior | provider endpoint or mutable state |
| `Requirement` | Needed capability without implementation choice | installed component |
| `Binding` | Exact association between identities/authority/implementation | general configuration bag |
| `Spec` | Complete compiled construction recipe | authored aspiration |
| `Manifest` | Ordered inventory of exact refs/digests/content | behavior or lifecycle |
| `Envelope` | Bounded facts crossing a boundary | durable entity with behavior |
| `Projection` | Derived read model/cache of authority | source of truth |
| `Snapshot` | Immutable captured restorable bytes/state | current environment facts |
| `Checkpoint` | Runtime execution state position | sandbox files or domain lifecycle |
| `Request` | Command intent | persisted outcome |
| `Decision` | Authoritative accept/reject/defer choice | raw model recommendation |
| `Receipt` | Acknowledgment/idempotent provider result | final domain result |
| `Record` | Durable historical fact | mutable command |
| `Result` | Completed typed outcome | in-progress state |
| `Ref` | Stable pointer; exact when revision+digest included | embedded mutable object |

Avoid generic `Config`, `Manager`, `Data`, `Info`, and `Utils` names. The one established exception is `EffectiveRunConfiguration`, which is a domain term and should remain.

### 10.2 Recommended versioned renames

Do not churn working v1 classes in place. Introduce clearer v2 names where the shape changes.

| Current name | Recommended target name | Reason |
|---|---|---|
| `WorkflowImplementationBindingDefinition` | `WorkflowImplementationDefinition` | every field is already an exact binding; removes the awkward double noun |
| `ControlProfileDefinition` | `WorkflowControlPolicyDefinition` | communicates authority/overlay semantics |
| `RuntimeProfileDefinition` | `RuntimeSelectionPolicyDefinition` | distinguishes authored selection from exact execution environment |
| control-plane `MCPServerDefinition` | `MCPServerCatalogDefinition` | avoids collision with runtime definition |
| graph-runtime `MCPServerDefinition` | `MCPServerBindingSpec` | it is an exact runtime recipe |
| control-plane `EvaluationProfileDefinition` | `WorkflowEvaluationPolicyDefinition` | semantic gates |
| graph-runtime `EvaluationProfileDefinition` | `EvaluatorAssemblyDefinition` | exact runtime evaluator assembly |
| control-plane `ModelPolicy` | `ModelSelectionPolicy` | authored provider/model preferences |
| operation `ModelPolicy` after binding | `ResolvedModelBinding` | exact per-operation mechanics |
| `Sandbox_Config` | `SandboxProfileDefinition` or `SandboxMaterializationRequest` | separates recipe from live instance |
| `Agent_Config` | profile + harness + operation assembly | avoids a cross-layer bag |
| `Filesystem_Config` | `WorkspaceAccessPolicyDefinition` + `FilesystemBackendBinding` | separates permissions from backend |

### 10.3 Identifier grammar

Use lowercase snake/kebab segments consistently by identity type:

```text
logical catalog ID:       schema-context-selection
stage ID:                 structural_validation
capability ID:            filesystem.search.grep
contract ref:             admission:schema-context-selection:v1
policy ref:               policy:async-reconcile:v1
qualified provider ID:    langgraph_thread_id, langgraph_run_id
content digest:           sha256:<64 lowercase hex>
```

Never use an unqualified `run_id`, `thread_id`, `assistant_id`, or `snapshot_id` at a boundary where both BellLabs and provider identities exist.

## 11. Explainability for humans and agents

### 11.1 Put explanation in four places

| Location | What belongs there |
|---|---|
| Pydantic docstring | one-sentence semantic meaning and authority boundary |
| `Field(description=...)` | field-level meaning, units, and non-obvious constraints; appears in JSON Schema |
| adjacent architecture/contract guide | cross-object narrative, examples, lifecycle, and rationale |
| `.cursor/rules` | contribution rules: layering, naming, required tests, forbidden imports/defaults |

`.cursor/rules` should not become a second domain specification. It should point to the authoritative contract guide and enforce how code changes preserve it.

### 11.2 Comment decision boundaries, not syntax

Good comment:

```python
# Historical snapshot capability shape is evidence only. Restore must intersect
# it with current authority and may never recreate a credential or live lease.
capability_shape: SnapshotCapabilityShape
```

Low-value comment:

```python
# Maximum concurrency
max_concurrency: int
```

### 11.3 Generate a searchable contract bundle

Add a build step that emits:

```text
generated/contracts/
  index.json
  workflow-type.schema.json
  stage-graph-blueprint.schema.json
  goal-directed-blueprint.schema.json
  workflow-implementation.schema.json
  run-plan-v3.schema.json
  validation-report.schema.json
  examples/
```

`index.json` should map stable terms to:

- schema ID and version;
- Python class/module;
- definition kind;
- owner layer;
- allowed references;
- validator set;
- example files;
- status (`draft`, `published`, `deprecated`).

This gives people and agents one search surface without duplicating executable schemas.

### 11.4 Submission should be layered

A coordinator should not submit a raw `RunPlan`. Recommended public flow:

```text
1. submit WorkflowDesignDraft
2. receive typed ValidationReport
3. prepare launch from exact selectors and admitted input manifest
4. inspect compiled ERC + RunPlan preview/diff
5. launch immutable ticket
6. receive BellLabs run identity + runtime-binding receipt
```

The preview should answer:

- which aliases resolved to which exact refs;
- which overlays were accepted/rejected/degraded;
- which validators ran;
- effective capabilities and budgets;
- stage-to-operation assembly mapping;
- model-visible tool names;
- middleware order;
- workspace/sandbox materializations;
- concurrency/resource envelope;
- unavailable or fallback surfaces;
- expected output/evidence contracts.

## 12. One complete conceptual example

The example is intentionally compact. Exact runtime submanifests remain separately content-addressed.

```yaml
workflow_type:
  logical_id: schema-context-selection
  purpose: Select the bounded schema context required for a research operation
  input_admission_contract: admission:schema-context-selection:v1
  invariants:
    - invariant:schema-selection-exact-lineage:v1
    - invariant:schema-selection-independent-review:v1
  obligations:
    - obligation:semantic-selection:v1
    - obligation:structural-validation:v1
    - obligation:independent-review:v1
  output_contracts:
    - output:accepted-schema-context-selection:v1

blueprint:
  family: stage_graph
  stages:
    - stage_id: materialize_context
      dependencies: []
      requirement_ref: stage-requirement:materialize-context@2
    - stage_id: semantic_selector
      dependencies:
        - {stage_id: materialize_context, dependency_class: required}
      requirement_ref: stage-requirement:semantic-selector@3
    - stage_id: structural_validation
      dependencies:
        - {stage_id: semantic_selector, dependency_class: required}
      requirement_ref: stage-requirement:structural-validation@2
    - stage_id: independent_reviewer
      dependencies:
        - {stage_id: structural_validation, dependency_class: required}
      requirement_ref: stage-requirement:independent-reviewer@2
    - stage_id: accept_selection
      dependencies:
        - {stage_id: independent_reviewer, dependency_class: required}
      requirement_ref: stage-requirement:accept-selection@2

run_plan_v3:
  workflow_implementation_ref: exact_ref
  graph_assembly:
    stage_execution_bindings:
      - stage_id: materialize_context
        operation_assembly_ref: operation-assembly:native-context-materializer@2
      - stage_id: semantic_selector
        operation_assembly_ref: operation-assembly:deep-agent-schema-selector@3
      - stage_id: structural_validation
        operation_assembly_ref: operation-assembly:native-schema-validator@2
      - stage_id: independent_reviewer
        operation_assembly_ref: operation-assembly:compiled-reviewer-graph@2
      - stage_id: accept_selection
        operation_assembly_ref: operation-assembly:native-selection-acceptor@2

operation_assembly_deep_agent_schema_selector:
  implementation_kind: agent_harness
  model_policy_ref: content_ref
  prompt_manifest_ref: content_ref
  middleware_manifest_ref: content_ref
  tool_manifest_ref: content_ref
  mcp_manifest_ref: content_ref
  skill_manifest_ref: content_ref
  context_assembly_ref: content_ref
  delegation_policy_ref: content_ref
  workspace_policy_ref: content_ref
  sandbox_profile_ref: content_ref
  verifier_ref: content_ref
  resource_envelope_ref: content_ref
  capability_manifest_ref: content_ref

operation_assembly_native_schema_validator:
  implementation_kind: native
  implementation_ref: content_ref
  validator_set_ref: validation-set:schema-selection-structural@2
  verifier_ref: content_ref
  resource_envelope_ref: content_ref
```

The Deep Agent proposes a selection. Trusted host code validates it. A separately bound reviewer evaluates it. Trusted host code accepts it. No agent owns all four roles.

## 13. Professional pipeline patterns worth adopting

Mature research and agent systems generally converge on these practices:

1. **Semantic/runtime separation.** Workflow intent and acceptance remain stable while execution providers evolve.
2. **Immutable, content-addressed assemblies.** Exact prompts, schemas, tools, models, middleware, packages, and evaluators are recoverable from a run.
3. **Deny-by-default registries.** A referenced validator/tool/evaluator is unavailable until an approved implementation with matching schemas/digests is installed.
4. **Typed evidence before acceptance.** Agent prose is a candidate; accepted claims and artifacts retain provenance and validation lineage.
5. **Independent verification for consequential outcomes.** Generation, deterministic validation, review, and acceptance are distinct roles.
6. **Resource hierarchy.** Tenant, run, stage, worker, provider, tool, and child concurrency are separately admitted and measured.
7. **Idempotent effects and settlement.** Technical execution may be at least once; effect claims, artifact promotion, charging, and lifecycle application are idempotent.
8. **Capability manifests and maturity gates.** Preview/beta/entitlement-dependent mechanics are explicit and default off until qualified.
9. **Checkpoint compatibility.** Running work stays pinned to graph, state schema, reducers, operation assemblies, and deployment compatibility.
10. **Generated schemas and examples.** Human documentation and agent tooling derive from executable contracts where possible.
11. **Evaluation as a versioned contract.** Dataset, evaluator, rubric, model/framework, budget, and assembly digests are recorded together.
12. **Research-to-ingestion separation.** Research output does not become canonical biotech knowledge until governed validation and promotion complete.

These practices are already visible in BellLabs. The next step is to make them easier to see and harder to bypass.

### 13.1 Service, persistence, and deployment contract

One modular BellLabs API/control service is the sole governed external API and MCP façade. Internal
provider APIs are restricted. Worker pools are separated into coordinator, agent, ingestion-I/O,
sandbox-control, and verification/reconciliation classes.

The initial path self-hosts Temporal on AWS with persistence and credentials separate from the
BellLabs application PostgreSQL. PostgreSQL remains run authority; existing Mongo and
content-addressed catalogs remain where designed; object storage retains artifacts. ECS, EKS, or EC2
is a Stage 8 evidence decision, not an architecture ambiguity.

LangSmith is required for tracing, evaluation, sandboxes, and selected bounded remote graph
deployments. Studio and the local Graph API are conveniences. Authenticated BellLabs callbacks are
persisted and deduplicated before a transactional outbox signals Temporal. Remote graph execution
always follows start-bind-wait/reconcile; asynchronous activity completion is optional for qualified
callbacks. The BellLabs durable event stream is product status authority.

Temporal worker and operation-assembly versions evolve independently and may bind only through an
exact compatibility manifest.

## 14. Specific recommendations by priority

### P0: document and freeze language

- Accept the seven-layer contract spine in this document.
- Freeze the Temporal root/family/operation hierarchy and record Agent Server-primary supersession.
- Keep only `StageGraph` and `GoalDirected` as canonical workflow families.
- Adopt the suffix vocabulary and qualified identity rule.
- Treat `RunPlanV3` as the target runtime assembly; do not grow v2 global defaults.

### P0: lifecycle, messaging, and operation children

- Implement the distinct `BellLabsRunWorkflow` and family-child bindings.
- Make every independently lifecycle-bearing semantic operation a Temporal child, including goal
  iterations.
- Add the authoritative ledger/inbox/outbox, monotonic sequencing, leases, full receipts, and
  immutable-ID redelivery.
- Add execution generations, certified post-model/pre-tool injection, and disruptive-intervention
  reconciliation.

### P1: deterministic validation surface

- Add `ValidatorDefinition`, `ValidatorImplementationBinding`, `ValidationBinding`, `ValidationFinding`, and `ValidationReport` as versioned contracts.
- Replace string-only registry resolution with exact ref plus implementation digest.
- Add startup/readiness validation and a read-only registry inventory.
- Adapt current `AdmissionPolicyRegistry` and domain validators rather than replacing them all at once.

### P1: StageGraph clarity

- Introduce `StageDependency` and `StageJoinPolicy` in a blueprint v2.
- Deprecate ambiguous `StageNode.concurrency_slots`; introduce `stage_slot_weight` and `max_parallel_instances` only if both semantics are required.
- Keep all lower-level capacity in `ExecutionResourceEnvelope`.

### P1: tool and filesystem manifests

- Add provider-neutral `ToolDefinition`/`ToolManifestDefinition`.
- Add `WorkspaceAccessPolicyDefinition` and `FilesystemBackendBinding`.
- Freeze middleware-contributed tools, prompt text, and state channels in the assembly manifest.

### P2: sandbox provider integration

- Keep BellLabs workspace/snapshot contracts as authority.
- Add `SandboxMaterializationRequest`, `SandboxMaterialization`, and a LangSmith adapter.
- Support both fresh recipe materialization and clone-from-snapshot.
- Reuse the existing clone/compatibility/resource-reacquisition conformance tests.

### P2: searchable contract experience

- Generate JSON Schemas and `index.json` from Pydantic.
- Add one complete example per Workflow Type and per canonical family.
- Return shared `ValidationReport` findings from coordinator validation.
- Add an ERC/RunPlan preview that renders stage-to-assembly and capability diffs.

### P3: governed delegation

- Qualify synchronous subagents first.
- Implement independent delegation through the BellLabs tool that starts Temporal children.
- Keep provider async subagents subordinate to those exact child bindings.
- Keep QuickJS/dynamic delegation separately gated and explicit.
- Use linked runs whenever authority, budget, lifecycle, or result ownership is independently meaningful.

## 15. Open decisions that should remain explicit

These do not block the architecture, but each needs a short ADR before implementation:

1. Whether `WorkflowImplementationBindingDefinition` is renamed only in a v2 schema or retained as the canonical public term.
2. Whether `stage_slot_weight` is needed at all; ordinary stages may all consume one scheduler slot.
3. Whether validator definitions live in the existing control-plane catalog or a dedicated validation catalog projected through it.
4. Whether sync subagents may share one provider sandbox with isolated directories or whether selected security classes always require separate sandboxes.
5. Which sandbox compatibility dimensions permit restore versus require fresh materialization.
6. Which validation reports are durable domain evidence versus diagnostic-only preview records.
7. Which Deep Agents built-in defaults are disabled globally and which may be selected through exact manifests.

## 16. Ecosystem facts checked for this proposal

The BellLabs design above does not delegate authority to the frameworks. These current framework facts influence adapter design:

- Agent Server deploys graphs with persistence and a task queue; assistants, threads, and runs are provider resources, and the server manages checkpoint injection. See [Agent Server](https://docs.langchain.com/langsmith/agent-server) and [Agent Server API reference](https://docs.langchain.com/langsmith/server-api-ref).
- LangGraph checkpoints are thread-organized runtime state used for durability, interrupts, fault recovery, replay, and forks. See [LangGraph persistence](https://docs.langchain.com/oss/python/langgraph/persistence).
- Deep Agents provides pluggable filesystem backends, built-in filesystem tools, optional sandbox `execute`, planning, summarization, and synchronous subagents. See [Deep Agents overview](https://docs.langchain.com/oss/python/deepagents/overview) and [synchronous subagents](https://docs.langchain.com/oss/python/deepagents/subagents).
- Async subagents are currently a preview feature with separate Agent Protocol threads/runs and start/check/update/cancel/list lifecycle tools. See [async subagents](https://docs.langchain.com/oss/python/deepagents/async-subagents).
- QuickJS interpreters are experimental, capability-scoped in-process runtimes rather than full OS sandboxes. See [interpreters](https://docs.langchain.com/oss/python/deepagents/interpreters).
- Deep Agents sandbox backends expose filesystem and shell operations but still require network, secret, and context-injection controls. See [sandboxes](https://docs.langchain.com/oss/python/deepagents/sandboxes).

## 17. Relationship to existing documents

This document updates and specializes, rather than replaces:

- [`CONTROLLED_RUN_PROOF_OF_REPRESENTATION.md`](migrations_instructions/architectural_documents/CONTROLLED_RUN_PROOF_OF_REPRESENTATION.md), which establishes the controlled-run narrative;
- [`APP_RUNTIME_PORTING_REFERENCE.md`](migrations_instructions/APP_RUNTIME_PORTING_REFERENCE.md), which identifies reusable OpenAI Agents SDK/Temporal semantics;
- [`AGENT_FRAMEWORK_COEXISTENCE_STRATEGY.md`](AGENT_FRAMEWORK_COEXISTENCE_STRATEGY.md), which keeps one BellLabs authority across multiple runtimes;
- [`06A_STAGES_3_TO_6_OPERATION_ASSEMBLY_CONCURRENCY_AND_LINEAGE_CONTRACT.md`](migrations_instructions/implementation_work_packages/06A_STAGES_3_TO_6_OPERATION_ASSEMBLY_CONCURRENCY_AND_LINEAGE_CONTRACT.md), which is the normative implementation sequence for the runtime stages.

When code and this proposal differ, current executable contracts remain authoritative until a versioned migration is accepted.
