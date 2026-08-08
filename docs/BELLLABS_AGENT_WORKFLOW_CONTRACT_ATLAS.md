# BellLabs agent workflow contract atlas

Status: accepted quick-reference companion to [`BELLLABS_AGENT_WORKFLOW_CONTRACT_ARCHITECTURE.md`](BELLLABS_AGENT_WORKFLOW_CONTRACT_ARCHITECTURE.md)  
Audience: workflow authors, coordinator agents, reviewers, and implementers

Decision history: earlier atlas revisions placed Agent Server directly above the macro scheduler.
That evaluated direction is preserved as history but was superseded on 2026-08-08. Temporal is the
sole macro runtime; Agent Server is an optional bounded remote operation runtime.

## The system in one screen

```text
AUTHORING / CATALOG

WorkflowTypeDefinition
  meaning, admission, invariants, obligations, outputs, authority, linked-run slots
        |
        +-- StageGraphBlueprint
        |     deterministic outer topology
        |
        +-- GoalDirectedBlueprint
              bounded adaptive outer topology
        |
WorkflowImplementationDefinition
  approved exact realization of the Workflow Type

                          compile
                             |
                             v
LAUNCH / AUTHORITY

EffectiveRunConfiguration (ERC)
  exact sources + effective authority + selected variants + workspace/evaluation policy
                             |
                             v
RunPlanV3
  exact per-stage requirements + operation assemblies + resource/compatibility manifests
                             |
                           admit
                             |
                             v
BellLabs Workflow Run
  authoritative lifecycle + budgets + waits + evidence + terminality
                             |
                          dispatch
                             |
                             v
RUNTIME

BellLabsRunWorkflow binding
  one Temporal root + BellLabs run/epoch + technical segment/Temporal Run ID
                             |
                 +-----------+-----------+
                 |                       |
       StageGraphWorkflow        GoalDirectedWorkflow
                 |                       |
                 +-----------+-----------+
                             |
       OperationWorkflow (semantic operation attempt)
          exact execution/intervention generation
                             |
                    OperationAssemblySpec
         native | local cognition | remote graph | MCP | sandbox
                             |
              optional agent thread/run/checkpoint binding
                             |
                   typed result + evidence
                             |
                    BellLabs validation
                             |
                  settlement / terminality
```

Runtime invariants:

- one `BellLabsRunWorkflow` owns commands, epochs, cancellation, continuation, and family children;
- Continue-As-New preserves BellLabs run + epoch and creates only a technical segment/new Temporal
  Run ID;
- a product fork creates a new BellLabs run at epoch `1`;
- independently messageable, cancellable, resumable, waiting, reusable, or effect-reconciling
  semantic work is a Temporal child; each goal iteration is an operation child;
- one agent thread normally maps to one semantic attempt; disruptive restart keeps the attempt,
  advances generation, and starts new thread/run lineage;
- local and remote cognition are exact separate variants.

Authority invariant:

```text
pure interpreters/application services decide semantic transitions
PostgreSQL owns run + command + effect + settlement authority
Temporal durably executes
LangGraph/Deep Agents own bounded cognition + checkpoints
LangSmith is required for traces + evaluation + sandboxes + selected remote graphs
Studio/local Graph API are conveniences
```

## Where to search in code

| Question | Contract or service | File |
|---|---|---|
| What does this workflow mean? | `WorkflowTypeDefinition` | [`app/domain/control_plane/contracts.py`](../app/domain/control_plane/contracts.py) |
| What are the two outer workflow shapes? | `StageGraphBlueprint`, `GoalDirectedBlueprint` | [`app/domain/control_plane/contracts.py`](../app/domain/control_plane/contracts.py) |
| Which exact implementation was approved? | `WorkflowImplementationBindingDefinition` | [`app/domain/control_plane/contracts.py`](../app/domain/control_plane/contracts.py) |
| What was compiled for this launch? | `EffectiveRunConfiguration` | [`app/domain/control_plane/contracts.py`](../app/domain/control_plane/contracts.py) |
| How is exact compilation performed? | `compile_effective_run_configuration` | [`app/domain/control_plane/compiler.py`](../app/domain/control_plane/compiler.py) |
| How is each stage implemented? | `StageCapabilityRequirement`, `StageExecutionBinding` | [`app/domain/graph_runtime/definitions.py`](../app/domain/graph_runtime/definitions.py) |
| What exactly does an operation receive? | `OperationAssemblySpec` | [`app/domain/graph_runtime/definitions.py`](../app/domain/graph_runtime/definitions.py) |
| What is the frozen runtime plan? | `RunPlanV3` | [`app/domain/graph_runtime/definitions.py`](../app/domain/graph_runtime/definitions.py) |
| How is a v3 plan compiled? | `compile_structural_graph_assembly`, `compile_run_plan_v3` | [`app/application/runtime_run_plan.py`](../app/application/runtime_run_plan.py) |
| Who owns lifecycle and budgets? | `RunProjection`, `BudgetState`, lifecycle actions | [`app/domain/run_control/contracts.py`](../app/domain/run_control/contracts.py) |
| What is the accepted macro hierarchy? | Temporal root/family/operation decision | [`TEMPORAL_LANGSMITH_DEEPAGENTS_BELLLABS_BACKEND_ARCHITECTURE_PROPOSAL.md`](TEMPORAL_LANGSMITH_DEEPAGENTS_BELLLABS_BACKEND_ARCHITECTURE_PROPOSAL.md) |
| How are child runs related? | `RunCompositionLink`, result admission decisions | [`app/domain/composition/contracts.py`](../app/domain/composition/contracts.py) |
| What are provider thread/run facts? | `RuntimeExecutionBinding` and identities | [`app/domain/graph_runtime/contracts.py`](../app/domain/graph_runtime/contracts.py), [`identities.py`](../app/domain/graph_runtime/identities.py) |
| What does the Agent Server run first? | runtime bootstrap reconciliation | [`app/application/runtime_bootstrap.py`](../app/application/runtime_bootstrap.py), [`app/agent_server/bootstrap.py`](../app/agent_server/bootstrap.py) |
| How is workspace ownership represented? | `WorkspaceOwner`, `WorkspaceSlotBinding`, manifest | [`app/domain/operation_execution/contracts.py`](../app/domain/operation_execution/contracts.py) |
| How do snapshots restore safely? | snapshot create/clone contracts and service | [`app/application/sandbox_snapshots.py`](../app/application/sandbox_snapshots.py) |
| Where is deny-by-default admission validation? | `AdmissionPolicyRegistry` | [`app/application/run_control.py`](../app/application/run_control.py) |
| What is the coexistence rule? | stable BellLabs core, plural runtimes | [`docs/AGENT_FRAMEWORK_COEXISTENCE_STRATEGY.md`](AGENT_FRAMEWORK_COEXISTENCE_STRATEGY.md) |

## Inbound contract map

The system has several input boundaries. “Submit a workflow” should refer to the coordinator launch flow, not to publishing definitions or directly invoking Agent Server.

| User/agent intent | Input contract | Output | Authority boundary |
|---|---|---|---|
| Save editable catalog work | `SaveDraftRequest` | `AuthoringHead` | control-plane authoring |
| Publish a new exact definition directly | `PublishRequest` | `PublishedDefinition` | control-plane publication |
| Publish the current draft | `PublishDraftRequest` | `PublishedDefinition` | control-plane publication |
| Move a convenience alias | `MoveAliasRequest` | `AliasBinding` | alias history; not a running plan |
| Compile exact workflow semantics | `CompileInvocation` | `EffectiveRunConfiguration` | pure control-plane compilation |
| Propose coordinator design | `WorkflowDesignDraft` | deterministic design findings | advisory design/repair surface |
| Prepare a launch | `WorkflowLaunchProposal` | `PreparedLaunchTicket` | freezes compile, admission, environment, policy, semantic binding, and optional RunPlan v3 |
| Admit a BellLabs run | frozen `RunRequest` inside ticket | `AdmissionDecision` | run-control authority |
| Dispatch an admitted epoch | Temporal root start binding | `BellLabsRunWorkflow` binding receipt | sole macro-runtime binding |
| Bind bounded cognition | exact local/remote operation adapter request | agent graph/thread/run/checkpoint facts | subordinate provider binding |
| Intervene in a runtime | typed intervention union | `InterventionReceipt` | revalidated runtime action |
| Submit an operation directly for a bounded adapter | `OperationExecutionRequest` | `OperationExecutionBinding`/result | exact operation boundary |

Recommended public submission sequence:

```text
WorkflowDesignDraft
  -> ValidationReport
  -> WorkflowLaunchProposal
  -> PreparedLaunchTicket (public preview hides sensitive payloads)
  -> launch ticket
  -> WorkflowLaunchHandle with BellLabs run identity
  -> Temporal root/family/operation bindings
  -> optional agent binding/provider receipt as subordinate facts
  -> WorkflowResultView
```

Direct `GraphExecutionSubmission` is an internal bounded-operation adapter contract. A coordinator
must not bypass compilation, run admission, or the Temporal root by calling Agent Server itself.

## Persistence model map

Domain contracts and persistence documents intentionally have different jobs. PostgreSQL owns run,
command, effect, and settlement authority. Existing Mongo/Beanie collections retain the control and
content-addressed catalog roles designed below; they must not become a second run authority.

### Control-plane Mongo collections

| Beanie document | Collection | Stores/indexes | Rehydrates to |
|---|---|---|---|
| `DefinitionHeadDocument` | `control_plane_definition_heads` | kind + logical ID, draft/published revisions, draft payload | `AuthoringHead` |
| `DefinitionAliasDocument` | `control_plane_definition_aliases` | current alias target per kind/logical ID/alias | `AliasBinding` with movement evidence |
| `DefinitionAliasMovementDocument` | `control_plane_definition_alias_movements` | immutable alias movement history | audit evidence |
| `PublishedDefinitionDocument` | `control_plane_published_definitions` | exact revision/digest and definition payload | `PublishedDefinition` |
| `DefinitionRetirementDocument` | `control_plane_definition_retirements` | exact retired revision/digest and actor/time | retirement state on published definition |
| `EffectiveRunConfigurationDocument` | `control_plane_effective_run_configurations` | ERC digest/compiler/compilation identity and inline or external payload ref | `EffectiveRunConfiguration` |
| `CatalogProjectionEventDocument` | `catalog_projection_events` | projection work, leases, retries, poison state | catalog search projection work |
| `CatalogProjectionAlertDocument` | `catalog_projection_alerts` | durable projection failure alert | operator alert/read model |

These documents are defined in [`app/models/control_plane.py`](../app/models/control_plane.py) and mapped by [`app/application/control_plane_repository.py`](../app/application/control_plane_repository.py).

### Workspace and snapshot Mongo collections

| Beanie document | Purpose |
|---|---|
| `WorkspaceSlotReservationDocument` | enforces exclusive slot/path ownership during materialization |
| `WorkspaceMaterializationManifestDocument` | stores the current immutable manifest revision/payload |
| `SandboxSnapshotDocument` | indexes immutable snapshot identity, payload address/digest, lineage, binding, and retention |
| `SandboxSnapshotCloneDocument` | records clone target, source snapshot, binding, and reacquired-resource facts |
| `SandboxSnapshotClaimDocument` | idempotent create/clone claims and fingerprints |

Documents must not become an alternate public schema. Repositories should validate stored payloads back into the versioned domain contract before returning them.

### Persistence naming rule

```text
...Definition / ...Request / ...Binding / ...Result  = domain/API meaning
...Document                                           = database representation
...Projection                                         = derived query/read state
...Record                                             = immutable domain history fact
```

Avoid adding Mongo-specific fields to domain definitions merely because they are convenient to index. Put indexes and storage envelopes in `...Document`; put meaning and invariants in the domain contract.

## Capability catalog map

There are two related catalogs today.

### Semantic/control-plane definition kinds

```text
workflow_type
workflow_implementation
blueprint
control_profile
runtime_profile
workspace_template
evaluation_profile
workflow_configuration
memory_policy                 reference-only today
agent_profile
capability_selection          reference-only today
prompt
skill
mcp_server
mcp_tool
plugin_package                reference-only today
```

These answer “What may be selected and what does it mean?”

### Runtime definition kinds

```text
graph_assembly
agent_harness
middleware_stack
context_policy
context_assembly
delegation_policy
mcp_server
prompt_context
interpreter_profile
sandbox_profile
execution_environment
evaluation_profile
capability_manifest
state_schema
reducer_registry
operation_registry
run_plan
```

These answer “Exactly how will the selected work execute?”

### Recommended missing/clarified capability kinds

| Proposed kind | Why it is needed |
|---|---|
| `tool` / `tool_manifest` | provider-neutral native, LangChain, Deep Agents built-in, and MCP tool catalog/assembly |
| `validator` / `validation_set` | exact bridge from semantic contract refs to approved deterministic server functions |
| `workspace_access_policy` | path/slot/operation permissions independent of backend |
| `filesystem_backend` | state/store/local/sandbox/composite routing independent of logical workspace |
| `model_selection_policy` / `model_binding` | distinguishes authored choice policy from exact per-operation model mechanics |
| `effect_policy` | idempotency, claim, retry, and consequential-action semantics |
| `trace_redaction_policy` | makes observability/redaction an exact inspectable binding |

Do not add a generic catch-all `capability_config`. Capabilities remain searchable assets; their exact effective surface is frozen into manifests and bindings.

## Noun test

When naming a new contract, choose the noun by answering this table.

| If the object... | Name it... |
|---|---|
| is a published reusable catalog asset | `...Definition` |
| describes authored selection preferences | `...Profile` |
| states rules and limits | `...Policy` |
| says what is needed without choosing how | `...Requirement` |
| associates an exact implementation/identity/authority | `...Binding` |
| is a complete compiled construction recipe | `...Spec` |
| inventories exact content | `...Manifest` |
| transports bounded facts across a boundary | `...Envelope` |
| is a derived read model | `...Projection` |
| captures immutable restorable bytes | `...Snapshot` |
| marks runtime graph position | `...Checkpoint` |
| asks for a state change | `...Request` |
| authoritatively accepts/rejects/defers | `...Decision` |
| acknowledges an idempotent request/provider action | `...Receipt` |
| records a durable historical fact | `...Record` |
| is a completed typed outcome | `...Result` |

## Workflow-family decision

```text
Can the host name meaningful phases and dependencies?
  yes -> StageGraph
  no  -> Can objective, acceptance, authority, budget, and limits be frozen?
           yes -> GoalDirected
           no  -> the request is not ready to become a governed Workflow Type
```

Use a linked run when the child needs independent:

- admission or authority;
- significant budget;
- durable lifecycle and waits;
- cancellation/continuation policy;
- reusable governed result;
- tenant or data boundary.

Use a built-in synchronous Deep Agents subagent only when the child is an operation-local,
non-addressable implementation detail. Use the custom BellLabs delegation tool to start a Temporal
child when independent lifecycle is required. Provider async subagents are subordinate adapters.

## StageGraph ownership split

| Concern | Belongs in |
|---|---|
| Dependencies and joins | `StageGraphBlueprint` |
| Completion/skip/fairness | `StageGraphBlueprint` |
| Required input/output/effect semantics | `StageCapabilityRequirement` |
| Exact native/agent/MCP/sandbox implementation | `OperationAssemblySpec` |
| Stage-to-implementation association | `StageExecutionBinding` |
| Worker/model/tool/child capacity | `ExecutionResourceEnvelope` |
| Runtime retry/cancellation/wait | runtime/effect/fallback policies |
| Actual macro runtime state | Temporal root/family/operation bindings plus BellLabs projections |
| Bounded cognitive state | LangGraph/Deep Agents checkpoint and optional agent binding |
| Acceptance and terminality | BellLabs validators/evaluators/run control |

## Agent and child ownership split

```text
AgentProfileDefinition
  reviewed role and maximum request
        |
AgentHarnessDefinition
  exact bounded model/tool loop construction
        |
OperationAssemblySpec
  exact operation-local prompts, tools, MCP, skills, context,
  children, filesystem, sandbox, verifier, resources, and policies
```

Child modes:

```text
sync dictionary child   -> blocking Deep Agents task
sync compiled child     -> blocking compiled LangGraph task
dynamic QuickJS child   -> programmatic blocking task() calls
independent child       -> BellLabs tool starts Temporal OperationWorkflow
provider async child    -> subordinate adapter under the Temporal child binding
linked run              -> independent BellLabs Workflow Run
```

## Workspace and sandbox nouns

| Noun | Meaning |
|---|---|
| `WorkspaceTemplateDefinition` | allowed logical slots and access modes |
| `WorkspaceAccessPolicyDefinition` | allowed filesystem operations by path/slot/owner |
| `WorkspaceMaterializationManifest` | exact logical file inventory for one workspace |
| `FilesystemBackendBinding` | state/store/local/sandbox/composite provider mapping |
| `SandboxProfileDefinition` | allowed sandbox construction recipe |
| `SandboxMaterialization` | one live ephemeral sandbox instance |
| `SandboxSnapshot` | immutable captured filesystem/runtime artifact; never live authority |
| `LangGraphCheckpoint` | recoverable graph state position |
| `ContextAssemblySpec` | exact bounded model context recipe |

Fresh sandbox:

```text
profile + image/packages + workspace manifest + current authority -> materialization
```

Restored sandbox:

```text
snapshot + compatibility + target manifest + current authority
  -> clone + reacquired credentials/leases/connections -> materialization
```

Sandbox ownership is operation-attempt + execution-generation scoped. A provider-neutral BellLabs
gateway may select LangSmith, Daytona, or custom containers from an exact binding.

## Communication and intervention map

```text
command/message ledger -> transactional outbox -> target attempt inbox
  -> lease + CAS claim of monotonic ordered bounded batch
  -> Temporal receives IDs/status, never payloads
  -> runtime observed
  -> post-model/pre-tool drain
  -> checkpoint containing injection commits
  -> applied receipt
```

Receipt states are `accepted`, `routed`, `runtime_observed`, `model_visible`, `applied`, `rejected`,
`expired`, and `superseded`. Crash recovery redelivers the same immutable IDs idempotently. There is
no silent stale retargeting.

Peer messages are typed candidate input. They do not establish a durable wait and cannot satisfy a
StageGraph dependency without settlement. Only privileged actors may alter user/system/developer
prompt authority.

Certified exact injection is post-model/pre-tool: checkpoint the response, drain messages, then
revalidate or supersede proposed tools/effects. Only certified remote graphs advertise this
guarantee. Disruptive intervention is best-effort cancel -> effect/checkpoint reconcile -> same
attempt/new generation -> late-output quarantine. Orphan overlap is policy-gated; cancellation and
injection are not atomic.

## Validation map

```text
ValidatorDefinition
  public meaning + phase + input/output schema
        |
ValidatorImplementationBinding
  installed implementation + digest + package/schema compatibility
        |
ValidatorImplementationRegistry
  deny-by-default server map to approved callables
        |
ValidationBinding
  exact validator set frozen into a plan/operation
        |
ValidationReport
  stable codes + JSON paths + evidence + repair hints
```

Use these terms precisely:

| Term | Meaning |
|---|---|
| schema validator | checks payload shape/canonical form |
| deterministic validator | pure host code evaluating exact inputs |
| admission policy | decides whether a run may begin |
| guardrail | intercepts agent input/output/tool behavior at runtime |
| evaluator | scores or judges evidence/results |
| verifier | independently tests a claimed completion against acceptance |
| attestation provider | observes external state and emits immutable evidence |

## Concurrency map

```text
tenant workflow-run slots
  -> StageGraph frontier slots
      -> operation worker slots
          -> model/tool/MCP call slots
          -> synchronous child slots (parent remains active)
          -> async child slots (parent may wait and release worker)
          -> linked run slots (independent admission)
```

Always reserve resumption capacity. Do not let child fan-out consume every worker needed to reconcile or resume the supervisor.

## Contract review checklist

Before accepting a new or changed contract, verify:

- [ ] The owner layer is named.
- [ ] The object has one lifecycle and one authority meaning.
- [ ] Provider IDs are qualified.
- [ ] Mutable aliases are resolved before launch.
- [ ] Every executable component has an exact ref/digest.
- [ ] No secret value can enter a definition, plan, snapshot, checkpoint, or trace.
- [ ] Capabilities requested by a child are subsets of the parent and delegation ceilings.
- [ ] Concurrency dimension and unit are explicit.
- [ ] Runtime defaults and middleware-contributed tools are frozen in a manifest.
- [ ] Deterministic validation uses exact inputs and shared finding codes.
- [ ] External observations are attestations, not hidden validator I/O.
- [ ] Workspace writes have one owner and outputs remain candidates until promoted.
- [ ] Snapshot restore clones and reacquires live resources.
- [ ] Agent/graph completion cannot directly terminalize the BellLabs run.
- [ ] The Temporal root/family/operation binding precedes any optional agent binding.
- [ ] Independently lifecycle-bearing work is a Temporal child workflow.
- [ ] Message targeting, sequence, immutable ID, claim lease, and complete receipt are explicit.
- [ ] `applied` means an injection-containing checkpoint committed.
- [ ] Local and remote cognitive variants cannot substitute silently.
- [ ] Temporal worker and operation-assembly versions meet an exact compatibility manifest.
- [ ] JSON Schema descriptions and at least one example are available.
- [ ] A versioned migration is used when meaning or persisted shape changes.

## Services and implementation sequence

Deployment contract:

- one modular BellLabs API/control service is the sole governed external API/MCP façade;
- provider APIs are restricted; callbacks persist/dedupe before outbox-signaling Temporal;
- worker pools are coordinator, agent, ingestion-I/O, sandbox-control, and
  verification/reconciliation;
- self-host Temporal initially on AWS with separate persistence DB/credentials;
- retain PostgreSQL run authority, existing Mongo/content-addressed catalogs, and object storage;
- remote graphs always use start-bind-wait/reconcile; async activity completion is optional for
  qualified callbacks;
- the BellLabs durable event stream is product status authority;
- exact ECS/EKS/EC2 placement is deferred to Stage 8 evidence.

Ordered implementation slices:

1. Freeze root/family/operation contracts and Agent Server supersession notes.
2. Implement the operation child, exact local/remote variants, generations, and compatibility
   manifest.
3. Implement ledger/inbox/outbox, monotonic ordered delivery, leases, and full receipts.
4. Prove StageGraph early joins and make each GoalDirected iteration an operation child.
5. Certify post-model/pre-tool injection and disruptive-intervention reconciliation.
6. Prove fork safe points, parent-owned active children, settled-result reuse, no implicit pending
   messages, and Continue-As-New child reconciliation.
7. Qualify callbacks, event status authority, provider-neutral sandboxes, and isolated worker pools.
8. Select ECS/EKS/EC2 and promote only after replay, failure, scale, and security gates pass.
