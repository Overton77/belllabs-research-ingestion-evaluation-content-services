# App runtime porting reference for LangGraph and Deep Agents

Status: implementation companion; source extraction, not a new source of domain authority  
Audience: agents porting BellLabs workflows to LangGraph, Deep Agents, and Agent Server  
Scope: reusable contracts and behavior in `app/`, especially the OpenAI Agents SDK + Temporal path

## Read this first

Port the **BellLabs semantics**, not the OpenAI or Temporal object graph.

The reusable chain is:

```text
Workflow Type + exact implementation
  -> pure compilation
  -> immutable Effective Run Configuration (ERC)
  -> authoritative admission and budget reservation
  -> exact RunPlan / runtime binding
  -> semantic operation claim
  -> bounded runtime execution
  -> typed evidence, usage, and artifact candidates
  -> BellLabs validation, settlement, and terminality
```

LangGraph owns graph scheduling, checkpoints, interrupts, streaming, and resume. Deep Agents may own a bounded operation loop, filesystem work, skills, and delegation. Neither owns lifecycle truth, budgets, authority, artifact admission, scientific truth, or terminality.

Use this source precedence when details conflict:

1. accepted `biotech-meta` product/domain specifications;
2. current pure domain/application contracts and tests;
3. accepted migration decisions and handoffs;
4. provider adapters as behavioral prior art only.

The existing contracts are useful but not presumed optimal. Preserve invariants and observable guarantees; improve shapes through versioned contracts and explicit migrations.

## Classification key

| Mark | Porting treatment |
|---|---|
| **KEEP** | Provider-neutral invariant or contract with strong evidence. |
| **ADAPT** | Good shape, but naming, persistence, or execution coupling should change. |
| **REFERENCE** | Useful implementation evidence, not a target abstraction. |
| **DROP** | Bootstrap/legacy behavior that must not enter the target path. |

## The contract spine

| Layer | Existing useful contract | Porting treatment |
|---|---|---|
| Domain meaning | `WorkflowTypeDefinition`: purpose, non-goals, admission contract, invariants, obligations, outputs, allowed implementations, authority ceiling, workspace contract, linked-run slots | **KEEP**. This is the meaning of a workflow, not its graph. |
| Workflow mechanics | `StageGraphBlueprint` or `GoalDirectedBlueprint` | **KEEP/ADAPT**. Preserve interpreters and policies; compile them to LangGraph rather than translating Temporal code. |
| Exact implementation | `WorkflowImplementationBindingDefinition`: exact workflow, blueprint, control, runtime, workspace, evaluation, configuration, obligation/output realizations, conformance evidence | **KEEP**. One Workflow Type may have several provider-qualified implementations. |
| Frozen run config | `EffectiveRunConfiguration` | **KEEP**. Execution must not dereference mutable aliases or profiles after preparation. |
| Target runtime assembly | `GraphAssemblyDefinition`, `GraphAssemblySpec`, `RunPlan` | **KEEP/EVOLVE**. These v2 contracts are the bridge from ERC to Agent Server. |
| Runtime identity | BellLabs run + execution epoch + provider-qualified thread/run/checkpoint IDs | **KEEP**. Never overload `run_id`, `thread_id`, `assistant_id`, or checkpoint IDs. |
| Semantic execution | `OperationExecutionRequest` / immutable `OperationExecutionBinding` | **KEEP/ADAPT**. Compile an equivalent exact operation assembly for Deep Agents. |
| Effects | Stable semantic attempt, effect claim, technical attempts, settlement identity | **KEEP**. Runtime execution is at-least-once; settlement and charging are idempotent. |
| Outputs | Typed result, immutable evidence refs, workspace candidates, admitted artifacts | **KEEP**. Prose and a successful graph run do not satisfy obligations by themselves. |

Do not collapse the layers. In particular:

- a Workflow Type is not a graph or Agent Server assistant;
- an ERC is not graph state;
- a semantic operation attempt is not a provider retry;
- an operation-local subagent is not a linked Workflow Run;
- a workspace file is not a durable artifact;
- an execution checkpoint is not domain state or memory.

## Workflow families worth preserving

### StageGraph

`StageGraphBlueprint` is a capability-aware typed DAG. Each `StageNode` declares dependencies and dependency classes, join/skip/completion policy, fairness, concurrency slots, reservation, optional semantic cycle policy, obligations, outputs, and variants. The pure interpreter derives runnable stages and applies typed results; Temporal currently supplies durable scheduling around it.

Target pattern:

```text
frozen blueprint
  -> pure runnable/blocked/completed decision
  -> reserve authoritative budget
  -> claim semantic operation
  -> choose exact native | agent-harness | compiled-subgraph implementation
  -> execute concurrently within compiled ceilings
  -> settle result and usage
  -> apply result through the pure interpreter
  -> evaluate workflow -> accept | fail | semantic cycle
```

Important distinctions:

- provider retry repeats a technical attempt;
- stage cycle performs new semantic work with a new binding and evidence;
- whole-workflow cycle creates a new workflow-cycle namespace;
- checkpoint/history rollover is runtime maintenance, not a business cycle.

### GoalDirected

`GoalDirectedBlueprint` defines a stable objective, acceptance contract, iteration ceiling, protected goal fields, session rollover, workspace/snapshot behavior, convergence rules, allowed operation classes, and an independent verifier. The model proposes work; deterministic host logic protects scope and the verifier decides acceptance.

Target pattern:

```text
protected goal + frozen acceptance contract
  -> bounded Deep Agent iteration
  -> host validation and durable evidence
  -> independent verification
  -> accept | revise | handoff/rollover | fail
```

Do not allow planner state, a todo list, model self-assessment, or `END` to terminalize the BellLabs run.

## Schema selection: the best concrete porting exemplar

The schema-grounding definitions demonstrate a compact, well-factored Workflow Type. Preserve the separation even if stage granularity changes.

### Schema Context Selection StageGraph

```text
materialize_selection_context
  -> semantic_selector (agent)
  -> structural_validation (trusted deterministic code)
  -> independent_reviewer (separately bound agent)
  -> accept_selection (trusted deterministic code)
```

Key properties:

- Tier-0 orientation and exact candidate details are read-only workspace slots; output is exclusively writable.
- Selection membership contains types, not individual properties; host code verifies canonical sorting, duplicates, closure, digests, and lineage.
- The agent emits a bounded draft; it does not author trusted lineage fields.
- Review is independently bound to the exact selection and deterministic validation facts.
- Acceptance requires structural validity and the required reviewer role.
- A maximum of two semantic revisions is explicit; revision is not confused with activity retry.
- Every stage has an output slot and reservation; the workflow has an evaluation contract.

This is a stronger pattern than “one deep agent selects schema.” In LangGraph, keep deterministic materialization, validation, and acceptance as native nodes; use the Deep Agents harness only for semantic selection and review.

### Supporting Graph Reconciliation

The StageGraph form is:

```text
admission -> derive_schema_context -> materialize_runtime_projection
  -> graph_authority_gate -> plan_bounded_queries -> execute_bounded_intents
  -> verify_evidence -> evaluate -> promote_result
```

Its GoalDirected alternative may plan iteratively, but still uses host-validated query intents, a read-only `SchemaOperationProjection`, bounded query kinds/labels/relationships/properties/limits/depth/timeouts, immutable query evidence, and independent acceptance.

Enhancement opportunity: compile each StageGraph stage to one of `native_operation`, `agent_harness`, or `compiled_graph` using `StageImplementationBinding`. This retains a single scheduler while allowing heterogeneous implementations and safe concurrency.

## Exact operation assembly for Deep Agents

The existing `OperationExecutionRequest` is the minimum useful checklist. A target operation assembly should freeze:

- BellLabs run, operation, semantic attempt, request scope, and idempotency identity;
- ERC and RunPlan digests plus accepted run-control revision;
- operation contract and typed output contract;
- ordered prompt/context segments with source revision, trust class, rendered digest, and truncation evidence;
- provider/model/settings/turn ceiling/fallback policy;
- exact tools, schemas, approval and side-effect policies;
- exact MCP servers, transports, tool filters, schemas, session/auth/elicitation policy;
- exact skill bundles, file manifests, mount paths, provenance, compatibility, and digests;
- middleware/guardrails and their implementation/configuration digests;
- delegation modes, profiles, depth/concurrency/count/budget ceilings, child workspace policy, and result admission;
- workspace slots, mounts, provider/image/runtime/package/environment digests, network policy, and snapshot lineage;
- capability/data/network/secret authority, budget reservation, tracing/redaction, and retention policies.

Compilation must intersect every requested capability with Workflow Type authority, caller/parent authority, deployment availability, approvals, and delegation ceilings. Prompts, skills, tool descriptions, MCP responses, workspace files, checkpoints, and model output can request work but cannot grant authority.

The target `AgentHarnessDefinition` and `GraphAssemblyDefinition` already encode much of this. Improve them by maintaining an explicit mapping from every assembled field back to the ERC/source definition and by failing closed on unknown or unsupported required controls.

## Skills

Treat skill discovery, catalog admission, compilation, mounting, and execution as separate steps.

Existing reusable contract:

- `SkillDefinition` records name, frontmatter, content-addressed bundle, complete file manifest, executable bits, required capabilities, runtime/executable/network/workspace compatibility, provenance, and review status.
- `ImmutableAssetBinding` freezes the exact definition ref, manifest digest, and mount path for one operation.
- The OpenAI adapter verifies bytes against the digest, mounts read-only, and gives the agent only an index line until it chooses to read the skill. Skill text is procedure, never authority.

Porting rules:

1. Search/discovery returns untrusted candidates, never executable grants.
2. Inspect and review the full package, including scripts, references, assets, licenses, dependencies, and hooks.
3. Publish immutable content-addressed definitions.
4. Compile only exact approved revisions into a RunPlan.
5. Materialize read-only in the sandbox and expose concise progressive-disclosure metadata.
6. Attribute use, usage, outputs, and failures to the operation binding.

Do not copy the current fixture-only byte map as a production package store. Add supply-chain scanning, dependency lock verification, media/size limits, and capability extraction before production use.

## MCP servers

The useful contract is more than a server URL: exact transport, endpoint/launch definition, credential references, allowed tools, approval rules, network requirements, schema snapshot/digest, tool side-effect class, timeout, retry, session, and elicitation policy.

The OpenAI adapter provides good enforcement evidence:

- only absolute HTTP(S) endpoints reach the sandbox runtime;
- endpoint host must be in the operation network grant;
- exposed tools are statically filtered to the compiled allowlist;
- approval policy must be resolved before execution;
- schema identity is part of the binding;
- `stdio` is rejected by this cloud-oriented path.

Target rules:

- use remote Streamable HTTP or a sandbox-contained adapter for deployment; do not depend on host `.tools` paths;
- use per-operation or explicitly per-thread sessions, never deployment-global credential-bearing sessions;
- store secret references, not tokens; resolve them just in time;
- map elicitation to typed BellLabs interventions/interrupts;
- wrap tool calls with effect classification, idempotency/claim policy, budget accounting, redaction, and result validation;
- detect schema drift before tool exposure, not after a call.

## Sandboxes, workspaces, artifacts, and snapshots

### Workspace invariant

A `WorkflowWorkspaceContract` defines logical slots. Compilation assigns one writer to each writable slot, read-only durable inputs, and explicit child-private areas. A `WorkspaceMaterializationManifest` maps governed logical paths to durable input refs, local candidates, admitted artifacts, or stale entries. Host paths are never domain identifiers.

Unmapped files remain local. Cross-stage/run exchange uses typed messages or admitted artifact refs. Artifact promotion is the only path from a candidate file to durable output and must verify slot ownership, producer binding, digest/media/size, permission, output contract, checks, and idempotency.

### Snapshot vocabulary (do not merge these)

| Object | Purpose | Authority |
|---|---|---|
| ERC/configuration snapshot | Exact compiled run configuration | Authoritative frozen input to execution |
| Input/collection snapshot | Immutable membership/input view | Authoritative input evidence for its purpose |
| LangGraph checkpoint | Thread state and execution position for resume | Non-authoritative runtime projection |
| Goal handoff checkpoint | Explicit goal/obligations/evidence handoff across agent rollover | BellLabs semantic record, distinct from LangGraph checkpoint |
| Sandbox snapshot | Immutable filesystem/runtime payload for debug, clone, or resume | Runtime evidence; never a capability or artifact grant |
| Session compaction | Short-term model context replacement | Runtime context only |
| Workspace/artifact manifest | Exact file lineage and admission state | Durable application evidence |

### Sandbox snapshot rules

- capture immutable metadata plus a content-addressed payload;
- record source workspace, parent snapshot, producer binding, filesystem/content/runtime/image/package/environment/workspace digests, capability shape, reason, and retention;
- restore by cloning to a new workspace identity;
- verify payload and compatibility before execution;
- re-resolve secrets, credentials, MCP connections, sockets, leases, mounts, and current authority;
- never restore live resources or writable ownership from the snapshot;
- require artifact promotion for files after restore.

The current OpenAI snapshot bridge is useful security prior art: it bounds archive members and sizes, normalizes/rejects unsafe paths, forbids links/devices/special files, rejects credential-shaped paths/content and resolved secret values, verifies digests, and retains explicit clone lineage.

## Temporal behavior to preserve without porting Temporal

Preserve:

- pure deterministic StageGraph and GoalDirected decision logic;
- frozen blueprint/configuration digest verification before execution;
- lifecycle compare-and-set and budget reservation before semantic dispatch;
- stable semantic attempt and side-effect identities across technical retries;
- bounded parallel stage execution with deterministic result application;
- typed wait/pause/approval/cancel interventions;
- durable claims, checkpoints, attempts, settlements, and reconciliation;
- independent workflow evaluation and BellLabs terminalization.

Do not port:

- `workflow.execute_activity` calls node-for-node;
- Temporal Event History as an application database;
- the SDK-in-Temporal `SandboxAgentProbeWorkflow`;
- activity retry counts as semantic cycles;
- Temporal signal names as the public application API.

Agent Server checkpoints replace orchestration mechanics, not PostgreSQL lifecycle/budget authority or immutable BellLabs records.

## API boundary

The existing FastAPI surface reveals the intended application seams:

| Surface | Important operations | Target use |
|---|---|---|
| `/control-plane/v1` | publish/draft/retire definitions, resolve aliases, compile and retrieve ERC, export JSON Schema | Keep behind runtime-neutral application services. |
| `/run-control/v1` | admit run request, issue typed lifecycle command, inspect run/budget/transitions/outbox, submit a bound operation | Keep authoritative; do not let Agent Server mutate around it. |
| `/schema-grounding/v1` | retrieve exact catalog/selection/projection/binding/compatibility/reconciliation/evaluation records | Reuse as immutable semantic/evidence reads. |
| `/v2/graph-runtime/schemas` | exports target submissions, receipts, bindings, interventions, interrupts, context/delegation/sandbox definitions, RunPlan and journal schemas | Contract foundation only; complete operational routes through the shared facade. |
| Socket.IO runtime stream | authenticated, request-scope-filtered durable runtime events and approval decisions | May be replaced/adapted, but preserve durable-before-publish and tenant checks. |
| Coordinator MCP | discovery, exact selection, compile/prepare/launch/observe through a facade | Keep runtime-neutral; never expose provider IDs or credentials as workflow meaning. |

Transport handlers should authorize and delegate. Domain/application services remain the reusable boundary for REST, MCP, coordinator, and Agent Server routes.

## Known legacy limitations: do not canonize them

- The OpenAI runtime is a Docker/provider adapter and still uses fixture-backed assets in important paths.
- Its in-memory side-effect cache is not durable authority.
- Automatic model fallback is unsupported; `stdio` MCP is unsupported in the governed sandbox path.
- Handoffs with sandbox tools are rejected because child-private sandbox enforcement is incomplete.
- Sandbox session retention and snapshot capture are adapter-specific and need a provider-neutral lifecycle port.
- The minimal `SandboxAgentProbeWorkflow` is bootstrap diagnostics only.
- Current API readiness is unconditional; capability readiness is separate and incomplete.
- v1 control/operation contracts and v2 graph-runtime contracts coexist; add explicit compatibility mapping rather than silently mixing fields.
- Current persistence spans PostgreSQL, MongoDB, Redis, object storage, Temporal, and provider state; follow the accepted migration transaction/authority decisions rather than current placement by inertia.

## Recommended enhancement order

1. **Canonical compatibility map:** map ERC and existing operation fields to `RunPlan`, `GraphAssemblySpec`, runtime binding, operation journal, and provider-qualified identities; version and test every transformation.
2. **Pure assembly compiler:** produce exact per-stage native/harness/subgraph implementations, middleware order, context assembly, skills, MCP, sandbox, delegation, and capability maturity from frozen inputs.
3. **Authoritative journal:** claim effects and settle attempts/usage through the accepted PostgreSQL transaction boundary while retaining immutable detailed bindings/evidence in their assigned store.
4. **Provider-neutral sandbox port:** implement materialize/execute/snapshot/clone/destroy with conformance tests and no provider types in domain modules.
5. **Deep Agents harness:** progressive skills, bounded filesystem, exact tools/MCP, typed output, sync delegation, explicit context packets, and no duplicate core middleware.
6. **Durable interventions:** typed interrupts, approvals, waits, steering, cancellation, async-task updates, forks, and reconciliation with optimistic version/checkpoint checks.
7. **Schema-selection parity slice:** port the five-stage selection workflow first; compare accepted selections, digests, review evidence, usage, resume, and failure behavior.
8. **Heterogeneous StageGraph proof:** mix native, Deep Agent, and compiled subgraph stages concurrently while preserving claims, workspaces, lineage, budgets, and deterministic commit.
9. **GoalDirected proof:** protect goal fields, use independent verification, test rollover/handoff/snapshot recovery, and reject model-authored terminality.
10. **Evaluation and cutover:** shadow with no consequential claims, then canary exact implementation bindings; retain rollback until legacy runs drain.

## Porting-agent acceptance checklist

- [ ] I can name the BellLabs Workflow Type and exact implementation revision being ported.
- [ ] I read the ERC/RunPlan and do not resolve mutable aliases during execution.
- [ ] Every graph node maps to a semantic operation or documented pure runtime step.
- [ ] Every agent/tool/MCP/skill/sandbox capability has an exact compiled binding and authority source.
- [ ] Graph state contains refs/digests and compact runtime state, not secrets, raw corpora, large artifacts, lifecycle authority, or budgets.
- [ ] Provider thread/run/checkpoint IDs are qualified and separate from BellLabs IDs.
- [ ] Technical retry, semantic attempt, stage cycle, workflow cycle, and goal iteration are distinct.
- [ ] Consequential effects use stable claims and idempotent settlement.
- [ ] Files cross boundaries only through typed messages or admitted artifact refs.
- [ ] Snapshot restore creates a new workspace and reacquires live authority/resources.
- [ ] Interrupts and approvals are typed, durable, tenant-scoped, and version checked.
- [ ] Deep Agent success is evidence; BellLabs validation/evaluation decides acceptance.
- [ ] Unsupported required capabilities fail closed; optional degradation is authored and recorded.
- [ ] Tests cover crash/resume, duplicate dispatch, stale intervention, tenant denial, budget exhaustion, schema drift, artifact/snapshot tampering, and redaction.

## Source map

Start with these exact sources; line numbers identify the relevant contract or boundary in the current worktree.

- Migration goal and ownership: `docs/migrations_instructions/implementation_work_packages/00_MAIN_GOAL_AND_INDEX.md:10`, `:22`, `:46`.
- Coexistence and runtime-neutral authority: `docs/AGENT_FRAMEWORK_COEXISTENCE_STRATEGY.md:47`, `:68`, `:88`, `:163`.
- Core control-plane contracts: `app/domain/control_plane/contracts.py:224` (`StageNode`), `:259` (`StageGraphBlueprint`), `:400` (`GoalDirectedBlueprint`), `:477` (`WorkflowTypeDefinition`), `:588` (implementation binding), `:720` (skill), `:749` (MCP), `:811` (agent profile), `:922` (ERC).
- Target graph runtime contracts: `app/domain/graph_runtime/definitions.py:102` (harness), `:139` (context policy), `:245` (delegation), `:273` (MCP), `:315` (snapshot policy), `:392` (graph assembly), `:437` (operation implementation variants), `:490` (RunPlan); `app/domain/graph_runtime/identities.py:15` and `:31`-`:83` (qualified identities).
- Operation binding and workspace/snapshot contracts: `app/domain/operation_execution/contracts.py:25`, `:105`, `:157`, `:185`, `:234`, `:314`, `:689`, `:786`, `:816`, `:842`.
- OpenAI adapter enforcement prior art: `app/integrations/openai_agents_runtime.py:111` (exact component registry), `:255` (runtime adapter), `:302` (execution), `:1071` (sandbox capabilities), `:1108` (MCP), `:1150` (progressive skill index).
- Snapshot security prior art: `app/integrations/openai_sandbox_snapshots.py:64`, `:149`, `:160`, `:268`, `:371`, `:379`, `:422`.
- Temporal workflow prior art: `app/temporal/stagegraph_workflow.py:39`, `app/temporal/goal_directed_workflow.py:35`, `app/temporal/operation_workflow.py:10`; bootstrap-only probe: `app/temporal/workflows.py:14`.
- Schema selection/reconciliation exemplar: `app/domain/schema_grounding/definitions.py:80`, `:588`, `:647`, `:695`; strict outputs/projection/intents: `app/domain/schema_context/contracts.py:13`, `:32`, `:45`, `:89`, `:108`, `:136`, `:181`.
- Trusted schema stage handlers: `app/application/schema_context_stage_handlers.py:82`, `:346` and the adjacent materialize/select/validate/review/accept handlers.
- API seams: `app/api/control_plane.py:38`, `:166`; `app/api/run_control.py:52`, `:213`, `:258`; `app/api/schema_grounding.py:32`; `app/api/graph_runtime_schemas.py:51`.
- Governing meta vocabulary: `../../../biotech-meta/docs/CONTEXT.md` entries for Workflow Agentic Configuration Contract, Effective Run Configuration, Operation Execution Binding, Agent Delegation, Execution Capability Profile, Sandbox Workspace, Workspace Materialization Manifest, and Sandbox Snapshot.
- Foundational meta design: `../../../biotech-meta/docs/system-control-plane-and-workflow-execution-configuration.md` and `../../../biotech-meta/docs/specs/pre-research/control-plane-foundations/04-operation-runtime-workspaces-artifacts-and-snapshots.md`.

When a referenced implementation moves, locate the named class/function rather than trusting the line number.
