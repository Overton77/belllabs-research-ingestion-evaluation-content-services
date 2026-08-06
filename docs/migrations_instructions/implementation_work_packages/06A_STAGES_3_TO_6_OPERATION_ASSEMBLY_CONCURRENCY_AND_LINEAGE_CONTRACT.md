# Stages 3–6 shared execution contract — operation assembly, concurrency, and lineage

Status: normative implementation contract for Stages 3–6  
Scope: StageGraph, GoalDirected, native operations, LangChain/Deep Agents harnesses, MCP, synchronous and asynchronous delegation, sandboxes, and linked runs  
Owner decisions preserved: D-02, D-04, D-05, D-08, D-11–D-16 and the accepted Stage 0 async-subagent requirement

## 1. Purpose

This document fixes the implementation boundary shared by the four work packages. An implementing agent must not invent a different division of responsibility inside an individual stage.

The required direction is:

```text
immutable workflow semantics
  -> pure capability selection and exact compilation
  -> authoritative admission and resource reservation
  -> generic graph scheduling
  -> one exact operation executor
  -> immutable result/evidence manifests
  -> deterministic BellLabs settlement
```

StageGraph controls **when** a stage may run. An exact operation assembly controls **how** that stage runs and which capabilities it can see. Deep Agents is one operation implementation, not the StageGraph scheduler and not BellLabs lifecycle authority.

The migration is a port and enhancement, not a requirement to preserve the model/provider choices used by the OpenAI Agents SDK/Temporal implementation. Behavioral parity applies to BellLabs contracts, authority, accepted evidence, obligations, typed results, failure distinctions, budgets, and owner-approved semantic tolerances. A new Workflow Implementation may select different models, prompts, harnesses, tools, specialists, or concurrency policies when those choices are exact, admitted, evaluated, and traceable.

## 2. Required implementation sequence

The four work packages execute in this order:

1. **Stage 3 — durable runtime kernel.** Implement identities, lineage, resource leases, runtime attempts, interrupts, cancellation, forks, event translation, and reconciliation. No business scheduler or Deep Agent assembly.
2. **Stage 4 — capability-aware but capability-mechanics-free StageGraph.** Implement the generic frontier scheduler, the operation-executor port, deterministic/native adapters, and the first parity slice. Do not construct a temporary Deep Agent or MCP stack.
3. **Stage 5A — stable operation compiler and harness.** Compile and construct the stable LangChain/Deep Agents surface: exact model, prompt, middleware, tools, reviewed skills, context, filesystem/workspace, synchronous specialists, verifier, and sandbox policy.
4. **Stage 5B — harness composition proof.** Plug the same stable harness into at least one StageGraph stage through the Stage 4 executor port. The scheduler topology must not change.
5. **Stage 5C — GoalDirected.** Port the deterministic GoalDirected outer graph as another consumer of the same operation compiler/executor/harness.
6. **Stage 6A — stable provider completion.** Add and harden outbound MCP, cross-operation context/Store behavior, skill publication/mounting, provider-neutral sandbox/snapshot lifecycle, and capability-readiness reporting.
7. **Stage 6B — required async-subagent implementation.** Implement durable async children and their StageGraph wait/resume integration behind the default-off qualification flag. The migration track is not optional under the accepted Stage 0 decision.
8. **Stage 6C — heterogeneous StageGraph proof and async qualification.** Prove one graph can concurrently schedule differently assembled native, Deep Agent, MCP/skill, sandbox, verifier, and async-child stages; complete the async promotion evidence through that production composition path.
9. **Stage 6D — optional QuickJS/dynamic track.** Implement only after its independent qualification and owner enablement. It may remain disabled without blocking the stable path.

Stages may be committed in smaller coherent slices, but their outgoing handoffs and gates must preserve this order.

## 3. Ownership boundaries

| Concern | Owner | Forbidden shortcut |
|---|---|---|
| Stage dependencies, joins, cycles, fairness, readiness | StageGraph blueprint plus pure interpreter | Model-generated scheduling authority |
| Goal revisions, convergence, rollover decision | GoalDirected interpreter plus BellLabs lifecycle | Deep Agent self-terminalization |
| Exact model/tool/skill/MCP/context/delegation surface | Operation assembly compiler | Runtime discovery or mutable alias resolution |
| Run/thread/checkpoint/attempt mechanics | Stage 3 runtime kernel and Agent Server | Provider IDs as domain authority |
| Budget, concurrency, approval, effect claims, settlements | BellLabs PostgreSQL/run control | Checkpoint-, prompt-, or tool-authored authority |
| Agent planning and bounded specialist delegation | LangChain/Deep Agents harness | Agent changing admitted Workflow Implementation |
| Evidence acceptance and terminality | BellLabs application/domain services | Verifier, model, trace, or Agent Server status terminalizing directly |

## 4. Normative exact contracts

The names below are canonical conceptual names. Stage 1 contracts may be extended in a schema-compatible way or versioned deliberately, but the fields and invariants are required.

### 4.1 `StageCapabilityRequirement`

This belongs to the immutable workflow/implementation definition and expresses requirements without choosing a provider:

```text
stage_id
operation_contract_ref
required_capability_ids
optional_capability_ids
input_contract_ref
output_contract_ref
context_purpose
effect_class                 # pure | read_only | idempotent_effect | consequential_effect
delegation_modes_allowed     # sync | async | linked_run; dynamic only when separately enabled
resource_class_ref
verification_contract_ref
degradation_contract_ref
speculation_policy_ref       # disabled unless explicitly published
```

A blueprint may retain topology-only `StageNode` records. The Workflow Implementation must bind every executable stage to exactly one requirement record. Requirements never contain credentials, provider sessions, or mutable aliases.

### 4.2 `OperationAssemblySpec`

This is the exact compiled runtime recipe for one stage/operation class:

```text
operation_assembly_id
schema_version
operation_contract_ref
implementation_kind          # native | agent_harness | compiled_graph | async_child | linked_run
implementation_ref
model_policy_ref
prompt_manifest_ref
middleware_manifest_ref
tool_manifest_ref
mcp_manifest_ref
skill_manifest_ref
context_assembly_ref
delegation_policy_ref
synchronous_subagent_refs
async_subagent_target_refs
workspace_policy_ref
sandbox_profile_ref
verifier_ref
resource_envelope_ref
effect_policy_ref
fallback_policy_ref
trace_redaction_policy_ref
capability_manifest_ref
compatibility_manifest_ref
operation_assembly_digest
```

Unused fields are represented by exact empty/disabled manifests, not omission that causes runtime defaults. The compiler rejects duplicate tools, duplicate core Deep Agents middleware, conflicting context editors, implicit general-purpose subagents, missing verifier bindings, unsupported transports, and any effective grant wider than caller/workflow authority.

### 4.3 `StageExecutionBinding`

This joins semantic topology to runtime assembly:

```text
stage_id
variant_name
stage_requirement_ref
operation_assembly_ref
operation_assembly_digest
input_projection_ref
output_projection_ref
resource_envelope_ref
compatibility_key
```

Every StageGraph stage and selected variant has exactly one binding in the frozen `GraphAssemblySpec`/`RunPlan`. Different stages may use different models, harnesses, tools, MCP servers, skills, subagents, contexts, sandboxes, and verifiers. No graph node contains a hard-coded model or tool list.

### 4.4 `ExecutionResourceEnvelope`

Every admitted operation carries a multi-level resource contract:

```text
tenant_limit_ref
workflow_run_slots
stage_slots
operation_worker_slots
model_call_slots
tool_call_slots
mcp_call_slots
sync_subagent_slots
async_child_slots
linked_run_slots
provider_quota_refs
budget_reservation_refs
deadline
lease_ttl
resumption_reserve
release_policy
```

The effective limit is the intersection of blueprint, Workflow Implementation, caller/parent authority, BellLabs admission, deployment capacity, provider quotas, and feature-specific ceilings. A stage cannot use one generic `concurrency_slots` value as a substitute for all subordinate resources.

### 4.5 `ExecutionLineageEnvelope`

Every runtime attempt, result, effect, child, and artifact carries or can resolve the following chain:

```text
request_scope
belllabs_run_id
execution_epoch
workflow_implementation_ref
graph_assembly_digest
workflow_cycle
stage_id
stage_cycle
semantic_operation_attempt_id
runtime_attempt_id
operation_binding_id
operation_assembly_digest
agent_invocation_id
parent_lineage_id
delegation_mode
child_task_id
child_thread_id
child_run_id
effect_claim_ids
input_manifest_digest
context_manifest_digest
result_manifest_ref
evidence_refs
usage_settlement_refs
trace_ref
```

Fields not applicable to a particular event are typed absent. They are never overloaded. Semantic attempts survive runtime retries; technical retries receive new runtime-attempt identities. Async task, child thread, and child run IDs remain distinct. A final result must be queryable back to every contributing accepted operation and capability assembly.

### 4.6 `OperationExecutor` port

Stage 4 defines and consumes one runtime-neutral async port equivalent to:

```text
execute(
  stage_request,
  exact_stage_execution_binding,
  execution_resource_lease,
  cancellation_context,
) -> OperationExecutionOutcome
```

Required outcomes are a discriminated union:

- `completed(result_manifest_ref, evidence_refs, usage_refs)`;
- `waiting(wait_binding_ref, retained_reservations, released_reservations)`;
- `paused(decision_ref)`;
- `degraded(reason_code, result_manifest_ref)`;
- `failed(failure_class, retryability, evidence_refs)`;
- `cancelled(settlement_refs)`.

The port never returns arbitrary lifecycle mutations. Every adapter—native, Deep Agent, compiled graph, MCP-backed, async child, and linked run—passes the same conformance suite.

## 5. Capability compilation phases

Capability compilation is not postponed wholesale to Stage 6.

### 5.1 Stage 1 structural compiler

Already-required Stage 1 work must freeze and validate exact refs, digests, maturity, authority intersections, feature flags, compatibility, and disabled fallbacks. It may produce an assembly that contains unavailable provider adapters, but it must report that unavailability deterministically.

### 5.2 Stage 5 stable runtime compiler

Stage 5 materializes the stable executable surface for native and LangChain/Deep Agents operations. Given identical definitions and environment facts, it must reproduce the same model-visible tool names, prompts, middleware order, skill mounts, child catalog, context policy, verifier, backend, and assembly digest.

### 5.3 Stage 6 provider/advanced compiler extensions

Stage 6 adds MCP transports/sessions, richer skill and sandbox providers, async children, and optional interpreter profiles. These extend the discriminated unions and validators; they do not replace the Stage 5 compiler or change scheduler topology.

Compilation always follows:

1. select an exact Workflow Type and Workflow Implementation;
2. load every stage requirement;
3. intersect requested mechanics with authority, policy, environment, maturity, and resources;
4. choose exact implementation versions and authored fallbacks;
5. predict the effective model-visible and runtime-visible surface;
6. freeze all manifests/digests into the RunPlan and launch ticket;
7. revalidate availability and acquire leases immediately before dispatch without widening the frozen surface.

## 6. Concurrency model

### 6.1 Hierarchy

Concurrency is admitted and observed separately at these levels:

1. workflow runs per tenant/environment;
2. ready StageGraph stages in the frontier;
3. operation workers dispatched for that frontier;
4. model/tool/MCP calls inside an operation;
5. synchronous specialist children that block their parent;
6. durable asynchronous children that release the parent worker;
7. separately admitted linked Workflow Runs;
8. deployment/provider/global quotas.

The scheduler reserves before `Send`. Operation-local fan-out reserves subordinate capacity before spawning calls. Capacity acquisition uses a canonical order to prevent deadlock. At least one supervisor/resumption slot is protected from child saturation.

### 6.2 Synchronous subagents

Synchronous children may run concurrently only when the compiled delegation policy permits it and aggregate capacity/budget is reserved before fan-out. The parent remains running and does not settle until all required child manifests reconcile. Children receive explicit `ContextSlice`, tools, skills, workspace mounts, and budgets; they do not inherit the full parent surface.

### 6.3 Asynchronous children

An async child persists a separate task/thread/run binding and transitions the parent operation to an authoritative wait. The parent worker is released. The release policy states which budget/quota leases remain held and which are reacquired on resume. Callback, polling, or reconciliation can wake the parent, but settlement requires a fresh status/result query and exact child manifest validation.

### 6.4 Evidence that concurrency is real

Tests must use barriers or controlled clocks to prove overlap and maximum-observed concurrency. Random result order alone is not sufficient evidence. Tests also prove no over-admission, lease leakage, starvation, or resumption deadlock.

## 7. Optimistic/speculative execution

Ordinary StageGraph concurrency is not speculative: a stage becomes runnable only after its declared join/readiness conditions pass.

Optimistic execution is a separate, default-disabled capability. It may be compiled only when:

- the stage effect class is `pure` or `read_only`;
- the published speculation policy declares assumptions, invalidation keys, maximum wasted budget, and expiry;
- inputs and outputs are immutable/content-addressed;
- speculative artifacts remain quarantined;
- no result, evidence, usage credit, lifecycle transition, or downstream consequential effect is accepted before the deterministic commit barrier;
- invalidated work is discarded and its actual cost is still settled;
- cancellation and duplicate execution tests pass.

`idempotent_effect` and `consequential_effect` stages are not speculated in Stages 3–6. Exactly-once settlement does not make arbitrary provider execution safe to speculate.

## 8. Stage and async-child state machines

The StageGraph operation state machine distinguishes:

```text
PENDING
  -> RESERVED
  -> DISPATCHED
  -> RUNNING
  -> WAITING_ON_DECISION | WAITING_ON_EXTERNAL | WAITING_ON_ASYNC_CHILD
  -> READY_TO_RECONCILE
  -> SETTLING
  -> COMPLETED | DEGRADED | FAILED | CANCELLED
```

Technical retries occur within the same semantic attempt only when policy permits. A semantic retry or cycle creates a new semantic identity. Resume always re-enters through Stage 3 reconciliation; no callback applies stage projection changes directly.

Async children use:

```text
REQUESTED -> STARTING -> RUNNING -> UPDATE_PENDING
          -> CANCELLING -> CANCELLED
          -> COMPLETED | FAILED | ORPHANED
```

Every transition is version checked, tenant scoped, idempotent, and reconciled. A distinct Workflow Type, separate authority, substantial budget, independent durable wait, or reusable governed result uses a linked Workflow Run instead.

## 9. Compatibility and runtime drift

- A running epoch remains bound to its exact graph, state schema, reducer registry, operation assemblies, model/tool/MCP/skill schemas, and compatibility manifest.
- Runtime readiness revalidation may make the operation unavailable; it may not substitute a different model/tool/server/skill or widen permissions.
- An authored exact fallback creates an explicitly recorded fallback binding/digest. Otherwise the stage waits, degrades, fails, or requests operator action according to its contract.
- Tool/MCP schema drift, model retirement, skill digest mismatch, sandbox-image change, and async-target incompatibility are checkpoint-resume compatibility events, not generic transient errors.
- Incompatible runs remain routed to a compatible blue/green deployment or move through an accepted epoch-rollover/fork/handoff policy.

## 10. Failure taxonomy

Every adapter maps errors to one of these stable classes:

- `authority_denied`;
- `capability_unavailable`;
- `capability_drift`;
- `resource_exhausted`;
- `budget_exhausted`;
- `approval_required`;
- `transient_provider_failure`;
- `ambiguous_external_effect`;
- `invalid_result_contract`;
- `incompatible_resume`;
- `cancelled`;
- `internal_invariant_violation`.

Retryability, fallback, degradation, wait, and operator-escalation behavior is authored per failure class. Catch-all retry or silent model/provider substitution is forbidden.

## 11. Mandatory conformance and lineage evidence

Stages 4–6 maintain one adapter conformance suite covering:

- exact-binding reproduction and no mutable alias resolution;
- authority/capability/resource intersection;
- cancellation and deadlines;
- effect claims and ambiguous results;
- immutable result/evidence/usage manifests;
- typed wait/pause/failure/cancel outcomes;
- trace/redaction and prohibited-state inspection;
- process loss before/after invocation and settlement;
- same semantic attempt with multiple technical attempts;
- no direct lifecycle mutation or terminality;
- complete lineage query from final result to inputs, assemblies, children, effects, and settlements.

Stage 4 runs it against native/test adapters, Stage 5 against the stable Deep Agent adapter, and Stage 6 against MCP, sandbox, async-child, and every enabled optional adapter.

## 12. Required cross-stage handoff artifacts

Each handoff updates rather than recreates:

- operation implementation kind matrix;
- stage requirement and execution-binding catalog;
- capability compiler version and prediction report;
- resource hierarchy/lease/release matrix;
- lineage schema and example end-to-end lineage query;
- failure/fallback/degradation matrix;
- compatibility and drift matrix;
- adapter conformance results;
- enabled/disabled maturity manifest;
- measured parallelism, utilization, waste, and checkpoint/state-size report.

## 13. Concrete implementation starting points

Inspect the current worktree before editing; these are seams, not permission to assume the planning snapshot is current:

- `app/domain/control_plane/contracts.py::StageNode` and `StageGraphBlueprint` for topology, joins, fairness, coarse stage slots, reservations, obligations, and output slots;
- `app/domain/orchestration/interpreter.py::StageGraphInterpreter` for the pure ready-frontier and semantic-attempt rules;
- `app/domain/orchestration/contracts.py::StageOperationRequest`, `StageOperationResult`, and stage execution state for the versioned operation outcome/wait projection;
- `app/domain/orchestration/goal_directed.py::GoalDirectedInterpreter` for protected deterministic GoalDirected semantics;
- `app/domain/graph_runtime/definitions.py::StageImplementationBinding`, `GraphAssemblySpec`, and `RunPlan` for the required versioned per-stage assembly extension;
- `app/domain/operation_execution/contracts.py::OperationExecutionBinding`, capability grants, delegations, workspace, and runtime-result contracts;
- `app/application/runtime_run_plan.py` and graph-runtime API schemas for structural compilation/serialization;
- Stage 1/2 migrations, repositories, dispatch services, graph exports, and their tests for compatibility-preserving additions;
- current coordinator semantic-binding/preparation services for eventual Stage 7 authoring and exact compilation.

Before implementation, create a requirements-to-code/test matrix that assigns every `06A` contract field to one authoritative model, compiler, repository, API schema, adapter, and test. Reject parallel duplicate models with overlapping authority.
