# Stage 4 — generic capability-aware StageGraph scheduler and native parity slice

Status: not started  
Mission type: production graph implementation around the existing deterministic StageGraph interpreter, with selected workflow shadow parity  
Depends on: accepted Stages 1–3

## 1. Mission

Port the BellLabs StageGraph execution lifecycle to the standard Agent Server graph without translating Temporal mechanics or weakening the existing interpreter. The graph must hydrate exact per-stage execution bindings, reconcile run control, compute the fair admitted frontier, reserve hierarchical resources before effects, dispatch bounded workers through the Stage 3 `OperationExecutor` port, settle results deterministically, evaluate cycles/joins/reuse, wait/interrupt durably, and materialize the existing typed result.

Use a representative deterministic/native schema-grounding implementation as the first vertical slice, selected from the accepted Stage 0 baseline. Prove behavior against the legacy path with one runtime holding consequential provider-effect claims. Do not choose a slice that forces Stage 4 to construct a temporary Deep Agent, outbound MCP client, skill runtime, sandbox provider, or subagent implementation.

Stage 4 is capability-aware because every stage is bound to an exact requirement and assembly digest. It is capability-mechanics-free because later adapters execute behind the unchanged operation port. Follow [06A_STAGES_3_TO_6_OPERATION_ASSEMBLY_CONCURRENCY_AND_LINEAGE_CONTRACT.md](06A_STAGES_3_TO_6_OPERATION_ASSEMBLY_CONCURRENCY_AND_LINEAGE_CONTRACT.md).

## 2. Permission to clarify or interview

The agent may interview the owner before starting. Clarify:

- which Workflow Type/Implementation is the first parity target;
- accepted parity dimensions and tolerances for semantic/agentic outputs;
- whether the direct reconciliation service remains one coarse operation or is decomposed into declared stages now;
- shadow execution policy for external reads/writes and cost;
- execution epoch greater than one: implement now or reject at admission;
- failure/degradation behavior for optional providers;
- generated native graph optimization remains out of scope unless owner changes D-03;
- the deterministic/native vertical slice that does not depend on Stage 5/6 adapters;
- whether optimistic execution remains disabled, as required by default, or a separately published pure/read-only policy will be designed for a later stage.

## 3. Existing BellLabs seams to preserve

Inspect and reuse:

- `app/domain/orchestration/interpreter.py::StageGraphInterpreter`;
- StageGraph contracts/identities/bindings in `app/domain/orchestration/`;
- `app/application/orchestration.py::StageGraphLaunchService` and `WorkflowLaunchDispatcher`;
- `StageOperationExecutor`, `WorkflowEvaluator`, and lifecycle gateway ports;
- `RunControlService`, pure lifecycle reducer, budget/outbox repositories;
- control-plane Workflow Type/Implementation/ERC compilation;
- coordinator prepared tickets/semantic bindings/results;
- schema-grounding application services and immutable records;
- legacy `app/temporal/stagegraph_workflow.py` as behavior evidence, not target topology;
- Stage 3 `OperationExecutor`, lineage, resource-envelope/lease, typed failure/outcome, and adapter-conformance contracts from `06A`.

Do not copy provider-specific workflow/activity retries into the new domain path.

## 4. Target graph shape

Implement stable nodes equivalent to:

```text
hydrate_runtime_binding
reconcile_run_control
compute_frontier
reserve_frontier
dispatch_ready
execute_operation
settle_frontier
evaluate_cycles_and_reuse
wait_or_interrupt
materialize_result
terminalize
reconcile_or_fail_safely
```

Exact names become checkpoint compatibility surfaces. If Stage 2 published placeholder names, preserve or version them deliberately.

## 5. Deliverables

### 5.1 StageGraph state and reducers

Implement compact channels for:

- authoritative stage projection ref/version;
- workflow cycle and fairness cursor;
- exact dispatch batch and semantic keys;
- pending result/failure refs with conflict reducer;
- typed `pending_external_work` refs with conflict reducer, capable of representing decision, provider job, sandbox job, async child, and linked-run waits without importing Stage 6 SDK types;
- wait projection and retained/released resource-lease refs;
- immutable reuse candidates;
- common Stage 3 channels.

Parallel workers return refs/manifests only and never mutate `stage_projection`. One deterministic settlement node sorts by semantic identity and invokes existing application/domain transitions through CAS.

Publish the generic operation-state projection from `06A` now, including `RESERVED`, `DISPATCHED`, `RUNNING`, `WAITING_ON_DECISION`, `WAITING_ON_EXTERNAL`, `WAITING_ON_ASYNC_CHILD`, `READY_TO_RECONCILE`, `SETTLING`, and typed terminal dispositions. Stage 6 fills async-child bindings behind these generic channels; it must not add an ad hoc scheduler node/channel or store Agent Protocol objects in checkpoint state.

### 5.2 Hydration/reconciliation

- load exact ERC/RunPlan/assembly and operation registry by digest;
- require exactly one `StageCapabilityRequirement` and `StageExecutionBinding` for every selected stage/variant;
- load the exact `OperationAssemblySpec`, resource envelope, input/output projections, and compatibility key without constructing provider resources;
- verify all published refs and implementation binding;
- verify runtime binding/endpoint/graph/schema compatibility;
- read current lifecycle/budget/decision state;
- rebuild compact projection if checkpoint lags accepted PostgreSQL truth;
- fail closed on mismatched digests or impossible transitions.

### 5.3 Fair frontier and reservations

Call the pure interpreter with current projections. Enforce the minimum of:

- blueprint maximum parallel stages;
- admitted BellLabs run concurrency;
- reserved provider/operation limits;
- deployment/runtime capacity;
- feature-specific ceilings.

Preserve fairness cursor, joins, cycle ceilings, waits, reuse, and invalidation. Persist semantic attempt identities and acquire budget/concurrency/effect claims plus the Stage 3 hierarchical resource leases before `Send` fan-out. Use the canonical acquisition order and protect resumption capacity. Frontier slots, operation-worker slots, and subordinate model/tool/MCP/subagent slots remain distinct.

Use barriers or controlled clocks to prove actual worker overlap and maximum-observed concurrency. Randomized result ordering is necessary reducer evidence but is not sufficient proof of parallel execution.

Ordinary Stage 4 scheduling is dependency-safe concurrency, not optimistic speculation. Keep every `speculation_policy_ref` disabled. A future pure/read-only speculative adapter must follow `06A`, quarantine outputs, and pass a separate gate; it cannot be inferred from idempotency or `Send` fan-out.

### 5.4 Operation registry

Implement exact registry dispatch through the Stage 3 `OperationExecutor` port for accepted initial kinds:

- deterministic async Python/application service;
- deterministic invocation-scoped subgraph when it has no Stage 5/6 capability dependency;
- typed test adapters for completed/waiting/paused/degraded/failed/cancelled conformance outcomes;
- non-executable, readiness-reporting placeholders for `agent_harness`, MCP-backed, sandbox, async-child, and linked-run kinds that later stages register behind the same port.

Registry inputs are frozen `StageExecutionBinding`/`OperationAssemblySpec` records. Reject unknown kind, schema drift, missing capability, mutable alias, absent per-stage binding, or deployment-incompatible implementation with the shared typed failure taxonomy. Do not silently fall back to a plain agent or hard-coded model.

### 5.5 Worker execution boundary

Every operation:

1. reloads/verifies the exact stage requirement, execution binding, operation assembly, resource lease, and compatibility manifest;
2. uses stable semantic and effect identities;
3. derives deadline/cancellation;
4. invokes native async adapter;
5. writes immutable result/error/usage/evidence refs;
6. never applies lifecycle transition directly;
7. returns one compact manifest for settlement.

The manifest includes or resolves the complete `ExecutionLineageEnvelope`. Worker code cannot choose a different model, prompt, tool, skill, MCP server, child, backend, verifier, or fallback at runtime.

Runtime replay reuses the same semantic identity. Semantic retry is created only by the interpreter/domain policy.

### 5.6 Deterministic settlement and cycle evaluation

- sort result/failure manifests deterministically;
- verify digests/schemas/attempt identities;
- settle effect/usage/budget exactly once;
- apply existing StageGraph interpreter/application transition through expected-version CAS;
- evaluate joins, stage/workflow cycles, descendant invalidation, and reuse;
- emit durable events and compact checkpoint updates;
- reconcile conflicts rather than last-writer-wins.

### 5.7 Wait, pause, resume, cancellation, and readiness

Map external wait, human decision, pause, and readiness conditions to durable BellLabs state plus Stage 3 interrupts. Resume begins by reconciliation. Cancellation cooperatively stops fan-out/operations and settles observed usage. Wait/resume must survive process loss.

### 5.8 Result materialization and terminality

Build the existing typed StageGraph workflow result from accepted immutable refs. Terminal node:

- verifies required obligations and outputs;
- checks no unresolved required wait/operation/effect/usage;
- calls BellLabs terminal completion service with expected version;
- records final result binding/ref;
- cannot terminalize from checkpoint state alone.

### 5.9 Schema-grounding lineage

For the selected schema workflow, preserve:

- exact catalog/selection/projection/deployment/workspace/capability lineage;
- deterministic graph admission before executor creation;
- no arbitrary Cypher;
- immutable intent/result/evidence records;
- successful-zero/rejected/failed distinctions;
- deterministic latest-record ordering;
- typed final result compatibility.

### 5.10 Shadow comparison harness

Run identical frozen bindings/input manifests through legacy and LangGraph paths.

Rules:

- one active runtime holds provider-effect claim;
- passive shadow uses captured/read-only results where a claim cannot be shared safely;
- compare schedule semantics, obligations, outputs, evidence, usage, budgets, and result contracts;
- do not require incidental trace ordering to match;
- preserve rejected/failure behavior as valid parity cases.

## 6. Required tests

### Pure/domain parity

- all existing `StageGraphInterpreter` tests against graph routing/settlement;
- dependency modes and joins;
- fairness under constrained slots;
- stage/workflow cycles and ceilings;
- invalidation and reuse;
- wait/readiness/failure/degradation;
- semantic identities and deterministic ordering.

### Reducer/concurrency

- multiple roots and joins via `Send`;
- global/per-stage/runtime caps before fan-out;
- randomized completion order;
- barrier/controlled-clock proof that eligible workers actually overlap;
- observed concurrency respects workflow, stage, operation-worker, tenant, and deployment limits;
- lease release on completion/failure/cancel and protected resumption capacity;
- duplicate same-digest result;
- duplicate conflicting result incident;
- cancellation mid-frontier;
- no shared mutable projection from workers.

### Effects/recovery

- crash before/after operation call and before/after settlement;
- provider timeout/ambiguous result;
- no duplicate external effect;
- restart from every meaningful checkpoint;
- interrupt/wait resume;
- stale lifecycle version reconciliation.

### Binding, adapter, and lineage

- missing or duplicate per-stage requirement/execution binding fails compilation/hydration;
- native and typed test adapters pass the shared `OperationExecutor` conformance suite;
- unimplemented agent/MCP/sandbox/async kinds return typed `capability_unavailable` and never construct a fallback;
- different stage variants select different exact assemblies without changing scheduler nodes;
- final typed result traces through every stage, semantic/runtime attempt, assembly, input/output manifest, effect, usage settlement, and trace ref;
- task/thread/run/operation IDs fail validation when placed in the wrong typed field;
- all Stage 4 speculation policies remain disabled.

### Vertical-slice E2E

- prepare/admit/dispatch/stream/wait/cancel/result through authenticated APIs;
- selected deterministic/native schema workflow accepted and rejected cases;
- legacy versus graph parity matrix;
- checkpoint state size and prohibited-data inspection;
- LangSmith trace hierarchy and redaction.

## 7. Gate

Stage 4 passes when:

- existing pure interpreter behavioral tests pass against graph runtime;
- Agent Server E2E proves joins, concurrency, fairness, cycles, wait/resume, crash recovery, invalidation/reuse, cancellation, and stable effects;
- chosen vertical slice matches accepted legacy contracts/results/evidence within owner-approved semantic tolerance;
- one runtime only owns consequential effects in shadow;
- schema-grounding lineage and typed results match accepted contracts;
- every stage/variant has an exact requirement and execution binding and every native adapter passes the shared conformance suite;
- measured worker overlap proves real bounded parallelism without resource over-admission or leakage;
- Deep Agents, MCP, skills, sandboxes, sync/async subagents, and speculative execution are not temporarily implemented in the Stage 4 graph;
- top-level checkpoints contain no full transcripts, large payloads, secrets, or PHI;
- execution epoch policy is implemented or rejected at admission;
- no Temporal imports exist in the Agent Server graph package;
- outgoing handoff is accepted.

## 8. Explicit non-goals

- Do not replace the generic interpreter with generated graphs.
- Do not make every stage an agent.
- Do not complete full GoalDirected/Deep Agents behavior.
- Do not construct a minimal or temporary agent/MCP capability stack; Stage 5/6 adapters plug into the Stage 3 port.
- Do not enable preview async/dynamic delegation.
- Do not change default runtime for broad production admissions.

## 9. Outgoing handoff additions

Include:

- stable graph topology/state/reducer manifest;
- stage requirement/execution-binding catalog and exact assembly digests;
- operation registry kind/readiness matrix and shared conformance results;
- parity matrix by legacy test/workflow behavior;
- measured concurrency/fairness/resource-lease/reducer proof;
- crash/effect/settlement matrix;
- selected vertical-slice trace/evidence/result refs;
- checkpoint size/prohibited-data report;
- known semantic differences and accepted decisions;
- shadow effect-ownership strategy;
- end-to-end lineage query/report for the native slice;
- reusable operation/hydration/reconciliation pieces for the Stage 5 Deep Agent adapter and GoalDirected.
