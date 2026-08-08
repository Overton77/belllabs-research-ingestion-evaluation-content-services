# Stages 3–6 shared contract — operation assembly, concurrency, lifecycle, and lineage

Status: `NOT_STARTED`
Document role: normative implementation contract for Stages 3–6
Scope: Temporal root/family/operation workflows, StageGraph, GoalDirected, native operations,
LangGraph/Deep Agents, MCP, sandboxes, external jobs, delegation, resources, effects, and settlement
Decision history: preserves D-02, D-04, D-05, D-08, D-11–D-16 and the accepted async-subagent
requirement; supersedes Agent Server macro-runtime assumptions with the accepted Temporal hierarchy

## 1. Governing boundary

The required flow is:

```text
immutable workflow semantics
  -> pure capability selection and exact compilation
  -> authoritative BellLabs admission and reservation
  -> BellLabsRunWorkflow
  -> StageGraphWorkflow | GoalDirectedWorkflow
  -> generic OperationWorkflow
  -> one exact OperationExecutor adapter
  -> immutable result/evidence/usage manifests
  -> deterministic BellLabs settlement
```

Temporal is the sole macro runtime. BellLabs application services and pure `StageGraphInterpreter`
and `GoalDirectedInterpreter` remain semantic authority. LangGraph and Deep Agents perform bounded
cognition inside operations. They do not schedule the macro graph, grant authority, accept evidence,
or terminalize a run.

`OperationExecutor` remains inside `OperationWorkflow`. It is not replaced by a separate workflow
per adapter. Local library/in-process execution and remote Agent Server execution are distinct exact
adapter variants with distinct bindings, manifests, task-queue needs, recovery behavior, and
compatibility keys.

## 2. Ownership

- Workflow definitions, RunPlans, assemblies, lifecycle, budgets, approvals, leases, effects,
  evidence acceptance, and terminality: BellLabs PostgreSQL/application services.
- Stage readiness, joins, fairness, cycles, and GoalDirected convergence: pure interpreters.
- Durable macro progress, child lifecycle, timers, retries, and runtime messaging: Temporal.
- Exact model/tool/skill/MCP/context/delegation surface: operation assembly compiler.
- Agent planning and bounded synchronous delegation: exact `OperationExecutor` adapter.
- Agent checkpoints: operation-scoped cognition state, never domain authority.
- Traces/evaluations: observational evidence, never lifecycle authority.

## 3. Exact operation contracts

### 3.1 `StageCapabilityRequirement`

```text
stage_id
operation_contract_ref
required_capability_ids
optional_capability_ids
input_contract_ref
output_contract_ref
context_purpose
effect_class                  # pure | read_only | idempotent_effect | consequential_effect
delegation_modes_allowed      # sync | async | linked_run
resource_class_ref
verification_contract_ref
degradation_contract_ref
speculation_policy_ref
```

Requirements contain no credentials, provider sessions, task queues, or mutable aliases.

### 3.2 `OperationAssemblySpec`

```text
operation_assembly_id
schema_version
operation_contract_ref
implementation_kind          # native | agent_harness | compiled_graph | async_child | linked_run
adapter_variant               # local_exact | remote_exact | other explicitly qualified variant
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
temporal_execution_profile_ref
compatibility_manifest_ref
operation_assembly_digest
```

Unused surfaces use exact empty/disabled manifests. The compiler rejects duplicate tools,
conflicting middleware/context editors, implicit general-purpose children, unsupported transports,
undeclared queues, runtime aliases, and grants wider than the authority intersection. One variant
cannot silently fall back to another.

### 3.3 `StageExecutionBinding`

```text
stage_id
variant_name
stage_requirement_ref
operation_assembly_ref
operation_assembly_digest
input_projection_ref
output_projection_ref
resource_envelope_ref
temporal_execution_profile_ref
compatibility_key
```

Every executable stage/variant has exactly one frozen binding. Scheduler nodes do not contain
hard-coded models, tools, queues, or adapter choices.

### 3.4 `OperationExecutor`

```text
execute(
  operation_request,
  exact_execution_binding,
  execution_resource_lease,
  cancellation_context,
  intervention_batch_ref?,
) -> OperationExecutionOutcome
```

The discriminated outcomes remain:

- `completed(result_manifest_ref, evidence_refs, usage_refs)`;
- `waiting(wait_binding_ref, retained_reservations, released_reservations)`;
- `paused(decision_ref)`;
- `degraded(reason_code, result_manifest_ref)`;
- `failed(failure_class, retryability, evidence_refs)`;
- `cancelled(settlement_refs)`.

The executor cannot mutate macro lifecycle directly. Native, local Deep Agent/LangGraph, remote
Agent Server, MCP, sandbox, async-child, and linked-run adapters pass one shared conformance suite.

## 4. Temporal execution hierarchy and parent lifecycle protocol

`BellLabsRunWorkflow` starts and owns one selected family child. A family child starts generic
`OperationWorkflow` children. An operation child owns one stable semantic operation attempt and
invokes its exact executor through one or more Activities.

Every parent uses this protocol:

1. **start** — derive the stable child Workflow ID, persist/start intent, invoke child start with
   exact digests, and reconcile `already started` or ambiguous transport;
2. **observe** — keep the child handle/binding, consume completion independently of siblings, and
   verify the compact returned manifest against BellLabs repositories;
3. **command** — authorize and persist a typed command, deliver through the `06C` service and
   Temporal Update, and record receipts;
4. **cancel** — persist cancellation intent, request cooperative cancellation, continue observing,
   and settle actual effects/usage;
5. **reconcile** — compare BellLabs binding, Temporal status, generation, external jobs, agent
   checkpoint, artifacts, effects, and settlement before deciding the next action.

An `OperationExecutionOutcome.waiting` does **not** close the child. `OperationWorkflow` remains open
durably on Temporal conditions, timers, Signals, or reconciliation Activities. It releases and
retains only resources declared by its lease policy.

## 5. Resource hierarchy and concurrency

### 5.1 `ExecutionResourceEnvelope`

```text
tenant_limit_ref
environment_limit_ref
workflow_run_slots
family_scheduler_slots
stage_slots
operation_workflow_slots
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

The effective ceiling intersects blueprint, implementation, parent/caller authority, BellLabs
admission, deployment capacity, provider quotas, and feature ceilings. Acquisition follows a
canonical order. Coordinator and resumption capacity is protected from operation saturation.

The family scheduler reserves before child start. Operation-local fan-out reserves subordinate
capacity before execution. Same semantic identity plus same envelope digest is idempotent; a
different digest conflicts. Leases are renewed, released, expired, and reconciled. Actual usage is
settled for failed, cancelled, speculative, quarantined, and discarded work.

### 5.2 Concurrency semantics

Synchronous children block their operation and receive bounded context/capabilities. Durable async
children release the parent worker but not the `OperationWorkflow`; callback or Signal only wakes
reconciliation and cannot settle directly. Linked runs are independently admitted BellLabs runs.

Real overlap is proven with barriers or controlled clocks, not result ordering. Tests measure
maximum concurrency and prove no over-admission, deadlock, starvation, lease leak, or resumption
deadlock.

Optimistic execution remains separately compiled and default-off. Only pure/read-only work may be
speculated; artifacts remain quarantined until deterministic acceptance and all observed usage is
settled.

## 6. Identity and lineage

### 6.1 Semantic and technical identities

```text
request_scope
belllabs_run_id
execution_epoch
technical_segment
workflow_implementation_ref
graph_assembly_digest
workflow_cycle
stage_id
stage_cycle
semantic_operation_attempt_id
execution_generation
runtime_attempt_id
operation_binding_id
operation_assembly_digest
```

- Fork: new `belllabs_run_id`, `execution_epoch = 1`, parent snapshot lineage.
- Continue-As-New: same run and epoch, incremented `technical_segment`, same Workflow ID, new
  Temporal Run ID.
- Disruptive operation restart: same semantic attempt, incremented `execution_generation`.
- Activity retry/resume: same semantic attempt and generation, new `runtime_attempt_id`.
- Semantic retry or a new stage/goal cycle: new semantic attempt.

### 6.2 Runtime lineage

```text
temporal_namespace_ref
root_workflow_id
root_temporal_run_id
family_workflow_id
family_temporal_run_id
operation_workflow_id
operation_temporal_run_id
activity_id
activity_attempt
task_queue_id
worker_build_id
agent_invocation_id
agent_thread_id
agent_checkpoint_ref
parent_lineage_id
delegation_mode
child_task_id
child_thread_id
child_run_id
external_job_id
```

### 6.3 Data, effect, and settlement lineage

```text
input_manifest_digest
context_manifest_digest
intervention_batch_refs
effect_claim_ids
result_manifest_ref
evidence_refs
usage_settlement_refs
effect_settlement_refs
trace_ref
```

Typed absence is required; fields are never overloaded. Every final result must resolve through
accepted operations to exact assemblies, inputs, contexts, messages, children, effects, usage,
artifacts, Temporal executions, agent checkpoints, and traces without treating any runtime record as
semantic authority.

## 7. Journal, effects, artifacts, and settlement

The operation journal is append-only and version checked. It records requested, reserved, started,
running, waiting, command, generation, result-observed, reconciling, settling, and terminal facts.
Temporal events may corroborate these facts but do not replace them.

Consequential work follows:

1. claim a stable BellLabs effect identity;
2. invoke or reconnect using provider idempotency where available;
3. persist immutable output/error/evidence/usage artifacts outside Temporal history;
4. reconcile ambiguous completion;
5. validate exact identity, generation, binding, and digest;
6. settle through BellLabs compare-and-set;
7. expose only the accepted compact manifest to the parent.

Same claim/same digest is idempotent. Same claim/different digest fails closed. A late result from an
old generation, cancelled child, invalidated fork branch, or orphan is quarantined and may settle
cost/effect liability but cannot become an accepted operation result.

Temporal Activity execution and command transport are at least once. There is no exactly-once
transport or provider-effect guarantee; BellLabs claims, inboxes, deduplication, immutable artifacts,
and settlement provide effective-once domain acceptance.

## 8. Operation state machine

```text
PENDING -> RESERVED -> STARTING -> RUNNING
  -> WAITING_ON_DECISION | WAITING_ON_EXTERNAL | WAITING_ON_AGENT_MESSAGE
  -> COMMAND_PENDING | CANCELLING | RECONCILING
  -> SETTLING
  -> COMPLETED | DEGRADED | FAILED | CANCELLED | ORPHANED
```

Disruptive recovery may transition `RUNNING|WAITING -> CANCELLING -> RECONCILING -> STARTING` while
retaining the semantic attempt and incrementing the execution generation. Orphan overlap is
default-denied and requires an exact policy, bounded liability, independent authority/resource
admission, and late-output quarantine.

Messages do not satisfy StageGraph dependencies. Only BellLabs settlement of a typed accepted
operation result changes dependency readiness.

## 9. Compatibility and failures

A running attempt remains bound to exact workflow code contract, task queue, retry/timeout profile,
graph/state/reducer schemas, operation assembly, adapter variant, model/tool/MCP/skill schemas,
agent checkpoint compatibility, and deployment/build manifest. Readiness revalidation may wait,
degrade, fail, or use an authored exact fallback; it may not widen or substitute silently.

Stable failure classes:

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
- `stale_execution_generation`;
- `cancelled`;
- `internal_invariant_violation`.

Retryability, backoff, fallback, wait, degradation, and escalation are authored per class. Catch-all
retry and unlimited expensive retries are forbidden.

## 10. Cross-stage implementation sequence

1. Stage 3 freezes this contract, implements the generic Temporal operation lifecycle with typed
   test/native fixtures, and publishes the conformance harness.
2. Stage 4 implements Temporal-native StageGraph frontier scheduling using child starts and
   incremental completion; it does not alter the executor boundary.
3. Stage 5 implements stable local Deep Agent/LangGraph exact adapters, remote adapter contract
   stubs, and GoalDirected consumption of the same child boundary.
4. Stage 6 implements and qualifies remote LangSmith adapters, then completes MCP, sandbox,
   external jobs, async children, heterogeneous composition, and remote intervention safe points.

## 11. Mandatory tests and handoff gate

Tests must prove:

- exact assembly reproduction and local/remote variant separation;
- parent start/observe/command/cancel/reconcile idempotency under ambiguous delivery;
- open durable waits across all worker restarts and Continue-As-New;
- stable semantic attempt with distinct segment, generation, Activity attempt, and runtime IDs;
- hierarchical ceilings, canonical acquisition, protected resumption, and wait release matrix;
- independent child overlap and incremental observation;
- effect ambiguity, duplicate execution, quarantine, and effective-once settlement;
- immutable result/evidence/usage manifests and complete lineage query;
- no direct adapter lifecycle mutation or terminality;
- cancellation, deadlines, generation restart, and policy-gated orphan overlap;
- no message-driven dependency satisfaction before settlement;
- no large or sensitive payload in Temporal surfaces.

The `06A` contract contribution is ready for `06-contract-frozen` when every field has one
authoritative implementation owner, the shared conformance harness runs against typed fixtures,
and `06B` and `06C` can reference these contracts without redefining identity, authority, resource,
executor, journal, effect, or settlement semantics. The gate authority records
`06-contract-frozen` only after these `06A` conditions and the contract-defining sections of `06`
are reviewed, versioned, and mutually consistent. That freeze authorizes downstream Stage 3
implementation; it is not aggregate Stage 3 acceptance.
