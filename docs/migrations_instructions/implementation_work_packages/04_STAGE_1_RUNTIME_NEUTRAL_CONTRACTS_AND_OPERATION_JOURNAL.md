# Stage 1 — runtime-neutral contracts, exact assembly, and PostgreSQL operation journal

Status: not started  
Mission type: production domain/application contracts, forward-only migrations, repositories, compatibility schemas, and transaction proofs  
Depends on: accepted Stage 0

## 1. Mission

Create the runtime-neutral anti-corruption layer that lets the existing BellLabs control plane and run control govern LangGraph/Deep Agents without importing provider semantics into domain truth. Publish exact graph/harness/context/delegation/environment contracts, establish canonical identity grammar, and add the authoritative PostgreSQL runtime/operation journal required for safe dispatch and settlement.

This stage must not yet port full StageGraph or GoalDirected execution. It makes those stages possible without creating a second authority or an exactly-once illusion.

## 2. Permission to clarify or interview

The agent may interview the owner before editing. Clarify if Stage 0 did not fully settle:

- exact public contract naming and `biotech-meta` authority;
- whether new definitions are individual kinds or four coordinator-facing bundles with content-addressed submanifests;
- PostgreSQL schema/table naming, migration/backfill window, and rollback authority;
- tenant/request-scope RLS model and DB roles;
- provider-neutral attempt vocabulary;
- context/Store retention and deletion semantics;
- typed intervention/fork surface and privileged repair policy;
- compatibility behavior for existing ERC digests and published records.

Do not change accepted vocabulary or data authority on an unrecorded assumption.

## 3. Required inputs

- accepted Stage 0 handoff and decision matrix;
- exact compatibility/maturity matrix;
- Stage 0 operation-transaction/backfill spike;
- current control-plane, run-control, orchestration, operation-execution contracts and repositories;
- current SQL migrations, RLS patterns, generic Mongo collections, Beanie operation documents;
- accepted `biotech-meta` naming and identity rules.

## 4. Target seams

Prefer extending these areas:

```text
app/domain/control_plane/
app/domain/operation_execution/
app/domain/orchestration/
app/domain/run_control/
app/domain/graph_runtime/             # only if a distinct runtime-fact lifecycle is accepted
app/application/runtime_execution_bindings.py
app/application/graph_runtime_dispatch.py
app/application/runtime_interventions.py
app/application/runtime_reconciliation.py
app/application/operation_execution.py
app/application/*_repository.py
app/migrations/
app/models/
app/integrations/
tests/
```

Names are targets, not permission to duplicate existing services. Inspect current code and merge with existing ports when ownership matches.

## 5. Deliverables

### 5.1 Canonical vocabulary and typed identities

Implement provider-qualified types/builders for:

- `request_scope`;
- `belllabs_run_id` and execution epoch;
- graph family, graph ID, graph assembly digest;
- `assistant_id`, deployment ID, deployment revision;
- Agent Server thread/run/checkpoint IDs;
- semantic operation/stage/iteration attempt keys;
- subagent profile, async task, child thread/run, and linked BellLabs run IDs;
- goal handoff checkpoint versus LangGraph checkpoint;
- runtime transport attempt versus semantic attempt.

Use suffix grammar from the migration plan. Reject ambiguous new fields such as unqualified `run_id` wherever both domains can occur. Preserve existing published logical IDs and semantic key grammar; changes require versioned migration.

### 5.2 Exact published/compiled definitions

Add or enhance strict Pydantic definitions for:

- `GraphAssemblyDefinition` / graph runtime profile;
- `AgentHarnessDefinition` / harness profile;
- ordered `MiddlewareStackDefinition`;
- `ContextPolicyDefinition` and compiled `ContextAssemblySpec`;
- `DelegationPolicyDefinition` with distinct continuity modes;
- synchronous subagent union: dictionary agent versus compiled graph;
- async subagent definition and runtime-policy refs;
- `MCPServerDefinition`/tool schema digest/session policy enhancements;
- `PromptContextBinding`;
- `InterpreterProfileDefinition`;
- `SandboxProfileDefinition` and snapshot policy;
- `ExecutionEnvironmentDefinition` or accepted equivalent bundle;
- `EvaluationProfileDefinition` enhancements;
- `StageImplementationBinding` operation implementation union;
- `GraphAssemblySpec` and stable compatibility manifests;
- granular capability/maturity record used by coordinator compilation.
- `StageCapabilityRequirement`, `OperationAssemblySpec`, and `StageExecutionBinding` (or deliberately versioned equivalents) from [06A_STAGES_3_TO_6_OPERATION_ASSEMBLY_CONCURRENCY_AND_LINEAGE_CONTRACT.md](06A_STAGES_3_TO_6_OPERATION_ASSEMBLY_CONCURRENCY_AND_LINEAGE_CONTRACT.md), so every stage/variant can bind a distinct exact model/harness/tool/MCP/skill/context/delegation/workspace/verifier/resource/fallback surface;
- `ExecutionResourceEnvelope`, `ExecutionLineageEnvelope`, typed failure taxonomy, and discriminated `OperationExecutionOutcome` contracts from `06A`.

The exact contract design may use four reusable bundles while preserving independently content-addressed submanifests. Every leaf must be an exact ref, digest/content ref, typed scalar, or authority-bounded value—not a mutable alias or secret value.

`StageImplementationBinding(stage_id, implementation)` alone is not sufficient for Stages 3–6 unless its implementation ref resolves immutably to the full per-stage operation assembly and compatibility/resource/lineage contracts. Prefer the explicit `StageExecutionBinding` join from `06A` and version the existing schema deliberately. Do not place one global harness/model/context policy on `RunPlan` when stages are permitted to differ without an exact per-stage override and deterministic resolution rule.

Implement the Stage 1 structural compiler phase from `06A`: validate exact refs/digests, authority and maturity intersections, feature flags, compatibility, complete stage coverage, disabled fallbacks, and predicted unavailable kinds. The stable executable compiler remains Stage 5; Stage 6 only extends it for providers and advanced delegation.

### 5.3 `RunPlan` and ERC integration

Compile exact semantic, control, graph, operations, harness, delegation, resources, context, and evaluation bindings into an ERC-backed `RunPlan` or equivalent exact refs/digests.

Requirements:

- existing ERC digest compatibility is explicitly decided;
- alias evidence is resolved once;
- graph assembly/state schema/reducer/operation registry digests are frozen;
- environment and feature maturity snapshot is frozen and revalidated at dispatch;
- coordinator preview can show redacted selected mechanics and incompatible combinations;
- runtime assembly cannot widen or reinterpret the plan.

### 5.4 Field governance metadata

For every new contract field record:

```text
writer
readers
authority_class
mutation_rule
retention
sensitivity
trace_policy
compatibility_behavior
```

Generate or validate this appendix from Pydantic schemas in CI. Unknown fields remain rejected.

### 5.5 Runtime API/domain contracts

Add strict types for:

- `GraphExecutionSubmission` and receipt;
- runtime execution binding/projection/view;
- runtime execution attempts;
- typed intervention discriminated union;
- durable interrupt envelope/response;
- async task binding/projection;
- BellLabs stream event with outbox cursor;
- fork request/receipt;
- redacted checkpoint summary;
- graph/runtime health and capability readiness;
- provider-neutral attempt metadata;
- subagent context slice and result manifest;
- context manifest/reconstruction result.

Normal intervention kinds include append input, satisfy wait, resume pause, respond to interrupt, update/cancel async task, cancel run, fork from checkpoint, and privileged operator reconcile. Each carries stable command/idempotency identity, expected BellLabs version, expected checkpoint when relevant, actor, reason, typed payload, and correlation.

### 5.6 PostgreSQL runtime coordination schema

Add forward-only migrations and repositories for the accepted form of:

- `runtime_execution_bindings`;
- `runtime_execution_attempts`;
- optional compact `runtime_checkpoint_observations`;
- `runtime_intervention_commands`;
- `runtime_interrupt_requests` and decisions, with compatibility to current decision tables;
- `runtime_async_tasks`;
- `operation_effect_claims`;
- `operation_execution_attempts`;
- `operation_settlements`.

Requirements:

- unique `(request_scope, belllabs_run_id, execution_epoch)`;
- unique scoped submission/idempotency identities with request digest conflict detection;
- one active binding per epoch;
- append-only attempt/journal history where required;
- stable identity and canonical digest references to the immutable
  MongoDB/Beanie-authoritative semantic `OperationExecutionBinding`; PostgreSQL must
  not become a competing semantic binding authority;
- typed identity/status/version/reconciliation columns; JSONB only for versioned detail payloads;
- request scope directly present or non-deferrably inherited from workflow run;
- enabled and forced RLS;
- least-privilege grants for migration owner, BellLabs runtime, Agent Server/runtime caller if separate, and read-only operations;
- indexes for pending reconciliation, lineage, provider IDs, leases/heartbeats, and idempotency;
- no checkpoint bodies, secrets, large results, or mutable semantic definitions.

### 5.7 Atomic operation journal and application service

Implement the accepted transaction boundary so related reservation, claim, attempt, settlement, lifecycle/version, budget, and outbox writes share one acquired async connection/transaction.

Rules:

- acquire claim before consequential effect;
- same key/same digest is idempotent;
- same key/different digest is a durable conflict;
- ambiguous effects enter reconciliation, not blind retry;
- providers without idempotency support are non-retryable or require claim plus result reconciliation;
- usage is recorded for failed/retried technical attempts;
- semantic retry creates a new semantic identity and reservation;
- cancellation remains distinct from failure;
- shadow execution cannot obtain the active consequential claim.

### 5.8 Mongo authority migration

Implement a migration repository/compatibility layer:

- select one authoritative write store by schema version;
- support bounded dual-read only;
- never unrecoverable dual-write;
- backfill canonical digests with original Mongo document IDs and timestamps;
- verify counts, digests, identities, and request scopes;
- keep old Mongo records read-only during rollback window;
- provide rollback/read-routing procedure;
- leave immutable definitions, semantic records, evidence metadata, context manifests, and artifact/snapshot metadata in their accepted Mongo roles.

### 5.9 Runtime-neutral ports

Add or adapt:

- `GraphRuntimeClient`;
- `WorkflowLaunchDispatcher` runtime-neutral implementation selection;
- `RuntimeExecutionBindingRepository`;
- runtime attempt/intervention/decision/async-task repositories;
- operation journal repository;
- reconciliation ports;
- capability/assembly resolver that loads exact refs only;
- runtime selector at exact Workflow Implementation granularity.

No pure domain module imports LangGraph, Deep Agents, Agent Server SDK, MCP adapters, or Sandbox clients.

### 5.10 Shared envelope and schema export foundation

Define the standard BellLabs v2 success/error envelope and shared principal mapping contracts without yet implementing the complete route set. Ensure schema export contains every new request, response, error, and discriminated union.

## 6. Required tests

### Contracts and schemas

- strict parsing and unknown-field rejection;
- canonical digest round trips and deterministic ordering;
- exact-ref/digest mismatch rejection;
- semantic compatibility snapshots;
- field-governance coverage test;
- old ERC/published-record compatibility or explicit migration failure;
- provider-qualified identity validation;
- four delegation modes cannot be confused or enabled by one boolean;
- middleware conflicts and duplicate core middleware reject at compilation;
- skill/backend/deployment and linked-run boundary validation;
- feature maturity/fallback compilation.

### PostgreSQL and repositories

- migration apply in a clean database and upgrade from current schema;
- RLS positive/negative tenant tests for every table;
- role/grant tests;
- idempotent submission and digest conflict;
- concurrent active-binding uniqueness;
- claim/reserve/settle/outbox atomicity;
- crash injection and reconciliation at every transaction boundary;
- cancellation and usage settlement;
- pending/lease/heartbeat indexes used by representative queries.

### Mongo backfill

- exact digest/count/source-lineage proof;
- replay idempotency;
- conflict quarantine;
- dual-read selection by schema version;
- rollback to old read authority during accepted window;
- no legacy record mutation or deletion.

### Runtime-neutral seam

- one no-op deterministic operation executes through legacy and stub graph adapters with the same frozen binding/result;
- launch ambiguity is reconciled by metadata/submission key;
- runtime identifiers from untrusted callers are rejected;
- graph adapter cannot mutate compile/admission authority.

## 7. Gate

Stage 1 passes when:

- accepted exact contracts and naming grammar are implemented and schema-exported;
- all forward migrations and RLS/grants pass;
- atomic journal crash/idempotency tests pass;
- backfill/rollback is proven on representative data without destructive deletion;
- one operation uses either runtime through the same domain contract;
- no domain authority moved into checkpoint/Store/provider state;
- all I/O repositories/services are native async;
- unresolved beta/preview features remain disabled in compiled capability records;
- full accepted lint/type/test baseline passes;
- outgoing handoff is accepted.

## 8. Explicit non-goals

- Do not export production Agent Server graphs yet.
- Do not implement full StageGraph/GoalDirected graph nodes.
- Do not expose all v2 routes yet.
- Do not switch default runtime or remove legacy dependencies.
- Do not attach online evaluators or deploy Cloud resources.

## 9. Outgoing handoff additions

Include:

- exact contract/class/schema inventory and digests;
- identity/naming grammar;
- authority/storage map;
- migration IDs, apply/rollback/backfill evidence, RLS/grants;
- operation transaction state machine and crash matrix;
- runtime-neutral port/interface examples;
- enabled/disabled capability/maturity manifest;
- compatibility impacts for ERCs, published definitions, APIs, and future checkpoints;
- exact starting points for Agent Server graph exports/auth in Stage 2.
