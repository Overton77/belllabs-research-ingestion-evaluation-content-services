# Stage 3B — Temporal workflow foundation

Status: `NOT_STARTED`
Document role: normative Stage 3 implementation contract
Depends on: recorded `06-contract-frozen` gate from the reviewed [`06`](06_STAGE_3_DURABILITY_HITL_STEERING_AND_RECOVERY.md) and [`06A`](06A_STAGES_3_TO_6_OPERATION_ASSEMBLY_CONCURRENCY_AND_LINEAGE_CONTRACT.md) contract sections
Feeds: [`06C`](06C_STAGE_3_COMMUNICATION_AND_INTERVENTION_QUALIFICATION.md) and the Stage 3 handoff in [`06`](06_STAGE_3_DURABILITY_HITL_STEERING_AND_RECOVERY.md)

## 1. Mission and authority

Implement the Temporal foundation for BellLabs macro execution without moving semantic authority
into Temporal. Temporal is the sole macro runtime; BellLabs PostgreSQL and application services own
lifecycle, budgets, approvals, effects, evidence, and terminality; pure interpreters own StageGraph
readiness and GoalDirected convergence.

Workflow code is deterministic coordination code. Database/network/provider/model/object-store work
occurs only through typed Activities. Event History stores compact execution facts and refs, not
large or sensitive application data.

## 2. Workflow hierarchy

### 2.1 `BellLabsRunWorkflow`

One distinct root workflow exists for every admitted BellLabs run. Its input contains:

```text
request_scope
belllabs_run_id
execution_epoch
technical_segment
workflow_implementation_ref
run_plan_ref
graph_assembly_digest
authoritative_projection_ref/version/digest
family_kind
parent_fork_lineage_ref?
compatibility_manifest_ref
```

The root:

- rehydrates and reconciles through BellLabs Activities before progress;
- starts exactly one selected family child;
- routes authorized lifecycle commands;
- enforces outer deadlines and parent-close/cancellation policy;
- maintains compact active-family and pending-command refs;
- reconciles family outcome before BellLabs terminalization;
- Continue-As-New at certified safe points.

It does not compute StageGraph readiness or GoalDirected convergence.

### 2.2 Family workflows

`StageGraphWorkflow` and `GoalDirectedWorkflow` share lifecycle libraries but remain explicit workflow
types. Each:

- hydrates its frozen definition, RunPlan, bindings, and authoritative projection;
- calls its pure interpreter with deterministic inputs;
- reserves through BellLabs Activities before dispatch;
- starts generic `OperationWorkflow` children;
- observes child completion independently and settles in deterministic semantic order;
- keeps slow siblings open, cancels them, or abandons them only under frozen policy;
- rehydrates after repair and continuation;
- returns a compact family outcome manifest.

Stage 3 implements typed fixtures and lifecycle mechanics, not StageGraph frontier or GoalDirected
business semantics. Those arrive in later stages.

### 2.3 `OperationWorkflow`

One child owns one `semantic_operation_attempt_id`. Input additionally contains:

```text
execution_generation
operation_binding_id
operation_assembly_digest
adapter_variant
resource_lease_ref/version
temporal_execution_profile_ref
input/context manifest refs
```

The operation workflow revalidates exact binding and lease, hydrates bounded context, invokes or
resumes `OperationExecutor` through Activities, coordinates commands and external waits, persists
immutable artifacts, reconciles effects/usage/results, and returns a compact
`OperationExecutionOutcome`.

Waiting never requires the child to close. Decision, external, provider, or agent-message waits stay
open on durable Temporal conditions/timers and release worker capacity.

## 3. Stable Workflow ID grammar

Workflow IDs are deterministic, opaque-safe, length-bounded, and derived from canonical encoded
typed IDs; raw user text is forbidden.

```text
root:      bl:{scope_hash}:run:{belllabs_run_id}:e:{execution_epoch}
family:    bl:{scope_hash}:run:{belllabs_run_id}:e:{execution_epoch}:family:{family_kind}
operation: bl:{scope_hash}:run:{belllabs_run_id}:e:{execution_epoch}:op:{semantic_attempt_id}:g:{execution_generation}
```

Namespace is an environment/deployment boundary and is recorded separately. `Continue-As-New`
retains Workflow ID and creates a new Temporal Run ID. A disruptive operation restart increments
generation and therefore creates a new operation Workflow ID. A fork creates a new BellLabs run at
epoch `1`.

Use explicit Workflow ID reuse/conflict policies. Same identity/same digest reconciles; same
identity/different digest fails closed. Search attributes and memo contain allowlisted,
non-sensitive, bounded operational fields only.

## 4. Query, Signal, and Update contracts

### Queries

Queries are compact, diagnostic, and side-effect free:

- `get_runtime_summary`;
- `get_active_children`;
- `get_wait_summary`;
- `get_pending_command_ids`;
- `get_last_reconciled_projection`.

They expose IDs, classes, versions, and bounded status only. Product APIs normally read BellLabs
projections; Query availability is not a product consistency guarantee.

### Signals

Signals carry durable, fire-and-forget facts:

- external job or artifact may be ready;
- wait condition may be satisfiable;
- cancellation intent changed;
- child progress or reconciliation wake-up occurred.

Handlers validate envelope shape, deduplicate compact fact IDs, update workflow-local wake state,
and return quickly. They do not authorize, settle, or terminalize.

### Updates

Updates are used when the caller requires acceptance/rejection:

- route an already-persisted command;
- request pause/resume/cancel;
- request approved deadline/priority/decision changes;
- prepare a semantic snapshot/fork;
- request operation intervention batch observation.

The Update input carries the PostgreSQL command ID and expected target/version, not the full
business payload. Validators reject malformed targets and incompatible workflow state; BellLabs
authorization and compare-and-set remain authoritative. Handlers serialize or explicitly guard
conflicting mutations and must quiesce before Continue-As-New.

`06C` defines the authoritative command/message ledger and receipt semantics.

## 5. Task queues and worker registrations

Logical queue profiles are stable contract IDs, not production hosting decisions:

```text
belllabs.coordinator.root
belllabs.coordinator.stagegraph
belllabs.coordinator.goal-directed
belllabs.operation.native
belllabs.operation.agent-local
belllabs.operation.agent-remote
belllabs.operation.ingestion-io
belllabs.operation.sandbox
belllabs.operation.verification
belllabs.operation.external-provider
belllabs.maintenance.reconciliation
```

Queue selection is compiled into the exact execution profile. Workflows, models, and incoming
commands cannot choose undeclared queues. Worker startup registers an exact allowlist of workflow
and Activity types plus build/compatibility identity. Coordinator/resumption workers retain
protected capacity and do not scale to zero in qualification assumptions.

Provide a registration manifest test that fails for duplicate type names, missing Activities,
unexpected queue/type combinations, or a build claiming unsupported history compatibility.

## 6. Retry, timeout, heartbeat, and cancellation profiles

Profiles are immutable versioned refs attached to exact bindings.

- Workflow Execution/Run timeouts are set only for real product bounds.
- Schedule-To-Start detects missing/misrouted worker capacity.
- Start-To-Close bounds one technical Activity attempt.
- Schedule-To-Close bounds the Activity including retries.
- Heartbeat Timeout detects lost long-running work.
- Durable timers implement delays, deadlines, backoff, and reconciliation.

Long-running Activities heartbeat compact phase, artifact/checkpoint ref, provider job typed ID,
processed cursor, usage summary, and last safe cancellation point. No sensitive content is allowed.

Retries are bounded by failure class. Validation, authorization, capability drift, stale generation,
and invalid result are non-retryable until an explicit repair/rebind. Ambiguous consequential
effects reconcile before retry. Expensive model/tool retries are bounded. Cancellation is
cooperative and Activities must heartbeat or poll cancellation where applicable.

Temporal does not provide exactly-once effects. Every consequential Activity uses `06A` BellLabs
effect claims and settlement.

## 7. Continue-As-New

Continue-As-New preserves:

- same BellLabs run and execution epoch;
- same Workflow ID and workflow chain;
- incremented `technical_segment`;
- new Temporal Run ID and fresh history.

The continuation payload contains only compact refs/digests:

```text
run/epoch/next-segment
authoritative projection ref/version/digest
exact compatibility refs
active child bindings and expected dispositions
processed fact/command sequence cursors
pending wait and cancellation refs
```

At a continuation safe point:

1. stop accepting conflicting transitions;
2. await all Update/Signal handlers;
3. persist/reconcile pending command receipts;
4. snapshot compact active-child bindings;
5. verify no unrecorded child-start intent exists;
6. continue as new.

The new segment rehydrates BellLabs state and reattaches/reconciles active children. It does not
assume that a recorded handle is current, cancel children merely to compact history, or create a new
BellLabs epoch.

## 8. Rehydration and active-child reconciliation

At workflow start, cache eviction recovery, explicit repair, and every new segment, use Activities
to load:

- authoritative run and family projection;
- exact definition/binding/compatibility refs;
- operation journal and active generation;
- resources, budgets, approvals, effect claims, and settlements;
- command/message cursors;
- child, external-job, agent-thread/checkpoint, artifact, and result bindings.

For each intended or active child:

1. derive its stable Workflow ID;
2. compare BellLabs start intent/binding with Temporal existence/status;
3. attach/observe if identity and digest match;
4. complete an unrecorded start observation idempotently;
5. reconcile closed result through BellLabs before applying it;
6. request authorized cancellation or restart when policy requires;
7. escalate identity/digest conflict without guessing.

BellLabs projection is rebuilt from authoritative journal/repositories and pure reducers. Temporal
status can trigger reconciliation but cannot advance semantic state by itself.

## 9. Replay and N/N+1 qualification

Check in or govern representative sanitized histories for:

- root/family/operation start and completion;
- waiting child and message delivery;
- Activity retry and ambiguous effect;
- cancellation and generation restart;
- child running across parent Continue-As-New;
- fork-derived run start;
- every workflow code-version branch.

Required matrix:

- code `N` replays histories produced by `N`;
- compatible code `N+1` replays supported `N` histories;
- `N+1` produces new histories that replay under `N+1`;
- intentionally incompatible changes fail qualification and require explicit versioning/routing;
- worker build retirement is blocked while compatible open histories remain.

Replay tests execute no real Activities or external effects. Workflow code uses Temporal-safe
determinism APIs and versioned behavior changes.

## 10. Local self-hosted Temporal qualification

Stage 3 runs against a local self-hosted Temporal service with isolated namespace, PostgreSQL test
stores, and deterministic worker manifests. This is qualification infrastructure, not a production
hosting decision.

The suite must:

- start/stop API, coordinator, and Activity workers independently;
- preserve Temporal service while killing workers for recovery tests;
- inject Activity timeout, heartbeat loss, duplicate command delivery, ambiguous child start, and
  database/object-store/provider failures;
- inspect Event Histories and payload metadata for size and prohibited content;
- retain sanitized histories for replay;
- cleanly isolate test tenants/namespaces and remove no evidence needed by the handoff.

## 11. Q/D durable skeleton implementation

**Exact supporting input:**
[`REFERENCE_BLUEPRINT_STAGE3_MAPPING.md`](../stage2_evidence/REFERENCE_BLUEPRINT_STAGE3_MAPPING.md).
That file is not another work package. Its operation table and Q/D identities belong to this
section (§11), its Temporal assertions are verified under §12, and its resulting evidence is named
in the §13 handoff. Persisted command delivery and receipt qualification remain package `06C` work.

Implement immutable Stage 3 versions of both `00A` reference blueprints. They are intentionally
small but run through production-shaped seams:

```text
BellLabs application/API command
  -> admission and frozen implementation
  -> BellLabsRunWorkflow
  -> explicit fixture family workflow
  -> generic OperationWorkflow
  -> deterministic native/small exact Activity
  -> authoritative journal/evidence/usage/result settlement
```

Q must at minimum normalize a tiny catalog fixture, classify current-offer evidence, wait for or
receive one typed human decision, and publish a typed product result. D must classify a tiny
company-relationship fixture, preserve an ambiguous ownership claim as unknown, accept one typed
evidence/wake fact, and publish a typed ownership result. Between the two runs exercise Query,
Signal, Update, cancellation, worker loss, open wait recovery, and Continue-As-New.

Mandatory replay/crash assertions use deterministic fixtures. A bounded live Tavily retrieval or
tiny exact LLM Activity may be included only if its binding, secret reference, budget, result
schema, and non-replay execution boundary are explicit. It cannot be the deterministic oracle and
there is no silent OpenAI/Anthropic/Tavily fallback.

The fixture family workflows exist only to prove common lifecycle mechanics and are replaced by
the real `StageGraphWorkflow`/`GoalDirectedWorkflow` without changing root, operation, journal,
command, or result contracts.

## 12. Acceptance tests

- stable IDs and conflict policies under duplicate/ambiguous start;
- root starts one exact family and rejects incompatible family rebinding;
- operation semantic identity survives Activity retries;
- open waits survive all worker restarts without polling loops that hold workers;
- parent observes one child completion while a sibling remains open;
- Continue-As-New keeps run+epoch, increments segment, deduplicates commands, and reattaches children;
- fork starts a new run at epoch `1`;
- all Query/Signal/Update authority restrictions pass;
- queue registration and exact adapter routing fail closed on drift;
- bounded retry/timeout/heartbeat/cancellation profiles behave by failure class;
- N/N and N+1/N histories replay;
- rehydration repairs every injected recoverable mismatch and escalates unsafe conflicts;
- no large document, transcript, secret, credential, PHI, or unrestricted command body appears in
  history, memo, search attributes, heartbeat, log, or error.
- both Q and D skeletons execute through the registered workers and real application/persistence
  ports, publish comparison manifests, and have no direct provider or demo-runtime bypass;
- configured-key checks reveal only presence/absence and never emit secret values.

## 13. Handoff gate

`06B` passes when the generic root/family/operation foundation runs on local self-hosted Temporal,
all acceptance tests pass, replay histories and worker manifests are published, active-child
continuity is proven, and `06C` can route persisted commands without inventing a second runtime or
authority channel. The handoff names exact Q/D versions, commands, histories, worker paths,
deterministic evidence, live-canary evidence or skip reason, and the Stage 4 seams that replace only
fixture family semantics.
