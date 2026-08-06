# Stage 3 — durable runtime kernel: binding, lineage, resources, HITL, steering, and recovery

Status: not started  
Mission type: production runtime coordination foundation shared by both graph families  
Depends on: accepted Stages 1 and 2

## 1. Mission

Implement the common durable execution kernel beneath StageGraph and GoalDirected: transactional graph dispatch, authoritative runtime identity and lineage binding, hierarchical resource leases, compact state/reducers, decision/interrupt bridge, typed interventions, cancellation, forks, resumable streams, checkpoint compatibility enforcement, and continuous reconciliation.

This stage must prove that process loss, ambiguous submission, replay, interrupt resume, or operator intervention cannot bypass BellLabs lifecycle/budget authority or duplicate a consequential effect.

This stage implements the shared primitives in [06A_STAGES_3_TO_6_OPERATION_ASSEMBLY_CONCURRENCY_AND_LINEAGE_CONTRACT.md](06A_STAGES_3_TO_6_OPERATION_ASSEMBLY_CONCURRENCY_AND_LINEAGE_CONTRACT.md). It does not schedule the StageGraph business frontier and does not construct models, Deep Agents, MCP clients, skills, subagents, or sandboxes.

## 2. Permission to clarify or interview

The agent may interview the owner before starting. Clarify:

- accepted concurrent-run policy per intervention type;
- interruption versus enqueue semantics for active work;
- operator repair roles and `update_state` scope;
- fork versus retry versus epoch-rollover policy;
- cancellation cascade policy for operations, MCP sessions, sandboxes, async tasks, and linked runs;
- event retention/reconnect expectations;
- checkpoint summary visibility and redaction;
- reconciliation SLOs, incident severity, and automatic versus human repair boundaries;
- checkpoint-incompatible deployment/fail-safe behavior.

Do not expose a broader state mutation or rollback mechanism because it is convenient in the SDK.

## 3. Required inputs

- Stage 1 runtime contracts, tables, repositories, and operation journal;
- Stage 2 graphs/auth/custom app/client adapter;
- Stage 0 interrupt/state/fork/checkpoint compatibility evidence;
- existing run-control reducer/service/outbox/Socket.IO behavior;
- current coordinator launch idempotency and semantic binding services;
- accepted blue/green endpoint policy;
- the shared operation assembly, concurrency, and lineage contract in `06A`.

If the accepted Stage 1/2 handoffs predate D-17–D-23, Stage 3 implementation must not begin until the targeted amendment in `02A` has versioned the required contracts and evaluated Stage 2 compatibility. This is a focused prerequisite amendment, not permission to discard valid earlier-stage work.

Run [05A_PRE_STAGE_3_ENTRY_GATE_CLOSURE.md](05A_PRE_STAGE_3_ENTRY_GATE_CLOSURE.md) as a separate Cursor task. Once its compact `stage2_evidence/PRE_STAGE_3_ENTRY_HANDOFF.md` is `ACCEPTED`, the Stage 3 implementation agent may use that handoff instead of loading the complete Stage 0–2 evidence history.

## 4. Deliverables

### 4.1 Compact common state and reducers

Implement accepted common channels such as:

- immutable identity and runtime binding ref;
- definition/assembly/state schema digests;
- lifecycle projection ref/version;
- pending durable decisions;
- monotonic outbox position;
- redacted diagnostics;
- final result ref.

Every channel declares writer, readers, parallelism, reducer/update rule, authority class, trace policy, retention, and compatibility behavior.

Required reducer behavior:

- immutable single assignment for identity/digests;
- monotonic cursor/version transitions;
- conflict-detecting keyed merge for parallel results/decisions/jobs;
- same key/same canonical digest is idempotent;
- same key/different digest fails closed and creates reconciliation incident;
- no last-writer-wins for accepted result/effect identity;
- operator replacement of reducer-backed data requires `Overwrite`, expected checkpoint, expected BellLabs version, actor, reason, and audit.

### 4.2 Runtime execution dispatch and binding

Implement the runtime-neutral dispatcher/outbox consumer:

1. read an authoritative requested binding/outbox item;
2. revalidate exact deployment/graph/assembly compatibility;
3. create or reconcile the thread for the BellLabs run epoch;
4. persist actual provider IDs before considering launch active;
5. submit the Agent Server run with stable submission metadata;
6. append a runtime execution attempt;
7. persist initial/latest run/checkpoint/trace observations;
8. settle the outbox action idempotently.

Ambiguous transport response must query Agent Server by persisted metadata/submission identity before retry. Never accept provider IDs from an untrusted caller. Keep each invocation/resume/steer/cancel as a separate attempt on the bound thread.

### 4.3 Canonical execution lineage

Implement the `ExecutionLineageEnvelope` and typed provider-qualified identities from `06A` across:

- BellLabs run and execution epoch;
- Workflow Implementation and graph assembly;
- workflow/stage cycles and semantic operation attempts;
- technical runtime attempts;
- operation binding/assembly digests;
- agent invocations, effect claims, child tasks/threads/runs;
- input/context/result/evidence manifests;
- usage/effect settlements and trace refs.

Persist parent/child edges explicitly. Never infer identity equality between a BellLabs run, Agent Server run, thread, task, operation attempt, or trace. Provide a repository/query service that later stages can use to reconstruct final-result provenance without reading model transcripts or checkpoints.

### 4.4 Hierarchical resource reservation and lease primitives

Implement the provider-neutral `ExecutionResourceEnvelope` and lease journal required by Stages 4–6:

- tenant/environment/workflow/stage/operation capacity;
- model, tool, MCP, synchronous-child, asynchronous-child, and linked-run slots;
- provider quota and budget reservation refs;
- deadline, lease TTL, renewal, release, expiry, and reconciliation;
- protected supervisor/resumption capacity;
- canonical acquisition order and deadlock prevention;
- retained-versus-released lease projection for durable waits.

This stage implements reservation mechanics and invariants, not StageGraph frontier selection. Duplicate acquisition with the same semantic identity is idempotent; a different envelope digest conflicts. Process loss must not leak capacity permanently. Actual usage is settled even when cancelled, failed, speculative, or discarded work produces no accepted result.

### 4.5 First-node authoritative reconciliation

Both graph families begin with a common bootstrap/reconcile node that:

- loads runtime binding by exact ref;
- verifies request scope, BellLabs run/epoch, graph/assembly/schema/deployment compatibility;
- loads current authoritative lifecycle/budget/decision projection;
- compares checkpoint projection version/digest;
- rebuilds compact derived projection when safe;
- fails/interrupts for reconciliation when inconsistent;
- never treats checkpoint or Agent Server status as permission to advance.

### 4.6 Durable interrupt and decision protocol

Implement the full protocol:

1. create durable decision request with type/schema/choices, evidence refs, expiry, expected lifecycle version, and policy;
2. call `interrupt()` with compact display data and decision ID only;
3. authenticate and authorize response through BellLabs API/facade;
4. validate schema, expiry, expected version, role, and idempotency;
5. persist response;
6. resume the same thread with decision ID and response digest;
7. allow the node to restart from the beginning, reread the durable decision, verify version/digest, and continue.

All code before `interrupt()` is idempotent. Prefer placing consequential side effects in separate nodes after the decision. Support parallel interrupts by runtime interrupt ID map without conflating runtime interrupt IDs with BellLabs decision IDs.

### 4.7 Typed interventions

Implement the accepted discriminated union and service/API foundation for:

- append input;
- satisfy wait;
- resume pause;
- respond to interrupt;
- update/cancel async task placeholders for Stage 6;
- cancel run;
- fork from checkpoint;
- privileged operator reconcile/repair.

Each intervention:

- first passes BellLabs lifecycle/authority/version policy;
- persists idempotently in PostgreSQL;
- then maps to Agent Server action;
- records a runtime attempt and outcome;
- reconciles ambiguous transport;
- never writes arbitrary unchecked graph state.

Default active-run behavior is `reject` unless accepted policy says otherwise. Enqueue non-preemptive input only when the workflow supports it. Agent Server interrupt strategy must follow a recorded BellLabs intervention. Do not use provider rollback for authoritative external effects.

### 4.8 Cooperative cancellation

Implement cancellation as a distinct lifecycle:

- accepted BellLabs cancel command is authority;
- dispatcher cancels/interrupts Agent Server attempts;
- graph nodes/tools observe deadlines/cancellation;
- child operation/MCP/sandbox/async/linked work follows accepted cascade or allow-to-finish policy;
- external usage/effects are reconciled and settled;
- cancellation never becomes success or generic failure;
- late completion cannot overwrite terminal cancellation state.

Stage 6 fills async-task-specific behavior; this stage provides the shared contracts and hooks.

### 4.9 Fork, replay, and epoch behavior

Implement policy distinctions:

- inspect: read-only checkpoint/state view;
- diagnostic replay: no-side-effect evaluation environment;
- retry: same run only under domain policy and stable semantic identity;
- fork: new BellLabs run, new thread, parent lineage, budget/admission, optional cloned snapshot;
- epoch rollover: new thread and compact verified handoff for same BellLabs run only under accepted policy;
- rollback: compensate or fork; never rewrite authoritative history.

Checkpoint/state continuation must verify assembly/schema/deployment compatibility. A fork cannot mutate the original lineage.

### 4.10 Resumable BellLabs event translation

Translate Agent Server runtime events into non-authoritative UI detail while preserving durable BellLabs events:

- accepted BellLabs events carry monotonic outbox cursor;
- reconnect accepts cursor and replays missed durable events;
- transient Agent Server stream position is not durability authority;
- values/debug/checkpoint detail is operator-restricted;
- event IDs/digests support deduplication;
- no secret/PHI/raw large output leakage;
- retry layer is explicit.

Maintain any Socket.IO compatibility bridge through coexistence without keeping two decision authorities.

### 4.11 Reconciliation loops and incident records

Implement idempotent, tenant-scoped, version-checked reconciliation for:

- requested binding without thread;
- thread without persisted initial run;
- active Agent Server run while BellLabs paused/cancelled/terminal;
- active BellLabs run with missing/failed/interrupted runtime run;
- accepted operation with unsettled usage/result/effect;
- stale interrupt/decision state;
- orphan async task/sandbox placeholders;
- terminal run without typed result;
- checkpoint on incompatible endpoint/deployment/assembly;
- stream/outbox cursor drift.
- expired or leaked resource lease;
- child/task/thread/run lineage gap or identity collision;
- accepted result whose operation assembly or context digest is missing.

Each action records actor/reason, before/after versions, evidence, and retry schedule. Unsafe cases stop and request operator decision rather than guessing.

### 4.12 Operation executor contracts and standalone persistence fixture

Publish the runtime-neutral async `OperationExecutor` port and discriminated `OperationExecutionOutcome` union defined in `06A`. Stage 3 supplies contract fixtures only; actual native, Deep Agent, MCP, sandbox, and async-child adapters are implemented and conformance-tested in later stages.

For production-like standalone tests/self-hosting only:

- construct `AsyncPostgresSaver`/Store in one async lifespan;
- run setup migrations once through the accepted release/test path;
- never create per invocation;
- use tenant/purpose namespaces;
- close cleanly on cancellation.

Exported managed graphs remain compiled without explicit checkpointer/Store.

## 5. Required tests

### Reducers/state

- associative, commutative, idempotent property tests;
- randomized merge order and duplicate replay;
- digest conflict fail-closed incident;
- update/Overwrite semantics and privileged audit;
- no large payload/transcript/secret in checkpoint fixtures.

### Dispatch/identity

- duplicate outbox delivery;
- same submission/same digest idempotency;
- same submission/different digest conflict;
- timeout after thread creation and after run creation;
- metadata reconciliation before retry;
- one thread per run epoch;
- provider IDs rejected from external request;
- incompatible assembly/deployment refusal;
- complete lineage parent/child creation for submit/resume/steer/cancel;
- semantic attempt identity remains stable while runtime attempt identity changes;
- task/thread/run/operation/trace identities cannot validate in the wrong typed field.

### Resources

- hierarchical authority/ceiling intersection;
- duplicate same-digest acquisition and conflicting-digest rejection;
- lease expiry/renewal/release and process-loss reconciliation;
- canonical acquisition order under randomized contenders;
- no starvation or deadlock at minimum accepted capacity;
- protected resumption capacity under child saturation;
- wait retains/releases only the declared resources;
- cancellation and failure release capacity while settling observed usage.

### Interrupt/intervention

- process restart across interrupt;
- duplicate resume;
- stale/expired/wrong-actor/wrong-scope/wrong-version response;
- parallel interrupts;
- non-idempotent pre-interrupt effect is prevented by test;
- cancel while model/tool/DB/stream boundary active;
- enqueue/reject policy;
- repair requires privilege and audit.

### Fork/recovery

- fork creates new run/thread/budget/lineage;
- original remains immutable;
- diagnostic replay cannot acquire effect claim;
- N-on-N resume after N+1 deployment;
- incompatible checkpoint fails safely;
- reconciliation crash/replay is idempotent.

### Security/streams

- cross-tenant thread/run/Store/interrupt/checkpoint denial;
- cursor reconnect/deduplication;
- operator-only debug/state detail;
- sentinel secret and synthetic PHI stream/error tests.

## 6. Gate

Stage 3 passes when:

- crash/restart loses no accepted transition;
- ambiguous submission and duplicate replay create no duplicate consequential effect;
- runtime binding/attempt/checkpoint correlations are authoritative runtime facts in PostgreSQL;
- durable interrupt/resume rereads BellLabs decisions;
- typed steering/cancel/fork paths pass authority/idempotency tests;
- reducer laws hold under randomized concurrency;
- cross-tenant resources and operator repair are protected;
- incompatible checkpoint routing follows accepted blue/green/fail-safe policy;
- reconciliation closes or safely escalates every injected inconsistency;
- lineage can be reconstructed from a typed result placeholder through every persisted binding/attempt/claim/settlement edge;
- resource leases survive duplicate delivery and process loss without over-admission, permanent leakage, or resumption deadlock;
- the runtime-neutral operation executor/outcome contracts and adapter conformance harness are published for Stages 4–6;
- full accepted verification suite passes;
- outgoing handoff is accepted.

## 7. Explicit non-goals

- Do not complete StageGraph or GoalDirected business execution.
- Do not expose arbitrary public checkpoint editing.
- Do not enable async subagents before Stage 6.
- Do not use Agent Server rollback to erase authoritative effects/history.
- Do not switch production default runtime.

## 8. Outgoing handoff additions

Include:

- common state/reducer manifest and compatibility version;
- runtime binding/attempt state machine;
- dispatcher ambiguity matrix;
- durable interrupt/intervention sequence and schemas;
- cancel/fork/epoch policies;
- stream event taxonomy/cursor semantics;
- reconciliation incident catalog and repair authority;
- managed versus standalone persistence proof;
- checkpoint/deployment compatibility and routing instructions;
- exact common nodes/services later graph stages must call;
- canonical lineage schema/query and identity-confusion test evidence;
- resource hierarchy, acquisition order, lease/release/wait matrix, and measured recovery evidence;
- operation executor/outcome protocol and shared adapter conformance harness.
