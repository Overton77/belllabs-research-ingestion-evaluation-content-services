# Stage 3 — Temporal durability, intervention, and recovery foundation

Status: `NOT_STARTED`
Document role: normative Stage 3 work-package index and aggregate acceptance contract
Mission type: macro-runtime foundation shared by StageGraph and GoalDirected
Depends on: accepted Stages 1 and 2 and accepted Pre-Stage-3 entry handoff
Companion contracts: [`06A`](06A_STAGES_3_TO_6_OPERATION_ASSEMBLY_CONCURRENCY_AND_LINEAGE_CONTRACT.md), [`06B`](06B_STAGE_3_TEMPORAL_WORKFLOW_FOUNDATION.md), and [`06C`](06C_STAGE_3_COMMUNICATION_AND_INTERVENTION_QUALIFICATION.md)

## 1. Accepted decision and preserved history

Stage 3 adopts Temporal as the **sole macro-workflow runtime** for admitted BellLabs runs. This
supersedes the earlier Stage 3 direction in which an Agent Server graph owned macro dispatch,
interrupt, checkpoint, and recovery mechanics. That earlier direction remains useful decision
history: its compact-state, typed-decision, authority, idempotency, lineage, resource, effect,
settlement, and reconciliation requirements are preserved, but they are now implemented around
Temporal workflows rather than a competing Agent Server scheduler.

The accepted ownership boundary is:

- BellLabs PostgreSQL, application services, and pure interpreters are semantic authority;
- Temporal owns durable macro execution, timers, child lifecycles, retries, and message delivery;
- a distinct `BellLabsRunWorkflow` is the root lifecycle shell;
- `StageGraphWorkflow` or `GoalDirectedWorkflow` is the family child;
- generic `OperationWorkflow` children durably own semantic operation attempts;
- `OperationExecutor` remains inside `OperationWorkflow`;
- LangGraph and Deep Agents provide bounded operation cognition, never macro scheduling authority;
- LangSmith remains tracing, evaluation, development, sandbox, and optional operation-runtime
  infrastructure.

Local in-process/library and remote Agent Server execution are separate, exact adapter variants.
Neither may be selected dynamically or treated as an invisible fallback for the other.

## 2. Identity and continuity decisions

- A BellLabs product fork creates a **new BellLabs run**, starts at execution epoch `1`, and records
  immutable parent-run/snapshot lineage. It is not Temporal Reset.
- `Continue-As-New` preserves the same BellLabs run and execution epoch and starts a **new technical
  execution segment** with the same Temporal Workflow ID and a new Temporal Run ID.
- A semantic operation attempt remains stable across Temporal Activity retries and across a
  disruptive cancel/reconcile/restart authorized by policy.
- Each restarted operation execution gets a new `execution_generation`; each Activity attempt gets
  its own technical `runtime_attempt_id`.
- Temporal Workflow ID, Temporal Run ID, execution epoch, technical segment, execution generation,
  agent thread/checkpoint, and BellLabs semantic IDs are distinct typed fields.

## 3. Mission

Implement and qualify the contracts required before Stage 4 may build production StageGraph
scheduling:

1. preserve exact operation assemblies, hierarchical resources, concurrency, lineage, journal,
   effect claims, usage, and deterministic settlement in `06A`;
2. establish the Temporal root/family/operation hierarchy and recovery foundation in `06B`;
3. establish PostgreSQL-authoritative command transport and durable waits in `06C`, while defining
   later adapter-level intervention contracts without claiming them prematurely;
4. execute durable skeleton increments of both Q and D through the real BellLabs application,
   persistence, Temporal, Activity, and worker seams;
5. prove process loss, replay, duplicate delivery, fixture intervention, cancellation, and reconciliation
   cannot bypass BellLabs authority or duplicate a consequential effect.

Stage 3 is therefore a vertical durable-execution kernel, not a general proof that every agent
runtime is steerable. Deterministic Q/D fixture operations prove replay and recovery. Optional
bounded live retrieval or tiny LLM operations prove configured integration only through an exact
Stage 2 binding. Model-visible safe points qualify with the real local adapter in Stage 4; reusable
and disruptive local steering completes in Stage 5 and remote equivalence in Stage 6.

Self-hosted Temporal on AWS is the accepted initial production direction. Stage 3 uses a
self-hosted local Temporal service for deterministic qualification without selecting or encoding
the eventual ECS, EKS, EC2, or combined AWS topology. Stage 8 selects and proves that exact
topology, worker hosting, autoscaling, and regional design from production-shaped evidence; the
Cloud-versus-self-host decision is not reopened here.

## 4. Required inputs and entry gate

Required inputs:

- accepted Stage 1 contracts, repositories, operation journal, effect claims, and outbox;
- accepted Stage 2 authentication, authorization, API, pure interpreters, graphs, and exact
  compilation contracts;
- accepted Stage 0 compatibility, replay, interrupt, and fork evidence;
- accepted [`05A_PRE_STAGE_3_ENTRY_GATE_CLOSURE.md`](05A_PRE_STAGE_3_ENTRY_GATE_CLOSURE.md) handoff;
- owner-approved intervention, cancellation, orphan-overlap, retention, and repair policies.

If earlier handoffs predate the operation-assembly, lineage, resource, or Temporal acceptance
decisions, amend those contracts explicitly. Do not silently reinterpret persisted fields.

## 5. Work-package sequence and `06-contract-frozen` gate

### 5.1 `06A` — shared semantic execution contract

First freeze:

- operation requirement, assembly, binding, executor, and typed outcome contracts;
- resource hierarchy, reservation, lease, release, and resumption behavior;
- semantic and technical identity grammar, including Temporal runtime identities;
- parent lifecycle protocol for start, observe, command, cancel, and reconcile;
- operation journal, effect-claim, immutable artifact, usage, and settlement invariants;
- compatibility, failure, wait, concurrency, and lineage rules.

### 5.2 `06B` — Temporal workflow foundation

Then implement:

- root, family, and operation workflow contracts;
- stable Workflow ID grammar and worker registrations;
- Query, Signal, and Update surfaces;
- task-queue, timeout, heartbeat, and retry profiles;
- `Continue-As-New`, replay, N/N+1, active-child reattachment, and reconciliation;
- BellLabs rehydration and deterministic projection rebuilding;
- local self-hosted Temporal qualification.

### 5.3 `06C` — communication and intervention qualification

Then implement and prove:

- PostgreSQL message/command ledger with inbox and transactional outbox;
- exact target attempt, monotonic sequence, immutable ordered batches, receipts, claims, leases, and
  idempotent redelivery;
- core accepted/routed/runtime-observed receipts and durable decision wait/resume;
- the cross-stage post-model/pre-tool intervention contract, first certified by Stage 4's exact
  local adapter rather than a Stage 3 fixture;
- typed peer messages and privileged prompt-role changes;
- durable agent waits only through `OperationWorkflow`;
- generic cancellation/restart generation fencing and quarantine fixtures; full local agent
  disruptive-restart qualification is a Stage 5 gate.

After the contract-defining sections of `06` and `06A` are reviewed, versioned, mutually
consistent, and backed by the shared contract-conformance record, the gate authority records
`06-contract-frozen`. This is an internal implementation-entry gate, not acceptance of `06` or
Stage 3.

`06B` may begin only after `06-contract-frozen`. `06C` may begin only after
`06-contract-frozen` and the `06B` implementation gate pass. Overlapping implementation is allowed
only after these dependencies are satisfied. Aggregate Stage 3 acceptance is recorded through this
`06` package only after both `06B` and `06C` pass.

## 6. Cross-cutting invariants

1. Temporal history is durable execution history, not BellLabs domain truth.
2. Temporal, LangGraph checkpoints, models, tools, traces, and provider status cannot authorize,
   settle, or terminalize BellLabs state.
3. Workflow code calls deterministic logic only. Database, network, object-store, provider, model,
   and clock/random effects occur through Activities with explicit contracts.
4. Temporal payloads contain compact IDs, refs, digests, versions, and bounded summaries only.
   Large documents, raw transcripts, secrets, credentials, and PHI stay in governed stores.
5. Every consequential external action requires a stable BellLabs effect claim and reconciliation.
   Temporal provides at-least-once Activity execution, not exactly-once effects.
6. Waiting child workflows remain open durably. A process-local task, LangGraph background task, or
   worker-held request is not a durable wait.
7. No public API writes arbitrary workflow/checkpoint state. Repair is typed, authorized,
   version-checked, reasoned, and audited.
8. Cancellation and message injection are cooperative; there is no atomic
   cancellation-and-injection primitive and no exactly-once transport claim.
9. Reconciliation is expected control flow, not an exceptional escape hatch.

## 7. Stage 3 implementation slices

### 7.1 Contract and persistence slice

- map every `06A`/`06B`/`06C` field to one model, repository, application service, workflow,
  activity, API schema, and test;
- add schema-compatible fields or explicit versioned migrations;
- reject duplicate models that create competing authority;
- publish compatibility and redaction manifests.

### 7.2 Temporal kernel slice

- register root/family/operation workflows and typed test Activities;
- start an admitted run through the root and selected family child;
- start, observe, command, cancel, and reconcile operation children;
- rehydrate BellLabs projection at start, repair, and continuation;
- preserve open waits and active child bindings across worker loss and `Continue-As-New`.

### 7.3 Communication and intervention slice

- persist commands/messages before runtime delivery;
- claim and route bounded batches through an authorized service;
- prove receipt progression and stale-target rejection;
- prove accepted/routed/runtime-observed receipts, durable wait/resume, and explicit unsupported
  dispositions for unqualified model-visible steering;
- implement generic generation fencing/quarantine primitives and publish the adapter conformance
  suite that Stages 4–6 run against exact implementations.

### 7.4 Reference blueprint execution slice

This aggregate slice is implemented concretely by package `06B` §11, using
[`REFERENCE_BLUEPRINT_STAGE3_MAPPING.md`](../stage2_evidence/REFERENCE_BLUEPRINT_STAGE3_MAPPING.md)
as its supporting Q/D input. The mapping file is evidence, not an additional numbered work package.
Package `06C` subsequently owns persisted command, receipt, and durable-wait qualification.

- compile immutable Stage 3 Q/D skeleton implementations from the Stage 1 blueprint contracts;
- run both via BellLabs application/API entry points into `BellLabsRunWorkflow`, fixture family
  workflows, and generic `OperationWorkflow` children;
- use deterministic tiny operations for the mandatory crash/replay gate and optional bounded live
  retrieval/LLM only through an exact declared binding;
- exercise Query, persisted Update command, Signal wake-up, durable decision wait, cancellation,
  worker loss, and result settlement across the two runs;
- publish a comparison against Stage 2 and prove no demo-only path or contract fork exists.

### 7.5 Recovery and compatibility slice

- capture histories for replay;
- qualify current code (`N`) against `N` histories and compatible next code (`N+1`) against `N`
  histories;
- test incompatible behavior as safe refusal or explicit routing, never silent drift;
- continuously reconcile BellLabs, Temporal, operation, agent, artifact, effect, and settlement
  bindings.

## 8. Concrete acceptance tests

### Authority and identity

- duplicate start with the same identity/digest is idempotent; a different digest conflicts;
- root, family, operation, Activity, agent, and BellLabs IDs cannot validate in the wrong field;
- `Continue-As-New` retains run+epoch and increments only technical segment;
- fork creates a new run at epoch `1`, preserves the parent, and leaves the source immutable;
- local and remote adapters produce distinct assembly and compatibility digests.

### Durability and recovery

- kill API, workflow worker, and Activity worker independently before and after each consequential
  boundary; accepted progress recovers without duplicate settlement;
- an open decision/external/agent wait survives all process restarts;
- active children reattach after parent `Continue-As-New`;
- ambiguous child start and Activity completion reconcile before retry;
- no accepted transition is inferred solely from Temporal or provider status.

### Scheduling prerequisites

- controlled child workflows overlap under hierarchical ceilings;
- one completion is observed without waiting for a slow sibling;
- protected coordinator/resumption capacity remains available under operation saturation;
- lease loss, cancellation, failure, and waiting release or retain exactly declared resources.

### Communication and intervention

- duplicate, delayed, and out-of-order delivery preserves per-target sequence and idempotency;
- stale target, wrong tenant, wrong actor, expired, and superseded commands fail closed;
- every receipt transition is queryable and immutable;
- an unqualified post-model/pre-tool command returns an explicit unsupported disposition;
- generic restart increments generation and late fixture output from the old generation is
  quarantined and cannot settle;
- neither tests nor documentation claim atomic cancellation/injection or exactly-once transport.

### Security and payloads

- sentinel secrets, synthetic PHI, large documents, and raw transcripts are absent from Event
  History, memo, search attributes, heartbeats, logs, and errors;
- cross-tenant Workflow ID, child, command, checkpoint, effect, and artifact access is denied;
- Queries return compact diagnostics only; product reads come from BellLabs projections.

## 9. Stage 3 handoff gate

Stage 3 passes only when:

- `06-contract-frozen` was recorded from the reviewed `06`/`06A` contract sections;
- the `06B` and `06C` implementation and proof gates both pass in sequence;
- Temporal is the only qualified macro execution path;
- the root/family/operation hierarchy and exact adapter variants are published;
- BellLabs can reconstruct lifecycle and final-result lineage without reading Temporal history or
  model transcripts;
- replay and N/N+1 evidence is accepted;
- process-loss, duplicate-delivery, cancellation, wait, intervention, effect, and reconciliation
  suites pass against local self-hosted Temporal;
- deterministic Q and D skeleton runs traverse the real API/application, persistence, workflow,
  Activity, and worker seams, with bounded live evidence or explicit skip reasons;
- no package claims `model_visible`, `applied`, post-model/pre-tool, tool-interrupt, or disruptive
  agent-restart capability before the exact later-stage adapter proof;
- history and payload inspections pass redaction and size limits;
- Stage 4 receives stable child-lifecycle, executor, resource, lineage, settlement, and
  communication contracts.

## 10. Explicit non-goals

- Do not implement StageGraph frontier business scheduling or GoalDirected convergence here.
- Do not complete production Deep Agent, MCP, sandbox, ingestion, or asynchronous-subagent
  capability sets.
- Do not run Agent Server as a second macro scheduler.
- Do not select production Temporal hosting or final worker infrastructure.
- Do not use Temporal Reset as a BellLabs fork or use checkpoint editing as rollback.
- Do not certify the remote post-model/pre-tool intervention safe point; that proof is deferred to
  Stage 6.
- Do not certify local model-visible steering merely from fixture message delivery; the first exact
  local adapter proof belongs to Stage 4 and the reusable/disruptive proof to Stage 5.

## 11. Outgoing handoff

Publish:

- accepted decision/supersession record;
- requirements-to-code/test ownership matrix;
- root/family/operation and message contracts;
- Workflow ID, epoch, segment, generation, and lineage grammar;
- task-queue and timeout/retry/heartbeat profiles;
- resource lease and wait matrix;
- journal/effect/settlement and reconciliation catalog;
- Stage 3 transport receipts, unsupported adapter-safe-point dispositions, and the conformance
  vectors handed to Stages 4–6;
- replay histories, N/N+1 report, payload inspection, and process-loss results;
- exact Stage 4 entry APIs and forbidden shortcuts.
