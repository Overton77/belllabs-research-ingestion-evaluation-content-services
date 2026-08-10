# Stage 3C — communication and intervention qualification

Status: `NOT_STARTED`
Document role: normative Stage 3 implementation and proof contract
Depends on: recorded `06-contract-frozen` gate from [`06`](06_STAGE_3_DURABILITY_HITL_STEERING_AND_RECOVERY.md)/[`06A`](06A_STAGES_3_TO_6_OPERATION_ASSEMBLY_CONCURRENCY_AND_LINEAGE_CONTRACT.md), plus the passed [`06B`](06B_STAGE_3_TEMPORAL_WORKFLOW_FOUNDATION.md) implementation gate
Feeds: Stage 3 handoff in [`06`](06_STAGE_3_DURABILITY_HITL_STEERING_AND_RECOVERY.md)

## 1. Mission

Implement an authoritative, auditable communication and intervention path for live operations.
PostgreSQL owns the BellLabs message/command ledger. Temporal provides durable runtime delivery and
observation. The agent runtime receives only authorized, ordered, bounded batches at certified safe
points.

This package has a **Stage 3 core gate** and a **cross-stage adapter conformance contract**. Stage 3
must implement and prove persistence, authorization, routing, ordering, dedupe, target/version
checks, runtime observation, durable waits, generic generation fencing, and explicit unsupported
dispositions. It must not claim model-visible or applied steering from fixture delivery alone.
Stage 4 first certifies the exact local post-model/pre-tool adapter, Stage 5 completes reusable local
HITL/disruptive restart, and Stage 6 certifies exact remote placements.

This contract does not claim atomic cancellation/injection, exactly-once transport, or arbitrary
mid-token/mid-tool mutation. It provides at-least-once delivery with durable deduplication,
monotonic ordering, explicit receipts, safe-point application, and reconciliation.

## 2. Authority and persistence boundary

The BellLabs API/control service:

1. authenticates and authorizes the actor and command type;
2. validates exact tenant/run/operation target, generation, schema, expiry, and expected versions;
3. persists the immutable ledger record and transactional outbox entry in one database transaction;
4. commits before any Temporal delivery attempt;
5. invokes a Temporal Update using only the persisted command ID and expected target;
6. reconciles timeout/ambiguity from ledger and workflow observations rather than recreating the
   command.

Signals are reserved for already-authorized facts/wake-ups where no synchronous acceptance result
is required. Public callers do not Signal workflows directly. Runtime claims and delivery occur only
through an authorized BellLabs service identity.

## 3. Ledger model

### 3.1 Immutable command/message

```text
message_id
request_scope
source_actor_ref
source_kind
message_kind
target_belllabs_run_id
target_execution_epoch
target_semantic_operation_attempt_id
target_execution_generation
target_agent_thread_id?
per_target_sequence
schema_ref/version
payload_manifest_ref
payload_digest
priority_class
created_at
expires_at?
supersedes_message_ids
authorization_decision_ref
expected_lifecycle_version
expected_operation_version
correlation_id
```

The semantic operation attempt is always the primary target. A current thread ID may narrow the
target but cannot replace semantic identity. Records are immutable; correction creates a new record
that explicitly supersedes earlier records.

Payloads live in governed PostgreSQL/object storage according to sensitivity and size. Temporal
receives compact refs and digests. Secrets, credentials, PHI, raw transcripts, and large content
never enter Temporal history.

### 3.2 Per-target ordering

`per_target_sequence` is monotonic for the tuple:

```text
(request_scope, run_id, epoch, semantic_operation_attempt_id)
```

Assignment and outbox creation are transactional. Delivery uses ordered, bounded batches with
explicit first/last sequence and batch digest. Gaps block later model-visible application until the
missing item is applied, rejected, expired, or superseded. Priority may control claim scheduling but
cannot reorder committed per-target application.

### 3.3 Inbox, outbox, claims, and leases

- transactional outbox records runtime-delivery intent;
- runtime inbox records deduplicated observation by message ID and digest;
- a claim lease grants one authorized router bounded delivery responsibility;
- lease expiry permits redelivery;
- same ID/same digest is idempotent;
- same ID/different digest is an integrity incident;
- retry never allocates a new message ID or sequence;
- claims, attempts, lease owner, expiry, and errors are auditable.

## 4. Receipt state machine

Every message has immutable receipt events and a derived projection:

```text
accepted
  -> routed
  -> runtime_observed
  -> model_visible
  -> applied
```

From any applicable nonterminal state:

```text
-> rejected | expired | superseded
```

Meanings:

- `accepted`: BellLabs committed authorization, immutable message, and outbox.
- `routed`: an authorized service issued/reattempted the Temporal Update or fact Signal.
- `runtime_observed`: the exact target workflow durably recorded the ID/digest.
- `model_visible`: the certified runtime included it in a checkpointed model-visible batch.
- `applied`: the checkpoint containing the injected batch committed and any superseded proposed
  tool calls were resolved according to policy.
- `rejected`: authorization, target, generation, schema, version, safe-point, or policy failed.
- `expired`: expiry passed before application.
- `superseded`: a later immutable command replaced it before application.

Receipt transitions are monotonic and idempotent. `routed` is not `runtime_observed`;
`runtime_observed` is not `model_visible`; `model_visible` is not `applied`.

Stage 3 qualifies through `runtime_observed` plus terminal dispositions. `model_visible` and
`applied` remain declared-but-unsupported for a fixture adapter and cannot appear without an exact
agent checkpoint/batch evidence reference. Stage 4 qualifies those states for its first local
adapter; Stage 5 generalizes them across the reusable local harness; Stage 6 repeats qualification
for each selected remote placement.

## 5. Targeting and stale-target behavior

Before runtime observation and again before model visibility, validate:

- tenant/scope and BellLabs run/epoch;
- semantic operation attempt;
- current execution generation;
- optional exact agent thread;
- lifecycle/operation expected versions;
- command type authorization, expiry, and supersession;
- exact assembly/adapter support.

A stale generation/thread/attempt is rejected. The router **must not retarget** a message to a newer
generation, thread, retry, sibling, or fork. The actor must issue a new authorized message with a new
ID and sequence when policy permits.

## 6. Message types and privilege

Only typed peer messages are supported for ordinary operation-to-operation or human-to-operation
communication, for example:

- additional evidence/artifact reference;
- clarification or constraint;
- answer to a declared question;
- progress/failure fact;
- approved decision response;
- request to pause, resume, cancel, or reconcile.

Ordinary messages enter as bounded peer/user context and cannot alter system/developer prompts,
tool grants, policy, assembly, or authority. Prompt-role elevation, policy changes, tool-surface
changes, and protected instruction changes are privileged command types requiring stronger role,
explicit policy, versioned recompilation/rebinding where applicable, and audit. A message body
cannot smuggle a privileged role.

## 7. Durable waits and StageGraph semantics

A durable agent wait exists only as an open `OperationWorkflow` wait state. A LangGraph interrupt,
Deep Agent checkpoint, provider session, or process-local task may be subordinate state, but cannot
be the sole durable owner of the wait.

Messages and receipts do not satisfy StageGraph dependencies, joins, obligations, or terminality.
Only settlement of a typed accepted `OperationExecutionOutcome` through BellLabs application
services may update the StageGraph semantic projection.

## 8. Cross-stage certified intervention safe point

### 8.1 Required exact local safe point — Stage 4 first proof, Stage 5 reusable proof

The first real local adapter in Stage 4 and the completed reusable harness in Stage 5 must certify
**post-model/pre-tool**:

1. receive and checkpoint the model response, including proposed tool calls;
2. pause before executing any proposed tool;
3. drain the next ordered bounded authorized message batch;
4. revalidate target, generation, versions, expiry, supersession, and policy;
5. checkpoint the injected model-visible batch and receipt observations;
6. determine whether proposed tool calls remain valid;
7. explicitly retain, reject, or supersede each proposed call;
8. resume cognition or tool execution;
9. mark messages `applied` only after the checkpoint commits.

If a new message invalidates a proposed consequential call, that call must not execute under its old
assumptions. A replacement call requires fresh model/policy output and normal effect claims.

Checkpoint commit and BellLabs receipt update are not one distributed atomic transaction. Recovery
reconciles by message ID, batch digest, checkpoint ref, generation, and tool-call/effect identity.
Duplicate injection produces the same checkpointed batch effect or fails closed.

Stage 3 publishes deterministic conformance vectors for these boundaries and proves that a fixture
adapter rejects this capability as unsupported. Passing those vectors without an actual model
response, proposed tool call, and adapter checkpoint does not qualify the safe point.

### 8.2 Remote adapter posture — Stage 6 proof

This package defines the same contract for the remote exact adapter but does not certify it. Remote
Agent Server post-model/pre-tool proof is deferred to Stage 6 because it depends on the deployed
graph/runtime interception surface. Until that proof passes, remote exact assemblies must declare
intervention capability unsupported, use a less disruptive authored policy, or reject such
commands; they may not claim local-equivalent steering.

## 9. Non-disruptive and disruptive intervention

Stage 3 implements the generic command, cancellation, generation, fencing, and quarantine
primitives with deterministic operations. Stage 5 must run the full saga below against the real
local agent/checkpoint/effect path before local disruptive steering becomes qualified. Stage 6 does
the same for each remote placement that advertises it.

### Non-disruptive

Use when the current adapter can reach the certified safe point within policy. Persist, route,
observe, drain, checkpoint, revalidate proposed calls, and continue the same generation/thread.

### Disruptive saga

When safe-point injection is unavailable or policy requires restart:

1. persist the command and disruptive-intervention intent;
2. request cooperative cancellation of current Activity/provider/model/tool work;
3. observe cancellation, timeout, or unresolved external work;
4. reconcile usage, effects, artifacts, checkpoint, and provider status;
5. quarantine every late output from the old generation;
6. increment `execution_generation` while preserving the same semantic operation attempt;
7. create an exact new agent thread/session or validated checkpoint-derived thread;
8. start a new `OperationWorkflow` generation;
9. route the still-valid command to that exact new target only through an explicit new-target
   command record or authored saga linkage; never silently retarget the original;
10. settle only generation-valid accepted output.

There is no atomic cancel-and-inject. Cancellation may race with completion, provider effects, and
late output; every race is reconciled.

### Orphan overlap

Starting a new generation while old work may still execute is default-denied. It requires:

- explicit policy and actor authority;
- independent resource/budget admission;
- effect-class compatibility and bounded liability;
- provider/job observability;
- generation fencing on every result/effect settlement;
- mandatory late-output quarantine;
- incident and operator visibility.

Consequential operations without adequate fencing cannot overlap.

## 10. Reconciliation

Continuously reconcile:

- accepted without outbox/routing;
- routed without runtime observation;
- runtime observed without model visibility;
- model visible without applied receipt;
- checkpointed batch without projected receipt;
- sequence gap or conflicting digest;
- expired/superseded message still pending;
- stale target or generation;
- cancellation requested while old execution remains active;
- new generation active with unresolved old effects;
- late output/artifact from old or orphan generation;
- applied message followed by a prohibited superseded tool effect.

Safe repair is tenant-scoped, version-checked, idempotent, and audited. Unsafe ambiguity creates an
incident and stops semantic acceptance.

## 11. Acceptance tests

Unless a subsection explicitly names a later stage, Stage 3 runs it. Adapter-dependent tests are
published now as reusable conformance cases but become blocking only at the owning stage.

### Ledger and delivery

- transaction rollback creates neither ledger record nor outbox;
- commit-before-Update survives API death at every boundary;
- duplicate Update/Signal/outbox delivery is idempotent;
- same ID/different digest fails closed;
- claim lease expiry causes ordered redelivery without new identity;
- concurrent producers receive monotonic per-target sequence;
- bounded batches preserve order and gap handling.

### Receipts and targeting

- all accepted/routed/runtime-observed/model-visible/applied states are distinct and queryable;
- rejected, expired, and superseded transitions are immutable and monotonic;
- wrong tenant/run/epoch/attempt/generation/thread/version/actor is rejected;
- stale targets are never retargeted;
- a fork cannot receive parent-run messages.

### Local certified safe point

Blocking in Stage 4 for the first exact local adapter and in Stage 5 for the reusable harness, not
in the Stage 3 core gate:

- inject after model response but before pure, idempotent, and consequential proposed tools;
- checkpoint response and injected batch before tool execution;
- superseded calls never execute;
- crash before/after every checkpoint and receipt boundary reconciles without duplicate model-visible
  application or duplicate effect acceptance;
- batches remain bounded and ordered under sustained concurrent senders.

### Disruptive recovery

Stage 3 blocks on deterministic generation/fencing/quarantine mechanics. Stage 5 blocks on the
same cases with real local agent calls, checkpoints, tool/effect boundaries, and context restart:

- cancellation races with model completion, tool start, provider completion, and checkpoint commit;
- same semantic attempt restarts with new generation/thread;
- old-generation output is quarantined and cannot settle;
- usage and effect liabilities from both generations are recorded;
- orphan overlap is denied by default and permitted only with all policy/fencing evidence.

### Authority and semantics

- only the authorized service can claim and route;
- typed peer messages cannot gain system/developer role or tool authority;
- privileged prompt-role command requires privilege and exact revalidation;
- durable wait survives all worker restarts because `OperationWorkflow` remains open;
- messages alone never satisfy a StageGraph dependency;
- no test asserts exactly-once transport or atomic cancellation/injection.

### Security and payload

- sentinel secrets, PHI, credentials, large bodies, and raw transcripts remain outside Temporal
  history, memo, search attributes, heartbeat, log, and error;
- payload retrieval enforces exact tenant/target/actor purpose;
- redacted diagnostics preserve IDs/digests without content leakage.

## 12. Stage 3 core handoff and later qualification gates

The Stage 3 core of `06C` passes when the PostgreSQL ledger, inbox/outbox, claims, ordered batches,
receipts through `runtime_observed`, stale-target rejection, durable wait/resume, deterministic
generation fencing/quarantine, settlement-before-readiness, explicit unsupported adapter steering,
and reconciliation tests pass on the `06B` Q/D local Temporal environment.

Stage 4 promotes `model_visible`, `applied`, and post-model/pre-tool capability only for its exact Q
local adapter after the corresponding tests pass. Stage 5 qualifies the reusable local harness and
full disruptive agent saga through D while rerunning Q. Stage 6 qualifies remote variants
individually. The handoff records all unqualified states as unsupported and may not imply that
transport proof equals cognition safe-point proof.
