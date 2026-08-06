# Stage 0 decision package

Recorded: 2026-08-04  
Owner disposition: the owner accepted the recommended D-01–D-16 bundle, selected
CLI-managed Standard/Serverless staging, kept `biotech-meta` read-only, prohibited
deployment/purchase work in this stage, required async subagents in the migration,
and kept QuickJS/PTC/dynamic subagents disabled while retaining their contracts.

## D-01 through D-16

| ID | Disposition | Accepted contract | Stage 1 input |
|---|---|---|---|
| D-01 | accepted | Standard Agent Server is primary. Managed Deep Agents is non-primary because custom routes, auth, coexistence, and BellLabs authority are required. | Runtime kind is `langgraph_agent_server`; no MDA identity enters domain contracts. |
| D-02 | accepted | Port the generic frontier-scheduler StageGraph first; retain the pure interpreter. | Define stable scheduler node/channel/reducer names around the existing interpreter. |
| D-03 | accepted | Generated graphs are a measured post-parity optimization. | No generated graph contract or checkpoint dependency in Stage 1. |
| D-04 | accepted | GoalDirected uses a deterministic outer graph, bounded agent operation, and independent BellLabs verifier. | Keep protected goal/verifier/terminality references in authoritative contracts. |
| D-05 | accepted with identity amendment | One parent thread per `(request_scope, belllabs_run_id, execution_epoch)`; a fork creates a new BellLabs run/thread, while linked runs and async subagents use explicitly bound child threads rather than sharing the parent thread. | Add typed parent/child builders, binding tables, and uniqueness rules; never expose a free-form `thread_id`. |
| D-06 | accepted | Shared router factories support standalone FastAPI and Agent Server coexistence. | Principal mapping, application facade, and error mapping are shared; HTTP mounting is adapter-specific. |
| D-07 | accepted | Cloud uses managed Agent Server persistence; explicit async saver/Store are for standalone tests/self-hosting only. | Do not compile a saver/Store into Cloud graph exports. |
| D-08 | accepted with schema amendment | PostgreSQL owns `RuntimeExecutionBinding`, one-to-many runtime attempts/async tasks, endpoint/epoch binding, and graph-assembly/state-schema digests. | Forward-only schema, RLS, grants, outbox, compatibility digests, and separate attempt/task identities are mandatory. |
| D-09 | accepted | Normal callers use typed interventions only; `update_state`/`Overwrite` require privileged audited repair. | Contract separate intervention intents from repair commands and evidence. |
| D-10 | accepted | Top-level lifecycle state is compact; transcripts/corpora stay behind refs or in bounded agent subgraphs. | Define state-size and prohibited-field invariants. |
| D-11 | accepted with API amendment | Use async, introspection-safe graph factories only when required. In `langgraph-sdk==0.4.2`, `ServerRuntime` is a type alias; `execution_runtime` returns the execution variant only for `threads.create_run` and `None` otherwise, and only that variant has `context`. | Bind behavior to the exact `access_context`/`execution_runtime` contract and preserve all four current contexts. |
| D-12 | accepted | I/O is native async; pure domain compilation/reduction remains synchronous. | Ports expose async I/O and bounded concurrency/deadline/cancellation contracts. |
| D-13 | accepted with source-precedence amendment | PostgreSQL becomes authoritative for effect claims, runtime attempts, usage, settlements, and lifecycle/outbox coordination. The immutable semantic `OperationExecutionBinding` remains MongoDB/Beanie-authoritative under accepted `biotech-meta`; PostgreSQL stores its stable identity/digest reference, not a competing semantic authority. | Design one transactional claim/attempt/settlement journal plus digest-verified backfill of the Mongo claim/settlement records; do not dual-write the semantic binding as authority. |
| D-14 | accepted | Context policy and immutable context assembly are first-class. Model summaries are derived manifests only. | Contract protected atoms, source digests, tombstones, contradictions, approvals, and assembly digest. |
| D-15 | accepted | Sync, dynamic-interpreter, async, and linked-run delegation are distinct. QuickJS/dynamic are disabled; async is required but remains default-off until its Stage 6 promotion gate passes. | Separate identity/result/capacity/fallback contracts for each mode. |
| D-16 | accepted | Use canonical BellLabs vocabulary and provider-qualified identity grammar. | Publish typed identifiers and reject ambiguous `run_id`, `agent_id`, and `checkpoint_id` fields. |

D-13's amendment is mandatory under source precedence. Accepted `biotech-meta`
currently assigns immutable Operation Execution Bindings to MongoDB/Beanie, and this
stage was not authorized to edit that repository. A future owner-authorized ADR may
move that semantic authority, but Stage 1 must not assume such an amendment.

## Canonical identity grammar

The exact encoding may be amended in Stage 1 without changing these semantic parts:

```text
BellLabsRunKey       = (request_scope, belllabs_run_id)
ExecutionEpochKey    = (request_scope, belllabs_run_id, execution_epoch)
AgentThreadKey       = ("langgraph", request_scope, belllabs_run_id, execution_epoch)
AgentRunKey          = ("langgraph", deployment_endpoint_id, agent_server_run_id)
CheckpointKey        = ("langgraph", deployment_endpoint_id, thread_id, checkpoint_id)
OperationAttemptKey  = (request_scope, belllabs_run_id, operation_id, semantic_attempt)
EffectClaimKey       = (request_scope, operation_contract_digest, idempotency_key)
SettlementKey        = (effect_claim_key, settlement_revision)
AsyncTaskKey         = ("deepagents", deployment_endpoint_id, async_task_id)
```

Provider IDs are runtime facts. They never replace BellLabs IDs.

## Deployment and compatibility

- Ownership path: CLI-managed.
- Initial topology: Standard Agent Server on Serverless staging.
- Dedicated: evidence-driven only.
- In-flight execution: remains bound to its original endpoint/revision.
- Checkpoint-incompatible changes: blue/green endpoint binding, not revision metadata.
- Rollback: route new admissions to the prior accepted endpoint; do not delete authority
  or evidence.

## Data and memory posture

The owner permits the same non-production research/test run classes already used by
this repository through the new interfaces. Until a formal classification policy is
accepted, the safe interpretation is:

- no PHI or credentials in graph state, Store, prompts, skills, traces, streams, or
  sandboxes;
- synthetic sentinel data is allowed for redaction tests;
- raw private corpora remain behind authorized artifact/context references;
- cross-thread Store is allowed for tenant-scoped procedural memory;
- scientific claims, approvals, budgets, and terminality are denied to Store;
- checkpoint encryption, retention, deletion, and trace retention remain blocking
  policy decisions before staging.

## Feature posture

The durable machine-readable posture is
[`spikes/stage0/capability_manifest.json`](../../../spikes/stage0/capability_manifest.json).
All new runtime flags default off while the legacy runtime remains available.
