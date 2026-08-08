# Stage 6 — capability providers, remote LangSmith certification, and advanced qualification

Status: `NOT_STARTED`
Document role: normative Stage 6 implementation package and aggregate Stage 6 gate
Mission type: provider integration, exact local/remote variants, hours-long failure qualification, and heterogeneous production proof
Depends on: accepted Stage 5, accepted provider spikes, [06A_STAGES_3_TO_6_OPERATION_ASSEMBLY_CONCURRENCY_AND_LINEAGE_CONTRACT.md](06A_STAGES_3_TO_6_OPERATION_ASSEMBLY_CONCURRENCY_AND_LINEAGE_CONTRACT.md), [06B_STAGE_3_TEMPORAL_WORKFLOW_FOUNDATION.md](06B_STAGE_3_TEMPORAL_WORKFLOW_FOUNDATION.md), and [06C_STAGE_3_COMMUNICATION_AND_INTERVENTION_QUALIFICATION.md](06C_STAGE_3_COMMUNICATION_AND_INTERVENTION_QUALIFICATION.md)

## 1. Mission and fixed architecture

Complete the provider-capability layer behind the Stage 3/5 operation contracts and certify
selected bounded agent graphs both locally and on LangSmith-hosted infrastructure. BellLabs
PostgreSQL/application services own admission, lifecycle authority, settlement, and terminality.
Temporal root/family workflows coordinate, route, and reconcile only:

- `BellLabsRunWorkflow` is the stable internal execution and communication-routing handle; it does
  not independently admit, authorize, settle, or terminalize.
- StageGraph/GoalDirected family workflows invoke the pure interpreters.
- Every significant operation is an `OperationWorkflow` child.
- LangGraph/Deep Agents graphs are bounded operation runtimes, local or remote.
- LangSmith Agent Server is never the StageGraph or GoalDirected macro-scheduler.

LangSmith is required for tracing, evaluation, sandbox integration, and selected bounded graph deployments. Local and remote deployments are exact implementation variants: each has a frozen deployment ref, transport, graph revision, capability surface, compatibility key, evaluation evidence, and assembly digest. "Same source" does not make two deployments interchangeable.

All qualified deployment types may be used. Only graph deployments that pass the remote command-injection certification may promise post-model/pre-tool injection. Others must expose only their certified intervention boundaries.

## 2. Implementation order

1. **6A — provider-neutral capability completion:** MCP, tools/effects, skills, context, readiness, and sandbox gateway.
2. **6B — LangSmith observability/evaluation/sandbox integration:** exact tracing, datasets, evaluators, experiments, and selected bounded deployments.
3. **6C — remote operation lifecycle:** start, bind, release, wait, reconcile, inject/cancel, and settle.
4. **6D — provider async-subagent adapter:** subordinate provider feature only; independent lifecycle is promoted to BellLabs Temporal delegation.
5. **6E — required internal exit proof:** after the required candidate adapters are stable within
   Stage 6, execute `09A`, including hours-long and injected-failure campaigns.
6. **6F — optional QuickJS/PTC/dynamic track:** only after independent qualification.

Optional 6F cannot delay the stable provider and heterogeneous gates.

## 3. Exact provider variants and certification

Extend the compiler without changing domain semantics. For every operation variant persist:

- operation implementation and graph source/revision;
- `local` or exact remote deployment kind;
- endpoint/deployment/assistant/graph refs as typed fields;
- transport and authentication-by-secret-ref policy;
- model, prompt, middleware, tool/MCP, skill, context, filesystem, sandbox, verifier, and output manifests;
- command/cancel/status/result capabilities;
- supported intervention boundaries;
- timeouts, polling cadence, backoff, quotas, resource envelope, and fallback;
- trace/evaluation project/dataset/evaluator refs;
- provider compatibility and graph assembly digests;
- maturity, last qualification, and expiration.

Local and remote variants must pass the same semantic operation contract and typed result suite. Differences in deployment, checkpoint implementation, transport, timing, or provider IDs are explicit compatibility dimensions. Runtime selection cannot switch variants unless an authored policy chose from a closed exact set before the semantic attempt.

Certification levels:

```text
execute_only
status_and_cancel
safe_boundary_command
post_model_pre_tool_command
provider_async_subagents
```

Capabilities are additive only after evidence. A graph may be production-qualified at a lower level.

## 4. LangSmith required integration

### 4.1 Tracing

Every operation trace must correlate:

- BellLabs run/epoch and Workflow Type/Implementation;
- family workflow and Temporal run generation;
- semantic operation and technical runtime attempts;
- exact operation/deployment/assembly/context digests;
- model, tool, MCP, child, verifier, and sandbox spans;
- effect claims, usage settlements, artifacts/evidence, and terminal result ref.

Apply exact sampling, redaction, metadata allowlists, tenant boundaries, retention, and trace-export failure policy. Trace failure cannot alter business authority. No raw secret, prohibited PHI, unbounded transcript, or mutable authority object is attached.

### 4.2 Evaluation

Publish immutable datasets and evaluator versions for:

- operation contract/schema conformance;
- citation/evidence quality;
- goal satisfaction and verifier agreement;
- tool/MCP correctness and effect safety;
- intervention timing and acknowledgement;
- local-versus-remote semantic equivalence;
- recovery, duplicate, cancellation, and drift cases;
- latency, cost, context, and sandbox thresholds.

Experiments bind dataset, split, graph/deployment revision, assembly digest, evaluator code/prompt/model, thresholds, and environment snapshot. Promotion requires owner-accepted thresholds and reproducible reports; a dashboard screenshot is not sufficient evidence.

### 4.3 Selected bounded graph deployments

Deploy only bounded operation graphs. The deployment cannot:

- create BellLabs stages or GoalDirected iterations;
- terminalize or revise authoritative lifecycle;
- widen tools, MCP, sandbox, children, budgets, or context;
- infer linked-run boundaries;
- act as a durable parent for independent BellLabs work.

## 5. Remote OperationWorkflow lifecycle

The remote adapter follows this mandatory protocol:

1. Validate the exact operation/deployment binding and reserve capacity/budget.
2. Derive a stable provider idempotency key from the semantic operation attempt.
3. Start the remote run idempotently.
4. Persist a durable `RemoteOperationBinding` containing separately typed BellLabs, Temporal, provider thread, provider run, deployment, and idempotency identities.
5. Release the activity worker immediately after start/bind.
6. Transition the operation to `WAITING_ON_EXTERNAL`; retain only declared leases and resumption capacity.
7. Wait in workflow code using durable timers plus signals and bounded polling activities.
8. Reconcile fresh provider status after every wakeup, duplicate callback, timeout, cancellation, or worker recovery.
9. Fetch and validate immutable result/usage/trace manifests.
10. Transition through `READY_TO_RECONCILE` and authoritative CAS settlement exactly once.

Never keep a polling activity open for the remote run's duration. Never sleep in activity code as the durable wait mechanism. Provider conversation history is not fresh status.

Async activity completion is an optional qualified callback optimization only. If enabled:

- the callback token is secret, short-lived, scoped, and never stored in model context;
- callback delivery is authenticated, deduplicated, journaled, and reconciled against fresh provider state;
- timeout, duplicate, late, lost, forged, and callback-after-cancel cases fall back to normal workflow reconciliation;
- it does not replace stable provider idempotency or authoritative settlement.

## 6. Remote command injection and cancellation

Commands enter through `BellLabsRunWorkflow` using the `06C` envelope. Routing to a remote operation requires exact target identities, generation, authority, dedupe, and deployment capability.

`post_model_pre_tool_command` certification must prove:

- the graph has a deterministic, observable safe boundary after model output and before tool execution;
- an accepted command can pause, replace/reject a proposed call, add bounded context, or cancel according to policy;
- no consequential tool effect begins before command disposition when the boundary is armed;
- duplicate, stale, unauthorized, late, and conflicting commands are typed and harmless;
- process/transport loss around the boundary recovers without duplicate effect;
- acknowledgement and final disposition correlate to the BellLabs command ID.

Graphs without this evidence may accept commands only at certified turn/operation boundaries. Cancellation must use provider cancellation when available, but BellLabs cancellation authority and late-result rejection remain independent of provider acknowledgement.

## 7. Outbound MCP, tools, and effects

Implement exact MCP bindings:

1. load frozen server/tool definitions and secret refs;
2. construct transport without serializing credentials;
3. discover outside model-visible context;
4. compare observed names/schemas to frozen allowlists/digests;
5. canonicalize model-visible names;
6. wrap calls for authority, approval, effect identity, budget, timeout, retry, cancellation, trace/redaction, and usage;
7. fail closed on requested missing/drifted tools;
8. close sessions and reconcile ambiguous effects.

Transport policy:

- Streamable HTTP is deployed default;
- SSE is pinned legacy compatibility only;
- stdio is local or controlled-sandbox only;
- stateful sessions are operation/tenant scoped;
- no deployment-global credential session.

Tool, MCP, model, sandbox, and provider retries are technical attempts under one semantic operation unless the interpreter authorizes a new semantic attempt. Consequential effects require stable claims before invocation and exact settlement after observation. Progress is non-authoritative; elicitation maps to durable decisions/interventions.

## 8. Provider-neutral sandbox gateway

Define one BellLabs `SandboxGateway` contract with adapters for:

- LangSmith sandbox;
- Daytona;
- custom container/runtime infrastructure.

The common contract covers create/start/stop/delete, execute, upload/download, mount manifest, health, usage, snapshot/clone/restore, reconnect, optional services/tunnels, egress, secrets-by-ref, quotas, cleanup, and orphan reconciliation.

Sandbox ownership is operation-scoped by default. A sandbox binding records owner semantic operation, tenant, policy, provider, resource ID, generation, lease, and current authority version. A provider handle is never authority.

Snapshots are immutable historical captures. Restore creates or binds a new current sandbox generation and must reacquire:

- current authority and resource lease;
- secrets and scoped credentials;
- MCP/tool sessions;
- sockets/tunnels/services;
- deployment compatibility;
- budget and egress policy.

Distinguish:

| Concept | Meaning |
|---|---|
| `sandbox_snapshot` | immutable filesystem/runtime capture |
| `temporal_history_generation` | durable workflow execution generation |
| `agent_runtime_checkpoint` | bounded graph/thread runtime position |
| `context_manifest` | content-addressed reconstruction recipe |
| `environment_snapshot` | preparation-time compatibility evidence |

None substitutes for another. Snapshot restore cannot resurrect revoked authority or ambient credentials.

## 9. Provider async subagents and lifecycle promotion

A provider's async-subagent feature may be integrated only as a subordinate operation adapter:

- it remains inside one parent semantic `OperationWorkflow`;
- provider task/thread/run IDs are separately typed;
- ContextSlice, tools, budget, tenant, cancel/update, and result schema are exact;
- parent status is reconciled from fresh provider state;
- provider task completion settles through the normal operation boundary;
- it cannot become an untracked BellLabs workflow or inherit run authority.

If requested work needs independent lifecycle, durable external addressability, distinct authority/budget, reusable governed output, or composition dependency, the compiler must promote it to the custom BellLabs Temporal delegation tool from Stage 5. Provider async convenience cannot hide a Temporal child or linked-run boundary.

Test launch/check/update/cancel/list where supported, task/thread/run ID separation, stale status, capacity, crash, orphan reconciliation, tenant isolation, and feature disablement. Unsupported provider semantics produce exact readiness/failure, not emulation by an ungoverned background task.

## 10. Capability readiness and drift

Expose:

```text
implementation_ref
variant_ref
qualified_version
certification_level
maturity
configured
available
authority_prerequisites
resource_requirements
incompatible_combinations
fallback
required_evidence
last_qualification
qualification_expiry
```

Revalidate before start and reconciliation. Drift in model, graph, tool/MCP schema, skill digest, sandbox image, endpoint, or intervention capability never silently substitutes a new variant. Only affected implementations block/degrade according to authored policy.

## 11. QuickJS/PTC/dynamic track — optional and gated

QuickJS remains disabled unless the exact pinned engine/bridge passes an independent gate. Start only with pure transforms:

- exact engine/package/source digest and `mode="call"`;
- no ambient network, shell, filesystem, secrets, clock, or environment;
- strict CPU/time/memory/output/eval-call limits;
- typed canonical input/output;
- cancellation, trace, and usage;
- no claim that same-process QuickJS is an OS sandbox.

`turn`/`thread`, programmatic tool calling, and dynamic subagents require separate evidence for persistence, serialization, aggregate reservations, approvals, effect claims, recursion/Promise fan-out limits, context isolation, cancellation, and middleware-bypass attempts. Failure leaves the surface disabled without blocking stable Stage 6.

## 12. Hours-long and injected-failure qualification

Run a production-shaped campaign lasting multiple hours, long enough to cross:

- multiple polling/timer cycles;
- activity and workflow worker restarts;
- at least one parent Continue-As-New;
- remote and sandbox lease refresh/reconciliation;
- trace/evaluation batching;
- provider transient failure and quota/backoff windows.

Inject, at minimum:

- Temporal worker loss during child start, wait, completion, and settlement;
- activity timeout/heartbeat loss;
- duplicate/lost/delayed callback;
- remote start ambiguity and stale status;
- provider 429/5xx/network partition;
- model/tool/MCP timeout and MCP schema drift;
- consequential effect completed before response loss;
- sandbox worker loss, snapshot failure, restore incompatibility, and orphan;
- command injection before/at/after the certified boundary;
- cancellation racing completion;
- CAS conflict and duplicate completion;
- task-queue rollout and incompatible deployment revision.

Every injection has expected authoritative state, lease/effect disposition, retry owner, terminal behavior, and lineage assertion. "Eventually completed" alone is not acceptance.

## 13. Required proof and gate

Complete every requirement in `09A` using production compiler, Temporal family workflows, `OperationWorkflow` children, provider adapters, journals, communication contracts, and result materializer.

`09A` is an internal Stage 6 exit proof. It depends on stable candidate adapters completed by the
preceding Stage 6 slices, not on accepted Stage 6. Package `09` cannot record aggregate Stage 6
acceptance until `09A` passes.

Stage 6 passes when:

- LangSmith tracing/evaluation/sandboxes and selected bounded deployments meet exact contracts;
- local and remote variants are independently frozen, evaluated, and certified;
- remote start/bind/release/wait/reconcile and optional callback optimization pass;
- post-model/pre-tool injection is advertised only by deployments that pass its certification;
- MCP/tools/effects and provider-neutral sandbox lifecycle pass;
- provider async subagents remain subordinate and independent work is promoted to Temporal delegation;
- hours-long and injected-failure gates pass;
- `09A` proves complete heterogeneous overlap, early joins, recovery, intervention, Continue-As-New, and lineage;
- no Agent Server macro scheduler exists;
- QuickJS/PTC/dynamic posture is reported separately and may remain disabled.

Handoff includes exact variant/certification catalog, LangSmith trace/evaluation artifacts, remote lifecycle and callback matrices, injection timing evidence, MCP/tool/effect matrix, sandbox adapter/snapshot evidence, provider-async promotion tests, hours-long run reports, failure-injection ledger, resource/history measurements, `09A` evidence, and complete lineage query.
