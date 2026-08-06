# Stage 6 — stable provider completion, heterogeneous StageGraph composition, required async subagents, and optional dynamic capabilities

Status: not started  
Mission type: stable provider/runtime completion, heterogeneous workflow proof, required preview async track, and optional beta dynamic track  
Depends on: accepted Stage 5 and the corresponding accepted Stage 0 qualification spikes

## 1. Mission

Extend the Stage 5 stable capability compiler and complete the governed operation-capability provider layer. Harden outbound MCP, filesystem/search, skills, context/memory, sandbox, and snapshot composition; implement required asynchronous subagents behind their default-off qualification flag; then prove and qualify the full heterogeneous capability assembly—including async wait/resume—inside the unchanged generic StageGraph. QuickJS/programmatic tool calling/dynamic delegation remains a separate optional disabled track unless independently accepted.

The accepted Stage 0 owner decision requires the async-subagent migration track. Stage 6 cannot pass by merely documenting or deferring async subagents; it must implement and pass their launch/wait/resume/update/cancel/crash/orphan/capacity/tenant gate. Default-off means unavailable to unaccepted Workflow Implementations until promotion, not optional implementation work. QuickJS/PTC/dynamic delegation may remain disabled without blocking the stable Stage 6 path.

Follow [06A_STAGES_3_TO_6_OPERATION_ASSEMBLY_CONCURRENCY_AND_LINEAGE_CONTRACT.md](06A_STAGES_3_TO_6_OPERATION_ASSEMBLY_CONCURRENCY_AND_LINEAGE_CONTRACT.md) and complete [09A_STAGE_6_HETEROGENEOUS_STAGEGRAPH_COMPOSITION_PROOF.md](09A_STAGE_6_HETEROGENEOUS_STAGEGRAPH_COMPOSITION_PROOF.md).

## 2. Permission to clarify or interview

The agent may interview the owner before starting. Clarify:

- exact MCP servers/tools/transports/session scopes to support first;
- external MCP/skill discovery and promotion workflow ownership;
- whether QuickJS pure transforms, PTC, dynamic subagents, and `turn`/`thread` modes are enabled or deferred;
- which accepted first deployed Workflow Implementation will exercise async subagents after their gate; implementation and qualification are already required by Stage 0;
- ASGI versus remote HTTP async-subagent topology and scaling ownership;
- capacity/resumption reservation policy and task update semantics;
- sandbox snapshot/retention/provider posture;
- capability maturity exposed to coordinator and user-facing degradation semantics;
- scientific memory and context-retrieval policies not settled in Stage 5.

Feature enablement requires accepted Stage 0 evidence for the exact pinned version. “Available in docs” is not enough.

## 3. Required inputs

- Stage 1 exact definitions/capability vocabulary;
- Stage 3 interrupt, cancel, wait, fork, stream, lineage, resource-lease, operation-port, and reconciliation foundation;
- Stage 5 stable compiler, exact per-stage assemblies, Deep Agent adapter conformance, StageGraph composition proof, GoalDirected integration, context, sync-subagent, and sandbox implementation;
- Stage 0 MCP, QuickJS, async-subagent, Store, Sandbox, and middleware spike evidence;
- approved internal catalog definitions and coordinator discovery rules.

## 4. Implementation order

Execute Stage 6 in this order:

1. **6A — stable provider completion:** compiler extensions, MCP, skill/filesystem/context/Store, sandbox/snapshot, readiness, and drift handling.
2. **6B — required async-subagent implementation:** durable child runtime and StageGraph wait/resume integration, kept default-off for qualification.
3. **6C — heterogeneous StageGraph proof and async qualification:** the required `09A` composition, concurrency, isolation, lineage, crash, compatibility, and async promotion evidence.
4. **6D — optional QuickJS/PTC/dynamic track:** only if separately qualified and enabled; otherwise retain exact disabled contracts and fallbacks.
5. **6E — optional bounded speculation track:** only for published pure/read-only stage policies; ordinary frontier concurrency does not depend on it.

Do not implement 6B as an agent-local convenience disconnected from StageGraph or Stage 3 reconciliation. Do not declare its gate passed until 6C exercises it through the required production composition path. Do not let optional 6D/6E delay the stable/required gates.

## 5. Deliverables

### 5.1 Capability compiler extension and assembly validator

Extend, rather than replace, the Stage 5 stable compiler. Compile requested operation mechanics in two steps:

1. domain selection: exact Workflow Type/Implementation, operation class, authority, budgets, workspace, linked-run slots;
2. runtime assembly: exact harness, middleware, context, tools/MCP, skills, delegation mode, interpreter, sandbox, model, verifier, and fallback.

The compiler must:

- intersect every requested capability with workflow, implementation, caller/parent authority, data/workspace/network policy, approvals, environment, runtime compatibility, and maturity policy;
- report exact implementation refs, availability, maturity, resource needs, conflicts, fallback, and evidence;
- reject duplicate core middleware, conflicting summarizers/context editors, duplicate/ambiguous filesystem tools, incompatible skill backends, missing persistence for interrupts, unsupported deployment transports, and hidden linked-run boundaries;
- freeze all refs/digests and `graph_assembly_digest` in the launch ticket;
- never treat installed packages, model preference, tool discovery, or assistant config as authority.

For every StageGraph stage/variant, emit the exact `StageCapabilityRequirement`, `StageExecutionBinding`, `OperationAssemblySpec`, predicted model-visible surface, resource envelope, compatibility key, and lineage root defined in `06A`. Different stages may select different models and capability surfaces. No rule requires the models used by the legacy OpenAI Agents SDK/Temporal path.

### 5.2 Runtime graph assembly hardening

Extend the Stage 2 factory only where declared composition affects construction:

- lifecycle topology stays stable;
- introspection graph exposes matching schema/topology without resources;
- execution loads exact `GraphAssemblySpec` and runtime binding;
- bind exact middleware/tools/subagents/backends/sandbox handles;
- per-run resources use async context management;
- secret-free structure cache is keyed by assembly digest;
- process-global cache excludes credentials, sessions, Store memory, tenant data, and handles;
- changed node/channel/reducer/interrupt compatibility creates new schema/blue-green binding.

Prefer static generic StageGraph. Use runtime assembly for GoalDirected only when thread/run-scoped resources actually require it.

### 5.3 Outbound MCP adapter

Implement `MultiServerMCPClient` behind BellLabs exact bindings:

1. load exact server/tool definitions and secret refs;
2. construct transport without serializing credentials;
3. discover tools outside model-visible context;
4. compare observed names/schemas with frozen allowlist/digests;
5. canonicalize runtime names such as `mcp__{server_id}__{tool_name}`;
6. wrap every call for authority, effect identity, approval, budget, timeout, retry, cancellation, tracing/redaction, and usage settlement;
7. fail closed on missing/extra requested/drifted tools;
8. close explicit sessions in async cleanup.

Session policy:

- stateless/read-only: short-lived adapter default;
- stateful: explicit tenant-specific operation/stage scope;
- Streamable HTTP deployed default;
- SSE pinned legacy compatibility only;
- stdio local or controlled sandbox only;
- no deployment-global credential session.

Map progress/logs to non-authoritative custom events. Map elicitation to Stage 3 durable decision/interrupt. Tool-returned state updates remain reducer/middleware bounded.

### 5.4 Agent skills and filesystem capability finalization

- internal skill catalog stores exact reviewed file manifest/digest/compatibility/provenance/review;
- operation binding mounts only exact skill refs read-only;
- Deep Agents progressive disclosure loads metadata, then instructions, then referenced resources;
- custom subagents receive explicit skill refs and compatible backend;
- skill text cannot add tools, network, shell, writes, credentials, budget, or delegation;
- external skill discovery remains quarantined until separately inspected/published;
- capability compiler maps vendor tool names to stable BellLabs filesystem/search IDs;
- do not expose both `grep` and `grep_search` without intentionally distinct scopes;
- sandbox-backed `execute` is required for deployed shell/process/browser work.

### 5.5 Context, Store, and memory hardening

Complete cross-operation context tests and tooling:

- exact context manifest/source/transformation lineage;
- purpose/tenant/environment filters;
- deterministic retrieval tie-breaking where required;
- context-budget allocation across parent/children;
- contamination, retraction, expiry, deletion/tombstone, and compaction-drift monitors;
- default-deny Store memory for scientific claims;
- reviewed procedural memory only;
- context manifest and Store retention independent of checkpoint retention;
- operator inspection without raw sensitive payload leakage.

### 5.6 Sandbox and snapshot completion

Complete provider-neutral operations before async/dynamic tracks:

- create/start/stop/delete;
- snapshot/clone/restore;
- upload/download and mount manifest;
- execute/reconnect/services/tunnels only if allowed;
- resource/network/egress/secret policy;
- health/usage/settlement;
- idempotent cleanup and orphan reconciliation.

Maintain four qualified concepts:

| Concept | Meaning |
|---|---|
| `sandbox_snapshot` | immutable historical filesystem/runtime capture |
| `langgraph_checkpoint` | runtime state position |
| `context_manifest` | content-addressed reconstruction recipe |
| `environment_snapshot` | preparation-time availability/compatibility evidence |

No one concept substitutes for another. Restore clones and reacquires secrets, leases, MCP connections, sockets, and current authority.

### 5.7 Async subagent runtime track — required, default-off until promoted

Enable only for the qualification Workflow Implementation until the exact preview version passes all Stage 0/Stage 6 evidence and the owner accepts promotion.

Definitions distinguish:

- graph ID/assistant target;
- ASGI co-deployment versus authenticated remote HTTP endpoint ref;
- exact graph assembly/context/update/cancel/reconcile policies;
- resource slots/ceilings/maturity/fallback.

On launch:

1. reserve parent/child capacity and budgets, including supervisor/resumption capacity;
2. start task through accepted Agent Protocol transport;
3. persist durable `AsyncTaskBinding` with separate task, child thread, and child run IDs;
4. transition the parent operation/stage to `WAITING_ON_ASYNC_CHILD`, persist retained/released leases, and release the parent worker;
5. resume from callback/poll/reconciler only after fresh status/result validation;
6. transition through `READY_TO_RECONCILE` and settle usage/result exactly once through the normal deterministic boundary.

Support the five framework tools through bounded wrappers:

- `start_async_task`;
- `check_async_task`;
- `update_async_task`;
- `cancel_async_task`;
- `list_async_tasks`.

Rules:

- task statuses in conversation history are stale; check/list before reporting;
- preserve full IDs; never rely on task ID equaling thread ID;
- update semantics require accepted policy, approval, a successor runtime attempt, and an explicit semantic-attempt decision;
- cancellation and crash can create orphan incidents that Stage 3 reconciliation owns;
- reserve at least resumption/supervisor capacity plus active child slots;
- parent and child tenant/context/authority are isolated;
- independently governed/durable Workflow Type work becomes a linked run instead.

### 5.8 Heterogeneous capability-aware StageGraph composition and async qualification

Complete every requirement in `09A`. This is a required gate, not an illustrative example. It must use the production compiler, operation executor, adapters, journals, reducers, runtime bindings, result materializer, and the Stage 6B async-child implementation.

At minimum prove concurrently eligible stages with different native/Deep Agent/model/tool/MCP/skill/context/workspace/verifier profiles, real measured overlap, exact isolation, deterministic settlement, full lineage, capability drift behavior, cancellation, and crash recovery. Include the async-child launch/wait/resume/update/cancel/crash/orphan/capacity/tenant path. The Stage 4 scheduler topology must remain stable unless a deliberate compatibility-version change is accepted.

### 5.9 Linked-run escalation

Implement or integrate the classifier and coordinator rules that force linked run for:

- recognized distinct Workflow Type;
- separate admission/authority;
- substantial separate budget;
- durable independent wait/recovery;
- reusable governed output;
- declared composition dependency.

Async subagent convenience cannot hide a linked-run boundary.

### 5.10 QuickJS pure interpreter track — optional

If accepted, start with bounded pure transforms:

- exact engine/package/source digest;
- explicit `mode="call"` default even if ecosystem default differs;
- no PTC/dynamic subagents;
- no ambient network, shell, filesystem, secrets, clock, or environment;
- CPU/time/memory/output/eval-call limits;
- typed inputs/outputs and deterministic canonicalization where claimed;
- cancellation/trace/usage;
- no claim that same-process QuickJS is an OS security sandbox.

`turn` and `thread` profiles are separate experimental definitions. `thread` requires snapshot serialization, checkpoint-size, recovery, and non-serializable-value handling evidence. Restoring interpreter state never rolls back external effects.

### 5.11 Programmatic tool calling and dynamic subagent track — optional

Enable only if accepted separately:

- explicit PTC allowlist with independently wrapped capabilities;
- exact allowed synchronous subagent refs;
- dynamic dispatch explicitly enabled/disabled, never implicit from middleware combination;
- aggregate budget/concurrency reserved before `eval`;
- maximum tool/subagent calls, fan-out, depth, result bytes, and execution duration;
- `task()` inputs and optional response schema validated;
- each child still receives ContextSlice and returns result manifest;
- all PTC/dynamic effects, approval, idempotency, cancellation, tracing, and settlement independently enforced because normal model tool/HITL paths may not apply;
- synthesized result admitted after child usage/effects reconcile.

Attempt bypasses through nested calls, Promise fan-out, recursion, and calls not visible in ordinary tool-call middleware. If guards cannot be proven, keep PTC/dynamic disabled while retaining pure interpreter transforms.

### 5.12 Bounded optimistic/speculative execution track — optional

If an accepted Workflow Implementation enables optimistic execution, implement the `06A` speculation contract without changing default StageGraph readiness semantics:

- exact `speculation_policy_ref` declaring assumptions, dependency/input digest predicates, invalidation keys, expiry, maximum wasted budget, and maximum speculative concurrency;
- compiler restriction to `pure` or `read_only` effect classes;
- separate speculative reservation/attempt identity linked to the eventual semantic stage attempt;
- immutable inputs and quarantined result/artifact/evidence manifests;
- deterministic commit barrier that revalidates all prerequisite digests and authoritative versions;
- accept/attach result only when the normal interpreter declares the stage runnable and the speculative assumptions still match;
- invalidate/discard otherwise, while settling actual cost/usage and preserving lineage;
- cancellation, duplicate, crash, and cleanup behavior;
- zero tool/MCP/sandbox/subagent capability with consequential effects in the effective speculative surface.

Do not label parallel execution of an already-admitted frontier as optimistic execution. `idempotent_effect` is not sufficient for speculation. If this optional track is not enabled, the compiler rejects enabled speculation policies and all stable workflows continue with ordinary bounded concurrency.

### 5.13 Capability readiness/degradation

Expose non-mutating readiness facts for every optional capability:

```text
implementation_ref
qualified_version
maturity
configured
available
authority_prerequisites
resource_requirements
incompatible_combinations
fallback
required_evidence
last_qualification
```

Unavailable optional capability must degrade only Workflow Implementations that require it and return typed reasons. A required capability—including async subagents for the accepted qualification implementation—blocks that implementation and its stage gate rather than silently degrading.

## 6. Required tests

### Capability/compiler

- authority/environment/maturity intersections;
- mutable alias and unreviewed external candidate denial;
- duplicate/conflict middleware/filesystem tools;
- skill/backend/deployment compatibility;
- predicted model/interpreter-visible tool surface;
- linked-run boundary enforcement;
- feature-disabled compile and runtime behavior.
- every heterogeneous stage has an exact requirement/execution binding/assembly/resource/compatibility record;
- different stages may bind different exact models without legacy-provider equality, while runtime preference cannot change a frozen choice;
- provider readiness revalidation never silently substitutes model/tool/MCP/skill/sandbox/async targets.

### MCP

- allowlist/schema digest/auth/session/timeout/cancel/retry/progress/elicitation;
- idempotent and non-idempotent effect behavior;
- session cleanup on success/error/cancel/interrupt;
- tenant credential isolation;
- stdio Cloud rejection;
- trace/redaction.

### Skills/context/Store

- progressive disclosure and exact file digests;
- explicit child skills;
- instruction-based authority escalation denied;
- context reconstruction/contamination/retraction/deletion thresholds;
- Store cannot authorize/terminalize;
- tenant/purpose isolation.

### QuickJS/dynamic

- resource/output/call/fan-out/recursion limits;
- `call`/`turn`/`thread` persistence as enabled;
- serialization/checkpoint/recovery;
- PTC/dynamic approval/authorization/idempotency/cancel/trace/usage bypass attempts;
- disabled bridge removes only that surface and preserves normal sync delegation.

### Async

- ASGI and accepted HTTP auth topology;
- launch/check/update/cancel/list and fresh status;
- crash/orphan/stale status/full IDs;
- StageGraph transitions through `WAITING_ON_ASYNC_CHILD` and `READY_TO_RECONCILE` without occupying a parent worker;
- exact retained/released lease behavior and fresh result validation before resume;
- capacity deadlock at minimum configuration;
- cancellation/usage settlement;
- context/tenant/authority isolation;
- feature disablement fallback and linked-run escalation.

### Heterogeneous StageGraph composition

- every compilation, scheduling, capability-isolation, lineage, recovery, cancellation, drift, and gate assertion in `09A`;
- barrier/controlled-clock evidence of real overlap for differently assembled stages;
- subordinate model/tool/MCP/sync-child/async-child capacity never exceeds the hierarchical envelope;
- randomized completion and injected crashes preserve deterministic settlement;
- final typed result resolves the full lineage across native, Deep Agent, MCP, sync-child, async-child, verifier, artifact, effect, usage, and trace records;
- Stage 4 scheduler topology/compatibility remains unchanged or is deliberately versioned with accepted migration evidence.

### Optimistic/speculative execution, when enabled

- compiler denies non-pure/read-only stages and any consequential effective capability;
- controlled dependency race proves quarantined work cannot settle before the commit barrier;
- matching assumptions attach the result exactly once when the stage becomes normally runnable;
- invalidated/expired work is discarded, actual usage is settled, and downstream stages cannot observe its artifacts;
- crash/cancel/duplicate execution leaves no promoted artifact, leaked lease, or ambiguous effect;
- wasted-budget and speculative-concurrency ceilings hold under randomized scheduling.

### Sandbox/snapshots

- isolation, egress, secrets, limits, snapshot/clone/reconnect/cleanup;
- no capability restoration from history;
- environment/checkpoint/context/sandbox snapshot confusion rejected;
- orphan reconciliation and usage settlement.

## 7. Gate

Stable Stage 6 path passes when:

- exact capability compiler predicts and enforces the real runtime surface;
- outbound MCP, skills, filesystem, context/Store, sandbox, and snapshot contracts pass;
- graph factory remains introspection-safe and cleans per-run resources;
- coordinator-facing maturity/readiness/fallback records are accurate;
- no external candidate becomes executable without publication;
- enabled capabilities have trace/redaction/idempotency/cancel/evaluation evidence.
- the required `09A` heterogeneous StageGraph composition proof passes;
- the async-subagent track passes launch/wait/resume/update/cancel/crash/orphan/capacity/tenant/lineage tests and remains default-off except for accepted qualification/implementation bindings;
- coordinator-authored or owner-authored workflows can select granular per-stage capability assemblies without embedding provider mechanics in StageGraph nodes.

QuickJS/dynamic and bounded speculation pass only when their independent guards pass all applicable tests. Either optional track may be deferred with its flags off. Failure or deferral of the required async-subagent or heterogeneous-composition gates makes Stage 6 `REWORK_REQUIRED` unless the owner formally amends the accepted Stage 0 decision.

## 8. Explicit non-goals

- Do not enable every available middleware/tool/capability.
- Do not treat QuickJS as a sandbox or async task as a linked run.
- Do not speculate consequential/idempotent-effect stages or expose quarantined outputs before the deterministic commit barrier.
- Do not permit arbitrary remote MCP/skill attachment.
- Do not let Store or context summaries become scientific authority.
- Do not broaden production traffic.

## 9. Outgoing handoff additions

Include:

- complete compiled capability/tool/middleware visibility manifest;
- per-stage requirement/execution-binding/assembly/resource/compatibility catalog;
- `09A` heterogeneous composition evidence, measured concurrency, isolation, crash matrix, and end-to-end lineage report;
- MCP server/tool/session/schema matrix;
- skill refs/backend/inheritance proof;
- context/Store policy and contamination/retraction results;
- QuickJS profiles and exact enabled/disabled PTC/dynamic posture;
- optimistic/speculative policy profiles, measured waste/concurrency, and exact enabled/disabled posture;
- async subagent topology/state/capacity/update/cancel/reconcile evidence;
- linked-run escalation rules;
- sandbox/snapshot/environment/checkpoint distinction and lifecycle evidence;
- graph assembly/factory compatibility manifest;
- feature flags, maturity, fallbacks, and deferred tracks, with async recorded as required/default-off and QuickJS/PTC/dynamic recorded separately as optional.
