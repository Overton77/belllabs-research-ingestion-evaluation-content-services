# Stage 5 — GoalDirected Temporal family and exact Deep Agents harness

Status: `NOT_STARTED`
Document role: normative Stage 5 implementation and qualification package
Mission type: reusable bounded agent runtime plus GoalDirected Temporal workflow-family implementation
Depends on: accepted Stage 4 and [06A_STAGES_3_TO_6_OPERATION_ASSEMBLY_CONCURRENCY_AND_LINEAGE_CONTRACT.md](06A_STAGES_3_TO_6_OPERATION_ASSEMBLY_CONCURRENCY_AND_LINEAGE_CONTRACT.md), [06B_STAGE_3_TEMPORAL_WORKFLOW_FOUNDATION.md](06B_STAGE_3_TEMPORAL_WORKFLOW_FOUNDATION.md), and [06C_STAGE_3_COMMUNICATION_AND_INTERVENTION_QUALIFICATION.md](06C_STAGE_3_COMMUNICATION_AND_INTERVENTION_QUALIFICATION.md)

## 1. Mission and accepted architecture

Complete the reusable exact Deep Agents operation harness introduced by the Stage 4 vertical slice, then implement GoalDirected as a Temporal workflow family under `BellLabsRunWorkflow`.

BellLabs PostgreSQL/application services own admission, lifecycle authority, accepted goal-revision
facts, settlement, and terminality. `BellLabsRunWorkflow` and `GoalDirectedWorkflow` coordinate,
route, and reconcile runtime work against that authority; they do not independently authorize,
settle, or terminalize BellLabs state.

Each significant GoalDirected iteration is one generic `OperationWorkflow` child. Model turns,
ordinary tool calls, planning, filesystem work, skills, and built-in synchronous subagents remain
internal to that bounded operation runtime. The Temporal family coordinates goal-revision and
iteration execution, durable waits, communication, independent verification, context rollover, and
delegation, while the pure interpreter supplies deterministic transition/convergence decisions and
BellLabs application services accept and settle them.

```text
BellLabsRunWorkflow
└── GoalDirectedWorkflow
    ├── OperationWorkflow(goal iteration; bounded Deep Agent runtime)
    ├── OperationWorkflow(independent verifier)
    └── delegated Temporal child/linked run when independent lifecycle is required
```

There is no outer LangGraph lifecycle and no Agent Server macro scheduler. A LangGraph/Deep Agents graph may execute inside an `OperationWorkflow` activity boundary as the exact bounded agent runtime.

## 2. Semantic boundaries

Keep these identities distinct:

- **Goal iteration:** durable GoalDirected semantic step selected by the pure `GoalDirectedInterpreter`.
- **Operation attempt:** one addressable `OperationWorkflow` child for that iteration or verifier.
- **Agent thread/session:** one semantic operation attempt's bounded model/tool conversation.
- **Model turn:** internal technical step; never a Temporal child and never BellLabs lifecycle authority.
- **Context rollover:** reconstruction of bounded agent context, possibly continuing the same semantic operation according to policy.
- **Temporal Continue-As-New:** history-management mechanism for a family workflow; it does not imply a new goal iteration, agent thread, or context rollover.
- **Delegated lifecycle:** distinct governed work started through BellLabs' Temporal delegation tool.

One agent thread corresponds to one semantic operation attempt. Runtime retry may reopen or reconstruct that exact attempt only under its frozen policy. A new semantic retry gets a new operation attempt and agent thread.

## 3. Implementation order

1. **5A — exact harness:** compile, construct, conformance-test, and evaluate native/LangChain/Deep Agents operation assemblies.
2. **5B — StageGraph completion proof:** rerun the Stage 4 heterogeneous slice with the full harness; preserve the Temporal StageGraph topology.
3. **5C — GoalDirected family:** implement interpreter-driven iterations, independent verifier children, interventions, rollover, and terminality.
4. **5D — GoalDirected research vertical proof:** run a production-shaped multi-iteration research case after StageGraph proof passes.

Repair the shared `OperationWorkflow`/executor abstraction if either workflow needs special-case provider mechanics.

## 4. Exact capability compiler

For each operation compile an immutable `OperationAssemblySpec` containing:

- implementation kind/ref and typed input/output contract;
- exact model and authored technical retry/fallback policy;
- base prompt, permitted dynamic slots, and rendering implementation;
- exact ordered middleware with hook scopes and digests;
- model-visible tools and schemas;
- reviewed skill bundles and mounts;
- context sources, trust classes, budgets, retrieval, compression, and preservation rules;
- filesystem/workspace backend and path policy;
- Store namespaces/purposes and denial rules;
- built-in synchronous subagent catalog and effective grants;
- custom BellLabs Temporal delegation tool policy;
- verifier, effect, approval, trace/redaction, resource, cancellation, and compatibility policies;
- predicted runtime surface, readiness facts, and assembly digest.

Compilation must reject mutable aliases, runtime model preference, implicit inheritance, duplicate core Deep Agents middleware, ambiguous tool/filesystem names, unreviewed skills, unsupported middleware ordering, undeclared delegation, missing verifier/output contract, and resource requests outside authority.

Identical immutable inputs reproduce the same assembly and predicted surface. Runtime availability checks may fail or follow only an authored exact fallback; they may not substitute another capability.

## 5. Bounded Deep Agents operation runtime

Construct the exact `create_agent`/`create_deep_agent` variant from the frozen assembly. Preserve the complete Deep Agents harness:

- planning and bounded model turns;
- typed operation context;
- exact middleware and native async hooks;
- wrapped tools with approval/effect/budget/cancellation;
- reviewed skills with progressive disclosure;
- explicit filesystem/workspace backend;
- purpose-scoped Store access;
- structured output and compact result persistence;
- operation-local synchronous subagents;
- usage, artifact, evidence, trace, and lineage manifests.

Deep Agents core planning, filesystem, skills, subagent, and context mechanics must not be duplicated by custom middleware. The harness may checkpoint internally only when the selected runtime supports it and the checkpoint is operation-local. Full messages never become GoalDirected workflow state.

Middleware order:

1. `before_agent`: binding, scope, phase, budget, lease, trace, and cancellation checks;
2. dynamic prompt: exact base plus typed permitted slots;
3. `before_model`: trust filtering, retrieval, redaction, context budget, compaction/offload;
4. model wrapper: exact model, timeout, technical retry/fallback, usage and trace;
5. `after_model`: structured output, tool-call, policy, and evidence validation;
6. tool wrapper: authority, approval, effect identity, timeout/retry/cancel, budget and settlement;
7. `after_agent`: compact manifests, usage settlement, snapshot/cleanup, durable events.

Test actual wrapper nesting and reverse after-hook ordering.

## 6. Context, filesystem, skills, and Store

The context policy must define source trust/admission/quarantine, total and reserved token budgets, deterministic retrieval filters/tie-breakers, immutable preservation set, compression trigger/schema, mutation writers, provenance, retention/deletion, and evaluation thresholds.

Never summarize or rewrite protected goal/scope, exact instructions, authority, approvals, budgets, semantic-attempt facts, source locators/digests, citation edges, contradictions, or accepted evidence.

Reconstruction:

1. load frozen prompt/policy/protected scope and current authoritative projection;
2. verify the accepted context manifest and source digests;
3. rehydrate bounded evidence/citations/contradictions;
4. retrieve only purpose-compatible non-authoritative Store items;
5. add bounded working summary and verifier feedback;
6. render and persist the new context-assembly digest;
7. fail safely or roll over when mandatory context cannot fit.

Filesystem roots, mounts, read/write/delete rights, size limits, exclusions, sensitivity, and promotion rules are exact. Deployed shell/process/browser work requires an approved sandbox adapter in Stage 6; Stage 5 may use only its qualified local/test backend. Skill text cannot grant capability. Scientific-claim memory is denied by default; Store cannot authorize, approve, prove, or terminalize.

## 7. Synchronous subagents versus Temporal delegation

Built-in Deep Agents subagents are operation-local, synchronous, non-addressable implementation details:

- fresh per invocation unless the frozen operation-local policy says otherwise;
- explicit prompt/model/tools/middleware/skills/filesystem/output grants;
- bounded `ContextSlice`, never parent transcript/secrets/authority;
- parent blocks until schema-validated result;
- aggregate subordinate capacity reserved before permitted fan-out;
- no durable external inbox, independent cancellation lifecycle, or reusable governed result;
- cannot terminalize, revise the GoalDirected workflow, or survive as an independently managed unit.

Work requiring any of the following must use the custom BellLabs delegation tool:

- durable independent wait or recovery;
- addressable update/cancel/query;
- distinct Workflow Type or authority;
- substantial independent budget;
- reusable governed result;
- declared lifecycle dependency.

The delegation tool validates a typed request, checks authority and resource policy, and asks the parent Temporal workflow/application service to start an `OperationWorkflow` child or linked `BellLabsRunWorkflow`. It returns a durable binding/ref, not a provider-specific task object. The model cannot select arbitrary workflow code, task queues, IDs, authority, or budget.

## 8. GoalDirected workflow loop

`GoalDirectedWorkflow` performs:

1. reconcile frozen goal/RunPlan/bindings and authoritative lifecycle;
2. call the pure `GoalDirectedInterpreter`;
3. claim and reserve one significant iteration;
4. start its `OperationWorkflow` idempotently;
5. process completion and interventions using canonical ordering;
6. start an independently bound verifier `OperationWorkflow`;
7. settle verifier output and ask the interpreter for accept, revise/repair, roll over, wait, handoff, fail, or continue;
8. persist accepted revisions/snapshots/context manifests through CAS;
9. recompute immediately;
10. terminalize only when verifier action and BellLabs authoritative transition agree.

Compact workflow state contains protected-scope digest, goal revision lineage, iteration projection, active child bindings, context/session refs, workspace snapshot refs, agent result and verifier refs, blockers/no-progress projection, communication inbox refs, continuation generation, and terminal result ref.

No top-level `messages`, provider client, filesystem handle, or mutable workspace is allowed.

## 9. Independent verifier

The verifier is a separately bound `OperationWorkflow`, not an after-hook in the worker session. It has exact model/prompt/tools/evidence/thresholds and:

- reads immutable operation outputs and evidence refs;
- evaluates goal/acceptance/citation contracts;
- returns typed action, reasons, measurements, and evidence;
- shares no mutable worker session or hidden worker authority;
- cannot invoke the worker's private tools or filesystem;
- cannot directly revise or terminalize;
- influences lifecycle only through interpreter and authoritative CAS settlement.

Technical verifier retry retains semantic verifier identity; a policy-driven re-verification creates a new semantic attempt.

## 10. Rollover, Continue-As-New, and handoff

Context rollover and Temporal continuation are independently triggered and independently recorded.

Context rollover:

- follows token, no-progress, provider-session, or workspace policy;
- closes the current operation attempt or reconstructs it only as explicitly authored;
- writes immutable context/workspace manifests and parent lineage;
- reacquires current secrets, leases, tool sessions, and authority;
- preserves protected scope, evidence, contradictions, approvals, budgets, and exact assembly.

Temporal Continue-As-New:

- bounds family-workflow history;
- carries compact goal state and active-child reconciliation records;
- never silently creates a new agent thread, iteration, revision, or context summary;
- reconciles child completion/cancellation and duplicate commands across generations.

Handoff is a governed semantic outcome with a complete context manifest, not a summary-only escape hatch.

## 11. Communication and intervention

Use `BellLabsRunWorkflow` as the stable external target and the `06C` envelope/disposition contracts. Support goal-scoped pause/resume/cancel, evidence injection, accepted scope-revision request, human decision, operation-addressed steering at declared safe boundaries, and delegated-child update/cancel.

Commands are authorized, deduplicated, journaled, and version-checked. Agent injection is admitted into the next safe context assembly; it is not spliced into an in-flight model response. Built-in synchronous subagents remain non-addressable.

## 12. GoalDirected research vertical proof

After the StageGraph harness proof, publish a research Workflow Implementation that demonstrates:

- immutable protected research objective and acceptance contract;
- at least two significant iterations, each a distinct `OperationWorkflow`;
- exact Deep Agents model, tools, reviewed skill, context, and local workspace;
- bounded synchronous specialist use;
- one custom BellLabs Temporal delegation request with independent lifecycle;
- independent verifier rejection followed by evidence-driven repair;
- context rollover distinct from parent Continue-As-New;
- inbox evidence injection and pause/resume;
- typed cited result, usage/effect settlement, and complete lineage.

Use fixture providers where live effects are unsafe, plus one owner-approved live evaluation path. Semantic acceptance uses explicit quality/citation thresholds, not provider/token/trace equality with legacy.

## 13. Required tests

### Compiler and harness

- deterministic assembly and predicted-versus-observed surface;
- mutable alias, implicit inheritance, duplicate middleware/tool, and drift denial;
- exact tool/skill/context/filesystem/subagent isolation;
- hook order, retry accounting, cancellation, effects, redaction, and structured output;
- operation-local checkpoint/recovery and bounded transcript size;
- Deep Agent adapter passes all shared `OperationExecutor` cases.

### Goal lifecycle

- protected-scope mutation denial and bounded accepted revision;
- stable iteration/operation/thread/verifier identities;
- no-progress, repeated blocker, convergence, and budget exhaustion;
- verifier reject/repair/accept and terminality agreement;
- crash before/after iteration start/result, verifier, revision, snapshot, and terminal CAS;
- deterministic duplicate and same-time completion handling.

### Delegation and capacity

- synchronous child isolation, depth/count/concurrency, cancellation, and release;
- built-in child cannot be addressed externally;
- independent-lifecycle classifier forces Temporal delegation;
- delegation start ambiguity, wait, update, cancel, crash, orphan reconciliation, and linked-run escalation;
- protected supervisor/resumption capacity under maximum subordinate load.

### Rollover and communication

- repeated context rollover preserves mandatory facts and lineage;
- Continue-As-New with active iteration/verifier/delegated child;
- no identity conflation between rollover and continuation;
- accepted, duplicate, stale, unauthorized, conflicting, and terminal commands;
- injection only at declared safe boundary.

### E2E

- StageGraph full-harness proof remains green;
- GoalDirected research prepare/admit/iterate/verify/repair/delegate/rollover/intervene/result;
- process loss at every durable boundary;
- compact histories/checkpoints and no prohibited data;
- trace/evaluation hierarchy and end-to-end lineage query.

## 14. Gate and handoff

Stage 5 passes when the exact reusable harness is reproducible; StageGraph still runs through Temporal `OperationWorkflow` children; GoalDirected iterations and independent verifiers are Temporal children; model turns remain internal; built-in subagents are operation-local/non-addressable; independent lifecycle uses BellLabs Temporal delegation; rollover and Continue-As-New remain distinct; the GoalDirected research proof passes; and only BellLabs authority terminalizes.

QuickJS, programmatic tool calling, provider async subagents, remote LangSmith deployments, and broad production cutover remain disabled for Stage 6.

Handoff includes compiler/harness manifests, predicted/observed surfaces, StageGraph proof evidence, GoalDirected topology/history policy, context reconstruction vectors, sync-child and Temporal-delegation classification evidence, verifier separation, communication qualification, crash/capacity matrices, research evaluation, compact-state measurements, and complete lineage reports.
