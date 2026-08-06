# Stage 5 — stable capability compiler, governed Deep Agents harness, StageGraph composition, and GoalDirected

Status: not started  
Mission type: reusable stable operation-capability compilation and execution, followed by deterministic GoalDirected lifecycle  
Depends on: accepted Stages 1–4

## 1. Mission

First implement the exact stable operation-capability compiler and bounded LangChain/Deep Agents harness selected by each per-stage/operation binding. Register it behind the Stage 3 `OperationExecutor` port and prove that Stage 4 can run a capability-bearing stage without changing scheduler topology. Then port the existing GoalDirected semantics to a deterministic outer LangGraph as another consumer of the same compiler, executor, and harness.

Preserve goal protection, iterations, budgets, revisions, convergence, rollover, handoffs, independent verification, and BellLabs terminality while adding the Deep Agents planning/filesystem/reviewed-skill/context/synchronous-specialist capabilities that benefit open-ended work.

This is the stable Deep Agents path. QuickJS/PTC/dynamic delegation remains an optional disabled Stage 6 track. Async subagents also remain disabled in Stage 5, but their Stage 6 implementation and qualification track is required by the accepted Stage 0 decision.

This migration does not constrain the new Workflow Implementations to the model/provider selections used by the OpenAI Agents SDK/Temporal path. Model, prompt, tool, specialist, and verifier choices are exact compiled implementation decisions. Parity is evaluated at BellLabs authority, obligations, evidence, typed-result, budget, failure, and owner-approved semantic-quality boundaries—not provider or incidental token/trace equality.

## 2. Permission to clarify or interview

The agent may interview the owner before starting. Clarify:

- first GoalDirected Workflow Type/Implementation and accepted parity baseline;
- protected goal/scope fields and allowed revision policy;
- independent verifier separation and accepted evidence contract;
- session/workspace reuse versus fresh/fresh-from-handoff modes;
- rollover triggers, snapshot retention, and maximum checkpoint/context size;
- permitted skills, filesystem capabilities, Store memory purposes, and scientific-memory denial;
- synchronous specialist catalog and dictionary versus compiled-graph construction;
- sandbox egress/mount/secret/cleanup requirements;
- output quality/citation/context-preservation thresholds;
- first non-legacy model policies for the stable Deep Agent and independent verifier, including authored fallback and evaluation thresholds;
- which StageGraph stage will be the Stage 5B capability-composition slice and confirmation that it does not require Stage 6-only MCP/async mechanics.

Do not enable an undeclared general-purpose subagent or let parent skills/tools/filesystem leak into custom children by default.

## 3. Existing BellLabs seams to preserve

Inspect and reuse:

- `app/domain/orchestration/goal_directed.py::GoalDirectedInterpreter`;
- GoalDirected contracts/identities/revisions/handoffs in `app/domain/orchestration/`;
- `GoalDirectedLaunchService`, iteration executor, handoff preparer, independent verifier, evaluator ports;
- operation-execution binding/materialization/delegation/snapshot services;
- current bounded live GoalDirected proof and workspace service;
- run-control lifecycle/budgets/evidence/terminalization;
- coordinator exact preparation/launch/result paths;
- Stage 4 operation port/registry, exact stage execution bindings, native adapter conformance suite, stable scheduler compatibility manifest, and measured concurrency evidence.

Legacy Agent SDK/Temporal behavior is a parity oracle, not a target architecture.

Follow [06A_STAGES_3_TO_6_OPERATION_ASSEMBLY_CONCURRENCY_AND_LINEAGE_CONTRACT.md](06A_STAGES_3_TO_6_OPERATION_ASSEMBLY_CONCURRENCY_AND_LINEAGE_CONTRACT.md). Do not build a GoalDirected-only harness that StageGraph cannot invoke.

## 4. Implementation order and target topology

Execute this work package in three gated internal units:

1. **Stage 5A — stable compiler and harness:** compile, predict, construct, and conformance-test the stable native/LangChain/Deep Agents operation surface.
2. **Stage 5B — StageGraph composition:** register the harness through the Stage 3 executor port and run at least one StageGraph stage with a distinct exact capability assembly. The Stage 4 scheduler node/state topology remains unchanged.
3. **Stage 5C — GoalDirected:** implement the deterministic outer lifecycle and use the same operation assembly/executor for each bounded iteration.

Do not begin 5C by embedding agent construction in GoalDirected nodes. If 5A or 5B fails, repair the shared abstraction before proceeding.

### GoalDirected target graph shape

Implement stable nodes equivalent to:

```text
hydrate_goal_binding
reconcile_and_claim_iteration
construct_operation_runtime
execute_bound_agent
independent_verify
decide_goal_action
persist_revision_or_repair
snapshot_and_handoff
wait_for_human_authority
materialize_and_terminalize
fail_or_fallback_handoff
```

The outer graph owns lifecycle. Agent messages exist only in the bounded agent subgraph/session.

## 5. Deliverables

### 5.1 Stable capability compiler and operation assembly

Extend the Stage 1 structural compiler into the Stage 5 stable runtime compiler defined in `06A`. For every native, plain LangChain, or Deep Agent operation, compile an exact `OperationAssemblySpec` and `StageExecutionBinding` containing:

- implementation kind/ref and operation contract;
- exact model and authored technical fallback policy;
- prompt, ordered middleware, tools, reviewed skills, and predicted model-visible names;
- context assembly, filesystem/workspace, Store purpose, and sandbox policy;
- explicit synchronous child catalog and effective child grants;
- verifier, output, effect, trace/redaction, resource, fallback, and compatibility policies;
- complete capability manifest, maturity/readiness facts, and assembly digest.

The compiler must reproduce the same assembly and predicted surface from identical immutable inputs. It rejects mutable aliases, runtime model selection, installed-package authority, implicit tool/skill inheritance, duplicate core Deep Agents middleware, ambiguous filesystem/search tools, unreviewed external assets, missing verifier/output contracts, unsupported capability combinations, and resource requests outside authority.

Outbound MCP bindings may be represented and predicted here, but an implementation that requires a real outbound MCP adapter remains unavailable until Stage 6 unless the exact adapter has already passed its Stage 0 qualification and is deliberately pulled forward with recorded owner acceptance. Do not create a one-off MCP client in Stage 5.

### 5.2 Stable operation executor and StageGraph composition

Implement the Deep Agent adapter behind the Stage 3 `OperationExecutor` port and run the shared conformance suite. Then bind at least one StageGraph stage to this adapter and prove:

- Stage 4 topology, frontier computation, reservation, and settlement are unchanged;
- the stage receives only its exact model/prompt/tools/skills/context/children/workspace/verifier surface;
- another concurrently eligible native or differently assembled stage cannot see or inherit that surface;
- capability unavailability returns the shared typed failure and authored fallback/degradation behavior;
- the result manifest contains the complete lineage envelope and settles through the existing deterministic boundary.

Use barriers or controlled clocks where two stages are eligible to prove actual bounded overlap. This Stage 5B slice is smaller than the required heterogeneous Stage 6 proof and must not depend on async subagents or optional QuickJS/dynamic delegation.

### 5.3 GoalDirected state/reducers

Implement compact channels for:

- protected scope ref/digest;
- goal revision ref with parent lineage;
- iteration projection and stable iteration/agent-run identities;
- agent session ref and declared rollover;
- workspace/sandbox snapshot ref lineage;
- agent result and independent verification refs;
- conflict-detecting blocker set;
- deterministic no-progress projection;
- handoff ref;
- Stage 3 common channels.

No top-level `messages`. Agent-local messages use the standard message reducer only inside the operation subgraph.

### 5.4 Deterministic outer lifecycle

- hydrate exact goal/RunPlan/operation/verifier bindings;
- reconcile/claim one bounded iteration and reserve budgets;
- keep protected scope immutable except through accepted revision contract;
- create stable semantic identity for every iteration/agent/verifier attempt;
- execute one bounded operation harness;
- independently verify against exact evidence and evaluation contract;
- deterministically decide accept, revise/repair, rollover/handoff, human decision, bounded failure, or fallback;
- terminalize only when verifier action and BellLabs transition agree.

### 5.5 Operation harness factory

Construct `create_agent` or `create_deep_agent` from the exact binding:

- exact model selected for this new Workflow Implementation and authored fallback/retry policy; no legacy-model equality requirement;
- exact prompt definition/revision and permitted dynamic slots;
- exact tool/MCP allowlists and schema digests;
- exact skill refs/mounts;
- exact synchronous subagent definitions;
- delegation depth/count/concurrency/model/tool/data/network/budget ceilings;
- exact `ExecutionResourceEnvelope`, subordinate reservation plan, acquisition order, deadlines, and lease release behavior;
- filesystem backend/sandbox/workspace policy;
- session reuse/rollover mode;
- context policy and compaction/offload behavior;
- trace/redaction policy;
- structured output and independent verification contract.

The harness factory never resolves mutable aliases or widens compiled capabilities. Use native async tools and async middleware hooks.

The factory does not choose a model based on runtime preference, cheapest availability, or a model-authored request. Any adaptive model routing must itself be a published deterministic policy with a closed exact candidate set, authority/budget intersection, traceable decision inputs, and a resulting exact invocation binding.

### 5.6 Ordered middleware manifest

Implement and validate exact ordered middleware responsibilities:

1. `before_agent`: binding/scope/phase/budget/trace checks;
2. dynamic prompt: exact base plus typed permitted slots and rendered digests;
3. `before_model`: purpose-compatible retrieval, redaction, context budget, compaction/offload while preserving evidence refs;
4. model wrapper: exact model, timeout, technical retry/fallback, trace, usage;
5. `after_model`: structured output/tool-call/policy/budget/evidence validation;
6. tool wrapper: capability, approval, effect identity, timeout/retry/cancel, budget, trace, settlement;
7. `after_agent`: compact result persistence, usage settlement, snapshot/cleanup, durable events.

Store exact implementation/version/config digest, hook set, allowed state/context channels, failure policy, and redaction class for every middleware entry. Account for wrapper nesting and reverse after-hooks.

Deep Agents already provides core planning, filesystem, subagent, skills, and context behavior. Do not add duplicate summarization/filesystem/subagent middleware. Plain LangChain agents may select their own explicit context middleware.

### 5.7 Dynamic prompt and context assembly

Permitted dynamic context includes current objective/iteration, exact prompt segments, approved evidence/artifacts, budget projection, schema/workspace refs, allowed memory, verifier feedback, and bounded prior summary.

Forbidden context includes mutable aliases, raw secrets/tokens, unapproved cross-tenant memory, unbounded checkpoint history, and model-authored authority.

Persist:

- base prompt ref/digest;
- rendered-input manifest digest;
- rendered prompt digest;
- rendering implementation/version;
- source/trust/redaction classes.

### 5.8 First-class context policy implementation

Implement the Stage 1 context definition/spec:

- source trust/admission/quarantine;
- total and reserved token budgets;
- retrieval namespaces/filters/top-k/score/tie-breakers;
- immutable preservation set;
- compression trigger/middleware/target/generation/refresh/schema;
- mutation writers and expected versions;
- full transformation provenance;
- retention/privacy/deletion/tombstones;
- evaluation thresholds.

Never summarize or model-rewrite exact instructions, protected goal, authority/approval/budget/attempt facts, source locators/digests, citation edges, or final accepted evidence.

Reconstruction after compaction/rollover:

1. load exact prompt/policy/protected scope/current authoritative projection;
2. load accepted context manifest and verify source digests;
3. rehydrate bounded evidence/citation/contradiction projection;
4. retrieve only purpose-compatible non-authoritative Store items;
5. add bounded working summary/verifier feedback;
6. render and record assembly digest;
7. fail closed or request rollover when mandatory context cannot fit.

### 5.9 Filesystem, skills, and Store

- select one model-visible implementation for each filesystem/search capability;
- real shell/mutable deployed filesystem work goes to approved sandbox;
- path roots/mounts/read/write/delete/size/exclusion/sensitivity/promotion are exact;
- skills are exact reviewed immutable bundles with progressive disclosure;
- skill contents cannot grant tools/network/write/budget/delegation;
- each custom subagent gets explicit skill refs and compatible backend;
- Store namespaces include environment, tenant, agent profile, subject, and purpose;
- scientific-claim memory is off by default;
- procedural memory requires provenance, expiry, contradiction/retraction, and deletion;
- Store never authorizes, approves, proves, or terminalizes.

### 5.10 Stable synchronous subagents

Support both:

- dictionary `SubAgent` for a small standard-loop specialist;
- `CompiledSubAgent` for reusable typed topology/state/gates/repair.

Requirements:

- default general-purpose subagent disabled unless exactly selected;
- name/description/prompt/model/tools/middleware/approval/skills/output/permissions are exact;
- tool inheritance is not relied upon; compile explicit effective tool grants;
- skills are explicitly attached;
- each invocation is operation-local/fresh unless compiled graph policy says otherwise;
- parent blocks and receives schema-validated `SubagentResultManifest`;
- concurrent sync-child fan-out occurs only when the compiled policy permits it and aggregate child/model/tool/budget capacity is reserved first;
- controlled-clock/barrier tests measure actual overlap, maximum concurrency, cancellation, and release without starvation;
- child gets `ContextSlice`, not full parent messages/secrets/filesystem/Store/authority;
- child cannot terminalize parent;
- known Workflow Type, separate authority, substantial budget, durable independent wait, or reusable governed result forces linked Workflow Run.

### 5.11 Sandbox provider and workspace lifecycle

Implement LangSmith Sandbox behind the BellLabs provider port for the selected workflow:

- thread-scoped by default for workflow workspace;
- assistant scope only for immutable non-tenant-shared assets;
- exact image/runtime/package policy;
- default-deny/allowlisted egress where available;
- secrets proxy/ref handling with no ambient credentials;
- read-only inputs and governed write destinations;
- CPU/RAM/disk/time/command limits;
- upload/download/execute/reconnect;
- snapshot before declared rollover/handoff;
- external governed snapshot record/digest;
- idempotent cleanup/orphan reconciliation/usage settlement.

QuickJS is not implemented here as a substitute. Sandboxes own OS/files/shell/packages/browser isolation.

### 5.12 Independent verifier and terminal gate

Bind verifier separately from worker with exact model/prompt/tools/evidence/thresholds. It must:

- verify exact operation output/evidence refs;
- evaluate goal/acceptance contract and required citations;
- return typed action/reason/evidence;
- not share mutable session memory or hidden authority with the worker;
- drive the outer deterministic decision only through accepted contract;
- never directly terminalize.

### 5.13 Rollover, snapshots, and handoff

- trigger by accepted token/session/no-progress/workspace policy;
- snapshot workspace immutably with parent lineage;
- write a context manifest, not a summary-only handoff;
- create new session/thread scope as policy requires;
- reacquire secrets/MCP/sandbox resources;
- preserve protected scope/evidence/contradictions/approvals/budget identities;
- verify clone compatibility and source digests;
- resume through outer graph reconciliation.

Every rollover/handoff preserves the `ExecutionLineageEnvelope`, exact operation assembly, model/prompt/context digests, child invocation edges, and accepted evidence chain. Reacquiring current resources may report unavailability but may not silently replace a frozen capability.

## 6. Required tests

### Goal lifecycle

- protected goal/scope mutation denial;
- bounded accepted revision;
- stable iteration/agent/verifier identities;
- no-progress/repeated-blocker/convergence;
- budget exhaustion and continuation decisions;
- rollover/handoff/fallback;
- interrupt/resume node restart;
- verifier/terminality agreement.

### Harness/middleware

- deterministic stable compiler reproduction and predicted-versus-observed surface equality;
- per-stage `OperationAssemblySpec`/`StageExecutionBinding` completeness and digest validation;
- exact binding reproduction and mutable-alias denial;
- hook order/nesting and async implementation;
- duplicate middleware/tool collision rejection;
- model/tool retry accounting and cancellation;
- structured output/tool-call validation;
- provider effect claim/settlement;
- trace/redaction;
- non-legacy model selection is exact, evaluated, and traceable; no test requires provider/model equality with the legacy path;
- unavailable model/capability follows only the authored exact fallback/wait/degrade/fail path.

### Context/skills/memory

- repeated compaction and reconstruction thresholds from Stage 0;
- citation/evidence/contradiction/protected instruction preservation;
- prompt-injection quarantine;
- Store tenant/purpose/deletion/retraction tests;
- skill progressive loading and no authority escalation;
- explicit child skill binding;
- mandatory context overflow fails/rolls over safely.

### Subagents/sandbox

- dictionary and compiled construction compatibility;
- context/secret/filesystem/Store isolation;
- depth/count/concurrency/budget ceilings;
- barrier/controlled-clock proof of permitted synchronous-child overlap and aggregate capacity reservation;
- no deadlock/starvation and protected resumption capacity under maximum child load;
- structured return size/schema/provenance;
- linked-run classification;
- sandbox egress/secrets/limits/snapshot/restore/reconnect/cleanup/tenant tests;
- checkpoint size and recovery.

### Vertical-slice E2E

- Stage 5B StageGraph execution through the stable Deep Agent adapter with unchanged Stage 4 scheduler topology;
- concurrent native and agent-harness stages with distinct capability surfaces and no cross-stage inheritance;
- prepare/admit/execute/verify/revise/rollover/interrupt/result;
- accepted and rejected live/fixture cases against legacy baseline;
- crash at operation/verifier/snapshot/terminal boundaries;
- only BellLabs terminality succeeds;
- trace/eval-ready outputs and redaction.

### Adapter and lineage conformance

- stable Deep Agent adapter passes every shared `OperationExecutor` conformance case;
- every final StageGraph/GoalDirected result resolves semantic/runtime attempts, exact assembly/model/prompt/context, child invocations, effects, artifacts/evidence, usage settlements, and traces;
- crash before/after agent invocation, sync-child fan-out, result persistence, verifier, snapshot, and settlement preserves one semantic lineage with distinct technical attempts;
- tool/skill/model/sandbox drift never silently changes a resumed binding.

## 7. Gate

Stage 5 passes when:

- Stage 5A stable compiler predicts and reproduces the exact runtime surface;
- Stage 5B proves the reusable harness executes inside StageGraph through the Stage 3 port without scheduler changes or capability leakage;
- GoalDirected parity and accepted enhancements pass;
- only independent verifier plus BellLabs lifecycle can terminalize;
- context reconstruction preserves all mandatory invariants within accepted thresholds;
- exact middleware/harness/tool/skill/subagent surface is reproducible from binding digests;
- child isolation and linked-run classification pass;
- sandbox secrets/egress/limits/snapshot lineage/cleanup pass;
- session/workspace rollover survives checkpoint resume and process loss;
- top-level state/checkpoints remain compact and free of prohibited data;
- complete lineage queries pass for the StageGraph capability slice and GoalDirected vertical slice;
- QuickJS/dynamic/async capabilities remain disabled pending Stage 6; the outgoing handoff marks async implementation/qualification as required, not optional;
- outgoing handoff is accepted.

## 8. Explicit non-goals

- Do not let the Deep Agent own the outer lifecycle.
- Do not make the harness GoalDirected-specific or bypass the Stage 3 executor port.
- Do not enable dynamic interpreter subagents or async subagents.
- Do not attach unreviewed external MCP/skills.
- Do not use Store as scientific truth.
- Do not switch broad production traffic yet.

## 9. Outgoing handoff additions

Include:

- stable capability compiler version, operation assemblies, predicted/observed surfaces, and conflict results;
- Stage 5B StageGraph composition trace, scheduler-compatibility proof, and measured overlap/isolation evidence;
- GoalDirected topology/state/compatibility manifest;
- exact agent harness and middleware manifests;
- context policy/reconstruction evidence;
- filesystem/skills/Store capability surface;
- synchronous subagent catalog and construction choices;
- linked-run classifier evidence;
- sandbox provider/lifecycle/snapshot evidence;
- verifier separation and terminality proof;
- checkpoint/context size measurements;
- end-to-end lineage reports for StageGraph and GoalDirected;
- stable extension points and disabled flags for Stage 6, explicitly recording the required async track and optional QuickJS/dynamic track.
