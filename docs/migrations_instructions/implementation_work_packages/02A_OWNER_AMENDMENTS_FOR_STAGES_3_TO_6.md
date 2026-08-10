# Owner amendments for Stages 3–6 — accepted Temporal macro architecture

Recorded: 2026-08-08
Status: accepted owner architecture direction; implementation evidence remains required
Scope: work packages `06`–`09A`, with binding consequences for Stages 0–2 reconciliation and Stages 7–8
Supersedes: Agent Server-primary macro-runtime meaning in the earlier migration packages

## 1. Why this amendment exists

Earlier work packages targeted a standard Agent Server/LangGraph deployment as the primary macro runtime. Subsequent architecture review and the independently durable operation experiment established that BellLabs needs hours/days execution, durable human/external waits, independently scalable heterogeneous workers, incremental sibling completion, and one authoritative macro scheduler.

The accepted correction is:

> Temporal is the sole macro execution runtime. BellLabs application services and pure StageGraph/GoalDirected interpreters remain semantic authority. LangGraph and Deep Agents perform bounded cognition inside exact operations. LangSmith supplies tracing, evaluation, sandboxes, development/registration, and selected bounded deployments. The BellLabs API is the sole governed public facade.

This amendment preserves decision history. It does not pretend the earlier Agent Server-primary direction was never proposed or implemented.

## 2. Explicit supersession of earlier decisions

| Decision | Earlier accepted/proposed meaning | Accepted amended meaning |
|---|---|---|
| D-01 | Standard Agent Server is the primary runtime | **Superseded for macro execution.** Temporal is the sole production macro runtime. Agent Server/LangSmith deployments may host bounded operation implementations, qualification/development graphs, Studio/interactive surfaces, or governed facade internals only. |
| D-05 | One parent Agent Server thread per `(request_scope, belllabs_run_id, execution_epoch)` with linked child threads | **Superseded identity root.** A distinct `BellLabsRunWorkflow` is the runtime root; family workflows are children and generic `OperationWorkflow` instances are independently durable children. Agent threads/runs/checkpoints are subordinate operation lineage. Continue-As-New keeps the same run/epoch and increments a technical segment; a product fork creates a new run at epoch `1`. |
| D-07 | Managed Agent Server persistence is the primary production durability boundary | **Superseded for macro durability.** Self-hosted Temporal is the initial runtime. Stage 8 selects and proves the final AWS self-host topology. LangGraph/Deep Agents persistence remains bounded agent-session state and never BellLabs lifecycle authority. |

The superseded text remains historical evidence. Implementations, handoffs, tests, and diagrams must label which meaning they implement.

## 3. Amendments to decisions retained by identity

| Decision | Preserved intent | Required amendment |
|---|---|---|
| D-02 | Generic frontier-scheduler StageGraph first | Stage 4 implements the generic **Temporal-native family workflow** around the pure interpreter and generic operation child. The first vertical is small and heterogeneous. |
| D-03 | Generated graphs only after measured parity | Still valid for bounded operation cognition; generated Agent Server graphs cannot become an alternate production macro scheduler. |
| D-04 | Deterministic GoalDirected outer mechanics, bounded agent, independent verifier | Stage 5 implements GoalDirected as a Temporal family child applying the pure interpreter. Deep Agents and verifier executions are exact subordinate operations. |
| D-06 | Shared router factories and coexistence | Routers live behind the modular BellLabs API. Coexistence cannot expose Agent Server or Temporal as an alternate governed public facade. |
| D-08 | Authoritative runtime bindings and attempt history | Extend bindings for Temporal root/family/operation IDs, epoch/segment, worker class, queue/profile, and subordinate agent/provider IDs. |
| D-09 | Typed interventions; privileged audited repair | Add authoritative inbox/ledger/outbox, disruptive-saga qualification, local/remote post-model/pre-tool injection certification, and explicit unsupported behavior before qualification. |
| D-10 | Compact top-level state; bounded messages | Temporal state/history and graph state remain compact. Product durable events and authoritative communication records live in BellLabs stores. |
| D-11 | Async, introspection-safe graph assembly | Applies only to bounded operation graphs. Temporal workflow registration and replay must also be deterministic and import-safe. |
| D-12 | Async I/O; synchronous pure domain logic | Retained. Temporal workflow code is deterministic; external I/O occurs through activities/application ports. |
| D-13 | PostgreSQL operation claims/attempts/settlements | Retained and extended with authoritative per-attempt inbox/ledger/outbox and product event projection. |
| D-14 | First-class context policy and immutable assembly | Retained for local and remote operation adapters; context cannot widen authority. |
| D-15 | Delegation modes remain distinct | Built-in synchronous subagents are operation-local; independent lifecycle uses custom Temporal delegation; provider async is a subordinate adapter; linked runs remain governed BellLabs runs. |
| D-16 | Canonical vocabulary and provider-qualified identities | Extend grammar for Temporal workflow/run/activity IDs, execution segment, worker class, remote binding, callback/poll facts, and product fork lineage. |

## 4. Preserved D-17–D-23 decisions

These owner decisions remain accepted, with “scheduler” and “runtime” interpreted through the Temporal correction:

| ID | Accepted direction | Temporal-aligned implementation consequence |
|---|---|---|
| D-17 | New Workflow Implementations are free to select different models/providers from the legacy implementation. | Models, prompts, tools, specialists, verifiers, adapters, and remote/local placement are exact versioned choices. Parity compares BellLabs contracts, semantics, authority, evidence, results, budgets, failures, and accepted quality thresholds. |
| D-18 | StageGraph stages encode granular execution requirements for heterogeneous workflows. | Every executable stage/variant has immutable `StageCapabilityRequirement`, `StageExecutionBinding`, and `OperationAssemblySpec`. No Temporal workflow, graph node, or model hard-codes or discovers an undeclared surface. |
| D-19 | Scheduling semantics and capability mechanics remain separate. | Pure interpreters decide readiness/convergence; Temporal supplies durable execution; operation adapters supply cognition/provider mechanics. |
| D-20 | Parallelism is hierarchical and explicit; speculation is separately governed. | Envelopes cover run, family, operation, model/tool/MCP/sync-child/Temporal-child/provider/sandbox capacity. Speculation is default-off, pure/read-only, quarantined, and commit-barriered. |
| D-21 | Capability compilation precedes consumers. | Structural compilation exists before Stage 3 execution; Stage 5 compiles the stable Deep Agents surface; Stage 6 extends remote/provider/advanced surfaces. |
| D-22 | One reusable Deep Agents harness serves StageGraph and GoalDirected. | The harness is a bounded operation adapter invoked by generic `OperationWorkflow`, not a private family scheduler. |
| D-23 | End-to-end lineage and heterogeneous composition need direct evidence. | Stages 3–6 share one canonical envelope; `09A` remains required and final-result lineage crosses Temporal, agent, tool/MCP, sandbox/provider, evidence, effect, settlement, and trace identities. |

## 5. New accepted decisions

| ID | Accepted direction | Implementation consequence |
|---|---|---|
| D-24 | Temporal is the sole macro runtime; `BellLabsRunWorkflow` is a distinct root with family children and generic `OperationWorkflow` children. | No Agent Server macro fallback. Root, family, operation input/result, parent-close, cancellation, replay, repair, and compatibility contracts are explicit. |
| D-25 | BellLabs API is the sole governed public facade. | All public commands, queries, streams, interventions, and results pass common application auth/tenant/policy ports. Provider, Temporal, Agent Server, and sandbox endpoints are subordinate/private. |
| D-26 | BellLabs owns authoritative per-attempt inbox, ledger, and outbox. | Signals, Updates, callbacks, polling, and agent messages carry typed deduplicated facts; handlers do not directly grant lifecycle or settlement transitions. |
| D-27 | Exact post-model/pre-tool communication injection is required and separately certified by placement. | Stage 3 defines the contract and certifies authoritative transport/durable waits. Stage 4 certifies its first local adapter, Stage 5 completes reusable local qualification, and Stage 6 certifies selected remote LangSmith deployment execution. Uncertified placement reports unsupported capability. |
| D-28 | Disruptive intervention is a governed saga. | Authorize/journal, quiesce or cancel, reconcile ambiguous effects, apply typed mutation, rebind/resume, and emit durable outcome facts with compensation/recovery evidence. |
| D-29 | Peer/subordinate communication is typed input, never readiness by itself. | No message affects StageGraph readiness or GoalDirected convergence until resulting evidence is accepted, settled, projected, and consumed by the pure interpreter. |
| D-30 | Delegation is classified by lifecycle. | Built-in sync subagents are operation-local. Independent lifecycle/cancellation/capacity/settlement uses custom Temporal delegation. Provider async is a subordinate adapter. |
| D-31 | Remote operation lifecycle is start-bind-wait/reconcile. | Persist remote IDs before waiting; reconcile ambiguous starts/completions; asynchronous callback completion is optional and converges with polling through the same journal/settlement path. |
| D-32 | Continue-As-New and product fork have distinct continuity. | Continue-As-New: same BellLabs run, same execution epoch, new technical segment. Fork: new BellLabs run, epoch `1`, immutable parent/snapshot lineage. Temporal Reset is operational repair only. |
| D-33 | Product durable events are authoritative. | Clients and downstream systems consume BellLabs outbox/projections. Temporal history/Queries and LangSmith traces are diagnostic/runtime evidence, not the product event stream. |
| D-34 | Initial and final deployment decisions are separated. | Implement and qualify against self-hosted Temporal initially. Stage 8 selects the final AWS self-host topology and proves it without changing semantic contracts. |
| D-35 | Five logical worker-pool classes are required. | Coordinator/family, agent/cognitive, ingestion/I/O, sandbox-control/external-job, and verification/reconciliation classes have isolated queues/capacity. Stage 8 chooses exact AWS services/counts/sizes. |
| D-36 | Q/D reference workflows are mandatory cumulative verticals. | Every stage executes the applicable immutable increment from `00A`; infrastructure is extracted from running verticals rather than accepted as a disconnected horizontal layer. |
| D-37 | Deterministic and live evidence are separate. | Sanitized fixtures gate replay/compatibility/CI; bounded live canaries prove current source/provider integration without treating changing web results as fixed regression oracles. |

## 6. Stage/package amendments

### Stage 3

Stage 3 is split into four coordinated packages:

- `06` becomes the overview and root/family/operation, message, continuity, intervention, and recovery contract owner.
- `06A` remains the cross-stage exact operation assembly, concurrency, lineage, journal, effect, and settlement contract.
- `06B_STAGE_3_TEMPORAL_WORKFLOW_FOUNDATION.md` implements self-host Temporal, the distinct root, family children, generic operation child, replay/recovery, Continue-As-New, and five logical worker classes.
- `06C_STAGE_3_COMMUNICATION_AND_INTERVENTION_QUALIFICATION.md` qualifies inbox/ledger/outbox,
  typed command transport, durable waits, dedupe, stale-target behavior, and
  settlement-before-readiness. It defines later model-visible/disruptive contracts but does not
  claim them before a real local adapter exists.

Stage 3 cannot hand off until all four package gates pass and durable skeleton increments of both Q
and D execute through the same application, persistence, workflow, Activity, and worker seams that
Stage 4 will extend.

### Stage 4

Implement a Temporal-native `StageGraphWorkflow` around the pure `StageGraphInterpreter`. Launch generic operation child workflows independently and process completion facts incrementally. Prove `all`, `any`, and `minimum(k)`, slow-sibling policy, bounded capacity, cycles/waits/reuse, deterministic settlement, and complete lineage.

The first vertical is Q as a small heterogeneous StageGraph with materially different exact
assemblies and the first local post-model/pre-tool steering proof. A deterministic D compatibility
slice remains green. Direct frontier `asyncio.gather()` and a production Agent Server StageGraph
are rejected.

### Stage 5

Implement the reusable bounded Deep Agents operation harness and `GoalDirectedWorkflow`. Goal iterations, independent verification, revision, subgoals, pause/cancel, context rollover, and convergence remain pure-interpreter semantics coordinated by Temporal.

The second vertical is D GoalDirected ownership research. It must reuse and complete the same
operation adapter path already available to StageGraph, qualify tool HITL and disruptive restart,
and rerun Q with the completed harness.

### Stage 6

Qualify advanced capabilities, selected remote LangSmith deployments, sandboxes, MCP/skills,
provider-async adapters, optional interpreter features, and heterogeneous implementations of both
Q and D. Remote execution must use start-bind-wait/reconcile and independently pass exact
post-model/pre-tool injection.

Stage 6 includes mandatory hours-long failure tests covering worker loss, remote ambiguity, provider failure, callback/poll races, cancellation, reconciliation, capacity, lineage, effects, and no duplicate settlement.

### Stage 7

Deliver the modular BellLabs API/control facade, coordinator path, authoritative durable product events, tracing/evaluation, authorization, tenant isolation, redaction, security, SLOs, and negative provider-bypass proof.

### Stage 8

Select and document the final AWS self-host Temporal/service/worker topology. Deploy five logical worker classes, prove autoscaling/capacity and N/N+1 replay/versioning, repeat hours-long failure tests in topology, shadow/canary, rollback, cut over, and drain or repurpose superseded paths.

## 7. Earlier-stage compatibility and unfinished Agent Server work

Before Stage 3 implementation:

1. inspect actual Stage 1 contracts, RunPlan compiler, schemas, persistence, migrations, tests, and evidence;
2. version or extend contracts without mutating published digests;
3. map Agent Server-primary identities and persistence assumptions to root/family/operation Temporal identities and subordinate agent lineage;
4. inventory every unfinished Agent Server macro graph and choose a bounded-operation, qualification/development, visualization/facade, or removal disposition;
5. preserve still-valid operation harness, auth, tracing, sandbox, and experiment evidence;
6. publish a focused compatibility handoff with exact paths and supersession labels.

Do not finish an unfinished Agent Server macro graph as a production fallback. Do not discard valid bounded-operation work merely because its earlier macro role was superseded.

## 8. Required acceptance evidence

- schemas and pure compilation tests for root/family/operation contracts and every per-stage requirement/binding/assembly;
- replay-safe self-host Temporal workflows and N/N+1 history fixtures;
- crash/restart proof before and after consequential boundaries;
- independently durable operation start and incremental completion proof;
- Stage 3 transport/durable-wait proof, Stage 4 first-local, Stage 5 reusable/disruptive-local, and
  Stage 6 remote post-model/pre-tool certification;
- inbox/ledger/outbox dedupe and disruptive-saga failure/compensation evidence;
- proof that typed peer input cannot alter readiness before settlement;
- Continue-As-New same-run/same-epoch/new-segment proof and fork new-run/epoch-1 proof;
- one stable Deep Agents adapter reused by both families;
- multiple heterogeneous operations running concurrently under one StageGraph;
- actual overlap, ceilings, backpressure, deadlock, cancellation, and recovery measurements;
- remote start-bind-wait/reconcile evidence with polling and optional async completion;
- final-result lineage through operation, agent, tool/MCP, provider/sandbox, artifact, evidence, effect, settlement, product event, and trace;
- Stage 6 and Stage 8 hours-long injected-failure evidence;
- BellLabs API facade and direct-provider/Temporal/Agent Server bypass negative tests;
- owner/gate-review disposition in every outgoing package handoff.

## 9. Non-negotiable preserved authority

Nothing in these amendments weakens:

- BellLabs exact compilation, admission, lifecycle, budgets, approvals, effect claims, evidence acceptance, settlement, and terminality;
- pure StageGraph readiness and GoalDirected convergence semantics;
- exact frozen capability and deployment bindings;
- stable claims and exactly-once settlement identities over at-least-once execution;
- tenant isolation, redaction, secret-reference handling, and no-PHI/no-secret artifacts;
- the requirement that optional/preview capabilities stay disabled until their own gates pass;
- `biotech-meta` remaining read-only absent explicit authorization.
