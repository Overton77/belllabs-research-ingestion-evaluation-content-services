# Mission Control architecture interview handoff

> **Historical input — superseded as architecture authority (2026-10-02).** Use the [canonical specification](general-mission-control/SPECIFICATION.md), [new system proposal](general-mission-control/MISSION_CONTROL_SYSTEM_PROPOSAL.md), and [cleanup plan](general-mission-control/DOCUMENTATION-CLEANUP.md). The retained text below records earlier work; conflicting runtime, storage, ownership or sequencing instructions do not govern new implementation.

**Status:** architecture interview closed; feature-specification phase open  
**Handoff date:** 2026-09-07  
**Next task:** write the M1 feature specifications (`F1.1` → `F1.2` → `F1.3`) per [`M0_M8_PROGRAM.md §3`](./M0_M8_PROGRAM.md), confirm each with the user, then cut M1 issues

## 1. Read this first

The architecture interview (Rounds 1–4) is complete and synthesized. Authority, in order:

1. [`../../CONTEXT.md`](../../CONTEXT.md) — ubiquitous language
2. [`MISSION_CONTROL_ARCHITECTURE.md`](./MISSION_CONTROL_ARCHITECTURE.md) — integrated architecture (v2)
3. [`workflow-types/index.md`](./workflow-types/index.md) — behavior and contracts, documents `00`–`09`
4. [`M0_M8_PROGRAM.md`](./M0_M8_PROGRAM.md) — milestones, exit proofs, feature-specification queue
5. [`../../adr/`](../../adr/) — ADRs 0001–0005
6. [`runtime-facts/`](./runtime-facts/) — verified Cursor SDK and Eve behavior

[`MISSION_CONTROL_SPEC.md`](./MISSION_CONTROL_SPEC.md) and [`MISSION_CONTROL_IMPLEMENTATION_SEQUENCE.md`](./MISSION_CONTROL_IMPLEMENTATION_SEQUENCE.md) carry supersession headers; use them only for the later-phase material those headers list.

Sections 2–14 below are retained as the interview record. Section 15 is the live queue.

Read the skills before writing feature specifications:

- `ai-engineer-meta/.agents/skills/grilling/SKILL.md`
- `ai-engineer-meta/.agents/skills/domain-modeling/SKILL.md`
- `ai-engineer-meta/.agents/skills/to-spec/SKILL.md`

## 2. User-directed implementation thesis

The immediate architectural target is M0–M8, not the entire eventual platform.

M0–M8 should produce:

1. authored, validated, immutable Mission contracts;
2. the compiler, revision ledger, Mission Runs, and durable kernel;
3. the four workflow systems and their complete lifecycles;
4. enough real agent capability to execute those systems with high fidelity;
5. Compaction, Continuation Checkpoints, and fresh-session Transfer where long-running agent work requires them;
6. Cursor SDK and Eve agent investigation/adapters sufficient to observe their real lifecycle and event behavior;
7. normalized events that can be streamed into the dashboard;
8. API and dashboard visibility, control, monitoring, intervention, and revision behavior; and
9. runnable lifecycle proofs at every milestone.

This changes the emphasis of the current implementation sequence:

- Minimum viable Compaction and Transfer can no longer remain entirely deferred to M12; Goal Loop fidelity requires it in the M0–M8 program.
- A minimal Eve lifecycle/event lane must be considered before or within M8 rather than leaving all Eve work to M9.
- Advanced capability discovery, broad materialization, rich memory, full knowledge integration, packaging, and platform distribution remain later.

Do not mechanically rewrite the sequence yet. First finish the relevant architecture branches, identify exact dependencies, and then revise M0–M8 with explicit exit proofs.

## 3. Later phases

After M0–M8:

1. add advanced capability search, admission, profiles, workspaces, sandboxes, hooks, memory, and model/runtime choice;
2. integrate Knowledge Services retrieval and vector-store management;
3. integrate the verification module and adjudication authority;
4. specify artifact admission, promotion, publication, and richer Mission-state preservation;
5. add remaining harnesses and production infrastructure; and
6. package Mission Control as releasable services, MCP/MCP Apps, CLI, Agent Skill/plugins, A2A, and a service callable by external agents.

Knowledge Services remains the owner of verification, retrieval, embeddings, vector-store management, and promotion mechanics. Mission Control invokes typed capabilities and links authoritative outcomes by immutable IDs; it does not absorb those algorithms.

## 4. Interview method

Continue using a decision tree in rounds:

1. Ask the whole currently unblocked frontier.
2. Number every question and provide a recommended answer.
3. Investigate factual code/vendor questions rather than asking the user to retrieve facts.
4. Wait for the user’s decisions.
5. Update `CONTEXT.md` immediately when language settles.
6. Update the relevant interview-draft specification immediately when behavior settles.
7. Keep unresolved matters explicitly listed.
8. Do not turn these drafts into implementation issues until the relevant feature specification is complete and its testing seams are confirmed.
9. **Existing schema and code are inputs, not constraints.** Nothing of the official Mission Control system has been implemented; only retrieval, vector-store management, one form of artifact promotion, and the in-progress verification module exist. Design the right system. Expect the M1 migration to overhaul `orchestration.mission`, artifact tables, and related Knowledge Services tables where they conflict with these specifications. Do not bend a specification to fit a current enum. (User-directed 2026-09-07.)

The user expects system-wide questions where necessary because this is a system of systems, but also expects focused architecture and feature specifications for M0–M8.

## 5. Accepted domain model

### Mission and goals

- A Mission is a durable, policy-bounded pursuit of one or more explicit Goals.
- Each Goal may contain an inspectable Objective tree. The Objective tree is not executable.
- Program Nodes perform work toward Objectives.
- Goals and Objectives may be instantiated from versioned reusable patterns.
- Execution completion, Goal acceptance, Mission acceptance, downstream admission, and publication are distinct.

### Authored intent and execution

- Authored intent is created, validated, stored, compiled, and only then executed.
- `MissionDefinition` is authored intent.
- `CompiledProgram` is the deterministic executable representation.
- A Mission owns a general recursive `program`, not a mandatory top-level graph.
- Every Program Node has exactly one behavior: workflow system, executor, durable control, or Child Mission Invocation.

### Revisions and Runs

- The Mission aggregate evolves; committed Revisions and execution history do not.
- Every submitted Revision snapshot is immutable.
- Authored edits and runtime graph changes use one Revision Proposal pipeline.
- A Mission has one active Scheduling Head; old in-flight work remains pinned to its starting Revision.
- Capability expansion, structural changes, and meaningful Objective changes require a Revision.
- Material Goal, tenant, authority, or success-meaning changes require a successor or forked Mission.
- Carry-Forward reuses compatible prior work through explicit lineage.
- Revert creates and validates a new Revision derived from old content; it never rewrites history.
- A Mission may have multiple Mission Runs. One Run executes one Revision against one immutable input snapshot.

### Mission composition and invocation

- Mission Composition binds Goals, program, inputs, policies, budgets, and proof.
- Mission Graph relates independently governed Missions pursuing a larger outcome.
- Mission Chain is one ordered path through that graph.
- Mission Invocation is caller-neutral across dashboard, API, CLI, MCP, agents, and Program Nodes.
- Spawn creates a new Mission identity; attachment links an existing Mission.
- Adoption as Child, dependency linkage, and use of outputs must not silently change ownership or provenance.

## 6. Accepted execution taxonomy

Do not use the old seven-member `NodeSpec.strategy` union as the target model.

### Workflow systems

- Stage Graph
- Goal Loop
- Parallel Swarm
- Evaluator Optimizer

Each is a composite control structure with its own child work, lifecycle, governors, completion semantics, and exit proofs.

### Executors

- Agent Executor
- Deterministic Executor

### Durable controls

- Event Wait
- Timer
- Human Gate
- Proof Gate

### Mission-boundary operation

- Child Mission Invocation

### Cross-cutting policies

Retry, continuation, Compaction, Session Policy, budgets, concurrency, failure propagation, capabilities, workspace/sandbox, proof, observability, and intervention are not workflow systems.

## 7. Accepted recursive-composition law

- Workflow systems may explicitly contain other workflow systems.
- A Stage may contain a Goal Loop; it does not become a Stage with miscellaneous “loop flags.”
- Stage Graph stages may contain any workflow system or executor.
- Goal Loop phases may contain subprograms.
- Parallel Swarm workers and convergence may be subprograms.
- Evaluator Optimizer producer and evaluator arms may be subprograms.
- Same-type nesting requires a distinct control scope and explicit governors.
- Nested systems pursue existing Objectives; independently governed Goals require another Mission.
- Every composite boundary has typed inputs, Output Projection, Objective references, budget scope, and its own execution identity.

## 8. Stage Graph decisions

The current accepted Stage Graph specification includes:

- separate Dependency Edges, Data Bindings, Gate Conditions, Output Projections, and historical lineage;
- deterministic typed release expressions;
- lifecycle `pending → ready → running ⇄ waiting → completed | cancelled`;
- separate terminal outcomes;
- static unreachable-work rejection and runtime Stalemate;
- parallel release subject to real worker/runtime/resource capacity;
- independent branches continuing unless `fail_fast` is explicit;
- Revisit Regions for known cyclic topology;
- hard region and per-Stage caps, deterministic termination, progress observation, no-progress limits, time, and budget governors;
- declared bounded dynamic fan-out that creates activations without mutating the committed program;
- Stage Completion Contracts;
- explicit separation of output production, output validity, verification, Stage Acceptance, Objective contribution, and Goal acceptance;
- normal bindings waiting for producer acceptance;
- Provisional Bindings for explicit speculative work only;
- Retry for infrastructure failure and visible Remediation for quality rejection; and
- Continuation preserving lineage without erasing Revisit identities.

Remaining Stage Graph work includes dependency-expression schema, conditional branching, graph-level acceptance/proof aggregation, and Revision activation through nested running systems.

## 9. Goal Loop decisions

Goal Loop is adaptive: the next useful action cannot be fully compiled in advance.

It has two orthogonal control systems:

1. goal progress: `observe → propose action → authorize → act → evaluate → review completion → decide`;
2. context health: `monitor → compact → synthesize → validate → transfer → resume`.

Accepted concepts:

- artifact-backed append-only Loop Journal, indexed and ordered by the Domain Ledger;
- bounded typed Loop State;
- versioned Loop Operating Contract materialized into the agent environment;
- committed Action Space;
- attributable Loop Controller distinct from executor and acceptance authority;
- typed Progress Reviews at meaningful actions, Iteration boundaries, major spend, pre-Compaction, rejection, and stagnation;
- Completion Candidate mapped to criteria and evidence;
- ordered deterministic, semantic, policy, and human checks;
- quality rejection becoming next-loop evidence rather than infrastructure failure;
- finite no-progress patience;
- explicit termination outcomes;
- output projection only after the Completion Contract accepts outputs; and
- no storage of hidden chain-of-thought in the Loop Journal.

One Session may execute several Iterations. One Iteration may cross Sessions through intra-activation Continuation.

**Resolved 2026-09-07 (Round 1):** Journal Entry / Segment / Digest model, Action Proposal and Action Authorization with Side-Effect Classes, shared Completion Contract, Context Health Policy as pinned catalog versions, same-Iteration Human Task resumption, and subset-only nested Action Spaces. See `02-GOAL_LOOP.md §4.1, §6.1–6.2, §9.1, §11.1, §13–14`.

## 9a. Parallel Swarm and Evaluator Optimizer decisions (Round 1, 2026-09-07)

Both systems are specified in `03-PARALLEL_SWARM.md` and `04-EVALUATOR_OPTIMIZER.md`. Highlights: Member Completion Policy is separate from Convergence; only accepted members count; `on_resolution` handles surplus members; feasibility rule `a + r ≥ k`; Member Variant is identity. Evaluator Optimizer: Mission Control computes acceptance from typed Evaluation Reports; fresh producer activation per round; `evaluator_independence` default `session`; `accept_best_at_cap` default false. Shared execution-state vocabulary (lifecycle / phase / terminal outcome / cycle decision) is accepted in `00 §6`. Names "Swarm Member" and "Convergence" are provisional pending the user's decision.

## 10. Continuation, Compaction, and Transfer decisions

Agent Sessions are disposable. Canonical logical state lives outside the Session.

Primary path:

`safe boundary → freeze actions → snapshot state/workspace → deterministic reduction → semantic synthesis → validate → seal checkpoint → provision fresh Session → hydrate → continuity check → resume`

Accepted laws:

- `compact_and_transfer` is the default long-running strategy;
- Context Health Policy is versioned by model, runtime, tool profile, and task class;
- soft thresholds schedule orderly Compaction;
- hard thresholds prohibit further agent work until transfer resolves;
- Continuation Checkpoint is typed, attributable, digest-bound, and validated;
- invalid checkpoints fail closed while the source execution remains parked;
- deterministic reducer and semantic compacting agent have separate roles;
- producer/verifier/evaluator independence survives transfer;
- complete transcripts are not replayed by default;
- pending Commands wait until target hydration is confirmed;
- intra-activation Continuation preserves logical execution identity;
- inter-activation handoff creates a new activation identity;
- checkpoint is not long-term memory;
- cache preservation is subordinate to fresh-session isolation; and
- transfer count, failure, budget, time, and no-progress governors are enforced.

Remaining work includes exact checkpoint schema compatibility, atomic workspace/sandbox snapshot behavior, queued-command ordering, risk thresholds for semantic validation, and reconciliation with provider-native Compaction.

## 11. Cursor SDK and Eve investigation required

The next architecture session must inspect current Cursor SDK and Eve behavior before freezing adapter and event contracts.

**Done 2026-09-07:** fact sheets exist at [`runtime-facts/CURSOR_SDK_FACTS.md`](./runtime-facts/CURSOR_SDK_FACTS.md) (`@cursor/sdk@1.0.31`) and [`runtime-facts/EVE_FACTS.md`](./runtime-facts/EVE_FACTS.md) (`eve@0.52.2`), each ending with a confirmed/refuted/unverified table against `MISSION_CONTROL_SPEC.md §10`. Items still marked UNVERIFIED there (Cursor local headless hook matrix, cloud stream retention seconds, Cursor and Eve concurrency/rate limits, Eve's use of `WorkflowAgent`) must be resolved by a live probe during M5, not assumed. The adapter/event round (frontier item 7) should read these sheets, not the §10 table.

For each runtime determine:

- run/session/turn identity hierarchy;
- start, resume, fork, follow-up, steer, interrupt, pause, cancel, and completion semantics;
- event stream, webhook, polling, and recovery behavior;
- token/context visibility and Compaction signals;
- workspace, branch, filesystem, and sandbox identity;
- tool, command, subagent, and hook events;
- delivery guarantees and unsupported controls;
- crash/reconnect behavior;
- usage/cost fields;
- raw native event retention policy; and
- the normalized Mission Event mapping.

Do not let provider-native status become canonical Mission state directly. Adapters report observations; Mission Control transition rules update the Domain Ledger.

## 12. M0–M8 specification program

**Superseded 2026-09-07 by [`M0_M8_PROGRAM.md`](./M0_M8_PROGRAM.md)**, which carries deliverables, exit proofs, dependencies, and the feature-specification list per milestone. The bullets below are the pre-synthesis intent and are kept for the record.

### M0 — document and decision lock

Reconcile the candidate parent spec with the accepted glossary and workflow suite. Decide which trade-offs actually warrant ADRs. Do not create ADRs mechanically.

### M1 — contracts and schema

Specify the new recursive Program Node model, Goal/Objectives, Mission Revision, Mission Run, system executions, Loop Journal Artifacts, Context Health, Completion Contracts, commands, events, and execution dimensions. Shared schema belongs only in `ai-engineer-db-contract`.

### M2 — compiler and revision ledger

Specify deterministic validation/compilation, canonicalization/digests, recursive program compilation, admission, Revision Proposal flow, Scheduling Head, Carry-Forward, and transition impact analysis.

### M3 — API, reads, event plane

Specify authoring, validation, commit, Run, Revision, graph/program, traceability, peek, command, Artifact, Journal, event replay, and stream contracts.

### M4 — durable kernel and Stage Graph

Specify Temporal workflow boundaries, Stage release, nested system execution, deterministic executors, gates, Revisit Regions, retries, receipts, worker recovery, and lifecycle proof.

### M5 — high-fidelity simple agent harnesses

Specify and fact-check the shared Agent Harness against Cursor SDK and the minimum Eve lane. Capture real native events and honest delivery semantics. Include Context Health observation and the minimal fresh-session Transfer path needed by M6.

### M6 — workflow-system lifecycles

Specify and prove Stage Graph, Goal Loop, Parallel Swarm, and Evaluator Optimizer on the simple high-fidelity harness. Include only the agent capabilities required for correct lifecycle behavior.

### M7 — control plane

Specify API/MCP/Agent Skill/dashboard monitoring, normalized event streaming, topology, inspect/peek, output rail, Human Tasks, budgets, and intervention across live M6 systems.

### M8 — evolution and Mission graphs

Specify Revision activation, reconciliation, Event Waits, Child Mission Invocation, Mission Graph portals, agent-proposed evolution, and full lifecycle control proofs.

## 13. Explicitly defer beyond M8

Unless an M0–M8 lifecycle cannot be proved without a narrow slice, defer:

- broad semantic capability search and external candidate ingestion;
- full multi-runtime capability materialization;
- advanced sandbox fleet and wallet/credential system;
- general long-term memory platform;
- full retrieval/vector-store integration;
- full verification and adjudication integration;
- complete artifact promotion/publication system;
- broad research/curriculum verticals;
- all remaining harness providers;
- final plugin marketplace packaging;
- full A2A distribution and production deployment.

Do not defer the contracts needed to preserve clean future boundaries.

## 14. Known parent-document conflicts

**Resolved 2026-09-07.** Every conflict below is mapped to its resolution in `MISSION_CONTROL_ARCHITECTURE.md §20`, and both candidate documents carry supersession headers. The list is kept for the record:

- singular `goal` rather than one or more Goals with Objective trees;
- one overloaded seven-member `strategy` union;
- `MissionDefinition.graph` instead of a general recursive `program`;
- `DETERMINISTIC`, `EVENT_TRIGGER`, and `MISSION_SPAWN` treated too much like workflow-system peers;
- no first-class Mission Run;
- insufficient separation of lifecycle, execution outcome, acceptance, admission, and publication;
- Compaction/Continuation implementation deferred to M12;
- Eve adapter deferred to M9;
- old session-reuse assumptions;
- `MissionEdge` mixing scheduling, dataflow, evaluation, and lineage;
- mutation language that can imply in-place changes.

Do not patch these piecemeal during the remaining interview. Once the M0–M8 branches are sufficiently settled, perform one coherent parent-spec and sequence synthesis with dated amendments and a consistency pass.

## 15. Feature-specification queue (live)

Interview frontier items 1–10 are complete. The remaining path from documents to issues is the feature-specification queue in `M0_M8_PROGRAM.md §3`, written in milestone order under `specs/mission-control/features/`:

| Milestone | Feature specifications | Status |
|---|---|---|
| M1 | `F1.1 Mission contracts` · `F1.2 mission_control schema` · `F1.3 Exemplars` | next |
| M2 | `F2.1 Validation and compilation` · `F2.2 Revision ledger` · `F2.3 missionctl authoring` | queued |
| M3 | `F3.1 Authoring and Run API` · `F3.2 Event plane and streams` · `F3.3 Command and Human Task ledgers` · `F3.4 missionctl reads` | queued |
| M4 | `F4.1 Temporal kernel` · `F4.2 Stage Graph substrate` · `F4.3 Deterministic Executors` · `F4.4 Durable Controls (simple)` · `F4.5 Command delivery (deterministic scope)` | queued |
| M5 | `F5.1 AgentHarness contract` · `F5.2 Cursor cloud adapter` · `F5.3 Eve adapter and mission-eve-agent` · `F5.4 direct_model adapter` · `F5.5 Minimal Compaction and Transfer` · `F5.6 Agent Stages` | queued |
| M6 | `F6.1 Stage Graph completion` · `F6.2 Evaluator Optimizer` · `F6.3 Parallel Swarm` · `F6.4 Goal Loop` · `F6.5 Semantic Compaction and Context Health` · `F6.6 Remaining Human Task kinds` | queued |
| M7 | `F7.1 Dashboard Live Mission` · `F7.2 Human Task inbox and budgets` · `F7.3 Commands on agent lanes` · `F7.4 MCP server and Skill v1` · `F7.5 missionctl intervention` | queued |
| M8 | `F8.1 Runtime Revision activation` · `F8.2 Agent-proposed evolution` · `F8.3 Event Wait rearm` · `F8.4 Child Mission Invocation and Mission Graph` | queued |

Rules for each feature specification:

1. Use the `to-spec` skill template. Cite the defining suite sections; do not restate behavior.
2. Name every state vocabulary it touches and where it is defined.
3. List testing seams and the exit proofs from the program it satisfies.
4. Grill only genuinely open implementation questions (library choices, file layout, fixture shape); the domain is settled.
5. Confirm with the user before cutting issues. Issues are slices of one feature specification, each with its proof.

Where a feature specification exposes a domain gap, amend the suite document and `CONTEXT.md` with a dated note first, then continue.

## 16. Session log

- **2026-09-07 (session 2).** Resumed at frontier items 1–3. Opened `workflow-types/03-PARALLEL_SWARM.md` and `04-EVALUATOR_OPTIMIZER.md` as skeletons (inherited laws + questions only). Issued interview Round 1 covering Goal Loop remaining contracts, cross-system shared vocabulary, Parallel Swarm, and Evaluator Optimizer. Dispatched the Cursor SDK / Eve fact investigation (frontier item 6); results land in `runtime-facts/CURSOR_SDK_FACTS.md` and `runtime-facts/EVE_FACTS.md`. Executors/Durable Controls (item 4) and Mission Invocation (item 5) are queued for Round 2 because several of their questions depend on Round 1 answers (convergence evaluators, member isolation, Human Task resumption).
- **2026-09-07 (session 2, Round 1 accepted).** All 28 Round 1 recommendations accepted; Q10 names held provisional. `CONTEXT.md` gained Completion Contract, Journal Entry/Segment/Digest, Action Proposal/Authorization, Side-Effect Class, Human Task, Parallel Swarm and Evaluator Optimizer terms, and the execution-state vocabulary. `00 §6` records the shared vocabularies; `02`, `03`, `04` updated; `01 §6` flags the Stage outcome reconciliation. Round 2 issued: vocabulary reconciliation, Executors, Durable Controls, Mission Invocation.
- **2026-09-07 (session 2, Round 2 accepted).** All 24 recommendations accepted. Outcome vocabulary unified for atomic and composite activations (`00 §6.3` + `skipped`, `superseded`); lifecycle terminal is `completed` only; Attempt Outcome and Failure Class added (`00 §6.5`); Stage Graph phases `releasing | draining`; names Swarm Member / Convergence final. Wrote `05-EXECUTORS_AND_DURABLE_CONTROLS.md` (identity hierarchy Attempt / Agent Session / Session Turn / Harness Execution, Agent Executor Binding, Operating Contract, Completion Candidate for every Agent Executor, Executor Kind registry, Retry Policy, Event Wait once/rearm, Timer, Human Gate/Human Task, Proof Gate, Gate Condition law) and `06-MISSION_INVOCATION.md` (modes and sub-modes, definition sources, await/track/detach, cancel cascade, outcome mirroring, Portal, Spawn Grant, Mission Relationship kinds, cross-Mission acceptance). Glossary gained ~20 terms. Frontier items 1–6 are now complete. Round 3 issued: normalized events and streams, monitoring and intervention, nested-system Revision transitions, Mission/Run state machines.
- **2026-09-07 (session 2, Round 3 accepted).** All 18 recommendations accepted. `07` gained Mission / Run / Proposal / Revision state machines (§10a), transition granularity and Transition Impact (§4.1–4.2), Carry-Forward Eligibility (§5.1), operation names. New `09-EVENTS_COMMANDS_AND_STREAMS.md`: Mission Event envelope, 16 event families, Native Event Store, at-least-once + `seq`, stream contract with Portal coalescing, 11 Commands with lifecycle/outcome, delivery semantics incl. `emulated`, authority scopes. Rubric schema owned by Knowledge Services. **Frontier items 1–8 are complete; the workflow-types suite has no open interview questions.** Remaining: item 9 (reconcile minimum-viable Compaction/Transfer and Eve into M0–M8) and item 10 (parent spec + sequence synthesis) — Round 4 issued on the sequencing decisions.
- **2026-09-07 (session 2, Round 4 accepted — interview closed).** All 12 sequencing and schema recommendations accepted: minimal Transfer in M5 and semantic Compaction in M6; Eve as the second M5 lane on a Mission-Control-owned `eve dev`/Docker deployment; `direct_model` in M5; M4 = Temporal kernel + Stage Graph substrate + Deterministic Executors + all four Durable Controls in simple form; M6 order Stage Graph completion → EO → Swarm → Goal Loop; M7 control plane, M8 evolution + Mission Graph; dashboard shell from M3 read models; new `mission_control` schema replacing the `orchestration` mission spine with FK rewrite in one M1 migration; five ADRs; system-proof exemplars plus `application_build_test_improve`. Synthesis written: `MISSION_CONTROL_ARCHITECTURE.md` (v2, with §20 source map), `M0_M8_PROGRAM.md` (deliverables, exit proofs, feature-spec queue, risks), ADRs `0001`–`0005` in `ai-engineer-architecture/adr/`, supersession headers on both candidate documents, `workflow-types/index.md` re-parented. **Frontier items 1–10 complete.** Next: feature specifications starting at `F1.1`.

## 17. Starter prompt for the next agent

> Continue Mission Control from `specs/mission-control/HANDOFF.md §15`. Read `CONTEXT.md`, `MISSION_CONTROL_ARCHITECTURE.md`, `M0_M8_PROGRAM.md`, and the `workflow-types/` documents each feature cites. Follow the `to-spec` skill to write the next feature specification in the queue under `specs/mission-control/features/`, using the glossary vocabulary exactly, citing suite sections rather than restating them, and naming testing seams and the exit proofs it satisfies. Grill only open implementation questions; the domain is settled. Confirm the feature specification with the user before cutting issues. Do not implement runtime code before its feature specification is confirmed.
