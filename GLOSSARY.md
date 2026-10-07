---
type: Glossary
title: Mission Control
description: "Mission Control is the shared control service that turns a human's research, ingestion, content or coding outcome into an admitted, durably scheduled mission for one of two applications, and computes whether the result…"
tags: [mission-control, language]
---
# Mission Control

Mission Control is the shared control service that turns a human's research, ingestion, content or coding outcome into an admitted, durably scheduled mission for one of two applications, and computes whether the result is accepted. This glossary is the language used in specs, code, issues and conversation. Seeded 2026-10-07 from the accepted specification and the code; sharpened in interview.

## Language

### Applications and identity

**Application**:
One of the two registered products Mission Control serves: `ai-engineer` or `biotech`.
_Avoid_: app, product, tenant

**Installation**:
The identity of one common schema release living inside one application's database.

**Application Binding**:
The immutable, operator-managed registry entry that ties an application to its installation, database, storage, auth issuers, Temporal queues and runtime persistence.
_Avoid_: deployment config, registry row

**Tenant**:
A grant boundary inside an application.

**Actor**:
The authenticated human or service principal acting inside an application and tenant.

**Scope**:
The installation, application, tenant and actor that every request and record carries.
_Avoid_: context (overloaded), request context

**Grant**:
Authority an actor holds in a scope. Effective authority is always the intersection of grants, application policy, mission action space and node scope; no tool, skill text or model output widens it.
_Avoid_: permission, role

### Missions and programs

**Mission**:
A durable statement of goals, success criteria and a recursive program, with exactly one scheduling head.

**Mission Definition**:
The typed document a mission is authored as: goals, objectives, criteria, inputs, program, policies, budget and completion contract.
_Avoid_: mission spec, mission YAML (the file format is not the definition)

**Goal**:
An outcome the mission exists to reach, weighted by importance.

**Objective**:
A step toward a goal, optionally nested under another objective.

**Success Criterion**:
A checkable condition on a goal, naming the evidence it requires and how it is accepted.

**Program**:
The recursive tree of program nodes that realizes a mission.

**Program Node**:
One unit of the program with a single behavior, its own inputs, outputs, policies, budget and completion contract.

**Behavior**:
The kind of a program node: a workflow system, an executor, a durable control or a child mission invocation.

**Compiled Program**:
The deterministic compiler output for a definition: canonical digests, capability attachment plan, expansion limits and validation report.
_Avoid_: ERC, effective run configuration (the code's name for the same thing)

**Revision**:
An immutable compiled version of a mission's definition.

**Revision Proposal**:
A submitted candidate revision awaiting validation, review and activation.

**Scheduling Head**:
The single active revision a mission schedules from.

**Transition Impact**:
The recorded effect of activating a successor revision on each in-flight activation.

**Carry-Forward**:
Reuse of completed work across revisions, decided by digest equality, never by assumption.

**Run**:
One durable execution of a mission from a starting revision with immutable inputs and bindings.
_Avoid_: job, execution, workflow instance

**Activation**:
One scheduled instance of a program node inside a run.

**Attempt**:
One try at an activation, with its own execution outcome and failure class.

**Agent Session**:
A runtime-owned conversation identity mapped to one attempt. It carries no acceptance authority.

**Session Turn**:
One exchange inside an agent session.

**Lifecycle, Phase, Terminal Outcome**:
The three separate fields that describe any record's state: where it is in its life, what it is doing now, and how it ended. A terminal outcome is absent until completion.

**Child Mission**:
An independently admitted mission started by another mission through a child mission invocation.

**Mission Relationship**:
A recorded link between missions: parent and child, chained, linked or superseded.

**Portal**:
The admitted way one mission invokes another.

**Spawn Grant**:
The bounded authority to create child missions or subordinates.

### Workflow systems

**Workflow System**:
One of the four program behaviors: Stage Graph, Goal Loop, Parallel Swarm, Evaluator Optimizer.
_Avoid_: workflow type, family (the code's word)

**Stage Graph**:
Dependency-driven release of typed stages through gates.
_Avoid_: DAG, pipeline, StageGraph (in prose)

**Stage**:
A node of a stage graph that releases when its dependencies, typed inputs and gates are satisfied.

**Goal Loop**:
The workflow system that iterates observe, propose, authorize, act, assess, decide until a goal is met or a governor stops it.
_Avoid_: GoalDirected (today's code family implementing a subset of it), agent loop

**Loop Journal**:
The sealed, append-only record of a goal loop's observations, actions, receipts and reviews.

**Loop State**:
The bounded, typed working state a goal loop carries between iterations.

**Action Space**:
The committed set of actions a goal loop may choose from. New capabilities need a revision.

**Progress Review**:
The assessment at the end of an iteration that decides whether the loop continues.

**Parallel Swarm**:
The workflow system that runs bounded member variants in parallel and converges their results.

**Member Variant**:
One declared member of a swarm.

**Convergence**:
The separate subprogram that turns swarm member results into one outcome.

**Evaluator Optimizer**:
The workflow system where an independent evaluator scores a producer's output against a rubric over bounded rounds.

**Optimization Round**:
One produce-and-evaluate cycle.

**Rubric**:
The typed scoring contract an evaluator applies.

**Executor**:
A program behavior that performs work directly: an Agent Executor (bounded cognition) or a Deterministic Executor (a registered executor kind with no model judgment).

**Durable Control**:
A program behavior that waits rather than works: Event Wait, Timer, Human Gate, Proof Gate.

**Human Task**:
The durable record of a decision only a human can make, resolved by an attributed action: approved, denied, answered, selected, review_accept, review_reject or overridden.
_Avoid_: approval request, HITL item

**Review Decision**:
The payload of a review-kind human task: approve, reject, request_changes or abstain. It is not a second state machine.

**Completion Candidate**:
An executor's submitted claim that its activation is done, with evidence.

**Completion Contract**:
The closed, typed expression that says what makes a node or mission accepted.

**Completion Decision**:
Mission Control's computed verdict on a candidate. Execution success never implies acceptance.

**Evidence Assessment**:
A registered capability's attributable judgment about evidence, returned as a disposition.

**Disposition**:
The typed result of an assessment that a completion contract can reference.

### Execution, effects and settlement

**Harness**:
The provider-neutral protocol every execution lane implements: prepare, start, send_turn, cancel_turn, observe, snapshot, usage, end_session.
_Avoid_: adapter (the code's word), driver

**Lane**:
One qualified harness implementation: Deep Agents, Cursor Cloud, Claude Agent SDK, Codex or Direct Model.
_Avoid_: runtime kind, provider, host

**Agent Host**:
A tool a human or coordinator works in that reads generated configuration: Cursor, Codex, Claude Code. A host is where authoring happens; a lane is where mission work executes.

**Execution Binding**:
The exact pinned set an attempt runs with: packages, model route, prompts, tools, skills, workspace, sandbox profile, budgets and side-effect allowances.
_Avoid_: config, settings

**Materialization**:
Turning an execution binding into a real session and workspace, recording intended and actual versions and refusing mismatches.

**Operation**:
A journaled unit of externally visible work with an intent before and a settlement after.

**Operation Intent**:
The durable record written before an operation acts.

**Settlement**:
The recorded outcome of an operation or effect, including its cost.

**Effect**:
An external side effect with its own identity. Infrastructure retries keep the identity; new logical actions get a new one.

**Effect Ledger**:
The record of every effect's identity, claim and settlement.

**Side-Effect Class**:
A capability's declared kind of external impact, which decides what review and receipt it needs.

**Uncertain Effect**:
An effect whose outcome is unknown. It is reconciled before anything is re-executed.

**Budget**:
The admitted ceiling for money, tokens, duration, tools and sandboxes, reserved before work and settled after.

**Governor**:
A hard bound that stops work: depth, fan-out, rounds, iterations, patience or budget.

**Technical Retry**:
Re-running the same attempt after a transient infrastructure failure, with the same effect identities.

**Semantic Retry**:
New work with changed instructions, inputs or capabilities. It is never disguised as a technical retry.

**Workspace**:
The owned filesystem an attempt works in, with read-only mission context, skill bundles, inputs and declared writable paths.

**Sandbox Profile**:
The pinned image, limits, lifetime, egress and cleanup rules for a workspace.

**Artifact**:
An immutable, digest-registered output. A file becomes an artifact only after registration.

**Subordinate**:
Bounded sync or async work delegated by an activation, with a dependency class of required_blocking, degradable_blocking, nonblocking or advisory.
_Avoid_: subagent (the runtime's word), child (that is a child mission)

### Control and recovery

**Command**:
An admitted, ordered instruction to a run. Lifecycle: accepted, queued, delivered, observed, completed.
_Avoid_: signal, message

**Delivery Report**:
What a lane actually did with a command, reported separately from what was requested.

**Stop Fence**:
The persisted marker that rejects new effect claims after an urgent stop.

**Intervention**:
A privileged operator command: cancel, resume, satisfy a wait, fork from a checkpoint or reconcile.

**Reducer**:
The deterministic function that owns lifecycle transitions and settlement. Nothing else changes lifecycle.

**Outbox**:
The transactional relay from the application database to Temporal.

**Checkpoint**:
A sealed, digest-verified manifest of a run's state that continuation resumes from.

**Checkpoint Lineage**:
The ordered ancestry of checkpoints a run has sealed.

**Snapshot**:
A read-only capture of a run at a safe boundary, used to inspect or fork.

**Fork**:
A new run or mission created from a snapshot. It never clones in-flight commands or active children.

**Continuation**:
Resuming work after a checkpoint, in a fresh session or workspace when needed.

**Compaction**:
Reducing carried context before continuation under a context health policy.

**Runtime Unit**:
The smallest provider-neutral unit of execution the kernel tracks and reconciles.

**Generation**:
A monotonically increasing number that fences stale results from an earlier launch.

**In Doubt**:
A unit whose provider state cannot be confirmed; it is reconciled, never duplicated.

### Catalog and capabilities

**Catalog**:
The per-application set of admitted, versioned assets.

**Capability**:
An installed, digest-pinned, admitted asset that a mission may bind. Its kind is one of the catalog's recorded kinds.

**Capability Kind**:
The category of a capability. The canonical list lives in the catalog, not in a document.

**Capability Pin**:
The exact version and digest a binding references.

**Capability Search**:
Semantic and filtered selection of catalog assets before compiling exact bindings.

**Search Projection**:
The read-only index built from the catalog for search. It is never an authorization store.

**Discovery**:
Finding external candidate assets. Candidates are quarantined until inspected and approved.

**Skill Bundle**:
A packaged, pinned skill directory a lane can materialize.

**Environment Profile**:
A catalog asset that pins a workspace and sandbox shape.

**Model Profile**:
An immutable record of a provider, model, endpoint, settings and limits.

**Workflow Template**:
A reusable, versioned program fragment.

### Authoring and coordination

**Coordinator**:
The API client with authoring capabilities that talks with the human and drives a mission from draft to start. Its chat is not execution authority.

**Authoring Session**:
The bounded, budgeted coordinator conversation that produces a mission draft. The interview is its question-asking mode.
_Avoid_: interview session

**Interview**:
The question-asking mode of an authoring session, used before research, ingestion, content and coding missions alike.

**Draft**:
An editable, not yet compiled mission definition.

**Validation Report**:
The typed result of validating a draft: errors with pointers, missing capabilities, policy conflicts and warnings. Validation launches nothing.

**Launch Ticket**:
The prepared, admitted request that starts a run.

### Knowledge and domain

**Knowledge Services**:
The shared contracts for evidence, source identity, provenance, governed domain writes and retrieval that applications implement in their own adapters.

**Domain Service**:
An application-owned service that judges evidence and performs entity writes. The kernel never writes entities.

**Intent**:
An immutable, digest-pinned request for a domain effect, applied only through its receipt contract.

**Receipt**:
The durable proof that a domain effect happened, with its idempotency identity.

**Advisory Memory**:
Recalled, attributable references that inform work and grant nothing until revalidated.

**Context Selection**:
The reproducible choice of what an attempt gets to read, captured as candidate identities, ranking and configuration.

**State Handoff**:
A typed delta plus base version and digest that moves state between activations through a deterministic reducer.

### Platform

**Common Component**:
The versioned schema release every application database installs identically.

**Release Attestation**:
The recorded proof that an installation holds a complete, fingerprint-matched release.

**Runtime Persistence**:
The subordinate, recoverable state of the agent runtime. It is never mission truth.

**Agent Server**:
The required execution host for bounded asynchronous subordinate graphs. It schedules nothing.

**Deep Agents**:
The first execution lane: bounded operation cognition on LangGraph.

**Temporal**:
The only mission scheduler.

**LangSmith**:
The sandbox and tracing provider. Traces are not the ledger.
