---
type: Workflow Specification
title: Mission Control — Execution Program Model
description: "Scope: shared model for composing and executing workflow systems inside one Mission"
tags: [mission-control, spec, workflow]
---
# Mission Control — Execution Program Model

Architecture: [General Mission Control](../general-mission-control/SPECIFICATION.md). Runtime/API binding: [RUNTIME-CONTRACTS.md](../general-mission-control/RUNTIME-CONTRACTS.md). Historical milestone references do not override the [current delivery plan](../general-mission-control/IMPLEMENTATION.md).

**Status:** canonical behavior contract; earlier interview dates below are provenance
**Last updated:** 2026-10-02 (general runtime consolidation)
**Scope:** shared model for composing and executing workflow systems inside one Mission

## 1. Purpose

A Mission may pursue one or more Goals and may compose several workflow systems in one execution program. This document separates the semantic outcome model from the executable program and establishes the vocabulary needed to specify each workflow system without treating unrelated runtime mechanisms as peers.

## 2. Settled laws

### 2.1 Outcome and execution are separate

- A Goal describes a desired state or outcome.
- An Objective tree makes a Goal operationally explicit but is not executable.
- A Node belongs to the execution program and declares the Objectives it serves.
- Node success does not establish Objective or Goal acceptance.
- Traceability must remain queryable from Goal through Objective, criterion, Node contribution, Artifact, Evidence Assessment, and Disposition.

### 2.2 Authored intent precedes execution

The Coordinator authors a Mission Definition. Mission Control validates and stores a submitted definition before deterministically compiling it. Execution starts only from an immutable committed Mission Revision and its Compiled Program.

### 2.3 A Mission is not a workflow type

A Mission can combine multiple workflow systems. Workflow-system choice belongs to a block of the execution program, not to Mission identity.

### 2.4 Mission composition and Mission chaining are distinct

- Mission Composition binds Goals, an execution program, inputs, policy, budgets, and proof requirements into one Mission Definition.
- Mission Chaining connects independently governed Missions through typed bindings, immutable Artifact references, events, and gates.
- A Root Mission evaluates its own Success Criteria; Child Mission success supplies evidence but does not automatically establish Root Mission acceptance.

### 2.5 Committed history is immutable

Future execution may change only through a new Mission Revision. Runtime discovery cannot silently expand Goals, authority, capabilities, or the accepted outcome contract.

## 3. Taxonomy problem in the candidate architecture

The parent candidate specification currently places these literals in one `NodeSpec.strategy` union:

`STAGE_GRAPH | GOAL_LOOP | PARALLEL_SWARM | DETERMINISTIC | EVALUATOR_OPTIMIZER | MISSION_SPAWN | EVENT_TRIGGER`

That union combines concepts at different abstraction levels:

- `STAGE_GRAPH`, `GOAL_LOOP`, `PARALLEL_SWARM`, and `EVALUATOR_OPTIMIZER` have internal control state, child work, governors, and completion semantics.
- `DETERMINISTIC` describes how an atomic unit executes.
- `EVENT_TRIGGER` describes a durable release or resumption condition.
- `MISSION_SPAWN` crosses a Mission boundary and creates an independently governed lifecycle.

Calling all seven either “workflow types” or “per-node execution strategies” prevents precise nesting, lifecycle, and contract rules.

## 4. Accepted classification

### 4.1 Workflow systems

Composite control structures with their own lifecycle and governors:

1. **Stage Graph** — dependency-directed release of program blocks
2. **Goal Loop** — repeated observe, act, and evaluate toward a criterion
3. **Parallel Swarm** — bounded parallel work with explicit convergence
4. **Evaluator Optimizer** — repeated production and independent evaluation

### 4.2 Executors

Bindings that perform atomic work:

- **Agent Executor** — delegates work to an admitted agent harness
- **Deterministic Executor** — performs typed mechanical work without model discretion

### 4.3 Durable controls

Program Node behaviors that durably govern release or suspension:

- event wait or trigger
- timer
- human gate
- proof gate

Pause, cancellation, retry, and continuation are Commands and cross-cutting policies (§4.5), not Program Node behaviors. _(Corrected 2026-09-07, session 2.)_

### 4.4 Mission-boundary operations

- invoke a Child Mission by creating a new Mission or attaching an existing one
- await, track, or detach from that Child Mission
- project its state through a portal in the parent program

### 4.5 Cross-cutting policies

Budgets, retries, continuation, concurrency, failure propagation, capabilities, workspaces, sandboxes, proof, observability, and intervention apply to program scopes but are not workflow systems.

## 5. Recursive program shape

The execution program is modeled as recursively composable Program Nodes rather than as a flat list in which every Node has one overloaded `strategy`. A Mission Definition owns a general `program`, not a mandatory top-level `graph`.

Every Program Node has:

- stable key and Revision identity
- Objective references
- typed inputs and outputs
- importance
- lifecycle and outcome
- budget and policy scope
- exactly one behavior: workflow system, executor, durable control, or Child Mission Invocation

Composite workflow systems contain child Program Nodes. A Stage is specifically a Program Node directly governed by a Stage Graph; it is not a synonym for every Program Node.

A nested workflow system pursues existing Objectives from the enclosing Mission and cannot introduce independently governed Goals. Only a new Mission Definition, reached through Child Mission Invocation, introduces such Goals.

Workflow systems nest explicitly. A Stage may contain a Goal Loop, for example; the Stage does not acquire an ambiguous collection of loop flags. Compilation resolves the hierarchy into an executable representation while preserving the authored structural path for inspection and traceability.

### 5.1 Nesting matrix

- Stage Graph stages may contain any workflow system or executor.
- Goal Loop phases may contain subprograms, including graphs and swarms.
- Parallel Swarm worker and convergence definitions may be subprograms.
- Evaluator Optimizer producer and evaluator definitions may be subprograms.
- Same-type nesting requires distinct control scope and governors.
- Every recursive boundary is statically visible and bounded in the committed program.

### 5.2 Composite boundaries

Every workflow-system boundary declares typed inputs, projected outputs, Objective references, acceptance behavior, and budget scope. Child outputs remain inspectable but do not become outputs of the enclosing system without explicit projection.

Every activation receives a system-execution identity, lifecycle, local governor counters, allocated budget, child Program Node executions, event cursor, and terminal outcome. The immutable Program Node definition remains separate from its activations.

### 5.3 Session independence

Workflow-system semantics do not require a provider session model. Agent Sessions are disposable and must not become workflow receipts.

The primary long-running strategy is:

`Compaction → validated Continuation Checkpoint → Continuation Transfer → fresh Agent Session`

Four policies remain distinct:

- the enclosing workflow or cycle policy decides whether another activation occurs;
- Continuation Policy decides whether and how one logical execution crosses a session boundary;
- Compaction Policy decides when and how a checkpoint is produced and validated;
- Session Policy decides whether to start fresh, reuse briefly, or resume from a checkpoint.

In-place session reuse is an explicit optimization, not the default architecture. Independent producer, verifier, and evaluator identities remain isolated where required.

### 5.4 Repetition law

- **Retry** — another Attempt after execution failure
- **Revisit** — another Stage activation caused by a legal Stage Graph cycle
- **Iteration** — another Goal Loop cycle because acceptance has not been reached
- **Optimization Round** — one producer/evaluator exchange
- **Continuation** — preservation of one logical execution across context or session boundaries

These are separate counters, events, governors, and lineage relationships.

## 6. Shared execution-state vocabulary (accepted 2026-09-07, session 2)

Every specification in this suite must state its state vocabularies explicitly, define each value, and reuse the shared vocabularies below rather than inventing local synonyms. Contract fields and ledger columns carry these exact names.

### 6.1 Three separate fields

Every activation records three independent facts. They are never collapsed into one `status`:

| Field | Question it answers | Vocabulary |
|---|---|---|
| `lifecycle` | Where is this activation in its universal progression? | shared, §6.2 |
| `phase` | What is the system doing right now? | per workflow system, declared in that system's specification |
| `terminal_outcome` | How did it end? | shared base + declared extensions, §6.3 |

### 6.2 Activation Lifecycle

`pending → ready → running ⇄ waiting → completed`

| Value | Meaning |
|---|---|
| `pending` | Defined in the Compiled Program for this Run; release conditions not yet satisfied. |
| `ready` | Release conditions satisfied; waiting only for capacity, lease, or fairness. |
| `running` | An Attempt is executing, or a workflow system is actively governing child work. |
| `waiting` | A declared durable wait: Human Task, Event Wait, Timer, gate, Continuation Transfer in progress, or queued Action Authorization. Not terminal. |
| `completed` | The only terminal lifecycle value. A Terminal Outcome has been recorded and says how it ended. |

`completed` is the sole terminal value (Round 2, Q2). Cancellation is a Terminal Outcome, not a lifecycle value, so "is it over?" and "how did it end?" are never encoded twice. `awaiting_input` is not a Terminal Outcome; it is the `waiting` lifecycle value.

### 6.3 Terminal Outcome

One vocabulary for **every** activation — atomic (Stage body, Swarm Member, executor, Durable Control) and composite (workflow system). Round 2 unified the earlier Stage-level list (`succeeded | failed | rejected | skipped | superseded`) into this one: `succeeded → accepted`, `rejected → not_accepted`, `failed → execution_failed`.

Base vocabulary:

| Value | Meaning |
|---|---|
| `accepted` | The activation's Completion Contract accepted its required outputs and evidence. The only outcome that projects outputs. |
| `not_accepted` | Execution finished but the Completion Contract rejected the result on quality, validity, or verification grounds. Not an infrastructure failure; never retried as one. |
| `stopped_by_policy` | A declared stop predicate or an explicit policy decision ended the activation before acceptance. |
| `governor_exhausted` | A hard cap was reached: iterations, rounds, visits, members, budget, wall-clock, or Continuation Transfers. |
| `no_progress` | A patience governor was exhausted without measurable progress. |
| `revision_required` | The activation cannot continue under the committed Revision (Action Space, structure, authority, or policy would need to change) and a Revision Proposal decision is needed. |
| `cancelled` | Terminated by Command or by an enclosing system's cancellation policy (`fail_fast`, `on_resolution`, `on_parent_cancel`). |
| `execution_failed` | Infrastructure failure after Retry policy exhaustion. |
| `skipped` | Never executed: release became conclusively impossible, or a Durable Control's `on_timeout: skip` fired. |
| `superseded` | Replaced by a Revision transition (`cancel_and_replace`, `supersede_after_completion`) before or after producing output; its outputs are lineage only. |

Declared extensions:

| Value | System | Meaning |
|---|---|---|
| `stalemate` | Stage Graph | Required Stages remain pending, nothing is active, no legal release exists. |
| `quorum_unreachable` | Parallel Swarm | The Member Completion Policy can no longer be satisfied by the remaining members. |
| `threshold_not_reached` | Evaluator Optimizer | `max_rounds` reached without the acceptance expression passing. |

Governor exhaustion, no progress, and quorum unreachability are governed outcomes, not infrastructure failures.

### 6.4 Cycle Decision

Shared by Goal Loop Iterations and Evaluator Optimizer Optimization Rounds:

| Value | Meaning |
|---|---|
| `continue` | Another Iteration or Round is justified. |
| `accept` | Submit for, or record, acceptance under the Completion Contract. |
| `stop` | End without acceptance under policy (`stopped_by_policy`, `governor_exhausted`, or `no_progress`). |
| `revise` | A Revision Proposal is required before continuing. |
| `await_input` | Park in `waiting` for a Human Task or event. |

### 6.5 Attempt Outcome and Failure Class (accepted Round 2)

An **Attempt** is one try at an activation. Its outcome describes *execution*, never acceptance:

`succeeded | failed | cancelled`

| Value | Meaning |
|---|---|
| `succeeded` | The Attempt ran to its native end and registered its Completion Candidate or receipt. Whether the result is accepted is the activation's Completion Contract decision, recorded as the activation's Terminal Outcome. |
| `failed` | Execution did not reach a usable end. Always carries a `failure_class`. |
| `cancelled` | Stopped by Command or enclosing policy. |

`failure_class`:

| Value | Meaning | Retryable |
|---|---|---|
| `infrastructure` | Worker, network, storage, or orchestrator fault. | yes |
| `timeout` | Declared Attempt or Turn timeout exceeded. | yes |
| `provider_error` | Harness or model provider returned an error (Cursor run `ERROR`, Deep Agents execution failure, provider 5xx). | yes |
| `capacity` | Rate limit, concurrency limit, or `agent_busy`-class refusal. | yes, with backoff |
| `policy_denied` | Mission Control or harness policy refused the action (unadmitted capability, Side-Effect Class outside grant). | no |
| `provenance` | Receipt, digest, or identity mismatch. Quarantines. | no |
| `cancelled_by_command` | Recorded on the Attempt when a Command ended it; the Attempt outcome is `cancelled`. | no |

Retry policy may target only `infrastructure | timeout | provider_error | capacity`.

### 6.6 Journals

Only Goal Loop keeps a Journal; its adaptive decisions need a narrative record. Stage Graph, Parallel Swarm, and Evaluator Optimizer routes are compiled, and their history is fully captured by activations, typed Artifacts (receipts, Evaluation Reports), and the Domain Ledger. A nested Goal Loop inside any system keeps its own Journal.

Shared memory contracts are part of the Knowledge Services boundary in the canonical specification. Full cross-mission memory behavior is subsequent design work; journals and mission ledgers already retain their defined execution responsibilities.

## 7. Questions still open

Resolved in Round 2 (2026-09-07): outcome unification (§6.3), Attempt Outcome and Failure Class (§6.5), Mission Invocation semantics (`06-MISSION_INVOCATION.md`), Executors and Durable Controls (`05-EXECUTORS_AND_DURABLE_CONTROLS.md`).

Revision behavior is defined in workflow 07; the discriminated ProgramNode compiler contract is in SPECIFICATION; normalized events/streams are in workflow 09 and RUNTIME-CONTRACTS. These replace the historical open questions.
