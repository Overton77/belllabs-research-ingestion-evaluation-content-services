---
type: Workflow Specification
title: Mission Control — Goal Loop
description: Goal Loop is the adaptive workflow system for pursuing an Objective when the next useful action cannot be fully compiled in advance.
tags: [mission-control, spec, workflow]
---
# Mission Control — Goal Loop

Architecture: [General Mission Control](../general-mission-control/SPECIFICATION.md). Runtime/API binding: [RUNTIME-CONTRACTS.md](../general-mission-control/RUNTIME-CONTRACTS.md). Historical milestone references do not override the [current delivery plan](../general-mission-control/IMPLEMENTATION.md).

**Status:** canonical behavior contract; earlier interview dates below are provenance
**Last updated:** 2026-10-02 (general runtime consolidation)
**Workflow-system kind:** `goal_loop`

## 1. Purpose

Goal Loop is the adaptive workflow system for pursuing an Objective when the next useful action cannot be fully compiled in advance.

Unlike a Stage Graph Revisit Region, Goal Loop does not merely repeat a known route. Each Iteration observes current state, chooses bounded action, evaluates progress, and decides whether another Iteration is justified.

## 2. Settled shared laws

- Goal Loop pursues existing Objectives and cannot introduce independently governed Goals.
- Its body may contain executors or nested workflow systems.
- Every Iteration has a distinct activation identity, inputs, outputs, decisions, evidence, and governor accounting.
- Retry, Revisit, Iteration, Optimization Round, and Continuation remain distinct.
- Long-running logical work defaults to Compaction and Continuation Transfer into fresh Agent Sessions.
- Goal acceptance and execution completion remain separate.

## 3. Two orthogonal control systems

Goal Loop coordinates:

1. **Goal-progress loop:** `observe → propose action → authorize → act → evaluate → review completion → decide`
2. **Context-health loop:** `monitor → compact → synthesize → validate → transfer → resume`

Iteration identity and Agent Session identity are independent. One healthy Session may perform several Iterations; one Iteration may cross Sessions through intra-activation Continuation.

## 4. Loop Journal and Loop State

Goal Loop does not create a second Mission ledger.

Its Loop Journal is an Artifact-backed, append-only logical record. Appends produce immutable Journal segments or manifests. The Domain Ledger records canonical identity, ordering, active head, authorship, digests, and relationships to the Goal Loop execution.

Agents append typed observations, proposals, Progress Reviews, decisions, outputs, and evidence. Mission Control validates these records and performs canonical state transitions. Workspace-local notes are non-canonical until registered and attributed.

### 4.1 Journal Entries and Segments (accepted 2026-09-07)

The unit of append is a **Journal Entry**. Entry kinds:

`observation | action_proposal | action_authorization | action_receipt | progress_review | completion_candidate | decision | evidence_ref | blocker | human_answer`

Every entry carries `entry_seq`, `iteration_id`, `author` (Loop Controller, executor, Mission Control, or human), `authored_at`, and typed payload per kind. Entries are grouped into **Journal Segments**: immutable Artifacts holding an ordered run of entries, sealed with `segment_digest` and `prev_segment_digest`. The Domain Ledger records, per Goal Loop activation, the ordered segment list, `active_head_segment`, author, and digests.

A segment is sealed:

- at every Iteration boundary;
- immediately before Compaction;
- when the open segment reaches its declared size cap; and
- at Goal Loop termination.

A **Journal Digest** is a sealed segment that summarizes earlier segments so Loop State can be materialized from the digest forward. Earlier segments are never rewritten or deleted; they remain inspectable lineage. Journal compaction is therefore append-only, like everything else in the Journal.

Rejected alternatives: one Artifact per entry (unbounded Artifact counts) and a single rewritten manifest (mutation of committed history).

Loop State is the bounded projection consumed by the Loop Controller:

- Objective and Success Criteria;
- current acceptance state;
- prior Iteration summaries;
- Artifact and evidence references;
- verification findings;
- workspace and sandbox snapshot;
- unresolved blockers and Human Tasks;
- event cursor;
- remaining governors and budgets; and
- admitted Action Space.

The complete accumulated transcript is not Loop State.

## 5. Loop Operating Contract

Every Goal-directed Agent Executor receives a versioned contract containing:

- Goal, Objectives, Success Criteria, and constraints;
- workspace and sandbox rules;
- admitted capabilities;
- Loop Journal interaction protocol;
- evidence requirements;
- Progress Review protocol;
- Completion Candidate protocol;
- Context Health and Compaction rules;
- budget and side-effect limits; and
- escalation, abstention, and human-input rules.

## 6. Action selection and authority

The committed Action Space defines the executors, Program Modules, capabilities, side-effect classes, output contracts, and nested workflow systems available to the Loop Controller.

The controller may instantiate and choose only within that space. Structural, capability, authority, or policy expansion creates a Revision Proposal.

The Loop Controller and selected action executor are separate attributable roles even when policy permits one deployment to perform both.

### 6.1 Action Proposal (accepted 2026-09-07)

An **Action Proposal** is a Journal Entry containing:

```text
ActionProposal {
  action_ref              // member of the committed Action Space
  input_refs[]            // immutable Artifact references
  expected_outputs[]      // names declared by the action's output contract
  side_effect_class       // read_only | workspace_write | external_write_reversible | external_write_irreversible | spend
  estimated_cost          // tokens, money, wall-clock
  justification           // concise; not chain-of-thought
  progress_review_ref     // the review that motivates this action
}
```

### 6.2 Action Authorization (accepted 2026-09-07)

**Action Authorization** is a Mission Control transition, never an agent step. It checks, in order: Action Space membership; remaining governors and budget; Side-Effect Class against the activation's grants; and pending Human Gates or gate conditions. Its recorded result is:

`authorized | denied(reason) | pending_human_task(task_ref)`

- Automatic when every check passes; still recorded as a Journal Entry.
- `pending_human_task` opens an `APPROVAL` Human Task and parks the Iteration in `waiting`.
- `denied` with reason `outside_action_space` makes `revise` the natural Cycle Decision; the Loop Controller may instead propose another admitted action. Authority is never silently expanded.

Side-Effect Class vocabulary:

| Value | Meaning |
|---|---|
| `read_only` | No state changes outside the agent's context. |
| `workspace_write` | Changes confined to the activation's workspace or sandbox. |
| `external_write_reversible` | Changes outside the workspace that can be undone (branch push, draft record). |
| `external_write_irreversible` | Merge, publish, send, delete, or any change that cannot be undone. |
| `spend` | Consumes budget beyond the activation's own model tokens (paid APIs, compute purchases). |

## 7. Iteration lifecycle

Each Iteration is an activation with the shared Activation Lifecycle (`00 §6.2`) and these Phases:

`observing → proposing → authorizing → acting → evaluating → reviewing → deciding`

The **Cycle Decision** is recorded separately (`00 §6.4`):

`continue | accept | stop | revise | await_input`

Authorization may be automatic within existing grants but remains an explicit transition.

## 8. Progress Review

A typed Progress Review is required:

- after each meaningful action;
- at every Iteration boundary;
- before major additional spend;
- before Compaction;
- after verification rejection; and
- when progress signals stagnate.

It records criteria status, evidence, remaining gaps, progress delta, uncertainty, assumptions, concise decision rationale, and the justified next action. It does not store hidden chain-of-thought.

Continuing requires measurable progress or consumes a finite patience governor.

## 9. Completion protocol

### 9.1 Goal Loop Completion Contract (accepted 2026-09-07)

Goal Loop uses the shared **Completion Contract** shape (required and optional outputs, schema and integrity rules, Verification Intents and tests, acceptable dispositions, gates, acceptance expression). Since Round 2, **Completion Candidate** is the single term for the typed submission every Agent Executor makes when it believes its work is done (see `05 §3.4`); Goal Loop's is the richest form. The Goal Loop rule is that the acceptance expression is evaluated against that Completion Candidate: every required Success Criterion must map to at least one evidence Artifact before deterministic validation begins. A candidate with an unmapped required criterion is returned as `incomplete_candidate` without running any check. Output Projection binds only to outputs named in the contract.

### 9.2 Protocol

1. The Loop Controller submits a Completion Candidate.
2. The candidate maps every required Success Criterion to evidence.
3. Deterministic validation runs first.
4. Independent semantic evaluation or LLM judgment runs where required.
5. Human review runs where policy requires.
6. Proof Policy computes the authoritative disposition.
7. Goal Loop accepts, continues, waits, revises, or stops.

The acting agent may propose completion but cannot declare authoritative acceptance.

Validation is ordered:

`identity and provenance → integrity and schema → deterministic tests → semantic support → policy acceptance`

Later probabilistic judgment cannot reverse an earlier deterministic failure.

## 10. Failures and quality rejection

- Infrastructure failure follows Retry policy.
- Quality rejection becomes typed input to the next Loop State and may release Remediation within the Action Space.
- Review or abstention produces durable waiting or explicit non-acceptance according to policy.
- Provenance mismatch or forged receipt produces quarantine and reconciliation.

Quality rejection is not infrastructure failure.

## 11. Context Health and Continuation

Context Health Policy is versioned by model, runtime, tool profile, and task class. It defines provider limit, quality-preserving target range, soft Compaction threshold, hard transfer threshold, reserved output and reasoning capacity, tool-payload allowance, observed degradation signals, and emergency margin.

The policy is informed by evaluation telemetry rather than one universal token percentage.

### 11.1 Policy versions and promotion (accepted 2026-09-07)

Context Health Policies are catalog records keyed by `model × runtime × tool_profile × task_class`, each version with status `candidate → active → deprecated`. Mission Control ships conservative default versions per model family. Promoting a new version is an explicit, attributable admission act informed by recorded telemetry; automatic policy learning is not part of the initial general runtime. A committed Mission Revision pins the policy versions it uses exactly as it pins capability versions, so a running Revision never changes thresholds underneath an execution.

When context health degrades, the Continuation Decision may:

- compact and transfer to the same admitted model and runtime;
- transfer to another admitted model or runtime;
- checkpoint and pause;
- decompose through the existing Action Space;
- propose a Revision;
- invoke a Child Mission within grant;
- request human input; or
- stop because further work is unjustified.

Compaction and Transfer follow the dedicated continuation specification.

## 12. Governors

- maximum Iterations
- tokens and monetary cost
- wall-clock
- no-progress patience
- stop predicate
- remaining Mission and credit-account budget
- maximum Continuation Transfers

Governor exhaustion is a governed terminal outcome, not infrastructure failure.

Repeated Progress Reviews without justified progress consume bounded patience, then require an alternate admitted action, Revision, Child Mission decomposition, human input, or explicit `no_progress` termination. Remaining budget alone never justifies continuation.

## 13. Human input during an Iteration (accepted 2026-09-07)

A Human Task raised from a Goal Loop declares `timeout` and `on_timeout: keep_waiting | escalate | default_answer | stop`. Default is `keep_waiting` with escalation notification.

- The Iteration parks in lifecycle `waiting` at the phase that raised the task (`authorizing` or `deciding`).
- On answer, the **same Iteration** resumes; the answer is appended as a `human_answer` Journal Entry and enters Loop State.
- A wait is not allowed to hold an Agent Session idle beyond the Session Policy's idle limit. If the Session was released, resumption is a Continuation Transfer from the last checkpoint under the same Iteration identity.
- A Human Task never starts a new Iteration by itself. The Human Task contract itself belongs to `05-EXECUTORS_AND_DURABLE_CONTROLS.md`.

## 14. Nested Goal Loops (accepted 2026-09-07)

A Goal Loop inside another Goal Loop's Action Space:

- declares an Action Space that must be a **subset** of the enclosing Action Space; validation rejects widening;
- declares its own governors, with budget carved from the outer allocation;
- keeps its own Loop Journal; and
- exposes to the outer loop only its projected outputs, Terminal Outcome, and a Journal reference Artifact.

## 15. Terminal Outcomes

Goal Loop uses the shared base vocabulary (`00 §6.3`) with no extensions:

`accepted | not_accepted | stopped_by_policy | governor_exhausted | no_progress | revision_required | cancelled | execution_failed`

Only `accepted` establishes Goal Loop acceptance. `awaiting_input` is the `waiting` lifecycle value, not an outcome.

## 16. Outputs

Only outputs selected by explicit Output Projection after the Goal Loop Completion Contract accepts them become outputs of the Goal Loop. Earlier and rejected outputs remain inspectable lineage.

## 17. Questions under interview

Resolved in Round 1 (2026-09-07): Journal entries/segments/digests, Action Proposal and Authorization, Completion Contract, Context Health Policy promotion, human input resumption, nested Action Spaces.

Remaining, owned by other documents:

1. Human Task contract and kinds — `05-EXECUTORS_AND_DURABLE_CONTROLS.md`.
2. Loop Operating Contract materialization into a specific harness — Agent Executor specification and runtime fact sheets.
3. Revision activation while an Iteration is running — `07`.
