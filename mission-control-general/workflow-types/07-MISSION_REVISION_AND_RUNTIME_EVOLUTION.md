---
type: Workflow Specification
title: Mission Control — Mission Revision and Runtime Evolution
description: "Scope: safe editing of authored Mission intent and execution programs before and during execution"
tags: [mission-control, spec, workflow]
---
# Mission Control — Mission Revision and Runtime Evolution

Architecture: [General Mission Control](../general-mission-control/SPECIFICATION.md). Runtime/API binding: [RUNTIME-CONTRACTS.md](../general-mission-control/RUNTIME-CONTRACTS.md). Historical milestone references do not override the [current delivery plan](../general-mission-control/IMPLEMENTATION.md).

**Status:** canonical behavior contract; earlier interview dates below are provenance
**Last updated:** 2026-10-02 (general runtime consolidation)
**Scope:** safe editing of authored Mission intent and execution programs before and during execution

## 1. Core distinction

A Mission is a durable aggregate whose lifecycle state, active head, relationships, and outcomes evolve. The following records are immutable once submitted or produced:

- validated Mission Definition snapshots
- committed Mission Revisions
- Compiled Programs
- Program Node activations, Attempts, and execution history
- Artifacts, evidence assessments, commands, receipts, and lineage records

Editing a Mission therefore produces a successor Mission Revision. It never overwrites committed intent or execution history.

## 2. Settled identity law

A Revision may change planning, decomposition, execution bindings, or a tightened interpretation while retaining Mission identity. A material change to Goals, authority, tenant, or the meaning of success creates a successor or forked Mission.

Private authoring state may be edited. Every submitted validation snapshot and committed Revision is immutable and attributable.

## 3. Revision flow

Authored edits and runtime graph changes use one pipeline:

`edit private draft → submit revision proposal → validate → compile → review when required → commit → activate scheduling head`

A `GraphMutationSet` is one structured authoring form for a Revision Proposal. It is not a separate mutation system and never edits a committed graph in place.

A proposed change records:

- base Revision
- author and initiating surface
- rationale and causation
- semantic and program diff
- affected Objectives, Program Nodes, policies, inputs, and capabilities
- expected treatment of pending and in-flight work
- validation, budget, authority, and proof impact

The stored result includes both the complete immutable Mission Definition and the structured semantic change set. The snapshot enables replay; the change set preserves explainability.

## 4. Scheduling-head and runtime evolution law

A Mission has exactly one active Scheduling Head. Only that Revision may release new work.

A running Mission may change future execution only through an activated successor Revision. Work already completed remains historical fact. Work in flight remains pinned to the Revision that started it unless an explicit transition policy cancels or supersedes it.

Each affected in-flight Program Node declares one transition:

- `finish_existing`
- `pause_and_checkpoint`
- `cancel_and_replace`
- `supersede_after_completion`

Unaffected work defaults to finishing. Materially changed work defaults to checkpointing or replacement. Side-effecting work requires explicit treatment.

### 4.1 Transition granularity (accepted Round 3)

Transition policies apply to **activations**. The atomic granularity is a Stage activation, Swarm Member, Iteration, Optimization Round, or Durable Control wait. A composite activation's transition cascades to its children unless a child's Program Node declares its own.

### 4.2 Transition Impact (accepted Round 3)

The compiler emits a **Transition Impact** per Program Node when a proposal is validated against the head:

| Impact | Meaning | Default treatment of a running activation |
|---|---|---|
| `unchanged` | Definition and policy digests equal. | Continue. In-flight children stay pinned to their starting Revision; new children release from the new head. |
| `policy_changed` | Governors, Action Space narrowing, member cap, budgets, or similar changed; structure did not. | Apply at the next child boundary without checkpointing. |
| `definition_changed` | Completion policy, Convergence, acceptance expression, Rubric, body, or bindings changed. | `pause_and_checkpoint` the composite at the next child boundary, then resume under the head. |
| `removed` | Node absent from the successor. | `supersede_after_completion` unless declared otherwise. |
| `added` | New node. | Releases from the head when its conditions hold. |

Transition Impact is part of the stored proposal record so the effect on running work is explainable before activation.

## 5. Carry-Forward

Completed work may be carried forward only when its Program Node definition, immutable inputs, relevant policies, capability versions, and proof requirements remain compatible.

Carry-Forward links the successor Revision to the original execution and Artifacts. It neither copies the execution nor claims that it ran under the successor Revision.

### 5.1 Carry-Forward Eligibility (accepted Round 3)

Eligibility is computed from digests, never by inspection. A completed activation is eligible when all of the following are equal between base and successor: `node_definition_digest`, `input_digests[]`, `policy_digest` (Proof Policy, capability versions, Context Health Policy versions), and `rubric_version` where applicable.

Proof freshness is separate: a Proof Policy may declare `evidence_max_age`. Stale evidence yields eligibility `re_verify_required` — outputs carry forward, acceptance re-runs — rather than `ineligible`.

Eligibility vocabulary: `eligible | re_verify_required | ineligible`.

## 6. Change classification

### 6.1 Capability changes

Adding an Agent Skill, MCP server, tool, credential scope, sandbox authority, or other capability changes the committed capability binding. It requires a Revision Proposal and admission validation.

An active Attempt cannot receive newly expanded authority. Mission Control checkpoints or cancels affected work and starts replacement execution under the successor Revision. Instructions and Artifact attachments may remain Commands when they do not expand authority.

Emergency capability or secret revocation may immediately pause or cancel affected executions through a durable Command. Resumption requires a valid Revision without the revoked authority.

### 6.2 Objective changes

Clarifying, decomposing, or tightening an Objective without replacing its Goal may remain within the same Mission through a Revision.

Expanding the desired outcome, invalidating previously sufficient evidence, fundamentally changing Success Criteria, or materially changing stakeholder expectations requires a successor or forked Mission.

### 6.3 Goal, authority, and tenant changes

A material change to Goals, authority, tenant, or the meaning of success cannot be activated as a Revision of the same Mission.

## 7. Authorization and review

- A Coordinator may activate changes within existing Goals, grants, budget, risk, and proof policy.
- Increased authority, budget, side-effect scope, or relaxed proof requires independent approval.
- A material Goal change is rejected as an in-place evolution and redirected to successor or fork creation.
- Emergency restriction may act immediately but never rewrites historical definitions.

## 8. Revert

Mission Control never silently reactivates an old Revision. Revert creates a new Revision Proposal derived from older content and validates it against current policy, capabilities, inputs, and environment state.

## 9. Mission Runs

One Mission Run executes one committed Mission Revision against one immutable input snapshot. A Mission may have multiple Runs for rerun, revalidation, recurrence, or optimization.

A Run completes. The Mission may become dormant and later start another Run. The Mission becomes terminal only when explicitly closed.

Every Program Node execution remains attributable to both its Mission Revision and Mission Run.

## 10. Concurrent editing

Every Revision Proposal names `base_revision_id`. Activation uses optimistic concurrency against the current Scheduling Head.

If another proposal advances the head first, later proposals must be explicitly rebased and revalidated. Mission Control never silently merges semantic, policy, or authority changes.

## 10a. State machines (accepted Round 3)

Each aggregate follows the three-field law of `00 §6`: lifecycle, optional phase, and a separate outcome or resolution.

### Mission

`lifecycle`: `drafting → ready → running ⇄ dormant → closed`

| Value | Meaning |
|---|---|
| `drafting` | No committed Revision yet. |
| `ready` | A committed Revision exists; no Run is active. |
| `running` | A Mission Run is active. |
| `dormant` | Runs have occurred; none is active; the Mission is not closed. |
| `closed` | Explicitly closed. Terminal. |

`closure_outcome` (recorded at `closed`): `mission_accepted | not_accepted | abandoned | superseded`. Mission Acceptance remains the separate aggregate Disposition over Goals; `paused` and `blocked` are Run- and activation-level facts, never Mission lifecycle.

### Mission Run

`lifecycle`: `pending → running ⇄ paused → completed`. `paused` is Command-driven and stops release; program-driven waits are activation-level `waiting`. A Run has no phases.

`terminal_outcome`: shared base — `accepted` (this Run satisfied Mission Acceptance policy), `not_accepted`, `stopped_by_policy`, `governor_exhausted`, `revision_required`, `cancelled`, `execution_failed`.

### Revision Proposal

`lifecycle`: `drafting → submitted → validated → awaiting_review → resolved`  
`resolution`: `approved | rejected | withdrawn | stale`

`stale` means another proposal advanced the Scheduling Head first; the proposal must be rebased and revalidated (§10).

### Mission Revision

Immutable, created when a proposal is approved. `head_status`: `committed` (exists; not yet Scheduling Head because transitions are in progress) → `head` → `superseded`. Exactly one `head` per Mission.

### Operations and idempotency

`propose_revision`, `validate_proposal`, `approve_proposal`, `reject_proposal`, `withdraw_proposal`, `rebase_proposal`, `activate_revision` (commit plus head transition). Public idempotency uses the scoped actor/action/request_id contract. Proposal identity and expected base/head versions constrain semantic transition and optimistic concurrency; they do not replace request receipts. Committed revision creation and scheduling-head activation are distinct recorded transitions even if an admitted handler commits both atomically; neither starts a run.

## 11. Terminology constraint

Mission Invocation creates or attaches Missions. Mission Revision evolves an existing Mission. “Mutate Mission” may be retained as a user-facing command family, but its successful result must be an immutable successor Revision rather than an in-place mutation.

## 12. Remaining questions

Resolved in Round 3 (2026-09-07): state machines (§10a), Carry-Forward Eligibility (§5.1), nested-system transitions (§4.1–4.2), operation names and idempotency (§10a).

Public operation routes, request identity and validation envelopes are bound by the canonical SPECIFICATION and RUNTIME-CONTRACTS. Remaining payload implementation work follows the current implementation plan.
