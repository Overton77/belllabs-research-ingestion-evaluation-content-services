---
type: Workflow Specification
title: Mission Control — Evaluator Optimizer
description: "Evaluator Optimizer raises the quality of one deliverable through repeated production and independent evaluation. Its route is fixed — produce, evaluate, decide — which distinguishes it from Goal Loop (the next action…"
tags: [mission-control, spec, workflow]
---
# Mission Control — Evaluator Optimizer

Architecture: [General Mission Control](../general-mission-control/SPECIFICATION.md). Runtime/API binding: [RUNTIME-CONTRACTS.md](../general-mission-control/RUNTIME-CONTRACTS.md). Historical milestone references do not override the [current delivery plan](../general-mission-control/IMPLEMENTATION.md).

**Status:** canonical behavior contract; earlier interview dates below are provenance
**Last updated:** 2026-10-02 (general runtime consolidation)
**Workflow-system kind:** `evaluator_optimizer`

## 1. Purpose

Evaluator Optimizer raises the quality of **one deliverable** through repeated production and independent evaluation. Its route is fixed — produce, evaluate, decide — which distinguishes it from Goal Loop (the next action is adaptive) and from a Revisit Region (an arbitrary known route repeats). Use it when quality must rise against a known Rubric and the producer should receive structured feedback each round.

## 2. Settled shared laws (inherited)

Accepted in [`00-EXECUTION_PROGRAM_MODEL.md`](./00-EXECUTION_PROGRAM_MODEL.md) and the canonical specification:

- Evaluator Optimizer pursues existing Objectives and cannot introduce independently governed Goals.
- Producer and evaluator definitions may be subprograms.
- An Optimization Round is one producer-and-evaluator exchange, distinct from Retry, Revisit, Iteration, and Continuation.
- Producer, verifier, and evaluator independence must survive Continuation Transfer.
- The system boundary declares typed inputs, Output Projection, Objective references, acceptance behavior, and budget scope.
- Evaluation relationships belong here, not to generic Stage Graph edges.
- LLM-as-judge is a typed arm with a Rubric version and recorded judge identity and is never the sole verifier of code or extraction.
- Later probabilistic judgment cannot reverse an earlier deterministic failure.
- Lifecycle, Phase, and Terminal Outcome are three separate fields (`00 §6`).

## 3. Roles and records (accepted 2026-09-07)

| Term | Meaning |
|---|---|
| **Producer** | The subprogram or Agent Executor that produces a Candidate each round. |
| **Evaluator** | A subprogram, Deterministic Executor, `direct_model` judge, or Human `REVIEW` that produces an Evaluation Report for a Candidate. One or more per system. |
| **Candidate** | The deliverable of one round: an Artifact set carrying `round_number`. |
| **Rubric** | A versioned Artifact of dimensions, scales, and blocking-finding definitions, pinned by the committed Revision. |
| **Evaluation Report** | Typed evaluator output; one Evidence Assessment of the Candidate against the Rubric. |
| **Best Candidate** | Highest aggregate-scoring Candidate when the system ends without acceptance; lineage, not output. |

```text
EvaluationReport {
  candidate_ref
  rubric_ref                 // Artifact ref with version
  evaluator_identity         // Program Node key, execution id, Agent Session id, model, runtime
  scores[]: { dimension, value, scale_ref }
  findings[]: { severity: blocking | major | minor, statement, locator? }
  disposition: accept | revise | reject | abstain
  confidence
}

ProducerRoundInput {
  round_number
  remaining_rounds
  prior_candidate_refs[]     // usually the immediately previous Candidate
  evaluation_reports[]       // the previous round's reports
  objective_and_criteria     // from the enclosing contract
}
```

## 4. Evaluator independence (accepted 2026-09-07)

Distinct **Program Node identity** and distinct **execution identity** are mandatory. Distinct **Agent Session** is mandatory (no shared context between producer and evaluator). Beyond that, independence is policy-selectable:

`evaluator_independence: node | session | model | runtime` — default `session`.

- Self-evaluation (same Program Node) fails validation.
- Same Agent Session fails at runtime.
- For code or extraction deliverables, the evaluator set must include at least one Deterministic Executor check.

## 5. Acceptance is computed by Mission Control (accepted 2026-09-07)

The evaluator's `disposition` is an input, not the verdict. Mission Control evaluates the system's Completion Contract acceptance expression over the round's aggregated Evaluation Reports — for example `min(score) ≥ threshold ∧ count(findings.severity = blocking) = 0` — and records the result. The acting producer proposes; the evaluator reports; policy decides.

### 5.1 Multiple evaluators

```text
evaluators[]: EvaluatorDefinition
aggregation: all_accept | any_accept | mean_gte { threshold } | min_gte { threshold } | weighted { weights[], threshold }
```

Default is a single evaluator. Panels are the intended home for "deterministic checks + LLM judge + optional human `REVIEW`", executed deterministic-first.

## 6. Rubric versioning (accepted 2026-09-07)

The Rubric is pinned by the committed Revision. Changing it is a Revision Proposal. In-flight rounds finish under the old Rubric (`finish_existing`); subsequent rounds use the new Scheduling Head. Scores across Rubric versions are not comparable, and the ledger records the version on every Evaluation Report so no cross-version comparison is implied.

## 7. Round continuity (accepted 2026-09-07)

Each round is a **fresh producer activation** receiving `ProducerRoundInput` as typed input. Session reuse across rounds is a Session Policy optimization (`brief_reuse`) permitted only below Context Health soft thresholds and never across a Compaction. Rounds are therefore individually attributable and the disposable-session law holds.

## 8. Round lifecycle and decisions (accepted 2026-09-07)

Each Optimization Round is an activation with the shared Activation Lifecycle and these Phases:

`producing → evaluating → deciding`

The **Cycle Decision** (`00 §6.4`) at `deciding`:

| Decision | Trigger |
|---|---|
| `accept` | Acceptance expression passes. |
| `continue` | Not accepted, rounds remain, and improvement patience is not exhausted. |
| `stop` | `max_rounds` reached (`threshold_not_reached`) or `no_improvement_patience` exhausted (`no_progress`). |
| `await_input` | Evaluator abstained with `on_evaluator_abstain: escalate_human`. |
| `revise` | The producer or evaluator reports that the Rubric, Action Space, or structure must change. |

Improvement is measured on the aggregate score: a round improves iff aggregate strictly increases by at least `min_improvement_delta` (default 0).

## 9. Failures, rejection, abstention (accepted 2026-09-07)

| Situation | Treatment |
|---|---|
| Evaluator infrastructure failure | Retry the evaluator within the **same round** (`max_evaluator_retries`); not a new round. |
| Evaluator `reject` or `revise` | Next round; the report is producer input. |
| Evaluator `abstain` | `on_evaluator_abstain: escalate_human (default) \| treat_as_reject \| stop`; `escalate_human` opens a `REVIEW` Human Task and parks the round in `waiting`. |
| Producer infrastructure failure | Retry policy for the producer activation. |
| Provenance mismatch or forged report | Quarantine and reconciliation. |

## 10. Governors and cap behavior (accepted 2026-09-07)

```text
governors {
  max_rounds
  no_improvement_patience     // rounds without aggregate improvement
  min_improvement_delta
  round_budget
  system_budget
  wall_clock
  max_evaluator_retries
}
accept_best_at_cap: boolean   // default false
```

At `max_rounds` without acceptance the Terminal Outcome is `threshold_not_reached`; the Best Candidate is recorded as lineage. It is projected as output only when `accept_best_at_cap: true` was declared **and** the Completion Contract's hard gates (deterministic checks, blocking findings, proof gates) still pass. Remaining budget never justifies another round.

## 11. Terminal Outcomes

Shared base (`00 §6.3`) plus `threshold_not_reached`. Only `accepted` projects outputs.

## 12. Journal

Evaluator Optimizer keeps no Journal (`00 §6.5`). A producer that is itself a Goal Loop keeps its own.

## 13. Questions under interview

Resolved in Round 2: an Optimization Round is an activation and uses the unified outcome vocabulary (`00 §6.3`); a round normally ends `accepted` (acceptance expression passed) or `not_accepted` (feedback produced, system continues), and the *system* activation carries `threshold_not_reached` / `no_progress`.

Resolved in Round 3: **Knowledge Services owns the Rubric Artifact schema** (it owns evaluation and verification contracts). Mission Control pins a Rubric by Artifact reference and version and stores Evaluation Reports as Evidence Assessments; it never defines scoring semantics. Revision activation during a round follows `07 §4.1–4.2` (a Round is the transition granularity).

None open.
