---
type: Workflow Specification
title: Mission Control — Stage Graph
description: "Stage Graph is the dependency-directed workflow system. It governs a set of Stages whose release is determined by explicit relationships, predicates, and policy rather than by list position or agent discretion."
tags: [mission-control, spec, workflow]
---
# Mission Control — Stage Graph

Architecture: [General Mission Control](../general-mission-control/SPECIFICATION.md). Runtime/API binding: [RUNTIME-CONTRACTS.md](../general-mission-control/RUNTIME-CONTRACTS.md). Historical milestone references do not override the [current delivery plan](../general-mission-control/IMPLEMENTATION.md).

**Status:** canonical behavior contract; earlier interview dates below are provenance
**Last updated:** 2026-10-02 (general runtime consolidation)
**Workflow-system kind:** `stage_graph`

## 1. Purpose

Stage Graph is the dependency-directed workflow system. It governs a set of Stages whose release is determined by explicit relationships, predicates, and policy rather than by list position or agent discretion.

Stage Graph is commonly, but not necessarily, the root of a Mission's execution program.

## 2. Settled structural laws

- A Stage is a Program Node directly governed by one Stage Graph.
- A Stage may contain an executor, durable control, Child Mission Invocation, or nested workflow system.
- Objective trees remain separate from the graph; every Stage declares the Objectives it serves.
- Nested workflow systems are explicit. A Stage does not acquire another system's semantics through miscellaneous flags.
- Every Stage and nested system has typed inputs and projected outputs.
- Committed graph definitions are immutable; structural evolution creates a successor Mission Revision.

## 3. Relationship model

Stage Graph uses distinct relationship contracts:

- **Dependency Edge** — participates in downstream Stage release
- **Data Binding** — maps typed outputs to inputs
- **Gate Condition** — qualifies release using proof, human, event, or policy state
- **Lineage Relationship** — records derivation, supersession, and historical provenance outside scheduling logic

Evaluation relationships belong to Evaluator Optimizer contracts rather than generic graph edges.

A required Data Binding creates an input-availability prerequisite automatically; it does not require a duplicate Dependency Edge. A normal binding also requires producer Stage Acceptance. A Provisional Binding deliberately relaxes acceptance while preserving provisional status. Dependency Edges express additional ordering, acceptance, or control relationships.

## 4. Execution behavior

1. Materialize the committed graph for one Stage Graph activation.
2. Evaluate release conditions for pending Stages.
3. Release eligible Stages under concurrency, importance, budget, and policy limits.
4. Record each Stage activation and terminal outcome.
5. Propagate typed outputs and dispositions.
6. Continue until the graph reaches an accepted terminal condition, blocks durably, or exhausts a governor.

Default graphs are acyclic. A cyclic graph must declare bounded revisit semantics explicitly. Revisit, Retry, Goal Loop Iteration, Optimization Round, and Continuation remain distinct.

## 5. Stage release

A Stage becomes ready only when:

1. its explicit dependency expression is satisfied;
2. every required input binding resolves;
3. its Gate Conditions pass;
4. budget and policy permit release; and
5. every enclosing workflow system is active.

The default dependency expression is `all`. Alternatives such as `any`, quorum, and conditional release are explicit. Predicates evaluate canonical typed facts and recorded dispositions, never agent prose.

## 6. Lifecycle and outcomes

Stage activations and the Stage Graph activation both use the shared Activation Lifecycle (`00 §6.2`):

`pending → ready → running ⇄ waiting → completed`

Terminal Outcome is recorded separately using the shared vocabulary (`00 §6.3`; unified 2026-09-07, Round 2). For a Stage the common values are `accepted | not_accepted | execution_failed | cancelled | skipped | superseded`; a Stage whose body is a nested workflow system may also end in that system's outcomes (`governor_exhausted`, `no_progress`, `revision_required`, `stopped_by_policy`). The Stage Graph activation adds `stalemate`.

Stage Graph Phases (accepted Round 2):

| Phase | Meaning |
|---|---|
| `releasing` | Evaluating release conditions and releasing eligible Stages under governors. |
| `draining` | Nothing further is releasable; waiting for running Stages to finish. Stalemate is detected here. |

`waiting` is an expected durable wait. A typed blocker is diagnostic state, not an ambiguous terminal lifecycle value.

One Stage Graph activation reaches execution completion when every reachable required Stage has a terminal disposition and no required release condition remains unresolved. Optional and best-effort branches retain their outcomes without necessarily preventing completion. Execution completion does not imply graph, Objective, Goal, or Mission acceptance.

Statically provable unreachable Stages fail compilation. Runtime Stalemate records the unsatisfied dependencies, inputs, gates, and policies, then pauses for intervention or Revision rather than reporting false completion.

Gate Conditions are predicates over recorded facts — dispositions, Human Task resolutions, Event Receipts (Round 2, Q16). The Durable Control nodes in `05-EXECUTORS_AND_DURABLE_CONTROLS.md` are what produce those facts and hold the wait. The compiler may synthesize a Durable Control node from an inline gate declaration, but the Compiled Program always contains the node explicitly; there are no hidden waits.

## 7. Concurrency and failure

All eligible Stages may execute concurrently subject to graph, subtree, runtime, tenant, resource, and budget limits. `serial` is a concurrency limit of one and does not change dependency semantics.

Real execution capacity comes from worker concurrency across processes, CPU cores, machines, and remote harnesses. Declared parallelism does not bypass available capacity, leases, fairness, or resource limits.

A failed required Stage does not automatically fail the graph. Independent branches continue. Dependent Stages become skipped only when their release requirements are conclusively impossible. Immediate sibling cancellation requires explicit `fail_fast`.

## 8. Revisit Regions

A cyclic portion of a Stage Graph is legal only as an explicit Revisit Region with:

- declared entry and exit;
- known Stage topology;
- deterministic termination predicate;
- progress observation;
- region round cap;
- per-Stage visit caps;
- no-progress limit;
- wall-clock and budget guards.

A Revisit Region is appropriate when the route is known but repetition may be required. Goal Loop is appropriate when choosing the next action is adaptive.

Supported termination modes are:

- `fixed_visits(n)`
- `until(predicate, max_visits)`
- `while(predicate, max_visits)`
- `until_no_progress(max_visits, patience)`

Every mode has a hard cap. Cap exhaustion is a governed terminal outcome rather than an infrastructure failure.

Progress is a typed observation such as verification-score improvement, newly accepted criteria, fewer unresolved findings, an accepted Artifact delta, a newly satisfied dependency, or an evaluator disposition. A region without a progress signal, fixed-count justification, or deterministic termination rule fails validation.

## 9. Dynamic fan-out

Runtime fan-out does not mutate the committed graph. The Mission Revision declares a typed expansion template, source Artifact binding, deterministic identity rule, and maximum cardinality.

Execution creates Stage activations from that declaration. Changing the template or exceeding the bound requires a successor Revision.

## 10. Outputs

Every produced output is stored and attributable to its Stage activation. Internal outputs become outputs of the Stage Graph only through explicit Output Projection.

Output production alone does not establish schema conformance, verification acceptance, Stage acceptance, Objective contribution, or Goal acceptance.

Normal downstream consumption waits for producer Stage Acceptance. A Provisional Binding may release speculative work once the bound Artifact exists and validates, but every derived output remains provisional. Provisional output cannot satisfy proof, publication, merge, or irreversible side-effect gates and must be invalidated or reconciled if its producer is rejected.

## 11. Stage Completion Contract

The Stage Completion Contract is the Stage form of the shared **Completion Contract** (the canonical specification vocabulary; accepted 2026-09-07). Every Stage declares:

- required and optional outputs;
- schema and integrity requirements;
- Verification Intents and testing requirements;
- acceptable dispositions;
- proof and human-review gates; and
- an acceptance expression.

The checkpoints remain distinct:

1. output produced;
2. output valid;
3. output verified;
4. Stage accepted;
5. Objective contribution accepted.

Verification may run as a nested subprogram or Knowledge Services operation. Stage Graph consumes the typed result without owning verification algorithms.

## 12. Retry and remediation

- Infrastructure failure may create a Retry.
- Invalid or quality-rejected output may release explicit Remediation.
- Review or abstention creates a durable wait or explicit non-acceptance according to policy.
- Provenance mismatch or forged receipt creates quarantine and reconciliation.

A Retry preserves the same Program Node definition and immutable logical inputs. It cannot silently change prompts, skills, inputs, or verification policy.

Remediation is visible structure: a repair Stage in a Revisit Region, nested Goal Loop, nested Evaluator Optimizer, or Coordinator-authored Revision. Rejecting evidence becomes typed remediation input.

## 13. Continuation during Revisit

Continuation does not erase Revisit identity. The default long-running path compacts execution state into a validated Continuation Checkpoint and transfers it into a fresh Agent Session. Each Revisit remains a distinct Stage activation with its own inputs, outputs, verification, and lineage.

## 14. Questions under interview

1. Dependency-expression schema and conditional-branch semantics.
2. Graph-level acceptance policy and proof aggregation.
3. Detailed Compaction, Continuation Checkpoint, and Transfer contracts.
4. Revision activation while a Stage Graph and nested systems are running.
