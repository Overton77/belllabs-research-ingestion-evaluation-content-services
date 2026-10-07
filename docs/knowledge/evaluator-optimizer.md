---
type: Concept
title: Evaluator Optimizer
description: How one deliverable improves through bounded rounds of independent evaluation against a pinned rubric, with acceptance computed by Mission Control; specified, not implemented.
tags: [mission-control, workflow-systems, evaluator-optimizer, specified-only]
---

# Evaluator Optimizer

[Evaluator Optimizer](../../GLOSSARY.md) raises the quality of one deliverable through
repeated production and independent evaluation. Its route is fixed, produce then
evaluate then decide, which separates it from a Goal Loop (the next action is
adaptive) and from a Stage Graph revisit region (a known route repeats)
(`../mission-control-general/workflow-types/04-EVALUATOR_OPTIMIZER.md`, section 1).

## Roles and records

The producer is the subprogram or Agent Executor that produces a candidate each
round. An evaluator is a subprogram, Deterministic Executor, direct-model judge or
human `REVIEW` task that produces an evaluation report. The [Rubric](../../GLOSSARY.md)
is a versioned artifact of dimensions, scales and blocking-finding definitions, pinned
by the committed revision; Knowledge Services owns its schema, and Mission Control
only pins it by reference and version (sections 3, 6 and 13).

An evaluation report carries the candidate and rubric refs, the evaluator identity
(node key, execution id, agent session, model, runtime), per-dimension scores,
findings with severity `blocking | major | minor`, a `disposition` of
`accept | revise | reject | abstain` and a confidence. Each round's producer receives
the prior candidate refs, the previous reports and the remaining round count as typed
input, in a fresh producer activation (sections 3 and 7).

## Independence

Producer and evaluator must have distinct program-node identity, distinct execution
identity and distinct agent sessions; `evaluator_independence` may tighten this to
`model` or `runtime`. Self-evaluation fails validation; a shared session fails at
runtime. For code or extraction deliverables the evaluator set must include at least
one deterministic check, executed before any model judge, and a later probabilistic
judgment never reverses an earlier deterministic failure (sections 2 and 4).

## Acceptance is computed, not reported

The evaluator's disposition is an input, not the verdict. Mission Control evaluates
the system's completion contract over the round's aggregated reports (for example
minimum score at or above a threshold and zero blocking findings) and records the
result. With several evaluators the aggregation is `all_accept`, `any_accept`,
`mean_gte`, `min_gte` or `weighted` (section 5). See [completion](completion.md) for
the closed expression form.

## Rounds, patience and abstention

An [Optimization Round](../../GLOSSARY.md) is one activation with phases
`producing → evaluating → deciding`. The cycle decision is `accept`, `continue`
(rounds remain and improvement patience is not exhausted), `stop` (`max_rounds`
reached gives `threshold_not_reached`; `no_improvement_patience` exhausted gives
`no_progress`), `await_input` or `revise`. A round improves only when the aggregate
score rises by at least `min_improvement_delta` (section 8).

Evaluator infrastructure failure retries within the same round; `reject` or `revise`
starts the next round with the report as producer input; `abstain` follows
`on_evaluator_abstain`: `escalate_human` (default, opens a `REVIEW` human task and
parks the round in `waiting`), `treat_as_reject` or `stop` (section 9). Governors are
`max_rounds`, `no_improvement_patience`, `min_improvement_delta`, `round_budget`,
`system_budget`, `wall_clock` and `max_evaluator_retries`. At the cap, the best
candidate is lineage; it is projected as output only when `accept_best_at_cap` was
declared and the contract's hard gates still pass. Remaining budget never justifies
another round (section 10). Changing the rubric is a revision proposal; in-flight
rounds finish under the old rubric (section 6, [revisions](revisions.md)).

## Implementation status

Evaluator Optimizer is specified and not implemented. Searching `src/` for
`evaluator_optimizer`, `EvaluatorOptimizer` and `optimization_round` returns nothing,
and neither the code's `WorkflowFamily` nor the schema's `program_node.behavior_kind`
has such a value. The nearest implemented relative is the `GoalDirected` verifier: a
`GoalVerifierPolicy` pins `rubric_ref`, `rubric_version` and `acceptance_version`
(`src/mission_control/domain/authoring/contracts.py`), the application layer refuses a
verifier result whose rubric or acceptance version differs from the admitted policy
(`src/mission_control/application/programs/goal_directed.py`), and the verifier
decision vocabulary is `accepted | rejected | revision_required | repair_required`
(`src/mission_control/domain/programs/contracts.py`, `GoalVerifierDecision`). That is
one independent verifier per iteration inside a goal loop, not an optimization system
with rounds, score aggregation, improvement patience or abstention handling.
Deterministic-first evaluator panels and the evidence-assessment table exist only as
schema (`evidence_assessment` in migration `0004_capability_artifacts.sql`).

# Citations

- `../mission-control-general/workflow-types/04-EVALUATOR_OPTIMIZER.md` (sections 1-13).
- `../mission-control-general/workflow-types/00-EXECUTION_PROGRAM_MODEL.md` (sections 5.4 and 6.4).
- `../mission-control-general/general-mission-control/SPECIFICATION.md` ("Common execution semantics").
- [ADR-0010](../adr/0010-completion-expressions-closed-typed-ast.md).
- [Authoring contracts](../../src/mission_control/domain/authoring/contracts.py) (`GoalVerifierPolicy`).
- [GoalDirected application service](../../src/mission_control/application/programs/goal_directed.py).
- [Program contracts](../../src/mission_control/domain/programs/contracts.py) (`GoalVerifierDecision`, `GoalVerificationResult`).
- [GoalDirected verifier tests](../../tests/unit/orchestration/test_wp_bp_020_goal_directed.py).
- [`evidence_assessment` table](../../packages/mission-control-db-contract/component/migrations/0004_capability_artifacts.sql).
