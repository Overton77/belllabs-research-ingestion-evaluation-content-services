---
id: WP-BP-020
title: Implement canonical GoalDirected runtime
status: draft
implements: [REQ-BP-GD-001, REQ-BP-GD-002, REQ-BP-GD-003, REQ-BP-GD-004, REQ-BP-GD-005, REQ-BP-GD-006, REQ-BP-GD-007, REQ-BP-GD-008, REQ-BP-GD-009, REQ-BP-GD-010]
governed_by: [ADR-0003, SPEC-BP-GOAL-DIRECTED]
contracts: [CON-BP-GOAL-DIRECTED-V1, CON-BP-GOAL-HANDOFF-V1, CON-BP-GOAL-VERIFICATION-V1, CON-CP-ASYNC-SUBAGENT-V1]
blocked_by: [WP-CP-030, WP-CP-040, WP-CP-045]
github_issue: null
evidence: [docs/migrations_instructions/evidence_v2/WP-BP-020/]
---

# Implement canonical GoalDirected runtime

## Outcome

GoalDirected executes bounded Deep Agent iterations with immutable revisions, fresh-session handoffs, independent verification, context rollover, governed async subgoals, deterministic convergence, and authoritative terminalization proposals.

## Current implementation baseline

Preserve the existing `GoalDirectedInterpreter`, Temporal prior art, goal handoff identities, and Deep Agents experiments where compatible. Remove any completion or fork authority from model/session state.

## Requirements implemented

All `REQ-BP-GD-*` requirements.

## Architectural seams affected

Blueprint/compiler, interpreter, Temporal GoalDirected workflow, generic operation iterations, verifier operations, handoff/context services, async delegation classifier, snapshots/forks, projections, and tests.

## Compatibility and migrations

Version goal revision, handoff, verifier, and interpreter-decision schemas. Preserve old goal/checkpoint evidence as lineage but do not infer new authority from it.

## Acceptance criteria

- [ ] Objective envelope and revision bounds reject expansion.
- [ ] Every significant iteration is independently durable.
- [ ] Completion always requires an independent accepted verifier.
- [ ] Fresh empty sessions resume from typed handoff/context slices.
- [ ] Context rollover preserves protected facts and semantic identity.
- [ ] Convergence precedence is deterministic.
- [ ] Async subgoals are classified and governed correctly.
- [ ] Broader changes route to control/fork/linked-run paths.

## Qualification and evidence

Run `QUAL-BP-GOAL-DIRECTED-CONVERGENCE` with replay, worker/session loss, verifier disagreement, rollover, blockers, limits, cancellation, async children, and Continue-As-New.

## Failure and rollback posture

Version workflow/interpreter contracts and retain compatibility workers/readers for admitted prior versions. Failed new revisions never mutate the accepted prior revision.

## Documentation and traceability updates

Publish decision tables, handoff/verifier schemas, tests, histories, and accepted evidence.

## Non-goals

Unbounded autonomous objectives or a separate Workflow Type per iteration/subgoal.

## Drift guards

Deep Agent completion, plan, prompt, checkpoint, or child output cannot assign convergence or terminality.

