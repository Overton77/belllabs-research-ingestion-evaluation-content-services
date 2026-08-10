---
id: WP-BP-020
title: Implement canonical GoalDirected runtime
status: ready
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

Blueprint/compiler, interpreter, Temporal GoalDirected workflow, generic operation iterations,
verifier operations, handoff/context services, the accepted async-delegation classifier port,
snapshots/forks, projections, and tests.

## Authorized implementation slice

- Replace only the GoalDirected blueprint and policy region in
  `app/domain/control_plane/contracts.py`, its GoalDirected compilation branch in
  `app/domain/control_plane/compiler.py`, and the GoalDirected
  revision/handoff/verifier/decision region in `app/domain/orchestration/contracts.py`.
- Replace `app/domain/orchestration/goal_directed.py` with a pure proposal interpreter that never
  uses `terminal` as family-owned state.
- Implement family mechanics only in `app/temporal/workflows/goal_directed.py`; executor iterations
  and verifiers are separate `OperationWorkflow` children.
- Implement typed fresh-session handoff/context rollover application services. Consume the
  `WP-CP-045` classifier from `app/domain/operation_execution/delegation.py` and add
  GoalDirected-specific routing fixtures; do not create or modify a second classifier owner.
- Delete direct goal activities, OpenAI Agents SDK goal/session runtime, and Agent Server macro goal
  graph at acceptance.
- Add revision-boundary, verifier-independence, fresh-session, rollover, convergence precedence,
  blocker/no-progress, async-subgoal, fork-routing, replay, cancellation, and continuation tests.

## Replacement and migrations

Implement new goal revision, handoff, verifier, and interpreter-decision schemas. Port useful test
fixtures only. Prototype goal/session/checkpoint records are disposable and cannot be interpreted
as canonical authority. Move the executable mechanics from the legacy
`app/temporal/goal_directed_workflow.py` module into the frozen
`app/temporal/workflows/goal_directed.py` owner and delete the legacy module at acceptance; a
re-export shim is not the accepted runtime.

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

The qualification also includes a credential-gated live acceptance test. It must submit through
the BellLabs API, pass through transactional admission and `BellLabsRunWorkflow`, execute the real
`GoalDirectedWorkflow`, and run separately bound executor and independent-verifier
`OperationWorkflow` children with real LLM calls through the accepted Deep Agents adapter.
Evidence must identify the API request/run, root/family/operation Workflow IDs, exact model and
binding, accepted iteration outputs, verifier decision, any revision/convergence decision, and the
run-control reducer's terminal outcome.

The live test complements deterministic unit/property, contract, API integration, Temporal
time-skipping, replay, recovery, cancellation, and continuation tests; it cannot replace them.
Use the smallest exact Deep Agent binding needed to prove GoalDirected semantics. MCP servers,
Skills, executable sandboxes, snapshots, and the complete advanced-capability combination remain
outside this package's live-test requirement and are proved cohesively by `WP-CP-050`.

## Failure and rollback posture

Version the new workflow/interpreter contracts and retain their captured histories for future
replay. No legacy compatibility workers/readers are required. Failed new revisions never mutate
the accepted prior revision.

## Documentation and traceability updates

Publish decision tables, handoff/verifier schemas, tests, histories, and accepted evidence.

## Non-goals

Unbounded autonomous objectives or a separate Workflow Type per iteration/subgoal.

## Drift guards

Deep Agent completion, plan, prompt, checkpoint, or child output cannot assign convergence or terminality.
