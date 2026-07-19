# 04 — Implement durable GoalDirected orchestration

**What to build:** Interpret a frozen GoalDirected blueprint in Temporal so an admitted run can pursue a bounded objective through independently verified iterations while preserving fixed scope, authority, inputs, budgets, and acceptance.

**Blocked by:**
- 02 — Implement transactional Run admission, lifecycle, budgets, and domain events

**Status:** ready-for-agent

**GitHub:** https://github.com/Overton77/belllabs-research-ingestion-evaluation-content-services/issues/4


**Read first:** [Local control-plane ticket index](INDEX.md)

## Implementation guidance

- Share the same accepted lifecycle, budget, operation, and activity boundaries as StageGraph while keeping GoalDirected stop precedence explicit.
- Represent executing-agent claims and proposed Goal Revisions as typed proposals; route all accepted decisions through independent verification and application authority.
- Keep continuing-session versus fresh-from-handoff behavior as an authored blueprint policy rather than an SDK default.

## Verification approach

Use bounded fixture goals and deterministic fake operation/verifier activities. Prove every limit and forbidden scope expansion through public outcomes and immutable revisions.

## Explicit non-goals

- Agent-authored objective expansion, self-verified completion, concrete research goals, and workflow-specific convergence thresholds.
- Linked child execution and production model tuning.

## Acceptance criteria

- [ ] An admitted GoalDirected fixture executes bounded iterations with declared operation classes, session/handoff policy, workspace/snapshot policy, and reservations.
- [ ] Independent verification gates completion and emits typed continue, repair, degrade, stop, fork, or escalation proposals.
- [ ] Invariant or authority breach, hard-budget exhaustion, verified completion, irrecoverable failure, no-progress, repeated-blocker, and iteration limits follow deterministic precedence.
- [ ] Every Goal Revision is immutable and records parent, evidence, unmet obligations, author, deciding authority, and applicability.
- [ ] Attempts to broaden objective, acceptance, inputs, authority, budget, or prohibited work are rejected or routed to a declared control revision, fork, linked run, or new run.
- [ ] Retries and replay preserve iteration and revision identities without duplicating provider effects, charges, or accepted decisions.

## Source basis

- Durable Blueprint Orchestration and Linked Runs
- Human Upgrade System Context

The source specifications and Human Upgrade System glossary were available locally when this ticket was authored. This ticket is intentionally self-contained for a fresh implementation agent.
