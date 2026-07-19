# 03 — Implement durable StageGraph orchestration

**What to build:** Interpret a frozen application-authored StageGraph in Temporal so admitted runs execute deterministic dependencies, joins, bounded parallelism, and evaluated semantic cycles without making Temporal or agents workflow-definition authorities.

**Blocked by:**
- 02 — Implement transactional Run admission, lifecycle, budgets, and domain events

**Status:** ready-for-agent

**GitHub:** https://github.com/Overton77/belllabs-research-ingestion-evaluation-content-services/issues/3


**Read first:** [Local control-plane ticket index](INDEX.md)

## Implementation guidance

- Interpret only the frozen StageGraph contract from ticket 01 and accepted decisions from ticket 02; do not add a runtime-authored workflow DSL.
- Separate deterministic Workflow code from activity ports for database, runtime, workspace, artifact, evaluation, and lifecycle interactions.
- Model semantic cycles explicitly around an acyclic stage graph, with stable identities distinct from Temporal retry attempts.

## Verification approach

Use Temporal's time-skipping environment with the real interpreter and fake nondeterministic ports. Verify scheduling and emitted public commands rather than private Workflow state.

## Explicit non-goals

- GoalDirected execution, linked-run composition, concrete workflow topology, and provider/runtime implementation.
- Worker autoscaling, production task-queue tuning, or broker selection.

## Acceptance criteria

- [ ] An admitted StageGraph fixture schedules only runnable stages and honors explicit dependencies, joins, skip/completion rules, fairness, concurrency, and reservations.
- [ ] Temporal Workflow code performs deterministic coordination only; all nondeterministic I/O occurs through idempotent activities and application services.
- [ ] Local waits and pauses affect scoped work while aggregate lifecycle is derived through the control-plane reducer.
- [ ] Temporal activity attempts, operation attempts, stage cycles, workflow cycles, and execution epochs have distinct identities and counters.
- [ ] Accepted stage evaluations create bounded cycles with new objectives, reservations, workspace namespaces, bindings, artifacts, evaluations, and handoff lineage.
- [ ] Accepted whole-workflow evaluations rerun only the invalidated descendant subgraph and reuse unaffected immutable outputs by reference.
- [ ] Replay and worker restart do not query mutable stores from Workflow code or duplicate operations, charges, artifacts, links, or lifecycle facts.

## Source basis

- Durable Blueprint Orchestration and Linked Runs
- Human Upgrade System Context

The source specifications and Human Upgrade System glossary were available locally when this ticket was authored. This ticket is intentionally self-contained for a fresh implementation agent.
