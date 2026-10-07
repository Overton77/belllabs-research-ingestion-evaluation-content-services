---
type: Concept
title: Parallel Swarm
description: Peers, not dependencies - how a swarm declares member variants, counts only accepted members, converges through a separate subprogram and stops at governors; specified, not implemented.
tags: [mission-control, workflow-systems, parallel-swarm, specified-only]
---

# Parallel Swarm

[Parallel Swarm](../../GLOSSARY.md) runs several units of work at the same time,
toward the same objectives, on the same immutable inputs, and then brings their results
together through one explicit convergence step. Its members are peers, not
dependencies: a Stage Graph runs two stages in parallel because neither depends on
the other; a swarm runs members because the work benefits from coverage, competition,
comparison, redundancy or judgment diversity
(`../mission-control-general/workflow-types/03-PARALLEL_SWARM.md`, section 1).

## Members and member variants

A swarm member's body is an executor or a subprogram. Members are declared in exactly
one of two forms, never mixed: a static list, or fan-out from a committed template
over a source output with an identity rule and a maximum cardinality (section 3.1).
Each member has a key, input bindings to immutable artifacts only, an optional
member-level completion contract and an importance of `required`, `optional` or
`best_effort`.

A [Member Variant](../../GLOSSARY.md) (`variant_key`, optional model, runtime,
instruction variant and input slice) is part of member identity and is recorded on
every member output, receipt and evaluation report. Experiments such as harness or
model comparison are swarms whose members differ by variant; there is no separate
experiment engine (section 3.2). Members share nothing but inputs: each has its own
workspace and agent sessions, and no member reads another's output during the swarm
(section 3.3).

## Member completion policy

The policy says how many accepted members the swarm needs before convergence:
`all`, `quorum { required_accepted }`, `first_accepted` or
`best_effort { min_accepted }`. `quorum` converges as soon as the k-th member is
accepted; `best_effort` waits for every member to reach a terminal state and then
checks the minimum. Only accepted members count. "First finished" never wins a race;
"first accepted" does, and a member whose contract rejects its output is
`not_accepted`, never retried as an infrastructure failure (sections 4.1 and 5).

Once the policy is satisfied the swarm is resolved and `on_resolution` decides the
stragglers: `cancel_remaining` after a grace period (default), `finish_best_effort`
(late acceptances are recorded but never enter convergence) or `finish_required`
(section 6). Feasibility is checked on every member terminal event: with k required,
a accepted and r not yet terminal, the policy is satisfiable only while a + r >= k;
otherwise the swarm ends `quorum_unreachable` and cancels the rest. `fail_fast` is
meaningful only with `all` (section 7).

## Convergence

[Convergence](../../GLOSSARY.md) is a subprogram that receives the accepted member
outputs (key, variant, artifact refs, dispositions) as one typed input and produces
the swarm's outputs. It may be a Deterministic Executor (`coalesce`,
`agreement_check`), an Evaluator Optimizer or direct-model ranking with a recorded
judge identity, a human `SELECTION` task, or a nesting of these. Convergence has its
own completion contract, and only its projected outputs become the swarm's outputs;
member outputs remain inspectable lineage with their variant attached (sections 4.2
and 10). A convergence that rejects leaves the swarm `not_accepted` without silently
re-running members.

## Governors, phases and outcomes

Governors are `max_members`, `concurrency`, `member_budget`, `swarm_budget`,
`member_timeout`, `swarm_wall_clock`, `straggler_grace` and `max_member_retries`;
exhaustion is the governed outcome `governor_exhausted` (section 8). Phases are
`expanding`, `running` and `converging`; terminal outcomes are the shared base plus
`quorum_unreachable` (section 9). A swarm keeps no journal; a member that is itself a
goal loop keeps its own (section 11). Revision activation while a swarm is running is
owned by [revisions](revisions.md).

## Implementation status

Parallel Swarm is specified and not implemented. A search of `src/` for `swarm`,
`member_variant`, `MemberVariant` and `parallel_swarm` returns no hits; the only
match in the repository is `tests/experiments/test_dynamic_research_swarm.py`, which
exercises an `experiments.dynamic_research_swarm` package, not the kernel. The code's
admitted families are only `StageGraph` and `GoalDirected`
(`src/mission_control/domain/programs/contracts.py`), and the common schema's
`program_node.behavior_kind` has no swarm value
(`packages/mission-control-db-contract/component/migrations/0002_authoring.sql`).
The spec places Swarm in the complete general release, after the Stage Graph and
Goal Loop vertical (`general-mission-control/SPECIFICATION.md`, "Authority and
scope"). See [workflow-systems](workflow-systems.md) for the full map.

# Citations

- `../mission-control-general/workflow-types/03-PARALLEL_SWARM.md` (sections 1-12).
- `../mission-control-general/workflow-types/00-EXECUTION_PROGRAM_MODEL.md` (sections 5.1 and 6.3).
- `../mission-control-general/general-mission-control/SPECIFICATION.md` ("Authority and scope", "Common execution semantics").
- [Program contracts](../../src/mission_control/domain/programs/contracts.py) (`WorkflowFamily`).
- [Experiment, not kernel](../../tests/experiments/test_dynamic_research_swarm.py).
- [`program_node` table](../../packages/mission-control-db-contract/component/migrations/0002_authoring.sql).
