---
type: Concept
title: Workflow systems as program-node behaviors
description: How the four workflow systems, the executors and the durable controls compose one recursive program, and which of them the code runs today.
tags: [mission-control, workflow-systems, programs, implementation]
---

# Workflow systems as program-node behaviors

A mission's [Program](../../GLOSSARY.md) is a recursive tree of program nodes, and
every node has exactly one [Behavior](../../GLOSSARY.md): a workflow system, an
executor, a durable control or a child mission invocation. The spec fixes this
classification because the earlier flat `strategy` union mixed composite control
structures with atomic execution and release conditions
(`../mission-control-general/workflow-types/00-EXECUTION_PROGRAM_MODEL.md`, sections 3-4).

## The four workflow systems

Workflow systems compose child work and own their own lifecycle and governors:

| System | Shape of control | Spec |
| --- | --- | --- |
| [Stage Graph](../../GLOSSARY.md) | release of typed stages by dependencies, inputs and gates | `workflow-types/01-STAGE_GRAPH.md` |
| [Goal Loop](../../GLOSSARY.md) | observe, propose, authorize, act, assess, decide until a criterion or governor stops it | `workflow-types/02-GOAL_LOOP.md` |
| [Parallel Swarm](parallel-swarm.md) | bounded member variants run as peers, then one convergence step | `workflow-types/03-PARALLEL_SWARM.md` |
| [Evaluator Optimizer](evaluator-optimizer.md) | produce, evaluate against a rubric, decide, over bounded rounds | `workflow-types/04-EVALUATOR_OPTIMIZER.md` |

A workflow system pursues the enclosing mission's objectives and cannot introduce
independently governed goals; only a child mission invocation does that. Systems nest
explicitly (a stage may hold a goal loop; swarm members and convergence may be
subprograms), and same-type nesting needs its own control scope and governors
(`00`, section 5.1). Only Goal Loop keeps a journal (`00`, section 6.6).

## Executors and durable controls

Executors do the atomic work: an Agent Executor delegates bounded cognition to an
admitted lane; a Deterministic Executor runs a registered executor kind with typed
input and output schemas and no model judgment (`workflow-types/05`, sections 3-4).
[Durable controls](durable-controls.md) wait instead of working: Event Wait, Timer,
Human Gate, Proof Gate. They are real program nodes with identity, lifecycle and
receipts; the compiler may synthesize one from an inline gate declaration, but the
compiled program always contains it explicitly (`05`, section 6). Pause, cancel,
retry and continuation are commands and cross-cutting policies, never node behaviors.

## Vocabulary every system reuses

Every activation records three separate fields: `lifecycle`
(`pending → ready → running ⇄ waiting → completed`), a system-specific `phase`, and a
`terminal_outcome` that is absent until completion (`00`, section 6). Retry, revisit,
iteration, optimization round and continuation are distinct counters with distinct
governors (ADR-0011). Execution success is never acceptance; see
[completion](completion.md).

## Implemented today

The code runs two program families, declared as
`WorkflowFamily = Literal["StageGraph", "GoalDirected"]` in
`src/mission_control/domain/programs/contracts.py` and admitted as `BlueprintFamily`
in `src/mission_control/domain/coordinator/launch.py`. The admitted root rejects any
other family before launch ("unsupported Mission Control workflow family").

- Stage Graph: `StageGraphInterpreter` in `src/mission_control/domain/programs/interpreter.py`
  interprets an immutable `StageGraphBlueprint` from
  `src/mission_control/domain/authoring/contracts.py`, built compactly by
  `src/mission_control/domain/authoring/stagegraph_builder.py`. Release, result
  decisions and completion proposals are tested in
  `tests/unit/orchestration/test_stagegraph_v2.py`.
- Goal Loop subset: `GoalDirectedInterpreter` in
  `src/mission_control/domain/programs/goal_directed.py`, runtime contracts in
  `goal_directed_runtime.py`, driven by
  `src/mission_control/application/programs/goal_directed.py`. The `GoalDirectedBlueprint`
  carries `GoalConvergencePolicy` (no-progress and repeated-blocker patience) and
  `GoalVerifierPolicy` (an independent verifier with a pinned rubric and acceptance
  version). Tests: `tests/unit/orchestration/test_wp_bp_020_goal_directed.py`.
- Both families launch governed operations through
  `src/mission_control/application/programs/service.py` and the Temporal workflows
  described in [execution](execution.md).

The compile step in `src/mission_control/application/authoring/service.py` accepts
only these two blueprint kinds, and the common schema's `program_node.behavior_kind`
check admits `stage_graph`, `goal_directed`, `stage`, `goal_iteration`, `operation`,
`gate`, `wait` and `join`
(`packages/mission-control-db-contract/component/migrations/0002_authoring.sql`).

## Specified only

Parallel Swarm and Evaluator Optimizer have no interpreter, blueprint, family or
schema behavior kind. Searching `src/` for `swarm`, `member_variant` and
`evaluator_optimizer` finds nothing; the only `swarm` hit in the repository is
`tests/experiments/test_dynamic_research_swarm.py`, which imports an `experiments`
package rather than the kernel. The registered deterministic executor kinds of `05`
section 4.1 (`verification_dispatch`, `artifact_register`, `coalesce`, ...) are absent.
Durable-control nodes exist only as run-level wait conditions and durable human-task
rows; see [durable-controls](durable-controls.md). The spec scopes Swarm, Evaluator
Optimizer and child mission invocation to the complete release and requires a runtime
to reject unsupported behavior before a run starts
(`general-mission-control/SPECIFICATION.md`, "Authority and scope").

# Citations

- `../mission-control-general/workflow-types/00-EXECUTION_PROGRAM_MODEL.md` (sections 3-6),
  `01-STAGE_GRAPH.md`, `02-GOAL_LOOP.md`, `05-EXECUTORS_AND_DURABLE_CONTROLS.md`.
- `../mission-control-general/general-mission-control/SPECIFICATION.md` ("Authority and scope", "Common execution semantics").
- [ADR-0011](../adr/0011-distinct-retry-counters-and-retry-classifier.md).
- [Program contracts](../../src/mission_control/domain/programs/contracts.py),
  [Stage Graph interpreter](../../src/mission_control/domain/programs/interpreter.py),
  [GoalDirected interpreter](../../src/mission_control/domain/programs/goal_directed.py).
- [Authoring contracts](../../src/mission_control/domain/authoring/contracts.py),
  [compile service](../../src/mission_control/application/authoring/service.py),
  [program service](../../src/mission_control/application/programs/service.py).
- [Stage Graph tests](../../tests/unit/orchestration/test_stagegraph_v2.py),
  [GoalDirected tests](../../tests/unit/orchestration/test_wp_bp_020_goal_directed.py).
- [`program_node` table](../../packages/mission-control-db-contract/component/migrations/0002_authoring.sql).
