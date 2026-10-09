---
type: Workflow Specification
title: Mission Control — Parallel Swarm
description: "Parallel Swarm is the workflow system for running several units of work at the same time, toward the same Objectives, on the same immutable inputs, and then bringing their results together through one explicit…"
tags: [mission-control, spec, workflow]
---
# Mission Control — Parallel Swarm

Architecture: [General Mission Control](../general-mission-control/SPECIFICATION.md). Runtime/API binding: [RUNTIME-CONTRACTS.md](../general-mission-control/RUNTIME-CONTRACTS.md). Historical milestone references do not override the [current delivery plan](../general-mission-control/IMPLEMENTATION.md).

**Status:** canonical behavior contract; earlier interview dates below are provenance
**Last updated:** 2026-10-02 (general runtime consolidation)
**Workflow-system kind:** `parallel_swarm`

## 1. Purpose and mental model

Parallel Swarm is the workflow system for running several units of work **at the same time, toward the same Objectives, on the same immutable inputs**, and then bringing their results together through one explicit convergence step.

The defining property is that the members are *peers*, not *dependencies*. In a Stage Graph, two Stages run in parallel because neither depends on the other; they are usually doing different things. In a Parallel Swarm, members exist because the work benefits from breadth, redundancy, competition, or comparison:

| Scenario | Why a swarm | Completion policy | Convergence |
|---|---|---|---|
| Research: assess 12 candidate sources for one claim | Coverage — one member per source | `all` (or `best_effort(min 10)`) | deterministic coalesce into one evidence table |
| Code: three different approaches to the same failing test | Competing hypotheses | `first_accepted` | none needed beyond selecting the winner |
| Experiment: same task on Cursor vs Claude vs Codex | Comparison — one Member Variant per runtime | `all` | Evaluator Optimizer or `direct_model` ranking, sealed via evaluation |
| Reliability: five independent extractions of a metric, accept when three agree | Redundancy | `quorum(3)` | deterministic agreement check |
| Review: two independent reviewers then a human picks | Judgment diversity | `all` | Human `SELECTION` |

Everything else in this document is the machinery that makes those five rows safe, bounded, and inspectable.

## 2. Settled shared laws (inherited)

Accepted in [`00-EXECUTION_PROGRAM_MODEL.md`](./00-EXECUTION_PROGRAM_MODEL.md) and the canonical specification:

- Parallel Swarm pursues existing Objectives and cannot introduce independently governed Goals.
- Member and Convergence definitions may be subprograms (executors or nested workflow systems).
- Same-type nesting requires a distinct control scope and explicit governors.
- The swarm boundary declares typed inputs, Output Projection, Objective references, acceptance behavior, and budget scope.
- Every activation has a system-execution identity, lifecycle, local governor counters, allocated budget, child executions, event cursor, and Terminal Outcome.
- Declared parallelism does not bypass real worker, runtime, tenant, resource, or budget capacity.
- Dynamic fan-out creates activations from a committed template, source Artifact binding, deterministic identity rule, and maximum cardinality; it never mutates the committed program.
- Agent Sessions are disposable; long-running member work uses Compaction and Continuation Transfer.
- Execution completion, acceptance, admission, and publication remain distinct.
- No child-to-parent token stream; members communicate through Artifacts and the Domain Ledger only.
- Lifecycle, Phase, and Terminal Outcome are three separate fields (`00 §6`).

## 3. Structure (accepted 2026-09-07)

### 3.1 Members

A **Swarm Member** (name accepted Round 2; rejected: worker, arm, participant, branch) is one unit of parallel work governed by the swarm. Its body is an executor or a subprogram. Members are declared in exactly one of two forms; the two forms are not mixed inside one swarm (a mixed need is two swarms inside a Stage Graph):

```text
members:
  StaticMembers   { list: SwarmMemberDefinition[] }
| FanOutMembers   { source_output, member_template: SwarmMemberDefinition, identity_rule, max_members }

SwarmMemberDefinition {
  member_key                 // stable within the swarm; for fan-out, derived by identity_rule from the source item digest
  variant: MemberVariant
  body: Executor | Subprogram
  input_bindings[]           // immutable Artifact references only
  completion_contract?       // defaults to the swarm's member_completion_contract
  importance: required | optional | best_effort
}
```

### 3.2 Member Variant

`MemberVariant { variant_key, model?, runtime?, instruction_variant_ref?, input_slice_ref? }` is part of member identity and is recorded on every member output, receipt, and Evaluation Report. Experiments (`harness_comparison`, `model_comparison`, `compaction_comparison`) are swarms whose members differ by variant; there is no separate experiment engine.

### 3.3 Isolation

Members share nothing but immutable inputs. Each member has its own workspace or sandbox and its own Agent Session(s). No member may read another member's outputs during the swarm. Cross-member exchange happens only in Convergence or in a following Stage. This is the inter-agent law applied within one Mission.

## 4. Two separate policies (accepted 2026-09-07)

### 4.1 Member Completion Policy

How many *accepted* members the swarm needs before Convergence:

| Value | Meaning | Convergence starts when |
|---|---|---|
| `all` | Every member must be accepted. | all members accepted |
| `quorum { required_accepted: k }` | At least `k` accepted; extra members are surplus. | the `k`-th acceptance |
| `first_accepted` | Exactly one accepted member is needed. | the first acceptance |
| `best_effort { min_accepted: k }` | Wait for every member to reach a terminal state (or time out), then converge if at least `k` were accepted. | all members terminal |

`quorum` converges *as soon as* `k` are accepted; `best_effort` waits for everyone and then checks the minimum. `fail_fast` is meaningful only with `all`.

### 4.2 Convergence

**Convergence** (name accepted Round 2; rejected: fan-in, coalesce, merge, reduce) is a subprogram that receives `AcceptedMemberOutputs[]` (member key, variant, output Artifact refs, dispositions) as one typed input and produces the swarm's outputs. It may be:

- a Deterministic Executor (coalesce, agreement check, table merge);
- an Evaluator Optimizer or `direct_model` ranking/selection with recorded judge identity;
- a Human `SELECTION` task; or
- a nested subprogram combining these.

Convergence has its own Completion Contract. The candidate spec's names map as: `speculative_race` = `first_accepted` + `on_resolution: cancel_remaining`; `map_reduce` = `all` + deterministic Convergence; `evaluator_fan_in` = `quorum|all` + evaluator Convergence.

## 5. Member acceptance (accepted 2026-09-07)

Every member has a Completion Contract and passes the same distinct checkpoints as a Stage: produced → valid → verified → accepted. Only **accepted** members count toward the Member Completion Policy. "First finished" never wins a race; "first accepted" does. A member whose contract rejects its output is `not_accepted` and never retried as an infrastructure failure.

## 6. Resolution and surplus members (accepted 2026-09-07)

When the Member Completion Policy is satisfied the swarm is *resolved*. Members still running are handled by:

| `on_resolution` | Behavior |
|---|---|
| `cancel_remaining` (default) | Cancel after `straggler_grace`; partial outputs remain inspectable lineage and are never projected. |
| `finish_best_effort` | Let them finish; late acceptances are recorded but do not enter Convergence and cannot delay it. |
| `finish_required` | Members with `importance: required` finish and must be accepted before Convergence; others are cancelled. |

## 7. Failure propagation (accepted 2026-09-07)

1. Member execution failure follows Retry policy first (`max_member_retries`).
2. After retries, the member is `execution_failed`; quality rejection is `not_accepted`. Both reduce feasibility identically.
3. Feasibility: with `k` required acceptances, `a` accepted so far, and `r` members not yet terminal, the policy is satisfiable iff `a + r ≥ k`. The swarm continues while satisfiable.
4. The moment the policy is unsatisfiable, the swarm records Terminal Outcome `quorum_unreachable` and cancels remaining members.
5. `fail_fast: true` (only with `all`) cancels siblings immediately on the first member failure or rejection instead of waiting for unsatisfiability to be proven.

## 8. Governors (accepted 2026-09-07)

```text
governors {
  max_members            // hard cap including fan-out; validation rejects a static list above it
  concurrency            // members released at once; the rest queue in `ready`
  member_budget          // per member
  swarm_budget           // whole activation, including Convergence
  member_timeout
  swarm_wall_clock
  straggler_grace        // delay after resolution before cancel_remaining fires
  max_member_retries
}
```

There is no `min_members` beyond what the Member Completion Policy implies. Governor exhaustion is `governor_exhausted`, a governed outcome.

## 9. Lifecycle, Phases, and Terminal Outcomes (accepted 2026-09-07)

Lifecycle is the shared Activation Lifecycle (`00 §6.2`). Phases:

| Phase | Meaning |
|---|---|
| `expanding` | Resolving the member list (static validation or fan-out from the source Artifact). |
| `running` | Members executing under the concurrency governor; feasibility checked on every member terminal event. |
| `converging` | Member Completion Policy satisfied; Convergence subprogram active. |

Terminal Outcomes: shared base (`00 §6.3`) plus `quorum_unreachable`.

- `accepted` — Convergence's Completion Contract accepted; outputs projected.
- `not_accepted` — Convergence rejected. Accepted member outputs are retained as lineage; the swarm does not silently re-run members. Remediation, if wanted, is a Revisit Region or enclosing Goal Loop.
- `quorum_unreachable` — §7.

## 10. Outputs

Only Convergence outputs selected by Output Projection become outputs of the swarm. Member outputs — accepted, rejected, cancelled, or late — remain inspectable lineage with their Member Variant attached.

## 11. Journal

Parallel Swarm keeps no Journal (`00 §6.5`). A member that is itself a Goal Loop keeps its own.

## 12. Questions under interview

Resolved in Round 2: names; member outcomes use the unified vocabulary (`00 §6.3`) — a member ends `accepted | not_accepted | execution_failed | cancelled | skipped | superseded` or, when its body is a workflow system, that system's outcomes.

1. Revision activation while a swarm is `running` (Round 3, owned by `07`).
