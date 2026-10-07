---
type: Concept
title: Completion and acceptance
description: Why execution completion, output validity, evidence disposition, activation acceptance, mission acceptance, domain admission and publication are seven different facts, and how the reducer settles a run today.
tags: [mission-control, completion, acceptance, evidence, implementation]
---

# Completion and acceptance

Mission Control must distinguish execution completion, output validity, evidence
disposition, activation acceptance, mission acceptance, downstream domain admission
and publication, and must never accept a result because an agent, provider, Temporal
workflow or tool reports success
(`../mission-control-general/general-mission-control/SPECIFICATION.md`, "Authority and scope").

## Seven facts, not one status

| Fact | Who records it | Vocabulary |
| --- | --- | --- |
| Execution completion | the attempt, from the lane's native end | `succeeded`, `failed` with a failure class, `cancelled` |
| Output validity | registration and schema or integrity checks on declared outputs | registered artifact, or `not_accepted(outputs_missing)` |
| Evidence disposition | a registered assessment capability | a typed [Disposition](../../GLOSSARY.md), e.g. `satisfied`, `unsatisfied`, `inconclusive` |
| Activation acceptance | the completion contract of that node | `accepted`, `not_accepted` and the other terminal outcomes |
| Mission acceptance | the aggregate disposition over goals | `mission_accepted`, `not_accepted`, `abandoned`, `superseded` at closure |
| Domain admission | the application's domain service, by intent and receipt | receipt outcome `applied`, `rejected`, `noop`, `partial` |
| Publication | an explicit domain capability with its own policy and review | its own side-effect class and receipt |

A native `FINISHED` is never, by itself, `accepted`; execution outcome describes
execution, acceptance describes the contract decision
(`workflow-types/05`, section 3.4; `00`, section 6.5). Child mission success supplies
evidence but does not establish the parent's acceptance (`00`, section 2.4).

## Candidate, contract, decision

A [Completion Candidate](../../GLOSSARY.md) is the executor's claim: activation and
attempt identity, named outputs bound to artifact refs, evidence refs, an optional
criteria mapping and concise notes with no chain of thought (`05`, section 3.4). If a
turn ends without one, `missing_output_policy` allows a bounded follow-up turn.

A [Completion Contract](../../GLOSSARY.md) is a closed, typed expression over `all`,
`any`, schema and integrity checks, named assessment dispositions, human resolutions
and criterion results. It cannot run SQL, Python, JavaScript or natural-language
predicates; semantic judgment goes through a registered assessment capability and
rubric that return a typed disposition, which keeps acceptance replayable, auditable
and immune to prompt injection (ADR-0010). The compiler checks the referenced schemas;
the kernel evaluates the typed disposition under the pinned policy.

The [Completion Decision](../../GLOSSARY.md) is Mission Control's computed verdict on a
candidate. An evaluator's `accept` and a human's `approved` are inputs to that
expression, not verdicts (`04`, section 5; [durable-controls](durable-controls.md)).
An [Evidence Assessment](../../GLOSSARY.md) is attributable: assessor identity,
capability version, rubric version and policy version travel with its disposition.

## Implemented today

Nothing evaluates a closed completion-contract AST. The run-level verdict is the
reducer's `_terminal_outcome` in `src/mission_control/domain/policies/reducer.py`: a
`TerminalizationProposal` (`src/mission_control/domain/policies/contracts.py`) is
accepted only when it binds the current run version, workflow type digest, obligation
revision and evidence frontier, when `required_obligations_accepted` matches the
authoritative `AcceptedObligationEvidence` (each carrying an
`accepted_by_authority_ref`), when `valid_output_refs` equals the authoritatively
accepted outputs, when no wait, link or required asynchronous subordinate remains, and
when budget and effects are settled. The result is a `RunOutcome` of `completed`,
`partially_completed` (degradable failures with some valid output), `failed` or
`cancelled`. Tested in `tests/unit/run_control/test_run_control.py`
(`test_terminalization_binds_authoritatively_accepted_obligation_evidence`).

Per family, Stage Graph results are observed and decided as
`ResultDecision.ADMIT | REJECT | QUARANTINE` (`ResultDispositionProposal`) and a
`StageGraphCompletionProposal` can terminalize only with required obligations
accepted, no pending dependencies and no open producer liability
(`src/mission_control/domain/programs/contracts.py`,
`tests/unit/orchestration/test_stagegraph_v2.py`). `GoalDirected` carries an
independent `GoalVerificationResult` whose `decision` is
`accepted | rejected | revision_required | repair_required` and a
`GoalTerminalizationProposal` with `proposed_outcome: complete | partial_or_fail | fail`.
Workspace files become artifacts only through captured candidates and immutable
registration (`CapturedWorkspaceCandidate` in
`src/mission_control/domain/execution/contracts.py`; promotion is covered in
[capabilities](capabilities.md) and [recovery](recovery.md)). The coordinator reads a
`TerminalWorkflowCompletion` view (`src/mission_control/domain/coordinator/launch.py`)
that reports execution completion, not mission acceptance.

The common schema already holds the target records: `completion_candidate`
(candidate JSON with digest, artifact and evidence refs, immutable) and
`completion_decision` (`policy_digest`, `contract_digest`, `result_disposition` of
`accepted | rejected | needs_more_work | abandoned`, requirement and result refs,
reason codes) in `0003_execution.sql`, and `evidence_assessment` (assessor identity,
capability, rubric and policy versions, disposition
`satisfied | unsatisfied | inconclusive`) in `0004_capability_artifacts.sql`. No
`src/` writer targets them yet.

## Specified only

The closed AST, per-node completion contracts, criteria mapping, mission acceptance
as an aggregate disposition over goals, carry-forward of accepted work and the
`accepted | not_accepted | ...` activation outcome vocabulary are spec only. The
code's run outcomes and the schema's activation outcomes
(`succeeded | failed | cancelled | skipped | superseded`) predate the unified
vocabulary in `00` section 6.3; see [workflow-systems](workflow-systems.md).

# Citations

- `../mission-control-general/general-mission-control/SPECIFICATION.md` ("Authority and scope", "Definition and compiler contracts", "Common execution semantics").
- `../mission-control-general/workflow-types/00-EXECUTION_PROGRAM_MODEL.md` (sections 2.4, 6.3, 6.5),
  `04-EVALUATOR_OPTIMIZER.md` (section 5), `05-EXECUTORS_AND_DURABLE_CONTROLS.md` (sections 3.4, 4.2).
- [ADR-0010](../adr/0010-completion-expressions-closed-typed-ast.md).
- [Reducer](../../src/mission_control/domain/policies/reducer.py),
  [policy contracts](../../src/mission_control/domain/policies/contracts.py).
- [Program contracts](../../src/mission_control/domain/programs/contracts.py),
  [execution contracts](../../src/mission_control/domain/execution/contracts.py),
  [coordinator result contracts](../../src/mission_control/domain/coordinator/launch.py).
- [Run-control tests](../../tests/unit/run_control/test_run_control.py),
  [Stage Graph tests](../../tests/unit/orchestration/test_stagegraph_v2.py),
  [GoalDirected tests](../../tests/unit/orchestration/test_wp_bp_020_goal_directed.py).
- [`0003_execution.sql`](../../packages/mission-control-db-contract/component/migrations/0003_execution.sql),
  [`0004_capability_artifacts.sql`](../../packages/mission-control-db-contract/component/migrations/0004_capability_artifacts.sql).
