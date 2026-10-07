---
type: Concept
title: Durable controls and human tasks
description: How Event Wait, Timer, Human Gate and Proof Gate hold a wait as program nodes, how a human task is resolved by an attributed action, and what the code persists today.
tags: [mission-control, durable-controls, human-task, implementation]
---

# Durable controls and human tasks

A [Durable Control](../../GLOSSARY.md) is a program node that waits rather than works.
It has identity, the shared activation lifecycle, receipts and a terminal outcome, and
other nodes may depend on it. Gate conditions on stages are predicates over the facts
these nodes record; the compiler may synthesize a control from an inline gate
declaration, but the compiled program always contains it explicitly, so there are no
hidden waits (`../mission-control-general/workflow-types/05-EXECUTORS_AND_DURABLE_CONTROLS.md`,
section 6; `01-STAGE_GRAPH.md`, section 6).

## The four controls

- Event Wait: an `event_type`, a match predicate over the typed payload, an optional
  timeout with `on_timeout: skip | fail | keep_waiting | escalate`, and a mode of
  `once` or `rearm { max_firings, cooldown, expiry }`. Each firing writes an event
  receipt artifact; in `rearm` mode each firing creates a new activation of the
  dependent subprogram under the same revision (section 6.1).
- Timer: `duration | until`; completes `accepted` when the instant passes. Recurrence
  is a mission-level run schedule, never a node, because a run must be able to
  complete (section 6.2).
- Human Gate: raises exactly one human task and waits on its resolution (section 6.3).
- Proof Gate: gates a consumer's release on evidence recorded elsewhere, through an
  `evidence_ref` or `verification_intent_ref`, a `required_disposition` and
  `on_reject: skip | fail | wait_for_remediation | escalate`. A completion contract
  gates a producer's acceptance; a proof gate gates a consumer's release (section 6.4).

## Human tasks

A [Human Task](../../GLOSSARY.md) has a kind of `APPROVAL`, `QUESTION`, `SELECTION`,
`REVIEW` or `POLICY_OVERRIDE` (`CREDENTIAL` is deferred), a prompt, context refs,
options for `SELECTION`, an assignee policy and a timeout with
`on_timeout: keep_waiting | escalate | default_answer | stop`. Its lifecycle is
`open → claimed → resolved | expired | cancelled`. Its resolution is one attributed
action: `approved`, `denied`, `answered`, `selected`, `review_accept`,
`review_reject` or `overridden`; the answer is an artifact, and an answer cannot
smuggle a policy change (section 6.3).

A [Review Decision](../../GLOSSARY.md) of `approve`, `reject`, `request_changes` or
`abstain` is the payload of a review-kind task, not a second state machine: the task
still resolves as `review_accept` or `review_reject`. No remote UI can resolve a task
without an attributed action, and every embedded review surface has a plain text or
link fallback (ADR-0016). Required human review is never satisfied by notification
delivery (ADR-0008).

The gate's outcome follows the resolution: `approved`, `answered`, `selected`,
`review_accept` and `overridden` give `accepted`; `denied` and `review_reject` give
`not_accepted`; `expired` follows `on_timeout` (`stop` gives `stopped_by_policy`,
`default_answer` gives `accepted` with the default recorded).

## Implemented today

There are no Event Wait, Timer, Human Gate or Proof Gate program nodes. What exists:

- Run-level waits. `WaitCondition.kind` in
  `src/mission_control/domain/policies/contracts.py` is `dependency`, `timer`,
  `approval`, `resource`, `budget`, `external_result` or `operator_reconciliation`;
  the reducer in `src/mission_control/domain/policies/reducer.py` sets and satisfies
  waits, and `satisfy_wait` is one of the three family boundary commands
  (`FamilyBoundaryCommandKind` in `src/mission_control/domain/programs/contracts.py`,
  delivered through [lifecycle](lifecycle.md)). An `operator_reconciliation` wait
  blocks terminalization until the typed `reconcile_unit` command resolves an
  in-doubt unit; see [recovery](recovery.md).
- Durable human-task rows. `mission_control.human_task` and `human_resolution` in
  `packages/mission-control-db-contract/component/migrations/0005_commands_effects_recovery.sql`
  hold a typed request packet, `kind`, `assignee_scope`, `deadline_at`, `on_timeout`
  and a lifecycle check of `open | resolved | expired | cancelled`; a resolution
  stores the attributed `actor_ref`, the `answer` JSON with its digest and the
  `expected_task_version`, exactly once per task. Two writers use them:
  `PostgresDecisionRepository` in
  `src/mission_control/adapters/postgres/runtime/stage3_kernel_repository.py`
  (kind prefix `runtime_decision:`, `DecisionRequest`/`DecisionResponse` from
  `src/mission_control/domain/graph_runtime/kernel.py`) and
  `PostgresRedisApprovalGateway` in `src/mission_control/adapters/realtime/postgres_redis.py`
  (kind `runtime_approval`, expiry on deadline). Both are tested in
  `tests/integration/postgres/test_stage3_kernel_postgres_integration.py` and
  `tests/unit/runtime/test_runtime_decisions_stage3.py`.
- An `event_receipt` table (`wait_key`, `source_event_key`, `matched_at`,
  `rearm_ordinal`) in the same migration, with no writer in `src/`.

The schema does not enforce the spec's `claimed` lifecycle value, the task-kind
vocabulary or the resolution vocabulary; the code records `approved` booleans and
`answered` statuses on its own request types rather than the seven resolution
actions. Proof Gate has no code or table beyond `evidence_assessment`
(see [completion](completion.md)).

# Citations

- `../mission-control-general/workflow-types/05-EXECUTORS_AND_DURABLE_CONTROLS.md` (sections 6-7).
- `../mission-control-general/workflow-types/01-STAGE_GRAPH.md` (section 6, gate conditions).
- `../mission-control-general/general-mission-control/SPECIFICATION.md` ("Public skill CLI MCP and dashboard contract", human decisions).
- [ADR-0008](../adr/0008-ordered-commands-with-urgent-stop-fence.md),
  [ADR-0016](../adr/0016-required-ui-surfaces-with-fallback.md).
- [Policy contracts](../../src/mission_control/domain/policies/contracts.py) (`WaitCondition`),
  [reducer](../../src/mission_control/domain/policies/reducer.py),
  [program contracts](../../src/mission_control/domain/programs/contracts.py) (`FamilyBoundaryCommandKind`).
- [Decision repository](../../src/mission_control/adapters/postgres/runtime/stage3_kernel_repository.py),
  [approval gateway](../../src/mission_control/adapters/realtime/postgres_redis.py),
  [kernel decision contracts](../../src/mission_control/domain/graph_runtime/kernel.py).
- [Stage-3 kernel PostgreSQL proof](../../tests/integration/postgres/test_stage3_kernel_postgres_integration.py),
  [decision tests](../../tests/unit/runtime/test_runtime_decisions_stage3.py).
- [`0005_commands_effects_recovery.sql`](../../packages/mission-control-db-contract/component/migrations/0005_commands_effects_recovery.sql).
