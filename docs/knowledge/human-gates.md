---
type: Concept
title: Human Gates and the Human Task Service
description: How a Human Gate runs today as the mc.human_gate.v1 control activation over the common human_task rows, the one HumanTaskService that HTTP, MCP and the socket resolve through, the StageGraph gate stage and GoalDirected review behind the mp10 patches, and what the production launch path does not yet lower.
tags: [mission-control, durable-controls, human-task, human-gate, implementation]
---

# Human Gates and the Human Task Service

A [Human Gate](../../GLOSSARY.md) opens one [Human Task](../../GLOSSARY.md) per activation and
review round and waits on its attributed resolution; timeout is never approval (SPEC-03
"Explicit Human Gate", [ADR-0038](../adr/0038-two-origins-of-human-control-over-one-human-task-service.md),
`proposed`). The specified control vocabulary is in [durable controls](durable-controls.md).
Built by MP-10 and integrated 2026-10-09; native provider approvals (MP-11) are not integrated.

## Rules (domain)

`domain/programs/human_gate.py` holds pure rules only. `HumanGateSpec` is the lowered policy:
`gate_key`, `task_kind` (`APPROVAL | QUESTION | SELECTION | REVIEW | POLICY_OVERRIDE`), prompt,
reviewers, packet sources, `timeout_seconds`, `on_timeout`
(`keep_waiting | escalate | default_answer | stop`), an explicit `default_decision` (only with
`default_answer`), a `remediation_target` and `max_review_rounds` (at most 20). Decisions are the
`workflow_gate` row of `mc.approval_binding.v1` (`approve | deny | request_changes`);
`request_changes` is admitted only with a remediation target and a round left. A REVIEW task
resolves `review_accept | review_reject` with the review decision as payload; other kinds resolve
`approved | denied`. `HumanGateActivation` freezes the review packet (refs and digests); its task
id is a uuid5 of the activation and round, stored in the existing `mission_control.human_task`
row with kind `human_gate:<KIND>`. `decide_resolution` refuses `already_resolved`,
`task_expired`, `task_cancelled`, `not_reviewer`, `stale_version`, `packet_digest_mismatch`,
`decision_not_admitted`, `feedback_required` and `deadline_passed`; the same request id again is a
`duplicate`, not a second answer. A reviewer entry `r` is satisfied by principal `r` or a verified
grant `reviewer:r`. `outcome_for` maps resolved, expired and cancelled tasks to `accepted`,
`not_accepted`, `changes_requested`, `stopped_by_policy` or `cancelled`, and `bind_outcome`
refuses an outcome naming another task, round or packet digest.

## Persistence and events

`adapters/postgres/human_tasks/repository.py` writes the 0005 `human_task` and
`human_resolution` rows and appends `human_task.created`, `human_task.resolved`,
`human_task.expired`, `human_task.cancelled` or `human_task.escalated` with its outbox row in the
same transaction (`canonical.append_events`). The alias projection publishes
`human_task.created` as `human_task.opened` ([events and commands](events-and-commands.md)). No
migration was added.

## The control activation

`adapters/temporal/workflows/human_gate.py` registers `mc.human_gate.v1` (`HumanGateWorkflow`)
on both family workers (`adapters/temporal/registration/workflows.py`). It runs activity
`human_gate.open`, then waits with `workflow.wait_condition` on a `resolution_committed` hint
signal or a bounded poll (default 300 s), holding no activity or cognition slot, and re-reads the
task through `human_gate.observe` on every wake. At the deadline the repository applies the
timeout policy; cancelling the activation runs `human_gate.cancel`. Activities are composed as
`HumanGateActivities(PostgresHumanTaskRepository, settlement=GateReservationSettlement(run_control))`
in `adapters/temporal/deployment_composition.py`.

- **StageGraph** (`workflows/stagegraph.py`, patch `mp10-stagegraph-human-gate`): a stage named
  in `StageGraphRunInput.human_gates` starts the activation as a child instead of an operation,
  releases its admitted reservation against zero usage (`human_gate.settle_stage_reservation`),
  and reports the outcome as the stage result; `request_changes` runs the declared remediation
  cycle before any consumer of the gate is admitted.
- **GoalDirected** (`workflows/goal_directed.py`, patch `mp10-goal-human-review`): with
  `GoalDirectedRunInput.human_review`, a verified completion proposal opens one review on the
  verified outputs and the verifier decision (`domain/programs/human_review.py`);
  `request_changes` continues the loop with the feedback as an untrusted executor instruction and
  every counter carries on; a rejection fails the run. Runs without a declared gate never reach
  either patch.

## The one service and its transports

`application/human_tasks/service.py::HumanTaskService` is tenant scoped and is the only mutation
path. A resolution commits first; then `TemporalHumanGateWake` signals the activation, and a lost
signal only delays until the next poll. The API composes one service per tenant as
`app.state.mission_control_human_task_services` (`bootstrap/api.py`). Transports:

- HTTP `GET /human-tasks`, `GET /human-tasks/{id}`, `POST /human-tasks/{id}/resolutions` under
  `/v1/applications/{application_id}` (`interfaces/http/human_tasks.py`; 404, 403, 409 or 422 by
  code; 503 `human_tasks_unavailable` when not composed).
- Socket event `resolve_human_task` on `/missions` (`interfaces/socketio/commands.py`): the HTTP
  body plus `application_id` and `human_task_id`; `UNSUPPORTED_OPERATION` when not composed
  ([mission stream](mission-stream.md)).
- MCP tools `mission_human_task_list|get|resolve` (`interfaces/mcp/human_task_tools.py`).

## Gaps, reported not resolved

- The production launch does not lower manifest gates: `prepare_bound` in
  `application/programs/service.py` passes neither `human_gates` nor `human_review`, and
  `stagegraph_human_gates` / `goal_human_review` (`application/programs/human_gates.py`) have no
  production caller. Only tests set them, so a submitted manifest with a `human_gate` node does
  not open a task in the configured deployment.
- `register_human_task_tools` is defined but no served MCP server calls it; the tools are proven
  only in `tests/unit/human_tasks/test_human_task_interfaces.py`.
- The manifest `HumanTaskSpec` cannot express a remediation target or a default answer, so
  `gate_spec_from_manifest` refuses `default_answer` and lowered gates admit only
  `approve | deny`.
- No `missionctl human-task` group; the spec lifecycle value `claimed` is not used.

# Citations

- Spec: [SPEC-03](../specs/multi-provider-2026-10/SPEC-03-human-control.md);
  [MP-10](../specs/multi-provider-2026-10/issues/MP-10-execute-human-gate-nodes-and-review-with-feedback-paths.md).
- ADR: [0038](../adr/0038-two-origins-of-human-control-over-one-human-task-service.md) (proposed).
- Code: [gate rules](../../src/mission_control/domain/programs/human_gate.py),
  [goal review](../../src/mission_control/domain/programs/human_review.py),
  [service](../../src/mission_control/application/human_tasks/service.py),
  [family wiring](../../src/mission_control/application/programs/human_gates.py),
  [repository](../../src/mission_control/adapters/postgres/human_tasks/repository.py),
  [control activation](../../src/mission_control/adapters/temporal/workflows/human_gate.py),
  [HTTP](../../src/mission_control/interfaces/http/human_tasks.py),
  [MCP tools](../../src/mission_control/interfaces/mcp/human_task_tools.py),
  [socket forwarding](../../src/mission_control/interfaces/socketio/commands.py).
- Tests: [gate rules](../../tests/unit/human_tasks/test_human_gate_rules.py),
  [interfaces](../../tests/unit/human_tasks/test_human_task_interfaces.py),
  [manifest lowering](../../tests/unit/human_tasks/test_manifest_gate_lowering.py),
  [tasks in PostgreSQL](../../tests/integration/postgres/test_human_gate_tasks_postgres.py),
  [restart, deadlines and lost wakes on Temporal](../../tests/integration/temporal/test_mp10_human_gate_restart.py).
