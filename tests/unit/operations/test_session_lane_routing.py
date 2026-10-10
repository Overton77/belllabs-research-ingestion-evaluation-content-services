"""Recovery 2026-10-09: claude/codex units route like Cursor units, and the MP-05 capacity
policy travels with every operation request.

A family builds `OperationWorkflowRequest` through `OperationHeartbeatPolicy`; a Session Lane
unit (cursor, claude, codex) must be segment-driven with bounds from its own binding and run
`lane.*` on the binding's task queue, and the deployment's `LimitWaitPolicy` must reach the
workflow's capacity planner instead of the hard-coded defaults.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from mission_control.domain.execution.contracts import (
    SESSION_LANE_RUNTIMES,
    OperationWorkflowRequest,
)
from mission_control.domain.execution.heartbeats import OperationHeartbeatPolicy
from mission_control.domain.execution.usage_admission import (
    LimitWaitPolicy,
    ProviderLimitSignal,
    plan_limit_response,
)
from tests.unit.claude.fixtures import claude_operation
from tests.unit.codex.support import codex_operation
from tests.unit.operations.test_operation_execution import operation_request

QUEUE = "mc-claude-lane-test"


def _request(operation: object, policy: OperationHeartbeatPolicy) -> OperationWorkflowRequest:
    return OperationWorkflowRequest(
        semantic_attempt_id=operation.identity.semantic_key,  # type: ignore[attr-defined]
        execution_generation=1,
        operation_kind="bound_operation",
        operation=operation,
        heartbeat_timeout_seconds=policy.timeout_for(operation),  # type: ignore[arg-type]
        segments=policy.segments_for(operation),  # type: ignore[arg-type]
        capacity_wait=policy.capacity_wait,
    )


def test_claude_and_codex_units_are_segment_driven_on_their_lane_queue() -> None:
    policy = OperationHeartbeatPolicy(bound_seconds=45)
    assert {"cursor", "claude", "codex"} == SESSION_LANE_RUNTIMES
    for operation in (claude_operation(task_queue=QUEUE), codex_operation()):
        request = _request(operation, policy)
        provider = operation.provider_binding
        assert provider is not None
        assert request.segment_driven
        assert request.segments is not None
        assert request.segments.heartbeat_timeout_s == 45
        assert request.segments.busy_wait_s == provider.budgets.wall_clock_s
        assert request.activity_task_queue == provider.task_queue


def test_a_deep_agents_unit_keeps_the_governed_path_by_default() -> None:
    request = _request(operation_request(), OperationHeartbeatPolicy())
    assert not request.segment_driven and request.segments is None


def test_the_capacity_policy_travels_and_is_absent_from_old_payloads() -> None:
    operation = claude_operation(task_queue=QUEUE)
    absent = _request(operation, OperationHeartbeatPolicy())
    assert absent.capacity_wait is None
    assert "capacity_wait" not in absent.model_dump(mode="json")
    strict = LimitWaitPolicy(max_waits=0)
    carried = _request(operation, OperationHeartbeatPolicy(capacity_wait=strict))
    assert carried.capacity_wait == strict
    restored = OperationWorkflowRequest.model_validate(carried.model_dump(mode="json"))
    assert restored.capacity_wait == strict

    # The planner applies the carried bounds: no waits allowed means reject, where the
    # defaults would wait for the provider's reset.
    now = datetime(2026, 10, 9, 12, tzinfo=UTC)
    signal = ProviderLimitSignal(
        lane_profile="claude_agent_sdk",
        kind="rate_limited",
        resets_at=now + timedelta(minutes=5),
        source="test",
    )
    deadline = now + timedelta(hours=2)
    assert plan_limit_response(signal, now=now, deadline=deadline).disposition == "wait"
    assert (
        plan_limit_response(signal, now=now, deadline=deadline, policy=strict).disposition
        == "reject"
    )
