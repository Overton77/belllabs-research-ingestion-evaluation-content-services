"""MP-09: a busy cloud is a scheduling condition (FIXTURE Cloud API).

`429` before any work is accepted surfaces `ProviderCapacityLimited` (journaled `declined`,
one request, no retry here: MP-05/06 wait on a Temporal timer and the next segment sends
once). `409 agent_busy` (this agent's own active run) stays `wait_then_send` per ADR-0030:
the send is journaled `declined/busy` and the workflow polls `lane.status` at the boundary.
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

from mission_control.application.execution.harness.dispatch import ProviderCapacityLimited
from tests.fixtures.cursor_cloud import cloud_stack
from tests.unit.cursor.support import (
    heid_of,
    identity,
    lanes_for,
    send_request,
    signals,
    turn,
)

PROFILE = "cursor_cloud"


async def test_a_429_on_create_is_a_capacity_condition_and_the_next_segment_creates_once(
    tmp_path: Path,
) -> None:
    stack = cloud_stack(tmp_path, api_changes={"rate_limited_creates": 1, "retry_after_s": 7})
    lanes = lanes_for(stack)
    scope, heid = (
        identity(stack, PROFILE).request_scope,
        identity(stack, PROFILE).harness_execution_id,
    )
    before = datetime.now(UTC)
    with pytest.raises(ProviderCapacityLimited) as limited:
        await lanes.service.turn(turn(stack, PROFILE), signals(lanes, stack, PROFILE))
    signal = limited.value.signal
    assert signal.lane_profile == PROFILE and signal.kind == "rate_limited"
    assert signal.source == "cursor_cloud.http_429" and signal.native_code == "rate_limited"
    assert signal.resets_at is not None
    assert 5 <= (signal.resets_at - before).total_seconds() <= 9, "Retry-After is the reset"
    assert signal.fixture is False
    # One request, no agent, no retry loop inside the lane or the service.
    assert len(stack.api.creates) == 1 and stack.api.rate_limit_refusals == 1
    assert stack.api.agents == {}
    state = await lanes.states.load(scope, heid)
    assert state is not None
    create = state.dispatch("create", f"{heid}:1:turn:1")
    assert create is not None and (create.phase, create.reason) == ("declined", "capacity")
    assert state.native_session_ref is None

    # What the workflow does after the MP-05 timer: run the segment again.
    result = await lanes.service.turn(turn(stack, PROFILE), signals(lanes, stack, PROFILE))
    assert result.done and result.operation_result is not None
    assert result.operation_result["status"] == "completed"
    assert len(stack.api.creates) == 2 and len(stack.api.agents) == 1
    state = await lanes.states.load(scope, heid)
    assert state is not None
    create = state.dispatch("create", f"{heid}:1:turn:1")
    assert create is not None and create.phase == "acknowledged" and create.attempts == 2


async def test_a_429_on_a_later_run_is_capacity_and_nothing_is_sent_twice(tmp_path: Path) -> None:
    stack = cloud_stack(tmp_path)
    lanes = lanes_for(stack)
    first = await lanes.service.turn(turn(stack, PROFILE), signals(lanes, stack, PROFILE))
    assert first.done
    heid = heid_of(stack, PROFILE)
    agent_id = next(iter(stack.api.agents))
    stack.harness.stage(heid, stack.operation)
    stack.api.rate_limited_runs = 1
    with pytest.raises(ProviderCapacityLimited) as limited:
        await stack.harness.send_turn(send_request(stack, PROFILE, heid, agent_id, 2))
    assert limited.value.signal.kind == "rate_limited"
    assert stack.api.created_runs == [stack.api.run_id], "the refused send created no run"
    posts = [r for r in stack.api.requests if r.method == "POST" and r.url.path.endswith("/runs")]
    assert len(posts) == 1, "one refused POST; the lane never retries on its own"
    # After the (workflow-timed) wait the same turn is sent exactly once.
    accepted = await stack.harness.send_turn(send_request(stack, PROFILE, heid, agent_id, 2))
    assert accepted.native_turn_ref is not None and accepted.status == "accepted"
    assert len(stack.api.created_runs) == 2


async def test_agent_busy_stays_wait_then_send_with_one_request_and_no_run(tmp_path: Path) -> None:
    stack = cloud_stack(tmp_path)
    lanes = lanes_for(stack)
    assert (await lanes.service.turn(turn(stack, PROFILE), signals(lanes, stack, PROFILE))).done
    heid = heid_of(stack, PROFILE)
    agent_id = next(iter(stack.api.agents))
    stack.harness.stage(heid, stack.operation)
    stack.api.busy = True
    busy = await stack.harness.send_turn(send_request(stack, PROFILE, heid, agent_id, 2))
    assert busy.status == "busy" and busy.native_turn_ref is None
    posts = [r for r in stack.api.requests if r.method == "POST" and r.url.path.endswith("/runs")]
    assert len(posts) == 1 and stack.api.created_runs == [stack.api.run_id]
    assert stack.harness.describe().delivery_semantics["queue_instruction"] == "wait_then_send"
    assert stack.harness.describe().delivery_semantics["resume"] == "wait_then_send"
