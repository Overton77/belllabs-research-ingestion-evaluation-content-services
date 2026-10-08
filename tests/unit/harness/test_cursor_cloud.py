"""FT-G5: the `cursor_cloud` Lane Profile against a Cloud Agents API v1 fake.

Branch control (`mc/<run>` pushed with projections and packet, head recorded), idempotent create
(client `agentId`, `409 agent_id_conflict` reattach, `Idempotency-Key` when env vars are bound,
`403 feature_unavailable` metadata recorded), `wait_then_send` on `409 agent_busy`, the SSE
stream (ids as provider keys, `Last-Event-ID` reconnect without duplicates, id-less leading
status deduplicated, heartbeats never persisted, retention recorded, `410` → one terminal frame
from the run record), closing facts with `git.branches[]` attribution, the SCM diff, artifacts
through presigned URLs with digests before archive, usage settlement and describe honesty.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from mission_control.adapters.cursor.cloud import (
    STREAM_START,
    client_agent_id,
    run_branch,
)
from mission_control.adapters.cursor.scm import GitBranchPublisher
from mission_control.adapters.cursor.sse import parse_sse_text
from mission_control.adapters.cursor.workspace import git
from mission_control.application.execution.harness.describe import CURSOR_CLOUD_DESCRIBE
from mission_control.application.execution.harness.lane_turns import LaneExecutionIdentity
from mission_control.application.execution.harness.protocol import SessionLane
from mission_control.domain.execution.contracts import OperationExecutionResult
from mission_control.domain.execution.lane_turns import (
    LaneSegmentBounds,
    LaneStatusRequest,
    LaneTurnRequest,
)
from mission_control.domain.execution.lanes import CursorCloudOptions
from mission_control.domain.frames.contracts import FrameKind
from tests.fixtures.cursor_cloud import CloudStack, cloud_stack
from tests.fixtures.lane_turns import RecordingSignals, lane_stack


def _turn(stack: CloudStack, **changes: Any) -> LaneTurnRequest:
    return LaneTurnRequest.model_validate(
        {"operation": stack.operation, "lane_profile": "cursor_cloud", "generation": 1, **changes}
    )


def _identity(stack: CloudStack) -> LaneExecutionIdentity:
    return LaneExecutionIdentity.of(stack.operation, "cursor_cloud", 1)


def _lanes(stack: CloudStack) -> Any:
    return lane_stack(stack.harness, operation=stack.operation)


def _frames(lanes: Any, stack: CloudStack) -> list[Any]:
    return list(lanes.frames._executions[_identity(stack).harness_execution_id].frames.values())


async def test_a_cloud_turn_runs_on_the_pre_created_branch_and_settles(tmp_path: Path) -> None:
    stack = cloud_stack(tmp_path, cost=1_250_000)
    lanes = _lanes(stack)
    heid = _identity(stack).harness_execution_id
    result = await lanes.service.turn(_turn(stack), RecordingSignals(lanes.frames, heid))

    assert result.done and result.closing_facts is not None
    facts = result.closing_facts
    assert facts.native_status == "finished" and facts.duration_ms == 95100
    assert [branch.branch for branch in facts.git_branches] == ["mc/run-operation"]
    assert facts.usage.disposition == "settled" and facts.usage.total_tokens == 2780
    assert facts.cost_disposition == "settled"
    settled = OperationExecutionResult.model_validate(result.operation_result)
    assert settled.status == "completed"
    # Branch control: `mc/<run>` carries projections and packet; the agent starts on it.
    branch = run_branch(stack.operation.cursor_binding, "run-operation")  # type: ignore[arg-type]
    listing = git("ls-tree", "-r", "--name-only", branch, cwd=stack.remote).splitlines()
    assert {".cursor/rules/mc-mission.mdc", ".cursor/hooks.json", "AGENTS.md"} <= set(listing)
    assert ".mission/operating-contract.md" in listing and ".cursor/mcp.json" in listing
    assert any(path.startswith(".cursor/agents/") for path in listing)
    hooks = json.loads(git("show", f"{branch}:.cursor/hooks.json", cwd=stack.remote))
    commands = [entry["command"] for entries in hooks["hooks"].values() for entry in entries]
    assert commands and all("kernel.py" not in command for command in commands), (
        "the cloud VM cannot call the worker back: command hooks only"
    )
    (create,) = stack.api.creates
    assert create["repos"] == [{"url": str(stack.remote), "startingRef": branch}]
    assert create["workOnCurrentBranch"] is True
    assert create["agentId"] == client_agent_id(str(heid), 1)
    assert create["metadata"]["mc_run_id"] == "run-operation"
    state = await lanes.states.load(_identity(stack).request_scope, heid)
    assert state is not None and state.cloud_branch.startswith(f"{branch}@")
    assert state.cloud_agent_url and state.native_turn_ref == stack.api.run_id
    # SSE: ids are provider keys, the leading status is stored once, heartbeats never.
    frames = _frames(lanes, stack)
    keys = [frame.provider_key for frame in frames]
    assert len(keys) == len(set(keys))
    # FT-G6: SSE ids are per run, so frames are keyed `sse:<run>:<id>`.
    run_id = stack.api.run_id
    assert {f"sse:{run_id}:{index}" for index in range(1, 9)} <= set(keys)
    assert all(frame.raw_kind != "heartbeat" for frame in frames)
    assert (
        sum(frame.raw_kind == "status" and "CREATING" in frame.body_excerpt for frame in frames)
        == 1
    )
    assert any('"retention_seconds":3600' in frame.body_excerpt for frame in frames)
    # The SCM diff is the patch; artifacts registered with digests before the agent was archived.
    assert facts.patch_ref is not None
    name, diff, _media = stack.artifacts.staged[facts.patch_ref]
    assert name.endswith("patch.diff") and b"src/feature.py" in diff
    assert b".cursor/" not in diff and b".mission/" not in diff
    manifest = next(
        json.loads(content)
        for staged_name, content, _m in stack.artifacts.staged.values()
        if staged_name.endswith("session-manifest.json")
    )
    assert {item["path"] for item in manifest["artifacts"]} == {
        "artifacts/report.md",
        "artifacts/chart.csv",
    }
    assert all(item["digest"].startswith("sha256:") for item in manifest["artifacts"])
    assert stack.api.archived == [create["agentId"]]
    assert len(stack.api.downloads) == 2
    archive_index = next(
        index
        for index, request in enumerate(stack.api.requests)
        if request.url.path.endswith("/archive")
    )
    assert all(
        index < archive_index
        for index, request in enumerate(stack.api.requests)
        if request.url.host == "s3.fake.invalid"
    )


async def test_a_lost_create_response_reattaches_instead_of_creating_twice(tmp_path: Path) -> None:
    stack = cloud_stack(tmp_path)
    lanes = _lanes(stack)
    heid = _identity(stack).harness_execution_id
    # An earlier attempt created the agent but its response was lost before it was recorded.
    agent_id = client_agent_id(str(heid), 1)
    await stack.client.create_agent(
        {
            "agentId": agent_id,
            "prompt": {"text": "x"},
            "repos": [{"url": "u", "startingRef": "mc/run-operation"}],
        }
    )
    stack.api.pushed = True  # this agent's work is not part of the fixture branch
    result = await lanes.service.turn(_turn(stack), RecordingSignals(lanes.frames, heid))
    assert result.done
    assert len(stack.api.agents) == 1, "409 agent_id_conflict means reattach, never a second agent"
    assert [body.get("agentId") for body in stack.api.creates] == [agent_id, agent_id]


async def test_metadata_unavailable_is_recorded_not_fatal(tmp_path: Path) -> None:
    stack = cloud_stack(tmp_path, api_changes={"metadata_unavailable": True})
    lanes = _lanes(stack)
    result = await lanes.service.turn(_turn(stack), RecordingSignals(lanes.frames))
    assert result.done
    assert "metadata" in stack.api.creates[0] and "metadata" not in stack.api.creates[1]
    manifest = next(
        json.loads(content)
        for name, content, _m in stack.artifacts.staged.values()
        if name.endswith("session-manifest.json")
    )
    assert manifest["metadata"] == "feature_unavailable"


async def test_env_vars_create_with_an_idempotency_key_and_no_agent_id(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="client-supplied agent id"):
        CursorCloudOptions(env_vars_ref="vault:mission/cloud-env")
    stack = cloud_stack(
        tmp_path,
        cloud_changes={"env_vars_ref": "vault:mission/cloud-env", "client_agent_id": False},
        env_vars={"MC_MODE": "fixture"},
    )
    lanes = _lanes(stack)
    result = await lanes.service.turn(_turn(stack), RecordingSignals(lanes.frames))
    assert result.done
    (create,) = stack.api.creates
    assert "agentId" not in create and create["envVars"] == {"MC_MODE": "fixture"}
    create_request = next(r for r in stack.api.requests if r.url.path == "/v1/agents")
    heid = _identity(stack).harness_execution_id
    assert create_request.headers["Idempotency-Key"] == f"{heid}:1:create"


async def test_a_reconnect_with_last_event_id_stores_nothing_twice(tmp_path: Path) -> None:
    stack = cloud_stack(tmp_path, api_changes={"cut_after": "4"})
    lanes = _lanes(stack)
    heid = _identity(stack).harness_execution_id
    first = await lanes.service.turn(_turn(stack), RecordingSignals(lanes.frames, heid))
    assert not first.done and first.cursor == f"{stack.api.run_id}@4"
    second = await lanes.service.turn(
        _turn(stack, phase="resume", cursor=first.cursor, segment_no=2),
        RecordingSignals(lanes.frames, heid),
    )
    assert second.done
    assert stack.api.stream_requests == [None, "4"]
    frames = _frames(lanes, stack)
    keys = [frame.provider_key for frame in frames]
    assert len(keys) == len(set(keys))
    leading = [f for f in frames if f.raw_kind == "status" and "CREATING" in f.body_excerpt]
    assert len(leading) == 1, "the id-less leading status is deduplicated by content"
    assert len(stack.api.creates) == 1


async def test_an_expired_stream_falls_back_to_the_run_record(tmp_path: Path) -> None:
    stack = cloud_stack(tmp_path, api_changes={"cut_after": "3", "expire_after": "3"})
    lanes = _lanes(stack)
    heid = _identity(stack).harness_execution_id
    first = await lanes.service.turn(_turn(stack), RecordingSignals(lanes.frames, heid))
    assert not first.done
    second = await lanes.service.turn(
        _turn(stack, phase="resume", cursor=first.cursor, segment_no=2),
        RecordingSignals(lanes.frames, heid),
    )
    assert second.done and second.closing_facts is not None
    assert second.closing_facts.native_status == "finished"
    terminal = [frame for frame in _frames(lanes, stack) if frame.kind == FrameKind.RUN_RESULT]
    assert len(terminal) == 1 and terminal[0].provider_key.startswith("run:")
    assert terminal[0].raw_kind == "run.final"


async def test_agent_busy_waits_then_sends_and_the_bound_fails_capacity(tmp_path: Path) -> None:
    from mission_control.domain.execution.lanes import (
        HarnessScope,
        SendTurnRequest,
        SessionHandle,
    )

    stack = cloud_stack(tmp_path)
    lanes = _lanes(stack)
    heid = _identity(stack).harness_execution_id
    await lanes.service.turn(_turn(stack), RecordingSignals(lanes.frames, heid))
    agent_id = next(iter(stack.api.agents))
    stack.api.busy = True
    # A later turn on a busy agent sends nothing (`wait_then_send`).
    stack.harness.stage(str(heid), stack.operation)
    turn = await stack.harness.send_turn(
        SendTurnRequest(
            scope=HarnessScope(
                installation_id="i", application_id="a", tenant_id="t", actor_id="mc"
            ),
            lane_profile="cursor_cloud",
            harness_execution_id=str(heid),
            binding_digest=stack.operation.cursor_binding.binding_digest,  # type: ignore[union-attr]
            idempotency_key=f"{heid}:1:turn:2",
            generation=1,
            session=SessionHandle(
                lane_profile="cursor_cloud",
                harness_execution_id=str(heid),
                generation=1,
                native_session_ref=agent_id,
            ),
            turn_no=2,
            instruction_ref="instruction:2",
        )
    )
    assert turn.status == "busy" and turn.native_turn_ref is None
    status = await lanes.service.status(
        LaneStatusRequest.model_validate(
            {"operation": stack.operation, "lane_profile": "cursor_cloud", "generation": 1}
        )
    )
    assert status.settled and status.idle
    exhausted = cloud_stack(tmp_path / "second")
    second = _lanes(exhausted)
    capacity = await second.service.turn(
        _turn(exhausted, capacity_exhausted=True), RecordingSignals(second.frames)
    )
    settled = OperationExecutionResult.model_validate(capacity.operation_result)
    assert settled.status == "failed" and settled.failure_code == "capacity"
    assert exhausted.api.creates == []


async def test_a_requested_cancel_posts_cancel_and_settles(tmp_path: Path) -> None:
    from mission_control.domain.execution.lane_turns import LaneCancelRequest

    stack = cloud_stack(tmp_path, api_changes={"cut_after": "2"})
    lanes = _lanes(stack)
    heid = _identity(stack).harness_execution_id
    bounds = LaneSegmentBounds(max_frames=50, max_duration_s=30, start_to_close_s=60)
    first = await lanes.service.turn(
        _turn(stack, segment=bounds), RecordingSignals(lanes.frames, heid)
    )
    assert not first.done
    outcome = await lanes.service.cancel(
        LaneCancelRequest.model_validate(
            {"operation": stack.operation, "lane_profile": "cursor_cloud", "generation": 1}
        )
    )
    assert outcome.settled and outcome.receipt.native_status == "cancelled"
    assert stack.api.cancelled == [stack.api.run_id]
    again = await lanes.service.cancel(
        LaneCancelRequest.model_validate(
            {"operation": stack.operation, "lane_profile": "cursor_cloud", "generation": 1}
        )
    )
    assert again.settled and stack.api.cancelled == [stack.api.run_id]


async def test_branches_of_another_run_are_not_attributed(tmp_path: Path) -> None:
    stack = cloud_stack(tmp_path)
    lanes = _lanes(stack)
    original = stack.api.handle

    def handle(request: Any) -> Any:
        response = original(request)
        parts = request.url.path.strip("/").split("/")
        if request.method == "GET" and len(parts) == 3 and parts[:2] == ["v1", "agents"]:
            body = response.json()
            body["agent"]["latestRunId"] = "run-of-someone-else"
            return type(response)(200, json=body)
        return response

    stack.api.handle = handle  # type: ignore[method-assign]
    stack.client._client._transport = stack.api.transport()
    result = await lanes.service.turn(_turn(stack), RecordingSignals(lanes.frames))
    assert result.closing_facts is not None and result.closing_facts.git_branches == ()


async def test_usage_settles_tokens_and_cost_only_when_reported(tmp_path: Path) -> None:
    stack = cloud_stack(tmp_path, cost=None)
    lanes = _lanes(stack)
    result = await lanes.service.turn(_turn(stack), RecordingSignals(lanes.frames))
    assert result.closing_facts is not None
    assert result.closing_facts.usage.disposition == "settled"
    assert result.closing_facts.cost_disposition == "estimated"


def test_sse_parsing_keeps_ids_and_leading_status_without_one() -> None:
    events = parse_sse_text(
        ': comment\nevent: status\ndata: {"status": "CREATING"}\n\nid: 1\nevent: assistant\n'
        "data: line one\ndata: line two\n\n"
    )
    assert [(event.event, event.id) for event in events] == [("status", None), ("assistant", "1")]
    assert events[1].data == "line one\nline two"
    assert events[0].json() == {"status": "CREATING"}


async def test_publishing_is_idempotent_per_branch(tmp_path: Path) -> None:
    from tests.fixtures.cursor_cloud import make_remote

    remote = make_remote(tmp_path)
    publisher = GitBranchPublisher(tmp_path / "mirrors")
    first = await publisher.publish(
        repository=str(remote), base_ref="main", branch="mc/run-1", files=[("a.txt", b"a")]
    )
    second = await publisher.publish(
        repository=str(remote), base_ref="main", branch="mc/run-1", files=[("a.txt", b"other")]
    )
    assert first.head == second.head and first.base_commit == second.base_commit


def test_describe_is_the_declared_unqualified_cloud_matrix(tmp_path: Path) -> None:
    stack = cloud_stack(tmp_path)
    describe = stack.harness.describe()
    assert describe == CURSOR_CLOUD_DESCRIBE and describe.qualified is False
    assert describe.controls["reattach"] == "native"
    assert describe.identity.cursor == "sse_event_id"
    assert "session_start" not in describe.hooks.events_supported
    assert "session_end" not in describe.hooks.events_supported
    assert isinstance(stack.harness, SessionLane)
    assert stack.harness.resume_cursor("sse:7") == "7"  # a key recorded before FT-G6
    assert stack.harness.resume_cursor("sse:run-1:7") == "run-1@7"
    assert stack.harness.resume_cursor("run:run-1:final") is None
    assert STREAM_START.startswith("^")


def test_the_worker_registry_carries_both_real_cursor_lanes(tmp_path: Path) -> None:
    from pydantic import SecretStr

    from mission_control.adapters.cursor.cloud import CursorCloudHarness
    from mission_control.adapters.operations.conformance import ConformanceRuntime
    from mission_control.adapters.temporal.deployment_composition import compose_lane_registry
    from mission_control.application.execution.harness.deep_agents_harness import (
        DeepAgentsHarness,
    )
    from tests.fixtures.isolated_settings import isolated_settings

    stack = cloud_stack(tmp_path)
    registry = compose_lane_registry(
        isolated_settings(cursor_api_key=SecretStr("cursor-test")),
        DeepAgentsHarness(ConformanceRuntime()),
        cursor_cloud=stack.harness,
    )
    assert isinstance(registry.for_profile("cursor_cloud"), CursorCloudHarness)
    assert registry.describe("cursor_cloud").qualified is False


def test_wait_then_send_is_bounded_by_the_binding_wall_clock(tmp_path: Path) -> None:
    from mission_control.domain.execution.heartbeats import OperationHeartbeatPolicy

    stack = cloud_stack(tmp_path)
    bounds = OperationHeartbeatPolicy().segments_for(stack.operation)
    assert bounds is not None and bounds.busy_wait_s == 3600
