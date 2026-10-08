"""FT-G2: the `mc.operation.v1` segment loop over `lane.turn`, `lane.status`, `lane.cancel`.

Time-skipping Temporal (and one local dev server for continue-as-new and visibility) drive
the real `OperationWorkflow` against the real lane activities: a scripted Session Lane for
the Cursor-runtime cases and the RRM-004 recovery harness (a real `create_deep_agent` graph
behind the governed boundary) for Deep Agents. Every history replays with `Replayer`.
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any

import pytest
from temporalio.client import WorkflowHandle, WorkflowUpdateFailedError
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Replayer, Worker

from mission_control.adapters.temporal.operation_activities import (
    OperationExecutionActivities,
    parse_operation_result,
)
from mission_control.adapters.temporal.registration.activities import agent_cognitive_activities
from mission_control.adapters.temporal.search_attributes import BELLLABS_SEARCH_ATTRIBUTE_KEYS
from mission_control.adapters.temporal.workflow_sandbox import coordinator_workflow_runner
from mission_control.adapters.temporal.workflows.operation import (
    LANE_PAUSE_PATCH,
    SEGMENT_LOOP_PATCH,
    OperationWorkflow,
)
from mission_control.application.execution.harness.lane_turns import LaneTurnService
from mission_control.application.execution.harness.state import InMemoryLaneExecutionStateStore
from mission_control.application.frames.sink import InMemoryFrameStore
from mission_control.domain.execution.contracts import (
    OperationExecutionRequest,
    OperationWorkflowRequest,
)
from mission_control.domain.execution.lanes import LaneSegmentBounds
from tests.fixtures.checkpoint_recovery import recovery_harness, stage_recovery_unit
from tests.fixtures.lane_turns import (
    LANE_QUEUE,
    LaneStack,
    ScriptedSessionLane,
    cursor_operation,
    lane_stack,
    scripted_frames,
)
from tests.fixtures.temporal_history import patch_ids

WORKFLOW_QUEUE = "ft-g2-operation-workflows"
SMALL = LaneSegmentBounds(
    max_frames=3,
    max_duration_s=30,
    start_to_close_s=60,
    heartbeat_timeout_s=10,
    status_poll_limit=2,
    status_poll_interval_s=1,
    busy_wait_s=30,
)


def _request(
    operation: OperationExecutionRequest | None = None, **changes: Any
) -> OperationWorkflowRequest:
    operation = operation or cursor_operation()
    return OperationWorkflowRequest.model_validate(
        {
            "semantic_attempt_id": operation.identity.semantic_key,
            "operation_kind": "bound_operation",
            "operation": operation,
            "segments": SMALL,
            **changes,
        }
    )


def _scheduled(history: Any) -> list[str]:
    return [
        event.activity_task_scheduled_event_attributes.activity_type.name
        for event in history.events
        if event.HasField("activity_task_scheduled_event_attributes")
    ]


async def _replays(handle: WorkflowHandle[Any, Any]) -> Any:
    history = await handle.fetch_history()
    await Replayer(
        workflows=[OperationWorkflow], workflow_runner=coordinator_workflow_runner()
    ).replay_workflow(history)
    return history


async def _time_skipping() -> WorkflowEnvironment:
    try:
        return await WorkflowEnvironment.start_time_skipping()
    except RuntimeError as error:
        pytest.skip(f"Temporal test server is unavailable: {error}")


def _workers(env: WorkflowEnvironment, stack: LaneStack) -> tuple[Worker, Worker]:
    activities = OperationExecutionActivities(
        stack.boundary, worker_identity="ft-g2-worker", lane_turns=stack.service
    )
    return (
        Worker(
            env.client,
            task_queue=WORKFLOW_QUEUE,
            workflows=[OperationWorkflow],
            workflow_runner=coordinator_workflow_runner(),
        ),
        Worker(
            env.client, task_queue=LANE_QUEUE, activities=agent_cognitive_activities(activities)
        ),
    )


async def _start(
    env: WorkflowEnvironment, request: OperationWorkflowRequest, workflow_id: str
) -> WorkflowHandle[Any, Any]:
    return await env.client.start_workflow(
        OperationWorkflow.run, request, id=workflow_id, task_queue=WORKFLOW_QUEUE
    )


@pytest.mark.asyncio
async def test_a_cursor_unit_runs_as_segments_and_settles_from_closing_facts() -> None:
    env = await _time_skipping()
    async with env:
        stack = lane_stack()
        workflow_worker, activity_worker = _workers(env, stack)
        async with workflow_worker, activity_worker:
            handle = await _start(env, _request(), "ft-g2-segments")
            result = await asyncio.wait_for(handle.result(), timeout=120)
            history = await _replays(handle)

    assert result.disposition == "completed"
    assert parse_operation_result(result.result).status == "completed"
    scheduled = _scheduled(history)
    # 9 frames in segments of 3: three `lane.turn` segments, nothing else.
    assert scheduled == ["lane.turn", "lane.turn", "lane.turn"]
    assert len(stack.lane.sends) == 1, "segments re-attach; the turn is sent once"
    assert SEGMENT_LOOP_PATCH in patch_ids(history)


@pytest.mark.asyncio
async def test_a_failed_segment_resumes_from_persisted_frames_without_a_second_send() -> None:
    env = await _time_skipping()
    async with env:
        lane = ScriptedSessionLane(frames=scripted_frames(), fail_at=4)
        stack = lane_stack(lane)
        workflow_worker, activity_worker = _workers(env, stack)
        request = _request(segments=SMALL.model_copy(update={"max_frames": 50}))
        async with workflow_worker, activity_worker:
            handle = await _start(env, request, "ft-g2-crash-resume")
            result = await asyncio.wait_for(handle.result(), timeout=120)
            history = await _replays(handle)

    assert result.disposition == "completed"
    assert _scheduled(history) == ["lane.turn"], "the retry is an attempt of the same segment"
    assert len(lane.sends) == 1 and "reattach" in lane.calls
    assert "observe:3" in lane.calls, "resumed after the last persisted frame"
    (execution,) = stack.frames._executions.values()
    ordinals = sorted(frame.arrival_ordinal for frame in execution.frames.values())
    assert ordinals == list(range(1, len(scripted_frames()) + 1))


@pytest.mark.asyncio
async def test_cancel_update_cancels_the_turn_then_lane_cancel_settles_and_rejects_repeats() -> (
    None
):
    env = await _time_skipping()
    async with env:
        lane = ScriptedSessionLane(frames=scripted_frames(), hold_after=1, cancel_delay_s=0.5)
        stack = lane_stack(lane)
        workflow_worker, activity_worker = _workers(env, stack)
        async with workflow_worker, activity_worker:
            handle = await _start(env, _request(), "ft-g2-cancel-update")
            await asyncio.wait_for(lane.held.wait(), timeout=60)
            receipt = await handle.execute_update("cancel_command", args=["cmd-1"])
            with pytest.raises(WorkflowUpdateFailedError):
                await handle.execute_update("cancel_command", args=["cmd-1"])
            result = await asyncio.wait_for(handle.result(), timeout=120)
            receipts = await handle.query(OperationWorkflow.command_receipts)
            history = await _replays(handle)

    assert receipt["delivery_semantics"] == "turn_boundary_guaranteed"
    assert receipts == [receipt]
    assert result.disposition == "cancelled"
    settled = parse_operation_result(result.result)
    assert settled.status == "cancelled" and settled.failure_code == "cancelled"
    # The activity's own cleanup cancelled the provider (a requested cancel), then the saga's
    # idempotent `lane.cancel` found it terminal and settled the unit once.
    assert lane.cancels == ["command", "command"]
    assert _scheduled(history) == ["lane.turn", "lane.cancel"]


@pytest.mark.asyncio
async def test_commands_carried_from_an_earlier_run_are_rejected() -> None:
    env = await _time_skipping()
    async with env:
        lane = ScriptedSessionLane(frames=scripted_frames(), hold_after=1)
        stack = lane_stack(lane)
        workflow_worker, activity_worker = _workers(env, stack)
        request = _request(seen_cmds=("cmd-old",))
        async with workflow_worker, activity_worker:
            handle = await _start(env, request, "ft-g2-carried-commands")
            await asyncio.wait_for(lane.held.wait(), timeout=60)
            with pytest.raises(WorkflowUpdateFailedError):
                await handle.execute_update("cancel_command", args=["cmd-old"])
            await handle.execute_update("cancel_command", args=["cmd-new"])
            result = await asyncio.wait_for(handle.result(), timeout=120)
            seen = await handle.query(OperationWorkflow.seen_commands)
            await _replays(handle)

    assert result.disposition == "cancelled"
    assert seen == ["cmd-new", "cmd-old"]


@pytest.mark.asyncio
async def test_a_status_poll_that_never_turns_terminal_parks_the_unit_in_doubt() -> None:
    env = await _time_skipping()
    async with env:
        lane = ScriptedSessionLane(frames=scripted_frames(), hold_after=1, never_terminal=True)
        stack = lane_stack(lane)
        workflow_worker, activity_worker = _workers(env, stack)
        async with workflow_worker, activity_worker:
            handle = await _start(env, _request(), "ft-g2-in-doubt")
            await asyncio.wait_for(lane.held.wait(), timeout=60)
            await handle.execute_update("cancel_command", args=["cmd-1"])

            async def parked() -> bool:
                history = await handle.fetch_history()
                return _scheduled(history).count("lane.cancel") >= 2

            for _ in range(300):
                if await parked():
                    break
                await asyncio.sleep(0.2)
            assert await parked(), "the unit parked in_doubt after the status poll bound"
            # The operator reconciles: the provider is now known terminal.
            lane.never_terminal = False
            await handle.signal(OperationWorkflow.unit_reconciliation_recorded, "decision-1")
            result = await asyncio.wait_for(handle.result(), timeout=120)
            history = await _replays(handle)

    scheduled = _scheduled(history)
    assert scheduled.count("lane.status") == SMALL.status_poll_limit
    # turn, cancel, (status x poll limit), cancel(in_doubt), then the reconciled cancel.
    assert scheduled[:2] == ["lane.turn", "lane.cancel"]
    assert scheduled.count("lane.cancel") == 3
    assert result.disposition == "cancelled"


@pytest.mark.asyncio
async def test_busy_waits_for_idle_then_sends_once() -> None:
    env = await _time_skipping()
    async with env:
        lane = ScriptedSessionLane(frames=scripted_frames(), busy_sends=1)
        lane.cancelled = True  # the fake reports idle once its prior run is terminal
        stack = lane_stack(lane)
        workflow_worker, activity_worker = _workers(env, stack)
        request = _request(segments=SMALL.model_copy(update={"max_frames": 50}))
        async with workflow_worker, activity_worker:
            handle = await _start(env, request, "ft-g2-busy")
            result = await asyncio.wait_for(handle.result(), timeout=120)
            history = await _replays(handle)

    assert result.disposition == "completed"
    assert _scheduled(history) == ["lane.turn", "lane.status", "lane.turn"]
    assert len(lane.sends) == 1


def _deep_agents_lane_turns(harness: Any) -> LaneTurnService:
    return LaneTurnService(
        lanes=harness.service._lanes,
        boundary=harness.service,
        frames=InMemoryFrameStore(),
        states=InMemoryLaneExecutionStateStore(),
    )


@pytest.mark.asyncio
async def test_a_deep_agents_unit_runs_its_governed_body_through_lane_turn() -> None:
    env = await _time_skipping()
    async with env:
        harness = await recovery_harness()
        unit = stage_recovery_unit(harness.run_id)
        operation = await harness.request(unit)
        activities = OperationExecutionActivities(
            harness.service, worker_identity="ft-g2-da", lane_turns=_deep_agents_lane_turns(harness)
        )
        request = OperationWorkflowRequest(
            semantic_attempt_id=operation.identity.semantic_key,
            operation_kind="bound_operation",
            operation=operation,
            timeout_seconds=60,
            heartbeat_timeout_seconds=10,
            segments=LaneSegmentBounds(),
        )
        async with (
            Worker(
                env.client,
                task_queue=WORKFLOW_QUEUE,
                workflows=[OperationWorkflow],
                workflow_runner=coordinator_workflow_runner(),
            ),
            Worker(
                env.client,
                task_queue=harness.binding.task_queue,
                activities=agent_cognitive_activities(activities),
            ),
        ):
            handle = await _start(env, request, "ft-g2-deep-agents")
            result = await asyncio.wait_for(handle.result(), timeout=120)
            history = await _replays(handle)

    assert result.disposition == "completed"
    settled = parse_operation_result(result.result)
    assert settled.status == "completed" and settled.result_checkpoint is not None
    assert _scheduled(history) == ["lane.turn"]


@pytest.mark.asyncio
async def test_a_deep_agents_cancel_reaches_cognition_and_lane_cancel_settles() -> None:
    env = await _time_skipping()
    async with env:
        harness = await recovery_harness()
        unit = stage_recovery_unit(harness.run_id)
        operation = await harness.request(unit)
        entered, _gate = harness.model.gate_on(1)
        activities = OperationExecutionActivities(
            harness.service, worker_identity="ft-g2-da", lane_turns=_deep_agents_lane_turns(harness)
        )
        request = OperationWorkflowRequest(
            semantic_attempt_id=operation.identity.semantic_key,
            operation_kind="bound_operation",
            operation=operation,
            timeout_seconds=60,
            heartbeat_timeout_seconds=10,
            segments=LaneSegmentBounds(),
        )
        async with (
            Worker(
                env.client,
                task_queue=WORKFLOW_QUEUE,
                workflows=[OperationWorkflow],
                workflow_runner=coordinator_workflow_runner(),
            ),
            Worker(
                env.client,
                task_queue=harness.binding.task_queue,
                activities=agent_cognitive_activities(activities),
            ),
        ):
            handle = await _start(env, request, "ft-g2-deep-agents-cancel")
            await asyncio.wait_for(entered.wait(), timeout=60)
            receipt = await handle.execute_update("cancel_command", args=["cmd-da"])
            result = await asyncio.wait_for(handle.result(), timeout=120)
            history = await _replays(handle)

    assert receipt["delivery_semantics"] == "turn_boundary_guaranteed"
    assert result.disposition == "cancelled"
    settled = parse_operation_result(result.result)
    assert settled.status == "cancelled" and settled.result_checkpoint is not None
    assert len(harness.model.calls) == 1, "the interrupted call never resumed"
    assert _scheduled(history) == ["lane.turn", "lane.cancel"]


# --- continue-as-new and visibility on a local dev server ------------------------------------


async def _start_local_suggesting_continue_as_new() -> WorkflowEnvironment:
    try:
        return await WorkflowEnvironment.start_local(
            search_attributes=BELLLABS_SEARCH_ATTRIBUTE_KEYS,
            dev_server_log_level="error",
            dev_server_extra_args=[
                "--dynamic-config-value",
                "limit.historyCount.suggestContinueAsNew=40",
            ],
        )
    except RuntimeError as error:
        pytest.skip(f"Temporal dev server is unavailable: {error}")


def _mc_phases(history: Any) -> list[str]:
    phases: list[str] = []
    for event in history.events:
        if not event.HasField("upsert_workflow_search_attributes_event_attributes"):
            continue
        fields = event.upsert_workflow_search_attributes_event_attributes.search_attributes
        payload = fields.indexed_fields.get("mc_phase")
        if payload is not None:
            phases.append(json.loads(payload.data))
    return phases


@pytest.mark.asyncio
async def test_continue_as_new_carries_commands_and_cursor_and_phases_upsert_at_boundaries() -> (
    None
):
    env = await _start_local_suggesting_continue_as_new()
    async with env:
        lane = ScriptedSessionLane(frames=scripted_frames(tool_calls=14))
        stack = lane_stack(lane)
        workflow_worker, activity_worker = _workers(env, stack)
        request = _request(
            segments=SMALL.model_copy(update={"max_frames": 1}),
            seen_cmds=("cmd-old",),
            search_attribute_policy="required",
        )
        async with workflow_worker, activity_worker:
            handle = await _start(env, request, "ft-g2-continue-as-new")
            result = await asyncio.wait_for(handle.result(), timeout=300)
            first = await env.client.get_workflow_handle(
                "ft-g2-continue-as-new", run_id=handle.first_execution_run_id
            ).fetch_history()
            latest = await handle.fetch_history()
            for history in (first, latest):
                await Replayer(
                    workflows=[OperationWorkflow], workflow_runner=coordinator_workflow_runner()
                ).replay_workflow(history)

    assert result.disposition == "completed"
    continued = [
        event.workflow_execution_continued_as_new_event_attributes
        for event in first.events
        if event.HasField("workflow_execution_continued_as_new_event_attributes")
    ]
    assert continued, "the history size suggestion triggered continue-as-new"
    carried = json.loads(continued[0].input.payloads[0].data)
    assert carried["seen_cmds"] == ["cmd-old"]
    assert carried["lane_resume"]["phase"] == "resume"
    assert int(carried["lane_resume"]["cursor"]) >= 0
    assert len(lane.sends) == 1, "continue-as-new never re-sends the turn"
    (execution,) = stack.frames._executions.values()
    assert len(execution.frames) == len(scripted_frames(tool_calls=14))
    # `mc_phase` changes only at boundaries and only when it changes: `executing` once (the
    # attributes carry over continue-as-new), then the closing disposition; never per frame.
    assert _mc_phases(first) == ["executing"]
    assert _mc_phases(latest) == ["completed"]


# --- FT-G4: Cursor lane controls through the segment loop -------------------------------------


class _GatedStatusLane(ScriptedSessionLane):
    """Busy at the first send; `lane.status` blocks until the test opens the gate."""

    def __init__(self) -> None:
        super().__init__(frames=scripted_frames(), busy_sends=1)
        self.status_entered = asyncio.Event()
        self.status_gate = asyncio.Event()

    async def status(self, request: Any) -> Any:
        from mission_control.domain.execution.lanes import ProviderStatus

        self.calls.append("status")
        self.status_entered.set()
        await self.status_gate.wait()
        return ProviderStatus(status="idle", terminal=True, idle=True)


@pytest.mark.asyncio
async def test_pause_mid_run_is_a_typed_rejection_on_a_cursor_unit() -> None:
    env = await _time_skipping()
    async with env:
        lane = ScriptedSessionLane(frames=scripted_frames(), hold_after=1)
        stack = lane_stack(lane)
        workflow_worker, activity_worker = _workers(env, stack)
        request = _request(segments=SMALL.model_copy(update={"max_frames": 50}))
        async with workflow_worker, activity_worker:
            handle = await _start(env, request, "ft-g4-pause-mid-run")
            await asyncio.wait_for(lane.held.wait(), timeout=60)
            with pytest.raises(WorkflowUpdateFailedError) as rejected:
                await handle.execute_update("pause_command", args=["pause-1"])
            assert not await handle.query(OperationWorkflow.paused)
            lane.released.set()
            result = await asyncio.wait_for(handle.result(), timeout=120)
            history = await _replays(handle)

    cause = rejected.value.cause
    assert getattr(cause, "type", None) == "unsupported_control"
    assert result.disposition == "completed"
    assert len(lane.sends) == 1
    assert LANE_PAUSE_PATCH not in patch_ids(history)


@pytest.mark.asyncio
async def test_pause_at_the_run_boundary_holds_the_next_segment_until_resume() -> None:
    env = await _time_skipping()
    async with env:
        lane = _GatedStatusLane()
        stack = lane_stack(lane)
        workflow_worker, activity_worker = _workers(env, stack)
        request = _request(segments=SMALL.model_copy(update={"max_frames": 50}))
        async with workflow_worker, activity_worker:
            handle = await _start(env, request, "ft-g4-pause-boundary")
            # The first send found the agent busy: nothing is in flight (a run boundary).
            await asyncio.wait_for(lane.status_entered.wait(), timeout=60)
            receipt = await handle.execute_update("pause_command", args=["pause-1"])
            lane.status_gate.set()
            await asyncio.sleep(2)
            assert await handle.query(OperationWorkflow.paused)
            assert lane.sends == [], "no new segment starts while paused"
            resumed = await handle.execute_update("resume_command", args=["resume-1"])
            result = await asyncio.wait_for(handle.result(), timeout=120)
            history = await _replays(handle)

    assert receipt["kind"] == "pause"
    assert receipt["delivery_semantics"] == "turn_boundary_guaranteed"
    assert resumed["delivery_semantics"] == "wait_then_send"
    assert result.disposition == "completed"
    assert len(lane.sends) == 1
    assert _scheduled(history) == ["lane.turn", "lane.status", "lane.turn"]
    assert LANE_PAUSE_PATCH in patch_ids(history)


def _control_workers(env: WorkflowEnvironment, stack: Any) -> tuple[Worker, Worker]:
    activities = OperationExecutionActivities(
        stack.lanes.boundary, worker_identity="ft-g4-worker", lane_turns=stack.service
    )
    return (
        Worker(
            env.client,
            task_queue=WORKFLOW_QUEUE,
            workflows=[OperationWorkflow],
            workflow_runner=coordinator_workflow_runner(),
        ),
        Worker(
            env.client, task_queue=LANE_QUEUE, activities=agent_cognitive_activities(activities)
        ),
    )


@pytest.mark.asyncio
async def test_inject_on_cursor_local_cancels_and_replaces_inside_the_segment_loop(
    tmp_path: Path,
) -> None:
    from tests.fixtures.cursor_controls import control_stack, states

    env = await _time_skipping()
    async with env:
        stack = await control_stack(
            tmp_path, "inject_run", launcher_changes={"hold_at": 6}, declared_outputs=("report.md",)
        )
        launcher = stack.local.launcher
        workflow_worker, activity_worker = _control_workers(env, stack)
        request = _request(stack.operation, segments=SMALL.model_copy(update={"max_frames": 50}))
        async with workflow_worker, activity_worker:
            handle = await _start(env, request, "ft-g4-inject")
            await asyncio.wait_for(launcher.held.wait(), timeout=60)
            receipt = await stack.command("interrupt_and_inject", "Use release/2.3.")
            result = await asyncio.wait_for(handle.result(), timeout=120)
            history = await _replays(handle)

    assert result.disposition == "completed"
    assert _scheduled(history) == ["lane.turn"], "replaced inside one segment"
    run_a, run_b = launcher.first_run, launcher.meta["later_runs"][0]
    assert run_a in launcher.cancelled_runs and launcher.runs == [run_a, run_b]
    assert "Use release/2.3." in launcher.sends[1][1]
    status = await stack.status(receipt)
    assert states(status)[-1] == "applied"
    report = status.receipts[3].delivery_report
    assert report is not None and report.delivered_semantics == "cancel_and_replace"


@pytest.mark.asyncio
async def test_fork_runs_its_first_segment_in_a_fresh_lease_restored_from_the_snapshot(
    tmp_path: Path,
) -> None:
    from mission_control.domain.execution.lanes import SnapshotRequest
    from tests.fixtures.cursor_controls import (
        fork_stack_from,
        harness_fields,
        register_run,
        started_session,
    )
    from tests.fixtures.cursor_local import local_stack

    source = local_stack(tmp_path / "source")
    register_run(source)
    heid, session = await started_session(source)
    root = Path(source.harness.lease_path(heid))
    (root / "src").mkdir(exist_ok=True)
    restored_name = "src/forked_module.py"
    (root / restored_name).write_bytes(b"FORKED = 1\n")
    manifest = await source.harness.snapshot(
        SnapshotRequest(**harness_fields(source.operation, heid), session=session, reason="fork")
    )
    source_agent = session.native_session_ref
    env = await _time_skipping()
    async with env:
        fork, _inputs = fork_stack_from(
            tmp_path / "fork", source, manifest.refs[0], agent_base="agent-forked-0002"
        )
        lanes = lane_stack(fork.harness, frames=fork.frames, operation=fork.operation)
        workflow_worker, activity_worker = _workers(env, lanes)
        request = _request(fork.operation, segments=SMALL.model_copy(update={"max_frames": 50}))
        async with workflow_worker, activity_worker:
            handle = await _start(env, request, "ft-g4-fork")
            result = await asyncio.wait_for(handle.result(), timeout=120)
            history = await _replays(handle)

    assert result.disposition == "completed"
    settled = parse_operation_result(result.result)
    assert fork.launcher.created and fork.launcher.agents[0] != source_agent
    patch = next(
        content
        for ref, (name, content, _media) in fork.artifacts.staged.items()
        if ref in settled.output_refs and name.endswith("patch.diff")
    )
    assert restored_name.encode("utf-8") in patch, "the derived run's patch carries the fork"
    assert _scheduled(history) == ["lane.turn"]


@pytest.mark.asyncio
async def test_request_continuation_hands_the_next_turn_to_a_hydrated_agent(
    tmp_path: Path,
) -> None:
    from tests.fixtures.cursor_controls import control_stack, seal_and_transfer

    env = await _time_skipping()
    async with env:
        stack = await control_stack(tmp_path, "inject_run", launcher_changes={"hold_at": 6})
        launcher = stack.local.launcher
        workflow_worker, activity_worker = _control_workers(env, stack)
        request = _request(stack.operation, segments=SMALL.model_copy(update={"max_frames": 50}))
        async with workflow_worker, activity_worker:
            handle = await _start(env, request, "ft-g4-continuation")
            await asyncio.wait_for(launcher.held.wait(), timeout=60)
            wired, outcome = await seal_and_transfer(stack)
            launcher.release.set()
            result = await asyncio.wait_for(handle.result(), timeout=120)
            history = await _replays(handle)

    assert result.disposition == "completed"
    assert outcome.receipt is not None
    assert launcher.sent_to == [launcher.meta["agent_id"], outcome.receipt.target_session_ref]
    assert [item.event for item in wired["events"].actions][-1] == "transferred"
    assert _scheduled(history) == ["lane.turn"]
