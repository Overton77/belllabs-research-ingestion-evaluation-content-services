"""MP-09: stream-retention expiry is reconciled apart from the run's own terminal state
(FIXTURE Cloud API): `410 stream_expired` writes one `stream.expired` status frame, the
terminal truth comes from `GET .../runs/{runId}` (FINISHED or ERROR, each with its own
closing facts), a run still running after expiry is polled from the record (never the
stream again) and a foreign cursor reopens from the start without storing anything twice.
"""

from __future__ import annotations

from pathlib import Path

from mission_control.domain.execution.contracts import OperationExecutionResult
from mission_control.domain.execution.lanes import ObserveRequest, SessionHandle, TurnHandle
from mission_control.domain.frames.contracts import FrameKind
from tests.fixtures.cursor_cloud import cloud_stack
from tests.fixtures.cursor_controls import harness_fields
from tests.unit.cursor.support import frames_of, heid_of, lanes_for, signals, turn

PROFILE = "cursor_cloud"


def _expired_frames(frames: list) -> list:  # type: ignore[type-arg]
    return [f for f in frames if f.provider_key.startswith("sse-expired:")]


async def test_retention_expiry_is_recorded_apart_from_the_runs_own_finish(tmp_path: Path) -> None:
    stack = cloud_stack(tmp_path, api_changes={"cut_after": "3", "expire_after": "3"})
    lanes = lanes_for(stack)
    first = await lanes.service.turn(turn(stack, PROFILE), signals(lanes, stack, PROFILE))
    assert not first.done and first.cursor == f"{stack.api.run_id}@3"
    second = await lanes.service.turn(
        turn(stack, PROFILE, phase="resume", cursor=first.cursor, segment_no=2),
        signals(lanes, stack, PROFILE),
    )
    assert second.done and second.closing_facts is not None
    assert second.closing_facts.native_status == "finished"
    frames = frames_of(lanes, stack, PROFILE)
    keys = [frame.provider_key for frame in frames]
    assert len(keys) == len(set(keys))
    (expired,) = _expired_frames(frames)
    assert '"stream":"expired"' in expired.body_excerpt.replace(" ", "")
    assert '"last_event_id":"3"' in expired.body_excerpt.replace(" ", "")
    assert expired.kind == FrameKind.STATUS and not expired.closing
    (final,) = [f for f in frames if f.kind == FrameKind.RUN_RESULT]
    assert final.raw_kind == "run.final"
    body = final.body_excerpt.replace(" ", "")
    assert '"synthesized_from":"run_record"' in body and '"stream":"expired"' in body
    assert stack.api.stream_requests == [None, "3"], "one 410; the stream is never re-asked"


async def test_a_task_failure_after_expiry_is_an_error_with_the_providers_code(
    tmp_path: Path,
) -> None:
    stack = cloud_stack(
        tmp_path,
        stream="error_stream",
        record="error_record",
        api_changes={"cut_after": "2", "expire_after": "2"},
    )
    lanes = lanes_for(stack)
    first = await lanes.service.turn(turn(stack, PROFILE), signals(lanes, stack, PROFILE))
    assert not first.done
    second = await lanes.service.turn(
        turn(stack, PROFILE, phase="resume", cursor=first.cursor, segment_no=2),
        signals(lanes, stack, PROFILE),
    )
    assert second.done and second.closing_facts is not None
    facts = second.closing_facts
    assert facts.native_status == "error" and facts.error_code == "provider_error"
    settled = OperationExecutionResult.model_validate(second.operation_result)
    assert settled.status == "failed" and settled.failure_code == "provider_error"
    frames = frames_of(lanes, stack, PROFILE)
    assert len(_expired_frames(frames)) == 1, "the transport fault is its own, non-closing fact"
    (final,) = [f for f in frames if f.kind == FrameKind.RUN_RESULT]
    assert '"status":"ERROR"' in final.body_excerpt.replace(" ", "")


async def test_a_run_still_running_after_expiry_is_polled_from_the_record_not_the_stream(
    tmp_path: Path,
) -> None:
    stack = cloud_stack(
        tmp_path, api_changes={"cut_after": "3", "expire_after": "3", "running_reads": 3}
    )
    lanes = lanes_for(stack)
    first = await lanes.service.turn(turn(stack, PROFILE), signals(lanes, stack, PROFILE))
    assert not first.done
    second = await lanes.service.turn(
        turn(stack, PROFILE, phase="resume", cursor=first.cursor, segment_no=2),
        signals(lanes, stack, PROFILE),
    )
    assert second.done and second.closing_facts is not None
    assert second.closing_facts.native_status == "finished"
    assert stack.api.stream_requests == [None, "3"], "expired once, then the record only"
    assert stack.api.run_reads >= 4, "the record was polled until the run ended"
    frames = frames_of(lanes, stack, PROFILE)
    keys = [frame.provider_key for frame in frames]
    assert len(keys) == len(set(keys))
    polled = [f for f in frames if f.provider_key.startswith("run-status:")]
    assert len(polled) == 1 and polled[0].provider_key.endswith(":running")
    assert len([f for f in frames if f.kind == FrameKind.RUN_RESULT]) == 1


async def test_a_foreign_cursor_reopens_from_the_start_and_stores_nothing_twice(
    tmp_path: Path,
) -> None:
    stack = cloud_stack(tmp_path, api_changes={"reject_foreign_cursor": True})
    lanes = lanes_for(stack)
    assert (await lanes.service.turn(turn(stack, PROFILE), signals(lanes, stack, PROFILE))).done
    heid = heid_of(stack, PROFILE)
    agent_id = next(iter(stack.api.agents))
    stack.harness.stage(heid, stack.operation)
    session = SessionHandle(
        lane_profile=PROFILE, harness_execution_id=heid, generation=1, native_session_ref=agent_id
    )
    observed = [
        frame
        async for frame in stack.harness.observe(
            ObserveRequest(
                **harness_fields(stack.operation, heid, PROFILE),
                turn=TurnHandle(session=session, turn_no=1, native_turn_ref=stack.api.run_id),
                after=f"{stack.api.run_id}@zz",
            )
        )
    ]
    assert stack.api.stream_requests[-2:] == ["zz", None], "400 then a replay from the start"
    keys = [frame.provider_key for frame in observed]
    assert len(keys) == len(set(keys)) and observed[-1].terminal
    stored = {frame.provider_key for frame in frames_of(lanes, stack, PROFILE)}
    assert {key for key in keys if key.startswith("sse:")} <= stored, "the same keys dedupe"
