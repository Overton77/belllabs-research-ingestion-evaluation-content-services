"""FT-G4: every SPEC-07 section 7 control on `cursor_local`, with its Delivery Report.

The real `cursor_local` harness over the replaying bridge and a real git repository, inside a
real Run (in-memory run control, receipt ledger and command mailbox). Commands are admitted
through `MissionControlService.command` and reach the lane exactly as on the worker:

- `cancel`: the provider run is cancelled, `lane.cancel` is idempotent, the attempt settles
  `cancelled(cancelled_by_command)` with usage from the last `TurnEndedUpdate`;
- `queue_instruction`: consumed once at the next send (`wait_then_send`); a second item waits
  for the following boundary;
- `interrupt_and_inject`: `cancel_and_replace` (a running tool call without a completion is an
  Uncertain Effect: settled first, or the unit parks `in_doubt` and nothing is re-sent);
- `pause`: a typed rejection mid-run; boundary only;
- `snapshot`, `fork`, `request_continuation`: emulated (patch plus files, a fresh lease or a
  fresh agent hydrated from the packet);
- `missing_output_policy`: one `stop`-hook follow-up, then `not_accepted(outputs_missing)`.

No Cursor agent is created and nothing is paid for.
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any

import pytest

from mission_control.adapters.cursor import snapshot as snapshots
from mission_control.adapters.cursor.kernel_hook_script import native_output
from mission_control.adapters.cursor.local import FOLLOW_UP_TEXT
from mission_control.adapters.cursor.workspace import git
from mission_control.application.execution.harness.controls import (
    UNSUPPORTED_CONTROL,
    open_tool_calls,
    pause_decision,
    usage_from_frames,
)
from mission_control.application.execution.harness.describe import (
    CURSOR_CLOUD_DESCRIBE,
    CURSOR_LOCAL_DESCRIBE,
    DEEP_AGENTS_DESCRIBE,
)
from mission_control.application.execution.harness.lane_turns import (
    LaneExecutionIdentity,
    harness_scope,
)
from mission_control.application.execution.harness.protocol import implements
from mission_control.application.execution.harness.state import (
    InMemoryLaneExecutionStateStore,
    LaneExecutionUpdate,
    NativeIdentityConflict,
)
from mission_control.domain.context.render import INPUTS_MANIFEST_PATH, bytes_digest
from mission_control.domain.execution.contracts import (
    OperationExecutionResult,
)
from mission_control.domain.execution.lane_turns import LaneCancelRequest
from mission_control.domain.execution.lanes import (
    CancelTurnRequest,
    PrepareRequest,
    SnapshotRequest,
    StartRequest,
)
from mission_control.domain.frames.contracts import FrameKind
from mission_control.domain.policies.mailbox import MailboxState
from tests.fixtures.cursor_controls import (
    ACTIVATION_UUID,
    RUN_UUID,
    ControlStack,
    control_stack,
    fork_stack_from,
    register_run,
    seal_and_transfer,
    started_session,
    states,
)
from tests.fixtures.cursor_local import MemoryArtifacts, local_stack
from tests.fixtures.lane_turns import SCOPE, RecordingSignals

INJECTED = "Redirect: build on release/2.3, not main."


async def _held(stack: ControlStack, signals: RecordingSignals) -> asyncio.Task[Any]:
    task = asyncio.create_task(stack.service.turn(stack.turn(), signals))
    await asyncio.wait_for(stack.local.launcher.held.wait(), timeout=10)
    return task


def _signals(stack: ControlStack, *, cancel: bool = False) -> RecordingSignals:
    return RecordingSignals(stack.local.frames, stack.identity.harness_execution_id, cancel=cancel)


def _frames(stack: ControlStack) -> list[Any]:
    execution = stack.local.frames._executions[stack.identity.harness_execution_id]
    return sorted(execution.frames.values(), key=lambda frame: frame.arrival_ordinal)


def _settled(result: Any) -> OperationExecutionResult:
    return OperationExecutionResult.model_validate(result.operation_result)


# --- cancel ----------------------------------------------------------------------------------


async def test_cancel_settles_cancelled_by_command_with_usage_from_the_last_turn_ended(
    tmp_path: Path,
) -> None:
    stack = await control_stack(tmp_path, launcher_changes={"hold_at": 10})
    task = await _held(stack, _signals(stack, cancel=True))
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    launcher = stack.local.launcher
    assert launcher.cancel_calls == 1, "the activity's requested cancel reached run.cancel()"
    cancel = LaneCancelRequest.model_validate(
        {"operation": stack.operation, "lane_profile": "cursor_local", "generation": 1}
    )
    outcome = await stack.service.cancel(cancel)
    assert outcome.settled and outcome.receipt.already_terminal
    settled = OperationExecutionResult.model_validate(outcome.operation_result)
    assert settled.status == "cancelled" and settled.failure_code == "cancelled"
    # Usage recorded from the last TurnEndedUpdate (estimated, never zero).
    assert settled.usage.amounts == {"tokens.total": 2242}
    state = await stack.lanes.states.load(SCOPE, stack.identity.harness_execution_id)
    assert state is not None and state.usage_disposition == "estimated"
    # Idempotent: a repeated lane.cancel returns the same settlement and no new provider call.
    again = await stack.service.cancel(cancel)
    assert again.settled and again.operation_result == outcome.operation_result
    assert launcher.cancel_calls == 2  # the saga's own lane.cancel found it already terminal


async def test_lane_cancel_on_a_finished_run_is_a_no_op(tmp_path: Path) -> None:
    stack = await control_stack(tmp_path)
    result = await stack.service.turn(stack.turn(), _signals(stack))
    assert _settled(result).status == "completed"
    outcome = await stack.service.cancel(
        LaneCancelRequest.model_validate(
            {"operation": stack.operation, "lane_profile": "cursor_local", "generation": 1}
        )
    )
    assert outcome.receipt.already_terminal and outcome.settled
    assert OperationExecutionResult.model_validate(outcome.operation_result).status == "completed"
    assert stack.local.launcher.cancel_calls == 0


async def test_cancel_turn_on_a_finished_run_reports_run_not_cancellable(tmp_path: Path) -> None:
    stack = local_stack(tmp_path)
    _register(stack)
    harness = stack.harness
    identity = LaneExecutionIdentity.of(stack.operation, "cursor_local", 1)
    heid = str(identity.harness_execution_id)
    fields = _fields(stack.operation, heid)
    harness.stage(heid, stack.operation)
    prepared = await harness.prepare(
        PrepareRequest(
            **fields,
            run_id=stack.operation.identity.run_id,
            operation_id=stack.operation.identity.operation_id,
            attempt_no=1,
        )
    )
    session = await harness.start(StartRequest(**fields, prepared=prepared))
    from mission_control.domain.execution.lanes import ObserveRequest, SendTurnRequest

    turn = await harness.send_turn(
        SendTurnRequest(**fields, session=session, turn_no=1, instruction_ref="turn-1")
    )
    async for frame in harness.observe(ObserveRequest(**fields, turn=turn)):
        if frame.terminal:
            break
    receipt = await harness.cancel_turn(CancelTurnRequest(**fields, turn=turn, reason="command"))
    assert receipt.acknowledged and receipt.already_terminal
    assert receipt.native_status == "finished"


def _register(stack: Any) -> None:
    operation = stack.operation
    stack.frames.register_run(
        SCOPE,
        operation.identity.run_id,
        RUN_UUID,
        {operation.identity.operation_id: ACTIVATION_UUID},
    )


def _fields(operation: Any, heid: str) -> dict[str, Any]:
    return {
        "scope": harness_scope(operation.request_scope),
        "lane_profile": "cursor_local",
        "harness_execution_id": heid,
        "binding_digest": operation.cursor_binding.binding_digest,
        "idempotency_key": f"{heid}:1:test",
        "generation": 1,
    }


# --- queue_instruction: wait_then_send -------------------------------------------------------


async def test_a_queued_instruction_rides_the_next_send_once_and_a_second_waits(
    tmp_path: Path,
) -> None:
    stack = await control_stack(tmp_path, launcher_changes={"hold_at": 6})
    first = await stack.command("queue_instruction", "Also update the README flag table.")
    delivered = await stack.deliver()
    assert [entry.command_id for entry in delivered] == [str(first.request_id)]
    task = await _held(stack, _signals(stack))
    # Consumed when the send carrying it was accepted, with a wait_then_send report.
    status = await stack.status(first)
    assert states(status)[-1] == "observed"
    report = status.receipts[-1].delivery_report
    assert report is not None
    assert report.requested_semantics == "wait_then_send"
    assert report.delivered_semantics == "wait_then_send"
    # A second instruction while the turn runs waits for the following boundary.
    second = await stack.command("queue_instruction", "Then run the linters.")
    stack.local.launcher.release.set()
    result = await asyncio.wait_for(task, timeout=30)
    assert _settled(result).status == "completed"
    assert states(await stack.status(first))[-2:] == ["observed", "applied"]
    waiting = {entry.command_id: entry for entry in await stack.entries()}
    assert waiting[str(second.request_id)].state == MailboxState.QUEUED
    # One send: the bridge never received a second turn for this unit.
    assert len(stack.local.launcher.sends) == 1
    # A retried send boundary consumes nothing twice.
    await stack.mailbox.turn_started(
        SCOPE,
        stack.run_id,
        delivery_key=stack.operation.idempotency_key,
        lane_profile="cursor_local",
    )
    assert states(await stack.status(first)).count("observed") == 1
    # The following boundary takes the second one.
    following = await stack.deliver("ft-g4:next-turn")
    assert [entry.command_id for entry in following] == [str(second.request_id)]


# --- interrupt_and_inject: cancel_and_replace -----------------------------------------------


async def test_inject_cancels_settles_the_running_tool_and_replaces_the_turn(
    tmp_path: Path,
) -> None:
    stack = await control_stack(
        tmp_path, "inject_run", launcher_changes={"hold_at": 6}, declared_outputs=("report.md",)
    )
    launcher = stack.local.launcher
    task = await _held(stack, _signals(stack))
    receipt = await stack.command("interrupt_and_inject", INJECTED)
    result = await asyncio.wait_for(task, timeout=30)

    settled = _settled(result)
    assert settled.status == "completed", settled
    run_a, run_b = launcher.first_run, launcher.meta["later_runs"][0]
    assert result.native.turn_ref == run_b
    # The running turn was cancelled at the provider, the replacement sent once.
    assert run_a in launcher.cancelled_runs
    assert [key.rsplit(":", 2)[-2] for key, _text in launcher.sends] == ["turn", "replace"]
    replacement_text = launcher.sends[1][1]
    assert INJECTED in replacement_text, "the replacement turn carries the injected item"
    # The interrupted turn's running tool call completed during the cancel: settled first.
    frames = _frames(stack)
    assert open_tool_calls(frames, run_a) == ()
    assert any(frame.native_turn_ref == run_b for frame in frames)
    status = await stack.status(receipt)
    assert states(status) == ["accepted", "queued", "delivered", "observed", "applied"]
    report = status.receipts[3].delivery_report
    assert report is not None
    assert report.delivered_semantics == "cancel_and_replace"
    assert report.native_refs.cancelled_turn_ref == run_a
    assert report.native_refs.replacement_turn_ref == run_b
    assert report.settled_effect_ids == ("call-shell-2",)
    # The recorded native turn moved by an explicit supersession.
    state = await stack.lanes.states.load(SCOPE, stack.identity.harness_execution_id)
    assert state is not None and state.native_turn_ref == run_b


async def test_inject_with_a_tool_call_that_never_closes_parks_the_unit_in_doubt(
    tmp_path: Path,
) -> None:
    stack = await control_stack(
        tmp_path, "inject_run", launcher_changes={"hold_at": 6, "drop_after_cancel": True}
    )
    launcher = stack.local.launcher
    task = await _held(stack, _signals(stack))
    receipt = await stack.command("interrupt_and_inject", INJECTED)
    result = await asyncio.wait_for(task, timeout=30)

    assert result.done and result.closing_facts is not None
    assert result.closing_facts.error_code == "unsettled_effect_claims"
    assert _settled(result).status == "in_doubt"
    assert len(launcher.sends) == 1, "no replacement turn is sent over an Uncertain Effect"
    assert launcher.first_run in launcher.cancelled_runs
    # The injected command waits for the next turn after reconciliation.
    (entry,) = [
        item for item in await stack.entries() if item.command_id == str(receipt.request_id)
    ]
    assert entry.state == MailboxState.QUEUED
    events = [
        event.event_type
        for event in stack.repository.mailbox.events.values()
        if event.correlation_id == str(receipt.request_id)
    ]
    assert "command.in_doubt" in events


# --- pause -----------------------------------------------------------------------------------


def test_pause_is_a_typed_rejection_mid_run_and_boundary_only() -> None:
    mid_run = pause_decision(CURSOR_LOCAL_DESCRIBE, turn_in_flight=True)
    assert not mid_run.accepted
    assert mid_run.reason_code == UNSUPPORTED_CONTROL
    assert mid_run.delivery_semantics == "unsupported"
    boundary = pause_decision(CURSOR_LOCAL_DESCRIBE, turn_in_flight=False)
    assert boundary.accepted and boundary.delivery_semantics == "turn_boundary_guaranteed"
    assert pause_decision(CURSOR_CLOUD_DESCRIBE, turn_in_flight=True).accepted is False
    # Deep Agents pauses at its tool gate mid-run (native).
    native = pause_decision(DEEP_AGENTS_DESCRIBE, turn_in_flight=True)
    assert native.accepted and native.delivery_semantics == "pause_at_tool_gate"
    assert CURSOR_LOCAL_DESCRIBE.controls["pause"] == "unsupported"


# --- snapshot and fork -----------------------------------------------------------------------


async def test_snapshot_manifest_lists_patch_untracked_mission_digests_and_native_refs(
    tmp_path: Path,
) -> None:
    stack = await control_stack(tmp_path, launcher_changes={"hold_at": 6})
    task = await _held(stack, _signals(stack))
    heid = str(stack.identity.harness_execution_id)
    root = Path(stack.local.harness.lease_path(heid))
    (root / "README.md").write_bytes(b"# target\n\nEdited by the agent.\n")
    (root / "src").mkdir(exist_ok=True)
    (root / "src/new_module.py").write_bytes(b"VALUE = 7\n")
    manifest = await stack.local.harness.snapshot(
        SnapshotRequest(**_fields(stack.operation, heid), session=_session(stack), reason="fork")
    )
    stack.local.launcher.release.set()
    await asyncio.wait_for(task, timeout=30)

    assert manifest.emulated and manifest.kind == "git_patch"
    refs = manifest.refs
    assert refs[0].startswith(snapshots.SNAPSHOT_REF_PREFIX)
    assert any(ref.startswith("patch_digest:sha256:") for ref in refs)
    assert f"untracked:src/new_module.py={bytes_digest(b'VALUE = 7' + bytes([10]))}" in refs
    mission = [ref for ref in refs if ref.startswith("mission:.mission/")]
    assert any("operating-contract.md=" in ref for ref in mission)
    assert not any(".token" in ref or "hooks/" in ref or "state/" in ref for ref in refs)
    launcher = stack.local.launcher
    assert f"native:agent_id={launcher.meta['agent_id']}" in refs
    assert f"native:run_id={launcher.first_run}" in refs
    frozen = await snapshots.load(refs[0], stack.local.artifacts)
    diff = stack.local.artifacts.staged[frozen.patch.ref][1]
    assert b"Edited by the agent" in diff and b"new_module.py" in diff
    assert b".mission" not in diff and b"AGENTS.md" not in diff


def _session(stack: ControlStack) -> Any:
    from mission_control.domain.execution.lanes import SessionHandle

    return SessionHandle(
        lane_profile="cursor_local",
        harness_execution_id=str(stack.identity.harness_execution_id),
        generation=1,
        native_session_ref=str(stack.local.launcher.meta["agent_id"]),
    )


async def test_fork_restores_the_snapshot_into_a_fresh_lease_with_a_new_agent(
    tmp_path: Path,
) -> None:
    source = await control_stack(tmp_path / "source", launcher_changes={"hold_at": 6})
    queued = await source.command("queue_instruction", "source-only follow-up")
    task = await _held(source, _signals(source))
    heid = str(source.identity.harness_execution_id)
    root = Path(source.local.harness.lease_path(heid))
    (root / "src").mkdir(exist_ok=True)
    (root / "src/forked.py").write_bytes(b"FORKED = True\n")
    (root / "outputs/notes.md").write_bytes(b"draft\n")
    manifest = await source.local.harness.snapshot(
        SnapshotRequest(**_fields(source.operation, heid), session=_session(source), reason="fork")
    )
    source.local.launcher.release.set()
    await asyncio.wait_for(task, timeout=30)

    fork, inputs_manifest = fork_stack_from(
        tmp_path / "fork", source.local, manifest.refs[0], agent_base="agent-forked-0001"
    )
    register_run(fork)
    fork_heid, session = await started_session(fork)

    fork_root = Path(fork.harness.lease_path(fork_heid))
    assert fork_root != root, "a fresh lease"
    assert (fork_root / "src/forked.py").read_bytes().replace(b"\r\n", b"\n") == (
        b"FORKED = True\n"
    )
    assert (fork_root / "outputs/notes.md").read_bytes() == b"draft\n"
    # The derived packet, not the source's, is the one on disk.
    assert (fork_root / INPUTS_MANIFEST_PATH).read_bytes() == inputs_manifest
    assert session.native_session_ref == "agent-forked-0001"
    assert session.native_session_ref != source.local.launcher.meta["agent_id"]
    # Nothing of the source run's mailbox is cloned: its queued item stays queued there.
    (entry,) = await source.entries()
    assert entry.command_id == str(queued.request_id)
    assert entry.state == MailboxState.QUEUED
    assert git("status", "--porcelain", cwd=fork_root)


async def test_fork_from_an_altered_snapshot_is_checkpoint_invalid(tmp_path: Path) -> None:
    artifacts = MemoryArtifacts()
    manifest_ref = await artifacts.stage(
        request_scope=SCOPE,
        name="manifest.json",
        content=b"{}",
        media_type="application/json",
    )
    with pytest.raises(Exception) as refused:
        await snapshots.load(snapshots.snapshot_ref_of(manifest_ref), artifacts)
    assert refused.type.__name__ in {"ValidationError", "LaneProjectionError"}
    with pytest.raises(snapshots.LaneProjectionError) as unknown:
        await snapshots.load("run-snapshot://snap-1", artifacts)
    assert unknown.value.code == snapshots.CHECKPOINT_INVALID


# --- request_continuation ----------------------------------------------------------------------


async def test_request_continuation_hydrates_a_fresh_agent_that_runs_the_next_turn(
    tmp_path: Path,
) -> None:
    from mission_control.application.context.continuation import TransferStatus

    stack = await control_stack(tmp_path, "inject_run", launcher_changes={"hold_at": 6})
    launcher = stack.local.launcher
    task = await _held(stack, _signals(stack))
    agent = str(launcher.meta["agent_id"])
    wired, outcome = await seal_and_transfer(stack)
    assert outcome.transfer.status == TransferStatus.TRANSFERRED
    assert outcome.receipt is not None
    new_agent = outcome.receipt.target_session_ref
    assert new_agent != agent and new_agent in launcher.agents
    assert [item.event for item in wired["events"].actions][-1] == "transferred"
    restored = outcome.receipt.restored
    assert "/.mission/context.md" in restored and "/.mission/operating-contract.md" in restored
    # The current turn finishes; the next turn runs on the hydrated agent.
    launcher.release.set()
    result = await asyncio.wait_for(task, timeout=30)
    assert _settled(result).status == "completed"
    assert launcher.sent_to == [agent, new_agent]
    assert "continuation" in launcher.sends[1][0]
    # The continuation turn is the checkpoint packet's admitted-input segment.
    assert "# Mission context packet" in launcher.sends[1][1]
    assert "- purpose: continuation" in launcher.sends[1][1]
    state = await stack.lanes.states.load(SCOPE, stack.identity.harness_execution_id)
    assert state is not None and state.native_session_ref == new_agent
    inits = [
        frame
        for frame in _frames(stack)
        if frame.kind == FrameKind.SESSION_INIT and frame.native_session_ref == new_agent
    ]
    assert inits, "the target session's session_init frame confirms the hydration"


# --- missing_output_policy follow-up ----------------------------------------------------------


async def test_missing_outputs_get_one_follow_up_then_not_accepted(tmp_path: Path) -> None:
    stack = await control_stack(tmp_path, "outputs_missing", declared_outputs=("summary.md",))
    result = await stack.service.turn(stack.turn(), _signals(stack))
    stops = [
        result for event, _expect, result in stack.local.launcher.hook_results if event == "stop"
    ]
    assert [bool(item.followup_message) for item in stops] == [True, False]
    assert "outputs/summary.md" in (stops[0].followup_message or "")
    assert result.closing_facts is not None
    assert result.closing_facts.missing_outputs == ("outputs/summary.md",)
    settled = _settled(result)
    assert settled.status == "failed" and settled.failure_code == "outputs_missing"
    text, code = native_output("stop", stops[0].model_dump(mode="json"))
    assert json.loads(text)["followup_message"] and code == 0


async def test_a_follow_up_that_produces_the_outputs_is_accepted(tmp_path: Path) -> None:
    stack = await control_stack(tmp_path, "full_run", declared_outputs=("report.md",))
    result = await stack.service.turn(stack.turn(), _signals(stack))
    stops = [
        result for event, _expect, result in stack.local.launcher.hook_results if event == "stop"
    ]
    # At the stop the report was not written yet: one follow-up asks for it.
    assert len(stops) == 1 and stops[0].followup_message == FOLLOW_UP_TEXT.format(
        missing="outputs/report.md"
    )
    assert _settled(result).status == "completed"
    assert result.closing_facts is not None and result.closing_facts.missing_outputs == ()


# --- helpers and contracts ---------------------------------------------------------------------


async def test_native_identity_moves_only_by_an_explicit_supersession() -> None:
    from uuid import uuid4

    store = InMemoryLaneExecutionStateStore()
    heid = uuid4()
    await store.record(
        SCOPE, heid, LaneExecutionUpdate(native_session_ref="a", native_turn_ref="r1")
    )
    with pytest.raises(NativeIdentityConflict):
        await store.record(SCOPE, heid, LaneExecutionUpdate(native_turn_ref="r2"))
    with pytest.raises(NativeIdentityConflict):
        await store.record(
            SCOPE, heid, LaneExecutionUpdate(native_turn_ref="r2", supersedes_turn_ref="r0")
        )
    moved = await store.record(
        SCOPE, heid, LaneExecutionUpdate(native_turn_ref="r2", supersedes_turn_ref="r1")
    )
    assert moved.native_turn_ref == "r2"
    assert "supersedes_turn_ref" not in LaneExecutionUpdate(supersedes_turn_ref="r1").fields()


def test_usage_and_open_tool_calls_read_persisted_frames() -> None:
    assert usage_from_frames((), None).disposition == "unknown"
    assert open_tool_calls((), None) == ()


def test_every_section_7_cell_of_cursor_local_is_declared_and_implemented() -> None:
    matrix = CURSOR_LOCAL_DESCRIBE
    section_7 = {
        "queue_instruction": "wait_then_send",
        "interrupt_and_inject": "cancel_and_replace",
        "pause": "unsupported",
        "hard_pause": "unsupported",
        "resume": "wait_then_send",
        "cancel": "turn_boundary_guaranteed",
        "fork": "emulated",
        "request_continuation": "emulated",
    }
    assert matrix.delivery_semantics == section_7
    assert matrix.controls["snapshot"] == "emulated" and matrix.controls["fork"] == "emulated"
    assert matrix.controls["pause"] == "unsupported" and matrix.qualified is False


def test_cursor_local_implements_every_declared_operation(tmp_path: Path) -> None:
    stack = local_stack(tmp_path)
    for operation, support in CURSOR_LOCAL_DESCRIBE.controls.items():
        if operation in {"pause", "fork"}:
            continue
        assert support in {"native", "emulated"}
        assert implements(stack.harness, operation), operation
    for capability in ("stage_turn", "pending_handover", "stop_followup"):
        assert callable(getattr(stack.harness, capability))


def test_workflow_pause_and_resume_semantics_equal_every_declared_matrix() -> None:
    from mission_control.application.execution.harness.describe import DECLARED_LANE_MATRICES
    from mission_control.domain.execution.lane_turns import (
        LANE_PAUSE_SEMANTICS,
        LANE_RESUME_SEMANTICS,
    )

    for profile, matrix in DECLARED_LANE_MATRICES.items():
        assert LANE_PAUSE_SEMANTICS[profile] == matrix.delivery_semantics["pause"]
        assert LANE_RESUME_SEMANTICS[profile] == matrix.delivery_semantics["resume"]
