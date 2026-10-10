"""MP-08: the `codex` lane over the FIXTURE app-server, driven by `LaneTurnService`.

Deterministic outcomes for: a full turn (every event a frame, unknown kinds stay unknown,
no credential in the child environment, project trust materialized); queued sends on an
active thread (`busy`, never a `turn/start` that would steer); explicit steering applied to
the exact turn or `stale_target` when the turn completed first; interrupt with the terminal
completion observed; a lost `turn/start` response reconciled from thread history and never
re-sent; a disconnect ending the segment and the next segment reattaching from history; a
limit refusal as `ProviderCapacityLimited` journaled `declined`; the describe matrix
implemented cell by cell; the binding's approval policy is exactly the pinned enum.
Approvals (MP-11 broker) are in `test_approvals.py`, segment cursors in `test_segments.py`,
MP-12 compaction/occupancy in `test_compaction.py`, launcher/composition in
`test_launcher.py`.
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path
from typing import Any

import pytest

from mission_control.adapters.codex.harness import CODEX_HOME_DIR
from mission_control.adapters.codex.launcher import child_environment, subprocess_supported
from mission_control.adapters.codex.transport import ResponseLost
from mission_control.adapters.cursor.projection import LaneProjectionError
from mission_control.application.execution.harness.controls import TurnTextStaging
from mission_control.application.execution.harness.dispatch import (
    DispatchReconcilingLane,
    ProviderCapacityLimited,
    SteeringLane,
)
from mission_control.application.execution.harness.protocol import (
    HARNESS_PROTOCOL_METHODS,
    implements,
)
from mission_control.bootstrap.provider_auth import provider_child_environment
from mission_control.domain.execution.contracts import OperationExecutionResult
from mission_control.domain.execution.lane_turns import LANE_COMMAND_SEMANTICS, LaneCancelRequest
from mission_control.domain.execution.lanes import (
    CancelTurnRequest,
    LaneSegmentBounds,
    PrepareRequest,
    SendTurnRequest,
    SessionHandle,
    StartRequest,
    StatusRequest,
    TurnHandle,
)
from mission_control.domain.frames.contracts import FrameKind
from tests.fixtures.lane_turns import RecordingSignals
from tests.unit.codex.fixture_app_server import FixtureLauncher, load_script
from tests.unit.codex.support import (
    FAKE_WORKER_ENV,
    StaticAuth,
    admission,
    build_harness,
    codex_binding,
    codex_operation,
    codex_stack,
)

BOUNDS = LaneSegmentBounds(max_duration_s=20, start_to_close_s=60, heartbeat_timeout_s=3)


def _settled(result: Any) -> OperationExecutionResult:
    return OperationExecutionResult.model_validate(result.operation_result)


def _fields(stack: Any, key: str = "test") -> dict[str, Any]:
    from mission_control.application.execution.harness.lane_turns import harness_scope

    return {
        "scope": harness_scope(stack.operation.request_scope),
        "lane_profile": "codex",
        "harness_execution_id": stack.heid,
        "binding_digest": stack.operation.effective_configuration_digest,
        "idempotency_key": f"{stack.heid}:1:{key}",
        "generation": 1,
    }


async def _started(stack: Any) -> SessionHandle:
    """Stage, prepare and start directly on the harness (no `lane.turn`)."""

    harness = stack.harness
    harness.stage(stack.heid, stack.operation)
    fields = _fields(stack)
    prepared = await harness.prepare(
        PrepareRequest(
            **fields,
            run_id=stack.operation.identity.run_id,
            operation_id=stack.operation.identity.operation_id,
            attempt_no=1,
        )
    )
    return await harness.start(StartRequest(**fields, prepared=prepared))


async def _sent(stack: Any, session: SessionHandle, key: str = "turn:1") -> TurnHandle:
    return await stack.harness.send_turn(
        SendTurnRequest(
            **_fields(stack, key),
            session=session,
            turn_no=1,
            instruction_ref=f"operation:{stack.operation.identity.semantic_key}:{key}",
        )
    )


# --- the full turn --------------------------------------------------------------------------


async def test_a_full_turn_persists_every_event_as_frames_and_settles(tmp_path: Path) -> None:
    stack = codex_stack(tmp_path, "turn_full")
    result = await stack.lanes.service.turn(
        stack.turn(segment=BOUNDS), RecordingSignals(stack.lanes.frames)
    )
    assert result.done and _settled(result).status == "completed"
    facts = result.closing_facts
    assert facts is not None
    assert facts.native_status == "finished"
    assert facts.result_excerpt == "Wrote outputs/report.md as asked."
    assert (facts.usage.disposition, facts.usage.total_tokens) == ("estimated", 160)
    assert facts.cost_disposition == "estimated", "Codex reports no cost"
    assert facts.model == "gpt-5-codex" and facts.duration_ms == 10_000
    assert result.native.session_ref == "thr-0001" and result.native.turn_ref is not None

    frames = stack.frames()
    kinds = [frame.kind for frame in frames]
    assert kinds.count(FrameKind.MESSAGE_DELTA) == 2, "every delta of one item is stored"
    assert FrameKind.TOOL_CALL_COMPLETED in kinds and FrameKind.MESSAGE in kinds
    assert FrameKind.USAGE in kinds and FrameKind.TURN_ENDED in kinds
    assert kinds.count(FrameKind.RUN_RESULT) == 1
    unknown = [frame.raw_kind for frame in frames if frame.kind == FrameKind.UNKNOWN]
    assert "fixture/unmappedEvent" in unknown and "thread/prediction/updated" in unknown, (
        "unmapped methods are stored as unknown, never dropped"
    )
    assert len({frame.provider_key for frame in frames}) == len(frames)
    assert all(frame.native_session_ref == "thr-0001" for frame in frames)

    server = stack.launcher.server
    methods = [method for method, _params in server.records]
    assert methods[:3] == ["initialize", "initialized", "thread/start"]
    assert methods.count("turn/start") == 1 and server.steer_by_turn_start == 0
    (turn_start,) = [params for method, params in server.records if method == "turn/start"]
    assert turn_start["clientUserMessageId"] == stack.send_key
    assert turn_start["input"][0]["type"] == "text"
    (thread_start,) = [params for method, params in server.records if method == "thread/start"]
    assert thread_start["approvalsReviewer"] == "user" and thread_start["ephemeral"] is False
    assert thread_start["approvalPolicy"] == "on-request"
    assert thread_start["sandbox"] == "workspace-write"
    assert (
        thread_start["cwd"]
        == Path(stack.leaser.leases[next(iter(stack.leaser.leases))].path).as_posix()
    )

    # The child environment: allow-listed names and CODEX_HOME only, never a credential.
    env = dict(stack.launcher.launches[0].spec.env)
    assert set(env) == {"PATH", "HOME", "CODEX_HOME"}
    assert "sk-" not in " ".join(env.values())
    codex_home = Path(env["CODEX_HOME"])
    assert codex_home.name == "codex-home" and codex_home.as_posix().endswith(CODEX_HOME_DIR)
    trust = (codex_home / "config.toml").read_text(encoding="utf-8")
    assert 'trust_level = "trusted"' in trust and "[projects." in trust
    # The projection was materialized under the lease and the patch stored before release.
    lease = next(iter(stack.leaser.leases.values()))
    assert (Path(lease.path) / ".codex" / "config.toml").is_file()
    assert lease.released and lease.patch_artifact_ref in stack.artifacts.stored
    assert facts.patch_ref == lease.patch_artifact_ref
    assert any(name.endswith("workspace-snapshot.json") for name in stack.artifacts.stored)
    assert stack.auth.calls == 1


# --- queued sends never become active-turn steering -----------------------------------------


async def test_a_send_on_an_active_thread_is_busy_and_never_a_turn_start(tmp_path: Path) -> None:
    stack = codex_stack(tmp_path, "turn_hold")
    session = await _started(stack)
    first = await _sent(stack, session, "turn:1")
    assert first.native_turn_ref == "turn-0002" and first.status == "accepted"
    server = stack.launcher.server
    await asyncio.wait_for(server.held.wait(), timeout=5)

    queued = await _sent(stack, session, "turn:2")
    again = await _sent(stack, session, "turn:2")
    assert queued.status == "busy" and again.status == "busy"
    assert [m for m, _p in server.records].count("turn/start") == 1
    assert server.steer_by_turn_start == 0, "a turn/start on an active thread would steer it"
    assert stack.harness.active_turn(stack.heid) == first.native_turn_ref

    server.release.set()
    frames = [
        frame
        async for frame in stack.harness.observe(
            __import__(
                "mission_control.domain.execution.lanes", fromlist=["ObserveRequest"]
            ).ObserveRequest(**_fields(stack), turn=first, after=None, max_frames=100)
        )
    ]
    assert frames[-1].terminal
    # Idle now: the queued send goes through as a new turn with its own identity.
    later = await _sent(stack, session, "turn:2")
    assert later.status == "accepted" and later.native_turn_ref not in {None, first.native_turn_ref}
    assert [m for m, _p in server.records].count("turn/start") == 2


# --- explicit steering --------------------------------------------------------------------


@pytest.mark.parametrize("stale", [False, True])
async def test_steer_lands_on_the_exact_turn_or_is_a_typed_stale_target(
    tmp_path: Path, stale: bool
) -> None:
    stack = codex_stack(tmp_path, "turn_steer", complete_before_steer=stale)
    session = await _started(stack)
    turn = await _sent(stack, session)
    server = stack.launcher.server
    await asyncio.wait_for(server.held.wait(), timeout=5)
    stack.harness.stage_turn(stack.heid, "inject:cmd-1", "Use release/2.3 instead.")

    steered = await stack.harness.steer(turn, instruction_ref="inject:cmd-1")

    if stale:
        assert steered.outcome == "stale_target" and "refused" in steered.detail
        assert server.steers == [], "the completed turn was never steered"
        assert stack.harness.turn_status(stack.heid, turn.native_turn_ref or "") == "completed"
    else:
        assert steered.outcome == "applied" and steered.target_turn_ref == turn.native_turn_ref
        (params,) = server.steers
        assert params["expectedTurnId"] == turn.native_turn_ref
        assert params["input"] == [{"type": "text", "text": "Use release/2.3 instead."}]
    assert server.interrupts == [] and [m for m, _p in server.records].count("turn/start") == 1
    # A steer whose text was never staged is requeued (stale_target), never sent blind.
    blind = await stack.harness.steer(turn, instruction_ref="inject:unstaged")
    assert blind.outcome == "stale_target" and "not staged" in blind.detail
    server.release.set()
    server.steer_received.set()


async def test_the_describe_interrupt_cell_follows_the_frozen_table(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from mission_control.adapters.codex.describe import codex_local_describe
    from mission_control.application.execution.harness.inject import resolve_interrupt_mode

    launcher = FixtureLauncher()
    harness, *_rest = build_harness(tmp_path, launcher)
    declared = LANE_COMMAND_SEMANTICS["interrupt_and_inject"]["codex"]
    assert harness.describe().delivery_semantics["interrupt_and_inject"] == declared
    assert resolve_interrupt_mode("codex", harness.describe(), harness) == declared
    assert codex_local_describe() == harness.describe()
    # The lane implements `turn/steer`: once the table and the declared matrix both say
    # `cooperative_inject` (a qualified decision), the same harness steers.
    monkeypatch.setitem(
        LANE_COMMAND_SEMANTICS["interrupt_and_inject"], "codex", "cooperative_inject"
    )
    steering = harness.describe().model_copy(
        update={
            "delivery_semantics": {
                **harness.describe().delivery_semantics,
                "interrupt_and_inject": "cooperative_inject",
            }
        }
    )
    assert resolve_interrupt_mode("codex", steering, harness) == "cooperative_inject"
    assert isinstance(harness, SteeringLane)


# --- interrupt: terminal completion is observed ---------------------------------------------


async def test_cancel_turn_interrupts_and_observes_the_terminal_completion(tmp_path: Path) -> None:
    stack = codex_stack(tmp_path, "turn_hold")
    session = await _started(stack)
    turn = await _sent(stack, session)
    server = stack.launcher.server
    await asyncio.wait_for(server.held.wait(), timeout=5)

    receipt = await stack.harness.cancel_turn(
        CancelTurnRequest(**_fields(stack), turn=turn, reason="command")
    )
    assert receipt.acknowledged and not receipt.already_terminal
    assert receipt.native_status == "interrupted", "observed from turn/completed, not assumed"
    assert server.interrupts == [turn.native_turn_ref]
    status = await stack.harness.status(StatusRequest(**_fields(stack), session=session, turn=turn))
    assert status.terminal and status.idle and status.status == "interrupted"
    again = await stack.harness.cancel_turn(
        CancelTurnRequest(**_fields(stack), turn=turn, reason="command")
    )
    assert again.already_terminal and again.native_status == "interrupted"
    from mission_control.domain.execution.lanes import ObserveRequest

    frames = [
        frame
        async for frame in stack.harness.observe(
            ObserveRequest(**_fields(stack), turn=turn, after=None, max_frames=100)
        )
    ]
    facts = stack.harness.closing_facts(turn, frames[-1])
    assert frames[-1].terminal and facts.native_status == "cancelled"


async def _held(stack: Any) -> None:
    """Wait until the fixture server was launched, its scripted turn is held and the lane
    state carries the native turn (the cancel then targets a recorded turn)."""

    await asyncio.wait_for(stack.launcher.launched.wait(), 10)
    await asyncio.wait_for(stack.launcher.server.held.wait(), 10)
    recorded = asyncio.Event()

    async def watch() -> None:
        while not recorded.is_set():
            state = await stack.lanes.states.load(
                stack.operation.request_scope, stack.identity.harness_execution_id
            )
            if state is not None and state.native_turn_ref is not None:
                recorded.set()
                return
            await asyncio.sleep(0.01)

    await asyncio.wait_for(watch(), 10)


async def test_lane_cancel_settles_cancelled_by_command_through_the_service(tmp_path: Path) -> None:
    stack = codex_stack(tmp_path, "turn_hold")
    signals = RecordingSignals(stack.lanes.frames, stack.identity.harness_execution_id, cancel=True)
    task = asyncio.create_task(stack.lanes.service.turn(stack.turn(segment=BOUNDS), signals))
    await _held(stack)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    outcome = await stack.lanes.service.cancel(
        LaneCancelRequest.model_validate(
            {"operation": stack.operation, "lane_profile": "codex", "generation": 1}
        )
    )
    settled = OperationExecutionResult.model_validate(outcome.operation_result)
    assert (settled.status, settled.failure_code) == ("cancelled", "cancelled")
    assert stack.launcher.server.interrupts, "the requested cancel reached the provider"


# --- lost response: reconciled from thread history, never re-sent ---------------------------


async def test_a_lost_turn_start_response_is_reconciled_from_history_and_never_resent(
    tmp_path: Path,
) -> None:
    stack = codex_stack(tmp_path, "turn_full", lose_response={"turn/start"}, request_timeout_s=0.3)
    with pytest.raises(ResponseLost):
        await stack.lanes.service.turn(
            stack.turn(segment=BOUNDS), RecordingSignals(stack.lanes.frames)
        )
    state = await stack.lanes.states.load(
        stack.operation.request_scope, stack.identity.harness_execution_id
    )
    assert state is not None and state.native_turn_ref is None
    record = state.dispatch("send", stack.send_key)
    assert record is not None and record.phase == "intended"

    result = await stack.lanes.service.turn(
        stack.turn(segment=BOUNDS), RecordingSignals(stack.lanes.frames)
    )

    assert result.done and _settled(result).status == "completed"
    server = stack.launcher.server
    assert [m for m, _p in server.records].count("turn/start") == 1, "reconciled, never re-sent"
    reads = [p for m, p in server.records if m == "thread/read" and p.get("includeTurns")]
    assert reads, "the lookup read the thread history"
    state = await stack.lanes.states.load(
        stack.operation.request_scope, stack.identity.harness_execution_id
    )
    assert state is not None and state.native_turn_ref == "turn-0002"
    acked = state.dispatch("send", stack.send_key)
    assert acked is not None and acked.phase == "acknowledged" and acked.native_ref == "turn-0002"
    assert isinstance(stack.harness, DispatchReconcilingLane)


# --- disconnect: the segment ends, the next one reattaches from history --------------------


async def test_a_disconnect_ends_the_segment_and_the_next_segment_reattaches(
    tmp_path: Path,
) -> None:
    stack = codex_stack(tmp_path, "turn_disconnect")
    task = asyncio.create_task(
        stack.lanes.service.turn(stack.turn(segment=BOUNDS), RecordingSignals(stack.lanes.frames))
    )
    await _held(stack)
    persisted = asyncio.Event()

    async def observed() -> None:
        while not persisted.is_set():
            if any(frame.tool_call_ref == "item-cmd-5" for frame in stack.frames()):
                persisted.set()
                return
            await asyncio.sleep(0.01)

    await asyncio.wait_for(observed(), 10)  # the segment is observing live
    stack.launcher.server.release.set()  # ... and now the app-server dies mid-turn
    first = await asyncio.wait_for(task, 30)
    assert not first.done, "the app-server died mid-turn: the segment ends without a settlement"
    assert first.native.turn_ref == "turn-0002"
    old = stack.launcher.launches[0]
    await asyncio.wait_for(old.task, timeout=5)  # the old fixture process finished offline

    second = await stack.lanes.service.turn(
        stack.turn(segment=BOUNDS, phase="resume", cursor=first.cursor, segment_no=2),
        RecordingSignals(stack.lanes.frames),
    )

    assert second.done and _settled(second).status == "completed"
    assert second.closing_facts is not None
    assert second.closing_facts.result_excerpt == "Finished while nobody was connected."
    assert len(stack.launcher.launches) == 2, "a relaunch over the same lease"
    new = stack.launcher.launches[1].server
    methods = [m for m, _p in new.records]
    assert "thread/resume" in methods and "turn/start" not in methods, "never sent again"
    frames = stack.frames()
    assert len({frame.provider_key for frame in frames}) == len(frames), "history deduped"
    assert sum(frame.kind == FrameKind.RUN_RESULT for frame in frames) == 1
    assert any(frame.kind == FrameKind.TOOL_CALL_COMPLETED for frame in frames)
    assert stack.launcher.launches[0].spec.cwd == stack.launcher.launches[1].spec.cwd


# --- capacity refusal -----------------------------------------------------------------------


async def test_a_limit_refusal_raises_capacity_limited_and_journals_declined(
    tmp_path: Path,
) -> None:
    stack = codex_stack(tmp_path, "turn_full", capacity_refusals=1)
    with pytest.raises(ProviderCapacityLimited) as limited:
        await stack.lanes.service.turn(
            stack.turn(segment=BOUNDS), RecordingSignals(stack.lanes.frames)
        )
    signal = limited.value.signal
    assert (signal.lane_profile, signal.kind, signal.native_code) == (
        "codex",
        "quota_exhausted",
        "usageLimitExceeded",
    )
    assert signal.source == "codex.turn_start"
    state = await stack.lanes.states.load(
        stack.operation.request_scope, stack.identity.harness_execution_id
    )
    assert state is not None
    record = state.dispatch("send", stack.send_key)
    assert record is not None and (record.phase, record.reason) == ("declined", "capacity")

    result = await stack.lanes.service.turn(
        stack.turn(segment=BOUNDS), RecordingSignals(stack.lanes.frames)
    )
    assert result.done and _settled(result).status == "completed"
    assert [m for m, _p in stack.launcher.server.records].count("turn/start") == 2


async def test_a_mid_turn_usage_limit_closes_with_the_capacity_error_code(tmp_path: Path) -> None:
    stack = codex_stack(tmp_path, "turn_limit_failure")
    result = await stack.lanes.service.turn(
        stack.turn(segment=BOUNDS), RecordingSignals(stack.lanes.frames)
    )
    assert result.done and result.closing_facts is not None
    assert result.closing_facts.native_status == "error"
    assert result.closing_facts.error_code == "capacity"
    assert result.closing_facts.error_message == "You have hit your usage limit."
    assert _settled(result).status == "failed"
    assert any(frame.kind == FrameKind.ERROR for frame in stack.frames())


# --- describe and host constraints ----------------------------------------------------------


async def test_the_describe_matrix_is_implemented_cell_by_cell(tmp_path: Path) -> None:
    harness, *_rest = build_harness(tmp_path, FixtureLauncher())
    describe = harness.describe()
    assert describe.is_v2 and describe.qualified is False
    assert describe.lane_profile == "codex" and describe.placement == "worker_hosted"
    for operation in HARNESS_PROTOCOL_METHODS:
        if operation == "describe":
            continue
        assert describe.control(operation) in {"native", "emulated"}, operation
        assert implements(harness, operation), operation
    assert describe.controls["fork"] == "unqualified" and not hasattr(harness, "fork")
    assert describe.controls["pause"] == "emulated" and callable(harness.hold_approvals)
    assert isinstance(harness, SteeringLane)
    assert isinstance(harness, DispatchReconcilingLane)
    assert isinstance(harness, TurnTextStaging)
    assert describe.identity.session_ref == "thread_id" and describe.identity.turn_ref == "turn_id"
    assert describe.identity.effect_ref == "item_id"
    assert describe.versions["codex_cli"] == "0.162.0"
    assert describe.compaction_control == "native" and callable(harness.compact)
    assert callable(harness.context_occupancy)
    assert all(not evidence.qualified for evidence in describe.features.values())
    assert describe.features["environment_selection"].status == "unsupported"
    assert describe.delivery_semantics["queue_instruction"] == "wait_then_send"


def test_the_binding_approval_policy_is_exactly_the_pinned_enum() -> None:
    """The contract no longer admits `on-failure` (absent from the pinned `AskForApproval`):
    a binding naming it is refused at validation, before any lane sees it."""

    from typing import get_args

    from pydantic import ValidationError

    from mission_control.adapters.codex.protocol import AskForApproval
    from mission_control.domain.execution.bindings import CodexAppServerOptions

    declared = CodexAppServerOptions.model_fields["approval_policy"].annotation
    assert set(get_args(declared)) == set(get_args(AskForApproval))
    with pytest.raises(ValidationError):
        codex_binding(
            provider_options={
                "provider": "codex_app_server",
                "approval_policy": "on-failure",
                "sandbox_mode": "read-only",
                "app_server_schema_version": "v2@0.162.0",
            }
        )


async def test_a_drifted_projection_is_capability_drift(tmp_path: Path) -> None:
    binding = codex_binding(materialization_digest="sha256:" + "d" * 64)
    stack = codex_stack(tmp_path, "turn_full", operation=codex_operation(binding))
    stack.harness.stage(stack.heid, stack.operation)
    with pytest.raises(LaneProjectionError) as refused:
        await stack.harness.prepare(
            PrepareRequest(
                **_fields(stack),
                run_id=stack.operation.identity.run_id,
                operation_id=stack.operation.identity.operation_id,
                attempt_no=1,
            )
        )
    assert refused.value.code == "CAPABILITY_DRIFT"


def test_child_environment_carries_no_credential_but_the_admitted_route(tmp_path: Path) -> None:
    build = provider_child_environment
    home = tmp_path / "home"
    login = child_environment(FAKE_WORKER_ENV, admission(), codex_home=home, builder=build)
    assert set(login) == {"PATH", "HOME", "CODEX_HOME"}
    api = child_environment(FAKE_WORKER_ENV, admission("api_key"), codex_home=home, builder=build)
    assert set(api) == {"PATH", "HOME", "CODEX_HOME", "OPENAI_API_KEY"}
    assert "ANTHROPIC_API_KEY" not in api and "CURSOR_API_KEY" not in api
    with pytest.raises(ValueError, match="re-introduce"):
        child_environment(
            FAKE_WORKER_ENV,
            admission(),
            codex_home=tmp_path,
            builder=build,
            extra={"OPENAI_API_KEY": "x"},
        )
    assert StaticAuth().record.route == "owner_cli_login"


def test_a_windows_selector_loop_cannot_run_the_lane(monkeypatch: pytest.MonkeyPatch) -> None:
    loop = asyncio.SelectorEventLoop()
    try:
        monkeypatch.setattr(sys, "platform", "win32")
        supported, detail = subprocess_supported(loop)
        assert not supported and "Linux/WSL" in detail
        monkeypatch.setattr(sys, "platform", "linux")
        assert subprocess_supported(loop) == (True, "posix event loop")
    finally:
        loop.close()


def test_fixture_scripts_are_labelled_and_never_claimed_recorded() -> None:
    from tests.unit.codex.fixture_app_server import SCRIPTS

    names = sorted(path.stem for path in SCRIPTS.glob("*.jsonl"))
    assert names == [
        "turn_disconnect",
        "turn_full",
        "turn_hold",
        "turn_limit_failure",
        "turn_pressure",
        "turn_steer",
        "turn_with_approval",
    ]
    for name in names:
        assert load_script(name)
