"""MP-07 conformance of the `claude_agent_sdk` lane through `lane.turn` (FIXTURE client):
describe honesty, start/observe, in-process resume, history loss after process death,
unknown frames, capacity refusal, dispatch reconciliation, the child environment and the
session state under the leased state root. No Claude Code runs."""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import UUID

import pytest

from mission_control.adapters.claude.describe import CLAUDE_AGENT_SDK_LANE_DESCRIBE
from mission_control.adapters.claude.harness import turn_reference
from mission_control.adapters.claude.workspace import (
    DispatchLedger,
    StateRootSessionStore,
    state_root,
)
from mission_control.application.execution.harness.describe import CLAUDE_AGENT_SDK_DESCRIBE
from mission_control.application.execution.harness.dispatch import (
    DispatchReconcilingLane,
    DispatchRecord,
    ProviderCapacityLimited,
)
from mission_control.application.execution.harness.lane_turns import (
    LaneExecutionIdentity,
    LaneTurnService,
    harness_scope,
)
from mission_control.application.execution.harness.protocol import (
    HARNESS_PROTOCOL_METHODS,
    NativeTurnLost,
    SessionLane,
    implements,
)
from mission_control.application.execution.harness.sessions import WorkerSessionManager
from mission_control.application.execution.harness.state import LaneExecutionUpdate
from mission_control.application.frames.kinds import UnknownKindCounter
from mission_control.domain.execution.contracts import OperationExecutionResult
from mission_control.domain.execution.lane_turns import (
    LANE_COMMAND_SEMANTICS,
    LANE_PAUSE_SEMANTICS,
    LANE_RESUME_SEMANTICS,
    LaneCancelRequest,
    LaneSegmentBounds,
    LaneStatusRequest,
    LaneTurnRequest,
)
from mission_control.domain.execution.lanes import HARNESS_OPERATIONS, ReattachRequest
from mission_control.domain.frames.contracts import FrameKind
from tests.fixtures.lane_turns import RecordingSignals, SimulatedWorkerLoss
from tests.unit.claude.fixtures import (
    PROFILE,
    SESSION_ID,
    ClaudeStack,
    claude_stack,
    fixture_admission,
)

SMALL = LaneSegmentBounds(
    max_frames=50,
    max_duration_s=20,
    start_to_close_s=40,
    heartbeat_timeout_s=3,
    status_poll_limit=2,
    status_poll_interval_s=1,
    busy_wait_s=10,
)


def _identity(stack: ClaudeStack) -> LaneExecutionIdentity:
    return LaneExecutionIdentity.of(stack.operation, PROFILE, 1)


def _turn(stack: ClaudeStack, **changes: Any) -> LaneTurnRequest:
    return LaneTurnRequest.model_validate(
        {
            "operation": stack.operation,
            "lane_profile": PROFILE,
            "generation": 1,
            "segment": SMALL,
            **changes,
        }
    )


def _frames(stack: ClaudeStack) -> list[Any]:
    execution = stack.frames._executions[_identity(stack).harness_execution_id]
    return sorted(execution.frames.values(), key=lambda frame: frame.arrival_ordinal)


def _settled(result: Any) -> OperationExecutionResult:
    return OperationExecutionResult.model_validate(result.operation_result)


def _fields(stack: ClaudeStack, key: str) -> dict[str, Any]:
    identity = _identity(stack)
    assert stack.operation.provider_binding is not None
    return {
        "scope": harness_scope(identity.request_scope),
        "lane_profile": PROFILE,
        "harness_execution_id": str(identity.harness_execution_id),
        "binding_digest": stack.operation.provider_binding.binding_digest,
        "idempotency_key": f"{identity.harness_execution_id}:1:{key}",
        "generation": 1,
    }


# --- describe -------------------------------------------------------------------------------


def test_the_proposed_describe_is_honest_and_unqualified(tmp_path: Path) -> None:
    stack = claude_stack(tmp_path)
    harness = stack.harness
    describe = harness.describe()
    assert describe is CLAUDE_AGENT_SDK_LANE_DESCRIBE and describe.is_v2
    assert describe.qualified is False
    assert all(not cell.qualified for cell in describe.features.values())
    for operation in HARNESS_OPERATIONS:
        support = describe.control(operation)
        assert support in {"native", "emulated"}, operation
        assert implements(harness, operation), operation
    for operation in HARNESS_PROTOCOL_METHODS:
        assert callable(getattr(harness, operation))
    assert (
        describe.controls["pause"] == "unqualified" and describe.controls["fork"] == "unqualified"
    )
    assert not hasattr(harness, "pause")
    # The frozen workflow tables and the MP-01 stub agree on every delivery semantic.
    assert describe.delivery_semantics == CLAUDE_AGENT_SDK_DESCRIBE.delivery_semantics
    assert LANE_PAUSE_SEMANTICS[PROFILE] == describe.delivery_semantics["pause"]
    assert LANE_RESUME_SEMANTICS[PROFILE] == describe.delivery_semantics["resume"]
    assert LANE_COMMAND_SEMANTICS["cancel"][PROFILE] == describe.delivery_semantics["cancel"]
    assert (
        LANE_COMMAND_SEMANTICS["interrupt_and_inject"][PROFILE]
        == describe.delivery_semantics["interrupt_and_inject"]
    )
    assert describe.versions["claude_agent_sdk"] == "0.2.165"
    assert describe.identity.session_ref == "session_id"
    assert describe.identity.turn_ref == "user_message_uuid"
    assert describe.identity.effect_ref == "tool_use_id"
    assert describe.usage.cost == "estimated"
    assert describe.compaction_control == "unqualified"
    assert describe.approval_modes == ("workflow_gate", "provider_permission", "governed_effect")
    assert isinstance(harness, SessionLane) and isinstance(harness, DispatchReconcilingLane)
    assert callable(harness.stage_turn)


# --- start / observe / end --------------------------------------------------------------------


async def test_a_full_turn_persists_frames_settles_and_keeps_state_under_the_lease(
    tmp_path: Path,
) -> None:
    stack = claude_stack(tmp_path)
    signals = RecordingSignals(stack.frames, _identity(stack).harness_execution_id)
    result = await stack.lanes.service.turn(_turn(stack), signals)
    assert result.done and result.closing_facts is not None
    assert result.closing_facts.native_status == "finished"
    assert result.closing_facts.result_excerpt == "BINDING-OK: report written."
    assert result.closing_facts.usage.disposition == "settled"
    assert result.closing_facts.cost_disposition == "estimated"
    assert result.closing_facts.patch_ref is not None and result.closing_facts.patch_ref.endswith(
        "patch.diff"
    )
    assert _settled(result).status == "completed"
    assert result.native.session_ref == SESSION_ID
    client = stack.factory.last
    (sent,) = client.sent
    identity = _identity(stack)
    assert sent[0] == turn_reference(f"{identity.harness_execution_id}:1:turn:1")
    assert result.native.turn_ref == sent[0]
    assert "Read .mission/context.md" in sent[1] and "Return BINDING-OK" in sent[1]
    kinds = [frame.kind for frame in _frames(stack)]
    assert kinds[0] is FrameKind.SESSION_INIT
    for expected in (
        FrameKind.MESSAGE,
        FrameKind.TOOL_CALL_STARTED,
        FrameKind.HOOK_INVOKED,
        FrameKind.HOOK_RESULT,
        FrameKind.TOOL_CALL_COMPLETED,
        FrameKind.TURN_ENDED,
        FrameKind.RUN_RESULT,
    ):
        assert expected in kinds, expected
    assert kinds[-1] is FrameKind.RUN_RESULT and kinds.count(FrameKind.RUN_RESULT) == 1
    # Every frame of the turn names the native session and the turn.
    assert {frame.native_session_ref for frame in _frames(stack)} == {SESSION_ID}
    assert {frame.native_turn_ref for frame in _frames(stack)[1:]} == {sent[0]}
    # Kernel hooks ran in-process: fence admitted the shell call, the intent was recorded.
    assert client.hook_calls == [("PreToolUse", "toolu_bash_01", False)]
    assert [intent.effect_ref for intent in stack.intents.intents()] == ["toolu_bash_01"]
    # The child environment came through provider_child_environment: API keys unset.
    env = client.environment
    assert "ANTHROPIC_API_KEY" not in env and "ANTHROPIC_AUTH_TOKEN" not in env
    assert env["OPENAI_API_KEY"] == "sk-FIXTURE-openai" and env["PATH"] == "/usr/bin"
    assert "CLAUDE_CONFIG_DIR" not in env, "the owner's CLI login keeps the owner's config dir"
    options = client.options
    assert options.setting_sources == ["project"]
    assert options.model == "claude-sonnet-4-5" and options.disallowed_tools == ["WebFetch"]
    assert options.hooks is not None and "PreToolUse" in options.hooks
    assert options.can_use_tool is not None and options.resume is None
    assert options.forward_subagent_text is True and options.session_store_flush == "eager"
    # Local session state lives under the leased state root.
    (lease,) = stack.workspace.registry.values()
    root = state_root(lease.path)
    assert isinstance(options.session_store, StateRootSessionStore)
    assert options.session_store.root == root / "sessions"
    assert options.session_store.transcript_present(SESSION_ID)
    ledger = json.loads((root / "dispatch.json").read_text(encoding="utf-8"))
    assert ledger[f"create:{identity.harness_execution_id}:1:turn:1"]["phase"] == "acknowledged"
    assert ledger[f"send:{identity.harness_execution_id}:1:turn:1"] == {
        "phase": "acknowledged",
        "native_ref": sent[0],
    }
    assert (Path(lease.path) / "CLAUDE.md").is_file()
    assert (Path(lease.path) / ".mission" / "operating-contract.md").is_file()
    assert (Path(lease.path) / ".claude" / "settings.json").is_file()
    # end_session: custody first, the lease released after it, the client disconnected.
    assert client.disconnected
    assert stack.workspace.captures and stack.workspace.released
    assert stack.workspace.released[0][1] is not None
    assert lease.released
    state = await stack.lanes.states.load(identity.request_scope, identity.harness_execution_id)
    assert state is not None and state.native_session_ref == SESSION_ID
    assert state.bridge_state_root == str(root)


async def test_an_api_key_route_relocates_the_cli_config_dir_under_the_lease(
    tmp_path: Path,
) -> None:
    stack = claude_stack(tmp_path, admission=fixture_admission(route="api_key", env_unset=()))
    signals = RecordingSignals(stack.frames, _identity(stack).harness_execution_id)
    result = await stack.lanes.service.turn(_turn(stack), signals)
    assert result.done and _settled(result).status == "completed"
    env = stack.factory.last.environment
    (lease,) = stack.workspace.registry.values()
    assert env["CLAUDE_CONFIG_DIR"] == str(state_root(lease.path) / "config")
    assert env["ANTHROPIC_API_KEY"] == "sk-ant-FIXTURE-0000", "the admitted credential stays"


async def test_a_lost_worker_resumes_in_process_from_the_cursor_without_a_second_send(
    tmp_path: Path,
) -> None:
    stack = claude_stack(tmp_path)
    identity = _identity(stack)
    signals = RecordingSignals(stack.frames, identity.harness_execution_id, fail_on=4)
    with pytest.raises(SimulatedWorkerLoss):
        await stack.lanes.service.turn(_turn(stack), signals)
    prior = signals.beats[-1][0]
    resumed = RecordingSignals(stack.frames, identity.harness_execution_id, prior=prior)
    result = await stack.lanes.service.turn(_turn(stack, phase="resume"), resumed)
    assert result.done and _settled(result).status == "completed"
    assert len(stack.factory.clients) == 1 and len(stack.factory.last.sent) == 1
    ordinals = [frame.arrival_ordinal for frame in _frames(stack)]
    assert ordinals == list(range(1, len(ordinals) + 1))
    keys = [frame.provider_key for frame in _frames(stack)]
    assert len(keys) == len(set(keys)), "a resume stores no frame twice"


async def test_history_loss_after_process_death_is_native_turn_lost_and_in_doubt(
    tmp_path: Path,
) -> None:
    registry: dict[UUID, Any] = {}
    first = claude_stack(tmp_path / "a", script="interrupted_then_replaced", registry=registry)
    identity = _identity(first)
    signals = RecordingSignals(first.frames, identity.harness_execution_id)
    running = asyncio.create_task(first.lanes.service.turn(_turn(first), signals))
    await asyncio.wait_for((await first.factory.connected()).held.wait(), timeout=10)
    async with asyncio.timeout(10):
        while True:
            recorded = await first.lanes.states.load(
                identity.request_scope, identity.harness_execution_id
            )
            if recorded is not None and recorded.native_turn_ref is not None:
                break
            await asyncio.sleep(0.01)
    # The worker dies with the turn running: the activity is cancelled without a requested
    # cancel (no provider cancel), the state keeps the native session and turn.
    running.cancel()
    with pytest.raises(asyncio.CancelledError):
        await running
    state = await first.lanes.states.load(identity.request_scope, identity.harness_execution_id)
    assert state is not None and state.native_session_ref == SESSION_ID
    assert state.native_turn_ref is not None
    # A new process (a new harness over the same lease registry and lane state).
    second = claude_stack(tmp_path / "a", script="full_run", registry=registry, frames=first.frames)
    second.lanes.states._states = first.lanes.states._states
    second.harness.stage(str(identity.harness_execution_id), second.operation)
    with pytest.raises(NativeTurnLost, match="new connection/generation"):
        await second.harness.reattach(
            ReattachRequest(
                **_fields(second, "turn:1"),
                native_session_ref=SESSION_ID,
                native_turn_ref=state.native_turn_ref,
            )
        )
    # The dead owner's lease must expire before another worker may take the session over
    # (MP-06): the takeover worker's clock is past it.
    takeover = LaneTurnService(
        lanes=second.lanes.service._lanes,
        boundary=second.lanes.boundary,
        frames=second.frames,
        states=second.lanes.states,
        sessions=WorkerSessionManager(owner_ref="worker-b"),
        clock=lambda: datetime.now(UTC) + timedelta(minutes=5),
    )
    result = await takeover.turn(_turn(second, phase="resume"), signals)
    assert result.done and result.closing_facts is not None
    assert result.closing_facts.native_status == "in_doubt"
    assert result.closing_facts.error_code == "native_turn_lost"
    assert _settled(result).status == "in_doubt"
    assert second.factory.clients == [], "nothing was resumed or sent for a lost turn"
    # Without a running turn the session resumes by id from the mirrored history.
    handle = await second.harness.reattach(
        ReattachRequest(**_fields(second, "turn:2"), native_session_ref=SESSION_ID)
    )
    assert handle.native_session_ref == SESSION_ID
    assert second.factory.last.options.resume == SESSION_ID
    assert second.factory.last.options.cwd == first.factory.last.options.cwd
    # And when the mirrored history is gone, the resume is refused as history loss.
    third = claude_stack(tmp_path / "a", script="full_run", registry=registry)
    third.harness.stage(str(identity.harness_execution_id), third.operation)
    (lease,) = registry.values()
    for path in StateRootSessionStore(state_root(lease.path)).transcript_paths(SESSION_ID):
        path.unlink()
    with pytest.raises(NativeTurnLost, match="no history"):
        await third.harness.reattach(
            ReattachRequest(**_fields(third, "turn:3"), native_session_ref=SESSION_ID)
        )
    assert third.factory.clients == []


async def test_unknown_frames_are_persisted_counted_and_never_close(tmp_path: Path) -> None:
    counter = UnknownKindCounter()
    stack = claude_stack(tmp_path, script="unknown_frames")
    stack.harness._counter = counter
    signals = RecordingSignals(stack.frames, _identity(stack).harness_execution_id)
    result = await stack.lanes.service.turn(_turn(stack), signals)
    assert result.done and _settled(result).status == "completed"
    frames = _frames(stack)
    # The SDK lifecycle facts with a kinds row are non-closing STATUS frames, persisted once.
    lifecycle = {"conversation_reset", "system.api_retry", "system.mirror_error"}
    mapped = [frame for frame in frames if frame.raw_kind in lifecycle]
    assert sorted(frame.raw_kind for frame in mapped) == sorted(lifecycle)
    assert all(frame.kind is FrameKind.STATUS and not frame.closing for frame in mapped)
    # A genuinely unmapped kind stays UNKNOWN: persisted, counted, never closing.
    unknown = [frame for frame in frames if frame.kind is FrameKind.UNKNOWN]
    assert [frame.raw_kind for frame in unknown] == ["system.fixture_unmapped_notice"]
    assert all(not frame.closing for frame in unknown)
    assert {raw for (_lane, raw) in counter.snapshot()} == {"system.fixture_unmapped_notice"}
    warning = next(frame for frame in frames if frame.raw_kind == "rate_limit_event")
    assert warning.kind is FrameKind.STATUS and not warning.closing
    assert frames[-1].kind is FrameKind.RUN_RESULT


async def test_a_capacity_refusal_at_connect_is_raised_before_any_send(tmp_path: Path) -> None:
    stack = claude_stack(tmp_path, script="capacity_rejected")
    identity = _identity(stack)
    signals = RecordingSignals(stack.frames, identity.harness_execution_id)
    with pytest.raises(ProviderCapacityLimited) as limited:
        await stack.lanes.service.turn(_turn(stack), signals)
    signal = limited.value.signal
    assert signal.kind == "rate_limited" and signal.source == "claude_sdk.rate_limit_event"
    assert signal.lane_profile == PROFILE and signal.resets_at is not None
    assert signal.window == "five_hour" and signal.utilization == 1.0
    assert stack.factory.last.sent == [], "nothing was written to the CLI"
    assert stack.factory.last.disconnected
    state = await stack.lanes.states.load(identity.request_scope, identity.harness_execution_id)
    assert state is not None
    create = state.dispatch("create", f"{identity.harness_execution_id}:1:turn:1")
    assert create is not None and (create.phase, create.reason) == ("declined", "capacity")


async def test_a_provider_error_result_settles_error_capacity(tmp_path: Path) -> None:
    stack = claude_stack(tmp_path, script="error_result")
    signals = RecordingSignals(stack.frames, _identity(stack).harness_execution_id)
    result = await stack.lanes.service.turn(_turn(stack), signals)
    assert result.done and result.closing_facts is not None
    assert (result.closing_facts.native_status, result.closing_facts.error_code) == (
        "error",
        "capacity",
    )
    assert _settled(result).status == "failed"


async def test_a_subprocess_that_dies_mid_turn_ends_with_a_synthesized_process_exit(
    tmp_path: Path,
) -> None:
    stack = claude_stack(tmp_path, script="process_dies")
    signals = RecordingSignals(stack.frames, _identity(stack).harness_execution_id)
    result = await stack.lanes.service.turn(_turn(stack), signals)
    assert result.done and result.closing_facts is not None
    assert (result.closing_facts.native_status, result.closing_facts.error_code) == (
        "error",
        "process_exit",
    )
    assert "exit code 143" in (result.closing_facts.error_message or "")
    final = _frames(stack)[-1]
    assert final.kind is FrameKind.RUN_RESULT
    assert json.loads(final.body_excerpt)["synthesized_from"] == "process_exit"


# --- MP-06 dispatch reconciliation -----------------------------------------------------------


async def test_reconcile_dispatch_answers_from_the_state_root_ledger(tmp_path: Path) -> None:
    stack = claude_stack(tmp_path)
    identity = _identity(stack)
    signals = RecordingSignals(stack.frames, identity.harness_execution_id)
    key = f"{identity.harness_execution_id}:1:turn:1"
    await stack.lanes.service.turn(_turn(stack), signals)
    (sent,) = stack.factory.last.sent
    stack.harness.stage(str(identity.harness_execution_id), stack.operation)
    execution = stack.harness.execution_view(str(identity.harness_execution_id))
    assert execution is not None
    (lease,) = stack.workspace.registry.values()
    execution.lease = lease

    def record(kind: str, idempotency_key: str) -> DispatchRecord:
        return DispatchRecord(
            kind=kind,  # type: ignore[arg-type]
            idempotency_key=idempotency_key,
            expected_generation=1,
            instruction_digest="sha256:" + "1" * 64,
            owner_ref="worker-a",
            owner_epoch=1,
            intended_at=_now(),
        )

    found = await stack.harness.reconcile_dispatch(record("send", key), session=None)
    assert (found.outcome, found.native_ref) == ("found", sent[0])
    created = await stack.harness.reconcile_dispatch(record("create", key), session=None)
    assert (created.outcome, created.native_ref) == ("found", SESSION_ID)
    never = await stack.harness.reconcile_dispatch(record("send", f"{key}:never"), session=None)
    assert never.outcome == "not_received"
    ledger = DispatchLedger(state_root(lease.path))
    await ledger.intend("send", f"{key}:ambiguous")
    ambiguous = await stack.harness.reconcile_dispatch(
        record("send", f"{key}:ambiguous"), session=None
    )
    assert ambiguous.outcome == "unknown"


async def test_a_lost_send_receipt_parks_in_doubt_instead_of_resending(tmp_path: Path) -> None:
    stack = claude_stack(tmp_path)
    identity = _identity(stack)
    signals = RecordingSignals(stack.frames, identity.harness_execution_id)

    original = stack.factory.create

    def create(options: Any, *, environment: Any) -> Any:
        client = original(options, environment=environment)
        real_query = client.query

        async def lossy(prompt: Any, session_id: str = "default") -> None:
            await real_query(prompt, session_id)
            raise ConnectionError("the receipt was lost after the CLI took the message")

        client.query = lossy  # type: ignore[method-assign]
        return client

    stack.factory.create = create  # type: ignore[method-assign]
    with pytest.raises(ConnectionError):
        await stack.lanes.service.turn(_turn(stack), signals)
    assert len(stack.factory.last.sent) == 1
    retried = await stack.lanes.service.turn(_turn(stack), signals)
    assert retried.done and retried.closing_facts is not None
    assert retried.closing_facts.native_status == "in_doubt"
    assert retried.closing_facts.error_code == "send_dispatch_ambiguous"
    assert len(stack.factory.last.sent) == 1, "an ambiguous send is never repeated"
    state = await stack.lanes.states.load(identity.request_scope, identity.harness_execution_id)
    assert state is not None
    send = state.dispatch("send", f"{identity.harness_execution_id}:1:turn:1")
    assert send is not None and send.phase == "in_doubt"


# --- status and state --------------------------------------------------------------------------


async def test_status_without_a_live_session_is_lost_and_cancel_is_unacknowledged(
    tmp_path: Path,
) -> None:
    stack = claude_stack(tmp_path)
    identity = _identity(stack)
    await stack.lanes.states.record(
        identity.request_scope,
        identity.harness_execution_id,
        LaneExecutionUpdate(native_session_ref=SESSION_ID, native_turn_ref="turn-x"),
    )
    status = await stack.lanes.service.status(
        LaneStatusRequest(operation=stack.operation, lane_profile=PROFILE, generation=1)
    )
    assert (status.status, status.terminal) == ("lost", False)
    cancel = await stack.lanes.service.cancel(
        LaneCancelRequest(operation=stack.operation, lane_profile=PROFILE, generation=1)
    )
    assert cancel.receipt.acknowledged is False and cancel.receipt.native_status == "unknown"
    assert cancel.settled is False


async def test_the_state_root_session_store_mirrors_lists_and_deletes(tmp_path: Path) -> None:
    store = StateRootSessionStore(tmp_path / "state")
    key = {"project_key": "/work/space", "session_id": SESSION_ID}
    await store.append(key, [{"type": "user", "uuid": "u-1"}, {"type": "assistant", "uuid": "a-1"}])
    await store.append(key, [{"type": "assistant", "uuid": "a-1"}, {"type": "title", "title": "x"}])
    loaded = await store.load(key)
    assert loaded is not None and [entry.get("uuid") for entry in loaded] == ["u-1", "a-1", None]
    assert store.transcript_present(SESSION_ID)
    sub = {**key, "subpath": "subagents/agent-1"}
    await store.append(sub, [{"type": "assistant", "uuid": "s-1"}])
    assert await store.list_subkeys({"project_key": "/work/space", "session_id": SESSION_ID}) == [
        "subagents_agent-1"
    ]
    listed = await store.list_sessions("/work/space")
    assert [item["session_id"] for item in listed] == [SESSION_ID]
    summaries = await store.list_session_summaries("/work/space")
    assert summaries[0]["session_id"] == SESSION_ID and summaries[0]["mtime"] == listed[0]["mtime"]
    assert await store.load({"project_key": "/work/space", "session_id": "absent"}) is None
    await store.delete(key)
    assert not store.transcript_present(SESSION_ID)
    assert await store.list_subkeys({"project_key": "/work/space", "session_id": SESSION_ID}) == []


def _now() -> datetime:
    return datetime.now(UTC)
