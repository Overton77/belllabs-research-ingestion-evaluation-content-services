"""MP-07 x MP-12: context occupancy and the sealed-checkpoint continuation into a fresh session.

FIXTURES: the scripted SDK client (`tests/unit/claude/fixtures.py`) and the MP-12 in-memory
continuation service (`tests/fixtures/continuation.py`). The continuation is sealed from the
live lease through `ClaudeWorkspaceSnapshots`, hydrated by `ClaudeSessionHydrator` into a
**fresh** `ClaudeSDKClient` session (no `resume`, no `fork_session`) and the continuation
turn runs through the production `LaneTurnService` handover. No Claude Code runs.
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

import pytest

from mission_control.adapters.claude.continuation import (
    WORKSPACE_SNAPSHOT_PREFIX,
    ClaudeSessionHydrator,
    ClaudeWorkspaceSnapshots,
    claude_continuation_registration,
)
from mission_control.adapters.claude.harness import occupancy_from_context_usage
from mission_control.application.context.continuation import ContinuationRejected
from mission_control.application.context.hydrators import LaneHydratorRegistry
from mission_control.application.context.lane_support import (
    CompactingLane,
    ContextOccupancyLane,
)
from mission_control.application.execution.harness.controls import SessionHandoverLane
from mission_control.application.execution.harness.lane_turns import LaneExecutionIdentity
from mission_control.domain.context import checkpoint
from mission_control.domain.context.checkpoint import (
    CheckpointIdentities,
    ContinuationUnsupported,
    continuation_delivery,
)
from mission_control.domain.execution.contracts import OperationExecutionResult
from mission_control.domain.execution.lane_turns import LaneSegmentBounds, LaneTurnRequest
from mission_control.domain.frames.contracts import FrameKind
from tests.fixtures.continuation import (
    CONT_SCOPE,
    FakeStaging,
    build_service,
    facts,
    seal_target,
    trigger,
)
from tests.fixtures.lane_turns import RecordingSignals
from tests.unit.claude.fixtures import (
    PROFILE,
    SESSION_ID,
    ClaudeStack,
    FixtureScript,
    claude_stack,
)

TARGET_SESSION = "sess-fixture-claude-0002"
SEGMENT = LaneSegmentBounds(
    max_frames=50,
    max_duration_s=20,
    start_to_close_s=40,
    heartbeat_timeout_s=3,
    status_poll_limit=2,
    status_poll_interval_s=1,
    busy_wait_s=10,
)
USAGE = {
    "categories": [{"name": "Messages", "tokens": 41_000, "color": "fixture"}],
    "totalTokens": 52_000,
    "maxTokens": 167_000,
    "rawMaxTokens": 200_000,
    "percentage": 26.0,
    "model": "claude-sonnet-4-5",
    "isAutoCompactEnabled": True,
    "memoryFiles": [],
    "mcpTools": [],
    "agents": [],
    "gridRows": [],
}


def _identity(stack: ClaudeStack) -> LaneExecutionIdentity:
    return LaneExecutionIdentity.of(stack.operation, PROFILE, 1)


def _turn(stack: ClaudeStack) -> LaneTurnRequest:
    return LaneTurnRequest(
        operation=stack.operation, lane_profile=PROFILE, generation=1, segment=SEGMENT
    )


def _frames(stack: ClaudeStack) -> list[Any]:
    execution = stack.frames._executions[_identity(stack).harness_execution_id]
    return sorted(execution.frames.values(), key=lambda frame: frame.arrival_ordinal)


# --- context occupancy -------------------------------------------------------------------------


def test_the_harness_exposes_occupancy_and_handover_but_no_compaction_control(
    tmp_path: Path,
) -> None:
    harness = claude_stack(tmp_path).harness
    assert isinstance(harness, ContextOccupancyLane)
    assert isinstance(harness, SessionHandoverLane)
    # `PreCompact` is observed only: no explicit compaction operation is claimed.
    assert not isinstance(harness, CompactingLane)
    assert harness.describe().compaction_control == "unqualified"


async def test_occupancy_reads_get_context_usage_against_the_raw_model_window(
    tmp_path: Path,
) -> None:
    stack = claude_stack(tmp_path, script="interrupted_then_replaced")
    stack.factory.context_usage = USAGE
    signals = RecordingSignals(stack.frames, _identity(stack).harness_execution_id)
    heid = str(_identity(stack).harness_execution_id)
    stack.harness.stage(heid, stack.operation)
    before = await stack.harness.context_occupancy(heid, _handle(stack), None)
    assert before is not None and not before.known and "no live Claude session" in before.reason
    # A live session (its turn held): the control request reads the window.
    running = asyncio.create_task(_live_turn(stack, signals))
    client = await stack.factory.connected()
    await asyncio.wait_for(client.held.wait(), timeout=10)
    assert stack.harness.live_execution(SESSION_ID) == heid
    occupancy = await stack.harness.context_occupancy(heid, _handle(stack), None)
    assert occupancy is not None and occupancy.known
    assert (occupancy.used_tokens, occupancy.window_tokens) == (52_000, 200_000)
    assert occupancy.source == "provider_context_window"
    assert str(occupancy.ratio) == "0.2600"
    assert client.context_usage_calls == 1
    client.release.set()
    await asyncio.wait_for(running, timeout=10)


def _handle(stack: ClaudeStack) -> Any:
    from mission_control.domain.execution.lanes import SessionHandle

    return SessionHandle(
        lane_profile=PROFILE,
        harness_execution_id=str(_identity(stack).harness_execution_id),
        generation=1,
        native_session_ref=SESSION_ID,
    )


async def _live_turn(stack: ClaudeStack, signals: RecordingSignals) -> Any:
    return await stack.lanes.service.turn(_turn(stack), signals)


@pytest.mark.parametrize(
    "usage",
    [
        {"totalTokens": 52_000},  # no window
        {"totalTokens": "52000", "rawMaxTokens": 200_000},  # not a count
        {"totalTokens": True, "rawMaxTokens": 200_000},
        {"totalTokens": 52_000, "rawMaxTokens": 0},
    ],
)
def test_incomplete_context_usage_is_unknown_never_invented(usage: dict[str, Any]) -> None:
    occupancy = occupancy_from_context_usage(usage)
    assert not occupancy.known and occupancy.used_tokens is None


async def test_a_failing_control_request_reports_unknown_with_the_reason(tmp_path: Path) -> None:
    stack = claude_stack(tmp_path, script="interrupted_then_replaced")
    stack.factory.context_usage = TimeoutError()
    signals = RecordingSignals(stack.frames, _identity(stack).harness_execution_id)
    running = asyncio.create_task(_live_turn(stack, signals))
    client = await stack.factory.connected()
    await asyncio.wait_for(client.held.wait(), timeout=10)
    heid = str(_identity(stack).harness_execution_id)
    occupancy = await stack.harness.context_occupancy(heid, _handle(stack), None)
    assert occupancy is not None and not occupancy.known
    assert "get_context_usage failed: TimeoutError" in occupancy.reason
    client.release.set()
    await asyncio.wait_for(running, timeout=10)


# --- continuation: seal from the live lease, hydrate a fresh session ---------------------------


async def _seal_and_transfer(
    stack: ClaudeStack, staging: FakeStaging
) -> tuple[dict[str, Any], Any, ClaudeSessionHydrator]:
    snapshots = ClaudeWorkspaceSnapshots(stack.harness, staging)
    hydrator = ClaudeSessionHydrator(stack.harness, staging)
    wired = build_service(snapshots=snapshots, staging=staging)
    service = wired["service"]
    transfer = await service.request(
        trigger(),
        request_scope=CONT_SCOPE,
        run_key="run-continuation-1",
        activation_key="unit-collect-1",
        logical_execution_id="logical-collect-1",
        lane_profile=PROFILE,
        source_session_ref=SESSION_ID,
    )
    base = facts()
    sealed = await service.seal(
        transfer.transfer_id,
        facts(
            lane_profile=PROFILE,
            identities=CheckpointIdentities(
                **{**base.identities.model_dump(), "source_agent_session_ref": SESSION_ID}
            ),
        ),
        seal_target(),
        request_scope=CONT_SCOPE,
    )
    assert sealed.checkpoint is not None, sealed.transfer
    outcome = await service.transfer(transfer.transfer_id, hydrator, request_scope=CONT_SCOPE)
    return wired, outcome, hydrator


def test_the_continuation_service_admits_this_lane_at_a_turn_boundary() -> None:
    """Integrated 2026-10-09: `domain/context/checkpoint._DELIVERY_BY_LANE` declares the
    lane's handover at the finished source turn's boundary; hosted Claude stays refused."""

    assert continuation_delivery(PROFILE) == "turn_boundary_guaranteed"
    with pytest.raises(ContinuationUnsupported):
        continuation_delivery("claude_cloud")


@pytest.fixture
def claude_delivery_row(monkeypatch: pytest.MonkeyPatch) -> None:
    """The lane's declared queue semantics (now in the integrator-owned table, kept explicit
    here): `turn_boundary_guaranteed` (the handover sends at the finished source turn's
    boundary)."""

    monkeypatch.setitem(checkpoint._DELIVERY_BY_LANE, PROFILE, "turn_boundary_guaranteed")


@pytest.mark.usefixtures("claude_delivery_row")
async def test_a_sealed_continuation_hydrates_a_fresh_session_that_runs_the_next_turn(
    tmp_path: Path,
) -> None:
    from mission_control.application.context.continuation import TransferStatus

    stack = claude_stack(tmp_path, script="continuation_source")
    stack.factory.fresh_scripts.append(FixtureScript.load("continuation_target"))
    identity = _identity(stack)
    signals = RecordingSignals(stack.frames, identity.harness_execution_id)
    running = asyncio.create_task(_live_turn(stack, signals))
    source_client = await stack.factory.connected()
    await asyncio.wait_for(source_client.held.wait(), timeout=10)
    root = Path(stack.harness.lease_path(str(identity.harness_execution_id)))
    (root / "outputs").mkdir(exist_ok=True)
    (root / "outputs" / "draft.md").write_text("draft from the source session\n", encoding="utf-8")

    staging = FakeStaging()
    wired, outcome, hydrator = await _seal_and_transfer(stack, staging)
    assert outcome.transfer.status == TransferStatus.TRANSFERRED, outcome.transfer
    assert outcome.receipt is not None
    receipt = outcome.receipt
    assert receipt.target_session_ref == TARGET_SESSION and hydrator.hydrated == [TARGET_SESSION]
    assert receipt.native_identity["conversation"] == "fresh"
    assert receipt.native_identity["source_session_id"] == SESSION_ID
    assert "/.mission/context.md" in receipt.restored and "/outputs/draft.md" in receipt.restored
    # The snapshot never carries the lane's state root (mirror, ledger, config dir).
    assert (root / ".mission" / "state" / "claude").is_dir()
    frozen = await wired["snapshots"].snapshot(
        request_scope=CONT_SCOPE, run_key="run-continuation-1", session_ref=SESSION_ID, roots=()
    )
    assert "/outputs/draft.md" in frozen.manifest
    assert not any(path.startswith("/.mission/state") for path in frozen.manifest)
    assert wired["events"].actions[-1].event == "transferred"
    # The target is a *fresh* SDK session: no resume, no conversation fork.
    target_client = stack.factory.clients[1]
    assert target_client.options.resume is None and target_client.options.fork_session is False
    assert target_client.options.cwd == source_client.options.cwd, "same lease"
    assert target_client.sent == [], "nothing is sent before the handover turn"

    # The source turn finishes; the next turn of the same segment runs on the target.
    source_client.release.set()
    result = await asyncio.wait_for(running, timeout=30)
    assert result.done
    settled = OperationExecutionResult.model_validate(result.operation_result)
    assert settled.status == "completed"
    assert result.closing_facts is not None
    assert result.closing_facts.result_excerpt.startswith("CONTINUED:")
    assert len(source_client.sent) == 1 and len(target_client.sent) == 1
    sent_text = target_client.sent[0][1]
    assert "# Mission context packet" in sent_text and "- purpose: continuation" in sent_text
    assert source_client.disconnected, "the superseded source connection is closed"
    state = await stack.lanes.states.load(identity.request_scope, identity.harness_execution_id)
    assert state is not None and state.native_session_ref == TARGET_SESSION
    inits = [
        frame
        for frame in _frames(stack)
        if frame.kind is FrameKind.SESSION_INIT and frame.native_session_ref == TARGET_SESSION
    ]
    assert inits, "the target session's session_init frame confirms the hydration"
    execution = stack.harness.execution_view(str(identity.harness_execution_id))
    assert execution is None or execution.pending is None


def _harness_fields(stack: ClaudeStack, key: str) -> dict[str, Any]:
    from mission_control.application.execution.harness.lane_turns import harness_scope

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


async def _hydrated(stack: ClaudeStack) -> tuple[str, Any]:
    """A started source session and a hydrated (not yet adopted) fresh target."""

    from mission_control.domain.execution.lanes import PrepareRequest, StartRequest

    harness = stack.harness
    heid = str(_identity(stack).harness_execution_id)
    harness.stage(heid, stack.operation)
    prepare = PrepareRequest(
        **_harness_fields(stack, "prepare"),
        run_id=stack.operation.identity.run_id,
        operation_id=stack.operation.identity.operation_id,
        attempt_no=1,
    )
    prepared = await harness.prepare(prepare)
    await harness.start(StartRequest(**_harness_fields(stack, "turn:1"), prepared=prepared))
    root = Path(harness.lease_path(heid))
    (root / ".mission").mkdir(exist_ok=True)
    (root / ".mission" / "context.md").write_text("continuation packet\n", encoding="utf-8")
    handover = await harness.hydrate_session(
        heid, transfer_id="tr-1", prompt_text="continue", source_session_ref=SESSION_ID
    )
    assert handover.session.native_session_ref == TARGET_SESSION
    # A later turn of the same generation never re-materializes over the continuation packet.
    assert await harness.prepare(prepare) == prepared
    assert (root / ".mission" / "context.md").read_text(encoding="utf-8") == (
        "continuation packet\n"
    )
    return heid, handover


async def test_sending_the_continuation_turn_consumes_the_pending_handover(
    tmp_path: Path,
) -> None:
    """MP-20 finding: a handover still pending after the activated target's first send was
    offered again at the target's terminal frame and sent the continuation turn twice."""

    from mission_control.domain.execution.lanes import SendTurnRequest

    stack = claude_stack(tmp_path, script="continuation_source")
    stack.factory.fresh_scripts.append(FixtureScript.load("continuation_target"))
    heid, handover = await _hydrated(stack)
    source, target = stack.factory.clients
    await stack.harness.send_turn(
        SendTurnRequest(
            **_harness_fields(stack, "turn:2"),
            session=handover.session,
            turn_no=2,
            instruction_ref=handover.instruction_ref,
        )
    )
    assert await stack.harness.pending_handover(heid) is None
    assert [text for _uuid, text in target.sent] == ["continue"] and source.sent == []
    assert source.disconnected
    # The in-segment completion is a no-op once the send adopted the target.
    await stack.harness.complete_handover(heid, handover.transfer_id)
    assert stack.harness.live_execution(TARGET_SESSION) == heid


async def test_reattaching_the_activated_target_adopts_it_without_a_resume(
    tmp_path: Path,
) -> None:
    from mission_control.domain.execution.lanes import ReattachRequest

    stack = claude_stack(tmp_path, script="continuation_source")
    stack.factory.fresh_scripts.append(FixtureScript.load("continuation_target"))
    heid, handover = await _hydrated(stack)
    session = await stack.harness.reattach(
        ReattachRequest(**_harness_fields(stack, "turn:2"), native_session_ref=TARGET_SESSION)
    )
    assert session.native_session_ref == TARGET_SESSION
    assert await stack.harness.pending_handover(heid) is None
    assert len(stack.factory.clients) == 2, "no second connection resumes the target"
    assert all(client.options.resume is None for client in stack.factory.clients)
    assert stack.factory.clients[0].disconnected and handover.transfer_id == "tr-1"


async def test_hydration_refuses_a_session_this_worker_does_not_hold(tmp_path: Path) -> None:
    stack = claude_stack(tmp_path)
    staging = FakeStaging()
    snapshots = ClaudeWorkspaceSnapshots(stack.harness, staging)
    with pytest.raises(ContinuationRejected) as rejected:
        await snapshots.snapshot(
            request_scope=CONT_SCOPE, run_key="run-1", session_ref="sess-elsewhere", roots=()
        )
    assert rejected.value.code == "CHECKPOINT_INVALID"
    assert await snapshots.load(request_scope=CONT_SCOPE, snapshot_ref="other:1") is None
    assert (
        await snapshots.load(
            request_scope=CONT_SCOPE, snapshot_ref=f"{WORKSPACE_SNAPSHOT_PREFIX}missing"
        )
        is None
    )


def test_the_registration_resolves_this_lane_only_and_stays_unqualified(tmp_path: Path) -> None:
    harness = claude_stack(tmp_path).harness
    registry = LaneHydratorRegistry()
    registry.register(claude_continuation_registration(harness, FakeStaging()))
    assert registry.profiles() == (PROFILE,)
    assert isinstance(registry.hydrator(PROFILE, CONT_SCOPE), ClaudeSessionHydrator)
    assert isinstance(registry.snapshots(PROFILE, CONT_SCOPE), ClaudeWorkspaceSnapshots)
    assert registry.registration(PROFILE).qualified is False
    with pytest.raises(ContinuationRejected):
        registry.hydrator("codex", CONT_SCOPE)
    # The declared describe states the emulated transfer, never qualified without a drill.
    cell = harness.describe().features["continuation"]
    assert cell.status == "emulated" and cell.implemented and not cell.qualified


def test_the_final_text_is_the_whole_result_never_the_excerpt(tmp_path: Path) -> None:
    """MP-20: the Completion Candidate reads `ResultMessage.result` whole (`FinalTextLane`);
    the closing facts keep only the bounded excerpt."""

    from mission_control.application.execution.harness.lane_turns import FinalTextLane
    from mission_control.domain.execution.lane_turns import MAX_EXCERPT_CHARS
    from mission_control.domain.execution.lanes import LaneFrame, TurnHandle

    harness = claude_stack(tmp_path).harness
    assert isinstance(harness, FinalTextLane)
    answer = '{"obligation_refs": ["p"], "note": "' + "x" * (2 * MAX_EXCERPT_CHARS) + '"}'
    frame = LaneFrame(
        harness_execution_id=str(_identity(claude_stack(tmp_path)).harness_execution_id),
        generation=1,
        provider_key="claude:r-1:result",
        cursor="1",
        kind="result",
        raw_kind="result",
        terminal=True,
        digest="sha256:" + "0" * 64,
        body={"type": "result", "subtype": "success", "is_error": False, "result": answer},
    )
    turn = TurnHandle(session=_handle(claude_stack(tmp_path)), turn_no=1, native_turn_ref="t-1")
    assert harness.final_text(turn, frame) == answer
    assert len(harness.closing_facts(turn, frame).result_excerpt) == MAX_EXCERPT_CHARS
    no_result = frame.model_copy(update={"body": {"type": "result", "result": None}})
    assert harness.final_text(turn, no_result) is None
