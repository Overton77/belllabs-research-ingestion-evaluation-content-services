"""MP-08 x MP-12: the codex continuation (sealed checkpoint -> a fresh thread).

FIXTURES: the FIXTURE app-server (`tests/unit/codex/fixture_app_server.py`) and the MP-12
in-memory continuation service (`tests/fixtures/continuation.py`). Proves:

- the snapshot port freezes `inputs/`, `outputs/`, `.mission/` and takes custody of the lease's
  git patch, never the lane's state root or `CODEX_HOME`;
- the hydrator restores the snapshot and the continuation packet into the live lease and
  starts a FRESH thread on a FRESH app-server (`thread/start`; no `thread/resume`, no
  `thread/fork`); nothing is sent to the target before the handover turn;
- the phase machine (`ContinuationPhaseService` + `LaneTurnService`): the requested
  continuation ends the source turn at its boundary, the phases activate the hydrated target,
  and the next `lane.turn` adopts it (the source app-server terminated) and sends exactly one
  continuation turn carrying the sealed packet;
- the in-segment handover (`SessionHandoverLane`): a continuation hydrated while the source
  turn runs is sent at that turn's boundary within the same segment;
- the registration resolves only `codex` and stays unqualified; the lane's delivery row is
  `wait_then_send`.
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

import pytest

from mission_control.adapters.codex.continuation import (
    PATCH_PATH,
    WORKSPACE_SNAPSHOT_PREFIX,
    CodexSessionHydrator,
    CodexWorkspaceSnapshots,
    codex_continuation_registration,
)
from mission_control.application.context.continuation import (
    ContinuationRejected,
    TransferStatus,
)
from mission_control.application.context.facts import FactsContext, OperationFactsCapture
from mission_control.application.context.hydrators import LaneHydratorRegistry
from mission_control.application.context.lane_continuation import (
    LaneContinuationCoordinator,
    LaneStateActivation,
)
from mission_control.application.context.phases import ContinuationPhaseService
from mission_control.application.execution.harness.controls import SessionHandoverLane
from mission_control.application.execution.harness.lane_turns import LaneTurnService
from mission_control.application.execution.harness.sessions import WorkerSessionManager
from mission_control.domain.context.checkpoint import CheckpointIdentities, continuation_delivery
from mission_control.domain.execution.contracts import OperationExecutionResult
from mission_control.domain.execution.lanes import LaneSegmentBounds
from mission_control.domain.frames.contracts import FrameKind
from tests.fixtures.continuation import (
    CONT_SCOPE,
    FakeStaging,
    build_service,
    facts,
    seal_target,
    trigger,
)
from tests.fixtures.lane_turns import SCOPE, RecordingSignals
from tests.unit.codex.fixture_app_server import FixtureLauncher, load_script
from tests.unit.codex.support import CodexStack, codex_stack

SOURCE = "thr-0001"
BOUNDS = LaneSegmentBounds(max_frames=200, max_duration_s=20, heartbeat_timeout_s=3)


def _stack(tmp_path: Path, source: str, target: str = "turn_full") -> CodexStack:
    launcher = FixtureLauncher(
        scripts=[load_script(source)], launch_scripts={1: [load_script(target)]}
    )
    return codex_stack(tmp_path, launcher=launcher, approvals=False)


def _methods(stack: CodexStack, launch: int) -> list[str]:
    return [method for method, _params in stack.launcher.launches[launch].server.records]


def _turn_texts(stack: CodexStack, launch: int) -> list[str]:
    return [
        "".join(item.get("text", "") for item in params.get("input", []))
        for method, params in stack.launcher.launches[launch].server.records
        if method == "turn/start"
    ]


# --- registration and delivery -----------------------------------------------------------------


def test_the_registration_resolves_this_lane_only_and_stays_unqualified(tmp_path: Path) -> None:
    stack = codex_stack(tmp_path, approvals=False)
    registry = LaneHydratorRegistry()
    registry.register(codex_continuation_registration(stack.harness, FakeStaging()))
    assert registry.profiles() == ("codex",)
    assert isinstance(registry.hydrator("codex", CONT_SCOPE), CodexSessionHydrator)
    assert isinstance(registry.snapshots("codex", CONT_SCOPE), CodexWorkspaceSnapshots)
    assert registry.registration("codex").qualified is False
    with pytest.raises(ContinuationRejected):
        registry.hydrator("claude_agent_sdk", CONT_SCOPE)
    assert stack.harness.describe().features["continuation"].qualified is False
    assert isinstance(stack.harness, SessionHandoverLane)
    # The continuation turn is a `turn/start` sent only on the idle fresh thread.
    assert continuation_delivery("codex") == "wait_then_send"


async def test_snapshots_refuse_a_thread_this_worker_does_not_hold(tmp_path: Path) -> None:
    stack = codex_stack(tmp_path, approvals=False)
    snapshots = CodexWorkspaceSnapshots(stack.harness, FakeStaging())
    with pytest.raises(ContinuationRejected) as rejected:
        await snapshots.snapshot(
            request_scope=CONT_SCOPE, run_key="run-1", session_ref="thr-elsewhere", roots=()
        )
    assert rejected.value.code == "CHECKPOINT_INVALID"
    assert await snapshots.load(request_scope=CONT_SCOPE, snapshot_ref="other:1") is None
    missing = f"{WORKSPACE_SNAPSHOT_PREFIX}missing"
    assert await snapshots.load(request_scope=CONT_SCOPE, snapshot_ref=missing) is None


# --- the phase machine: boundary, phases, activated target, continuation turn -------------------


async def test_a_requested_continuation_activates_a_fresh_thread_that_runs_the_next_turn(
    tmp_path: Path,
) -> None:
    stack = _stack(tmp_path, "turn_full")
    staging = FakeStaging()
    snapshots = CodexWorkspaceSnapshots(stack.harness, staging)
    wired = build_service(snapshots=snapshots, staging=staging)
    registry = LaneHydratorRegistry(
        {"codex": codex_continuation_registration(stack.harness, staging)}
    )
    phases = ContinuationPhaseService(
        wired["service"],
        lanes=registry,
        facts=OperationFactsCapture(),
        activation=LaneStateActivation(stack.lanes.states),
    )
    coordinator = LaneContinuationCoordinator(
        wired["store"], checkpoints=wired["service"].checkpoint_store, packets=wired["selections"]
    )
    service = LaneTurnService(
        lanes=stack.lanes.service._lanes,
        boundary=stack.lanes.boundary,
        frames=stack.lanes.frames,
        states=stack.lanes.states,
        frame_reader=stack.lanes.frames,
        sessions=WorkerSessionManager(owner_ref="worker-a"),
        continuations=coordinator,
    )
    identity = stack.identity
    heid = identity.harness_execution_id
    transfer = await wired["service"].request(
        trigger(),
        request_scope=SCOPE,
        run_key=identity.run_key,
        activation_key=identity.activation_key,
        logical_execution_id=identity.activation_key,
        lane_profile="codex",
        source_session_ref=SOURCE,
    )

    # 1. The source turn finishes; the segment ends at the continuation boundary.
    boundary = await service.turn(stack.turn(segment=BOUNDS), RecordingSignals(stack.lanes.frames))
    assert not boundary.done and boundary.closing_facts is not None
    assert boundary.native.session_ref == SOURCE
    root = Path(stack.harness.lease_path(str(heid)))
    (root / "outputs").mkdir(exist_ok=True)
    (root / "outputs" / "draft.md").write_text("draft from the source thread\n", encoding="utf-8")

    # 2. The phases: freeze, snapshot, seal, hydrate (a fresh thread), verify, activate.
    state = await stack.lanes.states.load(identity.request_scope, heid)
    stored = stack.frames()
    activated = await phases.drive(
        transfer.transfer_id,
        FactsContext(
            operation=stack.operation,
            harness_execution_id=str(heid),
            generation=1,
            turn_no=1,
            lane_state=state,
            frames=tuple(stored),
        ),
        request_scope=SCOPE,
    )
    assert activated.activated, activated
    target = activated.target_session_ref
    assert target is not None and target != SOURCE
    assert len(stack.launcher.launches) == 2, "a fresh app-server for the target"
    assert _methods(stack, 1) == ["initialize", "initialized", "thread/start"], (
        "a fresh thread: no thread/resume, no thread/fork, nothing sent yet"
    )
    assert not any(m in {"thread/resume", "thread/fork"} for m in _methods(stack, 0))
    snapshot = await snapshots.load(
        request_scope=SCOPE, snapshot_ref=activated.workspace_snapshot_ref or ""
    )
    assert snapshot is not None
    assert "/outputs/draft.md" in snapshot.manifest and f"/{PATCH_PATH}" in snapshot.manifest
    assert not any(path.startswith("/.mission/state") for path in snapshot.manifest)
    patch = await staging.retrieve(snapshot.durable_refs[f"/{PATCH_PATH}"])
    assert b"codex-home" not in patch and b".mission/state" not in patch
    assert (root / ".mission" / "state" / "codex" / "codex-home").is_dir(), "stays in the lease"

    # 3. The next lane.turn adopts the target and sends exactly one continuation turn.
    result = await service.turn(
        stack.turn(
            segment=BOUNDS, turn_no=2, instruction_ref=f"continuation:{transfer.transfer_id}"
        ),
        RecordingSignals(stack.lanes.frames),
    )
    assert result.done and result.native.session_ref == target
    assert OperationExecutionResult.model_validate(result.operation_result).status == "completed"
    assert _turn_texts(stack, 0) and len(_turn_texts(stack, 0)) == 1, "the source ran one turn"
    (sent,) = _turn_texts(stack, 1)
    assert "purpose: continuation" in sent
    assert stack.launcher.launches[0].client.closed, "the superseded source app-server ended"
    state = await stack.lanes.states.load(identity.request_scope, heid)
    assert state is not None and state.native_session_ref == target
    record = state.dispatch("send", f"{heid}:1:turn:2")
    assert record is not None and record.phase == "acknowledged"
    assert not any(m in {"thread/resume", "thread/fork"} for m in _methods(stack, 1))


# --- the in-segment handover: hydrated while the source turn runs ------------------------------


async def test_a_continuation_hydrated_mid_turn_is_handed_over_at_the_turn_boundary(
    tmp_path: Path,
) -> None:
    stack = _stack(tmp_path, "turn_hold")
    identity = stack.identity
    running = asyncio.create_task(
        stack.lanes.service.turn(stack.turn(segment=BOUNDS), RecordingSignals(stack.lanes.frames))
    )
    await asyncio.wait_for(stack.launcher.launched.wait(), 10)
    source_server = stack.launcher.launches[0].server
    await asyncio.wait_for(source_server.held.wait(), 10)
    root = Path(stack.harness.lease_path(str(identity.harness_execution_id)))
    (root / "outputs").mkdir(exist_ok=True)
    (root / "outputs" / "draft.md").write_text("draft\n", encoding="utf-8")

    staging = FakeStaging()
    hydrator = CodexSessionHydrator(stack.harness, staging)
    wired = build_service(
        snapshots=CodexWorkspaceSnapshots(stack.harness, staging), staging=staging
    )
    continuation = wired["service"]
    transfer = await continuation.request(
        trigger(),
        request_scope=CONT_SCOPE,
        run_key="run-continuation-1",
        activation_key="unit-collect-1",
        logical_execution_id="logical-collect-1",
        lane_profile="codex",
        source_session_ref=SOURCE,
    )
    base = facts()
    sealed = await continuation.seal(
        transfer.transfer_id,
        facts(
            lane_profile="codex",
            identities=CheckpointIdentities(
                **{**base.identities.model_dump(), "source_agent_session_ref": SOURCE}
            ),
        ),
        seal_target(),
        request_scope=CONT_SCOPE,
    )
    assert sealed.checkpoint is not None, sealed.transfer
    outcome = await continuation.transfer(transfer.transfer_id, hydrator, request_scope=CONT_SCOPE)
    assert outcome.transfer.status == TransferStatus.TRANSFERRED, outcome.transfer
    receipt: Any = outcome.receipt
    assert receipt is not None and receipt.native_identity["conversation"] == "fresh"
    target = receipt.target_session_ref
    assert target != SOURCE and hydrator.hydrated == [target]
    assert receipt.native_identity["source_thread_id"] == SOURCE
    assert "/.mission/context.md" in receipt.restored and "/outputs/draft.md" in receipt.restored
    assert f"/{PATCH_PATH}" in receipt.restored
    assert _methods(stack, 1) == ["initialize", "initialized", "thread/start"]
    pending = await stack.harness.pending_handover(str(identity.harness_execution_id))
    assert pending is not None and pending.session.native_session_ref == target
    # Idempotent per transfer: a repeated hydration returns the same target.
    repeat = await stack.harness.hydrate_session(
        str(identity.harness_execution_id),
        transfer_id=transfer.transfer_id,
        prompt_text="ignored",
        source_session_ref=SOURCE,
    )
    assert repeat == pending and len(stack.launcher.launches) == 2, "never a second target"

    source_server.release.set()
    result = await asyncio.wait_for(running, timeout=30)
    assert result.done
    assert OperationExecutionResult.model_validate(result.operation_result).status == "completed"
    assert result.native.session_ref == target
    (sent,) = _turn_texts(stack, 1)
    assert "purpose: continuation" in sent
    assert stack.launcher.launches[0].client.closed, "the source app-server was terminated"
    assert await stack.harness.pending_handover(str(identity.harness_execution_id)) is None
    state = await stack.lanes.states.load(identity.request_scope, identity.harness_execution_id)
    assert state is not None and state.native_session_ref == target
    inits = [
        frame
        for frame in stack.frames()
        if frame.kind is FrameKind.SESSION_INIT and frame.native_session_ref == target
    ]
    assert inits, "the target's session_init frame confirms the handover"


async def test_hydration_refuses_a_thread_without_a_live_lease(tmp_path: Path) -> None:
    from mission_control.application.execution.harness.protocol import NativeTurnLost

    stack = codex_stack(tmp_path, approvals=False)
    stack.harness.stage(stack.heid, stack.operation)
    with pytest.raises(NativeTurnLost):
        await stack.harness.hydrate_session(
            stack.heid, transfer_id="t-1", prompt_text="x", source_session_ref=SOURCE
        )
    assert stack.launcher.launches == []
