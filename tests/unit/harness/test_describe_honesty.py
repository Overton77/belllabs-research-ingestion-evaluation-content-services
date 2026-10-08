"""FT-G6: describe honesty — every cell `describe()` reports, exercised against fixtures.

For both Cursor profiles, each `controls` cell and each `delivery_semantics` cell of
`mc.lane_describe.v1` is driven through the real lane (replaying bridge or fake Cloud Agents
API, a real Run with its command mailbox, `lane.turn`) and the observed behavior is held to
the declared value: `native` and `emulated` cells do what SPEC-07 section 7 says, every
`unsupported` cell is refused with a typed rejection and never silently degraded, and the
Delivery Report names the semantics the lane actually used. The hooks, identity, usage,
instruction-channel, subagent and placement cells are checked the same way. `qualified` stays
false: only a recorded live drill (docs/qualification/lanes) may flip it.

No network, no credentials, no paid call.
"""

from __future__ import annotations

import asyncio
import json
import tempfile
from pathlib import Path
from typing import Any

import pytest

from mission_control.adapters.cursor.workspace import git
from mission_control.application.execution.harness.controls import pause_decision
from mission_control.application.execution.harness.describe import DECLARED_LANE_MATRICES
from mission_control.application.execution.harness.protocol import (
    HARNESS_PROTOCOL_METHODS,
    implements,
)
from mission_control.contracts.contracts import MissionCommandRequest
from mission_control.domain.capabilities.hooks import native_hook
from mission_control.domain.context.render import INPUTS_MANIFEST_PATH, bytes_digest
from mission_control.domain.execution.contracts import (
    OperationAttemptIdentity,
    OperationExecutionResult,
    WorkspaceOwner,
    WorkspaceOwnerKind,
    WorkspaceSlotBinding,
)
from mission_control.domain.execution.lane_turns import (
    LANE_COMMAND_SEMANTICS,
    LANE_PAUSE_SEMANTICS,
    LANE_RESUME_SEMANTICS,
    LaneCancelRequest,
)
from mission_control.domain.execution.lanes import SessionHandle, SnapshotRequest
from mission_control.domain.frames.contracts import FrameKind
from mission_control.domain.policies.mailbox import MailboxState
from tests.fixtures.cursor_cloud import cloud_stack, load_sse
from tests.fixtures.cursor_controls import (
    ControlStack,
    StaticInputs,
    cloud_control_stack,
    control_stack,
    harness_fields,
    seal_and_transfer,
    source_agent,
    started_session,
    states,
)
from tests.fixtures.cursor_local import local_stack
from tests.fixtures.lane_turns import RecordingSignals

PROFILES = ("cursor_local", "cursor_cloud")
INJECTED = "Redirect: build on release/2.3."


# --- scenario plumbing: one Run, either lane ------------------------------------------------------


async def _stack(
    tmp_path: Path,
    profile: str,
    *,
    hold: str | None = None,
    inject: bool = False,
    later: int = 0,
) -> ControlStack:
    """A started Run on `profile`; `hold` keeps the first turn open at a mid-run point."""

    if profile == "cursor_local":
        changes: dict[str, Any] = {}
        if hold is not None:
            changes["hold_at"] = int(hold)
        fixture = "inject_run" if (inject or later) else "full_run"
        return await control_stack(tmp_path, fixture, launcher_changes=changes)
    api: dict[str, Any] = {}
    if hold is not None:
        api["hold_after"] = hold
    if inject:
        api["after_cancel"] = load_sse("interrupted_after_cancel")
    if later:
        api["later"] = [load_sse("replacement_stream") for _ in range(later)]
    return await cloud_control_stack(tmp_path, api_changes=api)


def _held_event(stack: ControlStack) -> asyncio.Event:
    if stack.profile == "cursor_cloud":
        return stack.local.api.held
    return stack.local.launcher.held


def _release(stack: ControlStack) -> None:
    if stack.profile == "cursor_cloud":
        stack.local.api.release.set()
    else:
        stack.local.launcher.release.set()


def _sends(stack: ControlStack) -> list[tuple[str, str]]:
    """(agent, text) of every turn the provider received, in order."""

    if stack.profile == "cursor_cloud":
        api = stack.local.api
        return [
            (api.runs[run]["agentId"], api.run_prompts.get(run, "")) for run in api.created_runs
        ]
    launcher = stack.local.launcher
    pairs = zip(launcher.sent_to, launcher.sends, strict=True)
    return [(agent, text) for agent, (_key, text) in pairs]


def _frames(stack: ControlStack) -> list[Any]:
    execution = stack.lanes.frames._executions[stack.identity.harness_execution_id]
    return sorted(execution.frames.values(), key=lambda frame: frame.arrival_ordinal)


async def _held(stack: ControlStack, *, cancel: bool = False) -> asyncio.Task[Any]:
    signals = RecordingSignals(
        stack.lanes.frames, stack.identity.harness_execution_id, cancel=cancel
    )
    task = asyncio.create_task(stack.service.turn(stack.turn(), signals))
    await asyncio.wait_for(_held_event(stack).wait(), timeout=20)
    return task


def _settled(result: Any) -> OperationExecutionResult:
    return OperationExecutionResult.model_validate(result.operation_result)


HOLD = {
    "cursor_local": {"turn_ended": "10", "tool_running": "6"},
    "cursor_cloud": {"turn_ended": "7", "tool_running": "5"},
}


# --- the declared matrices ------------------------------------------------------------------


@pytest.mark.parametrize("profile", PROFILES)
def test_the_declared_matrix_is_section_7_and_never_qualified_by_fixtures(profile: str) -> None:
    describe = DECLARED_LANE_MATRICES[profile]  # type: ignore[index]
    assert describe.delivery_semantics == {
        "queue_instruction": "wait_then_send",
        "interrupt_and_inject": "cancel_and_replace",
        "pause": "unsupported",
        "hard_pause": "unsupported",
        "resume": "wait_then_send",
        "cancel": "turn_boundary_guaranteed",
        "fork": "emulated",
        "request_continuation": "emulated",
    }
    assert describe.controls["pause"] == "unsupported"
    assert describe.controls["snapshot"] == "emulated" and describe.controls["fork"] == "emulated"
    assert describe.controls["reattach"] == ("emulated" if profile == "cursor_local" else "native")
    # The workflow's constants (it cannot read the registry) say the same.
    assert LANE_PAUSE_SEMANTICS[profile] == describe.delivery_semantics["pause"]
    assert LANE_RESUME_SEMANTICS[profile] == describe.delivery_semantics["resume"]
    assert LANE_COMMAND_SEMANTICS["cancel"] == describe.delivery_semantics["cancel"]
    assert (
        LANE_COMMAND_SEMANTICS["interrupt_and_inject"]
        == (describe.delivery_semantics["interrupt_and_inject"])
    )
    assert describe.qualified is False


@pytest.mark.parametrize("profile", PROFILES)
async def test_every_native_or_emulated_operation_is_implemented(
    tmp_path: Path, profile: str
) -> None:
    stack = await _stack(tmp_path, profile)
    harness = stack.local.harness
    describe = harness.describe()
    for operation in HARNESS_PROTOCOL_METHODS:
        if operation == "describe":
            continue
        assert describe.control(operation) in {"native", "emulated"}, operation
        assert implements(harness, operation), operation
    # `pause` is not an operation any Cursor harness pretends to have.
    assert not hasattr(harness, "pause")
    assert callable(harness.stage_turn) and callable(harness.pending_handover)


# --- controls -------------------------------------------------------------------------------


@pytest.mark.parametrize("profile", PROFILES)
async def test_prepare_start_send_observe_usage_end_session_run_a_full_turn(
    tmp_path: Path, profile: str
) -> None:
    stack = await _stack(tmp_path, profile)
    result = await stack.service.turn(stack.turn(), RecordingSignals(stack.lanes.frames))
    assert _settled(result).status == "completed"
    state = await stack.lanes.states.load(
        stack.operation.request_scope, stack.identity.harness_execution_id
    )
    assert state is not None and state.native_session_ref and state.native_turn_ref
    frames = _frames(stack)
    assert any(frame.kind == FrameKind.RUN_RESULT for frame in frames)
    facts = result.closing_facts
    # usage: the turn's tokens are counted at settlement (never unknown, never zero); cost
    # stays estimated until the provider reports it.
    assert facts is not None and facts.usage.total_tokens > 0
    assert facts.usage.disposition in {"settled", "estimated"}
    assert facts.cost_disposition == "estimated"
    assert facts.patch_ref is not None, "end_session stored the patch"
    if profile == "cursor_cloud":
        assert stack.local.api.archived, "end_session archived the agent after its artifacts"
    else:
        (lease,) = stack.local.leases._leases.values()
        assert lease.released and lease.patch_artifact_ref == facts.patch_ref


@pytest.mark.parametrize("profile", PROFILES)
async def test_cost_settles_only_when_the_provider_reports_it(tmp_path: Path, profile: str) -> None:
    if profile == "cursor_local":
        stack = local_stack(tmp_path, "error_run")  # get_usage reports cost 2100 micros
        from tests.fixtures.lane_turns import lane_stack

        lanes = lane_stack(stack.harness, frames=stack.frames, operation=stack.operation)
        result = await lanes.service.turn(
            _single_turn(stack.operation, profile), RecordingSignals(stack.frames)
        )
    else:
        cloud = cloud_stack(tmp_path, cost=4_200)
        from tests.fixtures.lane_turns import lane_stack

        lanes = lane_stack(cloud.harness, operation=cloud.operation)
        result = await lanes.service.turn(
            _single_turn(cloud.operation, profile), RecordingSignals(lanes.frames)
        )
    assert result.closing_facts is not None
    assert result.closing_facts.cost_disposition == "settled"


def _single_turn(operation: Any, profile: str) -> Any:
    from mission_control.domain.execution.lane_turns import LaneTurnRequest

    return LaneTurnRequest.model_validate(
        {"operation": operation, "lane_profile": profile, "generation": 1}
    )


@pytest.mark.parametrize("profile", PROFILES)
async def test_reattach_resumes_by_native_identity_without_a_second_send(
    tmp_path: Path, profile: str
) -> None:
    stack = await _stack(tmp_path, profile)
    first = await stack.service.turn(
        stack.turn(segment=stack.turn().segment.model_copy(update={"max_frames": 3})),
        RecordingSignals(stack.lanes.frames),
    )
    assert not first.done and first.cursor is not None
    second = await stack.service.turn(
        stack.turn(phase="resume", cursor=first.cursor, segment_no=2),
        RecordingSignals(stack.lanes.frames),
    )
    assert second.done and _settled(second).status == "completed"
    assert len(_sends(stack)) == 1, "a resumed segment never sends again"
    frames = _frames(stack)
    assert len({frame.provider_key for frame in frames}) == len(frames)


@pytest.mark.parametrize("profile", PROFILES)
async def test_cancel_turn_is_native_and_settles_cancelled_by_command(
    tmp_path: Path, profile: str
) -> None:
    stack = await _stack(tmp_path, profile, hold=HOLD[profile]["turn_ended"])
    task = await _held(stack, cancel=True)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    request = LaneCancelRequest.model_validate(
        {"operation": stack.operation, "lane_profile": profile, "generation": 1}
    )
    outcome = await stack.service.cancel(request)
    settled = OperationExecutionResult.model_validate(outcome.operation_result)
    assert (settled.status, settled.failure_code) == ("cancelled", "cancelled")
    assert settled.usage.amounts.get("tokens.total", 0) > 0, "usage from the last turn end"
    again = await stack.service.cancel(request)
    assert again.settled and again.operation_result == outcome.operation_result


@pytest.mark.parametrize("profile", PROFILES)
async def test_snapshot_is_emulated(tmp_path: Path, profile: str) -> None:
    stack = await _stack(tmp_path, profile, hold=HOLD[profile]["turn_ended"])
    task = await _held(stack)
    heid = str(stack.identity.harness_execution_id)
    manifest = await stack.local.harness.snapshot(
        SnapshotRequest(
            **harness_fields(stack.operation, heid),
            session=_session(stack),
            reason="snapshot",
        )
    )
    _release(stack)
    await asyncio.wait_for(task, timeout=30)
    assert manifest.emulated
    if profile == "cursor_local":
        assert manifest.kind == "git_patch" and manifest.refs[0].startswith("cursor-snapshot:")
    else:
        assert manifest.kind == "branch" and manifest.refs[0].startswith("branch:")


def _session(stack: ControlStack) -> SessionHandle:
    return SessionHandle(
        lane_profile=stack.profile,  # type: ignore[arg-type]
        harness_execution_id=str(stack.identity.harness_execution_id),
        generation=1,
        native_session_ref=source_agent(stack),
    )


@pytest.mark.parametrize("profile", PROFILES)
async def test_pause_is_refused_mid_run_and_never_a_harness_operation(
    tmp_path: Path, profile: str
) -> None:
    describe = DECLARED_LANE_MATRICES[profile]  # type: ignore[index]
    refused = pause_decision(describe, turn_in_flight=True)
    assert not refused.accepted and refused.reason_code == "unsupported_control"
    assert refused.delivery_semantics == describe.delivery_semantics["pause"] == "unsupported"
    boundary = pause_decision(describe, turn_in_flight=False)
    assert boundary.accepted and boundary.delivery_semantics == "turn_boundary_guaranteed"
    # The workflow's pause_command Update applies exactly this (Temporal proof:
    # tests/integration/temporal/test_lane_turn.py -k pause).


def test_hard_pause_is_not_a_command_any_surface_accepts() -> None:
    kinds = MissionCommandRequest.model_json_schema()
    assert "hard_pause" not in json.dumps(kinds)


async def _fork_cloud(tmp_path: Path) -> tuple[Path, str, str]:
    source = await _stack(
        tmp_path / "source", "cursor_cloud", hold=HOLD["cursor_cloud"]["turn_ended"]
    )
    task = await _held(source)
    remote = source.local.remote
    _, branch = source.local.harness.session_branch(str(source.identity.harness_execution_id))
    with tempfile.TemporaryDirectory(prefix="agent-push-") as scratch:
        work = Path(scratch) / "clone"
        git("clone", "--quiet", "--branch", branch, str(remote), str(work), cwd=Path(scratch))
        (work / "src").mkdir(exist_ok=True)
        (work / "src" / "pushed_by_agent.py").write_bytes(b"PUSHED = True\n")
        git("add", "--all", cwd=work)
        git(
            "-c",
            "user.name=Agent",
            "-c",
            "user.email=agent@localhost",
            "commit",
            "--quiet",
            "-m",
            "mid-run work",
            cwd=work,
        )
        git("push", "--quiet", "origin", branch, cwd=work)
    heid = str(source.identity.harness_execution_id)
    manifest = await source.local.harness.snapshot(
        SnapshotRequest(
            **harness_fields(source.operation, heid), session=_session(source), reason="fork"
        )
    )
    _release(source)
    await asyncio.wait_for(task, timeout=30)
    inputs_manifest = json.dumps(
        {
            "schema_version": "mc.mission_inputs.v1",
            "packet_digest": "sha256:" + "d" * 64,
            "inputs": [],
            "workspace": {"snapshot_ref": manifest.refs[0], "restore_paths": ["/"]},
        }
    ).encode("utf-8")
    durable = "file-artifact://fork-inputs"
    owner = WorkspaceOwner(kind=WorkspaceOwnerKind.STAGE, owner_id="implement")
    workspace = {
        **source.operation.workspace.model_dump(mode="python"),
        "workflow_contract_digest": "sha256:" + "e" * 64,
        "slot_bindings": (
            WorkspaceSlotBinding(
                slot_name="ctx-mission-inputs",
                logical_path=f"/{INPUTS_MANIFEST_PATH}",
                access="read_only",
                owner=owner,
                durable_ref=durable,
                content_digest=bytes_digest(inputs_manifest),
            ),
            WorkspaceSlotBinding(
                slot_name="output",
                logical_path="/workspace/output",
                access="exclusive_write",
                owner=owner,
            ),
        ),
    }
    fork = cloud_stack(
        tmp_path / "fork",
        remote=remote,
        operation_changes={
            "workspace": workspace,
            "identity": OperationAttemptIdentity(
                run_id="run-forked", operation_id="op-forked", operation_attempt=1
            ),
        },
        inputs=StaticInputs({durable: inputs_manifest}),
    )
    fork_heid, session = await started_session(fork)
    _, fork_branch = fork.harness.session_branch(fork_heid)
    assert session.native_session_ref is not None
    return remote, fork_branch, session.native_session_ref


@pytest.mark.parametrize("profile", PROFILES)
async def test_fork_is_emulated_from_the_snapshot_with_a_new_agent(
    tmp_path: Path, profile: str
) -> None:
    if profile == "cursor_cloud":
        remote, branch, agent = await _fork_cloud(tmp_path)
        content = git("show", f"{branch}:src/pushed_by_agent.py", cwd=remote)
        assert content == "PUSHED = True\n", "the derived branch starts at the snapshot head"
        assert json.loads(git("show", f"{branch}:{INPUTS_MANIFEST_PATH}", cwd=remote))["workspace"][
            "snapshot_ref"
        ].startswith("branch:")
        assert agent.startswith("bc-")
        return
    # cursor_local: the FT-G4 proof (fresh lease restored from the patch, a new agent).
    from tests.unit.harness.test_cursor_controls import (
        test_fork_restores_the_snapshot_into_a_fresh_lease_with_a_new_agent as local_fork,
    )

    await local_fork(tmp_path)


# --- delivery semantics -----------------------------------------------------------------------


@pytest.mark.parametrize("profile", PROFILES)
async def test_queue_instruction_is_wait_then_send(tmp_path: Path, profile: str) -> None:
    stack = await _stack(tmp_path, profile, hold=HOLD[profile]["tool_running"])
    first = await stack.command("queue_instruction", "Also update the README flag table.")
    await stack.deliver()
    task = await _held(stack)
    status = await stack.status(first)
    report = status.receipts[-1].delivery_report
    assert states(status)[-1] == "observed" and report is not None
    assert report.requested_semantics == report.delivered_semantics == "wait_then_send"
    second = await stack.command("queue_instruction", "Then run the linters.")
    _release(stack)
    result = await asyncio.wait_for(task, timeout=30)
    assert _settled(result).status == "completed"
    assert states(await stack.status(first))[-1] == "applied"
    entries = {entry.command_id: entry for entry in await stack.entries()}
    assert entries[str(second.request_id)].state == MailboxState.QUEUED
    assert len(_sends(stack)) == 1


@pytest.mark.parametrize("profile", PROFILES)
async def test_interrupt_and_inject_is_cancel_and_replace(tmp_path: Path, profile: str) -> None:
    stack = await _stack(
        tmp_path, profile, hold=HOLD[profile]["tool_running"], inject=True, later=1
    )
    task = await _held(stack)
    receipt = await stack.command("interrupt_and_inject", INJECTED)
    result = await asyncio.wait_for(task, timeout=30)
    assert _settled(result).status == "completed"
    sends = _sends(stack)
    assert len(sends) == 2 and sends[0][0] == sends[1][0], "a replacement run of the same agent"
    assert INJECTED in sends[1][1]
    status = await stack.status(receipt)
    assert states(status)[-1] == "applied"
    report = status.receipts[3].delivery_report
    assert report is not None and report.delivered_semantics == "cancel_and_replace"
    assert report.native_refs.cancelled_turn_ref != report.native_refs.replacement_turn_ref
    frames = _frames(stack)
    assert len({frame.provider_key for frame in frames}) == len(frames)
    turns = {frame.native_turn_ref for frame in frames if frame.native_turn_ref}
    assert {report.native_refs.cancelled_turn_ref, report.native_refs.replacement_turn_ref} <= turns


@pytest.mark.parametrize("profile", PROFILES)
async def test_request_continuation_is_emulated_by_a_hydrated_agent(
    tmp_path: Path, profile: str
) -> None:
    stack = await _stack(tmp_path, profile, hold=HOLD[profile]["tool_running"], later=1)
    task = await _held(stack)
    wired, outcome = await seal_and_transfer(stack)
    assert outcome.receipt is not None
    new_agent = outcome.receipt.target_session_ref
    assert new_agent != source_agent(stack)
    assert [item.event for item in wired["events"].actions][-1] == "transferred"
    _release(stack)
    result = await asyncio.wait_for(task, timeout=30)
    assert _settled(result).status == "completed"
    sends = _sends(stack)
    assert [agent for agent, _text in sends] == [source_agent(stack), new_agent]
    assert "- purpose: continuation" in sends[1][1]
    inits = [
        frame
        for frame in _frames(stack)
        if frame.kind == FrameKind.SESSION_INIT and frame.native_session_ref == new_agent
    ]
    assert inits, "the target session's session_init frame confirms the transfer"


# --- hooks, identity, instruction channel, subagents, placement ----------------------------------


@pytest.mark.parametrize("profile", PROFILES)
def test_declared_hook_events_map_to_native_cursor_hooks(profile: str) -> None:
    describe = DECLARED_LANE_MATRICES[profile]  # type: ignore[index]
    for event in describe.hooks.events_supported:
        assert native_hook(profile, event) is not None, f"{profile} declares {event}, no hook"


@pytest.mark.parametrize("profile", PROFILES)
async def test_hooks_fail_closed_only_where_kernel_hooks_run(tmp_path: Path, profile: str) -> None:
    describe = DECLARED_LANE_MATRICES[profile]  # type: ignore[index]
    if profile == "cursor_local":
        stack = local_stack(tmp_path)
        projection = await stack.harness._projections.project(
            stack.operation, profile=profile, packet_index=None
        )
    else:
        stack = cloud_stack(tmp_path)
        projection = await stack.harness._projections.project(
            stack.operation, profile=profile, packet_index=None
        )
    hooks = json.loads(projection.file(".cursor/hooks.json").content)["hooks"]
    kernel = [
        entry
        for entries in hooks.values()
        for entry in entries
        if "kernel.py" in str(entry.get("command", ""))
    ]
    fail_closed = any(entry.get("failClosed") for entry in kernel)
    assert fail_closed is describe.hooks.fail_closed
    if profile == "cursor_cloud":
        assert kernel == [], "the cloud VM cannot reach the loopback callback"


@pytest.mark.parametrize("profile", PROFILES)
async def test_identity_and_cursor_are_per_run_and_resumable(tmp_path: Path, profile: str) -> None:
    stack = await _stack(tmp_path, profile)
    result = await stack.service.turn(stack.turn(), RecordingSignals(stack.lanes.frames))
    run = result.native.turn_ref
    assert run is not None
    keyed = [frame for frame in _frames(stack) if frame.native_turn_ref == run]
    cursors = {stack.local.harness.resume_cursor(frame.provider_key) for frame in keyed}
    cursors.discard(None)
    assert cursors and all(cursor.startswith(f"{run}@") for cursor in cursors)
    describe = stack.local.harness.describe()
    expected = "bridge_offset" if profile == "cursor_local" else "sse_event_id"
    assert describe.identity.cursor == expected
    assert describe.identity.session_ref == "agent_id" and describe.identity.turn_ref == "run_id"


@pytest.mark.parametrize("profile", PROFILES)
async def test_instruction_channel_subagents_and_placement(tmp_path: Path, profile: str) -> None:
    describe = DECLARED_LANE_MATRICES[profile]  # type: ignore[index]
    if profile == "cursor_local":
        stack = local_stack(tmp_path)
        from tests.fixtures.cursor_controls import register_run

        register_run(stack)
        heid, _session = await started_session(stack)
        root = Path(stack.harness.lease_path(heid))
        present = {
            path
            for path in ("AGENTS.md", ".cursor/rules/mc-mission.mdc")
            if (root / path).is_file()
        }
        assert any((root / ".cursor/agents").glob("*.md"))
        assert str(root).startswith(str(stack.lease_root)), "worker_hosted lease"
        assert "prompt_prefix" in describe.instruction_channel
        assert stack.launcher.created[0].setting_sources == ("project",)
    else:
        cloud = cloud_stack(tmp_path)
        heid, _session = await started_session(cloud)
        _repository, branch = cloud.harness.session_branch(heid)
        listing = git("ls-tree", "-r", "--name-only", branch, cwd=cloud.remote).splitlines()
        present = {
            path for path in ("AGENTS.md", ".cursor/rules/mc-mission.mdc") if path in listing
        }
        assert any(path.startswith(".cursor/agents/") for path in listing)
        assert "prompt_prefix" not in describe.instruction_channel
    assert (
        present
        == {"AGENTS.md", ".cursor/rules/mc-mission.mdc"}
        <= set(describe.instruction_channel)
    )
    assert describe.subagents.file == ".cursor/agents/*.md"
    assert describe.subagents.readonly_supported_inline is False
    assert describe.placement == ("worker_hosted" if profile == "cursor_local" else "cloud")


# --- qualification gate ---------------------------------------------------------------------


QUALIFICATION_DIR = Path(__file__).resolve().parents[3] / "docs" / "qualification" / "lanes"


def _records(profile: str, root: Path = QUALIFICATION_DIR) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for path in sorted(root.glob(f"{profile}-*.md")):
        text = path.read_text("utf-8")
        if not text.startswith("---"):
            continue
        header = text.split("---", 2)[1]
        values = dict(line.split(":", 1) for line in header.strip().splitlines() if ":" in line)
        records.append({key.strip(): value.strip().strip('"') for key, value in values.items()})
    return records


@pytest.mark.parametrize("profile", PROFILES)
def test_qualified_flips_only_with_a_recorded_live_drill(profile: str) -> None:
    describe = DECLARED_LANE_MATRICES[profile]  # type: ignore[index]
    recorded = [
        record
        for record in _records(profile)
        if record.get("outcome") == "qualified" and record.get("evidence") == "live_drill"
    ]
    if describe.qualified:
        assert recorded, f"{profile} is qualified without a recorded live drill"
    else:
        assert describe.qualified is False


def test_the_live_drill_record_is_what_the_gate_reads(tmp_path: Path) -> None:
    from tests.integration.cursor.live_drill import UNVERIFIED, DrillOutcome, Spend, write_record

    passed = DrillOutcome("cursor_local", Spend(budget_usd=2.0))
    passed.checks["full_turn_settles_from_closing_facts"] = True
    passed.settle("run_request_id", "refuted", "cursor-sdk 1.0.37 source")
    write_record(passed, root=tmp_path)
    (record,) = _records("cursor_local", tmp_path)
    assert record["outcome"] == "qualified" and record["evidence"] == "live_drill"
    text = next(tmp_path.glob("cursor_local-*.md")).read_text("utf-8")
    assert all(f"| {item} |" in text for item in UNVERIFIED)
    ambiguous = DrillOutcome("cursor_cloud", Spend(budget_usd=2.0, unknown=["agent?"]))
    ambiguous.checks["full_turn_settles_from_closing_facts"] = True
    write_record(ambiguous, root=tmp_path)
    (cloud,) = _records("cursor_cloud", tmp_path)
    assert cloud["outcome"] == "not_qualified", "an ambiguous paid effect never qualifies"
    with pytest.raises(ValueError):
        passed.settle("not-an-item", "verified", "")


def test_the_live_drill_refuses_without_an_approved_finite_budget(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import importlib.util

    root = Path(__file__).resolve().parents[3]
    spec = importlib.util.spec_from_file_location("lane_qualify", root / "scripts/lane_qualify.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    for name in ("CURSOR_API_KEY", "MC_PAID_BUDGET_USD", "MC_CURSOR_CLOUD_REPO"):
        monkeypatch.delenv(name, raising=False)
    assert len(module._live_preconditions("cursor_cloud")) == 3
    monkeypatch.setenv("CURSOR_API_KEY", "not-a-real-key")
    monkeypatch.setenv("MC_PAID_BUDGET_USD", "inf")
    assert module._live_preconditions("cursor_local") == [
        "MC_PAID_BUDGET_USD (a finite, owner-approved amount)"
    ]
    monkeypatch.setenv("MC_PAID_BUDGET_USD", "1.5")
    assert module._live_preconditions("cursor_local") == []
    from tests.integration.cursor.live_drill import live_enabled

    monkeypatch.delenv("MC_LIVE_CURSOR_QUALIFICATION", raising=False)
    assert not live_enabled(), "never without the explicit opt-in flag"
