"""FT-G3: the `cursor_local` Lane Profile over a replaying bridge and a real git repository.

Prepare (worktree lease at the base ref, Host Projection with Kernel Hooks first, packet,
digests, CAPABILITY_DRIFT, UNSUPPORTED_BEHAVIOR), the turn through `lane.turn` (native identity
first, frames keyed `bridge:<run>:<offset>`, no duplicate on resume, kernel hooks through the
callback), closing facts and usage dispositions, end_session (patch stored before the lease is
released) and describe honesty. No Cursor agent is created.
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any

import pytest

from mission_control.adapters.cursor.bridge import envelope_from_event
from mission_control.adapters.cursor.frames import (
    closing_facts,
    local_provider_key,
    map_envelope,
    offset_of,
    scrub,
)
from mission_control.adapters.cursor.kernel_hook_script import native_output
from mission_control.adapters.cursor.projection import (
    CAPABILITY_DRIFT,
    UNSUPPORTED_BEHAVIOR,
    LaneProjectionError,
    projection_digests,
)
from mission_control.adapters.cursor.workspace import git
from mission_control.application.execution.harness.describe import CURSOR_LOCAL_DESCRIBE
from mission_control.application.execution.harness.lane_turns import (
    LaneExecutionIdentity,
    harness_scope,
)
from mission_control.application.execution.harness.protocol import (
    HARNESS_PROTOCOL_METHODS,
    SessionLane,
    implements,
)
from mission_control.application.frames.reducer import DeriveContext, derive
from mission_control.domain.execution.contracts import OperationExecutionResult
from mission_control.domain.execution.lane_turns import LaneSegmentBounds, LaneTurnRequest
from mission_control.domain.execution.lanes import PrepareRequest
from mission_control.domain.frames.contracts import FrameKind, LaneProfile
from mission_control.domain.frames.facts import CompactionObservedFact, ToolEffectFact
from tests.fixtures.cursor_local import LocalStack, local_stack
from tests.fixtures.lane_turns import RecordingSignals, SimulatedWorkerLoss, lane_stack


def _identity(stack: LocalStack) -> LaneExecutionIdentity:
    return LaneExecutionIdentity.of(stack.operation, "cursor_local", 1)


def _prepare_request(stack: LocalStack) -> PrepareRequest:
    identity = _identity(stack)
    operation = stack.operation
    assert operation.cursor_binding is not None
    return PrepareRequest(
        scope=harness_scope(identity.request_scope),
        lane_profile="cursor_local",
        harness_execution_id=str(identity.harness_execution_id),
        binding_digest=operation.cursor_binding.binding_digest,
        idempotency_key=f"{identity.harness_execution_id}:1:prepare",
        generation=1,
        run_id=operation.identity.run_id,
        operation_id=operation.identity.operation_id,
        attempt_no=1,
    )


def _turn(stack: LocalStack, **changes: Any) -> LaneTurnRequest:
    return LaneTurnRequest.model_validate(
        {
            "operation": stack.operation,
            "lane_profile": "cursor_local",
            "generation": 1,
            **changes,
        }
    )


def _service(stack: LocalStack) -> Any:
    return lane_stack(stack.harness, frames=stack.frames, operation=stack.operation)


async def _prepared(stack: LocalStack) -> Path:
    request = _prepare_request(stack)
    stack.harness.stage(request.harness_execution_id, stack.operation)
    prepared = await stack.harness.prepare(request)
    assert prepared.workspace_ref is not None
    (lease,) = stack.leases._leases.values()
    return Path(lease.path)


async def test_prepare_leases_a_worktree_and_places_projections_and_the_packet(
    tmp_path: Path,
) -> None:
    stack = local_stack(tmp_path)
    root = await _prepared(stack)
    repo = tmp_path / "repo"
    assert git("rev-parse", "HEAD", cwd=root).strip() == git("rev-parse", "main", cwd=repo).strip()
    rule = (root / ".cursor/rules/mc-mission.mdc").read_text("utf-8")
    assert "alwaysApply: true" in rule and "Mission context: read .mission/context.md" in rule
    hooks = json.loads((root / ".cursor/hooks.json").read_text("utf-8"))["hooks"]
    for native in ("preToolUse", "beforeShellExecution", "beforeMCPExecution", "subagentStart"):
        entries = hooks[native]
        kernel = [entry for entry in entries if ".mission/hooks/kernel.py" in entry["command"]]
        assert kernel and all(entry["failClosed"] is True for entry in kernel)
        # Kernel Hooks first, catalog Hook Scripts after.
        first_catalog = next(
            (index for index, entry in enumerate(entries) if "run.py" in entry["command"]),
            len(entries),
        )
        assert all(
            index < first_catalog
            for index, entry in enumerate(entries)
            if "kernel.py" in entry["command"]
        )
    assert any("run.py" in entry["command"] for entry in hooks["preToolUse"])
    assert (root / ".cursor/skills/agent-browser/SKILL.md").is_file()
    assert "readonly: true" in (root / ".cursor/agents/verifier.md").read_text("utf-8")
    mcp = (root / ".cursor/mcp.json").read_text("utf-8")
    assert "${env:" in mcp and "tvly-FIXTURE-secret" not in mcp
    assert (root / ".mission/hooks/kernel.py").is_file()
    assert (root / ".mission/operating-contract.md").is_file()
    assert (root / "outputs").is_dir()
    assert stack.operation.cursor_binding is not None
    assert {
        "rules_digest": stack.operation.cursor_binding.projections.rules_digest,
        "agents_digest": stack.operation.cursor_binding.projections.agents_digest,
        "hooks_digest": stack.operation.cursor_binding.projections.hooks_digest,
    } == dict(projection_digests_of(root))


def projection_digests_of(root: Path) -> dict[str, str]:
    from mission_control.domain.agentic_components.projection import (
        HostProjection,
        ProjectedFile,
        ProjectionReport,
    )
    from mission_control.domain.capabilities.host_support import LaneProfile as HostProfile

    files = tuple(
        ProjectedFile(path=path.relative_to(root).as_posix(), content=path.read_bytes())
        for path in sorted(root.rglob("*"))
        if path.is_file()
        and ".git" not in path.relative_to(root).parts
        and (
            path.relative_to(root).as_posix().startswith(".cursor/")
            or path.relative_to(root).as_posix() == "AGENTS.md"
        )
    )
    projection = HostProjection(
        profile=HostProfile.CURSOR_LOCAL, files=files, report=ProjectionReport()
    )
    return projection_digests(projection)


async def test_a_projection_differing_from_the_binding_is_capability_drift(tmp_path: Path) -> None:
    stack = local_stack(tmp_path, drift=True)
    with pytest.raises(LaneProjectionError) as refused:
        await _prepared(stack)
    assert refused.value.code == CAPABILITY_DRIFT
    assert stack.launcher.launches == [], "nothing launches after a drifted projection"


async def test_a_sandbox_the_host_cannot_run_is_unsupported_behavior(tmp_path: Path) -> None:
    stack = local_stack(
        tmp_path,
        binding_changes={"sandbox_enabled": True},
        launcher_changes={"sandbox": False},
    )
    with pytest.raises(LaneProjectionError) as refused:
        await _prepared(stack)
    assert refused.value.code == UNSUPPORTED_BEHAVIOR
    assert stack.leases._leases == {}


async def test_a_full_turn_runs_through_lane_turn_with_kernel_hooks(tmp_path: Path) -> None:
    stack = local_stack(tmp_path)
    lanes = _service(stack)
    heid = _identity(stack).harness_execution_id
    signals = RecordingSignals(stack.frames, heid)
    result = await lanes.service.turn(_turn(stack), signals)

    assert result.done and result.closing_facts is not None
    facts = result.closing_facts
    assert facts.native_status == "finished" and facts.model == "composer-2"
    assert facts.usage.total_tokens == 2242
    # Local `get_usage` is account-gated: feature_unavailable keeps the cost estimated.
    assert facts.cost_disposition == "estimated"
    settled = OperationExecutionResult.model_validate(result.operation_result)
    assert settled.status == "completed"
    # Native identity was recorded before observation; one send with the idempotency key.
    state = await lanes.states.load(_identity(stack).request_scope, heid)
    assert state is not None and state.native_turn_ref == stack.launcher.meta["run_id"]
    assert state.cursor_sdk_version == "1.0.37" and state.bridge_state_root
    assert [key for key, _text in stack.launcher.sends] == [f"{heid}:1:turn:1"]
    assert stack.launcher.created[0].setting_sources == ("project",)
    # Every Cursor hook allowed; intents written for the gated effects before `allow`.
    assert {expect for _event, expect, _r in stack.launcher.hook_results} == {"allow"}
    assert all(result.decision.value == "allow" for *_x, result in stack.launcher.hook_results)
    intents = {intent.effect_ref for intent in stack.intents.intents()}
    assert {"tool_use:call-read-1", "tool_use:call-shell-1"} <= intents
    assert any(ref.startswith("shell:") for ref in intents)
    # Provider frames keyed by run and offset; hook frames on the same execution.
    frames = list(stack.frames._executions[heid].frames.values())
    run_id = stack.launcher.meta["run_id"]
    assert local_provider_key(run_id, "11") in {frame.provider_key for frame in frames}
    assert any(frame.kind == FrameKind.HOOK_RESULT for frame in frames)
    assert all("sk-fixture-secret" not in frame.body_excerpt for frame in frames)
    # The patch (agent changes only) was stored before the lease was released.
    assert facts.patch_ref is not None
    name, diff, _media = stack.artifacts.staged[facts.patch_ref]
    assert name.endswith("patch.diff") and b"src/feature.py" in diff
    assert b"AGENTS.md" not in diff and b".cursor/" not in diff and b".mission" not in diff
    assert any(
        staged[0].endswith("outputs/report.md") for staged in stack.artifacts.staged.values()
    )
    (lease,) = stack.leases._leases.values()
    assert lease.released and lease.patch_artifact_ref == facts.patch_ref
    # FT-G4: the snapshot the session froze is recorded on the lease; a fork restores it.
    assert lease.snapshot_ref is not None and lease.snapshot_ref.startswith("cursor-snapshot:")
    assert lease.snapshot_ref in facts.output_refs
    assert await stack.leases.sandbox_snapshot_refs(lease.request_scope, lease.run_id) == (
        lease.snapshot_ref,
    )
    assert not await asyncio.to_thread(Path(lease.path).exists)
    # The token is revoked with the session.
    (token,) = stack.tokens._tokens.values()
    assert token.revoked_at is not None


async def test_error_run_fails_and_settles_cost_when_the_provider_reports_it(
    tmp_path: Path,
) -> None:
    stack = local_stack(tmp_path, "error_run")
    lanes = _service(stack)
    result = await lanes.service.turn(_turn(stack), RecordingSignals(stack.frames))
    assert result.closing_facts is not None
    assert result.closing_facts.native_status == "error"
    assert result.closing_facts.error_code == "internal"
    assert result.closing_facts.cost_disposition == "settled"
    settled = OperationExecutionResult.model_validate(result.operation_result)
    assert settled.status == "failed" and settled.failure_code == "provider_error"


async def test_a_denied_hook_blocks_the_effect_and_the_reducer_sees_the_denial(
    tmp_path: Path,
) -> None:
    stack = local_stack(tmp_path, "hook_deny")
    lanes = _service(stack)
    heid = _identity(stack).harness_execution_id
    await lanes.service.turn(_turn(stack), RecordingSignals(stack.frames, heid))
    decisions = {(event, r.decision.value) for event, _e, r in stack.launcher.hook_results}
    assert ("beforeShellExecution", "deny") in decisions and ("preToolUse", "deny") in decisions
    for _event, expected, result in stack.launcher.hook_results:
        assert result.decision.value == expected
    assert stack.intents.intents() == (), "no intent is written for a refused effect"
    closing = [f for f in stack.frames._executions[heid].frames.values() if f.closing]
    facts = derive(closing, DeriveContext(lane=LaneProfile.CURSOR_LOCAL, current_generation=1))
    denied = [
        fact
        for fact in facts
        if isinstance(fact, ToolEffectFact) and fact.tool_call_ref == "call-shell-9"
    ]
    assert denied and denied[0].status == "denied"


async def test_pre_compact_is_a_hook_frame_and_compaction_is_observed(tmp_path: Path) -> None:
    stack = local_stack(tmp_path, "precompact")
    lanes = _service(stack)
    heid = _identity(stack).harness_execution_id
    result = await lanes.service.turn(_turn(stack), RecordingSignals(stack.frames, heid))
    assert result.closing_facts is not None
    assert result.closing_facts.cost_disposition == "settled"
    frames = list(stack.frames._executions[heid].frames.values())
    assert any(
        frame.kind == FrameKind.HOOK_INVOKED and "before_compaction" in frame.body_excerpt
        for frame in frames
    )
    closing = [frame for frame in frames if frame.closing]
    facts = derive(closing, DeriveContext(lane=LaneProfile.CURSOR_LOCAL, current_generation=1))
    assert any(isinstance(fact, CompactionObservedFact) for fact in facts)


async def test_a_resume_from_a_stored_offset_stores_no_frame_twice(tmp_path: Path) -> None:
    stack = local_stack(tmp_path, launcher_changes={"fail_at": 7})
    lanes = _service(stack)
    heid = _identity(stack).harness_execution_id
    with pytest.raises(ConnectionError):
        await lanes.service.turn(_turn(stack), RecordingSignals(stack.frames, heid))
    before = len(stack.frames._executions[heid].frames)
    retry = await lanes.service.turn(_turn(stack), RecordingSignals(stack.frames, heid))
    assert retry.done
    assert len(stack.launcher.sends) == 1
    assert stack.launcher.observed_after == [None, "6"]
    provider = [
        f
        for f in stack.frames._executions[heid].frames.values()
        if f.provider_key.startswith("bridge:")
    ]
    # Offsets 1..11 (the terminal result ends observation before `done`).
    assert len({frame.provider_key for frame in provider}) == len(provider) == 11
    assert len(stack.frames._executions[heid].frames) > before


async def test_a_new_worker_reattaches_by_resume_and_re_supplies_tools(tmp_path: Path) -> None:
    stack = local_stack(tmp_path, binding_changes={"disallowed_tools": ("webSearch",)})
    lanes = _service(stack)
    heid = _identity(stack).harness_execution_id
    bounds = LaneSegmentBounds(max_frames=3, max_duration_s=30, start_to_close_s=60)
    first = await lanes.service.turn(
        _turn(stack, segment=bounds), RecordingSignals(stack.frames, heid)
    )
    assert not first.done
    # A different worker process: no in-memory session, same lease store and bridge state.
    stack.harness._sessions.clear()
    second = await lanes.service.turn(
        _turn(
            stack,
            segment=bounds.model_copy(update={"max_frames": 50}),
            phase="resume",
            cursor=first.cursor,
            segment_no=2,
        ),
        RecordingSignals(stack.frames, heid),
    )
    assert second.done
    ((agent_id, spec),) = stack.launcher.resumed
    assert agent_id == stack.launcher.meta["agent_id"]
    assert spec.disallowed_tools == ("webSearch",), "tools are re-supplied on resume"
    assert len(stack.launcher.launches) == 2 and len(stack.launcher.sends) == 1


async def test_a_turn_the_bridge_store_lost_is_in_doubt(tmp_path: Path) -> None:
    stack = local_stack(tmp_path)
    lanes = _service(stack)
    heid = _identity(stack).harness_execution_id
    bounds = LaneSegmentBounds(max_frames=2, max_duration_s=30, start_to_close_s=60)
    first = await lanes.service.turn(
        _turn(stack, segment=bounds), RecordingSignals(stack.frames, heid)
    )
    stack.harness._sessions.clear()
    stack.launcher.lost = True
    lost = await lanes.service.turn(
        _turn(stack, phase="resume", cursor=first.cursor, segment_no=2),
        RecordingSignals(stack.frames, heid),
    )
    result = OperationExecutionResult.model_validate(lost.operation_result)
    assert result.status == "in_doubt" and result.failure_code == "native_turn_lost"
    assert len(stack.launcher.sends) == 1


async def test_a_requested_cancel_reaches_run_cancel_and_settles(tmp_path: Path) -> None:
    from mission_control.domain.execution.lane_turns import LaneCancelRequest

    stack = local_stack(tmp_path, launcher_changes={"hold_at": 6})
    lanes = _service(stack)
    heid = _identity(stack).harness_execution_id
    signals = RecordingSignals(stack.frames, heid, cancel=True)
    task = asyncio.create_task(lanes.service.turn(_turn(stack), signals))
    await asyncio.wait_for(stack.launcher.held.wait(), timeout=10)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert stack.launcher.cancel_calls == 1
    outcome = await lanes.service.cancel(
        LaneCancelRequest.model_validate(
            {"operation": stack.operation, "lane_profile": "cursor_local", "generation": 1}
        )
    )
    assert outcome.settled and outcome.receipt.already_terminal
    settled = OperationExecutionResult.model_validate(outcome.operation_result)
    assert settled.status == "cancelled"
    (lease,) = stack.leases._leases.values()
    assert lease.released and lease.patch_artifact_ref is not None


@pytest.mark.parametrize(
    ("status", "expected"),
    [
        ("finished", "finished"),
        ("error", "error"),
        ("cancelled", "cancelled"),
        ("expired", "expired"),
    ],
)
def test_closing_facts_map_every_run_result_status(status: str, expected: str) -> None:
    facts = closing_facts(
        {
            "status": status,
            "result": "x" * 10_000,
            "durationMs": 12,
            "model": {"id": "composer-2"},
            "usage": {"inputTokens": 3, "outputTokens": 4},
            "git": {"branches": [{"repoUrl": "github.com/acme/repo", "branch": "mc/run-1"}]},
        }
    )
    assert facts.native_status == expected and len(facts.result_excerpt) == 4_096
    assert facts.usage.disposition == "estimated" and facts.usage.total_tokens == 7
    assert facts.cost_disposition == "estimated" and facts.git_branches[0].branch == "mc/run-1"


def test_sdk_events_and_wire_envelopes_map_identically() -> None:
    from cursor_sdk.types import RunStreamEvent

    wire = {
        "offset": "4",
        "sdkMessage": {
            "type": "tool_call",
            "agentId": "agent-1",
            "runId": "run-1",
            "callId": "call-1",
            "name": "shell",
            "status": "completed",
            "result": {"exitCode": 0},
        },
    }
    parsed = envelope_from_event(RunStreamEvent.from_json(wire))
    assert parsed.offset == "4"
    from_sdk = map_envelope(parsed.envelope)
    from_wire = map_envelope({"sdkMessage": wire["sdkMessage"]})
    assert (from_sdk.raw_kind, from_sdk.tool_call_ref) == (
        from_wire.raw_kind,
        from_wire.tool_call_ref,
    )
    assert from_sdk.body["status"] == "completed"
    result = envelope_from_event(
        RunStreamEvent.from_json(
            {"offset": "9", "result": {"result": {"runId": "run-1", "status": "finished"}}}
        )
    )
    assert map_envelope(result.envelope).terminal


def test_offsets_round_trip_through_provider_keys() -> None:
    key = local_provider_key("run-1", "17")
    assert offset_of(key) == "17"
    assert offset_of("bridge:run-1:final") is None and offset_of("sse:1") is None


def test_scrub_removes_secret_values_and_emails() -> None:
    raw = {
        "user_email": "owner@example.com",
        "nested": ["contact owner@example.com", "key sk-live-123"],
        "text": "token sk-live-123 here",
    }
    cleaned = scrub(raw, secrets=("sk-live-123",))
    rendered = json.dumps(cleaned)
    assert "owner@example.com" not in rendered and "sk-live-123" not in rendered
    assert cleaned["user_email"] is None


def test_committed_fixtures_are_marked_synthetic_and_carry_no_email() -> None:
    from tests.fixtures.cursor_local import FIXTURES

    for path in FIXTURES.glob("*.jsonl"):
        text = path.read_text("utf-8")
        assert '"synthetic": true' in text.splitlines()[0], path.name
        assert "@" not in text.replace("@localhost", ""), path.name


@pytest.mark.parametrize(
    ("event", "decision", "expected", "code"),
    [
        ("before_tool", "deny", {"permission": "deny", "agent_message": "no"}, 2),
        ("before_shell", "allow", {"permission": "allow"}, 0),
        ("before_prompt", "defer", {"continue": False, "agent_message": "no"}, 2),
        ("after_tool", "allow", {}, 0),
        ("session_start", "allow", {"additional_context": "index"}, 0),
    ],
)
def test_kernel_script_answers_in_cursor_native_shape(
    event: str, decision: str, expected: dict[str, Any], code: int
) -> None:
    result: dict[str, Any] = {"decision": decision}
    if decision != "allow":
        result["reason"] = "no"
    if event == "session_start":
        result["additional_context"] = "index"
    text, exit_code = native_output(event, result)
    assert json.loads(text) == expected and exit_code == code


def test_describe_is_the_declared_unqualified_matrix_and_every_control_is_implemented(
    tmp_path: Path,
) -> None:
    stack = local_stack(tmp_path)
    describe = stack.harness.describe()
    assert describe == CURSOR_LOCAL_DESCRIBE and describe.qualified is False
    assert isinstance(stack.harness, SessionLane)
    for operation in HARNESS_PROTOCOL_METHODS:
        if operation == "describe":
            continue
        if describe.implemented(operation):
            assert implements(stack.harness, operation) or hasattr(stack.harness, operation)


async def test_worker_loss_between_persist_and_heartbeat_does_not_resend(tmp_path: Path) -> None:
    stack = local_stack(tmp_path)
    lanes = _service(stack)
    heid = _identity(stack).harness_execution_id
    with pytest.raises(SimulatedWorkerLoss):
        await lanes.service.turn(_turn(stack), RecordingSignals(stack.frames, heid, fail_on=3))
    stored = len(
        [
            f
            for f in stack.frames._executions[heid].frames.values()
            if f.provider_key.startswith("bridge:")
        ]
    )
    assert stored == 3
    again = await lanes.service.turn(_turn(stack), RecordingSignals(stack.frames, heid))
    assert again.done and len(stack.launcher.sends) == 1
    assert stack.launcher.observed_after[-1] == "3"


def test_the_worker_registry_carries_the_real_cursor_local_lane_when_cursor_is_bound(
    tmp_path: Path,
) -> None:
    from pydantic import SecretStr

    from mission_control.adapters.cursor import CursorLaneStub
    from mission_control.adapters.cursor.local import CursorLocalHarness
    from mission_control.adapters.operations.conformance import ConformanceRuntime
    from mission_control.adapters.temporal.deployment_composition import compose_lane_registry
    from mission_control.application.execution.harness.deep_agents_harness import (
        DeepAgentsHarness,
    )
    from tests.fixtures.isolated_settings import isolated_settings

    stack = local_stack(tmp_path)
    deep_agents = DeepAgentsHarness(ConformanceRuntime())
    unbound = compose_lane_registry(isolated_settings(), deep_agents)
    assert unbound.profiles() == ("deep_agents",)
    bound = compose_lane_registry(
        isolated_settings(cursor_api_key=SecretStr("cursor-test")),
        deep_agents,
        cursor_local=stack.harness,
    )
    assert isinstance(bound.for_profile("cursor_local"), CursorLocalHarness)
    assert isinstance(bound.for_profile("cursor_cloud"), CursorLaneStub)
    assert bound.describe("cursor_local").qualified is False
