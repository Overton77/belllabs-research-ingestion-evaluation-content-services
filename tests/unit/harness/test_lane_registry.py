"""FT-G1: lane registry, conformance of every registered profile, and the Deep Agents lane."""

from __future__ import annotations

import asyncio
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from mission_control.adapters.cursor import CursorLaneStub, cursor_lane_stubs
from mission_control.adapters.operations.conformance import ConformanceRuntime, ConformanceSandbox
from mission_control.application.execution.harness.deep_agents_harness import (
    TERMINAL_CURSOR,
    DeepAgentsHarness,
    HarnessSessionUnknown,
)
from mission_control.application.execution.harness.protocol import (
    AgentHarness,
    HarnessUnsupported,
    implements,
)
from mission_control.application.execution.harness.registry import (
    DescribeOnlyLane,
    LaneNotExecutable,
    LaneNotQualified,
    LaneRegistry,
    UnknownLaneProfile,
    describe_only_registry,
)
from mission_control.application.execution.operations.operation_execution import (
    bind_operation_execution_request,
)
from mission_control.domain.authoring.contracts import SecretRef
from mission_control.domain.execution.contracts import RuntimeInvocation, RuntimeResult
from mission_control.domain.execution.lanes import (
    HARNESS_OPERATIONS,
    CancelTurnRequest,
    EndSessionRequest,
    HarnessScope,
    LaneFrame,
    ObserveRequest,
    PrepareRequest,
    ReattachRequest,
    SendTurnRequest,
    SnapshotRequest,
    StartRequest,
    UsageRequest,
)
from tests.unit.operations.test_operation_execution import operation_request

DIGEST = "sha256:" + "b" * 64
SCOPE = HarnessScope(
    installation_id="installation", application_id="biotech", tenant_id="tenant", actor_id="t"
)


def _base(profile: str = "deep_agents", **extra: Any) -> dict[str, Any]:
    return {
        "scope": SCOPE,
        "lane_profile": profile,
        "harness_execution_id": "hx-1",
        "binding_digest": DIGEST,
        "idempotency_key": "hx-1:1",
        "generation": 1,
        **extra,
    }


class SlowRuntime(ConformanceRuntime):
    def __init__(self) -> None:
        super().__init__()
        self.release = asyncio.Event()
        self.cancelled = False

    async def execute(
        self, invocation: RuntimeInvocation, resolved_secrets: Mapping[str, str]
    ) -> RuntimeResult:
        try:
            await self.release.wait()
        except asyncio.CancelledError:
            self.cancelled = True
            raise
        return await super().execute(invocation, resolved_secrets)


class Secrets:
    async def resolve(self, refs: tuple[SecretRef, ...]) -> Mapping[str, str]:
        return {f"{ref.provider}:{ref.key}": "never-printed" for ref in refs}


async def _invocation() -> RuntimeInvocation:
    request = operation_request()
    binding = bind_operation_execution_request(request)
    return RuntimeInvocation(
        binding=binding,
        prompt_segments=request.prompt_segments,
        workspace=await ConformanceSandbox().materialize(binding),
    )


def _worker_registry(runtime: Any = None, *, allow_unqualified: bool = False) -> LaneRegistry:
    return LaneRegistry(
        [DeepAgentsHarness(runtime or ConformanceRuntime(), Secrets()), *cursor_lane_stubs()],
        allow_unqualified=allow_unqualified,
    )


def test_registry_resolves_profiles_and_refuses_unknown_and_duplicates() -> None:
    registry = _worker_registry()
    assert registry.profiles() == ("cursor_cloud", "cursor_local", "deep_agents")
    assert isinstance(registry.for_profile("deep_agents"), DeepAgentsHarness)
    assert isinstance(registry.for_profile("cursor_local"), CursorLaneStub)
    with pytest.raises(UnknownLaneProfile, match="claude_agent"):
        registry.for_profile("claude_agent")
    with pytest.raises(ValueError, match="already registered"):
        registry.register(CursorLaneStub("cursor_local"))
    assert [item.lane_profile for item in registry.describe_all()] == list(registry.profiles())
    assert isinstance(registry.for_profile("deep_agents"), AgentHarness)


def test_unqualified_lanes_are_refused_at_admission_unless_policy_allows() -> None:
    registry = _worker_registry()
    assert registry.admit("deep_agents") is registry.for_profile("deep_agents")
    with pytest.raises(LaneNotQualified, match="cursor_local") as refused:
        registry.admit("cursor_local")
    # The refusal names the local-proof flag and the qualification command.
    assert "MISSION_CONTROL_ALLOW_UNQUALIFIED_LANES=true" in str(refused.value)
    assert "make lane-qualify PROFILE=cursor_local LIVE=1" in str(refused.value)
    local_proof = _worker_registry(allow_unqualified=True)
    assert isinstance(local_proof.admit("cursor_cloud"), CursorLaneStub)


def test_dispatch_routes_by_runtime_and_profile() -> None:
    registry = _worker_registry(allow_unqualified=True)
    request = operation_request()
    assert request.execution_runtime == "native"
    lane = registry.lane_for(request)
    assert isinstance(lane, DeepAgentsHarness)
    assert lane.requires_checkpoint_lineage(request) is False
    explicit = request.model_copy(update={"lane_profile": "deep_agents"})
    assert registry.lane_for(explicit) is lane
    # A describe-only entry (or a stub) is registered but cannot execute an operation.
    with pytest.raises(LaneNotExecutable):
        LaneRegistry([DescribeOnlyLane(registry.describe("deep_agents"))]).lane_for(request)
    with pytest.raises(UnknownLaneProfile):
        LaneRegistry([CursorLaneStub("cursor_local")]).lane_for(request)


@pytest.mark.parametrize("profile", ["deep_agents", "cursor_local", "cursor_cloud"])
async def test_conformance_every_declared_control_is_honest(profile: str) -> None:
    harness = _worker_registry().for_profile(profile)
    describe = harness.describe()
    for operation in HARNESS_OPERATIONS:
        support = describe.control(operation)
        if support in {"native", "emulated"}:
            assert implements(harness, operation), f"{profile}.{operation} is a stub"
            continue
        assert not implements(harness, operation), f"{profile}.{operation} is undeclared"
        with pytest.raises(HarnessUnsupported) as raised:
            result = getattr(harness, operation)(object())
            if asyncio.iscoroutine(result):
                await result
        assert raised.value.operation == operation
        assert raised.value.lane_profile == profile
        assert raised.value.reason == support


def test_describe_only_registry_mirrors_the_worker_composition() -> None:
    plain = describe_only_registry(cursor_bound=False, allow_unqualified=False)
    assert plain.profiles() == ("deep_agents",)
    bound = describe_only_registry(cursor_bound=True, allow_unqualified=False)
    assert bound.profiles() == ("cursor_cloud", "cursor_local", "deep_agents")
    assert bound.describe("cursor_local") == CursorLaneStub("cursor_local").describe()
    assert bound.describe("deep_agents") == DeepAgentsHarness(ConformanceRuntime()).describe()


async def test_deep_agents_execute_path_is_the_wrapped_runtime_unchanged() -> None:
    runtime = ConformanceRuntime()
    harness = DeepAgentsHarness(runtime)
    invocation = await _invocation()
    result = await harness.execute(invocation, {"k": "v"})
    assert runtime.invocations == [invocation]
    assert result == await runtime.execute(invocation, {"k": "v"})


async def test_deep_agents_harness_runs_one_session_turn_through_the_protocol() -> None:
    runtime = ConformanceRuntime()
    harness = DeepAgentsHarness(runtime, Secrets())
    with pytest.raises(HarnessSessionUnknown):
        await harness.prepare(PrepareRequest(**_base(run_id="r", operation_id="o", attempt_no=1)))
    invocation = await _invocation()
    harness.stage("hx-1", 1, invocation)
    prepared = await harness.prepare(
        PrepareRequest(**_base(run_id="r", operation_id="o", attempt_no=1))
    )
    assert prepared.workspace_ref == f"workspace:{invocation.workspace.workspace_id}"
    session = await harness.start(StartRequest(**_base(prepared=prepared)))
    assert session.native_session_ref == f"binding:{invocation.binding.binding_id}"
    turn = await harness.send_turn(
        SendTurnRequest(**_base(session=session, turn_no=1, instruction_ref="instruction:1"))
    )
    assert turn.status == "accepted"
    frames: list[LaneFrame] = [
        frame async for frame in harness.observe(ObserveRequest(**_base(turn=turn)))
    ]
    assert [frame.kind for frame in frames] == ["terminal"]
    assert frames[0].terminal and frames[0].cursor == TERMINAL_CURSOR
    # Resuming after the terminal cursor yields nothing and never re-sends the turn.
    resumed = [
        frame
        async for frame in harness.observe(
            ObserveRequest(**_base(turn=turn, after=TERMINAL_CURSOR))
        )
    ]
    assert resumed == []
    again = await harness.send_turn(
        SendTurnRequest(**_base(session=session, turn_no=1, instruction_ref="instruction:1"))
    )
    assert again.native_turn_ref == turn.native_turn_ref and len(runtime.invocations) == 1
    usage = await harness.usage(UsageRequest(**_base(session=session, turn=turn)))
    assert usage.disposition == "settled" and usage.total_tokens == 3
    with pytest.raises(ValueError, match="checkpoint"):
        await harness.snapshot(SnapshotRequest(**_base(session=session, reason="fork")))
    receipt = await harness.end_session(EndSessionRequest(**_base(session=session, reason="done")))
    assert receipt.released
    assert not (
        await harness.end_session(EndSessionRequest(**_base(session=session, reason="done")))
    ).released


async def test_deep_agents_cancel_turn_reaches_cognition_and_is_idempotent() -> None:
    runtime = SlowRuntime()
    harness = DeepAgentsHarness(runtime, Secrets())
    harness.stage("hx-1", 1, await _invocation())
    prepared = await harness.prepare(
        PrepareRequest(**_base(run_id="r", operation_id="o", attempt_no=1))
    )
    session = await harness.start(StartRequest(**_base(prepared=prepared)))
    turn = await harness.send_turn(
        SendTurnRequest(**_base(session=session, turn_no=1, instruction_ref="instruction:1"))
    )
    busy = await harness.send_turn(
        SendTurnRequest(**_base(session=session, turn_no=2, instruction_ref="instruction:2"))
    )
    assert busy.status == "busy"
    await asyncio.sleep(0)
    receipt = await harness.cancel_turn(
        CancelTurnRequest(**_base(turn=turn, reason="command", urgency="normal"))
    )
    assert receipt.acknowledged and receipt.native_status == "cancelled"
    assert runtime.cancelled
    again = await harness.cancel_turn(CancelTurnRequest(**_base(turn=turn, reason="command")))
    assert again.already_terminal
    frames = [frame async for frame in harness.observe(ObserveRequest(**_base(turn=turn)))]
    assert [frame.excerpt for frame in frames] == ["cancelled"]


async def test_deep_agents_reattach_reads_the_latest_checkpoint_without_a_new_turn() -> None:
    class Observing(ConformanceRuntime):
        def __init__(self) -> None:
            super().__init__()
            self.observed = 0

        async def observe_latest(
            self, invocation: RuntimeInvocation, resolved_secrets: Mapping[str, str]
        ) -> RuntimeResult:
            self.observed += 1
            return RuntimeResult(output_text="latest")

    runtime = Observing()
    harness = DeepAgentsHarness(runtime, Secrets())
    invocation = await _invocation()
    harness.stage("hx-1", 1, invocation)
    session = await harness.reattach(
        ReattachRequest(**_base(native_session_ref=f"binding:{invocation.binding.binding_id}"))
    )
    assert session.native_session_ref and runtime.observed == 1 and runtime.invocations == []
    with pytest.raises(ValueError, match="another thread"):
        await harness.reattach(ReattachRequest(**_base(native_session_ref="thread:other")))


def test_harness_contracts_carry_no_secret_and_reject_bad_identity() -> None:
    deadline = datetime.now(UTC) + timedelta(minutes=1)
    request = PrepareRequest(**_base(run_id="r", operation_id="o", attempt_no=1, deadline=deadline))
    assert "secret" not in request.model_dump_json().lower()
    with pytest.raises(ValueError):
        PrepareRequest(**_base(run_id="r", operation_id="o", attempt_no=0))
    with pytest.raises(ValueError):
        PrepareRequest(**{**_base(run_id="r", operation_id="o", attempt_no=1), "generation": 0})
