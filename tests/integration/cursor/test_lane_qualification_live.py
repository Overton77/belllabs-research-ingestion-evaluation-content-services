"""FT-G6 live qualification drill (paid, owner-run; skipped unless explicitly enabled).

Runs only with `MC_LIVE_CURSOR_QUALIFICATION=1`, a `CURSOR_API_KEY` and a finite
`MC_PAID_BUDGET_USD` (`make lane-qualify PROFILE=cursor_local LIVE=1`); the cloud drill also
needs `MC_CURSOR_CLOUD_REPO` (a throwaway repository the worker's git can push to). The repeated
cancel-latency and busy drills run only with `MC_LANE_DRILL_APPROVAL_URL` naming the owner's
Linear approval comment (TEAM-WORKSPACE budget policy). CI and the fast-track agents never run
it.

Each drill drives the real lane end to end (projections, packet, kernel hooks through a real
loopback listener for `cursor_local`, frames, closing facts, settlement), records the traffic in
the offline fixture shapes (`recordings/`), measures what the describe matrix promises (cancel
latency, busy behavior), settles the UNVERIFIED items it can, and writes
`docs/qualification/lanes/<profile>-<date>.md`. Stop and record if a paid effect is ambiguous
(an agent created without a recorded id): the drill marks it `unknown` and never qualifies.
"""

from __future__ import annotations

import asyncio
import os
import platform
import socket
from contextlib import AsyncExitStack
from pathlib import Path
from typing import Any

import pytest
from pydantic import SecretStr

from mission_control.adapters.cursor.bridge import AgentBusy, LocalAgentSpec, SdkBridgeLauncher
from mission_control.adapters.cursor.hooks_callback import CursorHookMapper
from mission_control.adapters.cursor.local import CursorLocalHarness, CursorLocalSettings
from mission_control.adapters.cursor.projection import RenderedProjectionSource, static_rows
from mission_control.adapters.cursor.workspace import GitWorktreeLeaser
from mission_control.application.execution.harness.hook_callbacks import (
    HookCallbackService,
    InMemoryHookIntentLedger,
    InMemoryHookTokenStore,
)
from mission_control.application.execution.harness.leases import InMemoryWorkspaceLeaseStore
from mission_control.application.execution.stop_fence import (
    InMemoryStopFenceRepository,
    KernelHookFenceGate,
)
from mission_control.application.frames.sink import InMemoryFrameStore
from mission_control.domain.execution.contracts import OperationExecutionResult
from mission_control.domain.execution.lane_turns import LaneTurnRequest
from tests.fixtures.cursor_local import (
    MemoryArtifacts,
    make_repository,
    pinned_binding,
    projection_rows,
)
from tests.fixtures.lane_turns import RecordingSignals, cursor_operation, lane_stack
from tests.integration.cursor.live_drill import (
    DrillOutcome,
    RecordingLauncher,
    Spend,
    Stopwatch,
    live_enabled,
    repeated_drills_approved,
    write_local_recording,
    write_record,
)

PROFILE = os.environ.get("MC_LANE_QUALIFY_PROFILE", "cursor_local")


def _free_port() -> int:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return int(probe.getsockname()[1])


@pytest.mark.skipif(
    PROFILE != "cursor_local" or not live_enabled(),
    reason=(
        "paid live drill: set MC_LIVE_CURSOR_QUALIFICATION=1, CURSOR_API_KEY, "
        "MC_PAID_BUDGET_USD (and MC_LANE_QUALIFY_PROFILE=cursor_local)"
    ),
)
async def test_cursor_local_live_qualification(tmp_path: Path) -> None:
    from mission_control.adapters.cursor.projection import operating_contract
    from mission_control.bootstrap.worker import start_hook_callback_listener

    spend = Spend(budget_usd=float(os.environ["MC_PAID_BUDGET_USD"]))
    outcome = DrillOutcome("cursor_local", spend, approval_url=repeated_drills_approved())
    real = SdkBridgeLauncher(SecretStr(os.environ["CURSOR_API_KEY"]))
    launcher = RecordingLauncher(real, spend, "live drill: one full cursor_local turn")
    repository = make_repository(tmp_path / "repo")
    rows = projection_rows()
    base = cursor_operation()
    binding = pinned_binding(
        rows=rows, operation_contract=operating_contract(base), repository=repository
    )
    operation = cursor_operation(binding=binding)
    frames = InMemoryFrameStore()
    hooks = HookCallbackService(
        tokens=InMemoryHookTokenStore(),
        intents=InMemoryHookIntentLedger(),
        mapper=CursorHookMapper(),
        fences=KernelHookFenceGate(InMemoryStopFenceRepository()),
        frames=frames,
    )
    port = _free_port()
    lease_root = tmp_path / "leases"
    harness = CursorLocalHarness(
        launcher=launcher,
        leaser=GitWorktreeLeaser(InMemoryWorkspaceLeaseStore(), lease_root=lease_root),
        projections=RenderedProjectionSource(static_rows(rows)),
        hooks=hooks,
        artifacts=MemoryArtifacts(),
        settings=CursorLocalSettings(
            lease_root=lease_root, callback_base_url=f"http://127.0.0.1:{port}"
        ),
    )
    hooks.set_stop_policy(harness)
    lanes = lane_stack(harness, frames=frames, operation=operation)
    async with AsyncExitStack() as stack:
        await start_hook_callback_listener(stack, hooks, port=port)
        watch = Stopwatch()
        result = await lanes.service.turn(
            LaneTurnRequest(operation=operation, lane_profile="cursor_local", generation=1),
            RecordingSignals(frames),
        )
        outcome.measurements["full_turn_seconds"] = watch.seconds()
    settled = OperationExecutionResult.model_validate(result.operation_result)
    outcome.checks["full_turn_settles_from_closing_facts"] = result.done and (
        settled.status in {"completed", "failed"}
    )
    hook_frames = [
        frame
        for execution in frames._executions.values()
        for frame in execution.frames.values()
        if frame.provider_key.startswith("hook")
    ]
    outcome.checks["kernel_hooks_called_back"] = bool(hook_frames)
    outcome.recordings.append(str(write_local_recording("full_run", launcher.records)))
    if not launcher.records[0].get("agent_id"):
        spend.unknown.append("an agent may exist without a recorded id")

    # UNVERIFIED items this drill settles.
    sandbox = real.sandbox_supported()
    outcome.settle(
        "windows_sandbox",
        "open" if platform.system() != "Windows" else ("verified" if sandbox else "refuted"),
        f"host {platform.system()}: sandbox_supported()={sandbox}; the lane refuses "
        "sandbox_options.enabled where unsupported",
    )
    git_seen = any(
        record.get("kind") == "run_state" and record.get("git_branches")
        for record in launcher.records
    )
    outcome.settle(
        "local_run_git",
        "verified" if git_seen else "refuted",
        "RunResult.git on the recorded local run "
        + ("carried branches" if git_seen else "was empty; the lane computes the patch itself"),
    )
    outcome.settle(
        "run_request_id",
        "refuted",
        "cursor-sdk 1.0.37 source: Run/RunSnapshot expose no request_id (only "
        "SDKRequestMessage.request_id for interaction requests)",
    )
    outcome.settle(
        "rules_without_setting_sources",
        "open",
        "the lane always passes setting_sources=['project'] (not exercised)",
    )

    if outcome.approval_url is not None:
        await _cancel_and_busy_drills(real, repository, outcome, spend)
    else:
        outcome.settle(
            "concurrent_local_send",
            "open",
            "the repeated busy drill needs MC_LANE_DRILL_APPROVAL_URL",
        )
    record = write_record(outcome)
    assert record.is_file()
    assert not spend.unknown, f"ambiguous paid effects: {spend.unknown}"


async def _cancel_and_busy_drills(
    launcher: SdkBridgeLauncher, repository: Path, outcome: DrillOutcome, spend: Spend
) -> None:
    """Cancel latency and busy behavior on one extra agent (approved repeated drill)."""

    bridge = await launcher.launch(workspace=repository, state_root=repository / ".drill-state")
    try:
        agent_id = await bridge.create_agent(
            LocalAgentSpec(model="composer-2", name="mc-qualify-drill", cwd=str(repository))
        )
        spend.agents_created.append(agent_id)
        run_id = await bridge.send(
            agent_id,
            "Count slowly from 1 to 200, one number per line, then stop. Do not edit files.",
            idempotency_key="mc-qualify-drill:1",
        )
        spend.runs_started.append(run_id)
        try:
            await bridge.send(agent_id, "Also say hello.", idempotency_key="mc-qualify-drill:2")
            busy = "a second send was accepted while the run was active"
            outcome.settle("concurrent_local_send", "refuted", busy)
        except AgentBusy:
            outcome.settle(
                "concurrent_local_send", "verified", "AgentBusyError while a run is active"
            )
        await asyncio.sleep(3)
        watch = Stopwatch()
        await bridge.cancel(run_id, agent_id=agent_id)
        while not (await bridge.run_state(run_id)).terminal:
            await asyncio.sleep(0.25)
            if watch.seconds() > 120:
                spend.unknown.append(f"run {run_id} did not reach a terminal state")
                break
        outcome.measurements["cancel_latency_seconds"] = watch.seconds()
        outcome.checks["cancel_reaches_a_terminal_run"] = not spend.unknown
        await bridge.close_agent(agent_id)
    finally:
        await bridge.aclose()


@pytest.mark.skipif(
    PROFILE != "cursor_cloud" or not live_enabled(cloud=True),
    reason=(
        "paid live drill: set MC_LIVE_CURSOR_QUALIFICATION=1, CURSOR_API_KEY, "
        "MC_PAID_BUDGET_USD, MC_CURSOR_CLOUD_REPO (and MC_LANE_QUALIFY_PROFILE=cursor_cloud)"
    ),
)
async def test_cursor_cloud_live_qualification(tmp_path: Path) -> None:
    from mission_control.adapters.cursor.cloud import CursorCloudHarness
    from mission_control.adapters.cursor.cloud_api import CloudAgentsClient
    from mission_control.adapters.cursor.scm import GitBranchPublisher
    from tests.fixtures.cursor_cloud import cloud_operation, cloud_rows
    from tests.integration.cursor.live_drill import write_cloud_recording

    spend = Spend(budget_usd=float(os.environ["MC_PAID_BUDGET_USD"]))
    outcome = DrillOutcome("cursor_cloud", spend, approval_url=repeated_drills_approved())
    client = CloudAgentsClient(SecretStr(os.environ["CURSOR_API_KEY"]))
    recorder = _RecordingCloudClient(client, spend)
    rows = cloud_rows()
    operation = cloud_operation(os.environ["MC_CURSOR_CLOUD_REPO"], rows)
    harness = CursorCloudHarness(
        client=recorder,  # type: ignore[arg-type]
        publisher=GitBranchPublisher(tmp_path / "mirrors"),
        projections=RenderedProjectionSource(static_rows(rows), kernel_hooks=()),
        artifacts=MemoryArtifacts(),
    )
    lanes = lane_stack(harness, operation=operation)
    try:
        watch = Stopwatch()
        result = await lanes.service.turn(
            LaneTurnRequest(operation=operation, lane_profile="cursor_cloud", generation=1),
            RecordingSignals(lanes.frames),
        )
        while not result.done:
            result = await lanes.service.turn(
                LaneTurnRequest(
                    operation=operation,
                    lane_profile="cursor_cloud",
                    generation=1,
                    phase="resume",
                    cursor=result.cursor,
                    segment_no=result.segment_no + 1,
                ),
                RecordingSignals(lanes.frames),
            )
        outcome.measurements["full_turn_seconds"] = watch.seconds()
        outcome.checks["full_turn_settles_from_closing_facts"] = result.done
        if recorder.run_record is not None:
            stream, record = write_cloud_recording(
                "run_stream", recorder.events, recorder.run_record
            )
            outcome.recordings.extend([str(stream), str(record)])
        outcome.settle(
            "stream_retention",
            "verified" if recorder.retention is not None else "open",
            f"X-Cursor-Stream-Retention-Seconds={recorder.retention}",
        )
        outcome.settle(
            "rest_env_vars_and_metadata",
            "open" if recorder.metadata_status is None else "verified",
            f"metadata on create: {recorder.metadata_status or 'not observed'}; envVars "
            "not exercised",
        )
        outcome.settle(
            "cloud_idempotency_window",
            "open",
            "the lane creates with a client agentId (409 agent_id_conflict); the "
            "Idempotency-Key window needs an approved repeated drill",
        )
    finally:
        await client.aclose()
    write_record(outcome)
    assert not spend.unknown, f"ambiguous paid effects: {spend.unknown}"


class _RecordingCloudClient:
    """Proxies `CloudAgentsClient` and keeps the SSE events and the final run record."""

    def __init__(self, inner: Any, spend: Spend) -> None:
        self._inner = inner
        self._spend = spend
        self.events: list[Any] = []
        self.run_record: dict[str, Any] | None = None
        self.retention: int | None = None
        self.metadata_status: str | None = None

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)

    async def create_agent(self, body: dict[str, Any], **kwargs: Any) -> dict[str, Any]:
        try:
            created: dict[str, Any] = await self._inner.create_agent(body, **kwargs)
        except Exception as error:
            if "metadata" in body:
                self.metadata_status = f"refused ({type(error).__name__})"
            if body.get("agentId"):
                self._spend.unknown.append(f"create {body['agentId']} failed: confirm none exists")
            raise
        if "metadata" in body:
            self.metadata_status = "accepted"
        agent = created.get("agent") or {}
        self._spend.agents_created.append(str(agent.get("id")))
        if (created.get("run") or {}).get("id"):
            self._spend.runs_started.append(str(created["run"]["id"]))
        return created

    async def stream(self, agent_id: str, run_id: str, *, last_event_id: str | None) -> Any:
        from mission_control.adapters.cursor.cloud_api import StreamOpened

        async for item in self._inner.stream(agent_id, run_id, last_event_id=last_event_id):
            if isinstance(item, StreamOpened):
                self.retention = item.retention_seconds
            else:
                self.events.append(item)
            yield item

    async def get_run(self, agent_id: str, run_id: str) -> dict[str, Any]:
        record: dict[str, Any] = await self._inner.get_run(agent_id, run_id)
        self.run_record = {"synthetic": False, "recorded": True, "run": record}
        return record
