"""FT-G3 fixtures: a replaying Cursor local bridge, git repositories and the lane stack.

`ReplayBridgeLauncher` stands in for the pinned `cursor-sdk` bridge. It replays a recorded
(or hand-authored, marked synthetic) JSONL fixture under `tests/integration/cursor/fixtures/
local/`: `event` records are `ObserveRun` envelopes with their durable offsets, `hook` records
are Cursor hook invocations it fires through the kernel hook path while the run progresses
(as Cursor does), `workspace_write` records are files the agent writes, `run_state` and
`usage` answer `GetRun` and `GetUsage`. No Cursor agent is created and nothing is paid for.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from tests.fixtures.lane_turns import cursor_binding, cursor_operation

from mission_control.adapters.cursor.bridge import (
    AgentBusy,
    BridgeEvent,
    FeatureUnavailable,
    LocalAgentSpec,
    RunNotCancellable,
    RunNotFound,
    RunState,
    SandboxUnsupported,
    UsageState,
)
from mission_control.adapters.cursor.hooks_callback import CursorHookMapper
from mission_control.adapters.cursor.local import CursorLocalHarness, CursorLocalSettings
from mission_control.adapters.cursor.projection import (
    RenderedProjectionSource,
    projection_digests,
    static_rows,
)
from mission_control.adapters.cursor.workspace import GitWorktreeLeaser, git
from mission_control.application.agentic_components.projections import render_host_files
from mission_control.application.execution.harness.hook_callbacks import (
    HookCallbackService,
    InMemoryHookIntentLedger,
    InMemoryHookTokenStore,
    KernelHookCall,
)
from mission_control.application.execution.harness.leases import InMemoryWorkspaceLeaseStore
from mission_control.application.execution.stop_fence import (
    InMemoryStopFenceRepository,
    KernelHookFenceGate,
)
from mission_control.application.frames.sink import InMemoryFrameStore
from mission_control.contracts.hooks import HookResult, merge_results
from mission_control.domain.agentic_components.projection import ResolvedCapability
from mission_control.domain.capabilities.hooks import KERNEL_HOOK_EVENTS, KERNEL_HOOK_IDS, HookEvent
from mission_control.domain.execution.contracts import OperationExecutionRequest
from mission_control.domain.execution.lanes import CursorExecutionBinding

FIXTURES = Path(__file__).resolve().parents[1] / "integration" / "cursor" / "fixtures" / "local"


def load_fixture(name: str) -> list[dict[str, Any]]:
    lines = (FIXTURES / f"{name}.jsonl").read_text(encoding="utf-8").splitlines()
    return [json.loads(line) for line in lines if line.strip()]


def make_repository(root: Path) -> Path:
    """A small git repository with one commit on `main` (the target repository)."""

    root.mkdir(parents=True, exist_ok=True)
    git("init", "--quiet", "--initial-branch=main", cwd=root)
    (root / "README.md").write_text("# target\n", encoding="utf-8")
    (root / "AGENTS.md").write_text(
        "Repository guide (owned by the repository).\n", encoding="utf-8"
    )
    git("add", "--all", cwd=root)
    git(
        "-c",
        "user.name=Fixture",
        "-c",
        "user.email=fixture@localhost",
        "commit",
        "--quiet",
        "-m",
        "base",
        cwd=root,
    )
    return root


HookInvoker = Callable[[dict[str, Any], Path], Awaitable[HookResult]]


@dataclass
class ReplayBridge:
    launcher: ReplayBridgeLauncher
    workspace: Path
    state_root: Path

    async def create_agent(self, spec: LocalAgentSpec) -> str:
        self.launcher.created.append(spec)
        if spec.sandbox_enabled and not self.launcher.sandbox:
            raise SandboxUnsupported("the Cursor sandbox is unavailable on this host")
        return str(self.launcher.meta["agent_id"])

    async def resume_agent(self, agent_id: str, spec: LocalAgentSpec) -> str:
        self.launcher.resumed.append((agent_id, spec))
        if self.launcher.lost or agent_id != self.launcher.meta["agent_id"]:
            raise RunNotFound("unknown agent")
        return agent_id

    async def send(self, agent_id: str, text: str, *, idempotency_key: str) -> str:
        if self.launcher.busy_sends > 0:
            self.launcher.busy_sends -= 1
            raise AgentBusy("busy")
        self.launcher.sends.append((idempotency_key, text))
        return str(self.launcher.meta["run_id"])

    async def observe(self, run_id: str, *, after_offset: str | None) -> AsyncIterator[BridgeEvent]:
        self.launcher.observed_after.append(after_offset)
        after = int(after_offset) if after_offset else 0
        pending_hooks: list[dict[str, Any]] = []
        for record in self.launcher.records:
            if record["kind"] == "hook":
                pending_hooks.append(record)
                continue
            if record["kind"] != "event":
                continue
            offset = int(record["offset"])
            envelope = record["envelope"]
            if offset <= after:
                pending_hooks.clear()
                continue
            for hook in pending_hooks:
                await self.launcher.fire(hook, self.workspace)
            pending_hooks.clear()
            if self.launcher.fail_at is not None and offset == self.launcher.fail_at:
                self.launcher.fail_at = None
                raise ConnectionError("bridge stream dropped")
            if "result" in envelope:
                await asyncio.to_thread(self.launcher.write_files, self.workspace)
            if self.launcher.hold_at is not None and offset >= self.launcher.hold_at:
                self.launcher.held.set()
                await self.launcher.release.wait()
                if self.launcher.cancelled:
                    return
            yield BridgeEvent(offset=str(offset), envelope=envelope)
        for hook in pending_hooks:
            await self.launcher.fire(hook, self.workspace)

    async def run_state(self, run_id: str) -> RunState:
        if self.launcher.lost:
            raise RunNotFound("unknown run")
        state = next(r for r in self.launcher.records if r["kind"] == "run_state")
        status = "cancelled" if self.launcher.cancelled else state["status"]
        return RunState(
            run_id=run_id,
            agent_id=str(self.launcher.meta["agent_id"]),
            status=status,
            result=state.get("result", ""),
            duration_ms=state.get("duration_ms", 0),
            model=state.get("model"),
            usage=state.get("usage"),
        )

    async def cancel(self, run_id: str, *, agent_id: str) -> None:
        self.launcher.cancel_calls += 1
        if self.launcher.cancelled:
            raise RunNotCancellable("already terminal")
        self.launcher.cancelled = True
        self.launcher.release.set()

    async def usage(self, agent_id: str) -> UsageState:
        usage = next(r for r in self.launcher.records if r["kind"] == "usage")
        if usage.get("feature_unavailable"):
            raise FeatureUnavailable("local usage is not enabled")
        return UsageState(
            input_tokens=usage["input_tokens"],
            output_tokens=usage["output_tokens"],
            total_tokens=usage["total_tokens"],
            cost_micros_usd=usage.get("cost_micros_usd"),
        )

    async def close_agent(self, agent_id: str) -> None:
        self.launcher.closed.append(agent_id)

    async def aclose(self) -> None:
        self.launcher.bridges_closed += 1


@dataclass
class ReplayBridgeLauncher:
    records: list[dict[str, Any]]
    hook_invoker: HookInvoker | None = None
    sandbox: bool = True
    busy_sends: int = 0
    lost: bool = False
    fail_at: int | None = None
    hold_at: int | None = None
    launches: list[tuple[Path, Path]] = field(default_factory=list)
    created: list[LocalAgentSpec] = field(default_factory=list)
    resumed: list[tuple[str, LocalAgentSpec]] = field(default_factory=list)
    sends: list[tuple[str, str]] = field(default_factory=list)
    observed_after: list[str | None] = field(default_factory=list)
    hook_results: list[tuple[str, str, HookResult]] = field(default_factory=list)
    closed: list[str] = field(default_factory=list)
    bridges_closed: int = 0
    cancel_calls: int = 0
    cancelled: bool = False
    held: asyncio.Event = field(default_factory=asyncio.Event)
    release: asyncio.Event = field(default_factory=asyncio.Event)
    fired: set[int] = field(default_factory=set)

    @property
    def meta(self) -> dict[str, Any]:
        return next(r for r in self.records if r["kind"] == "meta")

    @property
    def versions(self) -> Mapping[str, str]:
        return {"cursor_sdk": "1.0.37", "bridge": "1.0.37", "protocol": "sdk.v1"}

    def sandbox_supported(self) -> bool:
        return self.sandbox

    async def launch(self, *, workspace: Path, state_root: Path) -> ReplayBridge:
        self.launches.append((workspace, state_root))
        return ReplayBridge(self, workspace, state_root)

    async def fire(self, record: dict[str, Any], workspace: Path) -> None:
        if id(record) in self.fired or self.hook_invoker is None:
            return
        self.fired.add(id(record))
        payload = _substitute(record["payload"], str(workspace))
        result = await self.hook_invoker({**record, "payload": payload}, workspace)
        self.hook_results.append((record["native_event"], record.get("expect", "allow"), result))

    def write_files(self, workspace: Path) -> None:
        for record in self.records:
            if record["kind"] == "workspace_write":
                target = workspace / record["path"]
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_text(record["content"], encoding="utf-8")


def _substitute(value: Any, workspace: str) -> Any:
    if isinstance(value, dict):
        return {key: _substitute(item, workspace) for key, item in value.items()}
    if isinstance(value, list):
        return [_substitute(item, workspace) for item in value]
    if isinstance(value, str):
        return value.replace("<workspace>", workspace)
    return value


def service_hook_invoker(service: HookCallbackService) -> HookInvoker:
    """Run every kernel hook Cursor runs for one native event, through the callback service,
    with the token the lane wrote into the lease (as `.mission/hooks/kernel.py` does)."""

    async def invoke(record: dict[str, Any], workspace: Path) -> HookResult:
        context = json.loads((workspace / ".mission/hooks/context.json").read_text("utf-8"))
        token = (workspace / ".mission/bin/.token").read_text("utf-8")
        event = HookEvent(record["mc_event"])
        results = []
        for hook_id in KERNEL_HOOK_IDS:
            if event not in KERNEL_HOOK_EVENTS[hook_id]:
                continue
            call = KernelHookCall(
                kernel_hook_id=hook_id,
                event=event,
                scope=context["scope"],
                harness_execution_id=context["harness_execution_id"],
                generation=context["generation"],
                payload=record["payload"],
            )
            results.append(await service.handle(call, token))
        return merge_results(results)

    return invoke


class MemoryArtifacts:
    def __init__(self) -> None:
        self.staged: dict[str, tuple[str, bytes, str]] = {}

    async def stage(self, *, request_scope: str, name: str, content: bytes, media_type: str) -> str:
        ref = f"payload://{len(self.staged)}/{name}"
        self.staged[ref] = (name, content, media_type)
        return ref


def projection_rows() -> tuple[ResolvedCapability, ...]:
    from tests.fixtures.projections.rows import hook_row, skill_row, tavily_row, verifier_row

    return (skill_row(), tavily_row(), hook_row(), verifier_row())


def pinned_binding(
    *,
    rows: tuple[ResolvedCapability, ...],
    operation_contract: str,
    repository: Path | None,
    **changes: Any,
) -> CursorExecutionBinding:
    projection = render_host_files(rows, "cursor_local", operation_contract, None)
    digests = projection_digests(projection)
    workspace = {"base_ref": "main"}
    if repository is not None:
        workspace["repo_url"] = str(repository)
    return cursor_binding(
        "cursor_local",
        projections={**digests},
        workspace=workspace,
        **changes,
    )


@dataclass
class LocalStack:
    harness: CursorLocalHarness
    launcher: ReplayBridgeLauncher
    hooks: HookCallbackService
    tokens: InMemoryHookTokenStore
    intents: InMemoryHookIntentLedger
    fences: InMemoryStopFenceRepository
    frames: InMemoryFrameStore
    leases: InMemoryWorkspaceLeaseStore
    artifacts: MemoryArtifacts
    operation: OperationExecutionRequest
    lease_root: Path


def local_stack(
    tmp_path: Path,
    fixture: str = "full_run",
    *,
    repository: bool = True,
    binding_changes: Mapping[str, Any] | None = None,
    launcher_changes: Mapping[str, Any] | None = None,
    frames: InMemoryFrameStore | None = None,
    drift: bool = False,
) -> LocalStack:
    records = load_fixture(fixture)
    rows = projection_rows()
    base = cursor_operation()
    from mission_control.adapters.cursor.projection import operating_contract

    repo = make_repository(tmp_path / "repo") if repository else None
    changes = dict(binding_changes or {})
    meta = next(r for r in records if r["kind"] == "meta")
    if meta.get("disallowed_tools") and "disallowed_tools" not in changes:
        changes["disallowed_tools"] = tuple(meta["disallowed_tools"])
    binding = pinned_binding(
        rows=rows, operation_contract=operating_contract(base), repository=repo, **changes
    )
    if drift:
        binding = cursor_binding(
            "cursor_local",
            projections={**binding.projections.model_dump(), "hooks_digest": "sha256:" + "f" * 64},
            workspace=binding.workspace.model_dump(exclude_none=True),
            **changes,
        )
    operation = cursor_operation(binding=binding)
    frames = frames or InMemoryFrameStore()
    tokens = InMemoryHookTokenStore()
    intents = InMemoryHookIntentLedger()
    fences = InMemoryStopFenceRepository()

    def context_reader(context: Any) -> str | None:
        path = Path(context.workspace_root or "") / ".mission/context.md"
        return path.read_text("utf-8") if path.is_file() else "packet index"

    hooks = HookCallbackService(
        tokens=tokens,
        intents=intents,
        mapper=CursorHookMapper(),
        fences=KernelHookFenceGate(fences),
        frames=frames,
        context_reader=context_reader,
    )
    launcher = ReplayBridgeLauncher(
        records, hook_invoker=service_hook_invoker(hooks), **dict(launcher_changes or {})
    )
    leases = InMemoryWorkspaceLeaseStore()
    artifacts = MemoryArtifacts()
    lease_root = tmp_path / "leases"
    harness = CursorLocalHarness(
        launcher=launcher,
        leaser=GitWorktreeLeaser(leases, lease_root=lease_root),
        projections=RenderedProjectionSource(static_rows(rows)),
        hooks=hooks,
        artifacts=artifacts,
        settings=CursorLocalSettings(lease_root=lease_root),
    )
    return LocalStack(
        harness,
        launcher,
        hooks,
        tokens,
        intents,
        fences,
        frames,
        leases,
        artifacts,
        operation,
        lease_root,
    )
