"""The `cursor_local` Lane Profile: Cursor's Python SDK bridge on the worker (SPEC-07 section 5).

One Session Turn of a bound operation, driven by `lane.turn` (FT-G2):

- `prepare` leases a git worktree at the binding's base ref, materializes the Context Packet
  (`.mission/`, `inputs/`, `outputs/`), places the FT-A4 Host Projection (Kernel Hooks first,
  fail-closed) plus `.mission/hooks/kernel.py`, and refuses `CAPABILITY_DRIFT` (projection
  digests differ from the binding) or `UNSUPPORTED_BEHAVIOR` (sandbox requested on a host
  without it);
- `start` launches the bridge with a pinned `state_root`, creates the agent with
  `setting_sources=["project"]`, then mints the hook task token (bound to scope, run, attempt,
  generation and harness execution; only its digest is stored) and writes it with mode 0600;
- `send_turn` sends the turn text with the idempotency key `<execution>:<generation>:turn:<n>`;
- `observe` maps `run.observe(after_offset)` envelopes to frames keyed
  `cursor_local_key("<run_id>:<offset>")`, synthesizing the terminal frame from the run record
  when a stream ends without one;
- `end_session` stores the git patch (diff plus untracked files) and `outputs/`, closes the
  agent and the bridge, revokes the token, and releases the lease only after the patch is
  stored.

`reattach` re-launches the bridge over the same lease and `state_root` and resumes the agent
(emulated: a turn the bridge store no longer knows raises `NativeTurnLost`). Qualification
stays false until FT-G6 records a real run (`describe().qualified=False`).
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Protocol
from uuid import UUID

from mission_control.adapters.cursor.bridge import (
    AgentBusy,
    CursorBridgeLauncher,
    CursorLocalBridge,
    FeatureUnavailable,
    LocalAgentSpec,
    RunNotCancellable,
    RunNotFound,
    SandboxUnsupported,
)
from mission_control.adapters.cursor.frames import (
    closing_facts,
    final_lane_frame,
    local_lane_frame,
    offset_of,
)
from mission_control.adapters.cursor.hooks_callback import kernel_hook_script
from mission_control.adapters.cursor.projection import (
    HOOK_CONTEXT_PATH,
    KERNEL_HOOK_PATH,
    STATE_ROOT,
    TOKEN_PATH,
    UNSUPPORTED_BEHAVIOR,
    DurableInputReader,
    LaneProjectionError,
    ProjectionSource,
    materialize_packet,
    projection_digests,
    turn_text,
    verify_projection,
    write_files,
    write_secret,
)
from mission_control.adapters.cursor.workspace import GitWorktreeLeaser
from mission_control.application.execution.harness.describe import CURSOR_LOCAL_DESCRIBE
from mission_control.application.execution.harness.hook_callbacks import (
    HookCallbackService,
    HookTokenContext,
)
from mission_control.application.execution.harness.lane_turns import (
    LaneExecutionIdentity,
    execution_start,
)
from mission_control.application.execution.harness.leases import WorkspaceLease
from mission_control.application.execution.harness.protocol import NativeTurnLost
from mission_control.domain.agentic_components.projection import HostProjection, ProjectedFile
from mission_control.domain.authoring.canonical import sha256_digest
from mission_control.domain.execution.contracts import OperationExecutionRequest
from mission_control.domain.execution.lane_turns import ClosingFacts
from mission_control.domain.execution.lanes import (
    CancelReceipt,
    CancelTurnRequest,
    CleanupReceipt,
    CursorExecutionBinding,
    EndSessionRequest,
    HarnessRequest,
    LaneDescribe,
    LaneFrame,
    ObserveRequest,
    PreparedSession,
    PrepareRequest,
    ProviderStatus,
    ReattachRequest,
    SendTurnRequest,
    SessionHandle,
    SnapshotManifest,
    SnapshotRequest,
    StartRequest,
    StatusRequest,
    TurnHandle,
    UsageReport,
    UsageRequest,
)

PROFILE = "cursor_local"


class LaneArtifactSink(Protocol):
    """Stores a lane artifact's bytes and returns a digest-bound durable reference."""

    async def stage(
        self, *, request_scope: str, name: str, content: bytes, media_type: str
    ) -> str: ...


@dataclass(frozen=True)
class CursorLocalSettings:
    lease_root: Path
    callback_base_url: str = "http://127.0.0.1:47555"
    token_ttl: timedelta = timedelta(hours=4)
    mount_root: str = ""
    # A worker-local checkout used when the binding names no repository (Mission 3 binds one).
    default_repository: str | None = None


@dataclass
class _Session:
    operation: OperationExecutionRequest
    binding: CursorExecutionBinding
    identity: LaneExecutionIdentity
    lease: WorkspaceLease | None = None
    projection: HostProjection | None = None
    digests: dict[str, str] = field(default_factory=dict)
    bridge: CursorLocalBridge | None = None
    agent_id: str | None = None
    run_id: str | None = None
    token_context: HookTokenContext | None = None


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _callback_url(base: str, request_scope: str) -> str:
    application = request_scope.split("/")[2] if request_scope.startswith("mc/") else "local"
    return f"{base.rstrip('/')}/v1/applications/{application}/internal/hook-callback"


def _scope_block(request_scope: str) -> dict[str, str]:
    parts = request_scope.split("/")
    if len(parts) == 4 and parts[0] == "mc":
        return {"installation_id": parts[1], "application_id": parts[2], "tenant_id": parts[3]}
    return {"installation_id": request_scope, "application_id": "local"}


class CursorLocalHarness:
    """`cursor_local` Session Lane over a `CursorBridgeLauncher` (the pinned SDK in production,
    a replaying fake in tests)."""

    def __init__(
        self,
        *,
        launcher: CursorBridgeLauncher,
        leaser: GitWorktreeLeaser,
        projections: ProjectionSource,
        hooks: HookCallbackService,
        artifacts: LaneArtifactSink,
        settings: CursorLocalSettings,
        inputs: DurableInputReader | None = None,
        describe: LaneDescribe = CURSOR_LOCAL_DESCRIBE,
        clock: Callable[[], datetime] = _utc_now,
    ) -> None:
        if describe.lane_profile != PROFILE:
            raise ValueError("the Cursor local harness describes the cursor_local profile")
        self._launcher = launcher
        self._leaser = leaser
        self._projections = projections
        self._hooks = hooks
        self._artifacts = artifacts
        self._settings = settings
        self._inputs = inputs
        self._describe = describe
        self._clock = clock
        self._sessions: dict[str, _Session] = {}

    def describe(self) -> LaneDescribe:
        return self._describe

    # --- staging --------------------------------------------------------------------------------

    def stage(self, harness_execution_id: str, operation: OperationExecutionRequest) -> None:
        binding = operation.cursor_binding
        if binding is None or binding.lane_profile != PROFILE:
            raise ValueError("cursor_local runs only an operation with a cursor_local binding")
        current = self._sessions.get(harness_execution_id)
        if current is not None:
            if current.operation != operation:
                raise ValueError("harness execution is staged with another operation")
            return
        self._sessions[harness_execution_id] = _Session(
            operation=operation,
            binding=binding,
            identity=LaneExecutionIdentity.of(operation, PROFILE, 1),
        )

    def _session(self, request: HarnessRequest) -> _Session:
        session = self._sessions.get(request.harness_execution_id)
        if session is None:
            raise NativeTurnLost(f"harness execution {request.harness_execution_id} is not staged")
        if session.identity.attempt_no != request.generation:
            session.identity = LaneExecutionIdentity.of(
                session.operation, PROFILE, request.generation
            )
        return session

    # --- prepare ------------------------------------------------------------------------------

    async def prepare(self, request: PrepareRequest) -> PreparedSession:
        session = self._session(request)
        binding = session.binding
        if binding.sandbox_enabled and not self._launcher.sandbox_supported():
            raise LaneProjectionError(
                UNSUPPORTED_BEHAVIOR, "the Cursor sandbox is not available on this worker host"
            )
        lease = await self._leaser.acquire(
            request_scope=session.identity.request_scope,
            lane_profile=PROFILE,
            harness_execution_id=UUID(request.harness_execution_id),
            generation=request.generation,
            run_id=request.run_id,
            attempt_no=request.attempt_no,
            repository=binding.workspace.repo_url or self._settings.default_repository,
            base_ref=binding.workspace.base_ref,
        )
        session.lease = lease
        root = Path(lease.path)
        packet = await materialize_packet(
            root, session.operation, self._inputs, mount_root=self._settings.mount_root
        )
        # The pinned projection points at `.mission/context.md`; the packet is bound by its
        # own digests (recorded below), so the rule and hook digests stay the binding's.
        projection = await self._projections.project(
            session.operation, profile=PROFILE, packet_index=None
        )
        digests = projection_digests(projection)
        verify_projection(binding, digests)
        files = (
            *projection.files,
            ProjectedFile(path=KERNEL_HOOK_PATH, content=kernel_hook_script(), mode=0o755),
        )
        written = await asyncio.to_thread(write_files, root, files)
        session.projection = projection
        session.digests = {**digests, "packet_digest": sha256_digest(packet)}
        return PreparedSession(
            lane_profile=PROFILE,
            harness_execution_id=request.harness_execution_id,
            generation=request.generation,
            workspace_ref=f"workspace_lease:{lease.lease_id}",
            projection_digests=session.digests,
            materialization_ref=sha256_digest(sorted(written)),
        )

    def _spec(self, session: _Session, root: Path) -> LocalAgentSpec:
        binding = session.binding
        agents: tuple[dict[str, Any], ...] = ()
        if session.projection is not None:
            raw = session.projection.send_options.get("agents")
            items = raw if isinstance(raw, list | tuple) else ()
            agents = tuple(dict(item) for item in items if isinstance(item, dict))
        return LocalAgentSpec(
            model=binding.model_id,
            name=f"mc-{session.identity.run_key}-{session.identity.attempt_no}"[:100],
            cwd=str(root),
            mode=binding.mode,
            setting_sources=("project",),
            sandbox_enabled=binding.sandbox_enabled,
            agents=agents,
            disallowed_tools=binding.disallowed_tools,
        )

    # --- start, reattach ------------------------------------------------------------------------

    async def start(self, request: StartRequest) -> SessionHandle:
        session = self._session(request)
        if session.lease is None:
            raise ValueError("start requires a prepared workspace lease")
        root = Path(session.lease.path)
        state_root = root / STATE_ROOT
        bridge = await self._launcher.launch(workspace=root, state_root=state_root)
        session.bridge = bridge
        try:
            agent_id = await bridge.create_agent(self._spec(session, root))
        except SandboxUnsupported as error:
            raise LaneProjectionError(UNSUPPORTED_BEHAVIOR, str(error)) from error
        session.agent_id = agent_id
        await self._write_hook_context(session, request, agent_id)
        return self._handle(request, agent_id, state_root)

    def _handle(self, request: HarnessRequest, agent_id: str, state_root: Path) -> SessionHandle:
        versions = self._launcher.versions
        return SessionHandle(
            lane_profile=PROFILE,
            harness_execution_id=request.harness_execution_id,
            generation=request.generation,
            native_session_ref=agent_id,
            native_details={
                "cursor_sdk_version": versions.get("cursor_sdk", ""),
                "bridge_state_root": str(state_root),
            },
        )

    async def _write_hook_context(
        self, session: _Session, request: HarnessRequest, agent_id: str
    ) -> None:
        """Mint the task token once the native session exists (before any send, so before any
        hook can fire) and write it with the hook context into the lease."""

        assert session.lease is not None
        identity = session.identity
        root = Path(session.lease.path)
        context = HookTokenContext(
            request_scope=identity.request_scope,
            run_id=identity.run_key,
            attempt_no=identity.attempt_no,
            generation=request.generation,
            harness_execution_id=identity.harness_execution_id,
            lane_profile=PROFILE,
            execution_start=execution_start(
                session.operation, identity, request.generation, agent_id
            ),
            disallowed_tools=session.binding.disallowed_tools,
            workspace_root=str(root),
        )
        token = await self._hooks.issue(context, ttl=self._settings.token_ttl)
        session.token_context = context
        hook_context = {
            "schema_version": "mc.hook_context.v1",
            "lane_profile": PROFILE,
            "scope": _scope_block(identity.request_scope),
            "run_id": identity.run_key,
            "attempt_no": identity.attempt_no,
            "generation": request.generation,
            "harness_execution_id": str(identity.harness_execution_id),
            "workspace_root": str(root),
            "callback": {
                "url": _callback_url(self._settings.callback_base_url, identity.request_scope),
                "token_path": TOKEN_PATH,
            },
        }
        await asyncio.to_thread(write_secret, root, TOKEN_PATH, token)
        await asyncio.to_thread(
            write_files,
            root,
            (
                ProjectedFile(
                    path=HOOK_CONTEXT_PATH,
                    content=json.dumps(hook_context, sort_keys=True, indent=2).encode("utf-8"),
                ),
            ),
        )

    async def _bridge(self, session: _Session, request: HarnessRequest) -> CursorLocalBridge:
        """The live bridge, or a relaunch over the same lease and `state_root` (emulated
        reattach after a lost worker); a lease that no longer exists loses the turn."""

        if session.bridge is not None:
            return session.bridge
        lease = session.lease
        if lease is None:
            lease = await self._leaser.find(
                request_scope=session.identity.request_scope,
                lane_profile=PROFILE,
                harness_execution_id=UUID(request.harness_execution_id),
                generation=request.generation,
            )
        if lease is None or lease.released or not await asyncio.to_thread(Path(lease.path).is_dir):
            raise NativeTurnLost("the workspace lease of this session is gone")
        session.lease = lease
        root = Path(lease.path)
        session.bridge = await self._launcher.launch(workspace=root, state_root=root / STATE_ROOT)
        return session.bridge

    async def reattach(self, request: ReattachRequest) -> SessionHandle:
        session = self._session(request)
        if session.bridge is not None and session.agent_id == request.native_session_ref:
            assert session.lease is not None
            return self._handle(
                request, request.native_session_ref, Path(session.lease.path) / STATE_ROOT
            )
        bridge = await self._bridge(session, request)
        assert session.lease is not None
        root = Path(session.lease.path)
        if session.projection is None:
            session.projection = await self._projections.project(
                session.operation, profile=PROFILE, packet_index=None
            )
        try:
            # Tools, disallowed tools and inline MCP servers are not persisted across resume.
            agent_id = await bridge.resume_agent(
                request.native_session_ref, self._spec(session, root)
            )
            if request.native_turn_ref is not None:
                await bridge.run_state(request.native_turn_ref)
        except RunNotFound as error:
            raise NativeTurnLost("the bridge store no longer knows this session") from error
        session.agent_id = agent_id
        session.run_id = request.native_turn_ref
        return self._handle(request, agent_id, root / STATE_ROOT)

    # --- turn ---------------------------------------------------------------------------------

    async def send_turn(self, request: SendTurnRequest) -> TurnHandle:
        session = self._session(request)
        bridge = await self._bridge(session, request)
        agent_id = request.session.native_session_ref or session.agent_id
        if agent_id is None:
            raise ValueError("send_turn requires a started session")
        try:
            run_id = await bridge.send(
                agent_id,
                turn_text(session.operation),
                idempotency_key=request.idempotency_key,
            )
        except AgentBusy:
            return TurnHandle(session=request.session, turn_no=request.turn_no, status="busy")
        session.run_id = run_id
        return TurnHandle(session=request.session, turn_no=request.turn_no, native_turn_ref=run_id)

    async def observe(self, request: ObserveRequest) -> AsyncIterator[LaneFrame]:
        session = self._session(request)
        run_id = request.turn.native_turn_ref or session.run_id
        if run_id is None:
            raise NativeTurnLost("no native turn to observe")
        bridge = await self._bridge(session, request)
        cursor = request.after
        try:
            async for event in bridge.observe(run_id, after_offset=request.after):
                frame = local_lane_frame(
                    event,
                    run_id=run_id,
                    harness_execution_id=request.harness_execution_id,
                    generation=request.generation,
                )
                cursor = frame.cursor
                yield frame
                if frame.terminal:
                    return
            state = await bridge.run_state(run_id)
        except RunNotFound as error:
            raise NativeTurnLost("the bridge store no longer knows this run") from error
        if state.terminal:
            yield final_lane_frame(
                state,
                harness_execution_id=request.harness_execution_id,
                generation=request.generation,
                cursor=cursor,
            )

    def closing_facts(self, turn: TurnHandle, frame: LaneFrame) -> ClosingFacts:
        body = frame.body if isinstance(frame.body, dict) else {}
        return closing_facts(body)

    def resume_cursor(self, provider_key: str) -> str | None:
        return offset_of(provider_key)

    async def cancel_turn(self, request: CancelTurnRequest) -> CancelReceipt:
        session = self._session(request)
        run_id = request.turn.native_turn_ref or session.run_id
        agent_id = request.turn.session.native_session_ref or session.agent_id
        if run_id is None or agent_id is None:
            return CancelReceipt(acknowledged=True, already_terminal=True, native_status="idle")
        try:
            bridge = await self._bridge(session, request)
        except NativeTurnLost:
            return CancelReceipt(acknowledged=False, native_status="unknown")
        already = False
        try:
            await bridge.cancel(run_id, agent_id=agent_id)
        except RunNotCancellable:
            already = True
        except RunNotFound:
            return CancelReceipt(acknowledged=False, native_status="unknown")
        try:
            state = await bridge.run_state(run_id)
        except RunNotFound:
            return CancelReceipt(acknowledged=True, already_terminal=already)
        return CancelReceipt(
            acknowledged=True, already_terminal=already, native_status=state.status
        )

    async def status(self, request: StatusRequest) -> ProviderStatus:
        session = self._session(request)
        run_id = (request.turn.native_turn_ref if request.turn else None) or session.run_id
        if run_id is None:
            return ProviderStatus(status="idle", terminal=False, idle=True)
        bridge = await self._bridge(session, request)
        try:
            state = await bridge.run_state(run_id)
        except RunNotFound as error:
            raise NativeTurnLost("the bridge store no longer knows this run") from error
        usage = (
            UsageReport(
                disposition="estimated",
                input_tokens=state.usage.get("input_tokens", 0),
                output_tokens=state.usage.get("output_tokens", 0),
                total_tokens=state.usage.get("total_tokens", 0),
            )
            if state.usage
            else None
        )
        return ProviderStatus(
            status=state.status, terminal=state.terminal, idle=state.terminal, usage=usage
        )

    async def usage(self, request: UsageRequest) -> UsageReport:
        """Tokens settle per turn; cost upgrades only when the provider reports it
        (`feature_unavailable` raises, and the closing usage stays `estimated`)."""

        session = self._session(request)
        agent_id = request.session.native_session_ref or session.agent_id
        if agent_id is None:
            return UsageReport(disposition="unknown")
        bridge = await self._bridge(session, request)
        try:
            usage = await bridge.usage(agent_id)
        except FeatureUnavailable:
            raise
        return UsageReport(
            disposition="settled",
            input_tokens=usage.input_tokens,
            output_tokens=usage.output_tokens,
            total_tokens=usage.total_tokens,
            cost_micros_usd=usage.cost_micros_usd,
        )

    # --- snapshot, end ------------------------------------------------------------------------

    def _excluded(self, session: _Session) -> tuple[str, ...]:
        projected = session.projection.paths() if session.projection is not None else ()
        return tuple(projected)

    async def _store_patch(self, session: _Session, request: HarnessRequest) -> str:
        assert session.lease is not None
        patch = await self._leaser.capture_patch(session.lease, exclude=self._excluded(session))
        manifest = json.dumps(
            {"base_commit": session.lease.base_commit, "untracked": list(patch.untracked)},
            sort_keys=True,
        ).encode("utf-8")
        name = f"cursor-local/{request.harness_execution_id}/{request.generation}"
        await self._artifacts.stage(
            request_scope=session.identity.request_scope,
            name=f"{name}/patch-manifest.json",
            content=manifest,
            media_type="application/json",
        )
        return await self._artifacts.stage(
            request_scope=session.identity.request_scope,
            name=f"{name}/patch.diff",
            content=patch.diff,
            media_type="text/x-diff",
        )

    async def snapshot(self, request: SnapshotRequest) -> SnapshotManifest:
        session = self._session(request)
        if session.lease is None:
            raise ValueError("no leased workspace to snapshot")
        ref = await self._store_patch(session, request)
        refs = (ref, *(f"{path}={digest}" for path, digest in sorted(session.digests.items())))
        return SnapshotManifest(
            lane_profile=PROFILE,
            harness_execution_id=request.harness_execution_id,
            generation=request.generation,
            kind="git_patch",
            refs=refs,
            emulated=True,
            digest=sha256_digest(list(refs)),
        )

    async def end_session(self, request: EndSessionRequest) -> CleanupReceipt:
        session = self._session(request)
        if session.lease is None:
            session.lease = await self._leaser.find(
                request_scope=session.identity.request_scope,
                lane_profile=PROFILE,
                harness_execution_id=UUID(request.harness_execution_id),
                generation=request.generation,
            )
        if session.lease is None or session.lease.released:
            self._sessions.pop(request.harness_execution_id, None)
            return CleanupReceipt(released=False)
        patch_ref = await self._store_patch(session, request)
        outputs: list[str] = []
        for path, content in await self._leaser.outputs(session.lease):
            outputs.append(
                await self._artifacts.stage(
                    request_scope=session.identity.request_scope,
                    name=f"cursor-local/{request.harness_execution_id}/{path}",
                    content=content,
                    media_type="application/octet-stream",
                )
            )
        if session.bridge is not None:
            if session.agent_id is not None:
                try:
                    await session.bridge.close_agent(session.agent_id)
                finally:
                    await session.bridge.aclose()
            else:
                await session.bridge.aclose()
            session.bridge = None
        if session.token_context is not None:
            await self._hooks.revoke(session.token_context)
        # The lease is released only after the patch is stored (SPEC-07 section 5.3).
        await self._leaser.release(session.lease, patch_artifact_ref=patch_ref)
        self._sessions.pop(request.harness_execution_id, None)
        return CleanupReceipt(
            released=True, artifact_refs=(patch_ref, *outputs), patch_ref=patch_ref
        )


__all__ = [
    "PROFILE",
    "CursorLocalHarness",
    "CursorLocalSettings",
    "LaneArtifactSink",
]
