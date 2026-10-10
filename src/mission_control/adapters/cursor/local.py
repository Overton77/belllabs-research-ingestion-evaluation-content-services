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
(emulated: a turn the bridge store no longer knows raises `NativeTurnLost`), re-supplying the
options `Agent.resume` does not persist (tools, disallowed tools, inline MCP servers and
subagents: `cursor_sdk` 1.0.37 `_async_client.resume_agent(agent_id, options)`) and recording
what it re-applied in the handle (MP-09 option rehydration). Qualification stays false until
FT-G6 / OVE-55 records a real run (`describe().qualified=False`).

MP-09 host gate: the bridge is a subprocess, and the production worker pins a
SelectorEventLoop on Windows, so `prepare` and `start` consult the launcher's `host_gate()`
and refuse `LANE_UNSUPPORTED_OS` before leasing or spawning anything; the supported path is a
WSL 2 or Linux worker (docs/qualification/lanes/cursor_local/README.md).

MP-09 `reconcile_dispatch` (MP-06 `DispatchReconcilingLane`): the local SDK store exposes no
lookup by idempotency key (`send(idempotency_key=...)` goes to the bridge and nothing reads it
back), so a journaled send is `found` only when this process still holds the run it produced
(a receipt lost between the bridge's answer and the journal write); after a process death
the agent loop died with it and the lane answers `unknown`, which parks the unit `in_doubt`
rather than guessing. A journaled create is `found` only from the same memory.

FT-G4 controls (SPEC-07 section 7): cursors are turn-qualified (`<run_id>@<offset>`, so a
replacement or continuation turn never resumes from another run's offset); a later turn's text
is staged by its instruction ref (`stage_turn`: the injected item of a `cancel_and_replace`
replacement, a continuation's hydration prompt); `snapshot` freezes `mc.cursor_snapshot.v1`
(patch, untracked files, packet files with digests, native refs); `prepare` restores the
packet's `workspace` item (a fork) into the fresh lease before anything else is placed; the
`stop` hook asks once for missing declared outputs (`missing_output_policy` follow-up) and a
finished session that still lacks them closes with `missing_outputs`; a continuation hydrates
a fresh agent in the same lease (`CursorSessionHydrator`, `adapters/cursor/controls.py`).
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator, Callable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Protocol
from uuid import UUID

from mission_control.adapters.cursor import snapshot as snapshots
from mission_control.adapters.cursor.bridge import (
    LANE_UNSUPPORTED_OS,
    AgentBusy,
    CursorBridgeLauncher,
    CursorLocalBridge,
    FeatureUnavailable,
    HostGate,
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
    run_of,
)
from mission_control.adapters.cursor.frames import first as first
from mission_control.adapters.cursor.hooks_callback import kernel_hook_script
from mission_control.adapters.cursor.projection import (
    HOOK_CONTEXT_PATH,
    KERNEL_HOOK_PATH,
    OUTPUTS_DIR,
    STATE_ROOT,
    TOKEN_PATH,
    UNSUPPORTED_BEHAVIOR,
    DurableInputReader,
    LaneProjectionError,
    ProjectionSource,
    materialize_packet,
    packet_files,
    projection_digests,
    turn_text,
    verify_projection,
    write_files,
    write_secret,
)
from mission_control.adapters.cursor.workspace import GitWorktreeLeaser
from mission_control.application.execution.harness.controls import SessionHandover
from mission_control.application.execution.harness.describe import CURSOR_LOCAL_DESCRIBE
from mission_control.application.execution.harness.dispatch import DispatchLookup, DispatchRecord
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
from mission_control.application.execution.operations.lane_outputs import LaneOutputCustody
from mission_control.domain.agentic_components.projection import HostProjection, ProjectedFile
from mission_control.domain.authoring.canonical import sha256_digest
from mission_control.domain.context.render import INPUTS_MANIFEST_PATH
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
    # FT-G4 `missing_output_policy`: one follow-up asking for the declared outputs (SPEC-07
    # user story 26); False closes a session without them `not_accepted` at once.
    missing_output_follow_up: bool = True


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
    # FT-G4: staged texts of later turns (by instruction ref) and a hydrated continuation.
    turn_texts: dict[str, str] = field(default_factory=dict)
    handover: SessionHandover | None = None
    restored_snapshot: str | None = None
    # MP-09: the run each dispatch key produced in this process (the only reconcile anchor
    # the local SDK offers) and what the last reattach re-applied.
    sent_keys: dict[str, str] = field(default_factory=dict)
    rehydrated: tuple[str, ...] = ()


def _utc_now() -> datetime:
    return datetime.now(UTC)


FOLLOW_UP_TEXT = (
    "Before you stop: the mission declares outputs that are not written yet: {missing}. "
    "Write each declared output under outputs/ as described in .mission/context.md, "
    "then stop. Do not start other work."
)
TURNS_DIR = ".mission/turns"


def declared_outputs(
    operation: OperationExecutionRequest, *, mount_root: str = ""
) -> tuple[str, ...]:
    """The operation's declared outputs: its writable paths under `outputs/` (relative)."""

    declared: list[str] = []
    prefix = mount_root.rstrip("/")
    for path in operation.workspace.exclusive_write_paths:
        logical = path
        if prefix and logical.startswith(prefix + "/"):
            logical = logical[len(prefix) :]
        relative = logical.lstrip("/")
        if relative.startswith(OUTPUTS_DIR + "/"):
            declared.append(relative)
    return tuple(dict.fromkeys(declared))


def missing_outputs(root: Path, declared: tuple[str, ...]) -> tuple[str, ...]:
    return tuple(path for path in declared if not (root / path).is_file())


def composite_cursor(run_id: str, offset: str) -> str:
    return f"{run_id}@{offset}"


def split_cursor(cursor: str | None, run_id: str) -> str | None:
    """The bridge offset a cursor names for `run_id`; another run's cursor resumes nothing."""

    if cursor is None:
        return None
    run, separator, offset = cursor.rpartition("@")
    if not separator:
        return cursor
    return offset if run == run_id and offset else None


def _callback_url(base: str, request_scope: str) -> str:
    application = request_scope.split("/")[2] if request_scope.startswith("mc/") else "local"
    return f"{base.rstrip('/')}/v1/applications/{application}/internal/hook-callback"


def _scope_block(request_scope: str) -> dict[str, str]:
    parts = request_scope.split("/")
    if len(parts) == 4 and parts[0] == "mc":
        return {"installation_id": parts[1], "application_id": parts[2], "tenant_id": parts[3]}
    return {"installation_id": request_scope, "application_id": "local"}


class _StageOnly:
    """A sink without reads: snapshots are stored but cannot be restored from it."""

    def __init__(self, sink: LaneArtifactSink) -> None:
        self._sink = sink

    async def stage(self, *, request_scope: str, name: str, content: bytes, media_type: str) -> str:
        return await self._sink.stage(
            request_scope=request_scope, name=name, content=content, media_type=media_type
        )

    async def retrieve(self, durable_ref: str) -> bytes:
        raise LookupError(durable_ref)


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
        snapshot_store: snapshots.SnapshotArtifacts | None = None,
        outputs: LaneOutputCustody | None = None,
    ) -> None:
        if describe.lane_profile != PROFILE:
            raise ValueError("the Cursor local harness describes the cursor_local profile")
        self._launcher = launcher
        # MP-20: declared outputs become workspace candidates of the operation when composed.
        self._outputs = outputs
        self._leaser = leaser
        self._projections = projections
        self._hooks = hooks
        self._artifacts = artifacts
        self._settings = settings
        self._inputs = inputs
        self._describe = describe
        self._clock = clock
        self._sessions: dict[str, _Session] = {}
        # FT-G4: where snapshot bytes are staged and read back (fork, continuation).
        store = snapshot_store
        if store is None and hasattr(artifacts, "retrieve"):
            store = artifacts  # type: ignore[assignment]
        self._snapshots: snapshots.SnapshotArtifacts | None = store

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

    def host_gate(self) -> HostGate | None:
        """The launcher's host gate (None for a launcher that spawns nothing, e.g. a replay)."""

        gate = getattr(self._launcher, "host_gate", None)
        return gate() if callable(gate) else None

    def _refuse_unsupported_host(self) -> None:
        gate = self.host_gate()
        if gate is not None and not gate.supported:
            raise LaneProjectionError(gate.code or LANE_UNSUPPORTED_OS, gate.message)

    async def prepare(self, request: PrepareRequest) -> PreparedSession:
        session = self._session(request)
        binding = session.binding
        # Refused before any lease exists: a Windows worker cannot spawn the bridge.
        self._refuse_unsupported_host()
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
        # A fork's (or continuation's) packet carries one `workspace` item: restore it into
        # the fresh lease first; the new packet and projection are placed over it.
        await self._restore_workspace_item(session, root)
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

    async def _restore_workspace_item(self, session: _Session, root: Path) -> None:
        files = dict(
            await packet_files(
                session.operation, self._inputs, mount_root=self._settings.mount_root
            )
        )
        raw = files.get(INPUTS_MANIFEST_PATH)
        if raw is None:
            return
        try:
            workspace = json.loads(raw.decode("utf-8")).get("workspace")
        except (ValueError, AttributeError):
            return
        if not isinstance(workspace, dict) or not workspace.get("snapshot_ref"):
            return
        ref = str(workspace["snapshot_ref"])
        if session.restored_snapshot == ref:
            return
        if self._snapshots is None:
            raise LaneProjectionError(
                snapshots.CHECKPOINT_INVALID, "no snapshot store is composed to restore " + ref
            )
        manifest = await snapshots.load(ref, self._snapshots)
        restored = await snapshots.restore(
            manifest,
            root,
            self._snapshots,
            restore_paths=tuple(workspace.get("restore_paths") or ("/",)),
        )
        expected = {
            path: digest
            for path, digest in manifest.workspace_manifest().items()
            if path in restored or not path.startswith("/.mission/")
        }
        if any(restored.get(path) != digest for path, digest in expected.items()):
            raise LaneProjectionError(
                snapshots.CHECKPOINT_INVALID, "restored files differ from the snapshot digests"
            )
        session.restored_snapshot = ref

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
        # Enforced where the lane spawns (the launcher refuses as well).
        self._refuse_unsupported_host()
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
        details = {
            "cursor_sdk_version": versions.get("cursor_sdk", ""),
            "bridge_state_root": str(state_root),
        }
        session = self._sessions.get(request.harness_execution_id)
        if session is not None and session.rehydrated:
            details["rehydrated"] = ",".join(session.rehydrated)
        return SessionHandle(
            lane_profile=PROFILE,
            harness_execution_id=request.harness_execution_id,
            generation=request.generation,
            native_session_ref=agent_id,
            native_details=details,
        )

    def rehydration_receipt(self, harness_execution_id: str) -> tuple[str, ...]:
        """Which non-persisted options the last reattach re-applied (empty when nothing was)."""

        session = self._sessions.get(harness_execution_id)
        return () if session is None else session.rehydrated

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
        rehydrated = ["bridge"]
        if session.projection is None:
            session.projection = await self._projections.project(
                session.operation, profile=PROFILE, packet_index=None
            )
            rehydrated.append("projection")
        spec = self._spec(session, root)
        try:
            # Tools, disallowed tools, inline MCP servers and subagents are not persisted
            # across `Agent.resume`: the pinned spec is re-supplied on every resume
            # (cursor_sdk 1.0.37 `_async_client.AsyncClient.resume_agent(agent_id, options)`).
            agent_id = await bridge.resume_agent(request.native_session_ref, spec)
            if request.native_turn_ref is not None:
                await bridge.run_state(request.native_turn_ref)
        except RunNotFound as error:
            raise NativeTurnLost("the bridge store no longer knows this session") from error
        rehydrated.extend(_rehydrated_options(spec))
        session.rehydrated = tuple(dict.fromkeys((*session.rehydrated, *rehydrated)))
        session.agent_id = agent_id
        session.run_id = request.native_turn_ref
        return self._handle(request, agent_id, root / STATE_ROOT)

    # --- MP-06: reconcile an ambiguous create/send ------------------------------------------------

    async def reconcile_dispatch(
        self, record: DispatchRecord, *, session: SessionHandle | None
    ) -> DispatchLookup:
        """`found` only from what this process still holds; the local SDK offers no
        idempotency read-back, and a process death kills the agent loop with the run."""

        harness_execution_id = record.idempotency_key.split(":", 1)[0]
        staged = self._sessions.get(harness_execution_id)
        if record.kind == "create":
            if staged is not None and staged.agent_id is not None and staged.bridge is not None:
                return DispatchLookup(outcome="found", native_ref=staged.agent_id)
            return DispatchLookup(
                outcome="unknown",
                detail="the local bridge store exposes no lookup of a create by key",
            )
        if staged is not None:
            remembered = staged.sent_keys.get(record.idempotency_key)
            if remembered is not None:
                return DispatchLookup(outcome="found", native_ref=remembered)
        return DispatchLookup(
            outcome="unknown",
            detail="the local SDK store exposes no lookup of a send by idempotency key; "
            "a process death ended the agent loop with it",
        )

    # --- turn ---------------------------------------------------------------------------------

    async def send_turn(self, request: SendTurnRequest) -> TurnHandle:
        session = self._session(request)
        bridge = await self._bridge(session, request)
        agent_id = request.session.native_session_ref or session.agent_id
        if agent_id is None:
            raise ValueError("send_turn requires a started session")
        text = session.turn_texts.get(request.instruction_ref) or turn_text(session.operation)
        try:
            run_id = await bridge.send(
                agent_id,
                text,
                idempotency_key=request.idempotency_key,
            )
        except AgentBusy:
            return TurnHandle(session=request.session, turn_no=request.turn_no, status="busy")
        session.run_id = run_id
        session.sent_keys[request.idempotency_key] = run_id
        return TurnHandle(session=request.session, turn_no=request.turn_no, native_turn_ref=run_id)

    def stage_turn(self, harness_execution_id: str, instruction_ref: str, text: str) -> None:
        """The text a later turn sends (FT-G4): kept for the send and written under
        `.mission/turns/` so the agent and the transcript see what was injected."""

        session = self._sessions.get(harness_execution_id)
        if session is None:
            raise NativeTurnLost(f"harness execution {harness_execution_id} is not staged")
        session.turn_texts[instruction_ref] = text
        if session.lease is not None:
            name = sha256_digest(instruction_ref).removeprefix("sha256:")[:16]
            write_files(
                Path(session.lease.path),
                (
                    ProjectedFile(
                        path=f"{TURNS_DIR}/{name}.md", content=text.encode("utf-8"), mode=0o444
                    ),
                ),
            )

    async def observe(self, request: ObserveRequest) -> AsyncIterator[LaneFrame]:
        session = self._session(request)
        run_id = request.turn.native_turn_ref or session.run_id
        if run_id is None:
            raise NativeTurnLost("no native turn to observe")
        bridge = await self._bridge(session, request)
        after = split_cursor(request.after, run_id)
        cursor = request.after if after is not None else None
        try:
            async for event in bridge.observe(run_id, after_offset=after):
                frame = local_lane_frame(
                    event,
                    run_id=run_id,
                    harness_execution_id=request.harness_execution_id,
                    generation=request.generation,
                )
                frame = frame.model_copy(update={"cursor": composite_cursor(run_id, frame.cursor)})
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
        facts = closing_facts(body)
        if facts.native_status != "finished":
            return facts
        session = self._sessions.get(turn.session.harness_execution_id)
        if session is None or session.lease is None:
            return facts
        # A native `finished` is never acceptance: declared outputs must be on disk.
        declared = declared_outputs(session.operation, mount_root=self._settings.mount_root)
        missing = missing_outputs(Path(session.lease.path), declared)
        return facts.model_copy(update={"missing_outputs": missing}) if missing else facts

    def final_text(self, turn: TurnHandle, frame: LaneFrame) -> str | None:
        """`FinalTextLane` (MP-20): the run's whole final text from the terminal frame body
        (the closing facts keep only a bounded excerpt of it)."""

        del turn
        body = frame.body if isinstance(frame.body, Mapping) else {}
        value = first(body, "result", "text")
        return value if isinstance(value, str) else None

    def resume_cursor(self, provider_key: str) -> str | None:
        offset = offset_of(provider_key)
        run = run_of(provider_key)
        if offset is None or run is None:
            return None
        return composite_cursor(run, offset)

    # --- FT-G4: missing_output_policy follow-up (Cursor `stop` hook) ---------------------------

    async def stop_followup(
        self, context: HookTokenContext, payload: Mapping[str, Any]
    ) -> str | None:
        """The follow-up a stopping agent gets when declared outputs are missing."""

        del payload
        if not self._settings.missing_output_follow_up:
            return None
        session = self._sessions.get(str(context.harness_execution_id))
        if session is None or context.workspace_root is None:
            return None
        declared = declared_outputs(session.operation, mount_root=self._settings.mount_root)
        missing = await asyncio.to_thread(missing_outputs, Path(context.workspace_root), declared)
        if not missing:
            return None
        return FOLLOW_UP_TEXT.format(missing=", ".join(missing))

    # --- FT-G4: continuation handover (a fresh agent in the same lease) -------------------------

    async def pending_handover(self, harness_execution_id: str) -> SessionHandover | None:
        session = self._sessions.get(harness_execution_id)
        return None if session is None else session.handover

    async def complete_handover(self, harness_execution_id: str, transfer_id: str) -> None:
        session = self._sessions.get(harness_execution_id)
        if session is not None and session.handover is not None:
            if session.handover.transfer_id == transfer_id:
                session.agent_id = session.handover.session.native_session_ref
                session.handover = None

    def live_session(self, agent_id: str) -> str | None:
        """The staged harness execution whose live agent is `agent_id` (a continuation's
        source), when its lease is on this worker."""

        for harness_execution_id, session in self._sessions.items():
            if session.agent_id == agent_id and session.lease is not None:
                return harness_execution_id
        return None

    def lease_path(self, harness_execution_id: str) -> str:
        lease = self._sessions[harness_execution_id].lease
        if lease is None:
            raise NativeTurnLost("the session holds no workspace lease")
        return lease.path

    async def open_bridge(self, harness_execution_id: str) -> CursorLocalBridge:
        session = self._sessions[harness_execution_id]
        if session.bridge is None:
            root = Path(self.lease_path(harness_execution_id))
            session.bridge = await self._launcher.launch(
                workspace=root, state_root=root / STATE_ROOT
            )
        return session.bridge

    def agent_spec(self, harness_execution_id: str) -> LocalAgentSpec:
        """The pinned agent options (tools are re-supplied on every create and resume)."""

        return self._spec(
            self._sessions[harness_execution_id], Path(self.lease_path(harness_execution_id))
        )

    def session_handle(self, harness_execution_id: str, agent_id: str) -> SessionHandle:
        session = self._sessions[harness_execution_id]
        root = Path(self.lease_path(harness_execution_id))
        return SessionHandle(
            lane_profile=PROFILE,
            harness_execution_id=harness_execution_id,
            generation=session.identity.attempt_no,
            native_session_ref=agent_id,
            native_details={
                "cursor_sdk_version": self._launcher.versions.get("cursor_sdk", ""),
                "bridge_state_root": str(root / STATE_ROOT),
            },
        )

    def offer_handover(self, harness_execution_id: str, handover: SessionHandover) -> None:
        """A hydrated continuation: the next turn of this execution goes to its agent."""

        self._sessions[harness_execution_id].handover = handover

    @property
    def snapshot_store(self) -> snapshots.SnapshotArtifacts | None:
        return self._snapshots

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

    async def _freeze(
        self, session: _Session, request: HarnessRequest, reason: str
    ) -> snapshots.FrozenSnapshot:
        """`mc.cursor_snapshot.v1` of the lease: patch, untracked files, packet files, refs."""

        assert session.lease is not None
        lease = session.lease
        patch = await self._leaser.capture_patch(lease, exclude=self._excluded(session))
        store = self._snapshots or _StageOnly(self._artifacts)
        return await snapshots.freeze(
            root=Path(lease.path),
            patch=patch,
            artifacts=store,
            request_scope=session.identity.request_scope,
            name=f"cursor-local/{request.harness_execution_id}/{request.generation}",
            harness_execution_id=request.harness_execution_id,
            generation=request.generation,
            reason=reason,
            base_commit=lease.base_commit,
            base_ref=lease.base_ref,
            repository=lease.repository,
            native={"agent_id": session.agent_id or "", "run_id": session.run_id or ""},
        )

    async def snapshot(self, request: SnapshotRequest) -> SnapshotManifest:
        """Emulated: a frozen `mc.cursor_snapshot.v1` named by `cursor-snapshot:<ref>`."""

        session = self._session(request)
        if session.lease is None:
            raise ValueError("no leased workspace to snapshot")
        frozen = await self._freeze(session, request, request.reason)
        refs = snapshots.manifest_refs(frozen)
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
        frozen = await self._freeze(session, request, f"end_session:{request.reason}")
        patch_ref = frozen.manifest.patch.ref
        outputs: list[str] = [frozen.snapshot_ref]
        for path, content in await self._leaser.outputs(session.lease):
            registered = (
                await self._outputs.register(
                    session.operation, path, content, mount_root=self._settings.mount_root
                )
                if self._outputs is not None
                else None
            )
            outputs.append(
                registered
                or await self._artifacts.stage(
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
        await self._leaser.release(
            session.lease, patch_artifact_ref=patch_ref, snapshot_ref=frozen.snapshot_ref
        )
        self._sessions.pop(request.harness_execution_id, None)
        return CleanupReceipt(
            released=True, artifact_refs=(patch_ref, *outputs), patch_ref=patch_ref
        )


def _rehydrated_options(spec: LocalAgentSpec) -> tuple[str, ...]:
    """The non-persisted option groups a resume re-supplied (names only, never values)."""

    applied = ["setting_sources", "mode", "sandbox"]
    if spec.agents:
        applied.append("agents")
    if spec.disallowed_tools:
        applied.append("disallowed_tools")
    if spec.mcp_servers:
        applied.append("mcp_servers")
    return tuple(applied)


__all__ = [
    "FOLLOW_UP_TEXT",
    "PROFILE",
    "CursorLocalHarness",
    "CursorLocalSettings",
    "LaneArtifactSink",
    "composite_cursor",
    "declared_outputs",
    "missing_outputs",
    "split_cursor",
]
