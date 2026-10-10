"""MP-07 FIXTURES: a scripted Claude Agent SDK client, a tmp-dir workspace port, a claude
operation with a sealed `mc.execution_binding.v2`, and the lane stack around them.

FIXTURES ONLY. `FixtureClient` stands in for `claude_agent_sdk.client.ClaudeSDKClient`: it
replays the wire-shaped records under `tests/fixtures/provider_frames/claude/` through the
SDK's own `parse_message`, simulates the CLI's hook and permission control requests against
the options the lane built, can hold a turn, honour `interrupt()` by draining the scripted
`after_interrupt` records, and die like a subprocess. It proves what the lane does with the
pinned SDK's documented shapes, never live provider behaviour. No Claude Code is launched.
"""

from __future__ import annotations

import asyncio
import json
import re
from collections.abc import AsyncIterable, AsyncIterator, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import UUID

from claude_agent_sdk._errors import CLIConnectionError, ProcessError
from claude_agent_sdk._internal.message_parser import parse_message
from claude_agent_sdk.types import (
    ClaudeAgentOptions,
    Message,
    PermissionResult,
    PermissionResultAllow,
    ToolPermissionContext,
)

from mission_control.adapters.claude.describe import CLAUDE_AGENT_SDK_LANE_DESCRIBE
from mission_control.adapters.claude.harness import (
    ClaudeAgentSdkHarness,
    ClaudeLaneSettings,
    StaticAuthAdmitter,
)
from mission_control.adapters.claude.permissions import PermissionBindingPort
from mission_control.adapters.cursor.projection import (
    RenderedProjectionSource,
    operating_contract,
    static_rows,
)
from mission_control.application.agentic_components.materialization import projection_digest
from mission_control.application.agentic_components.projections import render_host_files
from mission_control.application.execution.auth_admission import AuthAdmission
from mission_control.application.execution.harness.hook_callbacks import InMemoryHookIntentLedger
from mission_control.application.execution.harness.leases import WorkspaceLease, lease_identity
from mission_control.application.execution.stop_fence import InMemoryStopFenceRepository
from mission_control.application.frames.sink import InMemoryFrameStore
from mission_control.application.workspaces.service import AllocationRequest
from mission_control.application.workspaces.snapshots import CapturedSnapshot
from mission_control.bootstrap.provider_auth import provider_child_environment
from mission_control.domain.agentic_components.projection import ResolvedCapability
from mission_control.domain.authoring.canonical import sha256_digest
from mission_control.domain.execution.bindings import ProviderExecutionBinding, WorkspaceSnapshot
from mission_control.domain.execution.contracts import OperationExecutionRequest
from tests.fixtures.lane_turns import DIGEST, LANE_QUEUE, SCOPE, LaneStack, lane_stack

FIXTURES = Path(__file__).resolve().parents[2] / "fixtures" / "provider_frames" / "claude"
PROFILE = "claude_agent_sdk"
SESSION_ID = "sess-fixture-claude-0001"
FIXTURE_ENVIRON: dict[str, str] = {
    "PATH": "/usr/bin",
    "HOME": "/home/mc",
    "ANTHROPIC_API_KEY": "sk-ant-FIXTURE-0000",
    "ANTHROPIC_AUTH_TOKEN": "FIXTURE-TOKEN",
    "OPENAI_API_KEY": "sk-FIXTURE-openai",
    "CURSOR_API_KEY": "key_FIXTURE_cursor",
}
_END = object()


# --- the scripted SDK client ------------------------------------------------------------------


@dataclass(frozen=True)
class ScriptRecord:
    turn: int | None
    session: bool = False
    message: dict[str, Any] | None = None
    hold: bool = False
    after_interrupt: bool = False
    hook: dict[str, Any] | None = None
    permission: dict[str, Any] | None = None
    allowed: tuple[dict[str, Any], ...] = ()
    denied: tuple[dict[str, Any], ...] = ()
    die: str | None = None


@dataclass
class FixtureScript:
    name: str
    marker: dict[str, Any]
    records: list[ScriptRecord]

    @classmethod
    def load(cls, name: str) -> FixtureScript:
        lines = (FIXTURES / f"{name}.jsonl").read_text(encoding="utf-8").splitlines()
        marker = json.loads(lines[0])
        assert marker.get("recorded") is False and "FIXTURE" in marker.get("note", "")
        records = []
        for line in lines[1:]:
            if not line.strip():
                continue
            raw = json.loads(line)
            records.append(
                ScriptRecord(
                    turn=raw.get("turn"),
                    session=bool(raw.get("session")),
                    message=raw.get("message"),
                    hold=bool(raw.get("hold")),
                    after_interrupt=bool(raw.get("after_interrupt")),
                    hook=raw.get("_hook"),
                    permission=raw.get("_permission"),
                    allowed=tuple(raw.get("allowed", ())),
                    denied=tuple(raw.get("denied", ())),
                    die=raw.get("die"),
                )
            )
        return cls(name=name, marker=marker, records=records)

    @property
    def session_records(self) -> list[ScriptRecord]:
        return [record for record in self.records if record.session]

    def turn_records(self, turn: int) -> list[ScriptRecord]:
        return [record for record in self.records if record.turn == turn]

    @property
    def session_id(self) -> str:
        for record in self.session_records:
            if record.message and record.message.get("subtype") == "init":
                return str(record.message["session_id"])
        return SESSION_ID


class FixtureClient:
    """FIXTURE `ClaudeClient`: replays one script; never spawns anything."""

    def __init__(
        self,
        script: FixtureScript,
        options: ClaudeAgentOptions,
        environment: Mapping[str, str],
        *,
        refuse_connect: bool = False,
        context_usage: Mapping[str, Any] | BaseException | None = None,
    ) -> None:
        self.script = script
        self.options = options
        self.environment = dict(environment)
        self.refuse_connect = refuse_connect
        self.context_usage = context_usage
        self.context_usage_calls = 0
        self.permission_results: list[PermissionResult] = []
        self.queue: asyncio.Queue[Any] = asyncio.Queue()
        self.sent: list[tuple[str | None, str]] = []
        self.interrupts = 0
        self.turns_played = 0
        self.interrupted = False
        self.connected = False
        self.disconnected = False
        self.held = asyncio.Event()
        self.release = asyncio.Event()
        self.hook_calls: list[tuple[str, str | None, bool]] = []
        self.permission_calls: list[tuple[str, bool]] = []
        self._player: asyncio.Task[None] | None = None

    async def connect(self, prompt: str | AsyncIterable[dict[str, Any]] | None = None) -> None:
        del prompt
        if self.refuse_connect:
            raise CLIConnectionError("Failed to start Claude Code: fixture refused")
        self.connected = True
        for record in self.script.session_records:
            if record.message is not None:
                self.queue.put_nowait(record.message)
        store = self.options.session_store
        if store is not None:
            # The SDK's transcript mirror: entries are opaque JSONL lines (types.SessionStore).
            await store.append(
                {"project_key": "fixture", "session_id": self.script.session_id},
                [{"type": "summary", "uuid": "mirror-init", "summary": "fixture session"}],
            )

    async def receive_messages(self) -> AsyncIterator[Message]:
        while True:
            item = await self.queue.get()
            if item is _END:
                return
            if isinstance(item, BaseException):
                raise item
            parsed = parse_message(item)
            if parsed is not None:
                yield parsed

    async def query(
        self, prompt: str | AsyncIterable[dict[str, Any]], session_id: str = "default"
    ) -> None:
        del session_id
        if isinstance(prompt, str):
            uuid, text = None, prompt
        else:
            messages = [message async for message in prompt]
            uuid = messages[0].get("uuid")
            text = str(messages[0]["message"]["content"])
        self.sent.append((uuid, text))
        self.turns_played += 1
        self.interrupted = False
        self.held.clear()
        self.release.clear()
        self._player = asyncio.create_task(self._play(self.script.turn_records(self.turns_played)))

    async def _play(self, records: list[ScriptRecord]) -> None:
        for record in records:
            if record.after_interrupt:
                continue
            if record.hold:
                self.held.set()
                await self.release.wait()
                self.release.clear()
            if self.interrupted:
                break
            if record.die is not None:
                self.queue.put_nowait(ProcessError(record.die, exit_code=143))
                return
            if record.hook is not None:
                await self._fire_hook(record)
            elif record.permission is not None:
                await self._ask_permission(record)
            elif record.message is not None:
                self.queue.put_nowait(record.message)
            await asyncio.sleep(0)
        if self.interrupted:
            for record in records:
                if record.after_interrupt and record.message is not None:
                    self.queue.put_nowait(record.message)
                    await asyncio.sleep(0)

    async def _fire_hook(self, record: ScriptRecord) -> None:
        assert record.hook is not None
        event = str(record.hook["native_event"])
        tool_name = record.hook.get("tool_name")
        tool_use_id = record.hook.get("tool_use_id")
        payload = {
            "hook_event_name": event,
            "session_id": self.script.session_id,
            "transcript_path": "",
            "cwd": str(self.options.cwd or ""),
            "tool_name": tool_name,
            "tool_input": record.hook.get("tool_input", {}),
            "tool_use_id": tool_use_id,
        }
        denied = False
        for matcher in (self.options.hooks or {}).get(event, []):  # type: ignore[call-overload]
            pattern = matcher.matcher
            if pattern is not None and not re.fullmatch(pattern, str(tool_name or "")):
                continue
            for callback in matcher.hooks:
                output = await callback(payload, tool_use_id, {"signal": None})  # type: ignore[arg-type]
                specific = output.get("hookSpecificOutput") or {}
                if (
                    specific.get("permissionDecision") == "deny"
                    or output.get("continue_") is False
                    or output.get("decision") == "block"
                ):
                    denied = True
        self.hook_calls.append((event, tool_use_id, denied))
        for message in record.denied if denied else record.allowed:
            self.queue.put_nowait(message)

    async def _ask_permission(self, record: ScriptRecord) -> None:
        assert record.permission is not None
        tool_name = str(record.permission["tool_name"])
        allowed = False
        if self.options.can_use_tool is not None:
            result = await self.options.can_use_tool(
                tool_name,
                dict(record.permission.get("tool_input", {})),
                ToolPermissionContext(
                    tool_use_id=str(record.permission["tool_use_id"]),
                    agent_id=record.permission.get("agent_id"),
                    title=f"Claude wants to run {tool_name}",
                ),
            )
            allowed = isinstance(result, PermissionResultAllow)
            self.permission_results.append(result)
        self.permission_calls.append((tool_name, allowed))
        for message in record.allowed if allowed else record.denied:
            self.queue.put_nowait(message)

    async def interrupt(self) -> None:
        self.interrupts += 1
        self.interrupted = True
        self.release.set()

    async def get_context_usage(self) -> Mapping[str, Any]:
        """FIXTURE `ClaudeSDKClient.get_context_usage()` (`types.ContextUsageResponse` keys)."""

        self.context_usage_calls += 1
        if isinstance(self.context_usage, BaseException):
            raise self.context_usage
        if self.context_usage is None:
            raise CLIConnectionError("fixture: no context usage scripted")
        return dict(self.context_usage)

    async def disconnect(self) -> None:
        self.disconnected = True
        if self._player is not None and not self._player.done():
            self._player.cancel()
        self.queue.put_nowait(_END)


@dataclass
class FixtureClientFactory:
    """FIXTURE `ClaudeClientFactory`: every created client is kept for assertions."""

    script: FixtureScript
    refuse_connect: bool = False
    clients: list[FixtureClient] = field(default_factory=list)
    created: asyncio.Event = field(default_factory=asyncio.Event)
    # Scripts for later *fresh* clients (no `resume`), in order: a continuation target is a
    # new session with its own identity; a resumed client replays the base script.
    fresh_scripts: list[FixtureScript] = field(default_factory=list)
    context_usage: Mapping[str, Any] | BaseException | None = None

    @property
    def versions(self) -> Mapping[str, str]:
        return {"claude_agent_sdk": "0.2.165", "claude_code_bundled": "2.1.294"}

    def create(
        self, options: ClaudeAgentOptions, *, environment: Mapping[str, str]
    ) -> FixtureClient:
        script = self.script
        if self.clients and options.resume is None and self.fresh_scripts:
            script = self.fresh_scripts.pop(0)
        client = FixtureClient(
            script,
            options,
            environment,
            refuse_connect=self.refuse_connect,
            context_usage=self.context_usage,
        )
        self.clients.append(client)
        self.created.set()
        return client

    @property
    def last(self) -> FixtureClient:
        return self.clients[-1]

    async def connected(self, within_s: float = 10.0) -> FixtureClient:
        """The client a background `lane.turn` created, once it exists."""

        async with asyncio.timeout(within_s):
            await self.created.wait()
        return self.clients[-1]


# --- the workspace port ------------------------------------------------------------------------


class FixtureWorkspace:
    """FIXTURE `ClaudeWorkspace`: tmp directories and an in-memory lease registry (shared
    between harness instances to stand for the persisted ledger across process restarts)."""

    def __init__(self, root: Path, registry: dict[UUID, WorkspaceLease] | None = None) -> None:
        self.root = root
        self.registry: dict[UUID, WorkspaceLease] = registry if registry is not None else {}
        self.captures: list[tuple[UUID, str]] = []
        self.released: list[tuple[UUID, str | None]] = []

    async def acquire(self, request: AllocationRequest) -> WorkspaceLease:
        lease_id, key = lease_identity(
            request.lane_profile, request.harness_execution_id, request.generation
        )
        path = self.root / f"{request.harness_execution_id}-g{request.generation}"
        path.mkdir(parents=True, exist_ok=True)
        now = datetime.now(UTC)
        lease = WorkspaceLease(
            lease_id=lease_id,
            request_scope=request.request_scope,
            lease_key=key,
            lane_profile=request.lane_profile,
            harness_execution_id=request.harness_execution_id,
            generation=request.generation,
            run_id=request.run_id,
            attempt_no=request.attempt_no,
            path=str(path),
            repository=request.repository,
            base_ref=request.base_ref,
            base_commit="deadbeefcafe",
            fence=0,
            expires_at=now + timedelta(hours=1),
            slot=f"fixture:{request.harness_execution_id}",
            policy=request.policy,
            allocator_ref="fixture-workspace",
        )
        self.registry[lease_id] = lease
        return lease

    async def find(
        self, request_scope: str, harness_execution_id: UUID, generation: int
    ) -> WorkspaceLease | None:
        lease_id, _key = lease_identity(PROFILE, harness_execution_id, generation)
        lease = self.registry.get(lease_id)
        return lease if lease is not None and lease.request_scope == request_scope else None

    async def capture(self, lease: WorkspaceLease, *, name: str) -> CapturedSnapshot | None:
        self.captures.append((lease.lease_id, name))
        return CapturedSnapshot(
            snapshot=WorkspaceSnapshot(
                lane_profile=PROFILE,
                base_commit=lease.base_commit,
                head_commit=lease.base_commit,
                patch_artifact_ref=f"payload://fixture/{lease.lease_id}/patch.diff",
                manifest_digest=sha256_digest(name),
                producer_lease_id=str(lease.lease_id),
                producer_generation=lease.generation,
                captured_at=datetime.now(UTC),
            ),
            snapshot_ref=f"snapshot://fixture/{lease.lease_id}/{len(self.captures)}",
        )

    async def release(self, lease: WorkspaceLease, custody: CapturedSnapshot | None) -> None:
        self.released.append((lease.lease_id, custody.snapshot_ref if custody else None))
        self.registry[lease.lease_id] = lease.model_copy(update={"released_at": datetime.now(UTC)})


# --- the operation and its binding -------------------------------------------------------------


def projection_rows() -> tuple[ResolvedCapability, ...]:
    from tests.fixtures.projections.rows import hook_row, skill_row, tavily_row, verifier_row

    return (skill_row(), tavily_row(), hook_row(), verifier_row())


def base_operation() -> OperationExecutionRequest:
    from tests.unit.operations.test_operation_execution import operation_request

    return operation_request()


def materialization_digest_for(operation: OperationExecutionRequest) -> str:
    projection = render_host_files(projection_rows(), PROFILE, operating_contract(operation), None)
    return projection_digest(projection)


def claude_binding(
    *, materialization_digest: str, task_queue: str = LANE_QUEUE, **changes: Any
) -> ProviderExecutionBinding:
    fields: dict[str, Any] = {
        "lane_profile": PROFILE,
        "model": {"profile": "frontier.default", "model_id": "claude-sonnet-4-5"},
        "auth": {"profile": "anthropic.owner-login", "billing_mode": "subscription"},
        "environment": {"kind": "local_workspace", "host_profile": "worker.linux.default"},
        "workspace_policy": {
            "mode": "managed_worktree",
            "reuse": "within_run",
            "dirty_input": "reject",
            "cleanup": "retain_until_artifacts_registered",
        },
        "materialization_digest": materialization_digest,
        "requirements": {
            "controls": ["cancel"],
            "describe_digest": CLAUDE_AGENT_SDK_LANE_DESCRIBE.digest,
        },
        "policy_digest": DIGEST,
        "pins": {"sdk.claude_agent_sdk": "0.2.165", "cli.claude_code": "2.1.294"},
        "provider_options": {"provider": PROFILE, "disallowed_tools": ["WebFetch"]},
        "budgets": {"max_turns": 8, "max_segments": 16, "wall_clock_s": 3600},
        "task_queue": task_queue,
    }
    fields.update(changes)
    return ProviderExecutionBinding.sealed(**fields)


def claude_operation(
    *, binding: ProviderExecutionBinding | None = None, task_queue: str = LANE_QUEUE
) -> OperationExecutionRequest:
    base = base_operation()
    payload = base.model_dump(mode="python")
    payload.update(
        request_scope=SCOPE,
        execution_runtime="claude",
        lane_profile=PROFILE,
        cursor_binding=None,
        provider_binding=binding
        or claude_binding(
            materialization_digest=materialization_digest_for(base), task_queue=task_queue
        ),
        # A claude runtime carries no native placement; `OperationWorkflowRequest.
        # activity_task_queue` reads no provider binding either (frozen contract; MP-07
        # handoff delta 1), so the workflow cannot route a claude unit until it is applied.
        native_placement=None,
        budget_limits={"model.turns": 2, "tokens.total": 100_000},
    )
    return OperationExecutionRequest.model_validate(payload)


def fixture_admission(
    *,
    route: str = "owner_cli_login",
    env_unset: tuple[str, ...] = ("ANTHROPIC_API_KEY", "ANTHROPIC_AUTH_TOKEN"),
) -> AuthAdmission:
    """FIXTURE admission: the owner's CLI login route, API keys unset for the child."""

    return AuthAdmission(
        profile_id="anthropic.owner-login",
        lane_profile=PROFILE,
        route=route,  # type: ignore[arg-type]
        route_support="policy_restricted" if route == "owner_cli_login" else "documented",
        billing_mode="subscription" if route == "owner_cli_login" else "api",
        owner_attestation_ref="decision:fixture-owner-approval"
        if route == "owner_cli_login"
        else None,
        credential_ref=None if route == "owner_cli_login" else "env:ANTHROPIC_API_KEY",
        env_unset=env_unset,
        observation_digest="sha256:" + "0" * 64,
        observation_source="fixture",
        fixture=True,
        disclosures=("FIXTURE admission; no preflight ran",),
        admitted_at=datetime.now(UTC),
    )


# --- the stack ---------------------------------------------------------------------------------


@dataclass
class ClaudeStack:
    harness: ClaudeAgentSdkHarness
    factory: FixtureClientFactory
    workspace: FixtureWorkspace
    fences: InMemoryStopFenceRepository
    intents: InMemoryHookIntentLedger
    frames: InMemoryFrameStore
    operation: OperationExecutionRequest
    lanes: LaneStack
    environ: dict[str, str]
    admission: AuthAdmission


def claude_stack(
    tmp_path: Path,
    script: str = "full_run",
    *,
    operation: OperationExecutionRequest | None = None,
    admission: AuthAdmission | None = None,
    permissions: PermissionBindingPort | None = None,
    frames: InMemoryFrameStore | None = None,
    registry: dict[UUID, WorkspaceLease] | None = None,
    settings: ClaudeLaneSettings | None = None,
    environ: Mapping[str, str] | None = None,
    refuse_connect: bool = False,
    fences: InMemoryStopFenceRepository | None = None,
) -> ClaudeStack:
    operation = operation or claude_operation()
    admission = admission or fixture_admission()
    env = dict(environ if environ is not None else FIXTURE_ENVIRON)
    factory = FixtureClientFactory(FixtureScript.load(script), refuse_connect=refuse_connect)
    workspace = FixtureWorkspace(tmp_path / "leases", registry)
    fences = fences or InMemoryStopFenceRepository()
    intents = InMemoryHookIntentLedger()
    frames = frames or InMemoryFrameStore()
    harness = ClaudeAgentSdkHarness(
        clients=factory,
        workspaces=workspace,
        projections=RenderedProjectionSource(static_rows(projection_rows())),
        auth=StaticAuthAdmitter(admission),
        child_environment=provider_child_environment,
        environ=env,
        permissions=permissions,
        fences=fences,
        intents=intents,
        settings=settings
        or ClaudeLaneSettings(require_executables=False, drain_timeout_s=5.0, init_timeout_s=5.0),
    )
    lanes = lane_stack(harness, frames=frames, operation=operation)
    return ClaudeStack(
        harness, factory, workspace, fences, intents, frames, operation, lanes, env, admission
    )


__all__ = [
    "FIXTURES",
    "FIXTURE_ENVIRON",
    "PROFILE",
    "SESSION_ID",
    "ClaudeStack",
    "FixtureClient",
    "FixtureClientFactory",
    "FixtureScript",
    "FixtureWorkspace",
    "claude_binding",
    "claude_operation",
    "claude_stack",
    "fixture_admission",
    "materialization_digest_for",
    "projection_rows",
]
