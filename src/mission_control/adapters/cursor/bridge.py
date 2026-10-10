"""The Cursor local bridge behind a port (SPEC-07 section 5.2, ADR-0030; FT-G3).

`CursorBridgeLauncher` launches one `cursor-sdk-bridge` per leased workspace with a pinned
`state_root`; `CursorLocalBridge` is the small surface the `cursor_local` lane needs: create
or resume an agent, send one turn with an idempotency key, observe a run from a durable
offset, read a run's state and usage, cancel, close. Observed events are handed over in the
bridge's wire envelope shape (`{"offset", "sdkMessage" | "interactionUpdate" | "step" |
"result" | "done"}`) so recorded fixtures and the live SDK feed the same frame mapper.

`SdkBridgeLauncher` is the adapter over the pinned `cursor-sdk==1.0.37` async client. The API
key is passed in agent options only, never in the bridge environment (the bridge's shell
tools inherit it), and the SDK's environment fallback is disabled. Tests use a replaying fake
bridge; nothing here runs at import time and no agent is created by this module's tests.
"""

from __future__ import annotations

import asyncio
import dataclasses
import platform
import sys
from collections.abc import AsyncIterator, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol

from pydantic import SecretStr

CURSOR_SDK_PIN = "1.0.37"
BRIDGE_PROTOCOL = "sdk.v1"


class BridgeError(RuntimeError):
    """A bridge or SDK call failed; the message never carries provider text or secrets."""


class AgentBusy(BridgeError):
    """The agent already has an active run (`AgentBusyError`, `409 agent_busy`)."""


class RunNotFound(BridgeError):
    """The bridge's store does not know the run (the native turn is lost)."""


class FeatureUnavailable(BridgeError):
    """The account cannot use this surface yet (`feature_unavailable`, e.g. local usage)."""


class SandboxUnsupported(BridgeError):
    """The host cannot run the Cursor sandbox (`ConfigurationError` at agent create)."""


class RunNotCancellable(BridgeError):
    """The run is already terminal (`409 run_not_cancellable`)."""


class HostUnsupported(BridgeError):
    """The worker host cannot spawn the SDK bridge (MP-09 host gate): on Windows the worker
    runs a SelectorEventLoop (psycopg, `bootstrap/worker.run_cli`) and asyncio spawns
    subprocesses only on the Proactor loop (python.org asyncio-platforms#windows); run the
    `cursor_local` worker under WSL 2 or Linux (OWNER-FIXTURE-RUNBOOK B6 / 2.8)."""


LANE_UNSUPPORTED_OS = "LANE_UNSUPPORTED_OS"
_SUPPORTED_SYSTEMS = frozenset({"Linux", "Darwin"})
_WINDOWS_REASON = (
    "the worker runs a SelectorEventLoop on Windows (psycopg, bootstrap/worker.run_cli) and "
    "asyncio on Windows spawns subprocesses only on the Proactor loop"
)
_WSL_REMEDY = (
    "run the cursor_local worker under WSL 2 or Linux "
    "(docs/qualification/lanes/cursor_local/README.md, OWNER-FIXTURE-RUNBOOK 2.8)"
)


@dataclass(frozen=True)
class HostGate:
    """What the `cursor_local` host gate observed and decided (preflight and launch agree)."""

    system: str
    release: str
    event_loop: str
    wsl: bool
    supported: bool
    code: str | None
    reason: str
    remedy: str

    @property
    def message(self) -> str:
        return f"{self.reason}; {self.remedy}" if not self.supported else "host supported"


def _running_loop_name() -> str:
    try:
        return type(asyncio.get_running_loop()).__name__
    except RuntimeError:
        return "none"


def host_gate(
    *, system: str | None = None, release: str | None = None, event_loop: str | None = None
) -> HostGate:
    """The MP-22 lane-host rule (`bootstrap/preflight.LANE_HOSTS["cursor_local"]`) applied where
    the lane spawns: Linux (including WSL 2) and macOS are supported; Windows is refused
    whatever the loop, because the production worker pins the selector loop there and the
    Windows sandbox is UNVERIFIED (docs/qualification/lanes/README.md)."""

    name = system if system is not None else platform.system()
    rel = release if release is not None else platform.release()
    loop = event_loop if event_loop is not None else _running_loop_name()
    lowered = rel.lower()
    wsl = name == "Linux" and ("microsoft" in lowered or "wsl" in lowered)
    if name in _SUPPORTED_SYSTEMS:
        return HostGate(name, rel, loop, wsl, True, None, "supported host", "")
    reason = _WINDOWS_REASON if name == "Windows" else f"{name} is not a qualified bridge host"
    if name == "Windows" and loop not in {"SelectorEventLoop", "none"}:
        reason += f" (observed {loop}; the Windows sandbox and bridge stay UNVERIFIED)"
    return HostGate(name, rel, loop, wsl, False, LANE_UNSUPPORTED_OS, reason, _WSL_REMEDY)


@dataclass(frozen=True)
class BridgeEvent:
    """One observed run event: its durable offset and the wire envelope."""

    offset: str
    envelope: Mapping[str, Any]


@dataclass(frozen=True)
class RunState:
    run_id: str
    agent_id: str
    status: str
    result: str = ""
    duration_ms: int = 0
    model: str | None = None
    git_branches: tuple[Mapping[str, str], ...] = ()
    usage: Mapping[str, int] | None = None

    @property
    def terminal(self) -> bool:
        return self.status in {"finished", "error", "cancelled", "expired"}


@dataclass(frozen=True)
class UsageState:
    input_tokens: int = 0
    output_tokens: int = 0
    total_tokens: int = 0
    cost_micros_usd: int | None = None


@dataclass(frozen=True)
class LocalAgentSpec:
    """What `Agent.create` / `Agent.resume` receive (re-supplied on every resume: tools,
    disallowed tools and inline MCP servers are not persisted across `Agent.resume`)."""

    model: str
    name: str
    cwd: str
    mode: str = "agent"
    setting_sources: tuple[str, ...] = ("project",)
    sandbox_enabled: bool = False
    agents: tuple[Mapping[str, Any], ...] = ()
    disallowed_tools: tuple[str, ...] = ()
    mcp_servers: Mapping[str, Any] | None = None


class CursorLocalBridge(Protocol):
    async def create_agent(self, spec: LocalAgentSpec) -> str: ...

    async def resume_agent(self, agent_id: str, spec: LocalAgentSpec) -> str: ...

    async def send(self, agent_id: str, text: str, *, idempotency_key: str) -> str: ...

    def observe(self, run_id: str, *, after_offset: str | None) -> AsyncIterator[BridgeEvent]: ...

    async def run_state(self, run_id: str) -> RunState: ...

    async def cancel(self, run_id: str, *, agent_id: str) -> None: ...

    async def usage(self, agent_id: str) -> UsageState: ...

    async def close_agent(self, agent_id: str) -> None: ...

    async def aclose(self) -> None: ...


class CursorBridgeLauncher(Protocol):
    @property
    def versions(self) -> Mapping[str, str]: ...

    def sandbox_supported(self) -> bool: ...

    async def launch(self, *, workspace: Path, state_root: Path) -> CursorLocalBridge: ...


# --- wire envelopes from SDK objects --------------------------------------------------------


def _jsonable(value: Any) -> Any:
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        return {key: _jsonable(item) for key, item in dataclasses.asdict(value).items()}
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, list | tuple):
        return [_jsonable(item) for item in value]
    if isinstance(value, str | int | float | bool) or value is None:
        return value
    return str(value)


def envelope_from_event(event: Any) -> BridgeEvent:
    """A parsed SDK `RunStreamEvent` back in the wire envelope shape the mapper reads."""

    offset = str(getattr(event, "offset", "") or "")
    kind = getattr(event, "kind", "unknown")
    envelope: dict[str, Any]
    if kind == "sdk_message":
        envelope = {"sdkMessage": _jsonable(event.sdk_message)}
    elif kind == "interaction_update":
        envelope = {"interactionUpdate": _jsonable(event.interaction_update)}
    elif kind == "step":
        envelope = {"step": _jsonable(event.step)}
    elif kind == "result":
        result = _jsonable(event.result or {})
        envelope = {"result": {"result": result} if event.result_is_full else result}
    elif kind == "done":
        envelope = {"done": _jsonable(event.done or {})}
    else:
        envelope = {"unknown": {}}
    return BridgeEvent(offset=offset, envelope=envelope)


def _usage_tokens(usage: Any) -> dict[str, int] | None:
    if usage is None:
        return None
    return {
        "input_tokens": int(getattr(usage, "input_tokens", 0) or 0),
        "output_tokens": int(getattr(usage, "output_tokens", 0) or 0),
        "total_tokens": int(getattr(usage, "total_tokens", 0) or 0),
    }


def run_state_from_snapshot(snapshot: Any) -> RunState:
    git = getattr(snapshot, "git", None)
    branches = tuple(
        {"repo_url": branch.repo_url, "branch": branch.branch, "pr_url": branch.pr_url}
        for branch in (getattr(git, "branches", ()) or ())
    )
    model = getattr(snapshot, "model", None)
    return RunState(
        run_id=str(getattr(snapshot, "id", "")),
        agent_id=str(getattr(snapshot, "agent_id", "")),
        status=str(getattr(snapshot, "status", "running")),
        result=str(getattr(snapshot, "result", "") or ""),
        duration_ms=int(getattr(snapshot, "duration_ms", 0) or 0),
        model=getattr(model, "id", None) if model is not None else None,
        git_branches=branches,
        usage=_usage_tokens(getattr(snapshot, "usage", None)),
    )


# --- the pinned cursor-sdk adapter --------------------------------------------------------


@dataclass
class SdkLocalBridge:
    """`CursorLocalBridge` over a `cursor_sdk.asyncio.AsyncClient` that owns its bridge."""

    client: Any
    api_key: str
    agents: dict[str, Any] = field(default_factory=dict)

    def _options(self, spec: LocalAgentSpec) -> Any:
        from cursor_sdk.types import (
            AgentOptions,
            LocalAgentOptions,
            ModelSelection,
            SandboxOptions,
        )

        return AgentOptions(
            model=ModelSelection(id=spec.model),
            api_key=self.api_key,
            name=spec.name,
            mode=spec.mode,  # type: ignore[arg-type]
            local=LocalAgentOptions(
                cwd=spec.cwd,
                setting_sources=list(spec.setting_sources),
                sandbox_options=SandboxOptions(enabled=spec.sandbox_enabled),
            ),
            agents={
                str(item["name"]): {key: value for key, value in item.items() if key != "name"}
                for item in spec.agents
            }
            or None,
            disallowed_tools=list(spec.disallowed_tools) or None,
            mcp_servers=spec.mcp_servers,
        )

    async def create_agent(self, spec: LocalAgentSpec) -> str:
        from cursor_sdk.asyncio import AsyncAgent
        from cursor_sdk.errors import ConfigurationError

        try:
            agent = await AsyncAgent.create(self._options(spec), client=self.client)
        except ConfigurationError as error:
            if spec.sandbox_enabled:
                raise SandboxUnsupported(
                    "the Cursor sandbox is unavailable on this host"
                ) from error
            raise BridgeError(f"agent configuration rejected: {type(error).__name__}") from error
        self.agents[agent.agent_id] = agent
        return str(agent.agent_id)

    async def resume_agent(self, agent_id: str, spec: LocalAgentSpec) -> str:
        from cursor_sdk.errors import NotFoundError

        try:
            agent = await self.client.resume_agent(agent_id, self._options(spec))
        except NotFoundError as error:
            raise RunNotFound("the bridge store does not know this agent") from error
        self.agents[agent.agent_id] = agent
        return str(agent.agent_id)

    async def send(self, agent_id: str, text: str, *, idempotency_key: str) -> str:
        from cursor_sdk.errors import AgentBusyError

        agent = self.agents[agent_id]
        try:
            run = await agent.send(text, idempotency_key=idempotency_key)
        except AgentBusyError as error:
            raise AgentBusy("the agent already has an active run") from error
        return str(run.id)

    async def observe(self, run_id: str, *, after_offset: str | None) -> AsyncIterator[BridgeEvent]:
        async for event in self.client.observe_run(run_id, after_offset=after_offset):
            yield envelope_from_event(event)

    async def run_state(self, run_id: str) -> RunState:
        from cursor_sdk.errors import NotFoundError

        try:
            run = await self.client.get_run(run_id)
        except NotFoundError as error:
            raise RunNotFound("the bridge store does not know this run") from error
        return run_state_from_snapshot(run)

    async def cancel(self, run_id: str, *, agent_id: str) -> None:
        from cursor_sdk.errors import BadRequestError, UnsupportedRunOperationError

        try:
            await self.client.cancel_run(run_id, agent_id=agent_id)
        except (BadRequestError, UnsupportedRunOperationError) as error:
            raise RunNotCancellable("the run is already terminal") from error

    async def usage(self, agent_id: str) -> UsageState:
        from cursor_sdk.errors import InternalServerError

        agent = self.agents[agent_id]
        try:
            usage = await agent.get_usage()
        except InternalServerError as error:
            raise FeatureUnavailable("local usage is not enabled for this account") from error
        tokens = _usage_tokens(usage.usage) or {}
        cost = usage.cost
        return UsageState(
            input_tokens=tokens.get("input_tokens", 0),
            output_tokens=tokens.get("output_tokens", 0),
            total_tokens=tokens.get("total_tokens", 0),
            cost_micros_usd=(None if cost is None else round(float(cost.charged_cents) * 10_000)),
        )

    async def close_agent(self, agent_id: str) -> None:
        agent = self.agents.pop(agent_id, None)
        if agent is not None:
            await agent.close()

    async def aclose(self) -> None:
        await self.client.aclose()


class SdkBridgeLauncher:
    """Launches the vendored `cursor-sdk-bridge` of the pinned `cursor-sdk` per workspace."""

    def __init__(
        self,
        api_key: SecretStr,
        *,
        bridge_command: Sequence[str] | None = None,
        timeout_s: float = 30,
    ) -> None:
        self._api_key = api_key
        self._command = list(bridge_command) if bridge_command else None
        self._timeout = timeout_s

    @property
    def versions(self) -> Mapping[str, str]:
        return {"cursor_sdk": CURSOR_SDK_PIN, "bridge": CURSOR_SDK_PIN, "protocol": BRIDGE_PROTOCOL}

    def sandbox_supported(self) -> bool:
        # Linux (bubblewrap) and macOS (seatbelt) are documented; Windows is UNVERIFIED and
        # stays refused until a recorded qualification says otherwise (SPEC-07 section 12).
        return platform.system() in {"Linux", "Darwin"}

    def host_gate(self) -> HostGate:
        """The host gate the lane consults before it leases or spawns anything (MP-09)."""

        return host_gate()

    async def launch(self, *, workspace: Path, state_root: Path) -> CursorLocalBridge:
        gate = self.host_gate()
        if not gate.supported:
            # Enforced where the lane spawns: never launch a bridge on an unsupported host.
            raise HostUnsupported(f"{gate.code}: {gate.message}")
        if sys.platform == "win32":  # pragma: no cover - refused above; defensive
            raise HostUnsupported(f"{LANE_UNSUPPORTED_OS}: {_WINDOWS_REASON}; {_WSL_REMEDY}")
        from cursor_sdk.asyncio import AsyncClient

        await asyncio.to_thread(state_root.mkdir, parents=True, exist_ok=True)
        client = await AsyncClient.launch_bridge(
            self._command,
            workspace=workspace,
            state_root=state_root,
            timeout=self._timeout,
            allow_api_key_env_fallback=False,
        )
        return SdkLocalBridge(client=client, api_key=self._api_key.get_secret_value())


__all__ = [
    "BRIDGE_PROTOCOL",
    "CURSOR_SDK_PIN",
    "LANE_UNSUPPORTED_OS",
    "AgentBusy",
    "BridgeError",
    "BridgeEvent",
    "CursorBridgeLauncher",
    "CursorLocalBridge",
    "FeatureUnavailable",
    "HostGate",
    "HostUnsupported",
    "LocalAgentSpec",
    "RunNotCancellable",
    "RunNotFound",
    "RunState",
    "SandboxUnsupported",
    "SdkBridgeLauncher",
    "SdkLocalBridge",
    "UsageState",
    "envelope_from_event",
    "host_gate",
    "run_state_from_snapshot",
]
