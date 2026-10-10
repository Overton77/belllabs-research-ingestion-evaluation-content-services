"""The `claude_agent_sdk` worker-host gate (MP-07; same rule as `adapters/cursor/bridge.host_gate`).

The lane spawns Claude Code as a stdio subprocess (`claude_agent_sdk._internal.transport.
subprocess_cli.SubprocessCLITransport.connect` -> `anyio.open_process`). The production worker
runs a `SelectorEventLoop` on Windows (psycopg, `bootstrap/worker.run_cli`) and asyncio on
Windows spawns subprocesses only on the Proactor loop (python.org asyncio-platforms#windows),
so a Windows worker can never host this lane. Linux (including WSL 2) and macOS are
supported; anything else is refused. The gate is consulted where the lane is composed
(`compose.compose_claude_local`) and where it spawns (`transport.SdkClientFactory.create`),
so preflight and launch agree. Platform facts are injectable for tests.
"""

from __future__ import annotations

import asyncio
import platform
from dataclasses import dataclass
from typing import Final

LANE_UNSUPPORTED_OS: Final = "LANE_UNSUPPORTED_OS"
_SUPPORTED_SYSTEMS: Final = frozenset({"Linux", "Darwin"})
_WINDOWS_REASON: Final = (
    "the worker runs a SelectorEventLoop on Windows (psycopg, bootstrap/worker.run_cli) and "
    "asyncio on Windows spawns subprocesses only on the Proactor loop; the Claude Agent SDK "
    "spawns Claude Code as a stdio subprocess"
)
_REMEDY: Final = (
    "run the claude_agent_sdk worker under WSL 2 or Linux "
    "(docs/qualification/lanes/claude_agent_sdk/README.md, OS constraints)"
)


class HostUnsupported(RuntimeError):
    """The worker host cannot spawn the Claude Code subprocess (typed; never retried into a
    spawn): `code` is `LANE_UNSUPPORTED_OS`."""

    code: Final = LANE_UNSUPPORTED_OS


@dataclass(frozen=True)
class HostGate:
    """What the `claude_agent_sdk` host gate observed and decided."""

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

    def require(self) -> HostGate:
        """Raise `HostUnsupported` on an unsupported host; return the gate otherwise."""

        if not self.supported:
            raise HostUnsupported(f"{self.code}: {self.message}")
        return self


def _running_loop_name() -> str:
    try:
        return type(asyncio.get_running_loop()).__name__
    except RuntimeError:
        return "none"


def host_gate(
    *, system: str | None = None, release: str | None = None, event_loop: str | None = None
) -> HostGate:
    """Linux (WSL 2 included) and macOS are supported; Windows is refused whatever the loop
    (the production worker pins the selector loop there and no Windows spawn was verified)."""

    name = system if system is not None else platform.system()
    rel = release if release is not None else platform.release()
    loop = event_loop if event_loop is not None else _running_loop_name()
    lowered = rel.lower()
    wsl = name == "Linux" and ("microsoft" in lowered or "wsl" in lowered)
    if name in _SUPPORTED_SYSTEMS:
        return HostGate(name, rel, loop, wsl, True, None, "supported host", "")
    reason = _WINDOWS_REASON if name == "Windows" else f"{name} is not a qualified lane host"
    if name == "Windows" and loop not in {"SelectorEventLoop", "none"}:
        reason += f" (observed {loop}; a Windows spawn of this lane stays UNVERIFIED)"
    return HostGate(name, rel, loop, wsl, False, LANE_UNSUPPORTED_OS, reason, _REMEDY)


__all__ = ["LANE_UNSUPPORTED_OS", "HostGate", "HostUnsupported", "host_gate"]
