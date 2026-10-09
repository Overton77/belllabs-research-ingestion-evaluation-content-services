"""`AuthPreflightPort` for a worker: probe the provider child environment before launch.

The context is the environment the provider process will inherit (names only) plus the
stored-sign-in locations. A status runner is optional; when configured it executes the
vendor's read-only status command (`claude auth status --json`, `codex login status`,
`agent status`) with argv only, a timeout and the same child environment, and its output
goes straight to a redacting parser. No login is ever attempted and no output is kept.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Mapping, Sequence
from typing import Protocol

from mission_control.adapters.provider_auth.detection import (
    SURFACES,
    WorkerAuthContext,
    probe,
)
from mission_control.adapters.provider_auth.status import StatusOutput
from mission_control.application.execution.auth_admission import AuthObservation, AuthProfile


class StatusRunner(Protocol):
    async def run(self, argv: Sequence[str]) -> StatusOutput | None:
        """Run a read-only status command; None when it cannot run (absent binary)."""
        ...


class SubprocessStatusRunner:
    """Runs a status command with an explicit environment and a hard timeout."""

    def __init__(self, env: Mapping[str, str], *, timeout_s: float = 10.0) -> None:
        self._env = dict(env)
        self._timeout = timeout_s

    async def run(self, argv: Sequence[str]) -> StatusOutput | None:
        try:
            process = await asyncio.create_subprocess_exec(
                *argv,
                stdin=asyncio.subprocess.DEVNULL,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                env=self._env,
            )
        except (FileNotFoundError, PermissionError):
            return None
        try:
            stdout, stderr = await asyncio.wait_for(process.communicate(), self._timeout)
        except TimeoutError:
            process.kill()
            await process.wait()
            return None
        return StatusOutput(
            exit_code=process.returncode if process.returncode is not None else -1,
            stdout=stdout.decode("utf-8", errors="replace")[:8_192],
            stderr=stderr.decode("utf-8", errors="replace")[:8_192],
        )


class LocalAuthPreflight:
    """Probe each profile against the worker context the launcher will use."""

    def __init__(
        self,
        context: Callable[[AuthProfile], WorkerAuthContext],
        *,
        status_runner: StatusRunner | None = None,
        probe_versions: Mapping[str, str] | None = None,
    ) -> None:
        self._context = context
        self._status = status_runner
        self._versions = dict(probe_versions or {})

    async def preflight(self, profile: AuthProfile) -> AuthObservation:
        surface = SURFACES[profile.lane_profile]
        status: StatusOutput | None = None
        if self._status is not None and surface.status_argv is not None:
            status = await self._status.run(surface.status_argv)
        return probe(
            profile,
            self._context(profile),
            status=status,
            probe_versions=self._versions,
        )


__all__ = ["LocalAuthPreflight", "StatusRunner", "SubprocessStatusRunner"]
