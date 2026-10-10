"""Launching `codex app-server` as a worker subprocess with a credential-free environment.

The lane never passes the worker's process environment through. The child gets an explicit
allow-listed environment (`PATH`/`HOME`-class names and the location variables MP-05 allows),
the one credential variable the admitted auth route names (`credential_ref = env:NAME` on a
credential route; a login route carries none) and `CODEX_HOME`, all run through the injected
`ChildEnvironmentBuilder` (`bootstrap.provider_auth.provider_child_environment` in production;
adapters never import bootstrap) so the admission's `env_unset` is honoured and nothing
shadows the route (SPEC-02 "Authentication"). Any other credential-shaped name is dropped.

Version pin: the launcher probes `<binary> --version` once and refuses (typed
`CodexVersionMismatch`) a CLI other than the pinned `codex-cli 0.162.0` before it starts an
app-server, unless the composition passes the explicit `allow_version_mismatch` override: the
committed protocol schema is only valid for that exact pin.

Host constraint: `asyncio.create_subprocess_exec` needs a Proactor (Windows) or Unix event
loop; the worker's Windows `SelectorEventLoop` cannot spawn, so the local Codex lane runs on
Linux/WSL workers and refuses with `HostUnsupported` elsewhere (VALIDATION "Local Temporal
fallback": per-profile subprocess handling, visible in preflight, no global loop change).
"""

from __future__ import annotations

import asyncio
import logging
import re
import sys
from collections.abc import Mapping
from contextlib import suppress
from dataclasses import dataclass, field
from pathlib import Path
from typing import Final, Protocol

from mission_control.adapters.codex.protocol import PINNED_CODEX_CLI_VERSION
from mission_control.adapters.codex.transport import (
    DEFAULT_REQUEST_TIMEOUT_S,
    AppServerConnection,
    JsonLinesChannel,
)
from mission_control.adapters.provider_auth.detection import PATH_ENV_ALLOWLIST
from mission_control.application.execution.auth_admission import LOGIN_ROUTES, AuthAdmission

_LOGGER = logging.getLogger(__name__)
UNSUPPORTED_BEHAVIOR: Final = "UNSUPPORTED_BEHAVIOR"

# Names a provider child may inherit from the worker (never a credential).
BASE_ENV_ALLOWLIST: Final[frozenset[str]] = (
    frozenset(
        {
            "PATH",
            "HOME",
            "USERPROFILE",
            "SYSTEMROOT",
            "SYSTEMDRIVE",
            "COMSPEC",
            "TEMP",
            "TMP",
            "TMPDIR",
            "LANG",
            "LC_ALL",
            "LC_CTYPE",
            "TERM",
            "SHELL",
            "USER",
            "LOGNAME",
            "TZ",
            "SSL_CERT_FILE",
            "SSL_CERT_DIR",
            "HTTPS_PROXY",
            "HTTP_PROXY",
            "NO_PROXY",
            "XDG_CONFIG_HOME",
            "XDG_CACHE_HOME",
            "XDG_DATA_HOME",
        }
    )
    | PATH_ENV_ALLOWLIST
)
_CREDENTIAL_NAME: Final = re.compile(
    r"(KEY|TOKEN|SECRET|PASSWORD|PASSWD|CREDENTIAL)", re.IGNORECASE
)
_VERSION_LINE: Final = re.compile(r"codex-cli\s+(\S+)")


PROVIDER_PIN_MISMATCH: Final = "PROVIDER_PIN_MISMATCH"


class HostUnsupported(RuntimeError):
    """This worker cannot run a local CLI subprocess (Windows selector loop)."""

    code = UNSUPPORTED_BEHAVIOR


class CodexVersionMismatch(RuntimeError):
    """`codex --version` is not the pinned CLI (or could not be read): the committed
    app-server schema does not describe that binary, so nothing is launched."""

    code = PROVIDER_PIN_MISMATCH

    def __init__(self, observed: str, pinned: str = PINNED_CODEX_CLI_VERSION) -> None:
        super().__init__(
            f"codex-cli {observed} is not the pinned {pinned}; regenerate the schema and "
            "re-qualify, or pass the explicit allow_version_mismatch override"
        )
        self.observed = observed
        self.pinned = pinned


class ChildEnvironmentBuilder(Protocol):
    """`bootstrap.provider_auth.provider_child_environment` (injected by composition):
    the explicit environment of the admitted provider process."""

    def __call__(
        self,
        environ: Mapping[str, str],
        admission: AuthAdmission,
        *,
        extra: Mapping[str, str] | None = None,
    ) -> dict[str, str]: ...


def subprocess_supported(loop: asyncio.AbstractEventLoop | None = None) -> tuple[bool, str]:
    """Whether the running loop can spawn a subprocess (Windows needs a Proactor loop)."""

    if sys.platform != "win32":
        return True, "posix event loop"
    try:
        current = loop or asyncio.get_running_loop()
    except RuntimeError:
        return False, "no running event loop"
    if isinstance(current, asyncio.ProactorEventLoop):
        return True, "windows proactor loop"
    return (
        False,
        f"{type(current).__name__} on win32 cannot spawn a subprocess; run the codex lane on a "
        "Linux/WSL worker",
    )


def admitted_credential_name(admission: AuthAdmission) -> str | None:
    """The one environment variable the admitted route reads (None on a login route)."""

    if admission.route in LOGIN_ROUTES or admission.credential_ref is None:
        return None
    scheme, _, name = admission.credential_ref.partition(":")
    return name.upper() if scheme == "env" and name else None


def child_environment(
    environ: Mapping[str, str],
    admission: AuthAdmission,
    *,
    codex_home: Path,
    builder: ChildEnvironmentBuilder,
    extra: Mapping[str, str] | None = None,
) -> dict[str, str]:
    """The app-server child's environment: allow-listed names, the admitted credential and
    `CODEX_HOME`, through the injected builder (never the worker's whole env)."""

    admitted = admitted_credential_name(admission)
    base: dict[str, str] = {}
    for name, value in environ.items():
        upper = name.upper()
        allowed = upper in BASE_ENV_ALLOWLIST and upper != "CODEX_HOME"
        if allowed or (admitted is not None and upper == admitted):
            base[name] = value
    overrides = {"CODEX_HOME": str(codex_home), **(extra or {})}
    child = builder(base, admission, extra=overrides)
    for name in list(child):
        upper = name.upper()
        if _CREDENTIAL_NAME.search(upper) and upper != admitted and upper not in overrides:
            del child[name]
    return child


@dataclass(frozen=True)
class LaunchSpec:
    cwd: Path
    env: Mapping[str, str]
    codex_home: Path
    # `-c key=value` overrides (the CLI's documented dotted-path config overrides).
    config_overrides: tuple[str, ...] = ()


@dataclass
class LaunchedAppServer:
    connection: AppServerConnection
    versions: dict[str, str]
    pid: int | None = None
    process: asyncio.subprocess.Process | None = None
    stderr_tail: list[str] = field(default_factory=list)
    stderr_task: asyncio.Task[None] | None = None

    async def terminate(self, *, grace_s: float = 5.0) -> None:
        await self.connection.close("terminated by mission control")
        process = self.process
        if process is None or process.returncode is not None:
            return
        with suppress(ProcessLookupError):
            process.terminate()
        try:
            await asyncio.wait_for(process.wait(), grace_s)
        except TimeoutError:
            with suppress(ProcessLookupError):
                process.kill()
            with suppress(Exception):
                await asyncio.wait_for(process.wait(), grace_s)


class AppServerLauncher(Protocol):
    async def launch(self, spec: LaunchSpec) -> LaunchedAppServer: ...

    @property
    def versions(self) -> Mapping[str, str]: ...


class SubprocessAppServerLauncher:
    """`codex app-server` over stdio on a Linux/WSL worker (never a login, never a turn)."""

    def __init__(
        self,
        codex_binary: str = "codex",
        *,
        request_timeout_s: float = DEFAULT_REQUEST_TIMEOUT_S,
        probe_version: bool = True,
        allow_version_mismatch: bool = False,
        stderr_tail_lines: int = 200,
    ) -> None:
        self._binary = codex_binary
        self._timeout = request_timeout_s
        self._probe = probe_version
        self._allow_mismatch = allow_version_mismatch
        self._tail = stderr_tail_lines
        self._versions: dict[str, str] = {
            "codex_cli": "unverified",
            "codex_cli_pinned": PINNED_CODEX_CLI_VERSION,
        }

    @property
    def versions(self) -> Mapping[str, str]:
        return dict(self._versions)

    async def _probe_version(self, spec: LaunchSpec) -> None:
        if not self._probe or self._versions["codex_cli"] != "unverified":
            return
        try:
            process = await asyncio.create_subprocess_exec(
                self._binary,
                "--version",
                cwd=str(spec.cwd),
                env=dict(spec.env),
                stdin=asyncio.subprocess.DEVNULL,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.DEVNULL,
            )
            out, _ = await asyncio.wait_for(process.communicate(), 15)
        except (OSError, TimeoutError) as error:
            _LOGGER.warning("codex --version did not answer: %s", error)
            return
        match = _VERSION_LINE.search(out.decode("utf-8", errors="replace"))
        if match:
            self._versions["codex_cli"] = match.group(1)

    def check_version(self) -> None:
        """Refuse a CLI other than the pin (an unreadable version is not the pin)."""

        observed = self._versions["codex_cli"]
        if observed == PINNED_CODEX_CLI_VERSION:
            return
        if self._allow_mismatch:
            _LOGGER.warning(
                "codex-cli %s is not the pinned %s; launching under the explicit override",
                observed,
                PINNED_CODEX_CLI_VERSION,
            )
            self._versions["codex_cli_override"] = "allow_version_mismatch"
            return
        raise CodexVersionMismatch(observed)

    async def launch(self, spec: LaunchSpec) -> LaunchedAppServer:
        supported, detail = subprocess_supported()
        if not supported:
            raise HostUnsupported(detail)
        await self._probe_version(spec)
        if self._probe:
            self.check_version()
        argv = [self._binary, "app-server"]
        for override in spec.config_overrides:
            argv.extend(["-c", override])
        process = await asyncio.create_subprocess_exec(
            *argv,
            cwd=str(spec.cwd),
            env=dict(spec.env),
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            limit=64 * 1024 * 1024,
        )
        assert process.stdout is not None and process.stdin is not None
        connection = AppServerConnection(
            JsonLinesChannel(process.stdout, process.stdin), request_timeout_s=self._timeout
        )
        launched = LaunchedAppServer(
            connection=connection, versions=dict(self._versions), pid=process.pid, process=process
        )
        if process.stderr is not None:
            launched.stderr_task = asyncio.create_task(
                _drain_stderr(process.stderr, launched.stderr_tail, self._tail),
                name="codex-app-server-stderr",
            )
        await connection.open()
        return launched


async def _drain_stderr(stream: asyncio.StreamReader, tail: list[str], limit: int) -> None:
    with suppress(Exception):
        while True:
            line = await stream.readline()
            if not line:
                return
            text = line.decode("utf-8", errors="replace").rstrip()
            tail.append(text[:1_000])
            if len(tail) > limit:
                del tail[: len(tail) - limit]
            _LOGGER.debug("codex app-server stderr: %s", text[:500])


__all__ = [
    "BASE_ENV_ALLOWLIST",
    "PROVIDER_PIN_MISMATCH",
    "UNSUPPORTED_BEHAVIOR",
    "AppServerLauncher",
    "ChildEnvironmentBuilder",
    "CodexVersionMismatch",
    "HostUnsupported",
    "LaunchSpec",
    "LaunchedAppServer",
    "SubprocessAppServerLauncher",
    "admitted_credential_name",
    "child_environment",
    "subprocess_supported",
]
