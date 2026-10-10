"""The production client factory: `ClaudeSDKClient` over an explicit child environment (MP-07).

`claude_agent_sdk._internal.transport.subprocess_cli.SubprocessCLITransport.connect`
(0.2.165, lines 814-825) spawns the CLI with ``{**os.environ (minus CLAUDECODE),
CLAUDE_CODE_ENTRYPOINT, **options.env, CLAUDE_AGENT_SDK_VERSION}``: the worker's whole
environment, including every credential variable, always reaches the subprocess, and a
variable the auth admission asked to *unset* (`AuthAdmission.env_unset`, MP-05) cannot be
removed through `options.env`. `ExplicitEnvironmentCliTransport` keeps the SDK's command
line (`_build_command`), CLI resolution, stdio framing, stderr handling and shielded
`close()`, and replaces only the spawn environment with the one the launcher built through
`bootstrap.provider_auth.provider_child_environment` (injected; adapters do not import
bootstrap). It is a subclass of an SDK-internal class pinned to 0.2.165: the private names it
uses are listed in `_RELIED_ON` and checked at import, so an SDK bump that renames them fails
loudly rather than silently spawning with the inherited environment.

`ClaudeSDKClient(options, transport=...)` skips `_configure_can_use_tool` for a custom
transport only in the sense that the configured copy never reaches it
(`client.py::_connect_inner`), so the factory applies it here before the transport is built
(`--permission-prompt-tool stdio` routes `can_use_tool` over the control protocol).
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterable, AsyncIterator, Awaitable, Callable, Mapping
from subprocess import PIPE
from typing import Any, Final

import anyio
import anyio.to_thread
import claude_agent_sdk
from anyio.streams.text import TextReceiveStream, TextSendStream
from claude_agent_sdk._errors import CLIConnectionError, CLINotFoundError
from claude_agent_sdk._internal._task_compat import spawn_detached
from claude_agent_sdk._internal.transport import subprocess_cli as _cli
from claude_agent_sdk.client import ClaudeSDKClient
from claude_agent_sdk.types import ClaudeAgentOptions, _configure_can_use_tool

from mission_control.adapters.claude.describe import BUNDLED_CLI_VERSION, SDK_VERSION
from mission_control.adapters.claude.host import HostGate, host_gate
from mission_control.adapters.claude.session import ClaudeClient

_LOGGER = logging.getLogger(__name__)
_SDK_VERSION: Final = str(claude_agent_sdk.__version__)
# SDK-internal names this module relies on (claude_agent_sdk 0.2.165).
_RELIED_ON: Final = (
    "SubprocessCLITransport",
    "_ACTIVE_CHILDREN",
    "_SDK_READS_SESSION_STATE_ENV",
)
for _name in _RELIED_ON:
    if not hasattr(_cli, _name):  # pragma: no cover - guards an SDK bump
        raise ImportError(f"claude_agent_sdk {_SDK_VERSION} no longer exposes {_name}")
if _SDK_VERSION != SDK_VERSION:  # pragma: no cover - guards an SDK bump
    raise ImportError(f"claude_agent_sdk {_SDK_VERSION} is installed; MP-07 pins {SDK_VERSION}")

Spawn = Callable[..., Awaitable[Any]]


async def _empty_stream() -> AsyncIterator[dict[str, Any]]:
    # Mirrors `ClaudeSDKClient.connect(None)`: never yields, keeps the connection open.
    return
    yield {}


class ExplicitEnvironmentCliTransport(_cli.SubprocessCLITransport):
    """The SDK subprocess transport spawned with an explicit environment, nothing inherited."""

    def __init__(
        self,
        options: ClaudeAgentOptions,
        *,
        environment: Mapping[str, str],
        prompt: AsyncIterable[dict[str, Any]] | None = None,
        spawn: Spawn = anyio.open_process,
        check_version: bool = True,
    ) -> None:
        super().__init__(prompt=prompt if prompt is not None else _empty_stream(), options=options)
        self._environment = dict(environment)
        self._spawn = spawn
        self._check_version = check_version

    def spawn_environment(self) -> dict[str, str]:
        """Exactly what the CLI receives: the explicit environment plus the SDK's markers."""

        process_env: dict[str, str] = {
            **self._environment,
            "CLAUDE_CODE_ENTRYPOINT": "sdk-py",
            **self._options.env,
            "CLAUDE_AGENT_SDK_VERSION": _SDK_VERSION,
        }
        process_env.pop("CLAUDECODE", None)
        reads_state = _cli._SDK_READS_SESSION_STATE_ENV
        if not any(key.upper() == reads_state for key in process_env):
            process_env[reads_state] = "1"
        if self._options.enable_file_checkpointing:
            process_env["CLAUDE_CODE_ENABLE_SDK_FILE_CHECKPOINTING"] = "true"
        if self._cwd:
            process_env["PWD"] = self._cwd
        return process_env

    async def connect(self) -> None:
        if self._process:
            return
        if self._cli_path is None:
            self._cli_path = await anyio.to_thread.run_sync(self._find_cli)
        self._reject_windows_batch_cli(self._cli_path)
        if self._check_version and not self._environment.get("CLAUDE_AGENT_SDK_SKIP_VERSION_CHECK"):
            await self._check_claude_version()
        cmd = self._build_command()
        process_env = self.spawn_environment()
        stderr_dest = PIPE if self._options.stderr is not None else None
        try:
            self._process = await self._spawn(
                cmd,
                stdin=PIPE,
                stdout=PIPE,
                stderr=stderr_dest,
                cwd=self._cwd,
                env=process_env,
                user=self._options.user,
            )
        except FileNotFoundError as error:
            failure: CLIConnectionError = CLINotFoundError(
                f"Claude Code not found at: {self._cli_path}"
            )
            self._exit_error = failure
            raise failure from error
        except Exception as error:
            failure = CLIConnectionError(f"Failed to start Claude Code: {error}")
            self._exit_error = failure
            raise failure from error
        assert self._process is not None
        _cli._ACTIVE_CHILDREN.add(self._process)
        if self._process.stdout:
            self._stdout_stream = TextReceiveStream(self._process.stdout)
        if stderr_dest is PIPE and self._process.stderr:
            self._stderr_stream = TextReceiveStream(self._process.stderr)
            self._stderr_task = spawn_detached(self._handle_stderr())
        if self._process.stdin:
            self._stdin_stream = TextSendStream(self._process.stdin)
        self._ready = True


class SdkClientFactory:
    """`ClaudeClientFactory` over the pinned SDK: one explicit-environment subprocess per
    session. `spawn` and `check_version` exist for tests of the environment only; a real
    session spawns the bundled (or `cli_path`) Claude Code. `gate` is the host gate consulted
    before every client is built (`host.HostUnsupported` on Windows); tests inject one."""

    def __init__(
        self,
        *,
        spawn: Spawn = anyio.open_process,
        check_version: bool = True,
        gate: Callable[[], HostGate] = host_gate,
    ) -> None:
        self._spawn = spawn
        self._check_version = check_version
        self._gate = gate

    @property
    def versions(self) -> Mapping[str, str]:
        return {"claude_agent_sdk": _SDK_VERSION, "claude_code_bundled": BUNDLED_CLI_VERSION}

    def create(
        self, options: ClaudeAgentOptions, *, environment: Mapping[str, str]
    ) -> ClaudeClient:
        # Enforced where the lane spawns: never build a client on an unsupported host.
        self._gate().require()
        configured = _configure_can_use_tool(options)
        transport = ExplicitEnvironmentCliTransport(
            configured,
            environment=environment,
            spawn=self._spawn,
            check_version=self._check_version,
        )
        return ClaudeSDKClient(configured, transport=transport)


__all__ = ["ExplicitEnvironmentCliTransport", "SdkClientFactory"]
