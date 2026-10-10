"""MP-07: the production client factory spawns the CLI with the explicit child environment
(nothing inherited), the SDK command line and the `stdio` permission prompt tool. The spawn
is a FIXTURE: no Claude Code runs here."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from claude_agent_sdk.client import ClaudeSDKClient
from claude_agent_sdk.types import ClaudeAgentOptions, PermissionResultAllow

from mission_control.adapters.claude.host import (
    LANE_UNSUPPORTED_OS,
    HostGate,
    HostUnsupported,
    host_gate,
)
from mission_control.adapters.claude.transport import (
    ExplicitEnvironmentCliTransport,
    SdkClientFactory,
)
from mission_control.bootstrap.provider_auth import provider_child_environment
from tests.unit.claude.fixtures import FIXTURE_ENVIRON, fixture_admission


class _FakeProcess:
    stdin = None
    stdout = None
    stderr = None


@pytest.fixture
def spawned() -> dict[str, Any]:
    return {}


def _spawn(spawned: dict[str, Any]) -> Any:
    async def spawn(cmd: list[str], **kwargs: Any) -> _FakeProcess:
        spawned["cmd"] = cmd
        spawned["kwargs"] = kwargs
        return _FakeProcess()

    return spawn


def _linux() -> HostGate:
    return host_gate(system="Linux", release="6.6.87.2-microsoft-standard-WSL2", event_loop="none")


async def _can_use_tool(*_args: Any) -> PermissionResultAllow:
    return PermissionResultAllow()


async def test_the_spawn_environment_is_the_admitted_child_environment_only(
    tmp_path: Path, spawned: dict[str, Any]
) -> None:
    cli = tmp_path / "claude"
    cli.write_text("#!/bin/sh\n", encoding="utf-8")
    environment = provider_child_environment(
        FIXTURE_ENVIRON, fixture_admission(), extra={"CLAUDE_CONFIG_DIR": str(tmp_path / "cfg")}
    )
    options = ClaudeAgentOptions(
        cwd=str(tmp_path),
        cli_path=str(cli),
        setting_sources=["project"],
        resume="sess-fixture-claude-0001",
        env={"CLAUDE_CODE_PRINT_BG_WAIT_CEILING_MS": "1000"},
    )
    transport = ExplicitEnvironmentCliTransport(
        options, environment=environment, spawn=_spawn(spawned), check_version=False
    )
    await transport.connect()
    env = spawned["kwargs"]["env"]
    assert "ANTHROPIC_API_KEY" not in env and "ANTHROPIC_AUTH_TOKEN" not in env
    assert env["OPENAI_API_KEY"] == "sk-FIXTURE-openai", "only the admission's unset names go"
    assert env["CLAUDE_CONFIG_DIR"] == str(tmp_path / "cfg")
    assert env["CLAUDE_CODE_ENTRYPOINT"] == "sdk-py"
    assert env["CLAUDE_AGENT_SDK_VERSION"] == "0.2.165"
    assert env["CLAUDE_CODE_SDK_READS_SESSION_STATE"] == "1"
    assert env["CLAUDE_CODE_PRINT_BG_WAIT_CEILING_MS"] == "1000", "options.env still applies"
    assert env["PWD"] == str(tmp_path)
    assert "CLAUDECODE" not in env
    cmd = spawned["cmd"]
    assert cmd[0] == str(cli)
    assert "--resume=sess-fixture-claude-0001" in cmd
    assert "--setting-sources=project" in cmd
    assert cmd[-2:] == ["--input-format", "stream-json"]
    assert spawned["kwargs"]["cwd"] == str(tmp_path)
    assert transport.is_ready()


def test_the_factory_routes_permission_prompts_over_stdio_and_keeps_the_transport(
    tmp_path: Path, spawned: dict[str, Any]
) -> None:
    factory = SdkClientFactory(spawn=_spawn(spawned), check_version=False, gate=_linux)
    assert factory.versions == {"claude_agent_sdk": "0.2.165", "claude_code_bundled": "2.1.294"}
    options = ClaudeAgentOptions(cwd=str(tmp_path), can_use_tool=_can_use_tool)
    client = factory.create(options, environment={"PATH": "/usr/bin"})
    assert isinstance(client, ClaudeSDKClient)
    transport = client._custom_transport
    assert isinstance(transport, ExplicitEnvironmentCliTransport)
    # `_configure_can_use_tool` ran before the transport was built (client.py skips it for a
    # custom transport), so the CLI gets `--permission-prompt-tool stdio`.
    assert client.options.permission_prompt_tool_name == "stdio"
    assert transport.spawn_environment()["CLAUDE_AGENT_SDK_VERSION"] == "0.2.165"
    assert "ANTHROPIC_API_KEY" not in transport.spawn_environment()


# --- the host gate (injected platform facts) ------------------------------------------------------


@pytest.mark.parametrize(
    ("system", "release", "loop", "wsl"),
    [
        ("Linux", "6.6.87.2-microsoft-standard-WSL2", "SelectorEventLoop", True),
        ("Linux", "6.8.0-45-generic", "_UnixSelectorEventLoop", False),
        ("Darwin", "24.1.0", "none", False),
    ],
)
def test_linux_wsl_and_macos_hosts_are_supported(
    system: str, release: str, loop: str, wsl: bool
) -> None:
    gate = host_gate(system=system, release=release, event_loop=loop)
    assert gate.supported and gate.code is None and gate.wsl is wsl
    assert gate.require() is gate


@pytest.mark.parametrize("loop", ["SelectorEventLoop", "ProactorEventLoop", "none"])
def test_a_windows_host_is_refused_whatever_the_loop(loop: str) -> None:
    gate = host_gate(system="Windows", release="11", event_loop=loop)
    assert not gate.supported and gate.code == LANE_UNSUPPORTED_OS
    assert "SelectorEventLoop" in gate.reason and "WSL 2 or Linux" in gate.remedy
    with pytest.raises(HostUnsupported) as refused:
        gate.require()
    assert refused.value.code == LANE_UNSUPPORTED_OS
    assert str(refused.value).startswith(LANE_UNSUPPORTED_OS)
    if loop == "ProactorEventLoop":
        assert "UNVERIFIED" in gate.reason


def test_an_unknown_system_is_refused() -> None:
    gate = host_gate(system="FreeBSD", release="14.1", event_loop="none")
    assert not gate.supported and "not a qualified lane host" in gate.reason


def test_the_factory_never_builds_a_client_on_an_unsupported_host(
    tmp_path: Path, spawned: dict[str, Any]
) -> None:
    factory = SdkClientFactory(
        spawn=_spawn(spawned),
        check_version=False,
        gate=lambda: host_gate(system="Windows", release="11", event_loop="SelectorEventLoop"),
    )
    with pytest.raises(HostUnsupported):
        factory.create(ClaudeAgentOptions(cwd=str(tmp_path)), environment={"PATH": "/usr/bin"})
    assert spawned == {}, "nothing was spawned"
