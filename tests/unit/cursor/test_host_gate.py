"""MP-09: the `cursor_local` Windows/WSL host gate, enforced where the lane spawns.

The rule is MP-22's (`bootstrap/preflight.LANE_HOSTS["cursor_local"]`): Linux (WSL 2
included) and macOS; Windows is refused because the worker pins a SelectorEventLoop and
asyncio spawns subprocesses only on the Proactor loop. Nothing here spawns a bridge.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from pydantic import SecretStr

from mission_control.adapters.cursor.bridge import (
    LANE_UNSUPPORTED_OS,
    HostGate,
    HostUnsupported,
    SdkBridgeLauncher,
    host_gate,
)
from mission_control.adapters.cursor.projection import LaneProjectionError
from mission_control.application.execution.harness.lane_turns import LaneExecutionIdentity
from mission_control.domain.execution.lanes import PrepareRequest, StartRequest
from tests.fixtures.cursor_controls import harness_fields
from tests.fixtures.cursor_local import local_stack


def test_windows_is_refused_whatever_the_loop_and_posix_hosts_pass() -> None:
    selector = host_gate(system="Windows", release="11", event_loop="SelectorEventLoop")
    assert not selector.supported and selector.code == LANE_UNSUPPORTED_OS
    assert "SelectorEventLoop" in selector.reason and "WSL" in selector.remedy
    proactor = host_gate(system="Windows", release="11", event_loop="ProactorEventLoop")
    assert not proactor.supported and "UNVERIFIED" in proactor.reason
    wsl = host_gate(
        system="Linux", release="5.15.153.1-microsoft-standard-WSL2", event_loop="SelectorEventLoop"
    )
    assert wsl.supported and wsl.wsl and wsl.code is None
    linux = host_gate(system="Linux", release="6.8.0", event_loop="none")
    assert linux.supported and not linux.wsl
    assert host_gate(system="Darwin", release="24.0", event_loop="none").supported
    other = host_gate(system="FreeBSD", release="14", event_loop="none")
    assert not other.supported and other.code == LANE_UNSUPPORTED_OS
    assert host_gate().system in {"Windows", "Linux", "Darwin"} or not host_gate().supported


async def test_the_sdk_launcher_refuses_to_spawn_on_windows_before_touching_the_sdk(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import platform

    monkeypatch.setattr(platform, "system", lambda: "Windows")
    launcher = SdkBridgeLauncher(SecretStr("not-a-real-key"))
    assert not launcher.host_gate().supported
    with pytest.raises(HostUnsupported, match=LANE_UNSUPPORTED_OS):
        await launcher.launch(workspace=tmp_path, state_root=tmp_path / "state")
    assert not (tmp_path / "state").exists(), "nothing is created before the gate"


class _GatedLauncher:
    """The replay launcher behind a host gate the test flips (FIXTURE)."""

    def __init__(self, inner: Any) -> None:
        self.inner = inner
        self.supported = True

    def host_gate(self) -> HostGate:
        if self.supported:
            return host_gate(system="Linux", release="6.8.0", event_loop="none")
        return host_gate(system="Windows", release="11", event_loop="SelectorEventLoop")

    def __getattr__(self, name: str) -> Any:
        return getattr(self.inner, name)


async def test_the_lane_refuses_an_unsupported_host_before_leasing_and_before_spawning(
    tmp_path: Path,
) -> None:
    stack = local_stack(tmp_path)
    gated = _GatedLauncher(stack.launcher)
    stack.harness._launcher = gated  # type: ignore[assignment]
    heid = str(LaneExecutionIdentity.of(stack.operation, "cursor_local", 1).harness_execution_id)
    fields = harness_fields(stack.operation, heid, "cursor_local")
    stack.harness.stage(heid, stack.operation)
    prepare = PrepareRequest(
        **fields,
        run_id=stack.operation.identity.run_id,
        operation_id=stack.operation.identity.operation_id,
        attempt_no=1,
    )
    gated.supported = False
    with pytest.raises(LaneProjectionError) as refused:
        await stack.harness.prepare(prepare)
    assert refused.value.code == LANE_UNSUPPORTED_OS and "WSL" in str(refused.value)
    assert stack.leases._leases == {}, "no lease is taken on a refused host"
    assert stack.launcher.launches == []

    gated.supported = True
    prepared = await stack.harness.prepare(prepare)
    assert len(stack.leases._leases) == 1
    gated.supported = False
    with pytest.raises(LaneProjectionError) as at_start:
        await stack.harness.start(StartRequest(**fields, prepared=prepared))
    assert at_start.value.code == LANE_UNSUPPORTED_OS
    assert stack.launcher.launches == [], "the gate runs before the bridge is launched"
    assert stack.harness.host_gate() is not None and not stack.harness.host_gate().supported  # type: ignore[union-attr]


def test_a_launcher_without_a_gate_is_not_gated(tmp_path: Path) -> None:
    stack = local_stack(tmp_path)
    assert stack.harness.host_gate() is None, "the replay launcher spawns nothing"
