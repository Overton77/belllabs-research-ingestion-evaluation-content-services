"""MP-08 layering, version pin, required executables and `compose_codex_local`.

- `adapters/codex` never imports `mission_control.bootstrap`: the child-environment builder
  (`bootstrap.provider_auth.provider_child_environment`) is injected by composition;
- the subprocess launcher refuses a `codex --version` other than the pin (typed
  `CodexVersionMismatch`) before any app-server starts, unless the explicit override is set;
- a `rows` resolver turns on MP-03's required-executables check at `prepare`;
- `compose_codex_local` has the keyword signature the integrator's composition calls.
"""

from __future__ import annotations

import ast
import asyncio
import inspect
from pathlib import Path
from typing import Any

import pytest

from mission_control.adapters.codex import compose as compose_module
from mission_control.adapters.codex import harness as harness_module
from mission_control.adapters.codex import launcher as launcher_module
from mission_control.adapters.codex.compose import compose_codex_local
from mission_control.adapters.codex.launcher import (
    PROVIDER_PIN_MISMATCH,
    CodexVersionMismatch,
    LaunchSpec,
    SubprocessAppServerLauncher,
)
from mission_control.adapters.cursor.projection import LaneProjectionError
from mission_control.bootstrap.provider_auth import provider_child_environment
from mission_control.domain.execution.lanes import PrepareRequest
from tests.unit.codex.drive import fields
from tests.unit.codex.fixture_app_server import FixtureLauncher
from tests.unit.codex.support import (
    MemoryArtifacts,
    StaticAuth,
    StaticProjectionSource,
    TmpLeaser,
    approval_rig,
    codex_stack,
)

PACKAGE = Path(harness_module.__file__).resolve().parent


def test_the_codex_adapter_never_imports_bootstrap() -> None:
    offenders = []
    for path in sorted(PACKAGE.glob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            names: list[str] = []
            if isinstance(node, ast.Import):
                names = [alias.name for alias in node.names]
            elif isinstance(node, ast.ImportFrom) and node.module:
                names = [node.module]
            offenders += [f"{path.name}: {name}" for name in names if "bootstrap" in name]
    assert offenders == []


@pytest.mark.parametrize("observed", ["0.161.0", "0.163.0-alpha", "unverified"])
def test_a_cli_other_than_the_pin_is_refused(observed: str) -> None:
    launcher = SubprocessAppServerLauncher("codex")
    launcher._versions["codex_cli"] = observed
    with pytest.raises(CodexVersionMismatch) as refused:
        launcher.check_version()
    assert refused.value.code == PROVIDER_PIN_MISMATCH
    assert refused.value.observed == observed and refused.value.pinned == "0.162.0"
    override = SubprocessAppServerLauncher("codex", allow_version_mismatch=True)
    override._versions["codex_cli"] = observed
    override.check_version()
    assert override.versions["codex_cli_override"] == "allow_version_mismatch"
    pinned = SubprocessAppServerLauncher("codex")
    pinned._versions["codex_cli"] = "0.162.0"
    pinned.check_version()


async def test_the_launcher_refuses_before_starting_an_app_server(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    launcher = SubprocessAppServerLauncher("codex")
    spawned: list[tuple[Any, ...]] = []

    async def probe(spec: LaunchSpec) -> None:
        launcher._versions["codex_cli"] = "0.150.0"

    async def spawn(*argv: Any, **kwargs: Any) -> Any:
        spawned.append(argv)
        raise AssertionError("no app-server may start for an unpinned CLI")

    monkeypatch.setattr(launcher_module, "subprocess_supported", lambda: (True, "test"))
    monkeypatch.setattr(launcher, "_probe_version", probe)
    monkeypatch.setattr(asyncio, "create_subprocess_exec", spawn)
    with pytest.raises(CodexVersionMismatch):
        await launcher.launch(LaunchSpec(cwd=tmp_path, env={}, codex_home=tmp_path))
    assert spawned == []


async def test_a_rows_resolver_enforces_the_required_executables(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    resolved: list[Any] = []

    async def rows(operation: Any) -> tuple[Any, ...]:
        resolved.append(operation)
        return ()

    def needs(rows: Any, profile: Any) -> dict[str, tuple[str, ...]]:
        assert profile == "codex"
        return {"mc-definitely-missing-launcher-xyz": ("mcp.fixture",)}

    monkeypatch.setattr(harness_module, "required_executables", needs)
    stack = codex_stack(tmp_path, "turn_full")
    harness = harness_module.CodexLocalHarness(
        launcher=stack.launcher,
        leaser=TmpLeaser(tmp_path / "leases-rows"),
        projections=StaticProjectionSource(),
        artifacts=MemoryArtifacts(),
        auth=StaticAuth(),
        settings=harness_module.CodexLocalSettings(lease_root=tmp_path / "leases-rows"),
        child_environment=provider_child_environment,
        rows=rows,
    )
    harness.stage(stack.heid, stack.operation)
    with pytest.raises(LaneProjectionError) as refused:
        await harness.prepare(
            PrepareRequest(
                **fields(stack),
                run_id=stack.operation.identity.run_id,
                operation_id=stack.operation.identity.operation_id,
                attempt_no=1,
            )
        )
    assert refused.value.code == "CAPABILITY_DRIFT"
    assert "mc-definitely-missing-launcher-xyz" in str(refused.value)
    assert len(resolved) == 1 and stack.launcher.launches == []


def test_compose_codex_local_has_the_integrators_signature(tmp_path: Path) -> None:
    parameters = inspect.signature(compose_codex_local).parameters
    assert list(parameters) == [
        "leaser",
        "projections",
        "artifacts",
        "auth",
        "lease_root",
        "inputs",
        "rows",
        "launcher",
        "codex_binary",
        "codex_home_mode",
        "owner_codex_home",
        "default_repository",
        "child_environment",
        "environ",
        "broker",
        "approval_wait_seconds",
        "allow_version_mismatch",
        "approval_reviewers",
        # MP-20: the optional declared-output custody port (additive, defaults to None).
        "outputs",
    ]
    assert all(item.kind is inspect.Parameter.KEYWORD_ONLY for item in parameters.values())
    required = [
        name for name, item in parameters.items() if item.default is inspect.Parameter.empty
    ]
    assert required == [
        "leaser",
        "projections",
        "artifacts",
        "auth",
        "lease_root",
        "child_environment",
    ]
    assert parameters["approval_wait_seconds"].default == 300.0
    assert parameters["allow_version_mismatch"].default is False
    assert "bootstrap" not in inspect.getsource(compose_module).split('"""', 2)[2]


def test_compose_codex_local_wires_the_broker_and_the_pin_override(tmp_path: Path) -> None:
    rig = approval_rig("sha256:" + "a" * 64, review=False)
    harness = compose_codex_local(
        leaser=TmpLeaser(tmp_path / "leases"),
        projections=StaticProjectionSource(),
        artifacts=MemoryArtifacts(),
        auth=StaticAuth(),
        lease_root=tmp_path / "leases",
        child_environment=provider_child_environment,
        environ={"PATH": "/usr/bin"},
        broker=rig.broker,
        approval_wait_seconds=42.0,
        allow_version_mismatch=True,
        approval_reviewers=("approver",),
    )
    assert harness.approvals.broker is rig.broker and harness.approvals.wait_seconds == 42.0
    assert harness.approvals.reviewers == ("approver",)
    assert harness._settings.worker_environ == {"PATH": "/usr/bin"}
    assert harness._settings.require_executables is True
    launcher = harness._launcher
    assert isinstance(launcher, SubprocessAppServerLauncher) and launcher._allow_mismatch
    failing = compose_codex_local(
        leaser=TmpLeaser(tmp_path / "leases"),
        projections=StaticProjectionSource(),
        artifacts=MemoryArtifacts(),
        auth=StaticAuth(),
        lease_root=tmp_path / "leases",
        child_environment=provider_child_environment,
        launcher=FixtureLauncher(),
    )
    assert failing.approvals.broker is None, "no broker composed: approvals fail closed"
    with pytest.raises(ValueError, match="positive"):
        compose_codex_local(
            leaser=TmpLeaser(tmp_path / "leases"),
            projections=StaticProjectionSource(),
            artifacts=MemoryArtifacts(),
            auth=StaticAuth(),
            lease_root=tmp_path / "leases",
            child_environment=provider_child_environment,
            approval_wait_seconds=0,
        )
