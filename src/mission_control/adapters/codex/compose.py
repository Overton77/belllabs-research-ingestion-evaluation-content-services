"""Composition helpers for the `codex` lane (MP-08).

`bootstrap/**` and `adapters/temporal/deployment_composition.py` are integrator-owned; the
integrator's `compose_codex_local_lane` calls `compose_codex_local` below with the worker's
pieces: the workspace leaser, the projection source and its catalog `rows` resolver (MP-03
required executables), the artifact sink, MP-05's auth admission (`AdmissionServiceSource`),
`bootstrap.provider_auth.provider_child_environment` as `child_environment` (adapters never
import bootstrap) and the worker's MP-11 `ApprovalBroker`. Without a broker the lane's
approvals fail closed; a `codex --version` other than the pin is refused at start unless
`allow_version_mismatch` is passed explicitly.
"""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path

from mission_control.adapters.codex.approvals import (
    DEFAULT_APPROVAL_REVIEWERS,
    DEFAULT_APPROVAL_WAIT_SECONDS,
)
from mission_control.adapters.codex.harness import (
    ArtifactSink,
    AuthAdmissionSource,
    CodexHomeMode,
    CodexLocalHarness,
    CodexLocalSettings,
    WorkspaceLeaser,
)
from mission_control.adapters.codex.launcher import (
    AppServerLauncher,
    ChildEnvironmentBuilder,
    SubprocessAppServerLauncher,
)
from mission_control.adapters.cursor.projection import (
    DurableInputReader,
    ProjectionSource,
    RowsResolver,
)
from mission_control.application.execution.approvals_broker import ApprovalBroker
from mission_control.application.execution.auth_admission import AuthAdmission, AuthAdmissionService
from mission_control.application.execution.operations.lane_outputs import LaneOutputCustody
from mission_control.domain.execution.bindings import ProviderExecutionBinding


class AdmissionServiceSource:
    """`AuthAdmissionSource` over MP-05's `AuthAdmissionService` (profile from the binding)."""

    def __init__(self, service: AuthAdmissionService) -> None:
        self._service = service

    async def admit(self, binding: ProviderExecutionBinding) -> AuthAdmission:
        return await self._service.admit("codex", binding.auth.profile)


def compose_codex_local(
    *,
    leaser: WorkspaceLeaser,
    projections: ProjectionSource,
    artifacts: ArtifactSink,
    auth: AuthAdmissionSource,
    lease_root: Path,
    inputs: DurableInputReader | None = None,
    rows: RowsResolver | None = None,
    launcher: AppServerLauncher | None = None,
    codex_binary: str = "codex",
    codex_home_mode: CodexHomeMode = "isolated",
    owner_codex_home: Path | None = None,
    default_repository: str | None = None,
    child_environment: ChildEnvironmentBuilder,
    environ: Mapping[str, str] | None = None,
    broker: ApprovalBroker | None = None,
    approval_wait_seconds: float = DEFAULT_APPROVAL_WAIT_SECONDS,
    allow_version_mismatch: bool = False,
    approval_reviewers: tuple[str, ...] = DEFAULT_APPROVAL_REVIEWERS,
    outputs: LaneOutputCustody | None = None,
) -> CodexLocalHarness:
    """The `codex` harness.

    `rows` enables MP-03's required-executables check (production passes the catalog rows
    resolver); `environ` is the worker environment the child's allow-listed names are read
    from (`os.environ` when None); `broker` is the worker's MP-11 approval broker (approvals
    fail closed without one); `approval_wait_seconds` bounds each native approval wait and
    must stay below the `lane.turn` segment budget; `allow_version_mismatch` is the explicit
    override of the `codex-cli 0.162.0` pin (only for the subprocess launcher built here);
    `approval_reviewers` names who may resolve a codex approval task (a principal or a
    `reviewer:<role>` grant; the deployment's `mission_control_approval_reviewers`).
    """

    if approval_wait_seconds <= 0:
        raise ValueError("approval_wait_seconds is a positive bound")
    if not approval_reviewers or not all(approval_reviewers):
        raise ValueError("approval_reviewers names at least one non-empty reviewer")
    settings = CodexLocalSettings(
        lease_root=lease_root,
        codex_home_mode=codex_home_mode,
        owner_codex_home=owner_codex_home,
        default_repository=default_repository,
        worker_environ=environ,
        approval_wait_seconds=approval_wait_seconds,
        approval_reviewers=tuple(approval_reviewers),
    )
    return CodexLocalHarness(
        launcher=launcher
        or SubprocessAppServerLauncher(codex_binary, allow_version_mismatch=allow_version_mismatch),
        leaser=leaser,
        projections=projections,
        artifacts=artifacts,
        auth=auth,
        settings=settings,
        child_environment=child_environment,
        inputs=inputs,
        rows=rows,
        broker=broker,
        outputs=outputs,
    )


__all__ = ["AdmissionServiceSource", "compose_codex_local"]
