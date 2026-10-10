"""Production composition of the local provider lanes and their native approvals (MP-07,
MP-08, MP-11).

One worker process holds one `ApprovalBroker` whose `connection_ref` is the worker's session
owner ref (the identity MP-06 fences native sessions with), so a native permission request is
bound to a durable Human Task before any wait and a restarted worker never answers an old
handle. The Claude and Codex lanes are composed only when the deployment opts in, an MP-05
auth profile document is configured and the host can spawn their subprocesses (Linux/WSL);
otherwise they stay unregistered and the registry refuses the profile. Nothing here is
qualified: the registry admits these lanes only under the local-proof policy.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import asyncpg

from mission_control.adapters.claude.compose import broker_permissions, compose_claude_local
from mission_control.adapters.claude.continuation import claude_continuation_registration
from mission_control.adapters.claude.harness import ClaudeAgentSdkHarness, ClaudeLaneSettings
from mission_control.adapters.claude.host import host_gate as claude_host_gate
from mission_control.adapters.claude.workspace import AllocatedWorkspace
from mission_control.adapters.codex.compose import AdmissionServiceSource, compose_codex_local
from mission_control.adapters.codex.continuation import codex_continuation_registration
from mission_control.adapters.codex.harness import CodexLocalHarness
from mission_control.adapters.cursor.projection import RenderedProjectionSource
from mission_control.adapters.cursor.workspace import GitWorktreeLeaser
from mission_control.adapters.postgres.approvals.context import PostgresApprovalContextProbe
from mission_control.adapters.postgres.approvals.correlations import (
    PostgresApprovalCorrelationRepository,
)
from mission_control.adapters.postgres.approvals.tasks import PostgresApprovalTaskRepository
from mission_control.adapters.postgres.control_plane.definition_repository import (
    PostgresDefinitionRepository,
)
from mission_control.adapters.postgres.lanes.hook_tokens import PostgresHookIntentLedger
from mission_control.adapters.postgres.lanes.workspace_leases import PostgresWorkspaceLeaseStore
from mission_control.adapters.postgres.run_control.stop_fence import PostgresStopFenceRepository
from mission_control.adapters.storage.context_files import PayloadContextFiles
from mission_control.adapters.workspaces.git_workspaces import GitWorkspaceBackend
from mission_control.application.artifacts.artifact_promotion import ArtifactPayloadPort
from mission_control.application.authoring.provider_launch import (
    CatalogProjectionRows,
    ProviderBindingRows,
)
from mission_control.application.context.hydrators import LaneContinuationRegistration
from mission_control.application.execution.approvals_broker import ApprovalBroker
from mission_control.application.execution.operations.lane_outputs import LaneOutputCustody
from mission_control.application.workspaces.service import WorkspaceAllocator
from mission_control.bootstrap.provider_auth import (
    compose_auth_admission,
    provider_child_environment,
)
from mission_control.bootstrap.settings import PROJECT_ROOT, Settings

DEFAULT_LANE_WORKSPACE_ROOT = PROJECT_ROOT / ".workspaces"


def compose_approval_broker(pool: asyncpg.Pool, *, connection_ref: str) -> ApprovalBroker:
    """MP-11: the worker's one broker over the release 1.2.0 approval tables."""

    return ApprovalBroker(
        PostgresApprovalTaskRepository(pool),
        PostgresApprovalCorrelationRepository(pool),
        probe=PostgresApprovalContextProbe(pool),
        connection_ref=connection_ref,
        fences=PostgresStopFenceRepository(pool),
    )


def _lease_root(settings: Settings, configured: Path | None, name: str) -> Path:
    return configured or (
        (settings.deep_agent_sandbox_workspace_root or DEFAULT_LANE_WORKSPACE_ROOT) / name
    )


def compose_claude_local_lane(
    settings: Settings,
    pool: asyncpg.Pool,
    payloads: ArtifactPayloadPort,
    *,
    broker: ApprovalBroker | None,
    worker_ref: str,
    outputs: LaneOutputCustody | None = None,
) -> ClaudeAgentSdkHarness | None:
    """The `claude_agent_sdk` lane (MP-07) when opted in, authenticated and spawnable here."""

    if not settings.mission_control_claude_lane:
        return None
    auth = compose_auth_admission(settings)
    gate = claude_host_gate()
    if auth is None or not gate.supported:
        return None
    root = _lease_root(settings, settings.mission_control_claude_lease_root, "claude-leases")
    leases = PostgresWorkspaceLeaseStore(pool)
    files = PayloadContextFiles(payloads)
    scope = settings.mission_control_catalog_scope or ""
    # The rows of the capability pins the `mc.execution_binding.v2` carries, as the launch
    # author rendered them (so `prepare` recomputes the bound materialization digest).
    rows = ProviderBindingRows(
        CatalogProjectionRows(PostgresDefinitionRepository(pool, catalog_scope=scope))
    )
    return compose_claude_local(
        workspaces=AllocatedWorkspace(
            WorkspaceAllocator(
                ledger=leases,
                backend=GitWorkspaceBackend(root),
                root=root,
                allocator_ref=f"worker:{worker_ref}",
            ),
            leases,
            files,
        ),
        projections=RenderedProjectionSource(rows),
        rows=rows,
        auth=auth,
        child_environment=provider_child_environment,
        environ=os.environ,
        fences=PostgresStopFenceRepository(pool),
        intents=PostgresHookIntentLedger(pool),
        inputs=files,
        settings=ClaudeLaneSettings(
            init_timeout_s=settings.mission_control_claude_init_timeout_s,
            drain_timeout_s=settings.mission_control_claude_drain_timeout_s,
        ),
        permissions=(
            broker_permissions(
                broker,
                wait_seconds=settings.mission_control_approval_wait_s,
                reviewers=settings.mission_control_approval_reviewers,
            )
            if broker is not None
            else None
        ),
        host=gate,
        outputs=outputs,
    )


def compose_codex_local_lane(
    settings: Settings,
    pool: asyncpg.Pool,
    payloads: ArtifactPayloadPort,
    *,
    broker: ApprovalBroker | None,
    outputs: LaneOutputCustody | None = None,
) -> CodexLocalHarness | None:
    """The `codex` lane (MP-08) when opted in, authenticated and spawnable here (the
    app-server is a stdio subprocess; a Windows worker cannot spawn it)."""

    if not settings.mission_control_codex_lane or sys.platform == "win32":
        return None
    auth = compose_auth_admission(settings)
    if auth is None:
        return None
    root = _lease_root(settings, settings.mission_control_codex_lease_root, "codex-leases")
    scope = settings.mission_control_catalog_scope or ""
    # The rows of the capability pins the `mc.execution_binding.v2` carries, as the launch
    # author rendered them (so `prepare` recomputes the bound materialization digest).
    rows = ProviderBindingRows(
        CatalogProjectionRows(PostgresDefinitionRepository(pool, catalog_scope=scope))
    )
    files = PayloadContextFiles(payloads)
    return compose_codex_local(
        leaser=GitWorktreeLeaser(PostgresWorkspaceLeaseStore(pool), lease_root=root),
        projections=RenderedProjectionSource(rows),
        rows=rows,
        artifacts=files,
        inputs=files,
        auth=AdmissionServiceSource(auth),
        lease_root=root,
        codex_binary=settings.mission_control_codex_binary,
        codex_home_mode=settings.mission_control_codex_home_mode,
        owner_codex_home=settings.mission_control_codex_owner_home,
        default_repository=settings.mission_control_codex_repository,
        child_environment=provider_child_environment,
        environ=os.environ,
        broker=broker,
        approval_wait_seconds=settings.mission_control_approval_wait_s,
        allow_version_mismatch=settings.mission_control_codex_allow_version_mismatch,
        approval_reviewers=settings.mission_control_approval_reviewers,
        outputs=outputs,
    )


def provider_registrations(
    claude: ClaudeAgentSdkHarness | None,
    codex: CodexLocalHarness | None,
    snapshot_store: object,
) -> tuple[LaneContinuationRegistration, ...]:
    """MP-12: the local provider lanes' continuation registrations (never qualified)."""

    registrations: list[LaneContinuationRegistration] = []
    if claude is not None:
        registrations.append(claude_continuation_registration(claude, snapshot_store))  # type: ignore[arg-type]
    if codex is not None:
        registrations.append(codex_continuation_registration(codex, snapshot_store))  # type: ignore[arg-type]
    return tuple(registrations)


__all__ = [
    "compose_approval_broker",
    "compose_claude_local_lane",
    "compose_codex_local_lane",
    "provider_registrations",
]
