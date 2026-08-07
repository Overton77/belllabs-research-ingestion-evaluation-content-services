"""Qualification-only guarded resume adapter for Block C topology evidence.

This is **not** a Stage 3 / production dispatcher. It exists solely to qualify the
future Stage 3 topology: decide the exact N/N+1 compatibility route first, then
invoke a supplied Agent Server ``runs.wait`` callback only when allowed.

Provider fail-open behavior (direct N→N1 resume mutating schema) is documented
separately by an isolated live regression that intentionally bypasses this adapter.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from typing import Any

from app.agent_server.block_c_qualification.compat import AssemblyRole
from app.agent_server.block_c_qualification.compat_route import (
    CompatibilityRouteDecision,
    require_compatible_resume_route,
)
from app.agent_server.block_c_qualification.deployment_route import (
    DeploymentResumeDecision,
    require_deployment_resume_route,
)

RunsWaitCallback = Callable[..., Awaitable[Any]]


@dataclass(frozen=True)
class GuardedResumeResult:
    """Route decision plus provider result after an allowed dispatch."""

    decision: CompatibilityRouteDecision
    provider_result: Any


@dataclass(frozen=True)
class GuardedDeploymentResumeResult:
    """Deployment route decision plus provider result on the exact N assembly."""

    decision: DeploymentResumeDecision
    provider_result: Any
    dispatched_assembly_role: AssemblyRole


async def guarded_runs_wait(
    *,
    source_graph_id: str,
    source_compat_version: str,
    target_graph_id: str,
    target_compat_version: str,
    runs_wait: RunsWaitCallback,
    thread_id: str,
    assistant_id: str,
    command: Mapping[str, Any] | None = None,
    **wait_kwargs: Any,
) -> GuardedResumeResult:
    """Perform the exact compatibility decision, then optionally call ``runs_wait``.

    On deny, raises ``IncompatibleResumeRouteError`` and never invokes ``runs_wait``.
    On allow, awaits ``runs_wait(thread_id, assistant_id, command=command, **wait_kwargs)``.
    """

    decision = require_compatible_resume_route(
        source_graph_id=source_graph_id,
        source_compat_version=source_compat_version,
        target_graph_id=target_graph_id,
        target_compat_version=target_compat_version,
    )
    provider_result = await runs_wait(
        thread_id,
        assistant_id,
        command=dict(command) if command is not None else None,
        **wait_kwargs,
    )
    return GuardedResumeResult(decision=decision, provider_result=provider_result)


async def guarded_deployment_runs_wait(
    *,
    source_graph_id: str,
    source_compat_version: str,
    inspection_assembly_role: AssemblyRole,
    runs_wait_by_role: Mapping[AssemblyRole, RunsWaitCallback],
    assistant_id_by_graph: Mapping[str, str],
    thread_id: str,
    command: Mapping[str, Any] | None = None,
    **wait_kwargs: Any,
) -> GuardedDeploymentResumeResult:
    """Route an N checkpoint to the exact N assembly ``runs.wait`` callback.

    Even when ``inspection_assembly_role`` is ``n1``, an allowed N checkpoint is
    resumed only via ``runs_wait_by_role["n"]`` and the N graph assistant id.
    The N+1 callback is never invoked for an allowed N resume.
    """

    decision = require_deployment_resume_route(
        source_graph_id=source_graph_id,
        source_compat_version=source_compat_version,
        inspection_assembly_role=inspection_assembly_role,
    )
    try:
        runs_wait = runs_wait_by_role[decision.resume_assembly_role]
    except KeyError as error:
        raise RuntimeError(
            f"no runs.wait callback registered for assembly "
            f"{decision.resume_assembly_role!r}"
        ) from error
    try:
        assistant_id = assistant_id_by_graph[decision.resume_graph_id]
    except KeyError as error:
        raise RuntimeError(
            f"no assistant id registered for resume graph {decision.resume_graph_id!r}"
        ) from error

    provider_result = await runs_wait(
        thread_id,
        assistant_id,
        command=dict(command) if command is not None else None,
        **wait_kwargs,
    )
    return GuardedDeploymentResumeResult(
        decision=decision,
        provider_result=provider_result,
        dispatched_assembly_role=decision.resume_assembly_role,
    )
