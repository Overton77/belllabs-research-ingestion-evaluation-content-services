"""Qualification-only N/N+1 deployment resume routing (not Stage 3 production).

Maps an interrupted checkpoint to the exact assembly/endpoint that may resume it.
N checkpoints always resume on the N assembly (port 8133 topology), never on the
N+1 deployment (port 8134).

Pinned separate-deployment note: N+1 may ``threads.get`` a shared thread id, but
``threads.get_state`` / incompatible resume fail-closed with
``Graph 'block_c_qualification' not found`` because only N1 is registered there.
"""

from __future__ import annotations

from dataclasses import dataclass

from app.agent_server.block_c_qualification.compat import (
    ASSEMBLY_N,
    ASSEMBLY_N1,
    ASSEMBLY_ROLE_N,
    ASSEMBLY_ROLE_N1,
    COMPAT_VERSION_N,
    COMPAT_VERSION_N1,
    GRAPH_ID_N,
    GRAPH_ID_N1,
    AssemblyRole,
)


@dataclass(frozen=True)
class DeploymentResumeDecision:
    """Exact assembly routing decision for a checkpoint resume."""

    allowed: bool
    reason: str
    source_graph_id: str
    source_compat_version: str
    inspection_assembly_role: AssemblyRole
    resume_assembly_role: AssemblyRole
    resume_assembly_id: str
    resume_graph_id: str
    resume_compat_version: str


class IncompatibleDeploymentResumeError(ValueError):
    """Raised when deployment policy rejects a resume before Agent Server dispatch."""

    def __init__(self, decision: DeploymentResumeDecision) -> None:
        self.decision = decision
        super().__init__(decision.reason)


def decide_deployment_resume_route(
    *,
    source_graph_id: str,
    source_compat_version: str,
    inspection_assembly_role: AssemblyRole,
) -> DeploymentResumeDecision:
    """Decide where an interrupted checkpoint may be resumed.

    Accepted Block C deployment policy for this qualification fixture:
    - N checkpoint + N compat always resumes on assembly N / graph N;
    - inspection from N+1 does not authorize local N+1 resume;
    - N+1-origin checkpoints are out of scope for this fixture (deny).
    """

    source_graph_id = source_graph_id.strip()
    source_compat_version = source_compat_version.strip()
    if inspection_assembly_role not in {ASSEMBLY_ROLE_N, ASSEMBLY_ROLE_N1}:
        raise ValueError(
            f"inspection_assembly_role must be {ASSEMBLY_ROLE_N!r} or "
            f"{ASSEMBLY_ROLE_N1!r}, got {inspection_assembly_role!r}"
        )
    role: AssemblyRole = inspection_assembly_role

    if source_graph_id == GRAPH_ID_N and source_compat_version == COMPAT_VERSION_N:
        return DeploymentResumeDecision(
            allowed=True,
            reason=(
                "N checkpoint resumes only on exact N assembly/endpoint; "
                "never submit to N+1 deployment"
            ),
            source_graph_id=source_graph_id,
            source_compat_version=source_compat_version,
            inspection_assembly_role=role,
            resume_assembly_role=ASSEMBLY_ROLE_N,
            resume_assembly_id=ASSEMBLY_N,
            resume_graph_id=GRAPH_ID_N,
            resume_compat_version=COMPAT_VERSION_N,
        )

    if source_graph_id == GRAPH_ID_N1 or source_compat_version == COMPAT_VERSION_N1:
        return DeploymentResumeDecision(
            allowed=False,
            reason=(
                "N+1-origin checkpoint resume is out of scope for Block C "
                f"qualification (inspection={role})"
            ),
            source_graph_id=source_graph_id,
            source_compat_version=source_compat_version,
            inspection_assembly_role=role,
            resume_assembly_role=ASSEMBLY_ROLE_N1,
            resume_assembly_id=ASSEMBLY_N1,
            resume_graph_id=GRAPH_ID_N1,
            resume_compat_version=COMPAT_VERSION_N1,
        )

    return DeploymentResumeDecision(
        allowed=False,
        reason="unknown checkpoint compatibility for deployment resume routing",
        source_graph_id=source_graph_id,
        source_compat_version=source_compat_version,
        inspection_assembly_role=role,
        resume_assembly_role=ASSEMBLY_ROLE_N,
        resume_assembly_id=ASSEMBLY_N,
        resume_graph_id=source_graph_id,
        resume_compat_version=source_compat_version,
    )


def require_deployment_resume_route(
    *,
    source_graph_id: str,
    source_compat_version: str,
    inspection_assembly_role: AssemblyRole,
) -> DeploymentResumeDecision:
    """Return an allow decision or raise before any Agent Server invocation."""

    decision = decide_deployment_resume_route(
        source_graph_id=source_graph_id,
        source_compat_version=source_compat_version,
        inspection_assembly_role=inspection_assembly_role,
    )
    if not decision.allowed:
        raise IncompatibleDeploymentResumeError(decision)
    return decision
