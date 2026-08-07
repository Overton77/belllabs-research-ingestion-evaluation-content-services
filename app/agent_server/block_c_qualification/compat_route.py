"""Pure Block C qualification fixture for pre-dispatch N/N+1 resume routing.

This is not a Stage 3 runtime service. Callers must invoke it before any
Agent Server resume against a checkpoint so incompatible assistants are never
submitted to the provider (which may fail open).
"""

from __future__ import annotations

from dataclasses import dataclass

from app.agent_server.block_c_qualification.compat import (
    COMPAT_VERSION_N,
    COMPAT_VERSION_N1,
    GRAPH_ID_N,
    GRAPH_ID_N1,
)


@dataclass(frozen=True)
class CompatibilityRouteDecision:
    """Exact pre-dispatch allow/deny decision for a resume route."""

    allowed: bool
    reason: str
    source_graph_id: str
    source_compat_version: str
    target_graph_id: str
    target_compat_version: str


def decide_resume_route(
    *,
    source_graph_id: str,
    source_compat_version: str,
    target_graph_id: str,
    target_compat_version: str,
) -> CompatibilityRouteDecision:
    """Decide whether a checkpoint may be resumed on a target assistant/graph.

    Accepted BellLabs Block C policy for this qualification fixture:
    - exact N-on-N (same graph id + compat version) may resume;
    - N checkpoint to N+1 assistant/graph must be rejected before dispatch;
    - any other mismatch is rejected.
    """

    source_graph_id = source_graph_id.strip()
    source_compat_version = source_compat_version.strip()
    target_graph_id = target_graph_id.strip()
    target_compat_version = target_compat_version.strip()

    if (
        source_graph_id == GRAPH_ID_N
        and source_compat_version == COMPAT_VERSION_N
        and target_graph_id == GRAPH_ID_N
        and target_compat_version == COMPAT_VERSION_N
    ):
        return CompatibilityRouteDecision(
            allowed=True,
            reason="exact N-on-N resume route",
            source_graph_id=source_graph_id,
            source_compat_version=source_compat_version,
            target_graph_id=target_graph_id,
            target_compat_version=target_compat_version,
        )

    if (
        source_graph_id == GRAPH_ID_N
        and source_compat_version == COMPAT_VERSION_N
        and (
            target_graph_id == GRAPH_ID_N1
            or target_compat_version == COMPAT_VERSION_N1
        )
    ):
        return CompatibilityRouteDecision(
            allowed=False,
            reason=(
                "N checkpoint must not be submitted to N+1 assistant; "
                "provider may fail open and mutate schema"
            ),
            source_graph_id=source_graph_id,
            source_compat_version=source_compat_version,
            target_graph_id=target_graph_id,
            target_compat_version=target_compat_version,
        )

    return CompatibilityRouteDecision(
        allowed=False,
        reason="incompatible graph/compat resume route",
        source_graph_id=source_graph_id,
        source_compat_version=source_compat_version,
        target_graph_id=target_graph_id,
        target_compat_version=target_compat_version,
    )


class IncompatibleResumeRouteError(ValueError):
    """Raised when BellLabs policy rejects a resume before Agent Server dispatch."""

    def __init__(self, decision: CompatibilityRouteDecision) -> None:
        self.decision = decision
        super().__init__(decision.reason)


def require_compatible_resume_route(
    *,
    source_graph_id: str,
    source_compat_version: str,
    target_graph_id: str,
    target_compat_version: str,
) -> CompatibilityRouteDecision:
    """Return an allow decision or raise before any Agent Server invocation."""

    decision = decide_resume_route(
        source_graph_id=source_graph_id,
        source_compat_version=source_compat_version,
        target_graph_id=target_graph_id,
        target_compat_version=target_compat_version,
    )
    if not decision.allowed:
        raise IncompatibleResumeRouteError(decision)
    return decision
