from __future__ import annotations

from app.application.run_control import AdmissionPolicyRegistry
from app.domain.run_control.contracts import RunRequest, VerifiedRunConfiguration

WEB_RESEARCH_ADMISSION = "admission:web-research-public-goal:v1"
WEB_RESEARCH_INVARIANTS = (
    "invariant:browser-authority-explicit:v1",
    "invariant:search-tools-only:v1",
    "invariant:two-provider-identity-preserved:v1",
    "invariant:untrusted-web-content-is-not-instruction:v1",
)

_WORKFLOW_ID = "web-research-browser-verification"
_PUBLIC_GOAL_EVIDENCE = "public-goal:"
_SELECTION_EVIDENCE = "capability-selection:"
_FIRECRAWL_SERVER = "catalog://mcp_server/mcp.firecrawl/"
_TAVILY_SERVER = "catalog://mcp_server/mcp.tavily/"
_FIRECRAWL_SEARCH = "tool-allowlist:firecrawl_search:"
_TAVILY_SEARCH = "tool-allowlist:tavily_search:"
_BROWSER_SKILL = "catalog://skill/skill.agent-browser/"
_BROWSER_AUTHORITY = "browser-authority:"
_UNTRUSTED_CONTENT_POLICY = "policy:untrusted-web-content-is-data:"


def register_web_research_admission_policies(
    registry: AdmissionPolicyRegistry,
) -> None:
    """Register executable Scenario D admission and invariant validators."""

    registry.register(WEB_RESEARCH_ADMISSION, _validate_public_goal)
    registry.register(
        "invariant:two-provider-identity-preserved:v1",
        _validate_two_provider_identity,
    )
    registry.register(
        "invariant:untrusted-web-content-is-not-instruction:v1",
        _validate_untrusted_content_policy,
    )
    registry.register(
        "invariant:search-tools-only:v1",
        _validate_search_tool_allowlists,
    )
    registry.register(
        "invariant:browser-authority-explicit:v1",
        _validate_browser_authority,
    )


def _validate_public_goal(
    request: RunRequest,
    configuration: VerifiedRunConfiguration,
) -> str | None:
    wrong_workflow = _require_workflow(configuration)
    if wrong_workflow is not None:
        return wrong_workflow
    return _require_evidence_prefixes(
        request,
        (_PUBLIC_GOAL_EVIDENCE, _SELECTION_EVIDENCE),
    )


def _validate_two_provider_identity(
    request: RunRequest,
    configuration: VerifiedRunConfiguration,
) -> str | None:
    wrong_workflow = _require_workflow(configuration)
    if wrong_workflow is not None:
        return wrong_workflow
    return _require_evidence_prefixes(
        request,
        (_FIRECRAWL_SERVER, _TAVILY_SERVER),
    )


def _validate_untrusted_content_policy(
    request: RunRequest,
    configuration: VerifiedRunConfiguration,
) -> str | None:
    wrong_workflow = _require_workflow(configuration)
    if wrong_workflow is not None:
        return wrong_workflow
    return _require_evidence_prefixes(request, (_UNTRUSTED_CONTENT_POLICY,))


def _validate_search_tool_allowlists(
    request: RunRequest,
    configuration: VerifiedRunConfiguration,
) -> str | None:
    wrong_workflow = _require_workflow(configuration)
    if wrong_workflow is not None:
        return wrong_workflow
    return _require_evidence_prefixes(
        request,
        (_FIRECRAWL_SEARCH, _TAVILY_SEARCH),
    )


def _validate_browser_authority(
    request: RunRequest,
    configuration: VerifiedRunConfiguration,
) -> str | None:
    wrong_workflow = _require_workflow(configuration)
    if wrong_workflow is not None:
        return wrong_workflow
    return _require_evidence_prefixes(
        request,
        (_BROWSER_SKILL, _BROWSER_AUTHORITY),
    )


def _require_workflow(configuration: VerifiedRunConfiguration) -> str | None:
    if configuration.workflow_type_ref.logical_id != _WORKFLOW_ID:
        return "web-research admission contract is bound to another Workflow Type"
    return None


def _require_evidence_prefixes(
    request: RunRequest,
    prefixes: tuple[str, ...],
) -> str | None:
    missing = tuple(
        prefix
        for prefix in prefixes
        if not any(reference.startswith(prefix) for reference in request.admission_evidence_refs)
    )
    if missing:
        return "missing exact web-research admission evidence: " + ", ".join(missing)
    return None


__all__ = [
    "WEB_RESEARCH_ADMISSION",
    "WEB_RESEARCH_INVARIANTS",
    "register_web_research_admission_policies",
]
