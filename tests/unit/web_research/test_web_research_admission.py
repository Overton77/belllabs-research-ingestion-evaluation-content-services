from __future__ import annotations

from datetime import UTC, datetime

import pytest

from app.application.run_control.service import AdmissionPolicyRegistry
from app.application.run_control.web_research_admission import (
    WEB_RESEARCH_ADMISSION,
    WEB_RESEARCH_INVARIANTS,
    register_web_research_admission_policies,
)
from app.domain.control_plane.canonical import sha256_digest
from app.domain.control_plane.contracts import (
    DefinitionKind,
    ExactDefinitionRef,
    RunInputManifestRef,
)
from app.domain.run_control.contracts import (
    ActorContext,
    BudgetApplicability,
    BudgetDimensionLimit,
    BudgetEnvelope,
    RunRequest,
    VerifiedRunConfiguration,
)
from app.domain.run_control.errors import AdmissionRejected

NOW = datetime(2026, 7, 26, 16, 0, tzinfo=UTC)


def _configuration(
    logical_id: str = "web-research-browser-verification",
) -> VerifiedRunConfiguration:
    workflow_ref = ExactDefinitionRef(
        kind=DefinitionKind.WORKFLOW_TYPE,
        logical_id=logical_id,
        revision=2,
        digest=sha256_digest(f"workflow:{logical_id}"),
    )
    manifest = RunInputManifestRef(
        manifest_id="upgrade-labs-research",
        revision=1,
        digest=sha256_digest("upgrade-labs-research"),
    )
    return VerifiedRunConfiguration(
        effective_configuration_digest=sha256_digest("configuration"),
        workflow_type_ref=workflow_ref,
        input_manifest=manifest,
        effective_budget_ceilings={"concurrency.slots": 2},
        max_concurrency=2,
        input_admission_contract=WEB_RESEARCH_ADMISSION,
        invariant_refs=frozenset(WEB_RESEARCH_INVARIANTS),
        obligation_revision=sha256_digest("obligations"),
    )


def _request(
    evidence: tuple[str, ...],
    *,
    logical_id: str = "web-research-browser-verification",
) -> RunRequest:
    configuration = _configuration(logical_id)
    return RunRequest(
        request_scope="global",
        idempotency_issuer="coordinator-acceptance",
        request_id="upgrade-labs-acceptance",
        actor=ActorContext(
            actor_id="coordinator-acceptance",
            permissions=frozenset({"workflow_run.admit"}),
            authority_refs=frozenset({"authority:coordinator-acceptance"}),
        ),
        effective_configuration_digest=configuration.effective_configuration_digest,
        workflow_type_ref=configuration.workflow_type_ref,
        input_manifest=configuration.input_manifest,
        budget_envelope=BudgetEnvelope(
            dimensions=(
                BudgetDimensionLimit(
                    dimension="concurrency.slots",
                    applicability=BudgetApplicability.BOUNDED,
                    hard_cap=2,
                ),
            )
        ),
        requested_at=NOW,
        correlation_id="acceptance:upgrade-labs",
        sponsorship_ref="approval:user-live-acceptance",
        admission_evidence_refs=evidence,
    )


def _accepted_evidence() -> tuple[str, ...]:
    return (
        "public-goal:sha256:goal",
        "capability-selection:sha256:selection",
        "catalog://mcp_server/mcp.firecrawl/2?digest=sha256:firecrawl",
        "catalog://mcp_server/mcp.tavily/2?digest=sha256:tavily",
        "tool-allowlist:firecrawl_search:sha256:allowlist",
        "tool-allowlist:tavily_search:sha256:allowlist",
        "catalog://skill/skill.agent-browser/1?digest=sha256:browser",
        "browser-authority:sha256:grant",
        "policy:untrusted-web-content-is-data:sha256:policy",
    )


@pytest.mark.asyncio
async def test_web_research_admission_accepts_exact_governed_evidence() -> None:
    policies = AdmissionPolicyRegistry()
    register_web_research_admission_policies(policies)

    await policies.validate(_request(_accepted_evidence()), _configuration())


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "missing_prefix",
    (
        "public-goal:",
        "capability-selection:",
        "catalog://mcp_server/mcp.firecrawl/",
        "catalog://mcp_server/mcp.tavily/",
        "tool-allowlist:firecrawl_search:",
        "tool-allowlist:tavily_search:",
        "catalog://skill/skill.agent-browser/",
        "browser-authority:",
        "policy:untrusted-web-content-is-data:",
    ),
)
async def test_web_research_admission_rejects_each_missing_authority(
    missing_prefix: str,
) -> None:
    policies = AdmissionPolicyRegistry()
    register_web_research_admission_policies(policies)
    evidence = tuple(
        item for item in _accepted_evidence() if not item.startswith(missing_prefix)
    )

    with pytest.raises(AdmissionRejected, match="missing exact web-research"):
        await policies.validate(_request(evidence), _configuration())


@pytest.mark.asyncio
async def test_web_research_admission_rejects_cross_workflow_reuse() -> None:
    policies = AdmissionPolicyRegistry()
    register_web_research_admission_policies(policies)

    with pytest.raises(AdmissionRejected, match="bound to another Workflow Type"):
        await policies.validate(
            _request(_accepted_evidence(), logical_id="schema-context-selection"),
            _configuration("schema-context-selection"),
        )
