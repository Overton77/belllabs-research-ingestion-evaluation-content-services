from __future__ import annotations

from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from app.domain.control_plane.contracts import (
    AuthorityCeiling,
    BudgetCeiling,
    CatalogPayloadRef,
    DefinitionKind,
    ExactDefinitionRef,
    MCPServerDefinition,
    MCPToolDefinition,
    PromptDefinition,
    SourceProvenance,
    WorkflowTypeDefinition,
    WorkflowWorkspaceContract,
)
from app.domain.control_plane.stagegraph_builder import (
    StageGraphStageSpec,
    build_stagegraph_v2,
)
from app.domain.coordinator.contracts import (
    AuthorizationState,
    CandidateTrustTier,
    CapabilitySearchHit,
    CapabilitySearchRequest,
    CatalogAssetStatus,
    ExternalDiscoveryCandidate,
    ExternalDiscoverySource,
    InspectionStatus,
    PolicyReason,
    PolicyReasonCode,
    SearchDocumentMetadata,
    SearchDocumentSource,
    SelectionFacts,
    WorkflowDesignDraft,
)
from app.domain.coordinator.errors import CoordinatorDomainError, CoordinatorErrorCode
from app.domain.coordinator.policy import evaluate_selection, require_selectable
from app.domain.coordinator.search_document import (
    SEARCH_DOCUMENT_FORMAT_VERSION,
    render_search_document,
    search_document_source,
)

NOW = datetime(2026, 7, 25, 18, 0, tzinfo=UTC)
DIGEST = "sha256:" + "1" * 64
SECOND_DIGEST = "sha256:" + "2" * 64


def ref(kind: DefinitionKind, logical_id: str, *, digest: str = DIGEST) -> ExactDefinitionRef:
    return ExactDefinitionRef(
        kind=kind,
        logical_id=logical_id,
        revision=1,
        digest=digest,
    )


def authority(*capabilities: str) -> AuthorityCeiling:
    return AuthorityCeiling(
        capabilities=frozenset(capabilities),
        budgets=BudgetCeiling(dimensions={"tool.calls.total": 10}),
        max_concurrency=1,
    )


def test_search_request_normalizes_text_and_validates_workflow_reference() -> None:
    request = CapabilitySearchRequest(
        query="  current   web\nresearch ",
        kinds=frozenset({DefinitionKind.MCP_TOOL, DefinitionKind.SKILL}),
        tenant_scope=" tenant-1 ",
        workflow_type_ref=ref(DefinitionKind.WORKFLOW_TYPE, "web-research"),
        required_capabilities=frozenset({" web.search "}),
    )
    assert request.query == "current web research"
    assert request.tenant_scope == "tenant-1"
    assert request.required_capabilities == frozenset({"web.search"})
    assert request.status_filter == frozenset({CatalogAssetStatus.PUBLISHED})
    assert request.include_external_candidates is False

    with pytest.raises(ValidationError, match="Workflow Type"):
        CapabilitySearchRequest(
            query="research",
            tenant_scope="tenant-1",
            workflow_type_ref=ref(DefinitionKind.SKILL, "skill.research"),
        )
    with pytest.raises(ValidationError, match="status_filter"):
        CapabilitySearchRequest(
            query="research",
            tenant_scope="tenant-1",
            status_filter=frozenset(),
        )


def test_internal_search_hit_requires_exact_projection_evidence() -> None:
    exact = ref(DefinitionKind.SKILL, "skill.web-research")
    hit = CapabilitySearchHit(
        exact_ref=exact,
        kind=DefinitionKind.SKILL,
        title="Web research",
        summary="Reviewed procedure",
        lexical_rank=2,
        semantic_rank=1,
        fused_rank=0.038,
        compatibility_summary="compatible",
        authorization_state=AuthorizationState.SELECTABLE,
        reasons=(
            PolicyReason(code=PolicyReasonCode.SELECTABLE, message="Selectable."),
        ),
        source_digest=exact.digest,
        indexed_at=NOW,
        projection_generation="generation-1",
    )
    assert hit.lexical_rank == 2
    assert hit.semantic_rank == 1

    with pytest.raises(ValidationError, match="source digest"):
        hit.model_validate({**hit.model_dump(), "source_digest": SECOND_DIGEST})
    with pytest.raises(ValidationError, match="projection evidence"):
        hit.model_validate({**hit.model_dump(), "indexed_at": None})


def test_external_hit_cannot_masquerade_as_selectable_internal_asset() -> None:
    candidate_hit = CapabilitySearchHit(
        candidate_id="candidate:agent-browser",
        kind=DefinitionKind.SKILL,
        title="Agent browser",
        summary="Untrusted discovery result",
        fused_rank=0.02,
        compatibility_summary="not inspected",
        authorization_state=AuthorizationState.CANDIDATE_ONLY,
        reasons=(
            PolicyReason(
                code=PolicyReasonCode.EXTERNAL_CANDIDATE_REQUIRES_PROMOTION,
                message="Promotion required.",
            ),
        ),
    )
    assert candidate_hit.exact_ref is None
    with pytest.raises(ValidationError, match="candidate_only"):
        candidate_hit.model_validate(
            {
                **candidate_hit.model_dump(),
                "authorization_state": AuthorizationState.SELECTABLE,
            }
        )


def test_external_candidate_tracks_quarantine_and_promotion_identity() -> None:
    candidate = ExternalDiscoveryCandidate(
        candidate_id="candidate:agent-browser",
        source=ExternalDiscoverySource.NPX_SKILLS,
        target_kind=DefinitionKind.SKILL,
        upstream_identity="vercel-labs/agent-browser",
        upstream_version="commit:abc",
        locator="skills/agent-browser/SKILL.md",
        publisher="vercel-labs",
        discovered_at=NOW,
        query="interactive browser verification",
        raw_response_ref="s3://catalog/candidates/agent-browser.json",
        raw_response_digest=DIGEST,
        upstream_status="active",
        trust_tier=CandidateTrustTier.IDENTIFIED_UPSTREAM,
        inspection_status=InspectionStatus.PASSED,
        inspection_findings=("manifest valid",),
        requested_capabilities=frozenset({"browser.process"}),
        license_evidence=("Apache-2.0",),
        promoted_ref=ref(DefinitionKind.SKILL, "skill.agent-browser"),
    )
    assert candidate.promoted_ref is not None

    with pytest.raises(ValidationError, match="target kind"):
        candidate.model_validate(
            {
                **candidate.model_dump(),
                "promoted_ref": ref(DefinitionKind.MCP_SERVER, "mcp.agent-browser"),
            }
        )
    with pytest.raises(ValidationError, match="cannot claim"):
        candidate.model_validate(
            {
                **candidate.model_dump(),
                "inspection_status": InspectionStatus.NOT_INSPECTED,
            }
        )


def test_renderer_is_stable_labeled_and_normalizes_unordered_metadata() -> None:
    prompt = PromptDefinition(
        logical_id="prompt.web-research",
        title="  Current   web research ",
        description="Find current sources.\nReturn citations.",
        format="markdown",
        template_engine="none",
        body="SECRET BODY MUST NEVER BE INDEXED",
        trust_class="reviewed",
    )
    first = render_search_document(
        search_document_source(
            prompt,
            SearchDocumentMetadata(
                aliases=frozenset({"stable", "latest"}),
                intended_uses=frozenset({"source discovery", "current information"}),
                tags=frozenset({"citations", "web"}),
                domains=frozenset({"research", "biotech"}),
            ),
        )
    )
    second = render_search_document(
        search_document_source(
            prompt,
            SearchDocumentMetadata(
                aliases=frozenset({"latest", "stable"}),
                intended_uses=frozenset({"current information", "source discovery"}),
                tags=frozenset({"web", "citations"}),
                domains=frozenset({"biotech", "research"}),
            ),
        )
    )
    assert first == second
    assert first.search_document_format_version == SEARCH_DOCUMENT_FORMAT_VERSION == 1
    assert first.search_text.splitlines() == [
        "title: Current web research",
        (
            "logical identifier and aliases: prompt.web-research; "
            "aliases: latest, stable"
        ),
        "asset kind: prompt",
        "short description: Find current sources. Return citations.",
        (
            "intended uses: Render a governed prompt without indexing its body, "
            "current information, source discovery"
        ),
        "non-goals: none",
        "input summary: none",
        "output summary: markdown prompt",
        "capability and authority summary: trust class: reviewed",
        "compatibility summary: template engine: none",
        "tags and domains: tags: citations, web; domains: biotech, research",
        "parent server identity: none",
        "tool names: none",
    ]
    assert "SECRET BODY" not in first.search_text


def test_mcp_server_and_tool_project_independently_without_sibling_leakage() -> None:
    payload = CatalogPayloadRef(
        uri="s3://catalog/mcp/firecrawl-schema.json",
        digest=DIGEST,
        media_type="application/json",
        size_bytes=100,
    )
    provenance = SourceProvenance(
        source="local",
        locator="codex:mcp/firecrawl",
        upstream_identity="firecrawl",
        upstream_version="1",
    )
    server = MCPServerDefinition(
        logical_id="mcp.firecrawl",
        title="Firecrawl MCP",
        description="Governed web retrieval server",
        transport="streamable_http",
        endpoint="https://example.invalid/mcp/",
        credential_refs=(),
        allowed_tools=frozenset(
            {"firecrawl_scrape", "firecrawl_search", "firecrawl_interact"}
        ),
        network_requirements=(),
        schema_snapshot_ref=payload,
        schema_digest=DIGEST,
        source_provenance=provenance,
        review_status="approved",
    )
    server_render = render_search_document(search_document_source(server))
    assert (
        "tool names: firecrawl_interact, firecrawl_scrape, firecrawl_search"
        in server_render.search_text
    )

    tool = MCPToolDefinition(
        logical_id="mcp.firecrawl.firecrawl_search",
        title="Firecrawl search",
        description="Search the current web",
        server_ref=ref(DefinitionKind.MCP_SERVER, "mcp.firecrawl"),
        tool_name="firecrawl_search",
        input_schema={"type": "object", "properties": {"query": {"type": "string"}}},
        output_schema={"type": "object"},
        annotations={"readOnlyHint": True},
        schema_digest=SECOND_DIGEST,
        side_effect_class="read_only",
    )
    tool_render = render_search_document(search_document_source(tool))
    assert "parent server identity: mcp_server:mcp.firecrawl@1#" in tool_render.search_text
    assert "tool names: none" in tool_render.search_text
    assert "firecrawl_scrape" not in tool_render.search_text
    assert '"query"' in tool_render.search_text

    with pytest.raises(ValidationError, match="sibling"):
        SearchDocumentSource(
            **{
                **search_document_source(tool).model_dump(),
                "tool_names": frozenset({"firecrawl_scrape"}),
            }
        )


@pytest.mark.parametrize(
    ("updates", "state", "reason"),
    [
        (
            {"candidate_id": "candidate:skill", "exact_ref": None},
            AuthorizationState.CANDIDATE_ONLY,
            PolicyReasonCode.EXTERNAL_CANDIDATE_REQUIRES_PROMOTION,
        ),
        (
            {"tenant_visible": False},
            AuthorizationState.FORBIDDEN,
            PolicyReasonCode.TENANT_INACCESSIBLE,
        ),
        (
            {"policy_allowed": False},
            AuthorizationState.FORBIDDEN,
            PolicyReasonCode.POLICY_FORBIDDEN,
        ),
        (
            {"lifecycle_status": CatalogAssetStatus.REVOKED},
            AuthorizationState.UNAVAILABLE,
            PolicyReasonCode.ASSET_REVOKED,
        ),
        (
            {"source_digest_verified": False},
            AuthorizationState.UNAVAILABLE,
            PolicyReasonCode.SOURCE_DIGEST_MISMATCH,
        ),
        (
            {"schema_digest_verified": False},
            AuthorizationState.INCOMPATIBLE,
            PolicyReasonCode.CAPABILITY_SCHEMA_CHANGED,
        ),
        (
            {"runtime_compatible": False},
            AuthorizationState.INCOMPATIBLE,
            PolicyReasonCode.RUNTIME_INCOMPATIBLE,
        ),
        (
            {"runtime_available": False},
            AuthorizationState.UNAVAILABLE,
            PolicyReasonCode.RUNTIME_UNAVAILABLE,
        ),
    ],
)
def test_selection_policy_has_stable_states_and_reason_precedence(
    updates: dict[str, object],
    state: AuthorizationState,
    reason: PolicyReasonCode,
) -> None:
    values = {
        "exact_ref": ref(DefinitionKind.SKILL, "skill.web-research"),
        **updates,
    }
    decision = evaluate_selection(SelectionFacts(**values))
    assert decision.authorization_state == state
    assert decision.reasons[0].code == reason


def test_selection_policy_reports_sorted_missing_capabilities_and_ignores_rank() -> None:
    decision = evaluate_selection(
        SelectionFacts(
            exact_ref=ref(DefinitionKind.SKILL, "skill.agent-browser"),
            required_capabilities=frozenset({"browser.process", "network.web"}),
            granted_capabilities=frozenset(),
        )
    )
    assert decision.authorization_state == AuthorizationState.INCOMPATIBLE
    assert decision.missing_capabilities == frozenset(
        {"browser.process", "network.web"}
    )
    assert [reason.message.rsplit(": ", 1)[-1] for reason in decision.reasons] == [
        "browser.process",
        "network.web",
    ]


def test_require_selectable_produces_stable_transport_safe_error_envelope() -> None:
    decision = evaluate_selection(
        SelectionFacts(
            exact_ref=ref(DefinitionKind.SKILL, "skill.retired"),
            lifecycle_status=CatalogAssetStatus.RETIRED,
        )
    )
    with pytest.raises(CoordinatorDomainError) as caught:
        require_selectable(decision)
    envelope = caught.value.envelope()
    assert envelope.model_dump() == {
        "schema_version": "1",
        "code": CoordinatorErrorCode.CAPABILITY_UNAVAILABLE.value,
        "message": "The asset has been retired.",
        "retryable": True,
        "details": {"reason_code": PolicyReasonCode.ASSET_RETIRED.value},
    }


def test_selectable_exact_asset_passes_policy_gate() -> None:
    decision = evaluate_selection(
        SelectionFacts(
            exact_ref=ref(DefinitionKind.MCP_TOOL, "mcp.tavily.tavily_search"),
            required_capabilities=frozenset({"web.search"}),
            granted_capabilities=frozenset({"web.search", "web.extract"}),
        )
    )
    assert decision.authorization_state == AuthorizationState.SELECTABLE
    require_selectable(decision)


def _workflow_type(blueprint_ref: ExactDefinitionRef) -> WorkflowTypeDefinition:
    return WorkflowTypeDefinition(
        logical_id="draft.web-research",
        title="Draft web research",
        description="Draft only",
        purpose="Research current public information",
        input_admission_contract="contract:web-research-input:v1",
        invariants=frozenset({"invariant:cited-sources:v1"}),
        allowed_blueprints=frozenset({blueprint_ref}),
        allowed_control_profiles=frozenset(
            {ref(DefinitionKind.CONTROL_PROFILE, "control.web-research")}
        ),
        allowed_runtime_profiles=frozenset(
            {ref(DefinitionKind.RUNTIME_PROFILE, "runtime.web-research")}
        ),
        allowed_workspace_templates=frozenset(
            {ref(DefinitionKind.WORKSPACE_TEMPLATE, "workspace.web-research")}
        ),
        allowed_evaluation_profiles=frozenset(
            {ref(DefinitionKind.EVALUATION_PROFILE, "evaluation.web-research")}
        ),
        authority_ceiling=authority("web.search"),
        workspace_contract=WorkflowWorkspaceContract(),
    )


def test_workflow_design_draft_enforces_blueprint_family_shape() -> None:
    graph = build_stagegraph_v2(
        logical_id="draft.web-research-stagegraph",
        title="Draft web research graph",
        description="Search then verify",
        stages=(StageGraphStageSpec(stage_id="search"),),
    )
    graph_ref = ref(DefinitionKind.BLUEPRINT, graph.logical_id)
    common = {
        "draft_id": "draft-1",
        "purpose": "Research current public information",
        "proposed_workflow_type": _workflow_type(graph_ref),
        "input_contract": "contract:web-research-input:v1",
        "invariants": frozenset({"invariant:cited-sources:v1"}),
        "requested_authority": authority("web.search"),
        "workspace_requirements": WorkflowWorkspaceContract(),
        "budgets": BudgetCeiling(dimensions={"tool.calls.total": 10}),
        "rationale": "A static graph is sufficient.",
    }
    draft = WorkflowDesignDraft(
        **common,
        blueprint_family="StageGraph",
        proposed_stage_graph=graph,
    )
    assert draft.proposed_stage_graph == graph

    with pytest.raises(ValidationError, match="GoalDirected fields"):
        WorkflowDesignDraft(
            **common,
            blueprint_family="StageGraph",
            proposed_stage_graph=graph,
            proposed_objective="Research",
        )
    with pytest.raises(ValidationError, match="require proposed_objective"):
        WorkflowDesignDraft(
            **common,
            blueprint_family="GoalDirected",
        )
