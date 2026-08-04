from __future__ import annotations

from enum import StrEnum
from typing import Literal

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, field_validator, model_validator

from app.domain.control_plane.contracts import (
    AuthorityCeiling,
    BudgetCeiling,
    DefinitionKind,
    ExactDefinitionRef,
    LinkedRunSlotConstraint,
    StageGraphBlueprint,
    WorkflowTypeDefinition,
    WorkflowWorkspaceContract,
)

DIGEST_PATTERN = r"^sha256:[0-9a-f]{64}$"
IDENTIFIER_PATTERN = r"^[a-z0-9][a-z0-9._:-]*$"


class Contract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class CatalogAssetStatus(StrEnum):
    PUBLISHED = "published"
    DEPRECATED = "deprecated"
    RETIRED = "retired"
    REVOKED = "revoked"


class AuthorizationState(StrEnum):
    SELECTABLE = "selectable"
    CANDIDATE_ONLY = "candidate_only"
    INCOMPATIBLE = "incompatible"
    FORBIDDEN = "forbidden"
    UNAVAILABLE = "unavailable"


class PolicyReasonCode(StrEnum):
    SELECTABLE = "SELECTABLE"
    EXTERNAL_CANDIDATE_REQUIRES_PROMOTION = "EXTERNAL_CANDIDATE_REQUIRES_PROMOTION"
    TENANT_INACCESSIBLE = "TENANT_INACCESSIBLE"
    POLICY_FORBIDDEN = "POLICY_FORBIDDEN"
    ASSET_RETIRED = "ASSET_RETIRED"
    ASSET_REVOKED = "ASSET_REVOKED"
    SOURCE_DIGEST_MISMATCH = "SOURCE_DIGEST_MISMATCH"
    CAPABILITY_SCHEMA_CHANGED = "CAPABILITY_SCHEMA_CHANGED"
    MISSING_CAPABILITY = "MISSING_CAPABILITY"
    RUNTIME_INCOMPATIBLE = "RUNTIME_INCOMPATIBLE"
    RUNTIME_UNAVAILABLE = "RUNTIME_UNAVAILABLE"


class PolicyReason(Contract):
    code: PolicyReasonCode
    message: str = Field(min_length=1)


class CapabilitySearchRequest(Contract):
    query: str = Field(min_length=1, max_length=2_000)
    kinds: frozenset[DefinitionKind] = Field(default_factory=frozenset)
    tenant_scope: str = Field(min_length=1, max_length=256)
    workflow_type_ref: ExactDefinitionRef | None = None
    operation_class: str | None = Field(default=None, min_length=1, max_length=256)
    required_capabilities: frozenset[str] = Field(default_factory=frozenset)
    runtime: str | None = Field(default=None, min_length=1, max_length=256)
    status_filter: frozenset[CatalogAssetStatus] = frozenset(
        {CatalogAssetStatus.PUBLISHED}
    )
    include_external_candidates: bool = False
    limit: int = Field(default=10, ge=1, le=100)

    @field_validator("query", "tenant_scope", "operation_class", "runtime")
    @classmethod
    def normalize_text(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = " ".join(value.split())
        if not normalized:
            raise ValueError("text fields cannot be blank")
        return normalized

    @field_validator("required_capabilities")
    @classmethod
    def capability_names_are_nonblank(cls, value: frozenset[str]) -> frozenset[str]:
        if any(not item.strip() for item in value):
            raise ValueError("required capabilities cannot contain blank names")
        return frozenset(item.strip() for item in value)

    @model_validator(mode="after")
    def workflow_ref_has_expected_kind(self) -> CapabilitySearchRequest:
        if (
            self.workflow_type_ref is not None
            and self.workflow_type_ref.kind != DefinitionKind.WORKFLOW_TYPE
        ):
            raise ValueError("workflow_type_ref must reference a Workflow Type")
        if not self.status_filter:
            raise ValueError("status_filter cannot be empty")
        return self


class CapabilitySearchHit(Contract):
    exact_ref: ExactDefinitionRef | None = None
    candidate_id: str | None = Field(default=None, min_length=1)
    kind: DefinitionKind
    title: str = Field(min_length=1)
    summary: str = Field(min_length=1)
    lexical_rank: int | None = Field(default=None, ge=1)
    semantic_rank: int | None = Field(default=None, ge=1)
    fused_rank: float = Field(ge=0)
    compatibility_summary: str = Field(min_length=1)
    authorization_state: AuthorizationState
    reasons: tuple[PolicyReason, ...] = Field(min_length=1)
    source_digest: str | None = Field(default=None, pattern=DIGEST_PATTERN)
    indexed_at: AwareDatetime | None = None
    projection_generation: str | None = Field(default=None, min_length=1)
    parent_ref: ExactDefinitionRef | None = None

    @model_validator(mode="after")
    def validate_identity_and_projection_evidence(self) -> CapabilitySearchHit:
        if (self.exact_ref is None) == (self.candidate_id is None):
            raise ValueError("search hits require exactly one exact_ref or candidate_id")
        if self.exact_ref is not None:
            if self.exact_ref.kind != self.kind:
                raise ValueError("search hit kind must match its exact reference")
            if self.source_digest != self.exact_ref.digest:
                raise ValueError("internal search hit source digest must match its exact reference")
            if self.indexed_at is None or self.projection_generation is None:
                raise ValueError("internal search hits require projection evidence")
        elif self.authorization_state != AuthorizationState.CANDIDATE_ONLY:
            raise ValueError("external candidate hits are candidate_only")
        if (
            self.parent_ref is not None
            and (
                self.kind != DefinitionKind.MCP_TOOL
                or self.parent_ref.kind != DefinitionKind.MCP_SERVER
            )
        ):
            raise ValueError("only MCP Tool hits may carry an MCP Server parent")
        return self


class ExternalDiscoverySource(StrEnum):
    MCP_REGISTRY = "mcp_registry"
    NPX_SKILLS = "npx_skills"
    SKILLS_SEARCH_API = "skills_search_api"
    GIT = "git"


class CandidateTrustTier(StrEnum):
    UNTRUSTED = "untrusted"
    IDENTIFIED_UPSTREAM = "identified_upstream"
    VERIFIED_PUBLISHER = "verified_publisher"


class InspectionStatus(StrEnum):
    NOT_INSPECTED = "not_inspected"
    PENDING = "pending"
    PASSED = "passed"
    FAILED = "failed"


class ExternalDiscoveryCandidate(Contract):
    candidate_id: str = Field(min_length=1, pattern=IDENTIFIER_PATTERN)
    source: ExternalDiscoverySource
    target_kind: Literal[
        DefinitionKind.SKILL,
        DefinitionKind.MCP_SERVER,
        DefinitionKind.MCP_TOOL,
    ]
    upstream_identity: str = Field(min_length=1)
    upstream_version: str | None = Field(default=None, min_length=1)
    locator: str = Field(min_length=1)
    publisher: str | None = Field(default=None, min_length=1)
    discovered_at: AwareDatetime
    query: str = Field(min_length=1)
    raw_response_ref: str = Field(min_length=1)
    raw_response_digest: str = Field(pattern=DIGEST_PATTERN)
    upstream_status: str = Field(min_length=1)
    trust_tier: CandidateTrustTier = CandidateTrustTier.UNTRUSTED
    inspection_status: InspectionStatus = InspectionStatus.NOT_INSPECTED
    inspection_findings: tuple[str, ...] = ()
    requested_capabilities: frozenset[str] = Field(default_factory=frozenset)
    license_evidence: tuple[str, ...] = ()
    promoted_ref: ExactDefinitionRef | None = None

    @model_validator(mode="after")
    def promoted_ref_matches_target(self) -> ExternalDiscoveryCandidate:
        if self.promoted_ref is not None and self.promoted_ref.kind != self.target_kind:
            raise ValueError("candidate promoted_ref must match its target kind")
        if self.inspection_status == InspectionStatus.NOT_INSPECTED and self.inspection_findings:
            raise ValueError("uninspected candidates cannot claim inspection findings")
        return self


class RequestedAsset(Contract):
    exact_ref: ExactDefinitionRef | None = None
    candidate_id: str | None = Field(default=None, min_length=1)
    purpose: str = Field(min_length=1)

    @model_validator(mode="after")
    def exactly_one_identity(self) -> RequestedAsset:
        if (self.exact_ref is None) == (self.candidate_id is None):
            raise ValueError("requested assets require exactly one exact_ref or candidate_id")
        return self


class WorkflowDesignDraft(Contract):
    draft_id: str = Field(min_length=1)
    purpose: str = Field(min_length=1)
    proposed_workflow_type: WorkflowTypeDefinition
    blueprint_family: Literal["StageGraph", "GoalDirected"]
    proposed_stage_graph: StageGraphBlueprint | None = None
    proposed_objective: str | None = Field(default=None, min_length=1)
    proposed_acceptance: str | None = Field(default=None, min_length=1)
    input_contract: str = Field(min_length=1)
    invariants: frozenset[str] = Field(min_length=1)
    obligations: frozenset[str] = Field(default_factory=frozenset)
    output_contracts: frozenset[str] = Field(default_factory=frozenset)
    linked_run_slots: tuple[LinkedRunSlotConstraint, ...] = ()
    requested_assets: tuple[RequestedAsset, ...] = ()
    requested_authority: AuthorityCeiling
    workspace_requirements: WorkflowWorkspaceContract
    budgets: BudgetCeiling
    rationale: str = Field(min_length=1)
    validation_findings: tuple[str, ...] = ()

    @model_validator(mode="after")
    def family_specific_shape(self) -> WorkflowDesignDraft:
        if self.blueprint_family == "StageGraph":
            if self.proposed_stage_graph is None:
                raise ValueError("StageGraph drafts require proposed_stage_graph")
            if self.proposed_objective is not None or self.proposed_acceptance is not None:
                raise ValueError("StageGraph drafts cannot include GoalDirected fields")
        else:
            if self.proposed_stage_graph is not None:
                raise ValueError("GoalDirected drafts cannot include proposed_stage_graph")
            if self.proposed_objective is None or self.proposed_acceptance is None:
                raise ValueError(
                    "GoalDirected drafts require proposed_objective and proposed_acceptance"
                )
        return self


class SearchDocumentMetadata(Contract):
    aliases: frozenset[str] = Field(default_factory=frozenset)
    intended_uses: frozenset[str] = Field(default_factory=frozenset)
    tags: frozenset[str] = Field(default_factory=frozenset)
    domains: frozenset[str] = Field(default_factory=frozenset)
    compatibility_notes: frozenset[str] = Field(default_factory=frozenset)


class SearchDocumentSource(Contract):
    title: str = Field(min_length=1)
    logical_id: str = Field(min_length=1)
    aliases: frozenset[str] = Field(default_factory=frozenset)
    asset_kind: DefinitionKind
    description: str = Field(min_length=1)
    intended_uses: frozenset[str] = Field(default_factory=frozenset)
    non_goals: frozenset[str] = Field(default_factory=frozenset)
    input_summary: str = ""
    output_summary: str = ""
    capability_authority_summary: str = ""
    compatibility_summary: str = ""
    tags: frozenset[str] = Field(default_factory=frozenset)
    domains: frozenset[str] = Field(default_factory=frozenset)
    parent_server_ref: ExactDefinitionRef | None = None
    tool_names: frozenset[str] = Field(default_factory=frozenset)

    @model_validator(mode="after")
    def enforce_mcp_projection_boundaries(self) -> SearchDocumentSource:
        if self.asset_kind == DefinitionKind.MCP_TOOL:
            if (
                self.parent_server_ref is None
                or self.parent_server_ref.kind != DefinitionKind.MCP_SERVER
            ):
                raise ValueError("MCP Tool search documents require an MCP Server parent")
            if self.tool_names:
                raise ValueError("MCP Tool documents cannot include sibling tool names")
        elif self.parent_server_ref is not None:
            raise ValueError("only MCP Tool documents may contain a parent server")
        if self.asset_kind != DefinitionKind.MCP_SERVER and self.tool_names:
            raise ValueError("only MCP Server documents may summarize tool names")
        return self


class SelectionFacts(Contract):
    exact_ref: ExactDefinitionRef | None = None
    candidate_id: str | None = Field(default=None, min_length=1)
    lifecycle_status: CatalogAssetStatus = CatalogAssetStatus.PUBLISHED
    tenant_visible: bool = True
    policy_allowed: bool = True
    source_digest_verified: bool = True
    schema_digest_verified: bool = True
    required_capabilities: frozenset[str] = Field(default_factory=frozenset)
    granted_capabilities: frozenset[str] = Field(default_factory=frozenset)
    runtime_compatible: bool = True
    runtime_available: bool = True

    @model_validator(mode="after")
    def exactly_one_identity(self) -> SelectionFacts:
        if (self.exact_ref is None) == (self.candidate_id is None):
            raise ValueError("selection facts require exactly one exact_ref or candidate_id")
        return self


class SelectionDecision(Contract):
    authorization_state: AuthorizationState
    reasons: tuple[PolicyReason, ...] = Field(min_length=1)
    missing_capabilities: frozenset[str] = Field(default_factory=frozenset)


class CoordinatorErrorEnvelope(Contract):
    schema_version: Literal["1"] = "1"
    code: str = Field(min_length=1)
    message: str = Field(min_length=1)
    retryable: bool = False
    details: dict[str, str] = Field(default_factory=dict)
