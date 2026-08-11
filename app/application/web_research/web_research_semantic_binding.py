from __future__ import annotations

from collections.abc import Mapping
from typing import Protocol

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, model_validator

from app.application.orchestration.orchestration_routing import (
    OperationExecutionBindingReader,
    SemanticRoutingError,
)
from app.application.operations.semantic_operation_bindings import (
    SemanticOperationBindingTemplates,
    SemanticOperationExecutionBindingService,
)
from app.application.web_research.web_research_semantic_handlers import (
    build_web_research_run_binding,
)
from app.domain.control_plane.canonical import sha256_digest
from app.domain.control_plane.contracts import (
    AgentProfileDefinition,
    DefinitionKind,
    EffectiveRunConfiguration,
    ExactDefinitionRef,
    MCPServerDefinition,
    MCPToolDefinition,
    PublishedDefinition,
    RuntimeProfileDefinition,
    SkillDefinition,
    StageGraphBlueprint,
    WorkspaceTemplateDefinition,
)
from app.domain.coordinator.contracts import (
    AuthorizationState,
    CapabilitySearchHit,
    CapabilitySearchRequest,
)
from app.domain.coordinator.launch import (
    BlueprintFamily,
    PreparedLaunchTicket,
    SemanticBindingPlan,
    WorkflowLaunchProposal,
)
from app.domain.coordinator.web_research_runtime import (
    BrowserExecutionGrantBinding,
    ExactOperationExecutionBinding,
    GovernedMCPServerBinding,
    OperationExecutionBindingAuthority,
    ReviewedRuntimeArtifactBinding,
    ReviewedSkillMountBinding,
    WebResearchGoal,
)
from app.domain.operation_execution.contracts import OperationExecutionBinding
from app.domain.orchestration.bindings import RunSemanticInputBinding

DIGEST_PATTERN = r"^sha256:[0-9a-f]{64}$"
REQUIRED_SELECTED_IDENTITIES = frozenset(
    {
        (DefinitionKind.MCP_SERVER, "mcp.firecrawl"),
        (DefinitionKind.MCP_SERVER, "mcp.tavily"),
        (DefinitionKind.MCP_TOOL, "mcp.firecrawl:firecrawl_search"),
        (DefinitionKind.MCP_TOOL, "mcp.tavily:tavily_search"),
        (DefinitionKind.SKILL, "skill.firecrawl-search"),
        (DefinitionKind.SKILL, "skill.tavily-search"),
        (DefinitionKind.SKILL, "skill.agent-browser"),
        (
            DefinitionKind.AGENT_PROFILE,
            "agent-profile.web-research-browser-verification",
        ),
    }
)
REQUIRED_BROWSER_CAPABILITIES = frozenset(
    {
        "browser.process",
        "browser.navigation",
        "browser.screenshot",
        "network.web",
        "workspace.browser.read",
        "workspace.browser.write",
        "artifact.browser-evidence.write",
    }
)


class WebResearchBindingPlanInput(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    goal: WebResearchGoal
    retrieval_request: CapabilitySearchRequest
    selected_hits: tuple[CapabilitySearchHit, ...]
    firecrawl_runtime: ReviewedRuntimeArtifactBinding
    tavily_runtime: ReviewedRuntimeArtifactBinding
    browser_runtime: ReviewedRuntimeArtifactBinding
    runtime_profile_ref: ExactDefinitionRef
    workspace_template_ref: ExactDefinitionRef
    operation_bindings: SemanticOperationBindingTemplates
    created_at: AwareDatetime
    maximum_results: int = Field(default=5, ge=1, le=10)
    browser_verification_limit: int = Field(default=3, ge=1, le=5)

    @model_validator(mode="after")
    def selected_hits_are_exact_internal_evidence(
        self,
    ) -> WebResearchBindingPlanInput:
        refs = tuple(hit.exact_ref for hit in self.selected_hits)
        if (
            any(ref is None for ref in refs)
            or any(
                hit.authorization_state != AuthorizationState.SELECTABLE
                or hit.candidate_id is not None
                or hit.projection_generation is None
                or hit.source_digest != hit.exact_ref.digest  # type: ignore[union-attr]
                for hit in self.selected_hits
            )
            or len(set(refs)) != len(refs)
        ):
            raise ValueError("Scenario D requires distinct selectable internal search-hit evidence")
        if {
            (ref.kind, ref.logical_id) for ref in refs if ref is not None
        } != REQUIRED_SELECTED_IDENTITIES:
            raise ValueError(
                "Scenario D retrieval evidence must contain exactly its required "
                "server, tool, skill, and agent-profile classes"
            )
        return self


class WebResearchOperationBindingRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    request_scope: str = Field(min_length=1)
    run_id: str = Field(min_length=1)
    effective_configuration_digest: str = Field(pattern=DIGEST_PATTERN)
    runtime_profile_ref: ExactDefinitionRef
    workspace_template_ref: ExactDefinitionRef
    selected_capability_refs: tuple[ExactDefinitionRef, ...]
    created_at: AwareDatetime


class WebResearchOperationBindingAuthor(Protocol):
    """Create and durably persist the actual governed OperationExecutionBinding."""

    @property
    def templates(self) -> SemanticOperationBindingTemplates: ...

    async def author(
        self,
        request: WebResearchOperationBindingRequest,
        *,
        ticket: PreparedLaunchTicket,
    ) -> Mapping[str, OperationExecutionBinding]: ...


class SemanticServiceWebResearchOperationBindingAuthor:
    """Adapt the shared OEB freezer into Scenario D's exact three-stage author."""

    def __init__(
        self,
        templates: SemanticOperationBindingTemplates,
        service: SemanticOperationExecutionBindingService,
        reader: OperationExecutionBindingReader,
    ) -> None:
        required = {"search_firecrawl", "search_tavily", "browser_verify"}
        if set(templates.operations) != required:
            raise ValueError("Scenario D OEB templates must cover exactly its effectful stages")
        self._templates = templates
        self._service = service
        self._reader = reader

    @property
    def templates(self) -> SemanticOperationBindingTemplates:
        return self._templates

    async def author(
        self,
        request: WebResearchOperationBindingRequest,
        *,
        ticket: PreparedLaunchTicket,
    ) -> Mapping[str, OperationExecutionBinding]:
        refs = await self._service.freeze(
            self._templates,
            ticket,
            run_id=request.run_id,
            bound_at=request.created_at,
        )
        bindings: dict[str, OperationExecutionBinding] = {}
        for stage_id, binding_id in refs.items():
            binding = await self._reader.get_binding_by_id(
                binding_id,
                request_scope=request.request_scope,
            )
            if binding is None:
                raise SemanticRoutingError("persisted Scenario D OEB is unavailable after freeze")
            bindings[stage_id] = binding
        return bindings


class WebResearchSemanticBindingProvider:
    """Freeze coordinator retrieval evidence and author Scenario D's run binding."""

    def __init__(
        self,
        *,
        catalog_records: tuple[PublishedDefinition, ...],
        retrieval_request: CapabilitySearchRequest,
        retrieval_hits: tuple[CapabilitySearchHit, ...],
        goal: WebResearchGoal,
        firecrawl_runtime: ReviewedRuntimeArtifactBinding,
        tavily_runtime: ReviewedRuntimeArtifactBinding,
        browser_runtime: ReviewedRuntimeArtifactBinding,
        operation_bindings: WebResearchOperationBindingAuthor,
        maximum_results: int = 5,
        browser_verification_limit: int = 3,
    ) -> None:
        self._catalog: dict[tuple[DefinitionKind, str], PublishedDefinition] = {}
        for item in catalog_records:
            identity = (item.ref.kind, item.ref.logical_id)
            current = self._catalog.get(identity)
            if current is None or item.ref.revision > current.ref.revision:
                self._catalog[identity] = item
        self._retrieval_request = retrieval_request
        self._retrieval_hits = retrieval_hits
        self._goal = goal
        self._firecrawl_runtime = firecrawl_runtime
        self._tavily_runtime = tavily_runtime
        self._browser_runtime = browser_runtime
        self._operation_bindings = operation_bindings
        self._maximum_results = maximum_results
        self._browser_verification_limit = browser_verification_limit

    async def prepare(
        self,
        proposal: WorkflowLaunchProposal,
        configuration: EffectiveRunConfiguration,
    ) -> SemanticBindingPlan:
        blueprint = configuration.selected_blueprint
        if (
            configuration.workflow_type.logical_id != "web-research-browser-verification"
            or not isinstance(blueprint, StageGraphBlueprint)
            or blueprint.logical_id != "web-research-browser-verification-v1"
        ):
            raise SemanticRoutingError(
                "web-research binding provider requires the exact Scenario D StageGraph"
            )
        selected_refs = tuple(
            hit.exact_ref for hit in self._retrieval_hits if hit.exact_ref is not None
        )
        if set(proposal.selected_asset_refs) != set(selected_refs) or len(
            proposal.selected_asset_refs
        ) != len(selected_refs):
            raise SemanticRoutingError(
                "proposal selections differ from coordinator retrieval evidence"
            )
        _verify_selected_catalog_records(self._catalog, selected_refs)
        runtime_ref = _required_source_ref(
            configuration,
            DefinitionKind.RUNTIME_PROFILE,
            "web-research-browser-verification-runtime-v1",
        )
        workspace_ref = _required_source_ref(
            configuration,
            DefinitionKind.WORKSPACE_TEMPLATE,
            "web-research-browser-verification-workspace-v1",
        )
        inputs = WebResearchBindingPlanInput(
            goal=self._goal,
            retrieval_request=self._retrieval_request,
            selected_hits=self._retrieval_hits,
            firecrawl_runtime=self._firecrawl_runtime,
            tavily_runtime=self._tavily_runtime,
            browser_runtime=self._browser_runtime,
            runtime_profile_ref=runtime_ref,
            workspace_template_ref=workspace_ref,
            operation_bindings=self._operation_bindings.templates,
            created_at=proposal.admission.requested_at,
            maximum_results=self._maximum_results,
            browser_verification_limit=self._browser_verification_limit,
        )
        return SemanticBindingPlan.create(
            plan_ref=f"semantic-binding-plan:web-research:{proposal.idempotency_key}",
            blueprint_family=BlueprintFamily.STAGE_GRAPH,
            exact_input_refs=tuple(_catalog_uri(ref) for ref in selected_refs),
            payload=inputs.model_dump(mode="json"),
        )

    async def author(
        self,
        plan: SemanticBindingPlan,
        ticket: PreparedLaunchTicket,
        *,
        run_id: str,
    ) -> RunSemanticInputBinding:
        if (
            plan.blueprint_family != BlueprintFamily.STAGE_GRAPH
            or ticket.blueprint_family != BlueprintFamily.STAGE_GRAPH
            or plan.plan_ref != ticket.semantic_binding_plan_ref
            or plan.plan_digest != ticket.semantic_binding_plan_digest
        ):
            raise SemanticRoutingError(
                "web-research semantic plan differs from the frozen launch ticket"
            )
        inputs = WebResearchBindingPlanInput.model_validate(plan.payload)
        selected_refs = tuple(
            hit.exact_ref for hit in inputs.selected_hits if hit.exact_ref is not None
        )
        if set(selected_refs) != set(ticket.resolved_asset_refs) & set(selected_refs):
            raise SemanticRoutingError(
                "launch ticket no longer carries every retrieved Scenario D capability"
            )
        if inputs.operation_bindings != self._operation_bindings.templates:
            raise SemanticRoutingError("Scenario D OEB templates changed after launch preparation")
        operation_bindings = await self._operation_bindings.author(
            WebResearchOperationBindingRequest(
                request_scope=ticket.request_scope,
                run_id=run_id,
                effective_configuration_digest=ticket.effective_configuration_digest,
                runtime_profile_ref=inputs.runtime_profile_ref,
                workspace_template_ref=inputs.workspace_template_ref,
                selected_capability_refs=selected_refs,
                created_at=inputs.created_at,
            ),
            ticket=ticket,
        )
        mcp_servers, skills, browser_grant = verify_web_research_operation_bindings(
            operation_bindings,
            selected_refs=selected_refs,
            runtime_profile_ref=inputs.runtime_profile_ref,
            workspace_template_ref=inputs.workspace_template_ref,
            effective_configuration_digest=ticket.effective_configuration_digest,
            request_scope=ticket.request_scope,
            run_id=run_id,
            catalog=self._catalog,
        )
        by_id = {(ref.kind, ref.logical_id): ref for ref in selected_refs}
        return build_web_research_run_binding(
            request_scope=ticket.request_scope,
            run_id=run_id,
            goal=inputs.goal,
            firecrawl_tool_ref=by_id[(DefinitionKind.MCP_TOOL, "mcp.firecrawl:firecrawl_search")],
            tavily_tool_ref=by_id[(DefinitionKind.MCP_TOOL, "mcp.tavily:tavily_search")],
            browser_skill_ref=by_id[(DefinitionKind.SKILL, "skill.agent-browser")],
            firecrawl_runtime=inputs.firecrawl_runtime,
            tavily_runtime=inputs.tavily_runtime,
            browser_runtime=inputs.browser_runtime,
            mcp_servers=mcp_servers,
            skills=skills,
            browser_grant=browser_grant,
            operation_execution=OperationExecutionBindingAuthority(
                bindings={
                    stage_id: ExactOperationExecutionBinding(
                        binding_id=operation_binding.binding_id,
                        binding_digest=sha256_digest(operation_binding),
                    )
                    for stage_id, operation_binding in operation_bindings.items()
                },
                effective_configuration_digest=ticket.effective_configuration_digest,
            ),
            effective_configuration_digest=ticket.effective_configuration_digest,
            blueprint_digest=ticket.blueprint_ref.digest,
            created_at=inputs.created_at,
            maximum_results=inputs.maximum_results,
            browser_verification_limit=inputs.browser_verification_limit,
        )


def _verify_selected_catalog_records(
    catalog: dict[tuple[DefinitionKind, str], PublishedDefinition],
    refs: tuple[ExactDefinitionRef, ...],
) -> None:
    if {(ref.kind, ref.logical_id) for ref in refs} != REQUIRED_SELECTED_IDENTITIES:
        raise SemanticRoutingError(
            "Scenario D selected assets contain missing, extra, or wrong capability classes"
        )
    records: dict[tuple[DefinitionKind, str], PublishedDefinition] = {}
    for ref in refs:
        record = catalog.get((ref.kind, ref.logical_id))
        if record is None or record.ref != ref or record.retired_at is not None:
            raise SemanticRoutingError(
                "Scenario D search hit was not rehydrated as the exact current Mongo definition"
            )
        records[(ref.kind, ref.logical_id)] = record
    firecrawl_server = records[(DefinitionKind.MCP_SERVER, "mcp.firecrawl")].definition
    tavily_server = records[(DefinitionKind.MCP_SERVER, "mcp.tavily")].definition
    firecrawl_tool = records[(DefinitionKind.MCP_TOOL, "mcp.firecrawl:firecrawl_search")].definition
    tavily_tool = records[(DefinitionKind.MCP_TOOL, "mcp.tavily:tavily_search")].definition
    profile = records[
        (
            DefinitionKind.AGENT_PROFILE,
            "agent-profile.web-research-browser-verification",
        )
    ].definition
    browser_skill_record = records[(DefinitionKind.SKILL, "skill.agent-browser")].definition
    runtime_record = catalog.get(
        (
            DefinitionKind.RUNTIME_PROFILE,
            "web-research-browser-verification-runtime-v1",
        )
    )
    workspace_record = catalog.get(
        (
            DefinitionKind.WORKSPACE_TEMPLATE,
            "web-research-browser-verification-workspace-v1",
        )
    )
    if (
        not isinstance(firecrawl_server, MCPServerDefinition)
        or not isinstance(tavily_server, MCPServerDefinition)
        or not isinstance(firecrawl_tool, MCPToolDefinition)
        or not isinstance(tavily_tool, MCPToolDefinition)
        or not isinstance(profile, AgentProfileDefinition)
        or not isinstance(browser_skill_record, SkillDefinition)
        or runtime_record is None
        or not isinstance(runtime_record.definition, RuntimeProfileDefinition)
        or workspace_record is None
        or not isinstance(workspace_record.definition, WorkspaceTemplateDefinition)
        or firecrawl_tool.tool_name != "firecrawl_search"
        or tavily_tool.tool_name != "tavily_search"
        or firecrawl_tool.server_ref != records[(DefinitionKind.MCP_SERVER, "mcp.firecrawl")].ref
        or tavily_tool.server_ref != records[(DefinitionKind.MCP_SERVER, "mcp.tavily")].ref
    ):
        raise SemanticRoutingError("Scenario D selected catalog relationships are invalid")
    selected_skills = frozenset(ref for ref in refs if ref.kind == DefinitionKind.SKILL)
    selected_servers = frozenset(ref for ref in refs if ref.kind == DefinitionKind.MCP_SERVER)
    selected_tools = frozenset(ref for ref in refs if ref.kind == DefinitionKind.MCP_TOOL)
    if (
        profile.skill_refs != selected_skills
        or profile.mcp_server_refs != selected_servers
        or profile.tool_refs != selected_tools
    ):
        raise SemanticRoutingError(
            "Scenario D Agent Profile differs from the retrieved exact capability set"
        )
    if (
        not REQUIRED_BROWSER_CAPABILITIES <= browser_skill_record.required_capabilities
        or "agent-browser" not in browser_skill_record.compatibility.executables
        or not REQUIRED_BROWSER_CAPABILITIES <= profile.maximum_capability_request.capabilities
        or not REQUIRED_BROWSER_CAPABILITIES <= runtime_record.definition.required_capabilities
        or not {
            "workspace.browser.read",
            "workspace.browser.write",
            "artifact.browser-evidence.write",
        }
        <= workspace_record.definition.required_capabilities
    ):
        raise SemanticRoutingError(
            "skill.agent-browser lacks its required process, network, workspace, "
            "or artifact launch grant"
        )


def verify_web_research_operation_binding(
    binding: OperationExecutionBinding,
    *,
    selected_refs: tuple[ExactDefinitionRef, ...],
    runtime_profile_ref: ExactDefinitionRef,
    workspace_template_ref: ExactDefinitionRef,
    effective_configuration_digest: str,
    request_scope: str,
    run_id: str,
    catalog: dict[tuple[DefinitionKind, str], PublishedDefinition],
) -> tuple[
    tuple[GovernedMCPServerBinding, ...],
    tuple[ReviewedSkillMountBinding, ...],
    BrowserExecutionGrantBinding,
]:
    by_id = {(ref.kind, ref.logical_id): ref for ref in selected_refs}
    profile_ref = by_id[
        (
            DefinitionKind.AGENT_PROFILE,
            "agent-profile.web-research-browser-verification",
        )
    ]
    if (
        binding.run_id != run_id
        or binding.request_scope != request_scope
        or binding.effective_configuration_digest != effective_configuration_digest
        or binding.agent_profile_ref != profile_ref
        or binding.workspace.template_ref != workspace_template_ref
    ):
        raise SemanticRoutingError(
            "OperationExecutionBinding differs from the admitted Scenario D authority"
        )
    expected_servers = {
        "mcp.firecrawl": ("firecrawl_search", "mcp.firecrawl:firecrawl_search"),
        "mcp.tavily": ("tavily_search", "mcp.tavily:tavily_search"),
    }
    actual_servers = {item.server_id: item for item in binding.mcp_servers}
    if set(actual_servers) != set(expected_servers):
        raise SemanticRoutingError("OperationExecutionBinding must contain exactly two MCP servers")
    governed_servers: list[GovernedMCPServerBinding] = []
    for server_id, (tool_name, tool_logical_id) in expected_servers.items():
        server = actual_servers[server_id]
        server_ref = by_id[(DefinitionKind.MCP_SERVER, server_id)]
        tool_ref = by_id[(DefinitionKind.MCP_TOOL, tool_logical_id)]
        if server.revision != server_ref.revision or server.allowed_tools != frozenset({tool_name}):
            raise SemanticRoutingError("OperationExecutionBinding selected unrelated MCP tools")
        governed_servers.append(
            GovernedMCPServerBinding(
                server_ref=server_ref,
                tool_ref=tool_ref,
                allowed_tools=server.allowed_tools,
                server_schema_digest=server.schema_digest,
            )
        )
    expected_skill_ids = {
        "skill.firecrawl-search": "/skills/firecrawl-search/SKILL.md",
        "skill.tavily-search": "/skills/tavily-search/SKILL.md",
        "skill.agent-browser": "/skills/agent-browser/SKILL.md",
    }
    actual_skills = {item.ref.logical_id: item for item in binding.skills}
    if set(actual_skills) != set(expected_skill_ids):
        raise SemanticRoutingError(
            "OperationExecutionBinding must mount exactly three reviewed skills"
        )
    reviewed_skills: list[ReviewedSkillMountBinding] = []
    for logical_id, mount_path in expected_skill_ids.items():
        asset = actual_skills[logical_id]
        selected_ref = by_id[(DefinitionKind.SKILL, logical_id)]
        record = catalog[(DefinitionKind.SKILL, logical_id)]
        if (
            asset.ref != selected_ref
            or asset.mount_path != mount_path
            or not isinstance(record.definition, SkillDefinition)
            or asset.manifest_digest != record.definition.manifest_digest
        ):
            raise SemanticRoutingError(
                "OperationExecutionBinding skill bundle or mount differs from Mongo authority"
            )
        reviewed_skills.append(
            ReviewedSkillMountBinding(
                skill_ref=selected_ref,
                bundle_ref=record.definition.bundle_ref.uri,
                bundle_digest=record.definition.bundle_ref.digest,
                manifest_digest=asset.manifest_digest,
                mount_path=asset.mount_path,
            )
        )
    if not REQUIRED_BROWSER_CAPABILITIES <= binding.capability_grant.capabilities:
        raise SemanticRoutingError(
            "OperationExecutionBinding lacks browser process/network/workspace authority"
        )
    workspace_paths = frozenset((*binding.workspace.exclusive_write_paths,))
    if (
        not binding.capability_grant.network_hosts
        or "*" in binding.capability_grant.network_hosts
        or not {"/workspace/browser", "/artifacts/browser-evidence"} <= workspace_paths
    ):
        raise SemanticRoutingError(
            "OperationExecutionBinding lacks bounded browser network, workspace, or artifact grants"
        )
    grant_payload = {
        "agent_profile_ref": profile_ref.model_dump(mode="json"),
        "runtime_profile_ref": runtime_profile_ref.model_dump(mode="json"),
        "workspace_template_ref": workspace_template_ref.model_dump(mode="json"),
        "capabilities": sorted(binding.capability_grant.capabilities),
        "network_hosts": sorted(binding.capability_grant.network_hosts),
        "workspace_paths": sorted(workspace_paths),
    }
    browser_grant = BrowserExecutionGrantBinding(
        agent_profile_ref=profile_ref,
        runtime_profile_ref=runtime_profile_ref,
        workspace_template_ref=workspace_template_ref,
        executable="agent-browser",
        capabilities=binding.capability_grant.capabilities,
        network_hosts=binding.capability_grant.network_hosts,
        workspace_paths=workspace_paths,
        grant_digest=sha256_digest(grant_payload),
    )
    return tuple(governed_servers), tuple(reviewed_skills), browser_grant


def verify_web_research_operation_bindings(
    bindings: Mapping[str, OperationExecutionBinding],
    *,
    selected_refs: tuple[ExactDefinitionRef, ...],
    runtime_profile_ref: ExactDefinitionRef,
    workspace_template_ref: ExactDefinitionRef,
    effective_configuration_digest: str,
    request_scope: str,
    run_id: str,
    catalog: dict[tuple[DefinitionKind, str], PublishedDefinition],
) -> tuple[
    tuple[GovernedMCPServerBinding, ...],
    tuple[ReviewedSkillMountBinding, ...],
    BrowserExecutionGrantBinding,
]:
    required = {"search_firecrawl", "search_tavily", "browser_verify"}
    if set(bindings) != required:
        raise SemanticRoutingError("Scenario D requires one OEB for each effectful stage")
    verified = []
    for stage_id in sorted(required):
        binding = bindings[stage_id]
        if binding.operation_id != stage_id:
            raise SemanticRoutingError(
                "Scenario D OEB operation identity differs from its stage route"
            )
        verified.append(
            verify_web_research_operation_binding(
                binding,
                selected_refs=selected_refs,
                runtime_profile_ref=runtime_profile_ref,
                workspace_template_ref=workspace_template_ref,
                effective_configuration_digest=effective_configuration_digest,
                request_scope=request_scope,
                run_id=run_id,
                catalog=catalog,
            )
        )
    first = verified[0]
    if any(item != first for item in verified[1:]):
        raise SemanticRoutingError(
            "Scenario D effectful OEBs do not share exact capability authority"
        )
    return first


def _required_source_ref(
    configuration: EffectiveRunConfiguration,
    kind: DefinitionKind,
    logical_id: str,
) -> ExactDefinitionRef:
    matches = tuple(
        ref
        for ref in configuration.source_refs
        if ref.kind == kind and ref.logical_id == logical_id
    )
    if len(matches) != 1:
        raise SemanticRoutingError(
            f"Scenario D ERC is missing exact source authority: {kind.value}:{logical_id}"
        )
    return matches[0]


def _catalog_uri(ref: ExactDefinitionRef) -> str:
    return f"catalog://{ref.kind.value}/{ref.logical_id}/{ref.revision}?digest={ref.digest}"


__all__ = [
    "WebResearchBindingPlanInput",
    "SemanticServiceWebResearchOperationBindingAuthor",
    "WebResearchOperationBindingAuthor",
    "WebResearchOperationBindingRequest",
    "WebResearchSemanticBindingProvider",
    "verify_web_research_operation_binding",
    "verify_web_research_operation_bindings",
]
