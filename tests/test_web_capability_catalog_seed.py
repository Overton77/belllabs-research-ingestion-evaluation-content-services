from __future__ import annotations

from datetime import UTC, datetime

from app.application.control_plane import ControlPlaneService
from app.application.control_plane_repository import InMemoryDefinitionRepository
from app.domain.control_plane.canonical import canonical_json, sha256_digest
from app.domain.control_plane.contracts import (
    AgentProfileDefinition,
    ControlProfileDefinition,
    DefinitionKind,
    EvaluationProfileDefinition,
    MCPServerDefinition,
    MCPToolDefinition,
    PublishRequest,
    RuntimeProfileDefinition,
    SkillDefinition,
    StageGraphBlueprint,
    WorkflowImplementationBindingDefinition,
    WorkflowTypeDefinition,
    WorkspaceTemplateDefinition,
)
from app.domain.control_plane.extensions import ExtensionRegistry
from app.domain.coordinator.web_capability_fixtures import (
    BROWSER_CAPABILITIES,
    FIRECRAWL_SKILL_NAMES,
    FIRECRAWL_TOOL_NAMES,
    REQUIRED_TOOL_NAMES,
    SEARCH_TOOL_LOGICAL_IDS,
    TAVILY_SKILL_NAMES,
    TAVILY_TOOL_NAMES,
    WEB_RESEARCH_CAPABILITIES,
    web_capability_definitions,
)
from app.integrations.control_plane_payloads import InMemoryPayloadStore

NOW = datetime(2026, 7, 25, 19, 0, tzinfo=UTC)


def test_web_fixture_has_complete_deterministic_inventory() -> None:
    first = web_capability_definitions()
    second = web_capability_definitions()
    assert first == second
    assert canonical_json(first) == canonical_json(second)
    assert sha256_digest(first) == sha256_digest(second)

    servers = [
        definition for definition in first if isinstance(definition, MCPServerDefinition)
    ]
    tools = [
        definition for definition in first if isinstance(definition, MCPToolDefinition)
    ]
    skills = [
        definition for definition in first if isinstance(definition, SkillDefinition)
    ]
    profiles = [
        definition for definition in first if isinstance(definition, AgentProfileDefinition)
    ]
    assert len(first) == 36
    assert {server.logical_id for server in servers} == {"mcp.firecrawl", "mcp.tavily"}
    assert {tool.tool_name for tool in tools} == set(REQUIRED_TOOL_NAMES)
    assert len(tools) == 7
    assert {skill.skill_name for skill in skills} == {
        *FIRECRAWL_SKILL_NAMES,
        *TAVILY_SKILL_NAMES,
        "agent-browser",
    }
    assert len(skills) == 19
    assert len(profiles) == 1

    assert sum(isinstance(item, StageGraphBlueprint) for item in first) == 1
    assert sum(isinstance(item, ControlProfileDefinition) for item in first) == 1
    assert sum(isinstance(item, RuntimeProfileDefinition) for item in first) == 1
    assert sum(isinstance(item, WorkspaceTemplateDefinition) for item in first) == 1
    assert sum(isinstance(item, EvaluationProfileDefinition) for item in first) == 1
    assert sum(isinstance(item, WorkflowTypeDefinition) for item in first) == 1
    assert (
        sum(isinstance(item, WorkflowImplementationBindingDefinition) for item in first)
        == 1
    )


def test_mcp_servers_freeze_provider_identity_and_separate_exact_tool_rows() -> None:
    definitions = web_capability_definitions()
    servers = {
        server.logical_id: server
        for server in definitions
        if isinstance(server, MCPServerDefinition)
    }
    tools = [tool for tool in definitions if isinstance(tool, MCPToolDefinition)]

    assert servers["mcp.firecrawl"].allowed_tools == frozenset(FIRECRAWL_TOOL_NAMES)
    assert servers["mcp.tavily"].allowed_tools == frozenset(TAVILY_TOOL_NAMES)
    assert servers["mcp.firecrawl"].approval_policy["firecrawl_interact"] == "always"
    assert servers["mcp.firecrawl"].source_provenance.upstream_identity == "firecrawl"
    assert (
        servers["mcp.tavily"].source_provenance.upstream_identity
        == "tavily-remote-mcp"
    )
    assert servers["mcp.tavily"].endpoint == "https://mcp.tavily.com/mcp/"
    assert all(
        server.schema_digest == server.schema_snapshot_ref.digest
        for server in servers.values()
    )
    assert all(server.credential_refs for server in servers.values())

    parent_by_tool = {
        tool.tool_name: tool.server_ref.logical_id
        for tool in tools
    }
    assert parent_by_tool == {
        **{name: "mcp.firecrawl" for name in FIRECRAWL_TOOL_NAMES},
        **{name: "mcp.tavily" for name in TAVILY_TOOL_NAMES},
    }
    assert len({tool.logical_id for tool in tools}) == 7
    assert all(tool.schema_digest != tool.server_ref.digest for tool in tools)


def test_profile_selects_only_search_tools_and_has_explicit_browser_authority() -> None:
    definitions = web_capability_definitions()
    tools = {
        tool.logical_id: tool
        for tool in definitions
        if isinstance(tool, MCPToolDefinition)
    }
    skills = {
        skill.logical_id: skill
        for skill in definitions
        if isinstance(skill, SkillDefinition)
    }
    profile = next(
        definition
        for definition in definitions
        if isinstance(definition, AgentProfileDefinition)
    )
    selected_tool_ids = {
        ref.logical_id for ref in profile.tool_refs
    }
    assert selected_tool_ids == SEARCH_TOOL_LOGICAL_IDS
    assert {tools[logical_id].tool_name for logical_id in selected_tool_ids} == {
        "firecrawl_search",
        "tavily_search",
    }
    assert profile.mcp_server_refs == frozenset(
        {
            next(
                ref
                for ref in (
                    tool.server_ref for tool in tools.values()
                )
                if ref.logical_id == "mcp.firecrawl"
            ),
            next(
                ref
                for ref in (
                    tool.server_ref for tool in tools.values()
                )
                if ref.logical_id == "mcp.tavily"
            ),
        }
    )
    assert {ref.logical_id for ref in profile.skill_refs} == {
        "skill.firecrawl-search",
        "skill.tavily-search",
        "skill.agent-browser",
    }
    assert BROWSER_CAPABILITIES <= profile.maximum_capability_request.capabilities
    assert WEB_RESEARCH_CAPABILITIES == profile.maximum_capability_request.capabilities

    browser = skills["skill.agent-browser"]
    assert browser.source_provenance.upstream_identity == "vercel-labs/agent-browser"
    assert browser.compatibility.executables == frozenset({"agent-browser"})
    assert {
        "network.web",
    } <= browser.compatibility.network_capabilities
    assert {
        "workspace.browser.read",
        "workspace.browser.write",
        "artifact.browser-evidence.write",
    } <= browser.compatibility.workspace_capabilities
    assert BROWSER_CAPABILITIES <= browser.required_capabilities


def test_workflow_fixture_freezes_browser_runtime_workspace_and_artifact_contracts() -> None:
    definitions = web_capability_definitions()
    blueprint = next(
        definition for definition in definitions if isinstance(definition, StageGraphBlueprint)
    )
    runtime = next(
        definition for definition in definitions if isinstance(definition, RuntimeProfileDefinition)
    )
    workspace = next(
        definition
        for definition in definitions
        if isinstance(definition, WorkspaceTemplateDefinition)
    )
    workflow = next(
        definition for definition in definitions if isinstance(definition, WorkflowTypeDefinition)
    )
    implementation = next(
        definition
        for definition in definitions
        if isinstance(definition, WorkflowImplementationBindingDefinition)
    )

    assert workflow.logical_id == "web-research-browser-verification"
    assert workflow.allowed_blueprints == frozenset(
        {
            implementation.blueprint_ref,
        }
    )
    assert runtime.binding.endswith("+browser-runtime")
    assert WEB_RESEARCH_CAPABILITIES == runtime.required_capabilities
    assert WEB_RESEARCH_CAPABILITIES == workflow.authority_ceiling.capabilities
    assert {
        "workspace.browser.read",
        "workspace.browser.write",
        "artifact.browser-evidence.write",
        "artifact.research-report.write",
    } <= workspace.required_capabilities
    assert workflow.workspace_contract.slots == workspace.slots
    assert {slot.name for slot in workspace.slots} == {
        "research_input",
        "browser_workspace",
        "browser_evidence",
        "research_output",
    }
    stages = {stage.stage_id: stage for stage in blueprint.stages}
    assert stages["search_firecrawl"].depends_on == frozenset({"admit_public_goal"})
    assert stages["search_tavily"].depends_on == frozenset({"admit_public_goal"})
    assert stages["synthesize_citations"].depends_on == frozenset(
        {"search_firecrawl", "search_tavily"}
    )
    assert stages["browser_verify"].depends_on == frozenset({"synthesize_citations"})
    assert blueprint.max_parallel_stages == 2


def test_publication_order_places_every_exact_dependency_before_its_consumer() -> None:
    definitions = web_capability_definitions()
    positions: dict[tuple[DefinitionKind, str], int] = {
        (definition.kind, definition.logical_id): index
        for index, definition in enumerate(definitions)
    }

    for index, definition in enumerate(definitions):
        references = []
        if isinstance(definition, MCPToolDefinition):
            references.append(definition.server_ref)
        elif isinstance(definition, AgentProfileDefinition):
            references.extend(definition.skill_refs)
            references.extend(definition.mcp_server_refs)
            references.extend(definition.tool_refs)
        elif isinstance(definition, ControlProfileDefinition):
            references.append(definition.blueprint_ref)
        elif isinstance(definition, WorkflowTypeDefinition):
            references.extend(definition.allowed_blueprints)
            references.extend(definition.allowed_control_profiles)
            references.extend(definition.allowed_runtime_profiles)
            references.extend(definition.allowed_workspace_templates)
            references.extend(definition.allowed_evaluation_profiles)
        elif isinstance(definition, WorkflowImplementationBindingDefinition):
            references.extend(
                {
                    definition.workflow_type_ref,
                    definition.blueprint_ref,
                    definition.control_profile_ref,
                    definition.runtime_profile_ref,
                    definition.workspace_template_ref,
                    definition.evaluation_profile_ref,
                }
            )
        assert all(
            positions[(reference.kind, reference.logical_id)] < index
            for reference in references
        )


async def test_complete_fixture_publishes_through_existing_definition_lifecycle() -> None:
    service = ControlPlaneService(
        InMemoryDefinitionRepository(),
        ExtensionRegistry(),
        InMemoryPayloadStore(),
    )
    published = []
    revisions: dict[tuple[str, str], int] = {}
    for definition in web_capability_definitions():
        identity = (definition.kind.value, definition.logical_id)
        record = await service.publish(
            PublishRequest(
                definition=definition,
                actor_id="web-capability-fixture-publisher",
                published_at=NOW,
                expected_head_revision=revisions.get(identity, 0),
            )
        )
        revisions[identity] = record.ref.revision
        published.append(record)

    assert len(published) == 36
    assert all(record.ref.revision == 1 for record in published)
    assert all(record.ref.digest == sha256_digest(record.definition) for record in published)
    assert published[-1].definition.kind == DefinitionKind.WORKFLOW_IMPLEMENTATION
