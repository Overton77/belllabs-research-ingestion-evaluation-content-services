from __future__ import annotations

from collections.abc import Sequence

from pydantic import BaseModel, ConfigDict

from app.domain.control_plane.contracts import (
    AgentProfileDefinition,
    ControlProfileDefinition,
    Definition,
    EvaluationProfileDefinition,
    ExactDefinitionRef,
    MCPServerDefinition,
    MCPToolDefinition,
    PublishedDefinition,
    RuntimeProfileDefinition,
    SkillDefinition,
    WorkflowImplementationBindingDefinition,
    WorkflowTypeDefinition,
    WorkspaceTemplateDefinition,
)


class ProjectionClassification(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    operation_classes: frozenset[str] = frozenset()
    workflow_type_refs: frozenset[ExactDefinitionRef] = frozenset()
    capability_requirements: frozenset[str] = frozenset()
    compatible_runtimes: frozenset[str] = frozenset()


def classify_definition(
    definition: Definition,
    *,
    exact_ref: ExactDefinitionRef,
) -> ProjectionClassification:
    capabilities: frozenset[str] = frozenset()
    runtimes: frozenset[str] = frozenset()
    operation_classes: frozenset[str] = frozenset()
    workflow_refs: frozenset[ExactDefinitionRef] = frozenset()

    if isinstance(definition, SkillDefinition):
        capabilities = definition.required_capabilities
        runtimes = definition.compatibility.runtimes
    elif isinstance(definition, MCPToolDefinition):
        capabilities = _mcp_tool_capabilities(definition.tool_name)
        runtimes = frozenset({"governed-agent-runtime"})
    elif isinstance(definition, MCPServerDefinition):
        capabilities = _mcp_server_capabilities(definition.allowed_tools)
        runtimes = frozenset({"governed-agent-runtime"})
    elif isinstance(definition, AgentProfileDefinition):
        capabilities = definition.maximum_capability_request.capabilities
        runtimes = frozenset({"governed-agent-runtime"})
    elif isinstance(definition, WorkflowTypeDefinition):
        capabilities = definition.authority_ceiling.capabilities
        workflow_refs = frozenset({exact_ref})
    elif isinstance(definition, ControlProfileDefinition):
        capabilities = definition.authority_ceiling.capabilities
    elif isinstance(definition, RuntimeProfileDefinition):
        capabilities = definition.required_capabilities
        runtimes = frozenset({definition.binding, definition.logical_id})
    elif isinstance(definition, WorkspaceTemplateDefinition):
        capabilities = definition.required_capabilities
    elif isinstance(definition, EvaluationProfileDefinition):
        capabilities = definition.required_capabilities

    if _is_research_capability(capabilities):
        operation_classes = frozenset({"research"})
    if "browser.process" in capabilities or "browser-runtime" in runtimes:
        runtimes = runtimes | frozenset({"browser-runtime"})
    return ProjectionClassification(
        operation_classes=operation_classes,
        workflow_type_refs=workflow_refs,
        capability_requirements=capabilities,
        compatible_runtimes=runtimes,
    )


def build_workflow_compatibility(
    published: Sequence[PublishedDefinition],
) -> dict[ExactDefinitionRef, frozenset[ExactDefinitionRef]]:
    """Build discovery compatibility from immutable workflow/profile dependency closure."""
    by_ref = {item.ref: item.definition for item in published}
    result: dict[ExactDefinitionRef, set[ExactDefinitionRef]] = {ref: set() for ref in by_ref}
    for workflow in published:
        definition = workflow.definition
        if not isinstance(definition, WorkflowTypeDefinition):
            continue
        compatible: set[ExactDefinitionRef] = {
            workflow.ref,
            *definition.allowed_blueprints,
            *definition.allowed_control_profiles,
            *definition.allowed_runtime_profiles,
            *definition.allowed_workspace_templates,
            *definition.allowed_evaluation_profiles,
            *definition.allowed_workflow_configurations,
        }
        for item in published:
            candidate = item.definition
            if (
                isinstance(candidate, WorkflowImplementationBindingDefinition)
                and candidate.workflow_type_ref == workflow.ref
            ):
                compatible.update(
                    {
                        item.ref,
                        candidate.blueprint_ref,
                        candidate.control_profile_ref,
                        candidate.runtime_profile_ref,
                        candidate.workspace_template_ref,
                        candidate.evaluation_profile_ref,
                    }
                )
                if candidate.workflow_configuration_ref is not None:
                    compatible.add(candidate.workflow_configuration_ref)
            if isinstance(candidate, AgentProfileDefinition) and (
                candidate.maximum_capability_request.capabilities
                <= definition.authority_ceiling.capabilities
            ):
                compatible.add(item.ref)
                compatible.update(candidate.skill_refs)
                compatible.update(candidate.mcp_server_refs)
                compatible.update(candidate.tool_refs)

        # Discovery compatibility is wider than selection authority: compatible assets
        # still undergo exact rehydration, policy checks, and compilation before use.
        for ref, candidate in by_ref.items():
            classification = classify_definition(candidate, exact_ref=ref)
            requirements = classification.capability_requirements
            if requirements and requirements <= definition.authority_ceiling.capabilities:
                compatible.add(ref)
                if isinstance(candidate, MCPToolDefinition):
                    compatible.add(candidate.server_ref)

        for ref in compatible:
            if ref in result:
                result[ref].add(workflow.ref)
    return {ref: frozenset(workflows) for ref, workflows in result.items()}


def _mcp_server_capabilities(tool_names: frozenset[str]) -> frozenset[str]:
    capabilities = {"network.web"}
    for tool_name in tool_names:
        capabilities.update(_mcp_tool_capabilities(tool_name))
    return frozenset(capabilities)


def _mcp_tool_capabilities(tool_name: str) -> frozenset[str]:
    provider = ""
    if tool_name.startswith("firecrawl_"):
        provider = "firecrawl"
    elif tool_name.startswith("tavily_"):
        provider = "tavily"
    action = tool_name.removeprefix(f"{provider}_") if provider else tool_name
    operation = {
        "search": "web.search",
        "scrape": "web.extract",
        "extract": "web.extract",
        "interact": "browser.interact",
        "map": "web.map",
        "crawl": "web.crawl",
    }.get(action, f"mcp.tool.{action}")
    values = {"network.web"}
    values.add(f"{operation}.{provider}" if provider else operation)
    return frozenset(values)


def _is_research_capability(capabilities: frozenset[str]) -> bool:
    return any(
        capability.startswith(("web.", "browser.", "artifact.research"))
        for capability in capabilities
    )
