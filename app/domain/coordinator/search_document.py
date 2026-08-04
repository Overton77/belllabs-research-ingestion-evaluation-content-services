from __future__ import annotations

import json
from collections.abc import Iterable, Mapping
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from app.domain.control_plane.contracts import (
    AgentProfileDefinition,
    ControlProfileDefinition,
    Definition,
    EvaluationProfileDefinition,
    ExactDefinitionRef,
    MCPServerDefinition,
    MCPToolDefinition,
    PromptDefinition,
    RuntimeProfileDefinition,
    SkillDefinition,
    StageGraphBlueprint,
    WorkflowConfigurationDefinition,
    WorkflowImplementationBindingDefinition,
    WorkflowTypeDefinition,
    WorkspaceTemplateDefinition,
)
from app.domain.coordinator.contracts import SearchDocumentMetadata, SearchDocumentSource

SEARCH_DOCUMENT_FORMAT_VERSION = 1


class RenderedSearchDocument(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    search_text: str = Field(min_length=1)
    search_document_format_version: int = SEARCH_DOCUMENT_FORMAT_VERSION


def _text(value: object) -> str:
    return " ".join(str(value).split())


def _items(values: object) -> str:
    if isinstance(values, str | bytes) or isinstance(values, Mapping):
        return _text(values)
    if not isinstance(values, Iterable):
        return _text(values)
    normalized = sorted({_text(value) for value in values if _text(value)})
    return ", ".join(normalized)


def _json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _exact_ref(ref: ExactDefinitionRef) -> str:
    return f"{ref.kind.value}:{ref.logical_id}@{ref.revision}#{ref.digest}"


def search_document_source(
    definition: Definition,
    metadata: SearchDocumentMetadata | None = None,
) -> SearchDocumentSource:
    """Build a safe projection source without prompt bodies, bundles, or credentials."""
    metadata = metadata or SearchDocumentMetadata()
    intended_uses = set(metadata.intended_uses)
    non_goals: set[str] = set()
    input_summary = ""
    output_summary = ""
    authority_summary = ""
    compatibility_parts = set(metadata.compatibility_notes)
    parent_ref = None
    tool_names: set[str] = set()

    if isinstance(definition, WorkflowTypeDefinition):
        intended_uses.add(definition.purpose)
        non_goals.update(definition.non_goals)
        input_summary = definition.input_admission_contract
        output_summary = _items(definition.output_contracts)
        authority_summary = _authority(definition.authority_ceiling)
    elif isinstance(definition, StageGraphBlueprint):
        intended_uses.add(f"Execute a {definition.family} workflow blueprint")
        input_summary = "stages: " + _items(stage.stage_id for stage in definition.stages)
        output_summary = _items(definition.declared_output_slots)
        authority_summary = f"maximum parallel stages: {definition.max_parallel_stages}"
    elif isinstance(definition, ControlProfileDefinition):
        intended_uses.add("Apply bounded workflow control policy")
        authority_summary = _authority(definition.authority_ceiling)
        compatibility_parts.add(f"blueprint: {_exact_ref(definition.blueprint_ref)}")
    elif isinstance(definition, RuntimeProfileDefinition):
        intended_uses.add("Provide an executable runtime binding")
        authority_summary = "required capabilities: " + _items(
            definition.required_capabilities
        )
        compatibility_parts.add(f"runtime binding: {definition.binding}")
    elif isinstance(definition, WorkspaceTemplateDefinition):
        intended_uses.add("Materialize governed workflow workspace slots")
        input_summary = _items(slot.purpose for slot in definition.slots)
        output_summary = _items(slot.name for slot in definition.slots)
        authority_summary = "required capabilities: " + _items(
            definition.required_capabilities
        )
    elif isinstance(definition, EvaluationProfileDefinition):
        intended_uses.add("Evaluate workflow outputs against frozen gates")
        input_summary = _items(definition.gate_contract_refs)
        authority_summary = "required capabilities: " + _items(
            definition.required_capabilities
        )
    elif isinstance(definition, WorkflowConfigurationDefinition):
        intended_uses.add("Configure one exact Workflow Type")
        compatibility_parts.add(
            f"workflow type logical identifier: {definition.workflow_type_logical_id}"
        )
    elif isinstance(definition, WorkflowImplementationBindingDefinition):
        intended_uses.add("Bind an approved exact Workflow Type implementation")
        input_summary = f"workflow type: {_exact_ref(definition.workflow_type_ref)}"
        output_summary = _items(
            item.output_contract_ref for item in definition.output_contract_realizations
        )
        compatibility_parts.add(f"blueprint: {_exact_ref(definition.blueprint_ref)}")
    elif isinstance(definition, PromptDefinition):
        intended_uses.add("Render a governed prompt without indexing its body")
        input_summary = _items(variable.name for variable in definition.variables)
        output_summary = f"{definition.format} prompt"
        authority_summary = f"trust class: {definition.trust_class}"
        compatibility_parts.add(f"template engine: {definition.template_engine}")
    elif isinstance(definition, SkillDefinition):
        intended_uses.add(definition.body_summary)
        authority_summary = "required capabilities: " + _items(
            definition.required_capabilities
        )
        compatibility_parts.update(_skill_compatibility(definition))
    elif isinstance(definition, MCPServerDefinition):
        intended_uses.add("Provide governed MCP tools")
        tool_names.update(definition.allowed_tools)
        authority_summary = (
            f"transport: {definition.transport}; "
            f"credential references required: {len(definition.credential_refs)}; "
            f"network hosts: {_items(item.host for item in definition.network_requirements)}"
        )
        compatibility_parts.add(f"review status: {definition.review_status}")
    elif isinstance(definition, MCPToolDefinition):
        intended_uses.add(definition.description)
        input_summary = _json(definition.input_schema)
        output_summary = (
            _json(definition.output_schema)
            if definition.output_schema is not None
            else "unspecified"
        )
        authority_summary = f"side effect class: {definition.side_effect_class}"
        parent_ref = definition.server_ref
    elif isinstance(definition, AgentProfileDefinition):
        intended_uses.add("Configure an exact governed agent profile")
        input_summary = (
            f"prompts: {len(definition.prompt_refs)}; "
            f"skills: {len(definition.skill_refs)}; "
            f"MCP servers: {len(definition.mcp_server_refs)}; "
            f"tools: {len(definition.tool_refs)}"
        )
        output_summary = definition.output_schema_ref or "unstructured"
        authority_summary = _authority(definition.maximum_capability_request)
        compatibility_parts.add(
            f"model: {definition.model_policy.provider}/{definition.model_policy.model}"
        )

    return SearchDocumentSource(
        title=definition.title,
        logical_id=definition.logical_id,
        aliases=metadata.aliases,
        asset_kind=definition.kind,
        description=definition.description,
        intended_uses=frozenset(intended_uses),
        non_goals=frozenset(non_goals),
        input_summary=input_summary,
        output_summary=output_summary,
        capability_authority_summary=authority_summary,
        compatibility_summary=_items(compatibility_parts),
        tags=metadata.tags,
        domains=metadata.domains,
        parent_server_ref=parent_ref,
        tool_names=frozenset(tool_names),
    )


def render_search_document(source: SearchDocumentSource) -> RenderedSearchDocument:
    aliases = _items(source.aliases) or "none"
    tags = _items(source.tags) or "none"
    domains = _items(source.domains) or "none"
    parent = _exact_ref(source.parent_server_ref) if source.parent_server_ref else "none"
    lines = (
        ("title", source.title),
        (
            "logical identifier and aliases",
            f"{source.logical_id}; aliases: {aliases}",
        ),
        ("asset kind", source.asset_kind.value),
        ("short description", source.description),
        ("intended uses", _items(source.intended_uses) or "none"),
        ("non-goals", _items(source.non_goals) or "none"),
        ("input summary", source.input_summary or "none"),
        ("output summary", source.output_summary or "none"),
        (
            "capability and authority summary",
            source.capability_authority_summary or "none",
        ),
        ("compatibility summary", source.compatibility_summary or "none"),
        ("tags and domains", f"tags: {tags}; domains: {domains}"),
        ("parent server identity", parent),
        ("tool names", _items(source.tool_names) or "none"),
    )
    return RenderedSearchDocument(
        search_text="\n".join(f"{label}: {_text(value)}" for label, value in lines)
    )


def _authority(authority: Any) -> str:
    budgets = ", ".join(
        f"{name}={amount}" for name, amount in sorted(authority.budgets.dimensions.items())
    )
    return (
        f"capabilities: {_items(authority.capabilities) or 'none'}; "
        f"budgets: {budgets or 'none'}; "
        f"maximum concurrency: {authority.max_concurrency}"
    )


def _skill_compatibility(definition: SkillDefinition) -> set[str]:
    compatibility = definition.compatibility
    return {
        f"runtimes: {_items(compatibility.runtimes) or 'none'}",
        f"executables: {_items(compatibility.executables) or 'none'}",
        (
            "network capabilities: "
            f"{_items(compatibility.network_capabilities) or 'none'}"
        ),
        (
            "workspace capabilities: "
            f"{_items(compatibility.workspace_capabilities) or 'none'}"
        ),
    }
