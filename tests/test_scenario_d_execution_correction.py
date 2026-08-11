from __future__ import annotations

from datetime import UTC, datetime

import pytest

from app.application.control_plane import ControlPlaneService
from app.application.control_plane_repository import InMemoryDefinitionRepository
from app.application.reviewed_capability_promotion import (
    build_scenario_d_execution_correction,
    publish_scenario_d_execution_correction,
)
from app.application.web_research_semantic_handlers import (
    resolve_web_research_run_authority,
)
from app.domain.control_plane.contracts import (
    ControlProfileDefinition,
    DefinitionKind,
    PublishedDefinition,
    PublishRequest,
    StageGraphBlueprint,
    WorkflowImplementationBindingDefinition,
    WorkflowTypeDefinition,
)
from app.domain.control_plane.extensions import ExtensionRegistry
from app.domain.coordinator.web_capability_fixtures import (
    web_capability_definitions,
)
from app.domain.coordinator.web_research_runtime import WebResearchGoal
from app.integrations.control_plane_payloads import InMemoryPayloadStore
from tests.test_web_research_semantic_handlers import (
    BROWSER_RUNTIME,
    FIRECRAWL_RUNTIME,
    TAVILY_RUNTIME,
    binding_authority,
)

NOW = datetime(2026, 7, 26, 15, 0, tzinfo=UTC)


async def seeded_catalog() -> tuple[
    ControlPlaneService,
    InMemoryDefinitionRepository,
    tuple[PublishedDefinition, ...],
]:
    repository = InMemoryDefinitionRepository()
    service = ControlPlaneService(
        repository,
        ExtensionRegistry(),
        InMemoryPayloadStore(),
    )
    records = tuple(
        [
            await service.publish(
                PublishRequest(
                    definition=definition,
                    actor_id="fixture-publisher",
                    published_at=NOW,
                    expected_head_revision=0,
                )
            )
            for definition in web_capability_definitions()
        ]
    )
    return service, repository, records


@pytest.mark.asyncio
async def test_corrective_bundle_advances_exact_transitive_chain_and_preserves_rev1() -> None:
    service, repository, records = await seeded_catalog()
    original_blueprint = next(
        record for record in records if record.ref.kind == DefinitionKind.BLUEPRINT
    )
    assert isinstance(original_blueprint.definition, StageGraphBlueprint)
    original_admission = next(
        stage
        for stage in original_blueprint.definition.stages
        if stage.stage_id == "admit_public_goal"
    )
    assert original_admission.operation_slots[0].reservation == {
        "operation.attempts": 1
    }

    bundle = build_scenario_d_execution_correction(catalog_records=records)
    blueprint, control, workflow, implementation = bundle.definitions
    assert isinstance(blueprint, StageGraphBlueprint)
    assert isinstance(control, ControlProfileDefinition)
    assert isinstance(workflow, WorkflowTypeDefinition)
    assert isinstance(implementation, WorkflowImplementationBindingDefinition)
    corrected_admission = next(
        stage for stage in blueprint.stages if stage.stage_id == "admit_public_goal"
    )
    assert corrected_admission.reservation == {"operation.attempts": 1}
    blueprint_ref, control_ref, workflow_ref, implementation_ref = bundle.refs
    assert {ref.revision for ref in bundle.refs} == {2}
    assert control.blueprint_ref == blueprint_ref
    assert workflow.allowed_blueprints == frozenset({blueprint_ref})
    assert workflow.allowed_control_profiles == frozenset({control_ref})
    assert implementation.workflow_type_ref == workflow_ref
    assert implementation.blueprint_ref == blueprint_ref
    assert implementation.control_profile_ref == control_ref

    result = await publish_scenario_d_execution_correction(
        service=service,
        bundle=bundle,
        catalog_records=records,
        actor_id="scenario-d-correction",
        changed_at=NOW,
    )

    assert result.published == bundle.refs
    assert not result.reused
    assert (await repository.get(original_blueprint.ref)).retired_at is None
    assert (await repository.get(original_blueprint.ref)).definition == (
        original_blueprint.definition
    )
    assert (await repository.get(implementation_ref)).definition == implementation


@pytest.mark.asyncio
async def test_correction_is_resumable_and_binding_resolves_current_exact_heads() -> None:
    service, repository, records = await seeded_catalog()
    bundle = build_scenario_d_execution_correction(catalog_records=records)
    first = await publish_scenario_d_execution_correction(
        service=service,
        bundle=bundle,
        catalog_records=records,
        actor_id="scenario-d-correction",
        changed_at=NOW,
    )
    refreshed = records + tuple([await repository.get(ref) for ref in first.published])
    rebuilt = build_scenario_d_execution_correction(catalog_records=refreshed)
    second = await publish_scenario_d_execution_correction(
        service=service,
        bundle=rebuilt,
        catalog_records=refreshed,
        actor_id="scenario-d-correction",
        changed_at=NOW,
    )

    assert not second.published
    assert second.reused == rebuilt.refs
    head_by_identity = {
        (record.ref.kind, record.ref.logical_id): record.ref for record in refreshed
    }
    selected_capability_refs = tuple(
        head_by_identity[key]
        for key in (
            (DefinitionKind.MCP_SERVER, "mcp.firecrawl"),
            (DefinitionKind.MCP_SERVER, "mcp.tavily"),
            (
                DefinitionKind.MCP_TOOL,
                "mcp.firecrawl:firecrawl_search",
            ),
            (DefinitionKind.MCP_TOOL, "mcp.tavily:tavily_search"),
            (DefinitionKind.SKILL, "skill.firecrawl-search"),
            (DefinitionKind.SKILL, "skill.tavily-search"),
            (DefinitionKind.SKILL, "skill.agent-browser"),
            (
                DefinitionKind.AGENT_PROFILE,
                "agent-profile.web-research-browser-verification",
            ),
        )
    )
    firecrawl_ref = head_by_identity[(DefinitionKind.MCP_TOOL, "mcp.firecrawl:firecrawl_search")]
    tavily_ref = head_by_identity[(DefinitionKind.MCP_TOOL, "mcp.tavily:tavily_search")]
    browser_ref = head_by_identity[(DefinitionKind.SKILL, "skill.agent-browser")]
    authority = resolve_web_research_run_authority(
        catalog_records=refreshed,
        request_scope="tenant:scenario-d",
        run_id="run-scenario-d",
        goal=WebResearchGoal(question="Research a public company technology"),
        effective_configuration_digest="sha256:" + "a" * 64,
        created_at=NOW,
        firecrawl_runtime=FIRECRAWL_RUNTIME,
        tavily_runtime=TAVILY_RUNTIME,
        browser_runtime=BROWSER_RUNTIME,
        selected_capability_refs=selected_capability_refs,
        **binding_authority(
            firecrawl_ref,
            tavily_ref,
            browser_ref,
            configuration_digest="sha256:" + "a" * 64,
        ),
    )
    assert authority.blueprint_ref.revision == 2
    assert authority.semantic_binding.blueprint_digest == (authority.blueprint_ref.digest)
    assert authority.firecrawl_tool_ref.logical_id == ("mcp.firecrawl:firecrawl_search")
    assert authority.tavily_tool_ref.logical_id == "mcp.tavily:tavily_search"
    assert authority.browser_skill_ref.logical_id == "skill.agent-browser"
