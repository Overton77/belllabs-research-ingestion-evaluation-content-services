from __future__ import annotations

from datetime import UTC, datetime
from hashlib import sha256

import pytest
from fastmcp import Client, Context, FastMCP
from fastmcp.server.auth import AccessToken

from app.application.capability_search import CapabilitySearchService
from app.application.capability_search_repository import (
    CapabilityEmbedding,
    InMemoryCatalogSearchRepository,
)
from app.application.catalog_projection import CatalogProjector
from app.application.control_plane_repository import InMemoryDefinitionRepository
from app.application.coordinator_facade import (
    BlueprintRuntimeStatus,
    CoordinatorFeatureFlags,
    InMemoryCoordinatorAuditSink,
    ProductionCoordinatorFacade,
)
from app.domain.control_plane.canonical import sha256_digest
from app.domain.control_plane.contracts import (
    CatalogPayloadRef,
    DefinitionSelector,
    PromptDefinition,
    PromptVariable,
    SkillCompatibility,
    SkillDefinition,
    SkillFileManifestEntry,
    SourceProvenance,
    StageGraphBlueprint,
    WorkflowTypeDefinition,
)
from app.domain.coordinator.launch import BlueprintFamily
from app.domain.coordinator.web_capability_fixtures import (
    web_capability_definitions,
)
from app.mcp.coordinator_auth import VerifiedAccessTokenPrincipalResolver
from app.mcp.coordinator_server import (
    CoordinatorPrincipal,
    create_coordinator_server,
)

NOW = datetime(2026, 7, 25, 20, 0, tzinfo=UTC)
DIGEST = f"sha256:{'a' * 64}"


class DeterministicEmbeddings:
    async def embed(self, text: str) -> CapabilityEmbedding:
        normalized = text.casefold()
        vector = (
            1.0 if "web" in normalized else 0.0,
            1.0 if "research" in normalized else 0.0,
            1.0 if "browser" in normalized else 0.0,
        )
        return CapabilityEmbedding(
            vector=vector,
            model_id="text-embedding-3-small",
            dimensions=3,
            input_digest=f"sha256:{sha256(text.encode()).hexdigest()}",
        )


class ReadyRuntimes:
    async def snapshot(self) -> tuple[BlueprintRuntimeStatus, ...]:
        return (
            BlueprintRuntimeStatus(
                family=BlueprintFamily.STAGE_GRAPH,
                executable=True,
                reason="registered experiment worker passed preflight",
                evidence_ref="worker-preflight:stagegraph:test",
            ),
            BlueprintRuntimeStatus(
                family=BlueprintFamily.GOAL_DIRECTED,
                executable=True,
                reason="registered experiment worker passed preflight",
                evidence_ref="worker-preflight:goal-directed:test",
            ),
        )


class StaticPrincipalResolver:
    def __init__(self, permissions: frozenset[str]) -> None:
        self.principal = CoordinatorPrincipal(
            actor_id="operator-1",
            tenant_scope="tenant-a",
            roles=frozenset({"coordinator_planner"}),
            permissions=permissions,
        )

    async def resolve(self, _context: Context) -> CoordinatorPrincipal:
        return self.principal


def coordinator_skill() -> SkillDefinition:
    file_entry = SkillFileManifestEntry(
        path="SKILL.md",
        digest=DIGEST,
        size_bytes=512,
    )
    return SkillDefinition(
        logical_id="skill.belllabs-workflow-coordinator",
        title="BellLabs Workflow Coordinator",
        description="Search exact governed capabilities and prepare admitted workflows.",
        skill_name="belllabs-workflow-coordinator",
        frontmatter={"name": "belllabs-workflow-coordinator"},
        body_summary="Use the coordinator MCP surface progressively.",
        bundle_ref=CatalogPayloadRef(
            uri="s3://catalog/skills/belllabs-workflow-coordinator.tar",
            digest=DIGEST,
            media_type="application/x-tar",
            size_bytes=1_024,
        ),
        manifest_digest=sha256_digest((file_entry.model_dump(mode="json"),)),
        file_manifest=(file_entry,),
        compatibility=SkillCompatibility(runtimes=frozenset({"codex"})),
        source_provenance=SourceProvenance(
            source="belllabs",
            locator="catalog://skills/belllabs-workflow-coordinator",
            upstream_identity="belllabs-workflow-coordinator",
            upstream_version="1",
        ),
        review_status="approved",
    )


def propose_prompt() -> PromptDefinition:
    return PromptDefinition(
        logical_id="prompt.coordinator.propose-workflow",
        title="Propose governed workflow",
        description="Normalize an objective before catalog search and preparation.",
        format="markdown",
        template_engine="format",
        variables=(
            PromptVariable(
                name="objective",
                description="The operator's requested outcome.",
            ),
        ),
        body="Normalize this objective, then search Workflow Types first: {objective}",
        trust_class="reviewed",
    )


async def concrete_facade() -> tuple[
    ProductionCoordinatorFacade,
    InMemoryCoordinatorAuditSink,
]:
    definitions = InMemoryDefinitionRepository()
    search_index = InMemoryCatalogSearchRepository()
    embeddings = DeterministicEmbeddings()
    projector = CatalogProjector(
        definitions=definitions,
        search=search_index,
        embeddings=embeddings,
        embedding_model_id="text-embedding-3-small",
        embedding_dimensions=3,
        projection_generation="facade-test-1",
        clock=lambda: NOW,
    )
    records = []
    for definition in (*web_capability_definitions(), coordinator_skill(), propose_prompt()):
        record = await definitions.publish(
            definition,
            actor_id="publisher-1",
            published_at=NOW,
            expected_head_revision=0,
        )
        records.append(record)
        await projector.project(record.ref)
    skill_record = records[-2]
    prompt_record = records[-1]
    audit = InMemoryCoordinatorAuditSink()
    service = CapabilitySearchService(
        search=search_index,
        definitions=definitions,
        embeddings=embeddings,
        embedding_model_id="text-embedding-3-small",
        embedding_dimensions=3,
    )
    facade = ProductionCoordinatorFacade(
        definitions=definitions,
        catalog_index=search_index,
        search=service,
        readiness=ReadyRuntimes(),
        coordinator_skill=DefinitionSelector(exact=skill_record.ref),
        prompt_bindings={"propose_workflow": prompt_record.ref},
        flags=CoordinatorFeatureFlags(
            capability_search_enabled=True,
            external_discovery_enabled=False,
            coordinator_launch_enabled=False,
        ),
        audit=audit,
        clock=lambda: NOW,
    )
    return facade, audit


@pytest.mark.asyncio
async def test_impossible_enabled_provider_configuration_fails_startup() -> None:
    facade, audit = await concrete_facade()
    common = {
        "definitions": facade._definitions,
        "catalog_index": facade._catalog_index,
        "readiness": facade._readiness,
        "coordinator_skill": facade._coordinator_skill,
        "prompt_bindings": {},
        "audit": audit,
    }
    with pytest.raises(ValueError, match="search provider"):
        ProductionCoordinatorFacade(
            **common,
            search=None,
            flags=CoordinatorFeatureFlags(
                capability_search_enabled=True,
                external_discovery_enabled=False,
                coordinator_launch_enabled=False,
            ),
        )
    with pytest.raises(ValueError, match="discovery provider"):
        ProductionCoordinatorFacade(
            **common,
            search=None,
            flags=CoordinatorFeatureFlags(
                capability_search_enabled=False,
                external_discovery_enabled=True,
                coordinator_launch_enabled=False,
            ),
        )
    with pytest.raises(ValueError, match="without providers"):
        ProductionCoordinatorFacade(
            **common,
            search=None,
            flags=CoordinatorFeatureFlags(
                capability_search_enabled=False,
                external_discovery_enabled=False,
                coordinator_launch_enabled=True,
            ),
        )


@pytest.mark.asyncio
async def test_concrete_facade_through_in_memory_fastmcp() -> None:
    facade, audit = await concrete_facade()
    permissions = frozenset({"catalog.read"})
    server = create_coordinator_server(facade, StaticPrincipalResolver(permissions))

    async with Client(server) as client:
        bootstrap = await client.call_tool("coordinator_bootstrap")
        assert bootstrap.data["ok"] is True
        assert audit.events[-1].correlation_id == bootstrap.data["correlation_id"]
        assert bootstrap.data["data"]["executable_blueprint_families"] == [
            "StageGraph",
            "GoalDirected",
        ]
        assert (
            bootstrap.data["data"]["coordinator_skill_ref"]["logical_id"]
            == "skill.belllabs-workflow-coordinator"
        )
        assert bootstrap.data["data"]["root_tools"] == [
            "coordinator_bootstrap",
            "search_capabilities",
            "get_capability",
            "validate_workflow_design",
        ]
        assert bootstrap.data["data"]["prompts"] == ["propose_workflow"]
        tools = {tool.name for tool in await client.list_tools()}
        assert tools == set(bootstrap.data["data"]["root_tools"])
        prompts = {prompt.name for prompt in await client.list_prompts()}
        assert prompts == {"propose_workflow"}
        resource_templates = {
            str(resource.uriTemplate)
            for resource in await client.list_resource_templates()
        }
        assert resource_templates == set(
            bootstrap.data["data"]["resource_templates"]
        )

        search = await client.call_tool(
            "search_capabilities",
            {
                "query": "web research browser verification",
                "kinds": ["workflow_type"],
                "limit": 5,
            },
        )
        assert search.data["ok"] is True
        hits = search.data["data"]["hits"]
        assert any(
            hit["exact_ref"]["logical_id"] == "web-research-browser-verification"
            for hit in hits
        )
        assert {
            measurement["metric_kind"]
            for measurement in search.data["data"]["token_use"]
        } == {"search_query", "search_results"}
        assert all(
            measurement["estimated_tokens"] > 0
            for measurement in search.data["data"]["token_use"]
        )

        workflow_hit = next(
            hit
            for hit in hits
            if hit["exact_ref"]["logical_id"]
            == "web-research-browser-verification"
        )
        detail = await client.call_tool(
            "get_capability",
            {"exact_ref": workflow_hit["exact_ref"]},
        )
        assert detail.data["ok"] is True
        assert detail.data["data"]["definition"]["purpose"]
        assert detail.data["data"]["token_use"][0]["metric_kind"] == (
            "catalog_definition"
        )

        tool_search = await client.call_tool(
            "search_capabilities",
            {
                "query": "current public web search",
                "kinds": ["mcp_tool"],
                "limit": 5,
            },
        )
        tool_hit = next(
            hit
            for hit in tool_search.data["data"]["hits"]
            if hit["exact_ref"]["logical_id"]
            == "mcp.firecrawl:firecrawl_search"
        )
        tool_detail = await client.call_tool(
            "get_capability",
            {"exact_ref": tool_hit["exact_ref"]},
        )
        assert {
            measurement["metric_kind"]
            for measurement in tool_detail.data["data"]["token_use"]
        } == {"catalog_definition", "tool_schema"}

        resources = await client.read_resource(
            "belllabs://workflow-types/web-research-browser-verification/1/contract"
        )
        assert "authority_ceiling" in resources[0].text
        assert workflow_hit["exact_ref"]["digest"] in resources[0].text

        prompt = await client.get_prompt(
            "propose_workflow",
            {"objective": "verify a current company technology inventory"},
        )
        text = prompt.messages[0].content.text
        assert "prompt.coordinator.propose-workflow" in text
        assert "rendered_digest" in text
        assert "Workflow Types first" in text

    assert {event.operation for event in audit.events} >= {
        "coordinator_bootstrap",
        "search_capabilities",
        "get_capability",
        "read_resource",
        "render_prompt",
    }
    assert all(event.request_digest.startswith("sha256:") for event in audit.events)


@pytest.mark.asyncio
async def test_service_layer_rechecks_permissions_and_tenant_scope() -> None:
    facade, _audit = await concrete_facade()
    principal = CoordinatorPrincipal(
        actor_id="operator-1",
        tenant_scope="tenant-a",
        roles=frozenset({"coordinator_planner"}),
        permissions=frozenset(),
    )
    with pytest.raises(Exception) as denied:
        await facade.search(
            principal,
            {
                "query": "web research",
                "kinds": ["workflow_type"],
                "tenant_scope": "tenant-a",
            },
        )
    assert getattr(denied.value, "code", None).value == "FORBIDDEN"

    scoped = principal.__class__(
        actor_id=principal.actor_id,
        tenant_scope=principal.tenant_scope,
        roles=principal.roles,
        permissions=frozenset({"catalog.read"}),
    )
    with pytest.raises(Exception) as cross_tenant:
        await facade.search(
            scoped,
            {
                "query": "web research",
                "kinds": ["workflow_type"],
                "tenant_scope": "tenant-b",
            },
        )
    assert getattr(cross_tenant.value, "code", None).value == "FORBIDDEN"


@pytest.mark.asyncio
async def test_design_validation_never_turns_a_draft_into_launch_authority() -> None:
    facade, _audit = await concrete_facade()
    fixture = web_capability_definitions()
    workflow_type = next(
        item for item in fixture if isinstance(item, WorkflowTypeDefinition)
    )
    blueprint = next(item for item in fixture if isinstance(item, StageGraphBlueprint))
    principal = CoordinatorPrincipal(
        actor_id="operator-1",
        tenant_scope="tenant-a",
        roles=frozenset({"coordinator_planner"}),
        permissions=frozenset({"workflow.design.validate"}),
    )
    result = await facade.validate_workflow_design(
        principal,
        {
            "draft_id": "draft-web-research",
            "purpose": workflow_type.purpose,
            "proposed_workflow_type": workflow_type.model_dump(mode="json"),
            "blueprint_family": "StageGraph",
            "proposed_stage_graph": blueprint.model_dump(mode="json"),
            "input_contract": workflow_type.input_admission_contract,
            "invariants": sorted(workflow_type.invariants),
            "obligations": sorted(workflow_type.obligations),
            "output_contracts": sorted(workflow_type.output_contracts),
            "linked_run_slots": [],
            "requested_assets": [
                {
                    "candidate_id": "candidate:sha256:" + "b" * 64,
                    "purpose": "candidate-only browser procedure",
                }
            ],
            "requested_authority": workflow_type.authority_ceiling.model_dump(
                mode="json"
            ),
            "workspace_requirements": workflow_type.workspace_contract.model_dump(
                mode="json"
            ),
            "budgets": workflow_type.authority_ceiling.budgets.model_dump(mode="json"),
            "rationale": "Exercise draft validation without publication.",
        },
    )
    assert result.requires_publication is True
    assert result.launchable is False
    assert result.candidate_ids_requiring_promotion
    assert any("requires inspection and publication" in item for item in result.findings)


@pytest.mark.asyncio
async def test_verified_token_resolver_uses_claims_not_tool_arguments() -> None:
    token = AccessToken(
        token="opaque",
        client_id="coordinator-client",
        subject="operator-7",
        scopes=["catalog.read", "workflow.prepare"],
        claims={
            "tenant_scope": "tenant-verified",
            "request_scope": "request-verified",
            "request_scopes": ["request-verified"],
            "roles": ["coordinator_planner"],
            "permissions": ["capability.discover"],
        },
    )
    resolver = VerifiedAccessTokenPrincipalResolver(token_reader=lambda: token)
    principal = await resolver.resolve(Context(FastMCP("test")))
    assert principal.actor_id == "operator-7"
    assert principal.tenant_scope == "tenant-verified"
    assert principal.request_scope == "request-verified"
    assert principal.permissions == frozenset(
        {"catalog.read", "workflow.prepare", "capability.discover"}
    )


@pytest.mark.asyncio
async def test_authentication_failure_uses_stable_tool_envelope() -> None:
    facade, _audit = await concrete_facade()
    resolver = VerifiedAccessTokenPrincipalResolver(token_reader=lambda: None)
    server = create_coordinator_server(facade, resolver)
    async with Client(server) as client:
        result = await client.call_tool("coordinator_bootstrap")
    assert result.data["ok"] is False
    assert result.data["error"]["code"] == "UNAUTHENTICATED"
