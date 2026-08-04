from __future__ import annotations

from datetime import UTC, datetime

import pytest
from pydantic import TypeAdapter, ValidationError

from app.application.control_plane import ControlPlaneService
from app.application.control_plane_repository import InMemoryDefinitionRepository
from app.domain.control_plane.canonical import sha256_digest
from app.domain.control_plane.contracts import (
    AgentProfileDefinition,
    AuthorityCeiling,
    BudgetCeiling,
    CatalogPayloadRef,
    Definition,
    DefinitionKind,
    ExactDefinitionRef,
    MCPNetworkRequirement,
    MCPServerDefinition,
    MCPToolDefinition,
    ModelPolicy,
    MoveAliasRequest,
    PromptDefinition,
    PromptVariable,
    PublishRequest,
    RetireRequest,
    SkillCompatibility,
    SkillDefinition,
    SkillFileManifestEntry,
    SourceProvenance,
)
from app.domain.control_plane.errors import CompilationRejected
from app.domain.control_plane.extensions import ExtensionRegistry
from app.integrations.control_plane_payloads import InMemoryPayloadStore

NOW = datetime(2026, 7, 25, tzinfo=UTC)
FAKE_DIGEST = "sha256:" + "1" * 64
DEFINITION_ADAPTER: TypeAdapter[Definition] = TypeAdapter(Definition)


def payload_ref(digest: str = FAKE_DIGEST) -> CatalogPayloadRef:
    return CatalogPayloadRef(
        uri=f"s3://belllabs-catalog/{digest.removeprefix('sha256:')}",
        digest=digest,
        media_type="application/octet-stream",
        size_bytes=12,
    )


def provenance(identity: str) -> SourceProvenance:
    return SourceProvenance(
        source="local",
        locator=f".agents/skills/{identity}",
        upstream_identity=identity,
        upstream_version="1",
        license="Apache-2.0",
    )


def skill_definition() -> SkillDefinition:
    manifest = (
        SkillFileManifestEntry(
            path="SKILL.md",
            digest="sha256:" + "2" * 64,
            size_bytes=100,
        ),
    )
    return SkillDefinition(
        logical_id="skill.firecrawl-search",
        title="Firecrawl Search",
        description="Reviewed procedure for current web search through Firecrawl.",
        skill_name="firecrawl-search",
        frontmatter={"name": "firecrawl-search", "description": "Search the web."},
        body_summary="Search current public web sources and preserve citations.",
        bundle_ref=payload_ref("sha256:" + "3" * 64),
        manifest_digest=sha256_digest(manifest),
        file_manifest=manifest,
        required_capabilities=frozenset({"web.search.firecrawl"}),
        compatibility=SkillCompatibility(
            runtimes=frozenset({"python-3.12"}),
            network_capabilities=frozenset({"network.public_web"}),
        ),
        source_provenance=provenance("firecrawl-search"),
        review_status="approved",
    )


def server_definition() -> MCPServerDefinition:
    snapshot_digest = "sha256:" + "4" * 64
    return MCPServerDefinition(
        logical_id="mcp.firecrawl",
        title="Firecrawl MCP",
        description="Reviewed Firecrawl MCP server recipe.",
        transport="streamable_http",
        endpoint="https://mcp.firecrawl.dev/mcp",
        allowed_tools=frozenset({"firecrawl_search", "firecrawl_scrape"}),
        approval_policy={"firecrawl_search": "never", "firecrawl_scrape": "never"},
        network_requirements=(
            MCPNetworkRequirement(host="mcp.firecrawl.dev", port=443, protocol="https"),
        ),
        schema_snapshot_ref=payload_ref(snapshot_digest),
        schema_digest=snapshot_digest,
        source_provenance=provenance("firecrawl-mcp"),
        review_status="approved",
    )


def tool_definition(server_ref: ExactDefinitionRef) -> MCPToolDefinition:
    schema = {
        "tool_name": "firecrawl_search",
        "input_schema": {
            "type": "object",
            "properties": {"query": {"type": "string"}},
            "required": ["query"],
        },
        "output_schema": {"type": "object"},
        "annotations": {"readOnlyHint": True},
    }
    return MCPToolDefinition(
        logical_id="mcp.firecrawl.firecrawl-search",
        title="Firecrawl search",
        description="Search public web sources and return cited results.",
        server_ref=server_ref,
        tool_name="firecrawl_search",
        input_schema=schema["input_schema"],
        output_schema=schema["output_schema"],
        annotations=schema["annotations"],
        schema_digest=sha256_digest(schema),
        side_effect_class="read_only",
    )


async def service() -> ControlPlaneService:
    return ControlPlaneService(
        InMemoryDefinitionRepository(),
        ExtensionRegistry(),
        InMemoryPayloadStore(),
    )


async def publish(control_plane: ControlPlaneService, definition: Definition):
    return await control_plane.publish(
        PublishRequest(
            definition=definition,
            actor_id="publisher",
            published_at=NOW,
            expected_head_revision=0,
        )
    )


def test_all_agentic_asset_definitions_round_trip_through_union() -> None:
    prompt = PromptDefinition(
        logical_id="prompt.web-research",
        title="Web research",
        description="Reviewed web-research prompt.",
        format="markdown",
        template_engine="jinja2",
        variables=(
            PromptVariable(name="question", description="Research question"),
        ),
        body="Research {{ question }} using exact bound capabilities.",
        trust_class="privileged",
        eval_refs=frozenset({"eval.citation-integrity"}),
    )
    skill = skill_definition()
    server = server_definition()
    server_ref = ExactDefinitionRef(
        kind=DefinitionKind.MCP_SERVER,
        logical_id=server.logical_id,
        revision=1,
        digest=sha256_digest(server),
    )
    tool = tool_definition(server_ref)
    profile = AgentProfileDefinition(
        logical_id="agent.web-research",
        title="Web research agent",
        description="Bound two-provider web researcher.",
        prompt_refs=frozenset(
            {
                ExactDefinitionRef(
                    kind=DefinitionKind.PROMPT,
                    logical_id=prompt.logical_id,
                    revision=1,
                    digest=sha256_digest(prompt),
                )
            }
        ),
        skill_refs=frozenset(
            {
                ExactDefinitionRef(
                    kind=DefinitionKind.SKILL,
                    logical_id=skill.logical_id,
                    revision=1,
                    digest=sha256_digest(skill),
                )
            }
        ),
        mcp_server_refs=frozenset({server_ref}),
        tool_refs=frozenset(
            {
                ExactDefinitionRef(
                    kind=DefinitionKind.MCP_TOOL,
                    logical_id=tool.logical_id,
                    revision=1,
                    digest=sha256_digest(tool),
                )
            }
        ),
        model_policy=ModelPolicy(provider="openai", model="gpt-5.4-mini"),
        maximum_capability_request=AuthorityCeiling(
            capabilities=frozenset({"web.search.firecrawl"}),
            budgets=BudgetCeiling(dimensions={"tool.calls.total": 4}),
        ),
    )

    for definition in (prompt, skill, server, tool, profile):
        payload = definition.model_dump(mode="json")
        parsed = DEFINITION_ADAPTER.validate_python(payload)
        assert type(parsed) is type(definition)
        assert sha256_digest(parsed) == sha256_digest(definition)


@pytest.mark.asyncio
async def test_agentic_assets_publish_with_exact_parent_binding() -> None:
    control_plane = await service()
    prompt = await publish(
        control_plane,
        PromptDefinition(
            logical_id="prompt.search",
            title="Search",
            description="Search reviewed sources.",
            format="text",
            template_engine="none",
            body="Search reviewed sources.",
            trust_class="reviewed",
        ),
    )
    skill = await publish(control_plane, skill_definition())
    server = await publish(control_plane, server_definition())
    tool = await publish(control_plane, tool_definition(server.ref))
    profile = await publish(
        control_plane,
        AgentProfileDefinition(
            logical_id="agent.search",
            title="Search agent",
            description="Exact reviewed search bindings.",
            prompt_refs=frozenset({prompt.ref}),
            skill_refs=frozenset({skill.ref}),
            mcp_server_refs=frozenset({server.ref}),
            tool_refs=frozenset({tool.ref}),
            model_policy=ModelPolicy(provider="openai", model="gpt-5.4-mini"),
            maximum_capability_request=AuthorityCeiling(
                capabilities=frozenset({"web.search.firecrawl"})
            ),
        ),
    )

    assert profile.ref.kind == DefinitionKind.AGENT_PROFILE
    binding = await control_plane.move_alias(
        MoveAliasRequest(
            alias={
                "kind": "agent_profile",
                "logical_id": "agent.search",
                "alias": "default",
            },
            target=profile.ref,
            actor_id="publisher",
            moved_at=NOW,
        )
    )
    assert binding.target == profile.ref


@pytest.mark.asyncio
async def test_mcp_tool_schema_and_parent_allowlist_are_verified() -> None:
    control_plane = await service()
    server = await publish(control_plane, server_definition())
    valid = tool_definition(server.ref)

    with pytest.raises(CompilationRejected, match="schema digest"):
        await publish(
            control_plane,
            valid.model_copy(update={"schema_digest": "sha256:" + "9" * 64}),
        )

    with pytest.raises(CompilationRejected, match="allowlist"):
        await publish(
            control_plane,
            valid.model_copy(
                update={
                    "logical_id": "mcp.firecrawl.hidden",
                    "tool_name": "hidden_admin_tool",
                    "schema_digest": sha256_digest(
                        {
                            "tool_name": "hidden_admin_tool",
                            "input_schema": valid.input_schema,
                            "output_schema": valid.output_schema,
                            "annotations": valid.annotations,
                        }
                    ),
                }
            ),
        )


def test_catalog_contracts_reject_credential_bearing_urls_and_wrong_refs() -> None:
    with pytest.raises(ValidationError, match="sanitized"):
        server_definition().model_copy(
            update={"endpoint": "https://user:password@example.test/mcp?token=secret"}
        )
        MCPServerDefinition.model_validate(
            {
                **server_definition().model_dump(mode="json"),
                "endpoint": "https://user:password@example.test/mcp?token=secret",
            }
        )

    with pytest.raises(ValidationError, match="MCP Server"):
        tool_definition(
            ExactDefinitionRef(
                kind=DefinitionKind.SKILL,
                logical_id="skill.not-a-server",
                revision=1,
                digest=FAKE_DIGEST,
            )
        )


@pytest.mark.asyncio
async def test_skill_manifest_digest_is_verified_at_publication() -> None:
    control_plane = await service()
    with pytest.raises(CompilationRejected, match="manifest digest"):
        await publish(
            control_plane,
            skill_definition().model_copy(
                update={"manifest_digest": "sha256:" + "8" * 64}
            ),
        )


@pytest.mark.asyncio
async def test_publication_and_retirement_emit_deterministic_projection_events() -> None:
    repository = InMemoryDefinitionRepository()
    control_plane = ControlPlaneService(
        repository,
        ExtensionRegistry(),
        InMemoryPayloadStore(),
    )
    published = await publish(control_plane, skill_definition())
    initial_events = await repository.list_projection_events()

    assert len(initial_events) == 1
    assert initial_events[0]["operation"] == "upsert"
    assert initial_events[0]["source_digest"] == published.ref.digest
    assert initial_events[0]["tenant_scope"] == "global"

    await control_plane.retire(
        RetireRequest(
            ref=published.ref,
            actor_id="publisher",
            retired_at=NOW,
        )
    )
    events = await repository.list_projection_events()
    assert {event["operation"] for event in events} == {"upsert", "retire"}
    assert len({event["event_id"] for event in events}) == 2
