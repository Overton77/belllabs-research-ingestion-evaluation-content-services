from __future__ import annotations

import json
import shutil
from datetime import UTC, datetime
from pathlib import Path

import pytest

from app.application.control_plane.service import ControlPlaneService
from app.application.control_plane.control_plane_repository import InMemoryDefinitionRepository
from app.application.capability.reviewed_capability_promotion import (
    AGENT_BROWSER_COMMIT,
    FIRECRAWL_COMMIT,
    FIRECRAWL_SKILL_NAMES,
    FIRECRAWL_SNAPSHOT,
    FIRECRAWL_SNAPSHOT_DIGEST,
    REQUIRED_TOOL_NAMES,
    TAVILY_COMMIT,
    TAVILY_SKILL_NAMES,
    TAVILY_SNAPSHOT,
    TAVILY_SNAPSHOT_DIGEST,
    ReviewedCapabilityPromotionError,
    build_reviewed_capability_bundle,
    promote_reviewed_capabilities,
)
from app.domain.control_plane.canonical import sha256_digest
from app.domain.control_plane.contracts import (
    AgentProfileDefinition,
    AliasRef,
    DefinitionKind,
    MCPServerDefinition,
    MCPToolDefinition,
    MoveAliasRequest,
    PublishedDefinition,
    PublishRequest,
    RetireRequest,
    SkillDefinition,
)
from app.domain.control_plane.extensions import ExtensionRegistry
from app.domain.coordinator.web_capability_fixtures import (
    SEARCH_TOOL_LOGICAL_IDS,
    web_capability_definitions,
)
from app.integrations.control_plane_payloads import InMemoryPayloadStore

NOW = datetime(2026, 7, 26, 14, 0, tzinfo=UTC)
PAYLOADS = (
    Path(__file__).resolve().parents[1] / "app" / "domain" / "coordinator" / "reviewed_payloads"
)
WORKSPACE_AGENT_BROWSER = (
    Path(__file__).resolve().parents[2] / ".agents" / "skills" / "agent-browser"
)


def _skill_root(tmp_path: Path) -> Path:
    root = tmp_path / "skills"
    for name in (*FIRECRAWL_SKILL_NAMES, *TAVILY_SKILL_NAMES):
        skill = root / name
        skill.mkdir(parents=True)
        (skill / "SKILL.md").write_text(
            (
                "---\n"
                f"name: {name}\n"
                f"description: Reviewed procedure for {name}.\n"
                "allowed-tools:\n"
                "  - Bash(reviewed *)\n"
                "---\n\n"
                f"# {name}\n"
            ),
            encoding="utf-8",
        )
    shutil.copytree(WORKSPACE_AGENT_BROWSER, root / "agent-browser")
    return root


def _bundle(tmp_path: Path):
    return build_reviewed_capability_bundle(
        reviewed_payloads=PAYLOADS,
        workspace_skills=_skill_root(tmp_path),
    )


async def _seed_fixture() -> tuple[
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
    published = []
    for definition in web_capability_definitions():
        published.append(
            await service.publish(
                PublishRequest(
                    definition=definition,
                    actor_id="fixture",
                    published_at=NOW,
                    expected_head_revision=0,
                )
            )
        )
    return service, repository, tuple(published)


def test_builder_promotes_every_inspected_tool_with_exact_schema_and_provenance(
    tmp_path: Path,
) -> None:
    bundle = _bundle(tmp_path)
    servers = {
        item.logical_id: item
        for item in bundle.definitions
        if isinstance(item, MCPServerDefinition)
    }
    tools = [item for item in bundle.definitions if isinstance(item, MCPToolDefinition)]
    skills = [item for item in bundle.definitions if isinstance(item, SkillDefinition)]
    profile = next(item for item in bundle.definitions if isinstance(item, AgentProfileDefinition))

    firecrawl_snapshot = json.loads((PAYLOADS / FIRECRAWL_SNAPSHOT).read_bytes())
    tavily_snapshot = json.loads((PAYLOADS / TAVILY_SNAPSHOT).read_bytes())
    expected_rows = {
        row["name"]: row for row in (*firecrawl_snapshot["tools"], *tavily_snapshot["tools"])
    }
    assert len(firecrawl_snapshot["tools"]) == 26
    assert len(tavily_snapshot["tools"]) == 5
    assert len(tools) == 31
    assert {item.tool_name for item in tools} == set(expected_rows)
    for tool in tools:
        row = expected_rows[tool.tool_name]
        assert tool.input_schema == row["inputSchema"]
        assert tool.output_schema == row.get("outputSchema")
        assert tool.annotations == row.get("annotations", {})
        assert tool.schema_digest == sha256_digest(
            {
                "tool_name": tool.tool_name,
                "input_schema": tool.input_schema,
                "output_schema": tool.output_schema,
                "annotations": tool.annotations,
            }
        )

    assert servers["mcp.firecrawl"].schema_digest == FIRECRAWL_SNAPSHOT_DIGEST
    assert servers["mcp.tavily"].schema_digest == TAVILY_SNAPSHOT_DIGEST
    assert len(servers["mcp.firecrawl"].allowed_tools) == 26
    assert len(servers["mcp.tavily"].allowed_tools) == 5
    assert servers["mcp.firecrawl"].source_provenance.source == "mcp_registry"
    assert servers["mcp.firecrawl"].source_provenance.commit_digest == FIRECRAWL_COMMIT
    assert servers["mcp.tavily"].source_provenance.commit_digest == TAVILY_COMMIT

    assert len(skills) == 19
    browser = next(item for item in skills if item.skill_name == "agent-browser")
    assert browser.source_provenance.source == "npx_skills"
    assert browser.source_provenance.commit_digest == AGENT_BROWSER_COMMIT
    assert browser.source_provenance.license == "Apache-2.0"
    assert all(item.bundle_ref.digest == item.manifest_digest for item in skills)
    assert all("fixture" not in item.frontmatter for item in skills)

    assert {ref.logical_id for ref in profile.tool_refs} == SEARCH_TOOL_LOGICAL_IDS
    assert {ref.logical_id for ref in profile.skill_refs} == {
        "skill.firecrawl-search",
        "skill.tavily-search",
        "skill.agent-browser",
    }
    assert len(bundle.definitions) == 53
    new_tool_refs = [
        ref
        for ref in bundle.refs
        if ref.kind == DefinitionKind.MCP_TOOL
        and ref.logical_id.rsplit(":", 1)[-1] not in REQUIRED_TOOL_NAMES
    ]
    assert len(new_tool_refs) == 24
    assert {ref.revision for ref in new_tool_refs} == {1}
    assert all(ref.revision == 2 for ref in bundle.refs if ref not in new_tool_refs)


def test_builder_fails_closed_if_a_reviewed_snapshot_changes(tmp_path: Path) -> None:
    copied = tmp_path / "payloads"
    shutil.copytree(PAYLOADS, copied)
    snapshot = copied / FIRECRAWL_SNAPSHOT
    snapshot.write_bytes(snapshot.read_bytes() + b"\n")

    with pytest.raises(ReviewedCapabilityPromotionError, match="digest mismatch"):
        build_reviewed_capability_bundle(
            reviewed_payloads=copied,
            workspace_skills=_skill_root(tmp_path),
        )


async def test_promotion_publishes_all_targets_before_retiring_revision_one(
    tmp_path: Path,
) -> None:
    service, repository, records = await _seed_fixture()
    bundle = _bundle(tmp_path)

    result = await promote_reviewed_capabilities(
        service=service,
        bundle=bundle,
        catalog_records=records,
        aliases=(),
        actor_id="reviewed-publisher",
        changed_at=NOW,
        retire_superseded=True,
    )

    assert len(result.published) == 53
    assert not result.reused
    assert len(result.retired) == 29
    assert not result.retained
    for ref in bundle.refs:
        published = await repository.get(ref)
        assert published.definition == next(
            item
            for item in bundle.definitions
            if item.kind == ref.kind and item.logical_id == ref.logical_id
        )
    for ref in result.retired:
        assert (await repository.get(ref)).retired_at == NOW


async def test_promotion_is_resumable_but_rejects_a_conflicting_revision_two(
    tmp_path: Path,
) -> None:
    service, repository, records = await _seed_fixture()
    bundle = _bundle(tmp_path)
    first = await promote_reviewed_capabilities(
        service=service,
        bundle=bundle,
        catalog_records=records,
        aliases=(),
        actor_id="reviewed-publisher",
        changed_at=NOW,
        retire_superseded=False,
    )
    refreshed = records + tuple([await repository.get(ref) for ref in first.published])

    second = await promote_reviewed_capabilities(
        service=service,
        bundle=bundle,
        catalog_records=refreshed,
        aliases=(),
        actor_id="reviewed-publisher",
        changed_at=NOW,
        retire_superseded=False,
    )
    assert not second.published
    assert len(second.reused) == 53

    firecrawl = next(
        item
        for item in bundle.definitions
        if isinstance(item, MCPServerDefinition) and item.logical_id == "mcp.firecrawl"
    )
    conflicting = firecrawl.model_copy(update={"description": "Conflicting reviewed head"})
    conflicting_record = PublishedDefinition(
        ref=next(ref for ref in first.published if ref.logical_id == "mcp.firecrawl").model_copy(
            update={"digest": sha256_digest(conflicting)}
        ),
        definition=conflicting,
        published_at=NOW,
        published_by="other",
    )
    conflicted_records = tuple(
        conflicting_record
        if item.ref.kind == DefinitionKind.MCP_SERVER
        and item.ref.logical_id == "mcp.firecrawl"
        and item.ref.revision == 2
        else item
        for item in refreshed
    )
    with pytest.raises(ReviewedCapabilityPromotionError, match="conflicts"):
        await promote_reviewed_capabilities(
            service=service,
            bundle=bundle,
            catalog_records=conflicted_records,
            aliases=(),
            actor_id="reviewed-publisher",
            changed_at=NOW,
            retire_superseded=False,
        )


async def test_active_alias_retains_superseded_revision(tmp_path: Path) -> None:
    service, _repository, records = await _seed_fixture()
    bundle = _bundle(tmp_path)
    old_browser = next(item.ref for item in records if item.ref.logical_id == "skill.agent-browser")
    alias = await service.move_alias(
        MoveAliasRequest(
            alias=AliasRef(
                kind=DefinitionKind.SKILL,
                logical_id="skill.agent-browser",
                alias="default",
            ),
            target=old_browser,
            actor_id="operator",
            moved_at=NOW,
        )
    )

    result = await promote_reviewed_capabilities(
        service=service,
        bundle=bundle,
        catalog_records=records,
        aliases=(alias,),
        actor_id="reviewed-publisher",
        changed_at=NOW,
        retire_superseded=True,
    )

    assert old_browser in result.retained
    assert "active alias default" in result.retention_reasons["skill:skill.agent-browser@1"][0]


async def test_active_external_consumer_retains_its_revision_one_dependencies(
    tmp_path: Path,
) -> None:
    service, _repository, records = await _seed_fixture()
    old_profile = next(
        item.definition for item in records if isinstance(item.definition, AgentProfileDefinition)
    )
    external_profile = old_profile.model_copy(
        update={
            "logical_id": "agent-profile.external-revision-one-consumer",
            "title": "External revision-one consumer",
        }
    )
    external = await service.publish(
        PublishRequest(
            definition=external_profile,
            actor_id="external-publisher",
            published_at=NOW,
            expected_head_revision=0,
        )
    )

    result = await promote_reviewed_capabilities(
        service=service,
        bundle=_bundle(tmp_path),
        catalog_records=(*records, external),
        aliases=(),
        actor_id="reviewed-publisher",
        changed_at=NOW,
        retire_superseded=True,
    )

    old_firecrawl = next(item.ref for item in records if item.ref.logical_id == "mcp.firecrawl")
    assert old_firecrawl in result.retained
    assert any(
        "agent-profile.external-revision-one-consumer@1" in reason
        for reason in result.retention_reasons["mcp_server:mcp.firecrawl@1"]
    )


class _FailBeforeProfile:
    def __init__(self, delegate: ControlPlaneService) -> None:
        self.delegate = delegate
        self.retire_calls: list[RetireRequest] = []

    async def publish(self, request: PublishRequest) -> PublishedDefinition:
        if request.definition.logical_id == "agent-profile.web-research-browser-verification":
            raise RuntimeError("injected publication failure")
        return await self.delegate.publish(request)

    async def retire(self, request: RetireRequest) -> PublishedDefinition:
        self.retire_calls.append(request)
        return await self.delegate.retire(request)


async def test_publication_failure_never_starts_retirement(tmp_path: Path) -> None:
    service, repository, records = await _seed_fixture()
    failing = _FailBeforeProfile(service)

    with pytest.raises(RuntimeError, match="injected"):
        await promote_reviewed_capabilities(
            service=failing,
            bundle=_bundle(tmp_path),
            catalog_records=records,
            aliases=(),
            actor_id="reviewed-publisher",
            changed_at=NOW,
            retire_superseded=True,
        )

    assert not failing.retire_calls
    for item in records:
        assert (await repository.get(item.ref)).retired_at is None
