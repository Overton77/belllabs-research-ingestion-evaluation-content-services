from __future__ import annotations

import json

import pytest

from mission_control.adapters.capabilities.agentic_components.filesystem_repository import (
    FilesystemAgenticComponentRepository,
)
from mission_control.application.agentic_components.materialization import (
    MaterializationPlanner,
    MaterializationRejected,
)
from mission_control.application.agentic_components.repository import (
    InMemoryAgenticComponentRepository,
)
from mission_control.domain.agentic_components.contracts import (
    AgentHost,
    AgenticComponentRelease,
    Architecture,
    ComponentCoordinate,
    ComponentDescription,
    ComponentKind,
    ComponentQuery,
    DiffCodec,
    DiffCodecMetrics,
    DiffCodecQualification,
    HostCompatibility,
    MaterializationRequest,
    MCPRuntimeBinding,
    OperatingSystem,
    ReadinessProbe,
    SecretEnvironmentBinding,
    SourcePin,
    TrustStage,
)
from mission_control.domain.authoring.contracts import DefinitionKind, ExactDefinitionRef

DIGEST_A = "sha256:" + "a" * 64
DIGEST_B = "sha256:" + "b" * 64
DIGEST_C = "sha256:" + "c" * 64
DIGEST_D = "sha256:" + "d" * 64


def compatibility(host: AgentHost) -> HostCompatibility:
    config_paths = {
        AgentHost.CURSOR: ".cursor/mcp.json",
        AgentHost.CODEX: ".codex/config.toml",
        AgentHost.CLAUDE_CODE: ".mcp.json",
        AgentHost.AGENT_FRAMEWORK: "agent-framework.json",
    }
    return HostCompatibility(
        host=host,
        adapter_version="1",
        operating_systems=frozenset({OperatingSystem.LINUX, OperatingSystem.WINDOWS}),
        architectures=frozenset({Architecture.AMD64}),
        project_config_path=config_paths[host],
    )


def source(digest: str) -> SourcePin:
    return SourcePin(
        registry="mcp_official",
        locator="https://registry.modelcontextprotocol.io/v0.1/servers/example",
        upstream_identity="io.example/server",
        upstream_version="1.2.3",
        source_digest=digest,
        evidence_ref="evidence:registry:example",
    )


def mcp_release(
    *,
    digest: str = DIGEST_A,
    trust_stage: TrustStage = TrustStage.QUALIFIED,
) -> AgenticComponentRelease:
    return AgenticComponentRelease(
        coordinate=ComponentCoordinate(
            component_id="mcp.io.example.server",
            version="1.2.3",
            digest=digest,
        ),
        kind=ComponentKind.MCP_SERVER,
        description=ComponentDescription(
            title="Biomedical literature server",
            summary="Search reviewed biomedical literature sources.",
            capabilities=frozenset({"literature.search", "paper.retrieve"}),
            tags=frozenset({"biomedical", "research"}),
            biotech_domains=frozenset({"longevity"}),
        ),
        definition_ref=ExactDefinitionRef(
            kind=DefinitionKind.MCP_SERVER,
            logical_id="mcp.io.example.server",
            revision=3,
            digest=DIGEST_D,
        ),
        source=source(digest),
        compatibility=(
            compatibility(AgentHost.CURSOR),
            compatibility(AgentHost.CODEX),
            compatibility(AgentHost.CLAUDE_CODE),
        ),
        trust_stage=trust_stage,
        mcp=MCPRuntimeBinding(
            server_name="biomedical",
            transport="stdio",
            command="npx",
            arguments=("--yes", "@example/biomedical-mcp@1.2.3"),
            secret_environment=(
                SecretEnvironmentBinding(
                    environment_name="LITERATURE_API_KEY",
                    secret_ref="vault:biotech/literature-api-key",
                ),
            ),
            allowed_tools=frozenset({"search_papers", "get_paper"}),
            schema_digest=DIGEST_B,
            readiness_probe=ReadinessProbe(
                kind="mcp_initialize_and_list_tools",
                timeout_seconds=20,
                expected_schema_digest=DIGEST_B,
            ),
        ),
    )


def diff_release(
    *,
    digest: str,
    qualification_id: str,
    codec: DiffCodec,
    exact_apply_rate: float,
) -> AgenticComponentRelease:
    return AgenticComponentRelease(
        coordinate=ComponentCoordinate(
            component_id=f"diff.{codec.value}",
            version="1",
            digest=digest,
        ),
        kind=ComponentKind.DIFF_CODEC,
        description=ComponentDescription(
            title=f"{codec.value} qualification",
            summary="Model-qualified file mutation codec.",
        ),
        source=SourcePin(
            registry="belllabs",
            locator="urn:belllabs:diff-evaluation",
            upstream_identity="belllabs/diff-evaluation",
            upstream_version="1",
            source_digest=digest,
            evidence_ref="evaluation:diff:1",
        ),
        compatibility=(compatibility(AgentHost.AGENT_FRAMEWORK),),
        trust_stage=TrustStage.QUALIFIED,
        diff_qualification=DiffCodecQualification(
            qualification_id=qualification_id,
            model_pattern="gpt-5.*",
            codec=codec,
            engine_version="patch-engine/1",
            evaluation_digest=DIGEST_D,
            metrics=DiffCodecMetrics(
                syntax_validity=0.99,
                exact_apply_rate=exact_apply_rate,
                stale_context_recovery=0.90,
                unintended_change_rate=0.001,
                median_latency_ms=250,
                median_output_tokens=300,
            ),
            minimum_exact_apply_rate=0.95,
            maximum_unintended_change_rate=0.005,
            promoted=True,
        ),
    )


@pytest.mark.asyncio
async def test_query_returns_only_compatible_qualified_components() -> None:
    qualified = mcp_release()
    reviewed = mcp_release(digest=DIGEST_C, trust_stage=TrustStage.REVIEWED)
    repository = InMemoryAgenticComponentRepository((reviewed, qualified))

    results = await repository.search(
        ComponentQuery(
            text="biomedical literature",
            kinds=frozenset({ComponentKind.MCP_SERVER}),
            host=AgentHost.CODEX,
            operating_system=OperatingSystem.LINUX,
            architecture=Architecture.AMD64,
            required_capabilities=frozenset({"literature.search"}),
            minimum_trust_stage=TrustStage.QUALIFIED,
        )
    )

    assert results == (qualified,)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("host", "path"),
    [
        (AgentHost.CURSOR, ".cursor/mcp.json"),
        (AgentHost.CODEX, ".codex/config.toml"),
        (AgentHost.CLAUDE_CODE, ".mcp.json"),
    ],
)
async def test_materialization_renders_target_specific_project_config(
    host: AgentHost,
    path: str,
) -> None:
    release = mcp_release()
    repository = InMemoryAgenticComponentRepository((release,))

    plan = await MaterializationPlanner(repository).plan(
        MaterializationRequest(
            request_id=f"materialize:{host.value}",
            component_digests=(release.coordinate.digest,),
            host=host,
            operating_system=OperatingSystem.LINUX,
            architecture=Architecture.AMD64,
            workspace_root="/workspace",
        )
    )

    assert plan.generated_files[0].path == path
    assert "vault:biotech/literature-api-key" not in plan.generated_files[0].content
    assert any(step.kind == "inject_secrets" for step in plan.steps)
    assert any(step.kind == "probe_readiness" for step in plan.steps)
    if host == AgentHost.CODEX:
        assert 'env_vars = ["LITERATURE_API_KEY"]' in plan.generated_files[0].content
        assert '[mcp_servers."biomedical"]' in plan.generated_files[0].content
    else:
        payload = json.loads(plan.generated_files[0].content)
        assert payload["mcpServers"]["biomedical"]["command"] == "npx"


@pytest.mark.asyncio
async def test_reviewed_component_cannot_be_materialized() -> None:
    release = mcp_release(trust_stage=TrustStage.REVIEWED)
    planner = MaterializationPlanner(InMemoryAgenticComponentRepository((release,)))

    with pytest.raises(MaterializationRejected, match="not qualified"):
        await planner.plan(
            MaterializationRequest(
                request_id="materialize:reviewed",
                component_digests=(release.coordinate.digest,),
                host=AgentHost.CODEX,
                operating_system=OperatingSystem.LINUX,
                architecture=Architecture.AMD64,
                workspace_root="/workspace",
            )
        )


@pytest.mark.asyncio
async def test_diff_codec_is_selected_by_model_evidence_not_provider_name() -> None:
    weaker = diff_release(
        digest=DIGEST_B,
        qualification_id="diff-qualification:gpt-structured",
        codec=DiffCodec.OPERATIONS_STRUCTURED,
        exact_apply_rate=0.96,
    )
    stronger = diff_release(
        digest=DIGEST_C,
        qualification_id="diff-qualification:gpt-v4a",
        codec=DiffCodec.V4A_STRUCTURED,
        exact_apply_rate=0.99,
    )
    component = mcp_release()
    repository = InMemoryAgenticComponentRepository((weaker, stronger, component))

    plan = await MaterializationPlanner(repository).plan(
        MaterializationRequest(
            request_id="materialize:model-qualified-diff",
            component_digests=(component.coordinate.digest,),
            host=AgentHost.CODEX,
            operating_system=OperatingSystem.LINUX,
            architecture=Architecture.AMD64,
            workspace_root="/workspace",
            model_id="gpt-5.6-sol",
        )
    )

    assert plan.selected_diff_qualification is not None
    assert plan.selected_diff_qualification.codec == DiffCodec.V4A_STRUCTURED


@pytest.mark.asyncio
async def test_filesystem_repository_round_trips_immutable_release(tmp_path) -> None:
    release = mcp_release()
    repository = FilesystemAgenticComponentRepository(tmp_path / "catalog")

    first_path = await repository.put(release)
    second_path = await repository.put(release)
    retrieved = await repository.get_by_digest(release.coordinate.digest)

    assert first_path == second_path
    assert retrieved == release
    assert first_path.name == f"{'a' * 64}.json"
