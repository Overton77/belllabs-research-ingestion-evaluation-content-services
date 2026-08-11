from __future__ import annotations

import json
from datetime import UTC, datetime

import pytest

from app.application.web_research.external_candidate_repository import (
    InMemoryExternalCandidateRepository,
)
from app.application.web_research.external_capability_discovery import (
    ExternalCandidateRepository,
    ExternalCapabilityDiscoveryDisabled,
    ExternalCapabilityDiscoveryService,
    ExternalDiscoveryBatch,
    ExternalDiscoveryCandidate,
)
from app.integrations.mcp_registry import (
    MCPRegistryAdapter,
    MCPRegistryHttpRequest,
    MCPRegistryHttpResponse,
)
from app.integrations.npx_skills_discovery import (
    NpxSkillsDiscoveryAdapter,
    SkillDiscoveryDependencyError,
    SkillDiscoverySubprocessRequest,
    SkillDiscoverySubprocessResult,
)

NOW = datetime(2026, 7, 25, 16, 0, tzinfo=UTC)


class FakeRegistryRunner:
    def __init__(self, responses: list[dict[str, object]]) -> None:
        self.responses = list(responses)
        self.requests: list[MCPRegistryHttpRequest] = []

    async def get(self, request: MCPRegistryHttpRequest) -> MCPRegistryHttpResponse:
        self.requests.append(request)
        payload = self.responses.pop(0)
        return MCPRegistryHttpResponse(
            status_code=200,
            body=json.dumps(payload, sort_keys=True).encode(),
        )


class FakeSubprocessRunner:
    def __init__(
        self,
        result: SkillDiscoverySubprocessResult,
    ) -> None:
        self.result = result
        self.requests: list[SkillDiscoverySubprocessRequest] = []

    async def run(
        self,
        request: SkillDiscoverySubprocessRequest,
    ) -> SkillDiscoverySubprocessResult:
        self.requests.append(request)
        assert request.working_directory.exists()
        return self.result


class RecordingCandidates(ExternalCandidateRepository):
    def __init__(self) -> None:
        self.recorded: list[ExternalDiscoveryCandidate] = []

    async def record(
        self,
        batch: ExternalDiscoveryBatch,
    ) -> ExternalDiscoveryBatch:
        self.recorded.extend(batch.candidates)
        return batch


def registry_adapter(runner: FakeRegistryRunner) -> MCPRegistryAdapter:
    return MCPRegistryAdapter(
        runner,
        base_url="https://registry.modelcontextprotocol.io",
        api_version="v0.1",
        timeout_seconds=5,
        max_response_bytes=10_000,
        max_pages=3,
        max_retries=0,
        clock=lambda: NOW,
    )


def skills_adapter(runner: FakeSubprocessRunner) -> NpxSkillsDiscoveryAdapter:
    return NpxSkillsDiscoveryAdapter(
        runner,
        executable="npx",
        package_version="1.0.0",
        timeout_seconds=10,
        max_output_bytes=10_000,
        clock=lambda: NOW,
    )


@pytest.mark.asyncio
async def test_registry_adapter_paginates_and_records_untrusted_candidates() -> None:
    runner = FakeRegistryRunner(
        [
            {
                "servers": [
                    {
                        "server": {
                            "name": "io.github.firecrawl/firecrawl-mcp",
                            "version": "1.2.3",
                            "repository": {
                                "url": "https://github.com/firecrawl/firecrawl-mcp"
                            },
                        },
                        "status": "active",
                    }
                ],
                "metadata": {"next_cursor": "page-2"},
            },
            {
                "servers": [
                    {
                        "server": {
                            "name": "io.github.tavily/tavily-mcp",
                            "version": "2.0.0",
                        }
                    }
                ]
            },
        ]
    )

    batch = await registry_adapter(runner).search("web research", limit=2)

    assert [item.upstream_identity for item in batch.candidates] == [
        "io.github.firecrawl/firecrawl-mcp",
        "io.github.tavily/tavily-mcp",
    ]
    assert all(item.trust_tier == "untrusted" for item in batch.candidates)
    assert all(item.promoted_ref is None for item in batch.candidates)
    assert runner.requests[0].params["search"] == "web research"
    assert runner.requests[1].params["cursor"] == "page-2"
    assert len(batch.evidence) == 2
    assert all(item.raw_response_digest.startswith("sha256:") for item in batch.evidence)


@pytest.mark.asyncio
async def test_npx_adapter_uses_only_pinned_find_and_sanitized_environment() -> None:
    output = b"""
    Results:
    https://skills.sh/firecrawl/cli/firecrawl-search
    Install: npx skills add firecrawl/cli@firecrawl-search
    https://skills.sh/vercel-labs/agent-browser/agent-browser
    """
    runner = FakeSubprocessRunner(
        SkillDiscoverySubprocessResult(exit_code=0, stdout=output, stderr=b"")
    )

    batch = await skills_adapter(runner).search("web browser", limit=5)

    request = runner.requests[0]
    assert request.arguments == (
        "--yes",
        "skills@1.0.0",
        "find",
        "web browser",
    )
    assert "install" not in request.arguments
    assert not any("KEY" in key or "TOKEN" in key for key in request.environment)
    assert [item.upstream_identity for item in batch.candidates] == [
        "firecrawl/cli/firecrawl-search",
        "vercel-labs/agent-browser/agent-browser",
    ]
    assert batch.evidence[0].source_version == "skills@1.0.0"
    assert batch.evidence[0].raw_response_digest.startswith("sha256:")


@pytest.mark.asyncio
async def test_npx_adapter_rejects_floating_version_and_bounded_failures() -> None:
    runner = FakeSubprocessRunner(
        SkillDiscoverySubprocessResult(exit_code=1, stdout=b"", stderr=b"network failed")
    )
    with pytest.raises(ValueError, match="exact SemVer"):
        NpxSkillsDiscoveryAdapter(
            runner,
            executable="npx",
            package_version="latest",
            timeout_seconds=10,
            max_output_bytes=10_000,
        )
    with pytest.raises(SkillDiscoveryDependencyError, match="exited with code 1"):
        await skills_adapter(runner).search("search", limit=5)


@pytest.mark.asyncio
async def test_service_feature_gate_and_candidate_recording() -> None:
    registry_runner = FakeRegistryRunner(
        [{"servers": [{"server": {"name": "example/server", "version": "1.0.0"}}]}]
    )
    skill_runner = FakeSubprocessRunner(
        SkillDiscoverySubprocessResult(
            exit_code=0,
            stdout=b"https://skills.sh/example/repository/example-skill",
            stderr=b"",
        )
    )
    records = RecordingCandidates()
    disabled = ExternalCapabilityDiscoveryService(
        enabled=False,
        mcp_registry=registry_adapter(registry_runner),
        skills=skills_adapter(skill_runner),
        candidates=records,
        max_results=10,
    )
    with pytest.raises(ExternalCapabilityDiscoveryDisabled):
        await disabled.discover_mcp_servers("search")
    assert records.recorded == []

    enabled = ExternalCapabilityDiscoveryService(
        enabled=True,
        mcp_registry=registry_adapter(registry_runner),
        skills=skills_adapter(skill_runner),
        candidates=records,
        max_results=10,
    )
    batch = await enabled.discover_agent_skills("search", limit=99)
    assert len(batch.candidates) == 1
    assert records.recorded == list(batch.candidates)


@pytest.mark.asyncio
async def test_service_returns_durable_sanitized_evidence_reference() -> None:
    registry_runner = FakeRegistryRunner(
        [{"servers": [{"server": {"name": "example/server", "version": "1.0.0"}}]}]
    )
    skill_runner = FakeSubprocessRunner(
        SkillDiscoverySubprocessResult(exit_code=0, stdout=b"", stderr=b"")
    )
    records = InMemoryExternalCandidateRepository(clock=lambda: NOW)
    service = ExternalCapabilityDiscoveryService(
        enabled=True,
        mcp_registry=registry_adapter(registry_runner),
        skills=skills_adapter(skill_runner),
        candidates=records,
        max_results=10,
    )

    batch = await service.discover_mcp_servers("search")
    candidate = batch.candidates[0]
    assert candidate.raw_response_ref is not None
    assert candidate.raw_response_ref.startswith(
        "mongodb://external-discovery-evidence/"
    )
    persisted = await records.get_candidate(candidate.candidate_id)
    evidence = await records.get_evidence(persisted.evidence_id)
    assert evidence.evidence.query == "search"
    assert evidence.evidence.source_version == "v0.1"
    assert evidence.evidence.raw_response_digest == candidate.raw_response_digest


@pytest.mark.asyncio
async def test_npx_adapter_enforces_output_limit_after_injected_runner() -> None:
    runner = FakeSubprocessRunner(
        SkillDiscoverySubprocessResult(
            exit_code=0,
            stdout=b"x" * 10_001,
            stderr=b"",
        )
    )
    with pytest.raises(SkillDiscoveryDependencyError, match="output limit"):
        await skills_adapter(runner).search("search", limit=5)
