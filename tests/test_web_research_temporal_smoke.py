from __future__ import annotations

from datetime import UTC, datetime

import pytest
from temporalio.testing import WorkflowEnvironment

from app.application.orchestration_binding_repository import (
    InMemoryRunSemanticInputBindingRepository,
)
from app.application.reviewed_capability_promotion import (
    build_scenario_d_execution_correction,
)
from app.application.web_research_repository import (
    InMemoryWebResearchRecordRepository,
)
from app.application.web_research_semantic_handlers import (
    WebResearchHandlerDependencies,
    build_web_research_run_binding,
)
from app.domain.control_plane.canonical import sha256_digest
from app.domain.control_plane.contracts import (
    DefinitionKind,
    ExactDefinitionRef,
    PublishedDefinition,
)
from app.domain.coordinator.web_capability_fixtures import (
    web_capability_definitions,
)
from app.domain.coordinator.web_research_runtime import (
    BrowserPageVerification,
    GovernedBrowserVerificationRequest,
    GovernedBrowserVerificationResponse,
    GovernedSearchRequest,
    GovernedSearchResponse,
    NormalizedSearchResult,
    ReviewedRuntimeArtifactBinding,
    WebResearchGoal,
)
from app.domain.orchestration.contracts import (
    LifecycleCommandOutcome,
    LifecycleCommandRequest,
    StageGraphRunInput,
)
from app.temporal.web_research_smoke import run_web_research_stagegraph_smoke
from tests.test_web_research_semantic_handlers import (
    FakeOperationBindingReader,
    binding_authority,
)


def ref(kind: DefinitionKind, logical_id: str, digit: str) -> ExactDefinitionRef:
    return ExactDefinitionRef(
        kind=kind,
        logical_id=logical_id,
        revision=2,
        digest="sha256:" + digit * 64,
    )


FIRECRAWL_REF = ref(
    DefinitionKind.MCP_TOOL,
    "mcp.firecrawl:firecrawl_search",
    "1",
)
TAVILY_REF = ref(
    DefinitionKind.MCP_TOOL,
    "mcp.tavily:tavily_search",
    "2",
)
BROWSER_REF = ref(
    DefinitionKind.SKILL,
    "skill.agent-browser",
    "3",
)
FIRECRAWL_RUNTIME = ReviewedRuntimeArtifactBinding(
    package_name="firecrawl-mcp",
    package_version="3.22.4",
    module_locator="workspace://.tools/reviewed/firecrawl/dist/index.js",
    module_digest="sha256:69e305ec3cf14ddbfe62a7c509e218a9ec4b44c82604bffa023159130769498b",
    tools_snapshot_digest="sha256:b00747ddea6305fc08efcdd9fcaddcd69f62f0c3a59e2901d045475600c53bf2",
)
TAVILY_RUNTIME = ReviewedRuntimeArtifactBinding(
    package_name="tavily-mcp",
    package_version="0.2.21",
    module_locator="workspace://.tools/node_modules/tavily-mcp/build/index.js",
    module_digest="sha256:60d2f3d0553f4879225990fd42e43265244ef5ac6d02799f6bafa5aef2d2d05e",
    tools_snapshot_digest="sha256:65d256e03f0e82bb425b089cecf372f91f4c33b0c32fd2a94421475f2a9c922d",
)
BROWSER_RUNTIME = ReviewedRuntimeArtifactBinding(
    package_name="agent-browser",
    package_version="0.33.0",
    module_locator="workspace://.tools/node_modules/agent-browser/bin/agent-browser.js",
    module_digest="sha256:8e382f4a5ba22f45e1e0339abfe5a55ed95a19540b16a69ee3faf31c8dc8216a",
)


class Search:
    def __init__(self, name: str) -> None:
        self.name = name

    async def search(self, request: GovernedSearchRequest) -> GovernedSearchResponse:
        return GovernedSearchResponse(
            results=(
                NormalizedSearchResult(
                    title=f"{self.name} public result",
                    url=f"https://{self.name}.example/technology",
                    snippet=f"{self.name} public evidence",
                ),
            )
        )


class Browser:
    async def verify(
        self,
        request: GovernedBrowserVerificationRequest,
    ) -> GovernedBrowserVerificationResponse:
        return GovernedBrowserVerificationResponse(
            pages=tuple(
                BrowserPageVerification(
                    requested_url=url,
                    final_url=url,
                    status_code=200,
                    title="Verified public page",
                    text_excerpt="Rendered public evidence",
                    screenshot_ref=(f"belllabs://browser-evidence/{request.run_id}/{index}"),
                    verified=True,
                )
                for index, url in enumerate(request.urls)
            )
        )


class Lifecycle:
    async def execute(
        self,
        request: LifecycleCommandRequest,
    ) -> LifecycleCommandOutcome:
        terminal = request.action.get("kind") == "terminalize"
        return LifecycleCommandOutcome(
            accepted=True,
            resulting_run_version=request.expected_run_version + 1,
            phase="completed" if terminal else "active",
            reason_code="accepted",
            evidence_frontier_digest="sha256:" + "a" * 64,
            obligation_revision="obligation-revision:1",
            accepted_obligation_evidence_digest="sha256:" + "b" * 64,
            required_obligations_accepted=True,
        )


@pytest.mark.asyncio
async def test_callable_temporal_smoke_runs_exact_stagegraph_and_returns_refs() -> None:
    try:
        environment = await WorkflowEnvironment.start_time_skipping()
    except RuntimeError as error:
        pytest.skip(f"Temporal test server is unavailable: {error}")

    fixture = web_capability_definitions()
    fixture_records = tuple(
        PublishedDefinition(
            ref=ExactDefinitionRef(
                kind=item.kind,
                logical_id=item.logical_id,
                revision=1,
                digest=sha256_digest(item),
            ),
            definition=item,
            published_at=datetime(2026, 7, 25, tzinfo=UTC),
            published_by="fixture",
        )
        for item in fixture
    )
    blueprint = build_scenario_d_execution_correction(catalog_records=fixture_records).definitions[
        0
    ]
    blueprint_digest = sha256_digest(blueprint)
    binding = build_web_research_run_binding(
        request_scope="tenant:temporal-smoke",
        run_id="run-web-research-temporal-smoke",
        goal=WebResearchGoal(question="Research one public technology"),
        firecrawl_tool_ref=FIRECRAWL_REF,
        tavily_tool_ref=TAVILY_REF,
        browser_skill_ref=BROWSER_REF,
        firecrawl_runtime=FIRECRAWL_RUNTIME,
        tavily_runtime=TAVILY_RUNTIME,
        browser_runtime=BROWSER_RUNTIME,
        **binding_authority(
            FIRECRAWL_REF,
            TAVILY_REF,
            BROWSER_REF,
            configuration_digest="sha256:" + "c" * 64,
        ),
        effective_configuration_digest="sha256:" + "c" * 64,
        blueprint_digest=blueprint_digest,
        created_at=datetime(2026, 7, 26, tzinfo=UTC),
        maximum_results=2,
        browser_verification_limit=2,
    )
    run_input = StageGraphRunInput(
        run_id=binding.run_id,
        request_scope=binding.request_scope,
        effective_configuration_digest=binding.effective_configuration_digest,
        blueprint_digest=blueprint_digest,
        blueprint=blueprint.model_dump(mode="json"),
        max_concurrency=2,
        task_timeout_seconds=10,
        semantic_input_binding_ref=binding.binding_id,
        correlation_id="correlation:web-research-temporal-smoke",
    )
    dependencies = WebResearchHandlerDependencies(
        firecrawl=Search("firecrawl"),
        tavily=Search("tavily"),
        browser=Browser(),
        records=InMemoryWebResearchRecordRepository(),
    )
    bindings = InMemoryRunSemanticInputBindingRepository()

    async with environment:
        result = await run_web_research_stagegraph_smoke(
            environment.client,
            task_queue="web-research-temporal-smoke",
            workflow_id="web-research-temporal-smoke",
            run_input=run_input,
            semantic_binding=binding,
            bindings=bindings,
            lifecycle=Lifecycle(),  # type: ignore[arg-type]
            dependencies=dependencies,
            operation_bindings=FakeOperationBindingReader(
                request_scope=binding.request_scope,
                run_id=binding.run_id,
                configuration_digest=binding.effective_configuration_digest,
            ),  # type: ignore[arg-type]
        )

    assert result.temporal_run_id
    assert result.final_result_ref.startswith("belllabs://web-research/")
    assert len(result.exact_evidence_refs) == 6
    assert result.run_result.output_refs["promote_verified_result"] == (result.final_result_ref,)
