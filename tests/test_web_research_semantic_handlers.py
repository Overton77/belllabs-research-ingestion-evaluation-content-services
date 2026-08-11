from __future__ import annotations

import json
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any, cast

import pytest

from app.application.goal_directed import InMemoryGoalOperationTemplateRepository
from app.application.orchestration_binding_repository import (
    InMemoryRunSemanticInputBindingRepository,
)
from app.application.orchestration_routing import (
    BoundStageOperationExecutor,
    BoundWorkflowEvaluator,
    SemanticHandlerRegistry,
    SemanticRoutingError,
)
from app.application.web_research_repository import (
    InMemoryWebResearchRecordRepository,
)
from app.application.web_research_semantic_handlers import (
    WebResearchHandlerDependencies,
    build_web_research_run_binding,
    register_web_research_stagegraph_handlers,
)
from app.domain.control_plane.contracts import DefinitionKind, ExactDefinitionRef
from app.domain.coordinator.web_research_runtime import (
    BrowserExecutionGrantBinding,
    BrowserPageVerification,
    ExactOperationExecutionBinding,
    GovernedBrowserVerificationRequest,
    GovernedBrowserVerificationResponse,
    GovernedMCPServerBinding,
    GovernedSearchRequest,
    GovernedSearchResponse,
    NormalizedSearchResult,
    OperationExecutionBindingAuthority,
    ReviewedRuntimeArtifactBinding,
    ReviewedSkillMountBinding,
    WebResearchGoal,
)
from app.domain.orchestration.bindings import StageHandlerBinding
from app.domain.orchestration.contracts import (
    StageCandidateIdentity,
    StageExecutionIdentity,
    StageOperationRequest,
    WorkflowEvaluationRequest,
)
from app.domain.run_control.contracts import ActorContext
from app.temporal.coordinator_runtime import (
    GoalDirectedCoordinatorDependencies,
    StageGraphCoordinatorDependencies,
    create_routed_coordinator_activities,
)

CONFIGURATION_DIGEST = "sha256:" + "1" * 64
BLUEPRINT_DIGEST = "sha256:" + "2" * 64
SCOPE = "tenant:web-research-test"
RUN_ID = "web-research-run-1"
OPENAI_SENTINEL = "sk-proj-SENTINEL_OPENAI_KEY_1234567890"

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


def exact_ref(kind: DefinitionKind, logical_id: str, digit: str) -> ExactDefinitionRef:
    return ExactDefinitionRef(
        kind=kind,
        logical_id=logical_id,
        revision=2,
        digest="sha256:" + digit * 64,
    )


def binding_authority(
    firecrawl_tool_ref: ExactDefinitionRef,
    tavily_tool_ref: ExactDefinitionRef,
    browser_skill_ref: ExactDefinitionRef,
    *,
    configuration_digest: str = CONFIGURATION_DIGEST,
) -> dict[str, object]:
    firecrawl_server = exact_ref(DefinitionKind.MCP_SERVER, "mcp.firecrawl", "6")
    tavily_server = exact_ref(DefinitionKind.MCP_SERVER, "mcp.tavily", "7")
    firecrawl_skill = exact_ref(DefinitionKind.SKILL, "skill.firecrawl-search", "8")
    tavily_skill = exact_ref(DefinitionKind.SKILL, "skill.tavily-search", "9")
    profile = exact_ref(
        DefinitionKind.AGENT_PROFILE,
        "agent-profile.web-research-browser-verification",
        "a",
    )
    runtime = exact_ref(
        DefinitionKind.RUNTIME_PROFILE,
        "web-research-browser-verification-runtime-v1",
        "b",
    )
    workspace = exact_ref(
        DefinitionKind.WORKSPACE_TEMPLATE,
        "web-research-browser-verification-workspace-v1",
        "c",
    )
    return {
        "mcp_servers": (
            GovernedMCPServerBinding(
                server_ref=firecrawl_server,
                tool_ref=firecrawl_tool_ref,
                allowed_tools=frozenset({"firecrawl_search"}),
                server_schema_digest="sha256:" + "d" * 64,
            ),
            GovernedMCPServerBinding(
                server_ref=tavily_server,
                tool_ref=tavily_tool_ref,
                allowed_tools=frozenset({"tavily_search"}),
                server_schema_digest="sha256:" + "e" * 64,
            ),
        ),
        "skills": (
            ReviewedSkillMountBinding(
                skill_ref=firecrawl_skill,
                bundle_ref="belllabs://skills/firecrawl-search",
                bundle_digest="sha256:" + "1" * 64,
                manifest_digest="sha256:" + "2" * 64,
                mount_path="/skills/firecrawl-search/SKILL.md",
            ),
            ReviewedSkillMountBinding(
                skill_ref=tavily_skill,
                bundle_ref="belllabs://skills/tavily-search",
                bundle_digest="sha256:" + "3" * 64,
                manifest_digest="sha256:" + "4" * 64,
                mount_path="/skills/tavily-search/SKILL.md",
            ),
            ReviewedSkillMountBinding(
                skill_ref=browser_skill_ref,
                bundle_ref="belllabs://skills/agent-browser",
                bundle_digest="sha256:" + "5" * 64,
                manifest_digest="sha256:" + "6" * 64,
                mount_path="/skills/agent-browser/SKILL.md",
            ),
        ),
        "browser_grant": BrowserExecutionGrantBinding(
            agent_profile_ref=profile,
            runtime_profile_ref=runtime,
            workspace_template_ref=workspace,
            executable="agent-browser",
            capabilities=frozenset(
                {
                    "browser.process",
                    "browser.navigation",
                    "browser.screenshot",
                    "network.web",
                    "workspace.browser.read",
                    "workspace.browser.write",
                    "artifact.browser-evidence.write",
                }
            ),
            network_hosts=frozenset({"example.com"}),
            workspace_paths=frozenset({"/workspace/browser", "/artifacts/browser-evidence"}),
            grant_digest="sha256:" + "7" * 64,
        ),
        "operation_execution": OperationExecutionBindingAuthority(
            bindings={
                stage_id: ExactOperationExecutionBinding(
                    binding_id=f"operation-binding:web-research-test:{stage_id}",
                    binding_digest="sha256:" + digit * 64,
                )
                for stage_id, digit in (
                    ("search_firecrawl", "8"),
                    ("search_tavily", "9"),
                    ("browser_verify", "a"),
                )
            },
            effective_configuration_digest=configuration_digest,
        ),
    }


class FakeOperationBindingReader:
    def __init__(
        self,
        *,
        request_scope: str,
        run_id: str,
        configuration_digest: str,
    ) -> None:
        self._values = {
            f"operation-binding:web-research-test:{stage_id}": SimpleNamespace(
                binding_id=f"operation-binding:web-research-test:{stage_id}",
                request_scope=request_scope,
                run_id=run_id,
                effective_configuration_digest=configuration_digest,
                operation_id=stage_id,
            )
            for stage_id in (
                "search_firecrawl",
                "search_tavily",
                "browser_verify",
            )
        }

    async def get_binding_by_id(
        self,
        binding_id: str,
        *,
        request_scope: str,
    ) -> object | None:
        binding = self._values.get(binding_id)
        if binding is None or binding.request_scope != request_scope:
            return None
        return binding


class FakeSearch:
    def __init__(self, provider: str) -> None:
        self.provider = provider
        self.requests: list[GovernedSearchRequest] = []

    async def search(self, request: GovernedSearchRequest) -> GovernedSearchResponse:
        self.requests.append(request)
        return GovernedSearchResponse(
            results=(
                NormalizedSearchResult(
                    title=f"{self.provider} result",
                    url=f"https://{self.provider}.example/source?access_token=discard-me",
                    snippet=(
                        f"{self.provider} reports public evidence; "
                        f"api_key={OPENAI_SENTINEL}"
                    ),
                ),
            ),
            provider_request_id="provider-secret-request-id",
        )


class FakeBrowser:
    def __init__(self) -> None:
        self.requests: list[GovernedBrowserVerificationRequest] = []

    async def verify(
        self,
        request: GovernedBrowserVerificationRequest,
    ) -> GovernedBrowserVerificationResponse:
        self.requests.append(request)
        return GovernedBrowserVerificationResponse(
            pages=tuple(
                BrowserPageVerification(
                    requested_url=url,
                    final_url=url,
                    status_code=200,
                    title="Verified public page",
                    text_excerpt="Page says Bearer do-not-persist-this-token-value",
                    screenshot_ref=("belllabs://browser-evidence/screenshots/" + str(index)),
                    verified=True,
                )
                for index, url in enumerate(request.urls)
            )
        )


class FakeLifecycle:
    async def execute(self, request: object) -> object:
        raise AssertionError(f"not used: {request!r}")


def binding(goal: WebResearchGoal | None = None):
    firecrawl_ref = exact_ref(
        DefinitionKind.MCP_TOOL,
        "mcp.firecrawl:firecrawl_search",
        "3",
    )
    tavily_ref = exact_ref(
        DefinitionKind.MCP_TOOL,
        "mcp.tavily:tavily_search",
        "4",
    )
    browser_ref = exact_ref(
        DefinitionKind.SKILL,
        "skill.agent-browser",
        "5",
    )
    return build_web_research_run_binding(
        request_scope=SCOPE,
        run_id=RUN_ID,
        goal=goal
        or WebResearchGoal(
            question="Find public technologies used by Upgrade Labs",
        ),
        firecrawl_tool_ref=firecrawl_ref,
        tavily_tool_ref=tavily_ref,
        browser_skill_ref=browser_ref,
        firecrawl_runtime=FIRECRAWL_RUNTIME,
        tavily_runtime=TAVILY_RUNTIME,
        browser_runtime=BROWSER_RUNTIME,
        **binding_authority(firecrawl_ref, tavily_ref, browser_ref),
        effective_configuration_digest=CONFIGURATION_DIGEST,
        blueprint_digest=BLUEPRINT_DIGEST,
        created_at=datetime(2026, 7, 26, tzinfo=UTC),
        maximum_results=5,
        browser_verification_limit=2,
    )


def request(
    stage_id: str,
    *,
    input_refs: tuple[str, ...] = (),
    operation_attempt: int = 1,
) -> StageOperationRequest:
    return StageOperationRequest(
        identity=StageExecutionIdentity(
            run_id=RUN_ID,
            execution_epoch=1,
            candidate=StageCandidateIdentity(
                stage_id=stage_id,
                mapped_instance_presence=0,
                mapped_instance_id="NO_MAPPED_INSTANCE",
                workflow_cycle_ordinal=1,
                stage_cycle_ordinal=1,
                operation_slot_id="execute",
            ),
            semantic_attempt=operation_attempt,
        ),
        idempotency_key=f"{RUN_ID}:{stage_id}:cycle-1",
        objective="execute the exact governed web-research stage",
        input_refs=input_refs,
        reservation_id=f"reservation:{stage_id}",
        reservation={"tool.calls.total": 5},
        workspace_namespace="workspace:web-research",
        request_scope=SCOPE,
        semantic_input_binding_ref=binding().binding_id,
        effective_configuration_digest=CONFIGURATION_DIGEST,
        blueprint_digest=BLUEPRINT_DIGEST,
        cycle_evaluation_contract_ref=("evaluation:web-research-browser-verification:v1"),
        cycle_objective_contract_ref="objective:web-research:v1",
    )


async def runtime():
    bindings = InMemoryRunSemanticInputBindingRepository()
    records = InMemoryWebResearchRecordRepository()
    firecrawl = FakeSearch("firecrawl")
    tavily = FakeSearch("tavily")
    browser = FakeBrowser()
    registry = SemanticHandlerRegistry()
    register_web_research_stagegraph_handlers(
        registry,
        WebResearchHandlerDependencies(
            firecrawl=firecrawl,
            tavily=tavily,
            browser=browser,
            records=records,
        ),
    )
    frozen = binding()
    await bindings.create(frozen)
    operation_bindings = FakeOperationBindingReader(
        request_scope=SCOPE,
        run_id=RUN_ID,
        configuration_digest=CONFIGURATION_DIGEST,
    )
    return (
        BoundStageOperationExecutor(bindings, registry, operation_bindings),
        BoundWorkflowEvaluator(bindings, registry, operation_bindings),
        records,
        firecrawl,
        tavily,
        browser,
        frozen,
    )


@pytest.mark.asyncio
async def test_full_governed_web_research_handler_workflow() -> None:
    (
        operations,
        evaluator,
        records,
        firecrawl,
        tavily,
        browser,
        frozen,
    ) = await runtime()

    admission = await operations.execute(request("admit_public_goal"))
    firecrawl_result = await operations.execute(
        request("search_firecrawl", input_refs=admission.output_refs)
    )
    tavily_result = await operations.execute(
        request("search_tavily", input_refs=admission.output_refs)
    )
    synthesis = await operations.execute(
        request(
            "synthesize_citations",
            input_refs=firecrawl_result.output_refs + tavily_result.output_refs,
        )
    )
    browser_result = await operations.execute(
        request("browser_verify", input_refs=synthesis.output_refs)
    )
    promoted = await operations.execute(
        request("promote_verified_result", input_refs=browser_result.output_refs)
    )
    evaluated = await evaluator.evaluate(
        WorkflowEvaluationRequest(
            run_id=RUN_ID,
            workflow_cycle=1,
            objective="produce verified research",
            current_output_refs={"promote_verified_result": promoted.output_refs},
            execution_lineage=(
                admission,
                firecrawl_result,
                tavily_result,
                synthesis,
                browser_result,
                promoted,
            ),
            request_scope=SCOPE,
            semantic_input_binding_ref=frozen.binding_id,
            effective_configuration_digest=CONFIGURATION_DIGEST,
            blueprint_digest=BLUEPRINT_DIGEST,
            evaluation_contract_ref=("evaluation:web-research-browser-verification:v1"),
            objective_contract_ref="objective:web-research:v1",
        )
    )

    assert evaluated.action == "accept"
    assert promoted.output_contract_ref == ("schema:web-research-browser-verification-result:v1")
    assert len(firecrawl.requests) == 1
    assert len(tavily.requests) == 1
    assert len(browser.requests) == 1
    assert all("?" not in url for url in browser.requests[0].urls)

    final_record = await records.get(SCOPE, RUN_ID, promoted.output_refs[0])
    persisted = json.dumps(final_record.payload)
    assert "do-not-persist" not in persisted
    assert OPENAI_SENTINEL not in persisted
    assert "provider-secret-request-id" not in persisted


@pytest.mark.asyncio
async def test_provider_search_is_retry_idempotent_after_record_commit() -> None:
    operations, _evaluator, _records, firecrawl, _tavily, _browser, _frozen = await runtime()
    admission = await operations.execute(request("admit_public_goal"))
    provider_request = request(
        "search_firecrawl",
        input_refs=admission.output_refs,
    )

    first = await operations.execute(provider_request)
    second = await operations.execute(provider_request)

    assert first == second
    assert len(firecrawl.requests) == 1


@pytest.mark.asyncio
async def test_public_goal_admission_fails_closed_for_secret_or_active_authority() -> None:
    for question in (
        "Log in to a private account and inspect Upgrade Labs",
        "Research public pages using api_key=secret-material-value",
    ):
        bindings = InMemoryRunSemanticInputBindingRepository()
        records = InMemoryWebResearchRecordRepository()
        registry = SemanticHandlerRegistry()
        register_web_research_stagegraph_handlers(
            registry,
            WebResearchHandlerDependencies(
                firecrawl=FakeSearch("firecrawl"),
                tavily=FakeSearch("tavily"),
                browser=FakeBrowser(),
                records=records,
            ),
        )
        frozen = binding(WebResearchGoal(question=question))
        await bindings.create(frozen)

        with pytest.raises(SemanticRoutingError):
            await BoundStageOperationExecutor(bindings, registry).execute(
                request("admit_public_goal")
            )


def test_binding_uses_all_exact_published_handler_revisions() -> None:
    frozen = binding()

    assert {(item.stage_id, item.handler.exact_handler_ref) for item in frozen.stage_handlers} == {
        ("admit_public_goal", "web-research.admit-public-goal@1"),
        ("search_firecrawl", "web-research.search-firecrawl@1"),
        ("search_tavily", "web-research.search-tavily@1"),
        ("synthesize_citations", "web-research.synthesize-citations@1"),
        ("browser_verify", "web-research.verify-in-browser@1"),
        ("promote_verified_result", "web-research.promote-verified-result@1"),
    }
    assert frozen.workflow_evaluator is not None
    assert frozen.workflow_evaluator.exact_handler_ref == "web-research.evaluate@1"


def test_routed_activity_composition_registers_web_research_handlers() -> None:
    registry = SemanticHandlerRegistry()
    dependencies = WebResearchHandlerDependencies(
        firecrawl=FakeSearch("firecrawl"),
        tavily=FakeSearch("tavily"),
        browser=FakeBrowser(),
        records=InMemoryWebResearchRecordRepository(),
    )

    activities = create_routed_coordinator_activities(
        bindings=InMemoryRunSemanticInputBindingRepository(),
        handlers=registry,
        lifecycle=FakeLifecycle(),  # type: ignore[arg-type]
        goal_directed=GoalDirectedCoordinatorDependencies(
            run_control=cast(Any, object()),
            operation_bindings=cast(Any, object()),
            templates=InMemoryGoalOperationTemplateRepository(),
            documents=cast(Any, object()),
            actor=ActorContext(
                actor_id="web-research-test",
                permissions=frozenset({"workflow_run.goal_directed"}),
            ),
        ),
        stagegraph=StageGraphCoordinatorDependencies(
            run_control=cast(Any, object()),
            repository=cast(Any, object()),
            operation_bindings=cast(Any, object()),
            templates=cast(Any, object()),
        ),
        web_research=dependencies,
    )

    exact = next(
        item.handler for item in binding().stage_handlers if item.stage_id == "search_firecrawl"
    )
    assert registry.stage(exact).__class__.__name__ == "ProviderSearchHandler"
    assert activities.stagegraph is not None


def test_handler_ref_change_is_not_silently_routable() -> None:
    registry = SemanticHandlerRegistry()
    exact = next(
        item.handler for item in binding().stage_handlers if item.stage_id == "search_firecrawl"
    )
    changed = exact.model_copy(update={"handler_revision": 2})

    with pytest.raises(SemanticRoutingError, match="unknown semantic handler"):
        registry.stage(changed)


def test_stage_binding_is_immutable_and_schema_bound() -> None:
    frozen = binding()
    item = frozen.stage_handlers[0]

    assert isinstance(item, StageHandlerBinding)
    assert item.handler.input.schema_ref == "schema:web-research-stage-input:v1"
    assert item.handler.input.payload_digest.startswith("sha256:")
    route_refs = {
        route.stage_id: route.handler.operation_execution_binding_ref
        for route in frozen.stage_handlers
    }
    assert route_refs == {
        "admit_public_goal": None,
        "search_firecrawl": ("operation-binding:web-research-test:search_firecrawl"),
        "search_tavily": ("operation-binding:web-research-test:search_tavily"),
        "synthesize_citations": None,
        "browser_verify": ("operation-binding:web-research-test:browser_verify"),
        "promote_verified_result": None,
    }
    assert frozen.workflow_evaluator is not None
    assert frozen.workflow_evaluator.operation_execution_binding_ref is None
    assert frozen.operation_execution_binding_refs == tuple(
        sorted(ref for ref in route_refs.values() if ref is not None)
    )


def test_binding_rejects_unrelated_server_tools_missing_skills_and_browser_grants() -> None:
    firecrawl_ref = exact_ref(
        DefinitionKind.MCP_TOOL,
        "mcp.firecrawl:firecrawl_search",
        "3",
    )
    tavily_ref = exact_ref(
        DefinitionKind.MCP_TOOL,
        "mcp.tavily:tavily_search",
        "4",
    )
    browser_ref = exact_ref(
        DefinitionKind.SKILL,
        "skill.agent-browser",
        "5",
    )
    authority = binding_authority(firecrawl_ref, tavily_ref, browser_ref)
    servers = authority["mcp_servers"]
    assert isinstance(servers, tuple)
    authority["mcp_servers"] = (
        servers[0].model_copy(
            update={"allowed_tools": frozenset({"firecrawl_search", "firecrawl_scrape"})}
        ),
        servers[1],
    )
    with pytest.raises(ValueError, match="search-tool allowlist"):
        build_web_research_run_binding(
            request_scope=SCOPE,
            run_id=RUN_ID,
            goal=WebResearchGoal(question="Research public evidence"),
            firecrawl_tool_ref=firecrawl_ref,
            tavily_tool_ref=tavily_ref,
            browser_skill_ref=browser_ref,
            firecrawl_runtime=FIRECRAWL_RUNTIME,
            tavily_runtime=TAVILY_RUNTIME,
            browser_runtime=BROWSER_RUNTIME,
            effective_configuration_digest=CONFIGURATION_DIGEST,
            blueprint_digest=BLUEPRINT_DIGEST,
            created_at=datetime(2026, 7, 26, tzinfo=UTC),
            **authority,
        )

    authority = binding_authority(firecrawl_ref, tavily_ref, browser_ref)
    skills = authority["skills"]
    assert isinstance(skills, tuple)
    authority["skills"] = skills[:2]
    with pytest.raises(ValueError, match="at least 3 items"):
        build_web_research_run_binding(
            request_scope=SCOPE,
            run_id=RUN_ID,
            goal=WebResearchGoal(question="Research public evidence"),
            firecrawl_tool_ref=firecrawl_ref,
            tavily_tool_ref=tavily_ref,
            browser_skill_ref=browser_ref,
            firecrawl_runtime=FIRECRAWL_RUNTIME,
            tavily_runtime=TAVILY_RUNTIME,
            browser_runtime=BROWSER_RUNTIME,
            effective_configuration_digest=CONFIGURATION_DIGEST,
            blueprint_digest=BLUEPRINT_DIGEST,
            created_at=datetime(2026, 7, 26, tzinfo=UTC),
            **authority,
        )

    authority = binding_authority(firecrawl_ref, tavily_ref, browser_ref)
    grant = authority["browser_grant"]
    assert isinstance(grant, BrowserExecutionGrantBinding)
    authority["browser_grant"] = grant.model_copy(
        update={"capabilities": frozenset({"browser.navigation"})}
    )
    with pytest.raises(ValueError, match="execution authority"):
        build_web_research_run_binding(
            request_scope=SCOPE,
            run_id=RUN_ID,
            goal=WebResearchGoal(question="Research public evidence"),
            firecrawl_tool_ref=firecrawl_ref,
            tavily_tool_ref=tavily_ref,
            browser_skill_ref=browser_ref,
            firecrawl_runtime=FIRECRAWL_RUNTIME,
            tavily_runtime=TAVILY_RUNTIME,
            browser_runtime=BROWSER_RUNTIME,
            effective_configuration_digest=CONFIGURATION_DIGEST,
            blueprint_digest=BLUEPRINT_DIGEST,
            created_at=datetime(2026, 7, 26, tzinfo=UTC),
            **authority,
        )
