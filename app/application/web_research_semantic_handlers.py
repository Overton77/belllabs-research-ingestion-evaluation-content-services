from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Literal, Protocol

from pydantic import BaseModel, TypeAdapter

from app.application.orchestration_routing import (
    SemanticHandlerRegistry,
    SemanticRoutingError,
)
from app.application.web_research_repository import (
    WebResearchRecordRepository,
    web_research_record_ref,
)
from app.domain.control_plane.canonical import sha256_digest
from app.domain.control_plane.contracts import (
    ControlProfileDefinition,
    DefinitionKind,
    ExactDefinitionRef,
    PublishedDefinition,
    StageGraphBlueprint,
    WorkflowImplementationBindingDefinition,
    WorkflowTypeDefinition,
)
from app.domain.coordinator.web_research_runtime import (
    BrowserExecutionGrantBinding,
    BrowserVerificationEvidence,
    CitedFinding,
    CitedSynthesis,
    GovernedBrowserVerificationRequest,
    GovernedBrowserVerificationResponse,
    GovernedMCPServerBinding,
    GovernedSearchRequest,
    GovernedSearchResponse,
    NormalizedSearchResult,
    OperationExecutionBindingAuthority,
    ProviderEvidence,
    PublicGoalAdmission,
    ReviewedRuntimeArtifactBinding,
    ReviewedSkillMountBinding,
    VerifiedWebResearchResult,
    WebResearchGoal,
    WebResearchRecordEnvelope,
    WebResearchRecordKind,
    WebResearchStageInput,
)
from app.domain.orchestration.bindings import (
    RunSemanticInputBinding,
    SemanticHandlerBinding,
    SemanticInputPayload,
    StageHandlerBinding,
)
from app.domain.orchestration.contracts import (
    StageOperationRequest,
    StageOperationResult,
    WorkflowEvaluationRequest,
    WorkflowEvaluationResult,
)

WEB_RESEARCH_HANDLER_REVISION = 1
WEB_RESEARCH_EVALUATION_CONTRACT = "evaluation:web-research-browser-verification:v1"
WEB_RESEARCH_RESULT_CONTRACT = "schema:web-research-browser-verification-result:v1"

ADMIT_HANDLER = "web-research.admit-public-goal"
FIRECRAWL_HANDLER = "web-research.search-firecrawl"
TAVILY_HANDLER = "web-research.search-tavily"
SYNTHESIS_HANDLER = "web-research.synthesize-citations"
BROWSER_HANDLER = "web-research.verify-in-browser"
PROMOTION_HANDLER = "web-research.promote-verified-result"
EVALUATOR_HANDLER = "web-research.evaluate"

STAGE_INPUT_ADAPTER = TypeAdapter(WebResearchStageInput)
ADMISSION_ADAPTER = TypeAdapter(PublicGoalAdmission)
PROVIDER_EVIDENCE_ADAPTER = TypeAdapter(ProviderEvidence)
SYNTHESIS_ADAPTER = TypeAdapter(CitedSynthesis)
BROWSER_EVIDENCE_ADAPTER = TypeAdapter(BrowserVerificationEvidence)
RESULT_ADAPTER = TypeAdapter(VerifiedWebResearchResult)

_SECRET_PATTERNS = (
    re.compile(r"\bsk-(?:proj-)?[A-Za-z0-9_-]{12,}\b"),
    re.compile(r"\b(?:api[_-]?key|access[_-]?token|password)\s*[:=]\s*\S+", re.I),
    re.compile(r"\bBearer\s+[A-Za-z0-9._~+/=-]{12,}\b", re.I),
)
_PROHIBITED_GOAL_MARKERS = (
    "log in",
    "login",
    "authenticated",
    "private account",
    "submit form",
    "purchase",
    "send message",
    "upload",
    "download everything",
    "crawl entire",
)


class FirecrawlSearchPort(Protocol):
    async def search(self, request: GovernedSearchRequest) -> GovernedSearchResponse: ...


class TavilySearchPort(Protocol):
    async def search(self, request: GovernedSearchRequest) -> GovernedSearchResponse: ...


class AgentBrowserVerificationPort(Protocol):
    async def verify(
        self,
        request: GovernedBrowserVerificationRequest,
    ) -> GovernedBrowserVerificationResponse: ...


@dataclass(frozen=True)
class WebResearchHandlerDependencies:
    firecrawl: FirecrawlSearchPort
    tavily: TavilySearchPort
    browser: AgentBrowserVerificationPort
    records: WebResearchRecordRepository


@dataclass(frozen=True)
class ResolvedWebResearchRunAuthority:
    blueprint: StageGraphBlueprint
    blueprint_ref: ExactDefinitionRef
    firecrawl_tool_ref: ExactDefinitionRef
    tavily_tool_ref: ExactDefinitionRef
    browser_skill_ref: ExactDefinitionRef
    semantic_binding: RunSemanticInputBinding


class AdmitPublicGoalHandler:
    def __init__(self, records: WebResearchRecordRepository) -> None:
        self._records = records

    async def execute(
        self,
        request: StageOperationRequest,
        binding: SemanticHandlerBinding,
    ) -> StageOperationResult:
        _require_stage(request, "admit_public_goal")
        prior = await _prior(self._records, request)
        if prior is not None:
            _require_kind(prior, "admission")
            return _completed(request, binding, prior, tool_calls=0)
        value = binding.input.decode(STAGE_INPUT_ADAPTER)
        _admit_public_goal(value.goal)
        admission = PublicGoalAdmission(
            goal_digest=sha256_digest(value.goal),
        )
        record = await _append(
            self._records,
            request,
            "admission",
            admission,
        )
        return _completed(request, binding, record, tool_calls=0)


class ProviderSearchHandler:
    def __init__(
        self,
        *,
        provider: Literal["firecrawl", "tavily"],
        port: FirecrawlSearchPort | TavilySearchPort,
        records: WebResearchRecordRepository,
    ) -> None:
        self._provider = provider
        self._port = port
        self._records = records
        self._stage_id = f"search_{provider}"
        self._record_kind: WebResearchRecordKind = f"{provider}_evidence"  # type: ignore[assignment]

    async def execute(
        self,
        request: StageOperationRequest,
        binding: SemanticHandlerBinding,
    ) -> StageOperationResult:
        _require_stage(request, self._stage_id)
        prior = await _prior(self._records, request)
        if prior is not None:
            _require_kind(prior, self._record_kind)
            return _provider_result(request, binding, prior)
        value = binding.input.decode(STAGE_INPUT_ADAPTER)
        _admit_public_goal(value.goal)
        admission = await _load_single(
            self._records,
            request,
            expected_kind="admission",
        )
        admitted = _decode(admission, ADMISSION_ADAPTER)
        if admitted.goal_digest != sha256_digest(value.goal):
            raise SemanticRoutingError("provider search goal differs from the admitted public goal")
        response = await self._port.search(
            GovernedSearchRequest(
                query=value.goal.question,
                limit=value.maximum_results,
                include_domains=value.goal.include_domains,
                exclude_domains=value.goal.exclude_domains,
                idempotency_key=request.idempotency_key,
                exact_tool_ref=(
                    value.firecrawl_tool_ref
                    if self._provider == "firecrawl"
                    else value.tavily_tool_ref
                ),
                runtime_artifact=(
                    value.firecrawl_runtime
                    if self._provider == "firecrawl"
                    else value.tavily_runtime
                ),
            )
        )
        evidence = ProviderEvidence(
            provider=self._provider,
            query_digest=sha256_digest(value.goal.question),
            results=tuple(_sanitized_result(item) for item in response.results)[
                : value.maximum_results
            ],
            provider_request_id=None,
        )
        record = await _append(
            self._records,
            request,
            self._record_kind,
            evidence,
        )
        return _provider_result(request, binding, record)


class CitedSynthesisHandler:
    def __init__(self, records: WebResearchRecordRepository) -> None:
        self._records = records

    async def execute(
        self,
        request: StageOperationRequest,
        binding: SemanticHandlerBinding,
    ) -> StageOperationResult:
        _require_stage(request, "synthesize_citations")
        prior = await _prior(self._records, request)
        if prior is not None:
            _require_kind(prior, "cited_synthesis")
            return _completed(request, binding, prior, tool_calls=0)
        binding.input.decode(STAGE_INPUT_ADAPTER)
        provider_records = await _load_exact_kinds(
            self._records,
            request,
            {"firecrawl_evidence", "tavily_evidence"},
        )
        evidence = tuple(_decode(record, PROVIDER_EVIDENCE_ADAPTER) for record in provider_records)
        if {item.provider for item in evidence} != {"firecrawl", "tavily"}:
            raise SemanticRoutingError(
                "cited synthesis requires exact Firecrawl and Tavily evidence"
            )
        findings: list[CitedFinding] = []
        source_urls: list[str] = []
        for provider in evidence:
            for item in provider.results:
                if item.url not in source_urls:
                    source_urls.append(item.url)
                text = item.snippet.strip() or item.title
                findings.append(
                    CitedFinding(
                        finding=_scrub_text(text),
                        citation_urls=(item.url,),
                        provider_names=frozenset({provider.provider}),
                    )
                )
        if not findings:
            raise SemanticRoutingError("cited synthesis cannot proceed without provider results")
        synthesis = CitedSynthesis(
            findings=tuple(findings[:20]),
            source_urls=tuple(source_urls[:20]),
            provider_evidence_refs=(
                web_research_record_ref(provider_records[0]),
                web_research_record_ref(provider_records[1]),
            ),
        )
        record = await _append(
            self._records,
            request,
            "cited_synthesis",
            synthesis,
        )
        return _completed(request, binding, record, tool_calls=0)


class BrowserVerificationHandler:
    def __init__(
        self,
        *,
        browser: AgentBrowserVerificationPort,
        records: WebResearchRecordRepository,
    ) -> None:
        self._browser = browser
        self._records = records

    async def execute(
        self,
        request: StageOperationRequest,
        binding: SemanticHandlerBinding,
    ) -> StageOperationResult:
        _require_stage(request, "browser_verify")
        prior = await _prior(self._records, request)
        if prior is not None:
            _require_kind(prior, "browser_verification")
            return _browser_result(request, binding, prior)
        value = binding.input.decode(STAGE_INPUT_ADAPTER)
        synthesis_record = await _load_single(
            self._records,
            request,
            expected_kind="cited_synthesis",
        )
        synthesis = _decode(synthesis_record, SYNTHESIS_ADAPTER)
        response = await self._browser.verify(
            GovernedBrowserVerificationRequest(
                request_scope=request.request_scope,
                run_id=request.identity.run_id,
                urls=synthesis.source_urls[: value.browser_verification_limit],
                objective=value.goal.question,
                idempotency_key=request.idempotency_key,
                exact_skill_ref=value.browser_skill_ref,
                runtime_artifact=value.browser_runtime,
            )
        )
        pages = tuple(
            page.model_copy(
                update={
                    "title": _scrub_text(page.title),
                    "text_excerpt": _scrub_text(page.text_excerpt),
                }
            )
            for page in response.pages[: value.browser_verification_limit]
        )
        evidence = BrowserVerificationEvidence(
            pages=pages,
            synthesis_ref=web_research_record_ref(synthesis_record),
        )
        record = await _append(
            self._records,
            request,
            "browser_verification",
            evidence,
        )
        return _browser_result(request, binding, record)


class PromoteVerifiedResultHandler:
    def __init__(self, records: WebResearchRecordRepository) -> None:
        self._records = records

    async def execute(
        self,
        request: StageOperationRequest,
        binding: SemanticHandlerBinding,
    ) -> StageOperationResult:
        _require_stage(request, "promote_verified_result")
        prior = await _prior(self._records, request)
        if prior is not None:
            _require_kind(prior, "verified_result")
            return _completed(request, binding, prior, tool_calls=0)
        browser_record = await _load_single(
            self._records,
            request,
            expected_kind="browser_verification",
        )
        browser = _decode(browser_record, BROWSER_EVIDENCE_ADAPTER)
        synthesis_record = await self._records.get(
            request.request_scope,
            request.identity.run_id,
            browser.synthesis_ref,
        )
        _require_kind(synthesis_record, "cited_synthesis")
        synthesis = _decode(synthesis_record, SYNTHESIS_ADAPTER)
        verified_urls = tuple(page.final_url for page in browser.pages if page.verified)
        if not verified_urls:
            raise SemanticRoutingError(
                "verified result promotion requires browser-verified public evidence"
            )
        result = VerifiedWebResearchResult(
            findings=synthesis.findings,
            source_urls=synthesis.source_urls,
            provider_evidence_refs=synthesis.provider_evidence_refs,
            browser_verification_ref=web_research_record_ref(browser_record),
            verified_urls=verified_urls,
        )
        record = await _append(
            self._records,
            request,
            "verified_result",
            result,
        )
        return _completed(request, binding, record, tool_calls=0)


class WebResearchWorkflowEvaluator:
    def __init__(self, records: WebResearchRecordRepository) -> None:
        self._records = records

    async def evaluate(
        self,
        request: WorkflowEvaluationRequest,
        binding: SemanticHandlerBinding,
    ) -> WorkflowEvaluationResult:
        binding.input.decode(STAGE_INPUT_ADAPTER)
        if request.evaluation_contract_ref != WEB_RESEARCH_EVALUATION_CONTRACT:
            raise SemanticRoutingError(
                "web-research evaluator is not bound to the published evaluation contract"
            )
        final_refs = request.current_output_refs.get("promote_verified_result", ())
        if len(final_refs) != 1:
            return _workflow_failure(request, binding, "missing-verified-result")
        try:
            record = await self._records.get(
                request.request_scope,
                request.run_id,
                final_refs[0],
            )
            _require_kind(record, "verified_result")
            result = _decode(record, RESULT_ADAPTER)
        except (LookupError, ValueError):
            return _workflow_failure(request, binding, "invalid-verified-result")
        if (
            len(result.provider_evidence_refs) != 2
            or not result.findings
            or not result.verified_urls
            or not all(finding.citation_urls for finding in result.findings)
        ):
            return _workflow_failure(request, binding, "unmet-evidence-obligations")
        return WorkflowEvaluationResult(
            action="accept",
            evaluation_ref=_evaluation_ref(request, "accepted"),
            evaluation_contract_ref=request.evaluation_contract_ref,
            objective_contract_ref=request.objective_contract_ref,
            output_contract_ref=binding.output_contract_ref,
        )


def register_web_research_stagegraph_handlers(
    registry: SemanticHandlerRegistry,
    dependencies: WebResearchHandlerDependencies,
) -> None:
    registry.register_stage(
        ADMIT_HANDLER,
        WEB_RESEARCH_HANDLER_REVISION,
        AdmitPublicGoalHandler(dependencies.records),
    )
    registry.register_stage(
        FIRECRAWL_HANDLER,
        WEB_RESEARCH_HANDLER_REVISION,
        ProviderSearchHandler(
            provider="firecrawl",
            port=dependencies.firecrawl,
            records=dependencies.records,
        ),
    )
    registry.register_stage(
        TAVILY_HANDLER,
        WEB_RESEARCH_HANDLER_REVISION,
        ProviderSearchHandler(
            provider="tavily",
            port=dependencies.tavily,
            records=dependencies.records,
        ),
    )
    registry.register_stage(
        SYNTHESIS_HANDLER,
        WEB_RESEARCH_HANDLER_REVISION,
        CitedSynthesisHandler(dependencies.records),
    )
    registry.register_stage(
        BROWSER_HANDLER,
        WEB_RESEARCH_HANDLER_REVISION,
        BrowserVerificationHandler(
            browser=dependencies.browser,
            records=dependencies.records,
        ),
    )
    registry.register_stage(
        PROMOTION_HANDLER,
        WEB_RESEARCH_HANDLER_REVISION,
        PromoteVerifiedResultHandler(dependencies.records),
    )
    registry.register_workflow_evaluator(
        EVALUATOR_HANDLER,
        WEB_RESEARCH_HANDLER_REVISION,
        WebResearchWorkflowEvaluator(dependencies.records),
    )


def build_web_research_run_binding(
    *,
    request_scope: str,
    run_id: str,
    goal: WebResearchGoal,
    firecrawl_tool_ref: ExactDefinitionRef,
    tavily_tool_ref: ExactDefinitionRef,
    browser_skill_ref: ExactDefinitionRef,
    firecrawl_runtime: ReviewedRuntimeArtifactBinding,
    tavily_runtime: ReviewedRuntimeArtifactBinding,
    browser_runtime: ReviewedRuntimeArtifactBinding,
    mcp_servers: tuple[GovernedMCPServerBinding, ...],
    skills: tuple[ReviewedSkillMountBinding, ...],
    browser_grant: BrowserExecutionGrantBinding,
    operation_execution: OperationExecutionBindingAuthority,
    effective_configuration_digest: str,
    blueprint_digest: str,
    created_at: datetime,
    maximum_results: int = 5,
    browser_verification_limit: int = 3,
) -> RunSemanticInputBinding:
    """Freeze the exact published Scenario D stage and evaluator revisions."""

    if operation_execution.effective_configuration_digest != effective_configuration_digest:
        raise SemanticRoutingError(
            "Scenario D OperationExecutionBinding differs from the frozen ERC digest"
        )
    value = WebResearchStageInput(
        goal=goal,
        firecrawl_tool_ref=firecrawl_tool_ref,
        tavily_tool_ref=tavily_tool_ref,
        browser_skill_ref=browser_skill_ref,
        firecrawl_runtime=firecrawl_runtime,
        tavily_runtime=tavily_runtime,
        browser_runtime=browser_runtime,
        mcp_servers=mcp_servers,
        skills=skills,
        browser_grant=browser_grant,
        operation_execution=operation_execution,
        maximum_results=maximum_results,
        browser_verification_limit=browser_verification_limit,
    )
    handlers = (
        ("admit_public_goal", ADMIT_HANDLER, "schema:web-research-public-goal-admission:v1"),
        ("search_firecrawl", FIRECRAWL_HANDLER, "schema:web-search-provider-evidence:v1"),
        ("search_tavily", TAVILY_HANDLER, "schema:web-search-provider-evidence:v1"),
        ("synthesize_citations", SYNTHESIS_HANDLER, "schema:web-research-cited-synthesis:v1"),
        ("browser_verify", BROWSER_HANDLER, "schema:web-research-browser-evidence:v1"),
        ("promote_verified_result", PROMOTION_HANDLER, WEB_RESEARCH_RESULT_CONTRACT),
    )
    stage_bindings = tuple(
        StageHandlerBinding(
            stage_id=stage_id,
            handler=_handler_binding(
                stage_id=stage_id,
                handler_id=handler_id,
                value=value,
                output_contract_ref=output_contract_ref,
            ),
        )
        for stage_id, handler_id, output_contract_ref in handlers
    )
    evaluator = _handler_binding(
        stage_id=None,
        handler_id=EVALUATOR_HANDLER,
        value=value,
        output_contract_ref="schema:web-research-workflow-evaluation:v1",
    )
    return RunSemanticInputBinding.create(
        request_scope=request_scope,
        run_id=run_id,
        blueprint_family="StageGraph",
        effective_configuration_digest=effective_configuration_digest,
        blueprint_digest=blueprint_digest,
        stage_handlers=stage_bindings,
        workflow_evaluator=evaluator,
        created_at=created_at,
    )


def resolve_web_research_run_authority(
    *,
    catalog_records: tuple[PublishedDefinition, ...],
    request_scope: str,
    run_id: str,
    goal: WebResearchGoal,
    effective_configuration_digest: str,
    created_at: datetime,
    firecrawl_runtime: ReviewedRuntimeArtifactBinding,
    tavily_runtime: ReviewedRuntimeArtifactBinding,
    browser_runtime: ReviewedRuntimeArtifactBinding,
    selected_capability_refs: tuple[ExactDefinitionRef, ...],
    mcp_servers: tuple[GovernedMCPServerBinding, ...],
    skills: tuple[ReviewedSkillMountBinding, ...],
    browser_grant: BrowserExecutionGrantBinding,
    operation_execution: OperationExecutionBindingAuthority,
    maximum_results: int = 5,
    browser_verification_limit: int = 3,
) -> ResolvedWebResearchRunAuthority:
    """Resolve executable current heads and freeze their exact run-scoped binding."""

    heads: dict[tuple[DefinitionKind, str], PublishedDefinition] = {}
    for record in catalog_records:
        key = (record.ref.kind, record.ref.logical_id)
        current = heads.get(key)
        if current is None or record.ref.revision > current.ref.revision:
            heads[key] = record
    blueprint_record = _current_head(
        heads,
        DefinitionKind.BLUEPRINT,
        "web-research-browser-verification-v1",
    )
    control_record = _current_head(
        heads,
        DefinitionKind.CONTROL_PROFILE,
        "web-research-browser-verification-control-v1",
    )
    workflow_record = _current_head(
        heads,
        DefinitionKind.WORKFLOW_TYPE,
        "web-research-browser-verification",
    )
    implementation_record = _current_head(
        heads,
        DefinitionKind.WORKFLOW_IMPLEMENTATION,
        "web-research-browser-verification.implementation",
    )
    selected = {(ref.kind, ref.logical_id): ref for ref in selected_capability_refs}
    if len(selected) != len(selected_capability_refs):
        raise SemanticRoutingError("Scenario D selected capability refs are duplicated")
    required_selected = {
        (DefinitionKind.MCP_SERVER, "mcp.firecrawl"),
        (DefinitionKind.MCP_SERVER, "mcp.tavily"),
        (DefinitionKind.MCP_TOOL, "mcp.firecrawl:firecrawl_search"),
        (DefinitionKind.MCP_TOOL, "mcp.tavily:tavily_search"),
        (DefinitionKind.SKILL, "skill.firecrawl-search"),
        (DefinitionKind.SKILL, "skill.tavily-search"),
        (DefinitionKind.SKILL, "skill.agent-browser"),
        (
            DefinitionKind.AGENT_PROFILE,
            "agent-profile.web-research-browser-verification",
        ),
    }
    if set(selected) != required_selected:
        raise SemanticRoutingError(
            "Scenario D requires exactly the coordinator-selected server, tool, "
            "skill, and browser-profile refs"
        )
    selected_records = {key: _current_head(heads, *key) for key in required_selected}
    if any(selected_records[key].ref != ref for key, ref in selected.items()):
        raise SemanticRoutingError(
            "Scenario D selected capability is not the exact current Mongo head"
        )
    firecrawl_record = selected_records[(DefinitionKind.MCP_TOOL, "mcp.firecrawl:firecrawl_search")]
    tavily_record = selected_records[(DefinitionKind.MCP_TOOL, "mcp.tavily:tavily_search")]
    browser_record = selected_records[(DefinitionKind.SKILL, "skill.agent-browser")]
    if not isinstance(blueprint_record.definition, StageGraphBlueprint):
        raise SemanticRoutingError("Scenario D current blueprint has the wrong kind")
    if not isinstance(control_record.definition, ControlProfileDefinition):
        raise SemanticRoutingError("Scenario D current control profile has the wrong kind")
    if not isinstance(workflow_record.definition, WorkflowTypeDefinition):
        raise SemanticRoutingError("Scenario D current workflow has the wrong kind")
    if not isinstance(
        implementation_record.definition,
        WorkflowImplementationBindingDefinition,
    ):
        raise SemanticRoutingError("Scenario D current implementation has the wrong kind")
    admission = next(
        (
            stage
            for stage in blueprint_record.definition.stages
            if stage.stage_id == "admit_public_goal"
        ),
        None,
    )
    if admission is None or not any(
        slot.reservation for slot in admission.operation_slots
    ):
        raise SemanticRoutingError(
            "Scenario D current blueprint is not dispatchable; publish the execution correction"
        )
    if (
        control_record.definition.blueprint_ref != blueprint_record.ref
        or workflow_record.definition.allowed_blueprints != frozenset({blueprint_record.ref})
        or workflow_record.definition.allowed_control_profiles != frozenset({control_record.ref})
        or implementation_record.definition.workflow_type_ref != workflow_record.ref
        or implementation_record.definition.blueprint_ref != blueprint_record.ref
        or implementation_record.definition.control_profile_ref != control_record.ref
    ):
        raise SemanticRoutingError(
            "Scenario D current workflow heads contain stale exact dependencies"
        )
    semantic_binding = build_web_research_run_binding(
        request_scope=request_scope,
        run_id=run_id,
        goal=goal,
        firecrawl_tool_ref=firecrawl_record.ref,
        tavily_tool_ref=tavily_record.ref,
        browser_skill_ref=browser_record.ref,
        firecrawl_runtime=firecrawl_runtime,
        tavily_runtime=tavily_runtime,
        browser_runtime=browser_runtime,
        mcp_servers=mcp_servers,
        skills=skills,
        browser_grant=browser_grant,
        operation_execution=operation_execution,
        effective_configuration_digest=effective_configuration_digest,
        blueprint_digest=blueprint_record.ref.digest,
        created_at=created_at,
        maximum_results=maximum_results,
        browser_verification_limit=browser_verification_limit,
    )
    return ResolvedWebResearchRunAuthority(
        blueprint=blueprint_record.definition,
        blueprint_ref=blueprint_record.ref,
        firecrawl_tool_ref=firecrawl_record.ref,
        tavily_tool_ref=tavily_record.ref,
        browser_skill_ref=browser_record.ref,
        semantic_binding=semantic_binding,
    )


def _handler_binding(
    *,
    stage_id: str | None,
    handler_id: str,
    value: WebResearchStageInput,
    output_contract_ref: str,
) -> SemanticHandlerBinding:
    operation_binding = next(
        (
            binding
            for bound_stage_id, binding in value.operation_execution.bindings.items()
            if bound_stage_id == stage_id
        ),
        None,
    )
    return SemanticHandlerBinding(
        handler_id=handler_id,
        handler_revision=WEB_RESEARCH_HANDLER_REVISION,
        operation_execution_binding_ref=(
            operation_binding.binding_id if operation_binding is not None else None
        ),
        input=SemanticInputPayload.from_value(
            schema_ref="schema:web-research-stage-input:v1",
            value=value.model_dump(mode="json"),
        ),
        output_contract_ref=output_contract_ref,
    )


def _current_head(
    heads: dict[tuple[DefinitionKind, str], PublishedDefinition],
    kind: DefinitionKind,
    logical_id: str,
) -> PublishedDefinition:
    record = heads.get((kind, logical_id))
    if record is None or record.retired_at is not None:
        raise SemanticRoutingError(
            f"Scenario D selectable catalog head is unavailable: {kind.value}:{logical_id}"
        )
    return record


def _require_stage(request: StageOperationRequest, expected: str) -> None:
    if request.identity.stage_id != expected:
        raise SemanticRoutingError(
            f"web-research handler is not authorized for stage: {request.identity.stage_id}"
        )


def _admit_public_goal(goal: WebResearchGoal) -> None:
    normalized = goal.question.casefold()
    if any(marker in normalized for marker in _PROHIBITED_GOAL_MARKERS):
        raise SemanticRoutingError(
            "web-research goal exceeds public read-only search/browser authority"
        )
    if _contains_secret(goal.question):
        raise SemanticRoutingError("web-research goal appears to contain secret material")


async def _prior(
    records: WebResearchRecordRepository,
    request: StageOperationRequest,
) -> WebResearchRecordEnvelope | None:
    return await records.get_by_intent(
        request.request_scope,
        request.identity.run_id,
        request.idempotency_key,
    )


async def _append(
    records: WebResearchRecordRepository,
    request: StageOperationRequest,
    kind: WebResearchRecordKind,
    payload: BaseModel,
) -> WebResearchRecordEnvelope:
    payload_value = payload.model_dump(mode="json")
    content_digest = sha256_digest(payload_value)
    record_id = sha256_digest(
        {
            "run_id": request.identity.run_id,
            "stage_id": request.identity.stage_id,
            "intent_key": request.idempotency_key,
            "content_digest": content_digest,
        }
    ).removeprefix("sha256:")
    return await records.append(
        WebResearchRecordEnvelope(
            record_kind=kind,
            record_id=record_id,
            intent_key=request.idempotency_key,
            request_scope=request.request_scope,
            run_id=request.identity.run_id,
            payload=payload_value,
            content_digest=content_digest,
            created_at=datetime.now(UTC),
        )
    )


async def _load_single(
    records: WebResearchRecordRepository,
    request: StageOperationRequest,
    *,
    expected_kind: WebResearchRecordKind,
) -> WebResearchRecordEnvelope:
    loaded = await _load_exact_kinds(records, request, {expected_kind})
    return loaded[0]


async def _load_exact_kinds(
    records: WebResearchRecordRepository,
    request: StageOperationRequest,
    expected_kinds: set[WebResearchRecordKind],
) -> tuple[WebResearchRecordEnvelope, ...]:
    loaded = tuple(
        [
            await records.get(
                request.request_scope,
                request.identity.run_id,
                record_ref,
            )
            for record_ref in request.input_refs
        ]
    )
    observed = {record.record_kind for record in loaded}
    if observed != expected_kinds or len(loaded) != len(expected_kinds):
        raise SemanticRoutingError(
            "web-research stage inputs do not match the exact dependency evidence kinds"
        )
    return tuple(sorted(loaded, key=lambda item: item.record_kind))


def _decode[PayloadT: BaseModel](
    record: WebResearchRecordEnvelope,
    adapter: TypeAdapter[PayloadT],
) -> PayloadT:
    return adapter.validate_python(record.payload)


def _require_kind(
    record: WebResearchRecordEnvelope,
    kind: WebResearchRecordKind,
) -> None:
    if record.record_kind != kind:
        raise SemanticRoutingError(f"web-research evidence kind mismatch: expected {kind}")


def _sanitized_result(value: NormalizedSearchResult) -> NormalizedSearchResult:
    return NormalizedSearchResult(
        title=_scrub_text(value.title) or "Untitled public source",
        url=value.url,
        snippet=_scrub_text(value.snippet),
        published_at=(_scrub_text(value.published_at) if value.published_at is not None else None),
    )


def _scrub_text(value: str) -> str:
    scrubbed = value
    for pattern in _SECRET_PATTERNS:
        scrubbed = pattern.sub("[REDACTED]", scrubbed)
    return scrubbed


def _contains_secret(value: str) -> bool:
    return any(pattern.search(value) is not None for pattern in _SECRET_PATTERNS)


def _completed(
    request: StageOperationRequest,
    binding: SemanticHandlerBinding,
    record: WebResearchRecordEnvelope,
    *,
    tool_calls: int,
) -> StageOperationResult:
    return StageOperationResult(
        identity=request.identity,
        disposition="completed",
        output_refs=(web_research_record_ref(record),),
        actual_usage=({"tool.calls.total": tool_calls} if tool_calls > 0 else {}),
        output_contract_ref=binding.output_contract_ref,
    )


def _provider_result(
    request: StageOperationRequest,
    binding: SemanticHandlerBinding,
    record: WebResearchRecordEnvelope,
) -> StageOperationResult:
    evidence = _decode(record, PROVIDER_EVIDENCE_ADAPTER)
    if not evidence.results:
        return StageOperationResult(
            identity=request.identity,
            disposition="failed",
            output_refs=(web_research_record_ref(record),),
            actual_usage={"tool.calls.total": 1},
            output_contract_ref=binding.output_contract_ref,
        )
    return _completed(request, binding, record, tool_calls=1)


def _browser_result(
    request: StageOperationRequest,
    binding: SemanticHandlerBinding,
    record: WebResearchRecordEnvelope,
) -> StageOperationResult:
    evidence = _decode(record, BROWSER_EVIDENCE_ADAPTER)
    if not any(page.verified for page in evidence.pages):
        return StageOperationResult(
            identity=request.identity,
            disposition="failed",
            output_refs=(web_research_record_ref(record),),
            actual_usage={"tool.calls.total": len(evidence.pages)},
            output_contract_ref=binding.output_contract_ref,
        )
    return _completed(
        request,
        binding,
        record,
        tool_calls=len(evidence.pages),
    )


def _workflow_failure(
    request: WorkflowEvaluationRequest,
    binding: SemanticHandlerBinding,
    reason: str,
) -> WorkflowEvaluationResult:
    return WorkflowEvaluationResult(
        action="fail",
        evaluation_ref=_evaluation_ref(request, reason),
        evaluation_contract_ref=request.evaluation_contract_ref,
        objective_contract_ref=request.objective_contract_ref,
        output_contract_ref=binding.output_contract_ref,
    )


def _evaluation_ref(request: WorkflowEvaluationRequest, outcome: str) -> str:
    digest = sha256_digest(
        {
            "run_id": request.run_id,
            "workflow_cycle": request.workflow_cycle,
            "evaluation_contract_ref": request.evaluation_contract_ref,
            "outcome": outcome,
        }
    ).removeprefix("sha256:")
    return f"belllabs://web-research/{request.run_id}/evaluations/{digest}"
