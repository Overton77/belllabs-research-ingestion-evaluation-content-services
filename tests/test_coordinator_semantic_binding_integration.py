from __future__ import annotations

from dataclasses import dataclass

import pytest

from app.application.coordinator_launch import CoordinatorWorkflowLaunchService
from app.application.coordinator_semantic_bindings import (
    WorkflowSemanticBindingProviderRouter,
)
from app.application.operation_execution import InMemoryOperationBindingRepository
from app.application.orchestration_binding_repository import (
    InMemoryRunSemanticInputBindingRepository,
    RunSemanticInputBindingService,
)
from app.application.orchestration_routing import (
    BoundStageOperationExecutor,
    SemanticHandlerRegistry,
    SemanticRoutingError,
)
from app.application.schema_catalog import CATALOG_GENERATOR_VERSION
from app.application.schema_context_stage_handlers import (
    SchemaContextBindingPlanInput,
    SchemaContextSemanticBindingProvider,
)
from app.application.schema_grounding_semantic_handlers import (
    SupportingGraphBindingPlanInput,
    SupportingGraphSemanticBindingProvider,
)
from app.application.semantic_operation_bindings import (
    SemanticOperationBindingTemplates,
    SemanticOperationExecutionBindingService,
)
from app.application.web_research_semantic_binding import (
    SemanticServiceWebResearchOperationBindingAuthor,
    WebResearchOperationBindingRequest,
)
from app.domain.control_plane.canonical import sha256_digest
from app.domain.control_plane.contracts import DefinitionKind, ExactDefinitionRef
from app.domain.operation_execution.contracts import OperationAttemptIdentity
from app.domain.orchestration.contracts import (
    StageExecutionIdentity,
    StageOperationRequest,
    StageOperationResult,
)
from app.domain.run_control.contracts import AdmissionDecision, DecisionStatus
from app.domain.schema_context.contracts import SchemaContextSelectionRequest
from app.domain.schema_grounding.contracts import (
    DurableObjectRef,
    SchemaCatalogBuildRequest,
)
from tests.test_coordinator_launch_preparation import (
    NOW,
    SCOPE,
    launch_fixture,
)
from tests.test_operation_execution import operation_request
from tests.test_schema_grounding_services import _reconciliation_fixture


class AcceptingAdmission:
    def __init__(self, run_id: str) -> None:
        self.run_id = run_id

    async def admit(self, request):
        return AdmissionDecision(
            request_scope=request.request_scope,
            idempotency_issuer=request.idempotency_issuer,
            request_id=request.request_id,
            request_fingerprint=sha256_digest(request.model_dump(mode="json")),
            status=DecisionStatus.ACCEPTED,
            run_id=self.run_id,
            reason_code="accepted",
            reason="integration admission accepted",
            recorded_at=NOW,
        )


class CapturingDispatcher:
    def __init__(self) -> None:
        self.binding = None

    async def prepare_bound(
        self,
        request_scope,
        run_id,
        *,
        semantic_binding,
        binding_service,
        **_kwargs,
    ):
        self.binding = semantic_binding
        binding_ref = await binding_service.freeze(semantic_binding)
        return {
            "request_scope": request_scope,
            "run_id": run_id,
            "semantic_input_binding_ref": binding_ref,
        }


class CompletingStageHandler:
    async def execute(self, request, binding):
        return StageOperationResult(
            identity=request.identity,
            disposition="completed",
            output_refs=("evidence:oeb-authority-consumed",),
            output_contract_ref=binding.output_contract_ref,
        )


@dataclass
class CapturingSubmissions:
    payload: object | None = None

    async def submit(self, workflow_input, *, workflow_id, blueprint_family):
        self.payload = workflow_input
        return type(
            "Submission",
            (),
            {
                "workflow_id": workflow_id,
                "temporal_run_id": f"temporal:{blueprint_family.value}",
            },
        )()


def _operation_template(operation_id: str):
    template = operation_request()
    return template.model_copy(
        update={
            "identity": OperationAttemptIdentity(
                run_id="{run_id}",
                operation_id=operation_id,
                operation_attempt=1,
            ),
            "request_scope": SCOPE,
            "workspace": template.workspace.model_copy(
                update={
                    "namespace_id": "workspace-namespace:{run_id}",
                    "workspace_id": f"workspace:{{run_id}}:{operation_id}",
                }
            ),
            "budget_reservation_id": f"reservation:{{run_id}}:{operation_id}",
            "idempotency_key": f"side-effect:{{run_id}}:{operation_id}",
            "prior_binding_id": None,
        }
    )


def _durable(name: str) -> DurableObjectRef:
    content = name.encode()
    digest = sha256_digest(name)
    return DurableObjectRef(
        uri=f"memory://{name}/{digest.removeprefix('sha256:')}",
        digest=digest,
        size_bytes=len(content),
        media_type="application/octet-stream",
        version_id=digest,
    )


def _schema_context_provider(
    operation_repository: InMemoryOperationBindingRepository,
) -> SchemaContextSemanticBindingProvider:
    schema = _durable("schema")
    overlay = _durable("overlay")
    report = _durable("report")
    build = SchemaCatalogBuildRequest(
        build_id="catalog-build-launch",
        idempotency_key="catalog-build-launch",
        request_scope=SCOPE,
        schema_definition_ref=schema.uri,
        schema_definition_digest=schema.digest,
        semantic_overlay_ref=overlay.uri,
        semantic_overlay_revision="1",
        semantic_overlay_digest=overlay.digest,
        catalog_schema_version="1",
        generator_version=CATALOG_GENERATOR_VERSION,
        normalization_policy_version="graphql-sdl-normalization-v1",
        publication_target="memory://catalog",
        actor_id="coordinator-1",
        authority_ref="authority:schema",
        requested_at=NOW,
    )
    selection = SchemaContextSelectionRequest(
        request_id="selection-launch",
        purpose="pre_ingestion_graph_reconciliation",
        intended_operations=("read",),
        schema_definition_ref=schema.uri,
        schema_definition_digest=schema.digest,
        catalog_digest=sha256_digest("catalog"),
        report_ref=report.uri,
        report_digest=report.digest,
        coverage_obligations=("organization_identity",),
        workspace_ref="workspace:{run_id}",
        created_at=NOW,
    )
    templates = SemanticOperationBindingTemplates(
        operations={
            operation_id: _operation_template(operation_id)
            for operation_id in ("semantic_selector", "independent_reviewer")
        }
    )
    return SchemaContextSemanticBindingProvider(
        SchemaContextBindingPlanInput(
            build_request=build,
            selection_request=selection,
            schema_definition=schema,
            semantic_overlay=overlay,
            report=report,
            operation_bindings=templates,
            created_at=NOW,
        ),
        SemanticOperationExecutionBindingService(operation_repository),
    )


async def _supporting_graph_provider(
    operation_repository: InMemoryOperationBindingRepository,
) -> SupportingGraphSemanticBindingProvider:
    request, _records, _factory = await _reconciliation_fixture()
    workspace = request.admission.workspace_binding
    grant = request.admission.graph_capability
    admission = request.admission.model_copy(
        update={
            "request_scope": SCOPE,
            "run_id": "{run_id}",
            "workspace_binding": workspace.model_copy(
                update={"request_scope": SCOPE, "run_id": "{run_id}"}
            )
            if workspace is not None
            else None,
            "graph_capability": grant.model_copy(
                update={"request_scope": SCOPE, "run_id": "{run_id}"}
            )
            if grant is not None
            else None,
        }
    )
    request = request.model_copy(
        update={"request_scope": SCOPE, "run_id": "{run_id}", "admission": admission}
    )
    return SupportingGraphSemanticBindingProvider(
        SupportingGraphBindingPlanInput(
            request=request,
            minimum_successful_intents=1,
            handoff_instructions="Resume only from immutable graph evidence.",
            operation_bindings=SemanticOperationBindingTemplates(
                operations={"goal_iteration": _operation_template("goal_iteration")}
            ),
            created_at=NOW,
        ),
        SemanticOperationExecutionBindingService(operation_repository),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("family", "run_id", "expected_operations"),
    (
        (
            "StageGraph",
            "run-full-chain-a",
            {"semantic_selector", "independent_reviewer"},
        ),
        ("GoalDirected", "run-full-chain-c", {"goal_iteration"}),
    ),
)
async def test_full_coordinator_chain_freezes_real_oebs_before_dispatch(
    family: str,
    run_id: str,
    expected_operations: set[str],
) -> None:
    operation_repository = InMemoryOperationBindingRepository()
    provider = (
        _schema_context_provider(operation_repository)
        if family == "StageGraph"
        else await _supporting_graph_provider(operation_repository)
    )
    router = WorkflowSemanticBindingProviderRouter(
        {
            "schema-context-selection": provider,
            "supporting-graph-reconciliation": provider,
        }
    )
    preparation, tickets, proposal, context = await launch_fixture(
        family,
        initial_goal=(
            "Reconcile the bounded supporting graph." if family == "GoalDirected" else None
        ),
        semantic_bindings=router,
    )
    public = await preparation.prepare(proposal, context)
    semantic_repository = InMemoryRunSemanticInputBindingRepository()
    dispatcher = CapturingDispatcher()
    submissions = CapturingSubmissions()
    launcher = CoordinatorWorkflowLaunchService(
        tickets=tickets,
        admission=AcceptingAdmission(run_id),
        dispatcher=dispatcher,
        submissions=submissions,
        semantic_bindings=router,
        binding_service=RunSemanticInputBindingService(semantic_repository),
    )

    handle = await launcher.launch(public.ticket_id, context)

    assert handle.run_id == run_id
    assert dispatcher.binding is not None
    assert submissions.payload is not None
    if family == "StageGraph":
        refs = {
            route.handler.operation_execution_binding_ref
            for route in dispatcher.binding.stage_handlers
            if route.handler.operation_execution_binding_ref is not None
        }
    else:
        refs = {
            route.handler.operation_execution_binding_ref
            for route in dispatcher.binding.goal_operation_handlers
            if route.handler.operation_execution_binding_ref is not None
        }
    assert len(refs) == len(expected_operations)
    persisted = {
        operation_id: await operation_repository.get_binding(
            f"{run_id}:operation:{operation_id}:attempt:1"
        )
        for operation_id in expected_operations
    }
    assert all(binding is not None for binding in persisted.values())
    assert {binding.binding_id for binding in persisted.values() if binding is not None} == refs
    assert all(
        binding.effective_configuration_digest == handle.effective_configuration_digest
        for binding in persisted.values()
        if binding is not None
    )
    assert dispatcher.binding.operation_execution_binding_refs == tuple(sorted(refs))

    if family == "StageGraph":
        request = StageOperationRequest(
            identity=StageExecutionIdentity(
                run_id=run_id,
                stage_id="semantic_selector",
                workflow_cycle=0,
                stage_cycle=0,
                operation_attempt=1,
                execution_epoch=1,
            ),
            idempotency_key="operation:semantic-selector:1",
            objective="Consume the frozen selector authority.",
            input_refs=(),
            reservation_id="reservation:selector",
            reservation={"operation.attempts": 1},
            workspace_namespace=f"run/{run_id}/semantic-selector",
            request_scope=SCOPE,
            semantic_input_binding_ref=dispatcher.binding.binding_id,
            effective_configuration_digest=handle.effective_configuration_digest,
            blueprint_digest=handle.blueprint_ref.digest,
        )
        registry = SemanticHandlerRegistry()
        registry.register_stage(
            "schema-context.select",
            1,
            CompletingStageHandler(),
        )
        with pytest.raises(
            SemanticRoutingError,
            match="Operation Execution Binding repository",
        ):
            await BoundStageOperationExecutor(
                semantic_repository,
                registry,
            ).execute(request)
        result = await BoundStageOperationExecutor(
            semantic_repository,
            registry,
            operation_repository,
        ).execute(request)
        assert result.output_refs == ("evidence:oeb-authority-consumed",)


@pytest.mark.asyncio
async def test_scenario_d_operation_author_uses_shared_three_stage_freezer() -> None:
    operation_repository = InMemoryOperationBindingRepository()
    templates = SemanticOperationBindingTemplates(
        operations={
            stage_id: _operation_template(stage_id)
            for stage_id in (
                "search_firecrawl",
                "search_tavily",
                "browser_verify",
            )
        }
    )
    author = SemanticServiceWebResearchOperationBindingAuthor(
        templates,
        SemanticOperationExecutionBindingService(operation_repository),
        operation_repository,
    )
    preparation, tickets, proposal, context = await launch_fixture("StageGraph")
    public = await preparation.prepare(proposal, context)
    ticket = await tickets.get(public.ticket_id, request_scope=SCOPE)
    assert ticket is not None
    bindings = await author.author(
        WebResearchOperationBindingRequest(
            request_scope=SCOPE,
            run_id="run-scenario-d-oeb",
            effective_configuration_digest=ticket.effective_configuration_digest,
            runtime_profile_ref=ExactDefinitionRef(
                kind=DefinitionKind.RUNTIME_PROFILE,
                logical_id="web-research-browser-verification-runtime-v1",
                revision=1,
                digest=sha256_digest("runtime"),
            ),
            workspace_template_ref=ExactDefinitionRef(
                kind=DefinitionKind.WORKSPACE_TEMPLATE,
                logical_id="web-research-browser-verification-workspace-v1",
                revision=1,
                digest=sha256_digest("workspace"),
            ),
            selected_capability_refs=(),
            created_at=NOW,
        ),
        ticket=ticket,
    )
    assert set(bindings) == {
        "search_firecrawl",
        "search_tavily",
        "browser_verify",
    }
    assert all(
        binding.operation_id == stage_id
        and binding.run_id == "run-scenario-d-oeb"
        and binding.effective_configuration_digest == ticket.effective_configuration_digest
        for stage_id, binding in bindings.items()
    )
