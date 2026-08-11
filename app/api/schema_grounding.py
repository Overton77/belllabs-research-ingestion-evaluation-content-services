from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Request

from app.api.control_plane import (
    ControlPlanePrincipal,
    get_control_plane_principal,
    get_control_plane_service,
)
from app.application.schema.schema_grounding_repository import (
    BeanieSchemaGroundingRecordRepository,
    SchemaGroundingRecordRepository,
)
from app.domain.schema_context.contracts import (
    AcceptedSchemaContextSelection,
    SchemaOperationProjection,
)
from app.domain.schema_grounding.contracts import (
    BoundedQueryPlan,
    CatalogResourceRecord,
    GraphAdmissionDecision,
    SchemaCatalogBuildRecord,
    SchemaCatalogBuildRequest,
    SchemaGroundingRecordEnvelope,
    SupportingGraphReconciliationRecord,
    SupportingGraphReconciliationRequest,
)
from app.domain.schema_grounding.errors import SchemaGroundingRecordNotFound

router = APIRouter(prefix="/schema-grounding/v1", tags=["schema-grounding"])


async def get_schema_grounding_repository(
    request: Request,
) -> SchemaGroundingRecordRepository:
    repository = getattr(request.app.state, "schema_grounding_repository", None)
    if repository is not None:
        return repository
    # Reuse the control-plane Mongo initialization and Beanie registry. The records remain a
    # separate application repository and collection.
    await get_control_plane_service(request)
    repository = BeanieSchemaGroundingRecordRepository()
    request.app.state.schema_grounding_repository = repository
    return repository


Repository = Annotated[
    SchemaGroundingRecordRepository,
    Depends(get_schema_grounding_repository),
]
Principal = Annotated[ControlPlanePrincipal, Depends(get_control_plane_principal)]


def _authorize_read(principal: ControlPlanePrincipal, request_scope: str) -> None:
    if request_scope not in principal.tenant_scopes:
        raise HTTPException(status_code=404, detail="schema-grounding record not found")
    if not principal.roles & {"operator", "scheduler", "auditor"}:
        raise HTTPException(status_code=403, detail="schema-grounding read permission required")


@router.get(
    "/catalog-builds/{build_id}",
    response_model=SchemaCatalogBuildRecord,
)
async def get_catalog_build(
    build_id: str,
    request_scope: str,
    principal: Principal,
    repository: Repository,
) -> SchemaCatalogBuildRecord:
    _authorize_read(principal, request_scope)
    record = await repository.get(request_scope, "catalog_build", build_id)
    return SchemaCatalogBuildRecord.model_validate(record.payload)


@router.get(
    "/catalog-builds/{build_id}/resources",
    response_model=tuple[CatalogResourceRecord, ...],
)
async def get_catalog_resources(
    build_id: str,
    request_scope: str,
    principal: Principal,
    repository: Repository,
) -> tuple[CatalogResourceRecord, ...]:
    build = await get_catalog_build(
        build_id,
        request_scope,
        principal,
        repository,
    )
    return build.resources


@router.get(
    "/selections/{selection_id}",
    response_model=AcceptedSchemaContextSelection,
)
async def get_accepted_selection(
    selection_id: str,
    request_scope: str,
    principal: Principal,
    repository: Repository,
) -> AcceptedSchemaContextSelection:
    _authorize_read(principal, request_scope)
    record = await repository.get(
        request_scope,
        "accepted_selection",
        selection_id,
    )
    return AcceptedSchemaContextSelection.model_validate(record.payload)


@router.get(
    "/projections/{projection_id}",
    response_model=SchemaOperationProjection,
)
async def get_projection(
    projection_id: str,
    request_scope: str,
    principal: Principal,
    repository: Repository,
) -> SchemaOperationProjection:
    _authorize_read(principal, request_scope)
    record = await repository.get(
        request_scope,
        "operation_projection",
        projection_id,
    )
    return SchemaOperationProjection.model_validate(record.payload)


@router.get(
    "/runs/{run_id}/binding",
    response_model=SchemaGroundingRecordEnvelope,
)
async def get_run_binding(
    run_id: str,
    request_scope: str,
    principal: Principal,
    repository: Repository,
) -> SchemaGroundingRecordEnvelope:
    _authorize_read(principal, request_scope)
    records = await repository.list_for_run(
        request_scope,
        run_id,
        record_type="workspace_binding",
    )
    return _latest(records, "workspace binding", run_id)


@router.get(
    "/runs/{run_id}/compatibility",
    response_model=GraphAdmissionDecision,
)
async def get_run_compatibility(
    run_id: str,
    request_scope: str,
    principal: Principal,
    repository: Repository,
) -> GraphAdmissionDecision:
    _authorize_read(principal, request_scope)
    records = await repository.list_for_run(
        request_scope,
        run_id,
        record_type="compatibility_decision",
    )
    record = _latest(records, "compatibility decision", run_id)
    return GraphAdmissionDecision.model_validate(record.payload)


@router.get(
    "/runs/{run_id}/reconciliation",
    response_model=SupportingGraphReconciliationRecord,
)
async def get_run_reconciliation(
    run_id: str,
    request_scope: str,
    principal: Principal,
    repository: Repository,
) -> SupportingGraphReconciliationRecord:
    _authorize_read(principal, request_scope)
    records = await repository.list_for_run(
        request_scope,
        run_id,
        record_type="reconciliation",
    )
    record = _latest(records, "reconciliation", run_id)
    return SupportingGraphReconciliationRecord.model_validate(record.payload)


@router.get(
    "/runs/{run_id}/evaluation",
    response_model=SchemaGroundingRecordEnvelope,
)
async def get_run_evaluation(
    run_id: str,
    request_scope: str,
    principal: Principal,
    repository: Repository,
) -> SchemaGroundingRecordEnvelope:
    _authorize_read(principal, request_scope)
    records = await repository.list_for_run(
        request_scope,
        run_id,
        record_type="evaluation",
    )
    return _latest(records, "evaluation", run_id)


@router.get("/schemas")
async def schema_grounding_schemas() -> dict[str, object]:
    return {
        "catalog_build_request": SchemaCatalogBuildRequest.model_json_schema(),
        "catalog_build": SchemaCatalogBuildRecord.model_json_schema(),
        "accepted_selection": AcceptedSchemaContextSelection.model_json_schema(),
        "operation_projection": SchemaOperationProjection.model_json_schema(),
        "bounded_query_plan": BoundedQueryPlan.model_json_schema(),
        "supporting_graph_reconciliation_request": (
            SupportingGraphReconciliationRequest.model_json_schema()
        ),
        "supporting_graph_reconciliation": (
            SupportingGraphReconciliationRecord.model_json_schema()
        ),
        "graph_admission_decision": GraphAdmissionDecision.model_json_schema(),
        "record_envelope": SchemaGroundingRecordEnvelope.model_json_schema(),
    }


def _latest(
    records: tuple[SchemaGroundingRecordEnvelope, ...],
    name: str,
    run_id: str,
) -> SchemaGroundingRecordEnvelope:
    if not records:
        raise SchemaGroundingRecordNotFound(f"{name} not found for run {run_id}")
    return records[-1]
