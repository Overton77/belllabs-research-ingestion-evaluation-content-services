from __future__ import annotations

import base64
import json
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path, PurePosixPath
from tempfile import TemporaryDirectory
from typing import Any, Literal, cast
from urllib.parse import quote, unquote

from pydantic import BaseModel, ConfigDict, Field, TypeAdapter

from app.application.orchestration.orchestration_routing import (
    SemanticHandlerRegistry,
    SemanticRoutingError,
)
from app.application.schema.schema_catalog import SchemaCatalog
from app.application.schema.schema_catalog_build import SchemaCatalogBuildService
from app.application.schema.schema_context_selection import ReviewAgentPort, SelectionAgentPort
from app.application.schema.schema_grounding_repository import (
    SchemaGroundingRecordRepository,
    schema_grounding_record,
)
from app.application.operations.semantic_operation_bindings import (
    SemanticOperationBindingTemplates,
    SemanticOperationExecutionBindingService,
)
from app.domain.control_plane.canonical import sha256_digest as canonical_digest
from app.domain.control_plane.contracts import (
    EffectiveRunConfiguration,
    StageGraphBlueprint,
)
from app.domain.coordinator.launch import (
    BlueprintFamily,
    PreparedLaunchTicket,
    SemanticBindingPlan,
    WorkflowLaunchProposal,
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
from app.domain.schema_context.canonicalization import (
    sha256_digest as content_digest,
)
from app.domain.schema_context.canonicalization import (
    write_json,
)
from app.domain.schema_context.contracts import (
    AcceptedSchemaContextSelection,
    SchemaContextSelection,
    SchemaContextSelectionRequest,
    SchemaSelectionReview,
    SelectionValidationDiagnostic,
)
from app.domain.schema_context.errors import SchemaSelectionValidationError
from app.domain.schema_context.validation import accept_selection, validate_selection
from app.domain.schema_grounding.contracts import (
    DurableObjectRef,
    SchemaCatalogBuildRecord,
    SchemaCatalogBuildRequest,
    SchemaGroundingRecordEnvelope,
    SchemaGroundingRecordType,
)
from app.domain.schema_grounding.errors import SchemaGroundingRecordNotFound
from app.integrations.control_plane_payloads import (
    ContentAddress,
    ContentAddressedPayloadStore,
)

MATERIALIZE_SELECTION_CONTEXT_HANDLER = "schema-context.materialize"
SEMANTIC_SELECTOR_HANDLER = "schema-context.select"
STRUCTURAL_VALIDATION_HANDLER = "schema-context.validate"
INDEPENDENT_REVIEWER_HANDLER = "schema-context.review"
ACCEPT_SELECTION_HANDLER = "schema-context.accept"
SELECTION_WORKFLOW_EVALUATOR = "schema-context.evaluate"
SCHEMA_CONTEXT_HANDLER_REVISION = 1


class SelectionMaterializationInput(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    build_request: SchemaCatalogBuildRequest
    schema_definition: DurableObjectRef
    semantic_overlay: DurableObjectRef
    report_seed: DurableObjectRef | None = None


class SelectionOperationInput(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    request: SchemaContextSelectionRequest
    catalog_build_id: str = Field(min_length=1)
    report: DurableObjectRef


class SelectionAcceptanceInput(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    required_reviewer_role: str = "independent_schema_reviewer"


class SelectionWorkflowEvaluationInput(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    maximum_revisions: int = Field(default=2, ge=1, le=2)


class SchemaContextBindingPlanInput(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    build_request: SchemaCatalogBuildRequest
    selection_request: SchemaContextSelectionRequest
    schema_definition: DurableObjectRef
    semantic_overlay: DurableObjectRef
    report: DurableObjectRef
    report_seed: DurableObjectRef | None = None
    operation_bindings: SemanticOperationBindingTemplates
    created_at: datetime


MATERIALIZATION_ADAPTER = TypeAdapter(SelectionMaterializationInput)
SELECTION_OPERATION_ADAPTER = TypeAdapter(SelectionOperationInput)
ACCEPTANCE_ADAPTER = TypeAdapter(SelectionAcceptanceInput)
WORKFLOW_EVALUATION_ADAPTER = TypeAdapter(SelectionWorkflowEvaluationInput)


class MaterializeSelectionContextHandler:
    """Idempotently publish and authenticate the exact schema-selection catalog."""

    def __init__(
        self,
        catalog_builds: SchemaCatalogBuildService,
        sources: ContentAddressedPayloadStore,
        records: SchemaGroundingRecordRepository,
    ) -> None:
        self._catalog_builds = catalog_builds
        self._sources = sources
        self._records = records

    async def execute(
        self,
        request: StageOperationRequest,
        binding: SemanticHandlerBinding,
    ) -> StageOperationResult:
        value = binding.input.decode(MATERIALIZATION_ADAPTER)
        build_request = value.build_request
        if build_request.request_scope != request.request_scope:
            raise SemanticRoutingError(
                "schema catalog build is outside the active StageGraph request scope"
            )
        _require_address_matches(
            value.schema_definition,
            expected_ref=build_request.schema_definition_ref,
            expected_digest=build_request.schema_definition_digest,
        )
        _require_address_matches(
            value.semantic_overlay,
            expected_ref=build_request.semantic_overlay_ref,
            expected_digest=build_request.semantic_overlay_digest,
        )
        if (value.report_seed is None) != (build_request.candidate_seed_ref is None):
            raise SemanticRoutingError("schema catalog candidate seed binding is incomplete")
        if value.report_seed is not None:
            _require_address_matches(
                value.report_seed,
                expected_ref=build_request.candidate_seed_ref or "",
                expected_digest=build_request.candidate_seed_digest or "",
            )
        schema_definition = await _retrieve(self._sources, value.schema_definition)
        semantic_overlay = await _retrieve(self._sources, value.semantic_overlay)
        report_seed = (
            await _retrieve(self._sources, value.report_seed)
            if value.report_seed is not None
            else b""
        )
        record = await self._catalog_builds.build(
            build_request,
            schema_definition=schema_definition,
            semantic_overlay=semantic_overlay,
            report_seed=report_seed,
        )
        if record.status != "published":
            raise SemanticRoutingError("schema catalog materialization was rejected")
        envelope = await self._records.get(
            request.request_scope,
            "catalog_build",
            record.build_id,
        )
        return _completed(request, binding, schema_grounding_record_ref(envelope))


class SemanticSelectorStageHandler:
    """Run the governed selector against an authenticated catalog workspace."""

    def __init__(
        self,
        selector: SelectionAgentPort,
        records: SchemaGroundingRecordRepository,
        catalog_payloads: ContentAddressedPayloadStore,
        sources: ContentAddressedPayloadStore,
    ) -> None:
        self._selector = selector
        self._records = records
        self._catalog_payloads = catalog_payloads
        self._sources = sources

    async def execute(
        self,
        request: StageOperationRequest,
        binding: SemanticHandlerBinding,
    ) -> StageOperationResult:
        value = binding.input.decode(SELECTION_OPERATION_ADAPTER)
        _require_selection_request(value.request, request)
        build, build_envelope = await _catalog_record(
            self._records,
            request.request_scope,
            value.catalog_build_id,
        )
        _require_input_ref(request, schema_grounding_record_ref(build_envelope))
        _require_catalog_lineage(value.request, build)
        revision = request.identity.workflow_cycle + 1
        prior = await _draft_for_revision(
            self._records,
            request.request_scope,
            request.identity.run_id,
            revision,
        )
        if prior is not None:
            return _completed(request, binding, schema_grounding_record_ref(prior))
        feedback = await _revision_feedback(
            self._records,
            request.request_scope,
            request.identity.run_id,
        )
        parent = (
            await _draft_for_revision(
                self._records,
                request.request_scope,
                request.identity.run_id,
                revision - 1,
            )
            if revision > 1
            else None
        )
        report = await _retrieve(self._sources, value.report)
        if content_digest(report) != value.request.report_digest:
            raise SemanticRoutingError("selection report digest differs from frozen request")
        bundle = await _catalog_bundle(self._catalog_payloads, build)
        with _selection_workspace(
            bundle,
            selection_request=value.request,
            report=report,
        ) as run_root:
            selected = await self._selector.select(
                run_root,
                revision_feedback=feedback,
            )
        draft = (
            selected.output
            if isinstance(selected.output, SchemaContextSelection)
            else SchemaContextSelection.model_validate(selected.output)
        )
        _require_draft_lineage(
            draft,
            value.request,
            revision,
            parent_selection_id=parent.record_id if parent is not None else None,
        )
        envelope = schema_grounding_record(
            record_type="selection_draft",
            record_id=draft.selection_id,
            request_scope=request.request_scope,
            run_id=request.identity.run_id,
            payload=draft.model_dump(mode="json"),
            created_at=draft.created_at,
        )
        persisted = await self._records.append(envelope)
        return _completed(request, binding, schema_grounding_record_ref(persisted))


class StructuralValidationStageHandler:
    """Rehydrate the exact draft and apply the trusted structural validator."""

    def __init__(
        self,
        records: SchemaGroundingRecordRepository,
        catalog_payloads: ContentAddressedPayloadStore,
    ) -> None:
        self._records = records
        self._catalog_payloads = catalog_payloads

    async def execute(
        self,
        request: StageOperationRequest,
        binding: SemanticHandlerBinding,
    ) -> StageOperationResult:
        value = binding.input.decode(SELECTION_OPERATION_ADAPTER)
        _require_selection_request(value.request, request)
        draft_envelope = await _single_input_record(
            self._records,
            request,
            "selection_draft",
        )
        draft = SchemaContextSelection.model_validate(draft_envelope.payload)
        prior_id = f"{draft.selection_id}:validation"
        prior = await _optional_record(
            self._records,
            request.request_scope,
            "selection_validation",
            prior_id,
        )
        if prior is not None:
            return _completed(request, binding, schema_grounding_record_ref(prior))
        build, _ = await _catalog_record(
            self._records,
            request.request_scope,
            value.catalog_build_id,
        )
        _require_catalog_lineage(value.request, build)
        catalog = _catalog_from_bundle(await _catalog_bundle(self._catalog_payloads, build))
        validation = validate_selection(value.request, draft, catalog)
        envelope = schema_grounding_record(
            record_type="selection_validation",
            record_id=prior_id,
            request_scope=request.request_scope,
            run_id=request.identity.run_id,
            payload=validation.model_dump(mode="json"),
            created_at=draft.created_at,
        )
        persisted = await self._records.append(envelope)
        return _completed(request, binding, schema_grounding_record_ref(persisted))


class IndependentReviewerStageHandler:
    """Run the separately bound reviewer against immutable draft and validation facts."""

    def __init__(
        self,
        reviewer: ReviewAgentPort,
        records: SchemaGroundingRecordRepository,
        catalog_payloads: ContentAddressedPayloadStore,
        sources: ContentAddressedPayloadStore,
    ) -> None:
        self._reviewer = reviewer
        self._records = records
        self._catalog_payloads = catalog_payloads
        self._sources = sources

    async def execute(
        self,
        request: StageOperationRequest,
        binding: SemanticHandlerBinding,
    ) -> StageOperationResult:
        value = binding.input.decode(SELECTION_OPERATION_ADAPTER)
        _require_selection_request(value.request, request)
        validation_envelope = await _single_input_record(
            self._records,
            request,
            "selection_validation",
        )
        selection_id = validation_envelope.record_id.removesuffix(":validation")
        draft_envelope = await self._records.get(
            request.request_scope,
            "selection_draft",
            selection_id,
        )
        _require_same_run(draft_envelope, request)
        draft = SchemaContextSelection.model_validate(draft_envelope.payload)
        validation = SelectionValidationDiagnostic.model_validate(validation_envelope.payload)
        prior = await _review_for_selection(
            self._records,
            request.request_scope,
            request.identity.run_id,
            selection_id,
        )
        if prior is not None:
            return _completed(request, binding, schema_grounding_record_ref(prior))
        build, _ = await _catalog_record(
            self._records,
            request.request_scope,
            value.catalog_build_id,
        )
        _require_catalog_lineage(value.request, build)
        report = await _retrieve(self._sources, value.report)
        if content_digest(report) != value.request.report_digest:
            raise SemanticRoutingError("review report digest differs from frozen request")
        bundle = await _catalog_bundle(self._catalog_payloads, build)
        retry_reason: str | None = None
        review: SchemaSelectionReview | None = None
        for attempt in (1, 2):
            with _selection_workspace(
                bundle,
                selection_request=value.request,
                report=report,
                draft=draft,
                validation=validation,
            ) as run_root:
                reviewed = await self._reviewer.review(
                    run_root,
                    retry_reason=retry_reason,
                )
            review = (
                reviewed.output
                if isinstance(reviewed.output, SchemaSelectionReview)
                else SchemaSelectionReview.model_validate(reviewed.output)
            )
            if review.selection_id == selection_id:
                break
            retry_reason = (
                "selection_id binding mismatch; expected "
                f"`{selection_id}` but received `{review.selection_id}` "
                f"on attempt {attempt}."
            )
        assert review is not None
        if review.selection_id != selection_id:
            raise SemanticRoutingError(
                "independent reviewer did not bind its result to the exact selection"
            )
        envelope = schema_grounding_record(
            record_type="selection_review",
            record_id=review.review_id,
            request_scope=request.request_scope,
            run_id=request.identity.run_id,
            payload=review.model_dump(mode="json"),
            created_at=review.created_at,
        )
        persisted = await self._records.append(envelope)
        return _completed(request, binding, schema_grounding_record_ref(persisted))


class AcceptSelectionStageHandler:
    """Accept only a structurally valid draft approved by the independent reviewer."""

    def __init__(self, records: SchemaGroundingRecordRepository) -> None:
        self._records = records

    async def execute(
        self,
        request: StageOperationRequest,
        binding: SemanticHandlerBinding,
    ) -> StageOperationResult:
        value = binding.input.decode(ACCEPTANCE_ADAPTER)
        review_envelope = await _single_input_record(
            self._records,
            request,
            "selection_review",
        )
        review = SchemaSelectionReview.model_validate(review_envelope.payload)
        if review.reviewer_role != value.required_reviewer_role:
            raise SemanticRoutingError(
                "selection review was not issued by the frozen independent reviewer role"
            )
        draft_envelope = await self._records.get(
            request.request_scope,
            "selection_draft",
            review.selection_id,
        )
        validation_envelope = await self._records.get(
            request.request_scope,
            "selection_validation",
            f"{review.selection_id}:validation",
        )
        _require_same_run(draft_envelope, request)
        _require_same_run(validation_envelope, request)
        draft = SchemaContextSelection.model_validate(draft_envelope.payload)
        validation = SelectionValidationDiagnostic.model_validate(validation_envelope.payload)
        prior = await _optional_record(
            self._records,
            request.request_scope,
            "accepted_selection",
            draft.selection_id,
        )
        if prior is not None:
            _require_same_run(prior, request)
            return _completed(request, binding, schema_grounding_record_ref(prior))
        try:
            accepted = accept_selection(
                draft,
                validation,
                review,
                accepted_at=review.created_at,
            )
        except SchemaSelectionValidationError:
            # The workflow evaluator owns the bounded revision/failure decision.
            return _completed(request, binding, schema_grounding_record_ref(review_envelope))
        envelope = schema_grounding_record(
            record_type="accepted_selection",
            record_id=draft.selection_id,
            request_scope=request.request_scope,
            run_id=request.identity.run_id,
            payload=accepted.model_dump(mode="json"),
            created_at=accepted.accepted_at,
        )
        persisted = await self._records.append(envelope)
        return _completed(request, binding, schema_grounding_record_ref(persisted))


class SchemaContextWorkflowEvaluator:
    """Accept exact accepted evidence, otherwise authorize one bounded revision."""

    def __init__(self, records: SchemaGroundingRecordRepository) -> None:
        self._records = records

    async def evaluate(
        self,
        request: WorkflowEvaluationRequest,
        binding: SemanticHandlerBinding,
    ) -> WorkflowEvaluationResult:
        value = binding.input.decode(WORKFLOW_EVALUATION_ADAPTER)
        accepted_refs = request.current_output_refs.get("accept_selection", ())
        for output_ref in accepted_refs:
            parsed = parse_schema_grounding_record_ref(output_ref)
            if parsed is None or parsed[0] != "accepted_selection":
                continue
            envelope = await self._records.get(
                request.request_scope,
                "accepted_selection",
                parsed[1],
            )
            _require_ref_matches(output_ref, envelope)
            if envelope.run_id != request.run_id:
                raise SemanticRoutingError(
                    "accepted selection evidence belongs to a different Workflow Run"
                )
            AcceptedSchemaContextSelection.model_validate(envelope.payload)
            return _workflow_result(request, binding, action="accept")
        if request.workflow_cycle + 1 < value.maximum_revisions:
            return _workflow_result(
                request,
                binding,
                action="cycle",
                invalidation_frontier=("semantic_selector",),
                next_objective=(
                    "Revise the semantic selection using the latest deterministic "
                    "validation and independent-review findings."
                ),
            )
        return _workflow_result(request, binding, action="fail")


class SchemaContextSemanticBindingProvider:
    """Freeze and later author Scenario A's exact run-scoped semantic binding."""

    def __init__(
        self,
        inputs: SchemaContextBindingPlanInput,
        operation_bindings: SemanticOperationExecutionBindingService,
    ) -> None:
        self._inputs = inputs
        self._operation_bindings = operation_bindings

    async def prepare(
        self,
        proposal: WorkflowLaunchProposal,
        configuration: EffectiveRunConfiguration,
    ) -> SemanticBindingPlan:
        blueprint = configuration.selected_blueprint
        if (
            configuration.workflow_type.logical_id != "schema-context-selection"
            or not isinstance(blueprint, StageGraphBlueprint)
            or blueprint.logical_id != "schema-context-selection-v1"
        ):
            raise SemanticRoutingError(
                "schema-context binding provider requires the exact published "
                "schema-context-selection StageGraph"
            )
        if (
            proposal.request_scope != self._inputs.build_request.request_scope
            or proposal.request_scope != proposal.compilation.context.authority_scope
        ):
            raise SemanticRoutingError(
                "schema-context binding inputs belong to a different request scope"
            )
        exact_refs = (
            self._inputs.schema_definition.uri,
            self._inputs.semantic_overlay.uri,
            self._inputs.report.uri,
            *((self._inputs.report_seed.uri,) if self._inputs.report_seed is not None else ()),
            *(
                "operation-execution-request-template:"
                f"{operation_id}@{canonical_digest(request.model_dump(mode='json'))}"
                for operation_id, request in sorted(
                    self._inputs.operation_bindings.operations.items()
                )
            ),
        )
        if set(self._inputs.operation_bindings.operations) != {
            "semantic_selector",
            "independent_reviewer",
        }:
            raise SemanticRoutingError(
                "schema-context model stages require exact selector and reviewer "
                "Operation Execution Binding templates"
            )
        return SemanticBindingPlan.create(
            plan_ref=(
                "semantic-binding-plan:schema-context-selection:"
                + self._inputs.selection_request.request_id
            ),
            blueprint_family=BlueprintFamily.STAGE_GRAPH,
            exact_input_refs=exact_refs,
            payload=self._inputs.model_dump(mode="json"),
        )

    async def author(
        self,
        plan: SemanticBindingPlan,
        ticket: PreparedLaunchTicket,
        *,
        run_id: str,
    ) -> RunSemanticInputBinding:
        if (
            plan.blueprint_family != BlueprintFamily.STAGE_GRAPH
            or ticket.blueprint_family != BlueprintFamily.STAGE_GRAPH
            or plan.plan_ref != ticket.semantic_binding_plan_ref
            or plan.plan_digest != ticket.semantic_binding_plan_digest
        ):
            raise SemanticRoutingError(
                "schema-context semantic plan differs from the frozen launch ticket"
            )
        inputs = SchemaContextBindingPlanInput.model_validate(plan.payload)
        selection_request = inputs.selection_request.model_copy(
            update={
                "workspace_ref": inputs.selection_request.workspace_ref.replace(
                    "{run_id}",
                    run_id,
                )
            }
        )
        operation_binding_refs = await self._operation_bindings.freeze(
            inputs.operation_bindings,
            ticket,
            run_id=run_id,
            bound_at=inputs.created_at,
        )
        return build_schema_context_selection_run_binding(
            request_scope=ticket.request_scope,
            run_id=run_id,
            effective_configuration_digest=ticket.effective_configuration_digest,
            blueprint_digest=ticket.blueprint_ref.digest,
            build_request=inputs.build_request,
            selection_request=selection_request,
            schema_definition=inputs.schema_definition,
            semantic_overlay=inputs.semantic_overlay,
            report=inputs.report,
            report_seed=inputs.report_seed,
            operation_execution_binding_refs=operation_binding_refs,
            created_at=inputs.created_at,
        )


def register_schema_context_stage_handlers(
    registry: SemanticHandlerRegistry,
    *,
    catalog_builds: SchemaCatalogBuildService,
    sources: ContentAddressedPayloadStore,
    catalog_payloads: ContentAddressedPayloadStore,
    records: SchemaGroundingRecordRepository,
    selector: SelectionAgentPort,
    reviewer: ReviewAgentPort,
) -> None:
    registry.register_stage(
        MATERIALIZE_SELECTION_CONTEXT_HANDLER,
        SCHEMA_CONTEXT_HANDLER_REVISION,
        MaterializeSelectionContextHandler(catalog_builds, sources, records),
    )
    registry.register_stage(
        SEMANTIC_SELECTOR_HANDLER,
        SCHEMA_CONTEXT_HANDLER_REVISION,
        SemanticSelectorStageHandler(
            selector,
            records,
            catalog_payloads,
            sources,
        ),
    )
    registry.register_stage(
        STRUCTURAL_VALIDATION_HANDLER,
        SCHEMA_CONTEXT_HANDLER_REVISION,
        StructuralValidationStageHandler(records, catalog_payloads),
    )
    registry.register_stage(
        INDEPENDENT_REVIEWER_HANDLER,
        SCHEMA_CONTEXT_HANDLER_REVISION,
        IndependentReviewerStageHandler(
            reviewer,
            records,
            catalog_payloads,
            sources,
        ),
    )
    registry.register_stage(
        ACCEPT_SELECTION_HANDLER,
        SCHEMA_CONTEXT_HANDLER_REVISION,
        AcceptSelectionStageHandler(records),
    )
    registry.register_workflow_evaluator(
        SELECTION_WORKFLOW_EVALUATOR,
        SCHEMA_CONTEXT_HANDLER_REVISION,
        SchemaContextWorkflowEvaluator(records),
    )


def build_schema_context_selection_run_binding(
    *,
    request_scope: str,
    run_id: str,
    effective_configuration_digest: str,
    blueprint_digest: str,
    build_request: SchemaCatalogBuildRequest,
    selection_request: SchemaContextSelectionRequest,
    schema_definition: DurableObjectRef,
    semantic_overlay: DurableObjectRef,
    report: DurableObjectRef,
    created_at: datetime,
    report_seed: DurableObjectRef | None = None,
    operation_execution_binding_refs: dict[str, str] | None = None,
) -> RunSemanticInputBinding:
    """Freeze exact inputs for the canonical five-stage schema selection graph."""

    if request_scope != build_request.request_scope:
        raise ValueError("catalog build request scope differs from Workflow Run scope")
    materialization = SelectionMaterializationInput(
        build_request=build_request,
        schema_definition=schema_definition,
        semantic_overlay=semantic_overlay,
        report_seed=report_seed,
    )
    operation = SelectionOperationInput(
        request=selection_request,
        catalog_build_id=build_request.build_id,
        report=report,
    )
    operation_refs = operation_execution_binding_refs or {}
    stage_handlers = (
        _stage_binding(
            "materialize_selection_context",
            MATERIALIZE_SELECTION_CONTEXT_HANDLER,
            "schema:schema-selection-materialization-input:v1",
            materialization,
            "schema:schema-catalog-build-record:v1",
        ),
        _stage_binding(
            "semantic_selector",
            SEMANTIC_SELECTOR_HANDLER,
            "schema:schema-selection-operation-input:v1",
            operation,
            "schema:schema-context-selection:v1",
            operation_execution_binding_ref=operation_refs.get("semantic_selector"),
        ),
        _stage_binding(
            "structural_validation",
            STRUCTURAL_VALIDATION_HANDLER,
            "schema:schema-selection-operation-input:v1",
            operation,
            "schema:schema-context-selection-validation:v1",
        ),
        _stage_binding(
            "independent_reviewer",
            INDEPENDENT_REVIEWER_HANDLER,
            "schema:schema-selection-operation-input:v1",
            operation,
            "schema:schema-context-selection-review:v1",
            operation_execution_binding_ref=operation_refs.get("independent_reviewer"),
        ),
        _stage_binding(
            "accept_selection",
            ACCEPT_SELECTION_HANDLER,
            "schema:schema-selection-acceptance-input:v1",
            SelectionAcceptanceInput(),
            "schema:accepted-schema-context-selection:v1",
        ),
    )
    evaluator = SemanticHandlerBinding(
        handler_id=SELECTION_WORKFLOW_EVALUATOR,
        handler_revision=SCHEMA_CONTEXT_HANDLER_REVISION,
        input=SemanticInputPayload.from_value(
            schema_ref="schema:schema-selection-workflow-evaluation-input:v1",
            value=SelectionWorkflowEvaluationInput().model_dump(mode="json"),
        ),
        output_contract_ref="schema:schema-selection-workflow-evaluation:v1",
    )
    return RunSemanticInputBinding.create(
        request_scope=request_scope,
        run_id=run_id,
        blueprint_family="StageGraph",
        effective_configuration_digest=effective_configuration_digest,
        blueprint_digest=blueprint_digest,
        stage_handlers=stage_handlers,
        workflow_evaluator=evaluator,
        created_at=created_at,
    )


def schema_grounding_record_ref(record: SchemaGroundingRecordEnvelope) -> str:
    encoded_id = quote(record.record_id, safe="")
    return (
        f"belllabs://schema-grounding/{record.record_type}/{encoded_id}/"
        f"{record.content_digest.removeprefix('sha256:')}"
    )


def parse_schema_grounding_record_ref(
    value: str,
) -> tuple[SchemaGroundingRecordType, str, str] | None:
    prefix = "belllabs://schema-grounding/"
    if not value.startswith(prefix):
        return None
    parts = value.removeprefix(prefix).split("/")
    if len(parts) != 3 or len(parts[2]) != 64:
        return None
    record_type = parts[0]
    if record_type not in (
        "catalog_build",
        "catalog_resource",
        "selection_draft",
        "selection_validation",
        "selection_review",
        "accepted_selection",
        "expanded_slice",
        "operation_projection",
        "compatibility_decision",
        "workspace_binding",
        "query_intent",
        "query_result",
        "reconciliation",
        "evaluation",
    ):
        return None
    return (
        cast(SchemaGroundingRecordType, record_type),
        unquote(parts[1]),
        f"sha256:{parts[2]}",
    )


def _stage_binding(
    stage_id: str,
    handler_id: str,
    schema_ref: str,
    value: BaseModel,
    output_contract_ref: str,
    *,
    operation_execution_binding_ref: str | None = None,
) -> StageHandlerBinding:
    return StageHandlerBinding(
        stage_id=stage_id,
        handler=SemanticHandlerBinding(
            handler_id=handler_id,
            handler_revision=SCHEMA_CONTEXT_HANDLER_REVISION,
            input=SemanticInputPayload.from_value(
                schema_ref=schema_ref,
                value=value.model_dump(mode="json"),
            ),
            output_contract_ref=output_contract_ref,
            operation_execution_binding_ref=operation_execution_binding_ref,
        ),
    )


def _completed(
    request: StageOperationRequest,
    binding: SemanticHandlerBinding,
    output_ref: str,
) -> StageOperationResult:
    return StageOperationResult(
        identity=request.identity,
        disposition="completed",
        output_refs=(output_ref,),
        actual_usage={"operation.attempts": 1},
        output_contract_ref=binding.output_contract_ref,
    )


def _workflow_result(
    request: WorkflowEvaluationRequest,
    binding: SemanticHandlerBinding,
    *,
    action: Literal["accept", "cycle", "fail"],
    invalidation_frontier: tuple[str, ...] = (),
    next_objective: str = "",
) -> WorkflowEvaluationResult:
    payload = {
        "run_id": request.run_id,
        "workflow_cycle": request.workflow_cycle,
        "action": action,
        "outputs": request.current_output_refs,
    }
    return WorkflowEvaluationResult(
        action=action,
        evaluation_ref=(
            "evaluation:schema-context:" + canonical_digest(payload).removeprefix("sha256:")
        ),
        invalidation_frontier=invalidation_frontier,
        next_objective=next_objective,
        evaluation_contract_ref=request.evaluation_contract_ref,
        objective_contract_ref=request.objective_contract_ref,
        output_contract_ref=binding.output_contract_ref,
    )


async def _retrieve(
    store: ContentAddressedPayloadStore,
    ref: DurableObjectRef,
) -> bytes:
    return await store.retrieve(
        ContentAddress(
            uri=ref.uri,
            digest=ref.digest,
            size=ref.size_bytes,
            version_id=ref.version_id,
        )
    )


async def _catalog_bundle(
    store: ContentAddressedPayloadStore,
    build: SchemaCatalogBuildRecord,
) -> bytes:
    if build.status != "published" or build.bundle is None:
        raise SemanticRoutingError("schema catalog build is not a published bundle")
    return await _retrieve(store, build.bundle)


async def _catalog_record(
    records: SchemaGroundingRecordRepository,
    request_scope: str,
    build_id: str,
) -> tuple[SchemaCatalogBuildRecord, SchemaGroundingRecordEnvelope]:
    envelope = await records.get(request_scope, "catalog_build", build_id)
    build = SchemaCatalogBuildRecord.model_validate(envelope.payload)
    return build, envelope


async def _single_input_record(
    records: SchemaGroundingRecordRepository,
    request: StageOperationRequest,
    record_type: SchemaGroundingRecordType,
) -> SchemaGroundingRecordEnvelope:
    candidates = [
        parsed
        for value in request.input_refs
        if (parsed := parse_schema_grounding_record_ref(value)) is not None
        and parsed[0] == record_type
    ]
    if len(candidates) != 1:
        raise SemanticRoutingError(
            f"stage requires exactly one immutable {record_type} input reference"
        )
    _, record_id, digest = candidates[0]
    envelope = await records.get(request.request_scope, record_type, record_id)
    _require_same_run(envelope, request)
    if envelope.content_digest != digest:
        raise SemanticRoutingError("schema-grounding input reference digest mismatch")
    return envelope


async def _optional_record(
    records: SchemaGroundingRecordRepository,
    request_scope: str,
    record_type: SchemaGroundingRecordType,
    record_id: str,
) -> SchemaGroundingRecordEnvelope | None:
    try:
        return await records.get(request_scope, record_type, record_id)
    except SchemaGroundingRecordNotFound:
        return None


async def _draft_for_revision(
    records: SchemaGroundingRecordRepository,
    request_scope: str,
    run_id: str,
    revision: int,
) -> SchemaGroundingRecordEnvelope | None:
    values = await records.list_for_run(
        request_scope,
        run_id,
        record_type="selection_draft",
    )
    for envelope in reversed(values):
        draft = SchemaContextSelection.model_validate(envelope.payload)
        if draft.revision == revision:
            return envelope
    return None


async def _review_for_selection(
    records: SchemaGroundingRecordRepository,
    request_scope: str,
    run_id: str,
    selection_id: str,
) -> SchemaGroundingRecordEnvelope | None:
    values = await records.list_for_run(
        request_scope,
        run_id,
        record_type="selection_review",
    )
    for envelope in reversed(values):
        review = SchemaSelectionReview.model_validate(envelope.payload)
        if review.selection_id == selection_id:
            return envelope
    return None


async def _revision_feedback(
    records: SchemaGroundingRecordRepository,
    request_scope: str,
    run_id: str,
) -> str | None:
    values = await records.list_for_run(request_scope, run_id)
    validations = [
        SelectionValidationDiagnostic.model_validate(item.payload)
        for item in values
        if item.record_type == "selection_validation"
    ]
    reviews = [
        SchemaSelectionReview.model_validate(item.payload)
        for item in values
        if item.record_type == "selection_review"
    ]
    feedback: list[str] = []
    if validations:
        feedback.extend(validations[-1].errors)
        feedback.extend(validations[-1].warnings)
    if reviews:
        feedback.extend(reviews[-1].required_revisions)
    return "\n".join(feedback) or None


def _require_address_matches(
    ref: DurableObjectRef,
    *,
    expected_ref: str,
    expected_digest: str,
) -> None:
    if ref.uri != expected_ref or ref.digest != expected_digest:
        raise SemanticRoutingError(
            "durable schema input does not match the frozen catalog build request"
        )


def _require_selection_request(
    selection_request: SchemaContextSelectionRequest,
    stage_request: StageOperationRequest,
) -> None:
    if not (
        stage_request.request_scope
        and selection_request.workspace_ref
        and selection_request.request_id
    ):
        raise SemanticRoutingError("schema selection request is incomplete")


def _require_catalog_lineage(
    request: SchemaContextSelectionRequest,
    build: SchemaCatalogBuildRecord,
) -> None:
    if (
        build.status != "published"
        or request.schema_definition_ref != build.schema_definition_ref
        or request.schema_definition_digest != build.schema_definition_digest
        or request.catalog_digest != build.catalog_digest
    ):
        raise SemanticRoutingError(
            "schema selection request differs from the published catalog lineage"
        )


def _require_draft_lineage(
    draft: SchemaContextSelection,
    request: SchemaContextSelectionRequest,
    revision: int,
    *,
    parent_selection_id: str | None,
) -> None:
    if (
        draft.revision != revision
        or draft.purpose != request.purpose
        or draft.schema_definition_ref != request.schema_definition_ref
        or draft.schema_definition_digest != request.schema_definition_digest
        or draft.catalog_digest != request.catalog_digest
        or draft.report_ref != request.report_ref
        or draft.report_digest != request.report_digest
        or draft.coverage_obligations != request.coverage_obligations
        or draft.parent_selection_id != parent_selection_id
    ):
        raise SemanticRoutingError(
            "selector output differs from the frozen request lineage or revision"
        )


def _require_input_ref(
    request: StageOperationRequest,
    expected_ref: str,
) -> None:
    if expected_ref not in request.input_refs:
        raise SemanticRoutingError(
            "stage input references do not contain the exact preceding output"
        )


def _require_same_run(
    envelope: SchemaGroundingRecordEnvelope,
    request: StageOperationRequest,
) -> None:
    if envelope.run_id != request.identity.run_id:
        raise SemanticRoutingError("schema-grounding record belongs to a different Workflow Run")


def _require_ref_matches(
    value: str,
    envelope: SchemaGroundingRecordEnvelope,
) -> None:
    if value != schema_grounding_record_ref(envelope):
        raise SemanticRoutingError("schema-grounding output reference digest mismatch")


def _catalog_from_bundle(bundle: bytes) -> SchemaCatalog:
    payload = _bundle_payload(bundle)
    try:
        encoded = payload["files_base64"]["schema/catalog/catalog.json"]
        content = base64.b64decode(encoded, validate=True)
    except (KeyError, ValueError) as error:
        raise SemanticRoutingError(
            "catalog bundle does not contain its exact typed catalog"
        ) from error
    return SchemaCatalog.model_validate_json(content)


def _bundle_payload(bundle: bytes) -> dict[str, Any]:
    try:
        payload = json.loads(bundle)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise SemanticRoutingError("catalog bundle is not canonical JSON") from error
    if not isinstance(payload, dict) or not isinstance(payload.get("files_base64"), dict):
        raise SemanticRoutingError("catalog bundle file manifest is malformed")
    return payload


@contextmanager
def _selection_workspace(
    bundle: bytes,
    *,
    selection_request: SchemaContextSelectionRequest,
    report: bytes,
    draft: SchemaContextSelection | None = None,
    validation: SelectionValidationDiagnostic | None = None,
) -> Iterator[Path]:
    with TemporaryDirectory(prefix="belllabs-schema-selection-") as temporary:
        root = Path(temporary)
        _unpack_bundle(bundle, root)
        write_json(
            root / "inputs/request.json",
            selection_request.model_dump(mode="json"),
        )
        report_path = root / "inputs/report.md"
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_bytes(report)
        if draft is not None:
            write_json(
                root / "selection/draft.json",
                draft.model_dump(mode="json"),
            )
        if validation is not None:
            write_json(
                root / "selection/deterministic-validation.json",
                validation.model_dump(mode="json"),
            )
        yield root


def _unpack_bundle(bundle: bytes, root: Path) -> None:
    payload = _bundle_payload(bundle)
    for logical_path, encoded in payload["files_base64"].items():
        if not isinstance(logical_path, str) or not isinstance(encoded, str):
            raise SemanticRoutingError("catalog bundle contains malformed file entries")
        relative = PurePosixPath(logical_path)
        if relative.is_absolute() or ".." in relative.parts or not relative.parts:
            raise SemanticRoutingError("catalog bundle contains an unsafe logical path")
        destination = root.joinpath(*relative.parts)
        destination.parent.mkdir(parents=True, exist_ok=True)
        try:
            destination.write_bytes(base64.b64decode(encoded, validate=True))
        except ValueError as error:
            raise SemanticRoutingError("catalog bundle contains invalid base64 content") from error


__all__ = [
    "ACCEPT_SELECTION_HANDLER",
    "INDEPENDENT_REVIEWER_HANDLER",
    "MATERIALIZE_SELECTION_CONTEXT_HANDLER",
    "SCHEMA_CONTEXT_HANDLER_REVISION",
    "SELECTION_WORKFLOW_EVALUATOR",
    "SEMANTIC_SELECTOR_HANDLER",
    "STRUCTURAL_VALIDATION_HANDLER",
    "AcceptSelectionStageHandler",
    "IndependentReviewerStageHandler",
    "MaterializeSelectionContextHandler",
    "SchemaContextWorkflowEvaluator",
    "SchemaContextBindingPlanInput",
    "SchemaContextSemanticBindingProvider",
    "SemanticSelectorStageHandler",
    "StructuralValidationStageHandler",
    "build_schema_context_selection_run_binding",
    "parse_schema_grounding_record_ref",
    "register_schema_context_stage_handlers",
    "schema_grounding_record_ref",
]
