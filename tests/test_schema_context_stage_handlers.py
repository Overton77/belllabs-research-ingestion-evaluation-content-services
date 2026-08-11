from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
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
)
from app.application.schema_catalog import CATALOG_GENERATOR_VERSION
from app.application.schema_catalog_build import SchemaCatalogBuildService
from app.application.schema_context_selection import AgentRunOutput
from app.application.schema_context_stage_handlers import (
    build_schema_context_selection_run_binding,
    parse_schema_grounding_record_ref,
    register_schema_context_stage_handlers,
)
from app.application.schema_grounding_repository import (
    InMemorySchemaGroundingRecordRepository,
)
from app.domain.orchestration.contracts import (
    StageCandidateIdentity,
    StageExecutionIdentity,
    StageOperationRequest,
    WorkflowEvaluationRequest,
)
from app.domain.run_control.contracts import ActorContext
from app.domain.schema_context.contracts import (
    PropertyIntentHint,
    SchemaContextSelection,
    SchemaContextSelectionRequest,
    SchemaSelectionReview,
)
from app.domain.schema_grounding.contracts import (
    DurableObjectRef,
    SchemaCatalogBuildRequest,
)
from app.integrations.control_plane_payloads import ContentAddress, InMemoryPayloadStore
from app.temporal.coordinator_runtime import (
    GoalDirectedCoordinatorDependencies,
    SchemaGroundingCoordinatorRuntimeDependencies,
    StageGraphCoordinatorDependencies,
    create_schema_grounding_coordinator_runtime,
)
from tests.schema_context_helpers import SDL

NOW = datetime(2026, 7, 26, 12, 0, tzinfo=UTC)
DIGEST = "sha256:" + "a" * 64
BLUEPRINT_DIGEST = "sha256:" + "b" * 64
REPORT = b"Organization offers Product"
EMPTY_OVERLAY = json.dumps(
    {"overlay_version": "1", "modules": [], "elements": {}},
    sort_keys=True,
).encode()


class Selector:
    calls = 0

    def __init__(self, selection_request: SchemaContextSelectionRequest) -> None:
        self._request = selection_request

    async def select(
        self,
        run_root: Path,
        *,
        revision_feedback: str | None = None,
    ) -> AgentRunOutput:
        self.calls += 1
        assert (run_root / "schema/manifest.json").is_file()
        assert (run_root / "inputs/report.md").read_bytes() == REPORT
        assert revision_feedback is None
        return AgentRunOutput(
            output=SchemaContextSelection(
                selection_id="selection-1",
                revision=1,
                purpose=self._request.purpose,
                schema_definition_ref=self._request.schema_definition_ref,
                schema_definition_digest=self._request.schema_definition_digest,
                catalog_digest=self._request.catalog_digest,
                report_ref=self._request.report_ref,
                report_digest=self._request.report_digest,
                selected_node_types=("Organization", "Product"),
                selected_relationship_types=("OFFERS",),
                property_intent_hints=(
                    PropertyIntentHint(
                        node_type="Organization",
                        properties=("name",),
                    ),
                ),
                coverage_obligations=self._request.coverage_obligations,
                rationale=(
                    "Organization and Product cover the report; OrganizationState maps "
                    "to OrganizationSnapshot and ProductState maps to ProductSnapshot."
                ),
                evidence_locators=("inputs/report.md",),
                created_at=NOW,
            ),
            usage={"total_tokens": 10},
        )


class Reviewer:
    calls = 0

    async def review(
        self,
        run_root: Path,
        *,
        retry_reason: str | None = None,
    ) -> AgentRunOutput:
        self.calls += 1
        assert retry_reason is None
        draft = SchemaContextSelection.model_validate_json(
            (run_root / "selection/draft.json").read_text(encoding="utf-8")
        )
        assert (run_root / "selection/deterministic-validation.json").is_file()
        return AgentRunOutput(
            output=SchemaSelectionReview(
                review_id="review-1",
                selection_id=draft.selection_id,
                reviewer_role="independent_schema_reviewer",
                decision="accepted",
                structural_valid=True,
                coverage_findings=("covered",),
                missing_concepts=(),
                overbroad_selections=(),
                unjustified_selections=(),
                temporal_coverage="Legacy snapshot mapping is explicit.",
                identity_coverage="Organization identity is covered.",
                provenance_coverage="The report locator is present.",
                near_miss_assessment="No near miss changes membership.",
                required_revisions=(),
                rationale="Accepted by the independently bound reviewer operation.",
                created_at=NOW,
            ),
            usage={"total_tokens": 5},
        )


async def _put(
    store: InMemoryPayloadStore,
    payload: bytes,
    *,
    media_type: str,
) -> DurableObjectRef:
    address = await store.put(payload)
    return _durable(address, media_type)


def _durable(address: ContentAddress, media_type: str) -> DurableObjectRef:
    return DurableObjectRef(
        uri=address.uri,
        digest=address.digest,
        size_bytes=address.size,
        media_type=media_type,
        version_id=address.version_id,
    )


def _stage_request(
    binding_id: str,
    stage_id: str,
    input_refs: tuple[str, ...],
) -> StageOperationRequest:
    return StageOperationRequest(
        identity=StageExecutionIdentity(
            run_id="selection-run-1",
            execution_epoch=1,
            candidate=StageCandidateIdentity(
                stage_id=stage_id,
                mapped_instance_presence=0,
                mapped_instance_id="NO_MAPPED_INSTANCE",
                workflow_cycle_ordinal=0,
                stage_cycle_ordinal=0,
                operation_slot_id="execute",
            ),
            semantic_attempt=1,
        ),
        idempotency_key=f"operation:{stage_id}:1",
        objective=f"Execute {stage_id}",
        input_refs=input_refs,
        reservation_id=f"reservation:{stage_id}:1",
        reservation={"operation.attempts": 1},
        workspace_namespace=f"run/selection-run-1/stage/{stage_id}",
        request_scope="tenant-1",
        semantic_input_binding_ref=binding_id,
        effective_configuration_digest=DIGEST,
        blueprint_digest=BLUEPRINT_DIGEST,
    )


@pytest.mark.asyncio
async def test_real_schema_context_handlers_execute_all_five_stages() -> None:
    records = InMemorySchemaGroundingRecordRepository()
    sources = InMemoryPayloadStore()
    catalog_payloads = InMemoryPayloadStore()
    schema_ref = await _put(sources, SDL, media_type="application/graphql")
    overlay_ref = await _put(
        sources,
        EMPTY_OVERLAY,
        media_type="application/json",
    )
    report_ref = await _put(sources, REPORT, media_type="text/markdown")
    build_request = SchemaCatalogBuildRequest(
        build_id="catalog-build-1",
        idempotency_key="catalog-build:1",
        request_scope="tenant-1",
        schema_definition_ref=schema_ref.uri,
        schema_definition_digest=schema_ref.digest,
        semantic_overlay_ref=overlay_ref.uri,
        semantic_overlay_revision="1",
        semantic_overlay_digest=overlay_ref.digest,
        catalog_schema_version="1",
        generator_version=CATALOG_GENERATOR_VERSION,
        normalization_policy_version="graphql-sdl-normalization-v1",
        publication_target="object-storage:schema-catalogs",
        actor_id="schema-publisher",
        authority_ref="authority:schema-catalog-publisher",
        requested_at=NOW,
    )
    catalog_builds = SchemaCatalogBuildService(records, catalog_payloads)
    build = await catalog_builds.build(
        build_request,
        schema_definition=SDL,
        semantic_overlay=EMPTY_OVERLAY,
    )
    assert build.catalog_digest is not None
    selection_request = SchemaContextSelectionRequest(
        request_id="selection-request-1",
        purpose="pre_ingestion_graph_reconciliation",
        intended_operations=("read",),
        schema_definition_ref=build.schema_definition_ref,
        schema_definition_digest=build.schema_definition_digest,
        catalog_digest=build.catalog_digest,
        report_ref=report_ref.uri,
        report_digest=report_ref.digest,
        coverage_obligations=("organization_identity",),
        workspace_ref="workspace:selection-run-1",
        created_at=NOW,
    )
    selector = Selector(selection_request)
    reviewer = Reviewer()
    registry = SemanticHandlerRegistry()
    register_schema_context_stage_handlers(
        registry,
        catalog_builds=catalog_builds,
        sources=sources,
        catalog_payloads=catalog_payloads,
        records=records,
        selector=selector,
        reviewer=reviewer,
    )
    binding = build_schema_context_selection_run_binding(
        request_scope="tenant-1",
        run_id="selection-run-1",
        effective_configuration_digest=DIGEST,
        blueprint_digest=BLUEPRINT_DIGEST,
        build_request=build_request,
        selection_request=selection_request,
        schema_definition=schema_ref,
        semantic_overlay=overlay_ref,
        report=report_ref,
        created_at=NOW,
    )
    bindings = InMemoryRunSemanticInputBindingRepository()
    await bindings.create(binding)
    executor = BoundStageOperationExecutor(bindings, registry)
    outputs: dict[str, tuple[str, ...]] = {}
    prior: tuple[str, ...] = ()
    for stage_id in (
        "materialize_selection_context",
        "semantic_selector",
        "structural_validation",
        "independent_reviewer",
        "accept_selection",
    ):
        result = await executor.execute(
            _stage_request(binding.binding_id, stage_id, prior)
        )
        assert result.disposition == "completed"
        outputs[stage_id] = result.output_refs
        prior = result.output_refs

    accepted_ref = parse_schema_grounding_record_ref(
        outputs["accept_selection"][0]
    )
    assert accepted_ref is not None
    assert accepted_ref[0] == "accepted_selection"
    assert selector.calls == 1
    assert reviewer.calls == 1

    evaluation = await BoundWorkflowEvaluator(bindings, registry).evaluate(
        WorkflowEvaluationRequest(
            run_id="selection-run-1",
            workflow_cycle=0,
            objective="Select exact schema context.",
            current_output_refs=outputs,
            execution_lineage=(),
            request_scope="tenant-1",
            semantic_input_binding_ref=binding.binding_id,
            effective_configuration_digest=DIGEST,
            blueprint_digest=BLUEPRINT_DIGEST,
            evaluation_contract_ref="evaluation:schema-selection:v1",
            objective_contract_ref="objective:bounded-semantic-revision:v1",
        )
    )
    assert evaluation.action == "accept"

    # Activity retry/replay reuses immutable outputs and does not invoke agents again.
    repeated = await executor.execute(
        _stage_request(
            binding.binding_id,
            "semantic_selector",
            outputs["materialize_selection_context"],
        )
    )
    assert repeated.output_refs == outputs["semantic_selector"]
    assert selector.calls == 1


@pytest.mark.asyncio
async def test_selection_evaluator_cycles_only_the_semantic_frontier_once() -> None:
    records = InMemorySchemaGroundingRecordRepository()
    registry = SemanticHandlerRegistry()

    class Unused:
        async def select(
            self,
            run_root: Path,
            *,
            revision_feedback: str | None = None,
        ) -> AgentRunOutput:
            raise AssertionError

        async def review(
            self,
            run_root: Path,
            *,
            retry_reason: str | None = None,
        ) -> AgentRunOutput:
            raise AssertionError

    store = InMemoryPayloadStore()
    register_schema_context_stage_handlers(
        registry,
        catalog_builds=SchemaCatalogBuildService(records, store),
        sources=store,
        catalog_payloads=store,
        records=records,
        selector=Unused(),
        reviewer=Unused(),
    )
    # Only the evaluator is exercised; a minimal valid binding supplies its route.
    source = await _put(store, b"type Node { id: ID! }", media_type="application/graphql")
    overlay = await _put(store, EMPTY_OVERLAY, media_type="application/json")
    report = await _put(store, REPORT, media_type="text/markdown")
    build_request = SchemaCatalogBuildRequest(
        build_id="unused-build",
        idempotency_key="unused",
        request_scope="tenant-1",
        schema_definition_ref=source.uri,
        schema_definition_digest=source.digest,
        semantic_overlay_ref=overlay.uri,
        semantic_overlay_revision="1",
        semantic_overlay_digest=overlay.digest,
        catalog_schema_version="1",
        generator_version=CATALOG_GENERATOR_VERSION,
        normalization_policy_version="v1",
        publication_target="object-storage:test",
        actor_id="test",
        authority_ref="authority:test",
        requested_at=NOW,
    )
    selection_request = SchemaContextSelectionRequest(
        request_id="unused",
        purpose="pre_ingestion_graph_reconciliation",
        intended_operations=("read",),
        schema_definition_ref=source.uri,
        schema_definition_digest=source.digest,
        catalog_digest="sha256:" + "c" * 64,
        report_ref=report.uri,
        report_digest=report.digest,
        coverage_obligations=("identity",),
        workspace_ref="workspace:test",
        created_at=NOW,
    )
    binding = build_schema_context_selection_run_binding(
        request_scope="tenant-1",
        run_id="selection-run-1",
        effective_configuration_digest=DIGEST,
        blueprint_digest=BLUEPRINT_DIGEST,
        build_request=build_request,
        selection_request=selection_request,
        schema_definition=source,
        semantic_overlay=overlay,
        report=report,
        created_at=NOW,
    )
    bindings = InMemoryRunSemanticInputBindingRepository()
    await bindings.create(binding)
    evaluator = BoundWorkflowEvaluator(bindings, registry)

    first = await evaluator.evaluate(
        WorkflowEvaluationRequest(
            run_id=binding.run_id,
            workflow_cycle=0,
            objective="Select.",
            current_output_refs={},
            execution_lineage=(),
            request_scope=binding.request_scope,
            semantic_input_binding_ref=binding.binding_id,
            effective_configuration_digest=DIGEST,
            blueprint_digest=BLUEPRINT_DIGEST,
            evaluation_contract_ref="evaluation:schema-selection:v1",
            objective_contract_ref="objective:bounded-semantic-revision:v1",
        )
    )
    assert first.action == "cycle"
    assert first.invalidation_frontier == ("semantic_selector",)

    second = await evaluator.evaluate(
        WorkflowEvaluationRequest(
            run_id=binding.run_id,
            workflow_cycle=1,
            objective=first.next_objective,
            current_output_refs={},
            execution_lineage=(),
            request_scope=binding.request_scope,
            semantic_input_binding_ref=binding.binding_id,
            effective_configuration_digest=DIGEST,
            blueprint_digest=BLUEPRINT_DIGEST,
            evaluation_contract_ref="evaluation:schema-selection:v1",
            objective_contract_ref="objective:bounded-semantic-revision:v1",
        )
    )
    assert second.action == "fail"


def test_production_runtime_shares_postgres_binding_authority_with_workers() -> None:
    records = InMemorySchemaGroundingRecordRepository()
    store = InMemoryPayloadStore()

    class UnusedAgents:
        async def select(
            self,
            run_root: Path,
            *,
            revision_feedback: str | None = None,
        ) -> AgentRunOutput:
            raise AssertionError

        async def review(
            self,
            run_root: Path,
            *,
            retry_reason: str | None = None,
        ) -> AgentRunOutput:
            raise AssertionError

    runtime = create_schema_grounding_coordinator_runtime(
        application_postgres_pool=cast(Any, object()),
        dependencies=SchemaGroundingCoordinatorRuntimeDependencies(
            lifecycle=cast(Any, object()),
            records=records,
            catalog_builds=SchemaCatalogBuildService(records, store),
            sources=store,
            catalog_payloads=store,
            selector=UnusedAgents(),
            reviewer=UnusedAgents(),
            reconciliations=cast(Any, object()),
            goal_directed=GoalDirectedCoordinatorDependencies(
                run_control=cast(Any, object()),
                operation_bindings=cast(Any, object()),
                templates=InMemoryGoalOperationTemplateRepository(),
                documents=cast(Any, object()),
                actor=ActorContext(
                    actor_id="schema-context-test",
                    permissions=frozenset({"workflow_run.goal_directed"}),
                ),
            ),
            stagegraph=StageGraphCoordinatorDependencies(
                run_control=cast(Any, object()),
                repository=cast(Any, object()),
                operation_bindings=cast(Any, object()),
                templates=cast(Any, object()),
            ),
        ),
    )

    assert runtime.activities.stagegraph is not None
    assert runtime.activities.goal_directed is not None
    assert runtime.binding_service._repository is runtime.bindings
