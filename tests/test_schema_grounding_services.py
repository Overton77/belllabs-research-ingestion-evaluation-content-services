from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime
from typing import Any

import pytest

from app.application.orchestration_binding_repository import (
    InMemoryRunSemanticInputBindingRepository,
)
from app.application.orchestration_routing import (
    BoundGoalIndependentVerifier,
    BoundGoalIterationExecutor,
    SemanticHandlerRegistry,
)
from app.application.schema_catalog import CATALOG_GENERATOR_VERSION
from app.application.schema_catalog_build import SchemaCatalogBuildService
from app.application.schema_context_derivation import SchemaContextDerivationService
from app.application.schema_grounding_repository import (
    InMemorySchemaGroundingRecordRepository,
)
from app.application.schema_grounding_semantic_handlers import (
    build_supporting_graph_run_binding,
    register_supporting_graph_goal_handlers,
)
from app.application.schema_workspace_binding import SchemaGraphAdmissionService
from app.application.supporting_graph_reconciliation import (
    SupportingGraphReconciliationWorkflow,
)
from app.domain.orchestration.contracts import (
    GoalAgentRunIdentity,
    GoalExecutionClaim,
    GoalIterationIdentity,
    GoalVerificationRequest,
)
from app.domain.schema_context.canonicalization import sha256_digest
from app.domain.schema_context.contracts import (
    QueryExecutionIntent,
    QueryExecutionResult,
)
from app.domain.schema_grounding.contracts import (
    GraphAdmissionRequest,
    GraphCapabilityGrant,
    SchemaCatalogBuildRequest,
    SchemaDeploymentManifestRef,
    SchemaWorkspaceBindingRef,
    SupportingGraphReconciliationRequest,
)
from app.domain.schema_grounding.errors import SchemaSourceDigestMismatch
from app.integrations.control_plane_payloads import InMemoryPayloadStore
from tests.schema_context_helpers import SDL, accepted, catalog

NOW = datetime(2026, 7, 24, 12, 0, tzinfo=UTC)
EMPTY_OVERLAY = json.dumps(
    {"overlay_version": "1", "modules": [], "elements": {}},
    sort_keys=True,
).encode()


def _build_request(
    *,
    build_id: str = "catalog-build-1",
    source_ref: str = "s3://schemas/example/schema.graphql?versionId=1",
    source_digest: str | None = None,
) -> SchemaCatalogBuildRequest:
    return SchemaCatalogBuildRequest(
        build_id=build_id,
        idempotency_key=f"idempotency:{build_id}",
        request_scope="tenant-1",
        schema_definition_ref=source_ref,
        schema_definition_digest=source_digest or sha256_digest(SDL),
        schema_definition_media_type="application/graphql",
        semantic_overlay_ref="schema-overlay:empty:v1",
        semantic_overlay_revision="1",
        semantic_overlay_digest=sha256_digest(EMPTY_OVERLAY),
        candidate_seed_ref="artifact:report-seed:v1",
        candidate_seed_digest=sha256_digest(b"Organization offers Product"),
        catalog_schema_version="1",
        generator_version=CATALOG_GENERATOR_VERSION,
        normalization_policy_version="graphql-sdl-normalization-v1",
        publication_target="object-storage:schema-catalogs",
        actor_id="schema-publisher",
        authority_ref="authority:schema-catalog-publisher",
        requested_at=NOW,
    )


@pytest.mark.asyncio
async def test_catalog_build_is_content_addressed_idempotent_and_concurrency_safe() -> None:
    records = InMemorySchemaGroundingRecordRepository()
    payloads = InMemoryPayloadStore()
    service = SchemaCatalogBuildService(records, payloads)
    request = _build_request()

    first, second = await asyncio.gather(
        service.build(
            request,
            schema_definition=SDL,
            semantic_overlay=EMPTY_OVERLAY,
            report_seed=b"Organization offers Product",
        ),
        service.build(
            request,
            schema_definition=SDL,
            semantic_overlay=EMPTY_OVERLAY,
            report_seed=b"Organization offers Product",
        ),
    )
    replay = await service.build(
        request,
        schema_definition=SDL,
        semantic_overlay=EMPTY_OVERLAY,
        report_seed=b"Organization offers Product",
    )

    assert first == second == replay
    assert first.status == "published"
    assert first.bundle is not None
    assert first.bundle.digest.startswith("sha256:")
    assert first.resource_count == len(first.resources)
    assert first.tier0_size_bytes < 50 * 1024
    assert {"selection-tier0", "selection-candidates"} <= set(first.profiles)


@pytest.mark.asyncio
async def test_catalog_replay_authenticates_supplied_bytes_before_returning_record() -> None:
    service = SchemaCatalogBuildService(
        InMemorySchemaGroundingRecordRepository(),
        InMemoryPayloadStore(),
    )
    request = _build_request()
    await service.build(
        request,
        schema_definition=SDL,
        semantic_overlay=EMPTY_OVERLAY,
        report_seed=b"Organization offers Product",
    )

    with pytest.raises(SchemaSourceDigestMismatch):
        await service.build(
            request,
            schema_definition=b"type Substituted { id: ID! }",
            semantic_overlay=EMPTY_OVERLAY,
            report_seed=b"Organization offers Product",
        )


@pytest.mark.asyncio
async def test_catalog_physical_and_logical_digests_ignore_source_location() -> None:
    records = InMemorySchemaGroundingRecordRepository()
    service = SchemaCatalogBuildService(records, InMemoryPayloadStore())

    first = await service.build(
        _build_request(
            build_id="catalog-build-a",
            source_ref="s3://one/schema.graphql",
        ).model_copy(
            update={"candidate_seed_ref": None, "candidate_seed_digest": None}
        ),
        schema_definition=SDL,
        semantic_overlay=EMPTY_OVERLAY,
    )
    second = await service.build(
        _build_request(
            build_id="catalog-build-b",
            source_ref="s3://two/schema.graphql",
        ).model_copy(
            update={"candidate_seed_ref": None, "candidate_seed_digest": None}
        ),
        schema_definition=SDL,
        semantic_overlay=EMPTY_OVERLAY,
    )

    assert first.physical_schema_digest == second.physical_schema_digest
    assert first.catalog_digest == second.catalog_digest


@pytest.mark.asyncio
async def test_catalog_digest_mismatch_fails_before_publication_and_persists_rejection() -> None:
    records = InMemorySchemaGroundingRecordRepository()
    service = SchemaCatalogBuildService(records, InMemoryPayloadStore())
    request = _build_request(source_digest="sha256:" + "f" * 64).model_copy(
        update={"candidate_seed_ref": None, "candidate_seed_digest": None}
    )

    with pytest.raises(SchemaSourceDigestMismatch):
        await service.build(
            request,
            schema_definition=SDL,
            semantic_overlay=EMPTY_OVERLAY,
        )

    rejected = await service.get("tenant-1", request.build_id)
    assert rejected.status == "rejected"
    assert rejected.bundle is None
    assert rejected.diagnostics == ("SchemaSourceDigestMismatch",)


@pytest.mark.asyncio
async def test_derivation_persists_closure_and_purpose_bound_projection() -> None:
    value = catalog()
    records = InMemorySchemaGroundingRecordRepository()
    service = SchemaContextDerivationService(records)
    result = await service.derive(
        request_scope="tenant-1",
        run_id="run-1",
        accepted=accepted(value),
        catalog=value,
        derived_at=NOW,
    )

    assert result.expanded_slice.accepted_selection_digest == result.accepted_selection_digest
    assert result.projection.purpose == "read_query_reconciliation"
    assert set(result.projection.allowed_node_labels) == {"Organization", "Product"}
    persisted = await records.list_for_run("tenant-1", "run-1")
    assert {item.record_type for item in persisted} == {
        "expanded_slice",
        "operation_projection",
    }

    with pytest.raises(ValueError, match="cannot be reused"):
        await service.derive(
            request_scope="tenant-1",
            run_id="run-2",
            accepted=accepted(value),
            catalog=value,
            purpose="graph_mutation",
        )


class _Executor:
    def __init__(self) -> None:
        self.calls = 0
        self.closed = False

    async def execute(self, intent: QueryExecutionIntent, _projection: Any) -> QueryExecutionResult:
        self.calls += 1
        logical: dict[str, Any] = {
            "result_id": f"result-{intent.intent_id}",
            "intent_id": intent.intent_id,
            "intent_digest": sha256_digest(intent.model_dump(mode="json")),
            "query_kind": intent.query_kind,
            "status": "succeeded",
            "compiled_cypher": "MATCH (n:Organization {name: $value}) RETURN n LIMIT $limit",
            "redacted_parameters": {"limit": intent.limit},
            "columns": ("entity",),
            "records": ({"entity": {"id": "org-1", "name": "TruDiagnostic"}},),
            "record_count": 1,
            "truncated": False,
            "elapsed_ms": 1,
            "database": "neo4j",
            "server_info": {"agent": "test"},
            "diagnostics": (),
            "error_type": None,
            "started_at": NOW,
            "finished_at": NOW,
        }
        return QueryExecutionResult(
            **logical,
            result_digest=sha256_digest(
                {**logical, "started_at": NOW.isoformat(), "finished_at": NOW.isoformat()}
            ),
        )

    async def close(self) -> None:
        self.closed = True


class _ExecutorFactory:
    def __init__(self) -> None:
        self.calls = 0
        self.executor = _Executor()

    async def create(
        self,
        _admission: GraphAdmissionRequest,
        _decision: Any,
    ) -> _Executor:
        self.calls += 1
        return self.executor


async def _reconciliation_fixture(
    *,
    deployment: bool = True,
    capability: bool = True,
) -> tuple[
    SupportingGraphReconciliationRequest,
    InMemorySchemaGroundingRecordRepository,
    _ExecutorFactory,
]:
    value = catalog()
    records = InMemorySchemaGroundingRecordRepository()
    derivation = await SchemaContextDerivationService().derive(
        request_scope="tenant-1",
        run_id="run-reconciliation",
        accepted=accepted(value),
        catalog=value,
        derived_at=NOW,
    )
    workspace = SchemaWorkspaceBindingRef(
        binding_id="binding-1",
        binding_digest="sha256:" + "b" * 64,
        request_scope="tenant-1",
        run_id="run-reconciliation",
        workspace_id="workspace-1",
        slot_name="graph_query_runtime",
        catalog_build_id="catalog-build-1",
        catalog_digest=value.catalog_digest,
        resource_manifest_digest="sha256:" + "d" * 64,
        profile="graph-query-runtime",
        purpose="read_query_reconciliation",
        read_only=True,
        issuer_authority_ref="issue-13:schema-workspace-materialization",
        materializer_version="issue-13-materializer-v1",
        created_at=NOW,
    )
    manifest = (
        SchemaDeploymentManifestRef(
            manifest_id="manifest-1",
            manifest_digest="sha256:" + "c" * 64,
            environment="production",
            database="neo4j",
            schema_definition_ref=value.source_ref,
            deployed_sdl_digest=value.source_digest,
            deployment_id="deployment-1",
            issuer_authority_ref="issue-12:graph-schema-deployment",
            active=True,
            revoked=False,
            issued_at=NOW,
        )
        if deployment
        else None
    )
    grant = GraphCapabilityGrant(
        grant_id="grant-1",
        grant_digest="sha256:" + "a" * 64,
        request_scope="tenant-1",
        run_id="run-reconciliation",
        environment="production",
        database="neo4j",
        purpose="read_query_reconciliation",
        admitted=capability,
        query_kinds=frozenset({"exact_identity"}),
        allowed_node_labels=frozenset({"Organization"}),
        allowed_relationship_types=frozenset(),
        maximum_limit=10,
        maximum_traversal_depth=1,
        secret_ref="secret:neo4j-readonly:v1",
        budget_reservation_id="reservation:graph-reads",
        sensitive_data_policy_ref="policy:sensitive-data:v1",
        decided_by_authority_ref="graph-authority:read-capability",
        decided_at=NOW,
    )
    admission = GraphAdmissionRequest(
        request_scope="tenant-1",
        run_id="run-reconciliation",
        purpose="read_query_reconciliation",
        environment="production",
        database="neo4j",
        deployment_id="deployment-1",
        catalog_build_id="catalog-build-1",
        schema_definition_ref=value.source_ref,
        schema_definition_digest=value.source_digest,
        catalog_digest=value.catalog_digest,
        resource_manifest_digest="sha256:" + "d" * 64,
        projection_id=derivation.projection.projection_id,
        projection_digest=derivation.projection.projection_digest,
        deployment_manifest=manifest,
        workspace_binding=workspace,
        graph_capability=grant,
        requested_at=NOW,
    )
    intent = QueryExecutionIntent(
        intent_id="intent-1",
        sequence=1,
        purpose="pre_ingestion_graph_reconciliation",
        query_kind="exact_identity",
        projection_id=derivation.projection.projection_id,
        projection_digest=derivation.projection.projection_digest,
        schema_definition_digest=derivation.projection.source_schema_digest,
        selection_digest=derivation.projection.accepted_selection_digest,
        goal="Locate TruDiagnostic by exact name.",
        coverage_obligation_ids=("organization_identity",),
        labels=("Organization",),
        relationship_types=(),
        parameters={"field": "name", "value": "TruDiagnostic"},
        requested_fields=("id", "name"),
        limit=5,
        max_depth=0,
        stopping_evidence="One exact match or a valid zero result.",
        created_at=NOW,
    )
    factory = _ExecutorFactory()
    return (
        SupportingGraphReconciliationRequest(
            reconciliation_id="reconciliation-1",
            request_scope="tenant-1",
            run_id="run-reconciliation",
            question="Which existing organization matches TruDiagnostic?",
            admission=admission,
            projection=derivation.projection,
            intents=(intent,),
            maximum_intents=5,
            created_at=NOW,
        ),
        records,
        factory,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("deployment", "capability", "failure_code"),
    (
        (False, True, "deployment_manifest_missing"),
        (True, False, "graph_capability_denied"),
    ),
)
async def test_graph_gate_denial_prevents_executor_creation(
    deployment: bool,
    capability: bool,
    failure_code: str,
) -> None:
    request, records, factory = await _reconciliation_fixture(
        deployment=deployment,
        capability=capability,
    )
    workflow = SupportingGraphReconciliationWorkflow(
        admission=SchemaGraphAdmissionService(records),
        executor_factory=factory,
        records=records,
    )

    result = await workflow.run(request, completed_at=NOW)

    assert result.status == "rejected"
    assert result.admission_decision.failure_code == failure_code
    assert factory.calls == 0


@pytest.mark.asyncio
async def test_graph_gate_rejects_resource_manifest_substitution() -> None:
    request, records, factory = await _reconciliation_fixture()
    request = request.model_copy(
        update={
            "admission": request.admission.model_copy(
                update={"resource_manifest_digest": "sha256:" + "e" * 64}
            )
        }
    )
    workflow = SupportingGraphReconciliationWorkflow(
        admission=SchemaGraphAdmissionService(records),
        executor_factory=factory,
        records=records,
    )

    result = await workflow.run(request)

    assert result.status == "rejected"
    assert result.admission_decision.failure_code == "workspace_profile_invalid"
    assert factory.calls == 0


@pytest.mark.asyncio
async def test_supporting_reconciliation_persists_exact_intent_result_evidence() -> None:
    request, records, factory = await _reconciliation_fixture()
    workflow = SupportingGraphReconciliationWorkflow(
        admission=SchemaGraphAdmissionService(records),
        executor_factory=factory,
        records=records,
    )

    result = await workflow.run(request, completed_at=NOW)
    replay = await workflow.run(request)

    assert result.status == "completed"
    assert replay == result
    assert result.successful_count == 1
    assert result.intent_result_references == result.evidence.intent_result_references
    assert factory.calls == 1
    assert factory.executor.calls == 1
    assert factory.executor.closed
    record_types = {
        record.record_type
        for record in await records.list_for_run("tenant-1", "run-reconciliation")
    }
    assert {
        "compatibility_decision",
        "workspace_binding",
        "query_intent",
        "query_result",
        "reconciliation",
        "evaluation",
    } <= record_types


@pytest.mark.asyncio
async def test_goal_semantic_handlers_execute_and_independently_rehydrate_reconciliation() -> None:
    request, records, factory = await _reconciliation_fixture()
    workflow = SupportingGraphReconciliationWorkflow(
        admission=SchemaGraphAdmissionService(records),
        executor_factory=factory,
        records=records,
    )
    run_binding = build_supporting_graph_run_binding(
        request=request,
        effective_configuration_digest="sha256:" + "d" * 64,
        blueprint_digest="sha256:" + "c" * 64,
        acceptance_contract_ref="evaluation:supporting-graph-reconciliation:v1",
        created_at=NOW,
    )
    assert run_binding.goal_operation_handlers[0].operation_class == "goal_iteration"
    assert run_binding.goal_verifier is not None
    assert run_binding.goal_handoff is not None
    binding_repository = InMemoryRunSemanticInputBindingRepository()
    await binding_repository.create(run_binding)
    handlers = SemanticHandlerRegistry()
    register_supporting_graph_goal_handlers(
        handlers,
        workflow=workflow,
        records=records,
    )
    identity = GoalAgentRunIdentity(
        iteration=GoalIterationIdentity(
            run_id=request.run_id,
            goal_iteration=1,
            goal_revision_id="goal-revision:1",
            execution_epoch=1,
        ),
        agent_run=1,
        session_generation=1,
    )
    claim = GoalExecutionClaim(
        identity=identity,
        idempotency_key="goal-operation:reconciliation:1",
        operation_class="goal_iteration",
        objective=request.question,
        protected_scope_digest="sha256:" + "f" * 64,
        reservation_id="reservation:goal:1",
        reservation={"goal.iterations": 1, "graph.reads": 1},
        session_mode="reuse",
        session_id="goal-session:1",
        workspace_mode="shared",
        workspace_namespace="run/run-reconciliation/goal/workspace/1",
        snapshot_mode="on_failure",
        request_scope=request.request_scope,
        semantic_input_binding_ref=run_binding.binding_id,
        effective_configuration_digest=run_binding.effective_configuration_digest,
        blueprint_digest=run_binding.blueprint_digest,
    )

    execution = await BoundGoalIterationExecutor(
        binding_repository,
        handlers,
    ).execute(
        claim
    )

    verification = await BoundGoalIndependentVerifier(
        binding_repository,
        handlers,
    ).verify(
        GoalVerificationRequest(
            claim=claim,
            execution_result=execution,
            verifier_ref="verifier:supporting-graph-reconciliation:v1",
            acceptance_contract_ref=(
                "evaluation:supporting-graph-reconciliation:v1"
            ),
        )
    )

    assert execution.disposition == "completed"
    assert execution.completion_claim
    assert execution.actual_usage == {"graph.reads": 1}
    assert verification.action == "verified_completion"
    assert verification.evidence_refs == execution.output_refs
    assert factory.executor.calls == 1
