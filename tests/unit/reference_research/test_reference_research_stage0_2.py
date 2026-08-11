from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from pydantic import ValidationError

from app.agent_server.graphs import GRAPH_REGISTRY
from app.agent_server.operations.reference_research import ReferenceLangGraphCanaryExecutor
from app.api.reference_research_schemas import reference_research_contract_schemas
from app.application.control_plane.service import ControlPlaneService
from app.application.control_plane.control_plane_repository import InMemoryDefinitionRepository
from app.application.operations.operation_executor import (
    CancellationContext,
    ExactStageExecutionBinding,
    OperationExecutorConformanceHarness,
    StageOperationRequest,
)
from app.application.operations.operation_journal import (
    InMemoryAtomicOperationJournalRepository,
    OperationJournalService,
)
from app.application.reference_research.service import (
    ImmutableManifestStore,
    classify_reference_fixture,
    execute_reference_fixture,
    load_reference_fixture,
    prepare_reference_implementation,
    reconstruct_typed_result_from_journal,
)
from app.domain.control_plane.canonical import sha256_digest
from app.domain.control_plane.extensions import ExtensionRegistry
from app.domain.graph_runtime.kernel import (
    ResourceKind,
    ResourceLeaseRecord,
    ResourceLeaseRequest,
    ResourceLeaseStatus,
)
from app.domain.reference_research.contracts import (
    DAVE_FAMILY_ID,
    QUALIA_FAMILY_ID,
    CompanyRelationshipClass,
    DaveFixtureInput,
    DaveOwnershipResult,
    QualiaCatalogResult,
    QualiaFixtureInput,
)
from app.integrations.control_plane_payloads import InMemoryPayloadStore

NOW = datetime(2026, 1, 15, 12, 0, tzinfo=UTC)
FIXTURES = Path(__file__).parent / "fixtures" / "reference_blueprints"


def fixture(name: str) -> QualiaFixtureInput | DaveFixtureInput:
    return load_reference_fixture((FIXTURES / name).read_bytes())


def control_plane() -> ControlPlaneService:
    return ControlPlaneService(
        InMemoryDefinitionRepository(), ExtensionRegistry(), InMemoryPayloadStore()
    )


def lease(run_id: str, digest: str) -> ResourceLeaseRecord:
    request = ResourceLeaseRequest(
        lease_id=f"lease-{run_id}",
        request_scope="reference-fixtures",
        semantic_identity=f"{run_id}:canary",
        envelope_digest=digest,
        resources=(
            ResourceKind.TENANT,
            ResourceKind.WORKFLOW_RUN,
            ResourceKind.STAGE,
            ResourceKind.OPERATION_WORKER,
        ),
        requested_at=NOW,
        deadline=NOW + timedelta(seconds=5),
        ttl_seconds=5,
    )
    return ResourceLeaseRecord(
        request=request,
        status=ResourceLeaseStatus.ACQUIRED,
        acquired_at=NOW,
        expires_at=NOW + timedelta(seconds=5),
        canonical_digest=sha256_digest(request.model_dump(mode="json")),
    )


def test_sanitized_fixtures_and_typed_results_preserve_ambiguity() -> None:
    q = fixture("qualia_stage0_2_v1.json")
    d = fixture("dave_stage0_2_v1.json")
    assert isinstance(q, QualiaFixtureInput) and len(q.candidates) >= 3
    assert isinstance(d, DaveFixtureInput) and len(d.companies) >= 4

    q_result = classify_reference_fixture(q)
    d_result = classify_reference_fixture(d)
    assert isinstance(q_result, QualiaCatalogResult)
    assert {item.classification for item in q_result.products} == {
        "included",
        "excluded",
        "unknown_requires_review",
    }
    assert q_result.review_required_record_ids == ("q-product-focus",)
    assert isinstance(d_result, DaveOwnershipResult)
    by_id = {item.record_id: item for item in d_result.companies}
    assert by_id["d-company-upgrade-labs"].current_status == "affirmed"
    assert by_id["d-company-bulletproof"].current_status == "unknown_requires_review"
    assert by_id["d-company-forty-years"].current_status == "not_current"
    assert by_id["d-company-ambiguous-brand"].relationship_class == (
        CompanyRelationshipClass.CONFLICTING_OR_INSUFFICIENT_EVIDENCE
    )


def test_result_contract_refuses_founder_as_affirmed_owner() -> None:
    d = fixture("dave_stage0_2_v1.json")
    result = classify_reference_fixture(d)
    assert isinstance(result, DaveOwnershipResult)
    founder = next(item for item in result.companies if "bulletproof" in item.record_id)
    with pytest.raises(ValidationError, match="affirmative current-control"):
        founder.model_copy(update={"current_status": "affirmed"}).model_validate(
            {**founder.model_dump(mode="json"), "current_status": "affirmed"}
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("fixture_name", "family_id"),
    [
        ("qualia_stage0_2_v1.json", QUALIA_FAMILY_ID),
        ("dave_stage0_2_v1.json", DAVE_FAMILY_ID),
    ],
)
async def test_real_loader_compiler_executor_journal_and_reconstruction(
    fixture_name: str, family_id: str
) -> None:
    loaded = fixture(fixture_name)
    digest = sha256_digest(loaded.model_dump(mode="json"))
    prepared = await prepare_reference_implementation(
        control_plane(), family_id=family_id, fixture_digest=digest, now=NOW
    )
    assert prepared.blueprint_record.ref.logical_id.startswith(family_id)
    assert prepared.implementation_record.ref.logical_id.startswith(family_id)
    assert prepared.blueprint_record.ref.digest != prepared.implementation_record.ref.digest
    assert prepared.run_plan.graph_assembly == prepared.graph_assembly

    repository = InMemoryAtomicOperationJournalRepository()
    journal = OperationJournalService(repository)
    store = ImmutableManifestStore()
    first = await execute_reference_fixture(
        prepared=prepared,
        fixture=loaded,
        journal=journal,
        store=store,
        run_id=f"run-{family_id.rsplit('.', 1)[-1]}",
        now=NOW,
    )
    replay = await execute_reference_fixture(
        prepared=prepared,
        fixture=loaded,
        journal=journal,
        store=store,
        run_id=f"run-{family_id.rsplit('.', 1)[-1]}",
        now=NOW,
    )
    assert replay == first
    assert len(first.lineage) == len(prepared.graph_assembly.stage_execution_bindings)
    reconstructed = await reconstruct_typed_result_from_journal(journal, store, first)
    assert reconstructed.family_id == family_id
    assert all(item.runtime_attempt_id for item in first.lineage)
    assert all(item.semantic_operation_attempt_id for item in first.lineage)
    assert all(item.evidence_refs for item in first.lineage)

    changed = loaded.model_copy(update={"as_of": NOW + timedelta(seconds=1)})
    with pytest.raises(ValueError, match="fixture digest differs"):
        await execute_reference_fixture(
            prepared=prepared,
            fixture=changed,
            journal=journal,
            store=store,
            run_id=f"run-{family_id.rsplit('.', 1)[-1]}",
            now=NOW,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fixture_name",
    ["qualia_stage0_2_v1.json", "dave_stage0_2_v1.json"],
)
async def test_bounded_langgraph_canary_is_operation_only_and_fails_closed(
    fixture_name: str,
) -> None:
    loaded = fixture(fixture_name)
    prepared = await prepare_reference_implementation(
        control_plane(),
        family_id=loaded.family_id,
        fixture_digest=sha256_digest(loaded.model_dump(mode="json")),
        now=NOW,
    )
    binding = prepared.graph_assembly.stage_execution_bindings[-1]
    store = ImmutableManifestStore()
    executor = ReferenceLangGraphCanaryExecutor(loaded, store, binding.operation_assembly_digest)
    request = StageOperationRequest(
        request_scope="reference-fixtures",
        operation_id="bounded_reference_classification",
        semantic_attempt_id=f"canary:{loaded.family_id}:1",
        input_manifest_ref=f"fixture:{prepared.fixture_digest}",
        input_digest=prepared.fixture_digest,
    )
    exact = ExactStageExecutionBinding(
        binding_ref=binding.operation_assembly_ref.logical_id,
        operation_assembly_digest=binding.operation_assembly_digest,
    )
    outcome = await OperationExecutorConformanceHarness().assert_conforms(
        executor,
        request,
        exact,
        lease("canary", binding.resource_envelope_ref.digest),
        CancellationContext(
            cancellation_id="cancel-canary", cascade_policy_ref="policy:cooperative-cascade"
        ),
    )
    assert outcome.kind == "completed"
    assert executor.MAX_MODEL_CALLS == executor.MAX_TOOL_CALLS == executor.MAX_REQUESTS == 0
    assert "belllabs_reference_research" not in GRAPH_REGISTRY

    with pytest.raises(ValueError, match="digest drift"):
        await executor.execute(
            request,
            exact.model_copy(update={"operation_assembly_digest": "sha256:" + "f" * 64}),
            lease("drift", binding.resource_envelope_ref.digest),
            CancellationContext(
                cancellation_id="cancel-drift", cascade_policy_ref="policy:cooperative-cascade"
            ),
        )
    with pytest.raises(ValueError, match="cross-scope"):
        await executor.execute(
            request.model_copy(update={"request_scope": "another-tenant"}),
            exact,
            lease("tenant", binding.resource_envelope_ref.digest),
            CancellationContext(
                cancellation_id="cancel-tenant", cascade_policy_ref="policy:cooperative-cascade"
            ),
        )


def test_reference_transport_schemas_are_strict_and_secret_free() -> None:
    schemas = reference_research_contract_schemas()
    assert set(schemas) == {
        "qualia_fixture_input",
        "qualia_catalog_result",
        "dave_fixture_input",
        "dave_ownership_result",
    }
    serialized = str(schemas).lower()
    assert "api_key" not in serialized and "password" not in serialized
