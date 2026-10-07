from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

import asyncpg
import pytest
import pytest_asyncio

from mission_control.adapters.postgres.capability.external_candidates import (
    PostgresExternalCandidateInspectionRepository,
    PostgresExternalCandidateRepository,
)
from mission_control.adapters.postgres.capability.projection_events import (
    PostgresProjectionEventRepository,
)
from mission_control.adapters.postgres.control_plane.definition_repository import (
    PostgresDefinitionRepository,
)
from mission_control.application.capabilities.catalog_projection_events import (
    ProjectionEventFailure,
)
from mission_control.application.capabilities.external_candidate_inspection import (
    ExternalCandidateInspectionRequest,
    ExternalCandidateInspectionService,
    InspectionBounds,
    InspectionPrincipal,
    QuarantineInspectionObservations,
)
from mission_control.application.capabilities.external_candidate_repository import (
    ExternalCandidateNotFound,
)
from mission_control.application.capabilities.external_capability_discovery import (
    ExternalDiscoveryBatch,
    ExternalDiscoveryCandidate,
    ExternalDiscoveryEvidence,
    ExternalDiscoverySource,
)
from mission_control.domain.authoring.contracts import DefinitionKind, ExactDefinitionRef
from tests.integration.postgres.catalog_common import AI_ENGINEER_CATALOG, BIOTECH_CATALOG
from tests.integration.postgres.catalog_common import catalog_db as catalog_db
from tests.integration.postgres.catalog_common import runtime_pool as runtime_pool

pytestmark = pytest.mark.common_db


@pytest_asyncio.fixture
async def pool(runtime_pool: asyncpg.Pool) -> asyncpg.Pool:
    return runtime_pool


NOW = datetime(2026, 10, 3, tzinfo=UTC)
RAW_DIGEST = "sha256:" + "a" * 64


@pytest.fixture
def batch():
    evidence = ExternalDiscoveryEvidence(
        source=ExternalDiscoverySource.MCP_REGISTRY,
        source_version="v0.1",
        query="missing capability",
        retrieved_at=NOW,
        raw_response_digest=RAW_DIGEST,
        raw_response_size_bytes=100,
    )
    candidate = ExternalDiscoveryCandidate(
        candidate_id=f"candidate:sha256:{'d' * 64}",
        source=ExternalDiscoverySource.MCP_REGISTRY,
        upstream_identity="example/server",
        upstream_version="1.0.0",
        locator="https://registry.modelcontextprotocol.io/v0.1/servers/example",
        publisher="example",
        discovered_at=NOW,
        query="missing capability",
        raw_response_digest=RAW_DIGEST,
    )
    return ExternalDiscoveryBatch(
        source=ExternalDiscoverySource.MCP_REGISTRY,
        candidates=(candidate,),
        evidence=(evidence,),
    )


class Inspector:
    async def inspect(self, execution):
        return QuarantineInspectionObservations(
            manifest_valid=False,
            provenance_verified=False,
            network_requirement_hosts=execution.workspace.network_host_allowlist,
        )


@pytest.mark.asyncio
async def test_discovery_replay_scope_inspection_immutability(pool, batch):
    scope = BIOTECH_CATALOG
    candidates = PostgresExternalCandidateRepository(pool, catalog_scope=scope, clock=lambda: NOW)
    results = await asyncio.gather(*(candidates.record(batch) for _ in range(6)))
    assert all(item == results[0] for item in results)
    candidate = await candidates.get_candidate(batch.candidates[0].candidate_id)
    assert candidate.candidate.raw_response_ref.startswith("mission-control://")
    assert len(await candidates.list_candidate_records(candidate.candidate.candidate_id)) == 1
    assert (await candidates.get_evidence(candidate.evidence_id)).evidence == batch.evidence[0]
    other = PostgresExternalCandidateRepository(pool, catalog_scope=AI_ENGINEER_CATALOG)
    with pytest.raises(ExternalCandidateNotFound):
        await other.get_candidate_record(candidate.candidate_record_id)
    records = PostgresExternalCandidateInspectionRepository(pool, catalog_scope=scope)
    service = ExternalCandidateInspectionService(
        candidates=candidates,
        runner=Inspector(),
        records=records,
        bounds=InspectionBounds(),
        service_identity="inspector",
        clock=lambda: NOW,
        id_factory=lambda: "6" * 32,
    )
    report = await service.inspect(
        InspectionPrincipal(
            actor_id="planner", tenant_scope=scope, roles=frozenset({"coordinator_planner"})
        ),
        ExternalCandidateInspectionRequest(
            candidate_id=candidate.candidate.candidate_id, correlation_id="test", requested_at=NOW
        ),
    )
    assert await records.get_report(report.inspection_id) == report
    assert await records.append_report(report) == report
    from mission_control.domain.policies.errors import IdempotencyConflict

    with pytest.raises(IdempotencyConflict):
        await records.append_report(report.model_copy(update={"inspection_id": "inspection:other"}))


@pytest.mark.asyncio
async def test_projection_claim_reclaim_fencing_poison_scope(pool):
    scope = BIOTECH_CATALOG
    catalog = PostgresDefinitionRepository(pool, catalog_scope=scope)
    ref = ExactDefinitionRef(
        kind=DefinitionKind.SKILL, logical_id="skill.test", revision=1, digest=RAW_DIGEST
    )
    async with catalog._transaction(write=True) as conn:
        await catalog._event(conn, ref, "upsert", NOW)
    events = PostgresProjectionEventRepository(pool, catalog_scope=scope)
    claims = await asyncio.gather(
        *(
            events.claim_batch(
                owner=f"worker-{n}", now=NOW, lease_duration=timedelta(seconds=2), limit=1
            )
            for n in range(5)
        )
    )
    assert sum(map(len, claims)) == 1
    initial = next(item[0] for item in claims if item)
    other = PostgresProjectionEventRepository(pool, catalog_scope=AI_ENGINEER_CATALOG)
    assert not await other.claim_batch(
        owner="other", now=NOW, lease_duration=timedelta(seconds=1), limit=1
    )
    assert not await other.complete(initial, owner=initial.lease_owner, completed_at=NOW)
    reclaimed = (
        await events.claim_batch(
            owner="reclaimer",
            now=NOW + timedelta(seconds=3),
            lease_duration=timedelta(seconds=5),
            limit=1,
        )
    )[0]
    assert reclaimed.attempt_count == 2
    assert not await events.complete(
        initial, owner=initial.lease_owner, completed_at=NOW + timedelta(seconds=3)
    )
    poisoned = await events.fail(
        reclaimed,
        owner="reclaimer",
        failed_at=NOW + timedelta(seconds=4),
        failure=ProjectionEventFailure(error_code="INVALID_CATALOG", retryable=False),
        max_attempts=3,
        base_backoff=timedelta(seconds=1),
        max_backoff=timedelta(seconds=10),
    )
    assert poisoned.state.value == "poison"
    assert (
        await events.complete_for_ref(
            ref, tenant_scope=scope, completed_at=NOW + timedelta(seconds=5)
        )
        == 1
    )
    assert not await events.claim_batch(
        owner="done", now=NOW + timedelta(days=1), lease_duration=timedelta(seconds=1), limit=1
    )


@pytest.mark.asyncio
async def test_alias_listing_is_scoped_and_tracks_current_exact_target(pool):
    from mission_control.domain.authoring.contracts import AliasRef
    from tests.unit.control_plane.test_agentic_asset_definitions import skill_definition

    scope = BIOTECH_CATALOG
    catalog = PostgresDefinitionRepository(pool, catalog_scope=scope)
    published = await catalog.publish(
        skill_definition(), actor_id="publisher", published_at=NOW, expected_head_revision=0
    )
    alias = AliasRef(kind=published.ref.kind, logical_id=published.ref.logical_id, alias="stable")
    binding = await catalog.move_alias(alias, published.ref, "publisher", NOW)
    assert await catalog.list_alias_bindings() == (binding,)
    assert (
        await PostgresDefinitionRepository(
            pool, catalog_scope=AI_ENGINEER_CATALOG
        ).list_alias_bindings()
        == ()
    )


@pytest.mark.asyncio
async def test_discovery_custody_is_not_admission_and_is_immutable(pool, batch):
    from mission_control.adapters.postgres.scope import apply_catalog_scope_string

    candidates = PostgresExternalCandidateRepository(
        pool, catalog_scope=BIOTECH_CATALOG, clock=lambda: NOW
    )
    await candidates.record(batch)
    async with pool.acquire() as connection, connection.transaction():
        await apply_catalog_scope_string(connection, BIOTECH_CATALOG)
        assert (
            await connection.fetchval(
                "SELECT count(*) FROM mission_control.capability_discovery_record"
            )
            == 2
        )
        # Discovery never admits, installs or grants anything.
        for table in ("asset_version", "asset_decision", "capability_bundle_admission"):
            assert await connection.fetchval(f"SELECT count(*) FROM mission_control.{table}") == 0
    for sql in (
        "UPDATE mission_control.capability_discovery_record SET record_key='x'",
        "DELETE FROM mission_control.capability_discovery_record",
    ):
        async with pool.acquire() as connection, connection.transaction():
            await apply_catalog_scope_string(connection, BIOTECH_CATALOG)
            with pytest.raises(asyncpg.InsufficientPrivilegeError):
                await connection.execute(sql)
    with pytest.raises(ValueError):
        PostgresExternalCandidateRepository(pool, catalog_scope="not-a-catalog-scope")


@pytest.mark.asyncio
async def test_outbox_worker_role_claims_and_poisons_without_catalog_write(catalog_db, pool):
    from mission_control.adapters.postgres.scope import apply_catalog_scope_string

    catalog = PostgresDefinitionRepository(pool, catalog_scope=BIOTECH_CATALOG)
    ref = ExactDefinitionRef(
        kind=DefinitionKind.SKILL, logical_id="skill.worker", revision=1, digest=RAW_DIGEST
    )
    async with catalog._transaction(write=True) as conn:
        await catalog._event(conn, ref, "upsert", NOW)
    worker_pool = await catalog_db.pool("mission_control_outbox_worker")
    try:
        events = PostgresProjectionEventRepository(worker_pool, catalog_scope=BIOTECH_CATALOG)
        (claimed,) = await events.claim_batch(
            owner="worker", now=NOW, lease_duration=timedelta(seconds=5), limit=5
        )
        failed = await events.fail(
            claimed,
            owner="worker",
            failed_at=NOW + timedelta(seconds=1),
            failure=ProjectionEventFailure(error_code="INVALID_CATALOG", retryable=False),
            max_attempts=3,
            base_backoff=timedelta(seconds=1),
            max_backoff=timedelta(seconds=10),
        )
        assert failed is not None and failed.state.value == "poison"
        async with worker_pool.acquire() as connection, connection.transaction():
            await apply_catalog_scope_string(connection, BIOTECH_CATALOG)
            assert (
                await connection.fetchval(
                    "SELECT count(*) FROM mission_control.catalog_projection_alert"
                )
                == 1
            )
            with pytest.raises(asyncpg.InsufficientPrivilegeError):
                await connection.execute(
                    "INSERT INTO mission_control.catalog_record (installation_id, "
                    "application_id, catalog_record_id, contract, record_key, payload, "
                    "payload_digest, created_at, created_by_actor_ref) SELECT installation_id, "
                    "application_id, gen_random_uuid(), contract, 'x', payload, payload_digest, "
                    "now(), 'x' FROM mission_control.catalog_record LIMIT 1"
                )
    finally:
        await worker_pool.close()
