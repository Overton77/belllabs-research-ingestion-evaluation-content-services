from __future__ import annotations

import asyncio
import os
from datetime import UTC, datetime, timedelta
from uuid import uuid4

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
from mission_control.adapters.postgres.connections import apply_application_migrations
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

NOW = datetime(2026, 10, 3, tzinfo=UTC)
RAW_DIGEST = "sha256:" + "a" * 64


@pytest_asyncio.fixture
async def pool():
    dsn = os.environ.get("TEST_APPLICATION_POSTGRES_DSN")
    if not dsn:
        pytest.fail("Explicit disposable TEST_APPLICATION_POSTGRES_DSN required")
    owner = await asyncpg.create_pool(dsn, min_size=1, max_size=2)
    try:
        await apply_application_migrations(owner)
    finally:
        await owner.close()

    async def role(conn):
        await conn.execute("SET ROLE belllabs_control_runtime")

    runtime = await asyncpg.create_pool(dsn, min_size=1, max_size=8, setup=role)
    try:
        yield runtime
    finally:
        await runtime.close()


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
    batch = ExternalDiscoveryBatch(
        source=ExternalDiscoverySource.MCP_REGISTRY,
        candidates=(candidate,),
        evidence=(evidence,),
    )

    return batch


class Inspector:
    async def inspect(self, execution):
        return QuarantineInspectionObservations(
            manifest_valid=False,
            provenance_verified=False,
            network_requirement_hosts=execution.workspace.network_host_allowlist,
        )


@pytest.mark.asyncio
async def test_discovery_replay_scope_inspection_immutability(pool, batch):
    scope = uuid4().hex
    candidates = PostgresExternalCandidateRepository(pool, catalog_scope=scope, clock=lambda: NOW)
    results = await asyncio.gather(*(candidates.record(batch) for _ in range(6)))
    assert all(item == results[0] for item in results)
    candidate = await candidates.get_candidate(batch.candidates[0].candidate_id)
    assert candidate.candidate.raw_response_ref.startswith("mission-control://")
    assert len(await candidates.list_candidate_records(candidate.candidate.candidate_id)) == 1
    assert (await candidates.get_evidence(candidate.evidence_id)).evidence == batch.evidence[0]
    other = PostgresExternalCandidateRepository(pool, catalog_scope=uuid4().hex)
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
    scope = uuid4().hex
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
    other = PostgresProjectionEventRepository(pool, catalog_scope=uuid4().hex)
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

    scope = uuid4().hex
    catalog = PostgresDefinitionRepository(pool, catalog_scope=scope)
    published = await catalog.publish(
        skill_definition(), actor_id="publisher", published_at=NOW, expected_head_revision=0
    )
    alias = AliasRef(kind=published.ref.kind, logical_id=published.ref.logical_id, alias="stable")
    binding = await catalog.move_alias(alias, published.ref, "publisher", NOW)
    assert await catalog.list_alias_bindings() == (binding,)
    assert (
        await PostgresDefinitionRepository(pool, catalog_scope=uuid4().hex).list_alias_bindings()
        == ()
    )
