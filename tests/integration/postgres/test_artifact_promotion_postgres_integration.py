from __future__ import annotations

import asyncio

import asyncpg
import pytest

from mission_control.adapters.postgres.scope import apply_scope
from mission_control.adapters.postgres.workspaces.artifact_repository import (
    PostgresArtifactDurableReferenceRepository,
)
from mission_control.application.artifacts.artifact_promotion import artifact_durable_reference
from mission_control.domain.execution.contracts import (
    ArtifactMetadataRevision,
    ArtifactPromotionState,
)
from mission_control.domain.policies.errors import IdempotencyConflict, RunControlNotFound
from tests.fixtures.mission_control_common_db import CommonDatabase, canonical_scope
from tests.integration.postgres.catalog_common import catalog_db as catalog_db
from tests.integration.postgres.catalog_common import insert_fixture_run
from tests.integration.postgres.catalog_common import runtime_pool as runtime_pool
from tests.unit.workspaces.test_artifact_promotion import CONTENT_DIGEST, NOW, OWNER

pytestmark = pytest.mark.common_db

RUN_KEY = "run:postgres-artifact-integration"


def admitted_revision(run_id: str, request_scope: str | None = None) -> ArtifactMetadataRevision:
    scope = request_scope or canonical_scope("tenant-1")
    return ArtifactMetadataRevision(
        promotion_id="promotion:postgres-artifact",
        artifact_id="artifact:postgres-integration",
        intent_key="intent:postgres-integration",
        promotion_identity="sha256:" + "a" * 64,
        revision=4,
        state=ArtifactPromotionState.ADMITTED,
        request_scope=scope,
        run_id=run_id,
        semantic_attempt_key=f"{run_id}:operation:research:attempt:1",
        producer_binding_id="binding:postgres-integration",
        namespace_id=f"workspace-namespace:{run_id}",
        workspace_id=f"workspace:{run_id}",
        output_slot="report",
        logical_path="/workspace/output/report.md",
        owner=OWNER,
        candidate_id="candidate:postgres-integration",
        content_digest=CONTENT_DIGEST,
        media_type="text/markdown",
        size_bytes=57,
        permission_ref="permission:integration@1",
        permission_outcome="allowed",
        output_contract_ref="operation:generic-research@1",
        object_ref="s3://test-artifacts/sha256/content",
        manifest_revision=3,
        durable_reference=artifact_durable_reference(
            scope, run_id, "artifact:postgres-integration"
        ),
        recorded_at=NOW,
    )


async def _counts(pool: asyncpg.Pool, scope: str) -> tuple[int, int]:
    async with pool.acquire() as connection, connection.transaction():
        await apply_scope(connection, scope)
        artifacts = await connection.fetchval("SELECT count(*) FROM mission_control.artifact")
        outbox = await connection.fetchval("SELECT count(*) FROM mission_control.outbox")
    return int(artifacts), int(outbox)


async def test_postgres_artifact_reference_and_event_commit_atomically(
    catalog_db: CommonDatabase, runtime_pool: asyncpg.Pool
) -> None:
    scope = catalog_db.scope("tenant-1")
    await insert_fixture_run(catalog_db, scope, RUN_KEY)
    artifact = admitted_revision(RUN_KEY, scope)

    async def fail_before_commit(boundary: str) -> None:
        if boundary == "artifact_admission":
            raise RuntimeError("injected artifact transaction failure")

    failing = PostgresArtifactDurableReferenceRepository(
        runtime_pool, before_commit=fail_before_commit
    )
    with pytest.raises(RuntimeError, match="injected"):
        await failing.admit(request_scope=scope, run_id=RUN_KEY, artifact=artifact)
    assert await failing.get(scope, artifact.artifact_id) is None
    assert await failing.pending_events(scope) == ()
    assert await _counts(runtime_pool, scope) == (0, 0)

    repository = PostgresArtifactDurableReferenceRepository(runtime_pool)
    first = await repository.admit(request_scope=scope, run_id=RUN_KEY, artifact=artifact)
    replayed = await repository.admit(request_scope=scope, run_id=RUN_KEY, artifact=artifact)
    events = await repository.pending_events(scope)

    assert first == replayed
    assert await repository.get(scope, artifact.artifact_id) == first
    assert len(events) == 1
    assert events[0]["event_type"] == "artifact.admitted"
    assert events[0]["payload"]["metadata_revision"] == artifact.revision


async def test_promotion_replay_is_idempotent_and_conflicts_fail_closed(
    catalog_db: CommonDatabase, runtime_pool: asyncpg.Pool
) -> None:
    scope = catalog_db.scope("tenant-1")
    await insert_fixture_run(catalog_db, scope, RUN_KEY)
    artifact = admitted_revision(RUN_KEY, scope)
    repository = PostgresArtifactDurableReferenceRepository(runtime_pool)
    results = await asyncio.gather(
        *(
            repository.admit(request_scope=scope, run_id=RUN_KEY, artifact=artifact)
            for _ in range(6)
        )
    )
    assert len(set(results)) == 1
    # Registered once: one canonical artifact row and one outbox row, no duplicates.
    assert await _counts(runtime_pool, scope) == (1, 1)
    async with runtime_pool.acquire() as connection, connection.transaction():
        await apply_scope(connection, scope)
        row = await connection.fetchrow(
            "SELECT a.custody_state, a.content_digest, a.byte_size, a.storage_kind, "
            "o.delivery_key, o.destination_kind, o.delivery_state, o.aggregate_key "
            "FROM mission_control.artifact a JOIN mission_control.outbox o "
            "ON o.tenant_id = a.tenant_id"
        )
        assert (row["custody_state"], row["content_digest"], row["byte_size"]) == (
            "registered",
            CONTENT_DIGEST,
            57,
        )
        assert (row["destination_kind"], row["delivery_state"]) == ("artifact_reference", "pending")
    # A changed digest under the same artifact identity conflicts; nothing new is written.
    with pytest.raises(IdempotencyConflict):
        await repository.admit(
            request_scope=scope,
            run_id=RUN_KEY,
            artifact=artifact.model_copy(update={"content_digest": "sha256:" + "e" * 64}),
        )
    assert await _counts(runtime_pool, scope) == (1, 1)
    # Scoped read denial: another tenant and the other installation see nothing.
    other_tenant = catalog_db.scope("tenant-2")
    assert await repository.get(other_tenant, artifact.artifact_id) is None
    assert await repository.pending_events(other_tenant) == ()
    with pytest.raises(RunControlNotFound):
        await repository.admit(
            request_scope=other_tenant,
            run_id=RUN_KEY,
            artifact=admitted_revision(RUN_KEY, other_tenant),
        )
    foreign = canonical_scope("tenant-1", "ai-engineer")
    assert await repository.get(foreign, artifact.artifact_id) is None
    # Artifact custody rows are not mutable/deletable by the runtime role.
    async with runtime_pool.acquire() as connection, connection.transaction():
        await apply_scope(connection, scope)
        with pytest.raises(asyncpg.InsufficientPrivilegeError):
            await connection.execute("DELETE FROM mission_control.artifact")
