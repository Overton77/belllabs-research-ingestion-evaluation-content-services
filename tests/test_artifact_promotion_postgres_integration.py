from __future__ import annotations

import asyncpg
import pytest

from app.application.postgres_artifact_repository import (
    PostgresArtifactDurableReferenceRepository,
)
from app.application.postgres_run_control_repository import (
    PostgresRunControlRepository,
)
from app.domain.operation_execution.contracts import (
    ArtifactMetadataRevision,
    ArtifactPromotionState,
)
from app.integrations.postgres import apply_application_migrations
from tests.test_artifact_promotion import CONTENT_DIGEST, NOW, OWNER
from tests.test_run_control import request as run_request
from tests.test_run_control import service as run_control_service


def admitted_revision(run_id: str) -> ArtifactMetadataRevision:
    return ArtifactMetadataRevision(
        promotion_id="promotion:postgres-artifact",
        artifact_id="artifact:postgres-integration",
        intent_key="intent:postgres-integration",
        promotion_identity="sha256:" + "a" * 64,
        revision=4,
        state=ArtifactPromotionState.ADMITTED,
        request_scope="tenant-1",
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
        durable_reference=(f"artifact://tenant-1/{run_id}/artifact:postgres-integration"),
        recorded_at=NOW,
    )


async def test_postgres_artifact_reference_and_event_commit_atomically(
    test_application_postgres_dsn: str,
) -> None:
    pool = await asyncpg.create_pool(dsn=test_application_postgres_dsn, min_size=1, max_size=4)
    try:
        async with pool.acquire() as connection:
            await connection.execute("DROP SCHEMA IF EXISTS belllabs_control CASCADE")
        await apply_application_migrations(pool)
        run_service, _ = run_control_service(PostgresRunControlRepository(pool))  # type: ignore[arg-type]
        decision = await run_service.admit(run_request())
        assert decision.run_id is not None
        artifact = admitted_revision(decision.run_id)

        async def fail_before_commit(boundary: str) -> None:
            if boundary == "artifact_admission":
                raise RuntimeError("injected artifact transaction failure")

        failing = PostgresArtifactDurableReferenceRepository(pool, before_commit=fail_before_commit)
        with pytest.raises(RuntimeError, match="injected"):
            await failing.admit(
                request_scope="tenant-1",
                run_id=decision.run_id,
                artifact=artifact,
            )
        assert await failing.get("tenant-1", artifact.artifact_id) is None
        assert await failing.pending_events("tenant-1") == ()

        repository = PostgresArtifactDurableReferenceRepository(pool)
        first = await repository.admit(
            request_scope="tenant-1",
            run_id=decision.run_id,
            artifact=artifact,
        )
        replayed = await repository.admit(
            request_scope="tenant-1",
            run_id=decision.run_id,
            artifact=artifact,
        )
        events = await repository.pending_events("tenant-1")

        assert first == replayed
        assert await repository.get("tenant-1", artifact.artifact_id) == first
        assert len(events) == 1
        assert events[0]["event_type"] == "artifact.admitted"
        assert events[0]["payload"]["metadata_revision"] == artifact.revision
    finally:
        await pool.close()
