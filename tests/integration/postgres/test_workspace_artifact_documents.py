from __future__ import annotations

import asyncio
import json
from datetime import timedelta

import asyncpg
import pytest
import pytest_asyncio

from mission_control.adapters.postgres.scope import apply_scope
from mission_control.adapters.postgres.workspaces.artifact_metadata_repository import (
    PostgresArtifactMetadataRepository,
)
from mission_control.adapters.postgres.workspaces.workspace_manifest_repository import (
    PostgresWorkspaceManifestRepository,
)
from mission_control.application.artifacts.workspace_materialization import (
    InMemoryDurableWorkspaceInputs,
    WorkspaceMaterializationService,
)
from mission_control.domain.execution.contracts import ArtifactPromotionState
from mission_control.domain.execution.errors import WorkspaceSlotConflict
from mission_control.domain.policies.errors import IdempotencyConflict
from tests.fixtures.mission_control_common_db import CommonDatabase
from tests.integration.postgres.catalog_common import catalog_db as catalog_db
from tests.integration.postgres.catalog_common import runtime_pool as runtime_pool
from tests.integration.postgres.test_artifact_promotion_postgres_integration import (
    admitted_revision,
)
from tests.unit.workspaces.test_goal_role_slot_ownership import _goal_request, _rebound
from tests.unit.workspaces.test_workspace_materialization import RecordingProvisioner

pytestmark = pytest.mark.common_db


@pytest_asyncio.fixture
async def scoped_pool(runtime_pool: asyncpg.Pool) -> asyncpg.Pool:
    return runtime_pool


def materializer(
    repository: PostgresWorkspaceManifestRepository,
) -> WorkspaceMaterializationService:
    return WorkspaceMaterializationService(
        manifests=repository,
        provisioner=RecordingProvisioner(),
        durable_inputs=InMemoryDurableWorkspaceInputs({}),
    )


async def test_workspace_rebind_lineage_conflicts_and_scope(
    catalog_db: CommonDatabase, scoped_pool: asyncpg.Pool
) -> None:
    scope = catalog_db.scope("tenant-1")
    repo = PostgresWorkspaceManifestRepository(scoped_pool, request_scope=scope)
    service = materializer(repo)
    workspace = "workspace/one"
    first_request = _goal_request(workspace, 1, "executor")
    first = await service.materialize(first_request)
    assert await service.materialize(_rebound(first_request)) == first
    second_request = _goal_request(workspace, 2, "executor")
    await repo.reserve_writable_slots(second_request)  # crash before manifest, then rebind
    second = await service.materialize(_rebound(second_request))
    assert second.manifest_revision == 2
    assert (await service.materialize(second_request)).manifest_revision == 2
    with pytest.raises(WorkspaceSlotConflict):
        await service.materialize(_goal_request("workspace/other", 2, "executor"))
    with pytest.raises(IdempotencyConflict):
        await service.materialize(_goal_request(workspace, 3, "verifier"))
    stored = await repo.get_current(first_request.namespace_id, workspace)
    assert stored is not None
    rebound = stored.model_copy(update={"created_at": stored.created_at + timedelta(minutes=5)})
    assert await repo.append(rebound) == stored
    other = PostgresWorkspaceManifestRepository(
        scoped_pool, request_scope=catalog_db.scope("tenant-2")
    )
    assert await other.get_current(first_request.namespace_id, workspace) is None
    assert (await materializer(other).materialize(first_request)).manifest_revision == 1
    async with scoped_pool.acquire() as connection, connection.transaction():
        await apply_scope(connection, scope)
        assert (
            await connection.fetchval("SELECT count(*) FROM mission_control.workspace_manifest")
            == 2
        )
        with pytest.raises(asyncpg.InsufficientPrivilegeError):
            await connection.execute("DELETE FROM mission_control.workspace_manifest")


async def test_concurrent_workspace_claims_have_one_owner(
    catalog_db: CommonDatabase, scoped_pool: asyncpg.Pool
) -> None:
    repo = PostgresWorkspaceManifestRepository(scoped_pool, request_scope=catalog_db.scope())
    results = await asyncio.gather(
        repo.reserve_writable_slots(_goal_request("first", 1, "executor")),
        repo.reserve_writable_slots(_goal_request("second", 1, "executor")),
        return_exceptions=True,
    )
    assert sum(result is None for result in results) == 1
    assert sum(isinstance(result, WorkspaceSlotConflict) for result in results) == 1


async def test_slot_batch_rolls_back_when_later_slot_conflicts(
    catalog_db: CommonDatabase, scoped_pool: asyncpg.Pool
) -> None:
    repo = PostgresWorkspaceManifestRepository(scoped_pool, request_scope=catalog_db.scope())
    occupied = _goal_request("occupied", 2, "executor")
    await repo.reserve_writable_slots(occupied)
    first = _goal_request("contender", 1, "executor")
    second = _goal_request("contender", 2, "executor")
    batch = first.model_copy(update={"slots": first.slots + second.slots})
    with pytest.raises(WorkspaceSlotConflict):
        await repo.reserve_writable_slots(batch)
    # A different workspace succeeds only if the earlier insert rolled back atomically.
    await repo.reserve_writable_slots(_goal_request("new-owner", 1, "executor"))


async def test_artifact_revision_replay_reconciliation_and_scope(
    catalog_db: CommonDatabase, scoped_pool: asyncpg.Pool
) -> None:
    scope = catalog_db.scope("tenant-1")
    repo = PostgresArtifactMetadataRepository(scoped_pool, request_scope=scope)
    candidate = admitted_revision("run:workspace-test").model_copy(
        update={
            "request_scope": scope,
            "revision": 1,
            "state": ArtifactPromotionState.CANDIDATE,
            "object_ref": None,
            "manifest_revision": None,
        }
    )
    assert await asyncio.gather(repo.append(candidate), repo.append(candidate)) == [
        candidate,
        candidate,
    ]
    conflicting = candidate.model_copy(update={"reason": "conflicting"})
    with pytest.raises(IdempotencyConflict):
        await repo.append(conflicting)
    with pytest.raises(IdempotencyConflict):
        await repo.append(candidate.model_copy(update={"revision": 3}))
    with pytest.raises(IdempotencyConflict):
        await repo.append(candidate.model_copy(update={"intent_key": "other", "revision": 2}))
    reconciliation = candidate.model_copy(
        update={
            "revision": 2,
            "state": ArtifactPromotionState.RECONCILIATION_REQUIRED,
        }
    )
    await repo.append(reconciliation)
    assert await repo.reconciliation_required() == (reconciliation,)
    rejected = reconciliation.model_copy(
        update={"revision": 3, "state": ArtifactPromotionState.REJECTED}
    )
    await repo.append(rejected)
    assert await repo.reconciliation_required() == ()
    assert await repo.rejected() == (rejected,)
    assert await repo.get_by_intent(candidate.intent_key) == rejected
    assert await repo.get_by_artifact(candidate.artifact_id) == rejected
    other = PostgresArtifactMetadataRepository(
        scoped_pool, request_scope=catalog_db.scope("tenant-2")
    )
    assert await other.get_by_artifact(candidate.artifact_id) is None
    assert await other.rejected() == ()
    with pytest.raises(ValueError, match="scope"):
        await other.append(candidate)


async def test_workspace_sql_rejects_missing_null_and_mismatched_identity(
    catalog_db: CommonDatabase, scoped_pool: asyncpg.Pool
) -> None:
    scope = catalog_db.scope("tenant-1")
    valid = {
        "namespace_id": "namespace",
        "workspace_id": "workspace",
        "manifest_id": "manifest",
        "revision": 1,
        "manifest_digest": "sha256:" + "a" * 64,
        "prior_manifest_digest": None,
    }
    malformed = [{}]
    for key in valid:
        missing = dict(valid)
        del missing[key]
        malformed.append(missing)
        if key != "prior_manifest_digest":  # Initial manifests correctly have JSON null here.
            malformed.append({**valid, key: None})
    malformed.extend(
        (
            {**valid, "prior_manifest_digest": "sha256:" + "b" * 64},
            {**valid, "revision": "1"},
        )
    )
    async with scoped_pool.acquire() as connection:
        for payload in malformed:
            async with connection.transaction():
                await apply_scope(connection, scope)
                with pytest.raises(asyncpg.CheckViolationError):
                    async with connection.transaction():
                        await connection.execute(
                            """INSERT INTO mission_control.workspace_manifest
                               (installation_id, application_id, tenant_id,
                                workspace_manifest_id, namespace_key, workspace_key, revision,
                                manifest_key, manifest_digest, prior_manifest_digest, payload,
                                manifest_created_at, created_at, created_by_actor_ref)
                               SELECT mission_control.ctx_installation_id(),
                                      mission_control.ctx_application_id(),
                                      mission_control.ctx_tenant_id(), gen_random_uuid(),
                                      'namespace','workspace',1,'manifest',$1,NULL,$2::jsonb,
                                      now(),now(),'test'""",
                            valid["manifest_digest"],
                            json.dumps(payload),
                        )


async def test_artifact_sql_rejects_missing_null_and_mistyped_identity(
    catalog_db: CommonDatabase, scoped_pool: asyncpg.Pool
) -> None:
    scope = catalog_db.scope("tenant-1")
    valid = {
        "request_scope": scope,
        "artifact_id": "artifact",
        "intent_key": "intent",
        "promotion_id": "promotion",
        "revision": 1,
        "state": "candidate",
    }
    malformed = [{}]
    for key in valid:
        missing = dict(valid)
        del missing[key]
        malformed.extend((missing, {**valid, key: None}))
    malformed.append({**valid, "revision": "1"})
    async with scoped_pool.acquire() as connection:
        for payload in malformed:
            async with connection.transaction():
                await apply_scope(connection, scope)
                with pytest.raises(asyncpg.CheckViolationError):
                    async with connection.transaction():
                        await connection.execute(
                            """INSERT INTO mission_control.artifact_metadata_revision
                               (installation_id, application_id, tenant_id,
                                artifact_metadata_revision_id, request_scope, artifact_key,
                                intent_key, promotion_key, revision, state, payload,
                                recorded_at, created_at, created_by_actor_ref)
                               SELECT mission_control.ctx_installation_id(),
                                      mission_control.ctx_application_id(),
                                      mission_control.ctx_tenant_id(), gen_random_uuid(), $1,
                                      'artifact','intent','promotion',1,'candidate',$2::jsonb,
                                      now(),now(),'test'""",
                            scope,
                            json.dumps(payload),
                        )
