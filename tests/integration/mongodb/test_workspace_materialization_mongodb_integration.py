from __future__ import annotations

from datetime import UTC, timedelta
from typing import Any
from uuid import uuid4

import pytest
from beanie import init_beanie
from pymongo import AsyncMongoClient

from app.application.workspaces.mongo_workspace_repository import (
    MongoWorkspaceManifestRepository,
)
from app.application.workspaces.workspace_materialization import (
    InMemoryDurableWorkspaceInputs,
    WorkspaceMaterializationService,
)
from app.domain.operation_execution.errors import WorkspaceSlotConflict
from app.domain.operation_execution.materialization import (
    legacy_workspace_reservation_token,
)
from app.domain.run_control.errors import IdempotencyConflict
from app.integrations.mongodb import BEANIE_MODELS
from app.models.workspace_materialization import (
    WorkspaceMaterializationManifestDocument,
    WorkspaceSlotReservationDocument,
)
from tests.unit.workspaces.test_goal_role_slot_ownership import (
    NAMESPACE,
    _goal_request,
    _rebound,
)
from tests.unit.workspaces.test_workspace_materialization import (
    INPUT,
    RecordingProvisioner,
    owner,
    request,
)


async def test_mongodb_manifest_revisions_and_slot_reservations_are_durable(
    test_mongodb_uri: str,
) -> None:
    database_name = f"workspace_test_{uuid4().hex[:16]}"
    client: AsyncMongoClient[dict[str, Any]] = AsyncMongoClient(
        test_mongodb_uri,
        serverSelectionTimeoutMS=5_000,
        tz_aware=True,
        tzinfo=UTC,
    )
    try:
        database = client[database_name]
        await database.command("ping")
        await init_beanie(database=database, document_models=BEANIE_MODELS)
        repository = MongoWorkspaceManifestRepository()
        materializer = WorkspaceMaterializationService(
            manifests=repository,
            provisioner=RecordingProvisioner(),
            durable_inputs=InMemoryDurableWorkspaceInputs({"artifact:input-1": INPUT}),
        )

        first = await materializer.materialize(request())
        replayed = await materializer.materialize(request())
        assert first == replayed

        conflicting = WorkspaceMaterializationService(
            manifests=repository,
            provisioner=RecordingProvisioner(),
            durable_inputs=InMemoryDurableWorkspaceInputs({"artifact:input-1": INPUT}),
        )
        with pytest.raises(WorkspaceSlotConflict):
            await conflicting.materialize(
                request(workspace_id="workspace-2", write_owner=owner("agent:other"))
            )
    finally:
        await client.drop_database(database_name)
        await client.close()


async def test_mongodb_shared_goal_workspace_gains_one_revision_per_iteration(
    test_mongodb_uri: str,
) -> None:
    """RRM-020 on the production repository: the next iteration of a `shared` GoalDirected
    workspace is one durable manifest revision and one role-root reservation; a retry, also by
    a fresh service over the same store, appends nothing; another slot set still conflicts."""

    database_name = f"workspace_test_{uuid4().hex[:16]}"
    client: AsyncMongoClient[dict[str, Any]] = AsyncMongoClient(
        test_mongodb_uri,
        serverSelectionTimeoutMS=5_000,
        tz_aware=True,
        tzinfo=UTC,
    )
    try:
        database = client[database_name]
        await database.command("ping")
        await init_beanie(database=database, document_models=BEANIE_MODELS)

        def service() -> WorkspaceMaterializationService:
            return WorkspaceMaterializationService(
                manifests=MongoWorkspaceManifestRepository(),
                provisioner=RecordingProvisioner(),
                durable_inputs=InMemoryDurableWorkspaceInputs({}),
            )

        workspace_id = "run/run-1/execution-epoch/1/goal/workspace/1"
        await service().materialize(_goal_request(workspace_id, 1, "executor"))
        second = await service().materialize(_goal_request(workspace_id, 2, "executor"))
        retried = await service().materialize(_goal_request(workspace_id, 2, "executor"))
        assert second.manifest_revision == retried.manifest_revision == 2
        assert second.materialization_manifest == retried.materialization_manifest
        with pytest.raises(IdempotencyConflict):
            await service().materialize(_goal_request(workspace_id, 3, "verifier"))
        with pytest.raises(WorkspaceSlotConflict):
            await service().materialize(_goal_request("workspace/other", 2, "executor"))

        documents = await WorkspaceMaterializationManifestDocument.find(
            WorkspaceMaterializationManifestDocument.workspace_id == workspace_id
        ).to_list()
        assert sorted(document.revision for document in documents) == [1, 2]
        reservations = await WorkspaceSlotReservationDocument.find(
            WorkspaceSlotReservationDocument.namespace_id == NAMESPACE
        ).to_list()
        assert {item.logical_path: (item.workspace_id, item.owner_id) for item in reservations} == {
            "/goal/1/executor": (workspace_id, "goal-iteration/1/executor"),
            "/goal/2/executor": (workspace_id, "goal-iteration/2/executor"),
        }
    finally:
        await client.drop_database(database_name)
        await client.close()


async def test_mongodb_rebinding_after_a_crash_between_reservation_and_manifest(
    test_mongodb_uri: str,
) -> None:
    """RRM-020 review on the production repository: the reservation token omits `created_at`,
    a reservation stored under the earlier token (with it) is still accepted, and a manifest
    append differing only in `created_at` returns the stored revision."""

    database_name = f"workspace_test_{uuid4().hex[:16]}"
    client: AsyncMongoClient[dict[str, Any]] = AsyncMongoClient(
        test_mongodb_uri,
        serverSelectionTimeoutMS=5_000,
        tz_aware=True,
        tzinfo=UTC,
    )
    try:
        database = client[database_name]
        await database.command("ping")
        await init_beanie(database=database, document_models=BEANIE_MODELS)
        repository = MongoWorkspaceManifestRepository()
        service = WorkspaceMaterializationService(
            manifests=repository,
            provisioner=RecordingProvisioner(),
            durable_inputs=InMemoryDurableWorkspaceInputs({}),
        )
        workspace_id = "run/run-1/execution-epoch/1/goal/workspace/1"
        await service.materialize(_goal_request(workspace_id, 1, "executor"))

        # Attempt A reserves iteration 2's root and dies; attempt B re-binds.
        attempt_a = _goal_request(workspace_id, 2, "executor")
        await repository.reserve_writable_slots(attempt_a)
        second = await service.materialize(_rebound(attempt_a))
        assert second.manifest_revision == 2
        assert (await service.materialize(attempt_a)).manifest_revision == 2

        # A reservation row written before the review (token with `created_at`) still counts.
        legacy_request = _goal_request(
            "run/run-1/execution-epoch/1/goal/workspace/legacy", 1, "verifier"
        )
        await WorkspaceSlotReservationDocument(
            namespace_id=NAMESPACE,
            workspace_id=legacy_request.workspace_id,
            logical_path="/goal/1/verifier",
            owner_id="goal-iteration/1/verifier",
            reservation_token=legacy_workspace_reservation_token(legacy_request),
            reserved_at=legacy_request.created_at,
        ).insert()
        assert (await service.materialize(legacy_request)).manifest_revision == 1
        with pytest.raises(WorkspaceSlotConflict):
            await service.materialize(_goal_request("workspace/other", 1, "verifier"))

        # An append racing with another `created_at` returns the stored revision.
        stored = await service.current_manifest(NAMESPACE, workspace_id)
        raced = stored.model_copy(update={"created_at": stored.created_at + timedelta(minutes=5)})
        assert await repository.append(raced) == stored

        documents = await WorkspaceMaterializationManifestDocument.find(
            WorkspaceMaterializationManifestDocument.workspace_id == workspace_id
        ).to_list()
        assert sorted(document.revision for document in documents) == [1, 2]
    finally:
        await client.drop_database(database_name)
        await client.close()
