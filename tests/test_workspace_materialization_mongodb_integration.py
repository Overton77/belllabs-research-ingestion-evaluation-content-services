from __future__ import annotations

from datetime import UTC
from typing import Any
from uuid import uuid4

import pytest
from beanie import init_beanie
from pymongo import AsyncMongoClient

from app.application.mongo_workspace_repository import (
    MongoWorkspaceManifestRepository,
)
from app.application.workspace_materialization import (
    InMemoryDurableWorkspaceInputs,
    WorkspaceMaterializationService,
)
from app.domain.operation_execution.errors import WorkspaceSlotConflict
from app.integrations.mongodb import BEANIE_MODELS
from tests.test_workspace_materialization import (
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
