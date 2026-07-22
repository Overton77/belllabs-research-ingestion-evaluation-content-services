from __future__ import annotations

from datetime import UTC
from typing import Any
from uuid import uuid4

from beanie import init_beanie
from pymongo import AsyncMongoClient

from app.application.mongo_snapshot_repository import MongoSandboxSnapshotRepository
from app.application.sandbox_snapshots import (
    InMemorySnapshotPayloadStore,
    SandboxSnapshotService,
)
from app.integrations.mongodb import BEANIE_MODELS
from tests.test_sandbox_snapshots import (
    Authority,
    Resources,
    Sandbox,
    clone_request,
    create_request,
)


async def test_mongodb_persists_immutable_snapshot_and_clone_lineage(
    test_mongodb_uri: str,
) -> None:
    database_name = f"snapshot_test_{uuid4().hex[:16]}"
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
        repository = MongoSandboxSnapshotRepository()
        service = SandboxSnapshotService(
            snapshots=repository,
            payloads=InMemorySnapshotPayloadStore(),
            sandbox=Sandbox(),
            authority=Authority(),
            resources=Resources(),
        )

        snapshot = await service.create(create_request())
        replayed = await service.create(create_request())
        clone = await service.clone_restore(
            clone_request("workspace-mongo-clone", "clone-mongo")
        )

        assert replayed == snapshot
        assert await repository.get_snapshot(snapshot.snapshot_id) == snapshot
        assert (await repository.get_clone(clone.clone_id)).snapshot_id == snapshot.snapshot_id  # type: ignore[union-attr]
    finally:
        await client.drop_database(database_name)
        await client.close()
