from __future__ import annotations

from datetime import UTC
from typing import Any
from uuid import uuid4

from beanie import init_beanie
from pymongo import AsyncMongoClient

from app.application.mongo_artifact_repository import (
    MongoArtifactMetadataRepository,
)
from app.domain.operation_execution.contracts import ArtifactPromotionState
from app.integrations.mongodb import BEANIE_MODELS
from tests.test_artifact_promotion_postgres_integration import admitted_revision


async def test_mongodb_artifact_revisions_are_immutable_and_reconcilable(
    test_mongodb_uri: str,
) -> None:
    database_name = f"artifact_test_{uuid4().hex[:16]}"
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
        repository = MongoArtifactMetadataRepository()
        admitted = admitted_revision("run:mongodb-artifact")
        candidate = admitted.model_copy(
            update={
                "revision": 1,
                "state": ArtifactPromotionState.CANDIDATE,
                "object_ref": None,
                "manifest_revision": None,
            }
        )
        staged = candidate.model_copy(
            update={
                "revision": 2,
                "state": ArtifactPromotionState.PAYLOAD_STAGED,
                "object_ref": admitted.object_ref,
            }
        )
        reconciliation = staged.model_copy(
            update={
                "revision": 3,
                "state": ArtifactPromotionState.RECONCILIATION_REQUIRED,
                "reason": "injected",
            }
        )

        assert await repository.append(candidate) == candidate
        assert await repository.append(candidate) == candidate
        assert await repository.append(staged) == staged
        assert await repository.append(reconciliation) == reconciliation
        assert await repository.get_by_artifact(candidate.artifact_id) == reconciliation
        assert await repository.reconciliation_required() == (reconciliation,)
    finally:
        await client.drop_database(database_name)
        await client.close()
