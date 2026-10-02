"""RRM-018 reproduction: the GoalDirected executor re-persists its Goal Revision every iteration.

`GoalDirectedOperationPreparationService.prepare` persists the active Goal Revision before
each executor operation, with `recorded_at = decided_at` of that iteration.
`MongoGoalDirectedDocumentRepository` compares the whole stored document, `recorded_at`
included, on a duplicate key, so the second iteration of an unchanged revision raises
`IdempotencyConflict`. Found by RRM-016's real-Temporal demonstration (two iterations on
MongoDB documents); the fix belongs to RRM-018.
"""

from __future__ import annotations

from datetime import UTC, timedelta
from uuid import uuid4

import pytest
from beanie import init_beanie
from pymongo import AsyncMongoClient

from app.application.orchestration.mongo_goal_directed_repository import (
    MongoGoalDirectedDocumentRepository,
)
from app.domain.run_control.errors import IdempotencyConflict
from app.integrations.mongodb import BEANIE_MODELS
from tests.fixtures.goal_directed_journaled import SCOPE, goal_revision
from tests.unit.run_control.test_run_control import NOW


@pytest.mark.xfail(
    strict=True,
    raises=IdempotencyConflict,
    reason="RRM-018: an unchanged Goal Revision re-persisted at the next iteration conflicts",
)
async def test_unchanged_goal_revision_persists_again_at_the_next_iteration(
    test_mongodb_uri: str,
) -> None:
    client: AsyncMongoClient[dict[str, object]] = AsyncMongoClient(
        test_mongodb_uri, serverSelectionTimeoutMS=5_000, tz_aware=True, tzinfo=UTC
    )
    database = client[f"rrm018_{uuid4().hex[:16]}"]
    try:
        await init_beanie(database=database, document_models=BEANIE_MODELS)
        repository = MongoGoalDirectedDocumentRepository()
        revision = goal_revision("run-rrm-018")
        first = await repository.persist_revision(SCOPE, "run-rrm-018", revision, NOW)
        # Iteration 2's executor preparation persists the same revision again.
        second = await repository.persist_revision(
            SCOPE, "run-rrm-018", revision, NOW + timedelta(minutes=1)
        )
        assert second == first
    finally:
        await client.drop_database(database.name)
        await client.close()
