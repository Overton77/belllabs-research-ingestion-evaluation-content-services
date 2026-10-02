"""RRM-018: an unchanged Goal Revision is persisted idempotently across iterations.

`GoalDirectedOperationPreparationService.prepare` persists the active Goal Revision before
each executor operation, with `recorded_at = decided_at` of that iteration. Found by RRM-016's
real-Temporal demonstration (two iterations on MongoDB documents): the repository compared the
whole stored document, `recorded_at` included, so the second iteration of an unchanged revision
raised `IdempotencyConflict`. The observation time is not part of the immutable identity; any
other difference under the same identity still conflicts (REQ-BP-GD-002).
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from dataclasses import replace
from datetime import UTC, timedelta
from typing import Any
from uuid import uuid4

import pytest
from beanie import init_beanie
from pymongo import AsyncMongoClient

from app.application.orchestration.mongo_goal_directed_repository import (
    MongoGoalDirectedDocumentRepository,
)
from app.domain.run_control.errors import IdempotencyConflict
from app.integrations.mongodb import BEANIE_MODELS
from app.models.goal_directed import GoalRevisionDocument
from tests.fixtures.goal_directed_journaled import SCOPE, goal_revision
from tests.unit.run_control.test_run_control import NOW

RUN = "run-rrm-018"


@pytest.fixture
async def repository(test_mongodb_uri: str) -> AsyncIterator[MongoGoalDirectedDocumentRepository]:
    client: AsyncMongoClient[dict[str, Any]] = AsyncMongoClient(
        test_mongodb_uri, serverSelectionTimeoutMS=5_000, tz_aware=True, tzinfo=UTC
    )
    database = client[f"rrm018_{uuid4().hex[:16]}"]
    try:
        await init_beanie(database=database, document_models=BEANIE_MODELS)
        yield MongoGoalDirectedDocumentRepository()
    finally:
        await client.drop_database(database.name)
        await client.close()


async def test_unchanged_goal_revision_persists_again_at_the_next_iteration(
    repository: MongoGoalDirectedDocumentRepository,
) -> None:
    revision = goal_revision(RUN)
    first = await repository.persist_revision(SCOPE, RUN, revision, NOW)
    # Iteration 2's executor preparation persists the same revision again.
    second = await repository.persist_revision(SCOPE, RUN, revision, NOW + timedelta(minutes=1))
    assert second == first
    stored = await GoalRevisionDocument.find(
        {"request_scope": SCOPE, "run_id": RUN, "goal_revision_id": revision.revision_id}
    ).to_list()
    # One immutable document; the first observation time is kept.
    assert len(stored) == 1
    assert stored[0].recorded_at == NOW


async def test_changed_goal_revision_under_the_same_identity_still_conflicts(
    repository: MongoGoalDirectedDocumentRepository,
) -> None:
    revision = goal_revision(RUN)
    await repository.persist_revision(SCOPE, RUN, revision, NOW)
    changed = replace(revision, tactical_changes=("tactic:other",))
    with pytest.raises(IdempotencyConflict, match="immutable document identity conflict"):
        await repository.persist_revision(SCOPE, RUN, changed, NOW)
    with pytest.raises(IdempotencyConflict, match="immutable document identity conflict"):
        await repository.persist_revision(SCOPE, RUN, changed, NOW + timedelta(minutes=1))
