"""In-memory checkpoint lineage authority: the shared repository contract (REQ-CP-DA-017)."""

from __future__ import annotations

import pytest

from app.application.operations.checkpoint_lineage import InMemoryCheckpointLineageRepository
from tests.fixtures.checkpoint_lineage import assert_checkpoint_lineage_repository_contract


@pytest.mark.asyncio
async def test_in_memory_repository_satisfies_the_checkpoint_lineage_contract() -> None:
    await assert_checkpoint_lineage_repository_contract(
        InMemoryCheckpointLineageRepository(), request_scope="tenant-1", run_id="run-lineage"
    )
