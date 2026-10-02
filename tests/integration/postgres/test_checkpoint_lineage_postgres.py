"""PostgreSQL checkpoint lineage authority (migration 0019) against the disposable stack."""

from __future__ import annotations

import asyncio
from urllib.parse import urlparse
from uuid import uuid4

import asyncpg
import pytest

from app.application.operations.postgres_checkpoint_lineage import (
    PostgresCheckpointLineageRepository,
)
from app.application.run_control.postgres_run_control_repository import PostgresRunControlRepository
from app.domain.operation_execution.checkpoint_lineage import CheckpointNamespaceBusy
from app.integrations.postgres import apply_application_migrations
from tests.fixtures.checkpoint_lineage import (
    LINEAGE_NOW,
    activity_attempt,
    assert_checkpoint_lineage_repository_contract,
    goal_unit,
    namespace_claim,
)
from tests.unit.run_control.test_run_control import request as run_request
from tests.unit.run_control.test_run_control import service as run_control_service


def require_disposable_postgres(dsn: str) -> None:
    parsed = urlparse(dsn)
    if (
        parsed.hostname not in {"127.0.0.1", "localhost"}
        or parsed.port != 55432
        or parsed.path != "/belllabs"
        or parsed.username != "belllabs"
    ):
        raise RuntimeError("checkpoint lineage proofs require the disposable local PostgreSQL")


async def reset_application_schema(pool: asyncpg.Pool) -> None:
    async with pool.acquire() as connection:
        await connection.execute("DROP SCHEMA IF EXISTS belllabs_control CASCADE")
    await apply_application_migrations(pool)


async def admit_run(pool: asyncpg.Pool) -> str:
    run_service, _ = run_control_service(PostgresRunControlRepository(pool))  # type: ignore[arg-type]
    decision = await run_service.admit(run_request(request_id=f"lineage-{uuid4()}"))
    assert decision.run_id is not None
    return decision.run_id


@pytest.mark.asyncio
async def test_postgres_repository_satisfies_the_checkpoint_lineage_contract(
    test_application_postgres_dsn: str,
) -> None:
    require_disposable_postgres(test_application_postgres_dsn)
    pool = await asyncpg.create_pool(dsn=test_application_postgres_dsn, min_size=1, max_size=4)
    try:
        await reset_application_schema(pool)
        run_id = await admit_run(pool)
        await assert_checkpoint_lineage_repository_contract(
            PostgresCheckpointLineageRepository(pool),
            request_scope="tenant-1",
            run_id=run_id,
        )
    finally:
        await pool.close()


@pytest.mark.asyncio
async def test_concurrent_session_invocations_serialize_on_the_namespace_row(
    test_application_postgres_dsn: str,
) -> None:
    """REQ-BP-GD-012: two concurrent dispatches into one session admit exactly one."""

    require_disposable_postgres(test_application_postgres_dsn)
    pool = await asyncpg.create_pool(dsn=test_application_postgres_dsn, min_size=2, max_size=6)
    try:
        await reset_application_schema(pool)
        run_id = await admit_run(pool)
        repository = PostgresCheckpointLineageRepository(pool)
        units = [
            goal_unit(
                request_scope="tenant-1",
                run_id=run_id,
                operation_id=f"goal-iteration/{iteration}/executor",
                goal_iteration=iteration,
            )
            for iteration in (1, 2)
        ]
        outcomes = await asyncio.gather(
            *(
                repository.record_attempt(
                    unit=unit,
                    execution_generation=1,
                    attempt=activity_attempt(workflow_id=f"operation/{unit.unit_key}"),
                    binding_id=f"binding:{unit.unit_key}",
                    binding_digest=namespace_claim(unit).binding_digest,
                    namespace=namespace_claim(unit),
                    dispatching=True,
                    observed_at=LINEAGE_NOW,
                )
                for unit in units
            ),
            return_exceptions=True,
        )
        busy = [item for item in outcomes if isinstance(item, CheckpointNamespaceBusy)]
        admitted = [item for item in outcomes if not isinstance(item, BaseException)]
        assert len(busy) == 1 and len(admitted) == 1
    finally:
        await pool.close()
