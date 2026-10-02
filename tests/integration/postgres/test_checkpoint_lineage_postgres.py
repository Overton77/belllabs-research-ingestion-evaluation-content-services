"""PostgreSQL checkpoint lineage and recovery authority (migrations 0019, 0020).

Runs against the disposable stack only.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta
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
    assert_checkpoint_recovery_repository_contract,
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
        await assert_checkpoint_recovery_repository_contract(
            PostgresCheckpointLineageRepository(pool),
            request_scope="tenant-1",
            run_id=run_id,
        )
    finally:
        await pool.close()


RUNTIME_ROLE = "belllabs_control_runtime"


async def _assume_runtime_role(connection: asyncpg.Connection) -> None:
    await connection.execute(f"SET ROLE {RUNTIME_ROLE}")


@pytest.mark.asyncio
async def test_runtime_role_grants_and_rls_admit_the_full_lineage_contract(
    test_application_postgres_dsn: str,
) -> None:
    """Migrations 0019/0020 grants are sufficient for the production role, and no broader.

    Production pools connect as a non-owner member of `belllabs_control_runtime`
    (`app/integrations/postgres.py`). Every repository call here runs under that role, so
    forced RLS and the table grants are exercised instead of the owner's privileges.
    """

    require_disposable_postgres(test_application_postgres_dsn)
    owner = await asyncpg.create_pool(dsn=test_application_postgres_dsn, min_size=1, max_size=2)
    runtime = await asyncpg.create_pool(
        dsn=test_application_postgres_dsn,
        min_size=1,
        max_size=4,
        setup=_assume_runtime_role,
    )
    try:
        await reset_application_schema(owner)
        run_id = await admit_run(owner)
        async with runtime.acquire() as connection:
            assert await connection.fetchval("SELECT current_user") == RUNTIME_ROLE
            assert not await connection.fetchval(
                "SELECT rolsuper OR rolbypassrls FROM pg_roles WHERE rolname = current_user"
            )
            async with connection.transaction():
                await connection.execute(
                    "SELECT set_config('belllabs.request_scope', 'tenant-1', true)"
                )
                with pytest.raises(asyncpg.InsufficientPrivilegeError):
                    await connection.execute(
                        "SELECT 1 FROM belllabs_control.runtime_units FOR UPDATE"
                    )
        await assert_checkpoint_lineage_repository_contract(
            PostgresCheckpointLineageRepository(runtime),
            request_scope="tenant-1",
            run_id=run_id,
        )
        # RRM-004: lease, fenced result, incidents and reconciliation under the same role.
        await assert_checkpoint_recovery_repository_contract(
            PostgresCheckpointLineageRepository(runtime),
            request_scope="tenant-1",
            run_id=run_id,
        )
        async with runtime.acquire() as connection, connection.transaction():
            await connection.execute(
                "SELECT set_config('belllabs.request_scope', 'tenant-1', true)"
            )
            # The fenced result observation is insert-only for the runtime role.
            for statement in (
                "UPDATE belllabs_control.runtime_unit_result_observations SET claim_fence = 9",
                "DELETE FROM belllabs_control.runtime_unit_result_observations",
            ):
                with pytest.raises(asyncpg.InsufficientPrivilegeError):
                    async with connection.transaction():
                        await connection.execute(statement)
            assert await connection.fetchval(
                "SELECT count(*) FROM belllabs_control.runtime_reconciliation_incidents"
                " WHERE unit_key IS NOT NULL"
            ) == 2
        async with runtime.acquire() as connection, connection.transaction():
            await connection.execute(
                "SELECT set_config('belllabs.request_scope', 'tenant-2', true)"
            )
            assert (
                await connection.fetchval(
                    "SELECT count(*) FROM belllabs_control.runtime_checkpoint_transitions"
                )
                == 0
            ), "forced RLS hides another request scope's lineage"
            assert (
                await connection.fetchval(
                    "SELECT count(*) FROM belllabs_control.runtime_unit_result_observations"
                )
                == 0
            ), "forced RLS hides another request scope's results"
    finally:
        await runtime.close()
        await owner.close()


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


@pytest.mark.asyncio
async def test_concurrent_recoveries_of_one_unit_serialize_on_the_claim_lease(
    test_application_postgres_dsn: str,
) -> None:
    """REQ-CP-EXEC-014: after the holder's lease expired, concurrent recoveries of one unit
    serialize on the per-unit lock; exactly one takes the lease over (fence 2)."""

    require_disposable_postgres(test_application_postgres_dsn)
    pool = await asyncpg.create_pool(dsn=test_application_postgres_dsn, min_size=2, max_size=8)
    try:
        await reset_application_schema(pool)
        run_id = await admit_run(pool)
        repository = PostgresCheckpointLineageRepository(pool)
        unit = goal_unit(
            request_scope="tenant-1",
            run_id=run_id,
            operation_id="goal-iteration/1/executor",
            goal_iteration=1,
        )

        async def attempt(number: int, minutes: int):  # type: ignore[no-untyped-def]
            return await repository.record_attempt(
                unit=unit,
                execution_generation=1,
                attempt=activity_attempt(number, workflow_id=f"operation/{unit.unit_key}"),
                binding_id=f"binding:{unit.unit_key}",
                binding_digest=namespace_claim(unit).binding_digest,
                namespace=namespace_claim(unit),
                dispatching=True,
                observed_at=LINEAGE_NOW + timedelta(minutes=minutes),
                lease_expires_at=LINEAGE_NOW + timedelta(minutes=minutes + 5),
            )

        assert (await attempt(1, 0)).lease_granted
        outcomes = await asyncio.gather(*(attempt(number, 10) for number in (2, 3, 4)))
        granted = [item for item in outcomes if item.lease_granted]
        assert len(granted) == 1
        assert granted[0].took_over and granted[0].observation.claim_fence == 2
        assert {item.observation.claim_fence for item in outcomes} == {2}
        assert sorted(item.observation.dispatching for item in outcomes) == [False, False, True]
    finally:
        await pool.close()
