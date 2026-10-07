"""PostgreSQL checkpoint lineage and recovery authority on the common mission_control.

Runtime units are canonical activations and execution generations canonical attempts
(fencing token = claim fence); lineage evidence is support. Every repository call runs
under a restricted login of `mission_control_runtime` with forced RLS.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta
from uuid import uuid4

import asyncpg
import pytest

from mission_control.adapters.postgres.operations.checkpoint_lineage import (
    PostgresCheckpointLineageRepository,
)
from mission_control.adapters.postgres.run_control.run_control_repository import (
    PostgresRunControlRepository,
)
from mission_control.adapters.postgres.scope import apply_scope
from mission_control.domain.execution.checkpoint_lineage import CheckpointNamespaceBusy
from tests.fixtures.checkpoint_lineage import (
    LINEAGE_NOW,
    activity_attempt,
    assert_checkpoint_lineage_repository_contract,
    assert_checkpoint_recovery_repository_contract,
    goal_unit,
    namespace_claim,
)
from tests.fixtures.mission_control_common_db import CommonDatabase
from tests.integration.postgres.runtime_common import common_db as common_db
from tests.integration.postgres.runtime_common import owner_rows, scoped_request
from tests.unit.run_control.test_run_control import service as run_control_service

pytestmark = pytest.mark.common_db

RUNTIME_ROLE = "mission_control_runtime"


async def admit_run(pool: asyncpg.Pool, db: CommonDatabase) -> str:
    run_service, _ = run_control_service(PostgresRunControlRepository(pool))  # type: ignore[arg-type]
    decision = await run_service.admit(scoped_request(db, request_id=f"lineage-{uuid4()}"))
    assert decision.run_id is not None
    return decision.run_id


@pytest.mark.asyncio
async def test_postgres_repository_satisfies_the_checkpoint_lineage_contract(
    common_db: CommonDatabase,
) -> None:
    pool = await common_db.pool(max_size=4)
    try:
        run_id = await admit_run(pool, common_db)
        await assert_checkpoint_lineage_repository_contract(
            PostgresCheckpointLineageRepository(pool),
            request_scope=common_db.scope(),
            run_id=run_id,
        )
        await assert_checkpoint_recovery_repository_contract(
            PostgresCheckpointLineageRepository(pool),
            request_scope=common_db.scope(),
            run_id=run_id,
        )
        # Units are canonical activations of the admitted run; generations are canonical
        # attempts whose fencing token is the claim fence.
        rows = await owner_rows(
            common_db,
            """
            SELECT a.activation_key, t.attempt_no, t.fencing_token, g.execution_generation
            FROM mission_control.activation a
            JOIN mission_control.mission_run r USING (installation_id, application_id,
                                                      tenant_id, run_id)
            JOIN mission_control.attempt t
              ON t.installation_id = a.installation_id AND t.application_id = a.application_id
             AND t.tenant_id = a.tenant_id AND t.activation_id = a.activation_id
            JOIN mission_control.runtime_unit_generation g
              ON g.installation_id = t.installation_id AND g.application_id = t.application_id
             AND g.tenant_id = t.tenant_id AND g.attempt_key = t.attempt_key
            WHERE r.run_key = $1
            """,
            run_id,
        )
        assert rows
        assert all(row["attempt_no"] == row["execution_generation"] for row in rows)
    finally:
        await pool.close()


@pytest.mark.asyncio
async def test_runtime_role_grants_and_rls_admit_the_full_lineage_contract(
    common_db: CommonDatabase,
) -> None:
    """The release grants are sufficient for the production role, and no broader.

    Production pools connect as a non-owner login holding only `mission_control_runtime`,
    so forced RLS and the table grants are exercised instead of the owner's privileges.
    """

    runtime = await common_db.pool(max_size=4)
    scope = common_db.scope()
    try:
        run_id = await admit_run(runtime, common_db)
        async with runtime.acquire() as connection:
            assert await connection.fetchval(
                "SELECT pg_has_role(current_user, $1, 'MEMBER')", RUNTIME_ROLE
            )
            assert not await connection.fetchval(
                "SELECT rolsuper OR rolbypassrls FROM pg_roles WHERE rolname = current_user"
            )
            async with connection.transaction():
                await apply_scope(connection, scope)
                # Activations (unit identities) are insert-only for the runtime role.
                with pytest.raises(asyncpg.InsufficientPrivilegeError):
                    await connection.execute("SELECT 1 FROM mission_control.activation FOR UPDATE")
        await assert_checkpoint_lineage_repository_contract(
            PostgresCheckpointLineageRepository(runtime),
            request_scope=scope,
            run_id=run_id,
        )
        # RRM-004: lease, fenced result, incidents and reconciliation under the same role.
        await assert_checkpoint_recovery_repository_contract(
            PostgresCheckpointLineageRepository(runtime),
            request_scope=scope,
            run_id=run_id,
        )
        async with runtime.acquire() as connection, connection.transaction():
            await apply_scope(connection, scope)
            # The fenced result observation is insert-only for the runtime role.
            for statement in (
                "UPDATE mission_control.unit_result_observation SET claim_fence = 9",
                "DELETE FROM mission_control.unit_result_observation",
            ):
                with pytest.raises(asyncpg.InsufficientPrivilegeError):
                    async with connection.transaction():
                        await connection.execute(statement)
            assert (
                await connection.fetchval(
                    "SELECT count(*) FROM mission_control.reconciliation_case"
                    " WHERE target_kind = 'runtime_unit_in_doubt'"
                )
                == 3
            ), "two unit generations' incidents, one of them at revision 2"
        async with runtime.acquire() as connection, connection.transaction():
            await apply_scope(connection, common_db.scope("tenant-2"))
            assert (
                await connection.fetchval(
                    "SELECT count(*) FROM mission_control.checkpoint_transition"
                )
                == 0
            ), "forced RLS hides another tenant's lineage"
            assert (
                await connection.fetchval(
                    "SELECT count(*) FROM mission_control.unit_result_observation"
                )
                == 0
            ), "forced RLS hides another tenant's results"
        async with runtime.acquire() as connection, connection.transaction():
            # Missing scope context: no rows.
            assert await connection.fetchval("SELECT count(*) FROM mission_control.activation") == 0
    finally:
        await runtime.close()


@pytest.mark.asyncio
async def test_concurrent_session_invocations_serialize_on_the_namespace_row(
    common_db: CommonDatabase,
) -> None:
    """REQ-BP-GD-012: two concurrent dispatches into one session admit exactly one."""

    pool = await common_db.pool(min_size=2, max_size=6)
    try:
        run_id = await admit_run(pool, common_db)
        repository = PostgresCheckpointLineageRepository(pool)
        units = [
            goal_unit(
                request_scope=common_db.scope(),
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
        assert len(busy) == 1 and len(admitted) == 1, outcomes
    finally:
        await pool.close()


@pytest.mark.asyncio
async def test_concurrent_recoveries_of_one_unit_serialize_on_the_claim_lease(
    common_db: CommonDatabase,
) -> None:
    """REQ-CP-EXEC-014: after the holder's lease expired, concurrent recoveries of one unit
    serialize on the per-unit lock; exactly one takes the lease over (fence 2)."""

    pool = await common_db.pool(min_size=2, max_size=8)
    try:
        run_id = await admit_run(pool, common_db)
        repository = PostgresCheckpointLineageRepository(pool)
        unit = goal_unit(
            request_scope=common_db.scope(),
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
