from __future__ import annotations

import asyncio

import asyncpg
import pytest

from app.application.postgres_run_control_repository import PostgresRunControlRepository
from app.domain.run_control.contracts import (
    CommandStatus,
    RecordUsageAction,
    ReserveBudgetAction,
)
from app.domain.run_control.errors import IdempotencyConflict, RunControlNotFound
from app.domain.run_control.family_admission import FamilyAdmissionReceipt
from app.integrations.postgres import apply_application_migrations
from tests.test_atomic_family_admission import family_mutation, family_service
from tests.test_run_control import command, request


@pytest.mark.asyncio
@pytest.mark.parametrize("combined", [False, True])
async def test_postgres_stale_rejection_is_not_persisted(
    test_application_postgres_dsn: str,
    combined: bool,
) -> None:
    class PausingPostgresRepository(PostgresRunControlRepository):
        def __init__(
            self,
            pool: asyncpg.Pool,
            family_writer_pool: asyncpg.Pool,
        ) -> None:
            super().__init__(pool, family_writer_pool=family_writer_pool)
            self.target_run_id: str | None = None
            self.stale_budget_read = asyncio.Event()
            self.resume_command_read = asyncio.Event()
            self.paused = False

        async def get_effects(self, request_scope: str, run_id: str):  # type: ignore[no-untyped-def]
            if run_id == self.target_run_id and not self.paused:
                self.paused = True
                self.stale_budget_read.set()
                await self.resume_command_read.wait()
            return await super().get_effects(request_scope, run_id)

    pool = await asyncpg.create_pool(
        dsn=test_application_postgres_dsn, min_size=1, max_size=8
    )
    family_writer_pool = await asyncpg.create_pool(
        dsn=test_application_postgres_dsn, min_size=1, max_size=8
    )
    try:
        async with pool.acquire() as connection:
            await connection.execute("DROP SCHEMA IF EXISTS belllabs_control CASCADE")
        await apply_application_migrations(pool)
        repository = PausingPostgresRepository(pool, family_writer_pool)
        run_service, _ = family_service(repository)  # type: ignore[arg-type]
        parent = await run_service.admit(
            request(request_id=f"postgres-rejection-parent-{combined}")
        )
        assert parent.run_id is not None
        parent_run_id = parent.run_id
        parent_budget = await run_service.get_budget("tenant-1", parent_run_id)
        child_request = request(request_id=f"postgres-rejection-child-{combined}")
        child_request = child_request.model_copy(
            update={
                "parent_run_id": parent_run_id,
                "actor": child_request.actor.model_copy(
                    update={
                        "authority_refs": child_request.actor.authority_refs
                        | {f"workflow_run.parent:{parent_run_id}:sponsor"}
                    }
                ),
                "budget_envelope": child_request.budget_envelope.model_copy(
                    update={"parent_account_id": parent_budget.account_id}
                ),
            }
        )
        child = await run_service.admit(child_request)
        assert child.run_id is not None
        repository.target_run_id = parent_run_id
        lifecycle = command(
            parent_run_id,
            1,
            f"postgres-stale-rejection-{combined}",
            ReserveBudgetAction(
                reservation_id=f"postgres-stale-rejection-{combined}",
                amounts={"tokens.total": 70},
            ),
        )
        mutation = family_mutation(
            parent_run_id,
            mutation_id=f"postgres-stale-rejection-{combined}",
        )
        task = asyncio.create_task(
            run_service.execute_family_admission(lifecycle, mutation)
            if combined
            else run_service.execute(lifecycle)
        )
        await repository.stale_budget_read.wait()
        released = await run_service.execute(
            command(
                child.run_id,
                1,
                f"postgres-release-child-{combined}",
                RecordUsageAction(
                    usage_id=f"postgres-release-child-{combined}",
                    reservation_id="baseline",
                    actual_amounts={},
                    release_amounts={"tokens.total": 20},
                ),
            )
        )
        assert released.status == CommandStatus.ACCEPTED
        repository.resume_command_read.set()
        result_or_receipt = await task
        result = (
            result_or_receipt.command_result
            if isinstance(result_or_receipt, FamilyAdmissionReceipt)
            else result_or_receipt
        )
        assert result.status == CommandStatus.ACCEPTED

        async with pool.acquire() as connection:
            stored_status = await connection.fetchval(
                """
                SELECT result->>'status'
                FROM belllabs_control.lifecycle_command_results
                WHERE run_id = $1 AND command_id = $2
                """,
                parent_run_id,
                lifecycle.command_id,
            )
        assert stored_status == CommandStatus.ACCEPTED.value
        if combined:
            assert (
                await run_service.execute_family_admission(lifecycle, mutation)
                == result_or_receipt
            )
        else:
            assert await run_service.execute(lifecycle) == result
    finally:
        async with pool.acquire() as connection:
            await connection.execute("DROP SCHEMA IF EXISTS belllabs_control CASCADE")
        await family_writer_pool.close()
        await pool.close()


@pytest.mark.asyncio
async def test_postgres_parent_rollup_cannot_be_overwritten_by_family_admission(
    test_application_postgres_dsn: str,
) -> None:
    class PausingPostgresRepository(PostgresRunControlRepository):
        def __init__(
            self,
            pool: asyncpg.Pool,
            family_writer_pool: asyncpg.Pool,
        ) -> None:
            super().__init__(pool, family_writer_pool=family_writer_pool)
            self.target_run_id: str | None = None
            self.stale_budget_read = asyncio.Event()
            self.resume_family_read = asyncio.Event()
            self.paused = False

        async def get_effects(self, request_scope: str, run_id: str):  # type: ignore[no-untyped-def]
            if run_id == self.target_run_id and not self.paused:
                self.paused = True
                self.stale_budget_read.set()
                await self.resume_family_read.wait()
            return await super().get_effects(request_scope, run_id)

    pool = await asyncpg.create_pool(
        dsn=test_application_postgres_dsn, min_size=1, max_size=8
    )
    family_writer_pool = await asyncpg.create_pool(
        dsn=test_application_postgres_dsn, min_size=1, max_size=8
    )
    try:
        async with pool.acquire() as connection:
            await connection.execute("DROP SCHEMA IF EXISTS belllabs_control CASCADE")
        await apply_application_migrations(pool)
        pausing_repository = PausingPostgresRepository(pool, family_writer_pool)
        run_service, _ = family_service(pausing_repository)  # type: ignore[arg-type]
        parent = await run_service.admit(request(request_id="postgres-authority-parent"))
        assert parent.run_id is not None
        parent_run_id = parent.run_id
        pausing_repository.target_run_id = parent_run_id
        parent_budget = await run_service.get_budget("tenant-1", parent_run_id)
        child_request = request(request_id="postgres-authority-child")
        child_request = child_request.model_copy(
            update={
                "parent_run_id": parent_run_id,
                "actor": child_request.actor.model_copy(
                    update={
                        "authority_refs": child_request.actor.authority_refs
                        | {f"workflow_run.parent:{parent_run_id}:sponsor"}
                    }
                ),
                "budget_envelope": child_request.budget_envelope.model_copy(
                    update={"parent_account_id": parent_budget.account_id}
                ),
            }
        )

        family_task = asyncio.create_task(
            run_service.execute_family_admission(
                command(
                    parent_run_id,
                    1,
                    "postgres-authority-family",
                    ReserveBudgetAction(
                        reservation_id="postgres-authority-family",
                        amounts={"tokens.total": 10},
                    ),
                ),
                family_mutation(parent_run_id),
            )
        )
        await pausing_repository.stale_budget_read.wait()
        child = await run_service.admit(child_request)
        assert child.run_id is not None
        pausing_repository.resume_family_read.set()
        receipt = await family_task

        assert receipt.command_result.status == CommandStatus.ACCEPTED
        final_budget = await run_service.get_budget("tenant-1", parent_run_id)
        assert final_budget.reserved["tokens.total"] == 50
        assert final_budget.reservations["postgres-authority-family"] == {
            "tokens.total": 10
        }

        pausing_repository.paused = False
        pausing_repository.stale_budget_read = asyncio.Event()
        pausing_repository.resume_family_read = asyncio.Event()
        second_child_request = child_request.model_copy(
            update={"request_id": "postgres-authority-child-two"}
        )
        plain_task = asyncio.create_task(
            run_service.execute(
                command(
                    parent_run_id,
                    2,
                    "postgres-authority-plain",
                    ReserveBudgetAction(
                        reservation_id="postgres-authority-plain",
                        amounts={"tokens.total": 10},
                    ),
                )
            )
        )
        await pausing_repository.stale_budget_read.wait()
        second_child = await run_service.admit(second_child_request)
        assert second_child.run_id is not None
        pausing_repository.resume_family_read.set()
        plain_result = await plain_task

        assert plain_result.status == CommandStatus.ACCEPTED
        final_budget = await run_service.get_budget("tenant-1", parent_run_id)
        assert final_budget.reserved["tokens.total"] == 80
        assert final_budget.reservations["postgres-authority-plain"] == {
            "tokens.total": 10
        }
    finally:
        async with pool.acquire() as connection:
            await connection.execute("DROP SCHEMA IF EXISTS belllabs_control CASCADE")
        await family_writer_pool.close()
        await pool.close()


@pytest.mark.asyncio
async def test_postgres_atomic_family_admission_contract(
    test_application_postgres_dsn: str,
) -> None:
    pool = await asyncpg.create_pool(
        dsn=test_application_postgres_dsn, min_size=1, max_size=8
    )
    family_writer_pool = await asyncpg.create_pool(
        dsn=test_application_postgres_dsn, min_size=1, max_size=8
    )
    try:
        async with pool.acquire() as connection:
            await connection.execute("DROP SCHEMA IF EXISTS belllabs_control CASCADE")
        await apply_application_migrations(pool)
        async with pool.acquire() as connection:
            assert await connection.fetchval(
                "SELECT EXISTS (SELECT 1 FROM belllabs_control.schema_migrations "
                "WHERE version = '0017_atomic_family_admission_v1.sql')"
            )

        repository = PostgresRunControlRepository(
            pool,
            family_writer_pool=family_writer_pool,
        )
        run_service, _ = family_service(repository)  # type: ignore[arg-type]
        admitted = await run_service.admit(request())
        assert admitted.run_id is not None
        run_id = admitted.run_id
        with pytest.raises(ValueError, match="must be distinct"):
            PostgresRunControlRepository(pool, family_writer_pool=pool)
        missing_writer_service, _ = family_service(  # type: ignore[arg-type]
            PostgresRunControlRepository(pool)
        )
        with pytest.raises(RuntimeError, match="distinct family repository writer pool"):
            await missing_writer_service.execute_family_admission(
                command(
                    run_id,
                    1,
                    "missing-writer",
                    ReserveBudgetAction(
                        reservation_id="missing-writer",
                        amounts={"tokens.total": 1},
                    ),
                ),
                family_mutation(run_id, mutation_id="missing-writer"),
            )
        assert (await run_service.get_run("tenant-1", run_id)).version == 1
        with pytest.raises(RunControlNotFound):
            await repository.get_family_admission_receipt(
                "tenant-2", run_id, "operator", "missing"
            )
        with pytest.raises(RunControlNotFound):
            await repository.get_family_head(
                "tenant-2", run_id, "test_family", type(family_mutation(run_id))
            )

        for boundary in (
            "family_admission.after_run_control",
            "family_admission.after_family",
        ):
            async def fail(observed: str, expected: str = boundary) -> None:
                if observed == expected:
                    raise RuntimeError(f"injected {expected}")

            failing, _ = family_service(  # type: ignore[arg-type]
                PostgresRunControlRepository(
                    pool,
                    family_writer_pool=family_writer_pool,
                    before_commit=fail,
                )
            )
            with pytest.raises(RuntimeError, match="injected"):
                await failing.execute_family_admission(
                    command(
                        run_id,
                        1,
                        f"rollback-{boundary}",
                        ReserveBudgetAction(
                            reservation_id=f"rollback-{boundary}",
                            amounts={"tokens.total": 1},
                        ),
                    ),
                    family_mutation(run_id, mutation_id=f"rollback-{boundary}"),
                )
            assert (await run_service.get_run("tenant-1", run_id)).version == 1
            async with pool.acquire() as connection:
                assert (
                    await connection.fetchval(
                        "SELECT count(*) FROM belllabs_control.family_admission_results"
                    )
                    == 0
                )

        first_command = command(
            run_id,
            1,
            "concurrent-a",
            ReserveBudgetAction(reservation_id="concurrent-a", amounts={"tokens.total": 10}),
        )
        second_command = command(
            run_id,
            1,
            "concurrent-b",
            ReserveBudgetAction(reservation_id="concurrent-b", amounts={"tokens.total": 10}),
        )
        outcomes = await asyncio.gather(
            run_service.execute_family_admission(
                first_command, family_mutation(run_id, mutation_id="concurrent-a")
            ),
            run_service.execute_family_admission(
                second_command, family_mutation(run_id, mutation_id="concurrent-b")
            ),
        )
        accepted_receipts = [
            item
            for item in outcomes
            if item.command_result.status == CommandStatus.ACCEPTED
        ]
        stale_receipts = [
            item for item in outcomes if item.command_result.status == CommandStatus.STALE
        ]
        assert len(accepted_receipts) == 1
        assert len(stale_receipts) == 1
        accepted_receipt = accepted_receipts[0]
        stale_receipt = stale_receipts[0]
        winner_command = (
            first_command
            if accepted_receipt.command_result.command_id == "concurrent-a"
            else second_command
        )
        winner_mutation = family_mutation(
            run_id, mutation_id=accepted_receipt.command_result.command_id
        )
        assert (
            await run_service.execute_family_admission(winner_command, winner_mutation)
            == accepted_receipt
        )
        loser_command = second_command if winner_command is first_command else first_command
        loser_mutation = family_mutation(
            run_id, mutation_id=stale_receipt.command_result.command_id
        )
        assert (
            await run_service.execute_family_admission(loser_command, loser_mutation)
            == stale_receipt
        )
        with pytest.raises(IdempotencyConflict, match="reused by another command"):
            await run_service.execute_family_admission(
                command(
                    run_id,
                    2,
                    "mutation-collision-same",
                    ReserveBudgetAction(
                        reservation_id="mutation-collision-same",
                        amounts={"tokens.total": 1},
                    ),
                ),
                winner_mutation,
            )
        with pytest.raises(IdempotencyConflict, match="conflicting content"):
            await run_service.execute_family_admission(
                command(
                    run_id,
                    2,
                    "mutation-collision-different",
                    ReserveBudgetAction(
                        reservation_id="mutation-collision-different",
                        amounts={"tokens.total": 1},
                    ),
                ),
                winner_mutation.model_copy(update={"candidate_ref": "candidate:different"}),
            )

        async with pool.acquire() as connection:
            counts = await connection.fetchrow(
                """
                SELECT
                  (SELECT count(*) FROM belllabs_control.family_admission_heads) AS heads,
                  (SELECT count(*) FROM belllabs_control.family_admission_journal) AS journal,
                  (SELECT count(*) FROM belllabs_control.family_admission_results) AS results,
                  (SELECT count(*) FROM belllabs_control.budget_ledger
                   WHERE idempotency_id LIKE 'command:concurrent-%') AS ledger,
                  (SELECT count(*) FROM belllabs_control.outbox
                   WHERE aggregate_version = 2) AS outbox
                """
            )
            assert dict(counts) == {
                "heads": 1,
                "journal": 1,
                "results": 2,
                "ledger": 1,
                "outbox": 2,
            }

        tenant_two = await run_service.admit(
            request(request_scope="tenant-2", request_id="family-tenant-two")
        )
        assert tenant_two.run_id is not None
        async with pool.acquire() as connection, connection.transaction():
            await connection.execute("SET LOCAL ROLE belllabs_control_runtime")
            await connection.execute(
                "SELECT set_config('belllabs.request_scope', 'tenant-2', true)"
            )
            isolated_counts = await connection.fetchrow(
                """
                SELECT
                  (SELECT count(*) FROM belllabs_control.family_admission_heads) AS heads,
                  (SELECT count(*) FROM belllabs_control.family_admission_journal) AS journal,
                  (SELECT count(*) FROM belllabs_control.family_admission_results) AS results
                """
            )
            assert dict(isolated_counts) == {"heads": 0, "journal": 0, "results": 0}
            for table in (
                "family_admission_heads",
                "family_admission_journal",
                "family_admission_results",
            ):
                assert not await connection.fetchval(
                    "SELECT has_table_privilege("
                    "'belllabs_control_runtime', $1, 'INSERT') "
                    "OR has_table_privilege("
                    "'belllabs_control_runtime', $1, 'UPDATE')",
                    f"belllabs_control.{table}",
                )
            assert await connection.fetchval(
                "SELECT to_regprocedure("
                "'belllabs_control.commit_family_admission("
                "text,text,text,bigint,text,text,text,jsonb,timestamptz,"
                "text,text,text,jsonb,timestamptz,boolean)')"
            ) is None
            assert not await connection.fetchval(
                "SELECT pg_has_role("
                "'belllabs_control_runtime', "
                "'belllabs_family_repository_writer', 'MEMBER')"
            )
            assert not await connection.fetchval(
                """
                SELECT COALESCE((
                    SELECT pg_has_role(
                        app_role.oid, writer_role.oid, 'MEMBER'
                    )
                    FROM pg_roles app_role
                    CROSS JOIN pg_roles writer_role
                    WHERE app_role.rolname = 'belllabs_app'
                      AND writer_role.rolname = 'belllabs_family_repository_writer'
                ), false)
                """
            )
            with pytest.raises(asyncpg.PostgresError):
                await connection.execute(
                    """
                    INSERT INTO belllabs_control.family_admission_heads
                        (request_scope, run_id, family_kind, family_version,
                         mutation_fingerprint, mutation, updated_at)
                    VALUES ('tenant-1', $1, 'forged', 1, $2, '{}'::jsonb, clock_timestamp())
                    """,
                    run_id,
                    "sha256:" + "f" * 64,
                )

        plain = command(
            run_id,
            2,
            "plain-collision",
            ReserveBudgetAction(reservation_id="plain-collision", amounts={"tokens.total": 1}),
        )
        await run_service.execute(plain)
        async with pool.acquire() as connection, connection.transaction():
            await connection.execute("SET LOCAL ROLE belllabs_control_runtime")
            await connection.execute(
                "SELECT set_config('belllabs.request_scope', 'tenant-1', true)"
            )
            with pytest.raises(asyncpg.InsufficientPrivilegeError):
                await connection.execute(
                    """
                    INSERT INTO belllabs_control.family_admission_results
                        (request_scope, run_id, idempotency_issuer, command_id,
                         command_fingerprint, family_mutation_fingerprint,
                         receipt, recorded_at)
                    SELECT 'tenant-1', run_id, idempotency_issuer, command_id,
                           command_fingerprint, $2, '{}'::jsonb, clock_timestamp()
                    FROM belllabs_control.lifecycle_command_results
                    WHERE run_id = $1 AND command_id = 'plain-collision'
                    """,
                    run_id,
                    "sha256:" + "f" * 64,
                )
        with pytest.raises(IdempotencyConflict):
            await run_service.execute_family_admission(
                plain,
                family_mutation(
                    run_id,
                    mutation_id="plain-collision",
                    expected_family_version=1,
                ),
            )
        with pytest.raises(IdempotencyConflict):
            await run_service.execute(winner_command)
    finally:
        async with pool.acquire() as connection:
            await connection.execute("DROP SCHEMA IF EXISTS belllabs_control CASCADE")
        await family_writer_pool.close()
        await pool.close()
