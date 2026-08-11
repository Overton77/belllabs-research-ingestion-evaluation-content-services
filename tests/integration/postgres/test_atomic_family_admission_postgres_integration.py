from __future__ import annotations

import asyncio
import json

import asyncpg
import pytest

from app.application.run_control.postgres_run_control_repository import PostgresRunControlRepository
from app.domain.control_plane.canonical import sha256_digest
from app.domain.run_control.contracts import (
    ApplyAuthorityBatchAction,
    BudgetState,
    ClaimEffectAction,
    CommandStatus,
    RecordUsageAction,
    ReserveBudgetAction,
    SettlePendingUsageAction,
    StartAction,
)
from app.domain.run_control.errors import CommandRejected, IdempotencyConflict, RunControlNotFound
from app.domain.run_control.family_admission import FamilyAdmissionReceipt
from app.integrations.postgres import apply_application_migrations
from tests.unit.run_control.test_atomic_family_admission import authority_batch, family_mutation, family_service
from tests.unit.run_control.test_run_control import command, request


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


@pytest.mark.asyncio
async def test_postgres_authority_batch_is_atomic_and_preserves_outbox_finality(
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
        repository = PostgresRunControlRepository(
            pool,
            family_writer_pool=family_writer_pool,
        )
        run_service, _ = family_service(repository)  # type: ignore[arg-type]
        admitted = await run_service.admit(request(request_id="postgres-authority-batch"))
        assert admitted.run_id is not None
        run_id = admitted.run_id
        with pytest.raises(CommandRejected, match="strict exact-type"):
            await run_service.execute_family_admission(
                command(
                    run_id,
                    1,
                    "postgres-malformed-family-mutation",
                    ReserveBudgetAction(
                        reservation_id="postgres-malformed-family-mutation",
                        amounts={"tokens.total": 1},
                    ),
                ),
                family_mutation(run_id).model_copy(
                    update={"candidate_ref": "x" * 70_000}
                ),
            )
        await run_service.execute(
            command(
                run_id,
                1,
                "postgres-authority-batch-reserve",
                ReserveBudgetAction(
                    reservation_id="effect-reservation",
                    amounts={"tokens.total": 10},
                ),
            )
        )
        await run_service.execute(
            command(
                run_id,
                2,
                "postgres-authority-batch-claim",
                ClaimEffectAction(
                    effect_id="effect-1",
                    effect_kind="external_write",
                    operation_ref="operation:1",
                    provider_idempotency_key="provider-effect-1",
                    reservation_id="effect-reservation",
                ),
            )
        )

        malformed_batch = authority_batch().model_copy(update={"actions": (StartAction(),)})
        malformed_command = command(
            run_id,
            3,
            "postgres-malformed-authority-batch",
            authority_batch(),
        ).model_copy(update={"action": malformed_batch})
        with pytest.raises(CommandRejected, match="strict contract revalidation"):
            await run_service.execute_family_admission(
                malformed_command,
                family_mutation(
                    run_id,
                    mutation_id="postgres-malformed-authority-batch",
                ),
            )
        omitted_output = ApplyAuthorityBatchAction(actions=authority_batch().actions[:-1])
        with pytest.raises(CommandRejected, match="omits actions required"):
            await run_service.execute_family_admission(
                command(
                    run_id,
                    3,
                    "postgres-policy-omission",
                    omitted_output,
                ),
                family_mutation(run_id, mutation_id="postgres-policy-omission"),
            )
        with pytest.raises(CommandRejected, match="binding failed"):
            await run_service.execute_family_admission(
                command(
                    run_id,
                    3,
                    "postgres-policy-reference-mismatch",
                    authority_batch(),
                ),
                family_mutation(
                    run_id,
                    mutation_id="postgres-policy-reference-mismatch",
                    candidate_ref="output:not-accepted",
                ),
            )
        spoofed_actions = list(authority_batch().actions)
        spoofed_actions[3] = spoofed_actions[3].model_copy(
            update={"usage_settlement_ref": "settlement:spoofed"}
        )
        spoofed_lifecycle = command(
            run_id,
            3,
            "postgres-spoofed-usage-settlement",
            ApplyAuthorityBatchAction(actions=tuple(spoofed_actions)),
        )
        spoofed_mutation = family_mutation(
            run_id,
            mutation_id="postgres-spoofed-usage-settlement",
        )
        spoofed = await run_service.execute_family_admission(
            spoofed_lifecycle,
            spoofed_mutation,
        )
        assert spoofed.command_result.status == CommandStatus.REJECTED
        assert spoofed.command_result.reason_code == "usage_settlement_not_found"
        assert (
            await run_service.execute_family_admission(
                spoofed_lifecycle,
                spoofed_mutation,
            )
            == spoofed
        )
        assert (await run_service.get_run("tenant-1", run_id)).version == 3

        for boundary in (
            "family_admission.after_run_control",
            "family_admission.after_family",
        ):
            async def fail(observed: str, expected: str = boundary) -> None:
                if observed == expected:
                    raise RuntimeError("injected PostgreSQL authority batch failure")

            failing_service, _ = family_service(  # type: ignore[arg-type]
                PostgresRunControlRepository(
                    pool,
                    family_writer_pool=family_writer_pool,
                    before_commit=fail,
                )
            )
            with pytest.raises(
                RuntimeError,
                match="injected PostgreSQL authority batch failure",
            ):
                await failing_service.execute_family_admission(
                    command(
                        run_id,
                        3,
                        f"postgres-authority-batch-rollback-{boundary}",
                        authority_batch(),
                    ),
                    family_mutation(
                        run_id,
                        mutation_id=f"postgres-authority-batch-rollback-{boundary}",
                    ),
                )
            assert (await run_service.get_run("tenant-1", run_id)).version == 3
            assert (await run_service.get_budget("tenant-1", run_id)).consumed == {}
            assert (
                await run_service.get_effects("tenant-1", run_id)
            ).claims["effect-1"].settlement is None

        invalid_batch = authority_batch().model_copy(
            update={
                "actions": (
                    authority_batch().actions[0],
                    SettlePendingUsageAction(
                        settlement_id="accepted-usage-settlement",
                        usage_id="accepted-usage",
                        actual_amounts={"tokens.total": 6},
                    ),
                    *authority_batch().actions[2:],
                )
            }
        )
        rejected = await run_service.execute_family_admission(
            command(run_id, 3, "postgres-authority-batch-rejected", invalid_batch),
            family_mutation(run_id, mutation_id="postgres-authority-batch-rejected"),
        )
        assert rejected.command_result.status == CommandStatus.REJECTED
        assert (await run_service.get_run("tenant-1", run_id)).version == 3
        assert (await run_service.get_budget("tenant-1", run_id)).consumed == {}
        assert (
            await run_service.get_effects("tenant-1", run_id)
        ).claims["effect-1"].settlement is None

        lifecycle = command(run_id, 3, "postgres-authority-batch", authority_batch())
        mutation = family_mutation(run_id, mutation_id="postgres-authority-batch")
        receipt = await run_service.execute_family_admission(lifecycle, mutation)
        assert receipt.command_result.status == CommandStatus.ACCEPTED
        assert await run_service.execute_family_admission(lifecycle, mutation) == receipt
        projection = await run_service.get_run("tenant-1", run_id)
        assert projection.version == 4
        assert len(projection.accepted_obligation_evidence) == 1
        assert len(projection.accepted_output_evidence) == 1

        async with pool.acquire() as connection:
            counts = await connection.fetchrow(
                """
                SELECT
                  (SELECT count(*) FROM belllabs_control.lifecycle_transitions
                   WHERE run_id = $1 AND resulting_version = 4) AS transitions,
                  (SELECT count(*) FROM belllabs_control.budget_ledger
                   WHERE run_id = $1 AND idempotency_id IN
                     ('accepted-usage', 'accepted-usage-settlement')) AS budget_entries,
                  (SELECT count(*) FROM belllabs_control.effect_ledger_entries
                   WHERE run_id = $1 AND kind IN ('observation', 'settlement')) AS effect_entries,
                  (SELECT count(*) FROM belllabs_control.outbox
                   WHERE aggregate_id = $1 AND aggregate_version = 4) AS outbox
                """,
                run_id,
            )
            outbox = await connection.fetch(
                """
                SELECT event_type, sequence, is_version_final,
                       envelope->'payload'->>'authority_batch_digest' AS batch_digest,
                       CASE
                         WHEN envelope->'payload' ? 'action_identity_summary'
                         THEN jsonb_array_length(
                           envelope->'payload'->'action_identity_summary'
                         )
                         ELSE NULL
                       END AS identity_count
                FROM belllabs_control.outbox
                WHERE aggregate_id = $1 AND aggregate_version = 4
                ORDER BY sequence
                """,
                run_id,
            )
        assert dict(counts) == {
            "transitions": 1,
            "budget_entries": 4,
            "effect_entries": 2,
            "outbox": 2,
        }
        assert [row["event_type"] for row in outbox] == [
            "workflow_run.apply_authority_batch",
            "workflow_run.family_admission_committed",
        ]
        assert [row["sequence"] for row in outbox] == [1, 2]
        assert [row["is_version_final"] for row in outbox] == [False, True]
        assert outbox[0]["batch_digest"] == sha256_digest(authority_batch())
        assert outbox[0]["identity_count"] == 6
        assert outbox[1]["batch_digest"] is None
        assert outbox[1]["identity_count"] is None
    finally:
        async with pool.acquire() as connection:
            await connection.execute("DROP SCHEMA IF EXISTS belllabs_control CASCADE")
        await family_writer_pool.close()
        await pool.close()


@pytest.mark.asyncio
async def test_postgres_usage_settlement_provenance_is_exact_and_replayable(
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
        repository = PostgresRunControlRepository(
            pool,
            family_writer_pool=family_writer_pool,
        )
        run_service, _ = family_service(repository)  # type: ignore[arg-type]
        admitted = await run_service.admit(
            request(request_id="postgres-usage-provenance")
        )
        assert admitted.run_id is not None
        run_id = admitted.run_id
        await run_service.execute(
            command(
                run_id,
                1,
                "postgres-provenance-effect-reservation",
                ReserveBudgetAction(
                    reservation_id="effect-reservation",
                    amounts={"tokens.total": 10},
                ),
            )
        )
        await run_service.execute(
            command(
                run_id,
                2,
                "postgres-provenance-effect-claim",
                ClaimEffectAction(
                    effect_id="effect-1",
                    effect_kind="external_write",
                    operation_ref="operation:1",
                    provider_idempotency_key="provider-effect-1",
                    reservation_id="effect-reservation",
                ),
            )
        )
        await run_service.execute(
            command(
                run_id,
                3,
                "postgres-provenance-unrelated-reservation",
                ReserveBudgetAction(
                    reservation_id="unrelated-reservation",
                    amounts={"tokens.total": 2},
                ),
            )
        )
        await run_service.execute(
            command(
                run_id,
                4,
                "postgres-provenance-unrelated-usage",
                RecordUsageAction(
                    usage_id="unrelated-usage",
                    reservation_id="unrelated-reservation",
                    actual_amounts={"tokens.total": 2},
                ),
            )
        )
        observed = await run_service.execute(
            command(
                run_id,
                5,
                "postgres-provenance-observation",
                authority_batch().actions[2],
            )
        )
        forged_action = authority_batch().actions[3].model_copy(
            update={"usage_settlement_ref": "unrelated-usage"}
        )
        forged = await run_service.execute(
            command(
                run_id,
                observed.resulting_run_version,
                "postgres-provenance-forged-effect-settlement",
                forged_action,
            )
        )
        assert forged.status == CommandStatus.REJECTED
        assert forged.reason_code == "usage_settlement_provenance_mismatch"
        wrong_authority_usage = await run_service.execute(
            command(
                run_id,
                observed.resulting_run_version,
                "postgres-provenance-wrong-authority-usage",
                RecordUsageAction(
                    usage_id="wrong-authority-usage",
                    authority_ref="operation:other",
                    reservation_id="effect-reservation",
                    actual_amounts={"tokens.total": 1},
                ),
            )
        )
        wrong_authority = await run_service.execute(
            command(
                run_id,
                wrong_authority_usage.resulting_run_version,
                "postgres-provenance-wrong-authority-effect",
                authority_batch().actions[3].model_copy(
                    update={"usage_settlement_ref": "wrong-authority-usage"}
                ),
            )
        )
        assert wrong_authority.status == CommandStatus.REJECTED
        assert wrong_authority.reason_code == "usage_settlement_authority_mismatch"
        pending = await run_service.execute(
            command(
                run_id,
                wrong_authority_usage.resulting_run_version,
                "postgres-provenance-pending-usage",
                RecordUsageAction(
                    usage_id="pending-usage",
                    authority_ref="operation:1",
                    reservation_id="effect-reservation",
                    actual_amounts={"tokens.total": 3},
                    pending_external_amounts={"tokens.total": 5},
                    release_amounts={"tokens.total": 1},
                ),
            )
        )
        empty = await run_service.execute(
            command(
                run_id,
                pending.resulting_run_version,
                "postgres-provenance-empty-settlement",
                SettlePendingUsageAction(
                    settlement_id="empty-settlement",
                    usage_id="pending-usage",
                    actual_amounts={},
                ),
            )
        )
        assert empty.status == CommandStatus.REJECTED
        assert empty.reason_code == "empty_usage_settlement"
        collision = await run_service.execute(
            command(
                run_id,
                pending.resulting_run_version,
                "postgres-provenance-colliding-settlement",
                SettlePendingUsageAction(
                    settlement_id="wrong-authority-usage",
                    usage_id="pending-usage",
                    actual_amounts={"tokens.total": 5},
                ),
            )
        )
        assert collision.status == CommandStatus.REJECTED
        assert collision.reason_code == "settlement_identity_collision"
        settled_usage = await run_service.execute(
            command(
                run_id,
                pending.resulting_run_version,
                "postgres-provenance-correct-settlement",
                SettlePendingUsageAction(
                    settlement_id="correct-settlement",
                    usage_id="pending-usage",
                    actual_amounts={"tokens.total": 5},
                ),
            )
        )
        double = await run_service.execute(
            command(
                run_id,
                settled_usage.resulting_run_version,
                "postgres-provenance-double-settlement",
                SettlePendingUsageAction(
                    settlement_id="second-settlement",
                    usage_id="pending-usage",
                    actual_amounts={"tokens.total": 5},
                ),
            )
        )
        assert double.status == CommandStatus.REJECTED
        assert double.reason_code == "usage_already_settled"
        settle_effect_command = command(
            run_id,
            settled_usage.resulting_run_version,
            "postgres-provenance-effect-settlement",
            authority_batch().actions[3].model_copy(
                update={"usage_settlement_ref": "correct-settlement"}
            ),
        )
        settled_effect = await run_service.execute(settle_effect_command)
        assert settled_effect.status == CommandStatus.ACCEPTED
        assert await run_service.execute(settle_effect_command) == settled_effect
        budget = await run_service.get_budget("tenant-1", run_id)
        provenance = budget.usage_settlements["correct-settlement"]
        assert provenance.usage_id == "pending-usage"
        assert provenance.reservation_id == "effect-reservation"
        assert "empty-settlement" not in budget.usage_settlements
        effects = await run_service.get_effects("tenant-1", run_id)
        assert effects.claims["effect-1"].settlement is not None
    finally:
        async with pool.acquire() as connection:
            await connection.execute("DROP SCHEMA IF EXISTS belllabs_control CASCADE")
        await family_writer_pool.close()
        await pool.close()


@pytest.mark.asyncio
async def test_postgres_authority_cas_ignores_set_and_map_construction_order(
    test_application_postgres_dsn: str,
) -> None:
    class ReorderedPostgresRepository(PostgresRunControlRepository):
        async def get_budget(self, request_scope: str, run_id: str) -> BudgetState:
            state = await super().get_budget(request_scope, run_id)
            return state.model_copy(
                update={
                    "usage_ids": frozenset(reversed(sorted(state.usage_ids))),
                    "settlement_ids": frozenset(
                        reversed(sorted(state.settlement_ids))
                    ),
                    "usage_records": dict(reversed(tuple(state.usage_records.items()))),
                    "usage_settlements": dict(
                        reversed(tuple(state.usage_settlements.items()))
                    ),
                }
            )

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
        repository = ReorderedPostgresRepository(
            pool,
            family_writer_pool=family_writer_pool,
        )
        run_service, _ = family_service(repository)  # type: ignore[arg-type]
        admitted = await run_service.admit(request(request_id="postgres-authority-order"))
        assert admitted.run_id is not None
        run_id = admitted.run_id
        identifiers = [f"identity-{index:02d}" for index in range(40)]
        settlements = [f"settlement-{item}" for item in identifiers]
        async with pool.acquire() as connection:
            await connection.execute(
                """
                UPDATE belllabs_control.budget_accounts
                SET state = jsonb_set(
                    jsonb_set(state, '{usage_ids}', $2::jsonb),
                    '{settlement_ids}', $3::jsonb
                )
                WHERE run_id = $1
                """,
                run_id,
                json.dumps(identifiers),
                json.dumps(settlements),
            )
        plain = await run_service.execute(
            command(
                run_id,
                1,
                "postgres-authority-order-plain",
                ReserveBudgetAction(
                    reservation_id="postgres-authority-order-plain",
                    amounts={"tokens.total": 1},
                ),
            )
        )
        assert plain.status == CommandStatus.ACCEPTED
        combined = await run_service.execute_family_admission(
            command(
                run_id,
                2,
                "postgres-authority-order-combined",
                ReserveBudgetAction(
                    reservation_id="postgres-authority-order-combined",
                    amounts={"tokens.total": 1},
                ),
            ),
            family_mutation(
                run_id,
                mutation_id="postgres-authority-order-combined",
            ),
        )
        assert combined.command_result.status == CommandStatus.ACCEPTED
    finally:
        async with pool.acquire() as connection:
            await connection.execute("DROP SCHEMA IF EXISTS belllabs_control CASCADE")
        await family_writer_pool.close()
        await pool.close()
