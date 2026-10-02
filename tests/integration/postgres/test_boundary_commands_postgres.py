"""RRM-007: boundary commands and receipts are PostgreSQL authority (migration 0023).

Receipts are durable rows in `belllabs_control.boundary_commands` and
`belllabs_control.boundary_command_receipts`, scoped by forced RLS, insert-only for the
runtime role and readable by the read-only operations role. The same acceptance,
delivery and application flow the in-memory adapter proves holds on PostgreSQL, including
the per-target sequence, the closed state machine and the single application of a command.

Opt-in through `TEST_APPLICATION_POSTGRES_DSN` (disposable stack only).
"""

from __future__ import annotations

import asyncpg
import pytest

from app.application.run_control.postgres_run_control_repository import PostgresRunControlRepository
from app.domain.run_control.contracts import (
    CommandStatus,
    ReceiptState,
    RunPhase,
    SatisfyWaitAction,
    SetWaitAction,
)
from app.domain.run_control.errors import ReceiptTransitionRejected
from app.integrations.postgres import apply_application_migrations
from tests.unit.run_control.test_boundary_commands import (
    TARGET,
    apply,
    boundary_command,
    declared_wait,
    delivered,
    pause,
    resume,
    started,
    states,
)
from tests.unit.run_control.test_run_control import command, request, service

RUNTIME_ROLE = "belllabs_control_runtime"
READONLY_ROLE = "belllabs_operations_readonly"


@pytest.mark.asyncio
async def test_receipts_are_durable_scoped_and_role_bounded(
    test_application_postgres_dsn: str,
) -> None:
    pool = await asyncpg.create_pool(dsn=test_application_postgres_dsn, min_size=1, max_size=4)
    try:
        async with pool.acquire() as connection:
            await connection.execute("DROP SCHEMA IF EXISTS belllabs_control CASCADE")
        await apply_application_migrations(pool)
        async with pool.acquire() as connection:
            applied = {
                row["version"]
                for row in await connection.fetch(
                    "SELECT version FROM belllabs_control.schema_migrations"
                )
            }
        assert "0023_boundary_command_receipts_v1.sql" in applied

        run_service, _ = service(PostgresRunControlRepository(pool))  # type: ignore[arg-type]
        run_id = await started(run_service, "postgres-boundary", TARGET)
        # A second scope's run: its receipts are invisible under the first scope.
        other_service, _ = service(PostgresRunControlRepository(pool))  # type: ignore[arg-type]
        other = await other_service.admit(
            request(request_scope="tenant-2", request_id="postgres-boundary-other")
        )
        assert other.run_id is not None

        accepted = await run_service.execute(command(run_id, 2, "pause", pause()))
        assert accepted.status == CommandStatus.ACCEPTED
        assert accepted.reason_code == "accepted_pending_application"
        wait = await run_service.execute(
            command(
                run_id,
                2,
                "declare",
                SetWaitAction(condition=declared_wait(), runnable_work_remains=True),
            )
        )
        assert wait.resulting_run_version == 3
        release = await run_service.execute(
            command(
                run_id,
                3,
                "release",
                SatisfyWaitAction(
                    condition_id=declared_wait().condition_id,
                    verification_evidence_ref="evidence:operator",
                ),
            )
        )
        assert release.status == CommandStatus.ACCEPTED
        stale = await run_service.execute(command(run_id, 1, "stale", pause("p-stale")))
        assert stale.status == CommandStatus.STALE
        listed = await run_service.list_boundary_commands("tenant-1", run_id)
        assert [
            (item.command.command_id, item.command.target_sequence, item.state.value)
            for item in listed
        ] == [("stale", 0, "rejected"), ("pause", 1, "accepted"), ("release", 2, "accepted")]

        pause_status = listed[1]
        delivered_status = await run_service.record_boundary_receipt(
            "tenant-1", delivered(pause_status)
        )
        assert states(delivered_status) == ["accepted", "delivered"]
        assert await run_service.record_boundary_receipt(
            "tenant-1", delivered(pause_status)
        ) == delivered_status
        applied = await run_service.execute(
            boundary_command(run_id, 3, "apply:pause", apply("pause", pause()))
        )
        assert (applied.status, applied.phase, applied.resulting_run_version) == (
            CommandStatus.ACCEPTED,
            RunPhase.PAUSED,
            4,
        )
        duplicate = await run_service.execute(
            boundary_command(run_id, 4, "apply:pause:again", apply("pause", pause()))
        )
        assert duplicate.reason_code == "boundary_command_already_applied"
        status = await run_service.get_boundary_command("tenant-1", run_id, "pause")
        assert status is not None
        assert states(status) == ["accepted", "delivered", "applied"]
        assert status.receipts[-1].applied_run_version == 4
        with pytest.raises(ReceiptTransitionRejected):
            await run_service.record_boundary_receipt(
                "tenant-1",
                delivered(pause_status).model_copy(
                    update={"state": ReceiptState.REJECTED, "rejection_reason": "superseded"}
                ),
            )
        resumed_accept = await run_service.execute(command(run_id, 4, "resume", resume()))
        assert resumed_accept.status == CommandStatus.ACCEPTED
        resumed = await run_service.execute(
            boundary_command(run_id, 4, "apply:resume", apply("resume", resume(), runnable=True))
        )
        assert resumed.phase == RunPhase.ACTIVE
        assert (await run_service.reconstruct_projection("tenant-1", run_id)) == (
            await run_service.get_run("tenant-1", run_id)
        )

        # Forced RLS: the runtime role sees only its scope and may insert but never update
        # or delete; the read-only role may only read.
        async with pool.acquire() as connection, connection.transaction():
            await connection.execute(f"SET LOCAL ROLE {RUNTIME_ROLE}")
            await connection.execute(
                "SELECT set_config('belllabs.request_scope', 'tenant-1', true)"
            )
            assert (
                await connection.fetchval(
                    "SELECT count(*) FROM belllabs_control.boundary_commands"
                )
                == 4
            )
            assert (
                await connection.fetchval(
                    "SELECT count(*) FROM belllabs_control.boundary_command_receipts"
                )
                == 8
            ), "stale: 1; pause: 3; release: 1; resume: 3 (delivered recorded by the boundary)"
            await connection.execute(
                "SELECT set_config('belllabs.request_scope', 'tenant-2', true)"
            )
            assert (
                await connection.fetchval(
                    "SELECT count(*) FROM belllabs_control.boundary_commands"
                )
                == 0
            )
            privileges = {
                (row["privilege_type"])
                for row in await connection.fetch(
                    """
                    SELECT privilege_type FROM information_schema.role_table_grants
                    WHERE grantee = $1 AND table_schema = 'belllabs_control'
                      AND table_name = 'boundary_command_receipts'
                    """,
                    RUNTIME_ROLE,
                )
            }
            assert privileges == {"SELECT", "INSERT"}
            with pytest.raises(asyncpg.InsufficientPrivilegeError):
                await connection.execute(
                    "UPDATE belllabs_control.boundary_command_receipts SET state = 'applied'"
                )
        async with pool.acquire() as connection, connection.transaction(readonly=True):
            await connection.execute(f"SET LOCAL ROLE {READONLY_ROLE}")
            await connection.execute(
                "SELECT set_config('belllabs.request_scope', 'tenant-1', true)"
            )
            rows = await connection.fetch(
                """
                SELECT command_id, state FROM belllabs_control.boundary_command_receipts
                WHERE command_id = 'pause' ORDER BY ordinal
                """
            )
            assert [(row["command_id"], row["state"]) for row in rows] == [
                ("pause", "accepted"),
                ("pause", "delivered"),
                ("pause", "applied"),
            ]
            privileges = {
                (row["privilege_type"])
                for row in await connection.fetch(
                    """
                    SELECT privilege_type FROM information_schema.role_table_grants
                    WHERE grantee = $1 AND table_schema = 'belllabs_control'
                      AND table_name IN ('boundary_commands', 'boundary_command_receipts')
                    """,
                    READONLY_ROLE,
                )
            }
            assert privileges == {"SELECT"}
    finally:
        await pool.close()
