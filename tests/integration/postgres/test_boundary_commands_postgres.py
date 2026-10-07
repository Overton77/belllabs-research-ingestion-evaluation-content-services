"""RRM-007: boundary commands and receipts are PostgreSQL authority on mission_control.

Accepted commands are canonical ``mission_control.command`` rows; delivered, applied and
rejected receipts are immutable ``mission_control.delivery_report`` rows. Forced RLS
scopes both; the runtime role may insert commands and reports but never mutate a report,
and the read-only role may only read. The same acceptance, delivery and application flow
the in-memory adapter proves holds on PostgreSQL, including the per-target sequence, the
closed state machine and the single application of a command.
"""

from __future__ import annotations

import asyncio

import asyncpg
import pytest

from mission_control.adapters.postgres.run_control.run_control_repository import (
    PostgresRunControlRepository,
)
from mission_control.adapters.postgres.scope import apply_scope
from mission_control.domain.policies.contracts import (
    CommandStatus,
    ReceiptState,
    RunPhase,
    SatisfyWaitAction,
    SetWaitAction,
    StartAction,
)
from mission_control.domain.policies.errors import ReceiptTransitionRejected
from tests.fixtures.mission_control_common_db import CommonDatabase
from tests.integration.postgres.runtime_common import common_db as common_db
from tests.integration.postgres.runtime_common import (
    owner_rows,
    scoped_command,
    scoped_request,
)
from tests.unit.run_control.test_boundary_commands import (
    TARGET,
    apply,
    boundary_command,
    declared_wait,
    delivered,
    pause,
    resume,
    states,
)
from tests.unit.run_control.test_run_control import service

pytestmark = pytest.mark.common_db

RUNTIME_ROLE = "mission_control_runtime"
READONLY_ROLE = "mission_control_readonly"


@pytest.mark.asyncio
async def test_receipts_are_durable_scoped_and_role_bounded(common_db: CommonDatabase) -> None:
    db = common_db
    scope = db.scope("tenant-1")

    def command(run_id: str, version: int, command_id: str, action: object):  # type: ignore[no-untyped-def]
        return scoped_command(db, run_id, version, command_id, action)

    def family(run_id: str, version: int, command_id: str, action: object):  # type: ignore[no-untyped-def]
        return boundary_command(run_id, version, command_id, action).model_copy(
            update={"request_scope": scope}
        )

    pool = await db.pool("mission_control_runtime")
    readonly = await db.pool("mission_control_readonly")
    try:
        run_service, _ = service(PostgresRunControlRepository(pool))  # type: ignore[arg-type]
        admitted = await run_service.admit(scoped_request(db, request_id="postgres-boundary"))
        assert admitted.run_id is not None
        run_id = admitted.run_id
        started = await run_service.execute(
            command(run_id, 1, "start", StartAction(execution_target=TARGET))
        )
        assert started.status == CommandStatus.ACCEPTED
        # A second scope's run: its receipts are invisible under the first scope.
        other = await run_service.admit(
            scoped_request(db, "tenant-2", request_id="postgres-boundary-other")
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
        listed = await run_service.list_boundary_commands(scope, run_id)
        assert [
            (item.command.command_id, item.command.target_sequence, item.state.value)
            for item in listed
        ] == [("stale", 0, "rejected"), ("pause", 1, "accepted"), ("release", 2, "accepted")]
        assert await run_service.runs_with_pending_boundary_commands(scope) == (run_id,)

        pause_status = listed[1]
        delivered_status = await run_service.record_boundary_receipt(scope, delivered(pause_status))
        assert states(delivered_status) == ["accepted", "delivered"]
        assert (
            await run_service.record_boundary_receipt(scope, delivered(pause_status))
            == delivered_status
        )
        applied = await run_service.execute(
            family(run_id, 3, "apply:pause", apply("pause", pause()))
        )
        assert (applied.status, applied.phase, applied.resulting_run_version) == (
            CommandStatus.ACCEPTED,
            RunPhase.PAUSED,
            4,
        )
        duplicate = await run_service.execute(
            family(run_id, 4, "apply:pause:again", apply("pause", pause()))
        )
        assert duplicate.reason_code == "boundary_command_already_applied"
        status = await run_service.get_boundary_command(scope, run_id, "operator", "pause")
        assert status is not None
        assert states(status) == ["accepted", "delivered", "applied"]
        assert status.receipts[-1].applied_run_version == 4
        with pytest.raises(ReceiptTransitionRejected):
            await run_service.record_boundary_receipt(
                scope,
                delivered(pause_status).model_copy(
                    update={"state": ReceiptState.REJECTED, "rejection_reason": "superseded"}
                ),
            )
        resumed_accept = await run_service.execute(command(run_id, 4, "resume", resume()))
        assert resumed_accept.status == CommandStatus.ACCEPTED
        resumed = await run_service.execute(
            family(run_id, 4, "apply:resume", apply("resume", resume(), runnable=True))
        )
        assert resumed.phase == RunPhase.ACTIVE
        assert (await run_service.reconstruct_projection(scope, run_id)) == (
            await run_service.get_run(scope, run_id)
        )

        # F8: concurrent appends for one command keep the state machine valid.
        raced = await run_service.execute(command(run_id, 5, "raced", pause("p-raced")))
        assert raced.status == CommandStatus.ACCEPTED
        raced_status = await run_service.get_boundary_command(scope, run_id, "operator", "raced")
        assert raced_status is not None
        first, second = await asyncio.gather(
            run_service.record_boundary_receipt(scope, delivered(raced_status)),
            run_service.record_boundary_receipt(scope, delivered(raced_status)),
        )
        assert states(first) == states(second) == ["accepted", "delivered"]
        applied_receipt = delivered(raced_status).model_copy(
            update={
                "state": ReceiptState.APPLIED,
                "recorded_by": "boundary",
                "applied_run_version": 5,
            }
        )
        outcomes = await asyncio.gather(
            run_service.record_boundary_receipt(scope, applied_receipt),
            run_service.record_boundary_receipt(scope, applied_receipt),
            run_service.record_boundary_receipt(
                scope,
                delivered(raced_status).model_copy(
                    update={"state": ReceiptState.REJECTED, "rejection_reason": "superseded"}
                ),
            ),
            return_exceptions=True,
        )
        assert sum(isinstance(item, ReceiptTransitionRejected) for item in outcomes) == 1
        final = await run_service.get_boundary_command(scope, run_id, "operator", "raced")
        assert final is not None and states(final) == ["accepted", "delivered", "applied"]
        # Acceptance is the command row itself; delivery and application are distinct
        # immutable delivery reports; the command lifecycle follows the latest receipt.
        rows = await owner_rows(
            db,
            """
            SELECT c.lifecycle, r.observed_outcome, (r.detail->>'ordinal')::int AS ordinal
            FROM mission_control.command c
            JOIN mission_control.delivery_report r
              ON r.installation_id = c.installation_id AND r.application_id = c.application_id
             AND r.tenant_id = c.tenant_id AND r.command_id = c.command_id
            WHERE c.payload->'record'->>'command_id' = 'raced'
            ORDER BY ordinal
            """,
        )
        assert [(row["lifecycle"], row["observed_outcome"], row["ordinal"]) for row in rows] == [
            ("applied", "delivered", 2),
            ("applied", "applied", 3),
        ]

        # Forced RLS: the runtime role sees only its scope and may insert reports but never
        # update or delete them; the read-only role may only read.
        async with pool.acquire() as connection, connection.transaction():
            await apply_scope(connection, scope)
            assert (
                await connection.fetchval(
                    "SELECT count(*) FROM mission_control.command WHERE target_kind IS NOT NULL"
                )
                == 5
            )
            assert await connection.fetchval(
                "SELECT count(*) FROM mission_control.delivery_report"
            ) == (11 - 5), "11 receipts: one acceptance or rejection per command is the row"
            await apply_scope(connection, db.scope("tenant-2"))
            assert await connection.fetchval("SELECT count(*) FROM mission_control.command") == 0
            privileges = {
                row["privilege_type"]
                for row in await connection.fetch(
                    """
                    SELECT privilege_type FROM information_schema.role_table_grants
                    WHERE grantee = $1 AND table_schema = 'mission_control'
                      AND table_name = 'delivery_report'
                    """,
                    RUNTIME_ROLE,
                )
            }
            assert privileges == {"SELECT", "INSERT"}
        async with pool.acquire() as connection, connection.transaction():
            await apply_scope(connection, scope)
            with pytest.raises(asyncpg.InsufficientPrivilegeError):
                await connection.execute(
                    "UPDATE mission_control.delivery_report SET observed_outcome = 'applied'"
                )
        async with readonly.acquire() as connection, connection.transaction(readonly=True):
            await apply_scope(connection, scope)
            rows = await connection.fetch(
                """
                SELECT r.observed_outcome FROM mission_control.delivery_report r
                JOIN mission_control.command c USING (installation_id, application_id,
                                                      tenant_id, command_id)
                WHERE c.payload->'record'->>'command_id' = 'pause'
                ORDER BY (r.detail->>'ordinal')::int
                """
            )
            assert [row["observed_outcome"] for row in rows] == ["delivered", "applied"]
            privileges = {
                row["privilege_type"]
                for row in await connection.fetch(
                    """
                    SELECT privilege_type FROM information_schema.role_table_grants
                    WHERE grantee = $1 AND table_schema = 'mission_control'
                      AND table_name IN ('command', 'delivery_report')
                    """,
                    READONLY_ROLE,
                )
            }
            assert privileges == {"SELECT"}
    finally:
        await pool.close()
        await readonly.close()
