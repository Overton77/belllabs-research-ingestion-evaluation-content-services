"""RRM-009: the production composition's PostgreSQL reads and writes as restricted roles.

The journal opens unit-fenced claims as `mission_control_runtime` (canonical
operation_intent plus support operation_claim) and needs exactly its INSERT grant; the
boundary relay's read (`runs_with_pending_boundary_commands`) runs under the same role and
forced RLS; the family writer closes a cancelled run's boundary ledger with exactly its
grants (read commands, append delivery reports, advance the command lifecycle, never insert
commands). Every repository call runs as a restricted login, never as the owner.
"""

from __future__ import annotations

import asyncpg
import pytest

from mission_control.adapters.postgres.operations.operation_journal import (
    PostgresAtomicOperationJournalRepository,
)
from mission_control.adapters.postgres.run_control.run_control_repository import (
    PostgresRunControlRepository,
)
from mission_control.adapters.postgres.scope import apply_scope
from mission_control.application.execution.operations.journaled_operation_execution import (
    _claim_authority_command_id,
)
from mission_control.application.execution.operations.operation_journal import (
    OperationJournalMutation,
)
from mission_control.domain.authoring.canonical import sha256_digest
from mission_control.domain.policies.contracts import (
    CANCEL_SEQUENCE_SPACE,
    CancelAction,
    ClaimEffectAction,
    CommandStatus,
    RunPhase,
    StartAction,
)
from tests.fixtures.mission_control_common_db import CommonDatabase
from tests.integration.postgres.runtime_common import common_db as common_db
from tests.integration.postgres.runtime_common import (
    owner_rows,
    scoped_command,
    scoped_request,
)
from tests.integration.postgres.test_operation_journal_stage1 import _SCOPE, claim
from tests.unit.run_control.test_boundary_commands import TARGET, pause
from tests.unit.run_control.test_run_control import service

pytestmark = pytest.mark.common_db

RUNTIME_ROLE = "mission_control_runtime"
FAMILY_WRITER_ROLE = "mission_control_family_writer"
UNIT_KEY = "bl-unit-v1:" + "a" * 64


async def _started(run_service, db: CommonDatabase, request_id: str, tenant: str = "tenant-1"):  # type: ignore[no-untyped-def]
    admitted = await run_service.admit(scoped_request(db, tenant, request_id=request_id))
    assert admitted.run_id is not None
    started = await run_service.execute(
        scoped_command(
            db, admitted.run_id, 1, "start", StartAction(execution_target=TARGET), tenant
        )
    )
    assert started.status == CommandStatus.ACCEPTED
    return admitted.run_id


@pytest.mark.asyncio
async def test_runtime_role_opens_a_unit_fenced_claim_only_with_its_claim_grant(
    common_db: CommonDatabase,
) -> None:
    db = common_db
    runtime = await db.pool(max_size=2)
    _SCOPE[0] = db.scope()
    try:
        privileges = (
            await owner_rows(
                db,
                """
                SELECT
                  has_column_privilege($1, 'mission_control.operation_claim', 'unit_key',
                                       'INSERT') AS insert_unit_key,
                  has_column_privilege($1, 'mission_control.operation_claim', 'unit_key',
                                       'UPDATE') AS update_unit_key,
                  has_table_privilege($1, 'mission_control.operation_claim', 'DELETE')
                    AS delete_table,
                  (SELECT relforcerowsecurity FROM pg_class
                   WHERE oid = 'mission_control.operation_claim'::regclass) AS forced
                """,
                RUNTIME_ROLE,
            )
        )[0]
        # Least privilege: claims are inserted once; only lease/status columns change.
        assert dict(privileges) == {
            "insert_unit_key": True,
            "update_unit_key": False,
            "delete_table": False,
            "forced": True,
        }

        run_service, _ = service(PostgresRunControlRepository(runtime))  # type: ignore[arg-type]
        admitted = await run_service.admit(scoped_request(db, request_id="rrm009-runtime-claim"))
        assert admitted.run_id is not None
        operation_claim = claim(run_id=admitted.run_id).model_copy(update={"unit_key": UNIT_KEY})
        claim_command = scoped_command(
            db,
            admitted.run_id,
            1,
            _claim_authority_command_id(operation_claim),
            ClaimEffectAction(
                effect_id=operation_claim.effect_claim_id,
                effect_kind="operation.runtime",
                operation_ref=operation_claim.semantic_binding_id,
                provider_idempotency_key=operation_claim.idempotency_key,
                reservation_id="baseline",
                claim_payload_digest=sha256_digest(operation_claim.model_dump(mode="python")),
            ),
        )
        claimed = await run_service.execute(claim_command)
        assert claimed.status == CommandStatus.ACCEPTED
        mutation = OperationJournalMutation(
            request_scope=db.scope(),
            belllabs_run_id=admitted.run_id,
            expected_run_version=claimed.resulting_run_version,
            claim=operation_claim,
            authority_command=claim_command,
            authority_result=claimed,
        )
        journal = PostgresAtomicOperationJournalRepository(runtime)

        # Without the claim INSERT grant the runtime role cannot open any claim.
        await owner_rows(
            db, f"REVOKE INSERT ON mission_control.operation_claim FROM {RUNTIME_ROLE}"
        )
        with pytest.raises(asyncpg.InsufficientPrivilegeError):
            await journal.commit(mutation)
        await owner_rows(db, f"GRANT INSERT ON mission_control.operation_claim TO {RUNTIME_ROLE}")
        assert (await journal.commit(mutation)).status == "acquired"
        assert (await journal.commit(mutation)).status == "existing"
        async with runtime.acquire() as connection, connection.transaction():
            await apply_scope(connection, db.scope())
            assert (
                await connection.fetchval(
                    "SELECT unit_key FROM mission_control.operation_claim WHERE claim_key = $1",
                    operation_claim.effect_claim_id,
                )
                == UNIT_KEY
            )
    finally:
        _SCOPE[0] = "tenant-1"
        await runtime.close()


@pytest.mark.asyncio
async def test_relay_lists_pending_family_commands_under_the_runtime_role_and_scope(
    common_db: CommonDatabase,
) -> None:
    db = common_db
    runtime = await db.pool(max_size=4)
    tenant_1, tenant_2, tenant_3 = db.scope("tenant-1"), db.scope("tenant-2"), db.scope("tenant-3")
    try:
        run_service, _ = service(PostgresRunControlRepository(runtime))  # type: ignore[arg-type]
        quiet = await _started(run_service, db, "rrm009-relay-quiet")
        pending = await _started(run_service, db, "rrm009-relay-pending")
        assert quiet != pending
        accepted = await run_service.execute(scoped_command(db, pending, 2, "relay-pause", pause()))
        assert accepted.reason_code == "accepted_pending_application"

        # A run of another tenant with its own pending command.
        other = await _started(run_service, db, "rrm009-relay-other", "tenant-2")
        result = await run_service.execute(
            scoped_command(db, other, 2, "relay-pause-other", pause(), "tenant-2")
        )
        assert result.status == CommandStatus.ACCEPTED, result

        # The relay's read is scoped by forced RLS and lists only runs with pending commands.
        assert await run_service.runs_with_pending_boundary_commands(tenant_1) == (pending,)
        assert await run_service.runs_with_pending_boundary_commands(tenant_2) == (other,)
        assert await run_service.runs_with_pending_boundary_commands(tenant_3) == ()

        # RRM-008 composed: an accepted, undelivered cancel is pending in its own `cancel`
        # sequence space (it never consumes an `execution` sequence) and is listed too.
        cancelling = await _started(run_service, db, "rrm009-relay-cancel")
        cancelled = await run_service.execute(
            scoped_command(db, cancelling, 2, "relay-cancel", CancelAction())
        )
        assert cancelled.phase == RunPhase.CANCELLING
        (status,) = await run_service.list_boundary_commands(tenant_1, cancelling)
        assert status.command.target.sequence_space == CANCEL_SEQUENCE_SPACE
        assert [receipt.state.value for receipt in status.receipts] == ["accepted"]
        listed = await run_service.runs_with_pending_boundary_commands(tenant_1)
        assert sorted(listed) == sorted((pending, cancelling))
    finally:
        await runtime.close()


@pytest.mark.asyncio
async def test_family_writer_closes_the_ledger_of_a_cancelled_run_only_with_its_grants(
    common_db: CommonDatabase,
) -> None:
    """StageGraph terminalizes through the family admission, committed as the family writer:
    the terminal receipts of the run's boundary commands are written in that commit."""

    from tests.unit.run_control.test_rrm_009_family_terminal_receipts import (
        assert_ledger_closed,
        cancelled_run,
        terminal_family_service,
        terminalize_through_the_family,
    )

    db = common_db
    scope = db.scope()
    runtime = await db.pool(max_size=4)
    writer = await db.pool(FAMILY_WRITER_ROLE, max_size=2)
    try:
        privileges = (
            await owner_rows(
                db,
                """
                SELECT
                  has_table_privilege($1, 'mission_control.command', 'SELECT')
                    AS select_commands,
                  has_table_privilege($1, 'mission_control.command', 'INSERT')
                    AS insert_commands,
                  has_column_privilege($1, 'mission_control.command', 'lifecycle', 'UPDATE')
                    AS advance_commands,
                  has_column_privilege($1, 'mission_control.command', 'payload', 'UPDATE')
                    AS rewrite_commands,
                  has_table_privilege($1, 'mission_control.delivery_report', 'SELECT')
                    AS select_receipts,
                  has_table_privilege($1, 'mission_control.delivery_report', 'INSERT')
                    AS insert_receipts,
                  has_table_privilege($1, 'mission_control.delivery_report', 'UPDATE')
                    AS update_receipts,
                  has_table_privilege($1, 'mission_control.delivery_report', 'DELETE')
                    AS delete_receipts,
                  pg_has_role($2, $1, 'MEMBER') AS runtime_is_writer
                """,
                FAMILY_WRITER_ROLE,
                RUNTIME_ROLE,
            )
        )[0]
        # Least privilege: read commands, append reports, advance the lifecycle only.
        assert dict(privileges) == {
            "select_commands": True,
            "insert_commands": False,
            "advance_commands": True,
            "rewrite_commands": False,
            "select_receipts": True,
            "insert_receipts": True,
            "update_receipts": False,
            "delete_receipts": False,
            "runtime_is_writer": False,
        }
        run_service = terminal_family_service(
            PostgresRunControlRepository(runtime, family_writer_pool=writer)
        )
        run_id = await cancelled_run(run_service, scope)
        # Without its delivery-report grant the family writer cannot close the ledger, and
        # the whole terminalizing commit rolls back (the run stays cancelling).
        await owner_rows(
            db, f"REVOKE INSERT ON mission_control.delivery_report FROM {FAMILY_WRITER_ROLE}"
        )
        with pytest.raises(asyncpg.InsufficientPrivilegeError):
            await terminalize_through_the_family(run_service, run_id, scope)
        assert (await run_service.get_run(scope, run_id)).phase == RunPhase.CANCELLING
        await owner_rows(
            db, f"GRANT INSERT ON mission_control.delivery_report TO {FAMILY_WRITER_ROLE}"
        )
        receipt = await terminalize_through_the_family(run_service, run_id, scope)
        await assert_ledger_closed(run_service, run_id, receipt, scope)
    finally:
        await writer.close()
        await runtime.close()
