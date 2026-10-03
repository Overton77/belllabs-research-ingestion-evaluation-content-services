"""RRM-009: the production composition's PostgreSQL reads and writes as the runtime role.

Migration 0025 grants `belllabs_control_runtime` the column INSERT on
`operation_effect_claims.unit_key` that the journal repository writes on every claim (the
0013 column grant predates the 0019 column). The boundary relay's read
(`runs_with_pending_boundary_commands`) runs under the same role and forced RLS. Every
repository call below runs after `SET ROLE belllabs_control_runtime`, never as the owner.

Opt-in through `TEST_APPLICATION_POSTGRES_DSN` (disposable stack only).
"""

from __future__ import annotations

import asyncpg
import pytest

from mission_control.adapters.postgres.connections import MIGRATIONS_ROOT
from mission_control.adapters.postgres.operations.operation_journal import (
    PostgresAtomicOperationJournalRepository,
)
from mission_control.adapters.postgres.run_control.run_control_repository import (
    PostgresRunControlRepository,
)
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
from tests.integration.postgres.test_checkpoint_lineage_postgres import (
    require_disposable_postgres,
    reset_application_schema,
)
from tests.integration.postgres.test_operation_journal_stage1 import claim
from tests.unit.run_control.test_boundary_commands import TARGET, pause, started
from tests.unit.run_control.test_run_control import command, request, service

RUNTIME_ROLE = "belllabs_control_runtime"
MIGRATION = "0025_operation_claim_unit_key_runtime_grant_v1.sql"
UNIT_KEY = "bl-unit-v1:" + "a" * 64


async def _runtime_role(connection: asyncpg.Connection) -> None:
    await connection.execute(f"SET ROLE {RUNTIME_ROLE}")


@pytest.mark.asyncio
async def test_runtime_role_opens_a_unit_fenced_claim_only_with_migration_0025(
    test_application_postgres_dsn: str,
) -> None:
    require_disposable_postgres(test_application_postgres_dsn)
    owner = await asyncpg.create_pool(dsn=test_application_postgres_dsn, min_size=1, max_size=2)
    runtime = await asyncpg.create_pool(
        dsn=test_application_postgres_dsn, min_size=1, max_size=2, setup=_runtime_role
    )
    try:
        await reset_application_schema(owner)
        async with owner.acquire() as connection:
            assert await connection.fetchval(
                "SELECT version FROM belllabs_control.schema_migrations WHERE version = $1",
                MIGRATION,
            )
            privileges = await connection.fetchrow(
                """
                SELECT
                  has_column_privilege($1, 'belllabs_control.operation_effect_claims',
                                       'unit_key', 'INSERT') AS insert_unit_key,
                  has_column_privilege($1, 'belllabs_control.operation_effect_claims',
                                       'unit_key', 'UPDATE') AS update_unit_key,
                  has_table_privilege($1, 'belllabs_control.operation_effect_claims',
                                      'INSERT') AS insert_table,
                  (SELECT relforcerowsecurity FROM pg_class
                   WHERE oid = 'belllabs_control.operation_effect_claims'::regclass) AS forced
                """,
                RUNTIME_ROLE,
            )
        # Least privilege: one column of INSERT, no UPDATE of it, no table-wide INSERT,
        # and forced row-level security kept.
        assert dict(privileges) == {
            "insert_unit_key": True,
            "update_unit_key": False,
            "insert_table": False,
            "forced": True,
        }

        run_service, _ = service(PostgresRunControlRepository(runtime))  # type: ignore[arg-type]
        admitted = await run_service.admit(request(request_id="rrm009-runtime-claim"))
        assert admitted.run_id is not None
        operation_claim = claim(run_id=admitted.run_id).model_copy(update={"unit_key": UNIT_KEY})
        claim_command = command(
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
            request_scope="tenant-1",
            belllabs_run_id=admitted.run_id,
            expected_run_version=claimed.resulting_run_version,
            claim=operation_claim,
            authority_command=claim_command,
            authority_result=claimed,
        )
        journal = PostgresAtomicOperationJournalRepository(runtime)

        # Without the 0025 grant the runtime role cannot open any claim: the repository
        # always writes the `unit_key` column.
        async with owner.acquire() as connection:
            await connection.execute(
                "REVOKE INSERT (unit_key) ON belllabs_control.operation_effect_claims "
                f"FROM {RUNTIME_ROLE}"
            )
        with pytest.raises(asyncpg.InsufficientPrivilegeError):
            await journal.commit(mutation)
        async with owner.acquire() as connection:
            await connection.execute((MIGRATIONS_ROOT / MIGRATION).read_text(encoding="utf-8"))
        assert (await journal.commit(mutation)).status == "acquired"
        assert (await journal.commit(mutation)).status == "existing"
        async with runtime.acquire() as connection, connection.transaction():
            await connection.execute(
                "SELECT set_config('belllabs.request_scope', 'tenant-1', true)"
            )
            assert (
                await connection.fetchval(
                    "SELECT unit_key FROM belllabs_control.operation_effect_claims "
                    "WHERE effect_claim_id = $1",
                    operation_claim.effect_claim_id,
                )
                == UNIT_KEY
            )
    finally:
        await runtime.close()
        await owner.close()


@pytest.mark.asyncio
async def test_relay_lists_pending_family_commands_under_the_runtime_role_and_scope(
    test_application_postgres_dsn: str,
) -> None:
    require_disposable_postgres(test_application_postgres_dsn)
    owner = await asyncpg.create_pool(dsn=test_application_postgres_dsn, min_size=1, max_size=2)
    runtime = await asyncpg.create_pool(
        dsn=test_application_postgres_dsn, min_size=1, max_size=4, setup=_runtime_role
    )
    try:
        await reset_application_schema(owner)
        repository = PostgresRunControlRepository(runtime)
        run_service, _ = service(repository)  # type: ignore[arg-type]
        quiet = await started(run_service, "rrm009-relay-quiet", TARGET)
        pending = await started(run_service, "rrm009-relay-pending", TARGET)
        assert quiet != pending
        accepted = await run_service.execute(command(pending, 2, "relay-pause", pause()))
        assert accepted.reason_code == "accepted_pending_application"

        # A run of another scope with its own pending command.
        other = await run_service.admit(
            request(request_scope="tenant-2", request_id="rrm009-relay-other")
        )
        assert other.run_id is not None
        for version, command_id, action in (
            (1, "start", StartAction(execution_target=TARGET)),
            (2, "relay-pause-other", pause()),
        ):
            result = await run_service.execute(
                command(other.run_id, version, command_id, action).model_copy(
                    update={"request_scope": "tenant-2"}
                )
            )
            assert result.status == CommandStatus.ACCEPTED, result

        # The relay's read is scoped by forced RLS and lists only runs with pending commands.
        assert await run_service.runs_with_pending_boundary_commands("tenant-1") == (pending,)
        assert await run_service.runs_with_pending_boundary_commands("tenant-2") == (other.run_id,)
        assert await run_service.runs_with_pending_boundary_commands("tenant-3") == ()

        # RRM-008 composed: an accepted, undelivered cancel is pending in its own `cancel`
        # sequence space (it never consumes an `execution` sequence) and is listed too.
        cancelling = await started(run_service, "rrm009-relay-cancel", TARGET)
        cancelled = await run_service.execute(
            command(cancelling, 2, "relay-cancel", CancelAction())
        )
        assert cancelled.phase == RunPhase.CANCELLING
        (status,) = await run_service.list_boundary_commands("tenant-1", cancelling)
        assert status.command.target.sequence_space == CANCEL_SEQUENCE_SPACE
        assert [receipt.state.value for receipt in status.receipts] == ["accepted"]
        # Both runs' commands carry the fixture's single clock value, so acceptance order
        # ties: the set is exact, each run once.
        listed = await run_service.runs_with_pending_boundary_commands("tenant-1")
        assert sorted(listed) == sorted((pending, cancelling))
    finally:
        await runtime.close()
        await owner.close()


FAMILY_WRITER_ROLE = "belllabs_family_repository_writer"
TERMINAL_RECEIPTS_MIGRATION = "0026_family_writer_terminal_boundary_receipts_v1.sql"


async def _family_writer_role(connection: asyncpg.Connection) -> None:
    await connection.execute(f"SET ROLE {FAMILY_WRITER_ROLE}")


@pytest.mark.asyncio
async def test_family_writer_closes_the_ledger_of_a_cancelled_run_only_with_migration_0026(
    test_application_postgres_dsn: str,
) -> None:
    """StageGraph terminalizes through the family admission, committed as the family writer:
    the terminal receipts of the run's boundary commands are written in that commit."""

    from tests.unit.run_control.test_rrm_009_family_terminal_receipts import (
        assert_ledger_closed,
        cancelled_run,
        terminal_family_service,
        terminalize_through_the_family,
    )

    require_disposable_postgres(test_application_postgres_dsn)
    owner = await asyncpg.create_pool(dsn=test_application_postgres_dsn, min_size=1, max_size=2)
    runtime = await asyncpg.create_pool(
        dsn=test_application_postgres_dsn, min_size=1, max_size=4, setup=_runtime_role
    )
    writer = await asyncpg.create_pool(
        dsn=test_application_postgres_dsn, min_size=1, max_size=2, setup=_family_writer_role
    )
    try:
        await reset_application_schema(owner)
        async with owner.acquire() as connection:
            assert await connection.fetchval(
                "SELECT version FROM belllabs_control.schema_migrations WHERE version = $1",
                TERMINAL_RECEIPTS_MIGRATION,
            )
            privileges = await connection.fetchrow(
                """
                SELECT
                  has_table_privilege($1, 'belllabs_control.boundary_commands', 'SELECT')
                    AS select_commands,
                  has_table_privilege($1, 'belllabs_control.boundary_commands', 'INSERT')
                    AS insert_commands,
                  has_table_privilege($1, 'belllabs_control.boundary_command_receipts',
                                      'SELECT') AS select_receipts,
                  has_table_privilege($1, 'belllabs_control.boundary_command_receipts',
                                      'INSERT') AS insert_receipts,
                  has_table_privilege($1, 'belllabs_control.boundary_command_receipts',
                                      'UPDATE') AS update_receipts,
                  has_table_privilege($1, 'belllabs_control.boundary_command_receipts',
                                      'DELETE') AS delete_receipts
                """,
                FAMILY_WRITER_ROLE,
            )
        # Least privilege: read both insert-only ledgers, append receipts, nothing else.
        assert dict(privileges) == {
            "select_commands": True,
            "insert_commands": False,
            "select_receipts": True,
            "insert_receipts": True,
            "update_receipts": False,
            "delete_receipts": False,
        }
        run_service = terminal_family_service(
            PostgresRunControlRepository(runtime, family_writer_pool=writer)
        )
        run_id = await cancelled_run(run_service)
        # Without the 0026 grants the family writer cannot close the ledger, and the whole
        # terminalizing commit rolls back (the run stays cancelling).
        async with owner.acquire() as connection:
            await connection.execute(
                "REVOKE SELECT ON belllabs_control.boundary_commands, "
                "belllabs_control.boundary_command_receipts "
                f"FROM {FAMILY_WRITER_ROLE}"
            )
            await connection.execute(
                "REVOKE INSERT ON belllabs_control.boundary_command_receipts "
                f"FROM {FAMILY_WRITER_ROLE}"
            )
        with pytest.raises(asyncpg.InsufficientPrivilegeError):
            await terminalize_through_the_family(run_service, run_id)
        assert (await run_service.get_run("tenant-1", run_id)).phase == RunPhase.CANCELLING
        async with owner.acquire() as connection:
            await connection.execute(
                (MIGRATIONS_ROOT / TERMINAL_RECEIPTS_MIGRATION).read_text(encoding="utf-8")
            )
        receipt = await terminalize_through_the_family(run_service, run_id)
        await assert_ledger_closed(run_service, run_id, receipt)
    finally:
        await writer.close()
        await runtime.close()
        await owner.close()
