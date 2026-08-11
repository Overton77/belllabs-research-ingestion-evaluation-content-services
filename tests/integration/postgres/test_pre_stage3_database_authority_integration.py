from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from pathlib import Path

import asyncpg
import pytest

from app.application.operations.operation_journal_backfill import (
    CLAIMS_COLLECTION,
    BackfillBatch,
    QuarantineAdmission,
    SourceSnapshot,
)
from app.application.operations.postgres_operation_journal_backfill import (
    PostgresOperationJournalBackfillRepository,
)
from app.domain.run_control.errors import IdempotencyConflict
from app.integrations import postgres as postgres_integration
from app.integrations.postgres import MIGRATIONS_ROOT, apply_application_migrations

DIGEST = "sha256:" + "a" * 64
NOW = datetime(2026, 8, 8, 20, 0, tzinfo=UTC)

RLS_TABLES = (
    "runtime_execution_bindings",
    "runtime_execution_attempts",
    "runtime_checkpoint_observations",
    "runtime_intervention_commands",
    "runtime_interrupt_requests",
    "runtime_interrupt_decisions",
    "runtime_async_tasks",
    "operation_effect_claims",
    "operation_journal_mutations",
    "operation_execution_attempts",
    "operation_settlements",
    "operation_journal_backfill_batches",
    "operation_journal_backfill_applied_batches",
    "operation_journal_backfill_quarantine",
)
TABLE_PRIVILEGES = ("SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE", "REFERENCES", "TRIGGER")

RUNTIME_READ_WRITE = {
    "runtime_execution_bindings",
    "runtime_checkpoint_observations",
    "runtime_intervention_commands",
    "runtime_interrupt_requests",
    "runtime_interrupt_decisions",
    "runtime_async_tasks",
}
RUNTIME_APPEND = {
    "runtime_execution_attempts",
    "operation_journal_mutations",
    "operation_execution_attempts",
}
AGENT_READ = {
    "runtime_execution_bindings",
    "runtime_execution_attempts",
    "runtime_checkpoint_observations",
    "runtime_interrupt_requests",
    "runtime_async_tasks",
}
BACKFILL_TABLES = {
    "operation_journal_backfill_batches",
    "operation_journal_backfill_applied_batches",
    "operation_journal_backfill_quarantine",
}

RESTRICTED_COLUMN_GRANTS = {
    ("belllabs_control_runtime", "operation_effect_claims", "INSERT"): {
        "effect_claim_id",
        "request_scope",
        "belllabs_run_id",
        "operation_contract_digest",
        "idempotency_key",
        "request_digest",
        "semantic_binding_id",
        "semantic_binding_digest",
        "semantic_attempt_key",
        "claim_mode",
        "status",
        "claimed_by",
        "claimed_at",
        "heartbeat_at",
        "lease_expires_at",
    },
    ("belllabs_control_runtime", "operation_effect_claims", "UPDATE"): {
        "status",
        "heartbeat_at",
        "lease_expires_at",
    },
    ("belllabs_control_runtime", "operation_settlements", "INSERT"): {
        "settlement_id",
        "request_scope",
        "effect_claim_id",
        "settlement_revision",
        "settlement_digest",
        "status",
        "usage_payload",
        "pending_external_usage_payload",
        "result_manifest_ref",
        "result_manifest_digest",
        "result_manifest_size_bytes",
        "failure_code",
        "settlement_payload",
        "settled_at",
    },
    ("belllabs_operation_backfill", "operation_effect_claims", "UPDATE"): {
        "status",
        "heartbeat_at",
        "lease_expires_at",
    },
    ("belllabs_operation_backfill", "operation_journal_backfill_batches", "UPDATE"): {
        "status",
        "source_cursor",
        "source_claim_count",
        "source_settlement_count",
        "target_claim_count",
        "target_settlement_count",
        "quarantine_count",
        "source_aggregate_digest",
        "target_aggregate_digest",
        "updated_at",
        "completed_at",
        "failure_summary",
    },
    ("belllabs_operation_backfill", "operation_journal_backfill_quarantine", "UPDATE"): {
        "reason_code",
        "observed_digest",
        "expected_digest",
        "observed_request_scope",
    },
}


def _expected_table_privileges(role: str, table: str) -> set[str]:
    if role == "belllabs_control_runtime":
        if table in RUNTIME_READ_WRITE:
            return {"SELECT", "INSERT", "UPDATE"}
        if table in RUNTIME_APPEND:
            return {"SELECT", "INSERT"}
        if table in {"operation_effect_claims", "operation_settlements"}:
            return {"SELECT"}
        return set()
    if role == "belllabs_agent_runtime":
        return {"SELECT"} if table in AGENT_READ else set()
    if role == "belllabs_operations_readonly":
        return {"SELECT"}
    if role == "belllabs_operation_backfill":
        if table in BACKFILL_TABLES:
            return {"SELECT", "INSERT"}
        if table in {"operation_effect_claims", "operation_settlements"}:
            return {"SELECT", "INSERT"}
    return set()


async def _apply_through_0011(pool: asyncpg.Pool) -> None:
    paths = [
        path
        for path in sorted(MIGRATIONS_ROOT.glob("*.sql"))
        if "_capability_search" not in path.stem
        and path.name <= "0011_coordinator_workflow_results.sql"
    ]
    async with pool.acquire() as connection, connection.transaction():
        await connection.execute(
            """
            CREATE SCHEMA belllabs_control;
            CREATE TABLE belllabs_control.schema_migrations (
                version text PRIMARY KEY,
                applied_at timestamptz NOT NULL DEFAULT clock_timestamp()
            );
            """
        )
        for path in paths:
            await connection.execute(path.read_text(encoding="utf-8"))
            await connection.execute(
                "INSERT INTO belllabs_control.schema_migrations (version) VALUES ($1)",
                path.name,
            )


@pytest.mark.asyncio
async def test_upgrade_from_0011_applies_rls_and_least_privilege_role_matrix(
    test_application_postgres_dsn: str,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    pool = await asyncpg.create_pool(dsn=test_application_postgres_dsn, min_size=1, max_size=4)
    try:
        async with pool.acquire() as connection:
            await connection.execute("DROP SCHEMA IF EXISTS belllabs_control CASCADE")
        await _apply_through_0011(pool)
        async with pool.acquire() as connection:
            assert await connection.fetchval(
                "SELECT to_regclass('belllabs_control.operation_effect_claims')"
            ) is None

        isolated_migrations = tmp_path / "migrations"
        isolated_migrations.mkdir()
        for name in (
            "0012_graph_runtime_operation_journal.sql",
            "0013_legacy_operation_journal_backfill.sql",
        ):
            (isolated_migrations / name).write_text(
                (MIGRATIONS_ROOT / name).read_text(encoding="utf-8"),
                encoding="utf-8",
            )
        monkeypatch.setattr(postgres_integration, "MIGRATIONS_ROOT", isolated_migrations)
        await apply_application_migrations(pool)

        async with pool.acquire() as connection:
            versions = {
                row["version"]
                for row in await connection.fetch(
                    "SELECT version FROM belllabs_control.schema_migrations"
                )
            }
            assert {
                "0012_graph_runtime_operation_journal.sql",
                "0013_legacy_operation_journal_backfill.sql",
            } <= versions

            rls_rows = await connection.fetch(
                """
                SELECT cls.relname, cls.relrowsecurity, cls.relforcerowsecurity,
                       count(policy.policyname) AS policy_count
                FROM pg_class cls
                JOIN pg_namespace ns ON ns.oid = cls.relnamespace
                LEFT JOIN pg_policies policy
                  ON policy.schemaname = ns.nspname
                 AND policy.tablename = cls.relname
                 AND policy.policyname = 'request_scope_isolation'
                WHERE ns.nspname = 'belllabs_control'
                  AND cls.relname = ANY($1::text[])
                GROUP BY cls.relname, cls.relrowsecurity, cls.relforcerowsecurity
                """,
                list(RLS_TABLES),
            )
            assert {row["relname"] for row in rls_rows} == set(RLS_TABLES)
            assert all(
                row["relrowsecurity"]
                and row["relforcerowsecurity"]
                and row["policy_count"] == 1
                for row in rls_rows
            )

            role_rows = await connection.fetch(
                """
                SELECT rolname, rolsuper, rolcreatedb, rolcreaterole, rolbypassrls
                FROM pg_roles
                WHERE rolname = ANY($1::text[])
                """,
                [
                    "belllabs_control_runtime",
                    "belllabs_agent_runtime",
                    "belllabs_operations_readonly",
                    "belllabs_operation_backfill",
                ],
            )
            assert len(role_rows) == 4
            assert all(
                not row["rolsuper"]
                and not row["rolcreatedb"]
                and not row["rolcreaterole"]
                and not row["rolbypassrls"]
                for row in role_rows
            )

            roles = (
                "belllabs_control_runtime",
                "belllabs_agent_runtime",
                "belllabs_operations_readonly",
                "belllabs_operation_backfill",
            )
            for role in roles:
                for table in RLS_TABLES:
                    expected = _expected_table_privileges(role, table)
                    for privilege in TABLE_PRIVILEGES:
                        actual = await connection.fetchval(
                            "SELECT has_table_privilege($1, $2, $3)",
                            role,
                            f"belllabs_control.{table}",
                            privilege,
                        )
                        assert actual is (privilege in expected), (
                            role,
                            table,
                            privilege,
                            expected,
                        )

            for (role, table, privilege), allowed_columns in RESTRICTED_COLUMN_GRANTS.items():
                columns = {
                    row["column_name"]
                    for row in await connection.fetch(
                        """
                        SELECT column_name
                        FROM information_schema.columns
                        WHERE table_schema = 'belllabs_control' AND table_name = $1
                        """,
                        table,
                    )
                }
                actual_columns = {
                    column
                    for column in columns
                    if await connection.fetchval(
                        "SELECT has_column_privilege($1, $2, $3, $4)",
                        role,
                        f"belllabs_control.{table}",
                        column,
                        privilege,
                    )
                }
                assert actual_columns == allowed_columns

            for role in roles:
                expected_sequence = role == "belllabs_control_runtime"
                for privilege in ("USAGE", "SELECT", "UPDATE"):
                    actual = await connection.fetchval(
                        "SELECT has_sequence_privilege($1, $2, $3)",
                        role,
                        "belllabs_control.runtime_execution_attempts_attempt_id_seq",
                        privilege,
                    )
                    assert actual is (
                        expected_sequence and privilege in {"USAGE", "SELECT"}
                    )
    finally:
        async with pool.acquire() as connection:
            await connection.execute("DROP SCHEMA IF EXISTS belllabs_control CASCADE")
        await pool.close()


def _backfill_batch(*, reason_code: str = "missing_or_invalid_binding") -> BackfillBatch:
    snapshot = SourceSnapshot(
        request_scope="tenant-a",
        claim_high_watermark="claim-doc-1",
        settlement_high_watermark=None,
        record_count=1,
        aggregate_digest=DIGEST,
        captured_at=NOW,
    )
    return BackfillBatch(
        run_id="authority-proof-run",
        request_scope="tenant-a",
        previous_cursor=None,
        cursor=f"{CLAIMS_COLLECTION}:claim-doc-1",
        source_snapshot=snapshot,
        quarantines=(
            QuarantineAdmission(
                quarantine_id="quarantine-1",
                migration_stream="legacy-mongo-operation-journal-v1",
                source_collection=CLAIMS_COLLECTION,
                source_document_id="claim-doc-1",
                reason_code=reason_code,
                observed_digest=DIGEST,
                expected_digest=None,
                observed_request_scope="tenant-a",
                quarantined_at=NOW,
            ),
        ),
    )


@pytest.mark.asyncio
async def test_postgres_backfill_batch_concurrent_replay_is_idempotent_and_conflicts_fail(
    test_application_postgres_dsn: str,
) -> None:
    pool = await asyncpg.create_pool(dsn=test_application_postgres_dsn, min_size=1, max_size=4)
    try:
        async with pool.acquire() as connection:
            await connection.execute("DROP SCHEMA IF EXISTS belllabs_control CASCADE")
        await apply_application_migrations(pool)
        repository = PostgresOperationJournalBackfillRepository(pool)
        batch = _backfill_batch()

        await asyncio.gather(repository.apply_batch(batch), repository.apply_batch(batch))
        progress = await repository.load_progress(
            request_scope=batch.request_scope,
            run_id=batch.run_id,
        )
        assert progress is not None
        assert progress.cursor == batch.cursor
        assert progress.quarantine_count == 1

        with pytest.raises(IdempotencyConflict, match="conflicting content"):
            await repository.apply_batch(_backfill_batch(reason_code="invalid_digest"))
    finally:
        async with pool.acquire() as connection:
            await connection.execute("DROP SCHEMA IF EXISTS belllabs_control CASCADE")
        await pool.close()
