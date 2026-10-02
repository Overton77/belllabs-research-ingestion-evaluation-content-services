"""RRM-005 PostgreSQL inspection reads (REQ-CP-RUN-011/012, migration 0022).

The same scoped reads are served under the API runtime role (`belllabs_control_runtime`)
and the read-only operations role (`belllabs_operations_readonly`), each in one READ ONLY
transaction. Every table of the application schema is unchanged by the reads. Runs
against the disposable stack only.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from datetime import UTC, datetime

import asyncpg
import pytest
from langgraph.checkpoint.memory import InMemorySaver

from app.application.operations.postgres_checkpoint_lineage import (
    PostgresCheckpointLineageRepository,
)
from app.application.run_control.inspection import (
    InspectionCursorCodec,
    RuntimeInspectionService,
)
from app.application.run_control.postgres_inspection_repository import (
    PostgresInspectionReadRepository,
)
from app.application.run_control.postgres_run_control_repository import PostgresRunControlRepository
from app.domain.run_control.contracts import RunPhase
from app.domain.run_control.inspection import (
    CheckpointNotInUnitLineage,
    InspectionNotFound,
    InvalidInspectionCursor,
)
from app.integrations.agents.deep_agents.checkpoint_history import (
    LangGraphCheckpointHistoryReader,
)
from tests.fixtures.checkpoint_lineage import BINDING, CHECKPOINTER
from tests.fixtures.runtime_inspection import (
    PROMPT_TEXT,
    READ_AT,
    SECRET_TEXT,
    MutableClock,
    SeededRuns,
    seed_inspection_world,
)
from tests.integration.postgres.test_checkpoint_lineage_postgres import (
    require_disposable_postgres,
    reset_application_schema,
)
from tests.unit.run_control.test_run_control import service as run_control_service

RUNTIME_ROLE = "belllabs_control_runtime"
READONLY_ROLE = "belllabs_operations_readonly"


def _assume(role: str) -> Callable[[asyncpg.Connection], Awaitable[None]]:
    async def setup(connection: asyncpg.Connection) -> None:
        await connection.execute(f"SET ROLE {role}")

    return setup


async def _schema_digest(pool: asyncpg.Pool) -> dict[str, str]:
    """A content digest of every application table (the owner bypasses RLS)."""

    async with pool.acquire() as connection:
        tables = [
            row["table_name"]
            for row in await connection.fetch(
                """
                SELECT table_name FROM information_schema.tables
                WHERE table_schema = 'belllabs_control' AND table_type = 'BASE TABLE'
                ORDER BY table_name
                """
            )
        ]
        digests: dict[str, str] = {}
        for table in tables:
            digests[table] = await connection.fetchval(
                f"""
                SELECT md5(coalesce(string_agg(t::text, '|' ORDER BY t::text), ''))
                FROM belllabs_control.{table} t
                """
            )
    return digests


async def _seed_async_child(pool: asyncpg.Pool, run_id: str, operation_id: str) -> None:
    """Parent authority rows as the 0016 async-subagent authority writes them."""

    now = datetime.now(UTC)
    async with pool.acquire() as connection, connection.transaction():
        await connection.execute(
            """
            INSERT INTO belllabs_control.async_subagent_authority (
                request_scope, child_execution_id, parent_run_id, parent_operation_id,
                link_id, contract_id, contract_digest, reservation_id, dependency_class,
                execution_generation, created_at, updated_at
            )
            VALUES ('tenant-1', 'async-child:pg', $1, $2, 'link:pg', 'contract:helper',
                    $3, 'reservation:child', 'required_blocking', 1, $4, $4)
            """,
            run_id,
            operation_id,
            BINDING,
            now,
        )
        for index, status in enumerate(("submitted", "in_doubt")):
            await connection.execute(
                """
                INSERT INTO belllabs_control.async_subagent_facts (
                    fact_id, request_scope, child_execution_id, fact_kind, fact_ref,
                    recorded_at
                )
                VALUES ($1, 'tenant-1', 'async-child:pg', 'lifecycle', $2, $3)
                """,
                f"fact:pg:{index}",
                status,
                now.replace(microsecond=index),
            )


@pytest.mark.asyncio
async def test_scoped_reads_under_runtime_and_readonly_roles_never_write(
    test_application_postgres_dsn: str,
) -> None:
    require_disposable_postgres(test_application_postgres_dsn)
    owner = await asyncpg.create_pool(dsn=test_application_postgres_dsn, min_size=1, max_size=2)
    pools: dict[str, asyncpg.Pool] = {}
    try:
        await reset_application_schema(owner)
        run_service, _ = run_control_service(PostgresRunControlRepository(owner))  # type: ignore[arg-type]
        saver = InMemorySaver()
        seeded = await seed_inspection_world(
            run_service, PostgresCheckpointLineageRepository(owner), saver
        )
        await _seed_async_child(owner, seeded.active_run, seeded.in_doubt.semantic_operation_id)
        for role in (RUNTIME_ROLE, READONLY_ROLE):
            pools[role] = await asyncpg.create_pool(
                dsn=test_application_postgres_dsn, min_size=1, max_size=2, setup=_assume(role)
            )

        before = await _schema_digest(owner)
        results: dict[str, dict[str, object]] = {}
        for role, pool in pools.items():
            async with pool.acquire() as connection:
                assert await connection.fetchval("SELECT current_user") == role
                assert not await connection.fetchval(
                    "SELECT rolsuper OR rolbypassrls FROM pg_roles WHERE rolname = current_user"
                )
            clock = MutableClock(READ_AT)
            inspection = RuntimeInspectionService(
                PostgresInspectionReadRepository(pool),
                cursors=InspectionCursorCodec(b"k" * 32, clock=clock),
                checkpoints=LangGraphCheckpointHistoryReader({CHECKPOINTER: saver}),
                clock=clock,
            )
            results[role] = await _exercise_reads(inspection, seeded)
        after = await _schema_digest(owner)
        assert after == before, "inspection reads changed persisted state"
        # Both roles see exactly the same scoped read models.
        assert results[RUNTIME_ROLE] == results[READONLY_ROLE]

        # Least privilege: the read-only role cannot write, and forced RLS hides other
        # scopes even when it reads the tables directly.
        async with pools[READONLY_ROLE].acquire() as connection, connection.transaction():
            await connection.execute(
                "SELECT set_config('belllabs.request_scope', 'tenant-2', true)"
            )
            assert (
                await connection.fetchval("SELECT count(*) FROM belllabs_control.runtime_units")
                == 0
            )
            assert (
                await connection.fetchval("SELECT count(*) FROM belllabs_control.workflow_runs")
                == 1
            )
            for statement in (
                "UPDATE belllabs_control.workflow_runs SET version = version",
                "DELETE FROM belllabs_control.async_subagent_facts",
                "UPDATE belllabs_control.runtime_unit_generations SET claim_fence = 9",
            ):
                with pytest.raises(asyncpg.InsufficientPrivilegeError):
                    async with connection.transaction():
                        await connection.execute(statement)
    finally:
        for pool in pools.values():
            await pool.close()
        await owner.close()


async def _exercise_reads(
    inspection: RuntimeInspectionService, seeded: SeededRuns
) -> dict[str, object]:
    active, terminal = seeded.active_run, seeded.terminal_run
    settled, in_doubt = seeded.settled, seeded.in_doubt

    page = await inspection.list_runs("tenant-1", limit=1)
    assert page.sections["runs"].source == "postgres_authority"
    assert page.data.next_cursor is not None
    rest = await inspection.list_runs("tenant-1", limit=1, cursor=page.data.next_cursor)
    listed = [item.run_id for item in (*page.data.items, *rest.data.items)]
    assert listed == sorted([active, terminal])
    assert rest.data.next_cursor is None
    with pytest.raises(InvalidInspectionCursor):
        await inspection.list_runs("tenant-2", cursor=page.data.next_cursor)
    terminal_only = await inspection.list_runs("tenant-1", phases=frozenset({RunPhase.TERMINAL}))
    assert [item.run_id for item in terminal_only.data.items] == [terminal]

    with pytest.raises(InspectionNotFound):
        await inspection.get_run("tenant-2", active)
    with pytest.raises(InspectionNotFound):
        await inspection.get_unit("tenant-1", terminal, settled.unit_key)

    run = await inspection.get_run("tenant-1", active)
    assert run.data.projection.phase == RunPhase.ACTIVE
    assert run.data.reconciliation_state == "operator_required"
    assert {unit.unit_key: unit.status for unit in run.data.units} == {
        settled.unit_key: "active",
        in_doubt.unit_key: "in_doubt",
    }
    assert [effect.ambiguous for effect in run.data.effects] == [True]
    assert run.data.budget is not None and run.data.budget.reserved["tokens.total"] >= 5
    child = run.data.async_children[0]
    assert (child.child_execution_id, child.lifecycle) == ("async-child:pg", "in_doubt")
    assert run.sections["temporal"].freshness == "unavailable"
    assert run.sections["run"].projection_version == run.data.projection.version
    terminal_run = await inspection.get_run("tenant-1", terminal)
    assert terminal_run.data.projection.terminal_outcome is not None

    settled_unit = await inspection.get_unit("tenant-1", active, settled.unit_key)
    generation = settled_unit.data.generations[0]
    assert generation.transition is not None
    assert generation.transition.result_key.checkpoint_id == "c3"
    assert generation.result is not None
    assert generation.namespace_head is not None
    assert generation.attempts[0].attempt.attempt == 1
    assert generation.lease_state == "expired"
    doubt = await inspection.get_unit("tenant-1", active, in_doubt.unit_key)
    assert doubt.data.status == "in_doubt"
    assert doubt.data.generations[0].incidents[0].status == "operator_required"
    # Leases are judged against PostgreSQL's clock at the snapshot; the fixture's lease
    # deadline (2026-10-01T14:00Z) has passed, so the recorded holder no longer holds it.
    assert doubt.data.generations[0].lease_holder is not None
    assert doubt.data.generations[0].lease_state == "expired"
    assert run.sections["run"].observed_at is not None
    assert run.sections["run"].observed_at > doubt.data.generations[0].lease_expires_at
    assert [item.child_execution_id for item in doubt.data.async_children] == ["async-child:pg"]

    history = await inspection.get_checkpoint_history("tenant-1", active, settled.unit_key)
    assert [entry.key.checkpoint_id for entry in history.data.entries] == [
        "c1",
        "c3-parent",
        "c3",
    ]
    summary = await inspection.get_checkpoint_summary(
        "tenant-1", active, settled.unit_key, "c3-parent"
    )
    serialized = summary.model_dump_json()
    assert SECRET_TEXT not in serialized and PROMPT_TEXT not in serialized
    assert summary.data.facts.message_count == 3
    with pytest.raises(CheckpointNotInUnitLineage):
        await inspection.get_checkpoint_summary("tenant-1", active, settled.unit_key, "foreign")

    return {
        "list": listed,
        "run": run.data.model_dump(mode="json"),
        "settled": settled_unit.data.model_dump(mode="json"),
        "in_doubt": doubt.data.model_dump(mode="json"),
        "history": history.data.model_dump(mode="json", exclude={"next_cursor"}),
        "summary": summary.data.model_dump(mode="json", exclude={"observed_at"}),
    }
