"""RRM-005 PostgreSQL inspection reads (REQ-CP-RUN-011/012) on mission_control.

The same scoped reads are served under the API runtime role (`mission_control_runtime`)
and the read-only inspection role (`mission_control_readonly`), each in one READ ONLY
transaction, with restricted logins on a disposable common database. Every table of the
mission_control schema is unchanged by the reads.
"""

from __future__ import annotations

import asyncpg
import pytest
from langgraph.checkpoint.memory import InMemorySaver

from mission_control.adapters.deep_agents.checkpoint_history import (
    LangGraphCheckpointHistoryReader,
)
from mission_control.adapters.postgres.async_subagents.async_subagents import (
    PostgresAsyncSubagentAuthority,
)
from mission_control.adapters.postgres.operations.checkpoint_lineage import (
    PostgresCheckpointLineageRepository,
)
from mission_control.adapters.postgres.run_control.inspection_repository import (
    PostgresInspectionReadRepository,
)
from mission_control.adapters.postgres.run_control.run_control_repository import (
    PostgresRunControlRepository,
)
from mission_control.adapters.postgres.scope import apply_scope
from mission_control.application.execution.inspection import (
    InspectionCursorCodec,
    RuntimeInspectionService,
)
from mission_control.domain.policies.contracts import RunPhase
from mission_control.domain.policies.inspection import (
    CheckpointNotInUnitLineage,
    InspectionNotFound,
    InvalidInspectionCursor,
)
from tests.acceptance.control_plane.test_wp_cp_045 import request as spawn_request
from tests.fixtures.checkpoint_lineage import CHECKPOINTER
from tests.fixtures.mission_control_common_db import CommonDatabase
from tests.fixtures.runtime_inspection import (
    PROMPT_TEXT,
    READ_AT,
    SECRET_TEXT,
    MutableClock,
    SeededRuns,
    seed_inspection_world,
)
from tests.integration.postgres.runtime_common import common_db as common_db
from tests.unit.run_control.test_run_control import service as run_control_service

pytestmark = pytest.mark.common_db

RUNTIME_ROLE = "mission_control_runtime"
READONLY_ROLE = "mission_control_readonly"


async def _schema_digest(db: CommonDatabase) -> dict[str, str]:
    """A content digest of every mission_control table (the owner bypasses RLS)."""

    connection = await asyncpg.connect(db.owner_dsn)
    try:
        tables = [
            row["table_name"]
            for row in await connection.fetch(
                """
                SELECT table_name FROM information_schema.tables
                WHERE table_schema = 'mission_control' AND table_type = 'BASE TABLE'
                ORDER BY table_name
                """
            )
        ]
        digests: dict[str, str] = {}
        for table in tables:
            digests[table] = await connection.fetchval(
                f"""
                SELECT md5(coalesce(string_agg(t::text, '|' ORDER BY t::text), ''))
                FROM mission_control.{table} t
                """
            )
    finally:
        await connection.close()
    return digests


async def _seed_async_child(
    pool: asyncpg.Pool, db: CommonDatabase, run_id: str, operation_id: str
) -> None:
    """Parent authority rows written through the async-subagent authority repository."""

    authority = PostgresAsyncSubagentAuthority(pool)
    spawn = spawn_request().model_copy(
        update={
            "request_scope": db.scope(),
            "parent_run_id": run_id,
            "parent_operation_id": operation_id,
        }
    )
    await authority.reserve_and_admit(spawn, "async-child:pg", "link:pg")
    for status in ("submitted", "in_doubt"):
        await authority.record_fact(db.scope(), "async-child:pg", "lifecycle", status)


@pytest.mark.asyncio
async def test_scoped_reads_under_runtime_and_readonly_roles_never_write(
    common_db: CommonDatabase,
) -> None:
    db = common_db
    pools: dict[str, asyncpg.Pool] = {}
    writer = await db.pool(max_size=4)
    try:
        run_service, _ = run_control_service(PostgresRunControlRepository(writer))  # type: ignore[arg-type]
        saver = InMemorySaver()
        seeded = await seed_inspection_world(
            run_service,
            PostgresCheckpointLineageRepository(writer),
            saver,
            scope=db.scope("tenant-1"),
            other_scope=db.scope("tenant-2"),
        )
        await _seed_async_child(
            writer, db, seeded.active_run, f"binding:{seeded.in_doubt.unit_key}:1"
        )
        for role in (RUNTIME_ROLE, READONLY_ROLE):
            pools[role] = await db.pool(role, max_size=2)

        before = await _schema_digest(db)
        results: dict[str, dict[str, object]] = {}
        for role, pool in pools.items():
            async with pool.acquire() as connection:
                assert await connection.fetchval(
                    "SELECT pg_has_role(current_user, $1, 'MEMBER')", role
                )
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
            results[role] = await _exercise_reads(inspection, seeded, db)
        after = await _schema_digest(db)
        assert after == before, "inspection reads changed persisted state"
        # Both roles see exactly the same scoped read models.
        assert results[RUNTIME_ROLE] == results[READONLY_ROLE]

        # Least privilege: the read-only role cannot write, and forced RLS hides other
        # scopes even when it reads the tables directly.
        async with pools[READONLY_ROLE].acquire() as connection, connection.transaction():
            await apply_scope(connection, db.scope("tenant-2"))
            assert await connection.fetchval("SELECT count(*) FROM mission_control.activation") == 0
            assert (
                await connection.fetchval("SELECT count(*) FROM mission_control.mission_run") == 1
            )
            for statement in (
                "UPDATE mission_control.mission_run SET version = version",
                "DELETE FROM mission_control.native_observation",
                "UPDATE mission_control.attempt SET fencing_token = 9",
            ):
                with pytest.raises(asyncpg.InsufficientPrivilegeError):
                    async with connection.transaction():
                        await connection.execute(statement)
    finally:
        for pool in pools.values():
            await pool.close()
        await writer.close()


async def _exercise_reads(
    inspection: RuntimeInspectionService, seeded: SeededRuns, db: CommonDatabase
) -> dict[str, object]:
    tenant_1, tenant_2 = db.scope("tenant-1"), db.scope("tenant-2")
    active, terminal = seeded.active_run, seeded.terminal_run
    settled, in_doubt = seeded.settled, seeded.in_doubt

    page = await inspection.list_runs(tenant_1, limit=1)
    assert page.sections["runs"].source == "postgres_authority"
    assert page.data.next_cursor is not None
    rest = await inspection.list_runs(tenant_1, limit=1, cursor=page.data.next_cursor)
    listed = [item.run_id for item in (*page.data.items, *rest.data.items)]
    assert listed == sorted([active, terminal])
    assert rest.data.next_cursor is None
    with pytest.raises(InvalidInspectionCursor):
        await inspection.list_runs(tenant_2, cursor=page.data.next_cursor)
    terminal_only = await inspection.list_runs(tenant_1, phases=frozenset({RunPhase.TERMINAL}))
    assert [item.run_id for item in terminal_only.data.items] == [terminal]

    with pytest.raises(InspectionNotFound):
        await inspection.get_run(tenant_2, active)
    with pytest.raises(InspectionNotFound):
        await inspection.get_unit(tenant_1, terminal, settled.unit_key)

    run = await inspection.get_run(tenant_1, active)
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
    terminal_run = await inspection.get_run(tenant_1, terminal)
    assert terminal_run.data.projection.terminal_outcome is not None

    settled_unit = await inspection.get_unit(tenant_1, active, settled.unit_key)
    generation = settled_unit.data.generations[0]
    assert generation.transition is not None
    assert generation.transition.result_key.checkpoint_id == "c3"
    assert generation.result is not None
    assert generation.namespace_head is not None
    assert generation.attempts[0].attempt.attempt == 1
    assert generation.lease_state == "expired"
    doubt = await inspection.get_unit(tenant_1, active, in_doubt.unit_key)
    assert doubt.data.status == "in_doubt"
    assert doubt.data.generations[0].incidents[0].status == "operator_required"
    # Leases are judged against PostgreSQL's clock at the snapshot; the fixture's lease
    # deadline (2026-10-01T14:00Z) has passed, so the recorded holder no longer holds it.
    assert doubt.data.generations[0].lease_holder is not None
    assert doubt.data.generations[0].lease_state == "expired"
    assert run.sections["run"].observed_at is not None
    assert run.sections["run"].observed_at > doubt.data.generations[0].lease_expires_at
    assert [item.child_execution_id for item in doubt.data.async_children] == ["async-child:pg"]

    history = await inspection.get_checkpoint_history(tenant_1, active, settled.unit_key)
    assert [entry.key.checkpoint_id for entry in history.data.entries] == [
        "c1",
        "c3-parent",
        "c3",
    ]
    summary = await inspection.get_checkpoint_summary(
        tenant_1, active, settled.unit_key, "c3-parent"
    )
    serialized = summary.model_dump_json()
    assert SECRET_TEXT not in serialized and PROMPT_TEXT not in serialized
    assert summary.data.facts.message_count == 3
    with pytest.raises(CheckpointNotInUnitLineage):
        await inspection.get_checkpoint_summary(tenant_1, active, settled.unit_key, "foreign")

    return {
        "list": listed,
        "run": run.data.model_dump(mode="json"),
        "settled": settled_unit.data.model_dump(mode="json"),
        "in_doubt": doubt.data.model_dump(mode="json"),
        "history": history.data.model_dump(mode="json", exclude={"next_cursor"}),
        "summary": summary.data.model_dump(mode="json", exclude={"observed_at"}),
    }
