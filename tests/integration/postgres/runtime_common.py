"""Shared helpers for runtime-authority tests on the common mission_control component.

Each test gets a fresh disposable database from ``tests.fixtures.mission_control_common_db``
with restricted LOGIN roles (real grants, forced RLS) and canonical scopes.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any

import asyncpg
import pytest_asyncio

from mission_control.domain.policies.contracts import LifecycleCommand, RunRequest
from tests.fixtures.mission_control_common_db import (
    CommonDatabase,
    create_common_database,
    drop_common_database,
)
from tests.unit.run_control.test_run_control import command as unit_command
from tests.unit.run_control.test_run_control import request as unit_request


@pytest_asyncio.fixture
async def common_db() -> AsyncIterator[CommonDatabase]:
    database = await create_common_database()
    try:
        yield database
    finally:
        await drop_common_database(database)


@pytest_asyncio.fixture
async def poisoned_db() -> AsyncIterator[CommonDatabase]:
    database = await create_common_database(legacy_poison=True)
    try:
        yield database
    finally:
        await drop_common_database(database)


@pytest_asyncio.fixture
async def runtime_pool(common_db: CommonDatabase) -> AsyncIterator[asyncpg.Pool]:
    pool = await common_db.pool("mission_control_runtime")
    try:
        yield pool
    finally:
        await pool.close()


@pytest_asyncio.fixture
async def family_pool(common_db: CommonDatabase) -> AsyncIterator[asyncpg.Pool]:
    pool = await common_db.pool("mission_control_family_writer")
    try:
        yield pool
    finally:
        await pool.close()


def scoped_request(db: CommonDatabase, tenant: str = "tenant-1", **kwargs: Any) -> RunRequest:
    """The unit-test Run Request bound to a canonical scope of this database."""

    return unit_request(request_scope=db.scope(tenant), **kwargs)


def scoped_command(
    db: CommonDatabase,
    run_id: str,
    version: int,
    command_id: str,
    action: object,
    tenant: str = "tenant-1",
) -> LifecycleCommand:
    return unit_command(run_id, version, command_id, action).model_copy(
        update={"request_scope": db.scope(tenant)}
    )


async def owner_rows(db: CommonDatabase, query: str, *args: Any) -> list[asyncpg.Record]:
    """Owner-side inspection for lineage assertions (never used by the code under test)."""

    connection = await asyncpg.connect(db.owner_dsn)
    try:
        return list(await connection.fetch(query, *args))
    finally:
        await connection.close()


async def scoped_count(pool: asyncpg.Pool, scope: str | None, table: str) -> int:
    """Count rows visible to a restricted role with (or without) transaction scope."""

    from mission_control.adapters.postgres.scope import apply_scope

    async with pool.acquire() as connection, connection.transaction():
        if scope is not None:
            await apply_scope(connection, scope)
        return int(await connection.fetchval(f"SELECT count(*) FROM mission_control.{table}"))


async def assert_admission_lineage(db: CommonDatabase, run_key: str) -> None:
    """Exactly one mission/definition/revision/program/run linked by scoped keys, with a
    contiguous event sequence and outbox rows linked to events and their commit."""

    rows = await owner_rows(
        db,
        """
        SELECT run.run_id, run.mission_id, run.revision_id, run.scheduling_revision_id,
               rev.revision_no, rev.definition_snapshot_id, snap.definition_digest,
               prog.program_digest, m.next_event_seq, m.last_event_seq
        FROM mission_control.mission_run run
        JOIN mission_control.mission m USING (installation_id, application_id, tenant_id,
                                              mission_id)
        JOIN mission_control.mission_revision rev
          ON rev.installation_id = run.installation_id
         AND rev.application_id = run.application_id AND rev.tenant_id = run.tenant_id
         AND rev.revision_id = run.revision_id AND rev.mission_id = run.mission_id
        JOIN mission_control.definition_snapshot snap
          ON snap.installation_id = rev.installation_id
         AND snap.application_id = rev.application_id AND snap.tenant_id = rev.tenant_id
         AND snap.definition_snapshot_id = rev.definition_snapshot_id
        JOIN mission_control.compiled_program prog
          ON prog.installation_id = rev.installation_id
         AND prog.application_id = rev.application_id AND prog.tenant_id = rev.tenant_id
         AND prog.revision_id = rev.revision_id
        WHERE run.run_key = $1
        """,
        run_key,
    )
    assert len(rows) == 1, rows
    row = rows[0]
    assert row["revision_no"] == 1
    assert row["scheduling_revision_id"] == row["revision_id"]
    counts = await owner_rows(
        db,
        """
        SELECT
          (SELECT count(*) FROM mission_control.definition_snapshot WHERE mission_id = $1) AS s,
          (SELECT count(*) FROM mission_control.mission_revision WHERE mission_id = $1) AS r,
          (SELECT count(*) FROM mission_control.mission_run WHERE mission_id = $1) AS runs
        """,
        row["mission_id"],
    )
    assert (counts[0]["s"], counts[0]["r"], counts[0]["runs"]) == (1, 1, 1)
    events = await owner_rows(
        db,
        """
        SELECT e.seq, e.event_id, e.ledger_commit_id, o.event_id AS outbox_event,
               o.ledger_commit_id AS outbox_commit, c.first_event_seq, c.last_event_seq
        FROM mission_control.mission_event e
        JOIN mission_control.outbox o
          ON o.installation_id = e.installation_id AND o.application_id = e.application_id
         AND o.tenant_id = e.tenant_id AND o.event_id = e.event_id
        JOIN mission_control.ledger_commit c
          ON c.installation_id = e.installation_id AND c.application_id = e.application_id
         AND c.tenant_id = e.tenant_id AND c.ledger_commit_id = e.ledger_commit_id
        WHERE e.mission_id = $1 ORDER BY e.seq
        """,
        row["mission_id"],
    )
    assert [item["seq"] for item in events] == list(range(1, len(events) + 1))
    assert events, "admission must append mission events"
    assert row["last_event_seq"] == len(events)
    assert row["next_event_seq"] == len(events) + 1
    for item in events:
        assert item["outbox_commit"] == item["ledger_commit_id"]
        assert item["first_event_seq"] <= item["seq"] <= item["last_event_seq"]
