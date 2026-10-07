"""Representative repository round-trips never touch legacy schemas (plan section 8).

Two databases: one where ``belllabs_control``/``capability_search``/
``belllabs_langgraph`` never existed, one where they hold poison sentinels and every
privilege on them is revoked from PUBLIC, the capability roles and the generated
login roles. Admission plus inspection then runs through the real PostgreSQL
repositories with restricted pools. Evidence without ``pg_stat_statements`` or
``log_statement``: every statement and its exception is captured client-side by
asyncpg query loggers on the restricted pools; any denied/missing legacy access
would surface as an exception or a logged legacy statement.

Installation here uses ``tests.fixtures.mission_control_common_db`` (lead-owned
shared fixture, ``install_component``); see the switch list in the qualification
report — it must move to the real ``mission-db`` installer with the fixture.
"""

from __future__ import annotations

import re
from typing import Any
from uuid import uuid4

import asyncpg
import pytest

from tests.fixtures.mission_control_common_db import (
    LEGACY_SCHEMAS,
    CommonDatabase,
    create_common_database,
    drop_common_database,
    legacy_poison_intact,
)
from tests.qualification.two_project.disposable import require_admin_dsn

pytestmark = pytest.mark.common_db

LEGACY_TEXT = re.compile(
    r"belllabs_control|capability_search\.|belllabs_langgraph|belllabs\.(request|catalog)_scope"
)


class StatementLog:
    def __init__(self) -> None:
        self.statements: list[str] = []
        self.failures: list[str] = []

    def __call__(self, record: Any) -> None:
        self.statements.append(record.query)
        if record.exception is not None:
            self.failures.append(f"{type(record.exception).__name__}: {record.query[:160]}")


async def _revoke_legacy(database: CommonDatabase) -> None:
    connection = await asyncpg.connect(database.owner_dsn)
    try:
        grantees = sorted({*database.login_roles, *database.login_roles.values(), "PUBLIC"})
        for schema in LEGACY_SCHEMAS:
            if not await connection.fetchval(
                "SELECT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname=$1)", schema
            ):
                continue
            for grantee in grantees:
                target = "PUBLIC" if grantee == "PUBLIC" else f'"{grantee}"'
                await connection.execute(f'REVOKE ALL ON SCHEMA "{schema}" FROM {target}')
                await connection.execute(
                    f'REVOKE ALL ON ALL TABLES IN SCHEMA "{schema}" FROM {target}'
                )
    finally:
        await connection.close()


async def _legacy_privileges(database: CommonDatabase) -> list[str]:
    connection = await asyncpg.connect(database.owner_dsn)
    try:
        rows = await connection.fetch(
            """
            SELECT r.rolname, n.nspname FROM pg_roles r CROSS JOIN pg_namespace n
            WHERE n.nspname = ANY($1::text[]) AND r.rolname = ANY($2::text[])
              AND (has_schema_privilege(r.oid, n.oid, 'USAGE')
                   OR has_schema_privilege(r.oid, n.oid, 'CREATE'))
            """,
            list(LEGACY_SCHEMAS),
            sorted({*database.login_roles, *database.login_roles.values()}),
        )
        return [f"{row['rolname']}@{row['nspname']}" for row in rows]
    finally:
        await connection.close()


async def _canonical_counts(database: CommonDatabase, tenant: str) -> dict[str, int]:
    connection = await asyncpg.connect(database.owner_dsn)
    try:
        counts = {}
        for table in ("request_receipt", "mission", "mission_run", "mission_event", "outbox"):
            counts[table] = int(
                await connection.fetchval(
                    f"SELECT count(*) FROM mission_control.{table} "
                    "WHERE installation_id=$1 AND application_id=$2 AND tenant_id=$3",
                    database.installation_id,
                    database.application_id,
                    database.tenants[tenant],
                )
            )
        return counts
    finally:
        await connection.close()


async def _legacy_schemas_present(database: CommonDatabase) -> list[str]:
    connection = await asyncpg.connect(database.owner_dsn)
    try:
        rows = await connection.fetch(
            "SELECT nspname FROM pg_namespace WHERE nspname = ANY($1::text[]) ORDER BY 1",
            list(LEGACY_SCHEMAS),
        )
        return [row["nspname"] for row in rows]
    finally:
        await connection.close()


@pytest.mark.parametrize("legacy_poison", [False, True], ids=["legacy-absent", "legacy-poisoned"])
async def test_admission_and_inspection_round_trip_never_reach_legacy(legacy_poison: bool) -> None:
    from mission_control.adapters.postgres.run_control.inspection_repository import (
        PostgresInspectionReadRepository,
    )
    from mission_control.adapters.postgres.run_control.run_control_repository import (
        PostgresRunControlRepository,
    )
    from tests.unit.run_control.test_run_control import request, service

    server = require_admin_dsn()
    database = await create_common_database(legacy_poison=legacy_poison, server_dsn=server)
    log = StatementLog()

    async def instrument(connection: asyncpg.Connection) -> None:
        connection.add_query_logger(log)

    pools: list[asyncpg.Pool] = []
    try:
        await _revoke_legacy(database)
        assert await _legacy_privileges(database) == []
        runtime = await database.pool("mission_control_runtime", init=instrument)
        pools.append(runtime)
        readonly = await database.pool("mission_control_readonly", init=instrument)
        pools.append(readonly)
        scope = database.scope("tenant-1")

        authority, _ = service(PostgresRunControlRepository(runtime))  # type: ignore[arg-type]
        request_id = str(uuid4())
        admission = await authority.admit(request(request_scope=scope, request_id=request_id))
        assert admission.run_id is not None
        replay = await authority.admit(request(request_scope=scope, request_id=request_id))
        assert replay.run_id == admission.run_id, "exact admission replay must be idempotent"

        inspection = PostgresInspectionReadRepository(readonly)
        snapshot = await inspection.read_run(scope, admission.run_id)
        assert snapshot is not None
        assert snapshot.run.projection.run_id == admission.run_id
        other_scope = await inspection.read_run(database.scope("tenant-2"), admission.run_id)
        assert other_scope is None, "a run admitted for tenant-1 is visible to tenant-2"

        assert log.statements, "no SQL was observed; the round trip did not reach PostgreSQL"
        assert not log.failures, log.failures
        legacy = [query[:160] for query in log.statements if LEGACY_TEXT.search(query)]
        assert not legacy, legacy
        assert any("mission_control." in query for query in log.statements)

        counts = await _canonical_counts(database, "tenant-1")
        assert counts["request_receipt"] >= 1, counts
        assert counts["mission_run"] >= 1, counts
        assert counts["mission"] >= 1, counts
        assert counts["mission_event"] >= 1, counts
        assert counts["outbox"] >= 1, counts

        present = await _legacy_schemas_present(database)
        if legacy_poison:
            assert present == sorted(LEGACY_SCHEMAS)
            assert await legacy_poison_intact(database.owner_dsn)
        else:
            assert present == [], f"a fallback created legacy schemas: {present}"
    finally:
        for pool in pools:
            await pool.close()
        await drop_common_database(database, server_dsn=server)
