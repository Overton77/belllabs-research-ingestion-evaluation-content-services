"""The optional biotech adapter record component installed beside the common component.

`biotech_mission_adapters` stays its own optional component with its own role and scope
setting; this proof installs it (twice, idempotently) into a fresh database that already
carries the common `mission_control` component, and checks the two never share authority:
no legacy schema appears, the common component's release ledger is untouched, and no
`mission_control_*` capability role reaches the adapter records (nor the adapter role the
common tables).
"""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime
from pathlib import Path

import asyncpg
import pytest

from biotech_mission_adapters.adapters.postgres_records import (
    PostgresSchemaGroundingRecordRepository,
    PostgresWebResearchRecordRepository,
)
from biotech_mission_adapters.application.schema.schema_grounding_repository import (
    schema_grounding_record,
)
from biotech_mission_adapters.application.web_research.web_research_repository import (
    WebResearchRecordConflict,
    WebResearchRecordNotFound,
    web_research_record_ref,
)
from biotech_mission_adapters.domain.coordinator.web_research_runtime import (
    WebResearchRecordEnvelope,
)
from biotech_mission_adapters.domain.schema_grounding.errors import (
    CatalogPublicationConflict,
    SchemaGroundingRecordNotFound,
)
from mission_control.domain.authoring.canonical import sha256_digest
from tests.fixtures.mission_control_common_db import (
    CAPABILITY_ROLES,
    CommonDatabase,
    common_database,
)

pytestmark = pytest.mark.common_db
ADAPTER_ROLE = "biotech_mission_adapter_runtime"


async def _isolated_from_common_component(owner: asyncpg.Connection) -> None:
    assert await owner.fetchval("SELECT to_regnamespace('belllabs_control')") is None
    for capability in CAPABILITY_ROLES:
        assert not await owner.fetchval(
            "SELECT has_schema_privilege($1, 'biotech_mission_adapters', 'USAGE')"
            " OR has_table_privilege($1, 'biotech_mission_adapters.records',"
            " 'SELECT, INSERT, UPDATE, DELETE')",
            capability,
        ), capability
    assert not await owner.fetchval(
        "SELECT has_table_privilege($1, 'mission_control.mission_run',"
        " 'SELECT, INSERT, UPDATE, DELETE')",
        ADAPTER_ROLE,
    )


@pytest.fixture
async def records_pool():
    async with common_database() as database:
        async for pool in _records_pool(database):
            yield pool


async def _records_pool(database: CommonDatabase):
    owner = await asyncpg.connect(database.owner_dsn)
    migration = Path(__file__).resolve().parents[3] / (  # noqa: ASYNC240
        "integrations/biotech/src/biotech_mission_adapters/migrations/0001_scoped_records.sql"
    )
    sql = await asyncio.to_thread(migration.read_text, encoding="utf-8")
    try:
        releases = await owner.fetchval("SELECT count(*) FROM mission_control.component_release")
        async with owner.transaction():
            await owner.execute(sql)
        # Idempotent optional install beside the common component, which it never changes.
        async with owner.transaction():
            await owner.execute(sql)
        assert (
            await owner.fetchval("SELECT count(*) FROM mission_control.component_release")
            == releases
        )
        await _isolated_from_common_component(owner)
    finally:
        await owner.close()

    async def role(connection):
        await connection.execute(f"SET ROLE {ADAPTER_ROLE}")

    pool = await asyncpg.create_pool(database.owner_dsn, min_size=1, max_size=6, setup=role)
    try:
        yield pool
    finally:
        await pool.close()


async def test_schema_records_concurrency_scope_and_immutability(records_pool):
    repository = PostgresSchemaGroundingRecordRepository(records_pool)
    record = schema_grounding_record(
        record_type="catalog_build",
        record_id="build-1",
        request_scope="tenant-a",
        run_id="run-1",
        payload={"evidence": "one"},
        created_at=datetime.now(UTC),
    )
    assert await asyncio.gather(*(repository.append(record) for _ in range(4))) == [record] * 4
    assert await repository.get("tenant-a", "catalog_build", "build-1") == record
    assert await repository.list_for_run("tenant-a", "run-1") == (record,)
    with pytest.raises(SchemaGroundingRecordNotFound):
        await repository.get("tenant-b", "catalog_build", "build-1")
    changed = schema_grounding_record(
        record_type="catalog_build",
        record_id="build-1",
        request_scope="tenant-a",
        run_id="run-1",
        payload={"evidence": "changed"},
        created_at=record.created_at,
    )
    with pytest.raises(CatalogPublicationConflict):
        await repository.append(changed)
    async with records_pool.acquire() as connection, connection.transaction():
        assert (
            await connection.fetchval("SELECT count(*) FROM biotech_mission_adapters.records") == 0
        )
        await connection.execute("SELECT set_config('biotech.request_scope','tenant-b',true)")
        with pytest.raises(asyncpg.InsufficientPrivilegeError):
            await connection.execute(
                """INSERT INTO biotech_mission_adapters.records
                VALUES ('tenant-a','schema_grounding',
                        'catalog_build:other','run-1',NULL,$1::jsonb)""",
                record.model_copy(update={"record_id": "other"}).model_dump_json(),
            )


async def test_web_records_exact_digest_and_intent_conflicts(records_pool):
    repository = PostgresWebResearchRecordRepository(records_pool)
    payload = {"evidence": "local"}
    record = WebResearchRecordEnvelope(
        record_kind="firecrawl_evidence",
        record_id="one",
        intent_key="intent",
        request_scope="tenant-a",
        run_id="run-1",
        payload=payload,
        content_digest=sha256_digest(payload),
        created_at=datetime.now(UTC),
    )
    assert await repository.append(record) == record
    assert await repository.append(record) == record
    assert await repository.get_by_intent("tenant-a", "run-1", "intent") == record
    assert await repository.get("tenant-a", "run-1", web_research_record_ref(record)) == record
    with pytest.raises(WebResearchRecordConflict):
        await repository.append(record.model_copy(update={"record_id": "two"}))
    with pytest.raises(WebResearchRecordNotFound):
        await repository.get("tenant-b", "run-1", web_research_record_ref(record))
    with pytest.raises(WebResearchRecordNotFound):
        await repository.get("tenant-a", "run-1", web_research_record_ref(record)[:-64] + "0" * 64)


async def test_sql_rejects_missing_identity_payload(records_pool):
    async with records_pool.acquire() as connection:
        for payload in ({}, {"request_scope": "tenant-a", "record_id": None}):
            with pytest.raises(asyncpg.CheckViolationError):
                async with connection.transaction():
                    await connection.execute(
                        "SELECT set_config('biotech.request_scope','tenant-a',true)"
                    )
                    await connection.execute(
                        """INSERT INTO biotech_mission_adapters.records
                        VALUES ('tenant-a','schema_grounding',
                            'catalog_build:x',NULL,NULL,$1::jsonb)""",
                        json.dumps(payload),
                    )
