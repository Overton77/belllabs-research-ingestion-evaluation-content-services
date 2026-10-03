from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit
from uuid import uuid4

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


@pytest.fixture
async def records_pool(test_application_postgres_dsn):
    parsed = urlsplit(test_application_postgres_dsn)
    assert parsed.hostname in {"localhost", "127.0.0.1", "::1"}, "disposable loopback only"
    database = "biotech_adapters_test_" + uuid4().hex[:12]
    owner = await asyncpg.connect(test_application_postgres_dsn)
    try:
        await owner.execute(f'CREATE DATABASE "{database}"')
    finally:
        await owner.close()
    dsn = urlunsplit(parsed._replace(path="/" + database))
    owner = await asyncpg.connect(dsn)
    migration = Path(__file__).resolve().parents[3] / (  # noqa: ASYNC240
        "integrations/biotech/src/biotech_mission_adapters/migrations/0001_scoped_records.sql"
    )
    sql = await asyncio.to_thread(migration.read_text, encoding="utf-8")
    try:
        async with owner.transaction():
            await owner.execute(sql)
        # Idempotent optional install; no general Mission Control schema required.
        async with owner.transaction():
            await owner.execute(sql)
        assert await owner.fetchval("SELECT to_regnamespace('belllabs_control')") is None
    finally:
        await owner.close()

    async def role(connection):
        await connection.execute("SET ROLE biotech_mission_adapter_runtime")

    pool = await asyncpg.create_pool(dsn, min_size=1, max_size=6, setup=role)
    try:
        yield pool
    finally:
        await pool.close()
    # Preserve the uniquely named disposable database for audit; never drop shared data.


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
