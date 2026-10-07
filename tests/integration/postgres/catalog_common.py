"""Shared fixtures for catalog/artifact repository tests on the common component.

Every test gets a fresh disposable database (``tests.fixtures.mission_control_common_db``)
with restricted LOGIN roles, so the real grants and forced RLS are exercised. The
biotech database also holds a *disabled* ``ai-engineer`` installation row so that
cross-application denial is proven by row-level security, not by a missing FK target.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from uuid import UUID

import asyncpg
import pytest_asyncio

from mission_control.contracts.identities import parse_request_scope, uuid7
from tests.fixtures.mission_control_common_db import (
    AI_ENGINEER_INSTALLATION_ID,
    COMPONENT_VERSION,
    CommonDatabase,
    catalog_scope,
    create_common_database,
    drop_common_database,
)

FIXTURE_ACTOR = "fixture:catalog-artifacts-tests"
BIOTECH_CATALOG = catalog_scope("biotech")
AI_ENGINEER_CATALOG = catalog_scope("ai-engineer")


def _digest(value: object) -> str:
    return "sha256:" + hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()


async def add_disabled_installation(database: CommonDatabase) -> None:
    connection = await asyncpg.connect(database.owner_dsn)
    try:
        await connection.execute(
            """INSERT INTO mission_control.application_installation
               (installation_id, application_id, supabase_project_ref, environment,
                schema_component_version, state, created_at)
               VALUES ($1, 'ai-engineer', 'mcdisposableaieng', 'disposable', $2, 'disabled',
                       clock_timestamp())
               ON CONFLICT DO NOTHING""",
            AI_ENGINEER_INSTALLATION_ID,
            COMPONENT_VERSION,
        )
    finally:
        await connection.close()


@pytest_asyncio.fixture
async def catalog_db() -> AsyncIterator[CommonDatabase]:
    database = await create_common_database()
    try:
        await add_disabled_installation(database)
        yield database
    finally:
        await drop_common_database(database)


@pytest_asyncio.fixture
async def runtime_pool(catalog_db: CommonDatabase) -> AsyncIterator[asyncpg.Pool]:
    pool = await catalog_db.pool("mission_control_runtime")
    try:
        yield pool
    finally:
        await pool.close()


@pytest_asyncio.fixture
async def catalog_writer_pool(catalog_db: CommonDatabase) -> AsyncIterator[asyncpg.Pool]:
    pool = await catalog_db.pool("mission_control_catalog_writer")
    try:
        yield pool
    finally:
        await pool.close()


async def insert_fixture_run(database: CommonDatabase, request_scope: str, run_key: str) -> UUID:
    """Owner-inserted canonical mission/revision/run chain (test setup only).

    Stands in for the runtime lane's admission so artifact custody can reference a real
    ``mission_run`` row; it is labeled fixture data and never used outside tests.
    """
    scope = parse_request_scope(request_scope)
    key = (scope.installation_id, scope.application_id, scope.tenant_id)
    now = datetime.now(UTC)
    mission_id, snapshot_id, revision_id, run_id = uuid7(), uuid7(), uuid7(), uuid7()
    definition = {"fixture": run_key}
    connection = await asyncpg.connect(database.owner_dsn)
    try:
        async with connection.transaction():
            await connection.execute(
                """INSERT INTO mission_control.mission
                   (installation_id, application_id, tenant_id, mission_id, mission_key, title,
                    owner_actor_ref, lifecycle, next_event_seq, version, updated_at,
                    last_event_seq, created_at, created_by_actor_ref)
                   VALUES ($1,$2,$3,$4,$5,'fixture mission',$6,'active',1,1,$7,0,$7,$6)""",
                *key,
                mission_id,
                "mission:" + run_key,
                FIXTURE_ACTOR,
                now,
            )
            await connection.execute(
                """INSERT INTO mission_control.definition_snapshot
                   (installation_id, application_id, tenant_id, definition_snapshot_id,
                    mission_id, definition_contract_version, definition, definition_digest,
                    created_at, created_by_actor_ref)
                   VALUES ($1,$2,$3,$4,$5,'fixture/1',$6::jsonb,$7,$8,$9)""",
                *key,
                snapshot_id,
                mission_id,
                json.dumps(definition),
                _digest(definition),
                now,
                FIXTURE_ACTOR,
            )
            await connection.execute(
                """INSERT INTO mission_control.mission_revision
                   (installation_id, application_id, tenant_id, revision_id, mission_id,
                    revision_no, definition_snapshot_id, policy_digest, binding_digest,
                    committed_at, created_at, created_by_actor_ref)
                   VALUES ($1,$2,$3,$4,$5,1,$6,$7,$7,$8,$8,$9)""",
                *key,
                revision_id,
                mission_id,
                snapshot_id,
                _digest("fixture-policy"),
                now,
                FIXTURE_ACTOR,
            )
            await connection.execute(
                """INSERT INTO mission_control.mission_run
                   (installation_id, application_id, tenant_id, run_id, run_key, mission_id,
                    revision_id, scheduling_revision_id, request_key, input_manifest_ref,
                    input_digest, admission_binding_ref, admission_binding_digest,
                    workflow_family, phase, lifecycle, execution_epoch, execution_generation,
                    technical_segment, admitted_policy_ref, admitted_build_ref,
                    projection_contract, projection, version, admitted_at, updated_at,
                    created_at, created_by_actor_ref)
                   VALUES ($1,$2,$3,$4,$5,$6,$7,$7,$8,'fixture://input',$9,'fixture://binding',
                           $9,'stagegraph','active','active',1,1,1,'fixture-policy',
                           'fixture-build','fixture.run/1','{}'::jsonb,1,$10,$10,$10,$11)""",
                *key,
                run_id,
                run_key,
                mission_id,
                revision_id,
                "request:" + run_key,
                _digest(definition),
                now,
                FIXTURE_ACTOR,
            )
    finally:
        await connection.close()
    return run_id
