"""FT-A2 on a disposable common database: proposed registration and the bucket seed.

The Storage schema here is SYNTHETIC (tables and ``storage.foldername`` shaped like Supabase
Storage) so that the seeded policies can be applied and exercised under row-level security;
it is not Supabase. Nothing touches a live project or bucket.
"""

from __future__ import annotations

import json

import asyncpg
import pytest

import mission_control_db_contract.seeds as seed_engine
from mission_control.adapters.postgres.capability_bundles import PostgresBundleRegistry
from mission_control.adapters.postgres.control_plane.definition_repository import (
    PostgresDefinitionRepository,
)
from mission_control.adapters.postgres.scope import apply_catalog_scope_string
from mission_control.application.capabilities.bundle_custody import bundle_definition
from mission_control.domain.capabilities.bundles import build_bundle_manifest
from mission_control_db_contract.seeds import _apply_record, load_bundles
from mission_control_db_contract.storage import storage_available, verify_capability_bundles
from tests.fixtures.capability_search_catalog import SEEDS
from tests.fixtures.mission_control_common_db import CommonDatabase
from tests.integration.postgres.catalog_common import BIOTECH_CATALOG
from tests.integration.postgres.catalog_common import catalog_db as catalog_db
from tests.integration.postgres.catalog_common import catalog_writer_pool as catalog_writer_pool
from tests.integration.postgres.catalog_common import runtime_pool as runtime_pool

pytestmark = pytest.mark.common_db

FILES = (
    ("SKILL.md", b"---\nname: agent-browser\ndescription: Drive a real browser.\n---\n\nBody\n"),
    ("scripts/open.sh", b'#!/bin/sh\nagent-browser open "$1"\n'),
)
SYNTHETIC_STORAGE = """
DO $$ BEGIN CREATE ROLE authenticated NOLOGIN; EXCEPTION WHEN duplicate_object THEN NULL; END $$;
CREATE SCHEMA storage;
CREATE TABLE storage.buckets (
    id text PRIMARY KEY, name text NOT NULL, public boolean NOT NULL DEFAULT false,
    file_size_limit bigint, allowed_mime_types text[]);
CREATE TABLE storage.objects (
    id bigserial PRIMARY KEY, bucket_id text NOT NULL REFERENCES storage.buckets (id),
    name text NOT NULL, content bytea, UNIQUE (bucket_id, name));
ALTER TABLE storage.objects ENABLE ROW LEVEL SECURITY;
CREATE FUNCTION storage.foldername(name text) RETURNS text[]
    LANGUAGE sql IMMUTABLE AS $f$
    SELECT (string_to_array(name, '/'))[1:array_length(string_to_array(name, '/'), 1) - 1]
    $f$;
GRANT USAGE ON SCHEMA storage TO authenticated;
GRANT SELECT, INSERT, UPDATE, DELETE ON storage.objects TO authenticated;
GRANT USAGE, SELECT ON SEQUENCE storage.objects_id_seq TO authenticated;
"""


def _storage_bundle() -> dict:
    (bundle,) = [
        item
        for item in load_bundles([SEEDS / "common"])
        if item["seed_key"] == "mc.storage.capability-bundles"
    ]
    return bundle


async def _apply_records(database: CommonDatabase, bundle: dict) -> dict[str, int]:
    target = {
        "installation_id": str(database.installation_id),
        "application_id": database.application_id,
    }
    stats = {"created": 0, "reused": 0, "revoked": 0}
    connection = await asyncpg.connect(database.owner_dsn)
    try:
        async with connection.transaction():
            for record in bundle["records"]:
                await _apply_record(connection, target, bundle, record, stats)
    finally:
        await connection.close()
    return stats


@pytest.mark.asyncio
async def test_bundle_registration_is_proposed_idempotent_and_admittable(
    runtime_pool: asyncpg.Pool, catalog_writer_pool: asyncpg.Pool
) -> None:
    manifest = build_bundle_manifest(
        FILES,
        application_id="biotech",
        kind="skill_bundle",
        capability_id="skill.agent-browser",
        version="0.38.2",
    )
    definition = bundle_definition(manifest, FILES, {})
    registry = PostgresBundleRegistry(catalog_writer_pool, catalog_scope=BIOTECH_CATALOG)
    assert await registry.find(manifest) is None
    pin = await registry.register_proposed(manifest, definition, "operator:ft-a2")
    assert pin.startswith("skill.agent-browser@0.38.2#sha256:")
    assert await registry.find(manifest) == pin
    assert await registry.register_proposed(manifest, definition, "operator:ft-a2") == pin
    reader = PostgresDefinitionRepository(runtime_pool, catalog_scope=BIOTECH_CATALOG)
    assert await reader.list_published_definitions() == ()  # proposed rows are invisible
    async with catalog_writer_pool.acquire() as connection, connection.transaction():
        await apply_catalog_scope_string(connection, BIOTECH_CATALOG)
        row = await connection.fetchrow(
            "SELECT status, kind, manifest FROM mission_control.asset_version "
            "WHERE asset_id='definition:skill:skill.agent-browser'"
        )
        assert (row["status"], row["kind"]) == ("proposed", "skill_bundle")
        stored = json.loads(row["manifest"])["definition"]
        assert stored["bundle_ref"]["uri"] == f"capability-bundles://{manifest.object_prefix}"
        await connection.execute(
            "UPDATE mission_control.asset_version SET status='admitted', "
            "version_no=version_no+1, updated_at=clock_timestamp() "
            "WHERE asset_id='definition:skill:skill.agent-browser'"
        )
    listed = await reader.list_published_definitions()
    assert [item.ref.logical_id for item in listed] == ["skill.agent-browser"]


@pytest.mark.asyncio
async def test_bucket_seed_creates_private_bucket_and_four_policies(
    catalog_db: CommonDatabase,
) -> None:
    connection = await asyncpg.connect(catalog_db.owner_dsn)
    try:
        assert not await storage_available(connection)
        await connection.execute(SYNTHETIC_STORAGE)
        assert await storage_available(connection)
    finally:
        await connection.close()
    first = await _apply_records(catalog_db, _storage_bundle())
    assert first["created"] == 5
    replay = await _apply_records(catalog_db, _storage_bundle())
    assert replay["reused"] == 5 and replay["created"] == 0

    connection = await asyncpg.connect(catalog_db.owner_dsn)
    try:
        bucket = await connection.fetchrow(
            "SELECT public, file_size_limit, allowed_mime_types FROM storage.buckets "
            "WHERE id='capability-bundles'"
        )
        assert (bucket["public"], bucket["file_size_limit"]) == (False, 52_428_800)
        assert bucket["allowed_mime_types"] is None
        evidence = await verify_capability_bundles(connection, "biotech")
        assert evidence["status"] == "verified", evidence
        assert {item["cmd"] for item in evidence["policies"]} == {"INSERT", "SELECT", "ALL"}
        assert len(evidence["policies"]) == 4

        async def as_role(role: str, sql: str, *args: object) -> object:
            async with connection.transaction():
                await connection.execute("SET LOCAL ROLE authenticated")
                await connection.execute(
                    "SELECT set_config('request.jwt.claims', $1, true)",
                    json.dumps({"mc_capability_role": role}),
                )
                return await connection.execute(sql, *args)

        insert = "INSERT INTO storage.objects (bucket_id, name, content) VALUES ($1, $2, $3)"
        path = "biotech/skill_bundle/skill.x/1/" + "a" * 64 + "/SKILL.md"
        await as_role("publisher", insert, "capability-bundles", path, b"bytes")
        for role, name in (
            ("reader", "biotech/skill_bundle/skill.y/1/" + "b" * 64 + "/SKILL.md"),
            ("publisher", "ai-engineer/skill_bundle/skill.x/1/" + "a" * 64 + "/SKILL.md"),
        ):
            with pytest.raises(asyncpg.InsufficientPrivilegeError):
                await as_role(role, insert, "capability-bundles", name, b"x")
        # No UPDATE or DELETE policy: nothing is ever overwritten or removed.
        assert (
            await as_role("publisher", "UPDATE storage.objects SET content='x' WHERE name=$1", path)
            == "UPDATE 0"
        )
        assert (
            await as_role("publisher", "DELETE FROM storage.objects WHERE name=$1", path)
            == "DELETE 0"
        )
        assert (
            await as_role("reader", "SELECT 1 FROM storage.objects WHERE name=$1", path)
            == "SELECT 1"
        )
    finally:
        await connection.close()


@pytest.mark.asyncio
async def test_bucket_seed_is_blocked_without_storage(
    catalog_db: CommonDatabase, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def nothing(*_args: object, **_kwargs: object) -> None:
        return None

    async def version(*_args: object) -> str:
        return "1.0.0"

    monkeypatch.setattr(seed_engine, "check_database_identity", nothing)
    monkeypatch.setattr(seed_engine, "observe_state", nothing)
    monkeypatch.setattr(seed_engine, "verify_state", lambda *a, **k: None)
    monkeypatch.setattr(seed_engine, "_installation_version", version)
    target = {
        "installation_id": str(catalog_db.installation_id),
        "application_id": catalog_db.application_id,
    }
    connection = await asyncpg.connect(catalog_db.owner_dsn)
    try:
        result = await seed_engine.apply_bundle(
            connection,
            target,
            None,  # type: ignore[arg-type]  # release checks are stubbed above
            _storage_bundle(),
            reader_version="mission-control-runtime/1",
            writer_version="mission-control-runtime/1",
        )
        assert result["outcome"] == "blocked"
        receipts = await connection.fetchval(
            "SELECT count(*) FROM mission_control.installation_seed_receipt "
            "WHERE seed_key='mc.storage.capability-bundles'"
        )
        assert receipts == 0
    finally:
        await connection.close()
