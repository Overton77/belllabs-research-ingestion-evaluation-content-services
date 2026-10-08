"""FT-D1: migration 0028 (mission_chain, chain_link, authoring_provenance, relationship kinds)
on a disposable PostgreSQL 17: forced RLS, grants, forward-only guards and the chain contract
round trip through real restricted roles."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID, uuid4

import asyncpg
import pytest

from mission_control.adapters.postgres.scope import apply_scope
from mission_control.contracts.identities import parse_request_scope
from mission_control.domain.authoring.manifest import load_manifest_yaml, parse_manifest
from mission_control.domain.composition.chain import (
    ChainScope,
    MissionChain,
    build_mission_chain,
    compile_chain,
)
from tests.fixtures.mission_control_common_db import CommonDatabase
from tests.integration.postgres.runtime_common import common_db as common_db

pytestmark = pytest.mark.common_db

ROOT = Path(__file__).resolve().parents[3]
FIXTURE = ROOT / "tests/fixtures/manifests/two-mission-chain.yml"
NOW = datetime(2026, 10, 7, 12, tzinfo=UTC)
TABLES = ("mission_chain", "chain_link", "authoring_provenance")


async def insert_mission(connection: asyncpg.Connection, scope: ChainScope, key: str) -> UUID:
    mission_id = uuid4()
    await connection.execute(
        """
        INSERT INTO mission_control.mission (installation_id, application_id, tenant_id,
            mission_id, mission_key, title, owner_actor_ref, lifecycle, next_event_seq,
            version, updated_at, last_event_seq, created_at, created_by_actor_ref)
        VALUES ($1, $2, $3, $4, $5, $5, 'actor:owner', 'draft', 1, 1, $6, 0, $6, 'actor:owner')
        """,
        scope.installation_id,
        scope.application_id,
        scope.tenant_id,
        mission_id,
        key,
        NOW,
    )
    return mission_id


async def insert_chain(connection: asyncpg.Connection, chain: MissionChain) -> None:
    scope = chain.scope
    await connection.execute(
        """
        INSERT INTO mission_control.mission_chain (installation_id, application_id, tenant_id,
            chain_id, chain_key, title, manifest_digest, lifecycle, phase, terminal_outcome,
            members, version, created_at, updated_at, created_by_actor_ref)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NULL, $10::jsonb, 1, $11, $11, $12)
        """,
        scope.installation_id,
        scope.application_id,
        scope.tenant_id,
        chain.chain_id,
        chain.chain_key,
        chain.title,
        chain.manifest_digest,
        chain.lifecycle.value,
        chain.phase.value,
        json.dumps([member.model_dump(mode="json") for member in chain.members]),
        chain.created_at,
        chain.created_by_actor_ref,
    )
    for link in chain.links:
        await connection.execute(
            """
            INSERT INTO mission_control.chain_link (installation_id, application_id, tenant_id,
                link_id, chain_id, link_key, from_mission_key, to_mission_key, from_mission_id,
                to_mission_id, kind, bindings, release_condition, on_upstream_cancel,
                on_upstream_not_accepted, state, version, created_at, updated_at,
                created_by_actor_ref)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12::jsonb, $13::jsonb, $14,
                $15, $16, 1, $17, $17, $18)
            """,
            scope.installation_id,
            scope.application_id,
            scope.tenant_id,
            link.link_id,
            chain.chain_id,
            link.link_key,
            link.from_mission_key,
            link.to_mission_key,
            link.from_mission_id,
            link.to_mission_id,
            link.kind.value,
            json.dumps([binding.model_dump(mode="json") for binding in link.bindings]),
            json.dumps(link.on.model_dump(mode="json", exclude_none=True)),
            link.on_upstream_cancel.value,
            link.on_upstream_not_accepted,
            link.state.value,
            chain.created_at,
            chain.created_by_actor_ref,
        )


async def seed_chain(pool: asyncpg.Pool, db: CommonDatabase) -> MissionChain:
    request_scope = parse_request_scope(db.scope())
    scope = ChainScope(
        installation_id=request_scope.installation_id,
        application_id=request_scope.application_id,
        tenant_id=request_scope.tenant_id,
    )
    compilation = compile_chain(
        parse_manifest(load_manifest_yaml(FIXTURE.read_text(encoding="utf-8")))
    )
    assert compilation.resolution is not None
    async with pool.acquire() as connection, connection.transaction():
        await apply_scope(connection, db.scope())
        members = {
            key: (await insert_mission(connection, scope, f"{key}-{uuid4().hex[:6]}"), uuid4())
            for key in compilation.resolution.order
        }
        chain = build_mission_chain(
            resolution=compilation.resolution,
            chain_id=uuid4(),
            scope=scope,
            chain_key="two-mission-chain",
            title="Research then ingestion",
            manifest_digest="sha256:" + "c" * 64,
            members=members,
            created_at=NOW,
            created_by_actor_ref="actor:owner",
        )
        await insert_chain(connection, chain)
    return chain


async def scoped(pool: asyncpg.Pool, scope: str | None, query: str, *args: object) -> object:
    async with pool.acquire() as connection, connection.transaction():
        if scope is not None:
            await apply_scope(connection, scope)
        return await connection.fetchval(query, *args)


async def test_tables_force_rls_and_grant_least_privilege(common_db: CommonDatabase):
    owner = await asyncpg.connect(common_db.owner_dsn)
    try:
        rows = await owner.fetch(
            "SELECT relname, relrowsecurity, relforcerowsecurity FROM pg_class "
            "WHERE relnamespace = 'mission_control'::regnamespace AND relname = ANY($1)",
            list(TABLES),
        )
        flags = {row["relname"]: (row[1], row[2]) for row in rows}
        assert flags == dict.fromkeys(TABLES, (True, True))
        privileges = {
            (role, table): await owner.fetchval(
                "SELECT array_agg(p ORDER BY p) FROM unnest(ARRAY['SELECT','INSERT','UPDATE',"
                "'DELETE']) p WHERE has_table_privilege($1, 'mission_control.' || $2, p)",
                role,
                table,
            )
            for role in ("mission_control_runtime", "mission_control_readonly")
            for table in TABLES
        }
        assert privileges[("mission_control_readonly", "mission_chain")] == ["SELECT"]
        assert privileges[("mission_control_runtime", "authoring_provenance")] == [
            "INSERT",
            "SELECT",
        ]
        # Column-limited UPDATE: identity columns are not updatable by the runtime.
        assert await owner.fetchval(
            "SELECT has_column_privilege('mission_control_runtime', "
            "'mission_control.chain_link', 'state', 'UPDATE')"
        )
        assert not await owner.fetchval(
            "SELECT has_column_privilege('mission_control_runtime', "
            "'mission_control.chain_link', 'bindings', 'UPDATE')"
        )
        kinds = await owner.fetchval(
            "SELECT pg_get_constraintdef(oid) FROM pg_constraint "
            "WHERE conname = 'mission_relationship_kind_check'"
        )
        for kind in (
            "composition",
            "fork",
            "dependency",
            "supplies",
            "depends_on",
            "parent_of",
            "adopted_from",
            "successor_of",
            "forked_from",
        ):
            assert f"'{kind}'" in kinds
    finally:
        await owner.close()


async def test_chain_round_trip_is_scoped(common_db: CommonDatabase):
    pool = await common_db.pool("mission_control_runtime")
    try:
        chain = await seed_chain(pool, common_db)
        assert (
            await scoped(pool, common_db.scope(), "SELECT count(*) FROM mission_control.chain_link")
            == 2
        )
        assert await scoped(pool, None, "SELECT count(*) FROM mission_control.chain_link") == 0
        assert (
            await scoped(
                pool,
                common_db.scope("tenant-2"),
                "SELECT count(*) FROM mission_control.mission_chain",
            )
            == 0
        )
        members = await scoped(
            pool,
            common_db.scope(),
            "SELECT members::text FROM mission_control.mission_chain WHERE chain_id = $1",
            chain.chain_id,
        )
        assert [item["mission_key"] for item in json.loads(str(members))] == [
            "research",
            "ingestion",
        ]
        condition = await scoped(
            pool,
            common_db.scope(),
            "SELECT release_condition::text FROM mission_control.chain_link WHERE link_key = $1",
            "research->ingestion:supplies",
        )
        assert json.loads(str(condition)) == {"kind": "goal_accepted", "goal_key": "evidence_map"}
    finally:
        await pool.close()


async def expect_rejected(pool: asyncpg.Pool, db: CommonDatabase, query: str, *args: object) -> str:
    with pytest.raises(asyncpg.PostgresError) as caught:
        await scoped(pool, db.scope(), query, *args)
    return str(caught.value)


async def test_links_move_forward_only(common_db: CommonDatabase):
    pool = await common_db.pool("mission_control_runtime")
    try:
        chain = await seed_chain(pool, common_db)
        supplies, depends = chain.links
        # armed -> released with release facts.
        await scoped(
            pool,
            common_db.scope(),
            "UPDATE mission_control.chain_link SET state = 'released', released_at = $2, "
            "packet_digest = $3, version = version + 1, updated_at = $2 WHERE link_id = $1",
            supplies.link_id,
            NOW,
            "sha256:" + "d" * 64,
        )
        # released -> armed, a stale version, and rewriting release facts are refused.
        for query in (
            "UPDATE mission_control.chain_link SET state = 'armed', version = version + 1 "
            "WHERE link_id = $1",
            "UPDATE mission_control.chain_link SET state = 'detached', version = version + 2 "
            "WHERE link_id = $1",
            "UPDATE mission_control.chain_link SET state = 'detached', packet_digest = NULL, "
            "version = version + 1 WHERE link_id = $1",
        ):
            assert "forward" in await expect_rejected(pool, common_db, query, supplies.link_id)
        # blocked needs a reason (CHECK) and is final.
        await expect_rejected(
            pool,
            common_db,
            "UPDATE mission_control.chain_link SET state = 'blocked', version = version + 1 "
            "WHERE link_id = $1",
            depends.link_id,
        )
        await scoped(
            pool,
            common_db.scope(),
            "UPDATE mission_control.chain_link SET state = 'blocked', "
            "blocked_reason = 'upstream_not_accepted', version = version + 1 WHERE link_id = $1",
            depends.link_id,
        )
        assert "forward" in await expect_rejected(
            pool,
            common_db,
            "UPDATE mission_control.chain_link SET state = 'cancelled', blocked_reason = NULL, "
            "version = version + 1 WHERE link_id = $1",
            depends.link_id,
        )
        # Rows are never deleted (and the runtime has no DELETE grant at all).
        await expect_rejected(
            pool,
            common_db,
            "DELETE FROM mission_control.chain_link WHERE link_id = $1",
            depends.link_id,
        )
    finally:
        await pool.close()


async def test_chain_lifecycle_is_forward_only_and_completion_is_final(common_db: CommonDatabase):
    pool = await common_db.pool("mission_control_runtime")
    try:
        chain = await seed_chain(pool, common_db)
        update = (
            "UPDATE mission_control.mission_chain SET lifecycle = $2, terminal_outcome = $3, "
            "version = version + 1, updated_at = now() WHERE chain_id = $1"
        )
        # completed requires a terminal outcome (CHECK).
        await expect_rejected(pool, common_db, update, chain.chain_id, "completed", None)
        await scoped(pool, common_db.scope(), update, chain.chain_id, "running", None)
        await expect_rejected(pool, common_db, update, chain.chain_id, "pending", None)
        await scoped(pool, common_db.scope(), update, chain.chain_id, "completed", "accepted")
        assert "forward" in await expect_rejected(
            pool, common_db, update, chain.chain_id, "completed", "not_accepted"
        )
    finally:
        await pool.close()
