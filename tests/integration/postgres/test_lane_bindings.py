"""FT-G1 migration 0030 on a disposable PostgreSQL 17: the lane profile registry and the
execution binding lane columns, through real grants and forced RLS."""

from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime

import asyncpg
import pytest

from mission_control.application.execution.harness.describe import DECLARED_LANE_MATRICES
from tests.fixtures.mission_control_common_db import CommonDatabase
from tests.integration.postgres.runtime_common import common_db  # noqa: F401

pytestmark = pytest.mark.common_db

DIGEST = "sha256:" + "d" * 64


async def test_lane_profiles_are_seeded_with_their_declared_describes(
    common_db: CommonDatabase,  # noqa: F811
) -> None:
    owner = await asyncpg.connect(common_db.owner_dsn)
    try:
        rows = await owner.fetch(
            "SELECT lane_profile, lane, placement, describe, qualified, qualification_ref "
            "FROM mission_control.lane_profile ORDER BY lane_profile"
        )
    finally:
        await owner.close()
    assert [row["lane_profile"] for row in rows] == ["cursor_cloud", "cursor_local", "deep_agents"]
    for row in rows:
        declared = DECLARED_LANE_MATRICES[row["lane_profile"]]
        assert json.loads(row["describe"]) == declared.model_dump(mode="json")
        assert row["lane"] == declared.lane and row["placement"] == declared.placement
        assert row["qualified"] is declared.qualified
        assert (row["qualification_ref"] is not None) is declared.qualified


async def test_runtime_reads_but_never_writes_lane_profiles(
    common_db: CommonDatabase,  # noqa: F811
) -> None:
    pool = await common_db.pool("mission_control_runtime")
    try:
        async with pool.acquire() as connection:
            count = await connection.fetchval("SELECT count(*) FROM mission_control.lane_profile")
            assert count == 3
            with pytest.raises(asyncpg.InsufficientPrivilegeError):
                await connection.execute(
                    "UPDATE mission_control.lane_profile SET qualified = true "
                    "WHERE lane_profile = 'cursor_local'"
                )
            with pytest.raises(asyncpg.InsufficientPrivilegeError):
                await connection.execute(
                    "INSERT INTO mission_control.lane_profile (lane_profile, lane, placement, "
                    "describe) VALUES ('codex', 'cursor', 'cloud', '{}'::jsonb)"
                )
    finally:
        await pool.close()


async def _insert_binding(
    connection: asyncpg.Connection,
    db: CommonDatabase,
    lane_profile: str | None,
    lane_binding: object,
) -> None:
    tenant = db.tenants["tenant-1"]
    columns = (
        "installation_id, application_id, tenant_id, execution_binding_id, binding_key, "
        "binding_contract, manifest, manifest_digest, admission_decision, admitted_at, "
        "created_at, created_by_actor_ref"
    )
    values = (
        "$1, $2, $3, $4, $5, 'operation-binding/1', '{}'::jsonb, $6, 'admitted', $7, $7, 'ft-g1'"
    )
    args: list[object] = [
        db.installation_id,
        db.application_id,
        tenant,
        uuid.uuid4(),
        f"binding:{uuid.uuid4()}",
        DIGEST,
        datetime.now(UTC),
    ]
    if lane_profile is not None:
        columns += ", lane_profile, lane_binding"
        values += ", $8, $9::jsonb"
        args.extend([lane_profile, json.dumps(lane_binding) if lane_binding is not None else None])
    await connection.execute(
        f"INSERT INTO mission_control.execution_binding ({columns}) VALUES ({values})", *args
    )


async def test_execution_bindings_default_to_deep_agents_and_cursor_requires_its_binding(
    common_db: CommonDatabase,  # noqa: F811
) -> None:
    owner = await asyncpg.connect(common_db.owner_dsn)
    try:
        await _insert_binding(owner, common_db, None, None)
        assert await owner.fetchval(
            "SELECT array_agg(DISTINCT lane_profile) FROM mission_control.execution_binding"
        ) == ["deep_agents"]
        with pytest.raises(asyncpg.CheckViolationError):
            await _insert_binding(owner, common_db, "cursor_local", None)
        with pytest.raises(asyncpg.CheckViolationError):
            await _insert_binding(
                owner,
                common_db,
                "cursor_local",
                {"schema_version": "mc.cursor_binding.v1", "lane_profile": "cursor_cloud"},
            )
        with pytest.raises(asyncpg.ForeignKeyViolationError):
            await _insert_binding(owner, common_db, "codex", None)
        await _insert_binding(
            owner,
            common_db,
            "cursor_local",
            {"schema_version": "mc.cursor_binding.v1", "lane_profile": "cursor_local"},
        )
        assert (
            await owner.fetchval(
                "SELECT count(*) FROM mission_control.execution_binding WHERE lane_profile = $1",
                "cursor_local",
            )
            == 1
        )
    finally:
        await owner.close()
