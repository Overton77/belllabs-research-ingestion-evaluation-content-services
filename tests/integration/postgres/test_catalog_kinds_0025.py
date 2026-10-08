"""FT-A1 on a disposable common database: migration 0025 kinds, host support and plugins."""

from __future__ import annotations

import json
from datetime import UTC, datetime

import asyncpg
import pytest

from mission_control.adapters.postgres.control_plane.catalog_assets import (
    ASSET_KIND,
    definition_asset_id,
)
from mission_control.adapters.postgres.control_plane.definition_repository import (
    PostgresDefinitionRepository,
)
from mission_control.adapters.postgres.scope import apply_catalog_scope_string
from mission_control.domain.authoring.contracts import DefinitionKind, HookScriptDefinition
from mission_control.domain.capabilities.catalog_entry import summarize
from mission_control.domain.capabilities.host_support import (
    HostSupportStatus,
    LaneProfile,
    all_profiles,
)
from tests.integration.postgres.catalog_common import BIOTECH_CATALOG
from tests.integration.postgres.catalog_common import catalog_db as catalog_db
from tests.integration.postgres.catalog_common import catalog_writer_pool as catalog_writer_pool
from tests.integration.postgres.catalog_common import runtime_pool as runtime_pool

pytestmark = pytest.mark.common_db

NOW = datetime(2026, 10, 7, tzinfo=UTC)
ACTOR = "fixture:ft-a1"
HOST_SUPPORT = all_profiles(cursor_cloud=HostSupportStatus.UNQUALIFIED).model_dump(mode="json")


def _digest(index: int) -> str:
    return "sha256:" + f"{index:064x}"


async def _insert(
    connection: asyncpg.Connection,
    *,
    asset_id: str,
    kind: str,
    status: str = "proposed",
    index: int,
    host_support: dict[str, object] | None = None,
    secret_refs: list[str] | None = None,
) -> None:
    await connection.execute(
        """INSERT INTO mission_control.asset_version
           (installation_id, application_id, asset_version_id, asset_id, version, kind,
            contract, manifest_ref, manifest_digest, manifest, required_compatibility,
            status, version_no, updated_at, created_at, created_by_actor_ref,
            host_support, secret_refs)
           SELECT mission_control.ctx_installation_id(), mission_control.ctx_application_id(),
                  gen_random_uuid(), $1, '1', $2, 'fixture/1', 'fixture://' || $1, $3,
                  '{}'::jsonb, '{}'::text[], $4, 1, clock_timestamp(), clock_timestamp(), $5,
                  $6::jsonb, $7::text[]""",
        asset_id,
        kind,
        _digest(index),
        status,
        ACTOR,
        json.dumps(host_support or HOST_SUPPORT),
        secret_refs or [],
    )


async def _admit(connection: asyncpg.Connection, asset_id: str) -> None:
    await connection.execute(
        """UPDATE mission_control.asset_version
           SET status='admitted', version_no=version_no + 1, updated_at=clock_timestamp()
           WHERE asset_id=$1 AND version='1'""",
        asset_id,
    )


async def _member(
    connection: asyncpg.Connection, member: str, position: int, index: int, role: str
) -> None:
    await connection.execute(
        """INSERT INTO mission_control.capability_plugin_member
           (installation_id, application_id, plugin_asset_id, plugin_version, position,
            member_asset_id, member_version, member_digest, role, optional, created_at,
            created_by_actor_ref)
           VALUES (mission_control.ctx_installation_id(), mission_control.ctx_application_id(),
                   'plugin.web-research', '1', $1, $2, '1', $3, $4, false,
                   clock_timestamp(), $5)""",
        position,
        member,
        _digest(index),
        role,
        ACTOR,
    )


@pytest.mark.asyncio
async def test_new_kinds_host_support_and_plugin_admission(catalog_writer_pool):
    async with catalog_writer_pool.acquire() as connection:
        async with connection.transaction():
            await apply_catalog_scope_string(connection, BIOTECH_CATALOG)
            for index, kind in enumerate(
                ("skill_bundle", "mcp_server", "mcp_tool", "hook_script", "subagent_profile"),
                start=1,
            ):
                await _insert(
                    connection,
                    asset_id=f"fixture.{kind}",
                    kind=kind,
                    index=index,
                    secret_refs=["TAVILY_API_KEY"] if kind == "mcp_server" else [],
                )
            await _insert(connection, asset_id="plugin.web-research", kind="plugin", index=9)
            await _member(connection, "fixture.mcp_server", 0, 2, "mcp_server")
            await _member(connection, "fixture.skill_bundle", 1, 1, "skill")
        async with connection.transaction():
            await apply_catalog_scope_string(connection, BIOTECH_CATALOG)
            rows = await connection.fetch(
                "SELECT kind, host_support, secret_refs FROM mission_control.asset_version "
                "WHERE created_by_actor_ref=$1 ORDER BY asset_id",
                ACTOR,
            )
        assert len(rows) == 6
        assert {row["kind"] for row in rows} == {
            "skill_bundle",
            "mcp_server",
            "mcp_tool",
            "hook_script",
            "subagent_profile",
            "plugin",
        }
        assert all(
            json.loads(row["host_support"])["profiles"]["cursor_cloud"]["status"] == "unqualified"
            for row in rows
        )

        # Admitting the plugin while a member is proposed fails at commit (deferred trigger).
        with pytest.raises(asyncpg.CheckViolationError, match="member is not admitted"):
            async with connection.transaction():
                await apply_catalog_scope_string(connection, BIOTECH_CATALOG)
                await _admit(connection, "plugin.web-research")
                await _admit(connection, "fixture.mcp_server")

        async with connection.transaction():
            await apply_catalog_scope_string(connection, BIOTECH_CATALOG)
            await _admit(connection, "fixture.mcp_server")
            await _admit(connection, "fixture.skill_bundle")
            await _admit(connection, "plugin.web-research")
        status = await connection.fetchval(
            "SELECT status FROM mission_control.asset_version WHERE asset_id='plugin.web-research'"
        )
        assert status is None  # RLS: no scope bound outside a transaction
        async with connection.transaction():
            await apply_catalog_scope_string(connection, BIOTECH_CATALOG)
            assert (
                await connection.fetchval(
                    "SELECT status FROM mission_control.asset_version "
                    "WHERE asset_id='plugin.web-research'"
                )
                == "admitted"
            )
            members = await connection.fetch(
                "SELECT member_asset_id, role FROM mission_control.capability_plugin_member "
                "WHERE plugin_asset_id='plugin.web-research' ORDER BY position"
            )
            assert [(row["member_asset_id"], row["role"]) for row in members] == [
                ("fixture.mcp_server", "mcp_server"),
                ("fixture.skill_bundle", "skill"),
            ]


@pytest.mark.asyncio
async def test_plugin_without_members_and_bad_member_digest_are_refused(catalog_writer_pool):
    async with catalog_writer_pool.acquire() as connection:
        with pytest.raises(asyncpg.CheckViolationError, match="without recorded members"):
            async with connection.transaction():
                await apply_catalog_scope_string(connection, BIOTECH_CATALOG)
                await _insert(
                    connection,
                    asset_id="plugin.web-research",
                    kind="plugin",
                    index=9,
                    status="admitted",
                )
        async with connection.transaction():
            await apply_catalog_scope_string(connection, BIOTECH_CATALOG)
            await _insert(connection, asset_id="fixture.skill_bundle", kind="skill_bundle", index=1)
            await _insert(connection, asset_id="plugin.web-research", kind="plugin", index=9)
        with pytest.raises(asyncpg.CheckViolationError, match="digest does not match"):
            async with connection.transaction():
                await apply_catalog_scope_string(connection, BIOTECH_CATALOG)
                await _member(connection, "fixture.skill_bundle", 0, 7, "skill")
        with pytest.raises(asyncpg.CheckViolationError, match="must reference a plugin"):
            async with connection.transaction():
                await apply_catalog_scope_string(connection, BIOTECH_CATALOG)
                await connection.execute(
                    """INSERT INTO mission_control.capability_plugin_member
                       (installation_id, application_id, plugin_asset_id, plugin_version,
                        position, member_asset_id, member_version, member_digest, role,
                        created_at, created_by_actor_ref)
                       VALUES (mission_control.ctx_installation_id(),
                               mission_control.ctx_application_id(), 'fixture.skill_bundle',
                               '1', 0, 'plugin.web-research', '1', $1, 'resource',
                               clock_timestamp(), $2)""",
                    _digest(9),
                    ACTOR,
                )


@pytest.mark.asyncio
async def test_host_support_and_secret_refs_are_checked_and_immutable(catalog_writer_pool):
    async with catalog_writer_pool.acquire() as connection:
        for bad_support, bad_refs in (
            ({"schema_version": "mc.capability_host_support.v1", "profiles": {"x": {}}}, None),
            (
                {
                    "schema_version": "mc.capability_host_support.v1",
                    "profiles": {"codex": {"status": "maybe"}},
                },
                None,
            ),
            (None, ["sk-live-secret-value"]),
        ):
            with pytest.raises(asyncpg.CheckViolationError):
                async with connection.transaction():
                    await apply_catalog_scope_string(connection, BIOTECH_CATALOG)
                    await _insert(
                        connection,
                        asset_id="fixture.bad",
                        kind="mcp_server",
                        index=3,
                        host_support=bad_support,
                        secret_refs=bad_refs,
                    )
        with pytest.raises(asyncpg.CheckViolationError):
            async with connection.transaction():
                await apply_catalog_scope_string(connection, BIOTECH_CATALOG)
                await _insert(connection, asset_id="fixture.legacy", kind="plugin_package", index=4)


@pytest.mark.asyncio
async def test_host_support_columns_are_owner_immutable(catalog_db):
    connection = await asyncpg.connect(catalog_db.owner_dsn)
    try:
        async with connection.transaction():
            await apply_catalog_scope_string(connection, BIOTECH_CATALOG)
            await _insert(connection, asset_id="fixture.hook", kind="hook_script", index=5)
        with pytest.raises(asyncpg.RestrictViolationError, match="immutable"):
            async with connection.transaction():
                await apply_catalog_scope_string(connection, BIOTECH_CATALOG)
                await connection.execute(
                    "UPDATE mission_control.asset_version SET secret_refs='{X}' "
                    "WHERE asset_id='fixture.hook'"
                )
    finally:
        await connection.close()


@pytest.mark.asyncio
async def test_published_hook_script_writes_kind_and_host_support(runtime_pool):
    repository = PostgresDefinitionRepository(runtime_pool, catalog_scope=BIOTECH_CATALOG)
    hook = HookScriptDefinition.model_validate(
        {
            "logical_id": "hook.mc-policy-template",
            "title": "Policy template",
            "description": "Denies destructive shell commands.",
            "events": ["before_shell", "before_tool"],
            "fail_closed": True,
            "file_manifest": [{"path": "policy.py", "digest": _digest(1), "size_bytes": 10}],
            "manifest_digest": _digest(2),
            "entrypoint": "policy.py",
            "interpreter": "python",
            "host_support": HOST_SUPPORT,
        }
    )
    await repository.save_draft(hook, "author", NOW, 0)
    published = await repository.publish(hook, "publisher", NOW, 0, 1)
    listed = await repository.list_published_definitions()
    assert [item.ref for item in listed] == [published.ref]
    summary = summarize(listed[0])
    assert summary.kind is DefinitionKind.HOOK_SCRIPT
    assert LaneProfile.CURSOR_CLOUD not in summary.supported_profiles
    async with runtime_pool.acquire() as connection:
        async with connection.transaction():
            await apply_catalog_scope_string(connection, BIOTECH_CATALOG)
            row = await connection.fetchrow(
                "SELECT kind, host_support FROM mission_control.asset_version WHERE asset_id=$1",
                definition_asset_id(DefinitionKind.HOOK_SCRIPT, hook.logical_id),
            )
    assert row["kind"] == ASSET_KIND[DefinitionKind.HOOK_SCRIPT] == "hook_script"
    assert json.loads(row["host_support"]) == HOST_SUPPORT
