"""Migration 0033 (component 1.2.0) on a real disposable PostgreSQL 17.

Runs against MISSION_CONTROL_TEST_ADMIN_DSN (loopback disposable server only).

- MP-11 `approval_correlation`/`governed_effect_intent` and MP-15 `coordinator_inbox`,
  `coordinator_notification`, `coordinator_causation`: tenant scoped, forced RLS with one
  three-column scope policy, a tenant FK, REVOKE PUBLIC, runtime SELECT/INSERT plus the declared
  column-limited UPDATE grants, readonly SELECT, nothing for any other capability role.
- The guards refuse what they should: a closed correlation and a settled intent never move
  (including a settled intent's receipt digest), bound columns never change, nothing is deleted,
  causation is immutable and must name an existing inbox.
- The lane describe refresh: every `lane_profile` row equals `DECLARED_LANE_MATRICES`, still
  unqualified, and `lane_profile_immutable` plus FORCE RLS are restored.
- A NON-superuser CREATEROLE migration principal (the Supabase-like installer, package cluster
  B pattern) applies 0001..0033 and ends in the same lane/trigger/RLS state.
"""

from __future__ import annotations

import json
import secrets
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

import asyncpg
import pytest
import pytest_asyncio

from mission_control.adapters.postgres.run_control.run_control_repository import (
    PostgresRunControlRepository,
)
from mission_control.application.execution.harness.describe import DECLARED_LANE_MATRICES
from tests.fixtures.mission_control_common_db import (
    CommonDatabase,
    _with_database,
    admin_dsn,
    install_component,
    migration_paths,
)
from tests.integration.postgres.runtime_common import (  # noqa: F401
    common_db,
    owner_rows,
    scoped_count,
)
from tests.unit.run_control.test_run_control import request, service

pytestmark = pytest.mark.common_db

DIGEST = "sha256:" + "d" * 64
ACTOR = "test:release-0033"
TENANT_TABLES = (
    "approval_correlation",
    "governed_effect_intent",
    "coordinator_inbox",
    "coordinator_notification",
    "coordinator_causation",
)
RUNTIME_UPDATE_COLUMNS: dict[str, frozenset[str]] = {
    "approval_correlation": frozenset(
        {"state", "reply", "reply_digest", "close_reason", "replayed_from", "closed_at", "version"}
    ),
    "governed_effect_intent": frozenset(
        {
            "state",
            "reason",
            "claimant_ref",
            "claimed_at",
            "receipt",
            "receipt_digest",
            "version",
            "updated_at",
        }
    ),
    "coordinator_inbox": frozenset({"next_inbox_seq", "acked_inbox_seq", "version", "updated_at"}),
    "coordinator_notification": frozenset(
        {
            "inbox_seq",
            "state",
            "actionable",
            "seq_from",
            "seq_to",
            "event_count",
            "anchor_event_id",
            "depth",
            "suppressed_reason",
            "body",
            "sealed_at",
            "acknowledged_at",
            "acknowledged_by_actor_ref",
            "prompt_request_id",
            "prompt_state",
            "prompt_request",
            "prompt_detail",
            "prompted_at",
            "version",
            "updated_at",
        }
    ),
    "coordinator_causation": frozenset(),
}
OTHER_ROLES = (
    "mission_control_family_writer",
    "mission_control_catalog_writer",
    "mission_control_outbox_worker",
    "public",
)
TABLE_PRIVILEGES = ("SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE", "REFERENCES", "TRIGGER")


def _declared_describes() -> dict[str, Any]:
    return {
        profile: describe.model_dump(mode="json")
        for profile, describe in DECLARED_LANE_MATRICES.items()
    }


async def _lane_state(connection: asyncpg.Connection) -> dict[str, Any]:
    rows = await connection.fetch(
        "SELECT lane_profile, describe::text AS describe, qualified, qualified_at, "
        "qualification_ref FROM mission_control.lane_profile"
    )
    table = await connection.fetchrow(
        "SELECT c.relrowsecurity, c.relforcerowsecurity, pg_get_userbyid(c.relowner) AS owner, "
        "(SELECT t.tgenabled::text FROM pg_trigger t WHERE t.tgrelid = c.oid "
        " AND t.tgname = 'lane_profile_immutable') AS trigger_enabled, "
        "(SELECT array_agg(p.polname || ':' || p.polcmd::text) FROM pg_policy p "
        " WHERE p.polrelid = c.oid) AS policies "
        "FROM pg_class c WHERE c.oid = 'mission_control.lane_profile'::regclass"
    )
    return {"rows": {row["lane_profile"]: row for row in rows}, "table": dict(table)}


def _assert_lane_profiles_declared(state: dict[str, Any]) -> None:
    declared = _declared_describes()
    assert set(state["rows"]) == set(declared)
    for profile, row in state["rows"].items():
        assert json.loads(row["describe"]) == declared[profile], profile
        expected_qualified = profile == "deep_agents"
        assert row["qualified"] is expected_qualified, profile
        if not expected_qualified:
            assert (row["qualified_at"], row["qualification_ref"]) == (None, None), profile
    table = state["table"]
    assert (table["relrowsecurity"], table["relforcerowsecurity"]) == (True, True)
    # 'O' = enabled in origin and local mode, exactly how CREATE TRIGGER left it in 0030.
    assert table["trigger_enabled"] == "O"
    assert table["policies"] == ["lane_profile_read:r"]


# --------------------------------------------------------------------------- catalog shape


async def test_0033_tables_are_scoped_forced_rls_with_declared_grants(
    common_db: CommonDatabase,  # noqa: F811
) -> None:
    rows = await owner_rows(
        common_db,
        """
        SELECT c.relname, c.relrowsecurity, c.relforcerowsecurity,
               (SELECT array_agg(pg_get_expr(p.polqual, p.polrelid) || ' / '
                                 || pg_get_expr(p.polwithcheck, p.polrelid))
                FROM pg_policy p WHERE p.polrelid = c.oid) AS policies,
               EXISTS (SELECT 1 FROM pg_constraint k WHERE k.conrelid = c.oid
                       AND k.contype = 'f'
                       AND k.confrelid = 'mission_control.tenant'::regclass) AS tenant_fk,
               (SELECT array_agg(a.attname ORDER BY a.attnum) FROM pg_attribute a
                WHERE a.attrelid = c.oid AND a.attnum IN (1, 2, 3)) AS scope_columns,
               (SELECT count(*) FROM aclexplode(c.relacl) x WHERE x.grantee = 0) AS public_acl
        FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'mission_control' AND c.relname = ANY($1::text[])
        """,
        list(TENANT_TABLES),
    )
    assert {row["relname"] for row in rows} == set(TENANT_TABLES)
    for row in rows:
        name = row["relname"]
        assert (row["relrowsecurity"], row["relforcerowsecurity"]) == (True, True), name
        assert row["tenant_fk"], name
        assert row["scope_columns"] == ["installation_id", "application_id", "tenant_id"], name
        assert row["public_acl"] == 0, name
        (policy,) = row["policies"]
        for function in ("ctx_installation_id", "ctx_application_id", "ctx_tenant_id"):
            assert policy.count(function) == 2, (name, policy)  # USING and WITH CHECK
    for table, update_columns in RUNTIME_UPDATE_COLUMNS.items():
        assert await _table_privileges(common_db, "mission_control_runtime", table) == {
            "SELECT",
            "INSERT",
        }, table
        assert await _update_columns(common_db, "mission_control_runtime", table) == (
            update_columns
        ), table
        assert await _table_privileges(common_db, "mission_control_readonly", table) == {
            "SELECT"
        }, table
        assert await _update_columns(common_db, "mission_control_readonly", table) == set(), table
        for role in OTHER_ROLES:
            assert await _table_privileges(common_db, role, table) == set(), (role, table)
            assert await _update_columns(common_db, role, table) == set(), (role, table)
    # Every new foreign key is covered by an index whose leading columns are the FK columns.
    uncovered = await owner_rows(
        common_db,
        """
        SELECT k.conrelid::regclass::text AS relation, k.conname
        FROM pg_constraint k
        WHERE k.contype = 'f' AND k.conrelid = ANY(
            SELECT ('mission_control.' || t)::regclass FROM unnest($1::text[]) AS t)
          AND NOT EXISTS (
              SELECT 1 FROM pg_index i
              WHERE i.indrelid = k.conrelid
                AND (i.indkey::int2[])[0:cardinality(k.conkey) - 1] = k.conkey)
        """,
        list(TENANT_TABLES),
    )
    assert [dict(row) for row in uncovered] == []
    # The guard functions are not executable by PUBLIC.
    for function in ("governed_effect_intent_guard", "approval_correlation_guard"):
        assert not (
            await owner_rows(
                common_db,
                "SELECT has_function_privilege('public', $1, 'EXECUTE') AS allowed",
                f"mission_control.{function}()",
            )
        )[0]["allowed"], function


async def _table_privileges(db: CommonDatabase, role: str, table: str) -> set[str]:
    rows = await owner_rows(
        db,
        "SELECT privilege FROM unnest($3::text[]) AS privilege "
        "WHERE has_table_privilege($1, 'mission_control.' || $2, privilege)",
        role,
        table,
        list(TABLE_PRIVILEGES),
    )
    return {row["privilege"] for row in rows}


async def _update_columns(db: CommonDatabase, role: str, table: str) -> set[str]:
    rows = await owner_rows(
        db,
        "SELECT a.attname FROM pg_attribute a "
        "WHERE a.attrelid = ('mission_control.' || $2)::regclass AND a.attnum > 0 "
        "AND NOT a.attisdropped AND has_column_privilege($1, a.attrelid, a.attnum, 'UPDATE')",
        role,
        table,
    )
    return {row["attname"] for row in rows}


# --------------------------------------------------------------------------- guards


async def _admitted_run(db: CommonDatabase) -> tuple[str, UUID, UUID]:
    pool = await db.pool(max_size=2)
    try:
        authority, _ = service(PostgresRunControlRepository(pool))  # type: ignore[arg-type]
        admission = await authority.admit(
            request(request_scope=db.scope(), request_id=str(uuid4()))
        )
        assert admission.run_id is not None
    finally:
        await pool.close()
    (row,) = await owner_rows(
        db,
        "SELECT run_id, mission_id FROM mission_control.mission_run WHERE run_key = $1",
        admission.run_id,
    )
    return admission.run_id, row["run_id"], row["mission_id"]


@pytest_asyncio.fixture
async def owner(common_db: CommonDatabase) -> AsyncIterator[asyncpg.Connection]:  # noqa: F811
    connection = await asyncpg.connect(common_db.owner_dsn)
    try:
        yield connection
    finally:
        await connection.close()


def _scope(db: CommonDatabase) -> tuple[UUID, str, UUID]:
    return db.installation_id, db.application_id, db.tenants["tenant-1"]


async def test_mp11_guards_refuse_moving_closed_or_settled_rows(
    common_db: CommonDatabase,  # noqa: F811
    owner: asyncpg.Connection,
) -> None:
    scope = _scope(common_db)
    run_key, run_id, _mission_id = await _admitted_run(common_db)
    now = datetime.now(UTC)
    task_id, correlation_id, intent_id = uuid4(), uuid4(), uuid4()
    await owner.execute(
        "INSERT INTO mission_control.human_task (installation_id, application_id, tenant_id, "
        "human_task_id, task_key, target_ref, kind, request_packet_ref, assignee_scope, "
        "lifecycle, version, updated_at, created_at, created_by_actor_ref) VALUES "
        "($1, $2, $3, $4, $5, $6, 'approval:provider_permission', 'inline:test', 'tenant', "
        "'open', 1, $7, $7, $8)",
        *scope,
        task_id,
        f"task:{task_id}",
        f"run:{run_key}",
        now,
        ACTOR,
    )
    await owner.execute(
        "INSERT INTO mission_control.approval_correlation (installation_id, application_id, "
        "tenant_id, approval_correlation_id, human_task_id, run_key, harness_execution_id, "
        "generation, connection_ref, native, input_digest, state, opened_at, version, "
        "created_by_actor_ref) VALUES ($1, $2, $3, $4, $5, $6, 'hx-1', 1, 'conn-1', "
        "'{\"request_id\": \"r-1\"}', $7, 'live', $8, 1, $9)",
        *scope,
        correlation_id,
        task_id,
        run_key,
        DIGEST,
        now,
        ACTOR,
    )
    where = f"WHERE approval_correlation_id = '{correlation_id}'"
    with pytest.raises(asyncpg.RestrictViolationError, match="bound to its native request"):
        await owner.execute(
            f"UPDATE mission_control.approval_correlation SET native = '{{}}' {where}"
        )
    await owner.execute(
        "UPDATE mission_control.approval_correlation SET state = 'answered', "
        f"reply = '{{\"allow\": true}}', reply_digest = '{DIGEST}', closed_at = now(), "
        f"version = 2 {where}"
    )
    with pytest.raises(asyncpg.RestrictViolationError, match="closed approval correlation"):
        await owner.execute(
            f"UPDATE mission_control.approval_correlation SET close_reason = 'x' {where}"
        )
    with pytest.raises(asyncpg.RestrictViolationError, match="never deleted"):
        await owner.execute(f"DELETE FROM mission_control.approval_correlation {where}")

    await owner.execute(
        "INSERT INTO mission_control.governed_effect_intent (installation_id, application_id, "
        "tenant_id, governed_effect_intent_id, intent_key, run_id, run_key, "
        "harness_execution_id, generation, lane_profile, tool_name, effect_kind, arguments, "
        "input_digest, policy_digest, state, version, created_at, updated_at, "
        "created_by_actor_ref) VALUES ($1, $2, $3, $4, $5, $6, $7, 'hx-1', 1, "
        "'claude_agent_sdk', 'shell.exec', 'shell', '{\"cmd\": \"true\"}', $8, $8, 'ready', 1, "
        "$9, $9, $10)",
        *scope,
        intent_id,
        f"intent:{intent_id}",
        run_id,
        run_key,
        DIGEST,
        now,
        ACTOR,
    )
    where = f"WHERE governed_effect_intent_id = '{intent_id}'"
    with pytest.raises(asyncpg.RestrictViolationError, match="bound to its arguments"):
        await owner.execute(
            f"UPDATE mission_control.governed_effect_intent SET arguments = '{{}}' {where}"
        )
    await owner.execute(
        "UPDATE mission_control.governed_effect_intent SET state = 'executing', "
        f"claimant_ref = 'worker-1', claimed_at = now(), version = 2 {where}"
    )
    await owner.execute(
        "UPDATE mission_control.governed_effect_intent SET state = 'executed', "
        f"receipt = '{{\"exit\": 0}}', receipt_digest = '{DIGEST}', version = 3 {where}"
    )
    other = "sha256:" + "e" * 64
    for change in (
        f"receipt_digest = '{other}'",  # the receipt digest of a settled intent never moves
        "receipt = '{\"exit\": 1}'",
        "reason = 'late'",
        "state = 'in_doubt'",
    ):
        with pytest.raises(asyncpg.RestrictViolationError):
            await owner.execute(
                f"UPDATE mission_control.governed_effect_intent SET {change} {where}"
            )
    with pytest.raises(asyncpg.RestrictViolationError, match="never deleted"):
        await owner.execute(f"DELETE FROM mission_control.governed_effect_intent {where}")
    # Forced RLS: another tenant's runtime scope sees none of it.
    pool = await common_db.pool(max_size=2)
    try:
        for table in ("approval_correlation", "governed_effect_intent"):
            assert await scoped_count(pool, common_db.scope(), table) == 1, table
            assert await scoped_count(pool, common_db.scope("tenant-2"), table) == 0, table
            assert await scoped_count(pool, None, table) == 0, table
    finally:
        await pool.close()


async def test_mp15_guards_keep_causation_immutable_and_nothing_deleted(
    common_db: CommonDatabase,  # noqa: F811
    owner: asyncpg.Connection,
) -> None:
    scope = _scope(common_db)
    _run_key, _run_id, mission_id = await _admitted_run(common_db)
    (event,) = await owner_rows(
        common_db,
        "SELECT event_id FROM mission_control.mission_event WHERE mission_id = $1 "
        "ORDER BY seq LIMIT 1",
        mission_id,
    )
    now = datetime.now(UTC)
    subscription_id, notification_id, request_id = uuid4(), uuid4(), uuid4()
    await owner.execute(
        "INSERT INTO mission_control.mission_subscription (installation_id, application_id, "
        "tenant_id, subscription_id, target_kind, mission_id, event_types, channel_kind, "
        "channel, cursor_seq, state, failure_count, next_attempt_at, version, created_at, "
        "updated_at, created_by_actor_ref) VALUES ($1, $2, $3, $4, 'mission', $5, "
        "ARRAY['human_task.created'], 'stream_ticket', "
        '\'{"kind": "stream_ticket", "ticket": "coordinator-inbox:t"}\', 0, \'active\', 0, '
        "$6, 1, $6, $6, $7)",
        *scope,
        subscription_id,
        mission_id,
        now,
        ACTOR,
    )
    await owner.execute(
        "INSERT INTO mission_control.coordinator_inbox (installation_id, application_id, "
        "tenant_id, subscription_id, coordinator_ref, profile, delivery_kind, delivery, "
        "prompting, next_inbox_seq, acked_inbox_seq, version, created_at, updated_at, "
        "created_by_actor_ref) VALUES ($1, $2, $3, $4, 'coordinator-1', '{}', 'poll', "
        '\'{"kind": "poll"}\', false, 1, 0, 1, $5, $5, $6)',
        *scope,
        subscription_id,
        now,
        ACTOR,
    )
    await owner.execute(
        "INSERT INTO mission_control.coordinator_notification (installation_id, "
        "application_id, tenant_id, notification_id, subscription_id, state, kind, "
        "actionable, mission_id, seq_from, seq_to, event_count, anchor_event_id, depth, body, "
        "opened_at, version, created_at, updated_at, created_by_actor_ref) VALUES ($1, $2, $3, "
        "$4, $5, 'open', 'progress', false, $6, 1, 1, 1, $7, 0, "
        '\'{"schema_version": "mc.coordinator_notification.v1"}\', $8, 1, $8, $8, $9)',
        *scope,
        notification_id,
        subscription_id,
        mission_id,
        event["event_id"],
        now,
        ACTOR,
    )
    causation = (
        "INSERT INTO mission_control.coordinator_causation (installation_id, application_id, "
        "tenant_id, command_request_id, subscription_id, notification_id, origin, "
        "target_run_ref, depth, recorded_at, created_at, created_by_actor_ref) VALUES ($1, $2, "
        "$3, $4, $5, $6, 'prompt', 'run:coordinator', 1, $7, $7, $8)"
    )
    # Causation must name an existing inbox (FK added in review), not just a notification.
    with pytest.raises(asyncpg.ForeignKeyViolationError):
        await owner.execute(causation, *scope, uuid4(), uuid4(), notification_id, now, ACTOR)
    await owner.execute(causation, *scope, request_id, subscription_id, notification_id, now, ACTOR)
    with pytest.raises(asyncpg.RestrictViolationError, match="immutable"):
        await owner.execute(
            "UPDATE mission_control.coordinator_causation SET depth = 2 "
            f"WHERE command_request_id = '{request_id}'"
        )
    for table, key, value in (
        ("coordinator_causation", "command_request_id", request_id),
        ("coordinator_notification", "notification_id", notification_id),
        ("coordinator_inbox", "subscription_id", subscription_id),
    ):
        with pytest.raises(asyncpg.RestrictViolationError, match="immutable"):
            await owner.execute(f"DELETE FROM mission_control.{table} WHERE {key} = '{value}'")
    pool = await common_db.pool(max_size=2)
    try:
        for table in ("coordinator_inbox", "coordinator_notification", "coordinator_causation"):
            assert await scoped_count(pool, common_db.scope(), table) == 1, table
            assert await scoped_count(pool, common_db.scope("tenant-2"), table) == 0, table
    finally:
        await pool.close()


# --------------------------------------------------------------------------- lane profiles


async def test_lane_profiles_equal_declared_describes_with_trigger_and_rls_restored(
    common_db: CommonDatabase,  # noqa: F811
    owner: asyncpg.Connection,
) -> None:
    _assert_lane_profiles_declared(await _lane_state(owner))
    # The immutability trigger is live again: even the owner cannot revise a row.
    with pytest.raises(asyncpg.RestrictViolationError, match="immutable"):
        await owner.execute(
            "UPDATE mission_control.lane_profile SET describe = describe "
            "WHERE lane_profile = 'cursor_local'"
        )
    receipts = await owner_rows(
        common_db,
        "SELECT component_version, migration_key FROM mission_control.component_release "
        "WHERE migration_key LIKE '0033_%'",
    )
    assert [tuple(row) for row in receipts] == [
        ("1.2.0", "0033_approvals_coordinator_inbox_lane_describes")
    ]
    runtime = await asyncpg.connect(common_db.dsn())
    try:
        # Reference data: readable without tenant scope, never writable by the runtime role.
        assert await runtime.fetchval("SELECT count(*) FROM mission_control.lane_profile") == 7
        with pytest.raises(asyncpg.InsufficientPrivilegeError):
            await runtime.execute(
                "UPDATE mission_control.lane_profile SET qualified = qualified "
                "WHERE lane_profile = 'codex'"
            )
    finally:
        await runtime.close()


async def test_non_superuser_migration_principal_applies_0001_to_0033() -> None:
    server = admin_dsn()
    suffix = secrets.token_hex(5)
    database, migrator, password = f"mct_{suffix}", f"mct_{suffix}_migrator", secrets.token_hex(16)
    admin = await asyncpg.connect(server)
    try:
        await admin.execute(f'CREATE DATABASE "{database}"')
        await admin.execute(
            f'CREATE ROLE "{migrator}" LOGIN CREATEROLE NOSUPERUSER NOBYPASSRLS NOCREATEDB '
            f"PASSWORD '{password}'"
        )
        await admin.execute(f'GRANT CONNECT, CREATE ON DATABASE "{database}" TO "{migrator}"')
    finally:
        await admin.close()
    try:
        superuser = await asyncpg.connect(_with_database(server, database))
        try:
            await superuser.execute("CREATE SCHEMA extensions")
            for extension in ("vector", "pgcrypto", "pg_trgm"):
                await superuser.execute(f"CREATE EXTENSION {extension} SCHEMA extensions")
            await superuser.execute("GRANT USAGE ON SCHEMA extensions TO PUBLIC")
        finally:
            await superuser.close()
        connection = await asyncpg.connect(
            _with_database(server, database, user=migrator, password=password)
        )
        try:
            assert not await connection.fetchval(
                "SELECT rolsuper OR rolbypassrls FROM pg_roles WHERE rolname = current_user"
            )
            applied = await install_component(connection)
            assert applied == [path.stem for path in migration_paths()]
            assert applied[-1] == "0033_approvals_coordinator_inbox_lane_describes"
            state = await _lane_state(connection)
            _assert_lane_profiles_declared(state)
            assert state["table"]["owner"] == migrator
            owners = await connection.fetchval(
                "SELECT array_agg(DISTINCT pg_get_userbyid(c.relowner)) FROM pg_class c "
                "WHERE c.oid = ANY($1::regclass[])",
                [f"mission_control.{table}" for table in TENANT_TABLES],
            )
            assert owners == [migrator]
        finally:
            await connection.close()
    finally:
        admin = await asyncpg.connect(server)
        try:
            await admin.execute(f'DROP DATABASE IF EXISTS "{database}" WITH (FORCE)')
            await admin.execute(f'DROP ROLE IF EXISTS "{migrator}"')
        finally:
            await admin.close()
