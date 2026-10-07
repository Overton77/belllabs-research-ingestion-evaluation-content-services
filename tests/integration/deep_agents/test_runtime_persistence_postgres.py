"""Real PostgreSQL proofs for the private ``mission_control_runtime`` saver/store schema.

Requires ``MISSION_CONTROL_TEST_ADMIN_DSN``: a loopback administrator DSN for a disposable
``pgvector/pgvector:pg17`` server (the shared ``tests/fixtures/mission_control_common_db``
contract). Each test creates a fresh ``mct_*`` database with an ``extensions`` schema
(Supabase-like), installs the common component 0001..0005, then provisions the runtime
schema by executing ``packages/mission-control-db-contract/runtime/descriptor.json``
faithfully with the test-local executor below. The tests fail, never skip, when the
variable is absent.

Run: ``uv run pytest -m common_db <this file>`` with the variable set.
"""

from __future__ import annotations

import asyncio
import sys
from collections.abc import Awaitable, Callable, Coroutine
from pathlib import Path
from typing import Any

import asyncpg
import pytest
from langgraph.checkpoint.base import empty_checkpoint
from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
from langgraph.store.postgres.aio import AsyncPostgresStore
from pydantic import SecretStr
from tests.fixtures.isolated_settings import isolated_settings
from tests.fixtures.mission_control_common_db import (
    RUNTIME_DESCRIPTOR as DESCRIPTOR,
)
from tests.fixtures.mission_control_common_db import (
    CommonDatabase,
    StepHold,
    apply_descriptor,
    create_common_database,
    drop_common_database,
)
from tests.fixtures.mission_control_common_db import (
    checkpointer_login as _checkpointer_login,
)

from mission_control.adapters.deep_agents.persistence import (
    StandalonePersistenceLifespan,
    runtime_checkpoint_conninfo,
)
from mission_control.adapters.deep_agents.runtime_persistence_verifier import (
    RuntimePersistenceUnavailable,
    verify_runtime_persistence,
)
from mission_control.application.installations.registry import InstallationUnavailable
from mission_control.bootstrap.worker import inspect_checkpoint_binding

pytestmark = pytest.mark.common_db

ROOT = Path(__file__).resolve().parents[3]
SCHEMA = DESCRIPTOR["schema"]
ROLE = DESCRIPTOR["role"]


def run[T](coroutine: Coroutine[Any, Any, T]) -> T:
    """Psycopg's async driver needs a selector loop on Windows; keep it test-local."""
    factory = asyncio.SelectorEventLoop if sys.platform == "win32" else None
    return asyncio.run(coroutine, loop_factory=factory)


async def _with_provisioned(
    scenario: Callable[[CommonDatabase, asyncpg.Connection, str], Awaitable[None]],
    *,
    legacy_poison: bool = True,
) -> None:
    database = await create_common_database(legacy_poison=legacy_poison)
    try:
        owner = await asyncpg.connect(database.owner_dsn)
        try:
            assert await apply_descriptor(owner) == [s["id"] for s in DESCRIPTOR["steps"]]
            checkpointer = await _checkpointer_login(database, owner)
            await scenario(database, owner, checkpointer)
        finally:
            await owner.close()
    finally:
        await drop_common_database(database)


async def _reasons(dsn: str, database: str) -> tuple[str, ...]:
    with pytest.raises(RuntimePersistenceUnavailable) as raised:
        await verify_runtime_persistence(dsn, expected_database=database)
    return raised.value.reasons


async def _denied(dsn: str, statement: str) -> None:
    connection = await asyncpg.connect(dsn)
    try:
        with pytest.raises(asyncpg.InsufficientPrivilegeError):
            await connection.execute(statement)
    finally:
        await connection.close()


# ---------------------------------------------------------------- proofs


def test_descriptor_provisions_runtime_and_restricted_saver_store_round_trip() -> None:
    async def scenario(database: CommonDatabase, owner: asyncpg.Connection, login: str) -> None:
        conninfo = runtime_checkpoint_conninfo(login)
        evidence = await verify_runtime_persistence(conninfo, expected_database=database.name)
        assert evidence.saver_versions == tuple(range(10))
        assert evidence.store_versions == tuple(range(4))
        assert evidence.schema == SCHEMA and evidence.role.endswith("_ckpt")

        async with StandalonePersistenceLifespan(conninfo) as persistence:
            checkpoint = empty_checkpoint()
            checkpoint["channel_values"] = {"notes": ["durable", 2]}
            checkpoint["channel_versions"] = {"notes": "1"}
            config: Any = {"configurable": {"thread_id": "thread-1", "checkpoint_ns": ""}}
            saved = await persistence.saver.aput(
                config, checkpoint, {"source": "input", "step": -1}, {"notes": "1"}
            )
            await persistence.saver.aput_writes(saved, [("notes", "pending")], task_id="task-1")
            loaded = await persistence.saver.aget_tuple(saved)
            assert loaded is not None and loaded.checkpoint["id"] == checkpoint["id"]
            assert loaded.checkpoint["channel_values"] == {"notes": ["durable", 2]}
            assert [write[1:] for write in loaded.pending_writes or []] == [("notes", "pending")]
            namespace = ("mc/scope", "procedural")
            await persistence.store.aput(namespace, "item-1", {"fact": 1})
            item = await persistence.store.aget(namespace, "item-1")
            assert item is not None and item.value == {"fact": 1}

        counts = await owner.fetchrow(
            f"SELECT (SELECT count(*) FROM {SCHEMA}.checkpoints) AS checkpoints, "
            f"(SELECT count(*) FROM {SCHEMA}.checkpoint_blobs) AS blobs, "
            f"(SELECT count(*) FROM {SCHEMA}.checkpoint_writes) AS writes, "
            f"(SELECT count(*) FROM {SCHEMA}.store) AS store, "
            "to_regclass('public.checkpoints') IS NULL AS no_public"
        )
        assert dict(counts) == {
            "checkpoints": 1,
            "blobs": 1,
            "writes": 1,
            "store": 1,
            "no_public": True,
        }

        # The checkpointer reaches nothing outside its schema and cannot rewrite history.
        for statement in (
            "SELECT 1 FROM mission_control.tenant LIMIT 1",
            "INSERT INTO mission_control.component_release (component_version) VALUES ('x')",
            "SELECT 1 FROM mission_control_search.capability_projection LIMIT 1",
            "SELECT 1 FROM belllabs_langgraph.poison_sentinel",
            "SELECT 1 FROM belllabs_control.poison_sentinel",
            "CREATE TABLE public.ckpt_probe (x int)",
            f"CREATE TABLE {SCHEMA}.ckpt_probe (x int)",
            f"INSERT INTO {SCHEMA}.checkpoint_migrations (v) VALUES (99)",
            f"DELETE FROM {SCHEMA}.store_migrations",
            f"TRUNCATE {SCHEMA}.checkpoints",
        ):
            await _denied(login, statement)

        # The business runtime role cannot read or overwrite native checkpoints.
        business = database.dsn("mission_control_runtime")
        for statement in (
            f"SELECT 1 FROM {SCHEMA}.checkpoints",
            f"UPDATE {SCHEMA}.checkpoints SET metadata = '{{}}'",
            f"INSERT INTO {SCHEMA}.store (prefix, key, value) VALUES ('a', 'b', '{{}}')",
        ):
            await _denied(business, statement)
        assert any(
            "member of mission_control_checkpointer only" in reason
            for reason in await _reasons(runtime_checkpoint_conninfo(business), database.name)
        )

        # Superuser/owner credentials never qualify as the checkpoint identity.
        owner_reasons = await _reasons(
            runtime_checkpoint_conninfo(database.owner_dsn), database.name
        )
        assert any("NOSUPERUSER" in reason for reason in owner_reasons)

        # Extra business membership, a wrong database and a missing ledger row fail closed.
        login_role = database.login_roles[ROLE]
        await owner.execute(f'GRANT mission_control_runtime TO "{login_role}"')
        assert any(
            "member of mission_control_checkpointer only" in reason
            for reason in await _reasons(conninfo, database.name)
        )
        await owner.execute(f'REVOKE mission_control_runtime FROM "{login_role}"')
        assert any(
            "selected installation database" in reason
            for reason in await _reasons(conninfo, "some_other_database")
        )
        await owner.execute(f"DELETE FROM {SCHEMA}.checkpoint_migrations WHERE v = 9")
        assert any(
            "ledger checkpoint_migrations" in reason
            for reason in await _reasons(conninfo, database.name)
        )
        await owner.execute(f"INSERT INTO {SCHEMA}.checkpoint_migrations (v) VALUES (9)")
        await verify_runtime_persistence(conninfo, expected_database=database.name)

        # A conninfo without the private search_path is refused before connecting.
        assert any("search_path" in reason for reason in await _reasons(login, database.name))

    run(_with_provisioned(scenario))


def test_interrupted_concurrent_index_is_held_never_recorded_and_fails_readiness() -> None:
    async def scenario() -> None:
        database = await create_common_database()
        try:
            owner = await asyncpg.connect(database.owner_dsn)
            try:
                assert (await apply_descriptor(owner, stop_after="saver.v5"))[-1] == "saver.v5"
                # Simulate an interrupted CREATE INDEX CONCURRENTLY: a failed concurrent build
                # leaves the same-named index behind with indisvalid = false.
                await owner.execute(
                    f"INSERT INTO {SCHEMA}.checkpoints (thread_id, checkpoint_id, checkpoint) "
                    "VALUES ('dup', 'a', '{}'), ('dup', 'b', '{}')"
                )
                with pytest.raises(asyncpg.UniqueViolationError):
                    await owner.execute(
                        "CREATE UNIQUE INDEX CONCURRENTLY checkpoints_thread_id_idx "
                        f"ON {SCHEMA}.checkpoints (thread_id)"
                    )
                assert (
                    await owner.fetchval(
                        "SELECT indisvalid FROM pg_index "
                        f"WHERE indexrelid = to_regclass('{SCHEMA}.checkpoints_thread_id_idx')"
                    )
                    is False
                )
                ids = [step["id"] for step in DESCRIPTOR["steps"]]
                remaining = set(ids[ids.index("saver.v6") :])

                async def ledger() -> list[int]:
                    rows = await owner.fetch(f"SELECT v FROM {SCHEMA}.checkpoint_migrations")
                    return sorted(row["v"] for row in rows)

                # Resume holds at the precheck: the invalid index is never blindly retried.
                with pytest.raises(StepHold) as held:
                    await apply_descriptor(owner, only=remaining)
                assert (held.value.step_id, held.value.phase) == ("saver.v6", "precheck")
                assert await ledger() == list(range(6))
                # Even if the statement runs (IF NOT EXISTS silently accepts the invalid
                # index), verification refuses to record the version row.
                with pytest.raises(StepHold) as unverified:
                    await apply_descriptor(owner, only=remaining, skip_precheck=True)
                assert (unverified.value.step_id, unverified.value.phase) == ("saver.v6", "verify")
                assert await ledger() == list(range(6))

                # The vendor setup() masks the hazard (records v6..v9 over the invalid index);
                # readiness still fails closed on it.
                vendor = runtime_checkpoint_conninfo(database.owner_dsn)
                async with AsyncPostgresSaver.from_conn_string(vendor) as saver:
                    await saver.setup()
                async with AsyncPostgresStore.from_conn_string(vendor) as store:
                    await store.setup()
                assert await ledger() == list(range(10))
                await apply_descriptor(owner, only={"post.grants"})
                login = await _checkpointer_login(database, owner)
                reasons = await _reasons(runtime_checkpoint_conninfo(login), database.name)
                assert reasons == (
                    "runtime index checkpoints_thread_id_idx is invalid or not ready "
                    "(interrupted build)",
                )
            finally:
                await owner.close()
        finally:
            await drop_common_database(database)

    run(scenario())


def test_readiness_fails_closed_when_public_checkpoints_could_shadow_runtime() -> None:
    async def scenario(database: CommonDatabase, owner: asyncpg.Connection, login: str) -> None:
        settings = isolated_settings(langgraph_checkpoint_database_direct=SecretStr(login))
        await inspect_checkpoint_binding(settings, database.name)
        await owner.execute("CREATE TABLE public.checkpoints (thread_id text)")
        reasons = await _reasons(runtime_checkpoint_conninfo(login), database.name)
        assert reasons == (
            "same-named relations can shadow the runtime tables via search_path: "
            "public.checkpoints",
        )
        with pytest.raises(InstallationUnavailable, match="public.checkpoints"):
            await inspect_checkpoint_binding(settings, database.name)
        await owner.execute("DROP TABLE public.checkpoints")
        # A DSN that already carries options is rejected, not overridden.
        with pytest.raises(InstallationUnavailable, match="must not carry libpq options"):
            await inspect_checkpoint_binding(
                isolated_settings(
                    langgraph_checkpoint_database_direct=SecretStr(
                        login + "?options=-c%20work_mem%3D4MB"
                    )
                ),
                database.name,
            )
        await inspect_checkpoint_binding(settings, database.name)

    run(_with_provisioned(scenario, legacy_poison=False))
