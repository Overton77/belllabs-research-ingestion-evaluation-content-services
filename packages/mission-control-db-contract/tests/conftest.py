"""Shared fixtures: two REAL disposable PostgreSQL 17 + pgvector clusters.

``MCDB_TEST_ADMIN_DSN_A`` / ``MCDB_TEST_ADMIN_DSN_B`` must name loopback superuser DSNs
of explicitly disposable clusters (``scripts/disposable.py start``). Database tests
FAIL (never skip) when they are missing. Only databases named ``mcdb_t_<hex>`` and the
cluster-global ``mission_control_*`` / ``mcdb_migrator`` roles are created or dropped.

Container A installs as the superuser and carries the corpus/storage fixture.
Container B installs as a NON-superuser CREATEROLE migration principal (Supabase-like)
and carries the legacy capability_search and poisoned belllabs_control fixture.
"""

from __future__ import annotations

import asyncio
import os
import secrets
import shutil
from collections.abc import Iterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit, urlunsplit

import asyncpg
import pytest

from mission_control_db_contract.integrity import Release, load_release
from mission_control_db_contract.release import release_build, write_lock
from mission_control_db_contract.seeds import uuid7

PACKAGE = Path(__file__).resolve().parents[1]
COMPONENT = PACKAGE / "component"
FIXTURES = Path(__file__).resolve().parent / "fixtures"
VERSIONS = {
    "reader_version": "mission-control-runtime/1",
    "writer_version": "mission-control-runtime/1",
}
MIGRATOR = "mcdb_migrator"
MIGRATOR_PASSWORD = "mcdb-migrator-disposable"
MC_ROLES = (
    "mission_control_runtime",
    "mission_control_family_writer",
    "mission_control_catalog_writer",
    "mission_control_outbox_worker",
    "mission_control_readonly",
    "mission_control_checkpointer",
)
LOOPBACK = {"127.0.0.1", "localhost", "::1"}


def run(coro: Any) -> Any:
    return asyncio.run(coro)


def admin_dsn(which: str) -> str:
    name = f"MCDB_TEST_ADMIN_DSN_{which}"
    value = os.environ.get(name)
    if not value:
        pytest.fail(f"Set {name} to a disposable loopback PostgreSQL 17 cluster (see scripts/)")
    if urlsplit(value).hostname not in LOOPBACK:
        pytest.fail(f"{name} must point at a loopback disposable cluster")
    return value


def with_db(dsn: str, database: str, user: str | None = None, password: str | None = None) -> str:
    parts = urlsplit(dsn)
    netloc = parts.netloc
    if user is not None:
        host = netloc.rsplit("@", 1)[1]
        netloc = f"{user}:{password}@{host}"
    return urlunsplit(parts._replace(netloc=netloc, path="/" + database, query=""))


@dataclass
class Database:
    which: str
    name: str
    admin: str  # superuser DSN for this database
    installer: str  # DSN used by mission-db (superuser in A, migrator in B)
    installer_user: str
    env: dict[str, str] = field(default_factory=dict)

    async def connect(self) -> Any:
        return await asyncpg.connect(self.admin)

    async def fetchval(self, query: str, *args: Any) -> Any:
        connection = await self.connect()
        try:
            return await connection.fetchval(query, *args)
        finally:
            await connection.close()

    async def execute(self, query: str, *args: Any) -> None:
        connection = await self.connect()
        try:
            await connection.execute(query, *args)
        finally:
            await connection.close()


async def _drop_mc_roles(dsn: str) -> None:
    connection = await asyncpg.connect(dsn)
    try:
        for role in MC_ROLES:
            await connection.execute(f"DROP ROLE IF EXISTS {role}")
    finally:
        await connection.close()


async def _create_database(which: str, fixture: str | None) -> Database:
    dsn = admin_dsn(which)
    name = "mcdb_t_" + secrets.token_hex(6)
    admin = await asyncpg.connect(dsn)
    try:
        await admin.execute(f"CREATE DATABASE {name}")
        if which == "B":
            exists = await admin.fetchval("SELECT 1 FROM pg_roles WHERE rolname=$1", MIGRATOR)
            if not exists:
                await admin.execute(
                    f"CREATE ROLE {MIGRATOR} LOGIN CREATEROLE NOSUPERUSER NOBYPASSRLS "
                    f"PASSWORD '{MIGRATOR_PASSWORD}'"
                )
            await admin.execute(f"GRANT CREATE ON DATABASE {name} TO {MIGRATOR}")
    finally:
        await admin.close()
    database_dsn = with_db(dsn, name)
    if fixture is None:
        fixture = "protected_a.sql" if which == "A" else "protected_b.sql"
    connection = await asyncpg.connect(database_dsn)
    try:
        await connection.execute((FIXTURES / fixture).read_text(encoding="utf-8"))
        if which == "B":
            await connection.execute(f"GRANT USAGE ON SCHEMA extensions TO {MIGRATOR}")
    finally:
        await connection.close()
    if which == "B":
        installer = with_db(dsn, name, MIGRATOR, MIGRATOR_PASSWORD)
        user = MIGRATOR
    else:
        installer, user = database_dsn, str(urlsplit(dsn).username)
    return Database(which, name, database_dsn, installer, user)


async def _drop_database(database: Database) -> None:
    assert database.name.startswith("mcdb_t_")
    connection = await asyncpg.connect(admin_dsn(database.which))
    try:
        await connection.execute(f"DROP DATABASE IF EXISTS {database.name} WITH (FORCE)")
    finally:
        await connection.close()


@pytest.fixture
def make_db() -> Iterator[Any]:
    """Factory for fresh disposable databases; drops them and MC roles afterwards."""
    created: list[Database] = []

    def factory(which: str, fixture: str | None = None) -> Database:
        database = run(_create_database(which, fixture))
        created.append(database)
        return database

    for which in ("A", "B"):
        run(_drop_mc_roles(admin_dsn(which)))
    yield factory
    for database in created:
        run(_drop_database(database))
    for which in {d.which for d in created}:
        run(_drop_mc_roles(admin_dsn(which)))


def write_target(
    tmp: Path,
    database: Database,
    *,
    app: str = "biotech",
    installation_id: str | None = None,
    project_ref: str | None = None,
    env_name: str | None = None,
    use_admin: bool = False,
    monkeypatch: pytest.MonkeyPatch,
) -> Path:
    dsn = database.admin if use_admin else database.installer
    parsed = urlsplit(dsn)
    env_name = env_name or f"MCDB_T_{secrets.token_hex(4).upper()}"
    monkeypatch.setenv(env_name, dsn)
    target = tmp / f"target-{secrets.token_hex(4)}.toml"
    target.write_text(
        "\n".join(
            [
                "format_version = 1",
                "[target]",
                f'app = "{app}"',
                f'application_id = "{app}"',
                f'project_label = "disposable-{database.which.lower()}"',
                f'project_ref = "{project_ref or "disposable-" + database.name.replace("_", "-")}"',
                f'installation_id = "{installation_id or uuid7()}"',
                'environment = "disposable"',
                f'database_host = "{parsed.hostname}"',
                f"database_port = {parsed.port}",
                f'database_name = "{database.name}"',
                f'database_user = "{parsed.username}"',
                f'database_url_env = "{env_name}"',
                "[approval]",
                'approved_by = "disposable-test-fixture"',
                'identity_evidence = "tests/conftest.py disposable container"',
                "",
            ]
        ),
        encoding="utf-8",
    )
    return target


def copy_component(
    destination: Path, *, with_descriptor: bool = True, source: Path | None = None
) -> Path:
    """Copy component inputs (whatever migrations exist) into a temp root.

    ``source`` defaults to the live component; pass a built release root to reproduce
    exactly that release's inputs (other lanes keep adding migrations until G2).
    """
    source = source or COMPONENT
    root = destination / "component"
    (root / "migrations").mkdir(parents=True)
    for path in sorted((source / "migrations").glob("*.sql")):
        shutil.copyfile(path, root / "migrations" / path.name)
    shutil.copyfile(source / "release-spec.json", root / "release-spec.json")
    descriptor = source.parent / "runtime" / "descriptor.json"
    if with_descriptor and descriptor.exists():
        (destination / "runtime").mkdir()
        shutil.copyfile(descriptor, destination / "runtime" / "descriptor.json")
    return root


def build_and_lock(
    work: Path, component_root: Path, app: str = "biotech", which: str = "A"
) -> Release:
    os.environ.setdefault(f"MCDB_TEST_ADMIN_DSN_{which}", admin_dsn(which))
    run(release_build(component_root, f"MCDB_TEST_ADMIN_DSN_{which}"))
    deployments = work / "deployments"
    write_lock(component_root, deployments, app)
    return load_release(deployments / app / "release.lock.json", component_root)


@pytest.fixture(scope="session")
def release_work(tmp_path_factory: pytest.TempPathFactory) -> tuple[Path, Path]:
    """Session release built from the CURRENT migrations in a temp component copy."""
    work = tmp_path_factory.mktemp("release")
    component_root = copy_component(work)
    build_and_lock(work, component_root, "biotech")
    write_lock(component_root, work / "deployments", "ai-engineer")
    return work, component_root


@pytest.fixture
def release(release_work: tuple[Path, Path]) -> Release:
    work, component_root = release_work
    return load_release(work / "deployments" / "biotech" / "release.lock.json", component_root)


@pytest.fixture
def release_ai(release_work: tuple[Path, Path]) -> Release:
    work, component_root = release_work
    return load_release(work / "deployments" / "ai-engineer" / "release.lock.json", component_root)
