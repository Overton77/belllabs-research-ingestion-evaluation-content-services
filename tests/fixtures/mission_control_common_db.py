"""Disposable databases with the common mission_control component installed.

An explicitly supplied loopback administrator DSN (``MISSION_CONTROL_TEST_ADMIN_DSN``)
authorizes creating and dropping fresh ``mct_*`` databases and ``mct_*`` login roles
on that server. The server must be PostgreSQL 17 with pgvector available
(``pgvector/pgvector:pg17``). Required common-schema proofs fail, never skip, when the
variable is absent; offline runs exclude them explicitly with ``-m "not common_db"``.

Login roles are members of exactly one capability role, so tests exercise the real
grants and forced row-level security instead of an owner or superuser connection.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import secrets
from collections.abc import AsyncIterator, Iterable
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import quote, urlsplit, urlunsplit
from uuid import UUID, uuid5

import asyncpg
import pytest

ROOT = Path(__file__).resolve().parents[2]
COMPONENT_ROOT = ROOT / "packages" / "mission-control-db-contract" / "component"
MIGRATIONS_ROOT = COMPONENT_ROOT / "migrations"
ADMIN_DSN_ENV = "MISSION_CONTROL_TEST_ADMIN_DSN"
COMPONENT_VERSION = "1.0.0"
WRITER_VERSION = "mission-control-runtime/1"

_NAMESPACE = UUID("6d9c2f6e-6a43-4c7e-9a52-6f3b1b7f2a10")

BIOTECH_INSTALLATION_ID = UUID("0192a4f0-0000-7000-8000-00000000b10e")
AI_ENGINEER_INSTALLATION_ID = UUID("0192a4f0-0000-7000-8000-0000000a1e00")
INSTALLATIONS = {
    "biotech": (BIOTECH_INSTALLATION_ID, "mcdisposablebiotech"),
    "ai-engineer": (AI_ENGINEER_INSTALLATION_ID, "mcdisposableaieng"),
}
CAPABILITY_ROLES = (
    "mission_control_runtime",
    "mission_control_family_writer",
    "mission_control_catalog_writer",
    "mission_control_outbox_worker",
    "mission_control_readonly",
)
LEGACY_SCHEMAS = ("belllabs_control", "capability_search", "belllabs_langgraph")
RUNTIME_DESCRIPTOR = json.loads(
    (ROOT / "packages" / "mission-control-db-contract" / "runtime" / "descriptor.json").read_text(
        encoding="utf-8"
    )
)


def tenant_id(name: str, application_id: str = "biotech") -> UUID:
    """Deterministic test tenant UUID; equal names in two apps intentionally collide."""
    del application_id
    return uuid5(_NAMESPACE, f"tenant:{name}")


def canonical_scope(name: str = "tenant-1", application_id: str = "biotech") -> str:
    installation_id, _ref = INSTALLATIONS[application_id]
    return f"mc/{installation_id}/{application_id}/{tenant_id(name, application_id)}"


def catalog_scope(application_id: str = "biotech") -> str:
    installation_id, _ref = INSTALLATIONS[application_id]
    return f"mc/{installation_id}/{application_id}/catalog"


DEFAULT_TENANTS = ("tenant-1", "tenant-2", "tenant-3", "tenant-a", "tenant-b", "operator")


def migration_paths() -> list[Path]:
    paths = sorted(MIGRATIONS_ROOT.glob("[0-9][0-9][0-9][0-9]_*.sql"))
    if not paths:
        raise RuntimeError("common component migrations are missing")
    return paths


def admin_dsn() -> str:
    value = os.environ.get(ADMIN_DSN_ENV)
    if not value:
        pytest.fail(f"{ADMIN_DSN_ENV} must name a disposable loopback PostgreSQL 17 server")
    parsed = urlsplit(value)
    if parsed.hostname not in {"127.0.0.1", "localhost", "::1"}:
        pytest.fail(f"{ADMIN_DSN_ENV} must be a loopback disposable server")
    return value


def _with_database(
    dsn: str, database: str, *, user: str | None = None, password: str | None = None
) -> str:
    parsed = urlsplit(dsn)
    host = "127.0.0.1" if parsed.hostname == "localhost" else parsed.hostname
    netloc = f"{host}:{parsed.port or 5432}"
    if user is not None:
        credentials = quote(user, safe="")
        if password is not None:
            credentials += ":" + quote(password, safe="")
        netloc = f"{credentials}@{netloc}"
    else:
        netloc = parsed.netloc.rsplit("@", 1)[0] + "@" + netloc if "@" in parsed.netloc else netloc
    return urlunsplit(parsed._replace(netloc=netloc, path="/" + database))


@dataclass
class CommonDatabase:
    name: str
    application_id: str
    installation_id: UUID
    project_ref: str
    owner_dsn: str
    role_dsns: dict[str, str]
    login_roles: dict[str, str]
    tenants: dict[str, UUID] = field(default_factory=dict)

    def dsn(self, capability_role: str = "mission_control_runtime") -> str:
        return self.role_dsns[capability_role]

    def scope(self, tenant: str = "tenant-1") -> str:
        return canonical_scope(tenant, self.application_id)

    async def pool(
        self, capability_role: str = "mission_control_runtime", **kwargs: object
    ) -> asyncpg.Pool:
        options = {"min_size": 1, "max_size": 6, "command_timeout": 30, **kwargs}
        return await asyncpg.create_pool(dsn=self.dsn(capability_role), **options)

    async def owner_pool(self, **kwargs: object) -> asyncpg.Pool:
        options = {"min_size": 1, "max_size": 3, "command_timeout": 30, **kwargs}
        return await asyncpg.create_pool(dsn=self.owner_dsn, **options)

    async def add_tenants(self, names: Iterable[str]) -> None:
        connection = await asyncpg.connect(self.owner_dsn)
        try:
            await _insert_tenants(connection, self, names)
        finally:
            await connection.close()


async def _insert_tenants(
    connection: asyncpg.Connection, database: CommonDatabase, names: Iterable[str]
) -> None:
    for name in names:
        identity = tenant_id(name, database.application_id)
        async with connection.transaction():
            await connection.execute(
                "SELECT set_config('mc.installation_id',$1,true), "
                "set_config('mc.application_id',$2,true), set_config('mc.tenant_id',$3,true)",
                str(database.installation_id),
                database.application_id,
                str(identity),
            )
            await connection.execute(
                """
                INSERT INTO mission_control.tenant
                    (installation_id, application_id, tenant_id, external_tenant_ref, state,
                     qualification_fixture, created_at, created_by_actor_ref)
                VALUES ($1, $2, $3, $4, 'active', true, $5, 'fixture:mission-control-tests')
                ON CONFLICT (tenant_id) DO NOTHING
                """,
                database.installation_id,
                database.application_id,
                identity,
                f"test:{name}",
                datetime.now(UTC),
            )
        database.tenants[name] = identity


async def install_component(
    connection: asyncpg.Connection, paths: list[Path] | None = None
) -> list[str]:
    """Apply ordered component SQL with receipts, as the installer's core transaction does."""
    applied: list[str] = []
    for path in paths or migration_paths():
        payload = path.read_bytes()
        key = path.stem
        async with connection.transaction():
            await connection.execute(payload.decode("utf-8"))
            await connection.execute(
                "INSERT INTO mission_control.component_release "
                "(component_version, migration_key, migration_digest, applied_at, applied_by) "
                "VALUES ($1, $2, $3, clock_timestamp(), current_user)",
                COMPONENT_VERSION,
                key,
                "sha256:" + hashlib.sha256(payload).hexdigest(),
            )
        applied.append(key)
    return applied


async def poison_legacy_schemas(connection: asyncpg.Connection) -> None:
    """Create legacy namespaces holding sentinel rows that no restricted role may touch."""
    for schema in LEGACY_SCHEMAS:
        await connection.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
        await connection.execute(
            f'CREATE TABLE IF NOT EXISTS "{schema}".poison_sentinel '
            "(sentinel text PRIMARY KEY, note text NOT NULL)"
        )
        await connection.execute(
            f'INSERT INTO "{schema}".poison_sentinel VALUES '
            "('do-not-read','legacy access is a test failure') ON CONFLICT DO NOTHING"
        )
        await connection.execute(f'REVOKE ALL ON SCHEMA "{schema}" FROM PUBLIC')
        await connection.execute(f'REVOKE ALL ON ALL TABLES IN SCHEMA "{schema}" FROM PUBLIC')
    # Tables commonly queried by the transitional adapters, so a fallback would hit
    # an existing-but-forbidden relation rather than a missing one.
    for table in ("workflow_runs", "immutable_documents", "outbox", "schema_migrations"):
        await connection.execute(
            f"CREATE TABLE IF NOT EXISTS belllabs_control.{table} (poison text)"
        )
        await connection.execute(f"REVOKE ALL ON belllabs_control.{table} FROM PUBLIC")


async def legacy_poison_intact(owner_dsn: str) -> bool:
    connection = await asyncpg.connect(owner_dsn)
    try:
        for schema in LEGACY_SCHEMAS:
            rows = await connection.fetch(f'SELECT sentinel FROM "{schema}".poison_sentinel')
            if [row["sentinel"] for row in rows] != ["do-not-read"]:
                return False
        return True
    finally:
        await connection.close()


async def create_common_database(
    *,
    application_id: str = "biotech",
    tenants: Iterable[str] = DEFAULT_TENANTS,
    legacy_poison: bool = False,
    paths: list[Path] | None = None,
    server_dsn: str | None = None,
) -> CommonDatabase:
    server = server_dsn or admin_dsn()
    suffix = secrets.token_hex(5)
    name = f"mct_{suffix}"
    installation_id, project_ref = INSTALLATIONS[application_id]
    admin = await asyncpg.connect(server)
    try:
        await admin.execute(f'CREATE DATABASE "{name}"')
        login_roles: dict[str, str] = {}
        passwords: dict[str, str] = {}
        for capability in CAPABILITY_ROLES:
            short = capability.removeprefix("mission_control_")
            login = f"mct_{suffix}_{short}"
            password = secrets.token_hex(16)
            login_roles[capability] = login
            passwords[capability] = password
    finally:
        await admin.close()
    owner_dsn = _with_database(server, name)
    connection = await asyncpg.connect(owner_dsn)
    try:
        await connection.execute("CREATE SCHEMA IF NOT EXISTS extensions")
        await connection.execute("CREATE EXTENSION IF NOT EXISTS vector SCHEMA extensions")
        await connection.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto SCHEMA extensions")
        # Release-spec required extension since migration 0026 (trigram name search).
        await connection.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm SCHEMA extensions")
        await connection.execute("GRANT USAGE ON SCHEMA extensions TO PUBLIC")
        if legacy_poison:
            await poison_legacy_schemas(connection)
        await install_component(connection, paths)
        await connection.execute(
            """
            INSERT INTO mission_control.application_installation
                (installation_id, application_id, supabase_project_ref, environment,
                 schema_component_version, state, created_at)
            VALUES ($1, $2, $3, 'disposable', $4, 'active', clock_timestamp())
            """,
            installation_id,
            application_id,
            project_ref,
            COMPONENT_VERSION,
        )
        await connection.execute(
            """
            INSERT INTO mission_control.release_attestation
                (component_version, manifest_digest, contract_schema_digest, schema_fingerprint,
                 fingerprint_algorithm, supported_reader_versions, supported_writer_versions,
                 attested_at, attested_by)
            VALUES ($1, $2, $2, $2, 'fixture-unqualified', $3, $3, clock_timestamp(), current_user)
            """,
            COMPONENT_VERSION,
            "sha256:" + "0" * 64,
            [WRITER_VERSION],
        )
        for capability, login in login_roles.items():
            if not re.fullmatch(r"mct_[0-9a-f]{10}_[a-z_]+", login):
                raise ValueError("unexpected generated role name")
            await connection.execute(
                f'CREATE ROLE "{login}" LOGIN NOSUPERUSER NOBYPASSRLS NOCREATEDB NOCREATEROLE '
                f"INHERIT PASSWORD '{passwords[capability]}' IN ROLE {capability}"
            )
            await connection.execute(f'GRANT CONNECT ON DATABASE "{name}" TO "{login}"')
    finally:
        await connection.close()
    database = CommonDatabase(
        name=name,
        application_id=application_id,
        installation_id=installation_id,
        project_ref=project_ref,
        owner_dsn=owner_dsn,
        role_dsns={
            capability: _with_database(server, name, user=login, password=passwords[capability])
            for capability, login in login_roles.items()
        },
        login_roles=login_roles,
    )
    await database.add_tenants(tenants)
    return database


async def drop_common_database(database: CommonDatabase, *, server_dsn: str | None = None) -> None:
    server = server_dsn or admin_dsn()
    admin = await asyncpg.connect(server)
    try:
        await admin.execute(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = $1",
            database.name,
        )
        await admin.execute(f'DROP DATABASE IF EXISTS "{database.name}"')
        for login in database.login_roles.values():
            await admin.execute(f'DROP ROLE IF EXISTS "{login}"')
    finally:
        await admin.close()


@asynccontextmanager
async def common_database(**kwargs: object) -> AsyncIterator[CommonDatabase]:
    database = await create_common_database(**kwargs)  # type: ignore[arg-type]
    try:
        yield database
    finally:
        await drop_common_database(database)


# ---------------------------------------------------------------- runtime schema
# Faithful test executor of runtime/descriptor.json (same semantics as `mission-db
# runtime-apply`, without the release-attestation binding the fast fixture lacks).


class StepHold(RuntimeError):
    def __init__(self, step_id: str, phase: str, query: str | None = None) -> None:
        self.step_id, self.phase, self.query = step_id, phase, query
        super().__init__(f"{step_id}: {phase} failed")


async def _all_true(connection: asyncpg.Connection, step: dict, phase: str) -> None:
    for query in step[phase]:
        if await connection.fetchval(query) is not True:
            raise StepHold(step["id"], phase, query)


async def apply_descriptor(
    connection: asyncpg.Connection,
    *,
    only: set[str] | None = None,
    stop_after: str | None = None,
    skip_precheck: bool = False,
) -> list[str]:
    """Execute descriptor steps exactly as runtime/README.md specifies; returns step ids."""
    session = RUNTIME_DESCRIPTOR["session"]
    applied: list[str] = []
    await connection.execute(f"SET lock_timeout = '{session['lock_timeout']}'")
    await connection.execute(f"SET search_path TO {session['search_path']}")
    await connection.fetchval(session["advisory_lock_sql"])
    try:
        for step in RUNTIME_DESCRIPTOR["steps"]:
            if only is not None and step["id"] not in only:
                continue
            if hashlib.sha256(step["sql"].encode("utf-8")).hexdigest() != step["sha256"]:
                raise StepHold(step["id"], "sha256")
            if not skip_precheck:
                await _all_true(connection, step, "precheck")
            recorded = step["records_version"]
            if step["transactional"]:
                async with connection.transaction():
                    await connection.execute(step["sql"])
                    await _all_true(connection, step, "verify")
                    if recorded is not None:
                        await connection.execute(recorded["sql"])
                        await _all_true(connection, step, "verify_recorded")
            else:
                await connection.execute(step["sql"])  # autocommit: CONCURRENTLY
                await _all_true(connection, step, "verify")  # never record an invalid index
                async with connection.transaction():
                    await connection.execute(recorded["sql"])
                    await _all_true(connection, step, "verify_recorded")
            applied.append(step["id"])
            if step["id"] == stop_after:
                return applied
        if only is None:
            for query in RUNTIME_DESCRIPTOR["final_verify"]:
                if await connection.fetchval(query) is not True:
                    raise StepHold("final", "final_verify", query)
        return applied
    finally:
        await connection.fetchval(session["advisory_unlock_sql"])
        await connection.execute("RESET search_path")


def _login_dsn(database: CommonDatabase, login: str, password: str) -> str:
    parsed = urlsplit(database.owner_dsn)
    netloc = f"{quote(login, safe='')}:{quote(password, safe='')}@{parsed.hostname}:{parsed.port}"
    return urlunsplit(parsed._replace(netloc=netloc, path="/" + database.name, query=""))


async def checkpointer_login(database: CommonDatabase, owner: asyncpg.Connection) -> str:
    login, password = f"{database.name}_ckpt", secrets.token_hex(16)
    await owner.execute(
        f'CREATE ROLE "{login}" LOGIN NOSUPERUSER NOBYPASSRLS NOCREATEDB NOCREATEROLE '
        f"INHERIT PASSWORD '{password}' IN ROLE {RUNTIME_DESCRIPTOR['role']}"
    )
    database.login_roles[RUNTIME_DESCRIPTOR["role"]] = login  # dropped with the database
    await owner.execute(f'GRANT CONNECT ON DATABASE "{database.name}" TO "{login}"')
    return _login_dsn(database, login, password)


async def provision_runtime(database: CommonDatabase) -> str:
    """Provision ``mission_control_runtime`` and return a checkpointer-only login DSN."""
    owner = await asyncpg.connect(database.owner_dsn)
    try:
        await apply_descriptor(owner)
        return await checkpointer_login(database, owner)
    finally:
        await owner.close()
