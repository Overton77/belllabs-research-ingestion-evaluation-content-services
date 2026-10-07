"""Read-only readiness proof for the pinned LangGraph saver/store in ``mission_control_runtime``.

The check connects with the exact conninfo the saver/store will use (one libpq
``options`` value selecting the private schema), as the configured checkpoint login, and
fails closed unless every condition holds:

- the login is NOSUPERUSER/NOBYPASSRLS (and holds no other elevated attribute) and is a
  member of ``mission_control_checkpointer`` only - not of the business, family, catalog,
  outbox or schema-owner roles - and owns nothing in the runtime schema;
- it has USAGE but not CREATE on the runtime schema, CREATE on no schema at all (so not
  on ``public``), and no privilege on any business/legacy schema or relation;
- exactly the pinned tables exist with the pinned columns, every pinned index exists and
  is ``indisvalid AND indisready`` (an interrupted ``CREATE INDEX CONCURRENTLY`` is not),
  and the vendor version ledgers equal the pinned ranges;
- the session resolves every vendor table name to the runtime schema, and no same-named
  relation exists in ``public``, the session temp schema or any schema the login can use.

There is no fallback schema: a missing or invalid runtime persistence fails readiness.
Native checkpoint state is recovery data, never business acceptance.
"""

from __future__ import annotations

import asyncio
from collections.abc import Sequence
from dataclasses import dataclass
from importlib import metadata
from typing import Any

import psycopg
from langgraph.checkpoint.postgres.base import BasePostgresSaver
from langgraph.store.postgres.base import BasePostgresStore
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict

from mission_control.adapters.deep_agents.persistence import (
    CHECKPOINTER_ROLE,
    RUNTIME_SCHEMA,
    conninfo_search_path_schema,
)

PINNED_CHECKPOINT_POSTGRES = "3.1.1"
PINNED_SAVER_VERSIONS = tuple(range(10))
PINNED_STORE_VERSIONS = tuple(range(4))

_TS = "timestamp with time zone"
EXPECTED_COLUMNS: dict[str, tuple[tuple[str, str, bool], ...]] = {
    "checkpoint_migrations": (("v", "integer", True),),
    "checkpoints": (
        ("thread_id", "text", True),
        ("checkpoint_ns", "text", True),
        ("checkpoint_id", "text", True),
        ("parent_checkpoint_id", "text", False),
        ("type", "text", False),
        ("checkpoint", "jsonb", True),
        ("metadata", "jsonb", True),
    ),
    "checkpoint_blobs": (
        ("thread_id", "text", True),
        ("checkpoint_ns", "text", True),
        ("channel", "text", True),
        ("version", "text", True),
        ("type", "text", True),
        ("blob", "bytea", False),
    ),
    "checkpoint_writes": (
        ("thread_id", "text", True),
        ("checkpoint_ns", "text", True),
        ("checkpoint_id", "text", True),
        ("task_id", "text", True),
        ("idx", "integer", True),
        ("channel", "text", True),
        ("type", "text", False),
        ("blob", "bytea", True),
        ("task_path", "text", True),
    ),
    "store_migrations": (("v", "integer", True),),
    "store": (
        ("prefix", "text", True),
        ("key", "text", True),
        ("value", "jsonb", True),
        ("created_at", _TS, False),
        ("updated_at", _TS, False),
        ("expires_at", _TS, False),
        ("ttl_minutes", "integer", False),
    ),
}
EXPECTED_INDEXES: dict[str, str] = {
    "checkpoint_migrations_pkey": "checkpoint_migrations",
    "checkpoints_pkey": "checkpoints",
    "checkpoint_blobs_pkey": "checkpoint_blobs",
    "checkpoint_writes_pkey": "checkpoint_writes",
    "checkpoints_thread_id_idx": "checkpoints",
    "checkpoint_blobs_thread_id_idx": "checkpoint_blobs",
    "checkpoint_writes_thread_id_idx": "checkpoint_writes",
    "store_migrations_pkey": "store_migrations",
    "store_pkey": "store",
    "store_prefix_idx": "store",
    "idx_store_expires_at": "store",
}
READ_WRITE_TABLES = ("checkpoints", "checkpoint_blobs", "checkpoint_writes", "store")
LEDGER_TABLES = ("checkpoint_migrations", "store_migrations")
# Business authority, search projection, optional adapters and retired legacy namespaces.
FORBIDDEN_SCHEMAS = (
    "mission_control",
    "mission_control_search",
    "mission_control_agent_server",
    "biotech_mission_adapters",
    "belllabs_control",
    "capability_search",
    "belllabs_langgraph",
)
_ANY_TABLE_PRIVILEGE = "SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER"


class RuntimePersistenceUnavailable(RuntimeError):
    """The runtime saver/store binding is not ready; carries redacted reasons only."""

    def __init__(self, reasons: Sequence[str]) -> None:
        self.reasons = tuple(reasons)
        super().__init__("; ".join(self.reasons))


@dataclass(frozen=True)
class RuntimePersistenceEvidence:
    database: str
    role: str
    schema: str
    saver_versions: tuple[int, ...]
    store_versions: tuple[int, ...]


def verify_runtime_persistence_sync(
    conninfo: str,
    *,
    expected_database: str | None = None,
    schema: str = RUNTIME_SCHEMA,
    checkpointer_role: str = CHECKPOINTER_ROLE,
    connect_timeout: int = 10,
) -> RuntimePersistenceEvidence:
    reasons = _pin_reasons()
    if conninfo_search_path_schema(conninfo) != schema:
        reasons.append("checkpoint conninfo does not select the runtime schema search_path")
        raise RuntimePersistenceUnavailable(reasons)
    configured = "connect_timeout" in conninfo_to_dict(conninfo)
    extra: dict[str, Any] = {} if configured else {"connect_timeout": connect_timeout}
    try:
        connection = psycopg.connect(conninfo, autocommit=False, **extra)
    except psycopg.Error:
        raise RuntimePersistenceUnavailable(
            [*reasons, "checkpoint connection is unavailable"]
        ) from None
    try:
        connection.read_only = True
        with connection.transaction(), connection.cursor() as cursor:
            cursor.execute("SET LOCAL statement_timeout = '10s'")
            evidence = _inspect(
                cursor,
                reasons,
                expected_database=expected_database,
                schema=schema,
                checkpointer_role=checkpointer_role,
            )
    except psycopg.Error as error:
        reasons.append(f"checkpoint readiness query failed ({type(error).__name__})")
        raise RuntimePersistenceUnavailable(reasons) from None
    finally:
        connection.close()
    if reasons:
        raise RuntimePersistenceUnavailable(reasons)
    return evidence


async def verify_runtime_persistence(
    conninfo: str,
    *,
    expected_database: str | None = None,
    schema: str = RUNTIME_SCHEMA,
    checkpointer_role: str = CHECKPOINTER_ROLE,
) -> RuntimePersistenceEvidence:
    """Async wrapper; runs the synchronous Psycopg probe off the event loop."""

    return await asyncio.to_thread(
        verify_runtime_persistence_sync,
        conninfo,
        expected_database=expected_database,
        schema=schema,
        checkpointer_role=checkpointer_role,
    )


def _pin_reasons() -> list[str]:
    reasons: list[str] = []
    try:
        installed = metadata.version("langgraph-checkpoint-postgres")
    except metadata.PackageNotFoundError:
        installed = None
    if installed != PINNED_CHECKPOINT_POSTGRES:
        reasons.append("installed langgraph-checkpoint-postgres differs from the pinned release")
    if len(BasePostgresSaver.MIGRATIONS) != len(PINNED_SAVER_VERSIONS) or len(
        BasePostgresStore.MIGRATIONS
    ) != len(PINNED_STORE_VERSIONS):
        reasons.append("installed saver/store migrations differ from the pinned ranges")
    return reasons


def _inspect(
    cursor: psycopg.Cursor[tuple[Any, ...]],
    reasons: list[str],
    *,
    expected_database: str | None,
    schema: str,
    checkpointer_role: str,
) -> RuntimePersistenceEvidence:
    cursor.execute(
        """SELECT current_database(), current_user, r.rolsuper, r.rolbypassrls,
                  r.rolcreaterole, r.rolcreatedb, r.rolreplication
             FROM pg_catalog.pg_roles r WHERE r.rolname = current_user"""
    )
    identity = cursor.fetchone()
    if identity is None:
        reasons.append("checkpoint role is not visible in pg_roles")
        raise RuntimePersistenceUnavailable(reasons)
    database, role = str(identity[0]), str(identity[1])
    if expected_database is not None and database != expected_database:
        reasons.append("checkpoint persistence is not in the selected installation database")
    if identity[2] or identity[3]:
        reasons.append("checkpoint role must be NOSUPERUSER and NOBYPASSRLS")
    if identity[4] or identity[5] or identity[6]:
        reasons.append("checkpoint role must not hold CREATEROLE, CREATEDB or REPLICATION")

    cursor.execute(
        """SELECT r.rolname FROM pg_catalog.pg_roles r
            WHERE r.rolname <> current_user
              AND pg_catalog.pg_has_role(current_user, r.oid, 'MEMBER')
            ORDER BY r.rolname"""
    )
    memberships = [str(row[0]) for row in cursor.fetchall()]
    if memberships != [checkpointer_role]:
        reasons.append(f"checkpoint role must be a member of {checkpointer_role} only")

    cursor.execute(
        """SELECT pg_catalog.pg_has_role(current_user, n.nspowner, 'MEMBER'),
                  pg_catalog.has_schema_privilege(current_user, n.oid, 'USAGE'),
                  pg_catalog.has_schema_privilege(current_user, n.oid, 'CREATE')
             FROM pg_catalog.pg_namespace n WHERE n.nspname = %s""",
        (schema,),
    )
    namespace = cursor.fetchone()
    if namespace is None:
        reasons.append("runtime schema is not provisioned")
        raise RuntimePersistenceUnavailable(reasons)
    if namespace[0]:
        reasons.append("checkpoint role must not own the runtime schema")
    if not namespace[1]:
        reasons.append("checkpoint role lacks USAGE on the runtime schema")

    cursor.execute(
        """SELECT n.nspname FROM pg_catalog.pg_namespace n
            WHERE pg_catalog.has_schema_privilege(current_user, n.oid, 'CREATE')
            ORDER BY n.nspname"""
    )
    creatable = [str(row[0]) for row in cursor.fetchall()]
    if creatable:
        reasons.append("checkpoint role must not hold CREATE on any schema (including public)")

    cursor.execute(
        """SELECT n.nspname FROM pg_catalog.pg_namespace n
            WHERE n.nspname = ANY(%s)
              AND (pg_catalog.has_schema_privilege(current_user, n.oid, 'USAGE')
                   OR pg_catalog.has_schema_privilege(current_user, n.oid, 'CREATE'))""",
        (list(FORBIDDEN_SCHEMAS),),
    )
    if cursor.fetchall():
        reasons.append("checkpoint role must have no access to business or legacy schemas")
    cursor.execute(
        """SELECT pg_catalog.count(*) FROM pg_catalog.pg_class c
             JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = ANY(%s) AND c.relkind IN ('r', 'p', 'v', 'm', 'f', 'S')
              AND pg_catalog.has_table_privilege(current_user, c.oid, %s)""",
        (list(FORBIDDEN_SCHEMAS), _ANY_TABLE_PRIVILEGE),
    )
    granted = cursor.fetchone()
    if granted is None or int(granted[0]) != 0:
        reasons.append("checkpoint role must hold no privilege on business or legacy tables")

    _inspect_shape(cursor, reasons, schema)
    saver_versions, store_versions = _inspect_ledgers(cursor, reasons, schema)
    _inspect_runtime_privileges(cursor, reasons, schema)
    _inspect_search_path(cursor, reasons, schema)
    return RuntimePersistenceEvidence(database, role, schema, saver_versions, store_versions)


def _inspect_shape(
    cursor: psycopg.Cursor[tuple[Any, ...]], reasons: list[str], schema: str
) -> None:
    cursor.execute(
        """SELECT c.relname, c.relkind, pg_catalog.pg_has_role(current_user, c.relowner, 'MEMBER')
             FROM pg_catalog.pg_class c
             JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = %s AND c.relkind IN ('r', 'p', 'v', 'm', 'f', 'S')""",
        (schema,),
    )
    relations = {str(row[0]): (str(row[1]), bool(row[2])) for row in cursor.fetchall()}
    if set(relations) != set(EXPECTED_COLUMNS) or any(
        kind != "r" for kind, _owner in relations.values()
    ):
        reasons.append("runtime schema tables differ from the pinned saver/store")
    if any(owner for _kind, owner in relations.values()):
        reasons.append("checkpoint role must not own runtime tables")

    cursor.execute(
        """SELECT c.relname, a.attname, pg_catalog.format_type(a.atttypid, a.atttypmod),
                  a.attnotnull
             FROM pg_catalog.pg_attribute a
             JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
             JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = %s AND c.relkind = 'r' AND a.attnum > 0 AND NOT a.attisdropped
            ORDER BY c.relname, a.attnum""",
        (schema,),
    )
    columns: dict[str, list[tuple[str, str, bool]]] = {}
    for table, name, kind, not_null in cursor.fetchall():
        columns.setdefault(str(table), []).append((str(name), str(kind), bool(not_null)))
    for table, expected in EXPECTED_COLUMNS.items():
        if tuple(columns.get(table, ())) != expected:
            reasons.append(f"runtime table {table} columns differ from the pinned saver/store")

    cursor.execute(
        """SELECT c.relname, t.relname, i.indisvalid, i.indisready
             FROM pg_catalog.pg_index i
             JOIN pg_catalog.pg_class c ON c.oid = i.indexrelid
             JOIN pg_catalog.pg_class t ON t.oid = i.indrelid
             JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = %s""",
        (schema,),
    )
    indexes = {str(row[0]): (str(row[1]), bool(row[2]), bool(row[3])) for row in cursor.fetchall()}
    for index, (table, valid, ready) in sorted(indexes.items()):
        if not (valid and ready):
            reasons.append(f"runtime index {index} is invalid or not ready (interrupted build)")
        elif EXPECTED_INDEXES.get(index) != table:
            reasons.append(f"runtime index {index} is not part of the pinned saver/store")
    for index in sorted(set(EXPECTED_INDEXES) - set(indexes)):
        reasons.append(f"runtime index {index} is missing")


def _inspect_ledgers(
    cursor: psycopg.Cursor[tuple[Any, ...]], reasons: list[str], schema: str
) -> tuple[tuple[int, ...], tuple[int, ...]]:
    observed: list[tuple[int, ...]] = []
    for table, expected in (
        ("checkpoint_migrations", PINNED_SAVER_VERSIONS),
        ("store_migrations", PINNED_STORE_VERSIONS),
    ):
        cursor.execute(
            "SELECT pg_catalog.has_table_privilege(current_user, pg_catalog.to_regclass(%s), "
            "'SELECT')",
            (f"{schema}.{table}",),
        )
        readable = cursor.fetchone()
        if readable is None or not readable[0]:
            reasons.append(f"runtime ledger {table} is missing or unreadable")
            observed.append(())
            continue
        cursor.execute(
            sql.SQL("SELECT v FROM {}.{} ORDER BY v").format(
                sql.Identifier(schema), sql.Identifier(table)
            )
        )
        versions = tuple(int(row[0]) for row in cursor.fetchall())
        if versions != expected:
            reasons.append(f"runtime ledger {table} differs from the pinned vendor versions")
        observed.append(versions)
    return observed[0], observed[1]


def _inspect_runtime_privileges(
    cursor: psycopg.Cursor[tuple[Any, ...]], reasons: list[str], schema: str
) -> None:
    for table in READ_WRITE_TABLES:
        cursor.execute(
            """SELECT pg_catalog.has_table_privilege(current_user, c, 'SELECT')
                      AND pg_catalog.has_table_privilege(current_user, c, 'INSERT')
                      AND pg_catalog.has_table_privilege(current_user, c, 'UPDATE')
                      AND pg_catalog.has_table_privilege(current_user, c, 'DELETE')
                 FROM pg_catalog.to_regclass(%s) AS c WHERE c IS NOT NULL""",
            (f"{schema}.{table}",),
        )
        row = cursor.fetchone()
        if row is None or not row[0]:
            reasons.append(f"checkpoint role lacks read/write on runtime table {table}")
    for table in LEDGER_TABLES:
        cursor.execute(
            """SELECT pg_catalog.has_table_privilege(current_user, c,
                          'INSERT,UPDATE,DELETE,TRUNCATE')
                 FROM pg_catalog.to_regclass(%s) AS c WHERE c IS NOT NULL""",
            (f"{schema}.{table}",),
        )
        row = cursor.fetchone()
        if row is not None and row[0]:
            reasons.append(f"checkpoint role must not modify vendor ledger {table}")


def _inspect_search_path(
    cursor: psycopg.Cursor[tuple[Any, ...]], reasons: list[str], schema: str
) -> None:
    cursor.execute("SELECT pg_catalog.current_schema(), pg_catalog.current_schemas(false)")
    row = cursor.fetchone()
    current, path = (row[0], list(row[1])) if row is not None else (None, [])
    explicit = [item for item in path if not str(item).startswith("pg_temp")]
    if current != schema or explicit != [schema]:
        reasons.append("checkpoint session search_path does not resolve to the runtime schema only")
    for table in EXPECTED_COLUMNS:
        cursor.execute(
            "SELECT pg_catalog.to_regclass(%s)::oid IS NOT DISTINCT FROM "
            "pg_catalog.to_regclass(%s)::oid AND pg_catalog.to_regclass(%s) IS NOT NULL",
            (table, f"{schema}.{table}", table),
        )
        resolved = cursor.fetchone()
        if resolved is None or not resolved[0]:
            reasons.append(f"unqualified {table} does not resolve to the runtime schema")
    cursor.execute(
        """SELECT n.nspname, c.relname FROM pg_catalog.pg_class c
             JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            WHERE c.relname = ANY(%s) AND n.nspname <> %s
              AND c.relkind IN ('r', 'p', 'v', 'm', 'f')
              AND (n.nspname = 'public'
                   OR n.oid = pg_catalog.pg_my_temp_schema()
                   OR (n.nspname !~ '^pg_(temp|toast)'
                       AND pg_catalog.has_schema_privilege(current_user, n.oid, 'USAGE')))
            ORDER BY 1, 2""",
        (list(EXPECTED_COLUMNS), schema),
    )
    shadows = [f"{row[0]}.{row[1]}" for row in cursor.fetchall()]
    if shadows:
        reasons.append(
            "same-named relations can shadow the runtime tables via search_path: "
            + ", ".join(shadows)
        )
