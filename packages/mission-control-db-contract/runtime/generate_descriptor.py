"""Generate or check ``descriptor.json`` from the installed, pinned LangGraph saver/store.

The vendor SQL is captured by driving the pinned ``AsyncPostgresSaver.setup()`` and
``AsyncPostgresStore.setup()`` against a recording cursor that reports a fresh database,
so the descriptor holds exactly the statements (and order) the vendor would execute,
byte-for-byte, plus the reviewed prerequisite and grant steps around them.

    uv run python packages/mission-control-db-contract/runtime/generate_descriptor.py --check
    uv run python packages/mission-control-db-contract/runtime/generate_descriptor.py --write

``--check`` (the default) exits non-zero when an installed pin, a vendor source hash or
any step's SQL/sha256 differs from the committed descriptor. Never hand-edit vendor steps.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import sys
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from importlib import metadata
from pathlib import Path
from typing import Any

DESCRIPTOR_PATH = Path(__file__).resolve().with_name("descriptor.json")
DESCRIPTOR_SCHEMA = "mission-control.runtime-descriptor/1"
COMPONENT = "mission_control_runtime"
SCHEMA = "mission_control_runtime"
ROLE = "mission_control_checkpointer"
PINS = {
    "langgraph": "1.2.10",
    "langgraph-checkpoint": "4.1.1",
    "langgraph-checkpoint-postgres": "3.1.1",
    "psycopg": "3.3.4",
}
VENDOR_SOURCES = (
    "langgraph/checkpoint/postgres/base.py",
    "langgraph/checkpoint/postgres/aio.py",
    "langgraph/store/postgres/base.py",
    "langgraph/store/postgres/aio.py",
)
SAVER_LEDGER = "checkpoint_migrations"
STORE_LEDGER = "store_migrations"
RUNTIME_TABLES = ("checkpoints", "checkpoint_blobs", "checkpoint_writes", "store")
LEDGER_TABLES = (SAVER_LEDGER, STORE_LEDGER)

# Columns each vendor step must leave present: (name, format_type, not_null).
_TS = "timestamp with time zone"
STEP_COLUMNS: dict[tuple[str, int], tuple[str, tuple[tuple[str, str, bool], ...]]] = {
    ("saver", 0): (SAVER_LEDGER, (("v", "integer", True),)),
    ("saver", 1): (
        "checkpoints",
        (
            ("thread_id", "text", True),
            ("checkpoint_ns", "text", True),
            ("checkpoint_id", "text", True),
            ("parent_checkpoint_id", "text", False),
            ("type", "text", False),
            ("checkpoint", "jsonb", True),
            ("metadata", "jsonb", True),
        ),
    ),
    ("saver", 2): (
        "checkpoint_blobs",
        (
            ("thread_id", "text", True),
            ("checkpoint_ns", "text", True),
            ("channel", "text", True),
            ("version", "text", True),
            ("type", "text", True),
            ("blob", "bytea", False),
        ),
    ),
    ("saver", 3): (
        "checkpoint_writes",
        (
            ("thread_id", "text", True),
            ("checkpoint_ns", "text", True),
            ("checkpoint_id", "text", True),
            ("task_id", "text", True),
            ("idx", "integer", True),
            ("channel", "text", True),
            ("type", "text", False),
            ("blob", "bytea", True),
        ),
    ),
    ("saver", 4): ("checkpoint_blobs", (("blob", "bytea", False),)),
    ("saver", 9): ("checkpoint_writes", (("task_path", "text", True),)),
    ("store", 0): (
        "store",
        (
            ("prefix", "text", True),
            ("key", "text", True),
            ("value", "jsonb", True),
            ("created_at", _TS, False),
            ("updated_at", _TS, False),
        ),
    ),
    ("store", 2): ("store", (("expires_at", _TS, False), ("ttl_minutes", "integer", False))),
}
# Indexes each vendor step must leave valid and ready: (index, table).
STEP_INDEXES: dict[tuple[str, int], tuple[tuple[str, str], ...]] = {
    ("saver", 0): (("checkpoint_migrations_pkey", SAVER_LEDGER),),
    ("saver", 1): (("checkpoints_pkey", "checkpoints"),),
    ("saver", 2): (("checkpoint_blobs_pkey", "checkpoint_blobs"),),
    ("saver", 3): (("checkpoint_writes_pkey", "checkpoint_writes"),),
    ("saver", 6): (("checkpoints_thread_id_idx", "checkpoints"),),
    ("saver", 7): (("checkpoint_blobs_thread_id_idx", "checkpoint_blobs"),),
    ("saver", 8): (("checkpoint_writes_thread_id_idx", "checkpoint_writes"),),
    ("store", 0): (("store_pkey", "store"),),
    ("store", 1): (("store_prefix_idx", "store"),),
    ("store", 3): (("idx_store_expires_at", "store"),),
}
FINAL_COLUMN_COUNTS = {
    SAVER_LEDGER: 1,
    "checkpoints": 7,
    "checkpoint_blobs": 6,
    "checkpoint_writes": 9,
    STORE_LEDGER: 1,
    "store": 7,
}


def sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


# ---------------------------------------------------------------- vendor capture


class _RecordingCursor:
    def __init__(self, log: list[tuple[str, tuple[Any, ...] | None]]) -> None:
        self._log = log

    async def execute(self, sql: str, params: tuple[Any, ...] | None = None) -> _RecordingCursor:
        self._log.append((sql, params))
        return self

    async def fetchone(self) -> None:
        return None  # a fresh database: no ledger row yet


def capture_vendor_setup() -> dict[str, list[tuple[str, tuple[Any, ...] | None]]]:
    """Every statement the pinned async saver/store ``setup()`` runs on a fresh database."""

    from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
    from langgraph.store.postgres.aio import AsyncPostgresStore

    captured: dict[str, list[tuple[str, tuple[Any, ...] | None]]] = {"saver": [], "store": []}

    def recorder(kind: str) -> Any:
        @asynccontextmanager
        async def cursor(*_args: Any, **_kwargs: Any) -> AsyncIterator[_RecordingCursor]:
            yield _RecordingCursor(captured[kind])

        return cursor

    saver = object.__new__(AsyncPostgresSaver)
    saver.pipe = None
    saver._cursor = recorder("saver")  # type: ignore[method-assign]
    store = object.__new__(AsyncPostgresStore)
    store.index_config = None
    store._task = None  # no batch loop was started
    store._cursor = recorder("store")  # type: ignore[method-assign]

    async def run() -> None:
        await saver.setup()
        await store.setup()

    asyncio.run(run())
    return captured


# ---------------------------------------------------------------- verify SQL


def _table_exists(table: str) -> str:
    return (
        f"SELECT pg_catalog.to_regclass('{SCHEMA}.{table}') IS NOT NULL "
        f"AND (SELECT c.relkind = 'r' FROM pg_catalog.pg_class c "
        f"WHERE c.oid = pg_catalog.to_regclass('{SCHEMA}.{table}'))"
    )


def _columns_present(table: str, columns: tuple[tuple[str, str, bool], ...]) -> str:
    values = ", ".join(
        f"('{name}', '{kind}', {'true' if not_null else 'false'})"
        for name, kind, not_null in columns
    )
    return (
        "SELECT pg_catalog.count(*) = "
        f"{len(columns)} FROM pg_catalog.pg_attribute a "
        f"WHERE a.attrelid = pg_catalog.to_regclass('{SCHEMA}.{table}') AND a.attnum > 0 "
        "AND NOT a.attisdropped AND (a.attname::text, "
        "pg_catalog.format_type(a.atttypid, a.atttypmod), a.attnotnull) "
        f"IN (VALUES {values})"
    )


def _index_valid(index: str, table: str) -> str:
    return (
        "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_index i "
        "JOIN pg_catalog.pg_class c ON c.oid = i.indexrelid "
        "JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace "
        f"WHERE n.nspname = '{SCHEMA}' AND c.relname = '{index}' "
        f"AND i.indrelid = pg_catalog.to_regclass('{SCHEMA}.{table}') "
        "AND i.indisvalid AND i.indisready)"
    )


def _no_invalid_index(index: str) -> str:
    return (
        "SELECT NOT EXISTS (SELECT 1 FROM pg_catalog.pg_index i "
        "JOIN pg_catalog.pg_class c ON c.oid = i.indexrelid "
        "JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace "
        f"WHERE n.nspname = '{SCHEMA}' AND c.relname = '{index}' "
        "AND NOT (i.indisvalid AND i.indisready))"
    )


def _version_recorded(table: str, version: int) -> str:
    return f"SELECT EXISTS (SELECT 1 FROM {SCHEMA}.{table} WHERE v = {version})"


CURRENT_SCHEMA_IS_RUNTIME = f"SELECT pg_catalog.current_schema() = '{SCHEMA}'"


# ---------------------------------------------------------------- steps

PREREQUISITE_SQL = f"""DO $role$
DECLARE
    existing record;
BEGIN
    SELECT oid, rolsuper, rolbypassrls, rolcanlogin, rolcreaterole, rolcreatedb, rolreplication
      INTO existing FROM pg_catalog.pg_roles WHERE rolname = '{ROLE}';
    IF FOUND THEN
        IF existing.rolsuper OR existing.rolbypassrls OR existing.rolcanlogin
           OR existing.rolcreaterole OR existing.rolcreatedb OR existing.rolreplication THEN
            RAISE EXCEPTION 'existing role {ROLE} is not a restricted NOLOGIN role';
        END IF;
        IF EXISTS (SELECT 1 FROM pg_catalog.pg_auth_members m WHERE m.member = existing.oid) THEN
            RAISE EXCEPTION 'existing role {ROLE} holds role memberships';
        END IF;
    ELSE
        CREATE ROLE {ROLE} NOLOGIN NOSUPERUSER NOBYPASSRLS NOCREATEDB NOCREATEROLE
            NOINHERIT NOREPLICATION;
    END IF;
END
$role$;
CREATE SCHEMA {SCHEMA};
REVOKE ALL ON SCHEMA {SCHEMA} FROM PUBLIC;
REVOKE ALL ON SCHEMA {SCHEMA} FROM {ROLE};
GRANT USAGE ON SCHEMA {SCHEMA} TO {ROLE};
"""

PREREQUISITE_VERIFY = [
    f"SELECT n.nspowner = (SELECT r.oid FROM pg_catalog.pg_roles r "
    f"WHERE r.rolname = current_user) FROM pg_catalog.pg_namespace n "
    f"WHERE n.nspname = '{SCHEMA}'",
    f"SELECT NOT (r.rolsuper OR r.rolbypassrls OR r.rolcanlogin OR r.rolcreaterole "
    f"OR r.rolcreatedb OR r.rolreplication) FROM pg_catalog.pg_roles r "
    f"WHERE r.rolname = '{ROLE}'",
    f"SELECT NOT EXISTS (SELECT 1 FROM pg_catalog.pg_auth_members m "
    f"JOIN pg_catalog.pg_roles r ON r.oid = m.member WHERE r.rolname = '{ROLE}')",
    f"SELECT pg_catalog.has_schema_privilege('{ROLE}', '{SCHEMA}', 'USAGE') "
    f"AND NOT pg_catalog.has_schema_privilege('{ROLE}', '{SCHEMA}', 'CREATE')",
    f"SELECT NOT EXISTS (SELECT 1 FROM pg_catalog.pg_namespace n "
    f"CROSS JOIN LATERAL pg_catalog.aclexplode(n.nspacl) acl "
    f"WHERE n.nspname = '{SCHEMA}' AND acl.grantee = 0)",
    f"SELECT NOT EXISTS (SELECT 1 FROM pg_catalog.pg_namespace n "
    f"WHERE n.nspname IN ('mission_control', 'mission_control_search') "
    f"AND (pg_catalog.has_schema_privilege('{ROLE}', n.oid, 'USAGE') "
    f"OR pg_catalog.has_schema_privilege('{ROLE}', n.oid, 'CREATE')))",
]

_RW_TABLES = ", ".join(f"{SCHEMA}.{table}" for table in RUNTIME_TABLES)
_LEDGERS = ", ".join(f"{SCHEMA}.{table}" for table in LEDGER_TABLES)
GRANTS_SQL = f"""REVOKE ALL ON ALL TABLES IN SCHEMA {SCHEMA} FROM PUBLIC;
DO $grants$
DECLARE
    item record;
BEGIN
    FOR item IN
        SELECT DISTINCT n.nspname, c.relname, r.rolname
          FROM pg_catalog.pg_class c
          JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
          CROSS JOIN LATERAL pg_catalog.aclexplode(c.relacl) acl
          JOIN pg_catalog.pg_roles r ON r.oid = acl.grantee
         WHERE n.nspname = '{SCHEMA}' AND acl.grantee <> c.relowner
    LOOP
        EXECUTE pg_catalog.format(
            'REVOKE ALL ON %I.%I FROM %I', item.nspname, item.relname, item.rolname
        );
    END LOOP;
END
$grants$;
GRANT SELECT, INSERT, UPDATE, DELETE ON {_RW_TABLES} TO {ROLE};
GRANT SELECT ON {_LEDGERS} TO {ROLE};
"""


def _privileges(table: str, granted: tuple[str, ...], denied: tuple[str, ...]) -> str:
    checks = [
        f"pg_catalog.has_table_privilege('{ROLE}', '{SCHEMA}.{table}', '{p}')" for p in granted
    ] + [f"NOT pg_catalog.has_table_privilege('{ROLE}', '{SCHEMA}.{table}', '{p}')" for p in denied]
    return "SELECT " + " AND ".join(checks)


_ALL = ("SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE", "REFERENCES", "TRIGGER")
GRANTS_VERIFY = [
    *(_privileges(t, _ALL[:4], _ALL[4:]) for t in RUNTIME_TABLES),
    *(_privileges(t, _ALL[:1], _ALL[1:]) for t in LEDGER_TABLES),
    f"SELECT NOT EXISTS (SELECT 1 FROM pg_catalog.pg_class c "
    f"JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace "
    f"CROSS JOIN LATERAL pg_catalog.aclexplode(c.relacl) acl "
    f"WHERE n.nspname = '{SCHEMA}' AND acl.grantee <> c.relowner "
    f"AND acl.grantee <> (SELECT r.oid FROM pg_catalog.pg_roles r WHERE r.rolname = '{ROLE}'))",
]

FINAL_VERIFY = [
    # Exactly the pinned relations exist in the runtime schema.
    f"SELECT (SELECT pg_catalog.array_agg(c.relname::text ORDER BY c.relname) "
    f"FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace "
    f"WHERE n.nspname = '{SCHEMA}' AND c.relkind IN ('r', 'p', 'v', 'm', 'f', 'S')) = "
    f"ARRAY[{', '.join(repr(t) for t in sorted((*RUNTIME_TABLES, *LEDGER_TABLES)))}]::text[]",
    *(
        f"SELECT (SELECT pg_catalog.count(*) FROM pg_catalog.pg_attribute a "
        f"WHERE a.attrelid = pg_catalog.to_regclass('{SCHEMA}.{table}') AND a.attnum > 0 "
        f"AND NOT a.attisdropped) = {count}"
        for table, count in FINAL_COLUMN_COUNTS.items()
    ),
    # Every index in the schema is valid and ready (an interrupted CONCURRENTLY is not).
    f"SELECT NOT EXISTS (SELECT 1 FROM pg_catalog.pg_index i "
    f"JOIN pg_catalog.pg_class c ON c.oid = i.indexrelid "
    f"JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace "
    f"WHERE n.nspname = '{SCHEMA}' AND NOT (i.indisvalid AND i.indisready))",
]


def _vendor_steps(captured: dict[str, list[tuple[str, tuple[Any, ...] | None]]]) -> list[dict]:
    from langgraph.checkpoint.postgres.base import BasePostgresSaver
    from langgraph.store.postgres.base import BasePostgresStore

    steps: list[dict[str, Any]] = []
    for kind, migrations, ledger in (
        ("saver", list(BasePostgresSaver.MIGRATIONS), SAVER_LEDGER),
        ("store", list(BasePostgresStore.MIGRATIONS), STORE_LEDGER),
    ):
        log = list(captured[kind])
        # Ledger bootstrap: the saver runs MIGRATIONS[0] unconditionally (it is also v0);
        # the store creates its ledger with a setup()-local statement outside MIGRATIONS.
        bootstrap_sql, _ = log.pop(0)
        select_sql, _ = log.pop(0)
        if select_sql != f"SELECT v FROM {ledger} ORDER BY v DESC LIMIT 1":
            raise SystemExit(f"unexpected vendor {kind} ledger read: {select_sql!r}")
        if kind == "saver":
            if bootstrap_sql != migrations[0]:
                raise SystemExit("saver ledger bootstrap no longer equals MIGRATIONS[0]")
        else:
            steps.append(
                {
                    "id": "store.ledger",
                    "kind": "vendor",
                    "transactional": True,
                    "concurrent": False,
                    "sql": bootstrap_sql,
                    "sha256": sha256(bootstrap_sql),
                    "records_version": None,
                    "precheck": [CURRENT_SCHEMA_IS_RUNTIME],
                    "verify": [
                        _table_exists(STORE_LEDGER),
                        _columns_present(STORE_LEDGER, (("v", "integer", True),)),
                        _index_valid("store_migrations_pkey", STORE_LEDGER),
                    ],
                    "verify_recorded": [],
                }
            )
        if len(log) != 2 * len(migrations):
            raise SystemExit(f"vendor {kind} setup no longer runs one statement per migration")
        for version, sql in enumerate(migrations):
            run_sql, run_params = log[2 * version]
            insert_sql, insert_params = log[2 * version + 1]
            if run_sql != sql or run_params is not None:
                raise SystemExit(f"vendor {kind} v{version} differs from MIGRATIONS")
            if insert_sql != f"INSERT INTO {ledger} (v) VALUES (%s)" or insert_params != (version,):
                raise SystemExit(f"vendor {kind} v{version} ledger insert changed")
            concurrent = "CONCURRENTLY" in sql.upper()
            key = (kind, version)
            verify: list[str] = []
            if key in STEP_COLUMNS:
                table, columns = STEP_COLUMNS[key]
                verify += [_table_exists(table), _columns_present(table, columns)]
            indexes = STEP_INDEXES.get(key, ())
            verify += [_index_valid(index, table) for index, table in indexes]
            if concurrent and not indexes:
                raise SystemExit(f"concurrent vendor {kind} v{version} has no index expectation")
            precheck = [CURRENT_SCHEMA_IS_RUNTIME]
            if concurrent:
                precheck += [_no_invalid_index(index) for index, _ in indexes]
            steps.append(
                {
                    "id": f"{kind}.v{version}",
                    "kind": "vendor",
                    "transactional": not concurrent,
                    "concurrent": concurrent,
                    "sql": sql,
                    "sha256": sha256(sql),
                    "records_version": {
                        "table": ledger,
                        "v": version,
                        "sql": f"INSERT INTO {SCHEMA}.{ledger} (v) VALUES ({version})",
                    },
                    "precheck": precheck,
                    "verify": verify,
                    "verify_recorded": [_version_recorded(ledger, version)],
                }
            )
    return steps


def _package_root(distribution: str) -> Path:
    files = metadata.distribution(distribution).locate_file("")
    return Path(str(files))


def build_descriptor() -> dict[str, Any]:
    installed = {name: metadata.version(name) for name in PINS}
    if installed != PINS:
        raise SystemExit(f"installed packages differ from the descriptor pins: {installed}")
    root = _package_root("langgraph-checkpoint-postgres")
    sources = {
        path: hashlib.sha256((root / path).read_bytes()).hexdigest() for path in VENDOR_SOURCES
    }
    steps: list[dict[str, Any]] = [
        {
            "id": "prerequisite.schema_role",
            "kind": "sql",
            "transactional": True,
            "concurrent": False,
            "sql": PREREQUISITE_SQL,
            "sha256": sha256(PREREQUISITE_SQL),
            "records_version": None,
            "precheck": [f"SELECT pg_catalog.to_regnamespace('{SCHEMA}') IS NULL"],
            "verify": PREREQUISITE_VERIFY,
            "verify_recorded": [],
        },
        *_vendor_steps(capture_vendor_setup()),
        {
            "id": "post.grants",
            "kind": "sql",
            "transactional": True,
            "concurrent": False,
            "sql": GRANTS_SQL,
            "sha256": sha256(GRANTS_SQL),
            "records_version": None,
            "precheck": [],
            "verify": GRANTS_VERIFY,
            "verify_recorded": [],
        },
    ]
    return {
        "descriptor_schema": DESCRIPTOR_SCHEMA,
        "component": COMPONENT,
        "schema": SCHEMA,
        "role": ROLE,
        "pins": PINS,
        "vendor_sources_sha256": sources,
        "session": {
            "search_path": f"{SCHEMA}, pg_temp",
            "runtime_libpq_options": f"-c search_path={SCHEMA},pg_temp",
            "advisory_lock_sql": (
                "SELECT pg_catalog.pg_advisory_lock("
                f"pg_catalog.hashtextextended('{COMPONENT}.provision', 0))"
            ),
            "advisory_unlock_sql": (
                "SELECT pg_catalog.pg_advisory_unlock("
                f"pg_catalog.hashtextextended('{COMPONENT}.provision', 0))"
            ),
            "lock_timeout": "5s",
        },
        "steps": steps,
        "final_verify": FINAL_VERIFY,
    }


def render(descriptor: dict[str, Any]) -> str:
    return json.dumps(descriptor, indent=2, ensure_ascii=False) + "\n"


def check(path: Path = DESCRIPTOR_PATH) -> list[str]:
    """Differences between the committed descriptor and the installed vendor source."""

    expected = build_descriptor()
    committed = json.loads(path.read_text(encoding="utf-8"))
    problems: list[str] = []
    for key in ("descriptor_schema", "component", "schema", "role", "pins"):
        if committed.get(key) != expected[key]:
            problems.append(f"{key} differs")
    if committed.get("vendor_sources_sha256") != expected["vendor_sources_sha256"]:
        problems.append("vendor source sha256 differs from the installed package")
    by_id = {step["id"]: step for step in committed.get("steps", [])}
    if [step["id"] for step in committed.get("steps", [])] != [s["id"] for s in expected["steps"]]:
        problems.append("step ids/order differ")
    for step in expected["steps"]:
        found = by_id.get(step["id"])
        if found is None:
            continue
        if sha256(found.get("sql", "")) != found.get("sha256"):
            problems.append(f"{step['id']}: sql does not hash to its recorded sha256")
        if found.get("sha256") != step["sha256"]:
            problems.append(f"{step['id']}: sha256 differs from the installed vendor source")
        elif found != step:
            problems.append(f"{step['id']}: step metadata differs")
    if not problems and render(committed) != render(expected):
        problems.append("descriptor differs from generated output")
    return problems


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--write", action="store_true", help="regenerate descriptor.json")
    parser.add_argument("--check", action="store_true", help="verify (default)")
    arguments = parser.parse_args(argv)
    if arguments.write:
        DESCRIPTOR_PATH.write_text(render(build_descriptor()), encoding="utf-8", newline="\n")
        print(f"wrote {DESCRIPTOR_PATH.name}")
        return 0
    problems = check()
    for problem in problems:
        print(f"MISMATCH: {problem}", file=sys.stderr)
    if not problems:
        print("descriptor matches the installed pinned vendor source")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
