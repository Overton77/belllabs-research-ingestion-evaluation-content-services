"""Protected-object snapshot and comparison (read-only, hashes only).

Inventory is discovered: every schema except system schemas (``pg_*``,
``information_schema``) and the Mission Control owned/runtime schemas. For each one
the snapshot records a structural fingerprint (``mc-pg-catalog-v2`` normalization),
row counts and deterministic chunked row-content digests computed INSIDE the
database (sha256 over ``row_to_json`` text ordered by primary key, else by the full
row text), and sequence states. It also records roles/memberships (names and
attributes only), extensions and ``storage.buckets`` settings when present. Rows,
credentials and password hashes never leave the database.
"""

from __future__ import annotations

import fnmatch
from typing import Any

from . import OWNED_SCHEMAS, RUNTIME_SCHEMA
from .canonical import digest_value
from .errors import ContractError, sqlstate
from .fingerprint import FINGERPRINT_ALGORITHM, catalog_digest, category_digests, schema_catalog
from .target import check_database_identity, close_quietly, connect, public_identity

SNAPSHOT_FORMAT = "mission-control-protected-snapshot/v1"
ALLOWED_FORMAT = "mission-control-allowed-differences/v1"
EXCLUDED_SCHEMAS = (*OWNED_SCHEMAS, RUNTIME_SCHEMA, "information_schema")
BUCKET_SETTING_COLUMNS = (
    "id",
    "name",
    "public",
    "file_size_limit",
    "allowed_mime_types",
    "avif_autodetection",
    "type",
)


def quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


async def _relation_digest(
    connection: Any, schema: str, table: str, key_columns: list[str], chunk_size: int
) -> dict[str, Any]:
    qualified = f"{quote_ident(schema)}.{quote_ident(table)}"
    if key_columns:
        order = ", ".join(f"t.{quote_ident(c)}" for c in key_columns)
        ordering = "primary_key"
    else:
        order = 'row_to_json(t)::text COLLATE "C"'
        ordering = "row_text"
    query = (
        f"WITH r AS (SELECT row_to_json(t)::text AS j, "
        f"row_number() OVER (ORDER BY {order}) - 1 AS n FROM {qualified} t) "
        "SELECT (n / $1)::bigint AS chunk, count(*)::bigint AS rows, "
        "encode(sha256(convert_to(string_agg(j, E'\\n' ORDER BY n), 'UTF8')), 'hex') AS digest "
        "FROM r GROUP BY 1 ORDER BY 1"
    )
    try:
        async with connection.transaction():  # savepoint: an unreadable table never aborts
            rows = await connection.fetch(query, chunk_size)
    except Exception as exc:
        return {"status": "unreadable", "sqlstate": sqlstate(exc), "ordering": ordering}
    chunks = [{"index": r["chunk"], "rows": r["rows"], "digest": r["digest"]} for r in rows]
    return {
        "status": "hashed",
        "ordering": ordering,
        "row_count": sum(c["rows"] for c in chunks),
        "chunks": chunks,
        "content_digest": digest_value([c["digest"] for c in chunks]),
    }


async def capture(connection: Any, *, chunk_size: int) -> dict[str, Any]:
    schemas = [
        row["nspname"]
        for row in await connection.fetch(
            "SELECT nspname FROM pg_namespace WHERE nspname NOT LIKE 'pg\\_%' "
            "AND nspname <> ALL($1::text[]) ORDER BY 1",
            list(EXCLUDED_SCHEMAS),
        )
    ]
    me = await connection.fetchrow(
        "SELECT rolsuper OR rolbypassrls AS bypasses_rls FROM pg_roles WHERE rolname=current_user"
    )
    result_schemas: dict[str, Any] = {}
    for schema in schemas:
        catalog = await schema_catalog(connection, schema)
        relations: dict[str, Any] = {}
        tables = await connection.fetch(
            "SELECT c.relname, c.relrowsecurity, "
            "ARRAY(SELECT a.attname FROM unnest(i.indkey) WITH ORDINALITY k(attnum, ord) "
            "      JOIN pg_attribute a ON a.attrelid=c.oid AND a.attnum=k.attnum "
            "      ORDER BY k.ord) AS key_columns "
            "FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
            "LEFT JOIN pg_index i ON i.indrelid=c.oid AND i.indisprimary "
            "WHERE n.nspname=$1 AND c.relkind='r' ORDER BY c.relname",
            schema,
        )
        for table in tables:
            entry = await _relation_digest(
                connection, schema, table["relname"], list(table["key_columns"]), chunk_size
            )
            entry["rls_may_filter"] = bool(table["relrowsecurity"]) and not me["bypasses_rls"]
            relations[table["relname"]] = entry
        sequences: dict[str, Any] = {}
        for seq in await connection.fetch(
            "SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
            "WHERE n.nspname=$1 AND c.relkind='S' ORDER BY 1",
            schema,
        ):
            try:
                async with connection.transaction():
                    state = await connection.fetchrow(
                        f"SELECT last_value, is_called FROM {quote_ident(schema)}."
                        f"{quote_ident(seq['relname'])}"
                    )
                sequences[seq["relname"]] = {
                    "last_value": state["last_value"],
                    "is_called": state["is_called"],
                }
            except Exception as exc:
                sequences[seq["relname"]] = {"status": "unreadable", "sqlstate": sqlstate(exc)}
        result_schemas[schema] = {
            "structure_digest": catalog_digest(catalog),
            "structure": category_digests(catalog),
            "relations": relations,
            "sequences": sequences,
        }
    roles = {
        row["rolname"]: {k: row[k] for k in row.keys() if k != "rolname"}
        for row in await connection.fetch(
            "SELECT rolname, rolsuper, rolinherit, rolcreaterole, rolcreatedb, rolcanlogin, "
            "rolreplication, rolbypassrls, rolconnlimit, (rolvaliduntil IS NOT NULL) "
            "AS has_valid_until FROM pg_roles ORDER BY rolname"
        )
    }
    memberships = {
        f"{row['role']}<-{row['member']}": {
            "admin_option": row["admin_option"],
            "inherit_option": row["inherit_option"],
            "set_option": row["set_option"],
        }
        for row in await connection.fetch(
            "SELECT g.rolname AS role, m2.rolname AS member, m.admin_option, "
            "m.inherit_option, m.set_option FROM pg_auth_members m "
            "JOIN pg_roles g ON g.oid=m.roleid JOIN pg_roles m2 ON m2.oid=m.member ORDER BY 1,2"
        )
    }
    extensions = {
        row["extname"]: {"version": row["extversion"], "schema": row["schema"]}
        for row in await connection.fetch(
            "SELECT e.extname, e.extversion, n.nspname AS schema FROM pg_extension e "
            "JOIN pg_namespace n ON n.oid=e.extnamespace ORDER BY 1"
        )
    }
    buckets: dict[str, Any] | None = None
    if await connection.fetchval("SELECT to_regclass('storage.buckets') IS NOT NULL"):
        columns = [
            row["attname"]
            for row in await connection.fetch(
                "SELECT attname FROM pg_attribute WHERE attrelid='storage.buckets'::regclass "
                "AND attnum>0 AND NOT attisdropped AND attname = ANY($1::text[]) ORDER BY attnum",
                list(BUCKET_SETTING_COLUMNS),
            )
        ]
        if "id" in columns:
            selected = ", ".join(quote_ident(c) for c in columns)
            try:
                async with connection.transaction():
                    bucket_rows = await connection.fetch(
                        f"SELECT {selected} FROM storage.buckets ORDER BY id"
                    )
                buckets = {
                    str(row["id"]): {
                        k: (list(v) if isinstance(v, list | tuple) else v)
                        for k, v in row.items()
                        if k != "id"
                    }
                    for row in bucket_rows
                }
            except Exception as exc:
                buckets = {"$unreadable": {"sqlstate": sqlstate(exc)}}
    return {
        "format": SNAPSHOT_FORMAT,
        "fingerprint_algorithm": FINGERPRINT_ALGORITHM,
        "snapshot_method": {
            "isolation": "repeatable_read",
            "read_only": True,
            "row_digest": "sha256(string_agg(row_to_json(t)::text, '\\n') per chunk) in-database",
            "ordering": "primary key, else full row text COLLATE C",
            "chunk_size": chunk_size,
            "concurrent_writers": "not attributable; quiesce writers for equality proofs",
        },
        "server_version_num": int(await connection.fetchval("SHOW server_version_num")),
        "excluded_schemas": [*sorted(EXCLUDED_SCHEMAS), "pg_*"],
        "schemas": result_schemas,
        "roles": roles,
        "memberships": memberships,
        "extensions": extensions,
        "storage_buckets": buckets,
    }


async def snapshot_target(
    target: dict[str, str], *, chunk_size: int = 1000, statement_timeout_ms: int = 60000
) -> dict[str, Any]:
    if not 1 <= chunk_size <= 100000:
        raise ContractError("chunk size must be between 1 and 100000")
    connection = await connect(
        target, readonly=True, statement_timeout_ms=statement_timeout_ms, lock_timeout_ms=5000
    )
    try:
        async with connection.transaction(isolation="repeatable_read", readonly=True):
            await check_database_identity(connection, target)
            body = await capture(connection, chunk_size=chunk_size)
        body["target"] = {k: v for k, v in public_identity(target).items() if k != "approved_by"}
        return body
    except ContractError:
        raise
    except Exception as exc:
        raise ContractError(
            f"Protected snapshot failed (SQLSTATE {sqlstate(exc)}); details withheld"
        ) from None
    finally:
        await close_quietly(connection)


def flatten(value: Any, prefix: str = "") -> dict[str, Any]:
    if isinstance(value, dict):
        if not value and prefix:
            return {prefix: {}}
        out: dict[str, Any] = {}
        for key, item in value.items():
            out.update(flatten(item, f"{prefix}/{key}" if prefix else str(key)))
        return out
    if (
        isinstance(value, list)
        and prefix
        and value
        and all(isinstance(item, dict) for item in value)
    ):
        out = {}
        for index, item in enumerate(value):
            out.update(flatten(item, f"{prefix}/{index}"))
        return out
    return {prefix: value}


def _entity(path: str) -> str:
    """Collapse leaf paths to the entity that appeared/disappeared (roles/x, schemas/y)."""
    parts = path.split("/")
    if parts[0] in {"roles", "memberships", "extensions", "storage_buckets"}:
        return "/".join(parts[:2])
    if parts[0] == "schemas":
        if len(parts) >= 4 and parts[2] in {"relations", "sequences"}:
            return "/".join(parts[:4])
        return "/".join(parts[:2])
    return path


def compare(
    before: dict[str, Any], after: dict[str, Any], allowed: dict[str, Any] | None
) -> dict[str, Any]:
    for document in (before, after):
        if document.get("format") != SNAPSHOT_FORMAT:
            raise ContractError("Compare requires two protected snapshots")
    rules: list[dict[str, str]] = []
    if allowed is not None:
        if allowed.get("format") != ALLOWED_FORMAT or not isinstance(allowed.get("allowed"), list):
            raise ContractError("Allowed-differences manifest has an unsupported format")
        for rule in allowed["allowed"]:
            if (
                not isinstance(rule, dict)
                or rule.get("change") not in {"added", "removed", "changed"}
                or not isinstance(rule.get("path"), str)
                or not isinstance(rule.get("reason"), str)
            ):
                raise ContractError("Allowed rules need path, change and reason")
            rules.append(rule)
    ignore = {"target"}
    left = flatten({k: v for k, v in before.items() if k not in ignore})
    right = flatten({k: v for k, v in after.items() if k not in ignore})
    left_entities = {_entity(p) for p in left}
    right_entities = {_entity(p) for p in right}
    differences: list[dict[str, str]] = []
    for entity in sorted(right_entities - left_entities):
        differences.append({"path": entity, "change": "added"})
    for entity in sorted(left_entities - right_entities):
        differences.append({"path": entity, "change": "removed"})
    common = left_entities & right_entities
    for path in sorted(set(left) | set(right)):
        if _entity(path) in common and left.get(path, "$absent") != right.get(path, "$absent"):
            differences.append({"path": path, "change": "changed"})
    unallowed = []
    for difference in differences:
        match = next(
            (
                rule
                for rule in rules
                if rule["change"] == difference["change"]
                and fnmatch.fnmatchcase(difference["path"], rule["path"])
            ),
            None,
        )
        difference["allowed_by"] = match["reason"] if match else ""
        if match is None:
            unallowed.append(difference)
    return {
        "status": "identical" if not differences else ("allowed" if not unallowed else "blocked"),
        "difference_count": len(differences),
        "unallowed_count": len(unallowed),
        "differences": differences,
    }
