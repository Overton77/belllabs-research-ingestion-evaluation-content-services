"""Independent catalog/row evidence for two-project qualification.

Deliberately NOT built on ``mission_control_db_contract.fingerprint``: the reviewer's
normalization is separate so a defect in the implementation's fingerprint cannot hide
a difference. All queries are read-only and bind schema names as parameters.

* ``owned_catalog`` normalizes the common release namespaces (``mission_control`` and
  ``mission_control_search``). The migration principal is masked as ``$OWNER``.
* ``protected_inventory`` covers every other non-system schema present in the
  database (discovered, not a guessed list), database-level objects (extensions,
  default ACLs, event triggers, database ACL/settings) and every pre-existing cluster
  role outside the release's declared capability roles, with memberships.
* ``row_digests`` is per relation: count and SHA-256 over the ordered row text,
  computed inside PostgreSQL, plus sequence states. Rows never leave the database.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable
from typing import Any

OWNED_SCHEMAS = ("mission_control", "mission_control_search")
SYSTEM_SCHEMAS = ("pg_catalog", "information_schema", "pg_toast")
CAPABILITY_ROLES = (
    "mission_control_runtime",
    "mission_control_family_writer",
    "mission_control_catalog_writer",
    "mission_control_outbox_worker",
    "mission_control_readonly",
    "mission_control_checkpointer",
)

_SCHEMA_QUERIES: dict[str, str] = {
    "schema": """
        SELECT n.nspname, pg_get_userbyid(n.nspowner) AS owner,
               coalesce(n.nspacl::text, '') AS acl
        FROM pg_namespace n WHERE n.nspname = $1
    """,
    "relations": """
        SELECT c.relname, c.relkind::text AS kind, c.relpersistence::text AS persistence,
               c.relrowsecurity AS rls, c.relforcerowsecurity AS force_rls,
               pg_get_userbyid(c.relowner) AS owner, coalesce(c.reloptions::text, '') AS options
        FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = $1 AND c.relkind IN ('r','p','v','m','S','f','c','i','I')
    """,
    "columns": """
        SELECT c.relname, a.attname, a.attnum, format_type(a.atttypid, a.atttypmod) AS type,
               a.attnotnull AS not_null, a.attidentity::text AS identity,
               a.attgenerated::text AS generated,
               coalesce(pg_get_expr(d.adbin, d.adrelid), '') AS default_expr,
               coalesce(a.attacl::text, '') AS acl
        FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
        WHERE n.nspname = $1 AND a.attnum > 0 AND NOT a.attisdropped
          AND c.relkind IN ('r','p','v','m','f','c')
    """,
    "constraints": """
        SELECT coalesce(c.relname, '') AS relname, coalesce(t.typname, '') AS typname,
               k.conname, k.contype::text AS type, k.convalidated AS validated,
               k.condeferrable AS deferrable, pg_get_constraintdef(k.oid, true) AS definition
        FROM pg_constraint k JOIN pg_namespace n ON n.oid = k.connamespace
        LEFT JOIN pg_class c ON c.oid = k.conrelid LEFT JOIN pg_type t ON t.oid = k.contypid
        WHERE n.nspname = $1
    """,
    "indexes": """
        SELECT ic.relname AS index_name, c.relname AS table_name, i.indisvalid AS valid,
               i.indisready AS ready, i.indisunique AS is_unique,
               pg_get_indexdef(i.indexrelid) AS definition
        FROM pg_index i JOIN pg_class c ON c.oid = i.indrelid
        JOIN pg_class ic ON ic.oid = i.indexrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = $1
    """,
    "functions": """
        SELECT p.proname, pg_get_function_identity_arguments(p.oid) AS args,
               p.prosecdef AS security_definer, coalesce(p.proconfig::text, '') AS config,
               pg_get_userbyid(p.proowner) AS owner, coalesce(p.proacl::text, '') AS acl,
               md5(pg_get_functiondef(p.oid)) AS body_md5
        FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
        WHERE n.nspname = $1 AND p.prokind IN ('f','p','w')
    """,
    "aggregates": """
        SELECT p.proname, pg_get_function_identity_arguments(p.oid) AS args,
               pg_get_userbyid(p.proowner) AS owner
        FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
        WHERE n.nspname = $1 AND p.prokind = 'a'
    """,
    "policies": """
        SELECT c.relname, p.polname, p.polcmd::text AS cmd, p.polpermissive AS permissive,
               ARRAY(SELECT CASE WHEN r = 0 THEN 'PUBLIC' ELSE pg_get_userbyid(r) END
                     FROM unnest(p.polroles) r ORDER BY 1) AS roles,
               coalesce(pg_get_expr(p.polqual, p.polrelid), '') AS using_expr,
               coalesce(pg_get_expr(p.polwithcheck, p.polrelid), '') AS check_expr
        FROM pg_policy p JOIN pg_class c ON c.oid = p.polrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = $1
    """,
    "triggers": """
        SELECT c.relname, t.tgname, t.tgenabled::text AS enabled,
               pg_get_triggerdef(t.oid, true) AS definition
        FROM pg_trigger t JOIN pg_class c ON c.oid = t.tgrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = $1 AND NOT t.tgisinternal
    """,
    "views": """
        SELECT c.relname, pg_get_viewdef(c.oid, true) AS definition
        FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = $1 AND c.relkind IN ('v','m')
    """,
    "types": """
        SELECT t.typname, t.typtype::text AS type, pg_get_userbyid(t.typowner) AS owner,
               coalesce(t.typacl::text, '') AS acl,
               CASE WHEN t.typbasetype <> 0 THEN format_type(t.typbasetype, t.typtypmod) END
                 AS base_type,
               ARRAY(SELECT e.enumlabel FROM pg_enum e WHERE e.enumtypid = t.oid
                     ORDER BY e.enumsortorder) AS labels
        FROM pg_type t JOIN pg_namespace n ON n.oid = t.typnamespace
        WHERE n.nspname = $1 AND t.typtype IN ('e','d','c','r','m')
          AND NOT EXISTS (SELECT 1 FROM pg_class c WHERE c.reltype = t.oid
                          AND c.relkind <> 'c')
    """,
    "sequences": """
        SELECT c.relname, s.seqtypid::regtype::text AS type, s.seqstart, s.seqincrement,
               s.seqmax, s.seqmin, s.seqcache, s.seqcycle,
               coalesce(c.relacl::text, '') AS acl
        FROM pg_sequence s JOIN pg_class c ON c.oid = s.seqrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = $1
    """,
    "relation_acl": """
        SELECT c.relname, coalesce(c.relacl::text, '') AS acl
        FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = $1 AND c.relkind IN ('r','p','v','m','f')
    """,
    "default_acl": """
        SELECT pg_get_userbyid(d.defaclrole) AS role, d.defaclobjtype::text AS kind,
               coalesce(d.defaclacl::text, '') AS acl
        FROM pg_default_acl d JOIN pg_namespace n ON n.oid = d.defaclnamespace
        WHERE n.nspname = $1
    """,
}

_DATABASE_QUERIES: dict[str, str] = {
    "extensions": """
        SELECT e.extname, e.extversion, n.nspname AS schema,
               pg_get_userbyid(e.extowner) AS owner
        FROM pg_extension e JOIN pg_namespace n ON n.oid = e.extnamespace
    """,
    "global_default_acl": """
        SELECT pg_get_userbyid(defaclrole) AS role, defaclobjtype::text AS kind,
               coalesce(defaclacl::text, '') AS acl
        FROM pg_default_acl WHERE defaclnamespace = 0
    """,
    "event_triggers": """
        SELECT evtname, evtevent, evtenabled::text AS enabled,
               evtfoid::regprocedure::text AS function
        FROM pg_event_trigger
    """,
    "database": """
        SELECT pg_get_userbyid(datdba) AS owner, coalesce(datacl::text, '') AS acl,
               pg_encoding_to_char(encoding) AS encoding, datcollate, datctype
        FROM pg_database WHERE datname = current_database()
    """,
    "database_settings": """
        SELECT coalesce(pg_get_userbyid(s.setrole), '') AS role, s.setconfig::text AS config
        FROM pg_db_role_setting s
        WHERE s.setdatabase IN (0, (SELECT oid FROM pg_database
                                    WHERE datname = current_database()))
    """,
    "publications": (
        "SELECT pubname, puballtables, pubinsert, pubupdate, pubdelete FROM pg_publication"
    ),
}


def _canonical(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")


def digest(value: Any) -> str:
    return "sha256:" + hashlib.sha256(_canonical(value)).hexdigest()


def _rows(records: Iterable[Any], owner: str | None) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for record in records:
        row = {
            key: (list(value) if isinstance(value, tuple) else value)
            for key, value in dict(record).items()
        }
        if owner is not None:
            row = json.loads(json.dumps(row, default=str).replace(f'"{owner}"', '"$OWNER"'))
            for key, value in list(row.items()):
                if isinstance(value, str) and owner and f"{owner}=" in value:
                    row[key] = value.replace(f"{owner}=", "$OWNER=").replace(f"/{owner}", "/$OWNER")
        normalized.append(row)
    return sorted(normalized, key=_canonical)


async def schema_catalog(connection: Any, schema: str, *, mask_owner: str | None) -> dict[str, Any]:
    return {
        name: _rows(await connection.fetch(query, schema), mask_owner)
        for name, query in _SCHEMA_QUERIES.items()
    }


async def owned_catalog(connection: Any) -> dict[str, Any]:
    """Normalized structure of the two common namespaces (migration principal masked)."""
    principal = await connection.fetchval("SELECT session_user")
    return {
        schema: await schema_catalog(connection, schema, mask_owner=principal)
        for schema in OWNED_SCHEMAS
    }


async def protected_schemas(connection: Any) -> list[str]:
    rows = await connection.fetch(
        """
        SELECT nspname FROM pg_namespace
        WHERE nspname <> ALL($1::text[]) AND nspname <> ALL($2::text[])
          AND nspname NOT LIKE 'pg\\_temp\\_%' AND nspname NOT LIKE 'pg\\_toast\\_temp\\_%'
        ORDER BY nspname
        """,
        list(OWNED_SCHEMAS),
        list(SYSTEM_SCHEMAS),
    )
    return [row["nspname"] for row in rows]


async def protected_roles(
    connection: Any, exclude: Iterable[str] = CAPABILITY_ROLES
) -> dict[str, Any]:
    excluded = sorted(set(exclude))
    roles = await connection.fetch(
        """
        SELECT rolname, rolsuper, rolinherit, rolcreaterole, rolcreatedb, rolcanlogin,
               rolreplication, rolbypassrls, rolconnlimit,
               coalesce(rolconfig::text, '') AS config
        FROM pg_roles WHERE rolname <> ALL($1::text[])
        """,
        excluded,
    )
    memberships = await connection.fetch(
        """
        SELECT pg_get_userbyid(m.roleid) AS role, pg_get_userbyid(m.member) AS member,
               m.admin_option, m.inherit_option, m.set_option
        FROM pg_auth_members m
        WHERE pg_get_userbyid(m.roleid) <> ALL($1::text[])
           OR pg_get_userbyid(m.member) <> ALL($1::text[])
        """,
        excluded,
    )
    return {"roles": _rows(roles, None), "memberships": _rows(memberships, None)}


async def protected_inventory(connection: Any) -> dict[str, Any]:
    """Everything the common release must not alter (run inside one snapshot)."""
    schemas = await protected_schemas(connection)
    return {
        "schemas": schemas,
        "per_schema": {
            schema: await schema_catalog(connection, schema, mask_owner=None) for schema in schemas
        },
        "database": {
            name: _rows(await connection.fetch(query), None)
            for name, query in _DATABASE_QUERIES.items()
        },
        "roles": await protected_roles(connection),
    }


def _ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


async def row_digests(connection: Any, schemas: Iterable[str]) -> dict[str, Any]:
    """Per-relation count + ordered row-text SHA-256, and sequence states."""
    result: dict[str, Any] = {"relations": {}, "sequences": {}}
    for schema in schemas:
        relations = await connection.fetch(
            """
            SELECT c.relname, c.relkind::text AS kind FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = $1 AND c.relkind IN ('r','p','m','S') ORDER BY c.relname
            """,
            schema,
        )
        for relation in relations:
            qualified = f"{_ident(schema)}.{_ident(relation['relname'])}"
            if relation["kind"] == "S":
                state = await connection.fetchrow(f"SELECT last_value, is_called FROM {qualified}")
                result["sequences"][f"{schema}.{relation['relname']}"] = dict(state)
                continue
            if relation["kind"] == "p":
                continue  # partitions carry the rows
            row = await connection.fetchrow(
                f"""
                SELECT count(*) AS row_count,
                       encode(sha256(convert_to(coalesce(
                           string_agg(t::text, E'\\n' ORDER BY t::text), ''), 'UTF8')), 'hex')
                         AS row_sha256
                FROM {qualified} t
                """
            )
            result["relations"][f"{schema}.{relation['relname']}"] = dict(row)
    return result


async def protected_snapshot(connection: Any) -> dict[str, Any]:
    """Consistent REPEATABLE READ snapshot of protected catalog + data."""
    async with connection.transaction(isolation="repeatable_read", readonly=True):
        inventory = await protected_inventory(connection)
        data = await row_digests(connection, inventory["schemas"])
    return {"inventory": inventory, "data": data}


def diff_paths(before: Any, after: Any, prefix: str = "") -> list[str]:
    """Human-readable list of differing paths (bounded) for assertion messages."""
    if isinstance(before, dict) and isinstance(after, dict):
        paths: list[str] = []
        for key in sorted(set(before) | set(after)):
            if key not in before or key not in after:
                paths.append(f"{prefix}/{key} ({'added' if key not in before else 'removed'})")
            else:
                paths.extend(diff_paths(before[key], after[key], f"{prefix}/{key}"))
        return paths
    if isinstance(before, list) and isinstance(after, list):
        if before == after:
            return []
        added = [item for item in after if item not in before]
        removed = [item for item in before if item not in after]
        plus = json.dumps(added, default=str)[:400]
        minus = json.dumps(removed, default=str)[:400]
        return [f"{prefix}: +{plus} -{minus}"]
    return [] if before == after else [f"{prefix}: {before!r} -> {after!r}"]
