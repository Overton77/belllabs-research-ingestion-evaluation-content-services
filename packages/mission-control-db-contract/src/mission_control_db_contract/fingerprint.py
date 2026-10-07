"""Multi-schema catalog fingerprint protocol ``mc-pg-catalog-v2``.

Normalization is the v1 normalization applied per schema: relations, columns,
constraints, indexes, functions, policies, triggers, views, enums, types, sequences
and explicit grants. Database OIDs, owner role names (``$OWNER``), database/project
names and row contents are excluded. Named grants to other roles are retained so
privilege drift is visible across installations. Output is keyed per schema and lists
the schemas explicitly. Changing normalization requires a new algorithm identifier.
"""

from __future__ import annotations

import json
from collections.abc import Sequence
from typing import Any

from .canonical import canonical_bytes, digest_value, sha256_hex

FINGERPRINT_ALGORITHM = "mc-pg-catalog-v2"

# Fixed read-only queries; $1 is the schema name (bound parameter, never interpolated).
CATALOG_QUERIES: dict[str, str] = {
    "relations": """
        SELECT c.relname, c.relkind::text, c.relrowsecurity, c.relforcerowsecurity,
               c.reloptions, c.relpersistence::text,
               pg_get_expr(c.relpartbound,c.oid) AS partition_bound,
               pg_get_partkeydef(c.oid) AS partition_key
        FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
        WHERE n.nspname=$1 AND c.relkind IN ('r','p','v','m','S','f','c')
    """,
    "columns": """
        SELECT c.relname, a.attnum, a.attname, format_type(a.atttypid,a.atttypmod) AS type,
               a.attnotnull, a.attidentity::text, a.attgenerated::text,
               pg_get_expr(d.adbin,d.adrelid) AS default_expression
        FROM pg_attribute a JOIN pg_class c ON c.oid=a.attrelid
        JOIN pg_namespace n ON n.oid=c.relnamespace
        LEFT JOIN pg_attrdef d ON d.adrelid=a.attrelid AND d.adnum=a.attnum
        WHERE n.nspname=$1 AND a.attnum>0 AND NOT a.attisdropped
          AND c.relkind IN ('r','p','v','m','f','c')
    """,
    "constraints": """
        SELECT c.relname, t.typname, k.conname, k.contype::text,
               k.convalidated, pg_get_constraintdef(k.oid, true) AS definition
        FROM pg_constraint k JOIN pg_namespace n ON n.oid=k.connamespace
        LEFT JOIN pg_class c ON c.oid=k.conrelid LEFT JOIN pg_type t ON t.oid=k.contypid
        WHERE n.nspname=$1
    """,
    "indexes": """
        SELECT c.relname, i.indisvalid, i.indisready, pg_get_indexdef(i.indexrelid) AS definition
        FROM pg_index i JOIN pg_class c ON c.oid=i.indrelid
        JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=$1
    """,
    "functions": """
        SELECT p.proname, pg_get_function_identity_arguments(p.oid) AS arguments,
               pg_get_functiondef(p.oid) AS definition
        FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace
        WHERE n.nspname=$1 AND p.prokind IN ('f','p')
    """,
    "policies": """
        SELECT c.relname, p.polname, p.polcmd::text, p.polpermissive,
               ARRAY(SELECT CASE WHEN x=0 THEN 'PUBLIC' ELSE pg_get_userbyid(x) END
                     FROM unnest(p.polroles) x ORDER BY 1) AS roles,
               pg_get_expr(p.polqual,p.polrelid) AS using_expression,
               pg_get_expr(p.polwithcheck,p.polrelid) AS check_expression
        FROM pg_policy p JOIN pg_class c ON c.oid=p.polrelid
        JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=$1
    """,
    "triggers": """
        SELECT c.relname, t.tgname, t.tgenabled::text, pg_get_triggerdef(t.oid,true) AS definition
        FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid
        JOIN pg_namespace n ON n.oid=c.relnamespace
        WHERE n.nspname=$1 AND NOT t.tgisinternal
    """,
    "views": """
        SELECT c.relname, pg_get_viewdef(c.oid,true) AS definition
        FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
        WHERE n.nspname=$1 AND c.relkind IN ('v','m')
    """,
    "enums": """
        SELECT t.typname, e.enumsortorder::text, e.enumlabel
        FROM pg_type t JOIN pg_namespace n ON n.oid=t.typnamespace
        JOIN pg_enum e ON e.enumtypid=t.oid WHERE n.nspname=$1
    """,
    "types": """
        SELECT t.typname,t.typtype::text,t.typnotnull,t.typdefault,
               CASE WHEN t.typbasetype<>0 THEN format_type(t.typbasetype,t.typtypmod) END
                 AS base_type,
               CASE WHEN t.typelem<>0 THEN format_type(t.typelem,NULL) END AS element_type,
               CASE WHEN r.rngsubtype IS NOT NULL THEN format_type(r.rngsubtype,NULL) END
                 AS range_type
        FROM pg_type t JOIN pg_namespace n ON n.oid=t.typnamespace
        LEFT JOIN pg_range r ON r.rngtypid=t.oid
        WHERE n.nspname=$1
    """,
    "sequences": """
        SELECT c.relname, s.seqstart, s.seqincrement, s.seqmax, s.seqmin, s.seqcache, s.seqcycle
        FROM pg_sequence s JOIN pg_class c ON c.oid=s.seqrelid
        JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=$1
    """,
    "grants": """
        WITH objects AS (
          SELECT 'relation' AS kind,c.relname AS name,c.relowner AS owner,
            coalesce(c.relacl,acldefault(CASE WHEN c.relkind='S' THEN 'S'::"char"
                                       ELSE 'r'::"char" END,c.relowner)) AS acl
          FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
          WHERE n.nspname=$1 AND c.relkind IN ('r','p','v','m','S','f')
          UNION ALL
          SELECT 'schema',n.nspname,n.nspowner,coalesce(n.nspacl,acldefault('n',n.nspowner))
          FROM pg_namespace n WHERE n.nspname=$1
          UNION ALL
          SELECT 'function',p.proname||'('||pg_get_function_identity_arguments(p.oid)||')',
                 p.proowner,coalesce(p.proacl,acldefault('f',p.proowner))
          FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace
          WHERE n.nspname=$1
          UNION ALL
          SELECT 'column',c.relname||'.'||a.attname,c.relowner,a.attacl
          FROM pg_attribute a JOIN pg_class c ON c.oid=a.attrelid
          JOIN pg_namespace n ON n.oid=c.relnamespace
          WHERE n.nspname=$1 AND a.attnum>0 AND NOT a.attisdropped
          UNION ALL
          SELECT 'type',t.typname,t.typowner,coalesce(t.typacl,acldefault('T',t.typowner))
          FROM pg_type t JOIN pg_namespace n ON n.oid=t.typnamespace
          WHERE n.nspname=$1
        )
        SELECT o.kind,o.name,
               CASE WHEN g.grantee=o.owner THEN '$OWNER' WHEN g.grantee=0 THEN 'PUBLIC'
                    ELSE pg_get_userbyid(g.grantee) END AS grantee,
               g.privilege_type,g.is_grantable
        FROM objects o CROSS JOIN LATERAL aclexplode(o.acl) g
    """,
}


def _sort_key(row: dict[str, Any]) -> str:
    return json.dumps(row, sort_keys=True, default=str)


async def _with_catalog_search_path(connection: Any) -> str:
    previous = await connection.fetchval("SELECT current_setting('search_path')")
    await connection.execute("SELECT set_config('search_path','pg_catalog',true)")
    return str(previous)


async def _restore_search_path(connection: Any, previous: str) -> None:
    await connection.execute("SELECT set_config('search_path',$1,true)", previous)


async def schema_catalog(connection: Any, schema: str) -> dict[str, Any]:
    """Normalized catalog rows for one schema (must run inside a transaction)."""
    previous = await _with_catalog_search_path(connection)
    try:
        present = bool(
            await connection.fetchval(
                "SELECT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname=$1)", schema
            )
        )
        catalog: dict[str, Any] = {"present": present}
        for name, query in CATALOG_QUERIES.items():
            rows = [dict(row) for row in await connection.fetch(query, schema)]
            catalog[name] = sorted(
                (json.loads(json.dumps(row, default=str)) for row in rows), key=_sort_key
            )
        return catalog
    finally:
        await _restore_search_path(connection, previous)


def catalog_digest(catalog: dict[str, Any]) -> str:
    return "sha256:" + sha256_hex(canonical_bytes(catalog))


def category_digests(catalog: dict[str, Any]) -> dict[str, str]:
    return {name: digest_value(rows) for name, rows in sorted(catalog.items())}


async def catalog_fingerprint(
    connection: Any, schemas: Sequence[str]
) -> tuple[dict[str, Any], dict[str, dict[str, Any]]]:
    """Return (fingerprint document, raw per-schema catalogs).

    The document is ``{"algorithm", "schemas": [...], "per_schema": {schema: digest},
    "fingerprint": digest}``; ``fingerprint`` covers the algorithm, explicit schema list
    and every per-schema digest.
    """
    ordered = sorted(set(schemas))
    catalogs = {schema: await schema_catalog(connection, schema) for schema in ordered}
    per_schema = {schema: catalog_digest(catalogs[schema]) for schema in ordered}
    document: dict[str, Any] = {
        "algorithm": FINGERPRINT_ALGORITHM,
        "schemas": ordered,
        "per_schema": per_schema,
    }
    document["fingerprint"] = digest_value(
        {"algorithm": FINGERPRINT_ALGORITHM, "schemas": ordered, "per_schema": per_schema}
    )
    return document, catalogs


async def schema_fingerprint(connection: Any, schemas: Sequence[str]) -> str:
    document, _catalogs = await catalog_fingerprint(connection, schemas)
    return str(document["fingerprint"])


def contract_from_catalogs(
    catalogs: dict[str, dict[str, Any]], *, component: str, component_version: str
) -> dict[str, Any]:
    """Generated MC-only contract: per-schema structure, sorted and deterministic."""
    schemas: dict[str, Any] = {}
    for schema, catalog in sorted(catalogs.items()):
        tables: dict[str, Any] = {}
        for relation in catalog["relations"]:
            if relation["relkind"] not in {"r", "p", "v", "m", "f"}:
                continue
            tables[relation["relname"]] = {
                "kind": relation["relkind"],
                "row_level_security": relation["relrowsecurity"],
                "force_row_level_security": relation["relforcerowsecurity"],
                "columns": [],
                "constraints": [],
                "indexes": [],
                "policies": [],
                "triggers": [],
            }
        for column in sorted(catalog["columns"], key=lambda r: (r["relname"], r["attnum"])):
            if column["relname"] in tables:
                tables[column["relname"]]["columns"].append(
                    {
                        "name": column["attname"],
                        "type": column["type"],
                        "not_null": column["attnotnull"],
                        "default": column["default_expression"],
                        "identity": column["attidentity"] or None,
                        "generated": column["attgenerated"] or None,
                    }
                )
        for item in catalog["constraints"]:
            if item["relname"] in tables:
                tables[item["relname"]]["constraints"].append(
                    {
                        "name": item["conname"],
                        "type": item["contype"],
                        "definition": item["definition"],
                    }
                )
        for item in catalog["indexes"]:
            if item["relname"] in tables:
                tables[item["relname"]]["indexes"].append(item["definition"])
        for item in catalog["policies"]:
            if item["relname"] in tables:
                tables[item["relname"]]["policies"].append(
                    {
                        "name": item["polname"],
                        "command": item["polcmd"],
                        "permissive": item["polpermissive"],
                        "roles": item["roles"],
                        "using": item["using_expression"],
                        "with_check": item["check_expression"],
                    }
                )
        for item in catalog["triggers"]:
            if item["relname"] in tables:
                tables[item["relname"]]["triggers"].append(
                    {"name": item["tgname"], "definition": item["definition"]}
                )
        for table in tables.values():
            for key in ("constraints", "policies", "triggers"):
                table[key].sort(key=lambda entry: entry["name"])
            table["indexes"].sort()
        grants: dict[str, list[str]] = {}
        for grant in catalog["grants"]:
            label = f"{grant['kind']}:{grant['name']}"
            privilege = f"{grant['grantee']}:{grant['privilege_type']}" + (
                ":grantable" if grant["is_grantable"] else ""
            )
            grants.setdefault(label, []).append(privilege)
        schemas[schema] = {
            "present": catalog["present"],
            "tables": dict(sorted(tables.items())),
            "functions": sorted(
                (
                    {
                        "name": f["proname"],
                        "arguments": f["arguments"],
                        "definition": f["definition"],
                    }
                    for f in catalog["functions"]
                ),
                key=lambda f: (f["name"], f["arguments"]),
            ),
            "types": sorted(
                (
                    {"name": t["typname"], "type": t["typtype"], "base_type": t["base_type"]}
                    for t in catalog["types"]
                    if t["typtype"] in {"d", "e", "r", "m"}
                    or (t["typtype"] == "c" and t["typname"] not in tables)
                ),
                key=lambda t: t["name"],
            ),
            "enums": catalog["enums"],
            "sequences": catalog["sequences"],
            "views": catalog["views"],
            "grants": {label: sorted(values) for label, values in sorted(grants.items())},
        }
    return {
        "format": "mission-control-generated-contract/v1",
        "component": component,
        "component_version": component_version,
        "fingerprint_algorithm": FINGERPRINT_ALGORITHM,
        "generated": "Generated by mission-db release-build; do not edit by hand.",
        "schemas": schemas,
    }


def contract_markdown(contract: dict[str, Any], fingerprint: dict[str, Any]) -> str:
    lines = [
        f"# Generated contract: {contract['component']} {contract['component_version']}",
        "",
        "Generated by `mission-db release-build`; do not edit by hand.",
        "",
        f"Fingerprint algorithm: `{fingerprint['algorithm']}`",
        f"Release fingerprint: `{fingerprint['fingerprint']}`",
        "",
        "| Schema | Tables | RLS forced | Policies | Functions | Per-schema digest |",
        "| --- | ---: | ---: | ---: | ---: | --- |",
    ]
    for schema, body in contract["schemas"].items():
        tables = body["tables"]
        forced = sum(1 for t in tables.values() if t["force_row_level_security"])
        policies = sum(len(t["policies"]) for t in tables.values())
        lines.append(
            f"| `{schema}` | {len(tables)} | {forced} | {policies} | {len(body['functions'])} "
            f"| `{fingerprint['per_schema'][schema]}` |"
        )
    for schema, body in contract["schemas"].items():
        lines += ["", f"## `{schema}`", ""]
        if not body["tables"]:
            lines.append("No tables in this release.")
        for name, table in body["tables"].items():
            rls = (
                "FORCE RLS"
                if table["force_row_level_security"]
                else ("RLS" if table["row_level_security"] else "no RLS")
            )
            lines.append(f"- `{name}` ({len(table['columns'])} columns, {rls})")
    return "\n".join(lines) + "\n"
