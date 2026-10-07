"""Shared evidence for the independent two-project qualification (reviewer-owned).

``two_project_evidence`` runs ONE end-to-end proof per test module session:

1. build the release with the real ``mission_control_db_contract`` builder from a
   snapshot of the component tree; lock it for ``biotech`` and ``ai-engineer``;
2. create two disposable databases holding two different protected domains
   (``domain_fixtures``) and take protected snapshots (catalog + row digests);
3. plan/apply the release with the real installer; snapshot again; capture the
   independent owned-schema catalog, the package fingerprint, receipts, RLS,
   role and SECURITY DEFINER evidence; replay plan/apply (must be a no-op) and
   snapshot a third time; ``verify_release`` both targets;
4. drop both databases and their generated domain roles.

Failures inside a phase are captured as ``evidence["errors"]`` so each assertion
test fails with the phase message instead of erroring opaquely. The JSON evidence
is written under pytest's temporary directory (path printed in the report header).
"""

from __future__ import annotations

import asyncio
import json
import traceback
from pathlib import Path
from typing import Any

import asyncpg
import pytest

from tests.qualification.two_project import catalog_evidence as ce
from tests.qualification.two_project.disposable import (
    ADMIN_DSN_ENV,
    create_database,
    database_dsn,
    drop_database,
    new_suffix,
    require_admin_dsn,
    server_major,
)
from tests.qualification.two_project.domain_fixtures import create_domain_roles, install_domain
from tests.qualification.two_project.release_install import (
    build_release,
    plan_and_apply,
    relock,
    verify,
    write_target,
)

APPS = ("biotech", "ai-engineer")
INSTALLATION_IDS = {
    "biotech": "0192a4f0-0000-7000-8000-0000000c0b10",
    "ai-engineer": "0192a4f0-0000-7000-8000-0000000c0a1e",
}
PROJECT_REFS = {"biotech": "mcq-qual-biotech", "ai-engineer": "mcq-qual-ai-engineer"}

_RLS_QUERY = """
SELECT c.relname,
       ARRAY(SELECT a.attname::text FROM pg_attribute a WHERE a.attrelid = c.oid
             AND a.attnum > 0 AND NOT a.attisdropped ORDER BY a.attnum) AS columns,
       c.relrowsecurity AS rls, c.relforcerowsecurity AS force_rls,
       ARRAY(SELECT jsonb_build_object(
                 'name', p.polname, 'cmd', p.polcmd::text,
                 'using', coalesce(pg_get_expr(p.polqual, p.polrelid), ''),
                 'check', coalesce(pg_get_expr(p.polwithcheck, p.polrelid), ''))
             FROM pg_policy p WHERE p.polrelid = c.oid ORDER BY p.polname) AS policies
FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = ANY($1::text[]) AND c.relkind IN ('r','p')
ORDER BY n.nspname, c.relname
"""

_ROLE_QUERY = """
SELECT r.rolname, r.rolsuper, r.rolbypassrls, r.rolcanlogin, r.rolcreaterole,
       r.rolcreatedb, r.rolinherit, r.rolreplication,
       (SELECT count(*) FROM pg_shdepend d
        WHERE d.refclassid = 'pg_authid'::regclass AND d.refobjid = r.oid AND d.deptype = 'o'
          AND d.dbid IN (0, (SELECT oid FROM pg_database WHERE datname = current_database())))
         AS owned_objects,
       ARRAY(SELECT g.rolname::text FROM pg_auth_members m JOIN pg_roles g ON g.oid = m.roleid
             WHERE m.member = r.oid ORDER BY 1) AS member_of
FROM pg_roles r WHERE r.rolname LIKE 'mission\\_control\\_%' ORDER BY r.rolname
"""

_FUNCTION_QUERY = """
SELECT n.nspname || '.' || p.proname || '(' || pg_get_function_identity_arguments(p.oid) || ')'
         AS signature,
       p.prokind::text AS kind, p.prosecdef AS security_definer,
       coalesce(p.proconfig, ARRAY[]::text[]) AS config,
       has_function_privilege('public', p.oid, 'EXECUTE') AS public_execute
FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
WHERE n.nspname = ANY($1::text[]) ORDER BY 1
"""


async def _phase(evidence: dict[str, Any], name: str, coroutine: Any) -> Any:
    try:
        return await coroutine
    except Exception as exc:  # recorded and re-surfaced by every dependent test
        evidence["errors"].append(
            {
                "phase": name,
                "error": f"{type(exc).__name__}: {exc}",
                "trace": traceback.format_exc(limit=6),
            }
        )
        raise


async def _snapshot(dsn: str) -> dict[str, Any]:
    connection = await asyncpg.connect(dsn)
    try:
        return await ce.protected_snapshot(connection)
    finally:
        await connection.close()


async def _installed_evidence(dsn: str) -> dict[str, Any]:
    from mission_control_db_contract import OWNED_SCHEMAS
    from mission_control_db_contract.fingerprint import catalog_fingerprint

    connection = await asyncpg.connect(dsn)
    try:
        async with connection.transaction(isolation="repeatable_read", readonly=True):
            owned = await ce.owned_catalog(connection)
            package_fingerprint, _catalogs = await catalog_fingerprint(connection, OWNED_SCHEMAS)
            receipts = [
                dict(row)
                for row in await connection.fetch(
                    "SELECT component_version, migration_key, migration_digest "
                    "FROM mission_control.component_release ORDER BY migration_key"
                )
            ]
            rls = [dict(row) for row in await connection.fetch(_RLS_QUERY, list(ce.OWNED_SCHEMAS))]
            for row in rls:
                row["policies"] = [json.loads(item) for item in row["policies"]]
            roles = [dict(row) for row in await connection.fetch(_ROLE_QUERY)]
            functions = [
                dict(row) for row in await connection.fetch(_FUNCTION_QUERY, list(ce.OWNED_SCHEMAS))
            ]
        return {
            "owned_catalog": owned,
            "owned_digest": ce.digest(owned),
            "owned_digest_per_schema": {name: ce.digest(value) for name, value in owned.items()},
            "package_fingerprint": package_fingerprint,
            "receipts": receipts,
            "rls": rls,
            "capability_roles": roles,
            "functions": functions,
        }
    finally:
        await connection.close()


async def run_two_project(scratch: Path) -> dict[str, Any]:
    server = require_admin_dsn()
    evidence: dict[str, Any] = {"errors": [], "apps": {}, "server_major": None}
    created: dict[str, tuple[str, tuple[str, ...]]] = {}
    try:
        evidence["server_major"] = await _phase(evidence, "server", server_major(server))
        built = await _phase(
            evidence, "release_build", build_release(scratch, ADMIN_DSN_ENV, "biotech")
        )
        releases = {"biotech": built.release, "ai-engineer": relock(built, "ai-engineer")}
        evidence["release"] = {
            "build": built.build_report,
            "summary": built.release.summary(),
            "manifest_schema_fingerprint": built.release.manifest["schema_fingerprint"],
            "manifest_schema_fingerprints": built.release.manifest.get("schema_fingerprints"),
            "ordered_migrations": built.release.manifest["ordered_migrations"],
            "runtime_roles": built.release.manifest["runtime_roles"],
        }
        dsns: dict[str, str] = {}
        for app in APPS:
            suffix = new_suffix()
            name = await create_database(server, suffix)
            admin = await asyncpg.connect(server)
            try:
                roles = await create_domain_roles(admin, suffix)
            finally:
                await admin.close()
            created[app] = (name, tuple(roles.values()))
            dsns[app] = database_dsn(server, name)
            connection = await asyncpg.connect(dsns[app])
            try:
                await _phase(evidence, f"{app}:domain", install_domain(connection, app, roles))
            finally:
                await connection.close()
            evidence["apps"][app] = {"database": name, "domain_roles": roles}
        for app in APPS:
            evidence["apps"][app]["protected_before"] = await _phase(
                evidence, f"{app}:snapshot_before", _snapshot(dsns[app])
            )
        targets = {}
        for app in APPS:
            name = created[app][0]
            targets[app] = write_target(
                scratch / "targets",
                app=app,
                database=name,
                server_dsn=server,
                installation_id=INSTALLATION_IDS[app],
                project_ref=PROJECT_REFS[app],
                env_name=f"MCQ_QUAL_DSN_{app.upper().replace('-', '_')}",
            )
            evidence["apps"][app]["install"] = await _phase(
                evidence, f"{app}:apply", plan_and_apply(targets[app], releases[app])
            )
        for app in APPS:
            entry = evidence["apps"][app]
            entry["protected_after"] = await _phase(
                evidence, f"{app}:snapshot_after", _snapshot(dsns[app])
            )
            entry.update(await _phase(evidence, f"{app}:installed", _installed_evidence(dsns[app])))
            entry["replay"] = await _phase(
                evidence, f"{app}:replay", plan_and_apply(targets[app], releases[app])
            )
            entry["protected_after_replay"] = await _phase(
                evidence, f"{app}:snapshot_replay", _snapshot(dsns[app])
            )
            entry["verify"] = await _phase(
                evidence, f"{app}:verify", verify(targets[app], releases[app])
            )
    except Exception as exc:
        if not evidence["errors"]:
            evidence["errors"].append({"phase": "aborted", "error": repr(exc)})
    finally:
        for name, role_names in created.values():
            try:
                await drop_database(server, name, role_names)
            except Exception as exc:
                evidence["errors"].append({"phase": f"cleanup {name}", "error": repr(exc)})
    return evidence


def require(evidence: dict[str, Any], *paths: str) -> None:
    """Fail (never skip) with the recorded phase error when evidence is incomplete."""
    for path in paths:
        node: Any = evidence
        for part in path.split("/"):
            if not isinstance(node, dict) or part not in node:
                errors = (
                    "\n".join(f"[{e['phase']}] {e['error']}" for e in evidence.get("errors", []))
                    or "no phase error recorded"
                )
                pytest.fail(f"qualification evidence '{path}' is missing:\n{errors}")
            node = node[part]


@pytest.fixture(scope="session")
def two_project_evidence(tmp_path_factory: pytest.TempPathFactory) -> dict[str, Any]:
    try:
        require_admin_dsn()
    except pytest.fail.Exception as exc:  # each dependent test then FAILS via require()
        return {"errors": [{"phase": "admin_dsn", "error": str(exc)}], "apps": {}}
    scratch = tmp_path_factory.mktemp("two_project_qualification")
    evidence = asyncio.run(run_two_project(scratch))
    report = scratch / "two-project-evidence.json"
    report.write_text(json.dumps(evidence, indent=2, default=str), encoding="utf-8")
    evidence["report_path"] = str(report)
    return evidence
