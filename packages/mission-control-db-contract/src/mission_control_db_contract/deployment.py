"""Plan, apply and verify the common release; all schema SQL comes from verified bytes.

Protocol (``mission-control-sql/v2``):

* ``plan`` observes the target read-only (REPEATABLE READ) and emits a deterministic
  plan body bound to the before-fingerprint, observed identity/receipts/roles and the
  release digests. Its ``plan_digest`` is ``sha256`` over the canonical body.
* ``apply`` takes the migration advisory lock inside ONE transaction, re-observes the
  target, recomputes the plan body and rejects a stale plan (unless nothing is pending
  and the release verifies, which is a no-op replay). Migration SQL, component_release
  receipts, application_installation insert/version update and the release_attestation
  row commit atomically, after final fingerprint/role verification.
* Collisions hold before anything is created: an owned schema without receipts, or a
  ``mission_control_*`` role with LOGIN/SUPERUSER/BYPASSRLS/CREATEROLE/CREATEDB.
"""

from __future__ import annotations

from typing import Any

from . import OWNED_SCHEMAS
from .canonical import digest_value
from .errors import ContractError, sqlstate
from .fingerprint import FINGERPRINT_ALGORITHM, catalog_fingerprint
from .integrity import Migration, Release
from .target import (
    check_database_identity,
    close_quietly,
    connect,
    public_identity,
)

MIGRATION_LOCK = 0x4D435F44425F5632  # "MC_DB_V2"
PLAN_FORMAT = "mission-control-plan/v1"
DANGEROUS_ROLE_ATTRIBUTES = (
    "rolcanlogin",
    "rolsuper",
    "rolbypassrls",
    "rolcreaterole",
    "rolcreatedb",
)

_OWNED_OBJECT_OWNERS = """
    SELECT n.nspowner AS owner FROM pg_namespace n WHERE n.nspname = ANY($1::text[])
    UNION SELECT c.relowner FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
      WHERE n.nspname = ANY($1::text[])
    UNION SELECT p.proowner FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace
      WHERE n.nspname = ANY($1::text[])
    UNION SELECT t.typowner FROM pg_type t JOIN pg_namespace n ON n.oid=t.typnamespace
      WHERE n.nspname = ANY($1::text[])
"""


async def observe_state(connection: Any) -> dict[str, Any]:
    """Read-only observation of everything the plan binds to (inside a transaction)."""
    schemas = list(OWNED_SCHEMAS)
    present = [
        row["nspname"]
        for row in await connection.fetch(
            "SELECT nspname FROM pg_namespace WHERE nspname = ANY($1::text[]) ORDER BY 1",
            schemas,
        )
    ]
    roles = []
    for row in await connection.fetch(
        "SELECT r.rolname, r.rolcanlogin, r.rolsuper, r.rolbypassrls, r.rolcreaterole, "
        "r.rolcreatedb, r.rolinherit, "
        f"EXISTS(SELECT 1 FROM ({_OWNED_OBJECT_OWNERS}) owners "
        "        WHERE pg_has_role(r.oid, owners.owner, 'MEMBER')) AS owns_owned_objects, "
        "ARRAY(SELECT g.rolname FROM pg_auth_members m JOIN pg_roles g ON g.oid=m.roleid "
        "      WHERE m.member=r.oid ORDER BY 1) AS member_of "
        "FROM pg_roles r WHERE r.rolname LIKE 'mission\\_control\\_%' ORDER BY r.rolname",
        schemas,
    ):
        roles.append(dict(row))
    extensions = [
        dict(row)
        for row in await connection.fetch(
            "SELECT e.extname AS name, e.extversion AS version, n.nspname AS schema "
            "FROM pg_extension e JOIN pg_namespace n ON n.oid=e.extnamespace ORDER BY 1"
        )
    ]
    state: dict[str, Any] = {
        "server_version_num": int(await connection.fetchval("SHOW server_version_num")),
        "owned_schemas_present": present,
        "mission_control_roles": roles,
        "extensions": extensions,
        "receipts": [],
        "identity": [],
        "attestations": [],
        "fingerprint": None,
    }
    if await connection.fetchval(
        "SELECT to_regclass('mission_control.component_release') IS NOT NULL"
    ):
        state["receipts"] = [
            dict(row)
            for row in await connection.fetch(
                "SELECT component_version, migration_key, migration_digest "
                "FROM mission_control.component_release ORDER BY migration_key"
            )
        ]
    if await connection.fetchval(
        "SELECT to_regclass('mission_control.application_installation') IS NOT NULL"
    ):
        state["identity"] = [
            dict(row)
            for row in await connection.fetch(
                "SELECT installation_id::text, application_id, supabase_project_ref, "
                "environment, schema_component_version, state "
                "FROM mission_control.application_installation ORDER BY installation_id"
            )
        ]
    if await connection.fetchval(
        "SELECT to_regclass('mission_control.release_attestation') IS NOT NULL"
    ):
        state["attestations"] = [
            dict(row)
            for row in await connection.fetch(
                "SELECT component_version, manifest_digest, contract_schema_digest, "
                "schema_fingerprint, fingerprint_algorithm, supported_reader_versions, "
                "supported_writer_versions FROM mission_control.release_attestation "
                "ORDER BY component_version"
            )
        ]
    if present:
        document, _catalogs = await catalog_fingerprint(connection, schemas)
        state["fingerprint"] = document
    return state


def receipt_prefix(receipts: list[dict[str, Any]], migrations: tuple[Migration, ...]) -> int:
    expected = {m.key: m.digest for m in migrations}
    actual = {row["migration_key"]: row["migration_digest"] for row in receipts}
    if len(actual) != len(receipts) or any(expected.get(k) != v for k, v in actual.items()):
        raise ContractError("Unknown or changed applied migration checksum", code="RECEIPT_DRIFT")
    if set(actual) != {m.key for m in migrations[: len(actual)]}:
        raise ContractError(
            "Applied migration receipts are not an ordered release prefix", code="RECEIPT_DRIFT"
        )
    return len(actual)


def _expected_identity(target: dict[str, str]) -> tuple[str, str, str, str]:
    return (
        target["installation_id"],
        target["application_id"],
        target["project_ref"],
        target["environment"],
    )


def _attestation(release: Release) -> dict[str, Any]:
    manifest = release.manifest
    return {
        "component_version": manifest["component_version"],
        "manifest_digest": release.manifest_digest,
        "contract_schema_digest": manifest["contract_schema_digest"],
        "schema_fingerprint": manifest["schema_fingerprint"],
        "fingerprint_algorithm": manifest["schema_fingerprint_algorithm"],
        "supported_reader_versions": list(manifest["supported_reader_versions"]),
        "supported_writer_versions": list(manifest["supported_writer_versions"]),
    }


def evaluate(
    state: dict[str, Any],
    target: dict[str, str],
    release: Release,
    *,
    reader_version: str,
    writer_version: str,
) -> tuple[list[str], int]:
    """Pure gate evaluation; returns (holds, applied_prefix_length)."""
    manifest = release.manifest
    holds: list[str] = []
    if release.lock.get("app") != target["app"]:
        holds.append("Release lock app differs from the target app")
    if (
        reader_version not in manifest["supported_reader_versions"]
        or writer_version not in manifest["supported_writer_versions"]
    ):
        holds.append("Reader/writer versions are not admitted by the pinned release")
    version = state["server_version_num"]
    if version < manifest["min_postgres_version"] or version // 10000 != manifest["postgres_major"]:
        holds.append("PostgreSQL version is outside the fingerprint-qualified release")
    installed = {(e["name"], e["schema"]) for e in state["extensions"]}
    for extension in manifest["required_extensions"]:
        if (extension["name"], extension["schema"]) not in installed:
            holds.append(
                f"Required extension {extension['name']} is not provisioned in schema "
                f"{extension['schema']}; provisioning must precede application"
            )
    for role in state["mission_control_roles"]:
        flagged = [name for name in DANGEROUS_ROLE_ATTRIBUTES if role[name]]
        if flagged:
            holds.append(
                f"Role {role['rolname']} collides: {','.join(sorted(flagged))} is not permitted"
            )
    receipts = state["receipts"]
    if state["owned_schemas_present"] and not receipts:
        holds.append(
            "Owned schema exists without release receipts; explicit reconciliation required"
        )
    try:
        applied = receipt_prefix(receipts, release.migrations)
    except ContractError as exc:
        holds.append(str(exc))
        applied = 0
    identity = state["identity"]
    if identity:
        rows = [
            (r["installation_id"], r["application_id"], r["supabase_project_ref"], r["environment"])
            for r in identity
        ]
        if rows != [_expected_identity(target)]:
            holds.append("Database installation identity differs from explicit target")
    elif receipts:
        holds.append("Release receipts exist without an installation identity")
    fingerprint = state["fingerprint"]
    if state["owned_schemas_present"] and receipts and fingerprint is not None:
        accepted = (
            [manifest["schema_fingerprint"]]
            if applied == len(release.migrations)
            else list(manifest["compatible_previous_fingerprints"])
        )
        if fingerprint["fingerprint"] not in accepted:
            holds.append("Existing schema fingerprint is not an admitted state for this release")
    wanted = _attestation(release)
    for row in state["attestations"]:
        if row["component_version"] == wanted["component_version"] and _normalize(row) != wanted:
            holds.append("Existing release attestation for this version differs")
    return holds, applied


def _normalize(row: dict[str, Any]) -> dict[str, Any]:
    return {
        key: list(value) if isinstance(value, list | tuple) else value for key, value in row.items()
    }


def build_plan_body(
    state: dict[str, Any],
    target: dict[str, str],
    release: Release,
    *,
    reader_version: str,
    writer_version: str,
) -> dict[str, Any]:
    holds, applied = evaluate(
        state, target, release, reader_version=reader_version, writer_version=writer_version
    )
    pending = list(release.migrations[applied:]) if not holds else []
    existing_roles = {role["rolname"] for role in state["mission_control_roles"]}
    mutations: list[str] = []
    for migration in pending:
        mutations.append(f"execute migration {migration.key} ({migration.digest})")
    if pending:
        mutations += [
            f"create absent NOLOGIN role {role}"
            for role in release.manifest["runtime_roles"]
            if role not in existing_roles
        ]
        mutations.append("insert component_release receipts for pending migrations")
        if state["identity"]:
            mutations.append("update application_installation.schema_component_version")
        else:
            mutations.append("insert application_installation identity row")
    versions = {row["component_version"] for row in state["attestations"]}
    if not holds and release.manifest["component_version"] not in versions:
        mutations.append(f"insert release_attestation {release.manifest['component_version']}")
    return {
        "format": PLAN_FORMAT,
        "operation": "apply",
        "target": public_identity(target),
        "release": release.summary(),
        "compatibility": {"reader_version": reader_version, "writer_version": writer_version},
        "before": {
            "server_version_num": state["server_version_num"],
            "extensions": state["extensions"],
            "owned_schemas_present": state["owned_schemas_present"],
            "mission_control_roles": state["mission_control_roles"],
            "identity": state["identity"],
            "receipts": state["receipts"],
            "attestations": [_normalize(row) for row in state["attestations"]],
            "fingerprint": state["fingerprint"],
        },
        "pending_migrations": [m.key for m in pending],
        "mutations": mutations,
        "holds": holds,
        "status": "hold" if holds else ("noop" if not mutations else "pending"),
    }


def plan_digest(body: dict[str, Any]) -> str:
    return digest_value(body)


def verify_state(
    state: dict[str, Any],
    target: dict[str, str],
    release: Release,
    *,
    reader_version: str,
    writer_version: str,
) -> dict[str, Any]:
    holds, applied = evaluate(
        state, target, release, reader_version=reader_version, writer_version=writer_version
    )
    if holds:
        raise ContractError("; ".join(holds), code="VERIFY_HOLD")
    manifest = release.manifest
    if applied != len(release.migrations):
        raise ContractError("Pinned release migrations have not all been applied")
    identity = state["identity"]
    if (
        len(identity) != 1
        or identity[0]["schema_component_version"] != manifest["component_version"]
    ):
        raise ContractError("Installation component version does not match pinned release")
    if identity[0]["state"] != "active":
        raise ContractError("Installation identity is not active")
    wanted = _attestation(release)
    if not any(_normalize(row) == wanted for row in state["attestations"]):
        raise ContractError("Release attestation is absent or differs")
    roles = {role["rolname"]: role for role in state["mission_control_roles"]}
    declared = set(manifest["runtime_roles"])
    for name in manifest["runtime_roles"]:
        role = roles.get(name)
        if role is None:
            raise ContractError(f"Declared runtime role {name} is missing")
        if role["owns_owned_objects"]:
            raise ContractError(f"Declared runtime role {name} owns or inherits component objects")
        if declared & set(role["member_of"]):
            raise ContractError(
                f"Declared runtime role {name} is a member of another capability role"
            )
    fingerprint = state["fingerprint"]
    if fingerprint is None or fingerprint["fingerprint"] != manifest["schema_fingerprint"]:
        raise ContractError(
            "Schema fingerprint mismatch (includes constraints, RLS and grants)",
            code="FINGERPRINT_DRIFT",
        )
    return {
        "status": "verified",
        "identity_verified": True,
        "schema_fingerprint_verified": True,
        "schema_fingerprint": fingerprint["fingerprint"],
        "schema_fingerprints": fingerprint["per_schema"],
        "fingerprint_algorithm": FINGERPRINT_ALGORITHM,
        "component_version": manifest["component_version"],
        "migration_count": applied,
        "attestation_verified": True,
        "runtime_roles_verified": sorted(declared),
        "reader_version": reader_version,
        "writer_version": writer_version,
    }


async def plan_release(
    target: dict[str, str], release: Release, *, reader_version: str, writer_version: str
) -> dict[str, Any]:
    connection = await connect(target, readonly=True)
    try:
        async with connection.transaction(isolation="repeatable_read", readonly=True):
            await check_database_identity(connection, target)
            state = await observe_state(connection)
        body = build_plan_body(
            state, target, release, reader_version=reader_version, writer_version=writer_version
        )
        return {"plan": body, "plan_digest": plan_digest(body)}
    except ContractError:
        raise
    except Exception as exc:
        raise ContractError(
            f"Read-only planning failed (SQLSTATE {sqlstate(exc)}); details withheld"
        ) from None
    finally:
        await close_quietly(connection)


async def verify_release(
    target: dict[str, str], release: Release, *, reader_version: str, writer_version: str
) -> dict[str, Any]:
    connection = await connect(target, readonly=True)
    try:
        async with connection.transaction(isolation="repeatable_read", readonly=True):
            await check_database_identity(connection, target)
            state = await observe_state(connection)
        return verify_state(
            state, target, release, reader_version=reader_version, writer_version=writer_version
        )
    except ContractError:
        raise
    except Exception as exc:
        raise ContractError(
            f"Read-only verification failed (SQLSTATE {sqlstate(exc)}); details withheld"
        ) from None
    finally:
        await close_quietly(connection)


async def apply_release(
    target: dict[str, str],
    release: Release,
    *,
    confirmation: str,
    expected_plan_digest: str,
    reader_version: str,
    writer_version: str,
    statement_timeout_ms: int = 120000,
    lock_timeout_ms: int = 15000,
) -> dict[str, Any]:
    expected_confirmation = f"{target['project_ref']}:{target['installation_id']}"
    if confirmation != expected_confirmation:
        raise ContractError("Apply requires exact project-ref:installation-UUID confirmation")
    if not expected_plan_digest.startswith("sha256:"):
        raise ContractError("Apply requires --expected-plan-digest from a reviewed plan")
    connection = await connect(
        target,
        readonly=False,
        statement_timeout_ms=statement_timeout_ms,
        lock_timeout_ms=lock_timeout_ms,
    )
    stage = "lock"
    try:
        async with connection.transaction():
            await connection.execute("SELECT pg_advisory_xact_lock($1)", MIGRATION_LOCK)
            stage = "observe"
            await check_database_identity(connection, target)
            state = await observe_state(connection)
            body = build_plan_body(
                state, target, release, reader_version=reader_version, writer_version=writer_version
            )
            if body["holds"]:
                raise ContractError("; ".join(body["holds"]), code="HOLD")
            current = plan_digest(body)
            if current != expected_plan_digest:
                if body["status"] == "noop":
                    result = verify_state(
                        state,
                        target,
                        release,
                        reader_version=reader_version,
                        writer_version=writer_version,
                    )
                    result.update(
                        {"applied_migrations": [], "plan_matched": False, "outcome": "noop_replay"}
                    )
                    return result
                raise ContractError(
                    "STALE_PLAN: target state changed since the reviewed plan; re-plan",
                    code="STALE_PLAN",
                )
            await connection.execute("SELECT set_config('search_path','pg_catalog, pg_temp',true)")
            manifest = release.manifest
            applied_keys: list[str] = []
            for migration in release.migrations:
                if migration.key not in body["pending_migrations"]:
                    continue
                stage = f"migration {migration.key}"
                await connection.execute(migration.sql)
                await connection.execute(
                    "INSERT INTO mission_control.component_release "
                    "(component_version,migration_key,migration_digest,applied_at,applied_by) "
                    "VALUES ($1,$2,$3,clock_timestamp(),current_user)",
                    manifest["component_version"],
                    migration.key,
                    migration.digest,
                )
                applied_keys.append(migration.key)
            stage = "identity"
            if applied_keys and not state["identity"]:
                await connection.execute(
                    "INSERT INTO mission_control.application_installation "
                    "(installation_id,application_id,supabase_project_ref,environment,"
                    "schema_component_version,state,created_at) "
                    "VALUES ($1::uuid,$2,$3,$4,$5,'active',clock_timestamp())",
                    target["installation_id"],
                    target["application_id"],
                    target["project_ref"],
                    target["environment"],
                    manifest["component_version"],
                )
            elif applied_keys:
                await connection.execute(
                    "UPDATE mission_control.application_installation "
                    "SET schema_component_version=$1 WHERE installation_id=$2::uuid",
                    manifest["component_version"],
                    target["installation_id"],
                )
            stage = "attestation"
            attestation = _attestation(release)
            versions = {row["component_version"] for row in state["attestations"]}
            if attestation["component_version"] not in versions:
                await connection.execute(
                    "INSERT INTO mission_control.release_attestation "
                    "(component_version,manifest_digest,contract_schema_digest,schema_fingerprint,"
                    "fingerprint_algorithm,supported_reader_versions,supported_writer_versions,"
                    "attested_at,attested_by) "
                    "VALUES ($1,$2,$3,$4,$5,$6::text[],$7::text[],clock_timestamp(),current_user)",
                    attestation["component_version"],
                    attestation["manifest_digest"],
                    attestation["contract_schema_digest"],
                    attestation["schema_fingerprint"],
                    attestation["fingerprint_algorithm"],
                    attestation["supported_reader_versions"],
                    attestation["supported_writer_versions"],
                )
            stage = "final verification"
            after = await observe_state(connection)
            result = verify_state(
                after, target, release, reader_version=reader_version, writer_version=writer_version
            )
            result.update(
                {
                    "applied_migrations": applied_keys,
                    "plan_matched": True,
                    "outcome": "applied" if applied_keys else "noop",
                    "plan_digest": current,
                }
            )
            stage = "commit"
        return result
    except ContractError:
        raise
    except Exception as exc:
        if stage == "commit":
            raise ContractError(
                "Commit was not confirmed; inspect receipts read-only before retry",
                code="UNCONFIRMED",
            ) from None
        raise ContractError(
            f"Application failed at {stage} (SQLSTATE {sqlstate(exc)}); the transaction was "
            "rolled back; details withheld",
            code="APPLY_FAILED",
        ) from None
    finally:
        await close_quietly(connection)


async def inspect_target(target: dict[str, str]) -> dict[str, Any]:
    """Read-only inventory without a release (never mutates; reports, does not gate)."""
    connection = await connect(target, readonly=True, statement_timeout_ms=30000)
    try:
        async with connection.transaction(isolation="repeatable_read", readonly=True):
            await check_database_identity(connection, target)
            state = await observe_state(connection)
            rls = []
            if "mission_control" in state["owned_schemas_present"]:
                rls = [
                    dict(row)
                    for row in await connection.fetch(
                        "SELECT c.relname AS table_name, c.relrowsecurity AS rls_enabled, "
                        "c.relforcerowsecurity AS rls_forced FROM pg_class c "
                        "JOIN pg_namespace n ON n.oid=c.relnamespace "
                        "WHERE n.nspname='mission_control' AND c.relkind IN ('r','p') "
                        "ORDER BY c.relname"
                    )
                ]
            role = dict(
                await connection.fetchrow(
                    "SELECT rolsuper, rolbypassrls, rolcreaterole FROM pg_roles "
                    "WHERE rolname=current_user"
                )
            )
        identity = state["identity"]
        return {
            "status": "inspected",
            "target": public_identity(target),
            "identity_matches_target": bool(identity)
            and [
                (
                    r["installation_id"],
                    r["application_id"],
                    r["supabase_project_ref"],
                    r["environment"],
                )
                for r in identity
            ]
            == [_expected_identity(target)],
            "state": state,
            "tables": rls,
            "inspection_role": role,
            "mutations": [],
        }
    except ContractError:
        raise
    except Exception as exc:
        raise ContractError(
            f"Read-only inspection failed (SQLSTATE {sqlstate(exc)}); details withheld"
        ) from None
    finally:
        await close_quietly(connection)
