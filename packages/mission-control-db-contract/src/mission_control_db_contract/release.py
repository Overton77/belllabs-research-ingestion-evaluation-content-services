"""Reproducible release builder and app lock writer.

``release-build`` installs the component migrations into a fresh scratch database on a
qualified disposable PostgreSQL server (loopback only), computes the
``mc-pg-catalog-v2`` fingerprint over both owned schemas, exports the generated
MC-only contract and writes ``component/manifest.json``. The migration transaction is
rolled back and the scratch database dropped. Identical inputs yield identical bytes.

``lock`` writes ``deployments/<app>/release.lock.json`` exhaustively hashing every
payload file of the release root (manifest, migrations, generated contract, spec).
The manifest never contains its own hash.
"""

from __future__ import annotations

import os
import re
import secrets
import subprocess
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit, urlunsplit

from . import COMPONENT, INSTALLER_PROTOCOL, OWNED_SCHEMAS, SOURCE_IDENTITY, SUPPORTED_APPS
from .canonical import digest_value, pretty_bytes, read_json_object, sha256_hex, write_json
from .errors import ContractError, sqlstate
from .fingerprint import (
    FINGERPRINT_ALGORITHM,
    catalog_fingerprint,
    contract_from_catalogs,
    contract_markdown,
)
from .integrity import (
    LOCK_FORMAT_VERSION,
    MIGRATION_KEY,
    SEMVER,
    load_release,
    transaction_safe_sql,
    tree_files,
    validate_manifest,
)

SPEC_NAME = "release-spec.json"
CONTRACT_PATH = "generated/contract.json"
CONTRACT_MD_PATH = "generated/contract.md"
MANIFEST_PATH = "manifest.json"
LOOPBACK = {"127.0.0.1", "localhost", "::1"}
SPEC_FIELDS = {
    "component",
    "component_version",
    "installer_protocol",
    "owned_schemas",
    "postgres_major",
    "min_postgres_version",
    "required_extensions",
    "supported_reader_versions",
    "supported_writer_versions",
    "runtime_roles",
    "compatible_previous_fingerprints",
    "seed_compatibility",
    "runtime_descriptor_path",
}
IDENTIFIER = re.compile(r"[a-z_][a-z0-9_]*")


def load_spec(component_root: Path) -> dict[str, Any]:
    spec = read_json_object(component_root / SPEC_NAME)
    if set(spec) != SPEC_FIELDS:
        raise ContractError("release-spec.json fields are incomplete or unknown")
    if spec["component"] != COMPONENT or spec["installer_protocol"] != INSTALLER_PROTOCOL:
        raise ContractError("release-spec.json names an unsupported component or protocol")
    if list(spec["owned_schemas"]) != list(OWNED_SCHEMAS):
        raise ContractError("release-spec.json must own exactly the fixed common schemas")
    if not SEMVER.fullmatch(str(spec["component_version"])):
        raise ContractError("component_version must be semantic x.y.z")
    for extension in spec["required_extensions"]:
        if not (
            isinstance(extension, dict)
            and IDENTIFIER.fullmatch(str(extension.get("name")))
            and IDENTIFIER.fullmatch(str(extension.get("schema")))
        ):
            raise ContractError("required_extensions entries need identifier name and schema")
    return spec


def migration_inputs(component_root: Path) -> list[tuple[str, str, bytes]]:
    directory = component_root / "migrations"
    result = []
    for path in sorted(directory.iterdir()):
        if path.is_symlink() or path.is_junction() or not path.is_file():
            raise ContractError("Migration directory may contain only regular .sql files")
        if path.suffix != ".sql" or not MIGRATION_KEY.fullmatch(path.stem):
            raise ContractError(f"Unexpected migration file name: {path.name}")
        payload = path.read_bytes()
        transaction_safe_sql(payload.decode("utf-8"))
        result.append((path.stem, f"migrations/{path.name}", payload))
    if not result:
        raise ContractError("Component has no migrations")
    return result


def inputs_digest(component_root: Path, spec: dict[str, Any]) -> str:
    inputs = {rel: sha256_hex(payload) for _key, rel, payload in migration_inputs(component_root)}
    inputs[SPEC_NAME] = sha256_hex((component_root / SPEC_NAME).read_bytes())
    descriptor = runtime_descriptor_ref(component_root, spec)
    if descriptor is not None:
        inputs[descriptor["path"]] = descriptor["sha256"]
    return digest_value(inputs)


def runtime_descriptor_ref(component_root: Path, spec: dict[str, Any]) -> dict[str, str] | None:
    relative = spec["runtime_descriptor_path"]
    if relative is None:
        return None
    path = component_root / str(relative)
    if not path.is_file():
        return None
    return {"path": str(relative), "sha256": sha256_hex(path.read_bytes())}


def git_head(component_root: Path) -> str | None:
    try:
        completed = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=component_root,
            capture_output=True,
            text=True,
            timeout=20,
            check=False,
        )
    except OSError:
        return None
    head = completed.stdout.strip()
    return head if completed.returncode == 0 and re.fullmatch(r"[0-9a-f]{40}", head) else None


def admin_dsn(env_name: str) -> str:
    if not re.fullmatch(r"[A-Z][A-Z0-9_]*", env_name):
        raise ContractError("--admin-dsn-env must be an environment variable NAME")
    value = os.environ.get(env_name)
    if not value:
        raise ContractError("Administrator DSN environment reference is unset")
    if urlsplit(value).hostname not in LOOPBACK:
        raise ContractError("Release builds run only on a loopback disposable PostgreSQL server")
    return value


def _database_dsn(dsn: str, database: str) -> str:
    return urlunsplit(urlsplit(dsn)._replace(path="/" + database))


async def build_catalogs(
    dsn: str, spec: dict[str, Any], migrations: list[tuple[str, str, bytes]]
) -> tuple[dict[str, Any], dict[str, dict[str, Any]], int]:
    import asyncpg

    scratch = "mcdb_release_" + secrets.token_hex(6)
    try:
        admin = await asyncpg.connect(dsn, timeout=10, command_timeout=120)
    except Exception:
        raise ContractError("Cannot connect to the disposable release server") from None
    try:
        version = int(await admin.fetchval("SHOW server_version_num"))
        if version // 10000 != spec["postgres_major"] or version < spec["min_postgres_version"]:
            raise ContractError("Release server is not the qualified PostgreSQL major")
        await admin.execute(f'CREATE DATABASE "{scratch}"')
        try:
            connection = await asyncpg.connect(
                _database_dsn(dsn, scratch), timeout=10, command_timeout=300
            )
            try:
                for extension in spec["required_extensions"]:
                    await connection.execute(
                        f'CREATE SCHEMA IF NOT EXISTS "{extension["schema"]}"; '
                        f'CREATE EXTENSION IF NOT EXISTS "{extension["name"]}" '
                        f'SCHEMA "{extension["schema"]}"'
                    )
                transaction = connection.transaction()
                await transaction.start()
                stage = "setup"
                try:
                    await connection.execute(
                        "SELECT set_config('search_path','pg_catalog, pg_temp',true)"
                    )
                    for key, _rel, payload in migrations:
                        stage = key
                        await connection.execute(payload.decode("utf-8"))
                    stage = "fingerprint"
                    document, catalogs = await catalog_fingerprint(connection, OWNED_SCHEMAS)
                except ContractError:
                    raise
                except Exception as exc:
                    raise ContractError(
                        f"Release build failed at {stage} (SQLSTATE {sqlstate(exc)})"
                    ) from None
                finally:
                    await transaction.rollback()
            finally:
                await connection.close()
        finally:
            await admin.execute(f'DROP DATABASE IF EXISTS "{scratch}" WITH (FORCE)')
    finally:
        await admin.close()
    return document, catalogs, version


async def release_build(component_root: Path, admin_dsn_env: str) -> dict[str, Any]:
    component_root = component_root.resolve(strict=True)
    spec = load_spec(component_root)
    migrations = migration_inputs(component_root)
    dsn = admin_dsn(admin_dsn_env)
    document, catalogs, server_version = await build_catalogs(dsn, spec, migrations)
    contract = contract_from_catalogs(
        catalogs, component=COMPONENT, component_version=spec["component_version"]
    )
    contract_bytes = pretty_bytes(contract)
    markdown = contract_markdown(contract, document).encode("utf-8")
    inputs = inputs_digest(component_root, spec)
    head = git_head(component_root)
    manifest = {
        "component": COMPONENT,
        "component_version": spec["component_version"],
        "source_identity": SOURCE_IDENTITY,
        "source_revision": f"git:{head or 'unavailable'}+inputs:{inputs}",
        "source_inputs_digest": inputs,
        "installer_protocol": INSTALLER_PROTOCOL,
        "schema_fingerprint_algorithm": FINGERPRINT_ALGORITHM,
        "owned_schemas": list(OWNED_SCHEMAS),
        "schema_fingerprint": document["fingerprint"],
        "schema_fingerprints": document["per_schema"],
        "contract_path": CONTRACT_PATH,
        "contract_schema_digest": "sha256:" + sha256_hex(contract_bytes),
        "min_postgres_version": spec["min_postgres_version"],
        "postgres_major": spec["postgres_major"],
        "required_extensions": spec["required_extensions"],
        "ordered_migrations": [
            {"key": key, "path": rel, "sha256": sha256_hex(payload)}
            for key, rel, payload in migrations
        ],
        "supported_reader_versions": spec["supported_reader_versions"],
        "supported_writer_versions": spec["supported_writer_versions"],
        "runtime_roles": spec["runtime_roles"],
        "compatible_previous_fingerprints": spec["compatible_previous_fingerprints"],
        "seed_compatibility": spec["seed_compatibility"],
        "runtime_descriptor": runtime_descriptor_ref(component_root, spec),
    }
    validate_manifest(manifest)
    (component_root / "generated").mkdir(exist_ok=True)
    (component_root / CONTRACT_PATH).write_bytes(contract_bytes)
    (component_root / CONTRACT_MD_PATH).write_bytes(markdown)
    (component_root / MANIFEST_PATH).write_bytes(pretty_bytes(manifest))
    return {
        "status": "built",
        "component_version": manifest["component_version"],
        "schema_fingerprint": manifest["schema_fingerprint"],
        "schema_fingerprints": manifest["schema_fingerprints"],
        "contract_schema_digest": manifest["contract_schema_digest"],
        "manifest_digest": "sha256:" + sha256_hex((component_root / MANIFEST_PATH).read_bytes()),
        "source_revision": manifest["source_revision"],
        "build_server_major": server_version // 10000,
        "migrations": [m["key"] for m in manifest["ordered_migrations"]],
        "mutations": ["scratch database created, migrated in a rolled-back transaction, dropped"],
    }


def write_lock(component_root: Path, deployments_root: Path, app: str) -> dict[str, Any]:
    if app not in SUPPORTED_APPS:
        raise ContractError("Lock app must be one of the declared supported apps")
    component_root = component_root.resolve(strict=True)
    manifest = read_json_object(component_root / MANIFEST_PATH)
    validate_manifest(manifest)
    files = tree_files(component_root)
    if "sha256:" + files.get(manifest["contract_path"], "") != manifest["contract_schema_digest"]:
        raise ContractError("Generated contract no longer matches the manifest; rebuild")
    target_dir = (deployments_root / app).resolve()
    target_dir.mkdir(parents=True, exist_ok=True)
    release_root = Path(os.path.relpath(component_root, target_dir)).as_posix()
    lock = {
        "format_version": LOCK_FORMAT_VERSION,
        "status": "pinned",
        "component": COMPONENT,
        "source_identity": SOURCE_IDENTITY,
        "source_revision": manifest["source_revision"],
        "release_uri": f"mission-control:{release_root}",
        "release_root": release_root,
        "component_version": manifest["component_version"],
        "app": app,
        "manifest_path": MANIFEST_PATH,
        "files": files,
    }
    lock_path = target_dir / "release.lock.json"
    write_json(lock_path, lock)
    release = load_release(lock_path, component_root)
    return {
        "status": "locked",
        "lock_path": lock_path.as_posix(),
        "app": app,
        "lock_digest": release.lock_digest,
        "manifest_digest": release.manifest_digest,
        "file_count": len(files),
    }
