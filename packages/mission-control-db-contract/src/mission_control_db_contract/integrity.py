"""Exhaustive release-pin integrity and transaction-safety checks (no database access).

The app lock (``deployments/<app>/release.lock.json``) lists every payload file of the
component release root, including ``manifest.json`` and the generated contract. Path
traversal, links, omissions, extra files and changed bytes fail closed before any
connection. Integrity is not publisher authenticity: provenance (source revision and
reviewer) must be reviewed before a lock is accepted.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath
from typing import Any

from . import COMPONENT, INSTALLER_PROTOCOL, OWNED_SCHEMAS, SOURCE_IDENTITY, SUPPORTED_APPS
from .canonical import digest_value, read_json_object
from .errors import ContractError
from .fingerprint import FINGERPRINT_ALGORITHM

LOCK_FORMAT_VERSION = 2
DIGEST = re.compile(r"sha256:[0-9a-f]{64}")
HEX64 = re.compile(r"[0-9a-f]{64}")
SEMVER = re.compile(r"(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)")
MIGRATION_KEY = re.compile(r"[0-9]{4}_[a-z0-9_]+")
ROLE_NAME = re.compile(r"mission_control_[a-z0-9_]+")

MANIFEST_FIELDS = (
    "component",
    "component_version",
    "source_identity",
    "source_revision",
    "source_inputs_digest",
    "installer_protocol",
    "schema_fingerprint_algorithm",
    "owned_schemas",
    "schema_fingerprint",
    "schema_fingerprints",
    "contract_path",
    "contract_schema_digest",
    "min_postgres_version",
    "postgres_major",
    "required_extensions",
    "ordered_migrations",
    "supported_reader_versions",
    "supported_writer_versions",
    "runtime_roles",
    "compatible_previous_fingerprints",
    "seed_compatibility",
    "runtime_descriptor",
)


@dataclass(frozen=True)
class Migration:
    key: str
    path: str
    digest: str  # "sha256:<hex>"
    sql: str = field(repr=False)


@dataclass(frozen=True)
class Release:
    lock: dict[str, Any]
    lock_digest: str
    manifest: dict[str, Any]
    manifest_digest: str
    migrations: tuple[Migration, ...]
    release_root: Path

    def summary(self) -> dict[str, Any]:
        return {
            "component": self.manifest["component"],
            "component_version": self.manifest["component_version"],
            "source_revision": self.manifest["source_revision"],
            "manifest_digest": self.manifest_digest,
            "lock_digest": self.lock_digest,
            "contract_schema_digest": self.manifest["contract_schema_digest"],
            "schema_fingerprint": self.manifest["schema_fingerprint"],
            "schema_fingerprint_algorithm": self.manifest["schema_fingerprint_algorithm"],
            "migrations": [{"key": m.key, "digest": m.digest} for m in self.migrations],
        }


def safe_member(name: str) -> bool:
    parts = PurePosixPath(name)
    return not (
        not name
        or parts.is_absolute()
        or ".." in parts.parts
        or "\\" in name
        or ":" in name
        or parts.as_posix() != name
    )


def tree_files(root: Path) -> dict[str, str]:
    """Exhaustive {posix relative path: sha256 hex} for a release tree; links reject."""
    if root.is_symlink() or root.is_junction():
        raise ContractError("Release root cannot be a link")
    resolved = root.resolve(strict=True)
    files: dict[str, str] = {}
    for entry in sorted(resolved.rglob("*")):
        if entry.is_symlink() or entry.is_junction():
            raise ContractError("Release trees cannot contain links")
        if entry.is_file():
            files[entry.relative_to(resolved).as_posix()] = hashlib.sha256(
                entry.read_bytes()
            ).hexdigest()
    return files


def validate_manifest(manifest: dict[str, Any]) -> None:
    missing = [name for name in MANIFEST_FIELDS if name not in manifest]
    if missing:
        raise ContractError(f"Incomplete release manifest: {', '.join(missing)}")
    if manifest["component"] != COMPONENT or manifest["source_identity"] != SOURCE_IDENTITY:
        raise ContractError("Unsupported component or source authority")
    if manifest["installer_protocol"] != INSTALLER_PROTOCOL:
        raise ContractError("Release has not opted into the supported installer protocol")
    if manifest["schema_fingerprint_algorithm"] != FINGERPRINT_ALGORITHM:
        raise ContractError("Unsupported release fingerprint algorithm")
    if not isinstance(manifest["component_version"], str) or not SEMVER.fullmatch(
        manifest["component_version"]
    ):
        raise ContractError("Component version must be semantic x.y.z")
    if list(manifest["owned_schemas"]) != list(OWNED_SCHEMAS):
        raise ContractError("Release must own exactly the fixed common schemas")
    for name in ("schema_fingerprint", "contract_schema_digest", "source_inputs_digest"):
        if not isinstance(manifest[name], str) or not DIGEST.fullmatch(manifest[name]):
            raise ContractError(f"Invalid digest field: {name}")
    per_schema = manifest["schema_fingerprints"]
    if not isinstance(per_schema, dict) or sorted(per_schema) != sorted(OWNED_SCHEMAS):
        raise ContractError("Per-schema fingerprints must cover both owned schemas")
    if any(not isinstance(v, str) or not DIGEST.fullmatch(v) for v in per_schema.values()):
        raise ContractError("Invalid per-schema fingerprint")
    for name in ("min_postgres_version", "postgres_major"):
        if type(manifest[name]) is not int or manifest[name] <= 0:
            raise ContractError("Missing qualified PostgreSQL version")
    if manifest["min_postgres_version"] // 10000 != manifest["postgres_major"]:
        raise ContractError("Minimum PostgreSQL version disagrees with qualified major")
    for name in ("supported_reader_versions", "supported_writer_versions", "runtime_roles"):
        values = manifest[name]
        if (
            not isinstance(values, list)
            or not values
            or any(not isinstance(v, str) or not v for v in values)
        ):
            raise ContractError(f"Malformed release list: {name}")
    if any(not ROLE_NAME.fullmatch(role) for role in manifest["runtime_roles"]):
        raise ContractError("Runtime roles must be fixed mission_control_* identifiers")
    previous = manifest["compatible_previous_fingerprints"]
    if not isinstance(previous, list) or any(
        not isinstance(v, str) or not DIGEST.fullmatch(v) for v in previous
    ):
        raise ContractError("Malformed compatible previous fingerprints")
    extensions = manifest["required_extensions"]
    if not isinstance(extensions, list):
        raise ContractError("required_extensions must be a list")
    for extension in extensions:
        if (
            not isinstance(extension, dict)
            or not isinstance(extension.get("name"), str)
            or not isinstance(extension.get("schema"), str)
        ):
            raise ContractError("required_extensions entries need name and schema")
    migrations = manifest["ordered_migrations"]
    if not isinstance(migrations, list) or not migrations:
        raise ContractError("Release manifest has no ordered migrations")
    keys = [m.get("key") if isinstance(m, dict) else None for m in migrations]
    if any(not isinstance(k, str) or not MIGRATION_KEY.fullmatch(k) for k in keys):
        raise ContractError("Invalid migration key")
    if len(set(keys)) != len(keys) or keys != sorted(keys):  # type: ignore[type-var]
        raise ContractError("Migration keys must be unique and ordered")


def verify_bundle(lock_path: Path, release_root: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    """Validate an operator-reviewed app lock against a local release tree.

    Returns (lock, manifest). Fails closed on any integrity or schema defect.
    """
    lock = read_json_object(lock_path)
    if lock.get("status") != "pinned":
        raise ContractError("COMMON_COMPONENT_UNAVAILABLE: release pin is not established")
    if (
        lock.get("format_version") != LOCK_FORMAT_VERSION
        or lock.get("source_identity") != SOURCE_IDENTITY
    ):
        raise ContractError("Unsupported component pin or source authority")
    if lock.get("component") != COMPONENT:
        raise ContractError("Only the mission_control common component is accepted")
    if lock.get("app") not in SUPPORTED_APPS:
        raise ContractError("Lock must name a supported application")
    for name in ("component_version", "source_revision", "release_uri"):
        if not isinstance(lock.get(name), str) or not lock[name].strip():
            raise ContractError(f"Missing immutable provenance: {name}")
    files = lock.get("files")
    if not isinstance(files, dict) or not files:
        raise ContractError("Release pin requires exhaustive file checksums")
    for name, checksum in files.items():
        if not isinstance(name, str) or not safe_member(name):
            raise ContractError("Unsafe release member path")
        if not isinstance(checksum, str) or not HEX64.fullmatch(checksum):
            raise ContractError("Invalid SHA-256 pin")
    actual = tree_files(release_root)
    if set(files) != set(actual):
        raise ContractError("Release payload differs from exhaustive pin")
    if any(actual[name] != files[name] for name in files):
        raise ContractError("Release checksum mismatch")
    manifest_name = lock.get("manifest_path")
    if not isinstance(manifest_name, str) or manifest_name not in files:
        raise ContractError("Release manifest is not pinned")
    try:
        manifest = json.loads((release_root / manifest_name).read_bytes())
    except ValueError:
        raise ContractError("Release manifest is invalid JSON") from None
    if not isinstance(manifest, dict):
        raise ContractError("Release manifest must be an object")
    validate_manifest(manifest)
    if manifest["component_version"] != lock["component_version"]:
        raise ContractError("Manifest component version differs from pin")
    if manifest["source_revision"] != lock["source_revision"]:
        raise ContractError("Manifest source revision differs from pin")
    contract_path = manifest["contract_path"]
    if (
        not isinstance(contract_path, str)
        or contract_path not in files
        or "sha256:" + files[contract_path] != manifest["contract_schema_digest"]
    ):
        raise ContractError("Generated contract is not bound to its declared digest")
    return lock, manifest


def transaction_safe_sql(sql: str) -> None:
    """Reject transaction control outside literals/comments/dollar-quoted bodies.

    Release SQL is trusted reviewed owner code, not a hostile-SQL sandbox; this stops
    ordinary BEGIN/COMMIT statements from committing a partial release.
    """
    for tokens in statement_tokens(sql):
        _check_statement(tokens)


def statement_tokens(sql: str) -> list[list[str]]:
    """Top-level statements as uppercase keyword-token lists (empty statements dropped)."""
    statements: list[list[str]] = []
    tokens: list[str] = []
    index = 0
    while index < len(sql):
        if sql.startswith("--", index):
            end = sql.find("\n", index)
            index = len(sql) if end < 0 else end + 1
        elif sql.startswith("/*", index):
            depth = 1
            index += 2
            while depth and index < len(sql):
                if sql.startswith("/*", index):
                    depth += 1
                    index += 2
                elif sql.startswith("*/", index):
                    depth -= 1
                    index += 2
                else:
                    index += 1
            if depth:
                raise ContractError("Unterminated SQL comment")
        elif sql[index] in "'\"":
            quote = sql[index]
            escaped = (
                quote == "'"
                and index > 0
                and sql[index - 1 : index].lower() == "e"
                and (index == 1 or not (sql[index - 2].isalnum() or sql[index - 2] == "_"))
            )
            index += 1
            while index < len(sql):
                if escaped and sql[index] == "\\":
                    index += 2
                elif sql[index] == quote:
                    index += 1
                    if index < len(sql) and sql[index] == quote:
                        index += 1
                    else:
                        break
                else:
                    index += 1
            else:
                raise ContractError("Unterminated SQL literal")
        elif match := re.match(r"\$(?:[A-Za-z_][A-Za-z0-9_]*)?\$", sql[index:]):
            delimiter = match.group()
            end = sql.find(delimiter, index + len(delimiter))
            if end < 0:
                raise ContractError("Unterminated SQL body")
            index = end + len(delimiter)
        elif sql[index] == ";":
            if tokens:
                statements.append(tokens)
            tokens = []
            index += 1
        elif match := re.match(r"[A-Za-z_][A-Za-z0-9_]*", sql[index:]):
            tokens.append(match.group().upper())
            index += len(match.group())
        else:
            index += 1
    if tokens:
        statements.append(tokens)
    return statements


def _check_statement(tokens: list[str]) -> None:
    if tokens and (
        tokens[0] in {"BEGIN", "COMMIT", "END", "ROLLBACK", "ABORT", "SAVEPOINT", "RELEASE"}
        or tokens[:2] in (["START", "TRANSACTION"], ["PREPARE", "TRANSACTION"])
        or tokens[:3] == ["SET", "SESSION", "AUTHORIZATION"]
    ):
        raise ContractError("Release SQL cannot control installer transactions")


def load_release(lock_path: Path, release_root: Path) -> Release:
    """Freeze verified release bytes in memory before any connection or mutation."""
    lock_bytes = lock_path.read_bytes()
    lock, manifest = verify_bundle(lock_path, release_root)
    if lock_path.read_bytes() != lock_bytes:
        raise ContractError("Release pin changed during validation")
    files: dict[str, str] = lock["files"]
    migrations: list[Migration] = []
    used: set[str] = set()
    for item in manifest["ordered_migrations"]:
        name = item.get("path")
        sha = item.get("sha256")
        if not isinstance(name, str) or name in used or files.get(name) != sha:
            raise ContractError("Migration path is missing, duplicated, or not pinned")
        used.add(name)
        payload = (release_root / name).read_bytes()
        if hashlib.sha256(payload).hexdigest() != sha:
            raise ContractError("Migration changed after release verification")
        sql = payload.decode("utf-8")
        transaction_safe_sql(sql)
        migrations.append(Migration(item["key"], name, "sha256:" + sha, sql))
    unlisted = {name for name in files if name.startswith("migrations/") and name not in used}
    if unlisted:
        raise ContractError("Release contains migrations absent from the ordered manifest")
    return Release(
        lock=lock,
        lock_digest=digest_value(lock),
        manifest=manifest,
        manifest_digest="sha256:" + files[lock["manifest_path"]],
        migrations=tuple(migrations),
        release_root=release_root,
    )


def resolve_release_root(lock_path: Path, explicit: Path | None) -> Path:
    if explicit is not None:
        return explicit
    lock = read_json_object(lock_path)
    relative = lock.get("release_root")
    if not isinstance(relative, str) or not relative:
        raise ContractError("Lock declares no release_root; pass --release-root")
    return (lock_path.parent / relative).resolve()
