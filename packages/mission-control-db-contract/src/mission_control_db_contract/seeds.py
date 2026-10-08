"""Seed bundle validation and atomic, replayable application.

Bundle format ``mission-control-seed/v1`` (JSON Schema shipped as
``schemas/seed-bundle.v1.schema.json``; this module is the enforcing validator).
Whole input sets are validated first, including dependency closure. Then each bundle
applies in ONE transaction under an advisory lock keyed by installation:

* same (installation, seed_key, seed_version) and digest -> existing receipt (replay);
* same key/version with a different digest -> conflict;
* logical keys resolve to UUIDv7 ids in ``mission_control.seed_identity`` (allocated once);
* revoked grants are never revived; assets/decisions are append-only (no upserts);
* transaction-local ``mc.installation_id``/``mc.application_id`` (and ``mc.tenant_id`` per
  tenant-scoped record) satisfy FORCE RLS.
"""

from __future__ import annotations

import json
import os
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Any
from uuid import UUID

from . import storage
from .canonical import digest_value, read_json_object
from .deployment import observe_state, verify_state
from .errors import ContractError, sqlstate
from .integrity import SEMVER, Release
from .target import check_database_identity, close_quietly, connect, public_identity

SEED_FORMAT = "mission-control-seed/v1"
SEED_LOCK_CLASS = 0x4D435344  # "MCSD"
SEED_KEY = re.compile(r"mc(\.[a-z0-9-]+)+")
LOGICAL_KEY = re.compile(r"[a-z0-9][a-z0-9._:/@-]{0,199}")
CONSTRAINT = re.compile(r"(>=|<=|>|<|==)(\d+\.\d+\.\d+)")
KIND_ORDER = (
    "tenant",
    "actor_binding",
    "actor_grant",
    "asset_version",
    "asset_decision",
    "capability_grant",
    # FT-A2: Supabase Storage provisioning (bucket first, then its policies).
    "storage_bucket",
    "storage_policy",
)
STORAGE_KINDS = frozenset({"storage_bucket", "storage_policy"})
_STR = "str"
_BOOL = "bool"
_TS = "timestamp"
_STRLIST = "strlist"
_OBJ = "object"
_INT = "int"
# kind -> {field: (type, required)}; reference fields name the referenced kind.
FIELD_SPECS: dict[str, dict[str, tuple[str, bool]]] = {
    "tenant": {
        "external_tenant_ref": (_STR, True),
        "state": (_STR, True),
        "qualification_fixture": (_BOOL, False),
    },
    "actor_binding": {
        "tenant": ("ref:tenant", True),
        "issuer": (_STR, True),
        "subject": (_STR, True),
        "actor_ref": (_STR, True),
        "actor_kind": (_STR, True),
        "state": (_STR, True),
    },
    "actor_grant": {
        "tenant": ("ref:tenant", True),
        "actor_binding": ("ref:actor_binding", True),
        "scope": (_STR, True),
        "resource_selector": (_STR, True),
        "policy_ref": (_STR, True),
        "valid_from": (_TS, True),
        "valid_until": (_TS, False),
        "revocation_reason": (_STR, False),
    },
    "asset_version": {
        "asset_id": (_STR, True),
        "version": (_STR, True),
        "kind": (_STR, True),
        "contract": (_STR, True),
        "manifest_ref": (_STR, True),
        "manifest_digest": (_STR, True),
        "manifest": (_OBJ, True),
        "required_compatibility": (_STRLIST, True),
        "status": (_STR, True),
        # Migration 0025 agent-composition columns; omitted by older bundles.
        "host_support": (_OBJ, False),
        "secret_refs": (_STRLIST, False),
    },
    "asset_decision": {
        "asset_version": ("ref:asset_version", True),
        "decision": (_STR, True),
        "disposition": (_STR, True),
        "actor_ref": (_STR, True),
        "evidence_refs": (_STRLIST, True),
        "policy_ref": (_STR, True),
        "decided_at": (_TS, True),
    },
    "capability_grant": {
        "tenant": ("ref:tenant", True),
        "asset_version": ("ref:asset_version", True),
        "actor_selector": (_STR, True),
        "resource_selector": (_STR, True),
        "allowed_invocation_classes": (_STRLIST, True),
        "allowed_side_effect_classes": (_STRLIST, True),
        "ceilings": (_OBJ, True),
        "valid_from": (_TS, True),
        "valid_until": (_TS, False),
        "revoked": (_BOOL, False),
    },
    "storage_bucket": {
        "bucket_id": (_STR, True),
        "public": (_BOOL, True),
        "file_size_limit": (_INT, True),
    },
    "storage_policy": {
        "bucket_id": (_STR, True),
        "policy": (_STR, True),
        "command": (_STR, True),
        "capability_role": (_STR, True),
        "restrictive": (_BOOL, False),
    },
}
ENUMS = {
    ("tenant", "state"): {"active", "suspended", "retired"},
    ("actor_binding", "actor_kind"): {"human", "service", "agent"},
    ("actor_binding", "state"): {"active", "revoked"},
    ("asset_version", "status"): {"proposed", "admitted"},
    ("asset_decision", "decision"): {"admit", "revoke", "retire", "reject"},
    ("storage_policy", "command"): {"INSERT", "SELECT", "ALL"},
    ("storage_policy", "capability_role"): {"publisher", "reader", "*"},
}


def uuid7(*, unix_ms: int | None = None) -> UUID:
    """RFC 9562 UUIDv7: 48-bit Unix milliseconds, version 7, 74 random bits."""
    milliseconds = time.time_ns() // 1_000_000 if unix_ms is None else unix_ms
    if not 0 <= milliseconds < 1 << 48:
        raise ValueError("UUIDv7 timestamp out of range")
    random_bits = int.from_bytes(os.urandom(10), "big")
    value = (milliseconds << 80) | (0x7 << 76) | (((random_bits >> 62) & 0xFFF) << 64)
    value |= (0b10 << 62) | (random_bits & ((1 << 62) - 1))
    return UUID(int=value)


def _version_tuple(value: str) -> tuple[int, int, int]:
    major, minor, patch = (int(x) for x in value.split("."))
    return major, minor, patch


def satisfies(version: str, constraint: str) -> bool:
    parts = constraint.split()
    if not parts:
        return False
    current = _version_tuple(version)
    for part in parts:
        match = CONSTRAINT.fullmatch(part)
        if match is None:
            raise ContractError("Invalid component compatibility range")
        op, bound = match.group(1), _version_tuple(match.group(2))
        ok = {
            ">=": current >= bound,
            "<=": current <= bound,
            ">": current > bound,
            "<": current < bound,
            "==": current == bound,
        }[op]
        if not ok:
            return False
    return True


def _parse_ts(value: Any) -> datetime:
    if not isinstance(value, str):
        raise ValueError
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        raise ValueError
    return parsed


def validate_bundle(bundle: dict[str, Any]) -> dict[str, Any]:
    """Strict structural validation; returns the bundle with its seed digest attached."""
    allowed_top = {
        "format",
        "seed_key",
        "seed_version",
        "component_compatibility",
        "depends_on",
        "actor_ref",
        "description",
        "records",
    }
    if set(bundle) - allowed_top or bundle.get("format") != SEED_FORMAT:
        raise ContractError("Seed bundle has unknown fields or unsupported format")
    key, version = bundle.get("seed_key"), bundle.get("seed_version")
    if not isinstance(key, str) or not SEED_KEY.fullmatch(key):
        raise ContractError("seed_key must match mc.<segment>[.<segment>...]")
    if not isinstance(version, str) or not SEMVER.fullmatch(version):
        raise ContractError("seed_version must be semantic x.y.z")
    compatibility = bundle.get("component_compatibility")
    if not isinstance(compatibility, str):
        raise ContractError("component_compatibility range is required")
    satisfies("0.0.0", compatibility)  # syntax check
    actor = bundle.get("actor_ref")
    if not isinstance(actor, str) or not actor:
        raise ContractError("Seed actor_ref is required")
    depends = bundle.get("depends_on")
    if not isinstance(depends, list):
        raise ContractError("depends_on must be a list")
    for dependency in depends:
        if (
            not isinstance(dependency, dict)
            or set(dependency) != {"seed_key", "seed_version"}
            or not SEED_KEY.fullmatch(str(dependency["seed_key"]))
            or not SEMVER.fullmatch(str(dependency["seed_version"]))
        ):
            raise ContractError("depends_on entries need exactly seed_key and seed_version")
    records = bundle.get("records")
    if not isinstance(records, list) or not records:
        raise ContractError("Seed bundle requires at least one record")
    seen: set[tuple[str, str]] = set()
    for record in records:
        if not isinstance(record, dict) or set(record) != {"kind", "logical_key", "fields"}:
            raise ContractError("Seed records need exactly kind, logical_key and fields")
        kind, logical = record["kind"], record["logical_key"]
        if kind not in FIELD_SPECS:
            raise ContractError(f"Unsupported seed record kind: {kind}")
        if not isinstance(logical, str) or not LOGICAL_KEY.fullmatch(logical):
            raise ContractError("Invalid logical_key")
        if (kind, logical) in seen:
            raise ContractError(f"Duplicate seed record {kind}:{logical}")
        seen.add((kind, logical))
        fields = record["fields"]
        spec = FIELD_SPECS[kind]
        if not isinstance(fields, dict) or set(fields) - set(spec):
            raise ContractError(f"Unknown fields on {kind}:{logical}")
        for name, (kind_of, required) in spec.items():
            if name not in fields:
                if required:
                    raise ContractError(f"Missing field {name} on {kind}:{logical}")
                continue
            value = fields[name]
            try:
                if kind_of == _STR or kind_of.startswith("ref:"):
                    if not isinstance(value, str) or not value:
                        raise ValueError
                elif kind_of == _BOOL:
                    if not isinstance(value, bool):
                        raise ValueError
                elif kind_of == _TS:
                    _parse_ts(value)
                elif kind_of == _STRLIST:
                    if not isinstance(value, list) or any(not isinstance(v, str) for v in value):
                        raise ValueError
                elif kind_of == _OBJ and not isinstance(value, dict):
                    raise ValueError
                elif kind_of == _INT and (isinstance(value, bool) or not isinstance(value, int)):
                    raise ValueError
            except ValueError:
                raise ContractError(f"Invalid {name} on {kind}:{logical}") from None
            allowed = ENUMS.get((kind, name))
            if allowed is not None and value not in allowed:
                raise ContractError(f"Invalid {name} value on {kind}:{logical}")
        if kind == "asset_version" and not re.fullmatch(
            r"sha256:[0-9a-f]{64}", fields["manifest_digest"]
        ):
            raise ContractError(f"Invalid manifest_digest on {kind}:{logical}")
    checked = dict(bundle)
    checked["seed_digest"] = digest_value(bundle)
    return checked


def load_bundles(paths: list[Path]) -> list[dict[str, Any]]:
    files: list[Path] = []
    for path in paths:
        files.extend(sorted(path.rglob("*.json")) if path.is_dir() else [path])
    if not files:
        raise ContractError("No seed bundle files supplied")
    return [validate_bundle(read_json_object(path)) for path in files]


def order_bundles(
    bundles: list[dict[str, Any]], already_applied: set[tuple[str, str]]
) -> list[dict[str, Any]]:
    """Validate dependency closure and record references; return dependency order."""
    by_id: dict[tuple[str, str], dict[str, Any]] = {}
    for bundle in bundles:
        ident = (bundle["seed_key"], bundle["seed_version"])
        if ident in by_id:
            raise ContractError(f"Duplicate seed bundle {ident[0]}@{ident[1]}")
        by_id[ident] = bundle
    ordered: list[dict[str, Any]] = []
    state: dict[tuple[str, str], str] = {}

    def visit(ident: tuple[str, str]) -> None:
        if state.get(ident) == "done":
            return
        if state.get(ident) == "active":
            raise ContractError("Seed dependency cycle")
        state[ident] = "active"
        for dependency in by_id[ident]["depends_on"]:
            dep = (dependency["seed_key"], dependency["seed_version"])
            if dep in by_id:
                visit(dep)
            elif dep not in already_applied:
                raise ContractError(f"Dependency {dep[0]}@{dep[1]} is neither supplied nor applied")
        state[ident] = "done"
        ordered.append(by_id[ident])

    for ident in sorted(by_id):
        visit(ident)
    # Static reference closure for supplied bundles (applied dependencies resolve in-DB).
    for bundle in ordered:
        local: set[tuple[str, str]] = set()
        closure = _closure(bundle, by_id)
        for dep in closure:
            local |= {(r["kind"], r["logical_key"]) for r in by_id[dep]["records"]}
        own = {(r["kind"], r["logical_key"]) for r in bundle["records"]}
        unresolved_deps = [dep for dep in _declared_closure(bundle, by_id) if dep not in by_id]
        for record in bundle["records"]:
            for name, (kind_of, _required) in FIELD_SPECS[record["kind"]].items():
                if kind_of.startswith("ref:") and name in record["fields"]:
                    ref = (kind_of[4:], record["fields"][name])
                    if ref not in own and ref not in local and not unresolved_deps:
                        raise ContractError(
                            f"Reference {ref[0]}:{ref[1]} is outside the dependency closure"
                        )
    return ordered


def _closure(
    bundle: dict[str, Any], by_id: dict[tuple[str, str], dict[str, Any]]
) -> set[tuple[str, str]]:
    result: set[tuple[str, str]] = set()
    stack = [(d["seed_key"], d["seed_version"]) for d in bundle["depends_on"]]
    while stack:
        dep = stack.pop()
        if dep in result or dep not in by_id:
            continue
        result.add(dep)
        stack.extend((d["seed_key"], d["seed_version"]) for d in by_id[dep]["depends_on"])
    return result


def _declared_closure(
    bundle: dict[str, Any], by_id: dict[tuple[str, str], dict[str, Any]]
) -> set[tuple[str, str]]:
    result: set[tuple[str, str]] = set()
    stack = [(d["seed_key"], d["seed_version"]) for d in bundle["depends_on"]]
    while stack:
        dep = stack.pop()
        if dep in result:
            continue
        result.add(dep)
        if dep in by_id:
            stack.extend((d["seed_key"], d["seed_version"]) for d in by_id[dep]["depends_on"])
    return result


async def _set_context(connection: Any, target: dict[str, str], tenant: UUID | None) -> None:
    await connection.execute(
        "SELECT set_config('mc.installation_id',$1,true), set_config('mc.application_id',$2,true),"
        " set_config('mc.tenant_id',$3,true)",
        target["installation_id"],
        target["application_id"],
        str(tenant) if tenant else "",
    )


async def _receipt(connection: Any, target: dict[str, str], key: str, version: str) -> Any:
    await _set_context(connection, target, None)  # installation-scoped, forced RLS
    return await connection.fetchrow(
        "SELECT seed_key, seed_version, seed_digest, component_version, record_counts::text "
        "AS record_counts, applied_by FROM mission_control.installation_seed_receipt "
        "WHERE installation_id=$1::uuid AND seed_key=$2 AND seed_version=$3",
        target["installation_id"],
        key,
        version,
    )


def _receipt_dict(row: Any) -> dict[str, Any]:
    result = dict(row)
    result["record_counts"] = json.loads(result["record_counts"])
    return result


async def _resolve(
    connection: Any,
    target: dict[str, str],
    kind: str,
    logical: str,
    *,
    allocate: bool,
    bundle: dict[str, Any] | None = None,
) -> tuple[UUID, bool]:
    existing = await connection.fetchval(
        "SELECT record_id FROM mission_control.seed_identity WHERE installation_id=$1::uuid "
        "AND record_kind=$2 AND logical_key=$3",
        target["installation_id"],
        kind,
        logical,
    )
    if existing is not None:
        return UUID(str(existing)), False
    if not allocate or bundle is None:
        raise ContractError(f"Reference {kind}:{logical} has no allocated seed identity")
    record_id = uuid7()
    await connection.execute(
        "INSERT INTO mission_control.seed_identity (installation_id,application_id,logical_key,"
        "record_kind,record_id,first_seed_key,first_seed_version,created_at) "
        "VALUES ($1::uuid,$2,$3,$4,$5::uuid,$6,$7,clock_timestamp())",
        target["installation_id"],
        target["application_id"],
        logical,
        kind,
        str(record_id),
        bundle["seed_key"],
        bundle["seed_version"],
    )
    return record_id, True


def _same(existing: dict[str, Any], wanted: dict[str, Any]) -> bool:
    for key, value in wanted.items():
        current = existing.get(key)
        if isinstance(value, dict) and isinstance(current, str):
            current = json.loads(current)
        if isinstance(value, UUID):
            value = str(value)
            current = None if current is None else str(current)
        if isinstance(current, list | tuple):
            current = list(current)
        if current != value:
            return False
    return True


async def _apply_record(
    connection: Any,
    target: dict[str, str],
    bundle: dict[str, Any],
    record: dict[str, Any],
    stats: dict[str, int],
) -> None:
    kind, logical, fields = record["kind"], record["logical_key"], record["fields"]
    actor = bundle["actor_ref"]
    await _set_context(connection, target, None)
    if kind in STORAGE_KINDS:
        await _resolve(connection, target, kind, logical, allocate=True, bundle=bundle)
        resolved = {
            key: storage.resolve_application(value, target["application_id"])
            if isinstance(value, str)
            else value
            for key, value in fields.items()
        }
        if kind == "storage_bucket":
            created = await storage.apply_bucket(connection, resolved)
        else:
            created = await storage.apply_policy(connection, resolved, target["application_id"])
        stats["created" if created else "reused"] += 1
        return
    record_id, _new = await _resolve(
        connection, target, kind, logical, allocate=True, bundle=bundle
    )
    tenant: UUID | None = None
    if "tenant" in FIELD_SPECS[kind] and kind != "tenant":
        tenant, _ = await _resolve(connection, target, "tenant", fields["tenant"], allocate=False)
    if kind == "tenant":
        tenant = record_id
    await _set_context(connection, target, tenant)
    scope = (target["installation_id"], target["application_id"])
    if kind == "tenant":
        table, id_column = "tenant", "tenant_id"
        wanted: dict[str, Any] = {
            "external_tenant_ref": fields["external_tenant_ref"],
            "state": fields["state"],
            "qualification_fixture": fields.get("qualification_fixture", False),
        }
    elif kind == "actor_binding":
        table, id_column = "actor_binding", "actor_binding_id"
        wanted = {
            "tenant_id": tenant,
            "issuer": fields["issuer"],
            "subject": fields["subject"],
            "actor_ref": fields["actor_ref"],
            "actor_kind": fields["actor_kind"],
            "state": fields["state"],
        }
    elif kind == "actor_grant":
        table, id_column = "actor_grant", "actor_grant_id"
        binding, _ = await _resolve(
            connection, target, "actor_binding", fields["actor_binding"], allocate=False
        )
        wanted = {
            "tenant_id": tenant,
            "actor_binding_id": binding,
            "scope": fields["scope"],
            "resource_selector": fields["resource_selector"],
            "policy_ref": fields["policy_ref"],
            "valid_from": _parse_ts(fields["valid_from"]),
            "valid_until": _parse_ts(fields["valid_until"]) if "valid_until" in fields else None,
        }
    elif kind == "asset_version":
        table, id_column = "asset_version", "asset_version_id"
        wanted = {
            "asset_id": fields["asset_id"],
            "version": fields["version"],
            "kind": fields["kind"],
            "contract": fields["contract"],
            "manifest_ref": fields["manifest_ref"],
            "manifest_digest": fields["manifest_digest"],
            "manifest": fields["manifest"],
            "required_compatibility": fields["required_compatibility"],
            "status": fields["status"],
        }
        for optional in ("host_support", "secret_refs"):
            if optional in fields:
                wanted[optional] = fields[optional]
    elif kind == "asset_decision":
        table, id_column = "asset_decision", "asset_decision_id"
        asset, _ = await _resolve(
            connection, target, "asset_version", fields["asset_version"], allocate=False
        )
        wanted = {
            "asset_version_id": asset,
            "decision": fields["decision"],
            "disposition": fields["disposition"],
            "actor_ref": fields["actor_ref"],
            "evidence_refs": fields["evidence_refs"],
            "policy_ref": fields["policy_ref"],
            "decided_at": _parse_ts(fields["decided_at"]),
        }
    else:  # capability_grant
        table, id_column = "capability_grant", "capability_grant_id"
        asset, _ = await _resolve(
            connection, target, "asset_version", fields["asset_version"], allocate=False
        )
        wanted = {
            "tenant_id": tenant,
            "asset_version_id": asset,
            "actor_selector": fields["actor_selector"],
            "resource_selector": fields["resource_selector"],
            "allowed_invocation_classes": fields["allowed_invocation_classes"],
            "allowed_side_effect_classes": fields["allowed_side_effect_classes"],
            "ceilings": fields["ceilings"],
            "valid_from": _parse_ts(fields["valid_from"]),
            "valid_until": _parse_ts(fields["valid_until"]) if "valid_until" in fields else None,
        }
    existing_row = await connection.fetchrow(
        f"SELECT * FROM mission_control.{table} WHERE {id_column}=$1::uuid "
        "AND installation_id=$2::uuid AND application_id=$3",
        str(record_id),
        *scope,
    )
    revoke_wanted = (kind == "actor_grant" and "revocation_reason" in fields) or (
        kind == "capability_grant" and fields.get("revoked", False)
    )
    if existing_row is not None:
        existing = dict(existing_row)
        if not _same(existing, wanted):
            raise ContractError(
                f"Seed record {kind}:{logical} differs from its admitted row; append a new "
                "logical key/version instead (no last-write-wins)",
                code="SEED_CONFLICT",
            )
        if kind in {"actor_grant", "capability_grant"}:
            revoked = existing["revoked_at"] is not None
            if revoked and not revoke_wanted:
                raise ContractError(
                    f"Revoked grant {kind}:{logical} cannot be revived", code="SEED_CONFLICT"
                )
            if revoke_wanted and not revoked:
                await _revoke(connection, kind, record_id, fields)
                stats["revoked"] += 1
                return
            if (
                revoked
                and kind == "actor_grant"
                and (existing["revocation_reason"] != fields.get("revocation_reason"))
            ):
                raise ContractError(f"Revocation of {kind}:{logical} differs", code="SEED_CONFLICT")
        stats["reused"] += 1
        return
    columns = ["installation_id", "application_id", id_column, *wanted.keys()]
    values: list[Any] = [scope[0], scope[1], str(record_id)]
    casts = ["$1::uuid", "$2", "$3::uuid"]
    for value in wanted.values():
        index = len(values) + 1
        if isinstance(value, dict):
            values.append(json.dumps(value, sort_keys=True))
            casts.append(f"${index}::jsonb")
        elif isinstance(value, UUID):
            values.append(str(value))
            casts.append(f"${index}::uuid")
        elif isinstance(value, list):
            values.append(value)
            casts.append(f"${index}::text[]")
        else:
            values.append(value)
            casts.append(f"${index}")
    extra_columns, extra_values = _extra_columns(kind, actor)
    columns += extra_columns
    casts += extra_values
    await connection.execute(
        f"INSERT INTO mission_control.{table} ({', '.join(columns)}) VALUES ({', '.join(casts)})",
        *values,
    )
    stats["created"] += 1
    if revoke_wanted:
        await _revoke(connection, kind, record_id, fields)
        stats["revoked"] += 1


def _extra_columns(kind: str, actor: str) -> tuple[list[str], list[str]]:
    quoted = "'" + actor.replace("'", "''") + "'"
    base = (["created_at", "created_by_actor_ref"], ["clock_timestamp()", quoted])
    if kind == "asset_version":
        return (
            ["version_no", "updated_at", *base[0]],
            ["1", "clock_timestamp()", *base[1]],
        )
    return base


async def _revoke(connection: Any, kind: str, record_id: UUID, fields: dict[str, Any]) -> None:
    if kind == "actor_grant":
        await connection.execute(
            "UPDATE mission_control.actor_grant SET revoked_at=clock_timestamp(), "
            "revocation_reason=$2 WHERE actor_grant_id=$1::uuid AND revoked_at IS NULL",
            str(record_id),
            fields["revocation_reason"],
        )
    else:
        await connection.execute(
            "UPDATE mission_control.capability_grant SET revoked_at=clock_timestamp() "
            "WHERE capability_grant_id=$1::uuid AND revoked_at IS NULL",
            str(record_id),
        )


async def _installation_version(connection: Any, target: dict[str, str]) -> str:
    version = await connection.fetchval(
        "SELECT schema_component_version FROM mission_control.application_installation "
        "WHERE installation_id=$1::uuid AND application_id=$2 AND state='active'",
        target["installation_id"],
        target["application_id"],
    )
    if version is None:
        raise ContractError("Target installation identity is absent; apply the release first")
    return str(version)


async def _applied_set(connection: Any, target: dict[str, str]) -> dict[tuple[str, str], str]:
    await _set_context(connection, target, None)  # installation-scoped, forced RLS
    return {
        (row["seed_key"], row["seed_version"]): row["seed_digest"]
        for row in await connection.fetch(
            "SELECT seed_key, seed_version, seed_digest FROM "
            "mission_control.installation_seed_receipt WHERE installation_id=$1::uuid",
            target["installation_id"],
        )
    }


async def seed_plan(
    target: dict[str, str],
    release: Release,
    bundles: list[dict[str, Any]],
    *,
    reader_version: str,
    writer_version: str,
) -> dict[str, Any]:
    connection = await connect(target, readonly=True)
    try:
        async with connection.transaction(isolation="repeatable_read", readonly=True):
            await check_database_identity(connection, target)
            verify_state(
                await observe_state(connection),
                target,
                release,
                reader_version=reader_version,
                writer_version=writer_version,
            )
            await _set_context(connection, target, None)
            component_version = await _installation_version(connection, target)
            applied = await _applied_set(connection, target)
        ordered = order_bundles(bundles, set(applied))
        entries = []
        for bundle in ordered:
            ident = (bundle["seed_key"], bundle["seed_version"])
            if not satisfies(component_version, bundle["component_compatibility"]):
                status = "incompatible"
            elif ident not in applied:
                status = "pending"
            elif applied[ident] == bundle["seed_digest"]:
                status = "replay"
            else:
                status = "conflict"
            entries.append(
                {
                    "seed_key": ident[0],
                    "seed_version": ident[1],
                    "seed_digest": bundle["seed_digest"],
                    "status": status,
                    "record_counts": _counts(bundle),
                }
            )
        blocked = any(e["status"] in {"conflict", "incompatible"} for e in entries)
        return {
            "status": "hold" if blocked else "planned",
            "target": public_identity(target),
            "component_version": component_version,
            "dependency_order": [f"{e['seed_key']}@{e['seed_version']}" for e in entries],
            "bundles": entries,
            "seed_plan_digest": digest_value(entries),
            "mutations": [],
        }
    except ContractError:
        raise
    except Exception as exc:
        raise ContractError(f"Seed planning failed (SQLSTATE {sqlstate(exc)})") from None
    finally:
        await close_quietly(connection)


def _counts(bundle: dict[str, Any]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for record in bundle["records"]:
        counts[record["kind"]] = counts.get(record["kind"], 0) + 1
    return dict(sorted(counts.items()))


async def apply_bundle(
    connection: Any,
    target: dict[str, str],
    release: Release,
    bundle: dict[str, Any],
    *,
    reader_version: str,
    writer_version: str,
) -> dict[str, Any]:
    """Apply ONE bundle atomically on an open connection (its own transaction)."""
    ident = (bundle["seed_key"], bundle["seed_version"])
    async with connection.transaction():
        await connection.execute(
            "SELECT pg_advisory_xact_lock($1, hashtext($2))",
            SEED_LOCK_CLASS,
            target["installation_id"],
        )
        await check_database_identity(connection, target)
        verify_state(
            await observe_state(connection),
            target,
            release,
            reader_version=reader_version,
            writer_version=writer_version,
        )
        # Seed receipts/identities are installation-scoped under forced RLS.
        await _set_context(connection, target, None)
        component_version = await _installation_version(connection, target)
        if not satisfies(component_version, bundle["component_compatibility"]):
            raise ContractError("Seed bundle is incompatible with the installed component")
        existing = await _receipt(connection, target, *ident)
        if existing is not None:
            if existing["seed_digest"] != bundle["seed_digest"]:
                raise ContractError(
                    f"Seed {ident[0]}@{ident[1]} already applied with a different digest",
                    code="SEED_CONFLICT",
                )
            return {"outcome": "replay", "receipt": _receipt_dict(existing)}
        applied = await _applied_set(connection, target)
        for dependency in bundle["depends_on"]:
            if (dependency["seed_key"], dependency["seed_version"]) not in applied:
                raise ContractError("Seed dependency has not been applied")
        if any(r["kind"] in STORAGE_KINDS for r in bundle["records"]) and not (
            await storage.storage_available(connection)
        ):
            # No Storage schema (e.g. a local disposable cluster): nothing is written and no
            # receipt is recorded, so the bundle applies later where Storage exists.
            return {"outcome": "blocked", "reason": "storage schema is absent on this target"}
        stats = {"created": 0, "reused": 0, "revoked": 0}
        records = sorted(
            bundle["records"], key=lambda r: KIND_ORDER.index(r["kind"])
        )  # stable: preserves bundle order within a kind
        for record in records:
            await _apply_record(connection, target, bundle, record, stats)
        counts = {"records": _counts(bundle), **stats}
        await connection.execute(
            "INSERT INTO mission_control.installation_seed_receipt (installation_id,application_id,"
            "seed_key,seed_version,seed_digest,component_version,record_counts,applied_at,"
            "applied_by) VALUES ($1::uuid,$2,$3,$4,$5,$6,$7::jsonb,clock_timestamp(),current_user)",
            target["installation_id"],
            target["application_id"],
            ident[0],
            ident[1],
            bundle["seed_digest"],
            component_version,
            json.dumps(counts, sort_keys=True),
        )
        receipt = await _receipt(connection, target, *ident)
        return {"outcome": "applied", "receipt": _receipt_dict(receipt)}


async def seed_apply(
    target: dict[str, str],
    release: Release,
    bundles: list[dict[str, Any]],
    *,
    confirmation: str,
    reader_version: str,
    writer_version: str,
) -> dict[str, Any]:
    if confirmation != f"{target['project_ref']}:{target['installation_id']}":
        raise ContractError("Seed apply requires exact project-ref:installation-UUID confirmation")
    connection = await connect(target, readonly=False, lock_timeout_ms=15000)
    results = []
    try:
        async with connection.transaction(isolation="repeatable_read", readonly=True):
            applied = await _applied_set(connection, target)
        ordered = order_bundles(bundles, set(applied))
        for bundle in ordered:
            try:
                result = await apply_bundle(
                    connection,
                    target,
                    release,
                    bundle,
                    reader_version=reader_version,
                    writer_version=writer_version,
                )
            except ContractError:
                raise
            except Exception as exc:
                raise ContractError(
                    f"Seed {bundle['seed_key']}@{bundle['seed_version']} failed "
                    f"(SQLSTATE {sqlstate(exc)}); its transaction rolled back",
                    code="SEED_FAILED",
                ) from None
            results.append(
                {"seed_key": bundle["seed_key"], "seed_version": bundle["seed_version"], **result}
            )
        return {"status": "seeded", "target": public_identity(target), "bundles": results}
    except ContractError as exc:
        exc.args = (f"{exc.args[0]} (completed before failure: {len(results)} bundle(s))",)
        raise
    except Exception as exc:
        raise ContractError(f"Seed application failed (SQLSTATE {sqlstate(exc)})") from None
    finally:
        await close_quietly(connection)
