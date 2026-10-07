"""Runtime (vendor saver/store) setup phase, separate from the common DDL transaction.

Executes the runtime-persistence lane's ordered descriptor
(``runtime/descriptor.json``, ``descriptor_schema: mission-control.runtime-descriptor/1``):
inline step SQL whose exact sha256 is verified, boolean SQL probes (``precheck``,
``verify``, ``verify_recorded``, ``final_verify``) and optional vendor ledger rows
(``records_version``). Steps run on ONE dedicated session connection holding the
descriptor's session advisory lock; only that connection gets the private
``search_path``. ``concurrent`` steps (``CREATE INDEX CONCURRENTLY``) run outside any
transaction; their ledger row is written afterwards in its own transaction.

Completion is always derived from catalog probes, never from a progress table: a step
is complete when every ``verify`` and ``verify_recorded`` probe holds. Progress is
written to a per-run JSON receipt file. A failed precheck (for example an invalid
index left by an interrupted concurrent build) holds the phase; nothing is blindly
retried. Supported resume: a concurrent index that is valid but not yet recorded is
recorded (the vendor statement is ``IF NOT EXISTS``). No extra schema is invented.
"""

from __future__ import annotations

import hashlib
import re
import secrets
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from . import RUNTIME_SCHEMA
from .canonical import read_json_object, sha256_hex, write_json
from .deployment import observe_state, verify_state
from .errors import ContractError, sqlstate
from .integrity import Release, statement_tokens, transaction_safe_sql
from .target import check_database_identity, close_quietly, connect, public_identity

DESCRIPTOR_SCHEMA = "mission-control.runtime-descriptor/1"
RECEIPT_FORMAT = "mission-control-runtime-receipt/v1"
STEP_FIELDS = {
    "id",
    "kind",
    "transactional",
    "concurrent",
    "sql",
    "sha256",
    "records_version",
    "precheck",
    "verify",
    "verify_recorded",
}
STEP_ID = re.compile(r"[a-z][a-z0-9_]*(\.[a-z0-9_]+)+")
LOCK_SQL = re.compile(
    r"SELECT pg_catalog\.pg_advisory_(un)?lock\(pg_catalog\.hashtextextended\('[a-z0-9_.]+', 0\)\)"
)
LOCK_TIMEOUT = re.compile(r"[0-9]{1,4}(ms|s)")
SEARCH_PATH = re.compile(r"[a-z_][a-z0-9_]*(, ?[a-z_][a-z0-9_]*)*")


def _probe_list(value: Any, name: str) -> list[str]:
    if not isinstance(value, list) or any(
        not isinstance(q, str) or not q.lstrip().upper().startswith("SELECT ") for q in value
    ):
        raise ContractError(f"Runtime descriptor {name} must be a list of SELECT probes")
    for query in value:
        if len(statement_tokens(query)) != 1:
            raise ContractError(f"Runtime descriptor {name} probes must be single statements")
    return list(value)


def load_descriptor(path: Path) -> dict[str, Any]:
    """Validate the descriptor; verify each step's exact SQL sha256."""
    payload = path.read_bytes()
    descriptor = read_json_object(path)
    if descriptor.get("descriptor_schema") != DESCRIPTOR_SCHEMA:
        raise ContractError("Unsupported runtime descriptor schema")
    if descriptor.get("schema") != RUNTIME_SCHEMA:
        raise ContractError(f"Runtime descriptor must target the fixed {RUNTIME_SCHEMA} schema")
    pins = descriptor.get("pins")
    if not isinstance(pins, dict) or not pins:
        raise ContractError("Runtime descriptor must pin vendor package versions")
    session = descriptor.get("session")
    if not isinstance(session, dict):
        raise ContractError("Runtime descriptor needs a session section")
    search_path = session.get("search_path")
    if not isinstance(search_path, str) or not SEARCH_PATH.fullmatch(search_path):
        raise ContractError("Runtime session search_path is malformed")
    parts = [p.strip() for p in search_path.split(",")]
    if (
        parts[0] != RUNTIME_SCHEMA
        or parts[-1] != "pg_temp"
        or set(parts)
        - {
            RUNTIME_SCHEMA,
            "pg_catalog",
            "pg_temp",
        }
    ):
        raise ContractError(
            "Runtime search_path must start with the runtime schema and end with pg_temp"
        )
    for name in ("advisory_lock_sql", "advisory_unlock_sql"):
        if not isinstance(session.get(name), str) or not LOCK_SQL.fullmatch(session[name]):
            raise ContractError(f"Runtime session {name} is not a session advisory lock call")
    if not isinstance(session.get("lock_timeout"), str) or not LOCK_TIMEOUT.fullmatch(
        session["lock_timeout"]
    ):
        raise ContractError("Runtime session lock_timeout is malformed")
    steps = descriptor.get("steps")
    if not isinstance(steps, list) or not steps:
        raise ContractError("Runtime descriptor needs ordered steps")
    seen: set[str] = set()
    for step in steps:
        if not isinstance(step, dict) or set(step) != STEP_FIELDS:
            raise ContractError("Runtime step fields are incomplete or unknown")
        sid = step["id"]
        if not isinstance(sid, str) or not STEP_ID.fullmatch(sid) or sid in seen:
            raise ContractError("Runtime step ids must be unique dotted identifiers")
        seen.add(sid)
        if step["kind"] not in {"sql", "vendor"}:
            raise ContractError(f"Runtime step {sid} has an unknown kind")
        if not isinstance(step["sql"], str) or not isinstance(step["sha256"], str):
            raise ContractError(f"Runtime step {sid} needs sql and sha256")
        if hashlib.sha256(step["sql"].encode("utf-8")).hexdigest() != step["sha256"]:
            raise ContractError(f"Runtime step {sid} SQL checksum mismatch")
        transaction_safe_sql(step["sql"])
        if not isinstance(step["concurrent"], bool) or not isinstance(step["transactional"], bool):
            raise ContractError(f"Runtime step {sid} must declare transactional/concurrent")
        if step["concurrent"] == step["transactional"]:
            raise ContractError(f"Runtime step {sid}: concurrent steps are non-transactional")
        if step["concurrent"]:
            statements = statement_tokens(step["sql"])
            if len(statements) != 1 or "CONCURRENTLY" not in statements[0]:
                raise ContractError(f"Concurrent step {sid} must be one CONCURRENTLY statement")
        record = step["records_version"]
        if record is not None:
            if (
                not isinstance(record, dict)
                or set(record) != {"table", "v", "sql"}
                or type(record["v"]) is not int
                or record["sql"]
                != f"INSERT INTO {RUNTIME_SCHEMA}.{record['table']} (v) VALUES ({record['v']})"
            ):
                raise ContractError(f"Runtime step {sid} records_version is malformed")
        for name in ("precheck", "verify", "verify_recorded"):
            step[name] = _probe_list(step[name], f"{sid}.{name}")
        if record is not None and not step["verify_recorded"]:
            raise ContractError(f"Runtime step {sid} records a version without proof")
        if not step["verify"] and not step["verify_recorded"]:
            raise ContractError(f"Runtime step {sid} declares no catalog proof of completion")
    descriptor["final_verify"] = _probe_list(descriptor.get("final_verify"), "final_verify")
    descriptor["descriptor_sha256"] = sha256_hex(payload)
    return descriptor


def bind_descriptor(release: Release, descriptor: dict[str, Any], environment: str) -> bool:
    reference = release.manifest.get("runtime_descriptor")
    if reference is None:
        if environment != "disposable":
            raise ContractError(
                "Release manifest binds no runtime descriptor; only disposable targets may "
                "use an unbound descriptor"
            )
        return False
    if reference.get("sha256") != descriptor["descriptor_sha256"]:
        raise ContractError("Runtime descriptor differs from the one bound in the release")
    return True


async def _probe(connection: Any, query: str) -> bool:
    """Evaluate one boolean probe; an erroring probe (missing object) is false."""
    try:
        async with connection.transaction():
            return await connection.fetchval(query) is True  # no row/NULL/false fail
    except Exception:
        return False


async def _all(connection: Any, queries: list[str]) -> bool:
    for query in queries:
        if not await _probe(connection, query):
            return False
    return True


async def _invalid_indexes(connection: Any) -> list[str]:
    return [
        row["relname"]
        for row in await connection.fetch(
            "SELECT c.relname FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid "
            "JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=$1 "
            "AND NOT (i.indisvalid AND i.indisready) ORDER BY 1",
            RUNTIME_SCHEMA,
        )
    ]


async def step_status(connection: Any, step: dict[str, Any]) -> str:
    verified = await _all(connection, step["verify"])
    recorded = await _all(connection, step["verify_recorded"])
    if verified and recorded:
        return "complete"
    if step["records_version"] is not None and recorded and not verified:
        return "inconsistent"
    if step["concurrent"] and verified and not recorded:
        return "built_unrecorded"
    return "pending"


async def inspect_steps(connection: Any, descriptor: dict[str, Any]) -> dict[str, Any]:
    schema_exists = bool(
        await connection.fetchval("SELECT to_regnamespace($1) IS NOT NULL", RUNTIME_SCHEMA)
    )
    steps = []
    for step in descriptor["steps"]:
        steps.append(
            {
                "id": step["id"],
                "sha256": step["sha256"],
                "concurrent": step["concurrent"],
                "status": await step_status(connection, step),
            }
        )
    invalid = await _invalid_indexes(connection) if schema_exists else []
    holds = []
    if invalid:
        holds.append(
            f"Invalid runtime index(es) {', '.join(invalid)}; reconcile manually "
            "(never blindly retried)"
        )
    statuses = [s["status"] for s in steps]
    if schema_exists and statuses[0] != "complete":
        holds.append("Runtime schema exists without recognized descriptor progress")
    for entry in steps:
        if entry["status"] == "inconsistent":
            holds.append(f"Step {entry['id']} is recorded but its objects do not verify")
    first_open = next((i for i, s in enumerate(statuses) if s != "complete"), len(statuses))
    for entry in steps[first_open + 1 :]:
        if entry["status"] == "complete":
            holds.append(f"Step {entry['id']} is complete after an incomplete predecessor")
    complete = all(s == "complete" for s in statuses)
    final = await _all(connection, descriptor["final_verify"]) if complete else False
    if complete and not final:
        holds.append("All steps verify but final_verify fails (unexpected runtime objects)")
    return {
        "schema": RUNTIME_SCHEMA,
        "schema_exists": schema_exists,
        "invalid_indexes": invalid,
        "steps": steps,
        "holds": holds,
        "complete": complete and final,
    }


async def _verified_common(
    connection: Any,
    target: dict[str, str],
    release: Release,
    reader_version: str,
    writer_version: str,
) -> None:
    async with connection.transaction(isolation="repeatable_read", readonly=True):
        await check_database_identity(connection, target)
        verify_state(
            await observe_state(connection),
            target,
            release,
            reader_version=reader_version,
            writer_version=writer_version,
        )


async def _set_session_path(connection: Any, descriptor: dict[str, Any]) -> None:
    await connection.execute(
        "SELECT set_config('search_path', $1, false)", descriptor["session"]["search_path"]
    )


async def runtime_plan(
    target: dict[str, str],
    release: Release,
    descriptor: dict[str, Any],
    *,
    reader_version: str,
    writer_version: str,
) -> dict[str, Any]:
    bound = bind_descriptor(release, descriptor, target["environment"])
    connection = await connect(target, readonly=True)
    try:
        await _verified_common(connection, target, release, reader_version, writer_version)
        await _set_session_path(connection, descriptor)
        inspection = await inspect_steps(connection, descriptor)
        status = (
            "hold" if inspection["holds"] else ("complete" if inspection["complete"] else "pending")
        )
        return {
            "status": status,
            "target": public_identity(target),
            "descriptor_sha256": descriptor["descriptor_sha256"],
            "descriptor_bound_to_release": bound,
            "pins": descriptor["pins"],
            "inspection": inspection,
            "mutations": [
                f"execute step {s['id']}"
                for s in inspection["steps"]
                if s["status"] in {"pending", "built_unrecorded"}
            ],
        }
    except ContractError:
        raise
    except Exception as exc:
        raise ContractError(f"Runtime planning failed (SQLSTATE {sqlstate(exc)})") from None
    finally:
        await close_quietly(connection)


def _now() -> str:
    return datetime.now(UTC).isoformat()


async def _run_step(connection: Any, step: dict[str, Any], entry: dict[str, Any]) -> None:
    record = step["records_version"]
    if step["concurrent"]:
        if entry["observed"] != "built_unrecorded":
            await connection.execute(step["sql"])
        if not await _all(connection, step["verify"]):
            # Never record an invalid/not-ready index; the operator reconciles it.
            raise ContractError(f"Concurrent step {step['id']} did not produce a valid index")
        async with connection.transaction():
            if record is not None:
                await connection.execute(record["sql"])
            for query in step["verify_recorded"]:
                if await connection.fetchval(query) is not True:
                    raise ContractError(f"Runtime step {step['id']} ledger did not verify")
        return
    async with connection.transaction():
        # Executor contract: sql, every verify, THEN the ledger row, then verify_recorded;
        # any failure rolls the whole step back.
        await connection.execute(step["sql"])
        for query in step["verify"]:
            if await connection.fetchval(query) is not True:
                raise ContractError(f"Runtime step {step['id']} did not verify; rolled back")
        if record is not None:
            await connection.execute(record["sql"])
        for query in step["verify_recorded"]:
            if await connection.fetchval(query) is not True:
                raise ContractError(f"Runtime step {step['id']} ledger did not verify; rolled back")


async def runtime_apply(
    target: dict[str, str],
    release: Release,
    descriptor: dict[str, Any],
    *,
    confirmation: str,
    receipt_path: Path,
    reader_version: str,
    writer_version: str,
    statement_timeout_ms: int = 300000,
) -> dict[str, Any]:
    if confirmation != f"{target['project_ref']}:{target['installation_id']}":
        raise ContractError(
            "Runtime apply requires exact project-ref:installation-UUID confirmation"
        )
    bound = bind_descriptor(release, descriptor, target["environment"])
    receipt: dict[str, Any] = {
        "format": RECEIPT_FORMAT,
        "run_id": secrets.token_hex(8),
        "target": public_identity(target),
        "descriptor_sha256": descriptor["descriptor_sha256"],
        "descriptor_bound_to_release": bound,
        "pins": descriptor["pins"],
        "started_at": _now(),
        "steps": [],
        "status": "running",
    }
    write_json(receipt_path, receipt)
    connection = await connect(
        target, readonly=False, statement_timeout_ms=statement_timeout_ms, lock_timeout_ms=15000
    )
    locked = False
    try:
        await _verified_common(connection, target, release, reader_version, writer_version)
        await connection.execute(
            "SELECT set_config('lock_timeout', $1, false)", descriptor["session"]["lock_timeout"]
        )
        await connection.execute(descriptor["session"]["advisory_lock_sql"])
        locked = True
        await _set_session_path(connection, descriptor)
        before = await inspect_steps(connection, descriptor)
        if before["holds"]:
            raise ContractError("; ".join(before["holds"]), code="RUNTIME_HOLD")
        for step in descriptor["steps"]:
            observed = await step_status(connection, step)
            entry: dict[str, Any] = {
                "id": step["id"],
                "sha256": step["sha256"],
                "concurrent": step["concurrent"],
                "observed": observed,
            }
            receipt["steps"].append(entry)
            if observed == "complete":
                entry["status"] = "already_complete"
                write_json(receipt_path, receipt)
                continue
            if observed == "inconsistent":
                entry["status"] = "held"
                raise ContractError(
                    f"Runtime step {step['id']} is inconsistent", code="RUNTIME_HOLD"
                )
            failing = [q for q in step["precheck"] if not await _probe(connection, q)]
            if failing and observed != "built_unrecorded":
                entry.update({"status": "held", "failed_prechecks": len(failing)})
                raise ContractError(
                    f"Runtime step {step['id']} precheck failed; reconcile before resuming "
                    "(never blindly retried)",
                    code="RUNTIME_HOLD",
                )
            entry.update({"status": "started", "started_at": _now()})
            write_json(receipt_path, receipt)
            try:
                await _run_step(connection, step, entry)
            except ContractError:
                entry.update({"status": "failed", "failed_at": _now()})
                raise
            except Exception as exc:
                entry.update({"status": "failed", "sqlstate": sqlstate(exc), "failed_at": _now()})
                entry["invalid_indexes_after"] = await _invalid_indexes(connection)
                raise ContractError(
                    f"Runtime step {step['id']} failed (SQLSTATE {sqlstate(exc)}); inspect the "
                    "receipt; invalid indexes are never retried automatically",
                    code="RUNTIME_STEP_FAILED",
                ) from None
            if await step_status(connection, step) != "complete":
                entry["status"] = "unverified"
                raise ContractError(f"Runtime step {step['id']} ran but does not verify")
            entry.update({"status": "completed", "finished_at": _now()})
            write_json(receipt_path, receipt)
        final = await inspect_steps(connection, descriptor)
        if final["holds"] or not final["complete"]:
            raise ContractError("; ".join(final["holds"]) or "Runtime phase is incomplete")
        receipt.update({"status": "complete", "finished_at": _now(), "final": final})
        return receipt
    except ContractError as exc:
        receipt.update({"status": "failed", "error": str(exc), "finished_at": _now()})
        raise
    except Exception as exc:
        receipt.update({"status": "failed", "error": f"SQLSTATE {sqlstate(exc)}"})
        raise ContractError(f"Runtime phase failed (SQLSTATE {sqlstate(exc)})") from None
    finally:
        write_json(receipt_path, receipt)
        try:
            if locked:
                await connection.execute(descriptor["session"]["advisory_unlock_sql"])
        finally:
            await close_quietly(connection)
