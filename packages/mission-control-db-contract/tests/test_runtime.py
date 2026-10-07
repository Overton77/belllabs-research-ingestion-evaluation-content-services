"""Runtime phase: the pinned vendor descriptor end to end, plus interruption handling."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import pytest
from conftest import VERSIONS, build_and_lock, copy_component, run, write_target
from test_install import install

from mission_control_db_contract.errors import ContractError
from mission_control_db_contract.runtime import load_descriptor, runtime_apply, runtime_plan
from mission_control_db_contract.target import load_target

SCHEMA = "mission_control_runtime"


def _confirm(target: dict[str, str]) -> str:
    return f"{target['project_ref']}:{target['installation_id']}"


def test_pinned_descriptor_runtime_phase_and_rerun(make_db, release, tmp_path, monkeypatch):
    descriptor_path = release.release_root.parent / "runtime" / "descriptor.json"
    descriptor = load_descriptor(descriptor_path)
    db = make_db("B")
    target = load_target(write_target(tmp_path, db, monkeypatch=monkeypatch))
    install(target, release)
    plan = run(runtime_plan(target, release, descriptor, **VERSIONS))
    assert plan["status"] == "pending" and plan["descriptor_bound_to_release"] is True
    receipt_path = tmp_path / "runtime-receipts.json"
    receipt = run(
        runtime_apply(
            target,
            release,
            descriptor,
            confirmation=_confirm(target),
            receipt_path=receipt_path,
            **VERSIONS,
        )
    )
    assert receipt["status"] == "complete"
    assert [s["status"] for s in receipt["steps"]] == ["completed"] * len(descriptor["steps"])
    assert json.loads(receipt_path.read_text(encoding="utf-8"))["status"] == "complete"
    concurrent = [s["id"] for s in descriptor["steps"] if s["concurrent"]]
    assert concurrent, "pinned vendor setup includes CREATE INDEX CONCURRENTLY steps"
    assert run(
        db.fetchval(
            "SELECT bool_and(i.indisvalid) FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid "
            "JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=$1",
            SCHEMA,
        )
    )
    versions = run(
        db.fetchval(f"SELECT array_agg(v ORDER BY v) FROM {SCHEMA}.checkpoint_migrations")
    )
    assert versions == list(range(len(versions)))
    role = run(
        db.fetchval(
            "SELECT row_to_json(r)::text FROM (SELECT rolcanlogin, rolsuper, rolbypassrls FROM "
            "pg_roles WHERE rolname='mission_control_checkpointer') r"
        )
    )
    assert json.loads(role) == {"rolcanlogin": False, "rolsuper": False, "rolbypassrls": False}
    # Checkpointer has no access to business tables.
    assert not run(
        db.fetchval(
            "SELECT has_schema_privilege('mission_control_checkpointer','mission_control','USAGE')"
        )
    )
    again = run(
        runtime_apply(
            target,
            release,
            descriptor,
            confirmation=_confirm(target),
            receipt_path=tmp_path / "rerun.json",
            **VERSIONS,
        )
    )
    assert {s["status"] for s in again["steps"]} == {"already_complete"}
    assert run(runtime_plan(target, release, descriptor, **VERSIONS))["status"] == "complete"
    # A tampered step checksum is rejected before any connection.
    data = json.loads(descriptor_path.read_text(encoding="utf-8"))
    data["steps"][1]["sql"] += "\n-- tamper"
    tampered = tmp_path / "descriptor.json"
    tampered.write_text(json.dumps(data), encoding="utf-8")
    with pytest.raises(ContractError, match="checksum mismatch"):
        load_descriptor(tampered)


def _step(
    step_id: str,
    sql: str,
    *,
    concurrent: bool = False,
    version: int | None = None,
    verify: list[str],
    precheck: list[str] | None = None,
) -> dict[str, Any]:
    record = None
    recorded: list[str] = []
    if version is not None:
        record = {
            "table": "ledger",
            "v": version,
            "sql": f"INSERT INTO {SCHEMA}.ledger (v) VALUES ({version})",
        }
        recorded = [f"SELECT EXISTS (SELECT 1 FROM {SCHEMA}.ledger WHERE v = {version})"]
    return {
        "id": step_id,
        "kind": "vendor" if version is not None else "sql",
        "transactional": not concurrent,
        "concurrent": concurrent,
        "sql": sql,
        "sha256": hashlib.sha256(sql.encode("utf-8")).hexdigest(),
        "records_version": record,
        "precheck": precheck or [],
        "verify": verify,
        "verify_recorded": recorded,
    }


INDEX_VALID = (
    "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_index i JOIN pg_catalog.pg_class c ON "
    "c.oid = i.indexrelid JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE "
    f"n.nspname = '{SCHEMA}' AND c.relname = 'items_k_idx' AND i.indisvalid AND i.indisready)"
)
NO_INVALID = (
    "SELECT NOT EXISTS (SELECT 1 FROM pg_catalog.pg_index i JOIN pg_catalog.pg_class c ON "
    "c.oid = i.indexrelid JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE "
    f"n.nspname = '{SCHEMA}' AND c.relname = 'items_k_idx' AND NOT (i.indisvalid AND i.indisready))"
)


def fixture_descriptor(directory: Path) -> Path:
    """FIXTURE ONLY: a minimal descriptor shaped like the pinned vendor descriptor."""
    descriptor = {
        "descriptor_schema": "mission-control.runtime-descriptor/1",
        "component": "mission_control_runtime",
        "schema": SCHEMA,
        "role": "none",
        "pins": {"fixture": "0"},
        "session": {
            "search_path": f"{SCHEMA}, pg_temp",
            "advisory_lock_sql": "SELECT pg_catalog.pg_advisory_lock(pg_catalog."
            "hashtextextended('mission_control_runtime.provision', 0))",
            "advisory_unlock_sql": "SELECT pg_catalog.pg_advisory_unlock(pg_catalog."
            "hashtextextended('mission_control_runtime.provision', 0))",
            "lock_timeout": "5s",
        },
        "steps": [
            _step(
                "prerequisite.schema",
                f"CREATE SCHEMA {SCHEMA};",
                verify=[f"SELECT to_regnamespace('{SCHEMA}') IS NOT NULL"],
                precheck=[f"SELECT to_regnamespace('{SCHEMA}') IS NULL"],
            ),
            _step(
                "fixture.v0",
                "CREATE TABLE IF NOT EXISTS ledger (v integer PRIMARY KEY);\n"
                "CREATE TABLE IF NOT EXISTS items (k text);",
                version=0,
                verify=[f"SELECT to_regclass('{SCHEMA}.items') IS NOT NULL"],
            ),
            _step(
                "fixture.v1",
                "CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS items_k_idx ON items (k);",
                concurrent=True,
                version=1,
                verify=[INDEX_VALID],
                precheck=[NO_INVALID],
            ),
        ],
        "final_verify": [f"SELECT to_regclass('{SCHEMA}.items') IS NOT NULL"],
    }
    path = directory / "fixture-descriptor.json"
    path.write_text(json.dumps(descriptor, indent=2), encoding="utf-8")
    return path


@pytest.fixture
def unbound_release(tmp_path):
    root = copy_component(tmp_path / "unbound", with_descriptor=False)
    return build_and_lock(tmp_path / "unbound", root)


def _prepare(db, *, duplicates: bool, index: bool) -> None:
    statements = [
        f"CREATE SCHEMA {SCHEMA}",
        f"CREATE TABLE {SCHEMA}.ledger (v integer PRIMARY KEY)",
        f"CREATE TABLE {SCHEMA}.items (k text)",
        f"INSERT INTO {SCHEMA}.ledger VALUES (0)",
    ]
    if duplicates:
        statements.append(f"INSERT INTO {SCHEMA}.items VALUES ('dup'), ('dup')")
    if index:
        statements.append(f"CREATE UNIQUE INDEX items_k_idx ON {SCHEMA}.items (k)")
    for statement in statements:
        run(db.execute(statement))


def test_interrupted_concurrent_index_is_reported_never_retried(
    make_db, unbound_release, tmp_path, monkeypatch
):
    descriptor = load_descriptor(fixture_descriptor(tmp_path))
    db = make_db("A")
    target = load_target(write_target(tmp_path, db, monkeypatch=monkeypatch))
    install(target, unbound_release)
    _prepare(db, duplicates=True, index=False)
    receipt_path = tmp_path / "receipt.json"
    with pytest.raises(ContractError, match="fixture.v1 failed .*23505") as error:
        run(
            runtime_apply(
                target,
                unbound_release,
                descriptor,
                confirmation=_confirm(target),
                receipt_path=receipt_path,
                **VERSIONS,
            )
        )
    assert error.value.code == "RUNTIME_STEP_FAILED"
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    failed = receipt["steps"][-1]
    assert failed["id"] == "fixture.v1" and failed["status"] == "failed"
    assert failed["invalid_indexes_after"] == ["items_k_idx"]
    oid = run(db.fetchval(f"SELECT '{SCHEMA}.items_k_idx'::regclass::oid"))
    plan = run(runtime_plan(target, unbound_release, descriptor, **VERSIONS))
    assert plan["status"] == "hold" and plan["inspection"]["invalid_indexes"] == ["items_k_idx"]
    with pytest.raises(ContractError, match="never blindly retried") as again:
        run(
            runtime_apply(
                target,
                unbound_release,
                descriptor,
                confirmation=_confirm(target),
                receipt_path=tmp_path / "again.json",
                **VERSIONS,
            )
        )
    assert again.value.code == "RUNTIME_HOLD"
    assert run(db.fetchval(f"SELECT '{SCHEMA}.items_k_idx'::regclass::oid")) == oid
    assert not run(db.fetchval(f"SELECT EXISTS (SELECT 1 FROM {SCHEMA}.ledger WHERE v=1)"))


def test_built_but_unrecorded_concurrent_step_resumes(
    make_db, unbound_release, tmp_path, monkeypatch
):
    descriptor = load_descriptor(fixture_descriptor(tmp_path))
    db = make_db("A")
    target = load_target(write_target(tmp_path, db, monkeypatch=monkeypatch))
    install(target, unbound_release)
    _prepare(db, duplicates=False, index=True)
    plan = run(runtime_plan(target, unbound_release, descriptor, **VERSIONS))
    assert [s["status"] for s in plan["inspection"]["steps"]] == [
        "complete",
        "complete",
        "built_unrecorded",
    ]
    receipt = run(
        runtime_apply(
            target,
            unbound_release,
            descriptor,
            confirmation=_confirm(target),
            receipt_path=tmp_path / "r.json",
            **VERSIONS,
        )
    )
    assert [s["status"] for s in receipt["steps"]] == [
        "already_complete",
        "already_complete",
        "completed",
    ]
    assert run(db.fetchval(f"SELECT EXISTS (SELECT 1 FROM {SCHEMA}.ledger WHERE v=1)"))


def test_unbound_descriptor_rejected_by_bound_release(make_db, release, tmp_path, monkeypatch):
    descriptor = load_descriptor(fixture_descriptor(tmp_path))
    db = make_db("A")
    target = load_target(write_target(tmp_path, db, monkeypatch=monkeypatch))
    install(target, release)
    with pytest.raises(ContractError, match="differs from the one bound"):
        run(runtime_plan(target, release, descriptor, **VERSIONS))


def test_real_descriptor_interrupted_concurrent_index_is_held_without_version_row(
    make_db, release, tmp_path, monkeypatch
):
    """Fault injection at saver.v6 with the REAL pinned descriptor (mirrors the reference
    executor test): an interrupted CREATE INDEX CONCURRENTLY leaves the same-named index
    invalid; the phase holds, records no v6 row and never rebuilds the index."""
    import mission_control_db_contract.runtime as runtime_module

    descriptor = load_descriptor(release.release_root.parent / "runtime" / "descriptor.json")
    db = make_db("A")
    target = load_target(write_target(tmp_path, db, monkeypatch=monkeypatch))
    install(target, release)
    original = runtime_module._run_step

    async def interrupted(connection, step, entry):
        if step["id"] != "saver.v6":
            return await original(connection, step, entry)
        await connection.execute(
            f"INSERT INTO {SCHEMA}.checkpoints (thread_id, checkpoint_id, checkpoint) "
            "VALUES ('dup', 'a', '{}'), ('dup', 'b', '{}')"
        )
        await connection.execute(
            f"CREATE UNIQUE INDEX CONCURRENTLY checkpoints_thread_id_idx ON {SCHEMA}.checkpoints "
            "(thread_id)"
        )

    monkeypatch.setattr(runtime_module, "_run_step", interrupted)
    with pytest.raises(ContractError, match="saver.v6 failed .*23505"):
        run(
            runtime_apply(
                target,
                release,
                descriptor,
                confirmation=_confirm(target),
                receipt_path=tmp_path / "interrupted.json",
                **VERSIONS,
            )
        )
    monkeypatch.setattr(runtime_module, "_run_step", original)
    receipt = json.loads((tmp_path / "interrupted.json").read_text(encoding="utf-8"))
    assert receipt["status"] == "failed"
    assert receipt["steps"][-1]["invalid_indexes_after"] == ["checkpoints_thread_id_idx"]
    oid = run(db.fetchval(f"SELECT '{SCHEMA}.checkpoints_thread_id_idx'::regclass::oid"))

    async def ledger(connection_db) -> list[int]:
        connection = await connection_db.connect()
        try:
            rows = await connection.fetch(f"SELECT v FROM {SCHEMA}.checkpoint_migrations")
            return sorted(row["v"] for row in rows)
        finally:
            await connection.close()

    assert run(ledger(db)) == [0, 1, 2, 3, 4, 5]
    plan = run(runtime_plan(target, release, descriptor, **VERSIONS))
    assert plan["status"] == "hold"
    assert plan["inspection"]["invalid_indexes"] == ["checkpoints_thread_id_idx"]
    with pytest.raises(ContractError, match="never blindly retried") as held:
        run(
            runtime_apply(
                target,
                release,
                descriptor,
                confirmation=_confirm(target),
                receipt_path=tmp_path / "resume.json",
                **VERSIONS,
            )
        )
    assert held.value.code == "RUNTIME_HOLD"
    assert run(ledger(db)) == [0, 1, 2, 3, 4, 5]
    assert run(db.fetchval(f"SELECT '{SCHEMA}.checkpoints_thread_id_idx'::regclass::oid")) == oid
    assert (
        run(
            db.fetchval(
                "SELECT indisvalid FROM pg_index WHERE indexrelid = "
                f"'{SCHEMA}.checkpoints_thread_id_idx'::regclass"
            )
        )
        is False
    )


def test_authored_descriptor_and_bundles_match_shipped_json_schemas():
    import jsonschema  # root dev group dependency; a missing install fails, never skips
    from test_seeds import catalog_bundle, tenant_bundle

    package = Path(__file__).resolve().parents[1]
    schemas = package / "src" / "mission_control_db_contract" / "schemas"
    runtime_schema = json.loads((schemas / "runtime-descriptor.schema.json").read_text("utf-8"))
    descriptor = json.loads((package / "runtime" / "descriptor.json").read_text("utf-8"))
    jsonschema.validate(descriptor, runtime_schema)
    seed_schema = json.loads((schemas / "seed-bundle.v1.schema.json").read_text("utf-8"))
    for bundle in (tenant_bundle(), catalog_bundle()):
        jsonschema.validate(bundle, seed_schema)
