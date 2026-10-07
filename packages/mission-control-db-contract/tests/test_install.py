"""Real two-cluster installation proofs (fresh install, replay, drift, rollback, roles)."""

from __future__ import annotations

import asyncio
import json
import shutil
from pathlib import Path

import asyncpg
import pytest
from conftest import (
    VERSIONS,
    build_and_lock,
    copy_component,
    run,
    write_target,
)

from mission_control_db_contract.canonical import sha256_hex, write_json
from mission_control_db_contract.deployment import (
    apply_release,
    plan_release,
    verify_release,
)
from mission_control_db_contract.errors import ContractError
from mission_control_db_contract.integrity import load_release
from mission_control_db_contract.release import write_lock
from mission_control_db_contract.target import load_target


def install(target: dict[str, str], release, **overrides):
    plan = run(plan_release(target, release, **VERSIONS))
    assert plan["plan"]["holds"] == [], plan["plan"]["holds"]
    return run(
        apply_release(
            target,
            release,
            confirmation=f"{target['project_ref']}:{target['installation_id']}",
            expected_plan_digest=plan["plan_digest"],
            **{**VERSIONS, **overrides},
        )
    ), plan


def test_fresh_install_repeat_noop_and_identical_fingerprints(
    make_db, release, release_ai, tmp_path, monkeypatch
):
    db_a, db_b = make_db("A"), make_db("B")
    target_a = load_target(write_target(tmp_path, db_a, monkeypatch=monkeypatch))
    target_b = load_target(write_target(tmp_path, db_b, app="ai-engineer", monkeypatch=monkeypatch))
    expected_keys = [m.key for m in release.migrations]
    for target, rel in ((target_a, release), (target_b, release_ai)):
        result, plan = install(target, rel)
        assert plan["plan"]["status"] == "pending"
        assert plan["plan"]["pending_migrations"] == expected_keys
        assert result["outcome"] == "applied"
        assert result["applied_migrations"] == expected_keys
        assert result["schema_fingerprint"] == rel.manifest["schema_fingerprint"]
        # Second run: plan is a verified no-op; apply mutates nothing.
        again, replan = install(target, rel)
        assert replan["plan"]["status"] == "noop" and replan["plan"]["mutations"] == []
        assert again["applied_migrations"] == [] and again["outcome"] == "noop"
        assert run(verify_release(target, rel, **VERSIONS))["status"] == "verified"
    # Same logical schema in both clusters despite different owners (postgres vs migrator).
    assert db_a.installer_user != db_b.installer_user
    fa = run(verify_release(target_a, release, **VERSIONS))
    fb = run(verify_release(target_b, release_ai, **VERSIONS))
    assert fa["schema_fingerprint"] == fb["schema_fingerprint"]
    assert fa["schema_fingerprints"] == fb["schema_fingerprints"]
    assert release.manifest_digest == release_ai.manifest_digest
    # Identity and attestation rows.
    for db, target in ((db_a, target_a), (db_b, target_b)):
        row = run(
            db.fetchval(
                "SELECT row_to_json(a)::text FROM mission_control.application_installation a"
            )
        )
        identity = json.loads(row)
        assert identity["installation_id"] == target["installation_id"]
        assert identity["application_id"] == target["app"]
        assert identity["schema_component_version"] == "1.0.0"
        attestation = json.loads(
            run(
                db.fetchval(
                    "SELECT row_to_json(r)::text FROM mission_control.release_attestation r"
                )
            )
        )
        assert attestation["manifest_digest"] == release.manifest_digest
        assert attestation["supported_writer_versions"] == ["mission-control-runtime/1"]
        assert attestation["attested_by"] == db.installer_user


def test_runtime_roles_are_restricted(make_db, release, tmp_path, monkeypatch):
    db_b = make_db("B")
    target = load_target(write_target(tmp_path, db_b, monkeypatch=monkeypatch))
    install(target, release)
    rows = run(_roles(db_b))
    assert sorted(rows) == sorted(release.manifest["runtime_roles"])
    for name, row in rows.items():
        assert not row["rolcanlogin"] and not row["rolsuper"] and not row["rolbypassrls"], name
        assert not row["rolcreaterole"] and not row["rolcreatedb"], name
        assert row["owned_objects"] == 0, name
        assert row["member_of"] == [], name
    # Any owned table belongs to the migration principal, never a runtime role.
    owners = run(
        db_b.fetchval(
            "SELECT array_agg(DISTINCT pg_get_userbyid(c.relowner)) FROM pg_class c "
            "JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname LIKE 'mission_control%'"
        )
    )
    assert owners == [db_b.installer_user]


async def _roles(db):
    connection = await db.connect()
    try:
        rows = await connection.fetch(
            "SELECT r.rolname, r.rolcanlogin, r.rolsuper, r.rolbypassrls, r.rolcreaterole, "
            "r.rolcreatedb, (SELECT count(*) FROM pg_class c WHERE c.relowner=r.oid) "
            "AS owned_objects, ARRAY(SELECT g.rolname FROM pg_auth_members m JOIN pg_roles g "
            "ON g.oid=m.roleid WHERE m.member=r.oid) AS member_of FROM pg_roles r "
            "WHERE r.rolname LIKE 'mission\\_control\\_%'"
        )
        return {row["rolname"]: dict(row) for row in rows}
    finally:
        await connection.close()


def test_stale_plan_rejects_and_identity_mismatch_holds(make_db, release, tmp_path, monkeypatch):
    db = make_db("A")
    target = load_target(write_target(tmp_path, db, monkeypatch=monkeypatch))
    plan = run(plan_release(target, release, **VERSIONS))
    # Observed state drifts after review: a restricted same-named role appears.
    run(db.execute("CREATE ROLE mission_control_readonly NOLOGIN NOINHERIT"))
    with pytest.raises(ContractError, match="STALE_PLAN") as error:
        run(
            apply_release(
                target,
                release,
                confirmation=f"{target['project_ref']}:{target['installation_id']}",
                expected_plan_digest=plan["plan_digest"],
                **VERSIONS,
            )
        )
    assert error.value.code == "STALE_PLAN"
    assert not run(db.fetchval("SELECT to_regnamespace('mission_control') IS NOT NULL"))
    with pytest.raises(ContractError, match="confirmation"):
        run(
            apply_release(
                target, release, confirmation="wrong", expected_plan_digest="sha256:x", **VERSIONS
            )
        )
    install(target, release)
    other = load_target(
        write_target(
            tmp_path,
            db,
            project_ref=target["project_ref"],
            env_name=target["database_url_env"],
            monkeypatch=monkeypatch,
        )
    )
    plan = run(plan_release(other, release, **VERSIONS))
    assert any("identity differs" in h for h in plan["plan"]["holds"])
    bad_version = run(
        plan_release(
            target, release, reader_version="x", writer_version="mission-control-runtime/1"
        )
    )
    assert any("Reader/writer" in h for h in bad_version["plan"]["holds"])


def test_concurrent_installers_serialize(make_db, release, tmp_path, monkeypatch):
    db = make_db("B")
    target = load_target(write_target(tmp_path, db, monkeypatch=monkeypatch))
    plan = run(plan_release(target, release, **VERSIONS))
    confirmation = f"{target['project_ref']}:{target['installation_id']}"

    async def both():
        return await asyncio.gather(
            *(
                apply_release(
                    target,
                    release,
                    confirmation=confirmation,
                    expected_plan_digest=plan["plan_digest"],
                    **VERSIONS,
                )
                for _ in range(2)
            )
        )

    results = run(both())
    outcomes = sorted(r["outcome"] for r in results)
    assert outcomes == ["applied", "noop_replay"]
    assert run(db.fetchval("SELECT count(*) FROM mission_control.component_release")) == len(
        release.migrations
    )
    assert run(db.fetchval("SELECT count(*) FROM mission_control.release_attestation")) == 1


def test_receipt_drift_unknown_migration_and_fingerprint_drift(
    make_db, release, tmp_path, monkeypatch
):
    db = make_db("A")
    target = load_target(write_target(tmp_path, db, monkeypatch=monkeypatch))
    install(target, release)
    # Catalog drift with unchanged receipts.
    run(db.execute("GRANT INSERT ON mission_control.tenant TO mission_control_readonly"))
    with pytest.raises(ContractError, match="fingerprint") as error:
        run(verify_release(target, release, **VERSIONS))
    assert error.value.code == "VERIFY_HOLD"
    run(db.execute("REVOKE INSERT ON mission_control.tenant FROM mission_control_readonly"))
    assert run(verify_release(target, release, **VERSIONS))["status"] == "verified"
    # Checksum drift on an applied receipt (bypassing the immutability trigger as admin).
    first = release.migrations[0].key
    run(
        db.execute(
            "SET session_replication_role = replica; "
            "UPDATE mission_control.component_release SET migration_digest="
            f"'sha256:{'0' * 64}' WHERE migration_key='{first}'; "
            "SET session_replication_role = origin"
        )
    )
    with pytest.raises(ContractError, match="changed applied migration checksum"):
        run(verify_release(target, release, **VERSIONS))
    plan = run(plan_release(target, release, **VERSIONS))
    assert plan["plan"]["status"] == "hold" and plan["plan"]["pending_migrations"] == []


def test_unknown_applied_migration_rejects(make_db, release, tmp_path, monkeypatch):
    db = make_db("A")
    target = load_target(write_target(tmp_path, db, monkeypatch=monkeypatch))
    install(target, release)
    run(
        db.execute(
            "INSERT INTO mission_control.component_release VALUES "
            f"('1.0.0','0999_unknown','sha256:{'1' * 64}',now(),'intruder')"
        )
    )
    with pytest.raises(ContractError, match="Unknown or changed"):
        run(verify_release(target, release, **VERSIONS))


def test_collisions_hold_before_creating_anything(make_db, release, tmp_path, monkeypatch):
    db = make_db("A")
    target = load_target(write_target(tmp_path, db, monkeypatch=monkeypatch))
    run(db.execute("CREATE SCHEMA mission_control_search"))
    plan = run(plan_release(target, release, **VERSIONS))
    assert any("without release receipts" in h for h in plan["plan"]["holds"])
    assert plan["plan"]["pending_migrations"] == []
    run(db.execute("DROP SCHEMA mission_control_search"))
    for attribute in ("LOGIN", "BYPASSRLS", "CREATEDB"):
        run(db.execute(f"CREATE ROLE mission_control_family_writer {attribute}"))
        plan = run(plan_release(target, release, **VERSIONS))
        assert any("collides" in h for h in plan["plan"]["holds"]), attribute
        with pytest.raises(ContractError, match="collides"):
            run(
                apply_release(
                    target,
                    release,
                    confirmation=f"{target['project_ref']}:{target['installation_id']}",
                    expected_plan_digest=plan["plan_digest"],
                    **VERSIONS,
                )
            )
        run(db.execute("DROP ROLE mission_control_family_writer"))
    # An unrelated role name prefix-alike (mission_control_x) with LOGIN also holds.
    run(db.execute("CREATE ROLE mission_control_x LOGIN"))
    try:
        assert run(plan_release(target, release, **VERSIONS))["plan"]["holds"]
    finally:
        run(db.execute("DROP ROLE mission_control_x"))
    assert not run(db.fetchval("SELECT to_regnamespace('mission_control') IS NOT NULL"))


def _republish(component_root: Path) -> None:
    """Rewrite manifest migration digests for hand-modified fixture bytes (tests only)."""
    manifest_path = component_root / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    for item in manifest["ordered_migrations"]:
        item["sha256"] = sha256_hex((component_root / item["path"]).read_bytes())
    write_json(manifest_path, manifest)


def test_injected_middle_migration_failure_leaves_nothing(make_db, tmp_path, monkeypatch):
    db = make_db("B")
    component_root = copy_component(tmp_path)
    migrations = sorted(p.name for p in (component_root / "migrations").glob("*.sql"))
    middle = migrations[len(migrations) // 2].split("_")[0] + "_zz_injected.sql"
    marker = component_root / "migrations" / middle
    marker.write_text("CREATE TABLE mission_control.zz_injected (id int);\n", encoding="utf-8")
    build_and_lock(tmp_path, component_root)
    # Same release identity, but the middle migration now fails after creating objects.
    marker.write_text(
        "CREATE TABLE mission_control.zz_injected (id int);\nSELECT 1/0;\n", encoding="utf-8"
    )
    _republish(component_root)
    write_lock(component_root, tmp_path / "deployments", "biotech")
    release = load_release(
        tmp_path / "deployments" / "biotech" / "release.lock.json", component_root
    )
    target = load_target(write_target(tmp_path, db, monkeypatch=monkeypatch))
    plan = run(plan_release(target, release, **VERSIONS))
    with pytest.raises(ContractError, match="zz_injected.*22012") as error:
        run(
            apply_release(
                target,
                release,
                confirmation=f"{target['project_ref']}:{target['installation_id']}",
                expected_plan_digest=plan["plan_digest"],
                **VERSIONS,
            )
        )
    assert error.value.code == "APPLY_FAILED"
    for schema in ("mission_control", "mission_control_search"):
        assert not run(db.fetchval("SELECT to_regnamespace($1) IS NOT NULL", schema))
    assert (
        run(db.fetchval("SELECT count(*) FROM pg_roles WHERE rolname LIKE 'mission\\_control\\_%'"))
        == 0
    )


def test_upgrade_from_declared_predecessor(make_db, release, tmp_path, monkeypatch):
    db = make_db("A")
    target = load_target(write_target(tmp_path, db, monkeypatch=monkeypatch))
    install(target, release)
    component_root = copy_component(tmp_path, source=release.release_root)
    spec_path = component_root / "release-spec.json"
    spec = json.loads(spec_path.read_text(encoding="utf-8"))
    spec["component_version"] = "1.1.0"
    spec["compatible_previous_fingerprints"] = [release.manifest["schema_fingerprint"]]
    write_json(spec_path, spec)
    (component_root / "migrations" / "9000_test_upgrade.sql").write_text(
        "CREATE TABLE mission_control.zz_upgrade_probe (id int PRIMARY KEY);\n", encoding="utf-8"
    )
    upgraded = build_and_lock(tmp_path, component_root)
    result, plan = install(target, upgraded)
    assert plan["plan"]["pending_migrations"] == ["9000_test_upgrade"]
    assert result["applied_migrations"] == ["9000_test_upgrade"]
    assert run(db.fetchval("SELECT count(*) FROM mission_control.release_attestation")) == 2
    assert (
        run(
            db.fetchval(
                "SELECT schema_component_version FROM mission_control.application_installation"
            )
        )
        == "1.1.0"
    )
    # The old release no longer verifies against the upgraded database.
    with pytest.raises(ContractError):
        run(verify_release(target, release, **VERSIONS))


def test_tampered_release_rejects_before_connecting(release_work, tmp_path):
    work, component_root = release_work
    copy = tmp_path / "component"
    shutil.copytree(component_root, copy)
    lock = work / "deployments" / "biotech" / "release.lock.json"
    (copy / "migrations" / "0001_foundation.sql").write_bytes(
        (copy / "migrations" / "0001_foundation.sql").read_bytes() + b"\n-- tamper\n"
    )
    with pytest.raises(ContractError, match="checksum mismatch"):
        load_release(lock, copy)


def test_driver_errors_are_redacted(tmp_path, monkeypatch):
    target_path = tmp_path / "t.toml"
    target_path.write_text(
        "format_version = 1\n[target]\napp='biotech'\napplication_id='biotech'\n"
        "project_label='x'\nproject_ref='nowhere'\n"
        "installation_id='01990000-0000-7000-8000-000000000000'\nenvironment='disposable'\n"
        "database_host='127.0.0.1'\ndatabase_port=55509\ndatabase_name='postgres'\n"
        "database_user='postgres'\ndatabase_url_env='MCDB_T_REDACT'\n[approval]\n"
        "approved_by='t'\nidentity_evidence='t'\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("MCDB_T_REDACT", "postgresql://postgres:SECRET-PW@127.0.0.1:55509/postgres")
    with pytest.raises(ContractError) as error:
        run(verify_release(load_target(target_path), None, **VERSIONS))  # type: ignore[arg-type]
    assert "SECRET-PW" not in str(error.value)
    assert isinstance(asyncpg, object)
