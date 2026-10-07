"""Offline proofs: integrity, SQL safety, targets, receipts, uuid7, compare, CLI help."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from uuid import UUID

import pytest

from mission_control_db_contract.cli import build_parser, main
from mission_control_db_contract.deployment import receipt_prefix
from mission_control_db_contract.errors import ContractError
from mission_control_db_contract.integrity import (
    Migration,
    load_release,
    transaction_safe_sql,
    verify_bundle,
)
from mission_control_db_contract.seeds import satisfies, uuid7
from mission_control_db_contract.snapshot import compare
from mission_control_db_contract.target import load_target, target_dsn

REPO = Path(__file__).resolve().parents[3]
COMMANDS = (
    "inspect",
    "plan",
    "apply",
    "verify",
    "release-build",
    "lock",
    "snapshot",
    "compare",
    "seed-plan",
    "seed-apply",
    "runtime-plan",
    "runtime-apply",
    "qualify",
)


@pytest.mark.parametrize(
    "statement",
    [
        "COMMIT;",
        "BEGIN; SELECT 1;",
        "START TRANSACTION;",
        "ROLLBACK TO SAVEPOINT x;",
        "SELECT ';'; END;",
        "/* nested /* comment */ okay */ COMMIT;",
        "SELECT 'literal'; -- next\nCOMMIT;",
        "PREPARE TRANSACTION 'x';",
        "SET SESSION AUTHORIZATION other;",
    ],
)
def test_release_sql_cannot_escape_the_transaction(statement):
    with pytest.raises(ContractError, match="control installer transactions"):
        transaction_safe_sql(statement)


def test_function_bodies_and_literals_are_not_transaction_commands():
    transaction_safe_sql(
        "CREATE FUNCTION f() RETURNS void LANGUAGE plpgsql AS $body$ "
        "BEGIN RAISE NOTICE 'COMMIT;'; END; $body$; SELECT 'ROLLBACK;'; SELECT E'it\\'s';"
    )


def test_receipts_must_be_an_unchanged_ordered_prefix():
    migrations = (
        Migration("0001_a", "m/1", "sha256:a", ""),
        Migration("0002_b", "m/2", "sha256:b", ""),
    )
    assert receipt_prefix([], migrations) == 0
    assert (
        receipt_prefix([{"migration_key": "0001_a", "migration_digest": "sha256:a"}], migrations)
        == 1
    )
    for receipt in (
        {"migration_key": "0001_a", "migration_digest": "sha256:changed"},
        {"migration_key": "0999_unknown", "migration_digest": "sha256:a"},
        {"migration_key": "0002_b", "migration_digest": "sha256:b"},
    ):
        with pytest.raises(ContractError):
            receipt_prefix([receipt], migrations)


def _bundle(tmp_path: Path) -> tuple[Path, Path]:
    root = tmp_path / "component"
    (root / "migrations").mkdir(parents=True)
    (root / "migrations" / "0001_x.sql").write_text("SELECT 1;", encoding="utf-8")
    (root / "generated").mkdir()
    (root / "generated" / "contract.json").write_text("{}", encoding="utf-8")

    def sha(name: str) -> str:
        return hashlib.sha256((root / name).read_bytes()).hexdigest()

    digest = "sha256:" + "0" * 64
    manifest = {
        "component": "mission_control",
        "component_version": "1.0.0",
        "source_identity": "mission-control-db-contract",
        "source_revision": "git:unavailable+inputs:" + digest,
        "source_inputs_digest": digest,
        "installer_protocol": "mission-control-sql/v2",
        "schema_fingerprint_algorithm": "mc-pg-catalog-v2",
        "owned_schemas": ["mission_control", "mission_control_search"],
        "schema_fingerprint": digest,
        "schema_fingerprints": {"mission_control": digest, "mission_control_search": digest},
        "contract_path": "generated/contract.json",
        "contract_schema_digest": "sha256:" + sha("generated/contract.json"),
        "min_postgres_version": 170000,
        "postgres_major": 17,
        "required_extensions": [],
        "ordered_migrations": [
            {
                "key": "0001_x",
                "path": "migrations/0001_x.sql",
                "sha256": sha("migrations/0001_x.sql"),
            }
        ],
        "supported_reader_versions": ["r"],
        "supported_writer_versions": ["w"],
        "runtime_roles": ["mission_control_runtime"],
        "compatible_previous_fingerprints": [],
        "seed_compatibility": {},
        "runtime_descriptor": None,
    }
    (root / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    lock = {
        "format_version": 2,
        "status": "pinned",
        "component": "mission_control",
        "source_identity": "mission-control-db-contract",
        "source_revision": manifest["source_revision"],
        "release_uri": "fixture://unit",
        "component_version": "1.0.0",
        "app": "biotech",
        "manifest_path": "manifest.json",
        "files": {
            p.relative_to(root).as_posix(): sha(p.relative_to(root).as_posix())
            for p in root.rglob("*")
            if p.is_file()
        },
    }
    path = tmp_path / "release.lock.json"
    path.write_text(json.dumps(lock), encoding="utf-8")
    return path, root


def test_bundle_integrity_tamper_extra_missing_and_authority(tmp_path):
    lock, root = _bundle(tmp_path)
    assert [m.key for m in load_release(lock, root).migrations] == ["0001_x"]
    migration = root / "migrations" / "0001_x.sql"
    migration.write_text("SELECT 2;", encoding="utf-8")
    with pytest.raises(ContractError, match="checksum mismatch"):
        verify_bundle(lock, root)
    migration.write_text("SELECT 1;", encoding="utf-8")
    (root / "migrations" / "0002_extra.sql").write_text("SELECT 1;", encoding="utf-8")
    with pytest.raises(ContractError, match="exhaustive pin"):
        verify_bundle(lock, root)
    (root / "migrations" / "0002_extra.sql").unlink()
    (root / "generated" / "contract.json").unlink()
    with pytest.raises(ContractError, match="exhaustive pin"):
        verify_bundle(lock, root)
    lock2, root2 = _bundle(tmp_path / "second")
    for field, value, message in (
        ("source_identity", "ai-engineer-db-contract", "source authority"),
        ("status", "blocked_missing_common_release", "COMMON_COMPONENT_UNAVAILABLE"),
        ("app", "third-app", "supported application"),
        ("format_version", 1, "source authority"),
    ):
        data = json.loads(lock2.read_text(encoding="utf-8"))
        data[field] = value
        bad = tmp_path / f"bad-{field}.json"
        bad.write_text(json.dumps(data), encoding="utf-8")
        with pytest.raises(ContractError, match=message):
            verify_bundle(bad, root2)
    data = json.loads(lock2.read_text(encoding="utf-8"))
    data["files"]["../escape.sql"] = "0" * 64
    bad = tmp_path / "bad-path.json"
    bad.write_text(json.dumps(data), encoding="utf-8")
    with pytest.raises(ContractError, match="Unsafe release member"):
        verify_bundle(bad, root2)


def test_old_biotech_lock_is_not_accepted():
    old = REPO.parent / "biotech-postgres-db-contract" / "component.lock.json"
    if not old.exists():
        pytest.fail("Expected the superseded Biotech lock to remain in place")
    with pytest.raises(ContractError, match="COMMON_COMPONENT_UNAVAILABLE"):
        verify_bundle(old, REPO / "nonexistent")


VALID_TARGET = """format_version = 1
[target]
app = "ai-engineer"
application_id = "ai-engineer"
project_label = "supabase-blue-ocean"
project_ref = "abcdefghijklmnopqrst"
installation_id = "01990000-0000-7000-8000-000000000001"
environment = "disposable"
database_host = "127.0.0.1"
database_port = 55501
database_name = "postgres"
database_user = "postgres"
database_url_env = "MCDB_UNIT_URL"
[approval]
approved_by = "unit-test"
identity_evidence = "unit-test"
"""


def test_target_loader_is_generic_and_strict(tmp_path):
    path = tmp_path / "target.toml"
    path.write_text(VALID_TARGET, encoding="utf-8")
    assert load_target(path)["project_label"] == "supabase-blue-ocean"
    for old, new, message in (
        ('app = "ai-engineer"', 'app = "third"', "supported apps"),
        ('application_id = "ai-engineer"', 'application_id = "biotech"', "equal the declared app"),
        ('database_url_env = "MCDB_UNIT_URL"', 'database_url_env = "postgresql://x"', "NAME"),
        ('approved_by = "unit-test"', 'approved_by = "REPLACE_ME"', "approval reference"),
        ('environment = "disposable"', 'environment = "prod"', "environment"),
        ("[approval]", 'password = "x"\n[approval]', "Unknown target fields"),
    ):
        path.write_text(VALID_TARGET.replace(old, new), encoding="utf-8")
        with pytest.raises(ContractError, match=message):
            load_target(path)


@pytest.mark.parametrize(
    ("app", "ref", "session_user"),
    [
        ("biotech", "bxnetwiimwhtlrjlbtab", "postgres"),
        ("ai-engineer", "wkythqbofmckbuoothhn", "postgres"),
    ],
)
def test_approved_deployment_targets_are_complete_and_project_bound(app, ref, session_user):
    path = REPO / "deployments" / app / "target.toml"
    if not path.exists():
        pytest.fail(f"Lead-owned deployment target is missing: {path}")
    target = load_target(path)
    assert (target["app"], target["project_ref"], target["environment"]) == (
        app,
        ref,
        "production",
    )
    assert target["database_session_user"] == session_user
    assert ref in target["database_host"] or target["database_user"].endswith("." + ref)
    assert target["approved_by"] and target["identity_evidence"]


def test_session_user_must_be_bound_to_the_target_project(tmp_path):
    pooled = VALID_TARGET.replace(
        'database_user = "postgres"', 'database_user = "postgres.otherprojectref"'
    ).replace("[approval]", 'database_session_user = "postgres"\n\n[approval]', 1)
    path = tmp_path / "target.toml"
    path.write_text(pooled, encoding="utf-8")
    with pytest.raises(ContractError, match="project-bound pooler login"):
        load_target(path)


def test_dsn_must_match_target_and_errors_redact(tmp_path, monkeypatch):
    path = tmp_path / "target.toml"
    path.write_text(VALID_TARGET, encoding="utf-8")
    target = load_target(path)
    monkeypatch.setenv("MCDB_UNIT_URL", "postgresql://postgres:TOPSECRET@127.0.0.1:55501/postgres")
    assert target_dsn(target).endswith("/postgres")
    for bad in (
        "postgresql://postgres:TOPSECRET@127.0.0.1:55501/other",
        "postgresql://postgres:TOPSECRET@10.0.0.1:55501/postgres",
        "postgresql://postgres:TOPSECRET@127.0.0.1:55501/postgres?options=-csearch_path%3Dx",
    ):
        monkeypatch.setenv("MCDB_UNIT_URL", bad)
        with pytest.raises(ContractError) as error:
            target_dsn(target)
        assert "TOPSECRET" not in str(error.value)


def test_uuid7_and_semver_ranges():
    values = [uuid7() for _ in range(50)]
    assert all(
        isinstance(v, UUID) and v.version == 7 and v.variant == "specified in RFC 4122"
        for v in values
    )
    assert len(set(values)) == 50
    assert uuid7(unix_ms=0).int >> 80 == 0
    assert satisfies("1.4.2", ">=1.0.0 <2.0.0") and not satisfies("2.0.0", ">=1.0.0 <2.0.0")
    with pytest.raises(ContractError):
        satisfies("1.0.0", "~1.0")


def test_compare_requires_snapshots_and_rule_reasons():
    snap = {"format": "mission-control-protected-snapshot/v1", "roles": {"a": {"x": 1}}}
    assert compare(snap, snap, None)["status"] == "identical"
    with pytest.raises(ContractError):
        compare({"format": "x"}, snap, None)
    with pytest.raises(ContractError, match="reason"):
        compare(
            snap,
            snap,
            {
                "format": "mission-control-allowed-differences/v1",
                "allowed": [{"path": "*", "change": "added"}],
            },
        )


@pytest.mark.parametrize("command", COMMANDS)
def test_every_command_has_help(command, capsys):
    with pytest.raises(SystemExit) as exit_info:
        main([command, "--help"])
    assert exit_info.value.code == 0
    assert "usage: mission-db " + command in capsys.readouterr().out


def test_parser_lists_all_commands():
    parser = build_parser()
    help_text = parser.format_help()
    for command in COMMANDS:
        assert command in help_text


def test_blocked_gate_exits_2_with_structured_json(tmp_path, capsys):
    code = main(["inspect", "--target", str(REPO / "deployments" / "biotech" / "target.toml")])
    assert code == 2
    document = json.loads(capsys.readouterr().out)
    assert document["status"] == "blocked" and document["mutations"] == []
    code = main(
        [
            "verify",
            "--target",
            str(tmp_path / "missing.toml"),
            "--lock",
            str(tmp_path / "x"),
            "--reader-version",
            "r",
            "--writer-version",
            "w",
        ]
    )
    assert code == 2
