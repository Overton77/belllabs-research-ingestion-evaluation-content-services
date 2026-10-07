"""End-to-end CLI flow on a disposable cluster: exit codes, JSON output, evidence files."""

from __future__ import annotations

import json
from pathlib import Path

from conftest import write_target
from test_release_and_snapshot import ALLOWED
from test_seeds import tenant_bundle

from mission_control_db_contract.canonical import write_json
from mission_control_db_contract.cli import main
from mission_control_db_contract.target import load_target

VERSION_ARGS = [
    "--reader-version",
    "mission-control-runtime/1",
    "--writer-version",
    "mission-control-runtime/1",
]


def cli(capsys, *argv: str) -> tuple[int, dict]:
    code = main(list(argv))
    out = capsys.readouterr().out
    return code, json.loads(out)


def test_cli_plan_apply_verify_seed_qualify(make_db, release_work, tmp_path, monkeypatch, capsys):
    work, component_root = release_work
    lock = work / "deployments" / "biotech" / "release.lock.json"
    db = make_db("B")
    target_path = write_target(tmp_path, db, monkeypatch=monkeypatch)
    snapshot_path = write_target(tmp_path, db, use_admin=True, monkeypatch=monkeypatch)
    target = load_target(target_path)
    common = ["--target", str(target_path), "--lock", str(lock)]  # release_root from the lock
    evidence = tmp_path / "evidence"
    allowed = tmp_path / "allowed.json"
    write_json(allowed, ALLOWED)

    code, before = cli(
        capsys,
        "qualify",
        *common,
        *VERSION_ARGS,
        "--phase",
        "before",
        "--out-dir",
        str(evidence),
        "--snapshot-target",
        str(snapshot_path),
    )
    assert code == 0 and before["status"] == "pass"
    assert {
        "identity.json",
        "release.json",
        "plan.json",
        "schema-before.json",
        "protected-before.json",
        "decision.json",
    } <= set(before["files"])

    code, plan = cli(capsys, "plan", *common, *VERSION_ARGS, "--out", str(tmp_path / "plan.json"))
    assert code == 0 and plan["plan"]["status"] == "pending"
    confirm = f"{target['project_ref']}:{target['installation_id']}"
    code, blocked = cli(
        capsys,
        "apply",
        *common,
        *VERSION_ARGS,
        "--confirm-target",
        "x:y",
        "--expected-plan-digest",
        plan["plan_digest"],
    )
    assert code == 2 and blocked["status"] == "blocked" and blocked["mutations"] == []
    code, applied = cli(
        capsys,
        "apply",
        *common,
        *VERSION_ARGS,
        "--confirm-target",
        confirm,
        "--expected-plan-digest",
        plan["plan_digest"],
    )
    assert code == 0 and applied["outcome"] == "applied"
    code, verified = cli(capsys, "verify", *common, *VERSION_ARGS)
    assert code == 0 and verified["status"] == "verified"
    code, inspected = cli(capsys, "inspect", "--target", str(target_path))
    assert code == 0 and inspected["identity_matches_target"] is True

    bundle = tmp_path / "bundles" / "qualification.json"
    write_json(bundle, tenant_bundle())
    code, seed_plan = cli(
        capsys, "seed-plan", *common, *VERSION_ARGS, "--bundle", str(bundle.parent)
    )
    assert code == 0 and seed_plan["bundles"][0]["status"] == "pending"
    code, seeded = cli(
        capsys,
        "seed-apply",
        *common,
        *VERSION_ARGS,
        "--bundle",
        str(bundle),
        "--confirm-target",
        confirm,
    )
    assert code == 0 and seeded["bundles"][0]["outcome"] == "applied"

    descriptor = component_root.parent / "runtime" / "descriptor.json"
    code, rplan = cli(
        capsys, "runtime-plan", *common, *VERSION_ARGS, "--descriptor", str(descriptor)
    )
    assert code == 0 and rplan["status"] == "pending"
    code, rapplied = cli(
        capsys,
        "runtime-apply",
        *common,
        *VERSION_ARGS,
        "--descriptor",
        str(descriptor),
        "--confirm-target",
        confirm,
        "--receipt-out",
        str(evidence / "runtime-receipts.json"),
    )
    assert code == 0 and rapplied["status"] == "complete"

    code, after = cli(
        capsys,
        "qualify",
        *common,
        *VERSION_ARGS,
        "--phase",
        "after",
        "--out-dir",
        str(evidence),
        "--snapshot-target",
        str(snapshot_path),
        "--allowed",
        str(allowed),
    )
    assert code == 0 and after["status"] == "pass", after
    diff = json.loads((evidence / "protected-diff.json").read_text(encoding="utf-8"))
    # The runtime phase adds the checkpointer role; the allowed manifest covers mission_control_*.
    assert diff["unallowed_count"] == 0
    code, compared = cli(
        capsys,
        "compare",
        "--before",
        str(evidence / "protected-before.json"),
        "--after",
        str(evidence / "protected-after.json"),
    )
    assert code == 2 and compared["status"] == "blocked"
    # Evidence never contains credentials or protected row contents.
    for path in Path(evidence).iterdir():
        text = path.read_text(encoding="utf-8")
        for secret in (
            "mcdb-disposable-only",
            "mcdb-migrator-disposable",
            "BELLLABS-POISON-SENTINEL",
            "LEGACY-SEARCH-CONTENT",
        ):
            assert secret not in text, (path.name, secret)


def test_cli_lock_and_release_build_report(release_work, tmp_path, capsys):
    _work, component_root = release_work
    code, locked = cli(
        capsys,
        "lock",
        "--app",
        "ai-engineer",
        "--component-root",
        str(component_root),
        "--deployments-root",
        str(tmp_path / "deployments"),
    )
    assert code == 0 and locked["app"] == "ai-engineer" and locked["file_count"] >= 5
    lock = json.loads(
        (tmp_path / "deployments" / "ai-engineer" / "release.lock.json").read_text(encoding="utf-8")
    )
    assert lock["source_identity"] == "mission-control-db-contract" and lock["status"] == "pinned"
    code, blocked = cli(
        capsys,
        "lock",
        "--app",
        "third",
        "--component-root",
        str(component_root),
        "--deployments-root",
        str(tmp_path / "d2"),
    )
    assert code == 2 and blocked["status"] == "blocked"
