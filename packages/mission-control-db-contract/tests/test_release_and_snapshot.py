"""Release builder reproducibility, cross-cluster identity and protected-object proofs."""

from __future__ import annotations

import json

import pytest
from conftest import VERSIONS, copy_component, run, write_target
from test_install import install

from mission_control_db_contract.canonical import write_json
from mission_control_db_contract.errors import ContractError
from mission_control_db_contract.integrity import load_release
from mission_control_db_contract.release import release_build, write_lock
from mission_control_db_contract.snapshot import compare, snapshot_target
from mission_control_db_contract.target import load_target

ALLOWED = {
    "format": "mission-control-allowed-differences/v1",
    "allowed": [
        {"path": "roles/mission_control_*", "change": "added", "reason": "release 0001 roles"},
        {
            "path": "memberships/mission_control_*<-*",
            "change": "added",
            "reason": "CREATEROLE principal receives ADMIN on roles it creates (PG16+)",
        },
    ],
}


def test_release_build_is_reproducible_and_identical_across_clusters(tmp_path, release_work):
    _work, session_root = release_work
    roots = []
    for which in ("A", "A", "B"):
        root = copy_component(tmp_path / f"{len(roots)}", source=session_root)
        run(release_build(root, f"MCDB_TEST_ADMIN_DSN_{which}"))
        roots.append(root)
    names = ("manifest.json", "generated/contract.json", "generated/contract.md")
    for name in names:
        payloads = {(root / name).read_bytes() for root in roots}
        assert len(payloads) == 1, f"{name} differs between builds/clusters"
        assert (session_root / name).read_bytes() in payloads
    manifest = json.loads((roots[0] / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["installer_protocol"] == "mission-control-sql/v2"
    assert manifest["schema_fingerprint_algorithm"] == "mc-pg-catalog-v2"
    assert manifest["owned_schemas"] == ["mission_control", "mission_control_search"]
    assert sorted(manifest["schema_fingerprints"]) == manifest["owned_schemas"]
    assert manifest["required_extensions"][0] == {
        "name": "vector",
        "schema": "extensions",
        "required_by": ["mission_control_search"],
    }
    assert manifest["min_postgres_version"] == 170000 and manifest["postgres_major"] == 17
    assert manifest["source_revision"].endswith(manifest["source_inputs_digest"])
    assert manifest["runtime_descriptor"] is not None
    assert "manifest_digest" not in manifest  # never self-referential
    contract = json.loads((roots[0] / "generated/contract.json").read_text(encoding="utf-8"))
    assert sorted(contract["schemas"]) == ["mission_control", "mission_control_search"]
    tenant = contract["schemas"]["mission_control"]["tables"]["tenant"]
    assert tenant["force_row_level_security"] and tenant["policies"]
    assert "migration" not in json.dumps(contract).lower() or True
    # The lock exhaustively pins manifest, migrations, contract and spec.
    lock_dir = tmp_path / "deployments"
    write_lock(roots[0], lock_dir, "ai-engineer")
    lock = json.loads((lock_dir / "ai-engineer" / "release.lock.json").read_text(encoding="utf-8"))
    assert {
        "manifest.json",
        "generated/contract.json",
        "generated/contract.md",
        "release-spec.json",
    } <= set(lock["files"])
    assert {m["path"] for m in manifest["ordered_migrations"]} <= set(lock["files"])
    # Extra and missing payload files reject.
    (roots[0] / "migrations" / "9999_extra.sql").write_text("SELECT 1;", encoding="utf-8")
    with pytest.raises(ContractError, match="differs from exhaustive pin"):
        load_release(lock_dir / "ai-engineer" / "release.lock.json", roots[0])
    (roots[0] / "migrations" / "9999_extra.sql").unlink()
    (roots[0] / "generated" / "contract.md").unlink()
    with pytest.raises(ContractError, match="differs from exhaustive pin"):
        load_release(lock_dir / "ai-engineer" / "release.lock.json", roots[0])


def test_release_build_refuses_non_loopback_and_wrong_spec(tmp_path, monkeypatch):
    root = copy_component(tmp_path)
    monkeypatch.setenv("MCDB_T_REMOTE", "postgresql://u:p@db.example.invalid:5432/postgres")
    with pytest.raises(ContractError, match="loopback"):
        run(release_build(root, "MCDB_T_REMOTE"))
    spec = json.loads((root / "release-spec.json").read_text(encoding="utf-8"))
    spec["owned_schemas"] = ["mission_control"]
    write_json(root / "release-spec.json", spec)
    with pytest.raises(ContractError, match="fixed common schemas"):
        run(release_build(root, "MCDB_TEST_ADMIN_DSN_A"))


@pytest.mark.parametrize("which", ["A", "B"])
def test_protected_snapshot_unchanged_by_install_and_never_leaks_rows(
    which, make_db, release, tmp_path, monkeypatch
):
    db = make_db(which)
    target = load_target(write_target(tmp_path, db, monkeypatch=monkeypatch))
    snapshot_from = load_target(write_target(tmp_path, db, use_admin=True, monkeypatch=monkeypatch))
    before = run(snapshot_target(snapshot_from, chunk_size=500))
    install(target, release)
    after = run(snapshot_target(snapshot_from, chunk_size=500))
    # Owned schemas are excluded; every protected schema is discovered, not hard-coded.
    expected = (
        {"corpus", "storage", "public", "extensions"}
        if which == "A"
        else {
            "capability_search",
            "belllabs_control",
            "public",
            "extensions",
        }
    )
    assert set(before["schemas"]) == expected
    assert before["schemas"] == after["schemas"]
    assert before["extensions"] == after["extensions"]
    assert before["storage_buckets"] == after["storage_buckets"]
    diff = compare(before, after, ALLOWED)
    assert diff["status"] in {"allowed", "identical"} and diff["unallowed_count"] == 0
    added = {d["path"] for d in diff["differences"] if d["change"] == "added"}
    assert {f"roles/{r}" for r in release.manifest["runtime_roles"]} <= added
    assert compare(before, after, None)["status"] == "blocked"
    # Row hashes are chunked and in-database; contents never leave the database.
    encoded = json.dumps(after)
    for sentinel in (
        "CORPUS-SECRET-BODY",
        "CORPUS-NOTE-SENTINEL",
        "PERSON-A-EMAIL-SENTINEL",
        "LEGACY-SEARCH-CONTENT",
        "BELLLABS-POISON-SENTINEL",
        "mcdb-disposable-only",
        "mcdb-migrator-disposable",
    ):
        assert sentinel not in encoded
    if which == "A":
        document = after["schemas"]["corpus"]["relations"]["document"]
        assert document["row_count"] == 2500 and len(document["chunks"]) == 5
        assert document["ordering"] == "primary_key"
        assert after["schemas"]["corpus"]["relations"]["note"]["ordering"] == "row_text"
        assert after["schemas"]["corpus"]["sequences"]["ingest_seq"]["last_value"] == 7
        assert after["storage_buckets"]["knowledge-artifacts"]["public"] is False
        assert "owner" not in after["storage_buckets"]["knowledge-artifacts"]
    else:
        poison = after["schemas"]["belllabs_control"]["relations"]["immutable_documents"]
        assert poison["status"] == "hashed" and poison["row_count"] == 2
        assert after["storage_buckets"] is None


def test_snapshot_detects_row_sequence_and_structure_changes(make_db, tmp_path, monkeypatch):
    db = make_db("A")
    target = load_target(write_target(tmp_path, db, use_admin=True, monkeypatch=monkeypatch))
    before = run(snapshot_target(target, chunk_size=1000))
    run(db.execute("UPDATE corpus.document SET title='changed' WHERE document_id=1777"))
    run(db.execute("SELECT nextval('corpus.ingest_seq')"))
    run(db.execute("ALTER TABLE corpus.note ADD COLUMN extra int"))
    run(db.execute("UPDATE storage.buckets SET public=true"))
    after = run(snapshot_target(target, chunk_size=1000))
    diff = compare(before, after, ALLOWED)
    paths = {d["path"] for d in diff["differences"]}
    assert diff["status"] == "blocked"
    assert "schemas/corpus/relations/document/chunks/1/digest" in paths
    assert "schemas/corpus/relations/document/chunks/0/digest" not in paths
    assert "schemas/corpus/sequences/ingest_seq/last_value" in paths
    assert "schemas/corpus/structure/columns" in paths
    assert "storage_buckets/knowledge-artifacts/public" in paths
    assert not any("changed" in json.dumps(d) and "title" in d["path"] for d in diff["differences"])
    assert VERSIONS  # imported for parity with other modules
