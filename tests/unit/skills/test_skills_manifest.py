"""Behaviour of ``scripts/skills_manifest.py``: digests, drift detection and name rules."""

from __future__ import annotations

import hashlib
import importlib.util
import json
from pathlib import Path
from types import ModuleType

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
SCRIPT = REPO_ROOT / "scripts" / "skills_manifest.py"


def _load_script() -> ModuleType:
    spec = importlib.util.spec_from_file_location("skills_manifest", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


skills_manifest = _load_script()


def _digest(content: bytes) -> str:
    return f"sha256:{hashlib.sha256(content).hexdigest()}"


def _bundle(root: Path, name: str = "demo-skill") -> Path:
    bundle = root / name
    (bundle / "references").mkdir(parents=True)
    (bundle / "SKILL.md").write_bytes(b"---\nname: demo-skill\ndescription: demo\n---\nbody\n")
    (bundle / "references" / "b.md").write_bytes(b"b\n")
    (bundle / "references" / "a.md").write_bytes(b"a\n")
    return bundle


def _run(root: Path, mode: str) -> int:
    return skills_manifest.main([mode, "--root", str(root)])


def test_write_produces_sorted_digests_and_defaults(tmp_path: Path) -> None:
    bundle = _bundle(tmp_path)

    assert _run(tmp_path, "--write") == 0

    manifest = json.loads((bundle / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["schema_version"] == "mc.skill_bundle.v1"
    assert manifest["name"] == "demo-skill"
    assert manifest["version"] == "0.1.0"
    assert list(manifest["files"]) == sorted(manifest["files"])
    assert set(manifest["files"]) == {"SKILL.md", "references/a.md", "references/b.md"}
    assert manifest["files"]["references/a.md"] == _digest(b"a\n")
    assert "operation_catalog_digest" not in manifest
    assert _run(tmp_path, "--check") == 0


def test_write_is_byte_stable_and_preserves_version(tmp_path: Path) -> None:
    bundle = _bundle(tmp_path)
    _run(tmp_path, "--write")
    manifest = json.loads((bundle / "manifest.json").read_text(encoding="utf-8"))
    manifest["version"] = "0.2.0"
    manifest["service_contract_range"] = ">=0.2.0,<0.3.0"
    (bundle / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")

    _run(tmp_path, "--write")
    first = (bundle / "manifest.json").read_bytes()
    _run(tmp_path, "--write")

    assert (bundle / "manifest.json").read_bytes() == first
    rewritten = json.loads(first)
    assert rewritten["version"] == "0.2.0"
    assert rewritten["service_contract_range"] == ">=0.2.0,<0.3.0"


def test_changed_file_fails_check_and_names_the_path(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    bundle = _bundle(tmp_path)
    _run(tmp_path, "--write")
    (bundle / "references" / "a.md").write_bytes(b"changed\n")
    capsys.readouterr()

    assert _run(tmp_path, "--check") == 1

    assert "references/a.md" in capsys.readouterr().out


def test_added_file_fails_check(tmp_path: Path) -> None:
    bundle = _bundle(tmp_path)
    _run(tmp_path, "--write")
    (bundle / "references" / "c.md").write_bytes(b"new\n")

    assert _run(tmp_path, "--check") == 1


def test_missing_manifest_fails_check(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    _bundle(tmp_path)

    assert _run(tmp_path, "--check") == 1

    assert "MISSING" in capsys.readouterr().out


def test_directory_and_manifest_name_mismatch_fails_check(tmp_path: Path) -> None:
    bundle = _bundle(tmp_path)
    _run(tmp_path, "--write")
    manifest = json.loads((bundle / "manifest.json").read_text(encoding="utf-8"))
    manifest["name"] = "other-name"
    (bundle / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")

    assert _run(tmp_path, "--check") == 1


def test_operation_catalog_digest_tracks_operations_json(tmp_path: Path) -> None:
    bundle = _bundle(tmp_path)
    operations = bundle / "references" / "operations.json"
    operations.write_bytes(b'{"operations": []}\n')
    _run(tmp_path, "--write")
    manifest = json.loads((bundle / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["operation_catalog_digest"] == _digest(b'{"operations": []}\n')

    manifest["operation_catalog_digest"] = _digest(b"stale")
    (bundle / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    assert _run(tmp_path, "--check") == 1

    _run(tmp_path, "--write")
    operations.write_bytes(b'{"operations": ["x"]}\n')
    assert _run(tmp_path, "--check") == 1
    _run(tmp_path, "--write")
    rewritten = json.loads((bundle / "manifest.json").read_text(encoding="utf-8"))
    assert rewritten["operation_catalog_digest"] == _digest(b'{"operations": ["x"]}\n')


def test_directories_without_skill_md_are_not_bundles(tmp_path: Path) -> None:
    _bundle(tmp_path)
    (tmp_path / "not-a-bundle").mkdir()
    (tmp_path / "not-a-bundle" / "notes.txt").write_text("x", encoding="utf-8")
    _run(tmp_path, "--write")

    assert _run(tmp_path, "--check") == 0
    assert not (tmp_path / "not-a-bundle" / "manifest.json").exists()


def test_repository_bundles_are_clean() -> None:
    assert skills_manifest.main(["--check", "--root", str(REPO_ROOT / "skills")]) == 0


@pytest.mark.parametrize(
    "skill_md",
    [
        b"no frontmatter\n",
        b"---\nname: wrong-name\ndescription: x\n---\nbody\n",
        b"---\nname: demo-skill\n---\nbody\n",
    ],
)
def test_bad_frontmatter_fails_check(tmp_path: Path, skill_md: bytes) -> None:
    bundle = _bundle(tmp_path)
    (bundle / "SKILL.md").write_bytes(skill_md)
    _run(tmp_path, "--write")

    assert _run(tmp_path, "--check") == 1
