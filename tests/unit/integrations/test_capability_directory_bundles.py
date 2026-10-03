from pathlib import Path

import pytest

from mission_control.adapters.capabilities.capability_bundles import (
    BundleError,
    DirectoryBundleStore,
    bundle_digest,
    bytes_digest,
    directory_files,
    file_manifest,
    safe_relative_path,
)
from mission_control.adapters.capabilities.capability_pins import PinnedSkill, read_skill_bundle
from mission_control.adapters.deep_agents.materializer import ResolvedSkillBundle
from mission_control.domain.execution.errors import DeepAgentRuntimeDrift


def source(tmp_path: Path) -> Path:
    path = tmp_path / "source"
    path.mkdir()
    (path / "SKILL.md").write_bytes(b"---\nname: fixture\ndescription: test\n---\n")
    (path / ".config").write_bytes(b"complete hidden file")
    (path / "assets").mkdir()
    (path / "assets" / "image.bin").write_bytes(b"\x00\xff\x01")
    return path


def test_complete_binary_directory_is_immutable_and_materializer_compatible(tmp_path: Path) -> None:
    directory = source(tmp_path)
    store = DirectoryBundleStore(tmp_path / "store")
    manifest = store.publish(directory, asset_id="skill.fixture", version=1)
    assert store.publish(directory, asset_id="skill.fixture", version=1) == manifest
    loaded, files = store.load(manifest.digest)
    assert loaded == manifest
    assert {name for name, _ in files} == {"SKILL.md", ".config", "assets/image.bin"}
    pin = PinnedSkill.model_validate(
        {
            "ref": {
                "kind": "skill",
                "logical_id": "skill.fixture",
                "revision": 1,
                "digest": manifest.digest,
            },
            "skill_name": "fixture",
            "source_locator": "capability-bundles://" + manifest.digest,
            "bundle_digest": manifest.bundle_digest,
            "digest_format": "bytes_v1",
            "skill_md_digest": bytes_digest(dict(files)["SKILL.md"]),
            "mount_root": "/skills/fixture",
        }
    )
    resolved = pin.bundle(store=store)
    assert resolved.files == files
    resolved.verify(skill_md_digest=pin.skill_md_digest)
    assert read_skill_bundle(directory, digest_format="bytes_v1") == resolved
    (directory / "SKILL.md").write_bytes(b"changed")
    with pytest.raises(BundleError, match="conflicts"):
        store.publish(directory, asset_id="skill.fixture", version=1)
    assert store.load(manifest.digest)[1] == files
    assert store.publish(directory, asset_id="skill.fixture", version=2).digest != manifest.digest


@pytest.mark.parametrize(
    "path",
    [
        "../escape",
        "/absolute",
        "a/../x",
        "a//x",
        "./x",
        "C:/escape",
        "x:stream",
        "a\\x",
        "CON",
        "NUL.txt",
        "a/COM1.txt",
        "a. /x",
        "x.",
        "x\x00",
        "",
    ],
)
def test_portable_paths_reject_escapes_and_aliases(path: str) -> None:
    with pytest.raises(BundleError):
        safe_relative_path(path)


@pytest.mark.parametrize(
    "extra", [(("skill.md", b"x"),), (("a", b"x"), ("a/b", b"x")), (("x", b"a"), ("x", b"b"))]
)
def test_manifest_rejects_collisions(extra: tuple[tuple[str, bytes], ...]) -> None:
    with pytest.raises(BundleError):
        file_manifest((("SKILL.md", b"ok"), *extra))


def test_store_rejects_corruption_and_unpublished_version(tmp_path: Path) -> None:
    store = DirectoryBundleStore(tmp_path / "store")
    manifest = store.publish(source(tmp_path), asset_id="fixture", version=1)
    entry = manifest.files[0]
    (store.root / "objects" / entry.digest[7:]).write_bytes(b"corruption")
    with pytest.raises(BundleError, match="digest/size"):
        store.load(manifest.digest)


def test_directory_rejects_hardlinks(tmp_path: Path) -> None:
    directory = source(tmp_path)
    (directory / "linked").hardlink_to(directory / "SKILL.md")
    with pytest.raises(BundleError, match="hard link"):
        directory_files(directory)


def test_directory_rejects_symlink_before_read(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    directory = source(tmp_path)
    original = Path.is_symlink
    monkeypatch.setattr(Path, "is_symlink", lambda self: self.name == "assets" or original(self))
    with pytest.raises(BundleError, match="symlink"):
        directory_files(directory)


def test_raw_manifest_rejects_tampered_bytes() -> None:
    files = (("SKILL.md", b"skill"), ("asset", b"\xff"))
    bundle = ResolvedSkillBundle(bundle_digest(files), files, "bytes_v1")
    bundle.verify(skill_md_digest=bytes_digest(b"skill"))
    bad = ResolvedSkillBundle(bundle.bundle_digest, (("SKILL.md", b"evil"),), "bytes_v1")
    with pytest.raises(DeepAgentRuntimeDrift):
        bad.verify(skill_md_digest=bytes_digest(b"skill"))


@pytest.mark.asyncio
async def test_state_backend_rejects_binary_without_silently_dropping_assets() -> None:
    from dataclasses import replace

    from deepagents.backends import StateBackend

    from mission_control.adapters.deep_agents.materializer import ExactDeepAgentMaterializer
    from mission_control.domain.execution.errors import DeepAgentUnsupportedPlacement
    from tests.acceptance.control_plane.test_wp_cp_040 import exact_fixture, registry

    binding, _, original = exact_fixture()
    files = (*original.files, ("asset.bin", b"\xff"))
    bundle = ResolvedSkillBundle(bundle_digest(files), files, "bytes_v1")
    skill = binding.skills[0].model_copy(
        update={
            "bundle_digest": bundle.bundle_digest,
            "skill_md_digest": bytes_digest(dict(files)["SKILL.md"]),
        }
    )
    binding = binding.model_copy(update={"skills": (skill,)})
    components = replace(registry(binding, original), skill_bundles={bundle.bundle_digest: bundle})
    materializer = ExactDeepAgentMaterializer(components)
    with pytest.raises(DeepAgentUnsupportedPlacement, match="byte-capable"):
        await materializer._mount_skills(binding, StateBackend())


@pytest.mark.asyncio
async def test_mount_collisions_fail_before_backend_upload() -> None:
    from deepagents.backends import StateBackend

    from mission_control.adapters.deep_agents.materializer import ExactDeepAgentMaterializer
    from tests.acceptance.control_plane.test_wp_cp_040 import exact_fixture, registry

    binding, _, bundle = exact_fixture()
    materializer = ExactDeepAgentMaterializer(registry(binding, bundle))
    binding = binding.model_copy(update={"skills": (binding.skills[0], binding.skills[0])})
    with pytest.raises(DeepAgentRuntimeDrift, match="overlap"):
        await materializer._mount_skills(binding, StateBackend())
