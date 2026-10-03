"""Publish a reviewed directory to the offline immutable store; print its exact skill pin.

Does not contact providers, run skill scripts, alter deployment pins, or admit a catalog
asset. The definition digest must be supplied from the reviewed immutable definition.
"""

from __future__ import annotations

import argparse
from pathlib import Path

from mission_control.adapters.capabilities.capability_bundles import (
    DirectoryBundleStore,
    bytes_digest,
)
from mission_control.adapters.capabilities.capability_pins import PinnedExactRef, PinnedSkill
from mission_control.bootstrap.settings import PROJECT_ROOT
from mission_control.domain.authoring.contracts import DefinitionKind


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    parser.add_argument("--asset-id", required=True)
    parser.add_argument("--version", required=True, type=int)
    parser.add_argument("--skill-name", required=True)
    parser.add_argument("--definition-digest", required=True)
    parser.add_argument(
        "--store", type=Path, default=PROJECT_ROOT / ".runtime" / "capability-bundles"
    )
    args = parser.parse_args()
    # Validate identity before publishing any bytes.
    ref = PinnedExactRef(
        kind=DefinitionKind.SKILL,
        logical_id=args.asset_id,
        revision=args.version,
        digest=args.definition_digest,
    )
    store = DirectoryBundleStore(args.store)
    manifest = store.publish(args.directory, asset_id=ref.logical_id, version=ref.revision)
    _, files = store.load(manifest.digest)
    pin = PinnedSkill(
        ref=ref,
        skill_name=args.skill_name,
        source_locator="capability-bundles://" + manifest.digest,
        bundle_digest=manifest.bundle_digest,
        skill_md_digest=bytes_digest(dict(files)["SKILL.md"]),
        digest_format="bytes_v1",
        mount_root="/skills/" + args.skill_name,
    )
    pin.bundle(store=store)
    print(pin.model_dump_json(indent=2))


if __name__ == "__main__":
    main()
