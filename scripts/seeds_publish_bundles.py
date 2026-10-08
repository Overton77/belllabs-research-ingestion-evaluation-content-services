"""Upload the agent-skill seed bundles' bytes through the FT-A2 custody path (FT-A7).

For one target application, every ``skill_bundle`` and ``hook_script`` row the
``mc.app.<app>.agent-skills`` seed bundle registers is rebuilt from its source directory
(``packages/mission-control-db-contract/seeds/agent_skills.py``), checked against the
committed seed JSON (the row's ``bundle_ref`` object prefix and manifest digest must equal
the rebuilt manifest's), and uploaded with ``BundleCustodyService.upload``: missing objects
are written to their digest paths, existing objects are re-read and compared, nothing is
ever overwritten. A second run uploads nothing. Registration is the seed itself
(``mission-db seed-apply``), so this script never writes catalog rows.

    uv run python scripts/seeds_publish_bundles.py --target biotech --storage local:.runtime/bundles
    uv run python scripts/seeds_publish_bundles.py --target ai-engineer --storage supabase

``--storage supabase`` uses the deployment's publisher credential
(``CAPABILITY_BUNDLE_PUBLISHER_TOKEN``, never the service key) against ``SUPABASE_URL``;
it is the live path and needs an owner-approved target. ``--check`` only verifies the seed
JSON against the sources. Output is one JSON report; exit 0 on success, 1 on any drift.
"""

from __future__ import annotations

import argparse
import asyncio
import importlib.util
import json
import sys
from pathlib import Path
from typing import Any

from mission_control.application.capabilities.bundle_custody import (
    BundleCustodyService,
    BundleObjectStore,
)

ROOT = Path(__file__).resolve().parents[1]
SEEDS = ROOT / "packages" / "mission-control-db-contract" / "seeds"


def agent_skills_module() -> Any:
    spec = importlib.util.spec_from_file_location("mc_agent_skills", SEEDS / "agent_skills.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def committed_bundle_refs(application_id: str) -> dict[str, dict[str, str]]:
    """``logical_id -> {uri, digest}`` of every bundle_ref the committed seed registers."""

    module = agent_skills_module()
    path = SEEDS / application_id / f"{module.APP_KEYS[application_id]}-{module.SEED_VERSION}.json"
    bundle = json.loads(path.read_text(encoding="utf-8"))
    refs: dict[str, dict[str, str]] = {}
    for record in bundle["records"]:
        if record["kind"] != "asset_version":
            continue
        definition = record["fields"]["manifest"].get("definition", {})
        bundle_ref = definition.get("bundle_ref")
        if bundle_ref:
            refs[definition["logical_id"]] = {
                "uri": bundle_ref["uri"],
                "digest": bundle_ref["digest"],
                "manifest_digest": definition["manifest_digest"],
            }
    return refs


def check_seed(application_id: str) -> list[str]:
    """Drift between the sources and the committed seed JSON (empty when they agree)."""

    module = agent_skills_module()
    committed = committed_bundle_refs(application_id)
    problems: list[str] = []
    expected: set[str] = set()
    for entry in module.application_definitions(application_id):
        manifest = entry["manifest"]
        if manifest is None:
            continue
        expected.add(manifest.capability_id)
        ref = committed.get(manifest.capability_id)
        uri = f"capability-bundles://{manifest.object_prefix}"
        if ref is None:
            problems.append(f"{manifest.capability_id}: not in the committed seed")
        elif (ref["uri"], ref["digest"], ref["manifest_digest"]) != (
            uri,
            manifest.digest,
            manifest.digest,
        ):
            problems.append(f"{manifest.capability_id}: seed references a different prefix")
    problems += [f"{name}: committed but not produced" for name in set(committed) - expected]
    return problems


async def publish_application(application_id: str, store: BundleObjectStore) -> dict[str, Any]:
    """Upload every bundle of one application; idempotent (a rerun uploads nothing)."""

    module = agent_skills_module()
    custody = BundleCustodyService(store=store, actor_ref="seed:mission-control-catalog")
    reports: list[dict[str, Any]] = []
    for entry in module.application_definitions(application_id):
        manifest = entry["manifest"]
        if manifest is None:
            continue
        report = await custody.upload(manifest, entry["source"]["files"])
        reports.append(
            {
                "capability_id": manifest.capability_id,
                "pin": entry["pin"],
                "manifest_digest": manifest.digest,
                "object_prefix": manifest.object_prefix,
                "computed_hash": manifest.computed_hash,
                "uploaded": list(report.uploaded),
                "verified_existing": list(report.verified_existing),
            }
        )
    return {
        "application_id": application_id,
        "bundles": reports,
        "uploaded_objects": sum(len(item["uploaded"]) for item in reports),
        "verified_objects": sum(len(item["verified_existing"]) for item in reports),
    }


def open_store(storage: str) -> BundleObjectStore:
    if storage.startswith("local:"):
        from mission_control.adapters.capabilities.local_bundle_store import (
            FilesystemBundleObjectStore,
        )

        return FilesystemBundleObjectStore(Path(storage.removeprefix("local:")))
    if storage == "supabase":
        import httpx

        from mission_control.adapters.supabase_storage.bundles import (
            configured_supabase_bundle_custody,
        )
        from mission_control.bootstrap.settings import get_settings

        return configured_supabase_bundle_custody(
            get_settings(),
            http_client=httpx.Client(timeout=120, follow_redirects=False),
            credential="publisher",
        )
    raise SystemExit("--storage is local:<directory> or supabase")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--target", required=True, choices=("biotech", "ai-engineer"))
    parser.add_argument("--storage", default=None)
    parser.add_argument("--check", action="store_true", help="verify the seed JSON only")
    args = parser.parse_args(argv)
    problems = check_seed(args.target)
    result: dict[str, Any] = {"application_id": args.target, "seed_drift": problems}
    if not problems and not args.check:
        if args.storage is None:
            raise SystemExit("--storage is required unless --check")
        result.update(asyncio.run(publish_application(args.target, open_store(args.storage))))
    print(json.dumps(result, indent=2, sort_keys=True))
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
