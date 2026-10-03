"""Operator-only: stage a reviewed directory, verify bytes, and register its exact pin.

Uses deployment SUPABASE_*, CAPABILITY_BUNDLE_NAMESPACE, MISSION_CONTROL_CATALOG_SCOPE,
and application PostgreSQL settings. Does not apply migrations, amend deployment pins,
execute scripts, delete orphan objects, or obtain credentials from request arguments.
"""

from __future__ import annotations

import argparse
import asyncio
from pathlib import Path

import asyncpg
import httpx

from mission_control.adapters.capabilities.capability_bundles import BundleError, bytes_digest
from mission_control.adapters.capabilities.capability_pins import PinnedExactRef, PinnedSkill
from mission_control.adapters.postgres.capability_bundles import PostgresCapabilityBundleAdmissions
from mission_control.adapters.supabase_storage.bundles import configured_supabase_bundle_reader
from mission_control.bootstrap.settings import get_settings
from mission_control.domain.authoring.contracts import DefinitionKind


async def register(args: argparse.Namespace) -> PinnedSkill:
    settings = get_settings()
    if not settings.mission_control_catalog_scope or not settings.capability_bundle_namespace:
        raise BundleError("trusted catalog scope and storage namespace must be configured")
    ref = PinnedExactRef(
        kind=DefinitionKind.SKILL,
        logical_id=args.asset_id,
        revision=args.version,
        digest=args.definition_digest,
    )
    with httpx.Client(timeout=30, follow_redirects=False) as http:
        reader = configured_supabase_bundle_reader(settings, http_client=http)
        manifest = await asyncio.to_thread(
            reader.stage, args.directory, asset_id=ref.logical_id, version=ref.revision
        )
        _, files = await asyncio.to_thread(reader.load, manifest.digest)
        pin = PinnedSkill(
            ref=ref,
            skill_name=args.skill_name,
            source_locator="capability-bundles://" + manifest.digest,
            bundle_digest=manifest.bundle_digest,
            skill_md_digest=bytes_digest(dict(files)["SKILL.md"]),
            digest_format="bytes_v1",
            mount_root="/skills/" + args.skill_name,
        )
        pool = await asyncpg.create_pool(settings.application_postgres_dsn, min_size=1, max_size=1)
        try:
            await PostgresCapabilityBundleAdmissions(
                pool,
                catalog_scope=settings.mission_control_catalog_scope,
                storage_namespace=settings.capability_bundle_namespace,
            ).register(pin, reader=reader)
        finally:
            await pool.close()
    return pin


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    parser.add_argument("--asset-id", required=True)
    parser.add_argument("--version", required=True, type=int)
    parser.add_argument("--skill-name", required=True)
    parser.add_argument("--definition-digest", required=True)
    args = parser.parse_args()
    try:
        pin = asyncio.run(register(args))
    except Exception as error:
        # SDK/DB connection exception strings can include deployment endpoints/details.
        raise SystemExit(
            f"Bundle registration failed ({type(error).__name__}); staged objects are retained. "
            "Registration outcome may require retry with the same pin. No version was overwritten."
        ) from None
    print(pin.model_dump_json(indent=2))


if __name__ == "__main__":
    main()
