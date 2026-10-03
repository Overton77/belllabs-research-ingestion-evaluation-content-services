from __future__ import annotations

import argparse
import asyncio
import json
from datetime import UTC, datetime
from importlib.resources import files
from pathlib import Path
from typing import Any

from biotech_mission_adapters.application.capabilities.reviewed_capability_promotion import (
    build_reviewed_capability_bundle,
    build_scenario_d_execution_correction,
    preflight_reviewed_capabilities,
    promote_reviewed_capabilities,
    publish_scenario_d_execution_correction,
)
from mission_control.adapters.postgres.connections import create_postgres_pool
from mission_control.adapters.postgres.control_plane.definition_repository import (
    PostgresDefinitionRepository,
)
from mission_control.adapters.storage.control_plane_payloads import InMemoryPayloadStore
from mission_control.application.authoring.service import ControlPlaneService
from mission_control.bootstrap.catalog_scope import configured_catalog_scope
from mission_control.bootstrap.settings import Settings
from mission_control.domain.authoring.extensions import ExtensionRegistry

# Operator workspace supplies skill bundles; reviewed snapshots ship with the adapter.
WORKSPACE_ROOT = Path.cwd()
DEFAULT_PAYLOADS = Path(str(files("biotech_mission_adapters") / "resources" / "reviewed_payloads"))
DEFAULT_SKILLS = WORKSPACE_ROOT / ".agents" / "skills"


def _arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Preflight or apply the exact reviewed Firecrawl, Tavily, and "
            "agent-browser catalog promotion."
        )
    )
    parser.add_argument("--reviewed-payloads", type=Path, default=DEFAULT_PAYLOADS)
    parser.add_argument("--skills-root", type=Path, default=DEFAULT_SKILLS)
    parser.add_argument("--actor", default="reviewed-capability-promotion")
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Publish to the configured PostgreSQL catalog. Without this flag, read only.",
    )
    parser.add_argument(
        "--retire-superseded",
        action="store_true",
        help=(
            "After every target publishes, retire revision-one rows that have no "
            "active external consumer or alias."
        ),
    )
    return parser.parse_args()


async def _run(args: argparse.Namespace) -> dict[str, Any]:
    bundle = build_reviewed_capability_bundle(
        reviewed_payloads=args.reviewed_payloads,
        workspace_skills=args.skills_root,
    )
    settings = Settings()
    scope = configured_catalog_scope(settings, requested=getattr(args, "tenant", None))
    postgres_pool = await create_postgres_pool(settings)
    try:
        repository = PostgresDefinitionRepository(postgres_pool, catalog_scope=scope)
        refs = await repository.list_published_definition_refs()
        records = tuple([await repository.get(ref) for ref in refs])
        aliases = await repository.list_alias_bindings()
        preflight = preflight_reviewed_capabilities(
            bundle=bundle,
            catalog_records=records,
        )
        correction = build_scenario_d_execution_correction(
            catalog_records=records,
        )
        if not args.apply:
            return {
                "mode": "preflight",
                "definition_count": len(bundle.definitions),
                "new_count": len(preflight.new),
                "advance_count": len(preflight.advance),
                "reuse_count": len(preflight.reuse),
                "target_refs": [ref.model_dump(mode="json") for ref in bundle.refs],
                "scenario_d_execution_correction_refs": [
                    ref.model_dump(mode="json") for ref in correction.refs
                ],
                "retirement_requested": bool(args.retire_superseded),
            }
        service = ControlPlaneService(
            repository,
            ExtensionRegistry(),
            InMemoryPayloadStore(),
        )
        result = await promote_reviewed_capabilities(
            service=service,
            bundle=bundle,
            catalog_records=records,
            aliases=aliases,
            actor_id=args.actor,
            changed_at=datetime.now(UTC),
            retire_superseded=bool(args.retire_superseded),
        )
        refreshed_refs = await repository.list_published_definition_refs()
        refreshed_records = tuple([await repository.get(ref) for ref in refreshed_refs])
        correction = build_scenario_d_execution_correction(
            catalog_records=refreshed_records,
        )
        correction_result = await publish_scenario_d_execution_correction(
            service=service,
            bundle=correction,
            catalog_records=refreshed_records,
            actor_id=args.actor,
            changed_at=datetime.now(UTC),
        )
        return {
            "mode": "applied",
            "published": [ref.model_dump(mode="json") for ref in result.published],
            "reused": [ref.model_dump(mode="json") for ref in result.reused],
            "retired": [ref.model_dump(mode="json") for ref in result.retired],
            "retained": [ref.model_dump(mode="json") for ref in result.retained],
            "retention_reasons": result.retention_reasons,
            "scenario_d_execution_correction": {
                "published": [ref.model_dump(mode="json") for ref in correction_result.published],
                "reused": [ref.model_dump(mode="json") for ref in correction_result.reused],
            },
        }
    finally:
        await postgres_pool.close()


def main() -> None:
    print(json.dumps(asyncio.run(_run(_arguments())), sort_keys=True))


if __name__ == "__main__":
    main()
