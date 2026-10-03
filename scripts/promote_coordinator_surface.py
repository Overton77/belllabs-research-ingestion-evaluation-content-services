from __future__ import annotations

import argparse
import asyncio
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from mission_control.adapters.postgres.connections import create_postgres_pool
from mission_control.adapters.postgres.control_plane.definition_repository import (
    PostgresDefinitionRepository,
)
from mission_control.adapters.storage.control_plane_payloads import InMemoryPayloadStore
from mission_control.application.authoring.service import ControlPlaneService
from mission_control.application.coordinator.coordinator_surface_promotion import (
    build_coordinator_surface,
    plan_coordinator_surface_promotion,
    publish_coordinator_surface,
)
from mission_control.bootstrap.catalog_scope import configured_catalog_scope
from mission_control.bootstrap.settings import Settings
from mission_control.domain.authoring.extensions import ExtensionRegistry

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SKILL_ROOT = PROJECT_ROOT / ".agents" / "skills" / "mission-control-coordinator"


def _arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Preflight or publish the exact coordinator skill and reviewed prompt."
    )
    parser.add_argument("--skill-root", type=Path, default=DEFAULT_SKILL_ROOT)
    parser.add_argument("--actor", default="coordinator-surface-promotion")
    parser.add_argument("--apply", action="store_true")
    return parser.parse_args()


async def _run(args: argparse.Namespace) -> dict[str, Any]:
    definitions = build_coordinator_surface(args.skill_root)
    settings = Settings()
    scope = configured_catalog_scope(settings, requested=getattr(args, "tenant", None))
    postgres_pool = await create_postgres_pool(settings)
    try:
        repository = PostgresDefinitionRepository(postgres_pool, catalog_scope=scope)
        refs = await repository.list_published_definition_refs()
        records = tuple([await repository.get(ref) for ref in refs])
        plan = plan_coordinator_surface_promotion(definitions, records)
        if not args.apply:
            return {
                "mode": "preflight",
                "publish_count": len(plan.definitions),
                "reuse_count": len(plan.reused),
                "reused": [ref.model_dump(mode="json") for ref in plan.reused],
            }
        service = ControlPlaneService(
            repository,
            ExtensionRegistry(),
            InMemoryPayloadStore(),
        )
        published = await publish_coordinator_surface(
            service=service,
            plan=plan,
            actor_id=args.actor,
            published_at=datetime.now(UTC),
        )
        return {
            "mode": "applied",
            "published": [ref.model_dump(mode="json") for ref in published],
            "reused": [ref.model_dump(mode="json") for ref in plan.reused],
        }
    finally:
        await postgres_pool.close()


def main() -> None:
    print(json.dumps(asyncio.run(_run(_arguments())), sort_keys=True))


if __name__ == "__main__":
    main()
