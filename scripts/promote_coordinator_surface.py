from __future__ import annotations

import argparse
import asyncio
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from app.application.control_plane.service import ControlPlaneService
from app.application.control_plane.control_plane_repository import BeanieDefinitionRepository
from app.application.coordinator.coordinator_surface_promotion import (
    build_coordinator_surface,
    plan_coordinator_surface_promotion,
    publish_coordinator_surface,
)
from app.config import Settings
from app.domain.control_plane.extensions import ExtensionRegistry
from app.integrations.catalog_projection_admin import list_published_definition_refs
from app.integrations.control_plane_payloads import InMemoryPayloadStore
from app.integrations.mongodb import create_mongodb

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SKILL_ROOT = (
    PROJECT_ROOT / ".agents" / "skills" / "belllabs-workflow-coordinator"
)


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
    mongo_client, _database = await create_mongodb(settings)
    try:
        repository = BeanieDefinitionRepository()
        refs = await list_published_definition_refs()
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
        await mongo_client.close()


def main() -> None:
    print(json.dumps(asyncio.run(_run(_arguments())), sort_keys=True))


if __name__ == "__main__":
    main()
