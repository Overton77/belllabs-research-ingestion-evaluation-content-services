from __future__ import annotations

import argparse
import asyncio
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from app.application.control_plane import ControlPlaneService
from app.application.control_plane_repository import BeanieDefinitionRepository
from app.application.reviewed_capability_promotion import (
    build_reviewed_capability_bundle,
    build_scenario_d_execution_correction,
    preflight_reviewed_capabilities,
    promote_reviewed_capabilities,
    publish_scenario_d_execution_correction,
)
from app.config import Settings
from app.domain.control_plane.contracts import AliasBinding, AliasRef, ExactDefinitionRef
from app.domain.control_plane.extensions import ExtensionRegistry
from app.integrations.catalog_projection_admin import list_published_definition_refs
from app.integrations.control_plane_payloads import InMemoryPayloadStore
from app.integrations.mongodb import create_mongodb
from app.models import DefinitionAliasDocument

PROJECT_ROOT = Path(__file__).resolve().parents[1]
WORKSPACE_ROOT = PROJECT_ROOT.parent
DEFAULT_PAYLOADS = PROJECT_ROOT / "app" / "domain" / "coordinator" / "reviewed_payloads"
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
        help="Mutate MongoDB. Without this flag, the command is read-only.",
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
    mongo_client, _database = await create_mongodb(settings)
    try:
        repository = BeanieDefinitionRepository()
        refs = await list_published_definition_refs()
        records = tuple([await repository.get(ref) for ref in refs])
        alias_documents = await DefinitionAliasDocument.find_all().to_list()
        aliases = tuple(
            AliasBinding(
                alias_ref=AliasRef(
                    kind=document.kind,
                    logical_id=document.logical_id,
                    alias=document.alias,
                ),
                target=ExactDefinitionRef(
                    kind=document.kind,
                    logical_id=document.logical_id,
                    revision=document.target_revision,
                    digest=document.target_digest,
                ),
                moved_at=document.moved_at,
                moved_by=document.moved_by,
            )
            for document in alias_documents
        )
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
        refreshed_refs = await list_published_definition_refs()
        refreshed_records = tuple(
            [await repository.get(ref) for ref in refreshed_refs]
        )
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
                "published": [
                    ref.model_dump(mode="json")
                    for ref in correction_result.published
                ],
                "reused": [
                    ref.model_dump(mode="json")
                    for ref in correction_result.reused
                ],
            },
        }
    finally:
        await mongo_client.close()


def main() -> None:
    print(json.dumps(asyncio.run(_run(_arguments())), sort_keys=True))


if __name__ == "__main__":
    main()
