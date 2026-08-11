from __future__ import annotations

import argparse
import asyncio
import json
from datetime import UTC, datetime
from typing import Any

from app.application.capability.catalog_projection import CatalogProjector
from app.application.capability.catalog_projection_admin import (
    filter_projection_refs,
    rebuild_capability_search_projection,
    verify_capability_search_projection,
)
from app.application.capability.catalog_projection_metadata import build_workflow_compatibility
from app.application.control_plane.control_plane_repository import BeanieDefinitionRepository
from app.application.capability.postgres_capability_search_generation_repository import (
    PostgresProjectionGenerationRepository,
)
from app.application.capability.postgres_capability_search_repository import (
    PostgresCatalogSearchRepository,
)
from app.config import Settings
from app.domain.control_plane.contracts import DefinitionKind
from app.integrations.capability_embeddings import OpenAICapabilityEmbeddingAdapter
from app.integrations.catalog_projection_admin import (
    BeanieProjectionEventCompletionRepository,
    list_published_definition_refs,
)
from app.integrations.mongodb import create_mongodb
from app.integrations.postgres import create_postgres_pool


def _arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Verify the capability-search projection against MongoDB authority."
    )
    parser.add_argument("--tenant", required=True)
    parser.add_argument("--kind", choices=[kind.value for kind in DefinitionKind])
    parser.add_argument(
        "--generation",
        help="Verify one generation instead of resolving active per asset kind.",
    )
    parser.add_argument(
        "--repair",
        action="store_true",
        help="Exit with a repair instruction; use the rebuild command to mutate projection rows.",
    )
    return parser.parse_args()


async def _run(args: argparse.Namespace) -> dict[str, Any]:
    settings = Settings()
    mongo_client, _ = await create_mongodb(settings)
    postgres_pool = await create_postgres_pool(settings)
    try:
        all_refs = await list_published_definition_refs()
        refs = filter_projection_refs(
            all_refs,
            kind=DefinitionKind(args.kind) if args.kind else None,
        )
        definitions = BeanieDefinitionRepository()
        search = PostgresCatalogSearchRepository(postgres_pool)
        generations = PostgresProjectionGenerationRepository(postgres_pool)
        grouped: dict[str, list] = {}
        missing_active_kinds: list[str] = []
        for ref in refs:
            generation = args.generation or await generations.active_for_kind(
                args.tenant,
                ref.kind,
            )
            if generation is None:
                missing_active_kinds.append(ref.kind.value)
                continue
            grouped.setdefault(generation, []).append(ref)
        verifications = []
        for generation, generation_refs in sorted(grouped.items()):
            selected_kinds = frozenset(ref.kind for ref in generation_refs)
            verification = await verify_capability_search_projection(
                refs=generation_refs,
                definitions=definitions,
                search=search,
                tenant_scope=args.tenant,
                projection_generation=generation,
                embedding_model_id=settings.capability_embedding_model,
                embedding_dimensions=settings.capability_embedding_dimensions,
                search_document_format_version=1,
                selected_kinds=selected_kinds,
            )
            verifications.append(
                {
                    **verification.model_dump(mode="json"),
                    "valid": verification.valid,
                }
            )
        valid = (
            not missing_active_kinds
            and bool(verifications or not refs)
            and all(item["valid"] for item in verifications)
        )
        repair = None
        if not valid and args.repair:
            repair_generation = _repair_generation()
            selected_kinds = (
                frozenset({DefinitionKind(args.kind)})
                if args.kind
                else frozenset(ref.kind for ref in refs)
            )
            # Repairing one kind still needs the complete catalog to preserve
            # cross-kind workflow compatibility metadata.
            published = tuple([await definitions.get(ref) for ref in all_refs])
            projector = CatalogProjector(
                definitions=definitions,
                search=search,
                embeddings=OpenAICapabilityEmbeddingAdapter(settings),
                embedding_model_id=settings.capability_embedding_model,
                embedding_dimensions=settings.capability_embedding_dimensions,
                projection_generation=repair_generation,
            )
            rebuilt = await rebuild_capability_search_projection(
                refs=refs,
                projector=projector,
                events=BeanieProjectionEventCompletionRepository(),
                generations=generations,
                tenant_scope=args.tenant,
                projection_generation=repair_generation,
                selected_kinds=selected_kinds,
                workflow_compatibility=build_workflow_compatibility(published),
                batch_size=settings.capability_projection_batch_size,
            )
            repaired_verification = await verify_capability_search_projection(
                refs=refs,
                definitions=definitions,
                search=search,
                tenant_scope=args.tenant,
                projection_generation=repair_generation,
                embedding_model_id=settings.capability_embedding_model,
                embedding_dimensions=settings.capability_embedding_dimensions,
                search_document_format_version=1,
                selected_kinds=selected_kinds,
            )
            repair = {
                "rebuild": rebuilt.model_dump(mode="json"),
                "verification": {
                    **repaired_verification.model_dump(mode="json"),
                    "valid": repaired_verification.valid,
                },
            }
            valid = repaired_verification.valid
        return {
            "tenant_scope": args.tenant,
            "verifications": verifications,
            "missing_active_kinds": sorted(set(missing_active_kinds)),
            "valid": valid,
            "repair_requested": bool(args.repair),
            "repair": repair,
        }
    finally:
        await postgres_pool.close()
        await mongo_client.close()


def main() -> None:
    result = asyncio.run(_run(_arguments()))
    print(json.dumps(result, sort_keys=True))
    if not result["valid"]:
        raise SystemExit(1)


def _repair_generation() -> str:
    timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ")
    return f"capability-search-repair-{timestamp}"


if __name__ == "__main__":
    main()
