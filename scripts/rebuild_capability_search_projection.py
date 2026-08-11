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


def _default_generation() -> str:
    timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ")
    return f"capability-search-v1-openai-1536-{timestamp}"


def _arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Rebuild the disposable capability-search projection from MongoDB."
    )
    parser.add_argument("--tenant", required=True)
    parser.add_argument("--kind", choices=[kind.value for kind in DefinitionKind])
    parser.add_argument(
        "--generation",
        default=None,
        help="Stable generation identity for resumable rebuilds; defaults to a new value.",
    )
    parser.add_argument("--batch-size", type=int)
    return parser.parse_args()


async def _run(args: argparse.Namespace) -> dict[str, Any]:
    settings = Settings()
    generation = args.generation or _default_generation()
    mongo_client, _ = await create_mongodb(settings)
    postgres_pool = await create_postgres_pool(settings)
    try:
        definitions = BeanieDefinitionRepository()
        search = PostgresCatalogSearchRepository(postgres_pool)
        all_refs = await list_published_definition_refs()
        refs = filter_projection_refs(
            all_refs,
            kind=DefinitionKind(args.kind) if args.kind else None,
        )
        # Workflow compatibility is a cross-kind relationship. A targeted rebuild
        # must project only the selected kind while still deriving compatibility
        # from the complete immutable catalog.
        published = tuple([await definitions.get(ref) for ref in all_refs])
        workflow_compatibility = build_workflow_compatibility(published)
        projector = CatalogProjector(
            definitions=definitions,
            search=search,
            embeddings=OpenAICapabilityEmbeddingAdapter(settings),
            embedding_model_id=settings.capability_embedding_model,
            embedding_dimensions=settings.capability_embedding_dimensions,
            projection_generation=generation,
        )
        selected_kinds = (
            frozenset({DefinitionKind(args.kind)})
            if args.kind
            else frozenset(ref.kind for ref in refs)
        )
        rebuild = await rebuild_capability_search_projection(
            refs=refs,
            projector=projector,
            events=BeanieProjectionEventCompletionRepository(),
            generations=PostgresProjectionGenerationRepository(postgres_pool),
            tenant_scope=args.tenant,
            projection_generation=generation,
            selected_kinds=selected_kinds,
            workflow_compatibility=workflow_compatibility,
            batch_size=args.batch_size or settings.capability_projection_batch_size,
        )
        verification = await verify_capability_search_projection(
            refs=refs,
            definitions=definitions,
            search=search,
            tenant_scope=args.tenant,
            projection_generation=generation,
            embedding_model_id=settings.capability_embedding_model,
            embedding_dimensions=settings.capability_embedding_dimensions,
            search_document_format_version=1,
            selected_kinds=selected_kinds,
        )
        return {
            "rebuild": rebuild.model_dump(mode="json"),
            "verification": {
                **verification.model_dump(mode="json"),
                "valid": verification.valid,
            },
        }
    finally:
        await postgres_pool.close()
        await mongo_client.close()


def main() -> None:
    result = asyncio.run(_run(_arguments()))
    print(json.dumps(result, sort_keys=True))
    if not result["verification"]["valid"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
