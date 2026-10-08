from __future__ import annotations

import argparse
import asyncio
import json
from datetime import UTC, datetime
from typing import Any

from mission_control.adapters.capabilities.capability_embeddings import (
    OpenAICapabilityEmbeddingAdapter,
)
from mission_control.adapters.postgres.capability.capability_search_generation_repository import (
    PostgresProjectionGenerationRepository,
)
from mission_control.adapters.postgres.capability.capability_search_repository import (
    PostgresCatalogSearchRepository,
)
from mission_control.adapters.postgres.capability.projection_events import (
    PostgresProjectionEventRepository,
)
from mission_control.adapters.postgres.connections import create_postgres_pool
from mission_control.adapters.postgres.control_plane.definition_repository import (
    PostgresDefinitionRepository,
)
from mission_control.application.capabilities.catalog_projection import CatalogProjector
from mission_control.application.capabilities.catalog_projection_admin import (
    filter_projection_refs,
    rebuild_capability_search_projection,
    verify_capability_search_projection,
)
from mission_control.application.capabilities.catalog_projection_metadata import (
    build_workflow_compatibility,
)
from mission_control.bootstrap.catalog_scope import configured_catalog_scope
from mission_control.bootstrap.settings import Settings
from mission_control.domain.authoring.contracts import DefinitionKind


def _default_generation(lexical_only: bool = False) -> str:
    timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ")
    route = "lexical" if lexical_only else "openai-1536"
    return f"capability-search-v1-{route}-{timestamp}"


def _arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Rebuild the disposable capability-search projection from PostgreSQL."
    )
    parser.add_argument("--tenant", required=True)
    parser.add_argument("--kind", choices=[kind.value for kind in DefinitionKind])
    parser.add_argument(
        "--generation",
        default=None,
        help="Stable generation identity for resumable rebuilds; defaults to a new value.",
    )
    parser.add_argument("--batch-size", type=int)
    parser.add_argument(
        "--lexical-only",
        action="store_true",
        help=(
            "Write lexical search columns only (no embedding provider call); search runs "
            "lexical until a later rebuild with embeddings fills the vectors (FT-A3)."
        ),
    )
    return parser.parse_args()


async def _run(args: argparse.Namespace) -> dict[str, Any]:
    settings = Settings()
    generation = args.generation or _default_generation(args.lexical_only)
    scope = configured_catalog_scope(settings, requested=getattr(args, "tenant", None))
    postgres_pool = await create_postgres_pool(settings)
    try:
        definitions = PostgresDefinitionRepository(postgres_pool, catalog_scope=scope)
        search = PostgresCatalogSearchRepository(postgres_pool)
        all_refs = await definitions.list_published_definition_refs()
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
            embeddings=None if args.lexical_only else OpenAICapabilityEmbeddingAdapter(settings),
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
            events=PostgresProjectionEventRepository(postgres_pool, catalog_scope=scope),
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


def main() -> None:
    result = asyncio.run(_run(_arguments()))
    print(json.dumps(result, sort_keys=True))
    if not result["verification"]["valid"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
