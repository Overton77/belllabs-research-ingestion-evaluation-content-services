from __future__ import annotations

import argparse
import asyncio
import json
import socket
from datetime import timedelta

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
from mission_control.application.capabilities.catalog_projection_events import (
    CatalogProjectionEventProcessor,
)
from mission_control.bootstrap.catalog_scope import configured_catalog_scope
from mission_control.bootstrap.settings import Settings


def _arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Claim and process a bounded batch of capability projection events."
    )
    parser.add_argument(
        "--owner",
        default=f"{socket.gethostname()}:projection-worker",
    )
    parser.add_argument("--limit", type=int)
    return parser.parse_args()


async def _run(args: argparse.Namespace) -> dict[str, object]:
    settings = Settings()
    scope = configured_catalog_scope(settings, requested=getattr(args, "tenant", None))
    postgres_pool = await create_postgres_pool(settings)
    try:
        definitions = PostgresDefinitionRepository(postgres_pool, catalog_scope=scope)
        search = PostgresCatalogSearchRepository(postgres_pool)
        embeddings = OpenAICapabilityEmbeddingAdapter(settings)
        processor = CatalogProjectionEventProcessor(
            events=PostgresProjectionEventRepository(postgres_pool, catalog_scope=scope),
            generations=PostgresProjectionGenerationRepository(postgres_pool),
            projector_factory=lambda generation: CatalogProjector(
                definitions=definitions,
                search=search,
                embeddings=embeddings,
                embedding_model_id=settings.capability_embedding_model,
                embedding_dimensions=settings.capability_embedding_dimensions,
                projection_generation=generation,
            ),
            lease_duration=timedelta(seconds=settings.capability_projection_lease_seconds),
            max_attempts=settings.capability_projection_max_attempts,
            base_backoff=timedelta(seconds=settings.capability_projection_base_backoff_seconds),
            max_backoff=timedelta(seconds=settings.capability_projection_max_backoff_seconds),
        )
        summary = await processor.process_batch(
            owner=args.owner,
            limit=args.limit or settings.capability_projection_batch_size,
        )
        return summary.model_dump(mode="json")
    finally:
        await postgres_pool.close()


def main() -> None:
    print(json.dumps(asyncio.run(_run(_arguments())), sort_keys=True))


if __name__ == "__main__":
    main()
