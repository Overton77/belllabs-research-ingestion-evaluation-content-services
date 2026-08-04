from __future__ import annotations

import argparse
import asyncio
import json
import socket
from datetime import timedelta

from app.application.catalog_projection import CatalogProjector
from app.application.catalog_projection_events import (
    CatalogProjectionEventProcessor,
)
from app.application.control_plane_repository import BeanieDefinitionRepository
from app.application.postgres_capability_search_generation_repository import (
    PostgresProjectionGenerationRepository,
)
from app.application.postgres_capability_search_repository import (
    PostgresCatalogSearchRepository,
)
from app.config import Settings
from app.integrations.capability_embeddings import (
    OpenAICapabilityEmbeddingAdapter,
)
from app.integrations.catalog_projection_events import (
    BeanieProjectionEventRepository,
)
from app.integrations.mongodb import create_mongodb
from app.integrations.postgres import create_postgres_pool


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
    mongo_client, _ = await create_mongodb(settings)
    postgres_pool = await create_postgres_pool(settings)
    try:
        definitions = BeanieDefinitionRepository()
        search = PostgresCatalogSearchRepository(postgres_pool)
        embeddings = OpenAICapabilityEmbeddingAdapter(settings)
        processor = CatalogProjectionEventProcessor(
            events=BeanieProjectionEventRepository(),
            generations=PostgresProjectionGenerationRepository(postgres_pool),
            projector_factory=lambda generation: CatalogProjector(
                definitions=definitions,
                search=search,
                embeddings=embeddings,
                embedding_model_id=settings.capability_embedding_model,
                embedding_dimensions=settings.capability_embedding_dimensions,
                projection_generation=generation,
            ),
            lease_duration=timedelta(
                seconds=settings.capability_projection_lease_seconds
            ),
            max_attempts=settings.capability_projection_max_attempts,
            base_backoff=timedelta(
                seconds=settings.capability_projection_base_backoff_seconds
            ),
            max_backoff=timedelta(
                seconds=settings.capability_projection_max_backoff_seconds
            ),
        )
        summary = await processor.process_batch(
            owner=args.owner,
            limit=args.limit or settings.capability_projection_batch_size,
        )
        return summary.model_dump(mode="json")
    finally:
        await postgres_pool.close()
        await mongo_client.close()


def main() -> None:
    print(json.dumps(asyncio.run(_run(_arguments())), sort_keys=True))


if __name__ == "__main__":
    main()
