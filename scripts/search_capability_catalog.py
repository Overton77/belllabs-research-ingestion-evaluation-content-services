from __future__ import annotations

import argparse
import asyncio
import json
from typing import Any

from mission_control.adapters.capabilities.capability_embeddings import (
    OpenAICapabilityEmbeddingAdapter,
)
from mission_control.adapters.postgres.capability.capability_search_repository import (
    PostgresCatalogSearchRepository,
)
from mission_control.adapters.postgres.connections import create_postgres_pool
from mission_control.adapters.postgres.control_plane.definition_repository import (
    PostgresDefinitionRepository,
)
from mission_control.application.capabilities.capability_search import CapabilitySearchService
from mission_control.bootstrap.catalog_scope import configured_catalog_scope
from mission_control.bootstrap.settings import Settings
from mission_control.domain.authoring.contracts import DefinitionKind
from mission_control.domain.coordinator.contracts import CapabilitySearchRequest


def _arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Hybrid-search the capability catalog and rehydrate exact PostgreSQL refs."
    )
    parser.add_argument("--tenant", required=True)
    parser.add_argument("--query", required=True)
    parser.add_argument("--workflow-type")
    parser.add_argument(
        "--kind",
        action="append",
        choices=[kind.value for kind in DefinitionKind],
        default=[],
    )
    parser.add_argument("--required-capability", action="append", default=[])
    parser.add_argument("--runtime")
    parser.add_argument("--operation-class")
    parser.add_argument("--limit", type=int, default=10)
    return parser.parse_args()


async def _run(args: argparse.Namespace) -> dict[str, Any]:
    settings = Settings()
    scope = configured_catalog_scope(settings, requested=getattr(args, "tenant", None))
    postgres_pool = await create_postgres_pool(settings)
    try:
        definitions = PostgresDefinitionRepository(postgres_pool, catalog_scope=scope)
        refs = await definitions.list_published_definition_refs()
        workflow_ref = None
        if args.workflow_type:
            matches = [
                ref
                for ref in refs
                if ref.kind == DefinitionKind.WORKFLOW_TYPE and ref.logical_id == args.workflow_type
            ]
            if not matches:
                raise ValueError("requested Workflow Type is not published")
            workflow_ref = max(matches, key=lambda ref: ref.revision)
        service = CapabilitySearchService(
            search=PostgresCatalogSearchRepository(postgres_pool),
            definitions=definitions,
            embeddings=OpenAICapabilityEmbeddingAdapter(settings),
            embedding_model_id=settings.capability_embedding_model,
            embedding_dimensions=settings.capability_embedding_dimensions,
        )
        response = await service.search(
            CapabilitySearchRequest(
                query=args.query,
                kinds=frozenset(DefinitionKind(kind) for kind in args.kind),
                tenant_scope=args.tenant,
                workflow_type_ref=workflow_ref,
                operation_class=args.operation_class,
                required_capabilities=frozenset(args.required_capability),
                runtime=args.runtime,
                limit=args.limit,
            )
        )
        return response.model_dump(mode="json")
    finally:
        await postgres_pool.close()


def main() -> None:
    print(json.dumps(asyncio.run(_run(_arguments())), sort_keys=True))


if __name__ == "__main__":
    main()
