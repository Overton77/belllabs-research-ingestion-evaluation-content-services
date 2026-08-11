from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path
from statistics import median
from time import perf_counter
from typing import Any, TypedDict

from app.application.capability.capability_search import (
    CapabilitySearchResponse,
    CapabilitySearchService,
)
from app.application.control_plane.control_plane_repository import BeanieDefinitionRepository
from app.application.capability.postgres_capability_search_repository import (
    PostgresCatalogSearchRepository,
)
from app.config import Settings
from app.domain.control_plane.contracts import DefinitionKind
from app.domain.coordinator.contracts import (
    CapabilitySearchHit,
    CapabilitySearchRequest,
)
from app.integrations.capability_embeddings import OpenAICapabilityEmbeddingAdapter
from app.integrations.mongodb import create_mongodb
from app.integrations.postgres import create_postgres_pool

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DATASET = (
    PROJECT_ROOT / "tests" / "fixtures" / "coordinator_retrieval_evaluation.json"
)


class AggregatedTokenMetric(TypedDict):
    metric_kind: str
    character_count: int
    estimated_tokens: int
    method: str


def _arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Evaluate live coordinator retrieval without exact-name hints."
    )
    parser.add_argument("--tenant", default="global")
    parser.add_argument("--dataset", type=Path, default=DEFAULT_DATASET)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--limit", type=int, default=50)
    return parser.parse_args()


async def _evaluate(
    *,
    tenant: str,
    dataset: dict[str, Any],
    limit: int,
) -> dict[str, Any]:
    settings = Settings()
    mongo_client, _database = await create_mongodb(settings)
    postgres_pool = await create_postgres_pool(settings)
    try:
        service = CapabilitySearchService(
            search=PostgresCatalogSearchRepository(postgres_pool),
            definitions=BeanieDefinitionRepository(),
            embeddings=OpenAICapabilityEmbeddingAdapter(settings),
            embedding_model_id=settings.capability_embedding_model,
            embedding_dimensions=settings.capability_embedding_dimensions,
        )
        results: list[dict[str, Any]] = []
        latencies: list[float] = []
        loaded_tokens = 0
        workflow_hits = 0
        workflow_reciprocal_ranks: list[float] = []
        expected_capability_count = 0
        found_capability_count = 0
        for case in dataset["cases"]:
            query = case.get("query")
            if not isinstance(query, str):
                continue
            started = perf_counter()
            expected_workflow = case.get("expected_workflow_type")
            expected_assets = {
                str(item) for item in case.get("expected_capability_assets", [])
            }
            responses: list[CapabilitySearchResponse] = []
            workflow_response = await service.search(
                CapabilitySearchRequest(
                    query=query,
                    tenant_scope=tenant,
                    kinds=frozenset({DefinitionKind.WORKFLOW_TYPE}),
                    limit=min(limit, 10),
                )
            )
            responses.append(workflow_response)
            workflow_ref = next(
                (
                    hit.exact_ref
                    for hit in workflow_response.hits
                    if hit.exact_ref is not None
                    and hit.exact_ref.logical_id == expected_workflow
                ),
                None,
            )
            if expected_assets:
                for kind in (
                    DefinitionKind.MCP_SERVER,
                    DefinitionKind.MCP_TOOL,
                    DefinitionKind.SKILL,
                    DefinitionKind.AGENT_PROFILE,
                ):
                    responses.append(
                        await service.search(
                            CapabilitySearchRequest(
                                query=query,
                                tenant_scope=tenant,
                                kinds=frozenset({kind}),
                                workflow_type_ref=workflow_ref,
                                limit=min(limit, 20),
                            )
                        )
                    )
            elapsed_ms = (perf_counter() - started) * 1_000
            latencies.append(elapsed_ms)
            hits = _unique_hits(responses)
            hit_ids = [
                hit.exact_ref.logical_id
                for hit in hits
                if hit.exact_ref is not None
            ]
            workflow_rank = None
            workflow_hit_ids = [
                hit.exact_ref.logical_id
                for hit in workflow_response.hits
                if hit.exact_ref is not None
            ]
            if (
                isinstance(expected_workflow, str)
                and expected_workflow in workflow_hit_ids
            ):
                workflow_rank = workflow_hit_ids.index(expected_workflow) + 1
                workflow_hits += 1
                workflow_reciprocal_ranks.append(1 / workflow_rank)
            found_assets = expected_assets.intersection(hit_ids)
            expected_capability_count += len(expected_assets)
            found_capability_count += len(found_assets)
            token_metrics = _aggregate_token_metrics(responses)
            search_result_metric = token_metrics.get("search_results")
            if search_result_metric is not None:
                loaded_tokens += search_result_metric["estimated_tokens"]
            results.append(
                {
                    "case_id": case["case_id"],
                    "elapsed_ms": round(elapsed_ms, 3),
                    "workflow_rank": workflow_rank,
                    "expected_capabilities": sorted(expected_assets),
                    "found_capabilities": sorted(found_assets),
                    "hit_refs": [
                        hit.exact_ref.model_dump(mode="json")
                        for hit in hits
                        if hit.exact_ref is not None
                    ],
                    "token_use": token_metrics,
                }
            )
        workflow_case_count = sum(
            1
            for case in dataset["cases"]
            if isinstance(case.get("query"), str)
            and isinstance(case.get("expected_workflow_type"), str)
        )
        return {
            "dataset_version": dataset["dataset_version"],
            "tenant_scope": tenant,
            "limit": limit,
            "metrics": {
                "workflow_type_recall_at_k": (
                    workflow_hits / workflow_case_count if workflow_case_count else 0.0
                ),
                "capability_recall_at_k": (
                    found_capability_count / expected_capability_count
                    if expected_capability_count
                    else 0.0
                ),
                "web_capability_recall_at_k": (
                    found_capability_count / expected_capability_count
                    if expected_capability_count
                    else 0.0
                ),
                "exact_identifier_mrr": (
                    sum(workflow_reciprocal_ranks) / len(workflow_reciprocal_ranks)
                    if workflow_reciprocal_ranks
                    else 0.0
                ),
                "median_search_latency_ms": (
                    round(median(latencies), 3) if latencies else 0.0
                ),
                "catalog_tokens_loaded": loaded_tokens,
            },
            "cases": results,
        }
    finally:
        await postgres_pool.close()
        await mongo_client.close()


def _unique_hits(
    responses: list[CapabilitySearchResponse],
) -> tuple[CapabilitySearchHit, ...]:
    hits: list[CapabilitySearchHit] = []
    identities: set[tuple[str, str, int, str]] = set()
    for response in responses:
        for hit in response.hits:
            if hit.exact_ref is None:
                continue
            identity = (
                hit.exact_ref.kind.value,
                hit.exact_ref.logical_id,
                hit.exact_ref.revision,
                hit.exact_ref.digest,
            )
            if identity in identities:
                continue
            identities.add(identity)
            hits.append(hit)
    return tuple(hits)


def _aggregate_token_metrics(
    responses: list[CapabilitySearchResponse],
) -> dict[str, AggregatedTokenMetric]:
    totals: dict[str, AggregatedTokenMetric] = {}
    for response in responses:
        for measurement in response.token_use:
            current = totals.setdefault(
                measurement.metric_kind,
                {
                    "metric_kind": measurement.metric_kind,
                    "character_count": 0,
                    "estimated_tokens": 0,
                    "method": measurement.method,
                },
            )
            current["character_count"] = current["character_count"] + (
                measurement.character_count
            )
            current["estimated_tokens"] = current["estimated_tokens"] + (
                measurement.estimated_tokens
            )
    return totals


def main() -> None:
    args = _arguments()
    dataset_path = args.dataset.resolve(strict=True)
    dataset = json.loads(dataset_path.read_text(encoding="utf-8"))
    report = asyncio.run(
        _evaluate(
            tenant=args.tenant,
            dataset=dataset,
            limit=args.limit,
        )
    )
    rendered = json.dumps(report, sort_keys=True, indent=2) + "\n"
    if args.output is not None:
        output = args.output.resolve()
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(rendered, encoding="utf-8")
    print(rendered, end="")


if __name__ == "__main__":
    main()
