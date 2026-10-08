"""The seeded catalog as a search projection, for evaluation and parity tests (FT-A3).

``seeded_published`` reads every published definition from the committed seed bundles;
``build_catalog`` publishes them into an in-memory definition repository and projects them
(lexical-only unless an embedder is given). ``DeterministicEmbeddings`` is an offline,
recorded-by-construction embedder (hashed bag of words, unit length): no provider is called.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from collections.abc import Iterable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from mission_control.application.authoring.control_plane_repository import (
    InMemoryDefinitionRepository,
)
from mission_control.application.capabilities.capability_search_repository import (
    CapabilityEmbedding,
    InMemoryCatalogSearchRepository,
)
from mission_control.application.capabilities.catalog_projection import (
    CatalogProjectionInput,
    CatalogProjector,
)
from mission_control.domain.authoring.contracts import PublishedDefinition

ROOT = Path(__file__).resolve().parents[2]
SEEDS = ROOT / "packages" / "mission-control-db-contract" / "seeds"
EVAL = ROOT / "tests" / "fixtures" / "capability_search_eval.json"
MODEL_ID = "text-embedding-3-small"
DIMENSIONS = 1536
NOW = datetime(2026, 10, 7, tzinfo=UTC)
_WORD = re.compile(r"[a-z0-9]+")


def seeded_published(
    directories: Iterable[str] = ("common", "biotech", "ai-engineer", "qualification"),
) -> list[PublishedDefinition]:
    published: list[PublishedDefinition] = []
    seen: set[tuple[str, str]] = set()
    for directory in directories:
        for path in sorted((SEEDS / directory).glob("*.json")):
            bundle = json.loads(path.read_bytes())
            for record in bundle["records"]:
                manifest = record["fields"].get("manifest", {})
                if record["kind"] != "asset_version" or "definition" not in manifest:
                    continue
                item = PublishedDefinition.model_validate(manifest)
                key = (item.ref.kind.value, item.ref.logical_id)
                if key not in seen:
                    seen.add(key)
                    published.append(item)
    return published


class DeterministicEmbeddings:
    """Offline embedder: hashed bag of words into ``DIMENSIONS`` slots, L2-normalized."""

    def __init__(self, dimensions: int = DIMENSIONS, model_id: str = MODEL_ID) -> None:
        self.dimensions = dimensions
        self.model_id = model_id
        self.calls: list[tuple[str, ...]] = []

    def vector(self, text: str) -> tuple[float, ...]:
        values = [0.0] * self.dimensions
        for word in _WORD.findall(text.casefold()):
            slot = int(hashlib.sha256(word.encode()).hexdigest()[:8], 16) % self.dimensions
            values[slot] += 1.0
        norm = math.sqrt(sum(value * value for value in values)) or 1.0
        return tuple(value / norm for value in values)

    async def embed(self, text: str) -> CapabilityEmbedding:
        return (await self.embed_many((text,)))[0]

    async def embed_many(self, texts: tuple[str, ...]) -> tuple[CapabilityEmbedding, ...]:
        self.calls.append(texts)
        return tuple(
            CapabilityEmbedding(
                vector=self.vector(text),
                model_id=self.model_id,
                dimensions=self.dimensions,
                input_digest="sha256:" + hashlib.sha256(text.encode()).hexdigest(),
            )
            for text in texts
        )


async def build_catalog(
    embeddings: DeterministicEmbeddings | None = None,
    *,
    directories: Iterable[str] = ("common", "biotech", "ai-engineer", "qualification"),
) -> tuple[InMemoryDefinitionRepository, InMemoryCatalogSearchRepository, CatalogProjector]:
    definitions = InMemoryDefinitionRepository()
    for item in seeded_published(directories):
        published = await definitions.publish(item.definition, "seed", NOW, 0)
        assert published.ref.digest == item.ref.digest
    search = InMemoryCatalogSearchRepository()
    projector = CatalogProjector(
        definitions=definitions,
        search=search,
        embeddings=embeddings,
        embedding_model_id=MODEL_ID,
        embedding_dimensions=DIMENSIONS,
        projection_generation="eval-generation-1",
        clock=lambda: NOW,
    )
    refs = [item.ref for item in seeded_published(directories)]
    await projector.project_many(tuple(CatalogProjectionInput(ref=ref) for ref in refs))
    return definitions, search, projector


def evaluation_cases() -> list[dict[str, Any]]:
    document = json.loads(EVAL.read_bytes())
    cases: list[dict[str, Any]] = document["queries"]
    return cases
