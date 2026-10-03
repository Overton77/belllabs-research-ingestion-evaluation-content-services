from __future__ import annotations

from hashlib import sha256
from typing import Protocol

from openai import AsyncOpenAI

from mission_control.adapters.langsmith.tracing import (
    create_traced_async_openai,
    trace_capability_embeddings,
)
from mission_control.application.capabilities.capability_search_repository import (
    CapabilityEmbedding,
)
from mission_control.bootstrap.settings import Settings
from mission_control.domain.coordinator.errors import CoordinatorDomainError, CoordinatorErrorCode


class EmbeddingsClient(Protocol):
    @property
    def embeddings(self) -> object: ...


class CapabilityEmbeddingDependencyError(CoordinatorDomainError):
    def __init__(self, message: str = "capability embedding dependency is unavailable") -> None:
        super().__init__(
            CoordinatorErrorCode.PROJECTION_DEPENDENCY_UNAVAILABLE,
            message,
            retryable=True,
        )


class OpenAICapabilityEmbeddingAdapter:
    """Generate projection embeddings without exposing or persisting credentials."""

    def __init__(
        self,
        settings: Settings,
        *,
        client: AsyncOpenAI | None = None,
    ) -> None:
        self._model_id = settings.capability_embedding_model
        self._dimensions = settings.capability_embedding_dimensions
        if client is None:
            credential = settings.openai_api_key
            if credential is None or not credential.get_secret_value().strip():
                raise CapabilityEmbeddingDependencyError(
                    "capability embedding credential reference is unavailable"
                )
            client = create_traced_async_openai(credential.get_secret_value().strip())
        self._client = client

    async def embed(self, text: str) -> CapabilityEmbedding:
        return (await self.embed_many((text,)))[0]

    @trace_capability_embeddings
    async def embed_many(
        self,
        texts: tuple[str, ...],
    ) -> tuple[CapabilityEmbedding, ...]:
        normalized = tuple(text.strip() for text in texts)
        if not normalized:
            return ()
        if any(not text for text in normalized):
            raise ValueError("capability embedding input cannot be blank")
        results: list[CapabilityEmbedding] = []
        for offset in range(0, len(normalized), 64):
            batch = normalized[offset : offset + 64]
            try:
                response = await self._client.embeddings.create(
                    model=self._model_id,
                    input=list(batch),
                    dimensions=self._dimensions,
                    encoding_format="float",
                )
            except Exception as error:
                raise CapabilityEmbeddingDependencyError() from error
            ordered = sorted(response.data, key=lambda item: item.index)
            if len(ordered) != len(batch) or [item.index for item in ordered] != list(
                range(len(batch))
            ):
                raise CapabilityEmbeddingDependencyError(
                    "capability embedding response has invalid indexes"
                )
            for text, item in zip(batch, ordered, strict=True):
                vector = tuple(float(value) for value in item.embedding)
                if len(vector) != self._dimensions:
                    raise CapabilityEmbeddingDependencyError(
                        "capability embedding response has invalid dimensions"
                    )
                results.append(
                    CapabilityEmbedding(
                        vector=vector,
                        model_id=self._model_id,
                        dimensions=self._dimensions,
                        input_digest=_text_digest(text),
                    )
                )
        return tuple(results)


def _text_digest(value: str) -> str:
    return f"sha256:{sha256(value.encode()).hexdigest()}"
