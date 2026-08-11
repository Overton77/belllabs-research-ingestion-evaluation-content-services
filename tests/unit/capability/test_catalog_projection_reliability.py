from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from hashlib import sha256

import pytest

from app.application.capability.capability_search_repository import (
    CapabilityEmbedding,
    InMemoryCatalogSearchRepository,
)
from app.application.capability.catalog_projection import (
    CatalogProjectionInput,
    CatalogProjector,
)
from app.application.capability.catalog_projection_admin import (
    rebuild_capability_search_projection,
    verify_capability_search_projection,
)
from app.application.capability.catalog_projection_events import (
    CatalogProjectionEvent,
    CatalogProjectionEventProcessor,
    InMemoryProjectionEventRepository,
    ProjectionEventFailure,
    ProjectionEventState,
)
from app.application.capability.catalog_projection_generation import (
    InMemoryProjectionGenerationRepository,
    ProjectionGenerationSpec,
    projection_source_set_digest,
)
from app.application.control_plane.control_plane_repository import InMemoryDefinitionRepository
from app.domain.control_plane.contracts import (
    DefinitionKind,
    ExactDefinitionRef,
    PromptDefinition,
)

NOW = datetime(2026, 7, 25, 20, 0, tzinfo=UTC)
ZERO_DIGEST = "sha256:" + "0" * 64


class BatchEmbeddings:
    def __init__(self, *, wrong_last_digest: bool = False) -> None:
        self.batch_calls: list[tuple[str, ...]] = []
        self.single_calls: list[str] = []
        self.wrong_last_digest = wrong_last_digest

    async def embed(self, text: str) -> CapabilityEmbedding:
        self.single_calls.append(text)
        return self._embedding(text, wrong=False)

    async def embed_many(
        self,
        texts: tuple[str, ...],
    ) -> tuple[CapabilityEmbedding, ...]:
        self.batch_calls.append(texts)
        return tuple(
            self._embedding(
                text,
                wrong=self.wrong_last_digest and index == len(texts) - 1,
            )
            for index, text in enumerate(texts)
        )

    @staticmethod
    def _embedding(text: str, *, wrong: bool) -> CapabilityEmbedding:
        digest = (
            ZERO_DIGEST
            if wrong
            else f"sha256:{sha256(text.encode()).hexdigest()}"
        )
        return CapabilityEmbedding(
            vector=(1.0, 0.0, 0.0),
            model_id="text-embedding-3-small",
            dimensions=3,
            input_digest=digest,
        )


class EventCompletions:
    def __init__(self) -> None:
        self.refs: list[ExactDefinitionRef] = []

    async def complete_for_ref(
        self,
        ref: ExactDefinitionRef,
        *,
        tenant_scope: str,
        completed_at: datetime,
    ) -> int:
        assert tenant_scope == "global"
        assert completed_at.tzinfo is not None
        self.refs.append(ref)
        return 1


async def _publish_prompts(
    definitions: InMemoryDefinitionRepository,
    count: int,
) -> tuple[ExactDefinitionRef, ...]:
    refs = []
    for index in range(count):
        published = await definitions.publish(
            PromptDefinition(
                logical_id=f"prompt.projection-{index}",
                title=f"Projection prompt {index}",
                description=f"Searchable projection fixture {index}",
                format="text",
                template_engine="none",
                body=f"private prompt body {index}",
                trust_class="reviewed",
            ),
            "publisher",
            NOW,
            0,
        )
        refs.append(published.ref)
    return tuple(refs)


def _projector(
    definitions: InMemoryDefinitionRepository,
    search: InMemoryCatalogSearchRepository,
    embeddings: BatchEmbeddings,
    generation: str,
) -> CatalogProjector:
    return CatalogProjector(
        definitions=definitions,
        search=search,
        embeddings=embeddings,
        embedding_model_id="text-embedding-3-small",
        embedding_dimensions=3,
        projection_generation=generation,
        clock=lambda: NOW,
    )


def _spec(
    generation: str,
    refs: tuple[ExactDefinitionRef, ...],
) -> ProjectionGenerationSpec:
    return ProjectionGenerationSpec(
        tenant_scope="global",
        projection_generation=generation,
        embedding_model_id="text-embedding-3-small",
        embedding_dimensions=3,
        search_document_format_version=1,
        selected_kinds=frozenset({DefinitionKind.PROMPT}),
        expected_count=len(refs),
        expected_source_set_digest=projection_source_set_digest(refs),
    )


@pytest.mark.asyncio
async def test_projector_batches_embeddings_and_persists_nothing_on_bad_claim() -> None:
    definitions = InMemoryDefinitionRepository()
    refs = await _publish_prompts(definitions, 3)
    search = InMemoryCatalogSearchRepository()
    embeddings = BatchEmbeddings()
    projector = _projector(definitions, search, embeddings, "generation-batch")

    results = await projector.project_many(
        tuple(CatalogProjectionInput(ref=ref) for ref in refs)
    )

    assert len(results) == 3
    assert len(embeddings.batch_calls) == 1
    assert len(embeddings.batch_calls[0]) == 3
    assert embeddings.single_calls == []

    rejected_search = InMemoryCatalogSearchRepository()
    rejected_embeddings = BatchEmbeddings(wrong_last_digest=True)
    rejected = _projector(
        definitions,
        rejected_search,
        rejected_embeddings,
        "generation-rejected",
    )
    with pytest.raises(RuntimeError, match="embedding metadata"):
        await rejected.project_many(
            tuple(CatalogProjectionInput(ref=ref) for ref in refs)
        )
    assert (
        await rejected_search.list_generation("global", "generation-rejected")
        == ()
    )


@pytest.mark.asyncio
async def test_generation_is_invisible_until_verified_atomic_activation() -> None:
    definitions = InMemoryDefinitionRepository()
    refs = await _publish_prompts(definitions, 2)
    search = InMemoryCatalogSearchRepository()
    generations = InMemoryProjectionGenerationRepository(search)
    embeddings = BatchEmbeddings()

    old_spec = _spec("generation-old", refs)
    await generations.begin(old_spec, created_at=NOW)
    await _projector(
        definitions,
        search,
        embeddings,
        "generation-old",
    ).project_many(tuple(CatalogProjectionInput(ref=ref) for ref in refs))
    await generations.activate(old_spec, activated_at=NOW)

    new_spec = _spec("generation-new", refs)
    await generations.begin(new_spec, created_at=NOW + timedelta(minutes=1))
    await _projector(
        definitions,
        search,
        embeddings,
        "generation-new",
    ).project(refs[0])

    assert (
        await search.get(
            "global",
            refs[0].kind,
            refs[0].logical_id,
            refs[0].revision,
        )
    ).projection_generation == "generation-old"
    with pytest.raises(RuntimeError, match="verification failed"):
        await generations.activate(
            new_spec,
            activated_at=NOW + timedelta(minutes=2),
        )
    assert (
        await generations.active_for_kind("global", DefinitionKind.PROMPT)
        == "generation-old"
    )

    await _projector(
        definitions,
        search,
        embeddings,
        "generation-new",
    ).project(refs[1])
    await generations.activate(
        new_spec,
        activated_at=NOW + timedelta(minutes=3),
    )
    assert (
        await generations.active_for_kind("global", DefinitionKind.PROMPT)
        == "generation-new"
    )


@pytest.mark.asyncio
async def test_rebuild_verifies_digests_before_activation_and_is_idempotent() -> None:
    definitions = InMemoryDefinitionRepository()
    refs = await _publish_prompts(definitions, 2)
    search = InMemoryCatalogSearchRepository()
    generations = InMemoryProjectionGenerationRepository(search)
    completions = EventCompletions()
    projector = _projector(
        definitions,
        search,
        BatchEmbeddings(),
        "generation-rebuild",
    )

    summary = await rebuild_capability_search_projection(
        refs=refs,
        projector=projector,
        events=completions,
        generations=generations,
        tenant_scope="global",
        projection_generation="generation-rebuild",
        selected_kinds=frozenset({DefinitionKind.PROMPT}),
        clock=lambda: NOW,
    )
    assert summary.activated is True
    assert summary.selected_count == 2
    assert summary.source_set_digest == projection_source_set_digest(refs)
    assert completions.refs == list(refs)

    verification = await verify_capability_search_projection(
        refs=refs,
        definitions=definitions,
        search=search,
        tenant_scope="global",
        projection_generation="generation-rebuild",
        embedding_model_id="text-embedding-3-small",
        embedding_dimensions=3,
        search_document_format_version=1,
        selected_kinds=frozenset({DefinitionKind.PROMPT}),
    )
    assert verification.valid is True

    replay = await rebuild_capability_search_projection(
        refs=refs,
        projector=projector,
        events=completions,
        generations=generations,
        tenant_scope="global",
        projection_generation="generation-rebuild",
        selected_kinds=frozenset({DefinitionKind.PROMPT}),
        clock=lambda: NOW + timedelta(minutes=1),
    )
    assert replay.changed_count == 0
    assert replay.unchanged_count == 2
    assert replay.activation == summary.activation

    first_row = await search.get(
        "global",
        refs[0].kind,
        refs[0].logical_id,
        refs[0].revision,
        projection_generation="generation-rebuild",
    )
    assert first_row is not None
    await search.upsert(first_row.model_copy(update={"source_digest": ZERO_DIGEST}))
    stale = await verify_capability_search_projection(
        refs=refs,
        definitions=definitions,
        search=search,
        tenant_scope="global",
        projection_generation="generation-rebuild",
        embedding_model_id="text-embedding-3-small",
        embedding_dimensions=3,
        search_document_format_version=1,
        selected_kinds=frozenset({DefinitionKind.PROMPT}),
    )
    assert stale.valid is False
    assert stale.stale_refs == (
        refs[0].model_copy(update={"digest": ZERO_DIGEST}),
    )
    assert stale.observed_source_set_digest != stale.expected_source_set_digest


def _event() -> CatalogProjectionEvent:
    return CatalogProjectionEvent(
        event_id="sha256:" + "a" * 64,
        tenant_scope="global",
        asset_kind=DefinitionKind.PROMPT,
        logical_id="prompt.projection-0",
        revision=1,
        source_digest="sha256:" + "b" * 64,
        operation="upsert",
        state=ProjectionEventState.PENDING,
        attempt_count=0,
        next_attempt_at=NOW,
        created_at=NOW,
    )


@pytest.mark.asyncio
async def test_event_claim_is_exclusive_and_expired_lease_is_reclaimable() -> None:
    repository = InMemoryProjectionEventRepository((_event(),))
    first, second = await asyncio.gather(
        repository.claim_batch(
            owner="worker-a",
            now=NOW,
            lease_duration=timedelta(seconds=30),
            limit=1,
        ),
        repository.claim_batch(
            owner="worker-b",
            now=NOW,
            lease_duration=timedelta(seconds=30),
            limit=1,
        ),
    )
    claims = first + second
    assert len(claims) == 1
    first_claim = claims[0]
    first_owner = first_claim.lease_owner
    second_owner = "worker-b" if first_owner == "worker-a" else "worker-a"

    assert (
        await repository.claim_batch(
            owner=second_owner,
            now=NOW + timedelta(seconds=29),
            lease_duration=timedelta(seconds=30),
            limit=1,
        )
        == ()
    )
    reclaimed = await repository.claim_batch(
        owner=second_owner,
        now=NOW + timedelta(seconds=30),
        lease_duration=timedelta(seconds=30),
        limit=1,
    )
    assert reclaimed[0].attempt_count == 2
    assert (
        await repository.complete(
            first_claim,
            owner=first_owner or "",
            completed_at=NOW + timedelta(seconds=31),
        )
        is False
    )
    assert (
        await repository.complete(
            reclaimed[0],
            owner=second_owner,
            completed_at=NOW + timedelta(seconds=31),
        )
        is True
    )


@pytest.mark.asyncio
async def test_event_retry_backoff_is_bounded_and_poison_emits_one_alert() -> None:
    repository = InMemoryProjectionEventRepository((_event(),))
    first = (
        await repository.claim_batch(
            owner="worker",
            now=NOW,
            lease_duration=timedelta(minutes=1),
            limit=1,
        )
    )[0]
    retry = await repository.fail(
        first,
        owner="worker",
        failed_at=NOW,
        failure=ProjectionEventFailure(
            error_code="PROJECTION_DEPENDENCY_ERROR",
            retryable=True,
        ),
        max_attempts=2,
        base_backoff=timedelta(seconds=5),
        max_backoff=timedelta(seconds=6),
    )
    assert retry is not None
    assert retry.state == ProjectionEventState.RETRY
    assert retry.next_attempt_at == NOW + timedelta(seconds=5)
    assert (
        await repository.claim_batch(
            owner="worker",
            now=NOW + timedelta(seconds=4),
            lease_duration=timedelta(minutes=1),
            limit=1,
        )
        == ()
    )

    second = (
        await repository.claim_batch(
            owner="worker",
            now=NOW + timedelta(seconds=5),
            lease_duration=timedelta(minutes=1),
            limit=1,
        )
    )[0]
    poison = await repository.fail(
        second,
        owner="worker",
        failed_at=NOW + timedelta(seconds=5),
        failure=ProjectionEventFailure(
            error_code="PROJECTION_DEPENDENCY_ERROR",
            retryable=True,
        ),
        max_attempts=2,
        base_backoff=timedelta(seconds=5),
        max_backoff=timedelta(seconds=6),
    )
    assert poison is not None
    assert poison.state == ProjectionEventState.POISON
    assert poison.poison_reason == "PROJECTION_DEPENDENCY_ERROR"
    assert len(await repository.alerts()) == 1


@pytest.mark.asyncio
async def test_event_processor_completes_after_idempotent_projection_commit() -> None:
    definitions = InMemoryDefinitionRepository()
    refs = await _publish_prompts(definitions, 1)
    search = InMemoryCatalogSearchRepository()
    generations = InMemoryProjectionGenerationRepository(search)
    empty_spec = _spec("generation-live", ())
    await generations.begin(empty_spec, created_at=NOW)
    await generations.activate(empty_spec, activated_at=NOW)
    event = _event().model_copy(
        update={
            "logical_id": refs[0].logical_id,
            "source_digest": refs[0].digest,
        }
    )
    events = InMemoryProjectionEventRepository((event,))
    embeddings = BatchEmbeddings()
    processor = CatalogProjectionEventProcessor(
        events=events,
        generations=generations,
        projector_factory=lambda generation: _projector(
            definitions,
            search,
            embeddings,
            generation,
        ),
        clock=lambda: NOW + timedelta(seconds=1),
    )

    first = await processor.process_batch(owner="worker")
    replay = await processor.process_batch(owner="worker")

    assert first.completed == 1
    assert first.lease_lost == 0
    assert replay.claimed == 0
    assert (await events.get(event.event_id)).state == ProjectionEventState.COMPLETED
    projected = await search.get(
        "global",
        refs[0].kind,
        refs[0].logical_id,
        refs[0].revision,
    )
    assert projected is not None
    assert projected.source_digest == refs[0].digest
