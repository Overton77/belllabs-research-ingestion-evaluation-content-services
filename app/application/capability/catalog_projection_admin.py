from __future__ import annotations

from collections.abc import Callable, Sequence
from datetime import UTC, datetime
from typing import Protocol

from pydantic import BaseModel, ConfigDict, Field

from app.application.capability.capability_search_repository import CatalogSearchRepository
from app.application.capability.catalog_projection import (
    CatalogProjectionInput,
    CatalogProjector,
)
from app.application.capability.catalog_projection_generation import (
    ProjectionGenerationActivation,
    ProjectionGenerationRepository,
    ProjectionGenerationSpec,
    projection_source_set_digest,
)
from app.application.control_plane.control_plane_repository import DefinitionRepository
from app.domain.control_plane.contracts import DefinitionKind, ExactDefinitionRef
from app.domain.coordinator.search_document import SEARCH_DOCUMENT_FORMAT_VERSION


class ProjectionAdminContract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class ProjectionRebuildSummary(ProjectionAdminContract):
    tenant_scope: str
    projection_generation: str
    selected_count: int = Field(ge=0)
    changed_count: int = Field(ge=0)
    unchanged_count: int = Field(ge=0)
    completed_event_count: int = Field(ge=0)
    source_set_digest: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    activated: bool
    activation: ProjectionGenerationActivation | None = None


class ProjectionVerificationSummary(ProjectionAdminContract):
    tenant_scope: str
    projection_generation: str
    expected_count: int = Field(ge=0)
    observed_count: int = Field(ge=0)
    verified_count: int = Field(ge=0)
    expected_source_set_digest: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    observed_source_set_digest: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    missing_refs: tuple[ExactDefinitionRef, ...] = ()
    stale_refs: tuple[ExactDefinitionRef, ...] = ()
    unexpected_refs: tuple[ExactDefinitionRef, ...] = ()
    incompatible_refs: tuple[ExactDefinitionRef, ...] = ()

    @property
    def valid(self) -> bool:
        return (
            self.expected_count == self.observed_count == self.verified_count
            and self.expected_source_set_digest == self.observed_source_set_digest
            and not self.missing_refs
            and not self.stale_refs
            and not self.unexpected_refs
            and not self.incompatible_refs
        )


class ProjectionEventCompletionPort(Protocol):
    async def complete_for_ref(
        self,
        ref: ExactDefinitionRef,
        *,
        tenant_scope: str,
        completed_at: datetime,
    ) -> int: ...


async def rebuild_capability_search_projection(
    *,
    refs: Sequence[ExactDefinitionRef],
    projector: CatalogProjector,
    events: ProjectionEventCompletionPort,
    generations: ProjectionGenerationRepository,
    tenant_scope: str,
    projection_generation: str,
    selected_kinds: frozenset[DefinitionKind] | None = None,
    workflow_compatibility: dict[ExactDefinitionRef, frozenset[ExactDefinitionRef]] | None = None,
    batch_size: int = 64,
    clock: Callable[[], datetime] | None = None,
) -> ProjectionRebuildSummary:
    """Build, verify, and atomically activate a new disposable generation."""
    if projector.projection_generation != projection_generation:
        raise ValueError("projector generation does not match rebuild generation")
    if batch_size < 1:
        raise ValueError("projection rebuild batch size must be positive")
    exact_refs = filter_projection_refs(refs, kind=None)
    kinds = selected_kinds or frozenset(ref.kind for ref in exact_refs)
    if not kinds:
        raise ValueError("projection rebuild requires at least one selected kind")
    if any(ref.kind not in kinds for ref in exact_refs):
        raise ValueError("projection rebuild refs exceed selected kinds")
    now = clock or (lambda: datetime.now(UTC))
    source_set_digest = projection_source_set_digest(exact_refs)
    spec = ProjectionGenerationSpec(
        tenant_scope=tenant_scope,
        projection_generation=projection_generation,
        embedding_model_id=projector.embedding_model_id,
        embedding_dimensions=projector.embedding_dimensions,
        search_document_format_version=SEARCH_DOCUMENT_FORMAT_VERSION,
        selected_kinds=kinds,
        expected_count=len(exact_refs),
        expected_source_set_digest=source_set_digest,
    )
    generation_record = await generations.begin(spec, created_at=now())
    if generation_record.state == "active":
        verification = await verify_capability_search_projection(
            refs=exact_refs,
            definitions=projector.definitions,
            search=projector.search,
            tenant_scope=tenant_scope,
            projection_generation=projection_generation,
            embedding_model_id=projector.embedding_model_id,
            embedding_dimensions=projector.embedding_dimensions,
            search_document_format_version=SEARCH_DOCUMENT_FORMAT_VERSION,
            selected_kinds=kinds,
        )
        if not verification.valid:
            raise RuntimeError("active projection generation verification failed")
        if (
            generation_record.actual_count is None
            or generation_record.actual_source_set_digest is None
            or generation_record.activated_at is None
        ):
            raise RuntimeError("active projection generation evidence is incomplete")
        activation = ProjectionGenerationActivation(
            tenant_scope=tenant_scope,
            projection_generation=projection_generation,
            activated_count=generation_record.actual_count,
            activated_source_set_digest=(generation_record.actual_source_set_digest),
            activated_at=generation_record.activated_at,
        )
        completed_event_count = await _complete_events(
            events,
            exact_refs,
            tenant_scope=tenant_scope,
            completed_at=activation.activated_at,
        )
        return ProjectionRebuildSummary(
            tenant_scope=tenant_scope,
            projection_generation=projection_generation,
            selected_count=len(exact_refs),
            changed_count=0,
            unchanged_count=len(exact_refs),
            completed_event_count=completed_event_count,
            source_set_digest=source_set_digest,
            activated=True,
            activation=activation,
        )

    changed_count = 0
    for offset in range(0, len(exact_refs), batch_size):
        batch = exact_refs[offset : offset + batch_size]
        results = await projector.project_many(
            tuple(
                CatalogProjectionInput(
                    ref=ref,
                    workflow_type_refs=(
                        workflow_compatibility.get(ref, frozenset())
                        if workflow_compatibility is not None
                        else frozenset()
                    ),
                )
                for ref in batch
            ),
            tenant_scope=tenant_scope,
        )
        changed_count += sum(int(result.changed) for result in results)

    verification = await verify_capability_search_projection(
        refs=exact_refs,
        definitions=projector.definitions,
        search=projector.search,
        tenant_scope=tenant_scope,
        projection_generation=projection_generation,
        embedding_model_id=projector.embedding_model_id,
        embedding_dimensions=projector.embedding_dimensions,
        search_document_format_version=SEARCH_DOCUMENT_FORMAT_VERSION,
        selected_kinds=kinds,
    )
    if not verification.valid:
        await generations.mark_failed(tenant_scope, projection_generation)
        raise RuntimeError("projection generation verification failed")

    try:
        activation = await generations.activate(spec, activated_at=now())
    except Exception:
        await generations.mark_failed(tenant_scope, projection_generation)
        raise
    completed_event_count = await _complete_events(
        events,
        exact_refs,
        tenant_scope=tenant_scope,
        completed_at=activation.activated_at,
    )
    return ProjectionRebuildSummary(
        tenant_scope=tenant_scope,
        projection_generation=projection_generation,
        selected_count=len(exact_refs),
        changed_count=changed_count,
        unchanged_count=len(exact_refs) - changed_count,
        completed_event_count=completed_event_count,
        source_set_digest=source_set_digest,
        activated=True,
        activation=activation,
    )


async def _complete_events(
    events: ProjectionEventCompletionPort,
    refs: tuple[ExactDefinitionRef, ...],
    *,
    tenant_scope: str,
    completed_at: datetime,
) -> int:
    completed = 0
    for ref in refs:
        completed += await events.complete_for_ref(
            ref,
            tenant_scope=tenant_scope,
            completed_at=completed_at,
        )
    return completed


async def verify_capability_search_projection(
    *,
    refs: Sequence[ExactDefinitionRef],
    definitions: DefinitionRepository,
    search: CatalogSearchRepository,
    tenant_scope: str,
    projection_generation: str,
    embedding_model_id: str,
    embedding_dimensions: int,
    search_document_format_version: int,
    selected_kinds: frozenset[DefinitionKind] | None = None,
) -> ProjectionVerificationSummary:
    """Verify count, exact identities, digests, and embedding contract."""
    exact_refs = filter_projection_refs(refs, kind=None)
    kinds = selected_kinds or frozenset(ref.kind for ref in exact_refs)
    rows = await search.list_generation(
        tenant_scope,
        projection_generation,
        kinds=kinds,
    )
    expected_by_identity = {(ref.kind, ref.logical_id, ref.revision): ref for ref in exact_refs}
    observed_by_identity = {(row.asset_kind, row.logical_id, row.revision): row for row in rows}
    missing: list[ExactDefinitionRef] = []
    stale: list[ExactDefinitionRef] = []
    incompatible: list[ExactDefinitionRef] = []
    verified_count = 0
    for identity, ref in expected_by_identity.items():
        authoritative = await definitions.get(ref)
        row = observed_by_identity.get(identity)
        if row is None:
            missing.append(ref)
            continue
        if (
            authoritative.ref != ref
            or row.exact_ref != ref
            or row.source_digest != authoritative.ref.digest
        ):
            stale.append(row.exact_ref)
            continue
        if (
            row.projection_generation != projection_generation
            or row.embedding_model_id != embedding_model_id
            or row.embedding_dimensions != embedding_dimensions
            or row.search_document_format_version != search_document_format_version
        ):
            incompatible.append(row.exact_ref)
            continue
        verified_count += 1

    unexpected = [
        row.exact_ref
        for identity, row in observed_by_identity.items()
        if identity not in expected_by_identity
    ]
    expected_digest = projection_source_set_digest(exact_refs)
    observed_digest = projection_source_set_digest(tuple(row.exact_ref for row in rows))
    return ProjectionVerificationSummary(
        tenant_scope=tenant_scope,
        projection_generation=projection_generation,
        expected_count=len(exact_refs),
        observed_count=len(rows),
        verified_count=verified_count,
        expected_source_set_digest=expected_digest,
        observed_source_set_digest=observed_digest,
        missing_refs=tuple(missing),
        stale_refs=tuple(stale),
        unexpected_refs=tuple(unexpected),
        incompatible_refs=tuple(incompatible),
    )


def filter_projection_refs(
    refs: Sequence[ExactDefinitionRef],
    *,
    kind: DefinitionKind | None,
) -> tuple[ExactDefinitionRef, ...]:
    selected = (ref for ref in refs if kind is None or ref.kind == kind)
    return tuple(
        sorted(
            selected,
            key=lambda item: (
                item.kind.value,
                item.logical_id,
                item.revision,
                item.digest,
            ),
        )
    )
