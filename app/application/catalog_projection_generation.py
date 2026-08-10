from __future__ import annotations

from datetime import UTC, datetime
from hashlib import sha256
from typing import Protocol

from pydantic import BaseModel, ConfigDict, Field, model_validator

from app.application.capability_search_repository import (
    InMemoryCatalogSearchRepository,
)
from app.domain.control_plane.contracts import DefinitionKind, ExactDefinitionRef


class ProjectionGenerationContract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class ProjectionGenerationSpec(ProjectionGenerationContract):
    tenant_scope: str = Field(min_length=1)
    projection_generation: str = Field(min_length=1)
    embedding_model_id: str = Field(min_length=1)
    embedding_dimensions: int = Field(ge=1)
    search_document_format_version: int = Field(ge=1)
    selected_kinds: frozenset[DefinitionKind] = Field(min_length=1)
    expected_count: int = Field(ge=0)
    expected_source_set_digest: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")


class ProjectionGenerationRecord(ProjectionGenerationSpec):
    state: str = Field(pattern=r"^(building|active|failed|superseded)$")
    actual_count: int | None = Field(default=None, ge=0)
    actual_source_set_digest: str | None = Field(
        default=None,
        pattern=r"^sha256:[0-9a-f]{64}$",
    )
    created_at: datetime
    verified_at: datetime | None = None
    activated_at: datetime | None = None

    @model_validator(mode="after")
    def verified_generation_has_observations(self) -> ProjectionGenerationRecord:
        if self.state == "active" and (
            self.actual_count is None or self.actual_source_set_digest is None
        ):
            raise ValueError("active projection generation requires verification evidence")
        return self


class ProjectionGenerationActivation(ProjectionGenerationContract):
    tenant_scope: str
    projection_generation: str
    activated_count: int = Field(ge=0)
    activated_source_set_digest: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    activated_at: datetime


class ProjectionGenerationRepository(Protocol):
    async def begin(
        self,
        spec: ProjectionGenerationSpec,
        *,
        created_at: datetime,
    ) -> ProjectionGenerationRecord: ...

    async def get(
        self,
        tenant_scope: str,
        projection_generation: str,
    ) -> ProjectionGenerationRecord | None: ...

    async def active_for_kind(
        self,
        tenant_scope: str,
        kind: DefinitionKind,
    ) -> str | None: ...

    async def activate(
        self,
        spec: ProjectionGenerationSpec,
        *,
        activated_at: datetime,
    ) -> ProjectionGenerationActivation: ...

    async def mark_failed(
        self,
        tenant_scope: str,
        projection_generation: str,
    ) -> None: ...


class InMemoryProjectionGenerationRepository:
    def __init__(self, search: InMemoryCatalogSearchRepository) -> None:
        self._search = search
        self._records: dict[tuple[str, str], ProjectionGenerationRecord] = {}

    async def begin(
        self,
        spec: ProjectionGenerationSpec,
        *,
        created_at: datetime,
    ) -> ProjectionGenerationRecord:
        key = (spec.tenant_scope, spec.projection_generation)
        current = self._records.get(key)
        proposed = ProjectionGenerationRecord(
            **spec.model_dump(),
            state="building",
            created_at=created_at,
        )
        if current is None:
            self._search.register_generation_contract(
                spec.tenant_scope,
                spec.projection_generation,
                embedding_model_id=spec.embedding_model_id,
                embedding_dimensions=spec.embedding_dimensions,
                search_document_format_version=(spec.search_document_format_version),
            )
            self._records[key] = proposed
            return proposed
        if _spec_from_record(current) != spec or current.state not in {
            "building",
            "active",
        }:
            raise RuntimeError("projection generation identity is already in use")
        return current

    async def get(
        self,
        tenant_scope: str,
        projection_generation: str,
    ) -> ProjectionGenerationRecord | None:
        return self._records.get((tenant_scope, projection_generation))

    async def active_for_kind(
        self,
        tenant_scope: str,
        kind: DefinitionKind,
    ) -> str | None:
        return self._search.active_generation(tenant_scope, kind)

    async def activate(
        self,
        spec: ProjectionGenerationSpec,
        *,
        activated_at: datetime,
    ) -> ProjectionGenerationActivation:
        key = (spec.tenant_scope, spec.projection_generation)
        current = self._records.get(key)
        if current is None or current.state != "building":
            raise RuntimeError("projection generation is not building")
        rows = await self._search.list_generation(
            spec.tenant_scope,
            spec.projection_generation,
            kinds=spec.selected_kinds,
        )
        observed_digest = projection_source_set_digest(
            tuple(document.exact_ref for document in rows)
        )
        if (
            len(rows) != spec.expected_count
            or observed_digest != spec.expected_source_set_digest
            or any(
                document.embedding_model_id != spec.embedding_model_id
                or document.embedding_dimensions != spec.embedding_dimensions
                or document.search_document_format_version != spec.search_document_format_version
                for document in rows
            )
        ):
            raise RuntimeError("projection generation verification failed")
        for kind, active_generation in self._search.active_generations_for_tenant(
            spec.tenant_scope
        ).items():
            if kind in spec.selected_kinds:
                continue
            active = self._records.get((spec.tenant_scope, active_generation))
            if active is not None and (
                active.embedding_model_id != spec.embedding_model_id
                or active.embedding_dimensions != spec.embedding_dimensions
                or active.search_document_format_version != spec.search_document_format_version
            ):
                raise RuntimeError("projection generation is incompatible with another active kind")
        record = current.model_copy(
            update={
                "state": "active",
                "actual_count": len(rows),
                "actual_source_set_digest": observed_digest,
                "verified_at": activated_at,
                "activated_at": activated_at,
            }
        )
        self._records[key] = record
        self._search.activate_generation(
            spec.tenant_scope,
            spec.projection_generation,
            spec.selected_kinds,
        )
        active_ids = set(self._search.active_generations_for_tenant(spec.tenant_scope).values())
        for prior_key, prior in tuple(self._records.items()):
            if (
                prior_key[0] == spec.tenant_scope
                and prior_key[1] not in active_ids
                and prior.state == "active"
            ):
                self._records[prior_key] = prior.model_copy(update={"state": "superseded"})
        return ProjectionGenerationActivation(
            tenant_scope=spec.tenant_scope,
            projection_generation=spec.projection_generation,
            activated_count=len(rows),
            activated_source_set_digest=observed_digest,
            activated_at=activated_at,
        )

    async def mark_failed(
        self,
        tenant_scope: str,
        projection_generation: str,
    ) -> None:
        key = (tenant_scope, projection_generation)
        current = self._records.get(key)
        if current is not None and current.state == "building":
            self._records[key] = current.model_copy(update={"state": "failed"})


def projection_source_set_digest(refs: tuple[ExactDefinitionRef, ...]) -> str:
    material = "\n".join(
        f"{ref.kind.value}|{ref.logical_id}|{ref.revision}|{ref.digest}"
        for ref in sorted(
            refs,
            key=lambda item: (
                item.kind.value,
                item.logical_id,
                item.revision,
                item.digest,
            ),
        )
    )
    return f"sha256:{sha256(material.encode()).hexdigest()}"


def new_generation_timestamp() -> datetime:
    return datetime.now(UTC)


def _spec_from_record(record: ProjectionGenerationRecord) -> ProjectionGenerationSpec:
    return ProjectionGenerationSpec.model_validate(
        record.model_dump(
            exclude={
                "state",
                "actual_count",
                "actual_source_set_digest",
                "created_at",
                "verified_at",
                "activated_at",
            }
        )
    )
