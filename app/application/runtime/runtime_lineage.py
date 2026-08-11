from __future__ import annotations

import asyncio
from copy import deepcopy
from datetime import datetime
from typing import Protocol

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, model_validator

from app.domain.control_plane.canonical import sha256_digest
from app.domain.graph_runtime.definitions import ExecutionLineageEnvelope
from app.domain.graph_runtime.identities import DIGEST_PATTERN
from app.domain.graph_runtime.kernel import (
    LineageParentEdge,
    ProviderQualifiedLineageRecord,
)
from app.domain.run_control.errors import IdempotencyConflict


class PersistedExecutionLineage(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    lineage_id: str = Field(min_length=1)
    envelope: ExecutionLineageEnvelope
    qualified_identities: tuple[ProviderQualifiedLineageRecord, ...]
    parent_edges: tuple[LineageParentEdge, ...] = ()
    lineage_digest: str = Field(pattern=DIGEST_PATTERN)
    recorded_at: AwareDatetime
    retain_until: AwareDatetime

    @model_validator(mode="after")
    def lineage_is_canonical_and_scope_bound(self) -> PersistedExecutionLineage:
        if self.retain_until <= self.recorded_at:
            raise ValueError("lineage retention must end after it is recorded")
        if any(
            identity.request_scope != self.envelope.request_scope
            for identity in self.qualified_identities
        ):
            raise ValueError("lineage identities cannot cross request scopes")
        identity_keys = [item.canonical_key for item in self.qualified_identities]
        if len(identity_keys) != len(set(identity_keys)):
            raise ValueError("lineage contains a duplicate provider-qualified identity")
        identity_key_set = set(identity_keys)
        if any(
            edge.child.canonical_key not in identity_key_set
            or edge.parent.canonical_key not in identity_key_set
            for edge in self.parent_edges
        ):
            raise ValueError("lineage edges must reference persisted qualified identities")
        expected = sha256_digest(
            self.model_dump(
                mode="json",
                exclude={"lineage_digest", "recorded_at", "retain_until"},
            )
        )
        if expected != self.lineage_digest:
            raise ValueError("persisted execution lineage digest mismatch")
        return self

    @classmethod
    def create(
        cls,
        *,
        lineage_id: str,
        envelope: ExecutionLineageEnvelope,
        qualified_identities: tuple[ProviderQualifiedLineageRecord, ...],
        parent_edges: tuple[LineageParentEdge, ...] = (),
        recorded_at: datetime,
        retain_until: datetime,
    ) -> PersistedExecutionLineage:
        content = {
            "lineage_id": lineage_id,
            "envelope": envelope,
            "qualified_identities": qualified_identities,
            "parent_edges": parent_edges,
        }
        return cls(
            **content,
            lineage_digest=sha256_digest(content),
            recorded_at=recorded_at,
            retain_until=retain_until,
        )


class ExecutionLineageRepository(Protocol):
    async def append(self, lineage: PersistedExecutionLineage) -> PersistedExecutionLineage: ...

    async def provenance_for_result(
        self,
        request_scope: str,
        result_manifest_ref: str,
    ) -> tuple[PersistedExecutionLineage, ...]: ...


class InMemoryExecutionLineageRepository:
    """Immutable lineage journal with explicit parent traversal."""

    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._records: dict[tuple[str, str], PersistedExecutionLineage] = {}

    async def append(self, lineage: PersistedExecutionLineage) -> PersistedExecutionLineage:
        key = (lineage.envelope.request_scope, lineage.lineage_id)
        async with self._lock:
            prior = self._records.get(key)
            if prior is not None:
                if prior != lineage:
                    raise IdempotencyConflict("lineage identity was reused with conflicting facts")
                return deepcopy(prior)
            if lineage.envelope.parent_lineage_id is not None:
                parent_key = (
                    lineage.envelope.request_scope,
                    lineage.envelope.parent_lineage_id,
                )
                if parent_key not in self._records:
                    raise ValueError("lineage parent must be persisted before its child")
            if any(
                item.lineage_digest == lineage.lineage_digest
                and item.lineage_id != lineage.lineage_id
                for item in self._records.values()
            ):
                raise IdempotencyConflict("lineage digest is already bound to another identity")
            self._records[key] = deepcopy(lineage)
            return deepcopy(lineage)

    async def provenance_for_result(
        self,
        request_scope: str,
        result_manifest_ref: str,
    ) -> tuple[PersistedExecutionLineage, ...]:
        matches = [
            item
            for (scope, _), item in self._records.items()
            if scope == request_scope and item.envelope.result_manifest_ref == result_manifest_ref
        ]
        if not matches:
            raise LookupError("result manifest has no persisted lineage")
        collected: dict[str, PersistedExecutionLineage] = {}
        pending = list(matches)
        while pending:
            item = pending.pop()
            if item.lineage_id in collected:
                continue
            collected[item.lineage_id] = deepcopy(item)
            parent_id = item.envelope.parent_lineage_id
            if parent_id is not None:
                try:
                    pending.append(self._records[(request_scope, parent_id)])
                except KeyError as error:
                    raise ValueError("persisted lineage contains a parent gap") from error
        return tuple(
            sorted(
                collected.values(),
                key=lambda item: (item.recorded_at, item.lineage_id),
            )
        )
