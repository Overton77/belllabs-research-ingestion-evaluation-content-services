from __future__ import annotations

from copy import deepcopy
from datetime import datetime
from typing import Any, Protocol

from pydantic import TypeAdapter

from mission_control.domain.authoring.canonical import sha256_digest
from mission_control.domain.authoring.contracts import (
    AliasBinding,
    AliasRef,
    AuthoringHead,
    Definition,
    ExactDefinitionRef,
    PublishedDefinition,
)
from mission_control.domain.authoring.errors import (
    DefinitionConflict,
    DefinitionNotFound,
    ReferenceMismatch,
    RetiredDefinition,
)

DEFINITION_ADAPTER: TypeAdapter[Definition] = TypeAdapter(Definition)


class DefinitionRepository(Protocol):
    async def save_draft(
        self,
        definition: Definition,
        actor_id: str,
        updated_at: datetime,
        expected_draft_revision: int,
    ) -> AuthoringHead: ...

    async def get_draft(self, kind: str, logical_id: str) -> AuthoringHead: ...

    async def publish(
        self,
        definition: Definition,
        actor_id: str,
        published_at: datetime,
        expected_head_revision: int,
        expected_draft_revision: int | None = None,
    ) -> PublishedDefinition: ...

    async def get(self, ref: ExactDefinitionRef) -> PublishedDefinition: ...

    async def resolve(self, alias: AliasRef, *, selectable: bool = True) -> AliasBinding: ...

    async def move_alias(
        self, alias: AliasRef, target: ExactDefinitionRef, actor_id: str, moved_at: datetime
    ) -> AliasBinding: ...

    async def retire(
        self, ref: ExactDefinitionRef, actor_id: str, retired_at: datetime
    ) -> PublishedDefinition: ...

    async def save_erc_record(self, record: dict[str, Any]) -> None: ...

    async def get_erc_record(self, digest: str) -> dict[str, Any]: ...

    async def list_projection_events(self) -> tuple[dict[str, Any], ...]: ...


class InMemoryDefinitionRepository:
    def __init__(self) -> None:
        self._published_revisions: dict[tuple[str, str], int] = {}
        self._drafts: dict[tuple[str, str], AuthoringHead] = {}
        self._published: dict[tuple[str, str, int], PublishedDefinition] = {}
        self._aliases: dict[tuple[str, str, str], AliasBinding] = {}
        self._alias_movements: list[AliasBinding] = []
        self._retirements: dict[tuple[str, str, int], tuple[datetime, str]] = {}
        self._erc: dict[str, dict[str, Any]] = {}
        self._erc_by_compilation: dict[str, str] = {}
        self._projection_events: dict[str, dict[str, Any]] = {}

    async def save_draft(
        self,
        definition: Definition,
        actor_id: str,
        updated_at: datetime,
        expected_draft_revision: int,
    ) -> AuthoringHead:
        key = (definition.kind.value, definition.logical_id)
        current = self._drafts.get(key)
        current_revision = current.draft_revision if current is not None else 0
        if expected_draft_revision != current_revision:
            raise DefinitionConflict(
                f"expected draft revision {expected_draft_revision}, current revision is "
                f"{current_revision}"
            )
        head = AuthoringHead(
            kind=definition.kind,
            logical_id=definition.logical_id,
            draft_revision=current_revision + 1,
            published_revision=self._published_revisions.get(key, 0),
            definition=definition,
            updated_at=updated_at,
            updated_by=actor_id,
        )
        self._drafts[key] = head
        return head.model_copy(deep=True)

    async def get_draft(self, kind: str, logical_id: str) -> AuthoringHead:
        try:
            return self._drafts[(kind, logical_id)].model_copy(deep=True)
        except KeyError as exc:
            raise DefinitionNotFound(f"authoring head not found: {(kind, logical_id)}") from exc

    async def publish(
        self,
        definition: Definition,
        actor_id: str,
        published_at: datetime,
        expected_head_revision: int,
        expected_draft_revision: int | None = None,
    ) -> PublishedDefinition:
        head_key = (definition.kind.value, definition.logical_id)
        current = self._published_revisions.get(head_key, 0)
        if expected_head_revision != current:
            raise DefinitionConflict(
                f"expected head revision {expected_head_revision}, current revision is {current}"
            )
        draft = self._drafts.get(head_key)
        if expected_draft_revision is not None and (
            draft is None or draft.draft_revision != expected_draft_revision
        ):
            current_draft_revision = draft.draft_revision if draft is not None else 0
            raise DefinitionConflict(
                f"expected draft revision {expected_draft_revision}, "
                f"current revision is {current_draft_revision}"
            )
        revision = current + 1
        ref = ExactDefinitionRef(
            kind=definition.kind,
            logical_id=definition.logical_id,
            revision=revision,
            digest=sha256_digest(definition),
        )
        published = PublishedDefinition(
            ref=ref,
            definition=definition,
            published_at=published_at,
            published_by=actor_id,
        )
        self._published_revisions[head_key] = revision
        if draft is not None:
            self._drafts[head_key] = draft.model_copy(update={"published_revision": revision})
        self._published[(ref.kind.value, ref.logical_id, revision)] = published
        event = _projection_event_record(ref, "upsert", published_at)
        self._projection_events[str(event["event_id"])] = event
        return published.model_copy(deep=True)

    async def list_published_definitions(self) -> tuple[PublishedDefinition, ...]:
        """Every published revision (retirement applied), ordered like the PostgreSQL adapter."""
        return tuple(
            [
                await self.get(published.ref)
                for _, published in sorted(self._published.items(), key=lambda item: item[0])
            ]
        )

    async def list_published_definition_refs(self) -> tuple[ExactDefinitionRef, ...]:
        return tuple(item.ref for item in await self.list_published_definitions())

    async def get(self, ref: ExactDefinitionRef) -> PublishedDefinition:
        key = (ref.kind.value, ref.logical_id, ref.revision)
        try:
            published = self._published[key]
        except KeyError as exc:
            raise DefinitionNotFound(f"definition not found: {key}") from exc
        _verify_published(ref, published)
        retirement = self._retirements.get(key)
        if retirement is not None:
            published = PublishedDefinition(
                ref=published.ref.model_copy(update={"lifecycle_status": "retired"}),
                definition=published.definition,
                published_at=published.published_at,
                published_by=published.published_by,
                retired_at=retirement[0],
            )
        return published.model_copy(deep=True)

    async def resolve(self, alias: AliasRef, *, selectable: bool = True) -> AliasBinding:
        key = (alias.kind.value, alias.logical_id, alias.alias)
        try:
            binding = self._aliases[key]
        except KeyError as exc:
            raise DefinitionNotFound(f"alias not found: {key}") from exc
        target = await self.get(binding.target)
        if selectable and target.retired_at is not None:
            raise RetiredDefinition(f"alias points to retired definition: {binding.target}")
        return binding.model_copy(deep=True)

    async def move_alias(
        self, alias: AliasRef, target: ExactDefinitionRef, actor_id: str, moved_at: datetime
    ) -> AliasBinding:
        if alias.kind != target.kind or alias.logical_id != target.logical_id:
            raise ReferenceMismatch("alias identity and target identity must match")
        published = await self.get(target)
        if published.retired_at is not None:
            raise RetiredDefinition("cannot move an alias to a retired revision")
        binding = AliasBinding(
            alias_ref=alias,
            target=target,
            moved_at=moved_at,
            moved_by=actor_id,
        )
        self._aliases[(alias.kind.value, alias.logical_id, alias.alias)] = binding
        self._alias_movements.append(binding)
        return binding.model_copy(deep=True)

    async def retire(
        self, ref: ExactDefinitionRef, actor_id: str, retired_at: datetime
    ) -> PublishedDefinition:
        current = await self.get(ref)
        if current.retired_at is not None:
            return current
        key = (ref.kind.value, ref.logical_id, ref.revision)
        self._retirements[key] = (retired_at, actor_id)
        event = _projection_event_record(ref, "retire", retired_at)
        self._projection_events[str(event["event_id"])] = event
        return await self.get(ref)

    async def save_erc_record(self, record: dict[str, Any]) -> None:
        digest = str(record["digest"])
        compilation_id = str(record["compilation_id"])
        existing = self._erc.get(digest)
        if existing is not None and existing != record:
            raise DefinitionConflict(f"ERC digest collision: {digest}")
        existing_digest = self._erc_by_compilation.get(compilation_id)
        if existing_digest is not None and existing_digest != digest:
            raise DefinitionConflict(
                "compilation identity already belongs to a different Effective Run Configuration"
            )
        self._erc[digest] = deepcopy(record)
        self._erc_by_compilation[compilation_id] = digest

    async def get_erc_record(self, digest: str) -> dict[str, Any]:
        try:
            return deepcopy(self._erc[digest])
        except KeyError as exc:
            raise DefinitionNotFound(f"ERC not found: {digest}") from exc

    async def list_projection_events(self) -> tuple[dict[str, Any], ...]:
        return tuple(
            deepcopy(self._projection_events[event_id])
            for event_id in sorted(self._projection_events)
        )


def _projection_event_record(
    ref: ExactDefinitionRef,
    operation: str,
    occurred_at: datetime,
) -> dict[str, Any]:
    tenant_scope = "global"
    event_id = sha256_digest(
        {
            "tenant_scope": tenant_scope,
            "asset_kind": ref.kind.value,
            "logical_id": ref.logical_id,
            "revision": ref.revision,
            "source_digest": ref.digest,
            "operation": operation,
        }
    )
    return {
        "event_id": event_id,
        "tenant_scope": tenant_scope,
        "asset_kind": ref.kind.value,
        "logical_id": ref.logical_id,
        "revision": ref.revision,
        "source_digest": ref.digest,
        "operation": operation,
        "state": "pending",
        "attempt_count": 0,
        "lease_owner": None,
        "lease_expires_at": None,
        "next_attempt_at": occurred_at,
        "last_error_code": None,
        "poison_reason": None,
        "completed_at": None,
        "created_at": occurred_at,
    }


def _verify_published(ref: ExactDefinitionRef, published: PublishedDefinition) -> None:
    requested_identity = ref.model_dump(exclude={"lifecycle_status"})
    stored_identity = published.ref.model_dump(exclude={"lifecycle_status"})
    if stored_identity != requested_identity:
        raise ReferenceMismatch("requested exact reference does not match stored revision")
    actual = sha256_digest(published.definition)
    if actual != ref.digest:
        raise ReferenceMismatch(
            f"stored definition digest mismatch: expected {ref.digest}, got {actual}"
        )
