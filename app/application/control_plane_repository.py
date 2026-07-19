from __future__ import annotations

from copy import deepcopy
from datetime import datetime
from typing import Any, Protocol

from pydantic import TypeAdapter
from pymongo import ReturnDocument
from pymongo.errors import DuplicateKeyError

from app.domain.control_plane.canonical import sha256_digest
from app.domain.control_plane.contracts import (
    AliasBinding,
    AliasRef,
    AuthoringHead,
    Definition,
    ExactDefinitionRef,
    PublishedDefinition,
)
from app.domain.control_plane.errors import (
    DefinitionConflict,
    DefinitionNotFound,
    ReferenceMismatch,
    RetiredDefinition,
)
from app.models.control_plane import (
    DefinitionAliasDocument,
    DefinitionAliasMovementDocument,
    DefinitionHeadDocument,
    DefinitionRetirementDocument,
    EffectiveRunConfigurationDocument,
    PublishedDefinitionDocument,
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
        return published.model_copy(deep=True)

    async def get(self, ref: ExactDefinitionRef) -> PublishedDefinition:
        key = (ref.kind.value, ref.logical_id, ref.revision)
        try:
            published = self._published[key]
        except KeyError as exc:
            raise DefinitionNotFound(f"definition not found: {key}") from exc
        _verify_published(ref, published)
        retirement = self._retirements.get(key)
        if retirement is not None:
            published = published.model_copy(update={"retired_at": retirement[0]})
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
        return current.model_copy(update={"retired_at": retired_at})

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


class BeanieDefinitionRepository:
    async def save_draft(
        self,
        definition: Definition,
        actor_id: str,
        updated_at: datetime,
        expected_draft_revision: int,
    ) -> AuthoringHead:
        collection = DefinitionHeadDocument.get_pymongo_collection()
        query: dict[str, Any] = {
            "kind": definition.kind.value,
            "logical_id": definition.logical_id,
        }
        query["draft_revision"] = expected_draft_revision
        try:
            head = await collection.find_one_and_update(
                query,
                {
                    "$inc": {"draft_revision": 1},
                    "$set": {
                        "draft_definition": definition.model_dump(mode="json"),
                        "updated_at": updated_at,
                        "updated_by": actor_id,
                    },
                    "$setOnInsert": {
                        "kind": definition.kind.value,
                        "logical_id": definition.logical_id,
                        "published_revision": 0,
                    },
                },
                upsert=True,
                return_document=ReturnDocument.AFTER,
            )
        except DuplicateKeyError as exc:
            raise DefinitionConflict("authoring head changed concurrently") from exc
        if head is None:
            raise DefinitionConflict("authoring head changed concurrently")
        return _head_from_mapping(head)

    async def get_draft(self, kind: str, logical_id: str) -> AuthoringHead:
        document = await DefinitionHeadDocument.find_one(
            DefinitionHeadDocument.kind == kind,
            DefinitionHeadDocument.logical_id == logical_id,
        )
        if document is None or document.draft_definition is None:
            raise DefinitionNotFound(f"authoring head not found: {(kind, logical_id)}")
        return _head_from_mapping(document.model_dump(mode="python"))

    async def publish(
        self,
        definition: Definition,
        actor_id: str,
        published_at: datetime,
        expected_head_revision: int,
        expected_draft_revision: int | None = None,
    ) -> PublishedDefinition:
        collection = DefinitionHeadDocument.get_pymongo_collection()
        query: dict[str, Any] = {
            "kind": definition.kind.value,
            "logical_id": definition.logical_id,
        }
        query["published_revision"] = expected_head_revision
        if expected_draft_revision is not None:
            query["draft_revision"] = expected_draft_revision
        try:
            async with collection.database.client.start_session() as session:
                async with await session.start_transaction():
                    head = await collection.find_one_and_update(
                        query,
                        {
                            "$inc": {"published_revision": 1},
                            "$set": {
                                "updated_at": published_at,
                                "updated_by": actor_id,
                            },
                            "$setOnInsert": {
                                "kind": definition.kind.value,
                                "logical_id": definition.logical_id,
                                "draft_revision": 0,
                                "draft_definition": None,
                            },
                        },
                        upsert=True,
                        return_document=ReturnDocument.AFTER,
                        session=session,
                    )
                    if head is None:
                        raise DefinitionConflict("definition head changed concurrently")
                    revision = int(head["published_revision"])
                    if revision != expected_head_revision + 1:
                        raise DefinitionConflict("definition head changed concurrently")
                    ref = ExactDefinitionRef(
                        kind=definition.kind,
                        logical_id=definition.logical_id,
                        revision=revision,
                        digest=sha256_digest(definition),
                    )
                    document = PublishedDefinitionDocument(
                        kind=ref.kind.value,
                        logical_id=ref.logical_id,
                        revision=ref.revision,
                        digest=ref.digest,
                        definition=definition.model_dump(mode="json"),
                        published_at=published_at,
                        published_by=actor_id,
                    )
                    await document.insert(session=session)
        except DuplicateKeyError as exc:
            raise DefinitionConflict(
                "definition head changed or published revision already exists"
            ) from exc
        return await self.get(ref)

    async def get(self, ref: ExactDefinitionRef) -> PublishedDefinition:
        document = await PublishedDefinitionDocument.find_one(
            PublishedDefinitionDocument.kind == ref.kind.value,
            PublishedDefinitionDocument.logical_id == ref.logical_id,
            PublishedDefinitionDocument.revision == ref.revision,
        )
        if document is None:
            raise DefinitionNotFound(f"definition not found: {ref}")
        published = _published_from_document(document)
        _verify_published(ref, published)
        retirement = await DefinitionRetirementDocument.find_one(
            DefinitionRetirementDocument.kind == ref.kind.value,
            DefinitionRetirementDocument.logical_id == ref.logical_id,
            DefinitionRetirementDocument.revision == ref.revision,
        )
        if retirement is not None:
            published = published.model_copy(update={"retired_at": retirement.retired_at})
        return published

    async def resolve(self, alias: AliasRef, *, selectable: bool = True) -> AliasBinding:
        document = await DefinitionAliasDocument.find_one(
            DefinitionAliasDocument.kind == alias.kind.value,
            DefinitionAliasDocument.logical_id == alias.logical_id,
            DefinitionAliasDocument.alias == alias.alias,
        )
        if document is None:
            raise DefinitionNotFound(f"alias not found: {alias}")
        target = ExactDefinitionRef(
            kind=alias.kind,
            logical_id=alias.logical_id,
            revision=document.target_revision,
            digest=document.target_digest,
        )
        published = await self.get(target)
        if selectable and published.retired_at is not None:
            raise RetiredDefinition(f"alias points to retired definition: {target}")
        return AliasBinding(
            alias_ref=alias,
            target=target,
            moved_at=document.moved_at,
            moved_by=document.moved_by,
        )

    async def move_alias(
        self, alias: AliasRef, target: ExactDefinitionRef, actor_id: str, moved_at: datetime
    ) -> AliasBinding:
        if alias.kind != target.kind or alias.logical_id != target.logical_id:
            raise ReferenceMismatch("alias identity and target identity must match")
        published = await self.get(target)
        if published.retired_at is not None:
            raise RetiredDefinition("cannot move an alias to a retired revision")
        collection = DefinitionAliasDocument.get_pymongo_collection()
        movement = DefinitionAliasMovementDocument(
            kind=alias.kind.value,
            logical_id=alias.logical_id,
            alias=alias.alias,
            target_revision=target.revision,
            target_digest=target.digest,
            moved_at=moved_at,
            moved_by=actor_id,
        )
        async with collection.database.client.start_session() as session:
            async with await session.start_transaction():
                await collection.update_one(
                    {
                        "kind": alias.kind.value,
                        "logical_id": alias.logical_id,
                        "alias": alias.alias,
                    },
                    {
                        "$set": {
                            "target_revision": target.revision,
                            "target_digest": target.digest,
                            "moved_at": moved_at,
                            "moved_by": actor_id,
                        },
                        "$setOnInsert": {
                            "kind": alias.kind.value,
                            "logical_id": alias.logical_id,
                            "alias": alias.alias,
                        },
                    },
                    upsert=True,
                    session=session,
                )
                await movement.insert(session=session)
        return await self.resolve(alias)

    async def retire(
        self, ref: ExactDefinitionRef, actor_id: str, retired_at: datetime
    ) -> PublishedDefinition:
        published = await self.get(ref)
        if published.retired_at is not None:
            return published
        retirement = DefinitionRetirementDocument(
            kind=ref.kind.value,
            logical_id=ref.logical_id,
            revision=ref.revision,
            digest=ref.digest,
            retired_at=retired_at,
            retired_by=actor_id,
        )
        try:
            await retirement.insert()
        except DuplicateKeyError:
            return await self.get(ref)
        return await self.get(ref)

    async def save_erc_record(self, record: dict[str, Any]) -> None:
        document = EffectiveRunConfigurationDocument.model_validate(record)
        try:
            await document.insert()
        except DuplicateKeyError:
            comparable = document.model_dump(mode="json", exclude={"id"})
            existing_by_digest = await EffectiveRunConfigurationDocument.find_one(
                EffectiveRunConfigurationDocument.digest == document.digest
            )
            if existing_by_digest is not None:
                existing = existing_by_digest.model_dump(mode="json", exclude={"id"})
                if existing == comparable:
                    return
                raise DefinitionConflict(f"ERC digest collision: {document.digest}") from None
            existing_by_compilation = await EffectiveRunConfigurationDocument.find_one(
                EffectiveRunConfigurationDocument.compilation_id == document.compilation_id
            )
            if existing_by_compilation is not None:
                raise DefinitionConflict(
                    "compilation identity already belongs to a different "
                    "Effective Run Configuration"
                ) from None
            raise DefinitionConflict("Effective Run Configuration uniqueness conflict") from None

    async def get_erc_record(self, digest: str) -> dict[str, Any]:
        document = await EffectiveRunConfigurationDocument.find_one(
            EffectiveRunConfigurationDocument.digest == digest
        )
        if document is None:
            raise DefinitionNotFound(f"ERC not found: {digest}")
        return document.model_dump(mode="json", exclude={"id"})


def _published_from_document(document: PublishedDefinitionDocument) -> PublishedDefinition:
    definition = DEFINITION_ADAPTER.validate_python(document.definition)
    return PublishedDefinition(
        ref=ExactDefinitionRef(
            kind=definition.kind,
            logical_id=document.logical_id,
            revision=document.revision,
            digest=document.digest,
        ),
        definition=definition,
        published_at=document.published_at,
        published_by=document.published_by,
    )


def _head_from_mapping(head: dict[str, Any]) -> AuthoringHead:
    raw_definition = head.get("draft_definition")
    if not isinstance(raw_definition, dict):
        raise DefinitionNotFound("authoring head has no draft definition")
    definition = DEFINITION_ADAPTER.validate_python(raw_definition)
    return AuthoringHead(
        kind=definition.kind,
        logical_id=definition.logical_id,
        draft_revision=int(head["draft_revision"]),
        published_revision=int(head["published_revision"]),
        definition=definition,
        updated_at=head["updated_at"],
        updated_by=str(head["updated_by"]),
    )


def _verify_published(ref: ExactDefinitionRef, published: PublishedDefinition) -> None:
    if published.ref != ref:
        raise ReferenceMismatch("requested exact reference does not match stored revision")
    actual = sha256_digest(published.definition)
    if actual != ref.digest:
        raise ReferenceMismatch(
            f"stored definition digest mismatch: expected {ref.digest}, got {actual}"
        )
