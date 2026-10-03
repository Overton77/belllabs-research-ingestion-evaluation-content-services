"""Transactional installation catalog and immutable ERCs, without Mongo fallback.

The trusted composition root supplies catalog_scope. Request callers cannot select
another installation's catalog. Existing publication contracts are preserved;
this transitional schema does not claim common mission_control component parity.
"""

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any

import asyncpg

from mission_control.domain.authoring.canonical import (
    canonical_data,
    sha256_digest,
    stable_json_dump,
)
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


def _key(*parts: str | int) -> str:
    return json.dumps(parts, separators=(",", ":"))


def _json(value: Any) -> Any:
    return json.loads(value) if isinstance(value, str) else value


class PostgresDefinitionRepository:
    def __init__(self, pool: asyncpg.Pool, *, catalog_scope: str) -> None:
        if not catalog_scope:
            raise ValueError("trusted installation catalog scope is required")
        self._pool = pool
        self.catalog_scope = catalog_scope

    @asynccontextmanager
    async def _transaction(self, *, write: bool = False) -> AsyncIterator[asyncpg.Connection]:
        async with self._pool.acquire() as connection, connection.transaction():
            await connection.execute(
                "SELECT set_config('belllabs.catalog_scope', $1, true)",
                self.catalog_scope,
            )
            if write:
                # Catalog mutation volume is bounded; installation-wide serialization
                # makes publication + head + event and alias + retirement decisions atomic.
                await connection.execute(
                    "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
                    "definition-catalog:" + self.catalog_scope,
                )
            yield connection

    async def _record(
        self,
        connection: asyncpg.Connection,
        contract: str,
        identity: str,
    ) -> dict[str, Any] | None:
        row = await connection.fetchrow(
            """SELECT payload, payload_digest FROM belllabs_control.definition_catalog_records
               WHERE catalog_scope=$1 AND contract=$2 AND identity=$3""",
            self.catalog_scope,
            contract,
            identity,
        )
        if row is None:
            return None
        payload: dict[str, Any] = _json(row["payload"])
        if sha256_digest(payload) != row["payload_digest"]:
            raise ReferenceMismatch("catalog record payload digest mismatch")
        return payload

    async def _insert(
        self,
        connection: asyncpg.Connection,
        contract: str,
        identity: str,
        payload: dict[str, Any],
    ) -> None:
        payload = canonical_data(payload)["payload"]
        prior = await self._record(connection, contract, identity)
        if prior is not None:
            if sha256_digest(prior) != sha256_digest(payload):
                raise DefinitionConflict("immutable catalog identity already has different content")
            return
        await connection.execute(
            """INSERT INTO belllabs_control.definition_catalog_records
               (catalog_scope, contract, identity, payload, payload_digest)
               VALUES ($1,$2,$3,$4::jsonb,$5)""",
            self.catalog_scope,
            contract,
            identity,
            json.dumps(payload, allow_nan=False),
            sha256_digest(payload),
        )

    async def _head(
        self,
        connection: asyncpg.Connection,
        kind: str,
        logical_id: str,
    ) -> asyncpg.Record | None:
        return await connection.fetchrow(
            """SELECT draft, draft_revision, published_revision
               FROM belllabs_control.definition_catalog_heads
               WHERE catalog_scope=$1 AND kind=$2 AND logical_id=$3""",
            self.catalog_scope,
            kind,
            logical_id,
        )

    async def save_draft(
        self,
        definition: Definition,
        actor_id: str,
        updated_at: datetime,
        expected_draft_revision: int,
    ) -> AuthoringHead:
        async with self._transaction(write=True) as connection:
            prior = await self._head(connection, definition.kind.value, definition.logical_id)
            current = prior["draft_revision"] if prior else 0
            if current != expected_draft_revision:
                raise DefinitionConflict(
                    f"expected draft revision {expected_draft_revision}, current {current}"
                )
            head = AuthoringHead(
                kind=definition.kind,
                logical_id=definition.logical_id,
                draft_revision=current + 1,
                published_revision=prior["published_revision"] if prior else 0,
                definition=definition,
                updated_at=updated_at,
                updated_by=actor_id,
            )
            await connection.execute(
                """INSERT INTO belllabs_control.definition_catalog_heads
                   (catalog_scope, kind, logical_id, draft_revision, published_revision, draft)
                   VALUES ($1,$2,$3,$4,$5,$6::jsonb)
                   ON CONFLICT (catalog_scope,kind,logical_id) DO UPDATE
                   SET draft_revision=EXCLUDED.draft_revision, draft=EXCLUDED.draft""",
                self.catalog_scope,
                definition.kind.value,
                definition.logical_id,
                head.draft_revision,
                head.published_revision,
                json.dumps(stable_json_dump(head)),
            )
            return head

    async def get_draft(self, kind: str, logical_id: str) -> AuthoringHead:
        async with self._transaction() as connection:
            row = await self._head(connection, kind, logical_id)
            if row is None or row["draft"] is None:
                raise DefinitionNotFound(f"authoring head not found: {(kind, logical_id)}")
            return AuthoringHead.model_validate(
                {
                    **_json(row["draft"]),
                    "published_revision": row["published_revision"],
                }
            )

    async def _event(
        self,
        connection: asyncpg.Connection,
        ref: ExactDefinitionRef,
        operation: str,
        occurred_at: datetime,
    ) -> None:
        event: dict[str, Any] = {
            "tenant_scope": self.catalog_scope,
            "asset_kind": ref.kind.value,
            "logical_id": ref.logical_id,
            "revision": ref.revision,
            "source_digest": ref.digest,
            "operation": operation,
        }
        event_id = sha256_digest(event)
        event.update(
            {
                "event_id": event_id,
                "state": "pending",
                "attempt_count": 0,
                "lease_owner": None,
                "lease_expires_at": None,
                "next_attempt_at": occurred_at.isoformat(),
                "last_error_code": None,
                "poison_reason": None,
                "completed_at": None,
                "created_at": occurred_at.isoformat(),
            }
        )
        await self._insert(connection, "catalog-event/1", event_id, event)

    async def publish(
        self,
        definition: Definition,
        actor_id: str,
        published_at: datetime,
        expected_head_revision: int,
        expected_draft_revision: int | None = None,
    ) -> PublishedDefinition:
        async with self._transaction(write=True) as connection:
            head = await self._head(connection, definition.kind.value, definition.logical_id)
            current = head["published_revision"] if head else 0
            draft_revision = head["draft_revision"] if head else 0
            if current != expected_head_revision:
                raise DefinitionConflict("published definition head changed concurrently")
            if expected_draft_revision is not None and (
                head is None or head["draft"] is None or draft_revision != expected_draft_revision
            ):
                raise DefinitionConflict("draft definition head changed concurrently")
            ref = ExactDefinitionRef(
                kind=definition.kind,
                logical_id=definition.logical_id,
                revision=current + 1,
                digest=sha256_digest(definition),
            )
            published = PublishedDefinition(
                ref=ref,
                definition=definition,
                published_at=published_at,
                published_by=actor_id,
            )
            await self._insert(
                connection,
                "published-definition/1",
                _key(ref.kind.value, ref.logical_id, ref.revision),
                stable_json_dump(published),
            )
            await connection.execute(
                """INSERT INTO belllabs_control.definition_catalog_heads
                   (catalog_scope,kind,logical_id,published_revision) VALUES ($1,$2,$3,$4)
                   ON CONFLICT (catalog_scope,kind,logical_id) DO UPDATE
                   SET published_revision=EXCLUDED.published_revision""",
                self.catalog_scope,
                ref.kind.value,
                ref.logical_id,
                ref.revision,
            )
            await self._event(connection, ref, "upsert", published_at)
            return published

    async def _get(
        self, connection: asyncpg.Connection, ref: ExactDefinitionRef
    ) -> PublishedDefinition:
        identity = _key(ref.kind.value, ref.logical_id, ref.revision)
        payload = await self._record(connection, "published-definition/1", identity)
        if payload is None:
            raise DefinitionNotFound(f"definition not found: {identity}")
        published = PublishedDefinition.model_validate(payload)
        if published.ref.model_dump(exclude={"lifecycle_status"}) != ref.model_dump(
            exclude={"lifecycle_status"}
        ):
            raise ReferenceMismatch("requested exact reference does not match stored revision")
        if sha256_digest(published.definition) != ref.digest:
            raise ReferenceMismatch("stored definition digest mismatch")
        retirement = await self._record(connection, "definition-retirement/1", identity)
        if retirement is not None:
            published = PublishedDefinition(
                ref=published.ref.model_copy(update={"lifecycle_status": "retired"}),
                definition=published.definition,
                published_at=published.published_at,
                published_by=published.published_by,
                retired_at=retirement["retired_at"],
            )
        return published

    async def get(self, ref: ExactDefinitionRef) -> PublishedDefinition:
        async with self._transaction() as connection:
            return await self._get(connection, ref)

    async def resolve(self, alias: AliasRef, *, selectable: bool = True) -> AliasBinding:
        async with self._transaction() as connection:
            payload = await connection.fetchval(
                """SELECT binding FROM belllabs_control.definition_catalog_aliases
                   WHERE catalog_scope=$1 AND kind=$2 AND logical_id=$3 AND alias=$4""",
                self.catalog_scope,
                alias.kind.value,
                alias.logical_id,
                alias.alias,
            )
            if payload is None:
                raise DefinitionNotFound("alias not found")
            binding = AliasBinding.model_validate(_json(payload))
            target = await self._get(connection, binding.target)
            if selectable and target.retired_at is not None:
                raise RetiredDefinition("alias points to retired definition")
            return binding

    async def list_alias_bindings(self) -> tuple[AliasBinding, ...]:
        """Read verified alias targets only within the configured application catalog."""
        async with self._transaction() as connection:
            rows = await connection.fetch(
                """SELECT kind, logical_id, alias, binding
                   FROM belllabs_control.definition_catalog_aliases
                   WHERE catalog_scope=$1 ORDER BY kind, logical_id, alias""",
                self.catalog_scope,
            )
            bindings = []
            for row in rows:
                binding = AliasBinding.model_validate(_json(row["binding"]))
                if (
                    binding.alias_ref.kind.value,
                    binding.alias_ref.logical_id,
                    binding.alias_ref.alias,
                ) != (row["kind"], row["logical_id"], row["alias"]):
                    raise ReferenceMismatch("catalog alias payload identity mismatch")
                await self._get(connection, binding.target)
                bindings.append(binding)
            return tuple(bindings)

    async def move_alias(
        self,
        alias: AliasRef,
        target: ExactDefinitionRef,
        actor_id: str,
        moved_at: datetime,
    ) -> AliasBinding:
        if alias.kind != target.kind or alias.logical_id != target.logical_id:
            raise ReferenceMismatch("alias identity and target identity must match")
        async with self._transaction(write=True) as connection:
            published = await self._get(connection, target)
            if published.retired_at is not None:
                raise RetiredDefinition("cannot move an alias to a retired revision")
            binding = AliasBinding(
                alias_ref=alias, target=target, moved_at=moved_at, moved_by=actor_id
            )
            payload = stable_json_dump(binding)
            await self._insert(connection, "alias-movement/1", sha256_digest(payload), payload)
            await connection.execute(
                """INSERT INTO belllabs_control.definition_catalog_aliases
                   (catalog_scope,kind,logical_id,alias,binding) VALUES ($1,$2,$3,$4,$5::jsonb)
                   ON CONFLICT (catalog_scope,kind,logical_id,alias)
                   DO UPDATE SET binding=EXCLUDED.binding""",
                self.catalog_scope,
                alias.kind.value,
                alias.logical_id,
                alias.alias,
                json.dumps(payload),
            )
            return binding

    async def retire(
        self,
        ref: ExactDefinitionRef,
        actor_id: str,
        retired_at: datetime,
    ) -> PublishedDefinition:
        async with self._transaction(write=True) as connection:
            published = await self._get(connection, ref)
            if published.retired_at is not None:
                return published
            await self._insert(
                connection,
                "definition-retirement/1",
                _key(ref.kind.value, ref.logical_id, ref.revision),
                {"retired_at": retired_at.isoformat(), "retired_by": actor_id},
            )
            await self._event(connection, ref, "retire", retired_at)
            return await self._get(connection, ref)

    async def save_erc_record(self, record: dict[str, Any]) -> None:
        digest, compilation_id = str(record["digest"]), str(record["compilation_id"])
        async with self._transaction(write=True) as connection:
            await self._insert(connection, "effective-run-configuration/1", digest, record)
            await self._insert(
                connection, "compilation-index/1", compilation_id, {"digest": digest}
            )

    async def get_erc_record(self, digest: str) -> dict[str, Any]:
        async with self._transaction() as connection:
            record = await self._record(connection, "effective-run-configuration/1", digest)
            if record is None:
                raise DefinitionNotFound(f"ERC not found: {digest}")
            return record

    async def list_projection_events(self) -> tuple[dict[str, Any], ...]:
        async with self._transaction() as connection:
            rows = await connection.fetch(
                """SELECT identity FROM belllabs_control.definition_catalog_records
                   WHERE catalog_scope=$1 AND contract='catalog-event/1' ORDER BY identity""",
                self.catalog_scope,
            )
            result = []
            for row in rows:
                event = await self._record(connection, "catalog-event/1", row["identity"])
                if event is not None:
                    result.append(event)
            return tuple(result)

    async def list_published_definition_refs(self) -> tuple[ExactDefinitionRef, ...]:
        """List only this installation's verified immutable publication identities."""
        async with self._transaction() as connection:
            rows = await connection.fetch(
                """SELECT identity FROM belllabs_control.definition_catalog_records
                   WHERE catalog_scope=$1 AND contract='published-definition/1'
                   ORDER BY identity""",
                self.catalog_scope,
            )
            refs = []
            for row in rows:
                payload = await self._record(connection, "published-definition/1", row["identity"])
                if payload is not None:
                    publication = PublishedDefinition.model_validate(payload)
                    verified = await self._get(connection, publication.ref)
                    refs.append(verified.ref)
            return tuple(
                sorted(refs, key=lambda ref: (ref.kind.value, ref.logical_id, ref.revision))
            )
