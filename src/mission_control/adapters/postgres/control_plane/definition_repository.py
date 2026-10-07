"""Transactional installation catalog over the common mission_control component.

The trusted composition root supplies ``catalog_scope`` (``mc/{installation}/{app}/catalog``);
request callers cannot select another installation's catalog. Published definitions are
canonical ``asset_version`` rows with append-only ``asset_decision`` history (admit at
publication, retire at retirement; the row status moves forward only). Drafts, alias
pointers, effective run configurations, compilation indexes and catalog events are
support records. Every statement runs after ``apply_catalog_scope`` inside one explicit
transaction; writes serialize on an installation catalog advisory lock.
"""

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any, Final
from uuid import UUID

import asyncpg

from mission_control.adapters.postgres.control_plane.catalog_assets import (
    ASSET_KIND,
    CATALOG_SERVICE_ACTOR,
    PUBLICATION_POLICY_REF,
    PUBLISHED_DEFINITION_CONTRACT,
    definition_asset_id,
    definition_manifest_ref,
    insert_projection_job,
    json_value,
)
from mission_control.adapters.postgres.scope import apply_catalog_scope, parse_catalog_scope
from mission_control.application.capabilities.catalog_projection_events import (
    CatalogProjectionEvent,
)
from mission_control.contracts.identities import uuid7
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

_UNCHECKED: Final = object()


def _key(*parts: str | int) -> str:
    return json.dumps(parts, separators=(",", ":"))


class PostgresDefinitionRepository:
    def __init__(self, pool: asyncpg.Pool, *, catalog_scope: str) -> None:
        if not catalog_scope:
            raise ValueError("trusted installation catalog scope is required")
        self._installation_id, self._application_id = parse_catalog_scope(catalog_scope)
        self._pool = pool
        self.catalog_scope = catalog_scope

    @property
    def _scope(self) -> tuple[UUID, str]:
        return self._installation_id, self._application_id

    @asynccontextmanager
    async def _transaction(self, *, write: bool = False) -> AsyncIterator[asyncpg.Connection]:
        async with self._pool.acquire() as connection, connection.transaction():
            await apply_catalog_scope(connection, *self._scope)
            if write:
                # Catalog mutation volume is bounded; installation-wide serialization makes
                # publication + decision + event and alias + retirement decisions atomic.
                await connection.execute(
                    "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
                    "definition-catalog:" + self.catalog_scope,
                )
            yield connection

    # -- support records ------------------------------------------------------------

    async def _record(
        self, connection: asyncpg.Connection, contract: str, identity: str
    ) -> dict[str, Any] | None:
        row = await connection.fetchrow(
            """SELECT payload, payload_digest FROM mission_control.catalog_record
               WHERE installation_id=$1 AND application_id=$2 AND contract=$3
                 AND record_key=$4""",
            *self._scope,
            contract,
            identity,
        )
        if row is None:
            return None
        payload: dict[str, Any] = json_value(row["payload"])
        if sha256_digest(payload) != row["payload_digest"]:
            raise ReferenceMismatch("catalog record payload digest mismatch")
        return payload

    async def _insert(
        self,
        connection: asyncpg.Connection,
        contract: str,
        identity: str,
        payload: dict[str, Any],
        *,
        actor_ref: str = CATALOG_SERVICE_ACTOR,
    ) -> None:
        payload = canonical_data(payload)["payload"]
        prior = await self._record(connection, contract, identity)
        if prior is not None:
            if sha256_digest(prior) != sha256_digest(payload):
                raise DefinitionConflict("immutable catalog identity already has different content")
            return
        await connection.execute(
            """INSERT INTO mission_control.catalog_record
               (installation_id, application_id, catalog_record_id, contract, record_key,
                payload, payload_digest, created_at, created_by_actor_ref)
               VALUES ($1,$2,$3,$4,$5,$6::jsonb,$7,clock_timestamp(),$8)""",
            *self._scope,
            uuid7(),
            contract,
            identity,
            json.dumps(payload, allow_nan=False),
            sha256_digest(payload),
            actor_ref,
        )

    async def _head(
        self, connection: asyncpg.Connection, kind: str, logical_id: str
    ) -> asyncpg.Record | None:
        return await connection.fetchrow(
            """SELECT draft, draft_revision FROM mission_control.catalog_head
               WHERE installation_id=$1 AND application_id=$2 AND definition_kind=$3
                 AND logical_id=$4""",
            *self._scope,
            kind,
            logical_id,
        )

    async def _published_revision(
        self, connection: asyncpg.Connection, kind: str, logical_id: str
    ) -> int:
        value = await connection.fetchval(
            """SELECT coalesce(max(version::bigint), 0) FROM mission_control.asset_version
               WHERE installation_id=$1 AND application_id=$2 AND asset_id=$3
                 AND contract=$4 AND version ~ '^[1-9][0-9]*$'""",
            *self._scope,
            definition_asset_id(kind, logical_id),
            PUBLISHED_DEFINITION_CONTRACT,
        )
        return int(value)

    # -- drafts --------------------------------------------------------------------

    async def save_draft(
        self,
        definition: Definition,
        actor_id: str,
        updated_at: datetime,
        expected_draft_revision: int,
    ) -> AuthoringHead:
        kind, logical_id = definition.kind.value, definition.logical_id
        async with self._transaction(write=True) as connection:
            prior = await self._head(connection, kind, logical_id)
            current = int(prior["draft_revision"]) if prior else 0
            if current != expected_draft_revision:
                raise DefinitionConflict(
                    f"expected draft revision {expected_draft_revision}, current {current}"
                )
            head = AuthoringHead(
                kind=definition.kind,
                logical_id=logical_id,
                draft_revision=current + 1,
                published_revision=await self._published_revision(connection, kind, logical_id),
                definition=definition,
                updated_at=updated_at,
                updated_by=actor_id,
            )
            draft = json.dumps(stable_json_dump(head), allow_nan=False)
            if prior is None:
                await connection.execute(
                    """INSERT INTO mission_control.catalog_head
                       (installation_id, application_id, catalog_head_id, definition_kind,
                        logical_id, draft_revision, draft, version, updated_at, created_at,
                        created_by_actor_ref)
                       VALUES ($1,$2,$3,$4,$5,$6,$7::jsonb,$6,$8,clock_timestamp(),$9)""",
                    *self._scope,
                    uuid7(),
                    kind,
                    logical_id,
                    head.draft_revision,
                    draft,
                    updated_at,
                    actor_id,
                )
            else:
                updated = await connection.fetchval(
                    """UPDATE mission_control.catalog_head
                       SET draft_revision=$5, draft=$6::jsonb, version=$5, updated_at=$7
                       WHERE installation_id=$1 AND application_id=$2 AND definition_kind=$3
                         AND logical_id=$4 AND draft_revision=$8
                       RETURNING draft_revision""",
                    *self._scope,
                    kind,
                    logical_id,
                    head.draft_revision,
                    draft,
                    updated_at,
                    current,
                )
                if updated is None:
                    raise DefinitionConflict("draft definition head changed concurrently")
            return head

    async def get_draft(self, kind: str, logical_id: str) -> AuthoringHead:
        async with self._transaction() as connection:
            row = await self._head(connection, kind, logical_id)
            if row is None:
                raise DefinitionNotFound(f"authoring head not found: {(kind, logical_id)}")
            return AuthoringHead.model_validate(
                {
                    **json_value(row["draft"]),
                    "published_revision": await self._published_revision(
                        connection, kind, logical_id
                    ),
                }
            )

    # -- events --------------------------------------------------------------------

    async def _event(
        self,
        connection: asyncpg.Connection,
        ref: ExactDefinitionRef,
        operation: str,
        occurred_at: datetime,
        *,
        actor_ref: str = CATALOG_SERVICE_ACTOR,
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
        await self._insert(connection, "catalog-event/1", event_id, event, actor_ref=actor_ref)
        job = stable_json_dump(CatalogProjectionEvent.model_validate(event))
        await insert_projection_job(
            connection,
            installation_id=self._installation_id,
            application_id=self._application_id,
            event=job,
            actor_ref=actor_ref,
            recorded_at=occurred_at,
        )

    # -- publication -------------------------------------------------------------

    async def publish(
        self,
        definition: Definition,
        actor_id: str,
        published_at: datetime,
        expected_head_revision: int,
        expected_draft_revision: int | None = None,
    ) -> PublishedDefinition:
        kind, logical_id = definition.kind.value, definition.logical_id
        async with self._transaction(write=True) as connection:
            head = await self._head(connection, kind, logical_id)
            current = await self._published_revision(connection, kind, logical_id)
            if current != expected_head_revision:
                raise DefinitionConflict("published definition head changed concurrently")
            if expected_draft_revision is not None and (
                head is None or int(head["draft_revision"]) != expected_draft_revision
            ):
                raise DefinitionConflict("draft definition head changed concurrently")
            ref = ExactDefinitionRef(
                kind=definition.kind,
                logical_id=logical_id,
                revision=current + 1,
                digest=sha256_digest(definition),
            )
            published = PublishedDefinition(
                ref=ref, definition=definition, published_at=published_at, published_by=actor_id
            )
            manifest = canonical_data(stable_json_dump(published))["payload"]
            asset_version_id = uuid7()
            await connection.execute(
                """INSERT INTO mission_control.asset_version
                   (installation_id, application_id, asset_version_id, asset_id, version, kind,
                    contract, manifest_ref, manifest_digest, manifest, required_compatibility,
                    status, version_no, updated_at, created_at, created_by_actor_ref)
                   VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10::jsonb,'{}'::text[],'admitted',1,$11,
                           clock_timestamp(),$12)""",
                *self._scope,
                asset_version_id,
                definition_asset_id(definition.kind, logical_id),
                str(ref.revision),
                ASSET_KIND[definition.kind],
                PUBLISHED_DEFINITION_CONTRACT,
                definition_manifest_ref(definition.kind, logical_id, ref.revision),
                sha256_digest(manifest),
                json.dumps(manifest, allow_nan=False),
                published_at,
                actor_id,
            )
            await self._decision(
                connection, asset_version_id, "admit", "published", actor_id, published_at
            )
            await self._event(connection, ref, "upsert", published_at, actor_ref=actor_id)
            return published

    async def _decision(
        self,
        connection: asyncpg.Connection,
        asset_version_id: UUID,
        decision: str,
        disposition: str,
        actor_id: str,
        decided_at: datetime,
    ) -> None:
        await connection.execute(
            """INSERT INTO mission_control.asset_decision
               (installation_id, application_id, asset_decision_id, asset_version_id, decision,
                disposition, actor_ref, evidence_refs, policy_ref, decided_at, created_at,
                created_by_actor_ref)
               VALUES ($1,$2,$3,$4,$5,$6,$7,'{}'::text[],$8,$9,clock_timestamp(),$7)""",
            *self._scope,
            uuid7(),
            asset_version_id,
            decision,
            disposition,
            actor_id,
            PUBLICATION_POLICY_REF,
            decided_at,
        )

    async def _published(
        self, connection: asyncpg.Connection, ref: ExactDefinitionRef
    ) -> tuple[PublishedDefinition, UUID]:
        identity = _key(ref.kind.value, ref.logical_id, ref.revision)
        row = await connection.fetchrow(
            """SELECT asset_version_id, manifest, manifest_digest, status
               FROM mission_control.asset_version
               WHERE installation_id=$1 AND application_id=$2 AND asset_id=$3 AND version=$4
                 AND contract=$5""",
            *self._scope,
            definition_asset_id(ref.kind, ref.logical_id),
            str(ref.revision),
            PUBLISHED_DEFINITION_CONTRACT,
        )
        if row is None or row["status"] == "proposed":
            raise DefinitionNotFound(f"definition not found: {identity}")
        payload = json_value(row["manifest"])
        if sha256_digest(payload) != row["manifest_digest"]:
            raise ReferenceMismatch("catalog asset manifest digest mismatch")
        published = PublishedDefinition.model_validate(payload)
        if published.ref.model_dump(exclude={"lifecycle_status"}) != ref.model_dump(
            exclude={"lifecycle_status"}
        ):
            raise ReferenceMismatch("requested exact reference does not match stored revision")
        if sha256_digest(published.definition) != ref.digest:
            raise ReferenceMismatch("stored definition digest mismatch")
        if row["status"] in {"retired", "revoked"}:
            retired_at = await connection.fetchval(
                """SELECT min(decided_at) FROM mission_control.asset_decision
                   WHERE installation_id=$1 AND application_id=$2 AND asset_version_id=$3
                     AND decision IN ('retire', 'revoke')""",
                *self._scope,
                row["asset_version_id"],
            )
            if retired_at is None:
                raise ReferenceMismatch("retired catalog asset has no retirement decision")
            published = PublishedDefinition(
                ref=published.ref.model_copy(update={"lifecycle_status": "retired"}),
                definition=published.definition,
                published_at=published.published_at,
                published_by=published.published_by,
                retired_at=retired_at,
            )
        return published, row["asset_version_id"]

    async def _get(
        self, connection: asyncpg.Connection, ref: ExactDefinitionRef
    ) -> PublishedDefinition:
        return (await self._published(connection, ref))[0]

    async def get(self, ref: ExactDefinitionRef) -> PublishedDefinition:
        async with self._transaction() as connection:
            return await self._get(connection, ref)

    # -- aliases ---------------------------------------------------------------------

    async def resolve(self, alias: AliasRef, *, selectable: bool = True) -> AliasBinding:
        async with self._transaction() as connection:
            payload = await connection.fetchval(
                """SELECT binding FROM mission_control.catalog_alias
                   WHERE installation_id=$1 AND application_id=$2 AND definition_kind=$3
                     AND logical_id=$4 AND alias=$5""",
                *self._scope,
                alias.kind.value,
                alias.logical_id,
                alias.alias,
            )
            if payload is None:
                raise DefinitionNotFound("alias not found")
            binding = AliasBinding.model_validate(json_value(payload))
            target = await self._get(connection, binding.target)
            if selectable and target.retired_at is not None:
                raise RetiredDefinition("alias points to retired definition")
            return binding

    async def list_alias_bindings(self) -> tuple[AliasBinding, ...]:
        """Read verified alias targets only within the configured application catalog."""
        async with self._transaction() as connection:
            rows = await connection.fetch(
                """SELECT definition_kind, logical_id, alias, binding
                   FROM mission_control.catalog_alias
                   WHERE installation_id=$1 AND application_id=$2
                   ORDER BY definition_kind, logical_id, alias""",
                *self._scope,
            )
            bindings = []
            for row in rows:
                binding = AliasBinding.model_validate(json_value(row["binding"]))
                if (
                    binding.alias_ref.kind.value,
                    binding.alias_ref.logical_id,
                    binding.alias_ref.alias,
                ) != (row["definition_kind"], row["logical_id"], row["alias"]):
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
        *,
        expected_target: ExactDefinitionRef | None | object = _UNCHECKED,
    ) -> AliasBinding:
        """Move an alias by compare-and-swap; never last-write-wins.

        ``expected_target`` (when supplied) is the caller's observed old target, or ``None``
        when the alias must not exist yet. Independently of it, the pointer update is
        conditional on the exact row version and old target read under the catalog lock.
        """
        if alias.kind != target.kind or alias.logical_id != target.logical_id:
            raise ReferenceMismatch("alias identity and target identity must match")
        async with self._transaction(write=True) as connection:
            published, target_id = await self._published(connection, target)
            if published.retired_at is not None:
                raise RetiredDefinition("cannot move an alias to a retired revision")
            current = await connection.fetchrow(
                """SELECT catalog_alias_id, version, target_asset_version_id, binding
                   FROM mission_control.catalog_alias
                   WHERE installation_id=$1 AND application_id=$2 AND definition_kind=$3
                     AND logical_id=$4 AND alias=$5 FOR UPDATE""",
                *self._scope,
                alias.kind.value,
                alias.logical_id,
                alias.alias,
            )
            if expected_target is not _UNCHECKED:
                observed = (
                    None
                    if current is None
                    else AliasBinding.model_validate(json_value(current["binding"])).target
                )
                if observed != expected_target:
                    raise DefinitionConflict("alias target changed concurrently")
            binding = AliasBinding(
                alias_ref=alias, target=target, moved_at=moved_at, moved_by=actor_id
            )
            payload = stable_json_dump(binding)
            await self._insert(
                connection,
                "alias-movement/1",
                sha256_digest(payload),
                payload,
                actor_ref=actor_id,
            )
            encoded = json.dumps(payload, allow_nan=False)
            if current is None:
                await connection.execute(
                    """INSERT INTO mission_control.catalog_alias
                       (installation_id, application_id, catalog_alias_id, definition_kind,
                        logical_id, alias, target_asset_version_id, binding, version,
                        updated_at, created_at, created_by_actor_ref)
                       VALUES ($1,$2,$3,$4,$5,$6,$7,$8::jsonb,1,$9,clock_timestamp(),$10)""",
                    *self._scope,
                    uuid7(),
                    alias.kind.value,
                    alias.logical_id,
                    alias.alias,
                    target_id,
                    encoded,
                    moved_at,
                    actor_id,
                )
            else:
                moved = await connection.fetchval(
                    """UPDATE mission_control.catalog_alias
                       SET target_asset_version_id=$4, binding=$5::jsonb, version=version + 1,
                           updated_at=$6
                       WHERE installation_id=$1 AND application_id=$2 AND catalog_alias_id=$3
                         AND version=$7 AND target_asset_version_id=$8
                       RETURNING version""",
                    *self._scope,
                    current["catalog_alias_id"],
                    target_id,
                    encoded,
                    moved_at,
                    current["version"],
                    current["target_asset_version_id"],
                )
                if moved is None:
                    raise DefinitionConflict("alias target changed concurrently")
            return binding

    # -- retirement ----------------------------------------------------------------

    async def retire(
        self,
        ref: ExactDefinitionRef,
        actor_id: str,
        retired_at: datetime,
    ) -> PublishedDefinition:
        async with self._transaction(write=True) as connection:
            published, asset_version_id = await self._published(connection, ref)
            if published.retired_at is not None:
                return published
            await self._decision(
                connection, asset_version_id, "retire", "retired", actor_id, retired_at
            )
            changed = await connection.fetchval(
                """UPDATE mission_control.asset_version
                   SET status='retired', version_no=version_no + 1, updated_at=$4
                   WHERE installation_id=$1 AND application_id=$2 AND asset_version_id=$3
                     AND status='admitted'
                   RETURNING version_no""",
                *self._scope,
                asset_version_id,
                retired_at,
            )
            if changed is None:
                raise DefinitionConflict("catalog asset lifecycle changed concurrently")
            await self._event(connection, ref, "retire", retired_at, actor_ref=actor_id)
            return await self._get(connection, ref)

    # -- effective run configurations --------------------------------------------

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
                """SELECT record_key FROM mission_control.catalog_record
                   WHERE installation_id=$1 AND application_id=$2 AND contract='catalog-event/1'
                   ORDER BY record_key""",
                *self._scope,
            )
            result = []
            for row in rows:
                event = await self._record(connection, "catalog-event/1", row["record_key"])
                if event is not None:
                    result.append(event)
            return tuple(result)

    async def list_published_definition_refs(self) -> tuple[ExactDefinitionRef, ...]:
        """List only this installation's verified immutable publication identities."""
        async with self._transaction() as connection:
            rows = await connection.fetch(
                """SELECT manifest FROM mission_control.asset_version
                   WHERE installation_id=$1 AND application_id=$2 AND contract=$3
                     AND status <> 'proposed'
                   ORDER BY asset_id, version""",
                *self._scope,
                PUBLISHED_DEFINITION_CONTRACT,
            )
            refs = []
            for row in rows:
                publication = PublishedDefinition.model_validate(json_value(row["manifest"]))
                verified = await self._get(connection, publication.ref)
                refs.append(verified.ref)
            return tuple(
                sorted(refs, key=lambda ref: (ref.kind.value, ref.logical_id, ref.revision))
            )
