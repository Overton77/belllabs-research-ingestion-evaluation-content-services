"""Optional Biotech record persistence; never installed by the Mission Control kernel."""

from __future__ import annotations

import asyncpg

from biotech_mission_adapters.application.schema.schema_grounding_repository import _verify_record
from biotech_mission_adapters.application.web_research.web_research_repository import (
    WebResearchRecordConflict,
    WebResearchRecordNotFound,
    _record_id_from_ref,
    web_research_record_ref,
)
from biotech_mission_adapters.domain.coordinator.web_research_runtime import (
    WebResearchRecordEnvelope,
)
from biotech_mission_adapters.domain.schema_grounding.contracts import (
    SchemaGroundingRecordEnvelope,
    SchemaGroundingRecordType,
)
from biotech_mission_adapters.domain.schema_grounding.errors import (
    CatalogPublicationConflict,
    SchemaGroundingRecordNotFound,
)


class _Records:
    def __init__(self, pool: asyncpg.Pool) -> None:
        self.pool = pool

    async def scope(self, connection: asyncpg.Connection, request_scope: str) -> None:
        if not request_scope.strip():
            raise ValueError("Biotech records require an explicit request scope")
        await connection.execute(
            "SELECT set_config('biotech.request_scope', $1, true)", request_scope
        )

    async def append(
        self,
        kind: str,
        record: SchemaGroundingRecordEnvelope | WebResearchRecordEnvelope,
        identity: str,
        intent: str | None = None,
    ) -> str:
        async with self.pool.acquire() as connection, connection.transaction():
            await self.scope(connection, record.request_scope)
            for key in sorted({f"id:{identity}", f"intent:{record.run_id}:{intent}"}):
                await connection.execute(
                    "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
                    f"biotech:{record.request_scope}:{kind}:{key}",
                )
            rows = await connection.fetch(
                """SELECT payload FROM biotech_mission_adapters.records
                   WHERE request_scope=$1 AND record_kind=$2 AND
                   (record_identity=$3 OR
                    ($4::text IS NOT NULL AND run_id=$5 AND intent_key=$4))""",
                record.request_scope,
                kind,
                identity,
                intent,
                record.run_id,
            )
            if len(rows) > 1:
                raise ValueError("record identity and intent resolve to different records")
            if rows:
                return rows[0]["payload"]
            payload = record.model_dump_json()
            await connection.execute(
                """INSERT INTO biotech_mission_adapters.records
                   (request_scope,record_kind,record_identity,run_id,intent_key,payload)
                   VALUES ($1,$2,$3,$4,$5,$6::jsonb)""",
                record.request_scope,
                kind,
                identity,
                record.run_id,
                intent,
                payload,
            )
            return payload

    async def find(
        self,
        kind: str,
        scope: str,
        *,
        identity: str | None = None,
        run_id: str | None = None,
        intent: str | None = None,
    ) -> list[str]:
        async with self.pool.acquire() as connection, connection.transaction():
            await self.scope(connection, scope)
            rows = await connection.fetch(
                """SELECT payload FROM biotech_mission_adapters.records
                   WHERE request_scope=$1 AND record_kind=$2
                   AND ($3::text IS NULL OR record_identity=$3)
                   AND ($4::text IS NULL OR run_id=$4)
                   AND ($5::text IS NULL OR intent_key=$5)
                   ORDER BY payload->>'created_at', record_identity""",
                scope,
                kind,
                identity,
                run_id,
                intent,
            )
            return [row["payload"] for row in rows]


class PostgresSchemaGroundingRecordRepository:
    def __init__(self, pool: asyncpg.Pool) -> None:
        self._records = _Records(pool)

    async def append(self, record: SchemaGroundingRecordEnvelope) -> SchemaGroundingRecordEnvelope:
        _verify_record(record)
        prior = SchemaGroundingRecordEnvelope.model_validate_json(
            await self._records.append(
                "schema_grounding", record, f"{record.record_type}:{record.record_id}"
            )
        )
        _verify_record(prior)
        if prior != record:
            raise CatalogPublicationConflict("immutable schema record identity was reused")
        return prior

    async def get(
        self, request_scope: str, record_type: SchemaGroundingRecordType, record_id: str
    ) -> SchemaGroundingRecordEnvelope:
        rows = await self._records.find(
            "schema_grounding", request_scope, identity=f"{record_type}:{record_id}"
        )
        if not rows:
            raise SchemaGroundingRecordNotFound(f"{record_type} record not found: {record_id}")
        record = SchemaGroundingRecordEnvelope.model_validate_json(rows[0])
        _verify_record(record)
        return record

    async def list_for_run(
        self,
        request_scope: str,
        run_id: str,
        *,
        record_type: SchemaGroundingRecordType | None = None,
    ) -> tuple[SchemaGroundingRecordEnvelope, ...]:
        records = tuple(
            SchemaGroundingRecordEnvelope.model_validate_json(row)
            for row in await self._records.find("schema_grounding", request_scope, run_id=run_id)
        )
        for record in records:
            _verify_record(record)
        return tuple(
            record for record in records if record_type is None or record.record_type == record_type
        )


class PostgresWebResearchRecordRepository:
    def __init__(self, pool: asyncpg.Pool) -> None:
        self._records = _Records(pool)

    async def append(self, record: WebResearchRecordEnvelope) -> WebResearchRecordEnvelope:
        # Revalidate immutable payload digests even when callers used model_copy.
        record = WebResearchRecordEnvelope.model_validate_json(record.model_dump_json())
        try:
            prior = WebResearchRecordEnvelope.model_validate_json(
                await self._records.append(
                    "web_research", record, record.record_id, record.intent_key
                )
            )
        except ValueError as error:
            raise WebResearchRecordConflict(str(error)) from error
        if prior != record:
            raise WebResearchRecordConflict("web-research record identity or intent was reused")
        return prior

    async def get(
        self, request_scope: str, run_id: str, record_ref: str
    ) -> WebResearchRecordEnvelope:
        rows = await self._records.find(
            "web_research", request_scope, identity=_record_id_from_ref(record_ref), run_id=run_id
        )
        if not rows:
            raise WebResearchRecordNotFound(f"web-research record not found: {record_ref}")
        record = WebResearchRecordEnvelope.model_validate_json(rows[0])
        if web_research_record_ref(record) != record_ref:
            raise WebResearchRecordNotFound("web-research record digest mismatch")
        return record

    async def get_by_intent(
        self, request_scope: str, run_id: str, intent_key: str
    ) -> WebResearchRecordEnvelope | None:
        rows = await self._records.find(
            "web_research", request_scope, run_id=run_id, intent=intent_key
        )
        return WebResearchRecordEnvelope.model_validate_json(rows[0]) if rows else None
