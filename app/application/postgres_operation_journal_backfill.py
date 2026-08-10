from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

import asyncpg

from app.application.operation_journal_backfill import (
    MIGRATION_STREAM,
    BackfillBatch,
    BackfillProgress,
    ClaimAdmission,
    OperationJournalBackfillTarget,
    QuarantineAdmission,
    SettlementAdmission,
    SourceSnapshot,
)
from app.domain.control_plane.canonical import sha256_digest
from app.domain.operation_execution.journal import (
    OperationEffectClaim,
    OperationJournalSettlement,
)
from app.domain.run_control.errors import IdempotencyConflict


class PostgresOperationJournalBackfillRepository(OperationJournalBackfillTarget):
    """Transactional, resumable destination for immutable legacy Mongo facts."""

    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    async def load_progress(
        self,
        *,
        request_scope: str,
        run_id: str,
    ) -> BackfillProgress | None:
        async with self._pool.acquire() as connection, connection.transaction():
            await _set_scope(connection, request_scope)
            row = await connection.fetchrow(
                """
                SELECT *
                FROM belllabs_control.operation_journal_backfill_batches
                WHERE request_scope = $1
                  AND migration_stream = $2
                  AND run_id = $3
                """,
                request_scope,
                MIGRATION_STREAM,
                run_id,
            )
        return _progress_from_row(row) if row is not None else None

    async def apply_batch(self, batch: BackfillBatch) -> None:
        if batch.dry_run:
            raise ValueError("dry-run batches must not be sent to PostgreSQL")
        async with self._pool.acquire() as connection, connection.transaction():
            await _set_scope(connection, batch.request_scope)
            await connection.execute(
                "SELECT pg_advisory_xact_lock(hashtext($1))",
                f"{MIGRATION_STREAM}:{batch.request_scope}",
            )
            await self._ensure_batch_row(connection, batch)
            progress_row = await connection.fetchrow(
                """
                SELECT source_cursor, source_snapshot_digest, status
                FROM belllabs_control.operation_journal_backfill_batches
                WHERE request_scope = $1
                  AND migration_stream = $2
                  AND run_id = $3
                FOR UPDATE
                """,
                batch.request_scope,
                MIGRATION_STREAM,
                batch.run_id,
            )
            if progress_row is None:
                raise RuntimeError("backfill progress row was not created")
            if progress_row["source_snapshot_digest"] != batch.source_snapshot.snapshot_digest:
                raise IdempotencyConflict("backfill run reused with another source snapshot")
            if batch.completed:
                if progress_row["source_cursor"] != batch.previous_cursor:
                    raise IdempotencyConflict("backfill completion cursor is stale")
                await connection.execute(
                    """
                    UPDATE belllabs_control.operation_journal_backfill_batches
                    SET status = 'completed', updated_at = $4, completed_at = $4,
                        source_aggregate_digest = $5,
                        target_aggregate_digest = $6
                    WHERE request_scope = $1
                      AND migration_stream = $2
                      AND run_id = $3
                    """,
                    batch.request_scope,
                    MIGRATION_STREAM,
                    batch.run_id,
                    datetime.now(UTC),
                    batch.source_aggregate_digest,
                    batch.target_aggregate_digest,
                )
                return
            if batch.cursor is None:
                raise ValueError("non-empty backfill batch requires a cursor")
            prior_batch = await connection.fetchrow(
                """
                SELECT batch_digest
                FROM belllabs_control.operation_journal_backfill_applied_batches
                WHERE request_scope = $1
                  AND migration_stream = $2
                  AND run_id = $3
                  AND batch_cursor = $4
                """,
                batch.request_scope,
                MIGRATION_STREAM,
                batch.run_id,
                batch.cursor,
            )
            if prior_batch is not None:
                if (
                    prior_batch["batch_digest"] != batch.batch_digest
                    or progress_row["source_cursor"] != batch.cursor
                ):
                    raise IdempotencyConflict("backfill batch cursor has conflicting content")
                return
            if progress_row["source_cursor"] != batch.previous_cursor:
                raise IdempotencyConflict("backfill batch cursor is stale")
            for claim_admission in batch.claims:
                await _admit_claim(connection, claim_admission)
            for settlement_admission in batch.settlements:
                await _admit_settlement(connection, settlement_admission)
            for quarantine_admission in batch.quarantines:
                await _admit_quarantine(
                    connection,
                    batch.request_scope,
                    quarantine_admission,
                )
            now = datetime.now(UTC)
            source_claim_count = len(batch.claims) + sum(
                item.source_collection == "operation_execution_claims" for item in batch.quarantines
            )
            source_settlement_count = len(batch.settlements) + sum(
                item.source_collection == "operation_execution_settlements"
                for item in batch.quarantines
            )
            await connection.execute(
                """
                INSERT INTO belllabs_control.operation_journal_backfill_applied_batches (
                    request_scope, migration_stream, run_id, batch_cursor,
                    previous_cursor, batch_digest, source_count,
                    admitted_claim_count, admitted_settlement_count,
                    quarantine_count, applied_at
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                """,
                batch.request_scope,
                MIGRATION_STREAM,
                batch.run_id,
                batch.cursor,
                batch.previous_cursor,
                batch.batch_digest,
                source_claim_count + source_settlement_count,
                len(batch.claims),
                len(batch.settlements),
                len(batch.quarantines),
                now,
            )
            updated = await connection.fetchval(
                """
                UPDATE belllabs_control.operation_journal_backfill_batches
                SET status = 'running',
                    source_cursor = $5,
                    source_claim_count = source_claim_count + $6,
                    source_settlement_count = source_settlement_count + $7,
                    target_claim_count = target_claim_count + $8,
                    target_settlement_count = target_settlement_count + $9,
                    quarantine_count = quarantine_count + $10,
                    source_aggregate_digest = $11,
                    target_aggregate_digest = $12,
                    updated_at = $13
                WHERE request_scope = $1
                  AND migration_stream = $2
                  AND run_id = $3
                  AND source_cursor IS NOT DISTINCT FROM $4
                RETURNING run_id
                """,
                batch.request_scope,
                MIGRATION_STREAM,
                batch.run_id,
                batch.previous_cursor,
                batch.cursor,
                source_claim_count,
                source_settlement_count,
                len(batch.claims),
                len(batch.settlements),
                len(batch.quarantines),
                batch.source_aggregate_digest,
                batch.target_aggregate_digest,
                now,
            )
            if updated is None:
                raise IdempotencyConflict("backfill batch compare-and-set failed")

    async def verify(
        self,
        *,
        request_scope: str,
        run_id: str,
    ) -> BackfillProgress:
        async with self._pool.acquire() as connection, connection.transaction():
            await _set_scope(connection, request_scope)
            row = await connection.fetchrow(
                """
                SELECT *
                FROM belllabs_control.operation_journal_backfill_batches
                WHERE request_scope = $1
                  AND migration_stream = $2
                  AND run_id = $3
                  AND status = 'completed'
                """,
                request_scope,
                MIGRATION_STREAM,
                run_id,
            )
            if row is None:
                raise RuntimeError("backfill cannot verify before completion")
            claim_rows = await connection.fetch(
                """
                SELECT *
                FROM belllabs_control.operation_effect_claims
                WHERE request_scope = $1
                  AND source_system = 'mongodb'
                  AND source_collection = 'operation_execution_claims'
                ORDER BY source_collection, source_document_id
                """,
                request_scope,
            )
            settlement_rows = await connection.fetch(
                """
                SELECT *
                FROM belllabs_control.operation_settlements
                WHERE request_scope = $1
                  AND source_system = 'mongodb'
                  AND source_collection = 'operation_execution_settlements'
                ORDER BY source_collection, source_document_id
                """,
                request_scope,
            )
            quarantine_rows = await connection.fetch(
                """
                SELECT source_collection, source_document_id, observed_digest
                FROM belllabs_control.operation_journal_backfill_quarantine
                WHERE request_scope = $1 AND migration_stream = $2
                ORDER BY source_collection, source_document_id
                """,
                request_scope,
                MIGRATION_STREAM,
            )
        persisted_target_digests = [
            str(item["target_canonical_digest"]) for item in (*claim_rows, *settlement_rows)
        ]
        source_items = [
            (
                str(item["source_collection"]),
                str(item["source_document_id"]),
                str(item["source_canonical_digest"]),
            )
            for item in (*claim_rows, *settlement_rows)
        ] + [
            (
                str(item["source_collection"]),
                str(item["source_document_id"]),
                str(item["observed_digest"]),
            )
            for item in quarantine_rows
        ]
        source_digest = _aggregate_digests([digest for _, _, digest in sorted(source_items)])
        target_digest = _aggregate_digests(persisted_target_digests)
        if (
            source_digest != row["source_aggregate_digest"]
            or target_digest != row["target_aggregate_digest"]
        ):
            raise RuntimeError("persisted backfill count/digest verification failed")
        if (
            len(claim_rows) != row["target_claim_count"]
            or len(settlement_rows) != row["target_settlement_count"]
            or len(quarantine_rows) != row["quarantine_count"]
        ):
            raise RuntimeError("persisted backfill count verification failed")
        return _progress_from_row(row)

    async def _ensure_batch_row(
        self,
        connection: asyncpg.Connection,
        batch: BackfillBatch,
    ) -> None:
        now = datetime.now(UTC)
        await connection.execute(
            """
            INSERT INTO belllabs_control.operation_journal_backfill_batches (
                request_scope, migration_stream, run_id, status,
                source_cursor, source_snapshot_digest, source_snapshot_payload,
                started_at, updated_at
            )
            VALUES ($1, $2, $3, 'running', NULL, $4, $5::jsonb, $6, $6)
            ON CONFLICT (request_scope, migration_stream, run_id) DO NOTHING
            """,
            batch.request_scope,
            MIGRATION_STREAM,
            batch.run_id,
            batch.source_snapshot.snapshot_digest,
            json.dumps(
                {
                    "request_scope": batch.source_snapshot.request_scope,
                    "claim_high_watermark": (batch.source_snapshot.claim_high_watermark),
                    "settlement_high_watermark": (batch.source_snapshot.settlement_high_watermark),
                    "record_count": batch.source_snapshot.record_count,
                    "aggregate_digest": batch.source_snapshot.aggregate_digest,
                    "captured_at": batch.source_snapshot.captured_at.isoformat(),
                },
                sort_keys=True,
                separators=(",", ":"),
            ),
            now,
        )


async def _admit_claim(
    connection: asyncpg.Connection,
    admission: ClaimAdmission,
) -> None:
    claim = admission.claim
    lineage = admission.lineage
    prior = await connection.fetchrow(
        """
        SELECT effect_claim_id, source_canonical_digest
        FROM belllabs_control.operation_effect_claims
        WHERE request_scope = $1
          AND source_system = $2
          AND source_collection = $3
          AND source_document_id = $4
        """,
        claim.request_scope,
        lineage.source_system,
        lineage.source_collection,
        lineage.source_document_id,
    )
    if prior is not None:
        if (
            prior["effect_claim_id"] != claim.effect_claim_id
            or prior["source_canonical_digest"] != lineage.source_canonical_digest
        ):
            raise IdempotencyConflict("legacy claim source identity has conflicting content")
        return
    await connection.execute(
        """
        INSERT INTO belllabs_control.operation_effect_claims (
            effect_claim_id, request_scope, belllabs_run_id,
            operation_contract_digest, idempotency_key, request_digest,
            semantic_binding_id, semantic_binding_digest, semantic_attempt_key,
            claim_mode, status, claimed_by, claimed_at, heartbeat_at, lease_expires_at,
            source_system, source_collection, source_document_id,
            source_recorded_at, source_canonical_digest, target_canonical_digest
        )
        VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
            $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21
        )
        ON CONFLICT (request_scope, operation_contract_digest, idempotency_key)
        DO NOTHING
        """,
        claim.effect_claim_id,
        claim.request_scope,
        claim.belllabs_run_id,
        claim.operation_contract_digest,
        claim.idempotency_key,
        claim.request_digest,
        claim.semantic_binding_id,
        claim.semantic_binding_digest,
        claim.semantic_attempt_key,
        claim.claim_mode,
        claim.status.value,
        claim.claimed_by,
        claim.claimed_at,
        claim.heartbeat_at,
        claim.lease_expires_at,
        lineage.source_system,
        lineage.source_collection,
        lineage.source_document_id,
        lineage.source_recorded_at,
        lineage.source_canonical_digest,
        sha256_digest(claim),
    )
    persisted = await connection.fetchrow(
        """
        SELECT *
        FROM belllabs_control.operation_effect_claims
        WHERE request_scope = $1
          AND operation_contract_digest = $2
          AND idempotency_key = $3
        """,
        claim.request_scope,
        claim.operation_contract_digest,
        claim.idempotency_key,
    )
    if persisted is None or _claim_from_row(persisted) != claim:
        raise IdempotencyConflict("legacy claim conflicts with an existing effect claim")


async def _admit_settlement(
    connection: asyncpg.Connection,
    admission: SettlementAdmission,
) -> None:
    settlement = admission.settlement
    lineage = admission.lineage
    prior = await connection.fetchrow(
        """
        SELECT settlement_id, source_canonical_digest
        FROM belllabs_control.operation_settlements
        WHERE request_scope = $1
          AND source_system = $2
          AND source_collection = $3
          AND source_document_id = $4
        """,
        settlement.request_scope,
        lineage.source_system,
        lineage.source_collection,
        lineage.source_document_id,
    )
    if prior is not None:
        if (
            prior["settlement_id"] != settlement.settlement_id
            or prior["source_canonical_digest"] != lineage.source_canonical_digest
        ):
            raise IdempotencyConflict("legacy settlement source identity has conflicting content")
        return
    await connection.execute(
        """
        INSERT INTO belllabs_control.operation_settlements (
            settlement_id, request_scope, effect_claim_id, settlement_revision,
            settlement_digest, status, usage_payload, pending_external_usage_payload,
            result_manifest_ref, result_manifest_digest, result_manifest_size_bytes,
            failure_code, settlement_payload, settled_at,
            source_system, source_collection, source_document_id,
            source_recorded_at, source_canonical_digest, target_canonical_digest
        )
        VALUES (
            $1, $2, $3, $4, $5, $6, $7::jsonb, $8::jsonb,
            $9, $10, $11, $12, $13::jsonb, $14, $15, $16, $17, $18, $19, $20
        )
        ON CONFLICT (request_scope, effect_claim_id, settlement_revision)
        DO NOTHING
        """,
        settlement.settlement_id,
        settlement.request_scope,
        settlement.effect_claim_id,
        settlement.settlement_revision,
        settlement.settlement_digest,
        settlement.status,
        json.dumps(settlement.usage, sort_keys=True, separators=(",", ":")),
        json.dumps(
            settlement.pending_external_usage,
            sort_keys=True,
            separators=(",", ":"),
        ),
        settlement.result_manifest_ref,
        settlement.result_manifest_digest,
        settlement.result_manifest_size_bytes,
        settlement.failure_code,
        settlement.model_dump_json(),
        settlement.settled_at,
        lineage.source_system,
        lineage.source_collection,
        lineage.source_document_id,
        lineage.source_recorded_at,
        lineage.source_canonical_digest,
        sha256_digest(settlement),
    )
    persisted = await connection.fetchrow(
        """
        SELECT *
        FROM belllabs_control.operation_settlements
        WHERE request_scope = $1
          AND effect_claim_id = $2
          AND settlement_revision = $3
        """,
        settlement.request_scope,
        settlement.effect_claim_id,
        settlement.settlement_revision,
    )
    if persisted is None or _settlement_from_row(persisted) != settlement:
        raise IdempotencyConflict("legacy settlement conflicts with an existing settlement")
    claim_status = (
        "cancelled"
        if settlement.status == "cancelled"
        else "reconciliation_required"
        if settlement.status == "reconciliation_required"
        else "settled"
    )
    await connection.execute(
        """
        UPDATE belllabs_control.operation_effect_claims
        SET status = $3, heartbeat_at = $4
        WHERE request_scope = $1 AND effect_claim_id = $2
        """,
        settlement.request_scope,
        settlement.effect_claim_id,
        claim_status,
        settlement.settled_at,
    )


async def _admit_quarantine(
    connection: asyncpg.Connection,
    request_scope: str,
    admission: QuarantineAdmission,
) -> None:
    prior = await connection.fetchrow(
        """
        SELECT reason_code, observed_digest, expected_digest
        FROM belllabs_control.operation_journal_backfill_quarantine
        WHERE request_scope = $1
          AND migration_stream = $2
          AND source_collection = $3
          AND source_document_id = $4
        """,
        request_scope,
        admission.migration_stream,
        admission.source_collection,
        admission.source_document_id,
    )
    if prior is not None:
        if (
            prior["reason_code"] == admission.reason_code
            and prior["observed_digest"] == admission.observed_digest
            and prior["expected_digest"] == admission.expected_digest
        ):
            return
        await connection.execute(
            """
            UPDATE belllabs_control.operation_journal_backfill_quarantine
            SET reason_code = 'source_identity_digest_conflict',
                observed_digest = $5,
                expected_digest = $6,
                observed_request_scope = $7
            WHERE request_scope = $1
              AND migration_stream = $2
              AND source_collection = $3
              AND source_document_id = $4
            """,
            request_scope,
            admission.migration_stream,
            admission.source_collection,
            admission.source_document_id,
            admission.observed_digest,
            prior["observed_digest"],
            admission.observed_request_scope,
        )
        return
    await connection.execute(
        """
        INSERT INTO belllabs_control.operation_journal_backfill_quarantine (
            quarantine_id, request_scope, migration_stream,
            source_collection, source_document_id, reason_code,
            observed_digest, expected_digest, observed_request_scope, quarantined_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
        """,
        admission.quarantine_id,
        request_scope,
        admission.migration_stream,
        admission.source_collection,
        admission.source_document_id,
        admission.reason_code,
        admission.observed_digest,
        admission.expected_digest,
        admission.observed_request_scope,
        admission.quarantined_at,
    )


async def _set_scope(connection: asyncpg.Connection, request_scope: str) -> None:
    await connection.execute(
        "SELECT set_config('belllabs.request_scope', $1, true)",
        request_scope,
    )


def _claim_from_row(row: asyncpg.Record) -> OperationEffectClaim:
    return OperationEffectClaim(
        effect_claim_id=row["effect_claim_id"],
        request_scope=row["request_scope"],
        belllabs_run_id=row["belllabs_run_id"],
        operation_contract_digest=row["operation_contract_digest"],
        idempotency_key=row["idempotency_key"],
        request_digest=row["request_digest"],
        semantic_binding_id=row["semantic_binding_id"],
        semantic_binding_digest=row["semantic_binding_digest"],
        semantic_attempt_key=row["semantic_attempt_key"],
        claim_mode=row["claim_mode"],
        status=row["status"],
        claimed_by=row["claimed_by"],
        claimed_at=row["claimed_at"],
        heartbeat_at=row["heartbeat_at"],
        lease_expires_at=row["lease_expires_at"],
    )


def _settlement_from_row(row: asyncpg.Record) -> OperationJournalSettlement:
    payload: Any = row["settlement_payload"]
    if isinstance(payload, str):
        payload = json.loads(payload)
    return OperationJournalSettlement.model_validate(payload)


def _progress_from_row(row: asyncpg.Record) -> BackfillProgress:
    source_claims = int(row["source_claim_count"])
    source_settlements = int(row["source_settlement_count"])
    quarantines = int(row["quarantine_count"])
    snapshot_payload: Any = row["source_snapshot_payload"]
    if isinstance(snapshot_payload, str):
        snapshot_payload = json.loads(snapshot_payload)
    return BackfillProgress(
        run_id=row["run_id"],
        request_scope=row["request_scope"],
        cursor=row["source_cursor"],
        source_count=source_claims + source_settlements,
        admitted_claim_count=int(row["target_claim_count"]),
        admitted_settlement_count=int(row["target_settlement_count"]),
        quarantine_count=quarantines,
        source_aggregate_digest=row["source_aggregate_digest"] or sha256_digest([]),
        target_aggregate_digest=row["target_aggregate_digest"] or sha256_digest([]),
        source_snapshot=SourceSnapshot(
            request_scope=str(snapshot_payload["request_scope"]),
            claim_high_watermark=snapshot_payload["claim_high_watermark"],
            settlement_high_watermark=snapshot_payload["settlement_high_watermark"],
            record_count=int(snapshot_payload["record_count"]),
            aggregate_digest=str(snapshot_payload["aggregate_digest"]),
            captured_at=datetime.fromisoformat(snapshot_payload["captured_at"]),
        ),
        completed=row["status"] == "completed",
        dry_run=row["status"] == "dry_run",
    )


def _aggregate_digests(digests: list[str]) -> str:
    aggregate = sha256_digest([])
    for digest in digests:
        aggregate = sha256_digest(
            {
                "previous_aggregate_digest": aggregate,
                "item_digest": digest,
            }
        )
    return aggregate
