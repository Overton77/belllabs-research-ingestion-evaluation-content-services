from __future__ import annotations

import asyncio
import json
import os
import sys
from datetime import UTC
from pathlib import Path
from urllib.parse import urlparse
from uuid import uuid4

import asyncpg
from beanie import init_beanie
from pymongo import AsyncMongoClient

from app.application.mongo_operation_journal_backfill import (
    MongoLegacyOperationJournalSource,
)
from app.application.operation_execution import bind_operation_execution_request
from app.application.operation_journal_backfill import transform_legacy_claim
from app.application.operation_journal_read_routing import OperationJournalReadRouter
from app.application.postgres_operation_journal import (
    PostgresAtomicOperationJournalRepository,
)
from app.domain.operation_execution.contracts import OperationSettlement, RuntimeUsage
from app.integrations.mongodb import BEANIE_MODELS
from app.integrations.postgres import apply_application_migrations
from app.models.operation_execution import (
    OperationExecutionBindingDocument,
    OperationExecutionClaimDocument,
    OperationSettlementDocument,
)
from tests.test_operation_execution import operation_request

WORKER = Path(__file__).parent / "fixtures" / "operation_backfill_worker.py"


def _require_disposable_targets(postgres_dsn: str, mongodb_uri: str) -> None:
    postgres = urlparse(postgres_dsn)
    mongo = urlparse(mongodb_uri)
    if (
        postgres.hostname not in {"127.0.0.1", "localhost"}
        or postgres.port != 55432
        or postgres.path != "/belllabs"
        or postgres.username != "belllabs"
    ):
        raise RuntimeError(
            "destructive backfill proof requires the disposable local PostgreSQL target"
        )
    if mongo.hostname not in {"127.0.0.1", "localhost"} or mongo.port != 27017:
        raise RuntimeError("backfill proof requires the disposable local MongoDB target")


async def _run_worker(
    *,
    database_name: str,
    request_scope: str,
    run_id: str,
    crash_after: int = 0,
) -> tuple[int, str, str]:
    environment = {
        **os.environ,
        "BACKFILL_TEST_DATABASE": database_name,
        "BACKFILL_REQUEST_SCOPE": request_scope,
        "BACKFILL_RUN_ID": run_id,
    }
    if crash_after:
        environment["BACKFILL_CRASH_AFTER"] = str(crash_after)
    else:
        environment.pop("BACKFILL_CRASH_AFTER", None)
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        str(WORKER),
        cwd=Path(__file__).parents[1],
        env=environment,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await process.communicate()
    return process.returncode, stdout.decode(), stderr.decode()


async def _source_documents(database) -> tuple[tuple[str, tuple[dict, ...]], ...]:  # type: ignore[no-untyped-def]
    snapshots: list[tuple[str, tuple[dict, ...]]] = []
    for collection_name in (
        "operation_execution_bindings",
        "operation_execution_claims",
        "operation_execution_settlements",
    ):
        documents = await database[collection_name].find().sort("_id", 1).to_list(None)
        snapshots.append(
            (
                collection_name,
                tuple(
                    json.loads(json.dumps(item, default=str, sort_keys=True))
                    for item in documents
                ),
            )
        )
    return tuple(snapshots)


async def test_real_two_store_backfill_survives_process_loss_and_preserves_rollback_reads(
    test_application_postgres_dsn: str,
    test_mongodb_uri: str,
) -> None:
    _require_disposable_targets(test_application_postgres_dsn, test_mongodb_uri)
    database_name = f"backfill_test_{uuid4().hex[:20]}"
    request_scope = "tenant-backfill"
    migration_run_id = f"backfill-{uuid4().hex}"
    mongo_client = AsyncMongoClient(
        test_mongodb_uri,
        serverSelectionTimeoutMS=5_000,
        tz_aware=True,
        tzinfo=UTC,
    )
    database = mongo_client[database_name]
    pool = await asyncpg.create_pool(
        dsn=test_application_postgres_dsn,
        min_size=1,
        max_size=4,
    )
    try:
        await database.command("ping")
        await init_beanie(database=database, document_models=BEANIE_MODELS)
        async with pool.acquire() as connection:
            await connection.execute("DROP SCHEMA IF EXISTS belllabs_control CASCADE")
        await apply_application_migrations(pool)

        valid_request = operation_request()
        valid_request = valid_request.model_copy(
            update={"request_scope": request_scope},
        )
        valid_binding = bind_operation_execution_request(valid_request)
        malformed_request = operation_request(attempt=2).model_copy(
            update={"request_scope": request_scope},
        )
        malformed_binding = bind_operation_execution_request(malformed_request)
        fallback_request = operation_request(attempt=3).model_copy(
            update={"request_scope": request_scope},
        )
        fallback_binding = bind_operation_execution_request(fallback_request)

        async with pool.acquire() as connection:
            await connection.execute(
                """
                INSERT INTO belllabs_control.workflow_runs (
                    run_id, request_scope, idempotency_issuer, request_id,
                    version, phase, projection, updated_at
                )
                VALUES ($1, $2, 'integration-test', $1, 1, 'active', '{}'::jsonb, $3)
                """,
                valid_binding.run_id,
                request_scope,
                valid_binding.bound_at,
            )

        for binding in (valid_binding, malformed_binding):
            await OperationExecutionBindingDocument(
                request_scope=request_scope,
                binding_id=binding.binding_id,
                semantic_attempt_key=binding.semantic_attempt_key,
                request_fingerprint=binding.request_fingerprint,
                run_id=binding.run_id,
                operation_id=binding.operation_id,
                operation_attempt=binding.operation_attempt,
                payload=binding.model_dump(mode="json"),
                bound_at=binding.bound_at,
            ).insert()
        await OperationExecutionClaimDocument(
            request_scope=request_scope,
            side_effect_key=valid_binding.side_effect_key,
            binding_id=valid_binding.binding_id,
            claimed_at=valid_binding.bound_at,
        ).insert()
        await OperationExecutionClaimDocument(
            request_scope=request_scope,
            side_effect_key="conflicting-side-effect",
            binding_id=malformed_binding.binding_id,
            claimed_at=malformed_binding.bound_at,
        ).insert()
        settlement = OperationSettlement(
            settlement_id="legacy-settlement-valid",
            binding_id=valid_binding.binding_id,
            status="completed",
            output_text="private text remains in Mongo",
            structured_output={"private": True},
            output_refs=("artifact:legacy-result",),
            usage=RuntimeUsage(amounts={"tokens": 7}),
            provider_run_id="legacy-provider-run",
            event_payloads=({"private": "event"},),
            settled_at=valid_binding.bound_at,
        )
        await OperationSettlementDocument(
            request_scope=request_scope,
            settlement_id=settlement.settlement_id,
            binding_id=valid_binding.binding_id,
            payload=settlement.model_dump(mode="json"),
            settled_at=settlement.settled_at,
        ).insert()
        source_before = await _source_documents(database)

        crashed_code, _, crashed_stderr = await _run_worker(
            database_name=database_name,
            request_scope=request_scope,
            run_id=migration_run_id,
            crash_after=1,
        )
        assert crashed_code == 73, crashed_stderr

        resumed_code, stdout, stderr = await _run_worker(
            database_name=database_name,
            request_scope=request_scope,
            run_id=migration_run_id,
        )
        assert resumed_code == 0, stderr
        result = json.loads(stdout.strip().splitlines()[-1])
        assert result["completed"] is True
        assert result["source_count"] == 3
        assert result["admitted_claim_count"] == 1
        assert result["admitted_settlement_count"] == 1
        assert result["quarantine_count"] == 1
        assert result["source_aggregate_digest"].startswith("sha256:")
        assert result["target_aggregate_digest"].startswith("sha256:")
        assert await _source_documents(database) == source_before

        postgres = PostgresAtomicOperationJournalRepository(pool)
        source = MongoLegacyOperationJournalSource()
        valid_claim_source = await source.get_claim_for_binding(
            request_scope=request_scope,
            binding_id=valid_binding.binding_id,
        )
        assert valid_claim_source is not None
        valid_claim = transform_legacy_claim(valid_claim_source, valid_binding).claim
        cutover_router = OperationJournalReadRouter(
            postgres=postgres,
            legacy=source,
            legacy_fallback_enabled=False,
        )
        migrated_claim = await cutover_router.get_claim(
            valid_binding,
            effect_claim_id=valid_claim.effect_claim_id,
        )
        assert migrated_claim is not None
        assert migrated_claim.effect_claim_id == valid_claim.effect_claim_id
        assert migrated_claim.semantic_binding_digest == valid_claim.semantic_binding_digest
        assert migrated_claim.status.value == "settled"
        assert (
            await cutover_router.get_settlement(
                valid_binding,
                effect_claim_id=valid_claim.effect_claim_id,
            )
            is not None
        )

        await OperationExecutionBindingDocument(
            request_scope=request_scope,
            binding_id=fallback_binding.binding_id,
            semantic_attempt_key=fallback_binding.semantic_attempt_key,
            request_fingerprint=fallback_binding.request_fingerprint,
            run_id=fallback_binding.run_id,
            operation_id=fallback_binding.operation_id,
            operation_attempt=fallback_binding.operation_attempt,
            payload=fallback_binding.model_dump(mode="json"),
            bound_at=fallback_binding.bound_at,
        ).insert()
        await OperationExecutionClaimDocument(
            request_scope=request_scope,
            side_effect_key=fallback_binding.side_effect_key,
            binding_id=fallback_binding.binding_id,
            claimed_at=fallback_binding.bound_at,
        ).insert()
        fallback_settlement = OperationSettlement(
            settlement_id="legacy-settlement-fallback",
            binding_id=fallback_binding.binding_id,
            status="completed",
            output_refs=("artifact:legacy-fallback",),
            usage=RuntimeUsage(amounts={"tokens": 3}),
            settled_at=fallback_binding.bound_at,
        )
        await OperationSettlementDocument(
            request_scope=request_scope,
            settlement_id=fallback_settlement.settlement_id,
            binding_id=fallback_binding.binding_id,
            payload=fallback_settlement.model_dump(mode="json"),
            settled_at=fallback_settlement.settled_at,
        ).insert()
        fallback_source = await source.get_claim_for_binding(
            request_scope=request_scope,
            binding_id=fallback_binding.binding_id,
        )
        assert fallback_source is not None
        fallback_claim = transform_legacy_claim(fallback_source, fallback_binding).claim
        assert (
            await cutover_router.get_claim(
                fallback_binding,
                effect_claim_id=fallback_claim.effect_claim_id,
            )
            is None
        )
        assert (
            await cutover_router.get_settlement(
                fallback_binding,
                effect_claim_id=fallback_claim.effect_claim_id,
            )
            is None
        )
        rollback_router = OperationJournalReadRouter(
            postgres=postgres,
            legacy=source,
            legacy_fallback_enabled=True,
        )
        assert (
            await rollback_router.get_claim(
                fallback_binding,
                effect_claim_id=fallback_claim.effect_claim_id,
            )
            == fallback_claim
        )
        fallback_read = await rollback_router.get_settlement(
            fallback_binding,
            effect_claim_id=fallback_claim.effect_claim_id,
        )
        assert fallback_read is not None
        assert fallback_read.settlement_id == fallback_settlement.settlement_id
        assert fallback_read.effect_claim_id == fallback_claim.effect_claim_id

        async with pool.acquire() as connection:
            quarantines = await connection.fetch(
                """
                SELECT reason_code
                FROM belllabs_control.operation_journal_backfill_quarantine
                WHERE request_scope = $1
                """,
                request_scope,
            )
        assert [row["reason_code"] for row in quarantines] == [
            "malformed_or_conflicting_source"
        ]
    finally:
        async with pool.acquire() as connection:
            await connection.execute("DROP SCHEMA IF EXISTS belllabs_control CASCADE")
        await pool.close()
        await mongo_client.drop_database(database_name)
        await mongo_client.close()
