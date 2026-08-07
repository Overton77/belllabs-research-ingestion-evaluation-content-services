from __future__ import annotations

import asyncio
import json
import os

import asyncpg
from beanie import init_beanie
from pymongo import AsyncMongoClient

from app.application.mongo_operation_journal_backfill import (
    MongoLegacyOperationJournalSource,
)
from app.application.operation_journal_backfill import (
    BackfillBatch,
    OperationJournalBackfillService,
)
from app.application.postgres_operation_journal_backfill import (
    PostgresOperationJournalBackfillRepository,
)
from app.integrations.mongodb import BEANIE_MODELS


class CrashAfterCommittedBatch:
    """Terminate only after PostgreSQL has durably committed the requested batch."""

    def __init__(
        self,
        target: PostgresOperationJournalBackfillRepository,
        *,
        crash_after: int,
    ) -> None:
        self._target = target
        self._crash_after = crash_after
        self._committed = 0

    async def load_progress(self, **kwargs):  # type: ignore[no-untyped-def]
        return await self._target.load_progress(**kwargs)

    async def apply_batch(self, batch: BackfillBatch) -> None:
        await self._target.apply_batch(batch)
        if not batch.completed:
            self._committed += 1
            if self._committed >= self._crash_after:
                os._exit(73)

    async def verify(self, **kwargs):  # type: ignore[no-untyped-def]
        return await self._target.verify(**kwargs)


async def main() -> None:
    mongo_uri = os.environ["TEST_MONGODB_URI"]
    database_name = os.environ["BACKFILL_TEST_DATABASE"]
    postgres_dsn = os.environ["TEST_APPLICATION_POSTGRES_DSN"]
    request_scope = os.environ["BACKFILL_REQUEST_SCOPE"]
    run_id = os.environ["BACKFILL_RUN_ID"]
    crash_after = int(os.getenv("BACKFILL_CRASH_AFTER", "0"))

    mongo_client = AsyncMongoClient(
        mongo_uri,
        serverSelectionTimeoutMS=5_000,
        tz_aware=True,
    )
    pool = await asyncpg.create_pool(dsn=postgres_dsn, min_size=1, max_size=2)
    try:
        await init_beanie(
            database=mongo_client[database_name],
            document_models=BEANIE_MODELS,
        )
        target = PostgresOperationJournalBackfillRepository(pool)
        effective_target = (
            CrashAfterCommittedBatch(target, crash_after=crash_after)
            if crash_after
            else target
        )
        progress = await OperationJournalBackfillService(
            source=MongoLegacyOperationJournalSource(),
            target=effective_target,
            batch_size=1,
        ).run(request_scope=request_scope, run_id=run_id)
        print(
            json.dumps(
                {
                    "completed": progress.completed,
                    "source_count": progress.source_count,
                    "admitted_claim_count": progress.admitted_claim_count,
                    "admitted_settlement_count": progress.admitted_settlement_count,
                    "quarantine_count": progress.quarantine_count,
                    "source_aggregate_digest": progress.source_aggregate_digest,
                    "target_aggregate_digest": progress.target_aggregate_digest,
                },
                sort_keys=True,
            )
        )
    finally:
        await pool.close()
        await mongo_client.close()


if __name__ == "__main__":
    asyncio.run(main())
