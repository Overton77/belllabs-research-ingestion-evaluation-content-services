from __future__ import annotations

from datetime import UTC, datetime

import asyncpg
import pytest

from app.application.operation_journal import (
    InMemoryAtomicOperationJournalRepository,
    OperationJournalMutation,
    OperationJournalService,
)
from app.application.postgres_operation_journal import (
    PostgresAtomicOperationJournalRepository,
)
from app.application.postgres_run_control_repository import PostgresRunControlRepository
from app.domain.operation_execution.journal import (
    OperationEffectClaim,
    OperationJournalSettlement,
    OperationTechnicalAttempt,
)
from app.domain.run_control.errors import IdempotencyConflict
from app.integrations.postgres import apply_application_migrations
from tests.test_run_control import request, service

NOW = datetime(2026, 8, 5, 20, 0, tzinfo=UTC)
DIGEST = "sha256:" + "a" * 64


def claim(*, run_id: str = "run-1", request_digest: str = DIGEST) -> OperationEffectClaim:
    return OperationEffectClaim(
        effect_claim_id="claim-1",
        request_scope="tenant-1",
        belllabs_run_id=run_id,
        operation_contract_digest=DIGEST,
        idempotency_key="effect-key-1",
        request_digest=request_digest,
        semantic_binding_id="mongo-binding-1",
        semantic_binding_digest=DIGEST,
        semantic_attempt_key=f"{run_id}:operation:search:semantic-attempt:1",
        claimed_by="operation-worker",
        claimed_at=NOW,
    )


def attempt() -> OperationTechnicalAttempt:
    return OperationTechnicalAttempt(
        operation_attempt_id="technical-attempt-1",
        request_scope="tenant-1",
        effect_claim_id="claim-1",
        technical_attempt=1,
        provider="fixture-provider",
        disposition="failed",
        idempotency_supported=True,
        retry_class="safe",
        usage={"tokens.total": 7},
        started_at=NOW,
        finished_at=NOW,
        failure_code="fixture_failure",
    )


def settlement() -> OperationJournalSettlement:
    values = {
        "settlement_id": "settlement-1",
        "request_scope": "tenant-1",
        "effect_claim_id": "claim-1",
        "settlement_revision": 1,
        "status": "failed",
        "usage": {"tokens.total": 7},
        "failure_code": "fixture_failure",
        "detail": {"schema_version": "1"},
        "settled_at": NOW,
    }
    return OperationJournalSettlement.create(**values)


@pytest.mark.asyncio
async def test_claim_attempt_usage_and_settlement_are_idempotent_but_conflicts_durable() -> None:
    repository = InMemoryAtomicOperationJournalRepository()
    journal = OperationJournalService(repository)
    mutation = OperationJournalMutation(
        request_scope="tenant-1",
        belllabs_run_id="run-1",
        expected_run_version=1,
        claim=claim(),
        attempt=attempt(),
        settlement=settlement(),
    )

    first = await journal.commit(mutation)
    replay = await journal.commit(mutation)

    assert first.status == "acquired"
    assert replay.status == "existing"
    assert (await repository.get_settlement("tenant-1", "claim-1")).usage == {
        "tokens.total": 7
    }
    with pytest.raises(IdempotencyConflict, match="conflicting request"):
        await journal.commit(
            OperationJournalMutation(
                request_scope="tenant-1",
                belllabs_run_id="run-1",
                expected_run_version=1,
                claim=claim(request_digest="sha256:" + "b" * 64),
            )
        )
    revised_values = settlement().model_dump(exclude={"settlement_digest"})
    revised_values.update(
        {
            "settlement_id": "settlement-2",
            "settlement_revision": 2,
            "status": "completed",
            "failure_code": None,
        }
    )
    with pytest.raises(IdempotencyConflict, match="terminal"):
        await journal.commit(
            OperationJournalMutation(
                request_scope="tenant-1",
                belllabs_run_id="run-1",
                expected_run_version=1,
                claim=claim(),
                settlement=OperationJournalSettlement.create(**revised_values),
            )
        )


@pytest.mark.asyncio
async def test_claim_retry_reuses_persisted_generated_identity() -> None:
    repository = InMemoryAtomicOperationJournalRepository()
    journal = OperationJournalService(repository)
    first = await journal.commit(
        OperationJournalMutation(
            request_scope="tenant-1",
            belllabs_run_id="run-1",
            expected_run_version=1,
            claim=claim(),
        )
    )
    replayed_claim = claim().model_copy(
        update={
            "effect_claim_id": "regenerated-claim-id",
            "claimed_at": NOW.replace(second=1),
        }
    )
    replay = await journal.commit(
        OperationJournalMutation(
            request_scope="tenant-1",
            belllabs_run_id="run-1",
            expected_run_version=1,
            claim=replayed_claim,
        )
    )

    assert first.status == "acquired"
    assert replay.status == "existing"
    assert replay.claim is not None
    assert replay.claim.effect_claim_id == "claim-1"


@pytest.mark.asyncio
async def test_shadow_execution_cannot_acquire_consequential_claim() -> None:
    repository = InMemoryAtomicOperationJournalRepository()
    result = await OperationJournalService(repository).commit(
        OperationJournalMutation(
            request_scope="tenant-1",
            belllabs_run_id="run-1",
            expected_run_version=1,
            claim=claim().model_copy(update={"claim_mode": "shadow"}),
        )
    )
    assert result.status == "shadow_denied"
    assert await repository.get_claim("tenant-1", "claim-1") is None


@pytest.mark.asyncio
async def test_postgres_journal_crash_rolls_back_claim_attempt_and_settlement(
    test_application_postgres_dsn: str,
) -> None:
    pool = await asyncpg.create_pool(
        dsn=test_application_postgres_dsn,
        min_size=1,
        max_size=4,
    )
    try:
        async with pool.acquire() as connection:
            await connection.execute("DROP SCHEMA IF EXISTS belllabs_control CASCADE")
        await apply_application_migrations(pool)
        run_service, _ = service(PostgresRunControlRepository(pool))  # type: ignore[arg-type]
        admitted = await run_service.admit(request())
        assert admitted.run_id is not None
        operation_claim = claim(run_id=admitted.run_id)
        mutation = OperationJournalMutation(
            request_scope="tenant-1",
            belllabs_run_id=admitted.run_id,
            expected_run_version=1,
            claim=operation_claim,
            attempt=attempt(),
            settlement=settlement(),
        )

        async def fail_before_commit(boundary: str) -> None:
            if boundary == "operation_journal":
                raise RuntimeError("injected operation journal crash")

        failing = PostgresAtomicOperationJournalRepository(
            pool,
            before_commit=fail_before_commit,
        )
        with pytest.raises(RuntimeError, match="injected"):
            await failing.commit(mutation)
        assert await failing.get_claim("tenant-1", "claim-1") is None

        repository = PostgresAtomicOperationJournalRepository(pool)
        assert (await repository.commit(mutation)).status == "acquired"
        assert (await repository.commit(mutation)).status == "existing"
        persisted = await repository.get_settlement("tenant-1", "claim-1")
        assert persisted == settlement()
        with pytest.raises(IdempotencyConflict, match="conflicting immutable intent"):
            await repository.commit(
                OperationJournalMutation(
                    request_scope="tenant-1",
                    belllabs_run_id=admitted.run_id,
                    expected_run_version=1,
                    claim=claim(
                        run_id=admitted.run_id,
                        request_digest="sha256:" + "b" * 64,
                    ),
                )
            )
        revised_values = settlement().model_dump(exclude={"settlement_digest"})
        revised_values.update(
            {
                "settlement_id": "settlement-2",
                "settlement_revision": 2,
                "status": "completed",
                "failure_code": None,
            }
        )
        with pytest.raises(IdempotencyConflict, match="terminal"):
            await repository.commit(
                OperationJournalMutation(
                    request_scope="tenant-1",
                    belllabs_run_id=admitted.run_id,
                    expected_run_version=1,
                    claim=operation_claim,
                    settlement=OperationJournalSettlement.create(**revised_values),
                )
            )

        async with pool.acquire() as connection, connection.transaction():
            await connection.execute("SET LOCAL ROLE belllabs_control_runtime")
            await connection.execute(
                "SELECT set_config('belllabs.request_scope', 'tenant-2', true)"
            )
            assert (
                await connection.fetchval(
                    "SELECT count(*) FROM belllabs_control.operation_effect_claims"
                )
                == 0
            )
    finally:
        async with pool.acquire() as connection:
            await connection.execute("DROP SCHEMA IF EXISTS belllabs_control CASCADE")
        await pool.close()
