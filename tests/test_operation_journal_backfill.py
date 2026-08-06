from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

from app.application.operation_journal_backfill import (
    CLAIMS_COLLECTION,
    SETTLEMENTS_COLLECTION,
    BackfillBatch,
    BackfillProgress,
    LegacyMongoRecord,
    OperationJournalBackfillService,
    QuarantineAdmission,
    SourceSnapshot,
    transform_legacy_claim,
    transform_legacy_settlement,
)
from app.domain.control_plane.canonical import sha256_digest
from app.domain.operation_execution.contracts import (
    OperationExecutionBinding,
    OperationSettlement,
    RuntimeUsage,
)

DIGEST = "sha256:" + "b" * 64
NOW = datetime(2026, 8, 5, tzinfo=UTC)
PROJECT_ROOT = Path(__file__).parents[1]


def _binding() -> OperationExecutionBinding:
    return OperationExecutionBinding.model_construct(
        binding_id="binding-1",
        semantic_attempt_key="run-1:operation:op-1:attempt:1",
        request_fingerprint=DIGEST,
        request_scope="tenant-a",
        run_id="run-1",
        operation_id="op-1",
        operation_attempt=1,
        prior_binding_id=None,
        effective_configuration_digest=DIGEST,
        run_control_revision=1,
        operation_contract_ref="operation-contract:v1",
        prompt_sources=(),
        model_policy={"provider": "test", "model": "test-model"},
        tools=(),
        mcp_servers=(),
        skills=(),
        plugins=(),
        output_schema=None,
        guardrails=(),
        delegations=(),
        delegation_ceiling={},
        session_id=None,
        agent_profile_ref={"definition_id": "agent", "revision": 1, "digest": DIGEST},
        capability_grant={
            "capabilities": frozenset(),
            "tool_ids": frozenset(),
            "mcp_server_ids": frozenset(),
            "data_scope_refs": frozenset(),
            "network_hosts": frozenset(),
        },
        workspace={
            "namespace_id": "namespace-1",
            "workspace_id": "workspace-1",
            "provider": "test",
        },
        secret_refs=(),
        budget_reservation_id="reservation-1",
        budget_limits={},
        tracing_policy_ref="trace:test",
        sensitive_data_policy_ref="sensitive:test",
        snapshot_policy_ref="snapshot:test",
        applied_degradations=(),
        side_effect_key="effect-1",
        bound_at=NOW,
    )


def _claim_record(*, request_scope: str | None = None) -> LegacyMongoRecord:
    return LegacyMongoRecord(
        collection=CLAIMS_COLLECTION,
        document_id="claim-doc-1",
        payload={
            "request_scope": request_scope,
            "side_effect_key": "effect-1",
            "binding_id": "binding-1",
        },
        recorded_at=NOW,
    )


def _settlement_record() -> LegacyMongoRecord:
    settlement = OperationSettlement(
        settlement_id="settlement-1",
        binding_id="binding-1",
        status="completed",
        output_text="raw text must not be copied",
        structured_output={"private": "payload"},
        output_refs=("artifact:result:1",),
        usage=RuntimeUsage(
            amounts={"tokens": 12},
            pending_external_amounts={"external_tokens": 2},
        ),
        provider_run_id="provider-run-1",
        event_payloads=({"raw": "event"},),
        settled_at=NOW,
    )
    return LegacyMongoRecord(
        collection=SETTLEMENTS_COLLECTION,
        document_id="settlement-doc-1",
        payload={
            "request_scope": "tenant-a",
            "settlement_id": settlement.settlement_id,
            "binding_id": settlement.binding_id,
            "payload": settlement.model_dump(mode="json"),
        },
        recorded_at=NOW,
    )


def test_claim_and_settlement_transform_are_deterministic_and_policy_safe() -> None:
    binding = _binding()
    first = transform_legacy_claim(_claim_record(), binding)
    second = transform_legacy_claim(_claim_record(), binding)
    settlement = transform_legacy_settlement(
        _settlement_record(),
        binding,
        first.claim,
    )

    assert first == second
    assert first.claim.claimed_at == NOW
    assert first.claim.request_scope == "tenant-a"
    assert settlement.settlement.usage == {"tokens": 12}
    assert settlement.settlement.pending_external_usage == {"external_tokens": 2}
    serialized = settlement.settlement.model_dump_json()
    assert "raw text must not be copied" not in serialized
    assert '"private"' not in serialized
    assert '"raw"' not in serialized


def test_source_digest_includes_transformation_timestamp() -> None:
    first = _claim_record()
    second = LegacyMongoRecord(
        collection=first.collection,
        document_id=first.document_id,
        payload=first.payload,
        recorded_at=datetime(2026, 8, 6, tzinfo=UTC),
    )

    assert first.canonical_digest != second.canonical_digest


def test_backfill_migration_has_replay_ledger_and_dedicated_role() -> None:
    migration = (
        PROJECT_ROOT
        / "app"
        / "migrations"
        / "0013_legacy_operation_journal_backfill.sql"
    ).read_text(encoding="utf-8")

    assert "operation_journal_backfill_applied_batches" in migration
    assert "source_snapshot_digest" in migration
    assert "belllabs_operation_backfill" in migration
    assert (
        "operation_journal_backfill_batches,\n"
        "       belllabs_control.operation_journal_backfill_quarantine\n"
        "    TO belllabs_control_runtime"
    ) not in migration


def test_quarantine_timestamp_does_not_change_batch_replay_digest() -> None:
    snapshot = SourceSnapshot(
        request_scope="tenant-a",
        claim_high_watermark="claim-doc-1",
        settlement_high_watermark=None,
        record_count=1,
        aggregate_digest=DIGEST,
        captured_at=NOW,
    )
    values = {
        "quarantine_id": "quarantine-1",
        "migration_stream": "legacy-mongo-operation-journal-v1",
        "source_collection": CLAIMS_COLLECTION,
        "source_document_id": "claim-doc-1",
        "reason_code": "missing_or_invalid_binding",
        "observed_digest": DIGEST,
        "expected_digest": None,
        "observed_request_scope": "tenant-a",
    }
    first = BackfillBatch(
        run_id="run-1",
        request_scope="tenant-a",
        previous_cursor=None,
        cursor=f"{CLAIMS_COLLECTION}:claim-doc-1",
        source_snapshot=snapshot,
        quarantines=(
            QuarantineAdmission(**values, quarantined_at=NOW),  # type: ignore[arg-type]
        ),
    )
    second = BackfillBatch(
        run_id="run-1",
        request_scope="tenant-a",
        previous_cursor=None,
        cursor=f"{CLAIMS_COLLECTION}:claim-doc-1",
        source_snapshot=snapshot,
        quarantines=(
            QuarantineAdmission(
                **values,
                quarantined_at=datetime(2026, 8, 6, tzinfo=UTC),
            ),  # type: ignore[arg-type]
        ),
    )

    assert first.batch_digest == second.batch_digest


class FakeSource:
    def __init__(self, records: tuple[LegacyMongoRecord, ...]) -> None:
        self.records = records

    async def capture_snapshot(self, *, request_scope: str) -> SourceSnapshot:
        records = tuple(
            sorted(
                self.records,
                key=lambda item: item.cursor,
            )
        )
        aggregate = sha256_digest([])
        for record in records:
            aggregate = sha256_digest(
                {
                    "previous_aggregate_digest": aggregate,
                    "item_digest": record.canonical_digest,
                }
            )
        claims = [item for item in records if item.collection == CLAIMS_COLLECTION]
        settlements = [
            item for item in records if item.collection == SETTLEMENTS_COLLECTION
        ]
        return SourceSnapshot(
            request_scope=request_scope,
            claim_high_watermark=claims[-1].document_id if claims else None,
            settlement_high_watermark=(
                settlements[-1].document_id if settlements else None
            ),
            record_count=len(records),
            aggregate_digest=aggregate,
            captured_at=NOW,
        )

    async def read_batch(
        self,
        *,
        request_scope: str,
        after_cursor: str | None,
        limit: int,
        snapshot: SourceSnapshot,
    ) -> tuple[LegacyMongoRecord, ...]:
        assert snapshot.request_scope == request_scope
        return tuple(
            record
            for record in self.records
            if after_cursor is None or record.cursor > after_cursor
        )[:limit]

    async def get_binding(
        self,
        *,
        request_scope: str,
        binding_id: str,
    ) -> OperationExecutionBinding | None:
        return (
            _binding()
            if binding_id == "binding-1" and request_scope == "tenant-a"
            else None
        )

    async def get_claim_for_binding(
        self,
        *,
        request_scope: str,
        binding_id: str,
    ) -> LegacyMongoRecord | None:
        return (
            _claim_record()
            if binding_id == "binding-1" and request_scope == "tenant-a"
            else None
        )

    async def get_settlement_for_binding(
        self,
        *,
        request_scope: str,
        binding_id: str,
    ) -> LegacyMongoRecord | None:
        return (
            _settlement_record()
            if binding_id == "binding-1" and request_scope == "tenant-a"
            else None
        )


class FakeTarget:
    def __init__(self) -> None:
        self.batches: list[BackfillBatch] = []

    async def load_progress(
        self,
        *,
        request_scope: str,
        run_id: str,
    ) -> BackfillProgress | None:
        del request_scope, run_id
        return None

    async def apply_batch(self, batch: BackfillBatch) -> None:
        self.batches.append(batch)

    async def verify(
        self,
        *,
        request_scope: str,
        run_id: str,
    ) -> BackfillProgress:
        raise AssertionError("dry run must not verify the target")


class MutatingSource(FakeSource):
    def __init__(self, records: tuple[LegacyMongoRecord, ...]) -> None:
        super().__init__(records)
        self.capture_count = 0

    async def capture_snapshot(self, *, request_scope: str) -> SourceSnapshot:
        self.capture_count += 1
        if self.capture_count == 2:
            self.records += (
                LegacyMongoRecord(
                    collection=CLAIMS_COLLECTION,
                    document_id="claim-doc-2",
                    payload={
                        "request_scope": request_scope,
                        "side_effect_key": "effect-2",
                        "binding_id": "binding-2",
                    },
                    recorded_at=NOW,
                ),
            )
        return await super().capture_snapshot(request_scope=request_scope)


@pytest.mark.asyncio
async def test_dry_run_validates_full_set_without_target_writes() -> None:
    target = FakeTarget()
    service = OperationJournalBackfillService(
        source=FakeSource((_claim_record(), _settlement_record())),
        target=target,
        batch_size=1,
    )

    result = await service.run(
        request_scope="tenant-a",
        run_id="dry-run-1",
        dry_run=True,
    )

    assert result.completed is True
    assert result.source_count == 2
    assert result.admitted_claim_count == 1
    assert result.admitted_settlement_count == 1
    assert target.batches == []


@pytest.mark.asyncio
async def test_cross_scope_source_is_quarantined() -> None:
    target = FakeTarget()
    service = OperationJournalBackfillService(
        source=FakeSource((_claim_record(request_scope="tenant-b"),)),
        target=target,
    )

    result = await service.run(
        request_scope="tenant-a",
        run_id="dry-run-2",
        dry_run=True,
    )

    assert result.quarantine_count == 1
    assert result.admitted_claim_count == 0


@pytest.mark.asyncio
async def test_source_change_before_completion_fails_closed() -> None:
    target = FakeTarget()
    service = OperationJournalBackfillService(
        source=MutatingSource((_claim_record(),)),
        target=target,
    )

    with pytest.raises(RuntimeError, match="changed during backfill"):
        await service.run(
            request_scope="tenant-a",
            run_id="mutable-source",
            dry_run=True,
        )
