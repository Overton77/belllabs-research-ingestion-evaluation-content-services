from __future__ import annotations

from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from app.application.runtime.runtime_events import (
    OperatorDebugAuthorization,
    RuntimeEventTranslator,
)
from app.domain.graph_runtime.identities import ExecutionEpochKey
from app.domain.run_control.contracts import (
    ActorContext,
    DomainEventEnvelope,
    OutboxCursor,
    OutboxRecord,
)

NOW = datetime(2026, 8, 6, 20, 0, tzinfo=UTC)


def record(
    position: int,
    *,
    run_id: str = "run-1",
    payload: dict[str, object] | None = None,
) -> OutboxRecord:
    envelope = DomainEventEnvelope(
        event_id=f"event-{position}",
        event_type="workflow_run.runtime_observed",
        aggregate_id=run_id,
        aggregate_version=position,
        sequence=1,
        occurred_at=NOW,
        recorded_at=NOW,
        actor=ActorContext(actor_id="runtime"),
        correlation_id="correlation-1",
        payload=payload
        or {
            "status": "running",
            "result_manifest_ref": "result:1",
            "secret_id": "must-not-stream",
            "token_digest": "must-not-stream",
            "node_name": "operator-only-node",
        },
    )
    return OutboxRecord(
        envelope=envelope,
        cursor=OutboxCursor(
            position=position,
            recorded_at=NOW,
            aggregate_id=run_id,
            aggregate_version=position,
            sequence=1,
        ),
    )


class Outbox:
    def __init__(self, records: tuple[OutboxRecord, ...]) -> None:
        self.records = records

    async def list_outbox(self, _scope, *, after=None, limit=100):  # type: ignore[no-untyped-def]
        position = after.position if after is not None else 0
        return tuple(item for item in self.records if item.cursor.position > position)[:limit]


@pytest.mark.asyncio
async def test_reconnect_uses_monotonic_outbox_cursor_and_deduplicates() -> None:
    records = (record(1), record(2), record(3), record(4, run_id="other-run"))
    translator = RuntimeEventTranslator(Outbox(records))
    epoch = ExecutionEpochKey(
        request_scope="tenant-1",
        belllabs_run_id="run-1",
        execution_epoch=1,
    )

    first = await translator.replay(epoch=epoch, binding_id="binding-1", limit=2)
    resumed = await translator.replay(
        epoch=epoch,
        binding_id="binding-1",
        after=records[1].cursor,
    )

    assert [item.outbox_position for item in first] == [1, 2]
    assert [item.outbox_position for item in resumed] == [3]
    assert set(item.event_id for item in first + resumed) == {"event-1", "event-2", "event-3"}


@pytest.mark.asyncio
async def test_stream_payload_is_reference_only_and_debug_is_operator_gated() -> None:
    translator = RuntimeEventTranslator(Outbox((record(1),)))
    epoch = ExecutionEpochKey(
        request_scope="tenant-1",
        belllabs_run_id="run-1",
        execution_epoch=1,
    )

    public = (await translator.replay(epoch=epoch, binding_id="binding-1"))[0]
    operator = (
        await translator.replay(
            epoch=epoch,
            binding_id="binding-1",
            operator_debug=OperatorDebugAuthorization(
                request_scope="tenant-1",
                actor_id="operator-1",
                role="operator",
                approved=True,
            ),
        )
    )[0]

    assert public.redacted_payload == {
        "status": "running",
        "result_manifest_ref": "result:1",
    }
    assert public.payload_ref == "result:1"
    assert operator.redacted_payload["node_name"] == "operator-only-node"
    assert "secret" not in operator.redacted_payload
    assert "secret_id" not in operator.redacted_payload
    assert "patient_id" not in operator.redacted_payload
    assert "token_digest" not in operator.redacted_payload
    assert "raw_output" not in operator.redacted_payload
    with pytest.raises(PermissionError, match="scoped operator authorization"):
        await translator.replay(
            epoch=epoch,
            binding_id="binding-1",
            operator_debug=OperatorDebugAuthorization(
                request_scope="tenant-2",
                actor_id="operator-1",
                role="operator",
                approved=True,
            ),
        )


def test_outbox_contract_rejects_raw_secret_phi_and_content_payloads() -> None:
    with pytest.raises(ValidationError, match="raw secrets, PHI, or content"):
        record(1, payload={"raw_output": "must-not-enter-the-outbox"})
