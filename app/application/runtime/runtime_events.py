from __future__ import annotations

from collections.abc import Mapping
from typing import Literal, Protocol

from pydantic import BaseModel, ConfigDict, Field

from app.domain.graph_runtime.contracts import BellLabsStreamEvent
from app.domain.graph_runtime.identities import ExecutionEpochKey
from app.domain.run_control.contracts import OutboxCursor, OutboxRecord


class DurableOutboxReader(Protocol):
    async def list_outbox(
        self,
        request_scope: str,
        *,
        after: OutboxCursor | None = None,
        limit: int = 100,
    ) -> tuple[OutboxRecord, ...]: ...


class OperatorDebugAuthorization(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    request_scope: str = Field(min_length=1)
    actor_id: str = Field(min_length=1)
    role: Literal["operator"]
    approved: bool


class RuntimeEventTranslator:
    """Projects durable BellLabs outbox records into compact resumable UI events."""

    def __init__(self, outbox: DurableOutboxReader) -> None:
        self._outbox = outbox

    async def replay(
        self,
        *,
        epoch: ExecutionEpochKey,
        binding_id: str,
        after: OutboxCursor | None = None,
        limit: int = 100,
        operator_debug: OperatorDebugAuthorization | None = None,
    ) -> tuple[BellLabsStreamEvent, ...]:
        debug_allowed = False
        if operator_debug is not None:
            if (
                not isinstance(operator_debug, OperatorDebugAuthorization)
                or not operator_debug.approved
                or operator_debug.request_scope != epoch.request_scope
            ):
                raise PermissionError(
                    "operator debug requires matching scoped operator authorization"
                )
            debug_allowed = True
        records = await self._outbox.list_outbox(
            epoch.request_scope,
            after=after,
            limit=limit,
        )
        result: list[BellLabsStreamEvent] = []
        prior_position = after.position if after is not None else 0
        for record in records:
            if record.cursor.position <= prior_position:
                continue
            if record.envelope.aggregate_id != epoch.belllabs_run_id:
                continue
            payload = _redact_payload(
                record.envelope.payload,
                operator_debug=debug_allowed,
            )
            result.append(
                BellLabsStreamEvent(
                    event_id=record.envelope.event_id,
                    outbox_position=record.cursor.position,
                    event_type=record.envelope.event_type,
                    epoch=epoch,
                    binding_id=binding_id,
                    aggregate_version=record.envelope.aggregate_version,
                    sequence=record.envelope.sequence,
                    payload_ref=_payload_ref(payload),
                    redacted_payload=payload,
                    occurred_at=record.envelope.occurred_at,
                )
            )
            prior_position = record.cursor.position
        return tuple(result)


def _redact_payload(
    payload: Mapping[str, object],
    *,
    operator_debug: bool,
) -> dict[str, object]:
    allowed_exact = {
        "status",
        "phase",
        "reason_code",
        "retry_layer",
        "intervention_kind",
        "failure_class",
    }
    allowed_references = {
        "payload_ref",
        "result_manifest_ref",
        "evidence_ref",
        "evidence_refs",
        "artifact_ref",
        "artifact_refs",
        "policy_ref",
        "schema_ref",
        "assembly_digest",
        "run_plan_digest",
        "state_schema_digest",
        "response_digest",
        "request_digest",
    }
    operator_exact = {"node_name", "checkpoint_status", "provider_status"}
    redacted: dict[str, object] = {}
    for key, value in payload.items():
        normalized = key.lower().replace("-", "_")
        if normalized in allowed_exact or normalized in allowed_references:
            redacted[key] = value
        elif operator_debug and normalized in operator_exact:
            redacted[key] = value
    return redacted


def _payload_ref(payload: Mapping[str, object]) -> str | None:
    for key in ("payload_ref", "result_manifest_ref", "evidence_ref", "artifact_ref"):
        value = payload.get(key)
        if isinstance(value, str) and value:
            return value
    return None
