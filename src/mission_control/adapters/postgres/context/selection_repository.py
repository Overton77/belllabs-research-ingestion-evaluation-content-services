"""PostgreSQL ledger of sealed Context Packets and their selection records (FT-B2).

``mission_control.context_selection`` holds one immutable row per packet target
``(run, activation, attempt, generation, purpose)``. A replay with the same digest returns the
stored packet; a different digest for the same target is an idempotency conflict, so a packet
can never silently change under an admitted operation.
"""

from __future__ import annotations

import json
from typing import Any

import asyncpg

from mission_control.adapters.postgres.scope import apply_scope
from mission_control.contracts.identities import parse_request_scope, uuid7
from mission_control.domain.context.packet import ContextPacket
from mission_control.domain.context.render import ContextSelectionRecord
from mission_control.domain.policies.errors import IdempotencyConflict

_SERVICE_ACTOR = "service:mission-control-context"


class PostgresContextSelectionRepository:
    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    async def record(
        self,
        packet: ContextPacket,
        selection: ContextSelectionRecord,
        *,
        request_scope: str,
    ) -> ContextPacket:
        if (
            selection.packet_digest != packet.packet_digest
            or selection.selection_id != packet.context_selection_ref
        ):
            raise ValueError("selection record does not describe this packet")
        scope = parse_request_scope(request_scope)
        key = (scope.installation_id, scope.application_id, scope.tenant_id)
        target = packet.target
        async with self._pool.acquire() as connection, connection.transaction():
            await apply_scope(connection, scope)
            await connection.execute(
                """INSERT INTO mission_control.context_selection
                   (installation_id, application_id, tenant_id, context_selection_id,
                    selection_key, packet_key, run_key, node_key, activation_key, attempt_no,
                    generation, purpose, packer_version, packet_digest, prompt_plan_digest,
                    file_plan_digest, packet, selection, sealed_at, created_at,
                    created_by_actor_ref)
                   VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,
                           $17::jsonb,$18::jsonb,$19,clock_timestamp(),$20)
                   ON CONFLICT DO NOTHING""",
                *key,
                uuid7(),
                selection.selection_id,
                packet.packet_id,
                target.run_id,
                target.node_key,
                target.activation_id,
                target.attempt_no,
                target.generation,
                target.purpose.value,
                packet.packer_version,
                packet.packet_digest,
                selection.prompt_plan_digest,
                selection.file_plan_digest,
                packet.model_dump_json(),
                selection.model_dump_json(),
                packet.sealed_at,
                _SERVICE_ACTOR,
            )
            row = await connection.fetchrow(
                """SELECT packet, packet_digest FROM mission_control.context_selection
                   WHERE installation_id=$1 AND application_id=$2 AND tenant_id=$3
                     AND run_key=$4 AND activation_key=$5 AND attempt_no=$6
                     AND generation=$7 AND purpose=$8""",
                *key,
                target.run_id,
                target.activation_id,
                target.attempt_no,
                target.generation,
                target.purpose.value,
            )
        if row is None or row["packet_digest"] != packet.packet_digest:
            raise IdempotencyConflict(
                "a different context packet is already sealed for this target"
            )
        return ContextPacket.model_validate(_json(row["packet"]))

    async def get(self, packet_id: str, *, request_scope: str) -> ContextPacket | None:
        scope = parse_request_scope(request_scope)
        async with self._pool.acquire() as connection, connection.transaction():
            await apply_scope(connection, scope)
            value = await connection.fetchval(
                """SELECT packet FROM mission_control.context_selection
                   WHERE installation_id=$1 AND application_id=$2 AND tenant_id=$3
                     AND packet_key=$4""",
                scope.installation_id,
                scope.application_id,
                scope.tenant_id,
                packet_id,
            )
        return ContextPacket.model_validate(_json(value)) if value is not None else None

    async def list_for_run(
        self, run_id: str, *, request_scope: str
    ) -> tuple[ContextSelectionRecord, ...]:
        scope = parse_request_scope(request_scope)
        async with self._pool.acquire() as connection, connection.transaction():
            await apply_scope(connection, scope)
            rows = await connection.fetch(
                """SELECT selection FROM mission_control.context_selection
                   WHERE installation_id=$1 AND application_id=$2 AND tenant_id=$3
                     AND run_key=$4
                   ORDER BY sealed_at, node_key, activation_key, attempt_no, generation""",
                scope.installation_id,
                scope.application_id,
                scope.tenant_id,
                run_id,
            )
        return tuple(ContextSelectionRecord.model_validate(_json(row["selection"])) for row in rows)


def _json(value: Any) -> Any:
    return json.loads(value) if isinstance(value, str) else value
