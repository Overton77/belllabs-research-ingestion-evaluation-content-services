"""Governed effect intents and receipts (PROPOSED 0033 ``governed_effect_intent``; MP-11).

``prepare`` inserts once per intent key (a repeat returns the stored intent; another
argument set under the same key is a conflict). Every state move is a compare-and-set on the
row (``UPDATE ... WHERE state = ANY($from) RETURNING``), so two concurrent ``execute`` calls
cannot both claim ``executing``; the one receipt is written by a compare-and-set from
``executing`` and is immutable afterwards (trigger in the proposed DDL).
"""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any
from uuid import UUID

import asyncpg

from mission_control.adapters.postgres.run_control import canonical as mc
from mission_control.adapters.postgres.run_control.canonical import SCOPE
from mission_control.application.execution.approvals_governed import (
    GovernedIntent,
    GovernedIntentState,
    GovernedReceipt,
)
from mission_control.domain.policies.errors import IdempotencyConflict

ACTOR_REF = "mission-control-runtime/governed-gateway"
_COLUMNS = """
    governed_effect_intent_id, intent_key, run_key, harness_execution_id, generation,
    lane_profile, tool_name, effect_kind, arguments, input_digest, policy_digest,
    human_task_id, state, reason, claimant_ref, claimed_at, receipt, version, created_at,
    updated_at
"""


class PostgresGovernedIntentRepository:
    def __init__(self, pool: asyncpg.Pool, *, actor_ref: str = ACTOR_REF) -> None:
        self._pool = pool
        self._actor_ref = actor_ref

    async def prepare(self, intent: GovernedIntent) -> GovernedIntent:
        scope = intent.request_scope
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, scope)
            await mc.advisory_lock(
                connection, f"governed_effect_intent:{scope}:{intent.intent_key}"
            )
            prior = await _row(connection, args, intent.intent_id)
            if prior is not None:
                stored = _intent(prior, scope)
                if stored.intent_key != intent.intent_key or stored.arguments != intent.arguments:
                    raise IdempotencyConflict("governed intent identity has conflicting intent")
                return stored
            run = await mc.run_uuid(connection, args, intent.run_id)
            await connection.execute(
                """
                INSERT INTO mission_control.governed_effect_intent (
                    installation_id, application_id, tenant_id, governed_effect_intent_id,
                    intent_key, run_id, run_key, harness_execution_id, generation, lane_profile,
                    tool_name, effect_kind, arguments, input_digest, policy_digest,
                    human_task_id, state, version, created_at, updated_at, created_by_actor_ref
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13::jsonb, $14,
                        $15, $16, $17, 1, $18, $18, $19)
                """,
                *args,
                UUID(intent.intent_id),
                intent.intent_key,
                run,
                intent.run_id,
                intent.harness_execution_id,
                intent.generation,
                intent.lane_profile,
                intent.tool_name,
                intent.effect_kind,
                json.dumps(intent.arguments, sort_keys=True, separators=(",", ":")),
                intent.input_digest,
                intent.policy_digest,
                UUID(intent.human_task_id) if intent.human_task_id is not None else None,
                intent.state,
                intent.created_at,
                self._actor_ref,
            )
            row = await _row(connection, args, intent.intent_id)
        assert row is not None
        return _intent(row, scope)

    async def get(self, request_scope: str, intent_id: str) -> GovernedIntent | None:
        try:
            key = UUID(intent_id)
        except ValueError:
            return None
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, request_scope)
            row = await _row(connection, args, str(key))
        return _intent(row, request_scope) if row is not None else None

    async def transition(
        self,
        request_scope: str,
        intent_id: str,
        *,
        from_states: frozenset[str],
        to_state: GovernedIntentState,
        at: datetime,
        reason: str | None = None,
        claimant_ref: str | None = None,
    ) -> GovernedIntent | None:
        claiming = to_state == "executing"
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, request_scope)
            row = await connection.fetchrow(
                f"""
                UPDATE mission_control.governed_effect_intent
                SET state = $5, reason = $6, updated_at = $7, version = version + 1,
                    claimant_ref = CASE WHEN $8 THEN $9 ELSE claimant_ref END,
                    claimed_at = CASE WHEN $8 THEN $7 ELSE claimed_at END
                WHERE {SCOPE} AND governed_effect_intent_id = $4 AND state = ANY($10::text[])
                RETURNING {_COLUMNS}
                """,
                *args,
                UUID(intent_id),
                to_state,
                reason,
                at,
                claiming,
                claimant_ref,
                sorted(from_states),
            )
        return _intent(row, request_scope) if row is not None else None

    async def record_receipt(
        self, request_scope: str, intent_id: str, receipt: GovernedReceipt, *, at: datetime
    ) -> GovernedIntent | None:
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, request_scope)
            row = await connection.fetchrow(
                f"""
                UPDATE mission_control.governed_effect_intent
                SET state = 'executed', receipt = $5::jsonb, receipt_digest = $6,
                    updated_at = $7, version = version + 1
                WHERE {SCOPE} AND governed_effect_intent_id = $4 AND state = 'executing'
                RETURNING {_COLUMNS}
                """,
                *args,
                UUID(intent_id),
                json.dumps(receipt.model_dump(mode="json"), sort_keys=True, separators=(",", ":")),
                receipt.receipt_digest,
                at,
            )
        return _intent(row, request_scope) if row is not None else None


async def _row(
    connection: asyncpg.Connection, args: tuple[Any, ...], intent_id: str
) -> asyncpg.Record | None:
    return await connection.fetchrow(
        f"""
        SELECT {_COLUMNS} FROM mission_control.governed_effect_intent
        WHERE {SCOPE} AND governed_effect_intent_id = $4
        """,
        *args,
        UUID(intent_id),
    )


def _intent(row: asyncpg.Record, request_scope: str) -> GovernedIntent:
    receipt = _json(row["receipt"])
    return GovernedIntent(
        intent_id=str(row["governed_effect_intent_id"]),
        intent_key=row["intent_key"],
        request_scope=request_scope,
        run_id=row["run_key"],
        harness_execution_id=row["harness_execution_id"],
        generation=int(row["generation"]),
        lane_profile=row["lane_profile"],
        tool_name=row["tool_name"],
        effect_kind=row["effect_kind"],
        arguments=_json(row["arguments"]),
        input_digest=row["input_digest"],
        policy_digest=row["policy_digest"],
        human_task_id=str(row["human_task_id"]) if row["human_task_id"] is not None else None,
        state=row["state"],
        reason=row["reason"],
        claimant_ref=row["claimant_ref"],
        claimed_at=row["claimed_at"],
        receipt=GovernedReceipt.model_validate(receipt) if receipt is not None else None,
        version=int(row["version"]),
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


def _json(value: Any) -> Any:
    return json.loads(value) if isinstance(value, str) else value


__all__ = ["PostgresGovernedIntentRepository"]
