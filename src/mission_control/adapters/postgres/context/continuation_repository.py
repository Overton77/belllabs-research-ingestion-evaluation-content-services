"""PostgreSQL ledger of continuation transfers and sealed checkpoints (FT-B4).

- ``mission_control.continuation_transfer`` (migration 0027, section B4) holds one mutable
  saga row per (logical execution, trigger) with an optimistic ``version``.
- A sealed ``mc.continuation_checkpoint.v1`` is a canonical ``continuation_checkpoint`` row
  (0003; ``contract_version = 'mc.continuation_checkpoint.v1'``, the manifest jsonb, its
  digest) plus its ``checkpoint_validation`` verdict, written in one transaction. Both rows
  are immutable; a replay with the same digest returns the stored checkpoint and a
  different digest under the same id is an idempotency conflict.

Every statement runs under the transaction-local scope and filters on all three scope
columns in addition to the forced row-level security policies.
"""

from __future__ import annotations

import json
from typing import Any
from uuid import UUID

import asyncpg

from mission_control.adapters.postgres.run_control import canonical as mc
from mission_control.adapters.postgres.run_control.canonical import SCOPE
from mission_control.application.context.continuation import (
    ContinuationRejected,
    ContinuationTransfer,
    StoredCheckpoint,
)
from mission_control.contracts.identities import uuid7
from mission_control.domain.context.checkpoint import CHECKPOINT_SCHEMA_VERSION
from mission_control.domain.policies.errors import IdempotencyConflict

CONTINUATION_ACTOR = "service:mission-control-continuation"


class PostgresContinuationRepository:
    """Implements ``ContinuationTransferRepository`` over ``continuation_transfer``."""

    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    async def request(self, transfer: ContinuationTransfer) -> ContinuationTransfer:
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, transfer.request_scope)
            await mc.require_run(connection, args, transfer.run_key)
            await connection.execute(
                """INSERT INTO mission_control.continuation_transfer (
                       installation_id, application_id, tenant_id, continuation_transfer_id,
                       transfer_key, run_key, activation_key, logical_execution_id,
                       lane_profile, trigger_kind, trigger_ref, delivery, status,
                       source_session_ref, target_session_ref, checkpoint_key, released,
                       failure_reason, transfer, version, requested_at, updated_at,
                       created_at, created_by_actor_ref)
                   VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,
                           $19::jsonb,$20,$21,$22,clock_timestamp(),$23)
                   ON CONFLICT (installation_id, application_id, tenant_id, transfer_key)
                   DO NOTHING""",
                *args,
                uuid7(),
                *_columns(transfer),
                transfer.requested_at,
                transfer.updated_at,
                CONTINUATION_ACTOR,
            )
            row = await connection.fetchrow(
                f"""SELECT transfer FROM mission_control.continuation_transfer
                    WHERE {SCOPE} AND transfer_key = $4""",
                *args,
                transfer.transfer_id,
            )
        assert row is not None
        return ContinuationTransfer.model_validate(_json(row["transfer"]))

    async def get(self, request_scope: str, transfer_id: str) -> ContinuationTransfer | None:
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, request_scope)
            value = await connection.fetchval(
                f"""SELECT transfer FROM mission_control.continuation_transfer
                    WHERE {SCOPE} AND transfer_key = $4""",
                *args,
                transfer_id,
            )
        return ContinuationTransfer.model_validate(_json(value)) if value is not None else None

    async def update(
        self, transfer: ContinuationTransfer, *, expected_version: int
    ) -> ContinuationTransfer:
        if transfer.version != expected_version + 1:
            raise ValueError("an update advances the transfer version by exactly one")
        columns = _columns(transfer)
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, transfer.request_scope)
            updated = await connection.fetchval(
                f"""UPDATE mission_control.continuation_transfer
                    SET status = $5, target_session_ref = $6, checkpoint_key = $7,
                        released = $8, failure_reason = $9, transfer = $10::jsonb,
                        version = $11, updated_at = $12
                    WHERE {SCOPE} AND transfer_key = $4 AND version = $13
                    RETURNING version""",
                *args,
                transfer.transfer_id,
                transfer.status.value,
                transfer.target_session_ref,
                transfer.checkpoint_id,
                transfer.released,
                transfer.failure_reason,
                columns[14],
                transfer.version,
                transfer.updated_at,
                expected_version,
            )
        if updated is None:
            raise ContinuationRejected("stale_version", "continuation transfer changed")
        return transfer

    async def for_run(self, request_scope: str, run_key: str) -> tuple[ContinuationTransfer, ...]:
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, request_scope)
            rows = await connection.fetch(
                f"""SELECT transfer FROM mission_control.continuation_transfer
                    WHERE {SCOPE} AND run_key = $4
                    ORDER BY requested_at, transfer_key""",
                *args,
                run_key,
            )
        return tuple(ContinuationTransfer.model_validate(_json(row["transfer"])) for row in rows)


class PostgresCheckpointRepository:
    """Implements ``CheckpointRepository`` over ``continuation_checkpoint`` (0003)."""

    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    async def put(self, stored: StoredCheckpoint) -> StoredCheckpoint:
        checkpoint = stored.checkpoint
        key = _checkpoint_key(checkpoint.checkpoint_id)
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, stored.request_scope)
            await mc.advisory_lock(connection, f"continuation-checkpoint:{key}")
            prior = await connection.fetchrow(
                f"""SELECT manifest_digest, manifest FROM mission_control.continuation_checkpoint
                    WHERE {SCOPE} AND checkpoint_key = $4""",
                *args,
                key,
            )
            if prior is not None:
                if prior["manifest_digest"] != checkpoint.checkpoint_digest:
                    raise IdempotencyConflict(
                        "a different continuation checkpoint is sealed under this id"
                    )
                return _stored(_json(prior["manifest"]))
            run_id = await mc.run_uuid(connection, args, stored.run_key)
            checkpoint_uuid = uuid7()
            await connection.execute(
                """INSERT INTO mission_control.continuation_checkpoint (
                       installation_id, application_id, tenant_id, checkpoint_id, run_id,
                       checkpoint_key, activation_id, attempt_id, predecessor_checkpoint_id,
                       manifest_ref, manifest_digest, contract_version, runtime_checkpoint_ref,
                       sandbox_snapshot_ref, source_revision_digest, source_binding_digest,
                       source_input_digest, execution_epoch, execution_generation,
                       event_frontier, harness_format, manifest, created_at,
                       created_by_actor_ref)
                   VALUES ($1,$2,$3,$4,$5,$6,NULL,NULL,
                           (SELECT checkpoint_id FROM mission_control.continuation_checkpoint
                             WHERE installation_id = $1 AND application_id = $2
                               AND tenant_id = $3 AND checkpoint_key = $7),
                           $8,$9,$10,NULL,$11,NULL,NULL,NULL,1,$12,$13,$14,$15::jsonb,$16,$17)""",
                *args,
                checkpoint_uuid,
                run_id,
                key,
                _checkpoint_key(checkpoint.supersedes) if checkpoint.supersedes else None,
                f"checkpoint://{stored.run_key}/{checkpoint.checkpoint_id}",
                checkpoint.checkpoint_digest,
                CHECKPOINT_SCHEMA_VERSION,
                checkpoint.sandbox_snapshot_ref,
                max(stored.execution_generation, 1),
                f"mission-seq:{checkpoint.event_cursor}",
                checkpoint.versions.lane_profile,
                stored.model_dump_json(),
                stored.sealed_at,
                CONTINUATION_ACTOR,
            )
            reasons = ", ".join(
                f"{item.code.value}@{item.path}" for item in checkpoint.validator.reasons
            )
            await connection.execute(
                """INSERT INTO mission_control.checkpoint_validation (
                       installation_id, application_id, tenant_id, checkpoint_validation_id,
                       checkpoint_id, status, reason, decided_at, created_at,
                       created_by_actor_ref)
                   VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$8,$9)""",
                *args,
                uuid7(),
                checkpoint_uuid,
                checkpoint.validator.result,
                reasons[:1024] or None,
                stored.sealed_at,
                CONTINUATION_ACTOR,
            )
        return stored

    async def get(
        self, request_scope: str, run_key: str, checkpoint_id: str
    ) -> StoredCheckpoint | None:
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, request_scope)
            value = await connection.fetchval(
                f"""SELECT c.manifest FROM mission_control.continuation_checkpoint c
                    JOIN mission_control.mission_run r
                      ON r.installation_id = c.installation_id
                     AND r.application_id = c.application_id
                     AND r.tenant_id = c.tenant_id AND r.run_id = c.run_id
                    WHERE {mc.scoped("c")} AND c.checkpoint_key = $4
                      AND c.contract_version = $5 AND r.run_key = $6""",
                *args,
                _checkpoint_key(checkpoint_id),
                CHECKPOINT_SCHEMA_VERSION,
                run_key,
            )
        return _stored(_json(value)) if value is not None else None

    async def list(self, request_scope: str, run_key: str) -> tuple[StoredCheckpoint, ...]:
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, request_scope)
            rows = await connection.fetch(
                f"""SELECT c.manifest FROM mission_control.continuation_checkpoint c
                    JOIN mission_control.mission_run r
                      ON r.installation_id = c.installation_id
                     AND r.application_id = c.application_id
                     AND r.tenant_id = c.tenant_id AND r.run_id = c.run_id
                    WHERE {mc.scoped("c")} AND c.contract_version = $4 AND r.run_key = $5
                    ORDER BY c.created_at, c.checkpoint_key""",
                *args,
                CHECKPOINT_SCHEMA_VERSION,
                run_key,
            )
        return tuple(_stored(_json(row["manifest"])) for row in rows)


def _checkpoint_key(checkpoint_id: str) -> str:
    return f"continuation:{checkpoint_id}"


def _columns(transfer: ContinuationTransfer) -> tuple[Any, ...]:
    return (
        transfer.transfer_id,
        transfer.run_key,
        transfer.activation_key,
        transfer.logical_execution_id,
        transfer.lane_profile,
        transfer.trigger.kind.value,
        transfer.trigger.ref,
        transfer.delivery,
        transfer.status.value,
        transfer.source_session_ref,
        transfer.target_session_ref,
        transfer.checkpoint_id,
        transfer.released,
        transfer.failure_reason,
        transfer.model_dump_json(),
        transfer.version,
    )


def _stored(value: Any) -> StoredCheckpoint:
    return StoredCheckpoint.model_validate(value)


def _json(value: Any) -> Any:
    return json.loads(value) if isinstance(value, str) else value


class PostgresRunIds:
    """Resolves a run key to its ``mission_run.run_id`` under the request scope."""

    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    async def __call__(self, request_scope: str, run_key: str) -> UUID | None:
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, request_scope)
            value = await connection.fetchval(
                f"SELECT run_id FROM mission_control.mission_run WHERE {SCOPE} AND run_key = $4",
                *args,
                run_key,
            )
        return value if isinstance(value, UUID) else None
