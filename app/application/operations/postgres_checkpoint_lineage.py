"""Application PostgreSQL authority for runtime units and checkpoint lineage (migration 0019)."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any
from uuid import NAMESPACE_URL, uuid5

import asyncpg

from app.application.operations.checkpoint_lineage import (
    NamespaceRecord,
    UnitGenerationRecord,
    check_generation_admission,
    check_namespace_owner,
    decide_transition,
    linear_transition_order,
    reserve_in_flight,
)
from app.domain.graph_runtime.identities import QualifiedCheckpointKey, RuntimeUnitIdentity
from app.domain.operation_execution.checkpoint_lineage import (
    ActivityAttemptObservation,
    AttemptAdmission,
    CheckpointLineageConflict,
    CheckpointTransitionObservation,
    LineageWriteRejection,
    NamespaceClaim,
    OperationActivityAttempt,
    StaleClaimFence,
)


class PostgresCheckpointLineageRepository:
    """Serializes each unit and namespace with row locks; heads move only by CAS."""

    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    async def record_attempt(
        self,
        *,
        unit: RuntimeUnitIdentity,
        execution_generation: int,
        attempt: OperationActivityAttempt,
        binding_id: str,
        binding_digest: str,
        namespace: NamespaceClaim | None,
        dispatching: bool,
        observed_at: datetime,
    ) -> AttemptAdmission:
        scope = unit.request_scope
        unit_key = unit.unit_key
        async with self._pool.acquire() as connection, connection.transaction():
            await _set_scope(connection, scope)
            await connection.execute(
                """
                INSERT INTO belllabs_control.runtime_units (
                    request_scope, unit_key, schema_version, belllabs_run_id,
                    execution_epoch, family, unit_kind, semantic_operation_id,
                    semantic_attempt, identity_payload, recorded_at
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10::jsonb, $11)
                ON CONFLICT (request_scope, unit_key) DO NOTHING
                """,
                scope,
                unit_key,
                unit.schema_version,
                unit.belllabs_run_id,
                unit.execution_epoch,
                unit.family,
                unit.unit_kind,
                unit.semantic_operation_id,
                unit.semantic_attempt,
                _dump(unit.model_dump(mode="json")),
                observed_at,
            )
            await _lock_unit(connection, scope, unit_key)
            current = await _generation(connection, scope, unit_key, execution_generation)
            check_generation_admission(
                unit_key=unit_key,
                execution_generation=execution_generation,
                binding_id=binding_id,
                binding_digest=binding_digest,
                namespace=namespace,
                current=current,
                max_generation=await _max_generation(connection, scope, unit_key),
            )
            if current is None:
                current = UnitGenerationRecord(
                    unit_key=unit_key,
                    execution_generation=execution_generation,
                    claim_fence=1,
                    binding_id=binding_id,
                    binding_digest=binding_digest,
                    namespace=namespace.namespace if namespace else None,
                    state_schema_digest=namespace.state_schema_digest if namespace else None,
                )
                await connection.execute(
                    """
                    INSERT INTO belllabs_control.runtime_unit_generations (
                        request_scope, unit_key, execution_generation, claim_fence,
                        binding_id, binding_digest, cognitive_namespace,
                        state_schema_digest, recorded_at, updated_at
                    )
                    VALUES ($1, $2, $3, 1, $4, $5, $6, $7, $8, $8)
                    """,
                    scope,
                    unit_key,
                    execution_generation,
                    binding_id,
                    binding_digest,
                    current.namespace,
                    current.state_schema_digest,
                    observed_at,
                )
            existing = await _transition(connection, scope, unit_key, execution_generation)
            expected_source: QualifiedCheckpointKey | None = None
            if namespace is not None:
                await connection.execute(
                    """
                    INSERT INTO belllabs_control.runtime_cognitive_namespaces (
                        request_scope, cognitive_namespace, owner_kind, owner_digest,
                        updated_at
                    )
                    VALUES ($1, $2, $3, $4, $5)
                    ON CONFLICT (request_scope, cognitive_namespace) DO NOTHING
                    """,
                    scope,
                    namespace.namespace,
                    namespace.owner_kind,
                    namespace.owner_digest,
                    observed_at,
                )
                record = await _namespace(connection, scope, namespace.namespace)
                assert record is not None
                check_namespace_owner(namespace, record)
                if dispatching and existing is None:
                    reserved = reserve_in_flight(
                        record, unit_key=unit_key, execution_generation=execution_generation
                    )
                    if reserved != record:
                        await connection.execute(
                            """
                            UPDATE belllabs_control.runtime_cognitive_namespaces
                            SET in_flight_unit_key = $3, in_flight_generation = $4,
                                updated_at = $5
                            WHERE request_scope = $1 AND cognitive_namespace = $2
                            """,
                            scope,
                            namespace.namespace,
                            unit_key,
                            execution_generation,
                            observed_at,
                        )
                expected_source = existing.source_key if existing is not None else record.head
            observation = ActivityAttemptObservation(
                request_scope=scope,
                unit_key=unit_key,
                execution_generation=execution_generation,
                claim_fence=current.claim_fence,
                attempt=attempt,
                binding_id=binding_id,
                namespace=namespace.namespace if namespace else None,
                expected_source=expected_source,
                dispatching=dispatching,
                observed_at=observed_at,
            )
            await connection.execute(
                """
                INSERT INTO belllabs_control.runtime_activity_attempt_observations (
                    request_scope, observation_id, schema_version, unit_key,
                    execution_generation, claim_fence, temporal_workflow_id,
                    temporal_run_id, temporal_activity_id, activity_attempt,
                    worker_identity, binding_id, cognitive_namespace,
                    expected_source_checkpoint, dispatching, observation_payload, observed_at
                )
                VALUES (
                    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13,
                    $14::jsonb, $15, $16::jsonb, $17
                )
                ON CONFLICT (request_scope, observation_id) DO NOTHING
                """,
                scope,
                observation.observation_id,
                observation.schema_version,
                unit_key,
                execution_generation,
                observation.claim_fence,
                attempt.workflow_id,
                attempt.workflow_run_id,
                attempt.activity_id,
                attempt.attempt,
                attempt.worker_identity,
                binding_id,
                observation.namespace,
                _dump(expected_source.model_dump(mode="json")) if expected_source else None,
                dispatching,
                _dump(observation.model_dump(mode="json")),
                observed_at,
            )
            stored = await connection.fetchval(
                """
                SELECT observation_payload
                FROM belllabs_control.runtime_activity_attempt_observations
                WHERE request_scope = $1 AND observation_id = $2
                """,
                scope,
                observation.observation_id,
            )
        return AttemptAdmission(
            observation=ActivityAttemptObservation.model_validate(_load(stored)),
            existing_transition=existing,
        )

    async def record_transition(
        self, transition: CheckpointTransitionObservation
    ) -> CheckpointTransitionObservation:
        scope = transition.request_scope
        rejection: LineageWriteRejection | None = None
        async with self._pool.acquire() as connection, connection.transaction():
            await _set_scope(connection, scope)
            await _lock_unit(connection, scope, transition.unit_key)
            generation = await _generation(
                connection, scope, transition.unit_key, transition.execution_generation
            )
            namespace = await _namespace(connection, scope, transition.namespace)
            existing = await _transition(
                connection, scope, transition.unit_key, transition.execution_generation
            )
            decision = decide_transition(
                transition,
                generation=generation,
                max_generation=await _max_generation(connection, scope, transition.unit_key),
                namespace=namespace,
                existing=existing,
                rejected_at=datetime.now(UTC),
            )
            if isinstance(decision, LineageWriteRejection):
                rejection = decision
                await connection.execute(
                    """
                    INSERT INTO belllabs_control.runtime_lineage_write_rejections (
                        request_scope, rejection_id, unit_key, execution_generation,
                        presented_fence, current_fence, current_generation, reason,
                        payload_digest, rejected_at
                    )
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                    ON CONFLICT (request_scope, rejection_id) DO NOTHING
                    """,
                    scope,
                    _rejection_id(decision),
                    decision.unit_key,
                    decision.execution_generation,
                    decision.presented_fence,
                    decision.current_fence,
                    decision.current_generation,
                    decision.reason,
                    decision.payload_digest,
                    decision.rejected_at,
                )
            elif decision == "duplicate":
                assert existing is not None
                return existing
            else:
                await connection.execute(
                    """
                    INSERT INTO belllabs_control.runtime_checkpoint_transitions (
                        request_scope, transition_id, schema_version, unit_key,
                        execution_generation, claim_fence, cognitive_namespace,
                        source_checkpoint_id, result_checkpoint_id,
                        result_parent_checkpoint_id, ancestry_verified, binding_digest,
                        state_schema_digest, classification, invocation_id,
                        result_manifest_ref, result_manifest_digest,
                        redacted_summary_digest, content_digest, transition_payload,
                        observed_at
                    )
                    VALUES (
                        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14,
                        $15, $16, $17, $18, $19, $20::jsonb, $21
                    )
                    """,
                    scope,
                    transition.transition_id,
                    transition.schema_version,
                    transition.unit_key,
                    transition.execution_generation,
                    transition.claim_fence,
                    transition.namespace,
                    transition.source_key.checkpoint_id if transition.source_key else None,
                    transition.result_key.checkpoint_id,
                    transition.result_key.parent_checkpoint_id,
                    transition.ancestry_verified,
                    transition.binding_digest,
                    transition.state_schema_digest,
                    transition.classification.value,
                    transition.invocation_id,
                    transition.result_manifest_ref,
                    transition.result_manifest_digest,
                    transition.redacted_summary_digest,
                    transition.content_digest,
                    _dump(transition.model_dump(mode="json")),
                    transition.observed_at,
                )
                await connection.execute(
                    """
                    UPDATE belllabs_control.runtime_cognitive_namespaces
                    SET head_checkpoint = $3::jsonb, head_checkpoint_id = $4,
                        head_transition_id = $5, head_state_schema_digest = $6,
                        head_version = head_version + 1,
                        in_flight_unit_key = NULL, in_flight_generation = NULL,
                        updated_at = $7
                    WHERE request_scope = $1 AND cognitive_namespace = $2
                    """,
                    scope,
                    transition.namespace,
                    _dump(transition.result_key.model_dump(mode="json")),
                    transition.result_key.checkpoint_id,
                    transition.transition_id,
                    transition.state_schema_digest,
                    transition.observed_at,
                )
        if rejection is not None:
            raise StaleClaimFence(
                f"transition presented a superseded {rejection.reason.removeprefix('stale_')}"
            )
        return transition

    async def get_transition(
        self, request_scope: str, unit_key: str, execution_generation: int
    ) -> CheckpointTransitionObservation | None:
        async with self._pool.acquire() as connection, connection.transaction():
            await _set_scope(connection, request_scope)
            return await _transition(connection, request_scope, unit_key, execution_generation)

    async def get_namespace_head(
        self, request_scope: str, namespace: str
    ) -> QualifiedCheckpointKey | None:
        async with self._pool.acquire() as connection, connection.transaction():
            await _set_scope(connection, request_scope)
            record = await _namespace(connection, request_scope, namespace, lock=False)
        return record.head if record is not None else None

    async def list_attempts(
        self, request_scope: str, unit_key: str
    ) -> tuple[ActivityAttemptObservation, ...]:
        async with self._pool.acquire() as connection, connection.transaction():
            await _set_scope(connection, request_scope)
            rows = await connection.fetch(
                """
                SELECT observation_payload
                FROM belllabs_control.runtime_activity_attempt_observations
                WHERE request_scope = $1 AND unit_key = $2
                ORDER BY execution_generation, activity_attempt, observed_at
                """,
                request_scope,
                unit_key,
            )
        return tuple(
            ActivityAttemptObservation.model_validate(_load(row["observation_payload"]))
            for row in rows
        )

    async def list_transitions(
        self, request_scope: str, namespace: str
    ) -> tuple[CheckpointTransitionObservation, ...]:
        async with self._pool.acquire() as connection, connection.transaction():
            await _set_scope(connection, request_scope)
            rows = await connection.fetch(
                """
                SELECT transition_payload
                FROM belllabs_control.runtime_checkpoint_transitions
                WHERE request_scope = $1 AND cognitive_namespace = $2
                """,
                request_scope,
                namespace,
            )
        return tuple(
            linear_transition_order(
                [
                    CheckpointTransitionObservation.model_validate(_load(row["transition_payload"]))
                    for row in rows
                ]
            )
        )

    async def list_rejections(
        self, request_scope: str, unit_key: str
    ) -> tuple[LineageWriteRejection, ...]:
        async with self._pool.acquire() as connection, connection.transaction():
            await _set_scope(connection, request_scope)
            rows = await connection.fetch(
                """
                SELECT * FROM belllabs_control.runtime_lineage_write_rejections
                WHERE request_scope = $1 AND unit_key = $2
                ORDER BY rejected_at, rejection_id
                """,
                request_scope,
                unit_key,
            )
        return tuple(
            LineageWriteRejection(
                request_scope=row["request_scope"],
                unit_key=row["unit_key"],
                execution_generation=row["execution_generation"],
                presented_fence=row["presented_fence"],
                current_fence=row["current_fence"],
                current_generation=row["current_generation"],
                reason=row["reason"],
                payload_digest=row["payload_digest"],
                rejected_at=row["rejected_at"],
            )
            for row in rows
        )

    async def advance_claim_fence(
        self,
        request_scope: str,
        unit_key: str,
        execution_generation: int,
        *,
        expected_fence: int,
    ) -> int:
        """Claim-takeover seam for RRM-004: advance the fence only from the expected value."""

        async with self._pool.acquire() as connection, connection.transaction():
            await _set_scope(connection, request_scope)
            advanced = await connection.fetchval(
                """
                UPDATE belllabs_control.runtime_unit_generations
                SET claim_fence = claim_fence + 1, updated_at = clock_timestamp()
                WHERE request_scope = $1 AND unit_key = $2 AND execution_generation = $3
                  AND claim_fence = $4
                RETURNING claim_fence
                """,
                request_scope,
                unit_key,
                execution_generation,
                expected_fence,
            )
        if advanced is None:
            raise CheckpointLineageConflict("claim fence changed concurrently")
        return int(advanced)


async def _set_scope(connection: asyncpg.Connection, request_scope: str) -> None:
    await connection.execute("SELECT set_config('belllabs.request_scope', $1, true)", request_scope)


async def _lock_unit(connection: asyncpg.Connection, scope: str, unit_key: str) -> None:
    await connection.execute(
        """
        SELECT 1 FROM belllabs_control.runtime_units
        WHERE request_scope = $1 AND unit_key = $2
        FOR UPDATE
        """,
        scope,
        unit_key,
    )


async def _max_generation(connection: asyncpg.Connection, scope: str, unit_key: str) -> int | None:
    value = await connection.fetchval(
        """
        SELECT max(execution_generation)
        FROM belllabs_control.runtime_unit_generations
        WHERE request_scope = $1 AND unit_key = $2
        """,
        scope,
        unit_key,
    )
    return int(value) if value is not None else None


async def _generation(
    connection: asyncpg.Connection, scope: str, unit_key: str, execution_generation: int
) -> UnitGenerationRecord | None:
    row = await connection.fetchrow(
        """
        SELECT * FROM belllabs_control.runtime_unit_generations
        WHERE request_scope = $1 AND unit_key = $2 AND execution_generation = $3
        """,
        scope,
        unit_key,
        execution_generation,
    )
    if row is None:
        return None
    return UnitGenerationRecord(
        unit_key=row["unit_key"],
        execution_generation=row["execution_generation"],
        claim_fence=row["claim_fence"],
        binding_id=row["binding_id"],
        binding_digest=row["binding_digest"],
        namespace=row["cognitive_namespace"],
        state_schema_digest=row["state_schema_digest"],
    )


async def _namespace(
    connection: asyncpg.Connection, scope: str, namespace: str, *, lock: bool = True
) -> NamespaceRecord | None:
    row = await connection.fetchrow(
        """
        SELECT * FROM belllabs_control.runtime_cognitive_namespaces
        WHERE request_scope = $1 AND cognitive_namespace = $2
        """
        + (" FOR UPDATE" if lock else ""),
        scope,
        namespace,
    )
    if row is None:
        return None
    head = row["head_checkpoint"]
    return NamespaceRecord(
        namespace=row["cognitive_namespace"],
        owner_kind=row["owner_kind"],
        owner_digest=row["owner_digest"],
        head=QualifiedCheckpointKey.model_validate(_load(head)) if head is not None else None,
        head_transition_id=row["head_transition_id"],
        head_state_schema_digest=row["head_state_schema_digest"],
        in_flight_unit_key=row["in_flight_unit_key"],
        in_flight_generation=row["in_flight_generation"],
    )


async def _transition(
    connection: asyncpg.Connection, scope: str, unit_key: str, execution_generation: int
) -> CheckpointTransitionObservation | None:
    payload = await connection.fetchval(
        """
        SELECT transition_payload FROM belllabs_control.runtime_checkpoint_transitions
        WHERE request_scope = $1 AND unit_key = $2 AND execution_generation = $3
        """,
        scope,
        unit_key,
        execution_generation,
    )
    if payload is None:
        return None
    return CheckpointTransitionObservation.model_validate(_load(payload))


def _rejection_id(rejection: LineageWriteRejection) -> str:
    identity = (
        f"lineage-rejection:{rejection.request_scope}:{rejection.unit_key}:"
        f"{rejection.execution_generation}:{rejection.presented_fence}:"
        f"{rejection.payload_digest}"
    )
    return f"lineage-rejection:{uuid5(NAMESPACE_URL, identity)}"


def _dump(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def _load(value: Any) -> Any:
    return json.loads(value) if isinstance(value, str) else value
