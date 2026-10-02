"""Application PostgreSQL authority for runtime units and checkpoint lineage.

Migration 0019 (RRM-003) holds units, attempts, namespaces and transitions; migration 0020
(RRM-004) adds the claim lease and generation boundary on `runtime_unit_generations`, the
fenced `runtime_unit_result_observations`, and writes typed `in_doubt` incidents into
`runtime_reconciliation_incidents`.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import NAMESPACE_URL, uuid5

import asyncpg

from app.application.operations.checkpoint_lineage import (
    NamespaceRecord,
    UnitGenerationRecord,
    check_generation_admission,
    check_namespace_owner,
    decide_incident_opening,
    decide_lease,
    decide_result,
    decide_transition,
    effective_max_generation,
    lease_holder_id,
    linear_transition_order,
    release_in_flight,
    require_linked_result,
    reserve_in_flight,
    resolve_incident,
    stale_claim_error,
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
    UnitReconciliationIncident,
    UnitResultObservation,
)
from app.domain.run_control.contracts import UnitReconciliationDecision

INCIDENT_TYPE = "runtime_unit_in_doubt"
INCIDENT_ACTOR = "belllabs-operation-runtime"
INCIDENT_RETENTION = timedelta(days=3650)


class PostgresCheckpointLineageRepository:
    """Serializes each unit with an advisory lock and namespaces with row locks."""

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
        lease_expires_at: datetime | None = None,
    ) -> AttemptAdmission:
        scope = unit.request_scope
        unit_key = unit.unit_key
        holder = lease_holder_id(scope, unit_key, execution_generation, attempt)
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
            granted, took_over = True, False
            if lease_expires_at is not None and dispatching:
                lease = decide_lease(
                    current, holder=holder, requested_until=lease_expires_at, now=observed_at
                )
                granted, took_over, dispatching = lease.granted, lease.took_over, lease.granted
                if lease.granted:
                    current = lease.record
                    await connection.execute(
                        """
                        UPDATE belllabs_control.runtime_unit_generations
                        SET claim_fence = $4, lease_holder = $5, lease_expires_at = $6,
                            updated_at = $7
                        WHERE request_scope = $1 AND unit_key = $2
                          AND execution_generation = $3
                        """,
                        scope,
                        unit_key,
                        execution_generation,
                        current.claim_fence,
                        current.lease_holder,
                        current.lease_expires_at,
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
                result = await _result(connection, scope, unit_key, execution_generation)
                # A unit generation whose result is already fixed never re-reserves its
                # namespace: it only settles the recorded manifest (`observed_unsettled`).
                if dispatching and existing is None and result is None:
                    reserved = reserve_in_flight(
                        record, unit_key=unit_key, execution_generation=execution_generation
                    )
                    if reserved != record:
                        await _write_in_flight(connection, scope, reserved, observed_at)
                expected_source = existing.source_key if existing is not None else record.head
            prior_dispatch = bool(
                await connection.fetchval(
                    """
                    SELECT EXISTS (
                        SELECT 1 FROM belllabs_control.runtime_activity_attempt_observations
                        WHERE request_scope = $1 AND unit_key = $2
                          AND execution_generation = $3 AND dispatching
                          AND observation_id <> $4
                    )
                    """,
                    scope,
                    unit_key,
                    execution_generation,
                    holder,
                )
            )
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
            result = await _result(connection, scope, unit_key, execution_generation)
            incident = await _incident(connection, scope, unit_key, execution_generation)
        return AttemptAdmission(
            observation=ActivityAttemptObservation.model_validate(_load(stored)),
            existing_transition=existing,
            lease_granted=granted,
            took_over=took_over,
            prior_dispatch=prior_dispatch,
            existing_result=result,
            incident=incident,
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
                await _record_rejection(connection, decision)
            elif decision == "duplicate":
                assert existing is not None
                return existing
            else:
                await _insert_transition(connection, transition)
        if rejection is not None:
            raise stale_claim_error(rejection)
        return transition

    async def record_result(
        self,
        result: UnitResultObservation,
        *,
        transition: CheckpointTransitionObservation | None = None,
    ) -> UnitResultObservation:
        scope = result.request_scope
        rejection: LineageWriteRejection | None = None
        async with self._pool.acquire() as connection, connection.transaction():
            await _set_scope(connection, scope)
            await _lock_unit(connection, scope, result.unit_key)
            generation = await _generation(
                connection, scope, result.unit_key, result.execution_generation
            )
            max_generation = await _max_generation(connection, scope, result.unit_key)
            existing = await _result(
                connection, scope, result.unit_key, result.execution_generation
            )
            now = datetime.now(UTC)
            decision = decide_result(
                result,
                generation=generation,
                max_generation=max_generation,
                existing=existing,
                rejected_at=now,
            )
            if isinstance(decision, LineageWriteRejection):
                rejection = decision
                await _record_rejection(connection, decision)
            elif decision == "duplicate":
                assert existing is not None
                return existing
            else:
                assert generation is not None
                if transition is not None:
                    require_linked_result(result, transition)
                    namespace = await _namespace(connection, scope, transition.namespace)
                    transition_decision = decide_transition(
                        transition,
                        generation=generation,
                        max_generation=max_generation,
                        namespace=namespace,
                        existing=await _transition(
                            connection,
                            scope,
                            transition.unit_key,
                            transition.execution_generation,
                        ),
                        rejected_at=now,
                    )
                    if isinstance(transition_decision, LineageWriteRejection):
                        rejection = transition_decision
                        await _record_rejection(connection, transition_decision)
                    elif transition_decision == "accept":
                        await _insert_transition(connection, transition)
                elif generation.namespace is not None:
                    record = await _namespace(connection, scope, generation.namespace)
                    if record is not None:
                        released = release_in_flight(
                            record,
                            unit_key=result.unit_key,
                            execution_generation=result.execution_generation,
                        )
                        if released != record:
                            await _write_in_flight(connection, scope, released, now)
                if rejection is None:
                    await connection.execute(
                        """
                        INSERT INTO belllabs_control.runtime_unit_result_observations (
                            request_scope, observation_id, schema_version, unit_key,
                            execution_generation, claim_fence, binding_id, settlement_id,
                            status, result_manifest_ref, result_manifest_digest,
                            result_manifest_size_bytes, checkpoint_transition_id,
                            content_digest, result_payload, observed_at
                        )
                        VALUES (
                            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14,
                            $15::jsonb, $16
                        )
                        """,
                        scope,
                        result.observation_id,
                        result.schema_version,
                        result.unit_key,
                        result.execution_generation,
                        result.claim_fence,
                        result.binding_id,
                        result.settlement_id,
                        result.status,
                        result.result_manifest_ref,
                        result.result_manifest_digest,
                        result.result_manifest_size_bytes,
                        result.checkpoint_transition_id,
                        result.content_digest,
                        _dump(result.model_dump(mode="json")),
                        result.observed_at,
                    )
        if rejection is not None:
            raise stale_claim_error(rejection)
        return result

    async def get_result(
        self, request_scope: str, unit_key: str, execution_generation: int
    ) -> UnitResultObservation | None:
        async with self._pool.acquire() as connection, connection.transaction():
            await _set_scope(connection, request_scope)
            return await _result(connection, request_scope, unit_key, execution_generation)

    async def release_lease(
        self,
        request_scope: str,
        unit_key: str,
        execution_generation: int,
        *,
        holder: str,
        released_at: datetime,
    ) -> None:
        async with self._pool.acquire() as connection, connection.transaction():
            await _set_scope(connection, request_scope)
            await _lock_unit(connection, request_scope, unit_key)
            await connection.execute(
                """
                UPDATE belllabs_control.runtime_unit_generations
                SET lease_expires_at = $5, updated_at = $5
                WHERE request_scope = $1 AND unit_key = $2 AND execution_generation = $3
                  AND lease_holder = $4
                """,
                request_scope,
                unit_key,
                execution_generation,
                holder,
                released_at,
            )

    async def open_incident(
        self, incident: UnitReconciliationIncident
    ) -> UnitReconciliationIncident:
        scope = incident.request_scope
        async with self._pool.acquire() as connection, connection.transaction():
            await _set_scope(connection, scope)
            await _lock_unit(connection, scope, incident.unit_key)
            latest = await _incident(
                connection, scope, incident.unit_key, incident.execution_generation
            )
            if not decide_incident_opening(incident, latest):
                assert latest is not None
                return latest
            await connection.execute(
                """
                INSERT INTO belllabs_control.runtime_reconciliation_incidents (
                    incident_id, request_scope, binding_id, incident_type, severity,
                    status, identity_digest, actor_ref, reason, evidence_refs,
                    incident_payload, version, recorded_at, updated_at, retain_until,
                    unit_key
                )
                VALUES (
                    $1, $2, $3, $4, 'error', $5, $6, $7, $8, $9::jsonb, $10::jsonb, 1,
                    $11, $11, $12, $13
                )
                ON CONFLICT (request_scope, incident_type, identity_digest) DO NOTHING
                """,
                incident.incident_id,
                scope,
                incident.binding_id,
                INCIDENT_TYPE,
                incident.status,
                incident.identity_digest,
                INCIDENT_ACTOR,
                incident.reason,
                _dump([item.checkpoint_id for item in incident.candidates]),
                _dump(incident.model_dump(mode="json")),
                incident.recorded_at,
                incident.recorded_at + INCIDENT_RETENTION,
                incident.unit_key,
            )
            stored = await _incident(
                connection, scope, incident.unit_key, incident.execution_generation
            )
        assert stored is not None
        return stored

    async def get_incident(
        self, request_scope: str, unit_key: str, execution_generation: int
    ) -> UnitReconciliationIncident | None:
        async with self._pool.acquire() as connection, connection.transaction():
            await _set_scope(connection, request_scope)
            return await _incident(connection, request_scope, unit_key, execution_generation)

    async def apply_reconciliation(
        self, request_scope: str, decision: UnitReconciliationDecision
    ) -> UnitReconciliationIncident:
        async with self._pool.acquire() as connection, connection.transaction():
            await _set_scope(connection, request_scope)
            await _lock_unit(connection, request_scope, decision.unit_key)
            incident = await _incident(
                connection, request_scope, decision.unit_key, decision.execution_generation
            )
            if incident is None:
                raise CheckpointLineageConflict("no in_doubt incident exists for the decision")
            resolved = resolve_incident(incident, decision)
            if resolved is incident:
                return incident
            now = datetime.now(UTC)
            await connection.execute(
                """
                UPDATE belllabs_control.runtime_reconciliation_incidents
                SET status = 'resolved', incident_payload = $4::jsonb,
                    version = version + 1, updated_at = $5
                WHERE request_scope = $1 AND incident_type = $2 AND identity_digest = $3
                """,
                request_scope,
                INCIDENT_TYPE,
                incident.identity_digest,
                _dump(resolved.model_dump(mode="json")),
                now,
            )
            generation = await _generation(
                connection, request_scope, decision.unit_key, decision.execution_generation
            )
            if (
                decision.decision in {"abandon_unit", "start_new_generation"}
                and generation is not None
                and generation.namespace is not None
            ):
                record = await _namespace(connection, request_scope, generation.namespace)
                if record is not None:
                    released = release_in_flight(
                        record,
                        unit_key=decision.unit_key,
                        execution_generation=decision.execution_generation,
                    )
                    if released != record:
                        await _write_in_flight(connection, request_scope, released, now)
            if decision.decision == "start_new_generation" and generation is not None:
                await connection.execute(
                    """
                    UPDATE belllabs_control.runtime_unit_generations
                    SET superseded = true, updated_at = $4
                    WHERE request_scope = $1 AND unit_key = $2 AND execution_generation = $3
                    """,
                    request_scope,
                    decision.unit_key,
                    decision.execution_generation,
                    now,
                )
        return resolved

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

    async def get_namespace_in_flight(
        self, request_scope: str, namespace: str
    ) -> tuple[str, int] | None:
        async with self._pool.acquire() as connection, connection.transaction():
            await _set_scope(connection, request_scope)
            record = await _namespace(connection, request_scope, namespace, lock=False)
        if record is None or record.in_flight_unit_key is None:
            return None
        assert record.in_flight_generation is not None
        return record.in_flight_unit_key, record.in_flight_generation

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
        """Advance the fence only from the expected value (operator or test takeover seam)."""

        async with self._pool.acquire() as connection, connection.transaction():
            await _set_scope(connection, request_scope)
            await _lock_unit(connection, request_scope, unit_key)
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
    """Serialize every write of one unit with a transaction-scoped advisory lock.

    `runtime_units` stays insert-only (immutable identity), so the runtime role needs no
    UPDATE privilege on it; the advisory lock follows the journal and run-control convention.
    Concurrent recoveries of one unit therefore serialize here (REQ-CP-EXEC-014).
    """

    await connection.execute(
        "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
        f"belllabs-runtime-unit:{scope}:{unit_key}",
    )


async def _max_generation(connection: asyncpg.Connection, scope: str, unit_key: str) -> int | None:
    row = await connection.fetchrow(
        """
        SELECT execution_generation, superseded
        FROM belllabs_control.runtime_unit_generations
        WHERE request_scope = $1 AND unit_key = $2
        ORDER BY execution_generation DESC
        LIMIT 1
        """,
        scope,
        unit_key,
    )
    if row is None:
        return None
    return effective_max_generation(int(row["execution_generation"]), bool(row["superseded"]))


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
        lease_holder=row["lease_holder"],
        lease_expires_at=row["lease_expires_at"],
        superseded=row["superseded"],
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


async def _write_in_flight(
    connection: asyncpg.Connection, scope: str, record: NamespaceRecord, at: datetime
) -> None:
    await connection.execute(
        """
        UPDATE belllabs_control.runtime_cognitive_namespaces
        SET in_flight_unit_key = $3, in_flight_generation = $4, updated_at = $5
        WHERE request_scope = $1 AND cognitive_namespace = $2
        """,
        scope,
        record.namespace,
        record.in_flight_unit_key,
        record.in_flight_generation,
        at,
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


async def _result(
    connection: asyncpg.Connection, scope: str, unit_key: str, execution_generation: int
) -> UnitResultObservation | None:
    payload = await connection.fetchval(
        """
        SELECT result_payload FROM belllabs_control.runtime_unit_result_observations
        WHERE request_scope = $1 AND unit_key = $2 AND execution_generation = $3
        """,
        scope,
        unit_key,
        execution_generation,
    )
    if payload is None:
        return None
    return UnitResultObservation.model_validate(_load(payload))


async def _incident(
    connection: asyncpg.Connection, scope: str, unit_key: str, execution_generation: int
) -> UnitReconciliationIncident | None:
    payload = await connection.fetchval(
        """
        SELECT incident_payload FROM belllabs_control.runtime_reconciliation_incidents
        WHERE request_scope = $1 AND incident_type = $2 AND unit_key = $3
          AND (incident_payload->>'execution_generation')::bigint = $4
        ORDER BY COALESCE((incident_payload->>'revision')::bigint, 1) DESC
        LIMIT 1
        """,
        scope,
        INCIDENT_TYPE,
        unit_key,
        execution_generation,
    )
    if payload is None:
        return None
    return UnitReconciliationIncident.model_validate(_load(payload))


async def _insert_transition(
    connection: asyncpg.Connection, transition: CheckpointTransitionObservation
) -> None:
    scope = transition.request_scope
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


async def _record_rejection(
    connection: asyncpg.Connection, rejection: LineageWriteRejection
) -> None:
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
        rejection.request_scope,
        _rejection_id(rejection),
        rejection.unit_key,
        rejection.execution_generation,
        rejection.presented_fence,
        rejection.current_fence,
        rejection.current_generation,
        rejection.reason,
        rejection.payload_digest,
        rejection.rejected_at,
    )


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
