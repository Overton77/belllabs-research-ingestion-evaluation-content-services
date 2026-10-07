"""Runtime units and checkpoint lineage on mission_control.

A runtime unit is a canonical ``activation`` (activation key = unit key); each execution
generation is a canonical ``attempt`` whose fencing token is the claim fence and whose
lease is the claim lease. Generation detail, cognitive namespaces, activity attempt
observations, checkpoint transitions, unit results and stale-fence rejections are support
lineage records; typed ``in_doubt`` incidents are canonical ``reconciliation_case`` rows.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any
from uuid import NAMESPACE_URL, uuid5

import asyncpg

from mission_control.adapters.postgres.run_control import canonical as mc
from mission_control.adapters.postgres.run_control.canonical import SCOPE, scoped
from mission_control.application.execution.operations.checkpoint_lineage import (
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
from mission_control.contracts.identities import uuid7
from mission_control.domain.execution.checkpoint_lineage import (
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
from mission_control.domain.graph_runtime.identities import (
    QualifiedCheckpointKey,
    RuntimeUnitIdentity,
)
from mission_control.domain.policies.contracts import UnitReconciliationDecision

INCIDENT_TYPE = "runtime_unit_in_doubt"
INCIDENT_ACTOR = "mission-control-operation-runtime"

GENERATION_COLUMNS = """
    g.unit_key, g.execution_generation, a.fencing_token AS claim_fence, g.binding_key,
    g.binding_digest, g.cognitive_namespace, g.state_schema_digest, g.lease_holder,
    a.lease_expires_at, g.superseded
"""
GENERATION_JOIN = """
    FROM mission_control.runtime_unit_generation g
    JOIN mission_control.attempt a
      ON a.installation_id = g.installation_id AND a.application_id = g.application_id
     AND a.tenant_id = g.tenant_id AND a.attempt_key = g.attempt_key
"""
NAMESPACE_COLUMNS = """
    namespace_key, owner_kind, owner_digest, head_checkpoint, head_transition_key,
    head_state_schema_digest, in_flight_unit_key, in_flight_generation
"""


def attempt_key(unit_key: str, execution_generation: int) -> str:
    return f"{unit_key}:gen:{execution_generation}"


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
            args = await mc.begin(connection, scope)
            run = await mc.require_run(connection, args, unit.belllabs_run_id)
            await connection.execute(
                """
                INSERT INTO mission_control.activation (
                    installation_id, application_id, tenant_id, activation_id, run_id,
                    activation_key, revision_id, program_node_id, parent_activation_id,
                    expansion_key, repetition_ordinal, lifecycle, phase, terminal_outcome,
                    completion_decision_id, governor_projection, version, updated_at,
                    created_at, created_by_actor_ref
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, NULL, NULL, $8, $9, 'running', $10, NULL,
                        NULL, $11::jsonb, 1, $12, $12, $13)
                ON CONFLICT (installation_id, application_id, tenant_id, activation_key)
                DO NOTHING
                """,
                *args,
                uuid7(),
                run["run_id"],
                unit_key,
                run["revision_id"],
                f"{unit.family}:{unit.unit_kind}:{unit.semantic_operation_id}",
                unit.semantic_attempt - 1,
                f"runtime_unit:{unit.unit_kind}:epoch:{unit.execution_epoch}",
                _dump(unit.model_dump(mode="json")),
                observed_at,
                INCIDENT_ACTOR,
            )
            await _lock_unit(connection, scope, unit_key)
            current = await _generation(connection, args, unit_key, execution_generation)
            check_generation_admission(
                unit_key=unit_key,
                execution_generation=execution_generation,
                binding_id=binding_id,
                binding_digest=binding_digest,
                namespace=namespace,
                current=current,
                max_generation=await _max_generation(connection, args, unit_key),
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
                await _insert_generation(connection, args, run["run_id"], current, observed_at)
            granted, took_over = True, False
            if lease_expires_at is not None and dispatching:
                lease = decide_lease(
                    current, holder=holder, requested_until=lease_expires_at, now=observed_at
                )
                granted, took_over, dispatching = lease.granted, lease.took_over, lease.granted
                if lease.granted:
                    current = lease.record
                    await connection.execute(
                        f"""
                        UPDATE mission_control.attempt
                        SET fencing_token = $5, lease_expires_at = $6, version = version + 1,
                            updated_at = $7
                        WHERE {SCOPE} AND attempt_key = $4
                        """,
                        *args,
                        attempt_key(unit_key, execution_generation),
                        current.claim_fence,
                        current.lease_expires_at,
                        observed_at,
                    )
                    await connection.execute(
                        f"""
                        UPDATE mission_control.runtime_unit_generation
                        SET lease_holder = $6, updated_at = $7
                        WHERE {SCOPE} AND unit_key = $4 AND execution_generation = $5
                        """,
                        *args,
                        unit_key,
                        execution_generation,
                        current.lease_holder,
                        observed_at,
                    )
            existing = await _transition(connection, args, unit_key, execution_generation)
            expected_source: QualifiedCheckpointKey | None = None
            if namespace is not None:
                await connection.execute(
                    """
                    INSERT INTO mission_control.cognitive_namespace (
                        installation_id, application_id, tenant_id, cognitive_namespace_id,
                        namespace_key, owner_kind, owner_digest, head_version, updated_at,
                        created_at, created_by_actor_ref
                    )
                    VALUES ($1, $2, $3, $4, $5, $6, $7, 0, $8, $8, $9)
                    ON CONFLICT (installation_id, application_id, tenant_id, namespace_key)
                    DO NOTHING
                    """,
                    *args,
                    uuid7(),
                    namespace.namespace,
                    namespace.owner_kind,
                    namespace.owner_digest,
                    observed_at,
                    INCIDENT_ACTOR,
                )
                record = await _namespace(connection, args, namespace.namespace)
                assert record is not None
                check_namespace_owner(namespace, record)
                result = await _result(connection, args, unit_key, execution_generation)
                # A unit generation whose result is already fixed never re-reserves its
                # namespace: it only settles the recorded manifest (`observed_unsettled`).
                if dispatching and existing is None and result is None:
                    reserved = reserve_in_flight(
                        record, unit_key=unit_key, execution_generation=execution_generation
                    )
                    if reserved != record:
                        await _write_in_flight(connection, args, reserved, observed_at)
                expected_source = existing.source_key if existing is not None else record.head
            prior_dispatch = bool(
                await connection.fetchval(
                    f"""
                    SELECT EXISTS (
                        SELECT 1 FROM mission_control.activity_attempt_observation
                        WHERE {SCOPE} AND unit_key = $4 AND execution_generation = $5
                          AND dispatching AND observation_key <> $6
                    )
                    """,
                    *args,
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
                INSERT INTO mission_control.activity_attempt_observation (
                    installation_id, application_id, tenant_id, activity_attempt_observation_id,
                    observation_key, schema_version, unit_key, execution_generation, claim_fence,
                    temporal_workflow_id, temporal_run_id, temporal_activity_id,
                    activity_attempt, worker_identity, binding_key, cognitive_namespace,
                    expected_source_checkpoint, dispatching, observation_payload, observed_at,
                    created_at, created_by_actor_ref
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16,
                        $17::jsonb, $18, $19::jsonb, $20, $20, $14)
                ON CONFLICT (installation_id, application_id, tenant_id, observation_key)
                DO NOTHING
                """,
                *args,
                uuid7(),
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
                f"""
                SELECT observation_payload FROM mission_control.activity_attempt_observation
                WHERE {SCOPE} AND observation_key = $4
                """,
                *args,
                observation.observation_id,
            )
            result = await _result(connection, args, unit_key, execution_generation)
            incident = await _incident(connection, args, unit_key, execution_generation)
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
            args = await mc.begin(connection, scope)
            await _lock_unit(connection, scope, transition.unit_key)
            generation = await _generation(
                connection, args, transition.unit_key, transition.execution_generation
            )
            namespace = await _namespace(connection, args, transition.namespace)
            existing = await _transition(
                connection, args, transition.unit_key, transition.execution_generation
            )
            decision = decide_transition(
                transition,
                generation=generation,
                max_generation=await _max_generation(connection, args, transition.unit_key),
                namespace=namespace,
                existing=existing,
                rejected_at=datetime.now(UTC),
            )
            if isinstance(decision, LineageWriteRejection):
                rejection = decision
                await _record_rejection(connection, args, decision)
            elif decision == "duplicate":
                assert existing is not None
                return existing
            else:
                await _insert_transition(connection, args, transition)
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
            args = await mc.begin(connection, scope)
            await _lock_unit(connection, scope, result.unit_key)
            generation = await _generation(
                connection, args, result.unit_key, result.execution_generation
            )
            max_generation = await _max_generation(connection, args, result.unit_key)
            existing = await _result(connection, args, result.unit_key, result.execution_generation)
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
                await _record_rejection(connection, args, decision)
            elif decision == "duplicate":
                assert existing is not None
                return existing
            else:
                assert generation is not None
                if transition is not None:
                    require_linked_result(result, transition)
                    namespace = await _namespace(connection, args, transition.namespace)
                    transition_decision = decide_transition(
                        transition,
                        generation=generation,
                        max_generation=max_generation,
                        namespace=namespace,
                        existing=await _transition(
                            connection,
                            args,
                            transition.unit_key,
                            transition.execution_generation,
                        ),
                        rejected_at=now,
                    )
                    if isinstance(transition_decision, LineageWriteRejection):
                        rejection = transition_decision
                        await _record_rejection(connection, args, transition_decision)
                    elif transition_decision == "accept":
                        await _insert_transition(connection, args, transition)
                elif generation.namespace is not None:
                    record = await _namespace(connection, args, generation.namespace)
                    if record is not None:
                        released = release_in_flight(
                            record,
                            unit_key=result.unit_key,
                            execution_generation=result.execution_generation,
                        )
                        if released != record:
                            await _write_in_flight(connection, args, released, now)
                if rejection is None:
                    await connection.execute(
                        """
                        INSERT INTO mission_control.unit_result_observation (
                            installation_id, application_id, tenant_id,
                            unit_result_observation_id, observation_key, schema_version,
                            unit_key, execution_generation, claim_fence, binding_key,
                            settlement_key, status, result_manifest_ref, result_manifest_digest,
                            result_manifest_size_bytes, checkpoint_transition_key,
                            content_digest, result_payload, observed_at, created_at,
                            created_by_actor_ref
                        )
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14,
                                $15, $16, $17, $18::jsonb, $19, $19, $20)
                        """,
                        *args,
                        uuid7(),
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
                        INCIDENT_ACTOR,
                    )
        if rejection is not None:
            raise stale_claim_error(rejection)
        return result

    async def get_result(
        self, request_scope: str, unit_key: str, execution_generation: int
    ) -> UnitResultObservation | None:
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, request_scope)
            return await _result(connection, args, unit_key, execution_generation)

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
            args = await mc.begin(connection, request_scope)
            await _lock_unit(connection, request_scope, unit_key)
            await connection.execute(
                f"""
                UPDATE mission_control.attempt AS a
                SET lease_expires_at = $7, version = a.version + 1, updated_at = $7
                FROM mission_control.runtime_unit_generation AS g
                WHERE {scoped("a")} AND {scoped("g")} AND g.attempt_key = a.attempt_key
                  AND g.unit_key = $4 AND g.execution_generation = $5 AND g.lease_holder = $6
                """,
                *args,
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
            args = await mc.begin(connection, scope)
            await _lock_unit(connection, scope, incident.unit_key)
            latest = await _incident(
                connection, args, incident.unit_key, incident.execution_generation
            )
            if not decide_incident_opening(incident, latest):
                assert latest is not None
                return latest
            await insert_incident(
                connection,
                args,
                incident_type=INCIDENT_TYPE,
                identity_digest=incident.identity_digest,
                target_ref=incident.unit_key,
                reason=incident.reason,
                status=incident.status,
                payload=incident.model_dump(mode="json"),
                recorded_at=incident.recorded_at,
                actor_ref=INCIDENT_ACTOR,
            )
            stored = await _incident(
                connection, args, incident.unit_key, incident.execution_generation
            )
        assert stored is not None
        return stored

    async def get_incident(
        self, request_scope: str, unit_key: str, execution_generation: int
    ) -> UnitReconciliationIncident | None:
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, request_scope)
            return await _incident(connection, args, unit_key, execution_generation)

    async def apply_reconciliation(
        self, request_scope: str, decision: UnitReconciliationDecision
    ) -> UnitReconciliationIncident:
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, request_scope)
            await _lock_unit(connection, request_scope, decision.unit_key)
            incident = await _incident(
                connection, args, decision.unit_key, decision.execution_generation
            )
            if incident is None:
                raise CheckpointLineageConflict("no in_doubt incident exists for the decision")
            resolved = resolve_incident(incident, decision)
            if resolved is incident:
                return incident
            now = datetime.now(UTC)
            await resolve_incident_row(
                connection,
                args,
                incident_type=INCIDENT_TYPE,
                identity_digest=incident.identity_digest,
                payload=resolved.model_dump(mode="json"),
                updated_at=now,
            )
            generation = await _generation(
                connection, args, decision.unit_key, decision.execution_generation
            )
            if (
                decision.decision in {"abandon_unit", "start_new_generation"}
                and generation is not None
                and generation.namespace is not None
            ):
                record = await _namespace(connection, args, generation.namespace)
                if record is not None:
                    released = release_in_flight(
                        record,
                        unit_key=decision.unit_key,
                        execution_generation=decision.execution_generation,
                    )
                    if released != record:
                        await _write_in_flight(connection, args, released, now)
            if decision.decision == "start_new_generation" and generation is not None:
                await connection.execute(
                    f"""
                    UPDATE mission_control.runtime_unit_generation
                    SET superseded = true, updated_at = $6
                    WHERE {SCOPE} AND unit_key = $4 AND execution_generation = $5
                    """,
                    *args,
                    decision.unit_key,
                    decision.execution_generation,
                    now,
                )
        return resolved

    async def get_transition(
        self, request_scope: str, unit_key: str, execution_generation: int
    ) -> CheckpointTransitionObservation | None:
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, request_scope)
            return await _transition(connection, args, unit_key, execution_generation)

    async def get_namespace_head(
        self, request_scope: str, namespace: str
    ) -> QualifiedCheckpointKey | None:
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, request_scope)
            record = await _namespace(connection, args, namespace, lock=False)
        return record.head if record is not None else None

    async def get_namespace_in_flight(
        self, request_scope: str, namespace: str
    ) -> tuple[str, int] | None:
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, request_scope)
            record = await _namespace(connection, args, namespace, lock=False)
        if record is None or record.in_flight_unit_key is None:
            return None
        assert record.in_flight_generation is not None
        return record.in_flight_unit_key, record.in_flight_generation

    async def list_attempts(
        self, request_scope: str, unit_key: str
    ) -> tuple[ActivityAttemptObservation, ...]:
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, request_scope)
            rows = await connection.fetch(
                f"""
                SELECT observation_payload FROM mission_control.activity_attempt_observation
                WHERE {SCOPE} AND unit_key = $4
                ORDER BY execution_generation, activity_attempt, observed_at
                """,
                *args,
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
            args = await mc.begin(connection, request_scope)
            rows = await connection.fetch(
                f"""
                SELECT transition_payload FROM mission_control.checkpoint_transition
                WHERE {SCOPE} AND namespace_key = $4
                """,
                *args,
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
            args = await mc.begin(connection, request_scope)
            rows = await fetch_rejections(connection, args, [unit_key])
        return tuple(rejection_from_row(row, request_scope) for row in rows)

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
            args = await mc.begin(connection, request_scope)
            await _lock_unit(connection, request_scope, unit_key)
            advanced = await connection.fetchval(
                f"""
                UPDATE mission_control.attempt
                SET fencing_token = fencing_token + 1, version = version + 1,
                    updated_at = clock_timestamp()
                WHERE {SCOPE} AND attempt_key = $4 AND fencing_token = $5
                RETURNING fencing_token
                """,
                *args,
                attempt_key(unit_key, execution_generation),
                expected_fence,
            )
        if advanced is None:
            raise CheckpointLineageConflict("claim fence changed concurrently")
        return int(advanced)


async def _lock_unit(connection: asyncpg.Connection, scope: str, unit_key: str) -> None:
    """Serialize every write of one unit with a transaction-scoped advisory lock.

    The activation stays an insert-only identity, so the runtime role needs no UPDATE
    privilege on it; concurrent recoveries of one unit serialize here (REQ-CP-EXEC-014).
    """

    await mc.advisory_lock(connection, f"mission-control-runtime-unit:{scope}:{unit_key}")


async def _insert_generation(
    connection: asyncpg.Connection,
    args: tuple[Any, ...],
    run_id: Any,
    record: UnitGenerationRecord,
    observed_at: datetime,
) -> None:
    activation_id = await connection.fetchval(
        "SELECT activation_id FROM mission_control.activation "
        f"WHERE {SCOPE} AND activation_key = $4",
        *args,
        record.unit_key,
    )
    key = attempt_key(record.unit_key, record.execution_generation)
    await connection.execute(
        """
        INSERT INTO mission_control.attempt (
            installation_id, application_id, tenant_id, attempt_id, run_id, activation_id,
            attempt_key, attempt_no, execution_generation, execution_outcome, failure_class,
            binding_ref, binding_digest, lease_expires_at, fencing_token, started_at, ended_at,
            version, updated_at, created_at, created_by_actor_ref
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $8, NULL, NULL, $9, $10, NULL, $11, $12, NULL,
                1, $12, $12, $13)
        """,
        *args,
        uuid7(),
        run_id,
        activation_id,
        key,
        record.execution_generation,
        record.binding_id,
        record.binding_digest,
        record.claim_fence,
        observed_at,
        INCIDENT_ACTOR,
    )
    await connection.execute(
        """
        INSERT INTO mission_control.runtime_unit_generation (
            installation_id, application_id, tenant_id, runtime_unit_generation_id, attempt_key,
            unit_key, execution_generation, binding_key, binding_digest, cognitive_namespace,
            state_schema_digest, lease_holder, superseded, recorded_at, updated_at, created_at,
            created_by_actor_ref
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, NULL, false, $12, $12, $12, $13)
        """,
        *args,
        uuid7(),
        key,
        record.unit_key,
        record.execution_generation,
        record.binding_id,
        record.binding_digest,
        record.namespace,
        record.state_schema_digest,
        observed_at,
        INCIDENT_ACTOR,
    )


async def _max_generation(
    connection: asyncpg.Connection, args: tuple[Any, ...], unit_key: str
) -> int | None:
    row = await connection.fetchrow(
        f"""
        SELECT execution_generation, superseded FROM mission_control.runtime_unit_generation
        WHERE {SCOPE} AND unit_key = $4
        ORDER BY execution_generation DESC
        LIMIT 1
        """,
        *args,
        unit_key,
    )
    if row is None:
        return None
    return effective_max_generation(int(row["execution_generation"]), bool(row["superseded"]))


def generation_from_row(row: asyncpg.Record) -> UnitGenerationRecord:
    return UnitGenerationRecord(
        unit_key=row["unit_key"],
        execution_generation=row["execution_generation"],
        claim_fence=row["claim_fence"],
        binding_id=row["binding_key"],
        binding_digest=row["binding_digest"],
        namespace=row["cognitive_namespace"],
        state_schema_digest=row["state_schema_digest"],
        lease_holder=row["lease_holder"],
        lease_expires_at=row["lease_expires_at"],
        superseded=row["superseded"],
    )


async def _generation(
    connection: asyncpg.Connection,
    args: tuple[Any, ...],
    unit_key: str,
    execution_generation: int,
) -> UnitGenerationRecord | None:
    row = await connection.fetchrow(
        f"""
        SELECT {GENERATION_COLUMNS} {GENERATION_JOIN}
        WHERE {scoped("g")} AND g.unit_key = $4 AND g.execution_generation = $5
        """,
        *args,
        unit_key,
        execution_generation,
    )
    return generation_from_row(row) if row is not None else None


def namespace_from_row(row: asyncpg.Record) -> NamespaceRecord:
    head = row["head_checkpoint"]
    return NamespaceRecord(
        namespace=row["namespace_key"],
        owner_kind=row["owner_kind"],
        owner_digest=row["owner_digest"],
        head=QualifiedCheckpointKey.model_validate(_load(head)) if head is not None else None,
        head_transition_id=row["head_transition_key"],
        head_state_schema_digest=row["head_state_schema_digest"],
        in_flight_unit_key=row["in_flight_unit_key"],
        in_flight_generation=row["in_flight_generation"],
    )


async def _namespace(
    connection: asyncpg.Connection,
    args: tuple[Any, ...],
    namespace: str,
    *,
    lock: bool = True,
) -> NamespaceRecord | None:
    row = await connection.fetchrow(
        f"""
        SELECT {NAMESPACE_COLUMNS} FROM mission_control.cognitive_namespace
        WHERE {SCOPE} AND namespace_key = $4
        """
        + (" FOR UPDATE" if lock else ""),
        *args,
        namespace,
    )
    return namespace_from_row(row) if row is not None else None


async def _write_in_flight(
    connection: asyncpg.Connection,
    args: tuple[Any, ...],
    record: NamespaceRecord,
    at: datetime,
) -> None:
    await connection.execute(
        f"""
        UPDATE mission_control.cognitive_namespace
        SET in_flight_unit_key = $5, in_flight_generation = $6, updated_at = $7
        WHERE {SCOPE} AND namespace_key = $4
        """,
        *args,
        record.namespace,
        record.in_flight_unit_key,
        record.in_flight_generation,
        at,
    )


async def _transition(
    connection: asyncpg.Connection,
    args: tuple[Any, ...],
    unit_key: str,
    execution_generation: int,
) -> CheckpointTransitionObservation | None:
    payload = await connection.fetchval(
        f"""
        SELECT transition_payload FROM mission_control.checkpoint_transition
        WHERE {SCOPE} AND unit_key = $4 AND execution_generation = $5
        """,
        *args,
        unit_key,
        execution_generation,
    )
    if payload is None:
        return None
    return CheckpointTransitionObservation.model_validate(_load(payload))


async def _result(
    connection: asyncpg.Connection,
    args: tuple[Any, ...],
    unit_key: str,
    execution_generation: int,
) -> UnitResultObservation | None:
    payload = await connection.fetchval(
        f"""
        SELECT result_payload FROM mission_control.unit_result_observation
        WHERE {SCOPE} AND unit_key = $4 AND execution_generation = $5
        """,
        *args,
        unit_key,
        execution_generation,
    )
    if payload is None:
        return None
    return UnitResultObservation.model_validate(_load(payload))


async def _incident(
    connection: asyncpg.Connection,
    args: tuple[Any, ...],
    unit_key: str,
    execution_generation: int,
) -> UnitReconciliationIncident | None:
    payload = await connection.fetchval(
        f"""
        SELECT detail FROM mission_control.reconciliation_case
        WHERE {SCOPE} AND target_kind = $4 AND target_ref = $5
          AND (detail->>'execution_generation')::bigint = $6
        ORDER BY COALESCE((detail->>'revision')::bigint, 1) DESC
        LIMIT 1
        """,
        *args,
        INCIDENT_TYPE,
        unit_key,
        execution_generation,
    )
    if payload is None:
        return None
    return UnitReconciliationIncident.model_validate(_load(payload))


def incident_case_key(incident_type: str, identity_digest: str) -> str:
    return mc.json_key(incident_type, identity_digest)


async def insert_incident(
    connection: asyncpg.Connection,
    args: tuple[Any, ...],
    *,
    incident_type: str,
    identity_digest: str,
    target_ref: str,
    reason: str,
    status: str,
    payload: dict[str, Any],
    recorded_at: datetime,
    actor_ref: str,
) -> None:
    """One typed incident as a canonical reconciliation case (unique per identity)."""

    await connection.execute(
        """
        INSERT INTO mission_control.reconciliation_case (
            installation_id, application_id, tenant_id, case_id, case_key, target_kind,
            target_ref, uncertainty_reason, desired_state_ref, observed_state_ref, lease_owner,
            lease_expires_at, next_action, outcome, detail, version, updated_at, created_at,
            created_by_actor_ref
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NULL, NULL, NULL, NULL, $9, NULL, $10::jsonb, 1,
                $11, $11, $12)
        ON CONFLICT (installation_id, application_id, tenant_id, case_key) DO NOTHING
        """,
        *args,
        uuid7(),
        incident_case_key(incident_type, identity_digest),
        incident_type,
        target_ref,
        reason,
        status,
        _dump(payload),
        recorded_at,
        actor_ref,
    )


async def resolve_incident_row(
    connection: asyncpg.Connection,
    args: tuple[Any, ...],
    *,
    incident_type: str,
    identity_digest: str,
    payload: dict[str, Any],
    updated_at: datetime,
) -> None:
    await connection.execute(
        f"""
        UPDATE mission_control.reconciliation_case
        SET next_action = 'resolved', detail = $5::jsonb, version = version + 1,
            updated_at = $6
        WHERE {SCOPE} AND case_key = $4
        """,
        *args,
        incident_case_key(incident_type, identity_digest),
        _dump(payload),
        updated_at,
    )


async def _insert_transition(
    connection: asyncpg.Connection,
    args: tuple[Any, ...],
    transition: CheckpointTransitionObservation,
) -> None:
    await connection.execute(
        """
        INSERT INTO mission_control.checkpoint_transition (
            installation_id, application_id, tenant_id, checkpoint_transition_id,
            transition_key, schema_version, unit_key, execution_generation, claim_fence,
            namespace_key, source_checkpoint_key, result_checkpoint_key,
            result_parent_checkpoint_key, ancestry_verified, binding_digest, state_schema_digest,
            classification, invocation_digest, result_manifest_ref, result_manifest_digest,
            redacted_summary_digest, content_digest, transition_payload, observed_at, created_at,
            created_by_actor_ref
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18,
                $19, $20, $21, $22, $23::jsonb, $24, $24, $25)
        """,
        *args,
        uuid7(),
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
        INCIDENT_ACTOR,
    )
    await connection.execute(
        f"""
        UPDATE mission_control.cognitive_namespace
        SET head_checkpoint = $5::jsonb, head_checkpoint_key = $6,
            head_transition_key = $7, head_state_schema_digest = $8,
            head_version = head_version + 1,
            in_flight_unit_key = NULL, in_flight_generation = NULL,
            updated_at = $9
        WHERE {SCOPE} AND namespace_key = $4
        """,
        *args,
        transition.namespace,
        _dump(transition.result_key.model_dump(mode="json")),
        transition.result_key.checkpoint_id,
        transition.transition_id,
        transition.state_schema_digest,
        transition.observed_at,
    )


async def _record_rejection(
    connection: asyncpg.Connection,
    args: tuple[Any, ...],
    rejection: LineageWriteRejection,
) -> None:
    await connection.execute(
        """
        INSERT INTO mission_control.lineage_write_rejection (
            installation_id, application_id, tenant_id, lineage_write_rejection_id,
            rejection_key, unit_key, execution_generation, presented_fence, current_fence,
            current_generation, reason, payload_digest, rejected_at, created_at,
            created_by_actor_ref
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $13, $14)
        ON CONFLICT (installation_id, application_id, tenant_id, rejection_key) DO NOTHING
        """,
        *args,
        uuid7(),
        _rejection_id(rejection),
        rejection.unit_key,
        rejection.execution_generation,
        rejection.presented_fence,
        rejection.current_fence,
        rejection.current_generation,
        rejection.reason,
        rejection.payload_digest,
        rejection.rejected_at,
        INCIDENT_ACTOR,
    )


async def fetch_rejections(
    connection: asyncpg.Connection, args: tuple[Any, ...], unit_keys: list[str]
) -> list[asyncpg.Record]:
    return list(
        await connection.fetch(
            f"""
            SELECT unit_key, execution_generation, presented_fence, current_fence,
                   current_generation, reason, payload_digest, rejected_at
            FROM mission_control.lineage_write_rejection
            WHERE {SCOPE} AND unit_key = ANY($4::text[])
            ORDER BY rejected_at, rejection_key
            """,
            *args,
            unit_keys,
        )
    )


def rejection_from_row(row: asyncpg.Record, request_scope: str) -> LineageWriteRejection:
    return LineageWriteRejection(
        request_scope=request_scope,
        unit_key=row["unit_key"],
        execution_generation=row["execution_generation"],
        presented_fence=row["presented_fence"],
        current_fence=row["current_fence"],
        current_generation=row["current_generation"],
        reason=row["reason"],
        payload_digest=row["payload_digest"],
        rejected_at=row["rejected_at"],
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
