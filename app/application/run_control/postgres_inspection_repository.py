"""PostgreSQL inspection reads (REQ-CP-RUN-011): one READ ONLY, scope-bound snapshot.

Every read runs in a single `READ ONLY` transaction with `belllabs.request_scope` set, so
PostgreSQL itself refuses any write the read path might attempt, RLS confines rows to the
caller's scope, and a run's projection, ledgers, units and journal are observed at one
consistent point. The queries select explicit columns and digest-bound payloads only:
never message or command payloads, checkpoint bodies, transcripts, or secrets.

The same queries serve the API's runtime pool (`belllabs_control_runtime`) and the
read-only operations role (`belllabs_operations_readonly`, migration 0022).
"""

from __future__ import annotations

import json
from collections import defaultdict
from datetime import datetime
from typing import Any

import asyncpg

from app.application.operations.checkpoint_lineage import NamespaceRecord, UnitGenerationRecord
from app.application.run_control.inspection import RunRecord, RunSnapshot, UnitRecord
from app.domain.graph_runtime.identities import QualifiedCheckpointKey, RuntimeUnitIdentity
from app.domain.operation_execution.checkpoint_lineage import (
    ActivityAttemptObservation,
    CheckpointTransitionObservation,
    LineageWriteRejection,
    UnitReconciliationIncident,
    UnitResultObservation,
)
from app.domain.run_control.contracts import (
    BoundaryCommandReceipt,
    BoundaryCommandRecord,
    BoundaryCommandStatus,
    BudgetState,
    EffectLedgerState,
    RunPhase,
    RunProjection,
)
from app.domain.run_control.inspection import (
    AsyncChildInspection,
    JournalClaimInspection,
    JournalSettlementSummary,
    TechnicalAttempt,
)

UNIT_INCIDENT_TYPE = "runtime_unit_in_doubt"


class PostgresInspectionReadRepository:
    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    async def list_runs(
        self,
        request_scope: str,
        *,
        phases: frozenset[RunPhase],
        after_run_id: str | None,
        limit: int,
    ) -> tuple[tuple[RunRecord, ...], datetime]:
        async with self._pool.acquire() as connection:
            async with connection.transaction(readonly=True, isolation="repeatable_read"):
                observed_at = await _scope(connection, request_scope)
                rows = await connection.fetch(
                    """
                    SELECT projection, updated_at
                    FROM belllabs_control.workflow_runs
                    WHERE request_scope = $1
                      AND ($2::text IS NULL OR run_id > $2)
                      AND (cardinality($3::text[]) = 0 OR phase = ANY($3::text[]))
                    ORDER BY run_id
                    LIMIT $4
                    """,
                    request_scope,
                    after_run_id,
                    sorted(phase.value for phase in phases),
                    limit,
                )
        return (
            tuple(
                RunRecord(
                    projection=RunProjection.model_validate(_load(row["projection"])),
                    updated_at=row["updated_at"],
                )
                for row in rows
            ),
            observed_at,
        )

    async def read_run(
        self, request_scope: str, run_id: str, *, unit_key: str | None = None
    ) -> RunSnapshot | None:
        async with self._pool.acquire() as connection:
            async with connection.transaction(readonly=True, isolation="repeatable_read"):
                observed_at = await _scope(connection, request_scope)
                run = await connection.fetchrow(
                    """
                    SELECT projection, updated_at FROM belllabs_control.workflow_runs
                    WHERE request_scope = $1 AND run_id = $2
                    """,
                    request_scope,
                    run_id,
                )
                if run is None:
                    return None
                budget = await connection.fetchval(
                    "SELECT state FROM belllabs_control.budget_accounts WHERE run_id = $1",
                    run_id,
                )
                effects = await connection.fetchval(
                    "SELECT state FROM belllabs_control.effect_ledgers WHERE run_id = $1",
                    run_id,
                )
                units = await _units(connection, request_scope, run_id, unit_key)
                children = await _async_children(connection, request_scope, run_id)
                commands = await _boundary_commands(connection, request_scope, run_id)
        return RunSnapshot(
            run=RunRecord(
                projection=RunProjection.model_validate(_load(run["projection"])),
                updated_at=run["updated_at"],
            ),
            observed_at=observed_at,
            budget=BudgetState.model_validate(_load(budget)) if budget is not None else None,
            effects=(
                EffectLedgerState.model_validate(_load(effects)) if effects is not None else None
            ),
            units=units,
            async_children=children,
            boundary_commands=commands,
        )


async def _boundary_commands(
    connection: asyncpg.Connection, request_scope: str, run_id: str
) -> tuple[BoundaryCommandStatus, ...]:
    rows = await connection.fetch(
        """
        SELECT c.command,
               (SELECT array_agg(r.receipt ORDER BY r.ordinal)
                FROM belllabs_control.boundary_command_receipts r
                WHERE r.request_scope = c.request_scope AND r.run_id = c.run_id
                  AND r.command_id = c.command_id) AS receipts
        FROM belllabs_control.boundary_commands c
        WHERE c.request_scope = $1 AND c.run_id = $2
        ORDER BY c.sequence_space, c.target_sequence, c.recorded_at, c.command_id
        """,
        request_scope,
        run_id,
    )
    return tuple(
        BoundaryCommandStatus(
            command=BoundaryCommandRecord.model_validate(_load(row["command"])),
            receipts=tuple(
                BoundaryCommandReceipt.model_validate(_load(item)) for item in row["receipts"]
            ),
        )
        for row in rows
        if row["receipts"]
    )


async def _scope(connection: asyncpg.Connection, request_scope: str) -> datetime:
    await connection.execute("SELECT set_config('belllabs.request_scope', $1, true)", request_scope)
    observed_at: datetime = await connection.fetchval("SELECT clock_timestamp()")
    return observed_at


async def _units(
    connection: asyncpg.Connection, scope: str, run_id: str, unit_key: str | None
) -> tuple[UnitRecord, ...]:
    identity_rows = await connection.fetch(
        """
        SELECT unit_key, identity_payload FROM belllabs_control.runtime_units
        WHERE request_scope = $1 AND belllabs_run_id = $2
          AND ($3::text IS NULL OR unit_key = $3)
        ORDER BY recorded_at, unit_key
        """,
        scope,
        run_id,
        unit_key,
    )
    if not identity_rows:
        return ()
    keys = [row["unit_key"] for row in identity_rows]
    generations: dict[str, list[UnitGenerationRecord]] = defaultdict(list)
    for row in await connection.fetch(
        """
        SELECT unit_key, execution_generation, claim_fence, binding_id, binding_digest,
               cognitive_namespace, state_schema_digest, lease_holder, lease_expires_at,
               superseded
        FROM belllabs_control.runtime_unit_generations
        WHERE request_scope = $1 AND unit_key = ANY($2::text[])
        ORDER BY unit_key, execution_generation
        """,
        scope,
        keys,
    ):
        generations[row["unit_key"]].append(
            UnitGenerationRecord(
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
        )
    attempts = await _payloads(
        connection,
        """
        SELECT unit_key, observation_payload AS payload
        FROM belllabs_control.runtime_activity_attempt_observations
        WHERE request_scope = $1 AND unit_key = ANY($2::text[])
        ORDER BY execution_generation, activity_attempt, observed_at
        """,
        scope,
        keys,
    )
    transitions = await _payloads(
        connection,
        """
        SELECT unit_key, transition_payload AS payload
        FROM belllabs_control.runtime_checkpoint_transitions
        WHERE request_scope = $1 AND unit_key = ANY($2::text[])
        ORDER BY execution_generation
        """,
        scope,
        keys,
    )
    results = await _payloads(
        connection,
        """
        SELECT unit_key, result_payload AS payload
        FROM belllabs_control.runtime_unit_result_observations
        WHERE request_scope = $1 AND unit_key = ANY($2::text[])
        ORDER BY execution_generation
        """,
        scope,
        keys,
    )
    incidents = await _payloads(
        connection,
        """
        SELECT unit_key, incident_payload AS payload
        FROM belllabs_control.runtime_reconciliation_incidents
        WHERE request_scope = $1 AND incident_type = $3 AND unit_key = ANY($2::text[])
        ORDER BY (incident_payload->>'execution_generation')::bigint,
                 COALESCE((incident_payload->>'revision')::bigint, 1)
        """,
        scope,
        keys,
        UNIT_INCIDENT_TYPE,
    )
    rejections: dict[str, list[LineageWriteRejection]] = defaultdict(list)
    for row in await connection.fetch(
        """
        SELECT request_scope, unit_key, execution_generation, presented_fence, current_fence,
               current_generation, reason, payload_digest, rejected_at
        FROM belllabs_control.runtime_lineage_write_rejections
        WHERE request_scope = $1 AND unit_key = ANY($2::text[])
        ORDER BY rejected_at, rejection_id
        """,
        scope,
        keys,
    ):
        rejections[row["unit_key"]].append(
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
        )
    namespace_names = sorted(
        {
            record.namespace
            for records in generations.values()
            for record in records
            if record.namespace is not None
        }
    )
    namespaces: dict[str, NamespaceRecord] = {}
    for row in await connection.fetch(
        """
        SELECT cognitive_namespace, owner_kind, owner_digest, head_checkpoint,
               head_transition_id, head_state_schema_digest, in_flight_unit_key,
               in_flight_generation
        FROM belllabs_control.runtime_cognitive_namespaces
        WHERE request_scope = $1 AND cognitive_namespace = ANY($2::text[])
        """,
        scope,
        namespace_names,
    ):
        head = row["head_checkpoint"]
        namespaces[row["cognitive_namespace"]] = NamespaceRecord(
            namespace=row["cognitive_namespace"],
            owner_kind=row["owner_kind"],
            owner_digest=row["owner_digest"],
            head=QualifiedCheckpointKey.model_validate(_load(head)) if head is not None else None,
            head_transition_id=row["head_transition_id"],
            head_state_schema_digest=row["head_state_schema_digest"],
            in_flight_unit_key=row["in_flight_unit_key"],
            in_flight_generation=row["in_flight_generation"],
        )
    journal = await _journal(connection, scope, keys)
    units: list[UnitRecord] = []
    for row in identity_rows:
        key = row["unit_key"]
        unit_namespaces = {
            record.namespace for record in generations[key] if record.namespace is not None
        }
        units.append(
            UnitRecord(
                identity=RuntimeUnitIdentity.model_validate(_load(row["identity_payload"])),
                generations=tuple(generations[key]),
                attempts=tuple(
                    ActivityAttemptObservation.model_validate(item) for item in attempts[key]
                ),
                transitions=tuple(
                    CheckpointTransitionObservation.model_validate(item)
                    for item in transitions[key]
                ),
                results=tuple(UnitResultObservation.model_validate(item) for item in results[key]),
                incidents=tuple(
                    UnitReconciliationIncident.model_validate(item) for item in incidents[key]
                ),
                rejections=tuple(rejections[key]),
                namespaces=tuple(
                    namespaces[name] for name in sorted(unit_namespaces) if name in namespaces
                ),
                journal=journal.get(key, ()),
            )
        )
    return tuple(units)


async def _payloads(connection: asyncpg.Connection, query: str, *args: Any) -> dict[str, list[Any]]:
    grouped: dict[str, list[Any]] = defaultdict(list)
    for row in await connection.fetch(query, *args):
        grouped[row["unit_key"]].append(_load(row["payload"]))
    return grouped


async def _journal(
    connection: asyncpg.Connection, scope: str, keys: list[str]
) -> dict[str, tuple[JournalClaimInspection, ...]]:
    claims = await connection.fetch(
        """
        SELECT effect_claim_id, unit_key, semantic_binding_id, semantic_binding_digest,
               status, claimed_at
        FROM belllabs_control.operation_effect_claims
        WHERE request_scope = $1 AND unit_key = ANY($2::text[])
        ORDER BY claimed_at, effect_claim_id
        """,
        scope,
        keys,
    )
    if not claims:
        return {}
    claim_ids = [row["effect_claim_id"] for row in claims]
    attempts: dict[str, list[TechnicalAttempt]] = defaultdict(list)
    for row in await connection.fetch(
        """
        SELECT effect_claim_id, technical_attempt, provider, disposition, retry_class,
               started_at, finished_at, failure_code
        FROM belllabs_control.operation_execution_attempts
        WHERE request_scope = $1 AND effect_claim_id = ANY($2::text[])
        ORDER BY technical_attempt
        """,
        scope,
        claim_ids,
    ):
        attempts[row["effect_claim_id"]].append(
            TechnicalAttempt(
                technical_attempt=row["technical_attempt"],
                provider=row["provider"],
                disposition=row["disposition"],
                retry_class=row["retry_class"],
                started_at=row["started_at"],
                finished_at=row["finished_at"],
                failure_code=row["failure_code"],
            )
        )
    settlements: dict[str, list[JournalSettlementSummary]] = defaultdict(list)
    for row in await connection.fetch(
        """
        SELECT effect_claim_id, settlement_id, settlement_revision, status,
               result_manifest_ref, result_manifest_digest, failure_code, usage_payload,
               pending_external_usage_payload, settled_at
        FROM belllabs_control.operation_settlements
        WHERE request_scope = $1 AND effect_claim_id = ANY($2::text[])
        ORDER BY settlement_revision
        """,
        scope,
        claim_ids,
    ):
        settlements[row["effect_claim_id"]].append(
            JournalSettlementSummary(
                settlement_id=row["settlement_id"],
                settlement_revision=row["settlement_revision"],
                status=row["status"],
                result_manifest_ref=row["result_manifest_ref"],
                result_manifest_digest=row["result_manifest_digest"],
                failure_code=row["failure_code"],
                usage=_int_map(row["usage_payload"]),
                pending_external_usage=_int_map(row["pending_external_usage_payload"]),
                settled_at=row["settled_at"],
            )
        )
    grouped: dict[str, list[JournalClaimInspection]] = defaultdict(list)
    for row in claims:
        claim_id = row["effect_claim_id"]
        grouped[row["unit_key"]].append(
            JournalClaimInspection(
                effect_claim_id=claim_id,
                semantic_binding_id=row["semantic_binding_id"],
                semantic_binding_digest=row["semantic_binding_digest"],
                status=row["status"],
                claimed_at=row["claimed_at"],
                technical_attempts=tuple(attempts[claim_id]),
                settlements=tuple(settlements[claim_id]),
            )
        )
    return {key: tuple(items) for key, items in grouped.items()}


async def _async_children(
    connection: asyncpg.Connection, scope: str, run_id: str
) -> tuple[AsyncChildInspection, ...]:
    rows = await connection.fetch(
        """
        SELECT child_execution_id, parent_operation_id, link_id, contract_id,
               contract_digest, execution_generation, dependency_class,
               cancellation_requested, result_decision, result_manifest_digest,
               settlement_ref
        FROM belllabs_control.async_subagent_authority
        WHERE request_scope = $1 AND parent_run_id = $2
        ORDER BY created_at, child_execution_id
        """,
        scope,
        run_id,
    )
    if not rows:
        return ()
    lifecycle: dict[str, str] = {}
    for fact in await connection.fetch(
        """
        SELECT child_execution_id, fact_ref
        FROM belllabs_control.async_subagent_facts
        WHERE request_scope = $1 AND fact_kind = 'lifecycle'
          AND child_execution_id = ANY($2::text[])
        ORDER BY recorded_at, fact_id
        """,
        scope,
        [row["child_execution_id"] for row in rows],
    ):
        # Latest recorded lifecycle fact wins; values are shown as recorded (tolerant of
        # lifecycle values introduced after this reader, such as `in_doubt`).
        lifecycle[fact["child_execution_id"]] = fact["fact_ref"]
    return tuple(
        AsyncChildInspection(
            child_execution_id=row["child_execution_id"],
            parent_operation_id=row["parent_operation_id"],
            link_id=row["link_id"],
            contract_id=row["contract_id"],
            binding_digest=row["contract_digest"],
            execution_generation=row["execution_generation"],
            dependency_class=row["dependency_class"],
            lifecycle=lifecycle.get(row["child_execution_id"]),
            result_decision=row["result_decision"],
            result_manifest_digest=row["result_manifest_digest"],
            settlement_ref=row["settlement_ref"],
            cancellation_requested=row["cancellation_requested"],
        )
        for row in rows
    )


def _int_map(value: Any) -> dict[str, int]:
    loaded = _load(value) or {}
    return {str(key): int(amount) for key, amount in loaded.items() if isinstance(amount, int)}


def _load(value: Any) -> Any:
    return json.loads(value) if isinstance(value, str) else value
