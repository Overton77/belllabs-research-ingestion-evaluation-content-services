"""PostgreSQL inspection reads (REQ-CP-RUN-011): one READ ONLY, scope-bound snapshot.

Every read runs in a single `READ ONLY` repeatable-read transaction with the composite
mission_control scope applied, so PostgreSQL itself refuses any write the read path might
attempt, forced RLS confines rows to the caller's installation, application and tenant,
and a run's projection, ledgers, units and journal are observed at one consistent point.
The queries select explicit columns and digest-bound payloads only: never message or
command payloads, checkpoint bodies, transcripts, or secrets.

The same queries serve the API's runtime pool (`mission_control_runtime`) and the
read-only inspection role (`mission_control_readonly`).
"""

from __future__ import annotations

import json
from collections import defaultdict
from datetime import datetime
from typing import Any

import asyncpg

from mission_control.adapters.postgres.operations.checkpoint_lineage import (
    GENERATION_COLUMNS,
    GENERATION_JOIN,
    NAMESPACE_COLUMNS,
    fetch_rejections,
    generation_from_row,
    namespace_from_row,
    rejection_from_row,
)
from mission_control.adapters.postgres.run_control import canonical as mc
from mission_control.adapters.postgres.run_control.canonical import SCOPE, scoped
from mission_control.application.execution.inspection import RunRecord, RunSnapshot, UnitRecord
from mission_control.application.execution.operations.checkpoint_lineage import (
    NamespaceRecord,
    UnitGenerationRecord,
)
from mission_control.domain.execution.checkpoint_lineage import (
    ActivityAttemptObservation,
    CheckpointTransitionObservation,
    LineageWriteRejection,
    UnitReconciliationIncident,
    UnitResultObservation,
)
from mission_control.domain.graph_runtime.identities import RuntimeUnitIdentity
from mission_control.domain.policies.contracts import (
    BoundaryCommandReceipt,
    BoundaryCommandRecord,
    BoundaryCommandStatus,
    RunPhase,
    RunProjection,
)
from mission_control.domain.policies.inspection import (
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
                args, observed_at = await _scope(connection, request_scope)
                rows = await connection.fetch(
                    f"""
                    SELECT projection, updated_at
                    FROM mission_control.mission_run
                    WHERE {SCOPE}
                      AND ($4::text IS NULL OR run_key > $4)
                      AND (cardinality($5::text[]) = 0 OR phase = ANY($5::text[]))
                    ORDER BY run_key
                    LIMIT $6
                    """,
                    *args,
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
                args, observed_at = await _scope(connection, request_scope)
                run = await mc.run_row(connection, args, run_id)
                if run is None:
                    return None
                budget = await mc.budget_state(connection, args, run_key=run_id)
                effects = await mc.effect_state(connection, args, run_id)
                units = await _units(connection, args, request_scope, run_id, unit_key)
                children = await _async_children(connection, args, run_id)
                commands = await _boundary_commands(connection, args, run["run_id"])
        return RunSnapshot(
            run=RunRecord(
                projection=RunProjection.model_validate(_load(run["projection"])),
                updated_at=run["updated_at"],
            ),
            observed_at=observed_at,
            budget=budget,
            effects=effects,
            units=units,
            async_children=children,
            boundary_commands=commands,
        )


async def _boundary_commands(
    connection: asyncpg.Connection, args: tuple[Any, ...], run_uuid: Any
) -> tuple[BoundaryCommandStatus, ...]:
    rows = await connection.fetch(
        f"""
        SELECT c.payload,
               (SELECT array_agg(r.detail ORDER BY (r.detail->>'ordinal')::integer)
                FROM mission_control.delivery_report r
                WHERE r.installation_id = c.installation_id
                  AND r.application_id = c.application_id AND r.tenant_id = c.tenant_id
                  AND r.command_id = c.command_id) AS receipts
        FROM mission_control.command c
        WHERE {scoped("c")} AND c.run_id = $4 AND c.target_kind IS NOT NULL
        ORDER BY c.sequence_space, c.target_sequence, c.created_at,
                 c.payload->'record'->>'command_id'
        """,
        *args,
        run_uuid,
    )
    statuses: list[BoundaryCommandStatus] = []
    for row in rows:
        payload = _load(row["payload"])
        statuses.append(
            BoundaryCommandStatus(
                command=BoundaryCommandRecord.model_validate(payload["record"]),
                receipts=(
                    BoundaryCommandReceipt.model_validate(payload["initial_receipt"]),
                    *(
                        BoundaryCommandReceipt.model_validate(_load(item))
                        for item in row["receipts"] or ()
                    ),
                ),
            )
        )
    return tuple(statuses)


async def _scope(
    connection: asyncpg.Connection, request_scope: str
) -> tuple[tuple[Any, ...], datetime]:
    args = await mc.begin(connection, request_scope)
    observed_at: datetime = await connection.fetchval("SELECT clock_timestamp()")
    return args, observed_at


async def _units(
    connection: asyncpg.Connection,
    args: tuple[Any, ...],
    request_scope: str,
    run_id: str,
    unit_key: str | None,
) -> tuple[UnitRecord, ...]:
    identity_rows = await connection.fetch(
        f"""
        SELECT a.activation_key AS unit_key, a.governor_projection AS identity_payload
        FROM mission_control.activation a
        JOIN mission_control.mission_run run
          ON run.installation_id = a.installation_id AND run.application_id = a.application_id
         AND run.tenant_id = a.tenant_id AND run.run_id = a.run_id
        WHERE {scoped("a")} AND run.run_key = $4
          AND a.phase LIKE 'runtime_unit:%'
          AND ($5::text IS NULL OR a.activation_key = $5)
        ORDER BY a.created_at, a.activation_key
        """,
        *args,
        run_id,
        unit_key,
    )
    if not identity_rows:
        return ()
    keys = [row["unit_key"] for row in identity_rows]
    generations: dict[str, list[UnitGenerationRecord]] = defaultdict(list)
    for row in await connection.fetch(
        f"""
        SELECT {GENERATION_COLUMNS} {GENERATION_JOIN}
        WHERE {scoped("g")} AND g.unit_key = ANY($4::text[])
        ORDER BY g.unit_key, g.execution_generation
        """,
        *args,
        keys,
    ):
        generations[row["unit_key"]].append(generation_from_row(row))
    attempts = await _payloads(
        connection,
        f"""
        SELECT unit_key, observation_payload AS payload
        FROM mission_control.activity_attempt_observation
        WHERE {SCOPE} AND unit_key = ANY($4::text[])
        ORDER BY execution_generation, activity_attempt, observed_at
        """,
        *args,
        keys,
    )
    transitions = await _payloads(
        connection,
        f"""
        SELECT unit_key, transition_payload AS payload
        FROM mission_control.checkpoint_transition
        WHERE {SCOPE} AND unit_key = ANY($4::text[])
        ORDER BY execution_generation
        """,
        *args,
        keys,
    )
    results = await _payloads(
        connection,
        f"""
        SELECT unit_key, result_payload AS payload
        FROM mission_control.unit_result_observation
        WHERE {SCOPE} AND unit_key = ANY($4::text[])
        ORDER BY execution_generation
        """,
        *args,
        keys,
    )
    incidents = await _payloads(
        connection,
        f"""
        SELECT target_ref AS unit_key, detail AS payload
        FROM mission_control.reconciliation_case
        WHERE {SCOPE} AND target_kind = $5 AND target_ref = ANY($4::text[])
        ORDER BY (detail->>'execution_generation')::bigint,
                 COALESCE((detail->>'revision')::bigint, 1)
        """,
        *args,
        keys,
        UNIT_INCIDENT_TYPE,
    )
    rejections: dict[str, list[LineageWriteRejection]] = defaultdict(list)
    for row in await fetch_rejections(connection, args, keys):
        rejections[row["unit_key"]].append(rejection_from_row(row, request_scope))
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
        f"""
        SELECT {NAMESPACE_COLUMNS} FROM mission_control.cognitive_namespace
        WHERE {SCOPE} AND namespace_key = ANY($4::text[])
        """,
        *args,
        namespace_names,
    ):
        namespaces[row["namespace_key"]] = namespace_from_row(row)
    journal = await _journal(connection, args, keys)
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
    connection: asyncpg.Connection, args: tuple[Any, ...], keys: list[str]
) -> dict[str, tuple[JournalClaimInspection, ...]]:
    claims = await connection.fetch(
        f"""
        SELECT claim_key, unit_key, semantic_binding_key, semantic_binding_digest, status,
               claimed_at
        FROM mission_control.operation_claim
        WHERE {SCOPE} AND unit_key = ANY($4::text[])
        ORDER BY claimed_at, claim_key
        """,
        *args,
        keys,
    )
    if not claims:
        return {}
    claim_ids = [row["claim_key"] for row in claims]
    attempts: dict[str, list[TechnicalAttempt]] = defaultdict(list)
    for row in await connection.fetch(
        f"""
        SELECT claim_key, technical_attempt, provider, disposition, retry_class,
               started_at, finished_at, failure_code
        FROM mission_control.operation_technical_attempt
        WHERE {SCOPE} AND claim_key = ANY($4::text[])
        ORDER BY technical_attempt
        """,
        *args,
        claim_ids,
    ):
        attempts[row["claim_key"]].append(
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
        f"""
        SELECT claim_key, settlement_key, settlement_revision, status, result_manifest_ref,
               result_manifest_digest, failure_code, usage_payload,
               pending_external_usage_payload, settled_at
        FROM mission_control.operation_settlement
        WHERE {SCOPE} AND claim_key = ANY($4::text[])
        ORDER BY settlement_revision
        """,
        *args,
        claim_ids,
    ):
        settlements[row["claim_key"]].append(
            JournalSettlementSummary(
                settlement_id=row["settlement_key"],
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
        claim_id = row["claim_key"]
        grouped[row["unit_key"]].append(
            JournalClaimInspection(
                effect_claim_id=claim_id,
                semantic_binding_id=row["semantic_binding_key"],
                semantic_binding_digest=row["semantic_binding_digest"],
                status=row["status"],
                claimed_at=row["claimed_at"],
                technical_attempts=tuple(attempts[claim_id]),
                settlements=tuple(settlements[claim_id]),
            )
        )
    return {key: tuple(items) for key, items in grouped.items()}


async def _async_children(
    connection: asyncpg.Connection, args: tuple[Any, ...], run_id: str
) -> tuple[AsyncChildInspection, ...]:
    rows = await connection.fetch(
        f"""
        SELECT subordinate_key, subordinate_id, parent_operation_key, link_key, contract_key,
               contract_digest, execution_generation, dependency_class, cancellation_requested,
               result_decision, result_manifest_digest, settlement_ref
        FROM mission_control.subordinate_admission
        WHERE {SCOPE} AND parent_run_key = $4
        ORDER BY created_at, subordinate_key
        """,
        *args,
        run_id,
    )
    if not rows:
        return ()
    lifecycle: dict[str, str] = {}
    for fact in await connection.fetch(
        f"""
        SELECT payload->>'child_execution_id' AS child_execution_id,
               payload->>'fact_ref' AS fact_ref
        FROM mission_control.native_observation
        WHERE {SCOPE} AND payload->>'fact_kind' = 'lifecycle'
          AND subordinate_id = ANY($4::uuid[])
        ORDER BY received_at, native_event_key
        """,
        *args,
        [row["subordinate_id"] for row in rows],
    ):
        # Latest recorded lifecycle fact wins; values are shown as recorded (tolerant of
        # lifecycle values introduced after this reader, such as `in_doubt`).
        lifecycle[fact["child_execution_id"]] = fact["fact_ref"]
    return tuple(
        AsyncChildInspection(
            child_execution_id=row["subordinate_key"],
            parent_operation_id=row["parent_operation_key"],
            link_id=row["link_key"],
            contract_id=row["contract_key"],
            binding_digest=row["contract_digest"],
            execution_generation=row["execution_generation"],
            dependency_class=row["dependency_class"],
            lifecycle=lifecycle.get(row["subordinate_key"]),
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
