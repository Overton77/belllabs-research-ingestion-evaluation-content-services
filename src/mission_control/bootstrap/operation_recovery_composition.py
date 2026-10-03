"""Composition of runtime-unit recovery over application PostgreSQL (RRM-004).

A deployment `WorkerActivityCompositionFactory` (RRM-009) passes `lineage` to
`OperationExecutionService(lineage=...)` together with the journaled coordinator, the
persistent `AsyncPostgresSaver`, and a deployment-stable `journal_claimed_by`; the operator
facade calls `reconciliation.reconcile_unit`. Nothing here is family-, company- or
provider-specific.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta

import asyncpg

from mission_control.adapters.postgres.operations.checkpoint_lineage import (
    PostgresCheckpointLineageRepository,
)
from mission_control.application.execution.operations.checkpoint_lineage import (
    DEFAULT_CLAIM_LEASE,
    CheckpointLineageService,
)
from mission_control.application.execution.operations.unit_reconciliation import (
    AcceptedCheckpointVerifier,
    ReconciliationRunControl,
    UnitReconciliationNudge,
    UnitReconciliationService,
)


@dataclass(frozen=True)
class OperationRecoveryComposition:
    lineage: CheckpointLineageService
    reconciliation: UnitReconciliationService


def compose_postgres_operation_recovery(
    pool: asyncpg.Pool,
    *,
    run_control: ReconciliationRunControl,
    nudge: UnitReconciliationNudge | None = None,
    verifier: AcceptedCheckpointVerifier | None = None,
    clock: Callable[[], datetime] | None = None,
    default_lease: timedelta = DEFAULT_CLAIM_LEASE,
) -> OperationRecoveryComposition:
    repository = PostgresCheckpointLineageRepository(pool)
    return OperationRecoveryComposition(
        lineage=CheckpointLineageService(repository, clock=clock, default_lease=default_lease),
        reconciliation=UnitReconciliationService(
            run_control=run_control, lineage=repository, nudge=nudge, verifier=verifier
        ),
    )
