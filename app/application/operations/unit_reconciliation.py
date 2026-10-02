"""`reconcile_unit`: the privileged operator command for an `in_doubt` runtime unit.

REQ-CP-DA-018 / REQ-CP-RUN-007 (AMD-RRM-001, `CON-CP-LIFECYCLE-V1`). Run control accepts the
typed decision as authority (the `operator_reconciliation` wait is released and the
decision joins the run projection). The lineage repository then applies it: the incident is
resolved, `abandon_unit` and `start_new_generation` release the unit's stranded namespace
reservation, and `start_new_generation` fences the generation (REQ-CP-EXEC-005). Finally the
parked `OperationWorkflow` receives a compact wake-up hint; its next attempt reads the
accepted decision from authority and acts on it, so a lost hint is recoverable by resending
and a hint without a decision changes nothing.
"""

from __future__ import annotations

from typing import Protocol

from app.application.operations.checkpoint_lineage import CheckpointLineageRepository
from app.domain.graph_runtime.identities import QualifiedCheckpointKey
from app.domain.operation_execution.checkpoint_lineage import UnitReconciliationIncident
from app.domain.run_control.boundary_commands import receipt as boundary_receipt
from app.domain.run_control.contracts import (
    BoundaryCommandReceipt,
    BoundaryCommandStatus,
    CommandResult,
    CommandStatus,
    LifecycleCommand,
    ReceiptState,
    ReconcileUnitAction,
    RunProjection,
)

RECONCILIATION_RECORDER = "unit-reconciliation"


class UnitReconciliationRejected(ValueError):
    """The decision does not target a recorded in_doubt incident of this run."""


class ReconciliationRunControl(Protocol):
    async def execute(self, command: LifecycleCommand) -> CommandResult: ...

    async def get_run(self, request_scope: str, run_id: str) -> RunProjection: ...

    async def get_boundary_command(
        self, request_scope: str, run_id: str, idempotency_issuer: str, command_id: str
    ) -> BoundaryCommandStatus | None: ...

    async def record_boundary_receipt(
        self, request_scope: str, boundary_receipt: BoundaryCommandReceipt
    ) -> BoundaryCommandStatus: ...


class AcceptedCheckpointVerifier(Protocol):
    """Reads the registered checkpointer to verify a stamped root-namespace descendant."""

    async def is_stamped_descendant(
        self, incident: UnitReconciliationIncident, key: QualifiedCheckpointKey
    ) -> bool: ...


class UnitReconciliationNudge(Protocol):
    """Delivers a wake-up hint to the parked operation; never the authority itself."""

    async def nudge(self, *, operation_workflow_id: str, decision_id: str) -> None: ...


class UnitReconciliationService:
    def __init__(
        self,
        *,
        run_control: ReconciliationRunControl,
        lineage: CheckpointLineageRepository,
        nudge: UnitReconciliationNudge | None = None,
        verifier: AcceptedCheckpointVerifier | None = None,
    ) -> None:
        self._run_control = run_control
        self._lineage = lineage
        self._nudge = nudge
        self._verifier = verifier

    async def reconcile_unit(self, command: LifecycleCommand) -> CommandResult:
        action = command.action
        if not isinstance(action, ReconcileUnitAction):
            raise UnitReconciliationRejected("reconcile_unit requires a ReconcileUnitAction")
        incident = await self._lineage.get_incident(
            command.request_scope, action.unit_key, action.execution_generation
        )
        if (
            incident is None
            or incident.incident_id != action.incident_id
            or incident.belllabs_run_id != command.run_id
        ):
            raise UnitReconciliationRejected(
                "no recorded in_doubt incident matches this unit generation and run"
            )
        accepted = action.accepted_checkpoint
        if incident.status == "resolved":
            # An exact resend of the accepted command is the documented recovery when the
            # wake-up hint was lost: run control replays it idempotently, the lineage
            # application is idempotent, and the hint is sent again. Any other decision for
            # a resolved revision is rejected.
            run = await self._run_control.get_run(command.request_scope, command.run_id)
            recorded = next(
                (
                    item
                    for item in run.unit_reconciliations
                    if item.incident_id == action.incident_id
                    and item.unit_key == action.unit_key
                    and item.execution_generation == action.execution_generation
                ),
                None,
            )
            if (
                recorded is None
                or recorded.decision_id != command.command_id
                or incident.decision_id != command.command_id
                or recorded.decision != action.decision
                or recorded.accepted_checkpoint != accepted
            ):
                raise UnitReconciliationRejected(
                    "the incident revision is already resolved by another decision"
                )
        elif accepted is not None:
            # `CON-CP-CHECKPOINT-LINEAGE-V1`: the named key must be a stamped root-namespace
            # descendant of the source. It is checked before run control accepts anything,
            # so an invalid key can never release the operator wait.
            if incident.namespace is None or accepted.thread_id != incident.namespace:
                raise UnitReconciliationRejected(
                    "accept_descendant must name a checkpoint of the unit's own namespace"
                )
            if accepted not in incident.candidates and not (
                self._verifier is not None
                and await self._verifier.is_stamped_descendant(incident, accepted)
            ):
                raise UnitReconciliationRejected(
                    "accept_descendant must name a recorded candidate or a verified stamped "
                    "root-namespace descendant of the source"
                )
        result = await self._run_control.execute(command)
        if result.status != CommandStatus.ACCEPTED:
            return result
        run = await self._run_control.get_run(command.request_scope, command.run_id)
        decision = next(
            item
            for item in run.unit_reconciliations
            if item.unit_key == action.unit_key
            and item.execution_generation == action.execution_generation
            and item.incident_id == action.incident_id
        )
        await self._lineage.apply_reconciliation(command.request_scope, decision)
        if self._nudge is not None:
            await self._nudge.nudge(
                operation_workflow_id=incident.operation_workflow_id,
                decision_id=decision.decision_id,
            )
            # RRM-007: the hint reached the parked unit's execution; `applied` is recorded
            # by the operation boundary when it acts on the decision.
            status = await self._run_control.get_boundary_command(
                command.request_scope,
                command.run_id,
                command.idempotency_issuer,
                command.command_id,
            )
            if status is not None and status.state == ReceiptState.ACCEPTED:
                await self._run_control.record_boundary_receipt(
                    command.request_scope,
                    boundary_receipt(
                        status.command,
                        ordinal=1,
                        state=ReceiptState.DELIVERED,
                        recorded_by=RECONCILIATION_RECORDER,
                        detail="wake-up hint signalled to the parked operation",
                        transport_ref=incident.operation_workflow_id,
                        recorded_at=decision.decided_at,
                    ),
                )
        return result
