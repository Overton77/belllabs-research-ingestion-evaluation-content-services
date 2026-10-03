"""One lifecycle facade over the existing transactional reducer and boundary relay.

The authenticated application resolver supplies request_scope and an app-bound service.
No workflow or harness is invoked directly here. Acceptance, delivery and application
remain separate persisted facts; a pause receipt never manufactures quiescence.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime

from mission_control.application.execution.boundary_interventions import BoundaryInterventionService
from mission_control.application.execution.service import RunControlService
from mission_control.contracts.canonical import canonical_digest
from mission_control.contracts.contracts import (
    CancelPayload,
    MissionCommandReceipt,
    MissionCommandRequest,
    MissionControlRejected,
    MissionInspection,
    PausePayload,
    ResumePayload,
    WaitPayload,
)
from mission_control.domain.policies.contracts import (
    ActorContext,
    BoundaryCommandStatus,
    CancelAction,
    LifecycleCommand,
    PauseAction,
    ResumeAction,
    RunPhase,
    SatisfyWaitAction,
)
from mission_control.domain.policies.reducer import required_action_permissions


class MissionControlService:
    def __init__(
        self,
        run_control: RunControlService,
        interventions: BoundaryInterventionService,
        *,
        request_scope: str,
    ) -> None:
        if not request_scope:
            raise ValueError("an authenticated application/tenant request scope is required")
        self._run_control = run_control
        self._interventions = interventions
        self._scope = request_scope

    @property
    def request_scope(self) -> str:
        return self._scope

    async def command(
        self, run_id: str, request: MissionCommandRequest, actor: ActorContext
    ) -> MissionCommandReceipt:
        # Revalidate constructed/copied Python models at the application trust boundary.
        request = MissionCommandRequest.model_validate(request.model_dump(mode="python"))
        if request.target.id != run_id:
            raise MissionControlRejected("target_mismatch", "command target differs from route run")
        action = self._action(request)
        if not required_action_permissions(action).issubset(actor.permissions):
            raise MissionControlRejected("unauthorized", "actor lacks control permission")
        # JSON tuple encoding avoids ambiguous scope delimiters. Action and actor are part
        # of request identity, while actor permissions remain validated by the reducer.
        issuer = json.dumps(["mc.command.v1", actor.actor_id, request.kind], separators=(",", ":"))
        command_id = str(request.request_id)
        prior = await self._run_control.get_command_result(self._scope, run_id, issuer, command_id)
        if prior is None:
            projection = await self._run_control.get_run(self._scope, run_id)
            generation = (
                projection.execution_target.execution_generation
                if projection.execution_target is not None
                else 1
            )
            if request.expected_generation != generation:
                raise MissionControlRejected("stale_generation", "execution generation changed")
            # The reducer transaction validates this version again. Checking it here
            # ensures generation was read from the exact same optimistic version.
            if request.expected_version != projection.version:
                raise MissionControlRejected("stale_version", "run version changed")
        result = await self._interventions.execute(
            LifecycleCommand(
                command_id=command_id,
                idempotency_issuer=issuer,
                request_scope=self._scope,
                run_id=run_id,
                expected_run_version=request.expected_version,
                actor=actor,
                action=action,
                reason=request.reason,
                # Bind all public fields (including expected generation) to the durable
                # command fingerprint so a changed-body replay cannot escape validation.
                evidence_refs=(f"mc-request:{canonical_digest(request)}",),
                occurred_at=datetime.now(UTC),
                correlation_id=command_id,
            )
        )
        delivery = await self._run_control.get_boundary_command(
            self._scope, run_id, issuer, command_id
        )
        return MissionCommandReceipt(
            request_id=request.request_id,
            replay=prior is not None,
            admission=result,
            delivery=delivery,
        )

    async def inspect(self, run_id: str, actor: ActorContext) -> MissionInspection:
        self._authorize_read(actor)
        projection = await self._run_control.get_run(self._scope, run_id)
        lifecycle = {
            RunPhase.PENDING: "pending",
            RunPhase.ACTIVE: "running",
            RunPhase.WAITING: "running",
            RunPhase.PAUSED: "paused",
            RunPhase.CANCELLING: "running",
            RunPhase.TERMINAL: "completed",
        }[projection.phase]
        return MissionInspection(
            run_id=run_id,
            version=projection.version,
            execution_generation=(
                projection.execution_target.execution_generation
                if projection.execution_target is not None
                else 1
            ),
            lifecycle=lifecycle,
            phase=projection.phase.value,
            execution_outcome=projection.terminal_outcome,
            projection=projection,
        )

    async def commands(self, run_id: str, actor: ActorContext) -> tuple[BoundaryCommandStatus, ...]:
        self._authorize_read(actor)
        return await self._interventions.list_commands(self._scope, run_id)

    @staticmethod
    def _authorize_read(actor: ActorContext) -> None:
        if "workflow_run.read" not in actor.permissions:
            raise MissionControlRejected("unauthorized", "actor lacks workflow_run.read permission")

    @staticmethod
    def _action(
        request: MissionCommandRequest,
    ) -> PauseAction | ResumeAction | CancelAction | SatisfyWaitAction:
        payload = request.payload
        if isinstance(payload, PausePayload):
            return PauseAction(**payload.model_dump(mode="python"))
        if isinstance(payload, ResumePayload):
            return ResumeAction(**payload.model_dump(mode="python"))
        if isinstance(payload, WaitPayload):
            return SatisfyWaitAction(**payload.model_dump(mode="python"))
        if isinstance(payload, CancelPayload) and payload.urgency == "normal":
            return CancelAction()
        raise MissionControlRejected(
            "unsupported_control",
            "control requires a qualified delivery boundary or immediate-interruption profile",
        )
