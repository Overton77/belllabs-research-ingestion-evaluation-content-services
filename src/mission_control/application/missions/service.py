"""One lifecycle facade over the existing transactional reducer and boundary relay.

The authenticated application resolver supplies request_scope and an app-bound service.
No workflow or harness is invoked directly here. Acceptance, delivery and application
remain separate persisted facts; a pause receipt never manufactures quiescence.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Protocol

from mission_control.application.execution.boundary_interventions import BoundaryInterventionService
from mission_control.application.execution.service import RunControlService
from mission_control.application.execution.stop_fence import StopFenceRepository
from mission_control.contracts.canonical import canonical_digest
from mission_control.contracts.contracts import (
    CancelPayload,
    ContinuationRequestPayload,
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
    CommandStatus,
    LifecycleCommand,
    PauseAction,
    RequestContinuationAction,
    ResumeAction,
    RunPhase,
    SatisfyWaitAction,
)
from mission_control.domain.policies.reducer import required_action_permissions
from mission_control.domain.policies.stop_fence import (
    ImmediateCancelReport,
    StopFence,
    immediate_cancel_permissions,
)


class ContinuationCommandPort(Protocol):
    """FT-B4: resolves and records ``request_continuation`` triggers."""

    async def plan(
        self, run_id: str, command_id: str, activation_id: str | None
    ) -> RequestContinuationAction:
        """The accepted command's action (lane, source session, delivery); raises
        ``MissionControlRejected`` when no continuable session exists."""
        ...

    async def record(self, run_id: str, action: RequestContinuationAction, command_id: str) -> None:
        """Record the trigger the family seals at its next safe boundary (idempotent)."""
        ...


class MissionControlService:
    def __init__(
        self,
        run_control: RunControlService,
        interventions: BoundaryInterventionService,
        *,
        request_scope: str,
        stop_fences: StopFenceRepository | None = None,
        continuations: ContinuationCommandPort | None = None,
    ) -> None:
        if not request_scope:
            raise ValueError("an authenticated application/tenant request scope is required")
        self._run_control = run_control
        self._interventions = interventions
        self._scope = request_scope
        # FT-F3: an immediate cancel is admitted only where its Stop Fence can be persisted.
        self._stop_fences = stop_fences
        # FT-B4: request_continuation is admitted only where continuation is composed.
        self._continuations = continuations

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
        action: (
            PauseAction
            | ResumeAction
            | CancelAction
            | SatisfyWaitAction
            | RequestContinuationAction
        )
        if isinstance(request.payload, ContinuationRequestPayload):
            if self._continuations is None:
                raise MissionControlRejected(
                    "unsupported_control",
                    "request_continuation requires continuation in this composition",
                )
            action = await self._continuations.plan(
                run_id, str(request.request_id), request.payload.activation_id
            )
        else:
            action = self._action(request)
        if isinstance(action, CancelAction) and action.urgency == "immediate":
            if self._stop_fences is None:
                raise MissionControlRejected(
                    "unsupported_control",
                    "immediate cancel requires a Stop Fence store in this composition",
                )
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
        if isinstance(action, CancelAction) and action.urgency == "immediate":
            await self._fence(run_id, command_id, request, actor)
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
        if (
            isinstance(action, RequestContinuationAction)
            and result.status == CommandStatus.ACCEPTED
            and self._continuations is not None
        ):
            await self._continuations.record(run_id, action, command_id)
        delivery = await self._run_control.get_boundary_command(
            self._scope, run_id, issuer, command_id
        )
        return MissionCommandReceipt(
            request_id=request.request_id,
            replay=prior is not None,
            admission=result,
            delivery=delivery,
        )

    async def _fence(
        self,
        run_id: str,
        command_id: str,
        request: MissionCommandRequest,
        actor: ActorContext,
    ) -> StopFence:
        """ADR-0008: persist the Stop Fence before the cancel reaches Temporal or a provider.

        `mission.admin` (`workflow_run.admin`) is required while side-effecting work may be
        active, `mission.command` otherwise. A replayed request finds the same fence.
        """

        assert self._stop_fences is not None
        projection = await self._run_control.get_run(self._scope, run_id)
        if not immediate_cancel_permissions(projection.phase.value).issubset(actor.permissions):
            raise MissionControlRejected(
                "unauthorized", "immediate cancel of active work requires workflow_run.admin"
            )
        generation = (
            projection.execution_target.execution_generation
            if projection.execution_target is not None
            else 1
        )
        return await self._stop_fences.persist(
            StopFence(
                request_scope=self._scope,
                run_id=run_id,
                generation=generation,
                command_id=command_id,
                reason=request.reason[:2048],
                requested_at=datetime.now(UTC),
            )
        )

    async def stop_fence_report(
        self, run_id: str, actor: ActorContext
    ) -> ImmediateCancelReport | None:
        """The immediate cancel's Delivery Report: requested, fence persisted, provider
        acknowledged and settled, as separate timestamps."""

        self._authorize_read(actor)
        if self._stop_fences is None:
            return None
        return await self._stop_fences.report(self._scope, run_id)

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
        if isinstance(payload, CancelPayload):
            return CancelAction(urgency=payload.urgency)
        raise MissionControlRejected(
            "unsupported_control",
            "control requires a qualified delivery boundary or immediate-interruption profile",
        )
