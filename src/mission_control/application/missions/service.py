"""One lifecycle facade over the existing transactional reducer and boundary relay.

The authenticated application resolver supplies request_scope and an app-bound service.
No workflow or harness is invoked directly here. Acceptance, delivery and application
remain separate persisted facts; a pause receipt never manufactures quiescence.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime

from mission_control.application.execution.boundary_interventions import BoundaryInterventionService
from mission_control.application.execution.mailbox import MailboxDeliveryService
from mission_control.application.execution.service import RunControlService
from mission_control.application.execution.stop_fence import StopFenceRepository
from mission_control.contracts.canonical import canonical_digest
from mission_control.contracts.contracts import (
    AddContextPayload,
    CancelPayload,
    ContentRef,
    InlineText,
    InstructionPayload,
    MissionCommandReceipt,
    MissionCommandRequest,
    MissionControlRejected,
    MissionInspection,
    PausePayload,
    QueueInstructionPayload,
    ResumePayload,
    WaitPayload,
)
from mission_control.domain.policies.contracts import (
    ActorContext,
    AddContextAction,
    BoundaryCommandStatus,
    CancelAction,
    CommandStatus,
    LifecycleCommand,
    PauseAction,
    QueueInstructionAction,
    ResumeAction,
    RunPhase,
    SatisfyWaitAction,
)
from mission_control.domain.policies.mailbox import (
    MAX_INLINE_BYTES,
    MailboxContentTooLarge,
    check_inline_text,
    content_digest,
    inline_content_ref,
)
from mission_control.domain.policies.reducer import required_action_permissions
from mission_control.domain.policies.stop_fence import (
    ImmediateCancelReport,
    StopFence,
    immediate_cancel_permissions,
)


class MissionControlService:
    def __init__(
        self,
        run_control: RunControlService,
        interventions: BoundaryInterventionService,
        *,
        request_scope: str,
        stop_fences: StopFenceRepository | None = None,
        mailbox: MailboxDeliveryService | None = None,
        inline_cap_bytes: int = MAX_INLINE_BYTES,
    ) -> None:
        if not request_scope:
            raise ValueError("an authenticated application/tenant request scope is required")
        self._run_control = run_control
        self._interventions = interventions
        self._scope = request_scope
        # FT-F3: an immediate cancel is admitted only where its Stop Fence can be persisted.
        self._stop_fences = stop_fences
        # FT-F1: queued content is admitted only where a mailbox can deliver it.
        self._mailbox = mailbox
        self._inline_cap = inline_cap_bytes

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
        action, mailbox_text = self._action(request, inline_cap=self._inline_cap)
        if isinstance(action, QueueInstructionAction | AddContextAction) and self._mailbox is None:
            raise MissionControlRejected(
                "unsupported_control", "queued content requires a command mailbox composition"
            )
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
            frontier: dict[str, object] = {
                "version": projection.version,
                "execution_generation": generation,
                "phase": projection.phase.value,
            }
            if request.expected_generation != generation:
                raise MissionControlRejected(
                    "stale_generation", "execution generation changed", frontier=frontier
                )
            # The reducer transaction validates this version again. Checking it here
            # ensures generation was read from the exact same optimistic version.
            if request.expected_version != projection.version:
                raise MissionControlRejected(
                    "stale_version", "run version changed", frontier=frontier
                )
        if isinstance(action, CancelAction) and action.urgency == "immediate":
            await self._fence(run_id, command_id, request, actor)
        lifecycle_command = LifecycleCommand(
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
        result = (
            await self._interventions.execute(lifecycle_command, mailbox_text=mailbox_text)
            if mailbox_text is not None
            else await self._interventions.execute(lifecycle_command)
        )
        if (
            isinstance(action, CancelAction)
            and result.status == CommandStatus.ACCEPTED
            and self._mailbox is not None
        ):
            # SPEC-06: a cancel admitted before delivery supersedes every pending entry; the
            # agent never reads an instruction after a stop. Idempotent on replay.
            await self._mailbox.supersede(self._scope, run_id, superseded_by=command_id)
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
        request: MissionCommandRequest, *, inline_cap: int = MAX_INLINE_BYTES
    ) -> tuple[
        PauseAction
        | ResumeAction
        | CancelAction
        | SatisfyWaitAction
        | QueueInstructionAction
        | AddContextAction,
        str | None,
    ]:
        """The Reducer action of a public command, plus a mailbox command's inline body."""

        payload = request.payload
        if isinstance(payload, PausePayload):
            return PauseAction(**payload.model_dump(mode="python")), None
        if isinstance(payload, ResumePayload):
            return ResumeAction(**payload.model_dump(mode="python")), None
        if isinstance(payload, WaitPayload):
            return SatisfyWaitAction(**payload.model_dump(mode="python")), None
        if isinstance(payload, CancelPayload):
            return CancelAction(urgency=payload.urgency), None
        if request.kind in {"queue_instruction", "add_context"} and isinstance(
            payload, QueueInstructionPayload | AddContextPayload | InstructionPayload
        ):
            return _mailbox_action(request, payload, inline_cap=inline_cap)
        raise MissionControlRejected(
            "unsupported_control",
            "control requires a qualified delivery boundary or immediate-interruption profile",
        )


def _mailbox_action(
    request: MissionCommandRequest,
    payload: QueueInstructionPayload | AddContextPayload | InstructionPayload,
    *,
    inline_cap: int,
) -> tuple[QueueInstructionAction | AddContextAction, str | None]:
    """FT-F1: `queue_instruction` / `add_context` as a mailbox action for the expected
    Generation. Inline text is capped (typed `content_too_large`) and bound by digest."""

    text: str | None = None
    if isinstance(payload, InstructionPayload):
        content: ContentRef | InlineText = ContentRef(
            artifact_ref=payload.content_ref, content_digest=payload.content_digest
        )
    else:
        content = payload.content
    if isinstance(content, InlineText):
        try:
            size = check_inline_text(content.text, cap=inline_cap)
        except MailboxContentTooLarge as error:
            raise MissionControlRejected(error.code, str(error)) from None
        digest = content_digest(content.text)
        if content.content_digest is not None and content.content_digest != digest:
            raise MissionControlRejected(
                "content_digest_mismatch", "inline content_digest differs from the text"
            )
        text = content.text
        fields: dict[str, object] = {
            "content_ref": inline_content_ref(digest),
            "content_digest": digest,
            "media_type": content.media_type,
            "content_bytes": size,
            "inline": True,
        }
    else:
        fields = {
            "content_ref": content.artifact_ref,
            "content_digest": content.content_digest,
            "media_type": content.media_type,
            "content_bytes": content.size_bytes,
        }
    common: dict[str, object] = {
        **fields,
        "boundary": payload.boundary,
        "generation": request.expected_generation,
        "node_key": getattr(payload, "node_key", None),
        "deadline": getattr(payload, "deadline", None),
    }
    if isinstance(payload, AddContextPayload):
        return AddContextAction.model_validate({**common, "expand": payload.expand}), text
    return QueueInstructionAction.model_validate(common), text
