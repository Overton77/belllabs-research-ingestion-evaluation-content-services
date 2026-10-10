"""Admitted command gateway: socket mutations are thin parity over the HTTP handler.

`command` validates the same `mc.command.v1` body as `POST /runs/{run_id}/commands`,
resolves the tenant's `MissionControlService` from the same registry with the same scope
check, and calls `MissionControlService.command`; the reducer and mailbox decide acceptance
and the receipt reports the admission outcome, not a network acknowledgement. Nothing here
writes state or calls a provider.

`resolve_human_task` (MP-10) validates the same `HumanResolutionRequest` body as
`POST /human-tasks/{id}/resolutions` (plus `application_id` and `human_task_id`) and calls the
tenant's one `HumanTaskService.resolve`; reviewer authorization, version and packet checks are
the service's, and a retry of the same `request_id` is the stored resolution (`duplicate`).
An approval-origin body (MP-11 `ApprovalResolutionRequest`, the body of
`POST /human-tasks/{id}/approval-resolutions`: `reviewed_digest`, edited arguments, answers,
elicitation content) goes to `HumanTaskService.resolve_approval`. The two bodies are disjoint
(`reviewed_packet_digest` vs `reviewed_digest`, both closed), so the shape selects the route.
Rejections map from the HTTP statuses (`interfaces.http.human_tasks.REJECTION_STATUS`): 404 is
`TARGET_NOT_FOUND`, 403 `UNAUTHORIZED`, and every 409/422 code (including
`edited_arguments_required`, `answer_required`, `elicitation_content_invalid`,
`invalid_request`) is a non-retryable `COMMAND_CONFLICT` whose detail is the HTTP code.
A deployment without Human Tasks composed answers `UNSUPPORTED_OPERATION`.

Receipts are durable outcomes, not network acknowledgements (SPEC-04 `command_receipt`).
The first receipt is the admission (`accepted`, or the boundary ledger's current state);
`follow_command_receipts` then reads the command's own receipt ledger through the same
application handler as `GET /runs/{run_id}/commands` (`MissionControlService.commands`) and
emits each later state (`queued`, `delivered`, `observed`, `applied` or `rejected`,
`expired`, `failed`) once, in ledger order, until a final state or its bounded window ends
(`unfollowed`: the ledger keeps the rest, readable over HTTP).
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable, Mapping
from typing import Any, cast

from fastapi import FastAPI
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from mission_control.application.execution.approvals import ApprovalResolutionRequest
from mission_control.application.human_tasks.service import HumanTaskRejected, HumanTaskService
from mission_control.application.missions.service import MissionControlService
from mission_control.application.streams.service import StreamFailure
from mission_control.contracts.contracts import (
    MissionCommandReceipt,
    MissionCommandRequest,
    MissionControlRejected,
)
from mission_control.contracts.realtime import FINAL_STAGES, CommandReceiptProgress, ReceiptStage
from mission_control.domain.policies.contracts import (
    COMPLETED_RECEIPT_STATES,
    BoundaryCommandStatus,
)
from mission_control.domain.policies.errors import (
    CommandRejected,
    IdempotencyConflict,
    RunControlNotFound,
    RunVersionConflict,
)
from mission_control.domain.programs.human_gate import HumanResolutionRequest
from mission_control.interfaces.http.human_tasks import REJECTION_STATUS
from mission_control.interfaces.http.mission_control import (
    MissionPrincipal,
    principal_request_scope,
)

logger = logging.getLogger(__name__)


class SocketCommand(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    request_id: str = Field(min_length=1, max_length=512)
    application_id: str = Field(min_length=1, max_length=63)
    run_id: str = Field(min_length=1, max_length=512)
    command: MissionCommandRequest


def parse_command(data: Any) -> SocketCommand:
    try:
        return SocketCommand.model_validate(data)
    except ValidationError:
        raise StreamFailure("COMMAND_CONFLICT", "invalid command envelope") from None


def mission_service(app: FastAPI, principal: MissionPrincipal) -> MissionControlService:
    key = (principal.installation_id, principal.application_id, principal.tenant_id)
    service = getattr(app.state, "mission_control_services", {}).get(key)
    if not isinstance(service, MissionControlService):
        raise StreamFailure("TARGET_NOT_FOUND", "application unavailable", retryable=True)
    if service.request_scope != principal_request_scope(principal):
        raise StreamFailure("SCOPE_MISMATCH", "service scope differs from the caller's scope")
    return service


async def forward_command(
    app: FastAPI, principal: MissionPrincipal, command: SocketCommand
) -> dict[str, Any]:
    if command.application_id != principal.application_id:
        raise StreamFailure("SCOPE_MISMATCH", "application is not the authenticated one")
    service = mission_service(app, principal)
    try:
        receipt: MissionCommandReceipt = await service.command(
            command.run_id, command.command, principal.actor
        )
    except MissionControlRejected as error:
        if error.code == "unauthorized":
            raise StreamFailure("UNAUTHORIZED", "command not permitted") from None
        raise StreamFailure("COMMAND_CONFLICT", error.code) from None
    except RunControlNotFound:
        raise StreamFailure("TARGET_NOT_FOUND", "target not found") from None
    except (IdempotencyConflict, RunVersionConflict, CommandRejected):
        raise StreamFailure("COMMAND_CONFLICT", "request_conflict") from None
    stage, final = receipt_stage(receipt)
    return {
        "request_id": command.request_id,
        "application_id": principal.application_id,
        "outcome": receipt.admission.status.value,
        "replay": receipt.replay,
        "stage": stage,
        "final": final,
        "receipt": receipt.model_dump(mode="json"),
    }


def receipt_stage(receipt: MissionCommandReceipt) -> tuple[str, bool]:
    """The command's current durable stage and whether nothing further will be recorded."""

    if receipt.delivery is None:
        # No boundary ledger: the admission decision is the whole outcome.
        return receipt.admission.status.value, True
    state = receipt.delivery.state
    return state.value, state in COMPLETED_RECEIPT_STATES


def _matching(
    statuses: tuple[BoundaryCommandStatus, ...], command_id: str, issuer: str
) -> BoundaryCommandStatus | None:
    for status in statuses:
        if status.command.command_id == command_id and status.command.idempotency_issuer == issuer:
            return status
    return None


Emit = Callable[[dict[str, Any]], Awaitable[None]]


async def follow_command_receipts(
    app: FastAPI,
    principal: MissionPrincipal,
    command: SocketCommand,
    receipt: MissionCommandReceipt,
    emit: Emit,
    *,
    poll_interval: float,
    window: float,
) -> str:
    """Emit each later receipt state of one admitted command; returns the last stage."""

    if receipt.delivery is None:
        return receipt.admission.status.value
    command_id = receipt.delivery.command.command_id
    issuer = receipt.delivery.command.idempotency_issuer
    seen = len(receipt.delivery.receipts)
    stage = receipt.delivery.state.value

    async def send(stage: str, *, body: dict[str, Any] | None, detail: str = "") -> None:
        progress = CommandReceiptProgress(
            request_id=command.request_id,
            application_id=principal.application_id,
            run_id=command.run_id,
            command_id=command_id,
            stage=cast(ReceiptStage, stage),
            final=stage in FINAL_STAGES,
            receipt=body,
            detail=detail,
        )
        await emit(progress.model_dump(mode="json"))

    loop = asyncio.get_running_loop()
    deadline = loop.time() + window
    while stage not in FINAL_STAGES:
        remaining = deadline - loop.time()
        if remaining <= 0:
            await send(
                "unfollowed",
                body=None,
                detail="receipt window elapsed; the ledger is readable at GET /runs/{run}/commands",
            )
            return "unfollowed"
        await asyncio.sleep(min(poll_interval, remaining))
        try:
            statuses = await mission_service(app, principal).commands(
                command.run_id, principal.actor
            )
        except (StreamFailure, MissionControlRejected, RunControlNotFound):
            return stage
        status = _matching(statuses, command_id, issuer)
        if status is None:
            continue
        for item in status.receipts[seen:]:
            stage = item.state.value
            await send(stage, body=item.model_dump(mode="json"))
        seen = max(seen, len(status.receipts))
        if status.state in COMPLETED_RECEIPT_STATES:
            return status.state.value
    return stage


class SocketHumanTaskResolution(BaseModel):
    """`resolve_human_task`: the HTTP resolution body plus the application and task."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    application_id: str = Field(min_length=1, max_length=63)
    human_task_id: str = Field(min_length=1, max_length=128)
    resolution: HumanResolutionRequest | ApprovalResolutionRequest

    @property
    def request_id(self) -> str:
        return self.resolution.request_id


def parse_human_task_resolution(data: Any) -> SocketHumanTaskResolution:
    """The flat socket body: `application_id`, `human_task_id` and the HTTP body's fields."""

    if not isinstance(data, Mapping):
        raise StreamFailure("COMMAND_CONFLICT", "invalid human task resolution envelope")
    body = dict(data)
    envelope = {key: body.pop(key) for key in ("application_id", "human_task_id") if key in body}
    try:
        return SocketHumanTaskResolution.model_validate({**envelope, "resolution": body})
    except ValidationError:
        raise StreamFailure("COMMAND_CONFLICT", "invalid human task resolution envelope") from None


def human_task_service(app: FastAPI, principal: MissionPrincipal) -> HumanTaskService:
    key = (principal.installation_id, principal.application_id, principal.tenant_id)
    service = getattr(app.state, "mission_control_human_task_services", {}).get(key)
    if not isinstance(service, HumanTaskService):
        raise StreamFailure(
            "UNSUPPORTED_OPERATION", "human tasks are not composed for this application"
        )
    if service.request_scope != principal_request_scope(principal):
        raise StreamFailure("SCOPE_MISMATCH", "service scope differs from the caller's scope")
    return service


async def forward_human_task_resolution(
    app: FastAPI, principal: MissionPrincipal, request: SocketHumanTaskResolution
) -> dict[str, Any]:
    if request.application_id != principal.application_id:
        raise StreamFailure("SCOPE_MISMATCH", "application is not the authenticated one")
    service = human_task_service(app, principal)
    try:
        if isinstance(request.resolution, ApprovalResolutionRequest):
            receipt = await service.resolve_approval(
                request.human_task_id, request.resolution, principal.actor
            )
        else:
            receipt = await service.resolve(
                request.human_task_id, request.resolution, principal.actor
            )
    except HumanTaskRejected as error:
        raise resolution_failure(error.code) from None
    return {
        "request_id": request.request_id,
        "application_id": principal.application_id,
        "human_task_id": request.human_task_id,
        "status": receipt.status,
        "task": receipt.task.public(),
    }


def resolution_failure(code: str) -> StreamFailure:
    """A Human Task rejection in the socket vocabulary, consistent with the HTTP status."""

    status = REJECTION_STATUS.get(code, 422)
    if status == 404:
        return StreamFailure("TARGET_NOT_FOUND", "target not found")
    if status == 403:
        return StreamFailure("UNAUTHORIZED", "resolution not permitted")
    # 409 (stale or settled task) and 422 (invalid resolution body) are not retryable as
    # sent; the detail is the HTTP rejection code so clients branch the same way.
    return StreamFailure("COMMAND_CONFLICT", code)


__all__ = [
    "SocketCommand",
    "SocketHumanTaskResolution",
    "follow_command_receipts",
    "forward_command",
    "forward_human_task_resolution",
    "human_task_service",
    "mission_service",
    "parse_command",
    "parse_human_task_resolution",
    "receipt_stage",
    "resolution_failure",
]
