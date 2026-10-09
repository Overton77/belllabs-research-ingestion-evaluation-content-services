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
A deployment without Human Tasks composed answers `UNSUPPORTED_OPERATION`.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from fastapi import FastAPI
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from mission_control.application.human_tasks.service import HumanTaskRejected, HumanTaskService
from mission_control.application.missions.service import MissionControlService
from mission_control.application.streams.service import StreamFailure
from mission_control.contracts.contracts import (
    MissionCommandReceipt,
    MissionCommandRequest,
    MissionControlRejected,
)
from mission_control.domain.policies.errors import (
    CommandRejected,
    IdempotencyConflict,
    RunControlNotFound,
    RunVersionConflict,
)
from mission_control.domain.programs.human_gate import HumanResolutionRequest
from mission_control.interfaces.http.mission_control import (
    MissionPrincipal,
    principal_request_scope,
)


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
    return {
        "request_id": command.request_id,
        "application_id": principal.application_id,
        "outcome": receipt.admission.status.value,
        "replay": receipt.replay,
        "receipt": receipt.model_dump(mode="json"),
    }


class SocketHumanTaskResolution(BaseModel):
    """`resolve_human_task`: the HTTP resolution body plus the application and task."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    application_id: str = Field(min_length=1, max_length=63)
    human_task_id: str = Field(min_length=1, max_length=128)
    resolution: HumanResolutionRequest

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
        receipt = await service.resolve(request.human_task_id, request.resolution, principal.actor)
    except HumanTaskRejected as error:
        if error.code == "not_found":
            raise StreamFailure("TARGET_NOT_FOUND", "target not found") from None
        if error.code == "not_reviewer":
            raise StreamFailure("UNAUTHORIZED", "resolution not permitted") from None
        raise StreamFailure("COMMAND_CONFLICT", error.code) from None
    return {
        "request_id": request.request_id,
        "application_id": principal.application_id,
        "human_task_id": request.human_task_id,
        "status": receipt.status,
        "task": receipt.task.public(),
    }


__all__ = [
    "SocketCommand",
    "SocketHumanTaskResolution",
    "forward_command",
    "forward_human_task_resolution",
    "human_task_service",
    "mission_service",
    "parse_command",
    "parse_human_task_resolution",
]
