"""Coordinator inbox over HTTP (MP-15, SPEC-04 "Coordinator callbacks").

HTTP is the simplest durable path for a coordinator that holds no live connection: register an
inbox over a mission or run, poll pending notifications after the acknowledged cursor,
acknowledge them (idempotent, the cursor only moves forward), and admit a command because of a
notification under the recursion and rate bounds. The routes call the tenant's
``CoordinatorInboxService`` exactly as the MCP inbox tools do; a database without release
1.2.0 (migration 0033) has no inbox service composed and answers ``503 inbox_unavailable``.
"""

from __future__ import annotations

from typing import Annotated, Any, Literal
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import BaseModel, ConfigDict, Field

from mission_control.application.subscriptions.coordinator import CoordinatorProfile
from mission_control.application.subscriptions.coordinator_ports import PollDelivery
from mission_control.application.subscriptions.coordinator_service import (
    CoordinatorInboxService,
    CoordinatorRejected,
    CoordinatorSubscribeRequest,
)
from mission_control.contracts.contracts import MissionCommandRequest
from mission_control.domain.subscriptions.contracts import WebhookChannel
from mission_control.interfaces.http.mission_control import (
    MissionPrincipal,
    authorize_application,
    get_mission_principal,
    principal_request_scope,
    require_strict_json_body,
)

router = APIRouter(prefix="/v1/applications/{application_id}", tags=["coordinator-inbox"])
Principal = Annotated[MissionPrincipal, Depends(get_mission_principal)]

REJECTION_STATUS: dict[str, int] = {
    "unauthorized": 403,
    "not_found": 404,
    "closed": 409,
    "recursion_bound": 409,
    "rate_limited": 429,
    "egress_rejected": 422,
    "invalid": 422,
    "unavailable": 503,
}


def get_inbox_service(
    application_id: str, request: Request, principal: Principal
) -> CoordinatorInboxService:
    authorize_application(application_id, request, principal)
    registry = getattr(request.app.state, "mission_control_coordinator_inbox_services", {})
    service = registry.get((principal.installation_id, application_id, principal.tenant_id))
    if not isinstance(service, CoordinatorInboxService):
        raise HTTPException(status_code=503, detail={"code": "inbox_unavailable"})
    if service.request_scope != principal_request_scope(principal):
        raise HTTPException(status_code=503, detail={"code": "service_scope_mismatch"})
    return service


Service = Annotated[CoordinatorInboxService, Depends(get_inbox_service)]


def _error(exc: CoordinatorRejected) -> HTTPException:
    status = REJECTION_STATUS.get(exc.code, 422)
    return HTTPException(status_code=status, detail={"code": exc.code, "message": str(exc)})


class InboxSubscribeBody(BaseModel):
    """Poll or signed-webhook delivery; live MCP-session hints are an MCP-only channel."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    target: Literal["mission", "run"]
    target_id: UUID
    delivery: Literal["poll", "webhook"] = "poll"
    webhook_url: str | None = Field(default=None, min_length=1, max_length=2_048)
    secret_ref: str | None = Field(default=None, min_length=1, max_length=256)
    profile: dict[str, Any] = Field(default_factory=dict)
    coordinator_run_ref: str | None = Field(default=None, min_length=1, max_length=256)
    after_seq: int | None = Field(default=None, ge=0)


class InboxAckBody(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    notification_ids: tuple[UUID, ...] = Field(default=(), max_length=500)
    through_inbox_seq: int | None = Field(default=None, ge=0)


@router.post(
    "/coordinator-inboxes", status_code=201, dependencies=[Depends(require_strict_json_body)]
)
async def subscribe_inbox(
    body: InboxSubscribeBody, principal: Principal, service: Service
) -> dict[str, Any]:
    try:
        if body.delivery == "webhook":
            if body.webhook_url is None or body.secret_ref is None:
                raise CoordinatorRejected("invalid", "webhook delivery needs a url and secret")
            channel: PollDelivery | WebhookChannel = WebhookChannel.model_validate(
                {"url": body.webhook_url, "secret_ref": body.secret_ref}
            )
        else:
            channel = PollDelivery()
        inbox = await service.subscribe(
            CoordinatorSubscribeRequest(
                target=body.target,
                target_id=body.target_id,
                profile=CoordinatorProfile.model_validate(body.profile),
                delivery=channel,
                coordinator_run_ref=body.coordinator_run_ref,
                after_seq=body.after_seq,
            ),
            principal.actor,
        )
    except CoordinatorRejected as exc:
        raise _error(exc) from None
    return {
        "application_id": principal.application_id,
        "subscription_id": str(inbox.subscription_id),
        "state": inbox.state.value,
        "cursor_seq": inbox.cursor_seq,
        "acked_inbox_seq": inbox.acked_inbox_seq,
        "profile": inbox.profile.model_dump(mode="json"),
        "delivery": body.delivery,
    }


@router.get("/coordinator-inboxes/{subscription_id}/notifications")
async def poll_inbox(
    subscription_id: UUID,
    principal: Principal,
    service: Service,
    after_inbox_seq: Annotated[int | None, Query(ge=0)] = None,
    limit: Annotated[int, Query(ge=1, le=200)] = 50,
) -> dict[str, Any]:
    try:
        page = await service.poll(
            subscription_id, principal.actor, after_inbox_seq=after_inbox_seq, limit=limit
        )
    except CoordinatorRejected as exc:
        raise _error(exc) from None
    return {"application_id": principal.application_id, **page.model_dump(mode="json")}


@router.post(
    "/coordinator-inboxes/{subscription_id}/acks",
    dependencies=[Depends(require_strict_json_body)],
)
async def ack_inbox(
    subscription_id: UUID, body: InboxAckBody, principal: Principal, service: Service
) -> dict[str, Any]:
    try:
        outcome = await service.ack(
            subscription_id,
            principal.actor,
            notification_ids=body.notification_ids,
            through_inbox_seq=body.through_inbox_seq,
        )
    except CoordinatorRejected as exc:
        raise _error(exc) from None
    return {
        "application_id": principal.application_id,
        "acknowledged": [str(item) for item in outcome.acknowledged],
        "already": [str(item) for item in outcome.already],
        "unknown": [str(item) for item in outcome.unknown],
        "acked_inbox_seq": outcome.acked_inbox_seq,
    }


@router.post(
    "/coordinator-inboxes/{subscription_id}/notifications/{notification_id}/commands",
    dependencies=[Depends(require_strict_json_body)],
)
async def command_from_notification(
    subscription_id: UUID,
    notification_id: UUID,
    body: MissionCommandRequest,
    principal: Principal,
    service: Service,
) -> dict[str, Any]:
    """Admit an `mc.command.v1` because of a notification (recursion and rate bounded)."""

    try:
        receipt = await service.command_from_notification(
            subscription_id, notification_id, body.target.id, body, principal.actor
        )
    except CoordinatorRejected as exc:
        raise _error(exc) from None
    return {"application_id": principal.application_id, **receipt.model_dump(mode="json")}


@router.delete("/coordinator-inboxes/{subscription_id}")
async def close_inbox(
    subscription_id: UUID, principal: Principal, service: Service
) -> dict[str, Any]:
    try:
        inbox = await service.close(subscription_id, principal.actor)
    except CoordinatorRejected as exc:
        raise _error(exc) from None
    return {
        "application_id": principal.application_id,
        "subscription_id": str(inbox.subscription_id),
        "state": inbox.state.value,
    }


__all__ = ["REJECTION_STATUS", "get_inbox_service", "router"]
