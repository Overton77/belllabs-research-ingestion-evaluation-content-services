"""Subscriptions and the mission event stream (SPEC-06 "Subscriptions").

``POST/GET/DELETE /subscriptions`` manage durable Subscriptions; ``GET
/missions/{id}/events`` is the SSE stream (replay after ``after_seq`` or ``Last-Event-ID``,
then live, heartbeats, ``resync_required`` on an expired cursor). Authentication and scope
come from the same principal as the rest of the public API.
"""

from __future__ import annotations

from typing import Annotated, Any
from uuid import UUID

from fastapi import APIRouter, Depends, Header, HTTPException, Query, Request, Response
from fastapi.responses import StreamingResponse

from mission_control.application.subscriptions.service import (
    SubscriptionRejected,
    SubscriptionService,
)
from mission_control.domain.subscriptions.contracts import Subscription, SubscriptionRequest
from mission_control.interfaces.http.mission_control import (
    MissionPrincipal,
    authorize_application,
    get_mission_principal,
    principal_request_scope,
    require_strict_json_body,
)

router = APIRouter(prefix="/v1/applications/{application_id}", tags=["subscriptions"])
Principal = Annotated[MissionPrincipal, Depends(get_mission_principal)]


def get_subscription_service(
    application_id: str, request: Request, principal: Principal
) -> SubscriptionService:
    authorize_application(application_id, request, principal)
    registry = getattr(request.app.state, "mission_control_subscription_services", {})
    service = registry.get((principal.installation_id, application_id, principal.tenant_id))
    if not isinstance(service, SubscriptionService):
        raise HTTPException(status_code=503, detail={"code": "subscriptions_unavailable"})
    if service.request_scope != principal_request_scope(principal):
        raise HTTPException(status_code=503, detail={"code": "service_scope_mismatch"})
    return service


Service = Annotated[SubscriptionService, Depends(get_subscription_service)]


def _error(exc: SubscriptionRejected) -> HTTPException:
    status = {"unauthorized": 403, "not_found": 404, "closed": 409}.get(exc.code, 422)
    return HTTPException(status_code=status, detail={"code": exc.code, "message": str(exc)})


def _scoped(principal: MissionPrincipal, subscription: Subscription) -> dict[str, Any]:
    body = subscription.model_dump(mode="json", exclude={"request_scope"})
    channel = body.get("channel", {})
    if isinstance(channel, dict) and isinstance(channel.get("secret_ref"), dict):
        ref = channel["secret_ref"]
        channel["secret_ref"] = f"{ref['provider']}:{ref['key']}"
    return {"application_id": principal.application_id, **body}


@router.post("/subscriptions", status_code=201, dependencies=[Depends(require_strict_json_body)])
async def create_subscription(
    body: SubscriptionRequest, principal: Principal, service: Service
) -> dict[str, Any]:
    try:
        subscription = await service.subscribe(body, principal.actor)
    except SubscriptionRejected as exc:
        raise _error(exc) from None
    return _scoped(principal, subscription)


@router.get("/subscriptions")
async def list_subscriptions(
    principal: Principal,
    service: Service,
    mission_id: UUID | None = None,
    run_id: UUID | None = None,
) -> dict[str, Any]:
    try:
        items = await service.list(principal.actor, mission_id=mission_id, run_id=run_id)
    except SubscriptionRejected as exc:
        raise _error(exc) from None
    return {
        "application_id": principal.application_id,
        "subscriptions": [_scoped(principal, item) for item in items],
    }


@router.delete("/subscriptions/{subscription_id}")
async def close_subscription(
    subscription_id: UUID, principal: Principal, service: Service
) -> dict[str, Any]:
    try:
        subscription = await service.close(subscription_id, principal.actor)
    except SubscriptionRejected as exc:
        raise _error(exc) from None
    return _scoped(principal, subscription)


@router.post("/subscriptions/{subscription_id}/resume")
async def resume_subscription(
    subscription_id: UUID, principal: Principal, service: Service
) -> dict[str, Any]:
    try:
        subscription = await service.resume(subscription_id, principal.actor)
    except SubscriptionRejected as exc:
        raise _error(exc) from None
    return _scoped(principal, subscription)


@router.get("/missions/{mission_id}/events", response_model=None)
async def mission_events(
    mission_id: UUID,
    principal: Principal,
    service: Service,
    after_seq: Annotated[int | None, Query(ge=0)] = None,
    types: Annotated[str | None, Query(max_length=2048)] = None,
    node_key: Annotated[str | None, Query(max_length=128)] = None,
    last_event_id: Annotated[str | None, Header(alias="Last-Event-ID")] = None,
) -> Response:
    cursor = after_seq
    if cursor is None and last_event_id is not None:
        if not last_event_id.isdigit():
            raise HTTPException(status_code=400, detail={"code": "invalid_last_event_id"})
        cursor = int(last_event_id)
    event_types = tuple(item for item in (types or "*").split(",") if item) or ("*",)
    try:
        frames = await service.stream(
            mission_id,
            principal.actor,
            after_seq=cursor or 0,
            event_types=event_types,
            node_keys=(node_key,) if node_key else (),
        )
    except SubscriptionRejected as exc:
        raise _error(exc) from None
    except ValueError:
        raise HTTPException(status_code=422, detail={"code": "invalid_filter"}) from None
    return StreamingResponse(
        frames,
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )
