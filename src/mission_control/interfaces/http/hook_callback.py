"""Worker-only Kernel Hook callback (SPEC-07 section 5.5; FT-G3).

`POST /v1/applications/{app}/internal/hook-callback` on the worker's loopback listener (bound
to `127.0.0.1` by the worker, never mounted on the public API). The caller is
`.mission/hooks/kernel.py` inside a leased workspace; it authenticates with the task-scoped
bearer token minted for that harness execution and generation. The application in the path
must match the token's scope. Rejections are typed (`missing_task_token`,
`unknown_task_token`, `expired_task_token`, `task_token_scope_mismatch`, `STALE_GENERATION`);
the token value is never logged or echoed.
"""

from __future__ import annotations

from typing import Annotated, Any

from fastapi import APIRouter, FastAPI, Header, HTTPException, Request

from mission_control.application.execution.harness.hook_callbacks import (
    HookCallbackRejected,
    HookCallbackService,
    KernelHookCall,
)

HOOK_CALLBACK_PATH = "/v1/applications/{application_id}/internal/hook-callback"
router = APIRouter(tags=["internal"])


def _service(request: Request) -> HookCallbackService:
    service = getattr(request.app.state, "mission_control_hook_callbacks", None)
    if not isinstance(service, HookCallbackService):
        raise HTTPException(503, detail={"code": "hook_callbacks_unavailable"})
    return service


def _bearer(authorization: str | None) -> str | None:
    if authorization is None:
        return None
    scheme, _space, value = authorization.partition(" ")
    if scheme.lower() != "bearer":
        return None
    return value.strip() or None


@router.post(HOOK_CALLBACK_PATH)
async def hook_callback(
    application_id: str,
    call: KernelHookCall,
    request: Request,
    authorization: Annotated[str | None, Header()] = None,
) -> dict[str, Any]:
    service = _service(request)
    try:
        result = await service.handle(call, _bearer(authorization), application_id=application_id)
    except HookCallbackRejected as rejected:
        raise HTTPException(rejected.status_code, detail={"code": rejected.code}) from None
    return result.model_dump(mode="json", exclude_none=True)


def create_hook_callback_app(service: HookCallbackService) -> FastAPI:
    """The loopback app the worker serves; nothing else is mounted on it."""

    app = FastAPI(title="Mission Control hook callback", docs_url=None, redoc_url=None)
    app.state.mission_control_hook_callbacks = service
    app.include_router(router)
    return app


__all__ = ["HOOK_CALLBACK_PATH", "create_hook_callback_app", "router"]
