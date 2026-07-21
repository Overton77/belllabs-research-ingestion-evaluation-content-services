from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, suppress
from datetime import UTC, datetime

import asyncpg
import socketio
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from redis.asyncio import Redis
from redis.exceptions import RedisError
from supabase import AsyncClient

from app.api.control_plane import close_control_plane_resources
from app.api.control_plane import router as control_plane_router
from app.api.run_control import (
    close_run_control_resources,
    initialize_run_control_resources,
)
from app.api.run_control import router as run_control_router
from app.config import get_settings
from app.domain.control_plane.errors import ControlPlaneError
from app.domain.operation_execution.contracts import RuntimeApprovalDecision
from app.domain.run_control.errors import RunControlError
from app.integrations.openai_runtime_factory import DurableOpenAIAgentsRuntimeFactory
from app.integrations.supabase import create_supabase
from app.middleware.body_limit import BodySizeLimitMiddleware

settings = get_settings()
sio = socketio.AsyncServer(async_mode="asgi", cors_allowed_origins=settings.cors_origins)


class SupabaseRuntimeSocketAuthorizer:
    def __init__(self, server: socketio.AsyncServer, client: AsyncClient) -> None:
        self._server = server
        self._client = client

    async def authenticate(self, sid: str, auth: dict | None) -> bool:
        token = str((auth or {}).get("access_token", ""))
        if not token:
            return False
        response = await self._client.auth.get_user(token)
        if response is None:
            return False
        user = response.user
        if user is None:
            return False
        metadata = user.app_metadata or {}
        await self._server.save_session(
            sid,
            {
                "actor_id": str(user.id),
                "roles": tuple(metadata.get("roles", ())),
                "request_scopes": tuple(metadata.get("request_scopes", ())),
            },
        )
        return True

    async def __call__(
        self,
        *,
        sid: str,
        request_scope: str,
        action: str,
        **_resource: object,
    ) -> str:
        principal = await self._server.get_session(sid)
        if request_scope not in principal.get("request_scopes", ()):
            raise PermissionError("runtime resource is outside the authenticated tenant scope")
        roles = set(principal.get("roles", ()))
        allowed = {"operator", "scheduler", "auditor"} if action == "subscribe" else {"operator"}
        if not roles & allowed:
            raise PermissionError("authenticated principal lacks runtime permission")
        return str(principal["actor_id"])


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    await initialize_run_control_resources(app)
    relay_task: asyncio.Task[None] | None = None
    redis: Redis | None = None
    pool = getattr(app.state, "run_control_postgres_pool", None)
    if pool is not None:
        redis = Redis.from_url(
            settings.redis_url.get_secret_value(),
            decode_responses=True,
        )
        try:
            await redis.ping()
        except (OSError, RedisError):
            await redis.aclose()
            redis = None
            if settings.runtime_realtime_required:
                raise
        if redis is not None:
            infrastructure = DurableOpenAIAgentsRuntimeFactory(
                pool=pool,
                redis=redis,
                checkpoint_signing_key=settings.checkpoint_signing_key,
                approval_timeout_seconds=settings.runtime_approval_timeout_seconds,
            )
            app.state.runtime_redis = redis
            app.state.openai_runtime_factory = infrastructure
            app.state.runtime_approval_gateway = infrastructure.approvals
            app.state.runtime_socket_authorizer = SupabaseRuntimeSocketAuthorizer(
                sio,
                await create_supabase(settings),
            )
            relay_task = asyncio.create_task(_relay_runtime_events(redis, pool))
    try:
        yield
    finally:
        if relay_task is not None:
            relay_task.cancel()
            with suppress(asyncio.CancelledError):
                await relay_task
        if redis is not None:
            await redis.aclose()
        await close_run_control_resources(app)
        await close_control_plane_resources(app)


api = FastAPI(
    title="Biotech Research Ingestion Evaluation System",
    version="0.1.0",
    lifespan=lifespan,
)
api.add_middleware(BodySizeLimitMiddleware, max_bytes=1_000_000)
api.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
api.include_router(control_plane_router)
api.include_router(run_control_router)


@api.exception_handler(ControlPlaneError)
async def control_plane_error_handler(_request: Request, error: ControlPlaneError) -> JSONResponse:
    body: dict[str, object] = {"code": error.code, "message": error.message}
    decisions = getattr(error, "decisions", ())
    if decisions:
        body["decisions"] = [
            decision.model_dump(mode="json") if hasattr(decision, "model_dump") else decision
            for decision in decisions
        ]
    return JSONResponse(status_code=error.status_code, content=body)


@api.exception_handler(RunControlError)
async def run_control_error_handler(_request: Request, error: RunControlError) -> JSONResponse:
    return JSONResponse(
        status_code=error.status_code,
        content={"code": error.code, "message": error.message},
    )


@api.get("/health/live")
async def liveness() -> dict[str, str]:
    return {"status": "ok"}


@api.get("/health/ready")
async def readiness() -> dict[str, str]:
    # External checks live in app.preflight so readiness stays cheap and non-destructive.
    return {"status": "ready", "mode": "pre-emptive-bootstrap"}


@sio.event
async def connect(sid: str, _environ: dict, auth: dict | None = None) -> None:
    authorizer = getattr(api.state, "runtime_socket_authorizer", None)
    if authorizer is None or not await authorizer.authenticate(sid, auth):
        raise ConnectionRefusedError("runtime socket authentication failed")
    await sio.emit("server_ready", {"sid": sid, "authenticated": True}, to=sid)


@sio.event
async def ping(sid: str, data: dict | None = None) -> None:
    await sio.emit("pong", data or {}, to=sid)


@sio.event
async def subscribe_operation(sid: str, data: dict) -> None:
    scope = str(data.get("request_scope", ""))
    run_id = str(data.get("run_id", ""))
    authorizer = getattr(api.state, "runtime_socket_authorizer", None)
    if not scope or not run_id or authorizer is None:
        await sio.emit("runtime_error", {"code": "authorization_unavailable"}, to=sid)
        return
    await authorizer(sid=sid, request_scope=scope, run_id=run_id, action="subscribe")
    await sio.enter_room(sid, _runtime_room(scope, run_id))
    await sio.emit("operation_subscribed", {"run_id": run_id}, to=sid)


@sio.event
async def answer_runtime_approval(sid: str, data: dict) -> None:
    scope = str(data.get("request_scope", ""))
    binding_id = str(data.get("binding_id", ""))
    approval_id = str(data.get("approval_id", ""))
    authorizer = getattr(api.state, "runtime_socket_authorizer", None)
    gateway = getattr(api.state, "runtime_approval_gateway", None)
    if not scope or not binding_id or not approval_id or authorizer is None or gateway is None:
        await sio.emit("runtime_error", {"code": "approval_unavailable"}, to=sid)
        return
    actor_id = await authorizer(
        sid=sid,
        request_scope=scope,
        binding_id=binding_id,
        action="approve",
    )
    decision = RuntimeApprovalDecision(
        approval_id=approval_id,
        request_scope=scope,
        binding_id=binding_id,
        actor_id=actor_id,
        decision=data.get("decision"),
        reason=data.get("reason"),
        decided_at=datetime.now(UTC),
    )
    await gateway.decide(decision)
    await sio.emit(
        "runtime_approval_recorded",
        {"approval_id": approval_id, "decision": decision.decision},
        to=sid,
    )


async def _relay_runtime_events(redis: Redis, pool: asyncpg.Pool) -> None:
    pubsub = redis.pubsub()
    await pubsub.psubscribe("belllabs:runtime:*")
    try:
        async for message in pubsub.listen():
            if message["type"] not in {"message", "pmessage"}:
                continue
            payload = json.loads(message["data"])
            request_scope = str(payload.get("request_scope", ""))
            event_id = str(payload.get("event_id", ""))
            run_id = str(payload.get("run_id", ""))
            if not request_scope or not event_id or not run_id:
                continue
            async with pool.acquire() as connection, connection.transaction():
                await connection.execute(
                    "SELECT set_config('belllabs.request_scope', $1, true)",
                    request_scope,
                )
                durable = await connection.fetchval(
                    """
                    SELECT envelope
                    FROM belllabs_control.agent_runtime_events
                    WHERE request_scope = $1 AND event_id = $2
                    """,
                    request_scope,
                    event_id,
                )
            if durable is None:
                continue
            durable_payload = json.loads(durable) if isinstance(durable, str) else durable
            if durable_payload != payload:
                continue
            await sio.emit(
                "runtime_event",
                payload,
                room=_runtime_room(request_scope, run_id),
            )
    finally:
        await pubsub.aclose()


def _runtime_room(request_scope: str, run_id: str) -> str:
    return f"belllabs:runtime:{request_scope}:{run_id}"


asgi_app = socketio.ASGIApp(sio, other_asgi_app=api, socketio_path="socket.io")
