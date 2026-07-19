from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import socketio
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from app.api.control_plane import close_control_plane_resources
from app.api.control_plane import router as control_plane_router
from app.config import get_settings
from app.domain.control_plane.errors import ControlPlaneError

settings = get_settings()
sio = socketio.AsyncServer(async_mode="asgi", cors_allowed_origins=settings.cors_origins)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    # PRE-EMPTIVE SETUP: clients remain lazy so the API can boot while infrastructure is offline.
    try:
        yield
    finally:
        await close_control_plane_resources(app)


api = FastAPI(
    title="Biotech Research Ingestion Evaluation System",
    version="0.1.0",
    lifespan=lifespan,
)
api.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
api.include_router(control_plane_router)


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


@api.get("/health/live")
async def liveness() -> dict[str, str]:
    return {"status": "ok"}


@api.get("/health/ready")
async def readiness() -> dict[str, str]:
    # External checks live in app.preflight so readiness stays cheap and non-destructive.
    return {"status": "ready", "mode": "pre-emptive-bootstrap"}


@sio.event
async def connect(sid: str, _environ: dict, auth: dict | None = None) -> None:
    await sio.emit("server_ready", {"sid": sid, "authenticated": bool(auth)}, to=sid)


@sio.event
async def ping(sid: str, data: dict | None = None) -> None:
    await sio.emit("pong", data or {}, to=sid)


asgi_app = socketio.ASGIApp(sio, other_asgi_app=api, socketio_path="socket.io")
