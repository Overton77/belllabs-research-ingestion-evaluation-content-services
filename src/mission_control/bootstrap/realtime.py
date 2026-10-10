"""Public API plus the `/missions` Socket.IO namespace (SPEC-04, ADR-0036, MP-14).

`uvicorn mission_control.bootstrap.realtime:create_asgi_app --factory` serves the one FastAPI
application of `bootstrap.api.create_app`, wrapped once by `mount_mission_socketio`, so its
lifespan (pools, relays, scoped services) runs exactly once. `create_app` keeps working on
its own for a deployment that does not want the socket.

Hints only wake the pumps early; PostgreSQL stays the only replay ledger, so a missed hint
delays, never loses, an event. Two hint sources compose:

- PostgreSQL `LISTEN mc_stream_hint` (migration 0032 triggers), one listening connection per
  distinct application database credential of the deployment
  (`MISSION_SOCKET_POSTGRES_HINTS`, on by default; a database still at 0031 simply never
  notifies);
- Redis pub/sub with `MISSION_SOCKET_REDIS_FANOUT`, which also relays presence emits between
  API processes over `REDIS_URL`.
"""

from __future__ import annotations

import os
from collections.abc import AsyncIterator, Callable, Iterable, Mapping
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from typing import Any

from fastapi import FastAPI
from redis.asyncio import Redis

from mission_control.adapters.realtime.stream_hints import RedisStreamHints
from mission_control.adapters.realtime.stream_hints_postgres import (
    CompositeStreamHints,
    PostgresStreamHints,
)
from mission_control.application.streams.ports import StreamHintSource
from mission_control.bootstrap.api import (
    MissionDeployment,
    create_application,
    load_deployment,
    load_runtime_options,
)
from mission_control.bootstrap.settings import Settings, get_settings
from mission_control.interfaces.socketio.app import MissionSocketConfig, mount_mission_socketio
from mission_control.interfaces.socketio.server import MissionSocketLimits


def mission_socket_config(settings: Settings) -> MissionSocketConfig:
    """Explicit origins from `SOCKETIO_CORS_ORIGINS`; the Redis manager only with fanout."""

    return MissionSocketConfig(
        allowed_origins=tuple(settings.cors_origins),
        redis_url=(
            settings.redis_url.get_secret_value() if settings.mission_socket_redis_fanout else None
        ),
        limits=MissionSocketLimits(
            reauthorize_seconds=settings.mission_socket_reauthorize_seconds,
            command_follow_seconds=settings.mission_socket_command_follow_seconds,
        ),
    )


def postgres_hint_sources(
    secret_refs: Iterable[str], environ: Mapping[str, str] | None = None
) -> list[PostgresStreamHints]:
    """One listener per distinct configured application database (refs name env variables)."""

    environ = os.environ if environ is None else environ
    sources: list[PostgresStreamHints] = []
    seen: set[str] = set()
    for ref in secret_refs:
        dsn = environ.get(ref)
        if not dsn or dsn in seen:
            continue
        seen.add(dsn)
        sources.append(PostgresStreamHints(dsn))
    return sources


def create_asgi_app(
    settings: Settings | None = None,
    app: FastAPI | None = None,
    deployment: MissionDeployment | None = None,
) -> Any:
    """The ASGI application to serve: the public API with the `/missions` namespace."""

    settings = settings or get_settings()
    if app is None:
        deployment = deployment or load_deployment()
        app = create_application(deployment, runtime_options=load_runtime_options(deployment))
    sources: list[StreamHintSource] = []
    if settings.mission_socket_postgres_hints and deployment is not None:
        sources.extend(
            postgres_hint_sources(
                item.authentication.binding.database_secret_ref for item in deployment.applications
            )
        )
    redis: Redis | None = None
    if settings.mission_socket_redis_fanout:
        redis = Redis.from_url(settings.redis_url.get_secret_value())
        sources.append(RedisStreamHints(redis))
    hints = CompositeStreamHints(*sources) if sources else None
    asgi = mount_mission_socketio(app, mission_socket_config(settings), hints=hints)
    if redis is not None:
        # Outermost: the hint listener (inside the mount's lifespan) has stopped by the time
        # the client closes.
        app.router.lifespan_context = _closing(app.router.lifespan_context, redis)
    return asgi


def _closing(
    original: Callable[[Any], AbstractAsyncContextManager[Any]], redis: Redis
) -> Callable[[Any], AbstractAsyncContextManager[Any]]:
    @asynccontextmanager
    async def lifespan(application: Any) -> AsyncIterator[Any]:
        try:
            async with original(application) as state:
                yield state
        finally:
            await redis.aclose()

    return lifespan


__all__ = ["create_asgi_app", "mission_socket_config", "postgres_hint_sources"]
