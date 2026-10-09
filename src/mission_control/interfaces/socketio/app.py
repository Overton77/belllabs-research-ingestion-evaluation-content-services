"""Compose the `/missions` Socket.IO server around the public FastAPI application.

`mount_mission_socketio(app, config)` returns `socketio.ASGIApp(server, other_asgi_app=app)`
and serves it in place of `app`. Lifecycle is preserved and runs once:

- engine.io's ASGI driver hands the ASGI `lifespan` scope to `other_asgi_app` whenever
  neither `on_startup` nor `on_shutdown` is given (python-engineio 4.13
  `async_drivers/asgi.py`, `ASGIApp.lifespan`; python-socketio docs "Create Socket.IO ASGI
  app"), so FastAPI's own lifespan, with its pools, relays and any MCP sub-lifespan it
  enters, runs exactly once and is not duplicated by a second app object;
- the stream hub (hint listener, pumps) is entered inside FastAPI's lifespan context, after
  the services exist and before they close: on shutdown every pump is cancelled and every
  socket disconnected before pools close.

Multi-process fanout is optional: with `redis_url`, `socketio.AsyncRedisManager` relays
room emits (presence) between API processes (python-socketio docs "Using a Message Queue",
`AsyncServer(client_manager=AsyncRedisManager(url))`); with a `hints` source, commits
announced on Redis wake pumps early. Neither is ever the replay ledger: pumps always read
PostgreSQL, so a process that misses a message still delivers on its next poll.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator, Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from dataclasses import dataclass, field
from typing import Any

import socketio
from fastapi import FastAPI

from mission_control.application.streams.hub import StreamWakeups
from mission_control.application.streams.ports import StreamHintSource
from mission_control.interfaces.socketio.auth import PrincipalResolver, resolver_from_app
from mission_control.interfaces.socketio.server import (
    STREAM_SERVICES,
    MissionNamespace,
    MissionSocketLimits,
)

logger = logging.getLogger(__name__)

MOUNTED_STATE = "mission_control_socketio"


@dataclass(frozen=True)
class MissionSocketConfig:
    allowed_origins: tuple[str, ...]
    namespace: str = "/missions"
    socketio_path: str = "socket.io"
    redis_url: str | None = None
    redis_channel: str = "mission-control-socketio"
    max_http_buffer_size: int = 262_144
    ping_interval: float = 25.0
    ping_timeout: float = 20.0
    limits: MissionSocketLimits = field(default_factory=MissionSocketLimits)

    def __post_init__(self) -> None:
        if not self.namespace.startswith("/") or self.namespace == "/":
            raise ValueError("the mission namespace must be a named namespace")
        if "*" in self.allowed_origins:
            raise ValueError("mission sockets require explicit allowed origins")


@dataclass
class MountedMissionSocket:
    server: Any
    namespace: MissionNamespace
    wakeups: StreamWakeups
    started: int = 0
    stopped: int = 0


def mount_mission_socketio(
    app: FastAPI,
    config: MissionSocketConfig,
    *,
    resolver: PrincipalResolver | None = None,
    hints: StreamHintSource | None = None,
    client_manager: Any = None,
) -> Any:
    """Wrap `app`; returns the ASGI application to serve (exactly one per FastAPI app)."""

    if getattr(app.state, MOUNTED_STATE, None) is not None:
        raise RuntimeError("mission sockets are already mounted on this application")
    if client_manager is None and config.redis_url is not None:
        client_manager = socketio.AsyncRedisManager(config.redis_url, channel=config.redis_channel)
    server = socketio.AsyncServer(
        async_mode="asgi",
        cors_allowed_origins=list(config.allowed_origins),
        client_manager=client_manager,
        namespaces=[config.namespace],
        max_http_buffer_size=config.max_http_buffer_size,
        ping_interval=config.ping_interval,
        ping_timeout=config.ping_timeout,
        logger=False,
        engineio_logger=False,
    )
    wakeups = StreamWakeups()
    namespace = MissionNamespace(
        app,
        namespace=config.namespace,
        resolver=resolver or resolver_from_app(app),
        limits=config.limits,
        wakeups=wakeups,
    )
    server.register_namespace(namespace)
    mounted = MountedMissionSocket(server=server, namespace=namespace, wakeups=wakeups)
    if not hasattr(app.state, STREAM_SERVICES):
        setattr(app.state, STREAM_SERVICES, {})
    setattr(app.state, MOUNTED_STATE, mounted)
    app.router.lifespan_context = _with_streams(app.router.lifespan_context, mounted, hints)
    return socketio.ASGIApp(server, other_asgi_app=app, socketio_path=config.socketio_path)


def _with_streams(
    original: Callable[[Any], AbstractAsyncContextManager[Any]],
    mounted: MountedMissionSocket,
    hints: StreamHintSource | None,
) -> Callable[[Any], AbstractAsyncContextManager[Any]]:
    @asynccontextmanager
    async def lifespan(app: Any) -> AsyncIterator[Any]:
        async with original(app) as state:
            mounted.started += 1
            stop = asyncio.Event()
            listener = (
                asyncio.create_task(hints.run(mounted.wakeups.deliver, stop))
                if hints is not None
                else None
            )
            try:
                yield state
            finally:
                stop.set()
                await mounted.namespace.close()
                if listener is not None:
                    await asyncio.gather(listener, return_exceptions=True)
                mounted.stopped += 1

    return lifespan


__all__ = ["MOUNTED_STATE", "MissionSocketConfig", "MountedMissionSocket", "mount_mission_socketio"]
