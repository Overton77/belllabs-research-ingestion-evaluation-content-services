"""`bootstrap/realtime.py` composition (MP-14 integration): hint sources and the mount."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from typing import Any

import pytest
from fastapi import FastAPI
from pydantic import SecretStr

from mission_control.adapters.realtime.stream_hints_postgres import (
    CompositeStreamHints,
    PostgresStreamHints,
)
from mission_control.application.streams.ports import StreamHint
from mission_control.bootstrap.realtime import (
    create_asgi_app,
    mission_socket_config,
    postgres_hint_sources,
)
from mission_control.bootstrap.settings import Settings
from mission_control.interfaces.socketio.app import MOUNTED_STATE


def _settings(**overrides: Any) -> Settings:
    base = Settings(_env_file=None)  # type: ignore[call-arg]
    return base.model_copy(update=overrides)


def test_postgres_hint_sources_are_one_per_distinct_configured_database() -> None:
    environ = {
        "MC_A": "postgresql://runtime@127.0.0.1:55433/mc_a",
        "MC_B": "postgresql://runtime@127.0.0.1:55433/mc_a",
        "MC_C": "postgresql://runtime@127.0.0.1:55433/mc_c",
    }
    sources = postgres_hint_sources(["MC_A", "MC_B", "MC_C", "MC_ABSENT"], environ)
    assert [type(source) for source in sources] == [PostgresStreamHints, PostgresStreamHints]


def test_socket_config_uses_explicit_origins_and_redis_only_with_fanout() -> None:
    off = mission_socket_config(_settings(socketio_cors_origins="http://a.test, http://b.test"))
    assert off.allowed_origins == ("http://a.test", "http://b.test") and off.redis_url is None
    on = mission_socket_config(
        _settings(
            mission_socket_redis_fanout=True, redis_url=SecretStr("redis://127.0.0.1:16379/9")
        )
    )
    assert on.redis_url == "redis://127.0.0.1:16379/9"


async def test_create_asgi_app_wraps_the_given_app_once_and_runs_its_lifespan_once() -> None:
    entered: list[str] = []

    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        entered.append("start")
        yield
        entered.append("stop")

    app = FastAPI(lifespan=lifespan)
    asgi = create_asgi_app(settings=_settings(mission_socket_postgres_hints=False), app=app)
    mounted = getattr(app.state, MOUNTED_STATE)
    assert asgi is not None and mounted is not None
    async with app.router.lifespan_context(app):
        assert entered == ["start"]
    assert entered == ["start", "stop"]
    assert (mounted.started, mounted.stopped) == (1, 1)
    with pytest.raises(RuntimeError, match="already mounted"):
        create_asgi_app(settings=_settings(mission_socket_postgres_hints=False), app=app)


async def test_composite_hints_fan_every_source_into_one_deliver_and_stop_together() -> None:
    class Source:
        def __init__(self, scope: str) -> None:
            self.scope = scope

        async def run(self, deliver, stop: asyncio.Event) -> None:
            deliver(
                StreamHint(request_scope=self.scope, mission_id=None, harness_execution_id=None)
            )
            await stop.wait()

    delivered: list[StreamHint] = []
    stop = asyncio.Event()
    composite = CompositeStreamHints(Source("mc/a"), Source("mc/b"))
    task = asyncio.create_task(composite.run(delivered.append, stop))
    for _ in range(100):
        if len(delivered) == 2:
            break
        await asyncio.sleep(0.01)
    assert {hint.request_scope for hint in delivered} == {"mc/a", "mc/b"}
    stop.set()
    await asyncio.wait_for(task, timeout=2)
