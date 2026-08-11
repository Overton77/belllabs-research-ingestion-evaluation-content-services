from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager

import pytest

from app.integrations.langgraph_persistence import (
    StandalonePersistence,
    StandalonePersistenceLifespan,
)


class Resource:
    def __init__(self, kind: str, events: list[str]) -> None:
        self.kind = kind
        self.events = events
        self.setup_calls = 0

    async def setup(self) -> None:
        self.setup_calls += 1
        self.events.append(f"setup:{self.kind}")


def factory(kind: str, events: list[str]):  # type: ignore[no-untyped-def]
    @asynccontextmanager
    async def context(_dsn):  # type: ignore[no-untyped-def]
        resource = Resource(kind, events)
        events.append(f"enter:{kind}")
        try:
            yield resource
        finally:
            events.append(f"exit:{kind}")

    return context


@pytest.mark.asyncio
async def test_standalone_persistence_is_one_lifespan_and_setup_is_explicit() -> None:
    events: list[str] = []
    lifespan = StandalonePersistenceLifespan(
        "postgresql://disposable",
        run_setup=True,
        saver_factory=factory("saver", events),
        store_factory=factory("store", events),
    )

    async with lifespan as persistence:
        assert persistence.saver.setup_calls == 1  # type: ignore[attr-defined]
        assert persistence.store.setup_calls == 1  # type: ignore[attr-defined]
        assert StandalonePersistence.namespace("tenant-1", "procedural") == (
            "tenant-1",
            "procedural",
        )

    assert events == [
        "enter:saver",
        "enter:store",
        "setup:saver",
        "setup:store",
        "exit:store",
        "exit:saver",
    ]
    with pytest.raises(RuntimeError, match="cannot be entered twice"):
        async with lifespan:
            pass


@pytest.mark.asyncio
async def test_cancellation_closes_store_and_saver_cleanly() -> None:
    events: list[str] = []
    lifespan = StandalonePersistenceLifespan(
        "postgresql://disposable",
        saver_factory=factory("saver", events),
        store_factory=factory("store", events),
    )

    async def cancelled_scope() -> None:
        async with lifespan:
            raise asyncio.CancelledError

    with pytest.raises(asyncio.CancelledError):
        await cancelled_scope()

    assert events[-2:] == ["exit:store", "exit:saver"]


def test_store_namespace_is_tenant_and_purpose_scoped() -> None:
    with pytest.raises(ValueError, match="request scope and purpose"):
        StandalonePersistence.namespace("", "procedural")
    with pytest.raises(ValueError, match="request scope and purpose"):
        StandalonePersistence.namespace("tenant-1", "")
