from __future__ import annotations

from collections.abc import Callable
from contextlib import AbstractAsyncContextManager, AsyncExitStack
from dataclasses import dataclass
from types import TracebackType
from typing import Any

from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
from langgraph.store.postgres.aio import AsyncPostgresStore

PersistenceFactory = Callable[[str], AbstractAsyncContextManager[Any]]


@dataclass(frozen=True)
class StandalonePersistence:
    saver: AsyncPostgresSaver
    store: AsyncPostgresStore

    @staticmethod
    def namespace(request_scope: str, purpose: str) -> tuple[str, str]:
        scope = request_scope.strip()
        bounded_purpose = purpose.strip()
        if not scope or not bounded_purpose:
            raise ValueError("Store namespaces require request scope and purpose")
        return (scope, bounded_purpose)


class StandalonePersistenceLifespan:
    """One saver/Store pair for a standalone process lifespan, never per invocation."""

    def __init__(
        self,
        conn_string: str,
        *,
        run_setup: bool = False,
        saver_factory: PersistenceFactory | None = None,
        store_factory: PersistenceFactory | None = None,
    ) -> None:
        self._conn_string = conn_string
        self._run_setup = run_setup
        self._saver_factory = saver_factory or AsyncPostgresSaver.from_conn_string
        self._store_factory = store_factory or AsyncPostgresStore.from_conn_string
        self._stack: AsyncExitStack | None = None
        self._used = False

    async def __aenter__(self) -> StandalonePersistence:
        if self._used:
            raise RuntimeError("standalone persistence lifespan cannot be entered twice")
        self._used = True
        stack = AsyncExitStack()
        self._stack = stack
        try:
            saver = await stack.enter_async_context(self._saver_factory(self._conn_string))
            store = await stack.enter_async_context(self._store_factory(self._conn_string))
            if self._run_setup:
                await saver.setup()
                await store.setup()
            return StandalonePersistence(saver=saver, store=store)
        except BaseException:
            await stack.aclose()
            self._stack = None
            raise

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        if self._stack is not None:
            await self._stack.__aexit__(exc_type, exc, traceback)
            self._stack = None
