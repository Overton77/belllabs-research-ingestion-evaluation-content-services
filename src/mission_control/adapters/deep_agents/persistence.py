"""Standalone LangGraph saver/store bound to the private ``mission_control_runtime`` schema.

The pinned vendor SQL (``langgraph-checkpoint-postgres``) uses unqualified table names, so
the dedicated runtime connection selects its schema with exactly one libpq ``options``
value: ``-c search_path=<schema>,pg_temp``. ``pg_catalog`` stays implicitly first and
``pg_temp`` is last, so neither catalog nor temporary relations can shadow runtime tables.

Production never calls the vendor ``setup()``: the schema is provisioned by the
``mission-db runtime-apply`` phase from ``packages/mission-control-db-contract/runtime``
and verified read-only at readiness (``runtime_persistence_verifier``). The only setup
path left here is test-only and refuses the production runtime schema.
"""

from __future__ import annotations

from collections.abc import Callable
from contextlib import AbstractAsyncContextManager, AsyncExitStack
from dataclasses import dataclass
from types import TracebackType
from typing import Any

from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
from langgraph.store.postgres.aio import AsyncPostgresStore
from psycopg import ProgrammingError
from psycopg.conninfo import conninfo_to_dict, make_conninfo

PersistenceFactory = Callable[[str], AbstractAsyncContextManager[Any]]

RUNTIME_SCHEMA = "mission_control_runtime"
CHECKPOINTER_ROLE = "mission_control_checkpointer"
# Schemas that must never hold the saver/store (legacy or business authority).
FORBIDDEN_CHECKPOINT_SCHEMAS = frozenset(
    {
        "public",
        "pg_catalog",
        "pg_temp",
        "information_schema",
        "belllabs_langgraph",
        "belllabs_control",
        "capability_search",
        "mission_control",
        "mission_control_search",
        "mission_control_agent_server",
    }
)


class RuntimeConninfoError(ValueError):
    """A checkpoint DSN cannot be bound safely to its private runtime schema."""


def runtime_search_path_option(schema: str) -> str:
    return f"-c search_path={schema},pg_temp"


def runtime_checkpoint_conninfo(dsn: str, schema: str = RUNTIME_SCHEMA) -> str:
    """Bind ``dsn`` to ``schema`` with one libpq ``options`` value; never override silently.

    A DSN that already carries ``options`` (or any ``search_path``) is rejected instead of
    being rewritten: libpq keeps only the last duplicate key, so concatenation could hide
    an operator-supplied path. The error never includes the DSN (it may hold a password).
    """

    if schema in FORBIDDEN_CHECKPOINT_SCHEMAS or not _is_identifier(schema):
        raise RuntimeConninfoError("checkpoint schema is not an admitted runtime schema")
    if "search_path" in dsn.lower():
        raise RuntimeConninfoError("checkpoint DSN must not configure search_path itself")
    try:
        parsed = conninfo_to_dict(dsn)
    except ProgrammingError:
        raise RuntimeConninfoError(
            "checkpoint DSN is not a valid libpq connection string"
        ) from None
    if "options" in parsed:
        raise RuntimeConninfoError(
            "checkpoint DSN must not carry libpq options; the runtime binding sets them"
        )
    return make_conninfo(dsn, options=runtime_search_path_option(schema))


def conninfo_search_path_schema(conninfo: str) -> str | None:
    """The first schema the conninfo's single ``options`` value selects, if any."""

    try:
        options = conninfo_to_dict(conninfo).get("options")
    except ProgrammingError:
        return None
    if not isinstance(options, str):
        return None
    for token in options.replace("-c ", "-c").split():
        if token.startswith("-csearch_path="):
            return token.removeprefix("-csearch_path=").split(",", 1)[0].strip() or None
    return None


def _is_identifier(value: str) -> bool:
    return (
        0 < len(value) <= 63
        and (value[0].isascii() and (value[0].islower() or value[0] == "_"))
        and all(ch.isascii() and (ch.islower() or ch.isdigit() or ch == "_") for ch in value)
    )


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
    """One saver/Store pair for a standalone process lifespan, never per invocation.

    ``run_setup`` is a TEST-ONLY vendor ``setup()`` path for disposable schemas. It is
    refused for the production ``mission_control_runtime`` schema and for a conninfo
    without an explicit private search path (vendor DDL would otherwise land in
    ``public``). Production provisioning is the separate descriptor-driven phase.
    """

    def __init__(
        self,
        conn_string: str,
        *,
        run_setup: bool = False,
        saver_factory: PersistenceFactory | None = None,
        store_factory: PersistenceFactory | None = None,
    ) -> None:
        if run_setup:
            schema = conninfo_search_path_schema(conn_string)
            if schema is None or schema == RUNTIME_SCHEMA or schema in FORBIDDEN_CHECKPOINT_SCHEMAS:
                raise RuntimeConninfoError(
                    "test-only vendor setup requires an explicit disposable search_path schema; "
                    "mission_control_runtime is provisioned only by mission-db runtime-apply"
                )
        self._conn_string = conn_string
        self._test_only_vendor_setup = run_setup
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
            if self._test_only_vendor_setup:
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
