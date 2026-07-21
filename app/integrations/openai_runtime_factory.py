from __future__ import annotations

import asyncio
from typing import Any

import asyncpg
from redis.asyncio import Redis

from app.integrations.openai_agents_runtime import OpenAIAgentsSandboxRuntime
from app.integrations.postgres_agent_session import PostgresAgentSessionFactory
from app.integrations.runtime_realtime import (
    PostgresRedisApprovalGateway,
    PostgresRedisRuntimeEventBus,
)


class DurableOpenAIAgentsRuntimeFactory:
    """Composes the SDK adapter with mandatory durable runtime projections."""

    def __init__(
        self,
        *,
        pool: asyncpg.Pool,
        redis: Redis,
        checkpoint_signing_key: bytes,
        approval_timeout_seconds: int,
    ) -> None:
        self.events = PostgresRedisRuntimeEventBus(pool, redis)
        self.approvals = PostgresRedisApprovalGateway(
            pool,
            redis,
            checkpoint_signing_key=checkpoint_signing_key,
        )
        self.sessions = PostgresAgentSessionFactory(pool)
        self._approval_timeout_seconds = approval_timeout_seconds
        self._runtimes: set[OpenAIAgentsSandboxRuntime] = set()

    def create(self, **kwargs: Any) -> OpenAIAgentsSandboxRuntime:
        forbidden = {"event_sink", "approval_gateway", "session_factory"} & kwargs.keys()
        if forbidden:
            raise ValueError(
                "durable runtime infrastructure cannot be overridden: "
                + ", ".join(sorted(forbidden))
            )
        runtime = OpenAIAgentsSandboxRuntime(
            event_sink=self.events,
            approval_gateway=self.approvals,
            session_factory=self.sessions,
            approval_timeout_seconds=self._approval_timeout_seconds,
            **kwargs,
        )
        self._runtimes.add(runtime)
        return runtime

    async def aclose(self) -> None:
        runtimes = tuple(self._runtimes)
        self._runtimes.clear()
        await asyncio.gather(*(runtime.aclose() for runtime in runtimes))
