from __future__ import annotations

import asyncio
from typing import Any

from langgraph.types import Command

from .repository import ExperimentRepository, OutboxEvent


class WakeDispatcher:
    def __init__(self, graph: Any, repository: ExperimentRepository) -> None:
        self.graph = graph
        self.repository = repository

    async def deliver(self, event: OutboxEvent) -> bool:
        async with self.repository.pool.acquire() as connection:
            locked = await connection.fetchval(
                "SELECT pg_try_advisory_lock(hashtextextended($1, 0))", event.run_id
            )
            if not locked:
                return False
            try:
                await self.repository.increment_delivery_attempt(event.event_id)
                config = {"configurable": {"thread_id": event.thread_id}}
                snapshot = await self.graph.aget_state(config)
                interrupted = any(task.interrupts for task in snapshot.tasks)
                if not interrupted:
                    if not snapshot.next:
                        await self.repository.mark_outbox_delivered(event.event_id)
                        await self.repository.record_graph_event(
                            f"obsolete:{event.event_id}",
                            event.run_id,
                            "WAKE_PROVEN_OBSOLETE",
                            {"wake_event_id": event.event_id},
                        )
                        return True
                    return False
                await self.graph.ainvoke(
                    Command(
                        resume={
                            "wake_event_id": event.event_id,
                            "reason": "authoritative_state_changed",
                        }
                    ),
                    config=config,
                )
                await self.repository.mark_outbox_delivered(event.event_id)
                await self.repository.record_graph_event(
                    f"delivered:{event.event_id}",
                    event.run_id,
                    "WAKE_DELIVERED",
                    {"wake_event_id": event.event_id},
                )
                new_snapshot = await self.graph.aget_state(config)
                if any(task.interrupts for task in new_snapshot.tasks):
                    await self.repository.record_graph_event(
                        f"interrupt-after:{event.event_id}",
                        event.run_id,
                        "GRAPH_INTERRUPTED",
                        {"thread_id": event.thread_id},
                    )
                return True
            finally:
                await connection.execute(
                    "SELECT pg_advisory_unlock(hashtextextended($1, 0))", event.run_id
                )

    async def run(self, stop: asyncio.Event, poll_seconds: float = 0.2) -> None:
        while not stop.is_set():
            events = await self.repository.pending_events()
            seen_runs: set[str] = set()
            for event in events:
                if event.run_id in seen_runs:
                    continue
                seen_runs.add(event.run_id)
                await self.deliver(event)
            try:
                await asyncio.wait_for(stop.wait(), timeout=poll_seconds)
            except TimeoutError:
                pass
