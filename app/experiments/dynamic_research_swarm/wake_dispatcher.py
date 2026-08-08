from __future__ import annotations

import asyncio

from app.experiments.langgraph_temporal_stagegraph.wake_dispatcher import WakeDispatcher


class RunScopedWakeDispatcher(WakeDispatcher):
    """Recovery driver that cannot consume another experiment run's pending wake."""

    def __init__(self, graph, repository, run_id: str) -> None:
        super().__init__(graph, repository)
        self.run_id = run_id

    async def run(self, stop: asyncio.Event, poll_seconds: float = 0.2) -> None:
        while not stop.is_set():
            events = [
                event
                for event in await self.repository.pending_events(limit=100)
                if event.run_id == self.run_id
            ]
            if events:
                await self.deliver(events[0])
            try:
                await asyncio.wait_for(stop.wait(), timeout=poll_seconds)
            except TimeoutError:
                pass
