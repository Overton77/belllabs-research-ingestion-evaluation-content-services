"""A real uvicorn server and python-socketio clients for the `/missions` namespace."""

from __future__ import annotations

import asyncio
import base64
import contextlib
import json
import time
from collections import defaultdict
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

import socketio
import uvicorn

NAMESPACE = "/missions"
SERVER_EVENTS = (
    "subscribed",
    "snapshot",
    "mission_event",
    "provider_frame",
    "presence",
    "command_receipt",
    "resync_required",
    "stream_error",
)


@asynccontextmanager
async def serve(asgi: Any) -> AsyncIterator[tuple[str, uvicorn.Server]]:
    config = uvicorn.Config(
        asgi, host="127.0.0.1", port=0, lifespan="on", log_level="warning", ws="websockets-sansio"
    )
    server = uvicorn.Server(config)
    task = asyncio.create_task(server.serve())
    try:
        async with asyncio.timeout(15):
            while not server.started:
                if task.done():
                    task.result()
                await asyncio.sleep(0.01)
        port = server.servers[0].sockets[0].getsockname()[1]
        yield f"http://127.0.0.1:{port}", server
    finally:
        server.should_exit = True
        await asyncio.wait_for(task, timeout=15)


def fixture_token(subject: str, *, exp: float | None = None) -> str:
    """A JWT-shaped FIXTURE credential for the fake resolver (not signed or verified)."""

    claims = {"sub": subject, "exp": int(exp if exp is not None else time.time() + 300)}
    payload = base64.urlsafe_b64encode(json.dumps(claims).encode()).decode().rstrip("=")
    return f"e30.{payload}.fixture"


class MissionClient:
    """Records every server event in arrival order."""

    def __init__(self) -> None:
        self.sio = socketio.AsyncClient(reconnection=False)
        self.events: dict[str, list[Any]] = defaultdict(list)
        self.order: list[tuple[str, Any]] = []
        self.changed = asyncio.Event()
        for name in SERVER_EVENTS:
            self.sio.on(name, self._recorder(name), namespace=NAMESPACE)
        self.sio.on("disconnect", self._disconnected, namespace=NAMESPACE)

    async def _disconnected(self, *_args: Any) -> None:
        self.changed.set()

    def _recorder(self, name: str) -> Any:
        async def record(data: Any = None) -> None:
            self.events[name].append(data)
            self.order.append((name, data))
            self.changed.set()

        return record

    async def connect(self, url: str, auth: dict[str, Any], **kwargs: Any) -> None:
        await self.sio.connect(
            url,
            namespaces=[NAMESPACE],
            auth=auth,
            transports=["websocket"],
            wait_timeout=10,
            **kwargs,
        )

    async def call(self, event: str, data: Any) -> Any:
        return await self.sio.call(event, data, namespace=NAMESPACE, timeout=10)

    async def close(self) -> None:
        if self.sio.connected:
            await self.sio.disconnect()

    def seqs(self) -> list[int]:
        return [int(item["cursor"]["position"]) for item in self.events["mission_event"]]

    async def wait_for(self, predicate: Any, within: float = 10.0) -> None:
        """Wait until `predicate()` holds, re-checking after every received event."""

        async with asyncio.timeout(within):
            while not predicate():
                self.changed.clear()
                if predicate():
                    return
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(self.changed.wait(), timeout=0.25)
