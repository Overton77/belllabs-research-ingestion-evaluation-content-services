from __future__ import annotations

import asyncio

import socketio


async def main() -> None:
    client = socketio.AsyncClient()
    ready = asyncio.Event()

    @client.on("server_ready")
    async def on_server_ready(_data: dict) -> None:
        ready.set()

    await client.connect("http://127.0.0.1:8000", socketio_path="socket.io")
    await asyncio.wait_for(ready.wait(), timeout=5)
    await client.disconnect()
    print("socketio_ready=true")


if __name__ == "__main__":
    asyncio.run(main())
