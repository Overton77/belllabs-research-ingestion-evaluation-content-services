from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any

Message = dict[str, Any]
Receive = Callable[[], Awaitable[Message]]
Send = Callable[[Message], Awaitable[None]]


class RequestBodyTooLarge(Exception):
    pass


class BodySizeLimitMiddleware:
    def __init__(self, app: object, max_bytes: int = 1_000_000) -> None:
        self._app = app
        self._max_bytes = max_bytes

    async def __call__(
        self,
        scope: dict[str, Any],
        receive: Receive,
        send: Send,
    ) -> None:
        if scope["type"] != "http":
            await self._app(scope, receive, send)  # type: ignore[operator]
            return
        headers = dict(scope.get("headers", ()))
        content_length = headers.get(b"content-length")
        if content_length is not None and int(content_length) > self._max_bytes:
            await _reject(send)
            return
        received = 0

        async def limited_receive() -> Message:
            nonlocal received
            message = await receive()
            if message["type"] == "http.request":
                received += len(message.get("body", b""))
                if received > self._max_bytes:
                    raise RequestBodyTooLarge
            return message

        try:
            await self._app(scope, limited_receive, send)  # type: ignore[operator]
        except RequestBodyTooLarge:
            await _reject(send)


async def _reject(send: Send) -> None:
    body = b'{"detail":"request body exceeds 1000000 bytes"}'
    await send(
        {
            "type": "http.response.start",
            "status": 413,
            "headers": [
                (b"content-type", b"application/json"),
                (b"content-length", str(len(body)).encode()),
            ],
        }
    )
    await send({"type": "http.response.body", "body": body})
