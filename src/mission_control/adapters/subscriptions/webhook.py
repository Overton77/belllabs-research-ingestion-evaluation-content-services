"""Webhook transport for the subscription relay (httpx, no redirects, bounded timeout)."""

from __future__ import annotations

from collections.abc import Mapping
from time import perf_counter

import httpx

from mission_control.application.subscriptions.ports import (
    WebhookResponse,
    WebhookTransportError,
)


class HttpxWebhookTransport:
    def __init__(self, client: httpx.AsyncClient | None = None, *, timeout: float = 10.0) -> None:
        self._client = client or httpx.AsyncClient(timeout=timeout, follow_redirects=False)
        self._owned = client is None

    async def post(self, url: str, body: bytes, headers: Mapping[str, str]) -> WebhookResponse:
        started = perf_counter()
        try:
            response = await self._client.post(url, content=body, headers=dict(headers))
        except httpx.HTTPError as exc:
            raise WebhookTransportError(type(exc).__name__) from None
        return WebhookResponse(
            status_code=response.status_code,
            latency_ms=int((perf_counter() - started) * 1000),
        )

    async def aclose(self) -> None:
        if self._owned:
            await self._client.aclose()
