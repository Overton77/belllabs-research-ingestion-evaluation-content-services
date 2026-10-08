"""Cursor Cloud Agents API v1 client (research/cursor-platform.md section 4; FT-G5).

A thin `httpx.AsyncClient` adapter over the public-beta REST surface the `cursor_cloud` lane
uses: create an agent (its first run is enqueued with it), follow-up runs, run reads, the run
SSE stream with `Last-Event-ID`, cancel, artifacts (listed, then downloaded through presigned
URLs without the API key), usage, archive. Provider errors become typed exceptions by their
documented codes (`agent_id_conflict`, `agent_busy`, `run_not_cancellable`, `stream_expired`,
`feature_unavailable`, `run_not_found`); messages never carry the API key or provider text.

Tests drive it through `httpx.MockTransport`; nothing here calls the network by itself.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Mapping
from dataclasses import dataclass
from typing import Any

import httpx
from pydantic import SecretStr

from mission_control.adapters.cursor.bridge import (
    AgentBusy,
    BridgeError,
    FeatureUnavailable,
    RunNotCancellable,
    RunNotFound,
)
from mission_control.adapters.cursor.sse import SseEvent, parse_sse

CLOUD_API_BASE = "https://api.cursor.com"
RETENTION_HEADER = "X-Cursor-Stream-Retention-Seconds"


class AgentIdConflict(BridgeError):
    """`409 agent_id_conflict`: the client-supplied agent id already exists (reattach)."""


class StreamExpired(BridgeError):
    """`410 stream_expired`: read the terminal state from `GET .../runs/{runId}` instead."""


class AgentNotFound(BridgeError):
    """`404`: the agent (or its run) does not exist."""


class InvalidLastEventId(BridgeError):
    """`400 invalid_last_event_id`: the cursor belongs to another run."""


@dataclass(frozen=True)
class StreamOpened:
    retention_seconds: int | None
    last_event_id: str | None


def _code(response: httpx.Response) -> str:
    try:
        body = response.json()
    except ValueError:
        return ""
    if isinstance(body, Mapping):
        error = body.get("error")
        if isinstance(error, Mapping) and error.get("code"):
            return str(error["code"])
        if body.get("code"):
            return str(body["code"])
    return ""


def _raise(response: httpx.Response, operation: str) -> None:
    if response.is_success:
        return
    code = _code(response)
    status = response.status_code
    if status == 409 and code == "agent_id_conflict":
        raise AgentIdConflict(f"{operation}: agent id already exists")
    if status == 409 and code == "agent_busy":
        raise AgentBusy(f"{operation}: the agent has an active run")
    if status == 409 and code == "run_not_cancellable":
        raise RunNotCancellable(f"{operation}: the run is already terminal")
    if status == 410:
        raise StreamExpired(f"{operation}: the stream retention window has passed")
    if status == 403 and code == "feature_unavailable":
        raise FeatureUnavailable(f"{operation}: feature unavailable for this account")
    if status == 404 and code == "run_not_found":
        raise RunNotFound(f"{operation}: run not found")
    if status == 404:
        raise AgentNotFound(f"{operation}: not found")
    if status == 400 and code == "invalid_last_event_id":
        raise InvalidLastEventId(f"{operation}: the stream cursor belongs to another run")
    raise BridgeError(f"{operation}: HTTP {status} {code}".rstrip())


class CloudAgentsClient:
    def __init__(
        self,
        api_key: SecretStr,
        *,
        base_url: str = CLOUD_API_BASE,
        transport: httpx.AsyncBaseTransport | None = None,
        timeout_s: float = 30,
    ) -> None:
        self._client = httpx.AsyncClient(
            base_url=base_url,
            headers={"Authorization": f"Bearer {api_key.get_secret_value()}"},
            transport=transport,
            timeout=httpx.Timeout(timeout_s, read=None),
            follow_redirects=False,
        )
        # Presigned artifact URLs never receive the API key.
        self._downloads = httpx.AsyncClient(
            transport=transport, timeout=timeout_s, follow_redirects=False
        )

    async def aclose(self) -> None:
        await self._client.aclose()
        await self._downloads.aclose()

    async def create_agent(
        self, body: Mapping[str, Any], *, idempotency_key: str | None = None
    ) -> dict[str, Any]:
        headers = {"Idempotency-Key": idempotency_key} if idempotency_key else None
        response = await self._client.post("/v1/agents", json=dict(body), headers=headers)
        _raise(response, "create agent")
        result: dict[str, Any] = response.json()
        return result

    async def get_agent(self, agent_id: str) -> dict[str, Any]:
        response = await self._client.get(f"/v1/agents/{agent_id}")
        _raise(response, "get agent")
        result: dict[str, Any] = response.json()
        return result.get("agent", result) if isinstance(result.get("agent"), dict) else result

    async def create_run(self, agent_id: str, body: Mapping[str, Any]) -> dict[str, Any]:
        response = await self._client.post(f"/v1/agents/{agent_id}/runs", json=dict(body))
        _raise(response, "create run")
        result: dict[str, Any] = response.json()
        run = result.get("run", result)
        return dict(run)

    async def get_run(self, agent_id: str, run_id: str) -> dict[str, Any]:
        response = await self._client.get(f"/v1/agents/{agent_id}/runs/{run_id}")
        _raise(response, "get run")
        result: dict[str, Any] = response.json()
        run = result.get("run", result)
        return dict(run)

    async def cancel_run(self, agent_id: str, run_id: str) -> None:
        response = await self._client.post(f"/v1/agents/{agent_id}/runs/{run_id}/cancel")
        _raise(response, "cancel run")

    async def stream(
        self, agent_id: str, run_id: str, *, last_event_id: str | None
    ) -> AsyncIterator[SseEvent | StreamOpened]:
        """The run's SSE events; the first item reports the retention window."""

        headers = {"Accept": "text/event-stream"}
        if last_event_id:
            headers["Last-Event-ID"] = last_event_id
        async with self._client.stream(
            "GET", f"/v1/agents/{agent_id}/runs/{run_id}/stream", headers=headers
        ) as response:
            if not response.is_success:
                await response.aread()
                _raise(response, "stream run")
            retention = response.headers.get(RETENTION_HEADER)
            yield StreamOpened(
                retention_seconds=int(retention) if retention and retention.isdigit() else None,
                last_event_id=last_event_id,
            )
            async for event in parse_sse(response.aiter_lines()):
                yield event

    async def list_artifacts(self, agent_id: str) -> list[dict[str, Any]]:
        response = await self._client.get(f"/v1/agents/{agent_id}/artifacts")
        _raise(response, "list artifacts")
        items = response.json().get("items") or []
        return [dict(item) for item in items if isinstance(item, Mapping)]

    async def download_artifact(self, agent_id: str, path: str) -> bytes:
        response = await self._client.get(
            f"/v1/agents/{agent_id}/artifacts/download", params={"path": path}
        )
        _raise(response, "artifact download url")
        url = str(response.json()["url"])
        download = await self._downloads.get(url)
        _raise(download, "artifact download")
        return download.content

    async def usage(self, agent_id: str, run_id: str | None = None) -> dict[str, Any]:
        params = {"runId": run_id} if run_id else None
        response = await self._client.get(f"/v1/agents/{agent_id}/usage", params=params)
        _raise(response, "usage")
        result: dict[str, Any] = response.json()
        return result

    async def archive(self, agent_id: str) -> None:
        response = await self._client.post(f"/v1/agents/{agent_id}/archive")
        _raise(response, "archive agent")


__all__ = [
    "CLOUD_API_BASE",
    "RETENTION_HEADER",
    "AgentIdConflict",
    "AgentNotFound",
    "CloudAgentsClient",
    "InvalidLastEventId",
    "StreamExpired",
    "StreamOpened",
]
