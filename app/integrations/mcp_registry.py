from __future__ import annotations

import asyncio
import json
from collections.abc import Awaitable, Callable, Mapping
from datetime import UTC, datetime
from hashlib import sha256
from typing import Any, Protocol
from urllib.parse import quote

import httpx
from pydantic import BaseModel, ConfigDict, Field

from app.application.web_research.external_capability_discovery import (
    ExternalDiscoveryBatch,
    ExternalDiscoveryCandidate,
    ExternalDiscoveryEvidence,
    ExternalDiscoverySource,
)


class MCPRegistryContract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class MCPRegistryHttpRequest(MCPRegistryContract):
    url: str = Field(min_length=1)
    params: dict[str, str | int | bool]
    timeout_seconds: float = Field(ge=1, le=60)
    max_response_bytes: int = Field(ge=1_024)


class MCPRegistryHttpResponse(MCPRegistryContract):
    status_code: int = Field(ge=100, le=599)
    body: bytes


class MCPRegistryHttpRunner(Protocol):
    async def get(self, request: MCPRegistryHttpRequest) -> MCPRegistryHttpResponse: ...


class MCPRegistryDependencyError(RuntimeError):
    pass


class HttpxMCPRegistryRunner:
    """Production HTTP runner with redirects disabled and bounded response reads."""

    async def get(self, request: MCPRegistryHttpRequest) -> MCPRegistryHttpResponse:
        timeout = httpx.Timeout(request.timeout_seconds)
        try:
            async with httpx.AsyncClient(
                timeout=timeout,
                follow_redirects=False,
                headers={"Accept": "application/json"},
            ) as client:
                async with client.stream(
                    "GET",
                    request.url,
                    params=request.params,
                ) as response:
                    chunks: list[bytes] = []
                    size = 0
                    async for chunk in response.aiter_bytes():
                        size += len(chunk)
                        if size > request.max_response_bytes:
                            raise MCPRegistryDependencyError(
                                "MCP Registry response exceeded the configured size limit"
                            )
                        chunks.append(chunk)
        except MCPRegistryDependencyError:
            raise
        except httpx.HTTPError as error:
            raise MCPRegistryDependencyError("MCP Registry request failed") from error
        return MCPRegistryHttpResponse(
            status_code=response.status_code,
            body=b"".join(chunks),
        )


class MCPRegistryAdapter:
    """Bounded official Registry search that returns candidate-only records."""

    def __init__(
        self,
        runner: MCPRegistryHttpRunner,
        *,
        base_url: str,
        api_version: str,
        timeout_seconds: float,
        max_response_bytes: int,
        max_pages: int,
        max_retries: int,
        clock: Callable[[], datetime] | None = None,
        sleeper: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        if not base_url.startswith("https://"):
            raise ValueError("MCP Registry base URL must use HTTPS")
        if api_version != "v0.1":
            raise ValueError("unsupported MCP Registry API version")
        if max_pages < 1 or max_retries < 0:
            raise ValueError("MCP Registry bounds are invalid")
        self._runner = runner
        self._base_url = base_url.rstrip("/")
        self._api_version = api_version
        self._timeout_seconds = timeout_seconds
        self._max_response_bytes = max_response_bytes
        self._max_pages = max_pages
        self._max_retries = max_retries
        self._clock = clock or (lambda: datetime.now(UTC))
        self._sleeper = sleeper

    async def search(self, query: str, *, limit: int) -> ExternalDiscoveryBatch:
        normalized_query = _validated_query(query)
        if limit < 1 or limit > 100:
            raise ValueError("MCP Registry limit must be between 1 and 100")

        candidates: list[ExternalDiscoveryCandidate] = []
        evidence: list[ExternalDiscoveryEvidence] = []
        cursor: str | None = None
        seen: set[str] = set()
        for _page in range(self._max_pages):
            remaining = limit - len(candidates)
            if remaining <= 0:
                break
            params: dict[str, str | int | bool] = {
                "search": normalized_query,
                "limit": min(remaining, 100),
                "include_deleted": False,
            }
            if cursor:
                params["cursor"] = cursor
            response, request_count = await self._request(params)
            if len(response.body) > self._max_response_bytes:
                raise MCPRegistryDependencyError(
                    "MCP Registry response exceeded the configured size limit"
                )
            digest = _digest(response.body)
            retrieved_at = self._clock()
            payload = _decode_json_object(response.body)
            evidence.append(
                ExternalDiscoveryEvidence(
                    source=ExternalDiscoverySource.MCP_REGISTRY,
                    source_version=self._api_version,
                    query=normalized_query,
                    retrieved_at=retrieved_at,
                    raw_response_digest=digest,
                    raw_response_size_bytes=len(response.body),
                    request_count=request_count,
                )
            )
            for item in _server_items(payload):
                candidate = _candidate(
                    item,
                    query=normalized_query,
                    discovered_at=retrieved_at,
                    raw_response_digest=digest,
                )
                if candidate is None or candidate.candidate_id in seen:
                    continue
                seen.add(candidate.candidate_id)
                candidates.append(candidate)
                if len(candidates) >= limit:
                    break
            cursor = _next_cursor(payload)
            if not cursor:
                break
        return ExternalDiscoveryBatch(
            source=ExternalDiscoverySource.MCP_REGISTRY,
            candidates=tuple(candidates),
            evidence=tuple(evidence),
        )

    async def _request(
        self,
        params: dict[str, str | int | bool],
    ) -> tuple[MCPRegistryHttpResponse, int]:
        request = MCPRegistryHttpRequest(
            url=f"{self._base_url}/{self._api_version}/servers",
            params=params,
            timeout_seconds=self._timeout_seconds,
            max_response_bytes=self._max_response_bytes,
        )
        for attempt in range(self._max_retries + 1):
            try:
                response = await self._runner.get(request)
            except Exception as error:
                if attempt >= self._max_retries:
                    raise MCPRegistryDependencyError("MCP Registry request failed") from error
            else:
                if response.status_code == 200:
                    return response, attempt + 1
                if response.status_code not in {408, 429, 500, 502, 503, 504}:
                    raise MCPRegistryDependencyError(
                        f"MCP Registry returned HTTP {response.status_code}"
                    )
                if attempt >= self._max_retries:
                    raise MCPRegistryDependencyError(
                        f"MCP Registry returned HTTP {response.status_code}"
                    )
            await self._sleeper(min(0.25 * (2**attempt), 2.0))
        raise AssertionError("unreachable")


def _validated_query(query: str) -> str:
    normalized = query.strip()
    if not normalized or len(normalized) > 500:
        raise ValueError("MCP Registry query must contain 1 to 500 characters")
    return normalized


def _decode_json_object(body: bytes) -> Mapping[str, Any]:
    try:
        value = json.loads(body)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise MCPRegistryDependencyError("MCP Registry returned invalid JSON") from error
    if not isinstance(value, dict):
        raise MCPRegistryDependencyError("MCP Registry returned an invalid payload")
    return value


def _server_items(payload: Mapping[str, Any]) -> tuple[Mapping[str, Any], ...]:
    raw_items = payload.get("servers", payload.get("data", ()))
    if not isinstance(raw_items, list):
        raise MCPRegistryDependencyError("MCP Registry server list is invalid")
    return tuple(item for item in raw_items if isinstance(item, dict))


def _next_cursor(payload: Mapping[str, Any]) -> str | None:
    metadata = payload.get("metadata")
    candidate = (
        metadata.get("next_cursor") if isinstance(metadata, dict) else payload.get("next_cursor")
    )
    value = str(candidate).strip() if candidate is not None else ""
    return value or None


def _candidate(
    item: Mapping[str, Any],
    *,
    query: str,
    discovered_at: datetime,
    raw_response_digest: str,
) -> ExternalDiscoveryCandidate | None:
    server = item.get("server", item)
    if not isinstance(server, dict):
        return None
    identity = str(server.get("name", "")).strip()
    if not identity:
        return None
    version_value = server.get("version")
    version = str(version_value).strip() if version_value is not None else None
    if version == "":
        version = None
    locator = _server_locator(server, identity, version)
    publisher = identity.split("/", maxsplit=1)[0] if "/" in identity else None
    status = str(item.get("status", server.get("status", "active"))).strip() or "active"
    identity_digest = _digest(
        json.dumps(
            {
                "source": ExternalDiscoverySource.MCP_REGISTRY.value,
                "identity": identity,
                "version": version,
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
    )
    return ExternalDiscoveryCandidate(
        candidate_id=f"candidate:{identity_digest}",
        source=ExternalDiscoverySource.MCP_REGISTRY,
        upstream_identity=identity,
        upstream_version=version,
        locator=locator,
        publisher=publisher,
        discovered_at=discovered_at,
        query=query,
        raw_response_digest=raw_response_digest,
        upstream_status=status,
    )


def _server_locator(
    server: Mapping[str, Any],
    identity: str,
    version: str | None,
) -> str:
    repository = server.get("repository")
    if isinstance(repository, dict):
        url = str(repository.get("url", "")).strip()
        if url.startswith(("https://", "http://")):
            return url
    suffix = f"/versions/{quote(version, safe='')}" if version else ""
    return (
        f"https://registry.modelcontextprotocol.io/v0.1/servers/{quote(identity, safe='')}{suffix}"
    )


def _digest(value: bytes) -> str:
    return f"sha256:{sha256(value).hexdigest()}"
