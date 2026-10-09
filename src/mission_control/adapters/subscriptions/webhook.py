"""Webhook transports for subscription and coordinator callbacks (httpx, no redirects).

`HttpxWebhookTransport` is the FT-F5 transport (bounded timeout, no redirects).
`EgressGuardedWebhookTransport` adds destination/egress validation (SPEC-04, ADR-0040,
MP-15) so a registered callback cannot make this host call into internal networks:

- scheme `https`; plain `http` only to loopback and only when the policy allows loopback;
- no credentials or fragment in the URL; optional port allowlist;
- the host is resolved once per request and **every** resolved address must be public
  (`ipaddress` `is_global`, not multicast/unspecified; IPv4-mapped IPv6 is unwrapped), or
  inside an explicitly allowed network (loopback only with `allow_loopback`);
- the connection is pinned to the validated address (the URL host becomes that IP, the
  `Host` header and the TLS SNI/certificate hostname stay the original name: httpcore 1.0.9
  `_async/connection.py` passes the request extension `sni_hostname` as `server_hostname`),
  so a DNS answer cannot change between validation and connect (rebinding);
- redirects are never followed.

A rejection raises `WebhookEgressRejected` (`error_class = "egress_rejected"`) before any
byte is sent; the relay dead-letters it at once instead of retrying.
"""

from __future__ import annotations

import asyncio
import ipaddress
import socket
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from time import perf_counter
from urllib.parse import urlsplit, urlunsplit

import httpx

from mission_control.application.subscriptions.ports import (
    WebhookEgressRejected,
    WebhookResponse,
    WebhookTransportError,
)

IpAddress = ipaddress.IPv4Address | ipaddress.IPv6Address
Resolver = Callable[[str, int], Awaitable[Sequence[str]]]


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


async def system_resolver(host: str, port: int) -> Sequence[str]:
    infos = await asyncio.get_running_loop().getaddrinfo(
        host, port, type=socket.SOCK_STREAM, proto=socket.IPPROTO_TCP
    )
    return [str(info[4][0]) for info in infos]


@dataclass(frozen=True)
class PinnedDestination:
    url: str
    host: str
    port: int
    scheme: str
    address: IpAddress

    @property
    def pinned_url(self) -> str:
        parts = urlsplit(self.url)
        literal = f"[{self.address}]" if self.address.version == 6 else str(self.address)
        default = 443 if self.scheme == "https" else 80
        netloc = literal if self.port == default else f"{literal}:{self.port}"
        return urlunsplit((parts.scheme, netloc, parts.path or "/", parts.query, ""))

    @property
    def host_header(self) -> str:
        default = 443 if self.scheme == "https" else 80
        name = f"[{self.host}]" if ":" in self.host else self.host
        return name if self.port == default else f"{name}:{self.port}"


def _unwrap(address: IpAddress) -> IpAddress:
    if isinstance(address, ipaddress.IPv6Address) and address.ipv4_mapped is not None:
        return address.ipv4_mapped
    return address


class EgressPolicy:
    """Destination validation for outbound callbacks (implements WebhookDestinationPolicy)."""

    def __init__(
        self,
        *,
        allow_loopback: bool = False,
        allowed_networks: Sequence[str] = (),
        allowed_ports: frozenset[int] | None = None,
        resolver: Resolver = system_resolver,
    ) -> None:
        self._allow_loopback = allow_loopback
        self._allowed = tuple(ipaddress.ip_network(item) for item in allowed_networks)
        self._ports = allowed_ports
        self._resolver = resolver

    def _permitted(self, address: IpAddress) -> bool:
        address = _unwrap(address)
        if any(address in network for network in self._allowed):
            return True
        if address.is_loopback:
            return self._allow_loopback
        if address.is_multicast or address.is_unspecified:
            return False
        return address.is_global

    async def resolve(self, url: str) -> PinnedDestination:
        parts = urlsplit(url)
        host = parts.hostname
        if not host or parts.username or parts.password or parts.fragment:
            raise WebhookEgressRejected("callback URL needs a host and no credentials/fragment")
        try:
            port = parts.port or (443 if parts.scheme == "https" else 80)
        except ValueError:
            raise WebhookEgressRejected("callback URL port is invalid") from None
        if self._ports is not None and port not in self._ports:
            raise WebhookEgressRejected(f"callback port {port} is not allowed")
        try:
            literal: IpAddress | None = ipaddress.ip_address(host)
        except ValueError:
            literal = None
        if literal is not None:
            addresses: list[IpAddress] = [literal]
        else:
            try:
                resolved = await self._resolver(host, port)
            except (OSError, UnicodeError):
                raise WebhookEgressRejected("callback host does not resolve") from None
            addresses = [ipaddress.ip_address(item.split("%", 1)[0]) for item in resolved]
        if not addresses:
            raise WebhookEgressRejected("callback host does not resolve")
        blocked = [str(item) for item in addresses if not self._permitted(item)]
        if blocked:
            raise WebhookEgressRejected(
                f"callback host resolves into a blocked network ({', '.join(sorted(blocked))})"
            )
        if parts.scheme == "https":
            pass
        elif parts.scheme == "http" and all(_unwrap(item).is_loopback for item in addresses):
            if not self._allow_loopback:
                raise WebhookEgressRejected("plain http is allowed only to permitted loopback")
        else:
            raise WebhookEgressRejected("callback URL must be https")
        return PinnedDestination(url, host, port, parts.scheme, addresses[0])

    async def validate(self, url: str) -> None:
        await self.resolve(url)


class EgressGuardedWebhookTransport:
    """Validate, pin and POST; never follows redirects; never retries by itself."""

    def __init__(
        self,
        policy: EgressPolicy,
        client: httpx.AsyncClient | None = None,
        *,
        timeout: float = 10.0,
    ) -> None:
        self._policy = policy
        self._client = client or httpx.AsyncClient(timeout=timeout, follow_redirects=False)
        self._owned = client is None

    async def post(self, url: str, body: bytes, headers: Mapping[str, str]) -> WebhookResponse:
        destination = await self._policy.resolve(url)
        request_headers = {**dict(headers), "Host": destination.host_header}
        extensions = {"sni_hostname": destination.host} if destination.scheme == "https" else {}
        started = perf_counter()
        try:
            response = await self._client.post(
                destination.pinned_url,
                content=body,
                headers=request_headers,
                extensions=extensions,
            )
        except httpx.HTTPError as exc:
            raise WebhookTransportError(type(exc).__name__) from None
        return WebhookResponse(
            status_code=response.status_code,
            latency_ms=int((perf_counter() - started) * 1000),
        )

    async def aclose(self) -> None:
        if self._owned:
            await self._client.aclose()


__all__ = [
    "EgressGuardedWebhookTransport",
    "EgressPolicy",
    "HttpxWebhookTransport",
    "PinnedDestination",
    "system_resolver",
]
