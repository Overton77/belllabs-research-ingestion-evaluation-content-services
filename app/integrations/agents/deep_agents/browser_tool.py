"""The exact `agent_browser_page` Deep Agent tool over the pinned agent-browser CLI (RRM-009).

Research outbound access for a bounded Deep Agent is mediated by the worker: the sandbox
stays network-isolated, and this tool runs the reviewed `agent-browser` entrypoint as a
worker-side subprocess with a sanitized environment, a per-page host allowlist, bounded
output and a fresh single-host browser profile per call. The page's host must also be one
of the running operation's granted `network_hosts` (`granted_network_hosts`, bound by the
deployment runtime around each invocation). It returns the page's final URL,
title and a text excerpt as JSON text; it never returns screenshots or cookies.

The tool's input-schema digest is pinned in the capability pin file and verified by the
materializer (`_resolve_tool`), so the model-facing surface cannot drift silently.
"""

from __future__ import annotations

import json
import os
import re
import tempfile
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from contextvars import ContextVar
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from langchain_core.tools import BaseTool
from pydantic import BaseModel, ConfigDict, Field, PrivateAttr

from app.integrations.web_research_runtime import (
    AsyncioBrowserSubprocessRunner,
    BrowserSubprocessRequest,
    BrowserSubprocessRunner,
    WebResearchRuntimeDependencyError,
)

AGENT_BROWSER_PAGE_TOOL_NAME = "agent_browser_page"
# The network hosts the running operation's capability grant admits. The deployment runtime
# binds them around each invocation; a page outside them is refused before any subprocess.
GRANTED_NETWORK_HOSTS: ContextVar[frozenset[str] | None] = ContextVar(
    "belllabs_granted_network_hosts", default=None
)


@contextmanager
def granted_network_hosts(hosts: frozenset[str]) -> Iterator[None]:
    """Bind the operation's granted hosts for the tools its cognition runs."""

    token = GRANTED_NETWORK_HOSTS.set(frozenset(host.lower() for host in hosts))
    try:
        yield
    finally:
        GRANTED_NETWORK_HOSTS.reset(token)


_SESSION_SAFE = re.compile(r"[^A-Za-z0-9_-]+")
_BASE_ENVIRONMENT_KEYS = ("PATH", "PATHEXT", "SYSTEMROOT", "COMSPEC", "WINDIR")


class AgentBrowserPageInput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    url: str = Field(
        description="The public http(s) URL to open; only that page's host is reachable.",
        min_length=8,
        max_length=2_048,
    )


class AgentBrowserPageTool(BaseTool):
    """Open one public page with the pinned agent-browser and return its title and text."""

    name: str = AGENT_BROWSER_PAGE_TOOL_NAME
    description: str = (
        "Open one public web page in the governed browser and return JSON with its final "
        "URL, title and a text excerpt. Only the requested page's host is reachable."
    )
    args_schema: type[AgentBrowserPageInput] = AgentBrowserPageInput

    _node_executable: Path = PrivateAttr()
    _entrypoint: Path = PrivateAttr()
    _runner: BrowserSubprocessRunner = PrivateAttr()
    _command_timeout_seconds: float = PrivateAttr()
    _max_output_bytes: int = PrivateAttr()
    _excerpt_characters: int = PrivateAttr()

    def __init__(
        self,
        *,
        node_executable: Path,
        entrypoint: Path,
        runner: BrowserSubprocessRunner | None = None,
        command_timeout_seconds: float = 25,
        max_output_bytes: int = 250_000,
        excerpt_characters: int = 4_000,
    ) -> None:
        super().__init__()
        self._node_executable = node_executable.resolve(strict=True)
        self._entrypoint = entrypoint.resolve(strict=True)
        self._runner = runner or AsyncioBrowserSubprocessRunner()
        self._command_timeout_seconds = command_timeout_seconds
        self._max_output_bytes = max_output_bytes
        self._excerpt_characters = excerpt_characters

    def _run(self, url: str) -> str:
        raise NotImplementedError("agent_browser_page is invoked asynchronously")

    async def _arun(self, url: str) -> str:
        host = _public_host(url)
        granted = GRANTED_NETWORK_HOSTS.get()
        if granted is not None and host not in granted:
            raise WebResearchRuntimeDependencyError(
                "agent_browser_page host is outside the operation's granted network hosts"
            )
        with tempfile.TemporaryDirectory(
            prefix="belllabs-agent-browser-", ignore_cleanup_errors=True
        ) as directory:
            workspace = Path(directory)
            session = _SESSION_SAFE.sub("-", f"page-{host}-{workspace.name}").strip("-")[:64]
            environment = _environment(
                workspace, session=session, host=host, max_output_bytes=self._max_output_bytes
            )
            base = (
                str(self._entrypoint),
                "--allowed-domains",
                host,
                "--max-output",
                str(self._max_output_bytes),
                "--json",
            )
            try:
                await self._command(workspace, environment, (*base, "open", url))
                final_url = _scalar(
                    await self._command(workspace, environment, (*base, "get", "url"))
                )
                if _public_host(final_url) != host:
                    raise WebResearchRuntimeDependencyError(
                        "agent-browser redirected outside the requested host"
                    )
                title = _scalar(
                    await self._command(workspace, environment, (*base, "get", "title"))
                )
                excerpt = _scalar(
                    await self._command(
                        workspace,
                        environment,
                        (
                            *base,
                            "eval",
                            f"document.body?.innerText?.slice(0, {self._excerpt_characters}) ?? ''",
                        ),
                    )
                )
            finally:
                try:
                    await self._command(workspace, environment, (*base, "close"))
                except WebResearchRuntimeDependencyError:
                    pass
        return json.dumps(
            {
                "requested_url": url,
                "final_url": final_url,
                "title": title[:500],
                "text_excerpt": excerpt[: self._excerpt_characters],
            },
            ensure_ascii=False,
        )

    async def _command(
        self, workspace: Path, environment: Mapping[str, str], arguments: tuple[str, ...]
    ) -> object:
        result = await self._runner.run(
            BrowserSubprocessRequest(
                executable=self._node_executable,
                arguments=arguments,
                working_directory=workspace,
                environment=environment,
                timeout_seconds=self._command_timeout_seconds,
                max_output_bytes=self._max_output_bytes,
            )
        )
        if result.exit_code != 0:
            raise WebResearchRuntimeDependencyError(
                f"pinned agent-browser {arguments[6] if len(arguments) > 6 else 'command'} "
                f"exited with code {result.exit_code}"
            )
        text = result.stdout.decode("utf-8", errors="replace").strip()
        if not text:
            return {}
        try:
            return json.loads(text)
        except json.JSONDecodeError as error:
            raise WebResearchRuntimeDependencyError(
                "pinned agent-browser returned non-JSON output"
            ) from error


def _public_host(url: str) -> str:
    parts = urlsplit(url)
    host = (parts.hostname or "").lower()
    if parts.scheme not in {"http", "https"} or not host or host in {"localhost"}:
        raise WebResearchRuntimeDependencyError("agent_browser_page requires a public http(s) URL")
    # IP literals (IPv4, and IPv6, whose hostname contains ':') and local names are refused:
    # the tool reaches public hosts by name only.
    if host.endswith(".local") or ":" in host or re.fullmatch(r"\d+\.\d+\.\d+\.\d+", host):
        raise WebResearchRuntimeDependencyError("agent_browser_page refuses local addresses")
    return host


def _environment(
    workspace: Path, *, session: str, host: str, max_output_bytes: int
) -> dict[str, str]:
    environment = {key: os.environ[key] for key in _BASE_ENVIRONMENT_KEYS if key in os.environ}
    environment.update(
        {
            "CI": "1",
            "NO_COLOR": "1",
            "TEMP": str(workspace),
            "TMP": str(workspace),
            "AGENT_BROWSER_SESSION": session,
            "AGENT_BROWSER_NAMESPACE": session,
            "AGENT_BROWSER_SCREENSHOT_DIR": str(workspace),
            "AGENT_BROWSER_ALLOWED_DOMAINS": host,
            "AGENT_BROWSER_MAX_OUTPUT": str(max_output_bytes),
            "AGENT_BROWSER_DEFAULT_TIMEOUT": "20000",
            "AGENT_BROWSER_JSON": "1",
            "AGENT_BROWSER_RESTORE_SAVE": "never",
        }
    )
    return environment


def _scalar(value: Any) -> str:
    if isinstance(value, str):
        return value
    if isinstance(value, dict):
        for key in ("value", "text", "url", "title", "result"):
            item = value.get(key)
            if isinstance(item, str):
                return item
        data = value.get("data")
        if data is not None:
            return _scalar(data)
    raise WebResearchRuntimeDependencyError("pinned agent-browser response omitted its result")
