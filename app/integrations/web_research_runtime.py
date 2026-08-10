from __future__ import annotations

import asyncio
import json
import os
import re
import subprocess
import tempfile
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, Protocol
from urllib.parse import urlsplit

from mcp.types import TextContent, Tool
from pydantic import SecretStr

from app.application.artifact_promotion import ArtifactPayloadPort
from app.application.web_research_repository import (
    BeanieWebResearchRecordRepository,
)
from app.application.web_research_semantic_handlers import (
    AgentBrowserVerificationPort,
    FirecrawlSearchPort,
    TavilySearchPort,
    WebResearchHandlerDependencies,
)
from app.config import Settings
from app.domain.control_plane.contracts import ExactDefinitionRef
from app.domain.coordinator.web_research_runtime import (
    BrowserPageVerification,
    GovernedBrowserVerificationRequest,
    GovernedBrowserVerificationResponse,
    GovernedSearchRequest,
    GovernedSearchResponse,
    NormalizedSearchResult,
    ReviewedRuntimeArtifactBinding,
    normalized_public_url,
)

if TYPE_CHECKING:
    from fastmcp.client.client import CallToolResult
    from fastmcp.client.transports import ClientTransport
else:
    ClientTransport = Any

_PINNED_AGENT_BROWSER_VERSION = "0.33.0"
_FIRECRAWL_REVIEWED_MODULE_DIGEST = (
    "sha256:69e305ec3cf14ddbfe62a7c509e218a9ec4b44c82604bffa023159130769498b"
)
_FIRECRAWL_REVIEWED_TOOLS_DIGEST = (
    "sha256:b00747ddea6305fc08efcdd9fcaddcd69f62f0c3a59e2901d045475600c53bf2"
)
_FIRECRAWL_REVIEWED_TOOLS_CANONICAL_DIGEST = (
    "sha256:8513a7408398782f7a3a52b61d757bbbdfb25828e4a6a049d98bf00178209bfb"
)
_TAVILY_REVIEWED_MODULE_DIGEST = (
    "sha256:60d2f3d0553f4879225990fd42e43265244ef5ac6d02799f6bafa5aef2d2d05e"
)
_TAVILY_REVIEWED_TOOLS_DIGEST = (
    "sha256:65d256e03f0e82bb425b089cecf372f91f4c33b0c32fd2a94421475f2a9c922d"
)
_TAVILY_REVIEWED_TOOLS_CANONICAL_DIGEST = (
    "sha256:6d875c31d60cdd4a9f4c4cd1816cff7efe46bfef2cac1c06fa62f55b997628da"
)
_SESSION_SAFE = re.compile(r"[^a-zA-Z0-9_-]")


class WebResearchRuntimeDependencyError(RuntimeError):
    """A governed web provider or browser dependency failed closed."""


@dataclass(frozen=True)
class ReviewedWebResearchRuntimeArtifacts:
    firecrawl: ReviewedRuntimeArtifactBinding
    tavily: ReviewedRuntimeArtifactBinding
    browser: ReviewedRuntimeArtifactBinding


class BrowserScreenshotArtifactPort(Protocol):
    async def store(
        self,
        *,
        request_scope: str,
        run_id: str,
        idempotency_key: str,
        source_url: str,
        content: bytes,
        media_type: Literal["image/png"],
    ) -> str: ...


class ArtifactPayloadBrowserScreenshotAdapter:
    """Store screenshots in the configured durable artifact payload backend."""

    def __init__(self, payloads: ArtifactPayloadPort) -> None:
        self._payloads = payloads

    async def store(
        self,
        *,
        request_scope: str,
        run_id: str,
        idempotency_key: str,
        source_url: str,
        content: bytes,
        media_type: Literal["image/png"],
    ) -> str:
        digest = f"sha256:{sha256(content).hexdigest()}"
        artifact_id = (
            "browser-screenshot:"
            + sha256(
                (f"{request_scope}\0{run_id}\0{idempotency_key}\0{source_url}\0{digest}").encode()
            ).hexdigest()
        )
        address = await self._payloads.stage(
            artifact_id=artifact_id,
            content=content,
            content_digest=digest,
            media_type=media_type,
        )
        if not address.object_ref.startswith(("s3://", "gs://", "az://", "belllabs://")):
            raise WebResearchRuntimeDependencyError(
                "browser screenshot backend did not return a durable object reference"
            )
        return address.object_ref


class BrowserSubprocessRequest:
    def __init__(
        self,
        *,
        executable: Path,
        arguments: tuple[str, ...],
        working_directory: Path,
        environment: Mapping[str, str],
        timeout_seconds: float,
        max_output_bytes: int,
    ) -> None:
        self.executable = executable
        self.arguments = arguments
        self.working_directory = working_directory
        self.environment = dict(environment)
        self.timeout_seconds = timeout_seconds
        self.max_output_bytes = max_output_bytes


@dataclass(frozen=True)
class BrowserSubprocessResult:
    exit_code: int
    stdout: bytes
    stderr: bytes


class BrowserSubprocessRunner(Protocol):
    async def run(
        self,
        request: BrowserSubprocessRequest,
    ) -> BrowserSubprocessResult: ...


class AsyncioBrowserSubprocessRunner:
    """Run a pinned browser argv directly with bounded output and no shell."""

    async def run(
        self,
        request: BrowserSubprocessRequest,
    ) -> BrowserSubprocessResult:
        if os.name == "nt":
            try:
                return await asyncio.to_thread(_run_windows_browser_command, request)
            except subprocess.TimeoutExpired as error:
                raise WebResearchRuntimeDependencyError(
                    "pinned agent-browser command timed out"
                ) from error
        process = await asyncio.create_subprocess_exec(
            request.executable,
            *request.arguments,
            cwd=request.working_directory,
            env=request.environment,
            stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        assert process.stdout is not None
        assert process.stderr is not None
        try:
            async with asyncio.timeout(request.timeout_seconds):
                stdout, stderr, exit_code = await _collect_bounded_output(
                    process,
                    request.max_output_bytes,
                )
        except TimeoutError as error:
            process.kill()
            await process.wait()
            raise WebResearchRuntimeDependencyError(
                "pinned agent-browser command timed out"
            ) from error
        except BaseException:
            if process.returncode is None:
                process.kill()
                await process.wait()
            raise
        return BrowserSubprocessResult(
            exit_code=exit_code,
            stdout=stdout,
            stderr=stderr,
        )


def _run_windows_browser_command(
    request: BrowserSubprocessRequest,
) -> BrowserSubprocessResult:
    """Avoid Windows Proactor pipe inheritance across the browser daemon."""

    with tempfile.TemporaryFile() as stdout_file, tempfile.TemporaryFile() as stderr_file:
        completed = subprocess.run(
            [str(request.executable), *request.arguments],
            cwd=request.working_directory,
            env=request.environment,
            stdin=subprocess.DEVNULL,
            stdout=stdout_file,
            stderr=stderr_file,
            timeout=request.timeout_seconds,
            check=False,
        )
        stdout_file.seek(0)
        stderr_file.seek(0)
        stdout = stdout_file.read(request.max_output_bytes + 1)
        remaining = max(0, request.max_output_bytes + 1 - len(stdout))
        stderr = stderr_file.read(remaining)
    if len(stdout) + len(stderr) > request.max_output_bytes:
        raise WebResearchRuntimeDependencyError(
            "pinned agent-browser exceeded its configured output limit"
        )
    return BrowserSubprocessResult(
        exit_code=completed.returncode,
        stdout=stdout,
        stderr=stderr,
    )


class FirecrawlMCPSearchAdapter(FirecrawlSearchPort):
    def __init__(
        self,
        transport_factory: Callable[[], ClientTransport],
        *,
        exact_tool_ref: ExactDefinitionRef,
        runtime_artifact: ReviewedRuntimeArtifactBinding,
        timeout_seconds: float = 30,
        max_response_bytes: int = 1_000_000,
    ) -> None:
        _require_exact_tool(
            exact_tool_ref,
            logical_id="mcp.firecrawl:firecrawl_search",
        )
        self._transport_factory = transport_factory
        self._exact_tool_ref = exact_tool_ref
        self._runtime_artifact = runtime_artifact
        self._timeout_seconds = timeout_seconds
        self._max_response_bytes = max_response_bytes

    async def search(self, request: GovernedSearchRequest) -> GovernedSearchResponse:
        if request.exact_tool_ref != self._exact_tool_ref:
            raise WebResearchRuntimeDependencyError(
                "Firecrawl request is not bound to the configured exact tool revision"
            )
        if request.runtime_artifact != self._runtime_artifact:
            raise WebResearchRuntimeDependencyError(
                "Firecrawl request changed its reviewed runtime artifact"
            )
        payload = await _call_exact_search_tool(
            self._transport_factory,
            tool_name="firecrawl_search",
            arguments={
                "query": request.query,
                "limit": request.limit,
                **(
                    {"includeDomains": list(request.include_domains)}
                    if request.include_domains
                    else {}
                ),
                **(
                    {"excludeDomains": list(request.exclude_domains)}
                    if request.exclude_domains
                    else {}
                ),
                "sources": [{"type": "web"}],
            },
            timeout_seconds=self._timeout_seconds,
            max_response_bytes=self._max_response_bytes,
            expected_tools_snapshot_digest=(_FIRECRAWL_REVIEWED_TOOLS_CANONICAL_DIGEST),
        )
        return _normalized_search_response(
            payload,
            request.limit,
            candidates=_firecrawl_candidates(payload),
        )


class TavilyMCPSearchAdapter(TavilySearchPort):
    def __init__(
        self,
        transport_factory: Callable[[], ClientTransport],
        *,
        exact_tool_ref: ExactDefinitionRef,
        runtime_artifact: ReviewedRuntimeArtifactBinding,
        timeout_seconds: float = 30,
        max_response_bytes: int = 1_000_000,
    ) -> None:
        _require_exact_tool(
            exact_tool_ref,
            logical_id="mcp.tavily:tavily_search",
        )
        self._transport_factory = transport_factory
        self._exact_tool_ref = exact_tool_ref
        self._runtime_artifact = runtime_artifact
        self._timeout_seconds = timeout_seconds
        self._max_response_bytes = max_response_bytes

    async def search(self, request: GovernedSearchRequest) -> GovernedSearchResponse:
        if request.exact_tool_ref != self._exact_tool_ref:
            raise WebResearchRuntimeDependencyError(
                "Tavily request is not bound to the configured exact tool revision"
            )
        if request.runtime_artifact != self._runtime_artifact:
            raise WebResearchRuntimeDependencyError(
                "Tavily request changed its reviewed runtime artifact"
            )
        payload = await _call_exact_search_tool(
            self._transport_factory,
            tool_name="tavily_search",
            arguments={
                "query": request.query,
                "max_results": request.limit,
                **(
                    {"include_domains": list(request.include_domains)}
                    if request.include_domains
                    else {}
                ),
                **(
                    {"exclude_domains": list(request.exclude_domains)}
                    if request.exclude_domains
                    else {}
                ),
            },
            timeout_seconds=self._timeout_seconds,
            max_response_bytes=self._max_response_bytes,
            expected_tools_snapshot_digest=(_TAVILY_REVIEWED_TOOLS_CANONICAL_DIGEST),
        )
        return _normalized_search_response(
            payload,
            request.limit,
            candidates=_tavily_candidates(payload),
        )


class AgentBrowserSubprocessAdapter(AgentBrowserVerificationPort):
    """Verify public pages with a pinned, isolated, read-only agent-browser CLI."""

    def __init__(
        self,
        runner: BrowserSubprocessRunner,
        *,
        node_executable: Path,
        agent_browser_entrypoint: Path,
        screenshot_artifacts: BrowserScreenshotArtifactPort,
        runtime_artifact: ReviewedRuntimeArtifactBinding,
        timeout_seconds: float = 90,
        command_timeout_seconds: float = 25,
        max_output_bytes: int = 250_000,
        maximum_screenshot_bytes: int = 10_000_000,
    ) -> None:
        self._runner = runner
        self._node_executable = node_executable.resolve(strict=True)
        self._entrypoint = agent_browser_entrypoint.resolve(strict=True)
        _verify_agent_browser_version(self._entrypoint)
        actual_digest = "sha256:" + sha256(self._entrypoint.read_bytes()).hexdigest()
        if (
            runtime_artifact.package_name != "agent-browser"
            or runtime_artifact.package_version != _PINNED_AGENT_BROWSER_VERSION
            or runtime_artifact.module_digest != actual_digest
        ):
            raise WebResearchRuntimeDependencyError(
                "agent-browser entrypoint differs from its reviewed runtime binding"
            )
        self._runtime_artifact = runtime_artifact
        self._screenshot_artifacts = screenshot_artifacts
        self._timeout_seconds = timeout_seconds
        self._command_timeout_seconds = command_timeout_seconds
        self._max_output_bytes = max_output_bytes
        self._maximum_screenshot_bytes = maximum_screenshot_bytes

    async def verify(
        self,
        request: GovernedBrowserVerificationRequest,
    ) -> GovernedBrowserVerificationResponse:
        _require_exact_skill(request.exact_skill_ref)
        if request.runtime_artifact != self._runtime_artifact:
            raise WebResearchRuntimeDependencyError(
                "agent-browser request changed its reviewed runtime artifact"
            )
        normalized_urls = tuple(normalized_public_url(url) for url in request.urls)
        allowed_domains = tuple(
            dict.fromkeys(urlsplit(url).hostname or "" for url in normalized_urls)
        )
        if not all(allowed_domains):
            raise WebResearchRuntimeDependencyError(
                "agent-browser verification requires public URL hostnames"
            )
        pages: list[BrowserPageVerification] = []
        try:
            async with asyncio.timeout(self._timeout_seconds):
                for index, url in enumerate(normalized_urls):
                    page_domain = urlsplit(url).hostname or ""
                    for invocation_attempt in range(3):
                        retry_bootstrap = False
                        verified_page: BrowserPageVerification | None = None
                        # The pinned Windows runtime can lose its CDP target when one
                        # contained session crosses unrelated source domains. Keep each
                        # evidence page in a fresh, single-host daemon and browser profile.
                        with tempfile.TemporaryDirectory(
                            prefix="belllabs-agent-browser-",
                            ignore_cleanup_errors=True,
                        ) as directory:
                            workspace = Path(directory)
                            session = _session_id(
                                request.idempotency_key,
                                invocation=f"{index}:{workspace.name}",
                            )
                            environment = _browser_environment(
                                workspace,
                                session=session,
                                allowed_domains=(page_domain,),
                                max_output_bytes=self._max_output_bytes,
                            )
                            base_arguments = (
                                str(self._entrypoint),
                                "--allowed-domains",
                                page_domain,
                                "--max-output",
                                str(self._max_output_bytes),
                                "--json",
                            )
                            try:
                                verified_page = await self._verify_page(
                                    request=request,
                                    url=url,
                                    index=index,
                                    workspace=workspace,
                                    environment=environment,
                                    base_arguments=base_arguments,
                                    allowed_domains=frozenset({page_domain}),
                                )
                            except WebResearchRuntimeDependencyError as error:
                                if (
                                    invocation_attempt == 2
                                    or not _is_transient_browser_bootstrap_failure(error)
                                ):
                                    raise
                                retry_bootstrap = True
                            finally:
                                await self._close(
                                    workspace=workspace,
                                    environment=environment,
                                    base_arguments=base_arguments,
                                )
                        if not retry_bootstrap:
                            assert verified_page is not None
                            pages.append(verified_page)
                            break
                        await asyncio.sleep(0.5 * (invocation_attempt + 1))
        except TimeoutError as error:
            raise WebResearchRuntimeDependencyError(
                "pinned agent-browser verification exceeded its total time limit"
            ) from error
        if not pages:
            raise WebResearchRuntimeDependencyError(
                "pinned agent-browser returned no verification evidence"
            )
        return GovernedBrowserVerificationResponse(pages=tuple(pages))

    async def _verify_page(
        self,
        *,
        request: GovernedBrowserVerificationRequest,
        url: str,
        index: int,
        workspace: Path,
        environment: Mapping[str, str],
        base_arguments: tuple[str, ...],
        allowed_domains: frozenset[str],
    ) -> BrowserPageVerification:
        await self._command(
            workspace,
            environment,
            base_arguments + ("open", url),
        )
        final_url = _extract_scalar(
            await self._command(
                workspace,
                environment,
                base_arguments + ("get", "url"),
            )
        )
        try:
            final_url = normalized_public_url(final_url)
        except ValueError as error:
            raise WebResearchRuntimeDependencyError(
                "pinned agent-browser returned a non-public final URL"
            ) from error
        if (urlsplit(final_url).hostname or "") not in allowed_domains:
            raise WebResearchRuntimeDependencyError(
                "agent-browser redirected outside the exact public domain allowlist"
            )
        title = _extract_scalar(
            await self._command(
                workspace,
                environment,
                base_arguments + ("get", "title"),
            )
        )
        excerpt = _extract_scalar(
            await self._command(
                workspace,
                environment,
                base_arguments
                + (
                    "eval",
                    "document.body?.innerText?.slice(0, 4000) ?? ''",
                ),
            )
        )
        screenshot_path = workspace / f"verification-{index}.png"
        await self._command(
            workspace,
            environment,
            base_arguments + ("screenshot", str(screenshot_path)),
        )
        content = await asyncio.to_thread(screenshot_path.read_bytes)
        if not content or len(content) > self._maximum_screenshot_bytes:
            raise WebResearchRuntimeDependencyError(
                "agent-browser screenshot is empty or exceeds its size bound"
            )
        screenshot_ref = await self._screenshot_artifacts.store(
            request_scope=request.request_scope,
            run_id=request.run_id,
            idempotency_key=f"{request.idempotency_key}:screenshot:{index}",
            source_url=final_url,
            content=content,
            media_type="image/png",
        )
        return BrowserPageVerification(
            requested_url=url,
            final_url=final_url,
            status_code=200,
            title=title[:500],
            text_excerpt=excerpt[:4_000],
            screenshot_ref=screenshot_ref,
            verified=bool(title.strip() and excerpt.strip()),
        )

    async def _command(
        self,
        workspace: Path,
        environment: Mapping[str, str],
        arguments: tuple[str, ...],
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
            detail = _browser_failure_detail(result)
            command_name = arguments[6] if len(arguments) > 6 else "unknown"
            raise WebResearchRuntimeDependencyError(
                f"pinned agent-browser {command_name} exited with code {result.exit_code}"
                + (f": {detail}" if detail else "")
            )
        if len(result.stdout) + len(result.stderr) > self._max_output_bytes:
            raise WebResearchRuntimeDependencyError(
                "pinned agent-browser exceeded its configured output limit"
            )
        return _decode_json_output(result.stdout)

    async def _close(
        self,
        *,
        workspace: Path,
        environment: Mapping[str, str],
        base_arguments: tuple[str, ...],
    ) -> None:
        try:
            await self._command(
                workspace,
                environment,
                base_arguments + ("close",),
            )
        except Exception:
            return


def stdio_mcp_transport_factory(
    *,
    command: Path,
    arguments: tuple[str, ...],
    working_directory: Path,
    credential_environment_name: Literal[
        "FIRECRAWL_API_KEY",
        "TAVILY_API_KEY",
    ],
    credential: SecretStr,
    additional_path: Path | None = None,
) -> Callable[[], ClientTransport]:
    """Create a fresh sanitized stdio transport; credentials exist only in child env."""

    resolved_command = command.resolve(strict=True)
    resolved_working_directory = working_directory.resolve(strict=True)
    environment = _base_environment()
    if additional_path is not None:
        resolved_path = additional_path.resolve(strict=True)
        environment["PATH"] = str(resolved_path) + os.pathsep + environment.get("PATH", "")
    environment[credential_environment_name] = credential.get_secret_value()
    environment.update({"CI": "1", "NO_COLOR": "1"})

    def create() -> ClientTransport:
        # FastMCP installs import instrumentation when imported. Keep the import
        # on the activity edge so Temporal can construct its workflow sandbox
        # before any provider subprocess is opened.
        from fastmcp.client.transports import StdioTransport

        return StdioTransport(
            command=str(resolved_command),
            args=list(arguments),
            env=environment,
            cwd=str(resolved_working_directory),
        )

    return create


def build_live_web_research_handler_dependencies(
    *,
    settings: Settings,
    firecrawl_tool_ref: ExactDefinitionRef,
    tavily_tool_ref: ExactDefinitionRef,
    screenshot_artifacts: BrowserScreenshotArtifactPort,
    browser_runner: BrowserSubprocessRunner | None = None,
) -> WebResearchHandlerDependencies:
    """Build the live edge from typed Settings; no credential enters a binding."""

    if settings.firecrawl_api_key is None:
        raise WebResearchRuntimeDependencyError("Firecrawl MCP credential is required")
    if settings.tavily_api_key is None:
        raise WebResearchRuntimeDependencyError("Tavily MCP credential is required")
    if settings.web_research_agent_browser_node is None:
        raise WebResearchRuntimeDependencyError("pinned agent-browser Node executable is required")
    artifacts = attest_reviewed_web_research_runtime(settings)
    firecrawl_module = _verify_node_package_module(
        settings.web_research_firecrawl_mcp_module,
        package_name="firecrawl-mcp",
        package_version="3.22.4",
        expected_module_digest=_FIRECRAWL_REVIEWED_MODULE_DIGEST,
    )
    tavily_module = _verify_node_package_module(
        settings.web_research_tavily_mcp_module,
        package_name="tavily-mcp",
        package_version="0.2.21",
        expected_module_digest=_TAVILY_REVIEWED_MODULE_DIGEST,
    )
    records = BeanieWebResearchRecordRepository()
    return WebResearchHandlerDependencies(
        firecrawl=FirecrawlMCPSearchAdapter(
            stdio_mcp_transport_factory(
                command=(
                    settings.web_research_firecrawl_mcp_command
                    or settings.web_research_agent_browser_node
                ),
                arguments=(
                    str(firecrawl_module),
                    *settings.web_research_firecrawl_mcp_arguments,
                ),
                working_directory=firecrawl_module.parent.parent,
                credential_environment_name="FIRECRAWL_API_KEY",
                credential=settings.firecrawl_api_key,
            ),
            exact_tool_ref=firecrawl_tool_ref,
            runtime_artifact=artifacts.firecrawl,
            timeout_seconds=settings.web_research_mcp_timeout_seconds,
            max_response_bytes=settings.web_research_max_provider_output_bytes,
        ),
        tavily=TavilyMCPSearchAdapter(
            stdio_mcp_transport_factory(
                command=(
                    settings.web_research_tavily_mcp_command
                    or settings.web_research_agent_browser_node
                ),
                arguments=(
                    str(tavily_module),
                    *settings.web_research_tavily_mcp_arguments,
                ),
                working_directory=tavily_module.parent.parent,
                credential_environment_name="TAVILY_API_KEY",
                credential=settings.tavily_api_key,
            ),
            exact_tool_ref=tavily_tool_ref,
            runtime_artifact=artifacts.tavily,
            timeout_seconds=settings.web_research_mcp_timeout_seconds,
            max_response_bytes=settings.web_research_max_provider_output_bytes,
        ),
        browser=AgentBrowserSubprocessAdapter(
            browser_runner or AsyncioBrowserSubprocessRunner(),
            node_executable=settings.web_research_agent_browser_node,
            agent_browser_entrypoint=(settings.web_research_agent_browser_entrypoint),
            screenshot_artifacts=screenshot_artifacts,
            runtime_artifact=artifacts.browser,
            timeout_seconds=settings.web_research_browser_timeout_seconds,
            command_timeout_seconds=(settings.web_research_browser_command_timeout_seconds),
            max_output_bytes=settings.web_research_max_browser_output_bytes,
        ),
        records=records,
    )


def attest_reviewed_web_research_runtime(
    settings: Settings,
) -> ReviewedWebResearchRuntimeArtifacts:
    """Read and hash every durable workspace runtime before freezing a run."""

    _verify_node_package_module(
        settings.web_research_firecrawl_mcp_module,
        package_name="firecrawl-mcp",
        package_version="3.22.4",
        expected_module_digest=_FIRECRAWL_REVIEWED_MODULE_DIGEST,
    )
    _verify_node_package_module(
        settings.web_research_tavily_mcp_module,
        package_name="tavily-mcp",
        package_version="0.2.21",
        expected_module_digest=_TAVILY_REVIEWED_MODULE_DIGEST,
    )
    browser_entrypoint = settings.web_research_agent_browser_entrypoint.resolve(strict=True)
    _verify_agent_browser_version(browser_entrypoint)
    browser_digest = "sha256:" + sha256(browser_entrypoint.read_bytes()).hexdigest()
    if browser_digest != (
        "sha256:8e382f4a5ba22f45e1e0339abfe5a55ed95a19540b16a69ee3faf31c8dc8216a"
    ):
        raise WebResearchRuntimeDependencyError(
            "agent-browser entrypoint differs from its reviewed artifact digest"
        )
    return ReviewedWebResearchRuntimeArtifacts(
        firecrawl=ReviewedRuntimeArtifactBinding(
            package_name="firecrawl-mcp",
            package_version="3.22.4",
            module_locator=(
                "workspace://.tools/reviewed/firecrawl-mcp-"
                "7232b6d1cdd80335107d53a33b80c902b515a334/dist/index.js"
            ),
            module_digest=_FIRECRAWL_REVIEWED_MODULE_DIGEST,
            tools_snapshot_digest=_FIRECRAWL_REVIEWED_TOOLS_DIGEST,
            commit_digest="7232b6d1cdd80335107d53a33b80c902b515a334",
        ),
        tavily=ReviewedRuntimeArtifactBinding(
            package_name="tavily-mcp",
            package_version="0.2.21",
            module_locator=("workspace://.tools/node_modules/tavily-mcp/build/index.js"),
            module_digest=_TAVILY_REVIEWED_MODULE_DIGEST,
            tools_snapshot_digest=_TAVILY_REVIEWED_TOOLS_DIGEST,
            commit_digest="259bfd205de90d74a131e9d2b29cb69ebe11feb7",
        ),
        browser=ReviewedRuntimeArtifactBinding(
            package_name="agent-browser",
            package_version="0.33.0",
            module_locator=("workspace://.tools/node_modules/agent-browser/bin/agent-browser.js"),
            module_digest=browser_digest,
            commit_digest="3cc7022271235694b5b5ce8aaea8bbfaa66e8cd5",
        ),
    )


def _verify_node_package_module(
    module_path: Path,
    *,
    package_name: str,
    package_version: str,
    expected_module_digest: str,
) -> Path:
    module = module_path.resolve(strict=True)
    package_path = module.parent.parent / "package.json"
    try:
        package = json.loads(package_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise WebResearchRuntimeDependencyError(
            f"reviewed {package_name} package metadata is unavailable"
        ) from error
    if package.get("name") != package_name or package.get("version") != package_version:
        raise WebResearchRuntimeDependencyError(
            f"{package_name} must be pinned to {package_version}"
        )
    actual_module_digest = "sha256:" + sha256(module.read_bytes()).hexdigest()
    if actual_module_digest != expected_module_digest:
        raise WebResearchRuntimeDependencyError(
            f"{package_name} module does not match its reviewed artifact digest"
        )
    return module


async def _call_exact_search_tool(
    transport_factory: Callable[[], ClientTransport],
    *,
    tool_name: Literal["firecrawl_search", "tavily_search"],
    arguments: dict[str, object],
    timeout_seconds: float,
    max_response_bytes: int,
    expected_tools_snapshot_digest: str,
) -> object:
    from fastmcp import Client
    from fastmcp.client.client import CallToolResult

    async with Client(transport_factory(), timeout=timeout_seconds) as client:
        tools = await client.list_tools()
        matches = [tool for tool in tools if tool.name == tool_name]
        if len(matches) != 1:
            raise WebResearchRuntimeDependencyError(
                f"MCP server does not expose exact required tool: {tool_name}"
            )
        snapshot_digest = _tools_snapshot_digest(tools)
        if snapshot_digest != expected_tools_snapshot_digest:
            raise WebResearchRuntimeDependencyError(
                "MCP tools/list differs from reviewed snapshot: "
                f"{tool_name}; expected={expected_tools_snapshot_digest}; "
                f"actual={snapshot_digest}"
            )
        result = await client.call_tool(
            tool_name,
            arguments,
            timeout=timeout_seconds,
        )
    if not isinstance(result, CallToolResult) or result.is_error:
        raise WebResearchRuntimeDependencyError(f"MCP search tool returned an error: {tool_name}")
    payload = _mcp_result_payload(result, tool_name=tool_name)
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        default=str,
    ).encode("utf-8")
    if len(encoded) > max_response_bytes:
        raise WebResearchRuntimeDependencyError(
            f"MCP search tool exceeded its response bound: {tool_name}"
        )
    return payload


def _tools_snapshot_digest(tools: list[Tool]) -> str:
    snapshot = {
        "snapshot_format": "mcp-tools-list/1",
        "tools": [
            tool.model_dump(mode="json", exclude_none=True)
            for tool in sorted(tools, key=lambda item: item.name)
        ],
    }
    return _canonical_tools_snapshot_digest(snapshot)


def _canonical_tools_snapshot_digest(snapshot: object) -> str:
    """Hash parsed snapshot meaning; raw reviewed-file hashes remain provenance."""

    raw = (json.dumps(snapshot, sort_keys=True, separators=(",", ":")) + "\n").encode()
    return "sha256:" + sha256(raw).hexdigest()


def _mcp_result_payload(
    result: CallToolResult,
    *,
    tool_name: Literal["firecrawl_search", "tavily_search"],
) -> object:
    if result.data is not None:
        return result.data
    if result.structured_content is not None:
        return result.structured_content
    text = "\n".join(item.text for item in result.content if isinstance(item, TextContent))
    try:
        return json.loads(text)
    except json.JSONDecodeError as error:
        if tool_name == "tavily_search":
            return _parse_pinned_tavily_search_text(text)
        raise WebResearchRuntimeDependencyError(
            "MCP search tool returned non-JSON evidence"
        ) from error


_TAVILY_RESULT_BLOCK = re.compile(
    r"(?:^|\n\n)Title: (?P<title>[^\n]+)\n"
    r"URL: (?P<url>[^\n]+)\n"
    r"Content: (?P<content>.*?)"
    r"(?=\n\nTitle: [^\n]+\nURL: |\Z)",
    re.DOTALL,
)


def _parse_pinned_tavily_search_text(text: str) -> dict[str, object]:
    """Decode the exact human-readable envelope emitted by tavily-mcp 0.2.21."""

    normalized = text.replace("\r\n", "\n")
    if "\r" in normalized or not normalized.startswith("Detailed Results:\n\n"):
        raise WebResearchRuntimeDependencyError(
            "Tavily MCP search response does not match its reviewed text envelope"
        )
    body = normalized.removeprefix("Detailed Results:\n\n")
    results: list[dict[str, str]] = []
    cursor = 0
    for match in _TAVILY_RESULT_BLOCK.finditer(body):
        if match.start() != cursor:
            raise WebResearchRuntimeDependencyError(
                "Tavily MCP search response contains unreviewed text fields"
            )
        title = match.group("title").strip()
        url = match.group("url").strip()
        content = match.group("content").strip()
        parsed_url = urlsplit(url)
        if (
            not title
            or not content
            or any(marker in content for marker in ("\nRaw Content:", "\nFavicon:", "\nImages:"))
            or parsed_url.scheme not in {"http", "https"}
            or not parsed_url.hostname
        ):
            raise WebResearchRuntimeDependencyError(
                "Tavily MCP search response contains an invalid result"
            )
        results.append({"title": title, "url": url, "content": content})
        cursor = match.end()
    if not results or cursor != len(body):
        raise WebResearchRuntimeDependencyError(
            "Tavily MCP search response contains no complete reviewed results"
        )
    return {"results": results}


def _firecrawl_candidates(payload: object) -> list[Mapping[str, object]]:
    value = _mapping(payload)
    data = _mapping(value.get("data", value))
    candidates = data.get("web", data.get("results", ()))
    return _mapping_list(candidates)


def _tavily_candidates(payload: object) -> list[Mapping[str, object]]:
    value = _mapping(payload)
    candidates = value.get("results")
    if candidates is None:
        candidates = _mapping(value.get("data")).get("results")
    return _mapping_list(candidates or ())


def _normalized_search_response(
    payload: object,
    limit: int,
    *,
    candidates: list[Mapping[str, object]],
) -> GovernedSearchResponse:
    results: list[NormalizedSearchResult] = []
    for item in candidates[:limit]:
        url = item.get("url")
        title = item.get("title") or item.get("name") or "Untitled public source"
        snippet = item.get("description") or item.get("content") or item.get("snippet") or ""
        if not isinstance(url, str):
            continue
        try:
            results.append(
                NormalizedSearchResult(
                    title=str(title)[:500],
                    url=url,
                    snippet=str(snippet)[:4_000],
                    published_at=(
                        str(item["publishedDate"])[:100]
                        if item.get("publishedDate") is not None
                        else None
                    ),
                )
            )
        except ValueError:
            continue
    request_id = _mapping(payload).get("id")
    return GovernedSearchResponse(
        results=tuple(results),
        provider_request_id=(str(request_id)[:300] if request_id is not None else None),
    )


def _require_exact_tool(ref: ExactDefinitionRef, *, logical_id: str) -> None:
    if ref.kind.value != "mcp_tool" or ref.logical_id != logical_id:
        raise ValueError(f"adapter requires exact governed MCP tool: {logical_id}")


def _require_exact_skill(ref: ExactDefinitionRef) -> None:
    if ref.kind.value != "skill" or ref.logical_id != "skill.agent-browser":
        raise WebResearchRuntimeDependencyError(
            "browser request is not bound to exact skill.agent-browser"
        )


def _verify_agent_browser_version(entrypoint: Path) -> None:
    package_path = entrypoint.parent.parent / "package.json"
    try:
        package = json.loads(package_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise WebResearchRuntimeDependencyError(
            "agent-browser package metadata is unavailable"
        ) from error
    if (
        package.get("name") != "agent-browser"
        or package.get("version") != _PINNED_AGENT_BROWSER_VERSION
    ):
        raise WebResearchRuntimeDependencyError(
            f"agent-browser must be pinned to {_PINNED_AGENT_BROWSER_VERSION}"
        )


def _browser_environment(
    workspace: Path,
    *,
    session: str,
    allowed_domains: tuple[str, ...],
    max_output_bytes: int,
) -> dict[str, str]:
    environment = _base_environment()
    environment.update(
        {
            "CI": "1",
            "NO_COLOR": "1",
            "TEMP": str(workspace),
            "TMP": str(workspace),
            "AGENT_BROWSER_SESSION": session,
            "AGENT_BROWSER_NAMESPACE": session,
            "AGENT_BROWSER_SCREENSHOT_DIR": str(workspace),
            "AGENT_BROWSER_ALLOWED_DOMAINS": ",".join(allowed_domains),
            "AGENT_BROWSER_MAX_OUTPUT": str(max_output_bytes),
            "AGENT_BROWSER_DEFAULT_TIMEOUT": "20000",
            "AGENT_BROWSER_JSON": "1",
            "AGENT_BROWSER_RESTORE_SAVE": "never",
        }
    )
    return environment


def _base_environment() -> dict[str, str]:
    allowed = ("PATH", "PATHEXT", "SYSTEMROOT", "COMSPEC", "WINDIR")
    return {key: os.environ[key] for key in allowed if key in os.environ}


def _session_id(idempotency_key: str, *, invocation: str = "") -> str:
    normalized = _SESSION_SAFE.sub("-", idempotency_key).strip("-")
    if not normalized:
        return "belllabs-web-research"
    identity_digest = sha256(f"{idempotency_key}\0{invocation}".encode()).hexdigest()[:12]
    return f"belllabs-{identity_digest}"


def _decode_json_output(output: bytes) -> object:
    text = output.decode("utf-8", errors="replace").strip()
    if not text:
        return {}
    try:
        return json.loads(text)
    except json.JSONDecodeError as error:
        raise WebResearchRuntimeDependencyError(
            "pinned agent-browser returned non-JSON output"
        ) from error


def _browser_failure_detail(result: BrowserSubprocessResult) -> str | None:
    """Expose bounded pinned-runtime diagnostics without echoing arbitrary page output."""

    stderr = result.stderr.decode("utf-8", errors="replace").strip()
    if stderr:
        return _sanitized_browser_diagnostic(stderr)
    try:
        payload = json.loads(result.stdout.decode("utf-8", errors="replace"))
    except (json.JSONDecodeError, UnicodeDecodeError):
        return None
    if not isinstance(payload, dict):
        return None
    error = payload.get("error")
    if isinstance(error, str):
        return _sanitized_browser_diagnostic(error)
    if isinstance(error, dict):
        message = error.get("message")
        code = error.get("code")
        if isinstance(message, str):
            prefix = f"{code}: " if isinstance(code, str) else ""
            return _sanitized_browser_diagnostic(prefix + message)
    return None


def _is_transient_browser_bootstrap_failure(
    error: WebResearchRuntimeDependencyError,
) -> bool:
    message = str(error)
    return (
        "CDP error (Page.enable): Session with given id not found" in message
        or "pinned agent-browser returned a non-public final URL" in message
    )


def _sanitized_browser_diagnostic(value: str) -> str:
    compact = " ".join(value.split())
    redacted = re.sub(
        r"(?i)\b(api[_-]?key|authorization|password|secret|token)"
        r"(\s*[:=]\s*)(\S+)",
        r"\1\2[REDACTED]",
        compact,
    )
    return redacted[:2_000]


def _extract_scalar(value: object) -> str:
    if isinstance(value, str):
        return value
    if isinstance(value, dict):
        for key in ("value", "text", "url", "title", "result"):
            item = value.get(key)
            if isinstance(item, str):
                return item
        data = value.get("data")
        if data is not None:
            return _extract_scalar(data)
    raise WebResearchRuntimeDependencyError(
        "pinned agent-browser response omitted its scalar result"
    )


async def _collect_bounded_output(
    process: asyncio.subprocess.Process,
    max_output_bytes: int,
) -> tuple[bytes, bytes, int]:
    assert process.stdout is not None
    assert process.stderr is not None
    combined_size = 0
    size_lock = asyncio.Lock()
    stdout_buffer = bytearray()
    stderr_buffer = bytearray()

    async def read(stream: asyncio.StreamReader, buffer: bytearray) -> None:
        nonlocal combined_size
        while chunk := await stream.read(8_192):
            async with size_lock:
                combined_size += len(chunk)
                if combined_size > max_output_bytes:
                    raise WebResearchRuntimeDependencyError(
                        "pinned agent-browser exceeded its configured output limit"
                    )
            buffer.extend(chunk)

    readers = (
        asyncio.create_task(read(process.stdout, stdout_buffer)),
        asyncio.create_task(read(process.stderr, stderr_buffer)),
    )
    try:
        exit_code = await process.wait()
        try:
            await asyncio.wait_for(asyncio.gather(*readers), timeout=1.0)
        except TimeoutError:
            # The Windows agent-browser daemon can inherit the command's pipe
            # handles after the command process exits. Its output is complete,
            # but EOF will not arrive until the daemon closes. Bound the drain
            # instead of converting successful commands into false timeouts.
            for reader in readers:
                reader.cancel()
            await asyncio.gather(*readers, return_exceptions=True)
    except BaseException:
        for reader in readers:
            reader.cancel()
        await asyncio.gather(*readers, return_exceptions=True)
        raise
    return bytes(stdout_buffer), bytes(stderr_buffer), exit_code


def _mapping(value: object) -> Mapping[str, object]:
    return value if isinstance(value, dict) else {}


def _mapping_list(value: object) -> list[Mapping[str, object]]:
    if not isinstance(value, list | tuple):
        return []
    return [item for item in value if isinstance(item, dict)]
