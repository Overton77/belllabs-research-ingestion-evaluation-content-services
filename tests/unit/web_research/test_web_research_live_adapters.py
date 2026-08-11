from __future__ import annotations

import asyncio
import json
from hashlib import sha256
from pathlib import Path
from types import SimpleNamespace

import pytest
from mcp.types import TextContent

from app.domain.control_plane.contracts import DefinitionKind, ExactDefinitionRef
from app.domain.coordinator.web_research_runtime import (
    GovernedBrowserVerificationRequest,
    GovernedSearchRequest,
    ReviewedRuntimeArtifactBinding,
)
from app.integrations import web_research_runtime
from app.integrations.web_research_runtime import (
    AgentBrowserSubprocessAdapter,
    BrowserSubprocessRequest,
    BrowserSubprocessResult,
    FirecrawlMCPSearchAdapter,
    TavilyMCPSearchAdapter,
    WebResearchRuntimeDependencyError,
)


def exact_ref(kind: DefinitionKind, logical_id: str, digit: str) -> ExactDefinitionRef:
    return ExactDefinitionRef(
        kind=kind,
        logical_id=logical_id,
        revision=2,
        digest="sha256:" + digit * 64,
    )


FIRECRAWL_REF = exact_ref(
    DefinitionKind.MCP_TOOL,
    "mcp.firecrawl:firecrawl_search",
    "1",
)
TAVILY_REF = exact_ref(
    DefinitionKind.MCP_TOOL,
    "mcp.tavily:tavily_search",
    "2",
)
BROWSER_REF = exact_ref(
    DefinitionKind.SKILL,
    "skill.agent-browser",
    "3",
)
FIRECRAWL_RUNTIME = ReviewedRuntimeArtifactBinding(
    package_name="firecrawl-mcp",
    package_version="3.22.4",
    module_locator="workspace://.tools/reviewed/firecrawl/dist/index.js",
    module_digest="sha256:69e305ec3cf14ddbfe62a7c509e218a9ec4b44c82604bffa023159130769498b",
    tools_snapshot_digest="sha256:b00747ddea6305fc08efcdd9fcaddcd69f62f0c3a59e2901d045475600c53bf2",
)
TAVILY_RUNTIME = ReviewedRuntimeArtifactBinding(
    package_name="tavily-mcp",
    package_version="0.2.21",
    module_locator="workspace://.tools/node_modules/tavily-mcp/build/index.js",
    module_digest="sha256:60d2f3d0553f4879225990fd42e43265244ef5ac6d02799f6bafa5aef2d2d05e",
    tools_snapshot_digest="sha256:65d256e03f0e82bb425b089cecf372f91f4c33b0c32fd2a94421475f2a9c922d",
)
BROWSER_RUNTIME = ReviewedRuntimeArtifactBinding(
    package_name="agent-browser",
    package_version="0.33.0",
    module_locator="workspace://.tools/node_modules/agent-browser/bin/agent-browser.js",
    module_digest="sha256:8e382f4a5ba22f45e1e0339abfe5a55ed95a19540b16a69ee3faf31c8dc8216a",
)


def test_tools_snapshot_attestation_is_line_ending_stable_and_drift_sensitive() -> None:
    snapshot = {
        "snapshot_format": "mcp-tools-list/1",
        "tools": [
            {
                "name": "search",
                "description": "Search public sources.",
                "inputSchema": {
                    "type": "object",
                    "properties": {"query": {"type": "string"}},
                    "required": ["query"],
                },
            }
        ],
    }
    lf = json.dumps(snapshot, indent=2) + "\n"
    crlf = lf.replace("\n", "\r\n")

    expected = web_research_runtime._canonical_tools_snapshot_digest(
        json.loads(lf)
    )
    assert (
        web_research_runtime._canonical_tools_snapshot_digest(json.loads(crlf))
        == expected
    )
    drifted = json.loads(lf)
    drifted["tools"][0]["inputSchema"]["properties"]["limit"] = {
        "type": "integer"
    }
    assert (
        web_research_runtime._canonical_tools_snapshot_digest(drifted)
        != expected
    )


def test_pinned_tavily_text_envelope_is_decoded_without_accepting_arbitrary_text() -> None:
    payload = web_research_runtime._mcp_result_payload(
        SimpleNamespace(
            data=None,
            structured_content=None,
            content=[
                TextContent(
                    type="text",
                    text=(
                        "Detailed Results:\n\n"
                        "Title: First official source\n"
                        "URL: https://example.com/first\n"
                        "Content: First evidence.\n\n"
                        "Title: Second official source\n"
                        "URL: https://example.org/second\n"
                        "Content: Second evidence."
                    ),
                )
            ],
        ),
        tool_name="tavily_search",
    )

    assert payload == {
        "results": [
            {
                "title": "First official source",
                "url": "https://example.com/first",
                "content": "First evidence.",
            },
            {
                "title": "Second official source",
                "url": "https://example.org/second",
                "content": "Second evidence.",
            },
        ]
    }
    for malformed in (
        "Search results are available at https://example.com",
        (
            "Detailed Results:\n\nTitle: Missing URL\n"
            "Content: This must fail closed."
        ),
        (
            "Detailed Results:\n\nTitle: Unsafe URL\n"
            "URL: file:///tmp/evidence\nContent: This must fail closed."
        ),
        (
            "Detailed Results:\n\nTitle: Valid prefix\n"
            "URL: https://example.com\nContent: Evidence.\nImages:\nmalformed"
        ),
    ):
        with pytest.raises(WebResearchRuntimeDependencyError):
            web_research_runtime._mcp_result_payload(
                SimpleNamespace(
                    data=None,
                    structured_content=None,
                    content=[TextContent(type="text", text=malformed)],
                ),
                tool_name="tavily_search",
            )


@pytest.mark.asyncio
async def test_exact_mcp_search_adapters_expose_only_bound_search_calls(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[dict[str, object]] = []

    async def call(
        transport_factory: object,
        *,
        tool_name: str,
        arguments: dict[str, object],
        timeout_seconds: float,
        max_response_bytes: int,
        expected_tools_snapshot_digest: str,
    ) -> object:
        calls.append(
            {
                "factory": transport_factory,
                "tool_name": tool_name,
                "arguments": arguments,
                "timeout": timeout_seconds,
                "max_response": max_response_bytes,
                "snapshot_digest": expected_tools_snapshot_digest,
            }
        )
        if tool_name == "firecrawl_search":
            return {
                "id": "firecrawl-request",
                "data": {
                    "web": [
                        {
                            "title": "Firecrawl source",
                            "url": "https://firecrawl.example/report?token=removed",
                            "description": "Firecrawl evidence",
                        }
                    ]
                },
            }
        return {
            "results": [
                {
                    "title": "Tavily source",
                    "url": "https://tavily.example/report",
                    "content": "Tavily evidence",
                }
            ]
        }

    monkeypatch.setattr(web_research_runtime, "_call_exact_search_tool", call)
    factory = lambda: object()  # noqa: E731
    firecrawl = FirecrawlMCPSearchAdapter(
        factory,  # type: ignore[arg-type]
        exact_tool_ref=FIRECRAWL_REF,
        runtime_artifact=FIRECRAWL_RUNTIME,
    )
    tavily = TavilyMCPSearchAdapter(
        factory,  # type: ignore[arg-type]
        exact_tool_ref=TAVILY_REF,
        runtime_artifact=TAVILY_RUNTIME,
    )

    firecrawl_result = await firecrawl.search(
        GovernedSearchRequest(
            query="public research goal",
            limit=5,
            include_domains=("example.com",),
            idempotency_key="run:firecrawl",
            exact_tool_ref=FIRECRAWL_REF,
            runtime_artifact=FIRECRAWL_RUNTIME,
        )
    )
    tavily_result = await tavily.search(
        GovernedSearchRequest(
            query="public research goal",
            limit=5,
            exclude_domains=("excluded.example",),
            idempotency_key="run:tavily",
            exact_tool_ref=TAVILY_REF,
            runtime_artifact=TAVILY_RUNTIME,
        )
    )

    assert [item["tool_name"] for item in calls] == [
        "firecrawl_search",
        "tavily_search",
    ]
    assert calls[0]["arguments"] == {
        "query": "public research goal",
        "limit": 5,
        "includeDomains": ["example.com"],
        "sources": [{"type": "web"}],
    }
    assert calls[1]["arguments"] == {
        "query": "public research goal",
        "max_results": 5,
        "exclude_domains": ["excluded.example"],
    }
    assert firecrawl_result.results[0].url == "https://firecrawl.example/report"
    assert tavily_result.results[0].url == "https://tavily.example/report"

    with pytest.raises(WebResearchRuntimeDependencyError, match="exact tool revision"):
        await firecrawl.search(
            GovernedSearchRequest(
                query="public research goal",
                limit=1,
                idempotency_key="run:mismatch",
                exact_tool_ref=TAVILY_REF,
                runtime_artifact=TAVILY_RUNTIME,
            )
        )


class FakeBrowserRunner:
    def __init__(self) -> None:
        self.requests: list[BrowserSubprocessRequest] = []

    async def run(
        self,
        request: BrowserSubprocessRequest,
    ) -> BrowserSubprocessResult:
        self.requests.append(request)
        command = request.arguments[-1]
        if "screenshot" in request.arguments:
            screenshot = Path(request.arguments[-1])
            await asyncio.to_thread(
                screenshot.write_bytes,
                b"\x89PNG\r\n\x1a\nsafe-screenshot",
            )
            payload: object = {"success": True}
        elif command == "url":
            payload = {"data": {"value": "https://upgrade.example/technology"}}
        elif command == "title":
            payload = {"data": {"value": "Upgrade Labs technology"}}
        elif "innerText?.slice(0, 4000)" in command:
            payload = {"data": {"value": "Public rendered evidence"}}
        else:
            payload = {"success": True}
        return BrowserSubprocessResult(
            exit_code=0,
            stdout=json.dumps(payload).encode(),
            stderr=b"",
        )


class FlakyBootstrapBrowserRunner(FakeBrowserRunner):
    def __init__(self) -> None:
        super().__init__()
        self.failed_once = False

    async def run(
        self,
        request: BrowserSubprocessRequest,
    ) -> BrowserSubprocessResult:
        if not self.failed_once and request.arguments[-2] == "open":
            self.failed_once = True
            self.requests.append(request)
            return BrowserSubprocessResult(
                exit_code=1,
                stdout=b"",
                stderr=(
                    b"Failed to install browser network controls: CDP error "
                    b"(Page.enable): Session with given id not found."
                ),
            )
        return await super().run(request)


class FakeScreenshots:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    async def store(self, **kwargs: object) -> str:
        self.calls.append(kwargs)
        return "belllabs://browser-evidence/screenshots/verified-1"


def test_agent_browser_session_identity_preserves_full_run_uniqueness() -> None:
    suffix = (
        ":execution-epoch:1:workflow-cycle:0:stage:browser_verify:"
        "stage-cycle:0:operation-attempt:1"
    )
    first = web_research_runtime._session_id(  # noqa: SLF001
        "operation:run-one" + suffix
    )
    second = web_research_runtime._session_id(  # noqa: SLF001
        "operation:run-two" + suffix
    )

    assert first != second
    assert first.startswith("belllabs-")
    assert len(first) == 21


def test_agent_browser_session_identity_isolates_activity_retry_invocations() -> None:
    idempotency_key = (
        "operation:run-one:execution-epoch:1:workflow-cycle:0:"
        "stage:browser_verify:stage-cycle:0:operation-attempt:1"
    )

    first = web_research_runtime._session_id(  # noqa: SLF001
        idempotency_key,
        invocation="belllabs-agent-browser-first",
    )
    retry = web_research_runtime._session_id(  # noqa: SLF001
        idempotency_key,
        invocation="belllabs-agent-browser-retry",
    )

    assert first != retry
    assert len(first) == len(retry) == 21


def test_agent_browser_eval_result_is_a_supported_scalar_envelope() -> None:
    assert (
        web_research_runtime._extract_scalar(  # noqa: SLF001
            {"success": True, "data": {"result": "bounded page evidence"}}
        )
        == "bounded page evidence"
    )


def test_agent_browser_failure_detail_prefers_sanitized_stderr() -> None:
    detail = web_research_runtime._browser_failure_detail(  # noqa: SLF001
        BrowserSubprocessResult(
            exit_code=1,
            stdout=b'{"error":{"message":"less precise"}}',
            stderr=b"navigation failed\\n token=must-not-escape",
        )
    )

    assert detail == "navigation failed\\n token=[REDACTED]"


def test_agent_browser_failure_detail_uses_json_error_not_page_output() -> None:
    detail = web_research_runtime._browser_failure_detail(  # noqa: SLF001
        BrowserSubprocessResult(
            exit_code=1,
            stdout=json.dumps(
                {
                    "success": False,
                    "data": {"result": "arbitrary rendered page content"},
                    "error": {
                        "code": "NAVIGATION_FAILED",
                        "message": "page load timed out",
                    },
                }
            ).encode(),
            stderr=b"",
        )
    )

    assert detail == "NAVIGATION_FAILED: page load timed out"


@pytest.mark.asyncio
async def test_browser_output_collection_does_not_wait_forever_for_daemon_pipe_eof() -> None:
    stdout = asyncio.StreamReader()
    stderr = asyncio.StreamReader()
    stdout.feed_data(b'{"success":true}\n')

    class ExitedCommandWithInheritedPipes:
        async def wait(self) -> int:
            return 0

    process = ExitedCommandWithInheritedPipes()
    process.stdout = stdout  # type: ignore[attr-defined]
    process.stderr = stderr  # type: ignore[attr-defined]

    captured_stdout, captured_stderr, exit_code = await asyncio.wait_for(
        web_research_runtime._collect_bounded_output(  # noqa: SLF001
            process,  # type: ignore[arg-type]
            16_384,
        ),
        timeout=2,
    )

    assert captured_stdout == b'{"success":true}\n'
    assert captured_stderr == b""
    assert exit_code == 0


@pytest.mark.asyncio
async def test_pinned_agent_browser_uses_isolated_bounded_argv_and_artifact_port(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    package = tmp_path / "node_modules" / "agent-browser"
    entrypoint = package / "bin" / "agent-browser.js"
    entrypoint.parent.mkdir(parents=True)
    entrypoint_bytes = b"// pinned test entrypoint\n"
    entrypoint.write_bytes(entrypoint_bytes)
    (package / "package.json").write_text(
        json.dumps({"name": "agent-browser", "version": "0.33.0"}),
        encoding="utf-8",
    )
    node = tmp_path / "node.exe"
    node.write_bytes(b"test")
    runner = FakeBrowserRunner()
    screenshots = FakeScreenshots()
    browser_runtime = BROWSER_RUNTIME.model_copy(
        update={
            "module_digest": f"sha256:{sha256(entrypoint_bytes).hexdigest()}",
        }
    )
    monkeypatch.setenv("OPENAI_API_KEY", "must-not-reach-browser")
    adapter = AgentBrowserSubprocessAdapter(
        runner,
        node_executable=node,
        agent_browser_entrypoint=entrypoint,
        screenshot_artifacts=screenshots,
        runtime_artifact=browser_runtime,
        timeout_seconds=10,
        command_timeout_seconds=5,
        max_output_bytes=16_384,
    )

    response = await adapter.verify(
        GovernedBrowserVerificationRequest(
            request_scope="tenant:test",
            run_id="run-test",
            urls=("https://upgrade.example/technology?token=discard",),
            objective="verify public evidence",
            idempotency_key="run-test:browser_verify:cycle-1",
            exact_skill_ref=BROWSER_REF,
            runtime_artifact=browser_runtime,
        )
    )

    assert response.pages[0].verified is True
    assert response.pages[0].requested_url == ("https://upgrade.example/technology")
    assert response.pages[0].screenshot_ref.startswith("belllabs://")
    assert screenshots.calls[0]["request_scope"] == "tenant:test"
    assert screenshots.calls[0]["run_id"] == "run-test"
    assert all(
        request.executable == node
        and request.arguments[0] == str(entrypoint)
        and "--allowed-domains" in request.arguments
        and "upgrade.example" in request.arguments
        and "OPENAI_API_KEY" not in request.environment
        and request.working_directory.name.startswith("belllabs-agent-browser-")
        for request in runner.requests
    )
    assert any(
        "eval" in request.arguments
        and "innerText?.slice(0, 4000)" in request.arguments[-1]
        for request in runner.requests
    )
    assert all("--session" not in request.arguments for request in runner.requests)
    assert all("--namespace" not in request.arguments for request in runner.requests)
    assert all("--config" not in request.arguments for request in runner.requests)
    assert runner.requests[-1].arguments[-1] == "close"


@pytest.mark.asyncio
async def test_pinned_agent_browser_retries_stale_cdp_bootstrap_in_fresh_session(
    tmp_path: Path,
) -> None:
    package = tmp_path / "node_modules" / "agent-browser"
    entrypoint = package / "bin" / "agent-browser.js"
    entrypoint.parent.mkdir(parents=True)
    entrypoint_bytes = b"// pinned test entrypoint\n"
    entrypoint.write_bytes(entrypoint_bytes)
    (package / "package.json").write_text(
        json.dumps({"name": "agent-browser", "version": "0.33.0"}),
        encoding="utf-8",
    )
    node = tmp_path / "node.exe"
    node.write_bytes(b"test")
    runner = FlakyBootstrapBrowserRunner()
    browser_runtime = BROWSER_RUNTIME.model_copy(
        update={
            "module_digest": f"sha256:{sha256(entrypoint_bytes).hexdigest()}",
        }
    )
    adapter = AgentBrowserSubprocessAdapter(
        runner,
        node_executable=node,
        agent_browser_entrypoint=entrypoint,
        screenshot_artifacts=FakeScreenshots(),
        runtime_artifact=browser_runtime,
        timeout_seconds=10,
        command_timeout_seconds=5,
        max_output_bytes=16_384,
    )

    response = await adapter.verify(
        GovernedBrowserVerificationRequest(
            request_scope="tenant:test",
            run_id="run-retry",
            urls=("https://upgrade.example/technology",),
            objective="verify public evidence",
            idempotency_key="run-retry:browser_verify:cycle-1",
            exact_skill_ref=BROWSER_REF,
            runtime_artifact=browser_runtime,
        )
    )

    open_requests = [
        item for item in runner.requests if item.arguments[-2] == "open"
    ]
    assert response.pages[0].verified is True
    assert len(open_requests) == 2
    assert (
        open_requests[0].environment["AGENT_BROWSER_SESSION"]
        != open_requests[1].environment["AGENT_BROWSER_SESSION"]
    )


def test_agent_browser_rejects_unpinned_package(tmp_path: Path) -> None:
    package = tmp_path / "node_modules" / "agent-browser"
    entrypoint = package / "bin" / "agent-browser.js"
    entrypoint.parent.mkdir(parents=True)
    entrypoint.write_text("// unpinned\n", encoding="utf-8")
    (package / "package.json").write_text(
        json.dumps({"name": "agent-browser", "version": "0.34.0"}),
        encoding="utf-8",
    )
    node = tmp_path / "node.exe"
    node.write_bytes(b"test")

    with pytest.raises(WebResearchRuntimeDependencyError, match="0.33.0"):
        AgentBrowserSubprocessAdapter(
            FakeBrowserRunner(),
            node_executable=node,
            agent_browser_entrypoint=entrypoint,
            screenshot_artifacts=FakeScreenshots(),
            runtime_artifact=BROWSER_RUNTIME,
        )
