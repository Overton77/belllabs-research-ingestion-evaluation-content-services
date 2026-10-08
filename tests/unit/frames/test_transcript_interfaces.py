"""C3: the Transcript on HTTP, CLI and MCP returns the same entries under the same scope."""

from __future__ import annotations

import json
from typing import Any

import httpx
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from fastmcp import Client, FastMCP

from mission_control.application.frames.transcript import TranscriptService
from mission_control.application.installations.registry import (
    ApplicationBinding,
    ApplicationRegistry,
    InstallationObservation,
)
from mission_control.domain.policies.contracts import ActorContext
from mission_control.interfaces.cli import main as cli
from mission_control.interfaces.http.mission_control import MissionPrincipal, get_mission_principal
from mission_control.interfaces.http.transcript import router
from mission_control.interfaces.mcp.coordinator_server import CoordinatorPrincipal, _principal_call
from mission_control.interfaces.mcp.transcript_tools import (
    TRANSCRIPT_TOOL,
    ScopedTranscripts,
    register_transcript_tools,
)
from tests.fixtures.provider_frames import INSTALLATION, SCOPE, TENANT
from tests.fixtures.transcripts import (
    RUN_KEY,
    SECRET_VALUE,
    InMemoryMissionEvents,
    StaticFrames,
    transcript_events,
    transcript_frames,
)


def transcript_service() -> TranscriptService:
    frames = transcript_frames()
    return TranscriptService(
        InMemoryMissionEvents(transcript_events(frames)),
        StaticFrames(frames),  # type: ignore[arg-type]
        request_scope=SCOPE,
        secret_values=(SECRET_VALUE,),
    )


def http_app(permissions: frozenset[str] = frozenset({"workflow_run.read"})) -> FastAPI:
    registry = ApplicationRegistry(
        (
            ApplicationBinding.seal(
                application_id="biotech",
                installation_id=INSTALLATION,
                binding_version="1",
                supabase_project_ref="project",
                database_secret_ref="TEST_DATABASE_URL",
                accepted_issuers={"https://issuer.invalid"},
                accepted_audiences={"authenticated"},
                required_component_version="1",
            ),
        )
    )
    registry.observe(
        "biotech", InstallationObservation(INSTALLATION, "biotech", "project", frozenset({"1"}))
    )
    app = FastAPI()
    app.include_router(router)
    app.state.mission_control_registry = registry
    app.state.mission_control_transcript_services = {
        (INSTALLATION, "biotech", TENANT): transcript_service()
    }
    app.dependency_overrides[get_mission_principal] = lambda: MissionPrincipal(
        installation_id=INSTALLATION,
        application_id="biotech",
        tenant_id=TENANT,
        issuer="https://issuer.invalid",
        audiences=frozenset({"authenticated"}),
        actor=ActorContext(actor_id="reader", permissions=permissions),
    )
    return app


PATH = f"/v1/applications/biotech/runs/{RUN_KEY}/transcript"


def test_http_jsonl_markdown_json_and_errors() -> None:
    client = TestClient(http_app())
    jsonl = client.get(PATH)
    assert jsonl.status_code == 200
    assert jsonl.headers["content-type"].startswith("application/x-ndjson")
    lines = [json.loads(line) for line in jsonl.text.splitlines()]
    assert len(lines) == 15 and all(
        line["schema_version"] == "mc.transcript_entry.v1" for line in lines
    )
    next_cursor = jsonl.headers["x-transcript-next-cursor"]
    assert next_cursor == lines[-1]["cursor"]
    assert SECRET_VALUE not in jsonl.text
    markdown = client.get(PATH, headers={"Accept": "text/markdown"})
    assert markdown.headers["content-type"].startswith("text/markdown")
    assert markdown.text.startswith(f"# Transcript · run {RUN_KEY}")
    page = client.get(PATH, params={"format": "json", "canonical_only": "true", "limit": 2})
    body = page.json()
    assert len(body["entries"]) == 2 and body["has_more"] is True
    newer = client.get(PATH, params={"since": lines[11]["cursor"]})
    assert [json.loads(line)["cursor"] for line in newer.text.splitlines()] == [
        line["cursor"] for line in lines[12:]
    ]
    expired = client.get(PATH, params={"since": "garbage"})
    assert expired.status_code == 409 and expired.json()["detail"]["code"] == "CURSOR_EXPIRED"
    assert client.get(PATH.replace(RUN_KEY, "missing")).status_code == 404
    assert client.get(PATH, params={"full": "true"}).status_code == 501
    denied = TestClient(http_app(permissions=frozenset())).get(PATH)
    assert denied.status_code == 403
    other_app = TestClient(http_app()).get(PATH.replace("/biotech/", "/ai-engineer/"))
    assert other_app.status_code == 403


def test_http_frames_tail_is_sse_labelled_non_canonical() -> None:
    client = TestClient(http_app())
    response = client.get(
        f"/v1/applications/biotech/runs/{RUN_KEY}/frames/tail",
        params={"timeout": 0.2, "poll": 0.05},
    )
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/event-stream")
    events = [
        json.loads(line.removeprefix("data: "))
        for line in response.text.splitlines()
        if line.startswith("data: ")
    ]
    frames = [event for event in events if event.get("source") == "provider_frame"]
    assert len(frames) == 8 and all(event["canonical"] is False for event in frames)
    assert events[-1]["reason"] == "timeout" and events[-1]["canonical"] is False


def _cli_transport(app: FastAPI) -> Any:
    real_client = httpx.Client

    def factory(*args: Any, **kwargs: Any) -> httpx.Client:
        kwargs["transport"] = _ASGIBridge(app)
        return real_client(*args, **kwargs)

    return factory


class _ASGIBridge(httpx.BaseTransport):
    """Synchronous transport into a FastAPI app through its TestClient."""

    def __init__(self, app: FastAPI) -> None:
        self._client = TestClient(app)

    def handle_request(self, request: httpx.Request) -> httpx.Response:
        response = self._client.request(
            request.method,
            str(request.url),
            headers=dict(request.headers),
            content=request.content,
        )
        return httpx.Response(
            response.status_code, headers=response.headers, content=response.content
        )


@pytest.fixture
def cli_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MISSION_CONTROL_TOKEN", "token")
    monkeypatch.setenv("MISSION_CONTROL_APPLICATION_ID", "biotech")
    monkeypatch.setattr(cli.httpx, "Client", _cli_transport(http_app()))


def test_cli_transcript_formats_cursor_errors_and_follow_timeout(
    cli_env: None, capsysbinary: pytest.CaptureFixture[bytes]
) -> None:
    assert cli.main(["run", "transcript", RUN_KEY, "--format", "md"]) == 0
    out = capsysbinary.readouterr().out.decode("utf-8")
    assert out.startswith(f"# Transcript · run {RUN_KEY}") and "✓" in out
    assert cli.main(["run", "transcript", RUN_KEY, "--canonical-only", "--limit", "3"]) == 0
    lines = capsysbinary.readouterr().out.decode("utf-8").splitlines()
    assert len(lines) == 3 and all(json.loads(line)["canonical"] for line in lines)
    assert cli.main(["run", "transcript", RUN_KEY, "--since", "nope"]) == 2
    assert "CURSOR_EXPIRED" in capsysbinary.readouterr().out.decode("utf-8")
    assert cli.main(["run", "transcript", RUN_KEY, "--follow", "--wait", "0.3"]) == 6
    captured = capsysbinary.readouterr()
    assert len(captured.out.decode("utf-8").splitlines()) == 15, "follow prints once"
    assert b"wait_timeout" in captured.err
    assert cli.main(["run", "frames", RUN_KEY, "--tail", "--wait", "0.2", "--until-end"]) == 6
    tail = [json.loads(line) for line in capsysbinary.readouterr().out.decode().splitlines()]
    assert tail[-1]["event"] == "end" and all(item["canonical"] is False for item in tail)


@pytest.mark.asyncio
async def test_mcp_tool_and_resource_return_the_http_entries_read_only() -> None:
    principal = CoordinatorPrincipal(
        actor_id="coordinator",
        tenant_scope=SCOPE,
        roles=frozenset(),
        permissions=frozenset({"workflow.result.read"}),
        request_scope=SCOPE,
    )

    class Principals:
        async def resolve(self, context: Any) -> CoordinatorPrincipal:
            del context
            return principal

    server = FastMCP("transcript-test")
    register_transcript_tools(
        server, ScopedTranscripts({SCOPE: transcript_service()}), Principals(), call=_principal_call
    )
    http_lines = [json.loads(line) for line in TestClient(http_app()).get(PATH).text.splitlines()]
    async with Client(server) as client:
        tools = {tool.name: tool for tool in await client.list_tools()}
        assert tools[TRANSCRIPT_TOOL].annotations is not None
        assert tools[TRANSCRIPT_TOOL].annotations.readOnlyHint is True
        result = await client.call_tool(TRANSCRIPT_TOOL, {"run_id": RUN_KEY, "format": "json"})
        envelope = result.structured_content
        assert envelope is not None and envelope["ok"] is True
        mcp_entries = envelope["data"]["entries"]
        assert [entry["cursor"] for entry in mcp_entries] == [line["cursor"] for line in http_lines]
        resource = await client.read_resource(
            f"mc://applications/biotech/runs/{RUN_KEY}/transcript?since={http_lines[11]['cursor']}"
        )
        text = resource[0].text  # type: ignore[union-attr]
        assert [json.loads(line)["cursor"] for line in text.splitlines()] == [
            line["cursor"] for line in http_lines[12:]
        ]
        with pytest.raises(Exception, match="application scope denied|denied"):
            await client.read_resource(f"mc://applications/ai-engineer/runs/{RUN_KEY}/transcript")
