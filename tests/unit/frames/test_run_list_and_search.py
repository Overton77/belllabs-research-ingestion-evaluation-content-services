"""C4: the run-list grammar, ledger enrichment, and the HTTP, CLI and MCP surfaces."""

from __future__ import annotations

import json
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from fastmcp import Client, FastMCP

from mission_control.application.frames.search import (
    ProjectionReceipt,
    RunListService,
    TranscriptDocument,
    TranscriptSearchService,
    VisibilityExecution,
    transcript_documents,
)
from mission_control.application.installations.registry import (
    ApplicationBinding,
    ApplicationRegistry,
    InstallationObservation,
)
from mission_control.domain.frames.run_query import (
    RunQueryInvalid,
    installation_prefix,
    parse_run_query,
    visibility_filter,
)
from mission_control.domain.policies.contracts import ActorContext
from mission_control.domain.programs.search_attributes import search_attribute_scope_hash
from mission_control.interfaces.cli import main as cli
from mission_control.interfaces.http.mission_control import MissionPrincipal, get_mission_principal
from mission_control.interfaces.http.transcript import router
from mission_control.interfaces.mcp.coordinator_server import CoordinatorPrincipal, _principal_call
from mission_control.interfaces.mcp.transcript_tools import (
    LIST_TOOL,
    SEARCH_TOOL,
    ScopedTranscripts,
    register_transcript_tools,
)
from tests.fixtures.provider_frames import INSTALLATION, SCOPE, TENANT
from tests.fixtures.transcripts import RUN_KEY
from tests.unit.frames.test_transcript_interfaces import transcript_service
from tests.unit.run_control.test_boundary_commands import TARGET, started
from tests.unit.run_control.test_run_control import service as run_control_service

READER = ActorContext(actor_id="reader", permissions=frozenset({"workflow_run.read"}))
NOW = datetime(2026, 10, 8, 12, 0, tzinfo=UTC)


@pytest.mark.parametrize(
    ("text", "fragment"),
    [
        ("lane='deep_agents'", "mc_lane = 'deep_agents'"),
        ("phase='executing'", "mc_phase = 'executing'"),
        ("mission_id='m-1'", "mc_mission_id = 'm-1'"),
        ("forked_from='run-1'", "ForkedFromRunId = 'run-1'"),
        ("status='running'", "ExecutionStatus = 'Running'"),
        ("status='paused'", "ExecutionStatus = 'Paused'"),
        ("started_after>'2026-10-01T00:00:00+02:00'", "StartTime > '2026-09-30T22:00:00Z'"),
    ],
)
def test_grammar_translates_each_key(text: str, fragment: str) -> None:
    query = visibility_filter(parse_run_query(text), SCOPE)
    assert fragment in query
    # Every filter is bound to the caller's installation prefix and scope hash first.
    assert query.startswith(f"WorkflowId STARTS_WITH '{installation_prefix(SCOPE)}' AND ")
    assert f"BellLabsScopeHash = '{search_attribute_scope_hash(SCOPE)}'" in query


def test_grammar_combines_clauses_and_lists_everything_when_empty() -> None:
    parsed = parse_run_query("lane='deep_agents' AND phase='executing' and status='running'")
    assert [clause.key for clause in parsed.clauses] == ["lane", "phase", "status"]
    assert visibility_filter(parse_run_query(""), SCOPE).count(" AND ") == 1


@pytest.mark.parametrize(
    "text",
    [
        "tenant='other'",
        "lane='codex'",
        "phase='running'",
        "lane = deep_agents",
        "lane='deep_agents' OR phase='executing'",
        "mission_id='x' AND WorkflowId STARTS_WITH 'mc/'",
        "mission_id='a'' OR ''1'='1'",
        "started_after='yesterday'",
        "started_after='2026-10-01T00:00:00'",
        "lane>'deep_agents'",
        "status='exploded'",
        "x" * 2_000,
    ],
)
def test_grammar_rejects_everything_else(text: str) -> None:
    with pytest.raises(RunQueryInvalid):
        parse_run_query(text)


class FakeVisibility:
    def __init__(self, rows: Sequence[VisibilityExecution]) -> None:
        self.rows = tuple(rows)
        self.queries: list[str] = []

    async def list(self, query: str, *, limit: int) -> tuple[VisibilityExecution, ...]:
        self.queries.append(query)
        return self.rows[:limit]


async def _ledger_with_runs() -> tuple[Any, list[str]]:
    authority, _ = run_control_service()
    first = await started(authority, "c4-first", TARGET)
    second = await started(authority, "c4-second", TARGET)
    return authority, [first, second]


def _execution(run: str, kind: str, **values: Any) -> VisibilityExecution:
    return VisibilityExecution(
        workflow_id=f"mc/i/a/run/{run}/{kind}",
        run_key=run,
        workflow_kind=kind,
        status=values.pop("status", "RUNNING"),
        started_at=values.pop("started_at", NOW),
        **values,
    )


@pytest.mark.asyncio
async def test_rows_are_aggregated_per_run_enriched_and_unknown_runs_dropped() -> None:
    authority, (first, second) = await _ledger_with_runs()
    visibility = FakeVisibility(
        [
            _execution(first, "root", mission_id="mission-1"),
            _execution(first, "family", phase="executing"),
            _execution(first, "operation", lane="deep_agents", phase="executing"),
            _execution(
                second,
                "operation",
                lane="cursor_local",
                phase="completed",
                started_at=NOW + timedelta(minutes=5),
            ),
            _execution("ghost-run", "root"),
        ]
    )
    service = RunListService(visibility, _ScopedLedger(authority), request_scope=SCOPE)
    page = await service.list("phase='executing'", actor=READER)
    assert visibility.queries[0].endswith("mc_phase = 'executing'")
    assert page.dropped_missing_from_ledger == 1
    assert [row.run_id for row in page.rows] == [second, first]  # newest first
    row = page.rows[1]
    assert row.mission_id == "mission-1" and row.lanes == ("deep_agents",)
    assert row.phases == ("executing",) and row.temporal_status == "RUNNING"
    assert row.lifecycle == "active" and row.terminal_outcome is None
    limited = await service.list(None, actor=READER, limit=1)
    assert len(limited.rows) == 1 and limited.truncated
    with pytest.raises(PermissionError):
        await service.list(None, actor=ActorContext(actor_id="x", permissions=frozenset()))
    with pytest.raises(RunQueryInvalid):
        await service.list("tenant='x'", actor=READER)


class MemoryDocuments:
    def __init__(self) -> None:
        self.rows: dict[str, TranscriptDocument] = {}

    async def replace(
        self, request_scope: str, run_key: str, documents: Sequence[TranscriptDocument]
    ) -> ProjectionReceipt:
        fresh = {document.cursor: document for document in documents}
        changed = sum(1 for key, value in fresh.items() if self.rows.get(key) != value)
        deleted = len(set(self.rows) - set(fresh))
        self.rows = fresh
        return ProjectionReceipt(
            run_id=run_key, upserted=changed, deleted=deleted, documents=len(fresh)
        )

    async def search(
        self, request_scope: str, run_key: str, text: str, *, limit: int
    ) -> tuple[tuple[str, float], ...]:
        words = text.lower().split()
        scored = [
            (
                cursor,
                float(sum(word in f"{doc.title} {doc.body_excerpt}".lower() for word in words)),
            )
            for cursor, doc in self.rows.items()
        ]
        return tuple(sorted((item for item in scored if item[1]), key=lambda item: -item[1]))[
            :limit
        ]


def _registry() -> ApplicationRegistry:
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
    return registry


async def _services() -> tuple[RunListService, TranscriptSearchService, list[str]]:
    authority, runs = await _ledger_with_runs()
    run_list = RunListService(
        FakeVisibility([_execution(run, "root") for run in runs]),
        _ScopedLedger(authority),
        request_scope=SCOPE,
    )
    search = TranscriptSearchService(transcript_service(), MemoryDocuments())
    return run_list, search, runs


class _ScopedLedger:
    """The in-memory ledger admitted its runs under `tenant-1`; serve them under SCOPE."""

    def __init__(self, authority: Any) -> None:
        self._authority = authority

    async def get_run(self, request_scope: str, run_id: str) -> Any:
        return await self._authority.get_run("tenant-1", run_id)


def _app(run_list: RunListService, search: TranscriptSearchService) -> FastAPI:
    app = FastAPI()
    app.include_router(router)
    app.state.mission_control_registry = _registry()
    key = (INSTALLATION, "biotech", TENANT)
    app.state.mission_control_run_list_services = {key: run_list}
    app.state.mission_control_transcript_search_services = {key: search}
    app.dependency_overrides[get_mission_principal] = lambda: MissionPrincipal(
        installation_id=INSTALLATION,
        application_id="biotech",
        tenant_id=TENANT,
        issuer="https://issuer.invalid",
        audiences=frozenset({"authenticated"}),
        actor=READER,
    )
    return app


@pytest.mark.asyncio
async def test_http_cli_and_mcp_list_and_search(
    monkeypatch: pytest.MonkeyPatch, capsysbinary: pytest.CaptureFixture[bytes]
) -> None:
    run_list, search, runs = await _services()
    app = _app(run_list, search)
    client = TestClient(app)
    listed = client.get("/v1/applications/biotech/runs", params={"query": "lane='deep_agents'"})
    assert listed.status_code == 200
    assert {row["run_id"] for row in listed.json()["rows"]} == set(runs)
    invalid = client.get("/v1/applications/biotech/runs", params={"query": "tenant='x'"})
    assert invalid.status_code == 422 and invalid.json()["detail"]["code"] == "invalid_run_query"
    found = client.get(
        f"/v1/applications/biotech/runs/{RUN_KEY}/transcript/search", params={"q": "pubmed_search"}
    )
    assert found.status_code == 200
    hits = found.json()["hits"]
    assert hits and all("cursor" in hit["entry"] for hit in hits)
    assert found.json()["projection"]["documents"] == len(
        transcript_documents(await search._transcripts.projection_entries(RUN_KEY))
    )
    missing = client.get("/v1/applications/biotech/runs/nope/transcript/search", params={"q": "x"})
    assert missing.status_code == 404

    real_client = httpx.Client

    class Bridge(httpx.BaseTransport):
        def __init__(self) -> None:
            self._client = TestClient(app)

        def handle_request(self, request: httpx.Request) -> httpx.Response:
            response = self._client.request(
                request.method, str(request.url), headers=dict(request.headers)
            )
            return httpx.Response(
                response.status_code, headers=response.headers, content=response.content
            )

    def factory(*args: Any, **kwargs: Any) -> httpx.Client:
        kwargs["transport"] = Bridge()
        return real_client(*args, **kwargs)

    monkeypatch.setenv("MISSION_CONTROL_TOKEN", "token")
    monkeypatch.setenv("MISSION_CONTROL_APPLICATION_ID", "biotech")
    monkeypatch.setattr(cli.httpx, "Client", factory)
    assert cli.main(["run", "list", "--query", "phase='executing'"]) == 0
    lines = capsysbinary.readouterr().out.decode("utf-8").splitlines()
    assert {line.split("\t")[0] for line in lines} == set(runs)
    assert cli.main(["run", "list", "--json"]) == 0
    assert len(json.loads(capsysbinary.readouterr().out)["rows"]) == 2
    assert cli.main(["run", "list", "--query", "bogus='1'"]) == 2
    capsysbinary.readouterr()
    assert cli.main(["run", "search", RUN_KEY, "--query", "pubmed_search", "--limit", "3"]) == 0
    rows = capsysbinary.readouterr().out.decode("utf-8").splitlines()
    assert rows and all(len(row.split("\t")) == 5 for row in rows)
    assert cli.main(["run", "search", RUN_KEY, "--query", "pubmed_search", "--json"]) == 0
    assert json.loads(capsysbinary.readouterr().out)["hits"]

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

    server = FastMCP("c4-test")
    register_transcript_tools(
        server,
        ScopedTranscripts(
            {SCOPE: transcript_service()},
            searches={SCOPE: search},
            run_lists={SCOPE: run_list},
        ),
        Principals(),
        call=_principal_call,
    )
    async with Client(server) as mcp:
        tools = {tool.name: tool for tool in await mcp.list_tools()}
        assert tools[SEARCH_TOOL].annotations.readOnlyHint is True  # type: ignore[union-attr]
        assert tools[LIST_TOOL].annotations.readOnlyHint is True  # type: ignore[union-attr]
        searched = await mcp.call_tool(SEARCH_TOOL, {"run_id": RUN_KEY, "query": "pubmed_search"})
        assert searched.structured_content is not None
        assert searched.structured_content["ok"] is True
        listed_mcp = await mcp.call_tool(LIST_TOOL, {"query": "lane='deep_agents'"})
        assert listed_mcp.structured_content is not None
        assert listed_mcp.structured_content["ok"] is True


@pytest.mark.asyncio
async def test_projection_activity_is_idempotent_and_reports_missing_runs() -> None:
    from temporalio.testing import ActivityEnvironment

    from mission_control.adapters.temporal.activities.transcript_projection import (
        TranscriptProjectInput,
        TranscriptProjectionActivities,
    )
    from mission_control.application.frames.search import TranscriptProjector

    documents = MemoryDocuments()
    projector = TranscriptProjector(transcript_service(), documents)
    activities = TranscriptProjectionActivities(lambda scope: projector)
    env = ActivityEnvironment()
    request = TranscriptProjectInput(request_scope=SCOPE, run_ids=(RUN_KEY, "missing-run"))
    first = await env.run(activities.project, request)
    assert first.missing_runs == ("missing-run",)
    assert first.receipts[0].upserted == first.receipts[0].documents > 0
    again = await env.run(activities.project, request)
    assert (again.receipts[0].upserted, again.receipts[0].deleted) == (0, 0)
