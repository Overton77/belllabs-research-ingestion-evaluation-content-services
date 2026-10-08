"""FT-G5 fixtures: a Cloud Agents API v1 fake behind `httpx.MockTransport`, a bare remote.

`FakeCloudApi` replays the recorded shapes in `tests/integration/cursor/fixtures/cloud/`
(hand-authored from research/cursor-platform.md section 4, marked synthetic): agent create
with client `agentId` and `Idempotency-Key`, `409 agent_id_conflict`, `409 agent_busy`,
`403 feature_unavailable` on `metadata`, run reads, the SSE stream honoring `Last-Event-ID`
(optionally dropping the connection mid-stream, or answering `410 stream_expired`), cancel,
artifacts through presigned URLs that must not carry the API key, usage and archive. When a run
reaches its result the fake pushes the agent's commit to `mc/<run>` on the bare remote, as the
cloud agent would. No network is touched.
"""

from __future__ import annotations

import json
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from uuid import uuid4

import httpx
from pydantic import SecretStr
from tests.fixtures.cursor_local import MemoryArtifacts, make_repository
from tests.fixtures.lane_turns import cursor_binding, cursor_operation

from mission_control.adapters.cursor.cloud import CursorCloudHarness
from mission_control.adapters.cursor.cloud_api import RETENTION_HEADER, CloudAgentsClient
from mission_control.adapters.cursor.projection import (
    RenderedProjectionSource,
    operating_contract,
    projection_digests,
    static_rows,
)
from mission_control.adapters.cursor.scm import GitBranchPublisher
from mission_control.adapters.cursor.sse import SseEvent, parse_sse_text, render_sse
from mission_control.adapters.cursor.workspace import git
from mission_control.application.agentic_components.projections import render_host_files
from mission_control.domain.agentic_components.projection import ResolvedCapability
from mission_control.domain.execution.contracts import OperationExecutionRequest

FIXTURES = Path(__file__).resolve().parents[1] / "integration" / "cursor" / "fixtures" / "cloud"
API_KEY = "cursor-test-key-not-real"
PRESIGNED = "https://s3.fake.invalid/presigned"


def load_cloud_fixture() -> tuple[list[SseEvent], dict[str, Any]]:
    events = parse_sse_text((FIXTURES / "run_stream.sse").read_text(encoding="utf-8"))
    record = json.loads((FIXTURES / "run_record.json").read_text(encoding="utf-8"))
    return events, record


def make_remote(root: Path) -> Path:
    """A bare `origin` with one `main` commit (the target repository the cloud agent clones)."""

    source = make_repository(root / "source")
    bare = root / "remote.git"
    git("clone", "--quiet", "--bare", str(source), str(bare), cwd=root)
    return bare


@dataclass
class FakeCloudApi:
    events: list[SseEvent]
    record: dict[str, Any]
    remote: Path
    metadata_unavailable: bool = False
    busy: bool = False
    expire_after: str | None = None
    cut_after: str | None = None
    agents: dict[str, dict[str, Any]] = field(default_factory=dict)
    runs: dict[str, dict[str, Any]] = field(default_factory=dict)
    idempotency: dict[str, dict[str, Any]] = field(default_factory=dict)
    requests: list[httpx.Request] = field(default_factory=list)
    stream_requests: list[str | None] = field(default_factory=list)
    creates: list[dict[str, Any]] = field(default_factory=list)
    archived: list[str] = field(default_factory=list)
    cancelled: list[str] = field(default_factory=list)
    downloads: list[str] = field(default_factory=list)
    pushed: bool = False

    @property
    def run_id(self) -> str:
        return str(self.record["run"]["id"])

    def transport(self) -> httpx.MockTransport:
        return httpx.MockTransport(self.handle)

    @staticmethod
    def _error(status: int, code: str) -> httpx.Response:
        return httpx.Response(status, json={"error": {"code": code, "message": code}})

    def handle(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        url = request.url
        if url.host == "s3.fake.invalid":
            assert "authorization" not in {key.lower() for key in request.headers}
            self.downloads.append(url.path)
            path = url.params.get("path", "")
            return httpx.Response(200, content=self.record["artifacts"][path].encode("utf-8"))
        assert request.headers["Authorization"] == f"Bearer {API_KEY}"
        parts = url.path.strip("/").split("/")
        method = request.method
        if parts[:2] == ["v1", "agents"] and len(parts) == 2 and method == "POST":
            return self._create(request)
        agent_id = parts[2]
        agent = self.agents.get(agent_id)
        if agent is None:
            return self._error(404, "agent_not_found")
        rest = parts[3:]
        if not rest and method == "GET":
            return httpx.Response(200, json={"agent": agent})
        if rest == ["runs"] and method == "POST":
            run = self.runs[agent["latestRunId"]]
            if self.busy or run["status"] not in {"FINISHED", "ERROR", "CANCELLED", "EXPIRED"}:
                return self._error(409, "agent_busy")
            new = {"id": f"run-{uuid4()}", "agentId": agent_id, "status": "CREATING"}
            self.runs[new["id"]] = new
            agent["latestRunId"] = new["id"]
            return httpx.Response(200, json={"run": new})
        if len(rest) >= 2 and rest[0] == "runs":
            run = self.runs.get(rest[1])
            if run is None:
                return self._error(404, "run_not_found")
            if len(rest) == 2 and method == "GET":
                return httpx.Response(200, json={"run": run})
            if rest[2:] == ["cancel"] and method == "POST":
                if run["status"] in {"FINISHED", "ERROR", "CANCELLED", "EXPIRED"}:
                    return self._error(409, "run_not_cancellable")
                run["status"] = "CANCELLED"
                agent["status"] = "IDLE"
                self.cancelled.append(rest[1])
                return httpx.Response(200, json={"run": run})
            if rest[2:] == ["stream"] and method == "GET":
                return self._stream(request, agent, run)
        if rest == ["artifacts"] and method == "GET":
            items = [
                {"path": path, "sizeBytes": len(content), "updatedAt": "2026-10-08T12:00:00Z"}
                for path, content in self.record["artifacts"].items()
            ]
            return httpx.Response(200, json={"items": items})
        if rest == ["artifacts", "download"] and method == "GET":
            path = url.params["path"]
            return httpx.Response(
                200, json={"url": f"{PRESIGNED}?path={path}&sig=abc", "expiresAt": "later"}
            )
        if rest == ["usage"] and method == "GET":
            if url.params.get("runId") not in {None, *self.runs}:
                return self._error(404, "run_not_found")
            return httpx.Response(200, json=self.record["usage"])
        if rest == ["archive"] and method == "POST":
            self.archived.append(agent_id)
            agent["status"] = "ARCHIVED"
            return httpx.Response(200, json={})
        return self._error(404, "route_not_found")

    def _create(self, request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        self.creates.append(body)
        key = request.headers.get("Idempotency-Key")
        if key is not None and key in self.idempotency:
            return httpx.Response(200, json=self.idempotency[key])
        if "metadata" in body and self.metadata_unavailable:
            return self._error(403, "feature_unavailable")
        agent_id = body.get("agentId") or f"bc-{uuid4()}"
        if agent_id in self.agents:
            return self._error(409, "agent_id_conflict")
        run = {"id": self.run_id, "agentId": agent_id, "status": "CREATING"}
        agent = {
            "id": agent_id,
            "name": body.get("name"),
            "status": "ACTIVE",
            "url": f"https://cursor.com/agents/{agent_id}",
            "latestRunId": run["id"],
            "repos": body.get("repos"),
        }
        self.agents[agent_id] = agent
        self.runs[run["id"]] = run
        response = {"agent": agent, "run": run}
        if key is not None:
            self.idempotency[key] = response
        return httpx.Response(200, json=response)

    def _stream(
        self, request: httpx.Request, agent: dict[str, Any], run: dict[str, Any]
    ) -> httpx.Response:
        last = request.headers.get("Last-Event-ID")
        self.stream_requests.append(last)
        if (
            self.expire_after is not None
            and last is not None
            and int(last) >= int(self.expire_after)
        ):
            self._finish(agent, run)
            return self._error(410, "stream_expired")
        selected: list[SseEvent] = []
        seen_last = last is None
        for event in self.events:
            if event.id is None:
                # The leading status has no id and is re-sent on every reconnect.
                if event.event == "status" or seen_last:
                    selected.append(event)
                continue
            if not seen_last:
                seen_last = event.id == last
                continue
            selected.append(event)
            if event.event == "result":
                self._finish(agent, run)
            if self.cut_after is not None and event.id == self.cut_after:
                self.cut_after = None
                break
        return httpx.Response(
            200,
            headers={
                "Content-Type": "text/event-stream",
                RETENTION_HEADER: str(self.record["retention_seconds"]),
            },
            content=render_sse(selected).encode("utf-8"),
        )

    def _finish(self, agent: dict[str, Any], run: dict[str, Any]) -> None:
        final = self.record["run"]
        run.update({key: value for key, value in final.items() if key != "id"})
        agent["status"] = "IDLE"
        if not self.pushed:
            self._push_agent_commit(run)
            self.pushed = True

    def _push_agent_commit(self, run: dict[str, Any]) -> None:
        branch = next(
            (repo.get("startingRef") for agent in self.agents.values() for repo in agent["repos"]),
            None,
        )
        if branch is None:
            return
        commit = self.record["agent_branch_commit"]
        with tempfile.TemporaryDirectory(prefix="fake-cloud-agent-") as scratch:
            work = Path(scratch) / "clone"
            git(
                "clone",
                "--quiet",
                "--branch",
                branch,
                str(self.remote),
                str(work),
                cwd=Path(scratch),
            )
            target = work / commit["path"]
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(commit["content"], encoding="utf-8")
            git("add", "--all", cwd=work)
            git(
                "-c",
                "user.name=Cursor Agent",
                "-c",
                "user.email=agent@localhost",
                "commit",
                "--quiet",
                "-m",
                "agent work",
                cwd=work,
            )
            git("push", "--quiet", "origin", branch, cwd=work)


def cloud_rows() -> tuple[ResolvedCapability, ...]:
    from tests.fixtures.projections.rows import hook_row, tavily_row, verifier_row

    return (tavily_row(), hook_row(), verifier_row())


@dataclass
class CloudStack:
    harness: CursorCloudHarness
    api: FakeCloudApi
    client: CloudAgentsClient
    artifacts: MemoryArtifacts
    operation: OperationExecutionRequest
    remote: Path


def cloud_operation(
    remote: Path, rows: tuple[ResolvedCapability, ...], **cloud_changes: Any
) -> OperationExecutionRequest:
    base = cursor_operation("cursor_cloud")
    projection = render_host_files(rows, "cursor_cloud", operating_contract(base), None, ())
    binding = cursor_binding(
        "cursor_cloud",
        projections=projection_digests(projection),
        workspace={"base_ref": "main", "repo_url": str(remote)},
        cloud={"auto_create_pr": False, **cloud_changes},
    )
    return cursor_operation("cursor_cloud", binding=binding)


def cloud_stack(
    tmp_path: Path,
    *,
    cloud_changes: dict[str, Any] | None = None,
    api_changes: dict[str, Any] | None = None,
    cost: int | None = None,
    env_vars: dict[str, str] | None = None,
) -> CloudStack:
    events, record = load_cloud_fixture()
    remote = make_remote(tmp_path)
    rows = cloud_rows()
    operation = cloud_operation(remote, rows, **(cloud_changes or {}))
    api = FakeCloudApi(events, record, remote, **(api_changes or {}))
    client = CloudAgentsClient(SecretStr(API_KEY), transport=api.transport())
    artifacts = MemoryArtifacts()

    async def cost_reader(agent_id: str, run_id: str) -> int | None:
        return cost

    async def env_resolver(ref: str) -> dict[str, str]:
        return dict(env_vars or {})

    harness = CursorCloudHarness(
        client=client,
        publisher=GitBranchPublisher(tmp_path / "mirrors"),
        projections=RenderedProjectionSource(static_rows(rows), kernel_hooks=()),
        artifacts=artifacts,
        cost_reader=cost_reader,
        env_resolver=env_resolver,
    )
    return CloudStack(harness, api, client, artifacts, operation, remote)
