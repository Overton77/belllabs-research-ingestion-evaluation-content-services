"""FT-G5 fixtures: a Cloud Agents API v1 fake behind `httpx.MockTransport`, a bare remote.

`FakeCloudApi` replays the recorded shapes in `tests/integration/cursor/fixtures/cloud/`
(hand-authored from research/cursor-platform.md section 4, marked synthetic): agent create
with client `agentId` and `Idempotency-Key`, `409 agent_id_conflict`, `409 agent_busy`,
`403 feature_unavailable` on `metadata`, run reads, the SSE stream honoring `Last-Event-ID`
(optionally dropping the connection mid-stream, or answering `410 stream_expired`), cancel,
artifacts through presigned URLs that must not carry the API key, usage and archive. When a run
reaches its result the fake pushes the agent's commit to `mc/<run>` on the bare remote, as the
cloud agent would. No network is touched.

FT-G6: a stream can be held open after an event id (`hold_after`; released by a cancel or by
the test), deliver `after_cancel` events when the held run was cancelled (a tool call that
completes during the cancel), and later runs (`POST .../runs`, a continuation agent's create)
replay their own event lists (`later`), so every SPEC-07 section 7 control replays offline.
"""

from __future__ import annotations

import asyncio
import json
import tempfile
from collections.abc import AsyncIterator
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
from mission_control.adapters.workspaces.git_workspaces import GitWorkspaceBackend
from mission_control.application.agentic_components.projections import render_host_files
from mission_control.application.execution.harness.leases import InMemoryWorkspaceLeaseStore
from mission_control.application.workspaces.service import WorkspaceAllocator
from mission_control.domain.agentic_components.projection import ResolvedCapability
from mission_control.domain.execution.contracts import OperationExecutionRequest

FIXTURES = Path(__file__).resolve().parents[1] / "integration" / "cursor" / "fixtures" / "cloud"
API_KEY = "cursor-test-key-not-real"
PRESIGNED = "https://s3.fake.invalid/presigned"


def load_sse(name: str) -> list[SseEvent]:
    return parse_sse_text((FIXTURES / f"{name}.sse").read_text(encoding="utf-8"))


def load_cloud_fixture(
    stream: str = "run_stream", record: str = "run_record"
) -> tuple[list[SseEvent], dict[str, Any]]:
    events = load_sse(stream)
    loaded = json.loads((FIXTURES / f"{record}.json").read_text(encoding="utf-8"))
    return events, loaded


class _HeldStream(httpx.AsyncByteStream):
    def __init__(self, chunks: AsyncIterator[bytes]) -> None:
        self._chunks = chunks

    async def __aiter__(self) -> AsyncIterator[bytes]:
        async for chunk in self._chunks:
            yield chunk

    async def aclose(self) -> None:
        aclose = getattr(self._chunks, "aclose", None)
        if aclose is not None:
            await aclose()


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
    # FT-G6 controls.
    hold_after: str | None = None
    after_cancel: list[SseEvent] = field(default_factory=list)
    later: list[list[SseEvent]] = field(default_factory=list)
    run_events: dict[str, list[SseEvent]] = field(default_factory=dict)
    created_runs: list[str] = field(default_factory=list)
    run_prompts: dict[str, str] = field(default_factory=dict)
    held: asyncio.Event = field(default_factory=asyncio.Event)
    release: asyncio.Event = field(default_factory=asyncio.Event)
    hold_done: bool = False
    # MP-09 controls (FIXTURE): `429` refusals before any work is accepted, run creates whose
    # response is lost after the provider accepted, a stream that expires while the run is
    # still running (`GET run` reports RUNNING for `running_reads` more reads), and usage
    # responses that omit this run.
    rate_limited_creates: int = 0
    rate_limited_runs: int = 0
    retry_after_s: int | None = 7
    lose_run_responses: int = 0
    running_reads: int = 0
    usage_shape: str = "per_run"  # per_run | totals_only | empty | unavailable
    # `400 invalid_last_event_id` for a cursor that is not one of this run's event ids.
    reject_foreign_cursor: bool = False
    run_reads: int = 0
    rate_limit_refusals: int = 0
    expired_served: bool = False

    @property
    def run_id(self) -> str:
        return str(self.record["run"]["id"])

    def _rate_limited(self) -> httpx.Response:
        self.rate_limit_refusals += 1
        headers = {"Retry-After": str(self.retry_after_s)} if self.retry_after_s else {}
        return httpx.Response(
            429,
            headers=headers,
            json={"error": {"code": "rate_limited", "message": "too many requests"}},
        )

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
            if self.rate_limited_runs > 0:
                self.rate_limited_runs -= 1
                return self._rate_limited()
            run = self.runs[agent["latestRunId"]]
            if self.busy or run["status"] not in {"FINISHED", "ERROR", "CANCELLED", "EXPIRED"}:
                return self._error(409, "agent_busy")
            new = {"id": f"run-{uuid4()}", "agentId": agent_id, "status": "CREATING"}
            self._register_run(new, json.loads(request.content))
            agent["latestRunId"] = new["id"]
            agent["status"] = "ACTIVE"
            if self.lose_run_responses > 0:
                # The provider accepted the run; the response never reaches the caller.
                self.lose_run_responses -= 1
                raise httpx.ReadError("connection reset after the provider accepted the run")
            return httpx.Response(200, json={"run": new})
        if len(rest) >= 2 and rest[0] == "runs":
            run = self.runs.get(rest[1])
            if run is None:
                return self._error(404, "run_not_found")
            if len(rest) == 2 and method == "GET":
                self.run_reads += 1
                if self.running_reads > 0 and run["status"] != "CANCELLED":
                    # The stream retention passed while the run kept running.
                    self.running_reads -= 1
                    return httpx.Response(200, json={"run": {**run, "status": "RUNNING"}})
                if self.expired_served and run["status"] in {"CREATING", "RUNNING"}:
                    # The retention window passed while the run kept running; it is final now.
                    self._finish(agent, run)
                return httpx.Response(200, json={"run": run})
            if rest[2:] == ["cancel"] and method == "POST":
                if run["status"] in {"FINISHED", "ERROR", "CANCELLED", "EXPIRED"}:
                    return self._error(409, "run_not_cancellable")
                run["status"] = "CANCELLED"
                agent["status"] = "IDLE"
                self.cancelled.append(rest[1])
                self.release.set()
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
            if self.usage_shape == "unavailable":
                return self._error(403, "feature_unavailable")
            if self.usage_shape == "empty":
                return httpx.Response(200, json={})
            if self.usage_shape == "totals_only":
                return httpx.Response(
                    200, json={"totalUsage": self.record["usage"]["totalUsage"], "runs": []}
                )
            return httpx.Response(200, json=self.record["usage"])
        if rest == ["archive"] and method == "POST":
            self.archived.append(agent_id)
            agent["status"] = "ARCHIVED"
            return httpx.Response(200, json={})
        return self._error(404, "route_not_found")

    def _create(self, request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        self.creates.append(body)
        if self.rate_limited_creates > 0:
            self.rate_limited_creates -= 1
            return self._rate_limited()
        key = request.headers.get("Idempotency-Key")
        if key is not None and key in self.idempotency:
            return httpx.Response(200, json=self.idempotency[key])
        if "metadata" in body and self.metadata_unavailable:
            return self._error(403, "feature_unavailable")
        agent_id = body.get("agentId") or f"bc-{uuid4()}"
        if agent_id in self.agents:
            return self._error(409, "agent_id_conflict")
        run_id = self.run_id if self.run_id not in self.runs else f"run-{uuid4()}"
        run = {"id": run_id, "agentId": agent_id, "status": "CREATING"}
        agent = {
            "id": agent_id,
            "name": body.get("name"),
            "status": "ACTIVE",
            "url": f"https://cursor.com/agents/{agent_id}",
            "latestRunId": run["id"],
            "repos": body.get("repos"),
        }
        self.agents[agent_id] = agent
        self._register_run(run, body)
        response = {"agent": agent, "run": run}
        if key is not None:
            self.idempotency[key] = response
        return httpx.Response(200, json=response)

    def _register_run(self, run: dict[str, Any], body: dict[str, Any]) -> None:
        self.runs[run["id"]] = run
        self.created_runs.append(run["id"])
        prompt = (body.get("prompt") or {}).get("text")
        if prompt is not None:
            self.run_prompts[run["id"]] = str(prompt)
        if run["id"] != self.run_id and self.later:
            self.run_events[run["id"]] = self.later.pop(0)

    def _stream(
        self, request: httpx.Request, agent: dict[str, Any], run: dict[str, Any]
    ) -> httpx.Response:
        last = request.headers.get("Last-Event-ID")
        self.stream_requests.append(last)
        events_of_run = self.run_events.get(run["id"], self.events)
        if (
            self.reject_foreign_cursor
            and last is not None
            and last not in {event.id for event in events_of_run if event.id is not None}
        ):
            return self._error(400, "invalid_last_event_id")
        if (
            self.expire_after is not None
            and last is not None
            and int(last) >= int(self.expire_after)
        ):
            self.expired_served = True
            if self.running_reads <= 0:
                self._finish(agent, run)
            return self._error(410, "stream_expired")
        events = self.run_events.get(run["id"], self.events)
        held = run["id"] == self.run_id and self.hold_after is not None and not self.hold_done

        async def chunks() -> AsyncIterator[bytes]:
            if run["status"] == "CANCELLED" and run["id"] in self.cancelled:
                # A cancelled run's stream drains what closed during the cancel, then ends.
                for event in events:
                    if event.id is None and event.event == "status":
                        yield render_sse([event]).encode("utf-8")
                        break
                for extra in self.after_cancel:
                    if last is None or extra.id is None or int(extra.id) > int(last):
                        yield render_sse([extra]).encode("utf-8")
                return
            seen_last = last is None
            for event in events:
                if event.id is None:
                    # The leading status has no id and is re-sent on every reconnect.
                    if event.event == "status" or seen_last:
                        yield render_sse([event]).encode("utf-8")
                    continue
                if not seen_last:
                    seen_last = event.id == last
                    continue
                if event.event == "result":
                    # The run is final (and the agent's commit pushed) before the client
                    # reads the result event: the reader may close the stream right after it.
                    self._finish(agent, run)
                yield render_sse([event]).encode("utf-8")
                if self.cut_after is not None and event.id == self.cut_after:
                    self.cut_after = None
                    return
                if held and event.id == self.hold_after:
                    self.held.set()
                    await self.release.wait()
                    self.hold_done = True
                    if run["status"] == "CANCELLED":
                        for extra in self.after_cancel:
                            yield render_sse([extra]).encode("utf-8")
                        return

        return httpx.Response(
            200,
            headers={
                "Content-Type": "text/event-stream",
                RETENTION_HEADER: str(self.record["retention_seconds"]),
            },
            stream=_HeldStream(chunks()),
        )

    def _finish(self, agent: dict[str, Any], run: dict[str, Any]) -> None:
        if run.get("status") == "CANCELLED":
            agent["status"] = "IDLE"
            return
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
    # MP-09: the provider workspace ledger the harness records its branch leases in.
    leases: InMemoryWorkspaceLeaseStore | None = None
    workspaces: WorkspaceAllocator | None = None


def cloud_operation(
    remote: Path | str, rows: tuple[ResolvedCapability, ...], **cloud_changes: Any
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
    stream: str = "run_stream",
    record: str = "run_record",
    operation_changes: dict[str, Any] | None = None,
    remote: Path | None = None,
    inputs: Any = None,
    artifacts: MemoryArtifacts | None = None,
    workspaces: bool = True,
    harness_changes: dict[str, Any] | None = None,
) -> CloudStack:
    events, record_body = load_cloud_fixture(stream, record)
    remote = remote or make_remote(tmp_path)
    rows = cloud_rows()
    operation = cloud_operation(remote, rows, **(cloud_changes or {}))
    if operation_changes:
        operation = OperationExecutionRequest.model_validate(
            {**operation.model_dump(mode="python"), **operation_changes}
        )
    api = FakeCloudApi(events, record_body, remote, **(api_changes or {}))
    client = CloudAgentsClient(SecretStr(API_KEY), transport=api.transport())
    artifacts = artifacts or MemoryArtifacts()

    async def cost_reader(agent_id: str, run_id: str) -> int | None:
        return cost

    async def env_resolver(ref: str) -> dict[str, str]:
        return dict(env_vars or {})

    leases: InMemoryWorkspaceLeaseStore | None = None
    allocator: WorkspaceAllocator | None = None
    if workspaces:
        leases = InMemoryWorkspaceLeaseStore()
        root = tmp_path / "provider-workspaces"
        allocator = WorkspaceAllocator(
            ledger=leases,
            backend=GitWorkspaceBackend(root),
            root=root,
            allocator_ref="fixture:cursor-cloud",
        )
    harness = CursorCloudHarness(
        client=client,
        publisher=GitBranchPublisher(tmp_path / "mirrors"),
        projections=RenderedProjectionSource(static_rows(rows), kernel_hooks=()),
        artifacts=artifacts,
        inputs=inputs,
        cost_reader=cost_reader,
        env_resolver=env_resolver,
        workspaces=allocator,
        expired_poll_interval_s=0.01,
        **dict(harness_changes or {}),
    )
    return CloudStack(harness, api, client, artifacts, operation, remote, leases, allocator)
