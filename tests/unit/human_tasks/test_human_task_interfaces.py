"""MP-10: one Human Task service behind HTTP and MCP; reviewer authority in the service.

In-memory repository (FIXTURE semantics of the PostgreSQL repository, whose persistence and
concurrency are proven in tests/integration/postgres/test_human_gate_tasks_postgres.py).
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from fastmcp import Client, FastMCP

from mission_control.application.human_tasks.memory import InMemoryHumanTaskRepository
from mission_control.application.human_tasks.service import HumanTaskRejected, HumanTaskService
from mission_control.application.installations.registry import (
    ApplicationBinding,
    ApplicationRegistry,
    InstallationObservation,
)
from mission_control.domain.policies.contracts import ActorContext
from mission_control.domain.programs.human_gate import HumanTaskView
from mission_control.interfaces.http.human_tasks import router
from mission_control.interfaces.http.mission_control import MissionPrincipal, get_mission_principal
from mission_control.interfaces.mcp.coordinator_server import CoordinatorPrincipal, _principal_call
from mission_control.interfaces.mcp.human_task_tools import (
    HUMAN_TASK_GET_TOOL,
    HUMAN_TASK_LIST_TOOL,
    HUMAN_TASK_RESOLVE_TOOL,
    ScopedHumanTasks,
    register_human_task_tools,
)
from tests.fixtures.provider_frames import INSTALLATION, SCOPE, TENANT
from tests.unit.human_tasks.fixtures import activation, later, spec

BASE = "/v1/applications/biotech"


class RecordingWake:
    def __init__(self, *, fail: bool = False) -> None:
        self.woken: list[str] = []
        self.fail = fail

    async def resolution_committed(self, task: HumanTaskView) -> None:
        self.woken.append(task.human_task_id)
        if self.fail:
            raise RuntimeError("temporal unavailable")


async def _service(
    wake: RecordingWake | None = None,
) -> tuple[HumanTaskService, InMemoryHumanTaskRepository, list[HumanTaskView]]:
    repository = InMemoryHumanTaskRepository()
    tasks = [
        await repository.open(activation(scope=SCOPE, run_id=run), actor_ref="runtime")
        for run in ("run-1", "run-2")
    ]
    service = HumanTaskService(repository, request_scope=SCOPE, wake=wake, clock=lambda: later(30))
    return service, repository, tasks


def _app(service: HumanTaskService, actor: str = "owner") -> FastAPI:
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
    app.state.mission_control_human_task_services = {(INSTALLATION, "biotech", TENANT): service}
    app.dependency_overrides[get_mission_principal] = lambda: MissionPrincipal(
        installation_id=INSTALLATION,
        application_id="biotech",
        tenant_id=TENANT,
        issuer="https://issuer.invalid",
        audiences=frozenset({"authenticated"}),
        actor=ActorContext(actor_id=actor),
    )
    return app


def _body(task: HumanTaskView, **updates: Any) -> dict[str, Any]:
    return {
        "request_id": "http-1",
        "expected_task_version": task.version,
        "decision": "approve",
        "reviewed_packet_digest": task.activation.packet_digest,
        **updates,
    }


@pytest.mark.asyncio
async def test_http_list_read_and_one_attributed_resolution_with_typed_refusals() -> None:
    wake = RecordingWake()
    service, repository, (task, _other) = await _service(wake)
    client = TestClient(_app(service))
    listing = client.get(f"{BASE}/human-tasks", params={"run_id": "run-1"}).json()
    assert [item["human_task_id"] for item in listing["human_tasks"]] == [task.human_task_id]
    read = client.get(f"{BASE}/human-tasks/{task.human_task_id}").json()
    assert read["lifecycle"] == "open" and read["origin"] == "workflow_gate"
    assert read["packet_digest"] == task.activation.packet_digest
    assert read["permitted_decisions"] == ["approve", "deny"]

    path = f"{BASE}/human-tasks/{task.human_task_id}/resolutions"
    intruder = TestClient(_app(service, actor="intruder")).post(path, json=_body(task))
    assert intruder.status_code == 403 and intruder.json()["detail"]["code"] == "not_reviewer"
    stale = client.post(path, json=_body(task, expected_task_version=2))
    assert stale.status_code == 409 and stale.json()["detail"]["code"] == "stale_version"
    old_packet = activation(digest="sha256:" + "0" * 64).packet_digest
    changed = client.post(path, json=_body(task, reviewed_packet_digest=old_packet))
    assert changed.json()["detail"]["code"] == "packet_digest_mismatch"
    unadmitted = client.post(path, json=_body(task, decision="request_changes", comment="x"))
    assert unadmitted.status_code == 422
    assert wake.woken == []

    accepted = client.post(path, json=_body(task))
    assert accepted.status_code == 200 and accepted.json()["status"] == "accepted"
    resolution = accepted.json()["task"]["resolution"]
    assert resolution["actor_ref"] == "owner" and resolution["decision"] == "approve"
    retry = client.post(path, json=_body(task))
    assert retry.json()["status"] == "duplicate"
    assert retry.json()["task"]["resolution"] == resolution
    second = client.post(path, json=_body(task, request_id="http-2", decision="deny"))
    assert second.status_code == 409 and second.json()["detail"]["code"] == "already_resolved"
    assert wake.woken == [task.human_task_id, task.human_task_id]
    assert [event.event_type for event in repository.events].count("human_task.resolved") == 1
    assert client.get(f"{BASE}/human-tasks/not-a-uuid").status_code == 404


@pytest.mark.asyncio
async def test_two_concurrent_reviewers_get_exactly_one_resolution() -> None:
    service, repository, (task, _other) = await _service()
    body = _body(task)

    async def answer(actor: str, decision: str) -> str:
        from mission_control.domain.programs.human_gate import HumanResolutionRequest

        try:
            receipt = await service.resolve(
                task.human_task_id,
                HumanResolutionRequest.model_validate(
                    {**body, "request_id": f"{actor}-1", "decision": decision}
                ),
                ActorContext(actor_id=actor, permissions=frozenset({"reviewer:owner"})),
            )
        except HumanTaskRejected as rejected:
            return rejected.code
        return receipt.status

    results = await asyncio.gather(answer("alice", "approve"), answer("bob", "deny"))
    assert sorted(results) == ["accepted", "already_resolved"]
    stored = await repository.get(SCOPE, task.human_task_id)
    assert stored is not None and stored.lifecycle == "resolved" and stored.version == 2


@pytest.mark.asyncio
async def test_a_lost_wake_never_loses_a_committed_resolution() -> None:
    service, repository, (task, _other) = await _service(RecordingWake(fail=True))
    client = TestClient(_app(service))
    response = client.post(f"{BASE}/human-tasks/{task.human_task_id}/resolutions", json=_body(task))
    assert response.status_code == 200
    stored = await repository.get(SCOPE, task.human_task_id)
    assert stored is not None and stored.lifecycle == "resolved"


@pytest.mark.asyncio
async def test_mcp_tools_call_the_same_service_with_http_parity() -> None:
    service, _repository, (first, second) = await _service()
    principal = CoordinatorPrincipal(
        actor_id="owner",
        tenant_scope=SCOPE,
        roles=frozenset(),
        permissions=frozenset(),
        request_scope=SCOPE,
    )

    class Principals:
        async def resolve(self, context: Any) -> CoordinatorPrincipal:
            del context
            return principal

    server = FastMCP("human-task-test")
    register_human_task_tools(
        server, ScopedHumanTasks({SCOPE: service}), Principals(), call=_principal_call
    )
    http = TestClient(_app(service))
    async with Client(server) as client:
        tools = {tool.name: tool for tool in await client.list_tools()}
        assert tools[HUMAN_TASK_LIST_TOOL].annotations.readOnlyHint is True  # type: ignore[union-attr]
        assert tools[HUMAN_TASK_GET_TOOL].annotations.readOnlyHint is True  # type: ignore[union-attr]
        listed = await client.call_tool(HUMAN_TASK_LIST_TOOL, {"lifecycle": "open"})
        assert listed.structured_content is not None
        assert len(listed.structured_content["data"]["human_tasks"]) == 2
        resolved = await client.call_tool(
            HUMAN_TASK_RESOLVE_TOOL,
            {"human_task_id": second.human_task_id, "request": _body(second, request_id="mcp")},
        )
        envelope = resolved.structured_content
        assert envelope is not None and envelope["ok"] is True
        assert envelope["data"]["status"] == "accepted"
        via_http = http.get(f"{BASE}/human-tasks/{second.human_task_id}").json()
        assert via_http["resolution"] == envelope["data"]["task"]["resolution"]
        # The HTTP retry of the MCP resolution is the same idempotent answer.
        retry = http.post(
            f"{BASE}/human-tasks/{second.human_task_id}/resolutions",
            json=_body(second, request_id="mcp"),
        )
        assert retry.json()["status"] == "duplicate"
        refused = await client.call_tool(
            HUMAN_TASK_RESOLVE_TOOL,
            {
                "human_task_id": first.human_task_id,
                "request": _body(first, expected_task_version=9),
            },
            raise_on_error=False,
        )
        assert refused.structured_content is not None
        assert refused.structured_content["ok"] is False
        got = await client.call_tool(HUMAN_TASK_GET_TOOL, {"human_task_id": first.human_task_id})
        assert got.structured_content is not None
        assert got.structured_content["data"]["lifecycle"] == "open"


def test_reviewer_spec_grants_come_only_from_the_verified_principal() -> None:
    assert spec().reviewers == ("owner",)
