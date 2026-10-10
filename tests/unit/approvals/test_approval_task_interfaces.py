"""MP-11: approval-origin tasks through MP-10's one Human Task service, HTTP and socket.

The HTTP router and the socket gateway are MP-10's, unchanged; they resolve approval tasks
because the service (additively) serves both kinds. In-memory stores (FIXTURE semantics).
"""

from __future__ import annotations

from typing import Any

from fastapi import FastAPI
from fastapi.testclient import TestClient

from mission_control.application.execution.approvals_broker import ApprovalBroker
from mission_control.application.execution.approvals_memory import (
    InMemoryApprovalStore,
    StaticApprovalContext,
)
from mission_control.application.human_tasks.memory import InMemoryHumanTaskRepository
from mission_control.application.human_tasks.service import HumanTaskService
from mission_control.application.installations.registry import (
    ApplicationBinding,
    ApplicationRegistry,
    InstallationObservation,
)
from mission_control.domain.policies.contracts import ActorContext
from mission_control.interfaces.http.human_tasks import router
from mission_control.interfaces.http.mission_control import MissionPrincipal, get_mission_principal
from mission_control.interfaces.socketio.commands import (
    forward_human_task_resolution,
    parse_human_task_resolution,
)
from tests.fixtures.provider_frames import INSTALLATION, SCOPE, TENANT
from tests.unit.approvals.fixtures import POLICY, REVIEWER, permission_request
from tests.unit.human_tasks.fixtures import activation

BASE = "/v1/applications/biotech"


def _principal(actor: str = REVIEWER) -> MissionPrincipal:
    return MissionPrincipal(
        installation_id=INSTALLATION,
        application_id="biotech",
        tenant_id=TENANT,
        issuer="https://issuer.invalid",
        audiences=frozenset({"authenticated"}),
        actor=ActorContext(actor_id=actor),
    )


def _app(service: HumanTaskService, actor: str = REVIEWER) -> FastAPI:
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
    app.dependency_overrides[get_mission_principal] = lambda: _principal(actor)
    return app


async def _compose() -> tuple[HumanTaskService, ApprovalBroker, Any, Any]:
    store = InMemoryApprovalStore()
    broker = ApprovalBroker(
        store,
        store,
        probe=StaticApprovalContext(1, POLICY),
        connection_ref="worker-a#1",
        poll_seconds=0.01,
    )
    gates = InMemoryHumanTaskRepository()
    gate = await gates.open(activation(scope=SCOPE, run_id="run-1"), actor_ref="runtime")
    service = HumanTaskService(
        gates, request_scope=SCOPE, approvals=store, approval_wake=broker.hub
    )
    return service, broker, store, gate


def _body(task: dict[str, Any], **updates: Any) -> dict[str, Any]:
    return {
        "request_id": "http-1",
        "expected_task_version": task["version"],
        "decision": "approve",
        "reviewed_packet_digest": task["packet_digest"],
        **updates,
    }


async def test_one_service_lists_reads_and_resolves_gate_and_approval_tasks() -> None:
    service, broker, _store, gate = await _compose()
    bound = await broker.bind(permission_request())
    client = TestClient(_app(service))
    listing = client.get(f"{BASE}/human-tasks", params={"run_id": "run-1"}).json()["human_tasks"]
    origins = sorted(item["origin"] for item in listing)
    assert origins == ["provider_permission", "workflow_gate"]
    read = client.get(f"{BASE}/human-tasks/{bound.task.human_task_id}").json()
    assert read["origin"] == "provider_permission" and read["lifecycle"] == "open"
    assert read["pending_tool"]["tool_name"] == "Bash"
    assert read["permitted_decisions"] == ["approve", "approve_edited", "deny", "cancel"]
    assert "toolu_fixture_1" not in str(read) and "worker-a#1" not in str(read)
    only = await service.list(ActorContext(actor_id=REVIEWER), origin="provider_permission")
    assert [task.human_task_id for task in only] == [bound.task.human_task_id]
    gates = await service.list(ActorContext(actor_id=REVIEWER), origin="workflow_gate")
    assert [task.human_task_id for task in gates] == [gate.human_task_id]

    path = f"{BASE}/human-tasks/{bound.task.human_task_id}/resolutions"
    stranger = TestClient(_app(service, actor="intruder")).post(path, json=_body(read))
    assert stranger.status_code == 403
    unadmitted = client.post(path, json=_body(read, decision="request_changes", comment="x"))
    assert unadmitted.status_code == 422
    assert unadmitted.json()["detail"]["code"] == "decision_not_admitted"
    feedback = _body(read, decision="deny", comment="open a pull request instead")
    first = client.post(path, json=feedback)
    assert first.status_code == 200 and first.json()["status"] == "accepted"
    assert first.json()["task"]["resolution"]["resolution_action"] == "denied"
    retry = client.post(path, json=feedback)
    assert retry.json()["status"] == "duplicate"
    outcome = await broker.wait(bound, wait_seconds=1)
    assert outcome.status == "denied" and outcome.reply.interrupt is False
    assert outcome.reply.message is not None and "pull request" in outcome.reply.message


async def test_http_and_socket_retries_of_the_same_resolution_are_idempotent() -> None:
    service, broker, store, _gate = await _compose()
    bound = await broker.bind(permission_request())
    app = _app(service)
    task = TestClient(app).get(f"{BASE}/human-tasks/{bound.task.human_task_id}").json()
    body = _body(task, request_id="shared-1")
    http = TestClient(app).post(
        f"{BASE}/human-tasks/{bound.task.human_task_id}/resolutions", json=body
    )
    assert http.json()["status"] == "accepted"
    socket_request = parse_human_task_resolution(
        {"application_id": "biotech", "human_task_id": bound.task.human_task_id, **body}
    )
    replay = await forward_human_task_resolution(app, _principal(), socket_request)
    assert replay["status"] == "duplicate"
    assert replay["task"]["resolution"] == http.json()["task"]["resolution"]
    resolved = await store.get_task(SCOPE, bound.task.human_task_id)
    assert resolved is not None and resolved.version == 2
    assert [event.event_type for event in store.events].count("human_task.resolved") == 1
