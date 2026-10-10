"""FT-B4: request_continuation on the command path, session events, checkpoint reads."""

from __future__ import annotations

import json
from typing import Any
from uuid import uuid4

import httpx
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from mission_control.application.context.continuation import (
    CheckpointReadService,
    ContinuationCommands,
    ContinuationTriggers,
    InMemoryCheckpoints,
    LocatedSession,
    RunControlContinuationEvents,
    TransferStatus,
    redact_large,
)
from mission_control.application.execution.boundary_interventions import BoundaryInterventionService
from mission_control.application.installations.registry import (
    ApplicationBinding,
    ApplicationRegistry,
    InstallationObservation,
)
from mission_control.application.missions.service import MissionControlService
from mission_control.contracts.contracts import (
    CommandTarget,
    ContinuationRequestPayload,
    MissionCommandRequest,
    MissionControlRejected,
)
from mission_control.domain.policies.contracts import (
    ActorContext,
    RecordContinuationAction,
    RequestContinuationAction,
    RunPhase,
)
from mission_control.domain.policies.reducer import required_action_permissions
from mission_control.interfaces.cli import main as cli
from mission_control.interfaces.http.continuation import router
from mission_control.interfaces.http.mission_control import MissionPrincipal, get_mission_principal
from tests.fixtures.continuation import (
    CONT_SCOPE,
    RUN_KEY,
    build_service,
    facts,
    seal_target,
    trigger,
)
from tests.fixtures.provider_frames import INSTALLATION, TENANT
from tests.unit.run_control.test_boundary_commands import TARGET, started
from tests.unit.run_control.test_run_control import actor as control_actor
from tests.unit.run_control.test_run_control import service

PERMISSIONS = {
    "workflow_run.read",
    "workflow_run.request_continuation",
    "workflow_run.record_continuation",
}


def operator() -> ActorContext:
    source = control_actor()
    return source.model_copy(update={"permissions": source.permissions | PERMISSIONS})


class Sessions:
    def __init__(self, located: LocatedSession | None) -> None:
        self.located = located

    async def current_session(
        self, request_scope: str, run_key: str, activation_ref: str | None
    ) -> LocatedSession | None:
        return self.located


def _request(run_id: str, version: int, activation_id: str | None = None) -> MissionCommandRequest:
    return MissionCommandRequest(
        request_id=uuid4(),
        expected_version=version,
        expected_generation=1,
        target=CommandTarget(id=run_id),
        kind="request_continuation",
        payload=ContinuationRequestPayload(activation_id=activation_id),
        reason="context degraded; move to a fresh session",
    )


def _events(repository: Any, run_id: str) -> list[tuple[str, dict[str, Any]]]:
    return [
        (record.envelope.event_type, dict(record.envelope.payload))
        for record in sorted(repository._outbox.values(), key=lambda item: item.cursor.position)
        if record.envelope.aggregate_id == run_id
        and record.envelope.event_type.startswith("session.")
    ]


def test_continuation_actions_map_to_their_permissions() -> None:
    requested = RequestContinuationAction(
        transfer_id="t",
        activation_id="a",
        logical_execution_id="a",
        lane_profile="deep_agents",
        source_session_ref="thread",
        delivery="turn_boundary_guaranteed",
    )
    assert required_action_permissions(requested) == {"workflow_run.request_continuation"}
    recorded = RecordContinuationAction(
        event="continuation_failed",
        transfer_id="t",
        activation_id="a",
        logical_execution_id="a",
        lane_profile="deep_agents",
        trigger_kind="request_continuation",
        source_session_ref="thread",
        failure_reason="no valid checkpoint",
    )
    assert required_action_permissions(recorded) == {"workflow_run.record_continuation"}
    with pytest.raises(ValueError):
        RecordContinuationAction(
            event="transferred",
            transfer_id="t",
            activation_id="a",
            logical_execution_id="a",
            lane_profile="deep_agents",
            trigger_kind="request_continuation",
            source_session_ref="thread",
        )


@pytest.mark.asyncio
async def test_request_continuation_is_accepted_records_a_trigger_and_an_event() -> None:
    authority, repository = service()
    run_id = await started(authority, "mission-continue", TARGET)
    wired = build_service()
    store = wired["store"]
    facade = MissionControlService(
        authority,
        BoundaryInterventionService(authority),
        request_scope="tenant-1",
        continuations=ContinuationCommands(
            ContinuationTriggers(store),
            Sessions(LocatedSession("act-1", "deep_agents", "thread-1")),
            request_scope="tenant-1",
        ),
    )
    version = (await facade.inspect(run_id, operator())).version
    request = _request(run_id, version)
    receipt = await facade.command(run_id, request, operator())
    assert receipt.admission.status == "accepted"
    assert (await facade.inspect(run_id, operator())).projection.phase == RunPhase.ACTIVE
    replay = await facade.command(run_id, request, operator())
    assert replay.replay and replay.admission == receipt.admission
    transfers = await store.for_run("tenant-1", run_id)
    assert len(transfers) == 1
    transfer = transfers[0]
    assert transfer.status == TransferStatus.REQUESTED
    assert transfer.delivery == "turn_boundary_guaranteed"
    assert transfer.trigger.ref == f"command://{request.request_id}"
    assert transfer.source_session_ref == "thread-1"
    events = _events(repository, run_id)
    assert [name for name, _ in events] == ["session.continuation_requested"]
    assert events[0][1]["delivery"] == "turn_boundary_guaranteed"
    assert events[0][1]["source_session_ref"] == "thread-1"


@pytest.mark.asyncio
async def test_request_continuation_rejections() -> None:
    authority, _ = service()
    run_id = await started(authority, "mission-continue-rejected", TARGET)
    bare = MissionControlService(
        authority, BoundaryInterventionService(authority), request_scope="tenant-1"
    )
    version = (await bare.inspect(run_id, operator())).version
    with pytest.raises(MissionControlRejected) as unsupported:
        await bare.command(run_id, _request(run_id, version), operator())
    assert unsupported.value.code == "unsupported_control"

    def facade(located: LocatedSession | None) -> MissionControlService:
        return MissionControlService(
            authority,
            BoundaryInterventionService(authority),
            request_scope="tenant-1",
            continuations=ContinuationCommands(
                ContinuationTriggers(build_service()["store"]),
                Sessions(located),
                request_scope="tenant-1",
            ),
        )

    with pytest.raises(MissionControlRejected) as no_session:
        await facade(None).command(run_id, _request(run_id, version), operator())
    assert no_session.value.code == "no_active_session"
    with pytest.raises(MissionControlRejected) as codex:
        await facade(LocatedSession("a", "codex_cloud", "s")).command(
            run_id, _request(run_id, version), operator()
        )
    assert codex.value.code == "unsupported_control"
    reader = control_actor()
    with pytest.raises(MissionControlRejected) as denied:
        await facade(LocatedSession("a", "cursor_local", "agent-1")).command(
            run_id, _request(run_id, version), reader
        )
    assert denied.value.code == "unauthorized"


@pytest.mark.asyncio
async def test_run_control_events_are_written_once_per_transfer_and_event() -> None:
    authority, repository = service()
    run_id = await started(authority, "mission-continue-events", TARGET)
    events = RunControlContinuationEvents(authority, actor=operator())
    action = RecordContinuationAction(
        event="checkpoint_sealed",
        transfer_id="transfer-1",
        activation_id="act-1",
        logical_execution_id="act-1",
        lane_profile="deep_agents",
        trigger_kind="provider_compaction",
        source_session_ref="thread-1",
        checkpoint_id="cp-1",
        checkpoint_digest="sha256:" + "c" * 64,
        validator_result="valid",
    )
    await events.record("tenant-1", run_id, action)
    await events.record("tenant-1", run_id, action)
    recorded = _events(repository, run_id)
    assert [name for name, _ in recorded] == ["session.checkpoint_sealed"]
    payload = recorded[0][1]
    assert payload["checkpoint_digest"] == "sha256:" + "c" * 64
    assert payload["source"]["native_event_ref"] == "continuation_transfer:transfer-1"


def test_redaction_replaces_large_bodies_by_digest() -> None:
    redacted, paths = redact_large(
        {"small": "x", "big": "y" * 5_000, "items": ["z" * 3_000, "w" * 3_000]}, limit=4_096
    )
    assert isinstance(redacted, dict)
    assert redacted["small"] == "x"
    assert redacted["big"]["redacted"] is True and redacted["big"]["bytes"] > 4_096
    assert redacted["items"]["items"] == 2
    assert set(paths) == {"big", "items"}


async def _sealed_store() -> tuple[dict[str, Any], Any]:
    wired = build_service(
        session_files={"thread-collect-1": {"/inputs/a.json": "{}", "/outputs/r.md": "#"}}
    )
    transfer = await wired["service"].request(
        trigger(),
        request_scope=CONT_SCOPE,
        run_key=RUN_KEY,
        activation_key="unit-collect-1",
        logical_execution_id="logical-collect-1",
        lane_profile="deep_agents",
        source_session_ref="thread-collect-1",
    )
    long_actions = facts(pending_actions=tuple(f"step {index}: " + "a" * 900 for index in range(8)))
    outcome = await wired["service"].seal(
        transfer.transfer_id, long_actions, seal_target(), request_scope=CONT_SCOPE
    )
    return wired, outcome


def http_app(reads: CheckpointReadService, permissions: frozenset[str]) -> FastAPI:
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
    app.state.mission_control_checkpoint_services = {(INSTALLATION, "biotech", TENANT): reads}
    app.dependency_overrides[get_mission_principal] = lambda: MissionPrincipal(
        installation_id=INSTALLATION,
        application_id="biotech",
        tenant_id=TENANT,
        issuer="https://issuer.invalid",
        audiences=frozenset({"authenticated"}),
        actor=ActorContext(actor_id="reader", permissions=permissions),
    )
    return app


@pytest.mark.asyncio
async def test_http_and_cli_read_checkpoints_with_large_bodies_redacted(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    wired, outcome = await _sealed_store()
    checkpoint = outcome.checkpoint
    reads = CheckpointReadService(
        InMemoryCheckpoints(wired["store"]), wired["store"], request_scope=CONT_SCOPE
    )
    client = TestClient(http_app(reads, frozenset({"workflow_run.read"})))
    base = f"/v1/applications/biotech/runs/{RUN_KEY}/checkpoints"
    listed = client.get(base)
    assert listed.status_code == 200
    body = listed.json()["checkpoints"]
    assert [item["checkpoint_id"] for item in body] == [checkpoint.checkpoint_id]
    assert body[0]["validator_result"] == "valid" and body[0]["transfer_status"] == "sealed"
    assert "recommended_next_actions" in body[0]["redacted_paths"]
    assert body[0]["checkpoint"]["recommended_next_actions"]["redacted"] is True
    one = client.get(f"{base}/{checkpoint.checkpoint_id}", params={"full": "true"})
    assert one.status_code == 200 and one.json()["redacted_paths"] == []
    assert len(one.json()["checkpoint"]["recommended_next_actions"]) == 8
    assert client.get(f"{base}/missing").status_code == 404
    denied = TestClient(http_app(reads, frozenset())).get(base)
    assert denied.status_code == 403

    real_client = httpx.Client
    app = http_app(reads, frozenset({"workflow_run.read"}))

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
    assert cli.main(["run", "checkpoint", RUN_KEY, "--list"]) == 0
    printed = json.loads(capsys.readouterr().out)
    assert printed["checkpoints"][0]["checkpoint_id"] == checkpoint.checkpoint_id
    assert (
        cli.main(["run", "checkpoint", RUN_KEY, "--get", checkpoint.checkpoint_id, "--full"]) == 0
    )
    assert json.loads(capsys.readouterr().out)["redacted_paths"] == []
    assert cli.main(["run", "checkpoint", RUN_KEY, "--get", "missing"]) != 0
