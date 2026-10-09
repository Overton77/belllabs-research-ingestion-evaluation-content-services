"""FT-G1: request pairing, digest stability and `OperationExecutionService` dispatch by lane."""

from __future__ import annotations

from typing import Any

import httpx
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from pydantic import ValidationError

from mission_control.adapters.cursor import cursor_lane_stubs
from mission_control.application.execution.harness.deep_agents_harness import DeepAgentsHarness
from mission_control.application.execution.harness.registry import (
    LaneNotQualified,
    LaneRegistry,
    describe_only_registry,
)
from mission_control.application.execution.operations.operation_execution import (
    OperationExecutionService,
    bind_operation_execution_request,
)
from mission_control.domain.authoring.canonical import contract_fingerprint, sha256_digest
from mission_control.domain.execution.contracts import (
    OperationExecutionBinding,
    OperationExecutionRequest,
)
from mission_control.domain.execution.lanes import CursorExecutionBinding
from mission_control.interfaces.cli.main import main as missionctl
from mission_control.interfaces.http import lanes as lanes_http
from mission_control.interfaces.http.mission_control import get_mission_principal
from tests.unit.operations.test_operation_execution import operation_request, service_fixture

DIGEST = "sha256:" + "c" * 64


def _cursor_binding(profile: str = "cursor_local") -> CursorExecutionBinding:
    fields: dict[str, Any] = {
        "lane_profile": profile,
        "pins": {
            "cursor_sdk": "1.0.37",
            "bridge": "1.0.37",
            "protocol": "sdk.v1",
            "cloud_api": "v1",
        },
        "model_id": "composer-2",
        "workspace": {"base_ref": "main"},
        "projections": {"rules_digest": DIGEST, "agents_digest": DIGEST, "hooks_digest": DIGEST},
        "hook_callback": {"listen": "127.0.0.1:47555", "token_ttl_s": 3600},
        "budgets": {"max_turns": 4, "max_segments": 8, "wall_clock_s": 3600},
    }
    return CursorExecutionBinding.sealed(**fields)


def _payload(**changes: Any) -> dict[str, Any]:
    payload = operation_request().model_dump(mode="python")
    payload.update(changes)
    return payload


def _cursor_request(**changes: Any) -> OperationExecutionRequest:
    fields: dict[str, Any] = {
        "execution_runtime": "cursor",
        "native_placement": None,
        "lane_profile": "cursor_local",
        "cursor_binding": _cursor_binding(),
        **changes,
    }
    return OperationExecutionRequest.model_validate(_payload(**fields))


def test_cursor_runtime_pairs_with_exactly_one_cursor_binding() -> None:
    request = _cursor_request()
    assert request.execution_runtime == "cursor" and request.lane_profile == "cursor_local"
    with pytest.raises(ValidationError, match="Cursor binding"):
        _cursor_request(cursor_binding=None)
    with pytest.raises(ValidationError, match="Cursor binding"):
        OperationExecutionRequest.model_validate(_payload(cursor_binding=_cursor_binding()))
    with pytest.raises(ValidationError, match="lane profile"):
        _cursor_request(lane_profile="cursor_cloud")
    with pytest.raises(ValidationError, match="Cursor lane profile"):
        _cursor_request(lane_profile="deep_agents")
    with pytest.raises(ValidationError, match="deep_agents lane profile"):
        OperationExecutionRequest.model_validate(_payload(lane_profile="cursor_local"))
    binding = bind_operation_execution_request(request)
    assert binding.execution_runtime == "cursor" and binding.cursor_binding is not None
    assert OperationExecutionBinding.model_validate(binding.model_dump(mode="python")) == binding


def test_absent_lane_fields_change_no_dump_digest_or_fingerprint() -> None:
    request = operation_request()
    dumped = request.model_dump(mode="json")
    assert "lane_profile" not in dumped and "cursor_binding" not in dumped
    assert "provider_binding" not in dumped
    legacy_fields = {
        name: getattr(request, name)
        for name in type(request).model_fields
        if name not in {"lane_profile", "cursor_binding", "provider_binding"}
    }
    assert sha256_digest(request) == sha256_digest(legacy_fields)
    assert contract_fingerprint(request) == sha256_digest(legacy_fields)
    binding = bind_operation_execution_request(request)
    assert "lane_profile" not in binding.model_dump(mode="json")
    explicit = OperationExecutionRequest.model_validate(_payload(lane_profile="deep_agents"))
    assert contract_fingerprint(explicit) != contract_fingerprint(request)


async def test_service_dispatches_through_the_registry() -> None:
    service, _bindings, runtime, _events, _budget = service_fixture()
    result = await service.execute(operation_request())
    assert result.status == "completed" and len(runtime.invocations) == 1


async def test_service_refuses_an_unqualified_cursor_lane_before_binding() -> None:
    service, bindings, runtime, _events, _budget = service_fixture()
    lanes = LaneRegistry([DeepAgentsHarness(runtime), *cursor_lane_stubs()])
    guarded = OperationExecutionService(
        **{**_service_ports(service), "runtime": runtime, "lanes": lanes}
    )
    with pytest.raises(LaneNotQualified):
        await guarded.execute(_cursor_request())
    assert runtime.invocations == []
    assert (
        await bindings.get_binding(
            _cursor_request().identity.semantic_key, request_scope="tenant-1"
        )
        is None
    )


def _service_ports(service: OperationExecutionService) -> dict[str, Any]:
    return {
        "authority": service._authority,
        "bindings": service._bindings,
        "sandbox": service._sandbox,
        "assets": service._assets,
        "mcp": service._mcp,
        "secrets": service._secrets,
        "events": service._events,
        "budget": service._budget,
    }


def _lanes_app(registry: LaneRegistry) -> FastAPI:
    from mission_control.domain.policies.contracts import ActorContext
    from mission_control.interfaces.http.mission_control import MissionPrincipal

    app = FastAPI()
    app.include_router(lanes_http.router)
    app.state.mission_control_lanes = registry
    app.dependency_overrides[get_mission_principal] = lambda: MissionPrincipal(
        installation_id=__import__("uuid").uuid4(),
        application_id="biotech",
        tenant_id=__import__("uuid").uuid4(),
        issuer="https://issuer.invalid",
        audiences={"authenticated"},
        actor=ActorContext(actor_id="operator"),
    )
    return app


def test_lane_routes_list_and_describe(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(lanes_http, "authorize_application", lambda *args: None)
    client = TestClient(
        _lanes_app(describe_only_registry(cursor_bound=True, allow_unqualified=False))
    )
    listed = client.get("/v1/applications/biotech/lanes")
    assert listed.status_code == 200
    body = listed.json()
    assert [lane["lane_profile"] for lane in body["lanes"]] == [
        "cursor_cloud",
        "cursor_local",
        "deep_agents",
    ]
    assert {lane["lane_profile"]: lane["qualified"] for lane in body["lanes"]} == {
        "cursor_cloud": False,
        "cursor_local": False,
        "deep_agents": True,
    }
    described = client.get("/v1/applications/biotech/lanes/deep_agents")
    assert described.status_code == 200
    assert described.json()["schema_version"] == "mc.lane_describe.v1"
    assert client.get("/v1/applications/biotech/lanes/codex").status_code == 404
    missing = TestClient(_lanes_app(LaneRegistry()))
    missing.app.state.mission_control_lanes = None  # type: ignore[attr-defined]
    assert missing.get("/v1/applications/biotech/lanes").status_code == 503


def test_missionctl_lane_list_and_describe(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    seen: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request.url.path)
        if request.url.path.endswith("/lanes"):
            return httpx.Response(200, json={"lanes": [], "allow_unqualified": False})
        return httpx.Response(200, json={"lane_profile": "deep_agents"})

    real_client = httpx.Client

    def client_factory(**kwargs: Any) -> httpx.Client:
        return real_client(transport=httpx.MockTransport(handler), **kwargs)

    monkeypatch.setattr(httpx, "Client", client_factory)
    monkeypatch.setenv("MISSION_CONTROL_TOKEN", "token-never-printed")
    assert missionctl(["lane", "list", "--application", "biotech"]) == 0
    assert (
        missionctl(["lane", "describe", "deep_agents", "--application", "biotech", "--json"]) == 0
    )
    assert seen == [
        "/v1/applications/biotech/lanes",
        "/v1/applications/biotech/lanes/deep_agents",
    ]
    assert "token-never-printed" not in capsys.readouterr().out
