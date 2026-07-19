import pytest
from fastapi.testclient import TestClient

from app.api.control_plane import (
    ControlPlanePrincipal,
    get_control_plane_principal,
    get_control_plane_service,
)
from app.api.run_control import get_run_control_service
from app.application.control_plane import ControlPlaneService
from app.application.control_plane_repository import InMemoryDefinitionRepository
from app.domain.control_plane.extensions import ExtensionRegistry
from app.integrations.control_plane_payloads import InMemoryPayloadStore
from app.server import api
from tests.test_run_control import request as run_request
from tests.test_run_control import service as run_control_service


@pytest.fixture(autouse=True)
def disable_external_run_control_startup(monkeypatch: pytest.MonkeyPatch) -> None:
    async def noop(_application: object) -> None:
        return None

    monkeypatch.setattr("app.server.initialize_run_control_resources", noop)


def _test_service() -> ControlPlaneService:
    return ControlPlaneService(
        InMemoryDefinitionRepository(),
        ExtensionRegistry(),
        InMemoryPayloadStore(),
    )


def test_liveness() -> None:
    with TestClient(api) as client:
        response = client.get("/health/live")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_request_body_limit_rejects_oversized_payloads() -> None:
    with TestClient(api) as client:
        response = client.post(
            "/run-control/v1/run-requests",
            content=b"x" * 1_000_001,
            headers={"content-type": "application/json"},
        )
    assert response.status_code == 413


def test_control_plane_schema_and_typed_not_found_error() -> None:
    api.dependency_overrides[get_control_plane_service] = _test_service
    try:
        with TestClient(api) as client:
            schema_response = client.get("/control-plane/v1/schemas")
            missing_response = client.get(
                "/control-plane/v1/effective-run-configurations/sha256:" + "0" * 64
            )
    finally:
        api.dependency_overrides.pop(get_control_plane_service, None)

    assert schema_response.status_code == 200
    assert "effective_run_configuration" in schema_response.json()
    assert missing_response.status_code == 404
    assert missing_response.json()["code"] == "definition_not_found"


def test_control_plane_publish_route_uses_strict_contracts() -> None:
    request = {
        "definition": {
            "schema_version": "1",
            "logical_id": "api.generic-goal",
            "title": "API generic goal",
            "description": "Contract-only API fixture",
            "kind": "blueprint",
            "family": "GoalDirected",
            "objective_contract": "contract:objective@1",
            "acceptance_contract": "contract:acceptance@1",
            "independent_verification_required": True,
            "max_iterations": 1,
            "variant_names": [],
        },
        "actor_id": "api-test",
        "published_at": "2026-01-02T03:04:00Z",
        "expected_head_revision": 0,
    }
    api.dependency_overrides[get_control_plane_service] = _test_service
    api.dependency_overrides[get_control_plane_principal] = lambda: ControlPlanePrincipal(
        actor_id="api-test", roles=frozenset({"publisher"})
    )
    try:
        with TestClient(api) as client:
            response = client.post("/control-plane/v1/definitions", json=request)
    finally:
        api.dependency_overrides.pop(get_control_plane_service, None)
        api.dependency_overrides.pop(get_control_plane_principal, None)

    assert response.status_code == 201
    assert response.json()["ref"]["revision"] == 1


def test_run_control_route_uses_authenticated_actor_and_exports_schemas() -> None:
    service, _repository = run_control_service()
    api.dependency_overrides[get_run_control_service] = lambda: service
    api.dependency_overrides[get_control_plane_principal] = lambda: ControlPlanePrincipal(
        actor_id="operator",
        roles=frozenset({"operator"}),
        tenant_scopes=frozenset({"tenant-1"}),
        authority_refs=frozenset({"authority:lifecycle"}),
        sponsorship_refs=frozenset({"sponsorship:test"}),
        approval_refs=frozenset({"approval:test"}),
    )
    try:
        with TestClient(api) as client:
            schema_response = client.get("/run-control/v1/schemas")
            admitted_response = client.post(
                "/run-control/v1/run-requests",
                json=run_request().model_dump(mode="json"),
            )
            forged = run_request(request_id="forged").model_copy(
                update={
                    "actor": run_request().actor.model_copy(update={"actor_id": "another-actor"})
                }
            )
            forged_response = client.post(
                "/run-control/v1/run-requests",
                json=forged.model_dump(mode="json"),
            )
            cross_tenant_response = client.post(
                "/run-control/v1/run-requests",
                json=run_request(request_scope="tenant-2", request_id="cross-tenant").model_dump(
                    mode="json"
                ),
            )
    finally:
        api.dependency_overrides.pop(get_run_control_service, None)
        api.dependency_overrides.pop(get_control_plane_principal, None)

    assert schema_response.status_code == 200
    assert "lifecycle_command" in schema_response.json()
    assert admitted_response.status_code == 201
    assert admitted_response.json()["status"] == "accepted"
    assert forged_response.status_code == 403
    assert cross_tenant_response.status_code == 404
