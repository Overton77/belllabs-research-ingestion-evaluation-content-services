from fastapi.testclient import TestClient

from app.api.control_plane import (
    ControlPlanePrincipal,
    get_control_plane_principal,
    get_control_plane_service,
)
from app.application.control_plane import ControlPlaneService
from app.application.control_plane_repository import InMemoryDefinitionRepository
from app.domain.control_plane.extensions import ExtensionRegistry
from app.integrations.control_plane_payloads import InMemoryPayloadStore
from app.server import api


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
