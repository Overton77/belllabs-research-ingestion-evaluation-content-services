from __future__ import annotations

from uuid import uuid4

import httpx
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from tests.unit.run_control.test_run_control import service as run_control_service

from mission_control.application.execution.boundary_interventions import BoundaryInterventionService
from mission_control.application.installations.registry import (
    ApplicationBinding,
    ApplicationRegistry,
    InstallationObservation,
)
from mission_control.application.missions.service import MissionControlService
from mission_control.domain.policies.contracts import ActorContext
from mission_control.interfaces.cli.main import MissionClient, exit_status, main, strict_object
from mission_control.interfaces.http.mission_control import (
    MissionPrincipal,
    get_mission_principal,
    router,
)


def test_unconfigured_authentication_fails_closed():
    app = FastAPI()
    app.include_router(router)
    response = TestClient(app).get("/v1/applications/a/runs/r/inspection")
    assert response.status_code == 503


def test_cross_application_denied_before_service_selection():
    class ForbiddenRegistry:
        def get(self, key):
            pytest.fail("unauthorized principal reached service registry")

    app = FastAPI()
    app.include_router(router)
    app.state.mission_control_services = ForbiddenRegistry()
    app.dependency_overrides[get_mission_principal] = lambda: MissionPrincipal(
        installation_id=uuid4(),
        application_id="a",
        tenant_id=uuid4(),
        actor=ActorContext(actor_id="operator"),
        issuer="https://issuer.invalid",
        audiences=frozenset({"authenticated"}),
    )
    response = TestClient(app).get("/v1/applications/b/runs/r/inspection")
    assert response.status_code == 403


@pytest.mark.parametrize(
    ("issuer", "enabled", "status"),
    [
        ("https://attacker.invalid", True, 403),
        ("https://issuer.invalid", False, 503),
    ],
)
def test_registry_rejects_wrong_issuer_and_disabled_application(issuer, enabled, status):
    class ForbiddenRegistry:
        def get(self, key):
            pytest.fail("unverified installation reached service registry")

    installation = uuid4()
    registry = ApplicationRegistry(
        (
            ApplicationBinding.seal(
                application_id="biotech",
                installation_id=installation,
                binding_version="1",
                supabase_project_ref="project",
                database_secret_ref="TEST_DATABASE_URL",
                accepted_issuers={"https://issuer.invalid"},
                accepted_audiences={"authenticated"},
                required_component_version="1",
            ),
        )
    )
    if enabled:
        registry.observe(
            "biotech",
            InstallationObservation(
                installation,
                "biotech",
                "project",
                frozenset({"1"}),
            ),
        )
    app = FastAPI()
    app.include_router(router)
    app.state.mission_control_registry = registry
    app.state.mission_control_services = ForbiddenRegistry()
    app.dependency_overrides[get_mission_principal] = lambda: MissionPrincipal(
        installation_id=installation,
        application_id="biotech",
        tenant_id=uuid4(),
        issuer=issuer,
        audiences={"authenticated"},
        actor=ActorContext(actor_id="operator"),
    )
    response = TestClient(app).get("/v1/applications/biotech/runs/r/inspection")
    assert response.status_code == status


def test_skill_manifest_covers_all_files_and_operation_catalog():
    import hashlib
    import json
    from pathlib import Path

    root = Path(__file__).resolve().parents[3] / "skills" / "mission-control"
    manifest = json.loads((root / "manifest.json").read_text())
    actual = {
        path.relative_to(root).as_posix(): "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()
        for path in root.rglob("*")
        if path.is_file() and path.name != "manifest.json"
    }
    assert manifest["files"] == actual
    assert manifest["operation_catalog_digest"] == actual["references/operations.json"]


def test_misbound_tenant_service_is_unavailable_before_read():
    installation, tenant = uuid4(), uuid4()
    registry = ApplicationRegistry(
        (
            ApplicationBinding.seal(
                application_id="biotech",
                installation_id=installation,
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
        "biotech",
        InstallationObservation(
            installation,
            "biotech",
            "project",
            frozenset({"1"}),
        ),
    )
    run_control, _ = run_control_service()
    service = MissionControlService(
        run_control,
        BoundaryInterventionService(run_control),
        request_scope="another-tenant",
    )
    app = FastAPI()
    app.include_router(router)
    app.state.mission_control_registry = registry
    app.state.mission_control_services = {(installation, "biotech", tenant): service}
    app.dependency_overrides[get_mission_principal] = lambda: MissionPrincipal(
        installation_id=installation,
        application_id="biotech",
        tenant_id=tenant,
        issuer="https://issuer.invalid",
        audiences={"authenticated"},
        actor=ActorContext(actor_id="operator", permissions={"workflow_run.read"}),
    )
    response = TestClient(app).get("/v1/applications/biotech/runs/r/inspection")
    assert response.status_code == 503
    assert response.json()["detail"]["code"] == "service_scope_mismatch"


def test_client_preserves_application_and_request_identity():
    captured = []

    def receive(request):
        captured.append(request)
        return httpx.Response(202, json={"admitted": True})

    with httpx.Client(
        base_url="https://example.invalid", transport=httpx.MockTransport(receive)
    ) as c:
        client = MissionClient(c, "biotech")
        client.command("run-1", {"request_id": "request-1"})
    assert captured[0].url.path == "/v1/applications/biotech/runs/run-1/commands"
    assert captured[0].headers["Idempotency-Key"] == "request-1"


@pytest.mark.parametrize("body", ['{"x":1,"x":2}', '{"x":NaN}', '{"x":1e999}', "[]"])
def test_strict_request_file_rejects_ambiguous_json(tmp_path, body):
    path = tmp_path / "request.json"
    path.write_text(body)
    with pytest.raises(ValueError):
        strict_object(str(path))


@pytest.mark.parametrize(
    ("status", "expected"), [(200, 0), (202, 0), (401, 3), (403, 3), (409, 4), (422, 2), (503, 5)]
)
def test_exit_codes(status, expected):
    assert exit_status(status) == expected


def test_cli_never_sends_bearer_to_remote_http(monkeypatch, capsys):
    monkeypatch.setenv("MISSION_CONTROL_TOKEN", "sensitive")
    assert (
        main(["run", "inspect", "r", "--application", "a", "--url", "http://example.invalid"]) == 2
    )
    assert "sensitive" not in capsys.readouterr().err


def test_cli_rejects_unbounded_wait(monkeypatch):
    monkeypatch.setenv("MISSION_CONTROL_TOKEN", "sensitive")
    assert main(["--application", "a", "run", "inspect", "r", "--wait", "nan"]) == 2


@pytest.mark.parametrize(
    "prefix",
    [
        ["--application", "biotech", "run", "inspect", "r"],
        ["run", "--application", "biotech", "inspect", "r"],
        ["run", "inspect", "r", "--application", "biotech"],
    ],
)
@pytest.mark.parametrize(("outcome", "code"), [("completed", 0), ("failed", 6), ("cancelled", 6)])
def test_cli_flags_and_terminal_wait(monkeypatch, capsys, prefix, outcome, code):
    monkeypatch.setenv("MISSION_CONTROL_TOKEN", "sensitive")
    original_client = httpx.Client

    def receive(request):
        assert request.url.path == "/v1/applications/biotech/runs/r/inspection"
        return httpx.Response(200, json={"lifecycle": "completed", "execution_outcome": outcome})

    monkeypatch.setattr(
        httpx,
        "Client",
        lambda **kwargs: original_client(
            **kwargs,
            transport=httpx.MockTransport(receive),
        ),
    )
    assert main([*prefix, "--wait", "1"]) == code
    assert "sensitive" not in capsys.readouterr().out


def test_redirect_is_not_reported_as_success():
    assert exit_status(302) == 5


@pytest.mark.parametrize("suffix", ["commands", "snapshots", "forks", "reconcile-unit", "launch"])
@pytest.mark.parametrize(
    "body", ['{"expected_version":1,"expected_version":2}', '{"x":NaN}', '{"x":1e999}']
)
def test_http_rejects_ambiguous_json(suffix, body):
    app = FastAPI()
    app.include_router(router)
    response = TestClient(app).post(
        f"/v1/applications/biotech/runs/r/{suffix}",
        content=body,
        headers={"Content-Type": "application/json"},
    )
    assert response.status_code == 400
    assert response.json()["detail"]["code"] == "invalid_json_object"


def test_skill_operation_catalog_matches_typed_public_openapi():
    import json
    from pathlib import Path

    app = FastAPI()
    app.include_router(router)
    from mission_control.interfaces.http.catalog import router as catalog_router

    app.include_router(catalog_router)
    schema = app.openapi()
    root = Path(__file__).resolve().parents[3] / "skills" / "mission-control"
    operations = json.loads((root / "references" / "operations.json").read_text())["operations"]
    for operation in operations:
        path = "/v1/applications/{application_id}" + operation["path"]
        method = schema["paths"][path][operation["method"].lower()]
        success = next(value for code, value in method["responses"].items() if code.startswith("2"))
        assert "$ref" in success["content"]["application/json"]["schema"]
