from uuid import uuid4

import httpx
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from mission_control.domain.policies.contracts import ActorContext
from mission_control.interfaces.cli.main import MissionClient
from mission_control.interfaces.http.catalog import router
from mission_control.interfaces.http.mission_control import MissionPrincipal, get_mission_principal


def test_cross_application_catalog_denied_before_service_lookup():
    class Forbidden:
        def get(self, key):
            pytest.fail("cross-application request reached services")

    app = FastAPI()
    app.include_router(router)
    app.state.mission_control_catalog_services = Forbidden()
    app.dependency_overrides[get_mission_principal] = lambda: MissionPrincipal(
        installation_id=uuid4(),
        application_id="one",
        tenant_id=uuid4(),
        issuer="https://issuer.invalid",
        audiences={"authenticated"},
        actor=ActorContext(actor_id="reader", permissions={"catalog:read"}),
    )
    assert TestClient(app).get("/v1/applications/two/catalog/definitions").status_code == 403


def test_catalog_cli_routes_preserve_typed_request_body():
    requests = []

    def handle(request):
        requests.append(request)
        return httpx.Response(200, json={"ok": True})

    with httpx.Client(
        base_url="https://service.invalid", transport=httpx.MockTransport(handle)
    ) as http:
        client = MissionClient(http, "biotech")
        assert client.catalog("list").status_code == 200
        assert (
            client.catalog("components", {"kinds": ["plugin"], "text": "reviewed"}).status_code
            == 200
        )
        assert (
            client.catalog("discover", {"source": "skills", "query": "analysis"}).status_code == 200
        )
    assert [r.url.path for r in requests] == [
        "/v1/applications/biotech/catalog/definitions",
        "/v1/applications/biotech/catalog/components/search",
        "/v1/applications/biotech/catalog/discover",
    ]
    assert requests[1].read() == b'{"kinds":["plugin"],"text":"reviewed"}'


def test_plugin_search_preserves_trust_and_host_filters_and_does_not_install():
    from mission_control.application.agentic_components.repository import (
        InMemoryAgenticComponentRepository,
    )
    from mission_control.application.capabilities.catalog import CatalogService
    from mission_control.application.installations.registry import (
        ApplicationBinding,
        ApplicationRegistry,
        InstallationObservation,
    )
    from mission_control.domain.agentic_components.contracts import AgenticComponentRelease
    from mission_control.interfaces.http.mission_control import principal_request_scope
    from tests.unit.agentic_components.test_harness import mcp_release

    principal = MissionPrincipal(
        installation_id=uuid4(),
        application_id="biotech",
        tenant_id=uuid4(),
        issuer="https://issuer.invalid",
        audiences={"authenticated"},
        actor=ActorContext(actor_id="reader", permissions={"catalog:read"}),
    )
    registry = ApplicationRegistry(
        (
            ApplicationBinding.seal(
                application_id="biotech",
                installation_id=principal.installation_id,
                binding_version="1",
                supabase_project_ref="test",
                database_secret_ref="TEST_DB",
                accepted_issuers={principal.issuer},
                accepted_audiences=principal.audiences,
                required_component_version="1",
            ),
        )
    )
    registry.observe(
        "biotech",
        InstallationObservation(principal.installation_id, "biotech", "test", frozenset({"1"})),
    )
    values = mcp_release().model_dump(mode="python")
    values.update(kind="plugin", mcp=None, definition_ref=None)
    plugin = AgenticComponentRelease.model_validate(values)

    class Definitions:
        async def list_published_definition_refs(self):
            return ()

        async def get(self, ref):
            raise AssertionError("plugin search cannot execute authoring")

    service = CatalogService(
        principal_request_scope(principal),
        "catalog-scope",
        Definitions(),
        components=InMemoryAgenticComponentRepository((plugin,)),
    )
    app = FastAPI()
    app.include_router(router)
    app.state.mission_control_registry = registry
    app.state.mission_control_catalog_services = {
        (principal.installation_id, "biotech", principal.tenant_id): service
    }
    app.dependency_overrides[get_mission_principal] = lambda: principal
    client = TestClient(app)
    path = "/v1/applications/biotech/catalog/components/search"
    result = client.post(
        path, json={"kinds": ["plugin"], "host": "codex", "minimum_trust_stage": "qualified"}
    )
    assert result.status_code == 200, result.text
    assert result.json()["components"][0]["kind"] == "plugin"
    assert (
        client.post(path, json={"kinds": ["plugin"], "minimum_trust_stage": "accepted"}).json()[
            "components"
        ]
        == []
    )
    assert (
        client.post(
            path,
            content='{"kinds":[],"kinds":["plugin"]}',
            headers={"Content-Type": "application/json"},
        ).status_code
        == 400
    )
