"""A catalog HTTP app over an in-memory seeded catalog (FT-A3 / FT-A8 parity tests)."""

from __future__ import annotations

from dataclasses import dataclass
from uuid import UUID

from fastapi import FastAPI
from fastapi.testclient import TestClient

from mission_control.application.authoring.control_plane_repository import (
    InMemoryDefinitionRepository,
)
from mission_control.application.capabilities.capability_search import CapabilitySearchService
from mission_control.application.capabilities.catalog import CatalogService
from mission_control.application.installations.registry import (
    ApplicationBinding,
    ApplicationRegistry,
    InstallationObservation,
)
from mission_control.domain.policies.contracts import ActorContext
from mission_control.interfaces.http.catalog import router
from mission_control.interfaces.http.mission_control import (
    MissionPrincipal,
    get_mission_principal,
    principal_request_scope,
)

INSTALLATION = UUID("00000000-0000-4000-8000-000000000001")
TENANT = UUID("00000000-0000-4000-8000-000000000002")


@dataclass
class CatalogHarness:
    app: FastAPI
    client: TestClient
    principal: MissionPrincipal
    service: CatalogService

    @property
    def tenant_scope(self) -> str:
        return self.service.request_scope

    def url(self, path: str) -> str:
        return f"/v1/applications/{self.principal.application_id}/catalog/{path}"


def catalog_harness(
    definitions: InMemoryDefinitionRepository,
    search: CapabilitySearchService | None,
    permissions: frozenset[str] = frozenset({"catalog:read"}),
) -> CatalogHarness:
    principal = MissionPrincipal(
        installation_id=INSTALLATION,
        application_id="biotech",
        tenant_id=TENANT,
        issuer="https://issuer.invalid",
        audiences={"authenticated"},
        actor=ActorContext(actor_id="reader", permissions=set(permissions)),
    )
    registry = ApplicationRegistry(
        (
            ApplicationBinding.seal(
                application_id="biotech",
                installation_id=INSTALLATION,
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
        "biotech", InstallationObservation(INSTALLATION, "biotech", "test", frozenset({"1"}))
    )
    request_scope = principal_request_scope(principal)
    service = CatalogService(
        request_scope,
        request_scope,
        definitions,  # type: ignore[arg-type]
        search=search,
    )
    app = FastAPI()
    app.include_router(router)
    app.state.mission_control_registry = registry
    app.state.mission_control_catalog_services = {(INSTALLATION, "biotech", TENANT): service}
    app.dependency_overrides[get_mission_principal] = lambda: principal
    return CatalogHarness(app=app, client=TestClient(app), principal=principal, service=service)
