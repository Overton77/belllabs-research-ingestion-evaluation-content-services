from __future__ import annotations

from datetime import UTC, datetime

from biotech_mission_adapters.application.schema.schema_grounding_repository import (
    InMemorySchemaGroundingRecordRepository,
    schema_grounding_record,
)
from biotech_mission_adapters.interfaces.http.schema_grounding import (
    get_schema_grounding_repository,
    router,
)
from fastapi import FastAPI
from fastapi.testclient import TestClient

from mission_control.interfaces.http.control_plane import (
    ControlPlanePrincipal,
    get_control_plane_principal,
)
from tests.schema_context_helpers import accepted, catalog

NOW = datetime(2026, 7, 24, 16, 0, tzinfo=UTC)


def test_unmounted_domain_repository_reports_unavailable_without_implicit_database() -> None:
    application = FastAPI()
    application.include_router(router)
    application.dependency_overrides[get_control_plane_principal] = lambda: ControlPlanePrincipal(
        actor_id="auditor-1",
        roles=frozenset({"auditor"}),
        tenant_scopes=frozenset({"tenant-1"}),
    )
    with TestClient(application) as client:
        response = client.get(
            "/schema-grounding/v1/catalog-builds/unknown",
            params={"request_scope": "tenant-1"},
        )
    assert response.status_code == 503
    assert "not mounted" in response.json()["detail"]


def test_schema_grounding_query_surface_is_typed_authenticated_and_tenant_scoped() -> None:
    application = FastAPI()
    application.include_router(router)
    records = InMemorySchemaGroundingRecordRepository()
    value = accepted(catalog())
    application.dependency_overrides[get_schema_grounding_repository] = lambda: records
    application.dependency_overrides[get_control_plane_principal] = lambda: ControlPlanePrincipal(
        actor_id="auditor-1",
        roles=frozenset({"auditor"}),
        tenant_scopes=frozenset({"tenant-1"}),
    )

    async def seed() -> None:
        await records.append(
            schema_grounding_record(
                record_type="accepted_selection",
                record_id=value.selection.selection_id,
                request_scope="tenant-1",
                run_id="run-1",
                payload=value.model_dump(mode="json"),
                created_at=NOW,
            )
        )

    import asyncio

    asyncio.run(seed())
    with TestClient(application) as client:
        schemas = client.get("/schema-grounding/v1/schemas")
        selected = client.get(
            f"/schema-grounding/v1/selections/{value.selection.selection_id}",
            params={"request_scope": "tenant-1"},
        )
        cross_tenant = client.get(
            f"/schema-grounding/v1/selections/{value.selection.selection_id}",
            params={"request_scope": "tenant-2"},
        )

    assert schemas.status_code == 200
    assert "catalog_build_request" in schemas.json()
    assert "supporting_graph_reconciliation" in schemas.json()
    assert selected.status_code == 200
    assert selected.json()["selection"]["selection_id"] == value.selection.selection_id
    assert cross_tenant.status_code == 404
