"""Explicit domain registration; importing this package never modifies the core runtime."""

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from biotech_mission_adapters.application.execution.schema_grounding_admission import (
    register_schema_grounding_admission_policies,
)
from biotech_mission_adapters.application.execution.web_research_admission import (
    register_web_research_admission_policies,
)
from biotech_mission_adapters.application.schema.schema_grounding_repository import (
    SchemaGroundingRecordRepository,
)
from biotech_mission_adapters.domain.schema_grounding.definitions import (
    register_schema_grounding_extensions,
)
from biotech_mission_adapters.domain.schema_grounding.errors import SchemaGroundingError
from biotech_mission_adapters.interfaces.http.schema_grounding import router
from mission_control.application.execution.service import AdmissionPolicyRegistry
from mission_control.domain.authoring.extensions import ExtensionRegistry


def register_biotech_contracts(
    extensions: ExtensionRegistry, policies: AdmissionPolicyRegistry
) -> None:
    register_schema_grounding_extensions(extensions)
    register_schema_grounding_admission_policies(policies)
    register_web_research_admission_policies(policies)


def mount_biotech_read_api(app: FastAPI, records: SchemaGroundingRecordRepository) -> None:
    """Mount on the owning domain service with its authenticated principal dependency."""
    app.state.schema_grounding_repository = records
    app.include_router(router)

    @app.exception_handler(SchemaGroundingError)
    async def domain_error(_request: Request, error: SchemaGroundingError) -> JSONResponse:
        return JSONResponse(
            status_code=error.status_code, content={"code": error.code, "message": error.message}
        )
