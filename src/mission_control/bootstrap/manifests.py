"""Composition of the Mission Manifest service (FT-E2 compile, FT-E3 submit and start)."""

from __future__ import annotations

import asyncpg

from mission_control.adapters.postgres.control_plane.definition_repository import (
    PostgresDefinitionRepository,
)
from mission_control.application.authoring.manifest_service import (
    ManifestCompileService,
    ManifestProgramCompiler,
    MissionManifestService,
)
from mission_control.application.capabilities.catalog import CatalogService
from mission_control.application.ports.payloads import ContentAddressedPayloadStore
from mission_control.domain.authoring.extensions import ExtensionRegistry


def compose_manifest_service(
    pool: asyncpg.Pool,
    *,
    request_scope: str,
    catalog: CatalogService,
    extensions: ExtensionRegistry,
    payload_store: ContentAddressedPayloadStore,
) -> MissionManifestService:
    """Compile resolves through the tenant's catalog search and lowers onto the compiler of
    the installation catalog (dry-run overlay: nothing is persisted by compile)."""

    definitions = PostgresDefinitionRepository(pool, catalog_scope=catalog.catalog_scope)
    compiler = ManifestCompileService(
        definitions=catalog.definitions,
        search=catalog.search,
        programs=ManifestProgramCompiler(definitions, extensions, payload_store),
    )
    return MissionManifestService(compiler=compiler, request_scope=request_scope)
