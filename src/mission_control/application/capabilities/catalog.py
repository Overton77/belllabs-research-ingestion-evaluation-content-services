"""Operational capability catalog; domain entity/schema catalogs are separate ports."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from mission_control.application.agentic_components.repository import AgenticComponentRepository
from mission_control.application.capabilities.capability_search import CapabilitySearchService
from mission_control.application.capabilities.external_candidate_inspection import (
    ExternalCandidateInspectionService,
)
from mission_control.application.capabilities.external_capability_discovery import (
    ExternalCapabilityDiscoveryService,
)
from mission_control.domain.authoring.contracts import ExactDefinitionRef, PublishedDefinition


class CatalogDefinitions(Protocol):
    async def list_published_definition_refs(self) -> tuple[ExactDefinitionRef, ...]: ...
    async def list_published_definitions(self) -> tuple[PublishedDefinition, ...]: ...
    async def get(self, ref: ExactDefinitionRef) -> PublishedDefinition: ...


@dataclass(frozen=True)
class CatalogService:
    request_scope: str
    catalog_scope: str
    definitions: CatalogDefinitions
    search: CapabilitySearchService | None = None
    discovery: ExternalCapabilityDiscoveryService | None = None
    inspection: ExternalCandidateInspectionService | None = None
    components: AgenticComponentRepository | None = None
