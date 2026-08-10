from __future__ import annotations

from datetime import UTC
from typing import Any

from beanie import init_beanie
from pymongo import AsyncMongoClient
from pymongo.asynchronous.database import AsyncDatabase

from app.config import Settings
from app.models import (
    AsyncSubagentContractDocument,
    AsyncSubagentExecutionDocument,
    ArtifactMetadataRevisionDocument,
    CatalogProjectionAlertDocument,
    CatalogProjectionEventDocument,
    DefinitionAliasDocument,
    DefinitionAliasMovementDocument,
    DefinitionHeadDocument,
    DefinitionRetirementDocument,
    EffectiveRunConfigurationDocument,
    ExternalCandidateInspectionReportDocument,
    ExternalCandidateInspectionWorkspaceDocument,
    ExternalDiscoveryCandidateDocument,
    ExternalDiscoveryEvidenceDocument,
    InfrastructureMarker,
    OperationExecutionBindingAuthorityV2Document,
    OperationExecutionBindingDocument,
    OperationExecutionClaimDocument,
    OperationMigrationQuarantineDocument,
    OperationSettlementDocument,
    ParentAsyncSubagentLinkDocument,
    PublishedDefinitionDocument,
    SandboxSnapshotClaimDocument,
    SandboxSnapshotCloneDocument,
    SandboxSnapshotDocument,
    SchemaGroundingRecordDocument,
    WebResearchRecordDocument,
    WorkspaceMaterializationManifestDocument,
    WorkspaceSlotReservationDocument,
)

BEANIE_MODELS = [
    AsyncSubagentContractDocument,
    AsyncSubagentExecutionDocument,
    ArtifactMetadataRevisionDocument,
    CatalogProjectionAlertDocument,
    CatalogProjectionEventDocument,
    InfrastructureMarker,
    DefinitionHeadDocument,
    DefinitionAliasDocument,
    DefinitionAliasMovementDocument,
    PublishedDefinitionDocument,
    DefinitionRetirementDocument,
    EffectiveRunConfigurationDocument,
    ExternalDiscoveryEvidenceDocument,
    ExternalDiscoveryCandidateDocument,
    ExternalCandidateInspectionWorkspaceDocument,
    ExternalCandidateInspectionReportDocument,
    OperationExecutionBindingAuthorityV2Document,
    OperationExecutionBindingDocument,
    OperationExecutionClaimDocument,
    OperationMigrationQuarantineDocument,
    OperationSettlementDocument,
    ParentAsyncSubagentLinkDocument,
    SandboxSnapshotClaimDocument,
    SandboxSnapshotDocument,
    SandboxSnapshotCloneDocument,
    SchemaGroundingRecordDocument,
    WorkspaceSlotReservationDocument,
    WorkspaceMaterializationManifestDocument,
    WebResearchRecordDocument,
]


async def create_mongodb(settings: Settings) -> tuple[AsyncMongoClient, AsyncDatabase]:
    """Create Beanie on PyMongo AsyncMongoClient. Motor is intentionally not used."""
    client: AsyncMongoClient[dict[str, Any]] = AsyncMongoClient(
        settings.mongodb_uri.get_secret_value(),
        serverSelectionTimeoutMS=5_000,
        appname="biotech-research-ingestion-evaluation-system",
        tz_aware=True,
        tzinfo=UTC,
    )
    database = client[settings.mongodb_database]
    await database.command("ping")
    await init_beanie(database=database, document_models=BEANIE_MODELS)
    return client, database
