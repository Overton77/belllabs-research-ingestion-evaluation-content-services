from __future__ import annotations

from datetime import UTC
from typing import Any

from beanie import init_beanie
from pymongo import AsyncMongoClient
from pymongo.asynchronous.database import AsyncDatabase

from app.config import Settings
from app.models import (
    DefinitionAliasDocument,
    DefinitionAliasMovementDocument,
    DefinitionHeadDocument,
    DefinitionRetirementDocument,
    EffectiveRunConfigurationDocument,
    InfrastructureMarker,
    PublishedDefinitionDocument,
)

BEANIE_MODELS = [
    InfrastructureMarker,
    DefinitionHeadDocument,
    DefinitionAliasDocument,
    DefinitionAliasMovementDocument,
    PublishedDefinitionDocument,
    DefinitionRetirementDocument,
    EffectiveRunConfigurationDocument,
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
