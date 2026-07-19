from datetime import UTC, datetime

from beanie import Document
from pydantic import Field


class InfrastructureMarker(Document):
    """PRE-EMPTIVE SETUP: reserves Beanie wiring; not an ingestion-domain model."""

    component: str
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))

    class Settings:
        name = "_infrastructure"
