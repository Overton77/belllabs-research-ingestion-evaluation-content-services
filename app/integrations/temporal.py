from temporalio.client import Client

from app.config import Settings


async def create_temporal_client(
    settings: Settings, *, plugins: list[object] | None = None
) -> Client:
    """Connect to the local Temporal frontend with optional Agents SDK plugins."""
    return await Client.connect(
        settings.temporal_address,
        namespace=settings.temporal_namespace,
        plugins=plugins or [],
    )
