from neo4j import AsyncDriver, AsyncGraphDatabase

from biotech_mission_adapters.bootstrap.settings import BiotechSettings
from mission_control.bootstrap.settings import IntegrationConfigurationError


async def create_neo4j(settings: BiotechSettings) -> AsyncDriver:
    """PRE-EMPTIVE SETUP: create and verify the official Neo4j async driver."""
    if (
        not settings.neo4j_uri
        or not settings.neo4j_uri.strip()
        or not settings.neo4j_aura_username
        or not settings.neo4j_aura_username.strip()
        or settings.neo4j_aura_password is None
        or not settings.neo4j_aura_password.get_secret_value().strip()
    ):
        raise IntegrationConfigurationError(
            "NEO4J_URI, NEO4J_AURA_USERNAME and NEO4J_AURA_PASSWORD are required for Neo4j"
        )
    driver = AsyncGraphDatabase.driver(
        settings.neo4j_uri,
        auth=(settings.neo4j_aura_username, settings.neo4j_aura_password.get_secret_value()),
    )
    try:
        await driver.verify_connectivity()
    except BaseException:
        await driver.close()
        raise
    return driver
