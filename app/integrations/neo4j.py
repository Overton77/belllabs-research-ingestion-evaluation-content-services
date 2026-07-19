from neo4j import AsyncDriver, AsyncGraphDatabase

from app.config import Settings


async def create_neo4j(settings: Settings) -> AsyncDriver:
    """PRE-EMPTIVE SETUP: create and verify the official Neo4j async driver."""
    driver = AsyncGraphDatabase.driver(
        settings.neo4j_uri,
        auth=(settings.neo4j_aura_username, settings.neo4j_aura_password.get_secret_value()),
    )
    await driver.verify_connectivity()
    return driver
