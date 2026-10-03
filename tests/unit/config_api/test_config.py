import pytest
from pydantic import SecretStr, ValidationError

from app.config import Settings
from tests.fixtures.isolated_settings import isolated_settings


def test_preemptive_settings_contract() -> None:
    settings = isolated_settings(database_url=SecretStr("postgresql://settings.invalid:5432/db"))
    assert settings.mongodb_database == "belllabsbiotech"
    assert settings.openai_model == "gpt-5.4-nano"
    assert settings.firecrawl_api_key is None or settings.firecrawl_api_key.get_secret_value()
    assert settings.temporal_task_queue
    assert settings.postgres_dsn
    assert settings.coordinator_mcp_enabled is False
    assert settings.capability_search_enabled is False
    assert settings.external_capability_discovery_enabled is False
    assert settings.coordinator_launch_enabled is False
    assert settings.capability_embedding_model == "text-embedding-3-small"
    assert settings.capability_embedding_dimensions == 1536
    assert settings.coordinator_launch_ticket_ttl_seconds == 900


def test_coordinator_settings_reject_floating_skill_package_and_wrong_embedding() -> None:
    settings = isolated_settings()
    payload = settings.model_dump()
    # `neo4j_uri` is declared with a validation alias; without this the payloads below would
    # fail for a missing field rather than for the value under test.
    payload["NEO4J_URI"] = payload.pop("neo4j_uri")
    assert Settings.model_validate(payload) is not None
    with pytest.raises(ValidationError):
        Settings.model_validate(
            {
                **payload,
                "npx_skills_package_version": "latest",
            }
        )
    with pytest.raises(ValidationError):
        Settings.model_validate(
            {
                **payload,
                "capability_embedding_dimensions": 3072,
            }
        )
