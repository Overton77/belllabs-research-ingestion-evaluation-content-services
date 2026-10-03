"""Core settings need no unrelated credentials; selected adapters fail before I/O."""

from typing import cast

import httpx
import pytest
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.store.memory import InMemoryStore
from openai import AsyncOpenAI
from pydantic import SecretStr

from mission_control.adapters.capabilities.capability_embeddings import (
    CapabilityEmbeddingDependencyError,
    OpenAICapabilityEmbeddingAdapter,
)
from mission_control.adapters.capabilities.capability_pins import CapabilityPins
from mission_control.adapters.supabase_storage.bundles import configured_supabase_bundle_reader
from mission_control.adapters.supabase_storage.client import create_supabase
from mission_control.adapters.temporal.deployment_composition import (
    build_deployment_capability_registry,
)
from mission_control.bootstrap.settings import IntegrationConfigurationError, Settings


@pytest.fixture
def core_settings(monkeypatch):
    for name in Settings.model_fields:
        monkeypatch.delenv(name.upper(), raising=False)
    monkeypatch.delenv("NEO$J_URI", raising=False)
    return Settings(_env_file=None)


def test_empty_environment_constructs_core_and_local_component_registry(core_settings):
    assert core_settings.supabase_url is None
    assert core_settings.supabase_secret_key is None
    assert core_settings.supabase_publishable_key is None
    assert core_settings.openai_api_key is None
    assert "neo4j_uri" not in Settings.model_fields
    registry = build_deployment_capability_registry(
        core_settings, CapabilityPins(), saver=InMemorySaver(), store=InMemoryStore()
    )
    assert not registry.registry.model_factories
    assert not registry.registry.skill_bundles


@pytest.mark.asyncio
@pytest.mark.parametrize("privileged", [False, True])
async def test_selected_supabase_requires_its_key_without_client_creation(
    core_settings, monkeypatch, privileged
):
    monkeypatch.setattr(
        "mission_control.adapters.supabase_storage.client.acreate_client",
        lambda *args: pytest.fail("client must not be created"),
    )
    key_name = "SUPABASE_SECRET_KEY" if privileged else "SUPABASE_PUBLISHABLE_KEY"
    with pytest.raises(IntegrationConfigurationError, match=key_name):
        await create_supabase(core_settings, privileged=privileged)
    # A secret/service key must never substitute for a missing publishable key.
    configured = core_settings.model_copy(
        update={
            "supabase_url": "https://offline.invalid",
            "supabase_secret_key": SecretStr("fixture"),
        }
    )
    if not privileged:
        with pytest.raises(IntegrationConfigurationError, match="SUPABASE_PUBLISHABLE_KEY"):
            await create_supabase(configured)


def test_selected_bundle_backend_requires_supabase_credentials(core_settings):
    with httpx.Client(transport=httpx.MockTransport(lambda req: pytest.fail("no HTTP"))) as http:
        with pytest.raises(IntegrationConfigurationError, match="SUPABASE_SECRET_KEY"):
            configured_supabase_bundle_reader(core_settings, http_client=http)


@pytest.mark.asyncio
async def test_openai_embedding_requires_key_only_when_constructing_provider_client(core_settings):
    with pytest.raises(CapabilityEmbeddingDependencyError, match="credential reference"):
        OpenAICapabilityEmbeddingAdapter(core_settings)
    adapter = OpenAICapabilityEmbeddingAdapter(core_settings, client=cast(AsyncOpenAI, object()))
    assert await adapter.embed_many(()) == ()


def test_selected_signing_and_auth_require_explicit_configuration(core_settings):
    with pytest.raises(IntegrationConfigurationError, match="RUNTIME_CHECKPOINT_SIGNING_KEY"):
        _ = core_settings.checkpoint_signing_key
    with pytest.raises(IntegrationConfigurationError, match="COORDINATOR_MCP_JWT_ISSUER"):
        _ = core_settings.coordinator_jwt_issuer
    configured = core_settings.model_copy(
        update={
            "runtime_checkpoint_signing_key": SecretStr("fixture-signing-key-32-bytes-long"),
            "coordinator_mcp_jwt_issuer": "https://issuer.invalid",
        }
    )
    assert configured.checkpoint_signing_key == b"fixture-signing-key-32-bytes-long"
    assert configured.coordinator_jwt_issuer == "https://issuer.invalid"
    assert len(configured.inspection_cursor_secret) == 32
