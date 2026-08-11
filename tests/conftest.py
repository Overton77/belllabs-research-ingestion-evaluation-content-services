from __future__ import annotations

import asyncio
import os
import sys

import pytest

from app.application.control_plane.service import ControlPlaneService
from app.application.control_plane.control_plane_repository import InMemoryDefinitionRepository
from app.domain.control_plane.extensions import ExtensionRegistry
from app.integrations.control_plane_payloads import InMemoryPayloadStore


def pytest_asyncio_loop_factories(config, item):  # type: ignore[no-untyped-def]
    """Keep Psycopg's Windows selector requirement local to its experiment module."""

    del config
    if (
        sys.platform == "win32"
        and item.path.name == "test_langgraph_temporal_stagegraph.py"
        and item.path.parent.name == "experiments"
    ):
        return {"windows-selector": asyncio.SelectorEventLoop}
    return {"default": asyncio.new_event_loop}


def _required_external_test_service(name: str) -> str:
    value = os.getenv(name)
    if value:
        return value
    if os.getenv("BELL_LABS_REQUIRE_STAGE3_ENTRY_SERVICES") == "1":
        pytest.fail(f"{name} is required for the Stage 3 entry evidence job")
    pytest.skip(f"{name} is not configured")


@pytest.fixture
def in_memory_control_plane_service() -> ControlPlaneService:
    return ControlPlaneService(
        InMemoryDefinitionRepository(),
        ExtensionRegistry(),
        InMemoryPayloadStore(),
    )


@pytest.fixture
def test_mongodb_uri() -> str:
    return _required_external_test_service("TEST_MONGODB_URI")


@pytest.fixture
def test_application_postgres_dsn() -> str:
    return _required_external_test_service("TEST_APPLICATION_POSTGRES_DSN")


# Block C live fixtures / helpers for persistent Agent Server qualification.
pytest_plugins = ["tests.fixtures.agent_server_block_c"]
