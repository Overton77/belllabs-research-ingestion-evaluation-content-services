from __future__ import annotations

import os

import pytest

from app.application.control_plane import ControlPlaneService
from app.application.control_plane_repository import InMemoryDefinitionRepository
from app.domain.control_plane.extensions import ExtensionRegistry
from app.integrations.control_plane_payloads import InMemoryPayloadStore


@pytest.fixture
def in_memory_control_plane_service() -> ControlPlaneService:
    return ControlPlaneService(
        InMemoryDefinitionRepository(),
        ExtensionRegistry(),
        InMemoryPayloadStore(),
    )


@pytest.fixture
def test_mongodb_uri() -> str:
    uri = os.getenv("TEST_MONGODB_URI")
    if not uri:
        pytest.skip("TEST_MONGODB_URI is not configured")
    return uri


@pytest.fixture
def test_application_postgres_dsn() -> str:
    dsn = os.getenv("TEST_APPLICATION_POSTGRES_DSN")
    if not dsn:
        pytest.skip("TEST_APPLICATION_POSTGRES_DSN is not configured")
    return dsn
