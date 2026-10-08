from __future__ import annotations

import asyncio
import os
import sys

import pytest

from mission_control.adapters.storage.control_plane_payloads import InMemoryPayloadStore
from mission_control.application.authoring.control_plane_repository import (
    InMemoryDefinitionRepository,
)
from mission_control.application.authoring.service import ControlPlaneService
from mission_control.bootstrap.settings import PROJECT_ROOT
from mission_control.domain.authoring.extensions import ExtensionRegistry

# Deterministic tests must not inherit telemetry opt-in from a developer .env.
# Explicit process environment remains available to separately authorized live jobs.
os.environ.setdefault("LANGSMITH_TRACING", "false")
os.environ.setdefault("LANGCHAIN_TRACING_V2", "false")

# Fresh checkouts and worktrees have no `.env`, but the server builds `Settings()` at
# import. Supply non-routable placeholders for the required fields only in that case;
# explicit environment values always win, and service/provider tests keep their own opt-ins.
_OFFLINE_SETTINGS_PLACEHOLDERS = {
    "SUPABASE_URL": "https://offline-placeholder.invalid",
    "SUPABASE_PUBLISHABLE_KEY": "offline-placeholder",
    "SUPABASE_SECRET_KEY": "offline-placeholder",
    "OPENAI_API_KEY": "offline-placeholder",
    "NEO4J_URI": "neo4j://offline-placeholder.invalid:7687",
    "NEO4J_AURA_USERNAME": "offline-placeholder",
    "NEO4J_AURA_PASSWORD": "offline-placeholder",
    "DATABASE_URL": "postgresql://offline-placeholder.invalid:5432/offline",
    "APPLICATION_DATABASE_URL": "postgresql://offline-placeholder.invalid:5432/offline",
}
if not (PROJECT_ROOT / ".env").exists():
    for _name, _value in _OFFLINE_SETTINGS_PLACEHOLDERS.items():
        os.environ.setdefault(_name, _value)


_PSYCOPG_SELECTOR_MODULES = {
    ("experiments", "test_langgraph_temporal_stagegraph.py"),
    ("deep_agents", "test_checkpoint_lineage_postgres_saver.py"),
    ("deep_agents", "test_provider_frames_deep_agents.py"),
    ("temporal", "test_rrm_004_worker_restart_recovery.py"),
    ("agent_server", "test_rrm_013_async_subagent_live.py"),
    ("control_plane", "test_rrm_005_inspection.py"),
    ("control_plane", "test_rrm_006_semantic_forks.py"),
    ("control_plane", "test_rrm_016_goal_directed_demo.py"),
    ("control_plane", "test_rrm_009_production_composition.py"),
    ("control_plane", "test_rrm_009_live_capabilities.py"),
    ("control_plane", "test_rrm_009_production_cancellation.py"),
    ("control_plane", "test_rrm_009_object_store.py"),
    ("control_plane", "test_rrm_020_shared_goal_workspace.py"),
    ("control_plane", "test_rrm_008_cancellation_demo.py"),
    ("control_plane", "test_rrm_010_combined_smoke.py"),
    ("mission_control", "test_postgres_runtime_parity.py"),
    ("mission_control", "test_postgres_children_and_artifacts.py"),
    ("mission_control", "test_authenticated_scoped_runtime.py"),
    ("mission_control", "test_manifest_lifecycle.py"),
    ("mission_control", "test_chain_two_goal_loops.py"),
    ("postgres", "test_mission_worker_startup.py"),
}


def pytest_asyncio_loop_factories(config, item):  # type: ignore[no-untyped-def]
    """Keep Psycopg's Windows selector requirement local to the modules that need it."""

    del config
    if (
        sys.platform == "win32"
        and (item.path.parent.name, item.path.name) in _PSYCOPG_SELECTOR_MODULES
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
def test_application_postgres_dsn() -> str:
    return _required_external_test_service("TEST_APPLICATION_POSTGRES_DSN")


# Block C live fixtures / helpers for persistent Agent Server qualification.
pytest_plugins = ["tests.fixtures.agent_server_block_c"]
