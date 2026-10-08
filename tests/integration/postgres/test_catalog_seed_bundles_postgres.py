"""Catalog seed bundle CONTENT applied with the DB lane's seed record engine.

The engine's whole-bundle ``apply_bundle`` additionally verifies a qualified release
fingerprint, which disposable fixture installs do not carry; this test therefore drives
its per-record writer inside one owner transaction per bundle and proves that the
seeded rows are consumable by the restricted runtime repositories.
"""

from __future__ import annotations

import copy
import json
from datetime import UTC, datetime
from pathlib import Path

import asyncpg
import pytest

from mission_control.adapters.postgres.control_plane.definition_repository import (
    PostgresDefinitionRepository,
)
from mission_control.adapters.postgres.scope import apply_catalog_scope_string
from mission_control.bootstrap.coordinator_composition import load_coordinator_catalog_bindings
from mission_control.domain.authoring.errors import DefinitionConflict
from mission_control_db_contract.errors import ContractError
from mission_control_db_contract.seeds import _apply_record, load_bundles, order_bundles
from tests.fixtures.mission_control_common_db import CommonDatabase
from tests.integration.postgres.catalog_common import BIOTECH_CATALOG
from tests.integration.postgres.catalog_common import catalog_db as catalog_db
from tests.integration.postgres.catalog_common import runtime_pool as runtime_pool

pytestmark = pytest.mark.common_db

SEEDS = Path(__file__).resolve().parents[3] / "packages" / "mission-control-db-contract" / "seeds"


async def _apply(database: CommonDatabase, bundle: dict) -> dict[str, int]:
    target = {
        "installation_id": str(database.installation_id),
        "application_id": database.application_id,
    }
    stats = {"created": 0, "reused": 0, "revoked": 0}
    connection = await asyncpg.connect(database.owner_dsn)
    try:
        async with connection.transaction():
            for record in bundle["records"]:
                await _apply_record(connection, target, bundle, record, stats)
    finally:
        await connection.close()
    return stats


@pytest.mark.asyncio
async def test_seeded_catalog_is_readable_replayable_and_conflicts_on_change(
    catalog_db: CommonDatabase, runtime_pool: asyncpg.Pool
) -> None:
    bundles = order_bundles(
        load_bundles([SEEDS / "common", SEEDS / "biotech", SEEDS / "qualification"]), set()
    )
    first = [await _apply(catalog_db, bundle) for bundle in bundles]
    assert all(stats["reused"] == 0 and stats["created"] > 0 for stats in first)
    replay = [await _apply(catalog_db, bundle) for bundle in bundles]
    assert all(stats["created"] == 0 and stats["reused"] > 0 for stats in replay)

    repository = PostgresDefinitionRepository(runtime_pool, catalog_scope=BIOTECH_CATALOG)
    refs = await repository.list_published_definition_refs()
    logical_ids = {ref.logical_id for ref in refs}
    assert {
        "skill.mission-control-coordinator",
        "prompt.coordinator.propose-workflow",
        "fixture.generic-stage-graph",
        "fixture.generic-goal-directed",
        # FT-A6 agent capabilities: common plus the biotech-only servers.
        "mcp.tavily",
        "mcp.firecrawl",
        "mcp.agent-browser",
        "mcp.pubmed",
        "mcp.biomcp",
    } <= logical_ids
    assert "mcp.edgartools" not in logical_ids  # admitted in ai-engineer only
    for ref in refs:
        assert (await repository.get(ref)).retired_at is None
    selector, prompts = await load_coordinator_catalog_bindings(repository=repository)
    assert selector.exact is not None
    assert selector.exact.logical_id == "skill.mission-control-coordinator"
    assert set(prompts) == {"propose_workflow"}
    # A later publication continues from the seeded revision; it never rewrites it.
    seeded = next(ref for ref in refs if ref.logical_id == "skill.mission-control-coordinator")
    definition = (await repository.get(seeded)).definition
    now = datetime(2026, 10, 4, tzinfo=UTC)
    with pytest.raises(DefinitionConflict):
        await repository.publish(definition, "publisher", now, 0)
    assert (await repository.publish(definition, "publisher", now, 1)).ref.revision == 2

    # Changed bytes under an existing logical key never last-write-win.
    changed = copy.deepcopy(bundles[0])
    changed["records"][0]["fields"]["manifest"]["family"] = "Invented"
    with pytest.raises(ContractError, match="differs from its admitted row"):
        await _apply(catalog_db, changed)
    async with runtime_pool.acquire() as connection, connection.transaction():
        await apply_catalog_scope_string(connection, BIOTECH_CATALOG)
        family = await connection.fetchval(
            "SELECT manifest FROM mission_control.asset_version "
            "WHERE asset_id='workflow-family:stagegraph'"
        )
        assert json.loads(family)["family"] == "StageGraph"
        rows = await connection.fetch(
            "SELECT kind, host_support, secret_refs FROM mission_control.asset_version "
            "WHERE asset_id = 'definition:mcp_server:mcp.pubmed'"
        )
        assert [row["kind"] for row in rows] == ["mcp_server"]
        support = json.loads(rows[0]["host_support"])
        assert support["profiles"]["cursor_cloud"]["status"] == "unqualified"
        assert set(rows[0]["secret_refs"]) == {
            "NCBI_API_KEY",
            "NCBI_ADMIN_EMAIL",
            "UNPAYWALL_EMAIL",
        }
        tools = await connection.fetchval(
            "SELECT count(*) FROM mission_control.asset_version WHERE kind = 'mcp_tool' "
            "AND asset_id LIKE 'definition:mcp_tool:mcp.pubmed.tool.%'"
        )
        assert tools == 11
        # Only the opt-in qualification tenant exists; no actor bindings or grants.
        assert await connection.fetchval("SELECT count(*) FROM mission_control.actor_binding") == 0
        assert (
            await connection.fetchval("SELECT count(*) FROM mission_control.capability_grant") == 0
        )


@pytest.mark.asyncio
async def test_ai_engineer_seeds_admit_edgartools_only_there() -> None:
    from tests.fixtures.mission_control_common_db import (
        create_common_database,
        drop_common_database,
    )

    database = await create_common_database(application_id="ai-engineer")
    try:
        bundles = order_bundles(load_bundles([SEEDS / "common", SEEDS / "ai-engineer"]), set())
        first = [await _apply(database, bundle) for bundle in bundles]
        assert all(stats["created"] > 0 for stats in first)
        replay = [await _apply(database, bundle) for bundle in bundles]
        assert all(stats["created"] == 0 for stats in replay)
        connection = await asyncpg.connect(database.owner_dsn)
        try:
            ids = {
                row["asset_id"]
                for row in await connection.fetch(
                    "SELECT asset_id FROM mission_control.asset_version WHERE kind='mcp_server'"
                )
            }
        finally:
            await connection.close()
        assert ids == {
            "definition:mcp_server:mcp.tavily",
            "definition:mcp_server:mcp.firecrawl",
            "definition:mcp_server:mcp.agent-browser",
            "definition:mcp_server:mcp.edgartools",
        }
    finally:
        await drop_common_database(database)
