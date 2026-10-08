"""FT-A8: catalog CLI flags, pins, inspect, render, and CLI/HTTP/MCP parity."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

import pytest
from fastapi.testclient import TestClient
from fastmcp import Client, Context

import mission_control.interfaces.cli.main as cli
from mission_control.application.capabilities.capability_search import CapabilitySearchService
from mission_control.application.coordinator.coordinator_facade import (
    CoordinatorFeatureFlags,
    InMemoryCoordinatorAuditSink,
    ProductionCoordinatorFacade,
)
from mission_control.domain.authoring.contracts import (
    DefinitionKind,
    DefinitionSelector,
    PluginDefinition,
    PublishedDefinition,
)
from mission_control.domain.capabilities.catalog_entry import capability_pin
from mission_control.domain.capabilities.host_support import all_profiles
from mission_control.interfaces.mcp.coordinator_server import (
    CoordinatorPrincipal,
    create_coordinator_server,
)
from tests.fixtures.capability_search_catalog import build_catalog, evaluation_cases
from tests.fixtures.catalog_http import CatalogHarness, catalog_harness
from tests.unit.coordinator.test_coordinator_facade import ReadyRuntimes

NOW = datetime(2026, 10, 7, tzinfo=UTC)


class Resolver:
    def __init__(self, tenant_scope: str) -> None:
        self.principal = CoordinatorPrincipal(
            actor_id="operator-1",
            tenant_scope=tenant_scope,
            roles=frozenset({"coordinator_planner"}),
            permissions=frozenset({"catalog.read"}),
        )

    async def resolve(self, _context: Context) -> CoordinatorPrincipal:
        return self.principal


@pytest.fixture
async def world() -> dict[str, Any]:
    definitions, search, projector = await build_catalog()
    by_id: dict[str, PublishedDefinition] = {
        item.ref.logical_id: item for item in await definitions.list_published_definitions()
    }
    plugin = PluginDefinition.model_validate(
        {
            # FT-A7 seeds the real plugin.web-research; this fixture plugin is a separate id.
            "logical_id": "plugin.fixture-search-pair",
            "title": "Web research",
            "description": "Tavily and Firecrawl together for web research.",
            "manifest": {
                "members": [
                    {"pin": capability_pin(by_id["mcp.tavily"]).render(), "role": "mcp_server"},
                    {
                        "pin": capability_pin(by_id["mcp.firecrawl"]).render(),
                        "role": "mcp_server",
                    },
                ]
            },
            "host_support": all_profiles().model_dump(mode="json"),
        }
    )
    published_plugin = await definitions.publish(plugin, "operator", NOW, 0)
    from mission_control.application.capabilities.catalog_projection import (
        CatalogProjectionInput,
    )

    await projector.project_many((CatalogProjectionInput(ref=published_plugin.ref),))
    service = CapabilitySearchService(search=search, definitions=definitions)
    harness = catalog_harness(definitions, service)
    facade = ProductionCoordinatorFacade(
        definitions=definitions,
        catalog_index=search,
        search=service,
        readiness=ReadyRuntimes(),
        coordinator_skill=DefinitionSelector(exact=by_id["skill.mission-control-coordinator"].ref),
        prompt_bindings={"propose_workflow": by_id["prompt.coordinator.propose-workflow"].ref},
        flags=CoordinatorFeatureFlags(
            capability_search_enabled=True,
            external_discovery_enabled=False,
            coordinator_launch_enabled=False,
        ),
        audit=InMemoryCoordinatorAuditSink(),
        clock=lambda: NOW,
    )
    server = create_coordinator_server(facade, Resolver(harness.tenant_scope))
    return {
        "harness": harness,
        "server": server,
        "by_id": by_id,
        "plugin": published_plugin,
    }


def run_cli(
    harness: CatalogHarness,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    *argv: str,
) -> tuple[int, dict[str, Any]]:
    monkeypatch.setenv("MISSION_CONTROL_TOKEN", "test-token")
    monkeypatch.setattr(cli.httpx, "Client", lambda **_kwargs: TestClient(harness.app))
    status = cli.main(["catalog", *argv, "--application", "biotech", "--json"])
    out = capsys.readouterr().out.strip().splitlines()
    return status, json.loads(out[-1]) if out else {}


@pytest.mark.asyncio
async def test_search_with_flags_prints_mode_pins_support_and_provenance(
    world: dict[str, Any], monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    status, payload = run_cli(
        world["harness"],
        monkeypatch,
        capsys,
        "search",
        "--query",
        "pubmed literature retrieval",
        "--kind",
        "mcp_server",
        "--host",
        "deep_agents",
        "--limit",
        "3",
    )
    assert status == 0, payload
    assert payload["search_mode"] == "lexical"
    top = payload["hits"][0]
    assert top["pin"].startswith("mcp.pubmed@2.10.20#sha256:")
    assert top["kind"] == "mcp_server"
    assert "deep_agents" in top["supported_profiles"]
    assert set(top["rank_provenance"]) >= {"lexical_rank", "trigram_rank", "fused_score"}


@pytest.mark.asyncio
async def test_request_file_form_still_works(
    world: dict[str, Any],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Any,
) -> None:
    harness = world["harness"]
    request = tmp_path / "search.json"
    request.write_text(
        json.dumps({"query": "sec filings", "tenant_scope": harness.tenant_scope, "limit": 2})
    )
    status, payload = run_cli(
        harness, monkeypatch, capsys, "search", "--request-file", str(request)
    )
    assert status == 0 and payload["hits"][0]["exact_ref"]["logical_id"] == "mcp.edgartools"


@pytest.mark.asyncio
async def test_pin_unique_and_ambiguous(
    world: dict[str, Any], monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    harness = world["harness"]
    status, payload = run_cli(
        harness,
        monkeypatch,
        capsys,
        "pin",
        "--query",
        "sec filings",
        "--kind",
        "mcp_server",
        "--host",
        "deep_agents",
    )
    assert status == 0, payload
    assert payload["pin"] == capability_pin(world["by_id"]["mcp.edgartools"]).render()
    status, payload = run_cli(
        harness, monkeypatch, capsys, "pin", "--query", "web search", "--kind", "mcp_server"
    )
    assert status == 2
    assert payload["detail"]["code"] == "AMBIGUOUS_CAPABILITY"
    assert len(payload["detail"]["candidates"]) >= 2
    status, payload = run_cli(
        harness, monkeypatch, capsys, "pin", "--query", "zzzz nothing matches", "--kind", "plugin"
    )
    assert status == 2 and payload["detail"]["code"] == "CAPABILITY_NOT_FOUND"


@pytest.mark.asyncio
async def test_inspect_pin_shows_support_secret_names_and_plugin_members(
    world: dict[str, Any], monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    harness = world["harness"]
    tavily = capability_pin(world["by_id"]["mcp.tavily"]).render()
    status, payload = run_cli(harness, monkeypatch, capsys, "inspect", "--pin", tavily)
    assert status == 0, payload
    assert payload["secret_refs"] == ["TAVILY_API_KEY"]
    assert payload["host_support"]["profiles"]["cursor_cloud"]["overlay"]["url"]
    assert "tvly-" not in json.dumps(payload)
    plugin_pin = capability_pin(world["plugin"]).render()
    status, payload = run_cli(harness, monkeypatch, capsys, "inspect", "--pin", plugin_pin)
    assert status == 0
    members = payload["plugin_members"]
    assert [(item["role"], item["found"]) for item in members] == [
        ("mcp_server", True),
        ("mcp_server", True),
    ]
    assert members[0]["pin"] == tavily
    direct = harness.client.get(harness.url("pins/" + plugin_pin.replace("#", "%23")))
    assert direct.status_code == 200 and direct.json()["pin"] == plugin_pin
    assert harness.client.get(harness.url("pins/not-a-pin")).status_code == 422


@pytest.mark.asyncio
async def test_render_pin_previews_projection_with_secret_references(
    world: dict[str, Any], monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    harness = world["harness"]
    plugin_pin = capability_pin(world["plugin"]).render()
    status, payload = run_cli(
        harness, monkeypatch, capsys, "render", "--pin", plugin_pin, "--host", "cursor_local"
    )
    assert status == 0, payload
    files = {item["path"]: item for item in payload["files"]}
    mcp = json.loads(files[".cursor/mcp.json"]["content"])
    assert set(mcp["mcpServers"]) == {"tavily", "firecrawl"}
    assert mcp["mcpServers"]["tavily"]["env"] == {"TAVILY_API_KEY": "${env:TAVILY_API_KEY}"}
    assert payload["report"]["plugin_expansions"][0][0] == plugin_pin
    status, payload = run_cli(
        harness,
        monkeypatch,
        capsys,
        "render",
        "--pin",
        capability_pin(world["by_id"]["mcp.tavily"]).render(),
        "--host",
        "deep_agents",
    )
    assert status == 0
    assert payload["in_process"]["mcp_connections"]["tavily"]["env_refs"] == {
        "TAVILY_API_KEY": "TAVILY_API_KEY"
    }


@pytest.mark.asyncio
async def test_cli_http_and_mcp_return_identical_hits_and_pins(
    world: dict[str, Any], monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    harness = world["harness"]
    cases = evaluation_cases()[:10]
    assert len(cases) == 10
    async with Client(world["server"]) as mcp:
        tools = {tool.name for tool in await mcp.list_tools()}
        assert {"search_capabilities", "pin_capability", "get_capability"} <= tools
        for case in cases:
            argv = ["search", "--query", case["query"], "--limit", "5"]
            for kind in case.get("kinds", ()):
                argv += ["--kind", kind]
            status, from_cli = run_cli(harness, monkeypatch, capsys, *argv)
            assert status == 0, from_cli
            body = {"query": case["query"], "kinds": case.get("kinds", []), "limit": 5}
            from_http = harness.client.post(harness.url("search"), json=body).json()
            from_mcp = await mcp.call_tool(
                "search_capabilities",
                {"query": case["query"], "kinds": case.get("kinds", []), "limit": 5},
            )
            assert from_mcp.data["ok"] is True, from_mcp.data
            pins = [hit["pin"] for hit in from_http["hits"]]
            assert [hit["pin"] for hit in from_cli["hits"]] == pins
            assert [hit["pin"] for hit in from_mcp.data["data"]["hits"]] == pins
            assert from_mcp.data["data"]["search_mode"] == from_http["search_mode"]

        pinned = await mcp.call_tool(
            "pin_capability",
            {"query": "sec filings", "kinds": ["mcp_server"], "host_profiles": ["deep_agents"]},
        )
        expected = capability_pin(world["by_id"]["mcp.edgartools"]).render()
        assert pinned.data["data"]["pin"] == expected
        ambiguous = await mcp.call_tool(
            "pin_capability", {"query": "web search", "kinds": ["mcp_server"]}
        )
        assert ambiguous.data["ok"] is False
        assert ambiguous.data["error"]["code"] == "AMBIGUOUS_CAPABILITY"
        detail = await mcp.call_tool(
            "get_capability", {"pin": capability_pin(world["plugin"]).render()}
        )
        assert detail.data["ok"] is True, detail.data
        assert len(detail.data["data"]["plugin_members"]) == 2
        assert detail.data["data"]["host_support"]["schema_version"] == (
            "mc.capability_host_support.v1"
        )
        resource = await mcp.read_resource(
            f"belllabs://catalog/{DefinitionKind.PLUGIN.value}/plugin.fixture-search-pair/1"
        )
        assert "plugin.fixture-search-pair" in resource[0].text


@pytest.mark.asyncio
async def test_discovery_candidates_never_appear_in_search(world: dict[str, Any]) -> None:
    harness = world["harness"]
    response = harness.client.post(
        harness.url("search"), json={"query": "pubmed", "include_external_candidates": True}
    ).json()
    assert all(hit["exact_ref"] is not None for hit in response["hits"])
    assert all(hit["authorization_state"] != "candidate_only" for hit in response["hits"])
