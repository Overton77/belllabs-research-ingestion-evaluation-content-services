"""FT-A6: seeded MCP servers, their tools, pins, secrets and per-application admission."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

from mission_control.domain.authoring.contracts import (
    MCPServerDefinition,
    MCPToolDefinition,
    PublishedDefinition,
)
from mission_control.domain.capabilities.catalog_entry import capability_pin
from mission_control.domain.capabilities.host_support import HostSupportStatus, LaneProfile
from mission_control.domain.capabilities.pins import CapabilityPin
from mission_control_db_contract.seeds import load_bundles

ROOT = Path(__file__).resolve().parents[3]
SEEDS = ROOT / "packages" / "mission-control-db-contract" / "seeds"
FIXTURES = ROOT / "tests" / "fixtures" / "mcp"
EXPECTED_TOOLS = {
    "mcp.tavily": 5,
    "mcp.firecrawl": 27,
    "mcp.agent-browser": 12,
    "mcp.pubmed": 11,
    "mcp.biomcp": 7,
    "mcp.edgartools": 13,
}
ADMISSION = {
    "common": {"mcp.tavily", "mcp.firecrawl", "mcp.agent-browser"},
    "biotech": {"mcp.pubmed", "mcp.biomcp"},
    "ai-engineer": {"mcp.edgartools"},
}


def _published(directory: str) -> list[PublishedDefinition]:
    out = []
    for bundle in load_bundles([SEEDS / directory]):
        if "agent-capabilities" not in bundle["seed_key"]:
            continue
        for record in bundle["records"]:
            if record["kind"] == "asset_version":
                out.append(PublishedDefinition.model_validate(record["fields"]["manifest"]))
    return out


def _servers(directory: str) -> dict[str, MCPServerDefinition]:
    return {
        item.definition.logical_id: item.definition
        for item in _published(directory)
        if isinstance(item.definition, MCPServerDefinition)
    }


@pytest.mark.parametrize("directory", ["common", "biotech", "ai-engineer"])
def test_admission_per_application(directory: str) -> None:
    assert set(_servers(directory)) == ADMISSION[directory]


def test_every_server_lists_its_tools_and_tool_rows() -> None:
    for directory in ADMISSION:
        published = _published(directory)
        servers = _servers(directory)
        tools = [i.definition for i in published if isinstance(i.definition, MCPToolDefinition)]
        for name, server in servers.items():
            exposed = {tool.name for tool in server.exposed_tools()}
            assert len(exposed) == EXPECTED_TOOLS[name], name
            children = {tool.tool_name for tool in tools if tool.server_ref.logical_id == name}
            assert children == exposed, name


def test_spec_details_per_server() -> None:
    common = _servers("common")
    tavily = common["mcp.tavily"]
    assert tavily.package_pin == "npm:tavily-mcp@0.2.22"
    assert "tavily_feedback" not in {tool.name for tool in tavily.tools}
    assert tavily.env_refs == {"TAVILY_API_KEY": "TAVILY_API_KEY"}
    cloud = tavily.host_support.overlay(LaneProfile.CURSOR_CLOUD)
    assert cloud["url"] == "https://mcp.tavily.com/mcp/" and "?" not in str(cloud["url"])
    firecrawl = common["mcp.firecrawl"]
    assert "firecrawl_extract" not in {tool.name for tool in firecrawl.exposed_tools()}
    assert firecrawl.env == {
        "FIRECRAWL_NO_SEARCH_FEEDBACK": "1",
        "FIRECRAWL_NO_ENDPOINT_FEEDBACK": "1",
    }
    browser = common["mcp.agent-browser"]
    assert browser.secret_refs == ()
    assert browser.host_support.status("cursor_cloud") is HostSupportStatus.UNQUALIFIED
    assert "agent-browser install" in browser.description
    biotech = _servers("biotech")
    pubmed = biotech["mcp.pubmed"]
    assert pubmed.package_pin == "npm:@cyanheads/pubmed-mcp-server@2.10.20"
    assert pubmed.env == {"MCP_TRANSPORT_TYPE": "stdio"}
    assert set(pubmed.secret_refs) == {"NCBI_API_KEY", "NCBI_ADMIN_EMAIL", "UNPAYWALL_EMAIL"}
    assert all(tool.read_only_hint for tool in biotech["mcp.biomcp"].tools)
    edgar = _servers("ai-engineer")["mcp.edgartools"]
    assert edgar.launch_template == ("uvx", "--from", "edgartools[ai]==5.61.1", "edgartools-mcp")
    assert edgar.secret_refs == ("EDGAR_IDENTITY",)


def test_no_author_hosted_or_key_in_url_endpoints() -> None:
    text = "".join(path.read_text(encoding="utf-8") for path in SEEDS.rglob("*agent-capabilities*"))
    for forbidden in ("caseyjhand", "tavilyApiKey=", "mcp.firecrawl.dev/fc-", "sec-edgar-mcp"):
        assert forbidden not in text


def test_pins_parse_and_tool_digests_match_fixtures() -> None:
    for directory in ADMISSION:
        for published in _published(directory):
            pin = capability_pin(published)
            assert CapabilityPin.parse(pin.render()) == pin
            definition = published.definition
            if isinstance(definition, MCPServerDefinition):
                document = json.loads(
                    (FIXTURES / f"{definition.logical_id}.tools.json").read_bytes()
                )
                encoded = json.dumps(
                    document["tools"], sort_keys=True, separators=(",", ":"), ensure_ascii=False
                ).encode()
                import hashlib

                assert (
                    definition.tools_list_digest == "sha256:" + hashlib.sha256(encoded).hexdigest()
                )
                assert pin.version == definition.source_provenance.upstream_version


def test_seeds_validate_script_passes() -> None:
    completed = subprocess.run(
        [sys.executable, str(ROOT / "scripts" / "seeds_validate.py")],
        capture_output=True,
        timeout=120,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr.decode()


def test_rejected_candidates_are_recorded() -> None:
    notes = (SEEDS / "AGENT_CAPABILITY_SEED_NOTES.md").read_text(encoding="utf-8")
    for candidate in ("sec-edgar-mcp", "pubmedmcp", "pubmed-mcp-server", "key-in-URL"):
        assert candidate in notes
