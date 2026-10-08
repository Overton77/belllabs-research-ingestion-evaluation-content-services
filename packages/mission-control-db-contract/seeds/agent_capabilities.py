"""Agent-capability seed definitions (FT-A6, SPEC-01 "Seeds"): MCP servers and their tools.

Imported by ``generate_catalog_seeds.py``. Every server is a published ``MCPServerDefinition``
(revision 1) with exact package pins, secret references by NAME only, the tool list from
``tests/fixtures/mcp/<id>.tools.json`` (``tools_list_digest`` = digest of that list) and a
``mc.capability_host_support.v1`` matrix; each tool is an ``MCPToolDefinition`` child row.
Admission per application is the bundle a server is in: common (both applications), biotech
or ai-engineer. Rejected candidates and their reasons are in AGENT_CAPABILITY_SEED_NOTES.md.
"""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[3]
FIXTURES = REPO_ROOT / "tests" / "fixtures" / "mcp"
AGENT_SEED_TIME = datetime(2026, 10, 7, tzinfo=UTC)
COMMON_KEY = "mc.catalog.agent-capabilities"
APP_KEYS = {
    "biotech": "mc.app.biotech.agent-capabilities",
    "ai-engineer": "mc.app.ai-engineer.agent-capabilities",
}
SEED_VERSION = "1.0.0"

PROFILES = ("deep_agents", "cursor_local", "cursor_cloud", "claude_agent_sdk", "codex")


def _support(
    *,
    unqualified: tuple[str, ...] = (),
    overlays: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    profiles: dict[str, Any] = {}
    for profile in PROFILES:
        entry: dict[str, Any] = {
            "status": "unqualified" if profile in unqualified else "supported",
        }
        if overlays and profile in overlays:
            entry["overlay"] = overlays[profile]
        profiles[profile] = entry
    return {"schema_version": "mc.capability_host_support.v1", "profiles": profiles}


def _remote(url: str, secret: str) -> dict[str, Any]:
    return {"transport": "streamable_http", "url": url, "header_refs": {"Authorization": secret}}


_SELF_HOSTED = {"transport": "streamable_http", "url_ref": "MCP_SELF_HOSTED_URL"}

SERVERS: tuple[dict[str, Any], ...] = (
    {
        "id": "mcp.tavily",
        "bundle": "common",
        "title": "Tavily web search, extract, crawl, map and research",
        "description": (
            "Tavily MCP: web search, page extraction, crawl, site map and multi-source research. "
            "Keyless mode supports search and extract only. Cloud lanes use the remote endpoint "
            "with an Authorization header carrying the key reference (never the key-in-URL form)."
        ),
        "upstream": ("tavily-mcp", "0.2.22", "https://github.com/tavily-ai/tavily-mcp", "MIT"),
        "package_pin": "npm:tavily-mcp@0.2.22",
        "launch": ["npx", "-y", "tavily-mcp@0.2.22"],
        "secret_refs": ["TAVILY_API_KEY"],
        "env_refs": {"TAVILY_API_KEY": "TAVILY_API_KEY"},
        "host_support": _support(
            overlays={"cursor_cloud": _remote("https://mcp.tavily.com/mcp/", "TAVILY_API_KEY")}
        ),
        "tags": ["web", "search", "research", "extract", "crawl"],
    },
    {
        "id": "mcp.firecrawl",
        "bundle": "common",
        "title": "Firecrawl scrape, search, crawl, parse, research papers and monitors",
        "description": (
            "Firecrawl MCP: scrape pages to markdown or JSON, web and developer and government "
            "search, crawl and map sites, parse documents, research paper index (PubMed, bioRxiv, "
            "medRxiv, arXiv), agent research and change monitors. firecrawl_extract is deprecated "
            "and excluded by the tool allowlist; feedback tools are disabled by environment."
        ),
        "upstream": (
            "firecrawl-mcp",
            "3.28.2",
            "https://github.com/firecrawl/firecrawl-mcp-server",
            "MIT",
        ),
        "package_pin": "npm:firecrawl-mcp@3.28.2",
        "launch": ["npx", "-y", "firecrawl-mcp@3.28.2"],
        "env": {"FIRECRAWL_NO_SEARCH_FEEDBACK": "1", "FIRECRAWL_NO_ENDPOINT_FEEDBACK": "1"},
        "secret_refs": ["FIRECRAWL_API_KEY"],
        "env_refs": {"FIRECRAWL_API_KEY": "FIRECRAWL_API_KEY"},
        "exclude_tools": ["firecrawl_extract"],
        "host_support": _support(
            overlays={
                "cursor_cloud": _remote("https://mcp.firecrawl.dev/v2/mcp", "FIRECRAWL_API_KEY")
            }
        ),
        "tags": ["web", "scrape", "search", "crawl", "papers", "research", "parse"],
    },
    {
        "id": "mcp.agent-browser",
        "bundle": "common",
        "title": "agent-browser real browser automation (core tool profile)",
        "description": (
            "Vercel agent-browser MCP server (agent-browser mcp --tools core): open pages, "
            "accessibility snapshots with element refs, click, fill, type, press, wait, "
            "screenshot and evaluate. No secrets. Environment Profile requirement: run "
            "`agent-browser install` (Chrome for Testing) before first use. cursor_cloud is "
            "unqualified."
        ),
        "upstream": (
            "agent-browser",
            "0.38.2",
            "https://github.com/vercel-labs/agent-browser",
            "Apache-2.0",
        ),
        "package_pin": "npm:agent-browser@0.38.2",
        "launch": ["agent-browser", "mcp", "--tools", "core"],
        "secret_refs": [],
        "env_refs": {},
        "host_support": _support(unqualified=("cursor_cloud",)),
        "tags": ["browser", "automation", "web", "testing", "ui"],
    },
    {
        "id": "mcp.pubmed",
        "bundle": "biotech",
        "title": "PubMed literature search, fetch, full text, citations and MeSH",
        "description": (
            "cyanheads PubMed MCP server: PubMed and Europe PMC literature retrieval, article "
            "metadata and full text, citation formatting, related articles, spell check, MeSH "
            "lookup and identifier conversion (biomedical literature). Requires Node >= 24 or "
            "Bun >= 1.4. NCBI allows 3 requests/s without NCBI_API_KEY, 10 with it. Cloud lanes "
            "need a self-hosted Streamable HTTP endpoint; the author's public endpoint is never "
            "referenced."
        ),
        "upstream": (
            "@cyanheads/pubmed-mcp-server",
            "2.10.20",
            "https://github.com/cyanheads/pubmed-mcp-server",
            "Apache-2.0",
        ),
        "package_pin": "npm:@cyanheads/pubmed-mcp-server@2.10.20",
        "launch": ["npx", "-y", "@cyanheads/pubmed-mcp-server@2.10.20"],
        "env": {"MCP_TRANSPORT_TYPE": "stdio"},
        "secret_refs": ["NCBI_API_KEY", "NCBI_ADMIN_EMAIL", "UNPAYWALL_EMAIL"],
        "env_refs": {
            "NCBI_API_KEY": "NCBI_API_KEY",
            "NCBI_ADMIN_EMAIL": "NCBI_ADMIN_EMAIL",
            "UNPAYWALL_EMAIL": "UNPAYWALL_EMAIL",
        },
        "host_support": _support(
            unqualified=("cursor_cloud",), overlays={"cursor_cloud": _SELF_HOSTED}
        ),
        "tags": ["pubmed", "literature", "biomedical", "papers", "citations", "mesh"],
    },
    {
        "id": "mcp.biomcp",
        "bundle": "biotech",
        "title": "BioMCP biomedical search across literature, trials, variants and genes",
        "description": (
            "GenomOncology BioMCP (biomcp serve): seven read-only tools pivoting across "
            "literature (PubMed, PubTator3, Europe PMC), clinical trials, variants (ClinVar, "
            "ClinGen) and genes. Optional secret refs: NCBI_API_KEY, S2_API_KEY, "
            "OPENFDA_API_KEY, NCI_API_KEY, ONCOKB_TOKEN, ALPHAGENOME_API_KEY, DISGENET_API_KEY. "
            "Cloud lanes need biomcp serve-http self-hosted."
        ),
        "upstream": ("biomcp-cli", "0.9.1", "https://github.com/genomoncology/biomcp", "MIT"),
        "package_pin": "pypi:biomcp-cli==0.9.1",
        "launch": ["biomcp", "serve"],
        "secret_refs": [
            "NCBI_API_KEY",
            "S2_API_KEY",
            "OPENFDA_API_KEY",
            "NCI_API_KEY",
            "ONCOKB_TOKEN",
            "ALPHAGENOME_API_KEY",
            "DISGENET_API_KEY",
        ],
        "env_refs": {},
        "host_support": _support(
            unqualified=("cursor_cloud",), overlays={"cursor_cloud": _SELF_HOSTED}
        ),
        "tags": ["biomedical", "variants", "genes", "clinical-trials", "literature"],
    },
    {
        "id": "mcp.edgartools",
        "bundle": "ai-engineer",
        "title": "edgartools SEC EDGAR filings, financials and ownership",
        "description": (
            "edgartools MCP server (uvx --from edgartools[ai]==5.61.1 edgartools-mcp): SEC "
            "EDGAR company profiles, filings, full-text search, XBRL financial trends, insider "
            "and institutional ownership, funds and proxy compensation (sec filings). SEC "
            "requires a declared identity (EDGAR_IDENTITY) and allows 10 requests/s. Cloud lanes "
            "need the streamable-http transport self-hosted."
        ),
        "upstream": ("edgartools", "5.61.1", "https://github.com/dgunning/edgartools", "MIT"),
        "package_pin": "pypi:edgartools[ai]==5.61.1",
        "launch": ["uvx", "--from", "edgartools[ai]==5.61.1", "edgartools-mcp"],
        "secret_refs": ["EDGAR_IDENTITY"],
        "env_refs": {"EDGAR_IDENTITY": "EDGAR_IDENTITY"},
        "host_support": _support(
            unqualified=("cursor_cloud",), overlays={"cursor_cloud": _SELF_HOSTED}
        ),
        "tags": ["sec", "edgar", "filings", "financials", "ownership"],
    },
)


def _lf(path: Path) -> bytes:
    return path.read_bytes().replace(b"\r\n", b"\n")


def _sha256(payload: bytes) -> str:
    return "sha256:" + hashlib.sha256(payload).hexdigest()


def tools_list_digest(tools: list[dict[str, Any]]) -> str:
    """Digest of the canonical ``tools`` list (sorted keys, compact) of a tools fixture."""
    encoded = json.dumps(tools, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return _sha256(encoded.encode("utf-8"))


def fixture(server_id: str) -> tuple[dict[str, Any], bytes]:
    path = FIXTURES / f"{server_id}.tools.json"
    payload = _lf(path)
    return json.loads(payload), payload


def server_definition(spec: dict[str, Any]) -> Any:
    from mission_control.domain.authoring.contracts import MCPServerDefinition

    document, payload = fixture(spec["id"])
    tools = document["tools"]
    names = [tool["name"] for tool in tools]
    excluded = set(spec.get("exclude_tools", ()))
    package, version, locator, license_name = spec["upstream"]
    return MCPServerDefinition.model_validate(
        {
            "logical_id": spec["id"],
            "title": spec["title"],
            "description": spec["description"],
            "transport": "stdio",
            "launch_template": spec["launch"],
            "allowed_tools": [name for name in names if name not in excluded],
            "schema_snapshot_ref": {
                "uri": f"repo://tests/fixtures/mcp/{spec['id']}.tools.json",
                "digest": _sha256(payload),
                "media_type": "application/json",
                "size_bytes": len(payload),
            },
            "schema_digest": tools_list_digest(tools),
            "source_provenance": {
                "source": "git",
                "locator": locator,
                "upstream_identity": package,
                "upstream_version": version,
                "license": license_name,
            },
            "review_status": "approved",
            "maturity": "experimental",
            "host_support": spec["host_support"],
            "secret_refs": spec["secret_refs"],
            "env": spec.get("env", {}),
            "env_refs": spec["env_refs"],
            "package_pin": spec["package_pin"],
            "tools": [
                {
                    "name": tool["name"],
                    "description": tool["description"],
                    "side_effect_class": tool["side_effect_class"],
                    "read_only_hint": tool["read_only_hint"],
                }
                for tool in tools
            ],
            "tools_list_digest": tools_list_digest(tools),
            "tool_allowlist": sorted(name for name in names if name not in excluded)
            if excluded
            else None,
        }
    )


def tool_definitions(server: Any, server_ref: Any) -> list[Any]:
    from mission_control.domain.authoring.contracts import MCPToolDefinition

    document, _ = fixture(server.logical_id)
    definitions = []
    for tool in document["tools"]:
        if server.tool_allowlist is not None and tool["name"] not in server.tool_allowlist:
            continue
        slug = tool["name"].replace("_", "-").lower()
        definitions.append(
            MCPToolDefinition.model_validate(
                {
                    "logical_id": f"{server.logical_id}.tool.{slug}",
                    "title": tool["name"],
                    "description": tool["description"] or tool["name"],
                    "server_ref": server_ref.model_dump(mode="json"),
                    "tool_name": tool["name"],
                    "input_schema": tool["input_schema"],
                    "annotations": {
                        "readOnlyHint": tool["read_only_hint"],
                        "schema_source": document["capture_method"],
                    },
                    "schema_digest": tools_list_digest([tool]),
                    "side_effect_class": tool["side_effect_class"],
                    "maturity": "experimental",
                }
            )
        )
    return definitions


def bundles(published_records: Any, seed_format: str, compatibility: str, actor: str) -> dict:
    """Seed bundles: common (both applications) plus one per application."""
    from mission_control.domain.authoring.canonical import sha256_digest
    from mission_control.domain.authoring.contracts import DefinitionKind, ExactDefinitionRef

    grouped: dict[str, list[dict[str, Any]]] = {"common": [], "biotech": [], "ai-engineer": []}
    for spec in SERVERS:
        server = server_definition(spec)
        evidence = [
            "packages/mission-control-db-contract/seeds/agent_capabilities.py",
            f"tests/fixtures/mcp/{spec['id']}.tools.json",
            "docs/specs/fast-track-2026-10/research/seed-capabilities-and-formats.md",
        ]
        records = published_records(server, "agent-capability", evidence)
        server_ref = ExactDefinitionRef(
            kind=DefinitionKind.MCP_SERVER,
            logical_id=server.logical_id,
            revision=1,
            digest=sha256_digest(server),
        )
        for tool in tool_definitions(server, server_ref):
            records += published_records(tool, "agent-capability", evidence)
        grouped[spec["bundle"]] += records
    common = {
        "format": seed_format,
        "seed_key": COMMON_KEY,
        "seed_version": SEED_VERSION,
        "component_compatibility": compatibility,
        "depends_on": [{"seed_key": "mc.catalog.approved-assets", "seed_version": "1.0.0"}],
        "actor_ref": actor,
        "description": (
            "Agent-capability MCP servers admitted in every application (FT-A6): Tavily, "
            "Firecrawl and agent-browser, each a published MCP Server definition with exact "
            "package pins, secret reference NAMES only, host_support and one MCP Tool row per "
            "tool. Requires migration 0025 (agent-composition kinds). Tool lists are "
            "transcribed fixtures (capture_method=transcribed) until re-captured live."
        ),
        "records": grouped["common"],
    }
    result = {f"common/{COMMON_KEY}-{SEED_VERSION}.json": common}
    for app, key in APP_KEYS.items():
        names = ", ".join(spec["id"] for spec in SERVERS if spec["bundle"] == app)
        result[f"{app}/{key}-{SEED_VERSION}.json"] = {
            "format": seed_format,
            "seed_key": key,
            "seed_version": SEED_VERSION,
            "component_compatibility": compatibility,
            "depends_on": [{"seed_key": COMMON_KEY, "seed_version": SEED_VERSION}],
            "actor_ref": actor,
            "description": (
                f"Agent-capability MCP servers admitted only in '{app}' (FT-A6): {names}. "
                "Secret reference NAMES only; requires migration 0025."
            ),
            "records": grouped[app],
        }
    return result


def seed_pins() -> dict[str, str]:
    """``capability_id -> Capability Pin`` for every seeded server (upstream version, digest)."""
    from mission_control.domain.authoring.canonical import sha256_digest

    pins = {}
    for spec in SERVERS:
        server = server_definition(spec)
        pins[spec["id"]] = f"{spec['id']}@{spec['upstream'][1]}#{sha256_digest(server)}"
    return pins
