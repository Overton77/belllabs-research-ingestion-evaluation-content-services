"""Fixture capability rows for Host Projection golden tests (FT-A4).

Secret *names* appear here; secret *values* never do. ``FIXTURE_SECRET_VALUES`` are values
a careless renderer might leak; the tests assert none of them appear in any rendered byte.
"""

from __future__ import annotations

import hashlib

from mission_control.domain.agentic_components.projection import BundleFile, ResolvedCapability
from mission_control.domain.authoring.contracts import (
    HookScriptDefinition,
    MCPServerDefinition,
    PluginDefinition,
    SkillDefinition,
    SubagentProfileDefinition,
)
from mission_control.domain.capabilities.host_support import HostSupportStatus, all_profiles
from mission_control.domain.capabilities.pins import CapabilityPin

FIXTURE_SECRET_VALUES = ("tvly-FIXTURE-secret-0001", "fc-FIXTURE-secret-0002")
INSTRUCTION = "Collect peer-reviewed evidence on rapamycin and lifespan.\nCite every claim."
PACKET_INDEX = "- inputs/sources/source_manifest.json (materialized)\n- journal digest (reference)"


def _digest(data: bytes) -> str:
    return "sha256:" + hashlib.sha256(data).hexdigest()


def _pin(capability_id: str, version: str, seed: str) -> CapabilityPin:
    return CapabilityPin(
        capability_id=capability_id, version=version, digest=_digest(seed.encode())
    )


def _entry(item: BundleFile) -> dict[str, object]:
    return {"path": item.path, "digest": _digest(item.content), "size_bytes": len(item.content)}


SKILL_FILES = (
    BundleFile(
        path="SKILL.md",
        content=(
            b"---\nname: agent-browser\ndescription: Drive a real browser with agent-browser.\n"
            b"---\n\nRun `agent-browser skills get core` for the full guide.\n"
        ),
    ),
    BundleFile(
        path="scripts/open.sh", content=b'#!/bin/sh\nagent-browser open "$1"\n', executable=True
    ),
)

HOOK_FILES = (
    BundleFile(
        path="policy.py",
        content=(
            b"import json\n\n"
            b'print(json.dumps({"schema_version": "mc.hook_result.v1", "decision": "allow"}))\n'
        ),
    ),
)


def skill_row() -> ResolvedCapability:
    definition = SkillDefinition.model_validate(
        {
            "logical_id": "skill.agent-browser",
            "title": "agent-browser",
            "description": "Drive a real browser.",
            "skill_name": "agent-browser",
            "frontmatter": {"name": "agent-browser"},
            "body_summary": "Browser automation guide.",
            "bundle_ref": {
                "uri": "capability-bundles://biotech/skill_bundle/skill.agent-browser/0.38.2/x",
                "digest": _digest(b"bundle"),
                "media_type": "application/vnd.mc.skill-bundle+json",
                "size_bytes": 2,
            },
            "manifest_digest": _digest(b"manifest"),
            "file_manifest": [_entry(item) for item in SKILL_FILES],
            "source_provenance": {
                "source": "git",
                "locator": "https://github.com/vercel-labs/agent-browser",
                "upstream_identity": "vercel-labs/agent-browser",
                "upstream_version": "0.38.2",
            },
            "host_support": all_profiles().model_dump(mode="json"),
        }
    )
    return ResolvedCapability(
        pin=_pin("skill.agent-browser", "0.38.2", "skill"), definition=definition, files=SKILL_FILES
    )


def _mcp(logical_id: str, **values: object) -> MCPServerDefinition:
    base: dict[str, object] = {
        "logical_id": logical_id,
        "title": logical_id,
        "description": f"{logical_id} server",
        "schema_snapshot_ref": {
            "uri": f"mc-catalog://mcp/{logical_id}",
            "digest": _digest(logical_id.encode()),
            "media_type": "application/json",
            "size_bytes": 1,
        },
        "schema_digest": _digest(logical_id.encode()),
        "source_provenance": {
            "source": "local",
            "locator": f"npm:{logical_id}",
            "upstream_identity": logical_id,
            "upstream_version": "1",
        },
    }
    base.update(values)
    return MCPServerDefinition.model_validate(base)


def tavily_row() -> ResolvedCapability:
    support = all_profiles().model_dump(mode="json")
    support["profiles"]["cursor_cloud"]["overlay"] = {
        "transport": "streamable_http",
        "url": "https://mcp.tavily.com/mcp/",
        "header_refs": {"Authorization": "TAVILY_API_KEY"},
    }
    definition = _mcp(
        "mcp.tavily",
        transport="stdio",
        launch_template=["npx", "-y", "tavily-mcp@0.2.22"],
        secret_refs=["TAVILY_API_KEY"],
        env_refs={"TAVILY_API_KEY": "TAVILY_API_KEY"},
        package_pin="npm:tavily-mcp@0.2.22",
        tools=[{"name": name} for name in ("tavily_search", "tavily_extract", "tavily_crawl")],
        host_support=support,
    )
    return ResolvedCapability(pin=_pin("mcp.tavily", "0.2.22", "tavily"), definition=definition)


def firecrawl_row() -> ResolvedCapability:
    definition = _mcp(
        "mcp.firecrawl",
        transport="streamable_http",
        endpoint="https://mcp.firecrawl.dev/v2/mcp",
        secret_refs=["FIRECRAWL_API_KEY"],
        header_refs={"Authorization": "FIRECRAWL_API_KEY"},
        tools=[{"name": "firecrawl_scrape"}, {"name": "firecrawl_extract"}],
        tool_allowlist=["firecrawl_scrape"],
        host_support=all_profiles().model_dump(mode="json"),
    )
    return ResolvedCapability(
        pin=_pin("mcp.firecrawl", "3.28.2", "firecrawl"), definition=definition
    )


def agent_browser_mcp_row() -> ResolvedCapability:
    definition = _mcp(
        "mcp.agent-browser",
        transport="stdio",
        launch_template=["agent-browser", "mcp", "--tools", "core"],
        tools=[{"name": "agent_browser_open"}],
        host_support=all_profiles(cursor_cloud=HostSupportStatus.UNQUALIFIED).model_dump(
            mode="json"
        ),
    )
    return ResolvedCapability(
        pin=_pin("mcp.agent-browser", "0.38.2", "agent-browser-mcp"), definition=definition
    )


def hook_row() -> ResolvedCapability:
    definition = HookScriptDefinition.model_validate(
        {
            "logical_id": "hook.mc-policy-template",
            "title": "Policy template",
            "description": "Denies destructive shell commands.",
            "events": ["before_shell", "before_tool", "session_start", "before_model"],
            "fail_closed": True,
            "timeout_seconds": 10,
            "file_manifest": [_entry(item) for item in HOOK_FILES],
            "manifest_digest": _digest(b"hook-manifest"),
            "entrypoint": "policy.py",
            "interpreter": "python",
            "host_support": all_profiles().model_dump(mode="json"),
        }
    )
    return ResolvedCapability(
        pin=_pin("hook.mc-policy-template", "1.0.0", "hook"),
        definition=definition,
        files=HOOK_FILES,
    )


def verifier_row() -> ResolvedCapability:
    definition = SubagentProfileDefinition.model_validate(
        {
            "logical_id": "subagent.verifier",
            "title": "Verifier",
            "description": "Validates completed work.",
            "profile": {
                "name": "verifier",
                "description": "Validates completed work. Use proactively after any task is done.",
                "prompt": "You are a skeptical validator. Run the tests before agreeing.",
                "readonly": True,
                "skills": ["agent-browser"],
                "mcp_servers": ["mcp.tavily"],
            },
            "host_support": all_profiles().model_dump(mode="json"),
        }
    )
    return ResolvedCapability(
        pin=_pin("subagent.verifier", "1.0.0", "verifier"), definition=definition
    )


def summarizer_row() -> ResolvedCapability:
    definition = SubagentProfileDefinition.model_validate(
        {
            "logical_id": "subagent.summarizer",
            "title": "Summarizer",
            "description": "Summarizes sources.",
            "profile": {
                "name": "summarizer",
                "description": "Summarizes collected sources into short notes.",
                "prompt": "Summarize each source in three bullets.",
                "background": True,
            },
            "host_support": all_profiles().model_dump(mode="json"),
        }
    )
    return ResolvedCapability(
        pin=_pin("subagent.summarizer", "1.0.0", "summarizer"), definition=definition
    )


def plugin_row() -> ResolvedCapability:
    members = (tavily_row(), skill_row(), agent_browser_mcp_row())
    roles = ("mcp_server", "skill", "mcp_server")
    definition = PluginDefinition.model_validate(
        {
            "logical_id": "plugin.web-research",
            "title": "Web research",
            "description": "Tavily, agent-browser skill and agent-browser MCP.",
            "manifest": {
                "members": [
                    {"pin": member.pin.render(), "role": role, "optional": index == 2}
                    for index, (member, role) in enumerate(zip(members, roles, strict=True))
                ]
            },
            "host_support": all_profiles().model_dump(mode="json"),
        }
    )
    return ResolvedCapability(
        pin=_pin("plugin.web-research", "1.0.0", "plugin"), definition=definition, members=members
    )


def fixture_rows() -> tuple[ResolvedCapability, ...]:
    """The set every golden profile renders: a plugin plus loose rows of every kind."""
    return (plugin_row(), firecrawl_row(), hook_row(), verifier_row(), summarizer_row())
