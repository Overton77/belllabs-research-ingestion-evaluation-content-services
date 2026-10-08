"""FT-A1: agent-composition kinds, host support, pins, plugins and subagent profiles."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest
from pydantic import TypeAdapter, ValidationError

from mission_control.adapters.postgres.control_plane.catalog_assets import (
    AGENT_COMPOSITION_ASSET_KINDS,
    ASSET_KIND,
    LEGACY_ASSET_KIND,
    asset_kinds_for,
    capability_core_columns,
)
from mission_control.domain.authoring.canonical import sha256_digest, stable_json_dump
from mission_control.domain.authoring.contracts import (
    CapabilityKind,
    CapabilityRequirement,
    Definition,
    DefinitionKind,
    ExactDefinitionRef,
    HookScriptDefinition,
    MCPServerDefinition,
    PluginDefinition,
    PublishedDefinition,
    SubagentProfileDefinition,
)
from mission_control.domain.capabilities.catalog_entry import summarize
from mission_control.domain.capabilities.host_support import (
    AgentCapabilityKind,
    CapabilityHostSupport,
    HostSupportStatus,
    LaneProfile,
    all_profiles,
)
from mission_control.domain.capabilities.pins import CapabilityPin, CapabilityPinError, is_pin
from mission_control.domain.capabilities.plugins import PluginManifest
from mission_control.domain.capabilities.subagents import SubagentProfile

ROOT = Path(__file__).resolve().parents[3]
MIGRATION = (
    ROOT
    / "packages/mission-control-db-contract/component/migrations"
    / "0025_capability_kinds_and_host_support.sql"
)
DIGEST = "sha256:" + "a" * 64
DIGEST_B = "sha256:" + "b" * 64


def _file(path: str) -> dict[str, object]:
    return {"path": path, "digest": DIGEST, "size_bytes": 10}


# --- vocabulary reconciliation ------------------------------------------------------------


def test_asset_kind_mapping_is_total_and_injective_for_agent_kinds() -> None:
    assert set(ASSET_KIND) == set(DefinitionKind)
    agent = {
        kind: ASSET_KIND[kind]
        for kind in (
            DefinitionKind.SKILL,
            DefinitionKind.MCP_SERVER,
            DefinitionKind.MCP_TOOL,
            DefinitionKind.HOOK_SCRIPT,
            DefinitionKind.SUBAGENT_PROFILE,
            DefinitionKind.PLUGIN,
        )
    }
    assert len(set(agent.values())) == len(agent)
    assert set(agent.values()) == AGENT_COMPOSITION_ASSET_KINDS
    assert ASSET_KIND[DefinitionKind.MIDDLEWARE] == "middleware"
    assert "hook" not in ASSET_KIND.values()
    assert "plugin_package" not in {kind.value for kind in DefinitionKind}


def test_every_sql_literal_is_allowed_by_migration_0025() -> None:
    sql = MIGRATION.read_text(encoding="utf-8")
    check = sql[sql.index("ADD CONSTRAINT asset_version_kind_check") :]
    check = check[: check.index(";")]
    for literal in {*ASSET_KIND.values(), *LEGACY_ASSET_KIND.values()}:
        assert f"'{literal}'" in check, literal


def test_legacy_literals_are_still_read() -> None:
    assert asset_kinds_for(DefinitionKind.SKILL) == {"skill_bundle", "skill"}
    assert asset_kinds_for(DefinitionKind.MCP_TOOL) == {"mcp_tool", "tool"}
    assert asset_kinds_for(DefinitionKind.HOOK_SCRIPT) == {"hook_script"}


@pytest.mark.parametrize(
    ("capability_kind", "definition_kind"),
    [
        (CapabilityKind.HOOK_SCRIPT, DefinitionKind.HOOK_SCRIPT),
        (CapabilityKind.SUBAGENT_PROFILE, DefinitionKind.SUBAGENT_PROFILE),
        (CapabilityKind.PLUGIN, DefinitionKind.PLUGIN),
    ],
)
def test_capability_requirement_accepts_new_kinds(
    capability_kind: CapabilityKind, definition_kind: DefinitionKind
) -> None:
    ref = ExactDefinitionRef(kind=definition_kind, logical_id="x.y", revision=1, digest=DIGEST)
    CapabilityRequirement(
        requirement_id="req.one",
        capability_kind=capability_kind,
        allowed_refs=frozenset({ref}),
        attachment_target="agent.main",
    )
    wrong = ref.model_copy(update={"kind": DefinitionKind.SKILL})
    with pytest.raises(ValidationError, match="wrong family"):
        CapabilityRequirement(
            requirement_id="req.one",
            capability_kind=capability_kind,
            allowed_refs=frozenset({wrong}),
            attachment_target="agent.main",
        )


# --- host support --------------------------------------------------------------------------


def test_host_support_rejects_unknown_profiles_and_overlay_keys() -> None:
    with pytest.raises(ValidationError, match="unknown lane profile"):
        CapabilityHostSupport.model_validate({"profiles": {"windsurf": {"status": "supported"}}})
    support = CapabilityHostSupport.model_validate(
        {"profiles": {"cursor_local": {"status": "supported", "overlay": {"readonly": True}}}}
    )
    support.validate_for(AgentCapabilityKind.SUBAGENT_PROFILE)
    with pytest.raises(ValueError, match="unknown key"):
        support.validate_for(AgentCapabilityKind.MCP_SERVER)
    codex_only = CapabilityHostSupport.model_validate(
        {"profiles": {"codex": {"status": "supported", "overlay": {"readonly": True}}}}
    )
    with pytest.raises(ValueError, match="codex"):
        codex_only.validate_for(AgentCapabilityKind.SUBAGENT_PROFILE)


def test_host_support_status_defaults_to_unsupported() -> None:
    support = CapabilityHostSupport.model_validate(
        {"profiles": {"deep_agents": {"status": "supported"}}}
    )
    assert support.supported_profiles() == (LaneProfile.DEEP_AGENTS,)
    assert support.status("codex") is HostSupportStatus.UNSUPPORTED


# --- pins ------------------------------------------------------------------------------------


def test_pin_round_trip_and_rejections() -> None:
    text = f"mcp.pubmed@2.10.20#{DIGEST}"
    pin = CapabilityPin.parse(text)
    assert (pin.capability_id, pin.version, pin.digest) == ("mcp.pubmed", "2.10.20", DIGEST)
    assert pin.render() == text == str(pin)
    assert CapabilityPin.model_validate(text) == pin
    assert is_pin(text)
    for bad in (
        "mcp.pubmed",
        "mcp.pubmed@2.10.20",
        f"mcp.pubmed#{DIGEST}",
        "mcp.pubmed@2.10.20#sha256:abc",
        f"MCP@1#{DIGEST}",
        f"@1#{DIGEST}",
    ):
        with pytest.raises(CapabilityPinError):
            CapabilityPin.parse(bad)
        assert not is_pin(bad)


# --- plugins ---------------------------------------------------------------------------------


def _plugin_manifest() -> PluginManifest:
    return PluginManifest.model_validate(
        {
            "members": [
                {"pin": f"mcp.tavily@0.2.22#{DIGEST}", "role": "mcp_server"},
                {"pin": f"skill.agent-browser@0.38.2#{DIGEST_B}", "role": "skill"},
                {
                    "pin": f"mcp.agent-browser@0.38.2#{DIGEST}",
                    "role": "mcp_server",
                    "optional": True,
                },
            ]
        }
    )


def test_plugin_host_support_is_member_intersection_honouring_optional() -> None:
    support = {
        "mcp.tavily": all_profiles(),
        "skill.agent-browser": all_profiles(codex=HostSupportStatus.UNQUALIFIED),
        "mcp.agent-browser": all_profiles(cursor_cloud=HostSupportStatus.UNSUPPORTED),
    }
    result = _plugin_manifest().host_support(lambda pin: support[pin.capability_id])
    assert result.status("cursor_cloud") is HostSupportStatus.SUPPORTED  # optional member
    assert result.status("codex") is HostSupportStatus.UNQUALIFIED
    assert result.status("deep_agents") is HostSupportStatus.SUPPORTED


def test_plugin_manifest_rejects_duplicates_and_all_optional() -> None:
    with pytest.raises(ValidationError, match="once"):
        PluginManifest.model_validate(
            {
                "members": [
                    {"pin": f"mcp.tavily@0.2.22#{DIGEST}", "role": "mcp_server"},
                    {"pin": f"mcp.tavily@0.2.23#{DIGEST}", "role": "mcp_server"},
                ]
            }
        )
    with pytest.raises(ValidationError, match="required member"):
        PluginManifest.model_validate(
            {"members": [{"pin": f"a.b@1#{DIGEST}", "role": "skill", "optional": True}]}
        )
    with pytest.raises(ValidationError):
        PluginManifest.model_validate({"members": [{"pin": "a.b@1", "role": "skill"}]})


# --- subagent profiles -----------------------------------------------------------------------


@pytest.mark.parametrize("name", ["explore", "shell", "bash", "browser", "general-purpose"])
def test_subagent_name_cannot_shadow_lane_builtins(name: str) -> None:
    with pytest.raises(ValidationError, match="built-in"):
        SubagentProfile(name=name, description="d", prompt="p")


def test_subagent_profile_contract() -> None:
    profile = SubagentProfile(
        name="verifier", description="Validates completed work.", prompt="Be skeptical."
    )
    assert profile.schema_version == "mc.subagent_profile.v1"
    assert profile.model == "inherit"
    with pytest.raises(ValidationError, match="exactly one"):
        SubagentProfile(name="verifier", description="d")
    with pytest.raises(ValidationError):
        SubagentProfile(name="verifier", description="d", prompt="x" * 8193)


# --- definitions -----------------------------------------------------------------------------


def _hook(**overrides: object) -> HookScriptDefinition:
    values: dict[str, object] = {
        "logical_id": "hook.mc-policy-template",
        "title": "Policy template",
        "description": "Denies destructive shell commands.",
        "events": ["before_shell", "before_tool"],
        "matcher": "shell|execute",
        "fail_closed": True,
        "file_manifest": [_file("policy.py")],
        "manifest_digest": DIGEST,
        "entrypoint": "policy.py",
        "interpreter": "python",
        "host_support": all_profiles().model_dump(mode="json"),
    }
    values.update(overrides)
    return HookScriptDefinition.model_validate(values)


def test_hook_script_definition_validation() -> None:
    hook = _hook()
    assert hook.kind is DefinitionKind.HOOK_SCRIPT
    with pytest.raises(ValidationError, match="entrypoint"):
        _hook(entrypoint="missing.py")
    with pytest.raises(ValidationError, match="subset"):
        _hook(required_events=["stop"])
    with pytest.raises(ValidationError, match="regex"):
        _hook(matcher="(")
    # session_start cannot run on cursor_cloud; a required event there is a contradiction
    with pytest.raises(ValidationError, match="cursor_cloud"):
        _hook(events=["session_start"], required_events=["session_start"])
    with pytest.raises(ValidationError, match="never values"):
        _hook(secret_refs=["sk-live-123"])


def _mcp(**overrides: object) -> MCPServerDefinition:
    values: dict[str, object] = {
        "logical_id": "mcp.tavily",
        "title": "Tavily",
        "description": "Web search.",
        "transport": "stdio",
        "launch_template": ["npx", "-y", "tavily-mcp@0.2.22"],
        "schema_snapshot_ref": {
            "uri": "mc-catalog://x",
            "digest": DIGEST,
            "media_type": "application/json",
            "size_bytes": 1,
        },
        "schema_digest": DIGEST,
        "source_provenance": {
            "source": "local",
            "locator": "npm:tavily-mcp",
            "upstream_identity": "tavily-mcp",
            "upstream_version": "0.2.22",
        },
        "secret_refs": ["TAVILY_API_KEY"],
        "env_refs": {"TAVILY_API_KEY": "TAVILY_API_KEY"},
        "package_pin": "npm:tavily-mcp@0.2.22",
        "tools": [{"name": "tavily_search"}, {"name": "tavily_extract"}],
        "tool_allowlist": ["tavily_search"],
        "host_support": all_profiles().model_dump(mode="json"),
    }
    values.update(overrides)
    return MCPServerDefinition.model_validate(values)


def test_mcp_server_core_fields() -> None:
    server = _mcp()
    assert [tool.name for tool in server.exposed_tools()] == ["tavily_search"]
    with pytest.raises(ValidationError, match="not declared in secret_refs"):
        _mcp(secret_refs=[])
    with pytest.raises(ValidationError, match="tool_allowlist"):
        _mcp(tool_allowlist=["tavily_crawl"])
    with pytest.raises(ValidationError, match="unknown key"):
        _mcp(host_support={"profiles": {"codex": {"status": "supported", "overlay": {"x": 1}}}})


def test_additive_fields_are_digest_neutral_at_default() -> None:
    """Definitions published before 0025 keep their digest and stored manifest bytes."""
    plain = _mcp(
        secret_refs=(),
        env_refs={},
        package_pin=None,
        tools=(),
        tool_allowlist=None,
        host_support=CapabilityHostSupport().model_dump(mode="json"),
    )
    dumped = stable_json_dump(plain)
    for added in ("host_support", "secret_refs", "env_refs", "tools", "package_pin"):
        assert added not in dumped
    assert sha256_digest(plain) != sha256_digest(_mcp())


def test_seeded_skill_definition_digest_is_unchanged() -> None:
    bundle = json.loads(
        (
            ROOT
            / "packages/mission-control-db-contract/seeds/common"
            / "mc.catalog.approved-assets-1.0.0.json"
        ).read_text(encoding="utf-8")
    )
    checked = 0
    for record in bundle["records"]:
        manifest = record["fields"].get("manifest", {})
        if record["fields"].get("contract") == "mission-control.published-definition/1":
            published = PublishedDefinition.model_validate(manifest)
            assert sha256_digest(published.definition) == published.ref.digest
            checked += 1
    assert checked


def test_definition_union_round_trips_new_kinds_and_summaries() -> None:
    adapter = TypeAdapter(Definition)
    subagent = SubagentProfileDefinition.model_validate(
        {
            "logical_id": "subagent.verifier",
            "title": "Verifier",
            "description": "Validates completed work.",
            "profile": {"name": "verifier", "description": "Validates.", "prompt": "Check."},
            "host_support": {
                "profiles": {
                    "cursor_local": {"status": "supported", "overlay": {"readonly": True}},
                    "deep_agents": {"status": "supported"},
                }
            },
        }
    )
    plugin = PluginDefinition.model_validate(
        {
            "logical_id": "plugin.web-research",
            "title": "Web research",
            "description": "Tavily, Firecrawl and agent-browser.",
            "manifest": _plugin_manifest().model_dump(mode="json"),
            "host_support": all_profiles().model_dump(mode="json"),
        }
    )
    for definition in (subagent, plugin, _hook(), _mcp()):
        restored = adapter.validate_python(definition.model_dump(mode="json"))
        assert restored == definition
        ref = ExactDefinitionRef(
            kind=definition.kind,
            logical_id=definition.logical_id,
            revision=1,
            digest=sha256_digest(definition),
        )
        published = PublishedDefinition(
            ref=ref,
            definition=definition,
            published_at=datetime(2026, 10, 7, tzinfo=UTC),
            published_by="test",
        )
        summary = summarize(published)
        assert summary.kind is definition.kind
        assert summary.host_support == definition.host_support
        document, refs = capability_core_columns(definition)
        assert json.loads(document)["schema_version"] == "mc.capability_host_support.v1"
        assert refs == list(definition.secret_refs)
    assert summarize(
        PublishedDefinition(
            ref=ExactDefinitionRef(
                kind=DefinitionKind.SUBAGENT_PROFILE,
                logical_id="subagent.verifier",
                revision=1,
                digest=sha256_digest(subagent),
            ),
            definition=subagent,
            published_at=datetime(2026, 10, 7, tzinfo=UTC),
            published_by="test",
        )
    ).supported_profiles == (LaneProfile.DEEP_AGENTS, LaneProfile.CURSOR_LOCAL)


def test_new_definitions_export_json_schema() -> None:
    for model in (HookScriptDefinition, SubagentProfileDefinition, PluginDefinition):
        schema = model.model_json_schema()
        assert schema["properties"]["kind"]["const"] in {
            "hook_script",
            "subagent_profile",
            "plugin",
        }
    assert CapabilityHostSupport.model_json_schema()["properties"]["schema_version"]
