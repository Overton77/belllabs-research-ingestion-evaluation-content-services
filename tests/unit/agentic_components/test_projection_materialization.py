"""MP-03: pinned-bundle projection checks, plugin expansion, per-provider native formats and
workspace materialization receipts (SPEC-02 "Capabilities and hook scripts", V01)."""

from __future__ import annotations

import json
import tomllib
from pathlib import Path

import pytest

from mission_control.application.agentic_components.materialization import (
    MaterializationRejected,
    ProjectionMaterialization,
    materialize_projection,
    projection_digest,
    required_executables,
)
from mission_control.application.agentic_components.projections import render_host_files
from mission_control.domain.agentic_components.projection import (
    KERNEL_HOOK_SCRIPT,
    BundleFile,
    HostProjection,
    ProjectedFile,
    ProjectionError,
    ProjectionReport,
    ResolvedCapability,
)
from mission_control.domain.authoring.contracts import HookScriptDefinition, SkillDefinition
from mission_control.domain.capabilities.hooks import CLAUDE_SDK_CALLBACK_EVENTS, HookEvent
from mission_control.domain.capabilities.host_support import (
    HostSupportStatus,
    LaneProfile,
    all_profiles,
)
from tests.fixtures.projections import regen
from tests.fixtures.projections.rows import (
    INSTRUCTION,
    PACKET_INDEX,
    _entry,
    _pin,
    fixture_rows,
    hook_row,
    plugin_row,
    skill_row,
    tavily_row,
)

# --- Digest identity ----------------------------------------------------------------------------


@pytest.mark.parametrize("profile", list(LaneProfile))
def test_digest_identical_inputs_render_identical_bytes(profile: LaneProfile) -> None:
    first = render_host_files(fixture_rows(), profile, INSTRUCTION, PACKET_INDEX)
    second = render_host_files(fixture_rows(), profile, INSTRUCTION, PACKET_INDEX)
    assert [(f.path, f.mode, f.content) for f in first.files] == [
        (f.path, f.mode, f.content) for f in second.files
    ]
    assert projection_digest(first) == projection_digest(second)
    assert all(b"\r\n" not in item.content for item in first.files)
    changed = render_host_files(fixture_rows(), profile, INSTRUCTION + "\nOne more.", PACKET_INDEX)
    assert projection_digest(changed) != projection_digest(first)


def test_digest_covers_profile_modes_and_send_options() -> None:
    base = regen.project(LaneProfile.CURSOR_LOCAL)
    first = base.files[0]
    remode = base.model_copy(
        update={"files": (first.model_copy(update={"mode": 0o600}), *base.files[1:])}
    )
    assert projection_digest(remode) != projection_digest(base)
    resend = base.model_copy(update={"send_options": {"setting_sources": ["user"]}})
    assert projection_digest(resend) != projection_digest(base)
    relabel = base.model_copy(update={"profile": LaneProfile.CURSOR_CLOUD})
    assert projection_digest(relabel) != projection_digest(base)


# --- Pinned bundles and names -------------------------------------------------------------------


def _skill_with(content: bytes) -> ResolvedCapability:
    row = skill_row()
    files = (BundleFile(path="SKILL.md", content=content), *row.files[1:])
    data = row.definition.model_dump(mode="json")
    data["file_manifest"] = [_entry(item) for item in files]
    return row.model_copy(
        update={"files": files, "definition": SkillDefinition.model_validate(data)}
    )


def test_bundle_bytes_must_match_pinned_digests() -> None:
    row = skill_row()
    tampered = (BundleFile(path="SKILL.md", content=row.files[0].content + b"x"), *row.files[1:])
    with pytest.raises(ProjectionError, match="SKILL.md does not match its pinned digest"):
        render_host_files((row.model_copy(update={"files": tampered}),), "codex", INSTRUCTION)
    hook = hook_row()
    bad = (BundleFile(path="policy.py", content=b"print('swapped')\n"),)
    with pytest.raises(ProjectionError, match="policy.py does not match its pinned digest"):
        render_host_files((hook.model_copy(update={"files": bad}),), "cursor_local", INSTRUCTION)


def test_skill_frontmatter_name_and_description_are_checked() -> None:
    wrong = _skill_with(b"---\nname: other\ndescription: d\n---\n\nbody\n")
    with pytest.raises(ProjectionError, match="frontmatter name 'other' does not match"):
        render_host_files((wrong,), LaneProfile.CLAUDE_AGENT_SDK, INSTRUCTION)
    missing = _skill_with(b"---\nname: agent-browser\n---\n\nbody\n")
    with pytest.raises(ProjectionError, match="has no description"):
        render_host_files((missing,), LaneProfile.CODEX, INSTRUCTION)
    none = _skill_with(b"# no frontmatter\n")
    with pytest.raises(ProjectionError, match="frontmatter name None"):
        render_host_files((none,), LaneProfile.CURSOR_LOCAL, INSTRUCTION)


# --- Plugin expansion ---------------------------------------------------------------------------


def test_plugin_expansion_is_deterministic_and_skips_unqualified_optionals() -> None:
    projection = regen.project(LaneProfile.CURSOR_CLOUD)
    plugin = plugin_row()
    assert projection.report.plugin_expansions == (
        (plugin.pin.render(), (plugin.members[0].pin.render(), plugin.members[1].pin.render())),
    )
    assert projection.report.skipped_members == (plugin.members[2].pin.render(),)


def test_plugin_expansion_rejects_repeats_and_version_conflicts() -> None:
    with pytest.raises(ProjectionError, match="selected more than once: .*plugin.web-research"):
        render_host_files((plugin_row(), tavily_row()), LaneProfile.CODEX, INSTRUCTION)
    newer = tavily_row().model_copy(update={"pin": _pin("mcp.tavily", "0.3.0", "tavily-newer")})
    with pytest.raises(ProjectionError, match="conflicting versions of mcp.tavily"):
        render_host_files((plugin_row(), newer), LaneProfile.CODEX, INSTRUCTION)


def test_plugin_cycle_is_rejected_with_its_trail() -> None:
    # A row graph cannot be cyclic by construction, so build the self-reference by hand.
    outer = plugin_row()
    looped = outer.model_copy(update={"members": (*outer.members[:2], outer)})
    with pytest.raises(ProjectionError, match="plugin dependency cycle: plugin.web-research"):
        render_host_files((looped,), LaneProfile.CURSOR_LOCAL, INSTRUCTION)


# --- Host support and hooks ---------------------------------------------------------------------


def _hook(
    required: set[str],
    status: HostSupportStatus = HostSupportStatus.SUPPORTED,
    **support: HostSupportStatus,
) -> ResolvedCapability:
    row = hook_row()
    definition = HookScriptDefinition.model_validate(
        {
            **row.definition.model_dump(mode="json", exclude={"host_support"}),
            "events": ["before_model", "before_tool"],
            "required_events": sorted(required),
            "host_support": all_profiles(status, **support).model_dump(mode="json"),
        }
    )
    return row.model_copy(update={"definition": definition})


def test_rows_without_host_support_for_the_profile_fail() -> None:
    hook = _hook(set(), codex=HostSupportStatus.UNSUPPORTED)
    with pytest.raises(ProjectionError, match="declares no support for lane profile codex"):
        render_host_files((hook,), LaneProfile.CODEX, INSTRUCTION)


def test_unqualified_rows_are_reported_not_silently_projected() -> None:
    projection = render_host_files((skill_row(),), LaneProfile.CLAUDE_CLOUD, INSTRUCTION)
    assert any(
        "skill.agent-browser" in line and "unqualified" in line
        for line in projection.report.unqualified
    )
    # "Not yet proven on this host" is not "lost on this host".
    assert not any("unqualified" in line for line in projection.report.degraded)


def test_required_hook_event_without_native_hook_fails_with_pointed_error() -> None:
    # before_model has no Claude hook; declaring it supported is refused at authoring...
    with pytest.raises(ValueError, match="cannot run required"):
        _hook({"before_model"})
    # ...and a profile left unqualified cannot smuggle it into a projection.
    hook = _hook(
        {"before_model"}, HostSupportStatus.UNQUALIFIED, deep_agents=HostSupportStatus.SUPPORTED
    )
    with pytest.raises(
        ProjectionError,
        match="required hook event.* claude_agent_sdk: hook.mc-policy-template before_model",
    ):
        render_host_files((hook,), LaneProfile.CLAUDE_AGENT_SDK, INSTRUCTION)
    # Optional unsupported events stay reported.
    optional = render_host_files((_hook(set()),), LaneProfile.CODEX, INSTRUCTION)
    assert {(item.event, item.required) for item in optional.report.unsupported_on_lane} == {
        (HookEvent.BEFORE_MODEL, False)
    }


def test_claude_sdk_kernel_hooks_follow_the_python_callback_union() -> None:
    projection = regen.project(LaneProfile.CLAUDE_AGENT_SDK)
    callbacks = projection.send_options["hook_callbacks"]
    assert isinstance(callbacks, list) and callbacks
    events = {HookEvent(str(item["mc_event"])) for item in callbacks}
    assert events <= CLAUDE_SDK_CALLBACK_EVENTS
    assert HookEvent.SESSION_START not in events and HookEvent.AFTER_COMPACTION not in events
    settings = projection.file(".claude/settings.json").content.decode()
    assert KERNEL_HOOK_SCRIPT not in settings
    gap = [line for line in projection.report.degraded if line.startswith("mc.frame_capture")]
    assert gap and "session_start" in gap[0] and "after_compaction" in gap[0]


def test_kernel_hook_gaps_are_named_on_file_lanes() -> None:
    cursor = regen.project(LaneProfile.CURSOR_LOCAL).report.degraded
    assert (
        "mc.frame_capture: no native hook on cursor_local for before_model, after_compaction"
        in (cursor)
    )
    assert not any(
        line.startswith("mc.") for line in regen.project(LaneProfile.DEEP_AGENTS).report.degraded
    )


# --- Codex native formats -----------------------------------------------------------------------


def test_codex_custom_agents_are_toml_with_required_fields() -> None:
    projection = regen.project(LaneProfile.CODEX)
    assert not [
        path
        for path in projection.paths()
        if path.startswith(".codex/agents/") and not path.endswith(".toml")
    ]
    verifier = tomllib.loads(projection.file(".codex/agents/verifier.toml").content.decode())
    assert verifier == {
        "name": "verifier",
        "description": "Validates completed work. Use proactively after any task is done.",
        "sandbox_mode": "read-only",
        "developer_instructions": "You are a skeptical validator. Run the tests before agreeing.",
    }
    summarizer = tomllib.loads(projection.file(".codex/agents/summarizer.toml").content.decode())
    assert set(summarizer) == {"name", "description", "developer_instructions"}
    degraded = projection.report.degraded
    assert any("verifier" in line and "MCP server restriction" in line for line in degraded)
    assert any("summarizer" in line and "background" in line for line in degraded)


def test_codex_project_layer_entries_all_require_trust() -> None:
    trust = set(regen.project(LaneProfile.CODEX).report.requires_trust)
    assert {"mcp.tavily", "mcp.firecrawl", "subagent.verifier", "subagent.summarizer"} <= trust
    assert "hook.mc-policy-template" in trust


def test_codex_hook_overlay_renders_additional_context_limit() -> None:
    support = all_profiles().model_dump(mode="json")
    support["profiles"]["codex"]["overlay"] = {"additional_context_limit": 4000}
    row = hook_row()
    definition = HookScriptDefinition.model_validate(
        {
            **row.definition.model_dump(mode="json", exclude={"host_support"}),
            "host_support": support,
        }
    )
    projection = render_host_files(
        (row.model_copy(update={"definition": definition}),), "codex", INSTRUCTION, kernel_hooks=()
    )
    hooks = json.loads(projection.file(".codex/hooks.json").content)
    handlers = [h for groups in hooks["hooks"].values() for g in groups for h in g["hooks"]]
    assert handlers and all(h["additionalContextLimit"] == 4000 for h in handlers)


# --- Workspace materialization ------------------------------------------------------------------


def _materialize(
    projection: HostProjection, root: Path, expected_digest: str | None = None
) -> ProjectionMaterialization:
    return materialize_projection(
        projection, root, expected_digest=expected_digest, which=lambda _: "/bin/x"
    )


def test_materialize_writes_exact_bytes_and_is_idempotent(tmp_path: Path) -> None:
    projection = regen.project(LaneProfile.CLAUDE_AGENT_SDK)
    receipt = materialize_projection(projection, tmp_path, which=lambda _: "/usr/bin/x")
    assert receipt.projection_digest == projection_digest(projection)
    for item in projection.files:
        assert (tmp_path / item.path).read_bytes() == item.content
    assert [item.path for item in receipt.files] == list(projection.paths())
    again = materialize_projection(
        projection, tmp_path, expected_digest=receipt.projection_digest, which=lambda _: "/x"
    )
    assert again.unchanged == projection.paths()
    dumped = receipt.model_dump_json()
    assert str(tmp_path) not in dumped


def test_materialize_rejects_digest_drift_and_collisions(tmp_path: Path) -> None:
    projection = regen.project(LaneProfile.CODEX)
    with pytest.raises(MaterializationRejected, match="differs from the pinned"):
        _materialize(projection, tmp_path, expected_digest="sha256:" + "0" * 64)
    (tmp_path / "AGENTS.md").write_bytes(b"developer notes\n")
    with pytest.raises(
        MaterializationRejected, match="collides with different existing content: AGENTS.md"
    ):
        _materialize(projection, tmp_path)
    assert not (tmp_path / ".codex").exists(), "nothing is written when any path is refused"


@pytest.mark.parametrize("path", ["../escape.md", "/etc/passwd", "C:/Windows/x", "~/.claude/x"])
def test_materialize_rejects_traversal(tmp_path: Path, path: str) -> None:
    projection = HostProjection(
        profile=LaneProfile.CODEX,
        files=(ProjectedFile(path=path, content=b"x"),),
        report=ProjectionReport(),
    )
    with pytest.raises(MaterializationRejected, match="escapes the workspace"):
        _materialize(projection, tmp_path)


def test_materialize_rejects_symlinked_components(tmp_path: Path) -> None:
    outside = tmp_path / "outside"
    outside.mkdir()
    root = tmp_path / "root"
    root.mkdir()
    try:
        (root / ".claude").symlink_to(outside, target_is_directory=True)
    except (OSError, NotImplementedError):
        pytest.skip("this host cannot create symlinks")
    projection = render_host_files((skill_row(),), LaneProfile.CLAUDE_AGENT_SDK, INSTRUCTION)
    with pytest.raises(MaterializationRejected, match="crosses a symlink"):
        _materialize(projection, root)
    assert not any(outside.iterdir())


def test_required_executables_cover_interpreters_and_stdio_launchers(tmp_path: Path) -> None:
    rows = fixture_rows()
    needed = required_executables(rows, LaneProfile.CURSOR_LOCAL)
    assert needed["npx"] == ("mcp.tavily",)
    assert needed["agent-browser"] == ("mcp.agent-browser",)
    assert "hook.mc-policy-template" in needed["python"]
    # cursor_cloud: tavily is remote by overlay and the unqualified optional member is skipped
    cloud = required_executables(rows, LaneProfile.CURSOR_CLOUD)
    assert "npx" not in cloud and "agent-browser" not in cloud
    assert required_executables(rows, LaneProfile.CODEX_CLOUD) == {}
    projection = regen.project(LaneProfile.CURSOR_LOCAL)
    with pytest.raises(
        MaterializationRejected, match="launch executable\\(s\\) not found: agent-browser"
    ):
        materialize_projection(
            projection,
            tmp_path,
            executables=needed,
            which=lambda command: None if command == "agent-browser" else "/bin/" + command,
        )
    receipt = materialize_projection(
        projection,
        tmp_path,
        executables=needed,
        which=lambda command: None if command == "agent-browser" else "/bin/" + command,
        require_executables=False,
    )
    assert receipt.missing_executables == ("agent-browser",)
