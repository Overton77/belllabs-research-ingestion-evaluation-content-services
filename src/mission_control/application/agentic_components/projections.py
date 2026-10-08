"""Host Projection: one deterministic renderer for every kind and lane profile (ADR-0023).

``render_host_files`` turns resolved capability rows into the native files each lane
profile's provider reads (SPEC-01 "Host projection"), plus Deep Agents in-process objects.
It is pure: the same inputs give the same bytes. Secrets appear only as the lane's native
reference syntax (``${env:NAME}`` on Cursor, ``${NAME}`` on Claude, ``env_vars`` on Codex,
unresolved ``*_refs`` on Deep Agents). Kernel hooks come first in every hook array and are
fail-closed on Cursor. Anything a profile cannot run is reported, never silently dropped.

``render_release_host_files`` is the older MCP-only renderer over Agentic Component
releases used by the materialization planner.
"""

from __future__ import annotations

import json
import re
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from hashlib import sha256
from pathlib import Path, PurePosixPath

from mission_control.domain.agentic_components.contracts import (
    AgentHost,
    AgenticComponentRelease,
    GeneratedFile,
)
from mission_control.domain.agentic_components.projection import (
    DEFAULT_KERNEL_HOOKS,
    KERNEL_HOOK_SCRIPT,
    HostProjection,
    InProcessProjection,
    KernelHook,
    ProjectedFile,
    ProjectionError,
    ProjectionReport,
    ResolvedCapability,
    UnsupportedHookEvent,
)
from mission_control.domain.authoring.contracts import (
    HookScriptDefinition,
    MCPServerDefinition,
    PluginDefinition,
    SkillDefinition,
    SubagentProfileDefinition,
)
from mission_control.domain.capabilities.hooks import HookEvent, NativeHook, native_hook
from mission_control.domain.capabilities.host_support import HostSupportStatus, LaneProfile

HOOK_RUNNER_SCRIPT = ".mission/hooks/run.py"
CODEX_AGENTS_MD_LIMIT = 32 * 1024
CURSOR_CLOUD_SUBAGENT_LIMIT = 20
_READ_ONLY_PREFIXES = ("inputs/", ".mission/inputs/", ".git/")
_HOOK_ROOT = ".mission/hooks"
_INTERPRETER_COMMAND = {"sh": "sh", "bash": "bash", "python": "python", "node": "node"}
_SKILL_ROOTS = {
    LaneProfile.CURSOR_LOCAL: ".cursor/skills",
    LaneProfile.CURSOR_CLOUD: ".cursor/skills",
    LaneProfile.CLAUDE_AGENT_SDK: ".claude/skills",
    LaneProfile.CODEX: ".agents/skills",
}
_EXEC = 0o755
_FILE = 0o644


# ------------------------------------------------------------------------------------------
# Public entry point
# ------------------------------------------------------------------------------------------


def render_host_files(
    rows: Sequence[ResolvedCapability],
    profile: LaneProfile | str,
    instruction: str,
    packet_index: str | None = None,
    kernel_hooks: Sequence[KernelHook] = DEFAULT_KERNEL_HOOKS,
    *,
    agent_server_bound: bool = False,
) -> HostProjection:
    """Project resolved capability rows into one lane profile's native configuration."""
    lane = LaneProfile(profile)
    state = _State(lane)
    expanded = _expand(rows, lane, state)
    skills = [row for row in expanded if isinstance(row.definition, SkillDefinition)]
    servers = [row for row in expanded if isinstance(row.definition, MCPServerDefinition)]
    hooks = [row for row in expanded if isinstance(row.definition, HookScriptDefinition)]
    agents = [row for row in expanded if isinstance(row.definition, SubagentProfileDefinition)]
    _check_unique_names(skills, servers, agents)
    for row in hooks:
        _report_hook_events(row, lane, state)
    instructions = _instruction_text(instruction, packet_index)

    if lane is LaneProfile.DEEP_AGENTS:
        in_process = _deep_agents(
            skills,
            servers,
            hooks,
            agents,
            kernel_hooks,
            state,
            instructions,
            system_prompt=instruction,
            agent_server_bound=agent_server_bound,
        )
        return state.result(in_process=in_process)

    _write_skills(skills, _SKILL_ROOTS[lane], state)
    _write_hook_scripts(hooks, state)
    if lane in {LaneProfile.CURSOR_LOCAL, LaneProfile.CURSOR_CLOUD}:
        _cursor(servers, hooks, agents, kernel_hooks, instructions, state)
    elif lane is LaneProfile.CLAUDE_AGENT_SDK:
        _claude(servers, hooks, agents, kernel_hooks, instructions, state)
    else:
        _codex(servers, hooks, agents, kernel_hooks, instructions, state)
    return state.result()


# ------------------------------------------------------------------------------------------
# Shared state, path hygiene and expansion
# ------------------------------------------------------------------------------------------


@dataclass
class _State:
    lane: LaneProfile
    files: dict[str, ProjectedFile] = field(default_factory=dict)
    unsupported: list[UnsupportedHookEvent] = field(default_factory=list)
    requires_trust: list[str] = field(default_factory=list)
    degraded: list[str] = field(default_factory=list)
    overflow: list[str] = field(default_factory=list)
    skipped: list[str] = field(default_factory=list)
    expansions: list[tuple[str, tuple[str, ...]]] = field(default_factory=list)
    send_options: dict[str, object] = field(default_factory=dict)

    def write(self, path: str, content: bytes | str, mode: int = _FILE) -> None:
        normalized = _safe_path(path)
        if normalized in self.files:
            raise ProjectionError(f"duplicate projected path: {normalized}")
        data = content.encode("utf-8") if isinstance(content, str) else content
        self.files[normalized] = ProjectedFile(path=normalized, content=data, mode=mode)

    def result(self, *, in_process: InProcessProjection | None = None) -> HostProjection:
        return HostProjection(
            profile=self.lane,
            files=tuple(self.files[path] for path in sorted(self.files)),
            report=ProjectionReport(
                unsupported_on_lane=tuple(self.unsupported),
                requires_trust=tuple(dict.fromkeys(self.requires_trust)),
                degraded=tuple(self.degraded),
                overflow=tuple(self.overflow),
                skipped_members=tuple(self.skipped),
                plugin_expansions=tuple(self.expansions),
            ),
            in_process=in_process,
            send_options=dict(self.send_options),
        )


def _safe_path(path: str) -> str:
    raw = path.replace("\\", "/")
    pure = PurePosixPath(raw)
    if pure.is_absolute() or ".." in pure.parts or raw.startswith("~"):
        raise ProjectionError(f"projected path escapes the workspace: {path}")
    normalized = pure.as_posix()
    if normalized in {"", "."}:
        raise ProjectionError("projected path is empty")
    if any(normalized.startswith(prefix) for prefix in _READ_ONLY_PREFIXES):
        raise ProjectionError(f"projected path writes into a read-only seed: {normalized}")
    return normalized


def _expand(
    rows: Iterable[ResolvedCapability], lane: LaneProfile, state: _State
) -> list[ResolvedCapability]:
    """Flatten plugins into their members in position order; skip unsupported optionals."""
    out: list[ResolvedCapability] = []
    for row in rows:
        definition = row.definition
        if isinstance(definition, PluginDefinition):
            optional = {m.pin for m in definition.manifest.members if m.optional}
            taken: list[str] = []
            for member in row.members:
                if member.pin in optional and not _runs_on(member, lane):
                    state.skipped.append(member.pin.render())
                    continue
                out.extend(_expand((member,), lane, state))
                taken.append(member.pin.render())
            state.expansions.append((row.pin.render(), tuple(taken)))
            continue
        out.append(row)
    return out


def _runs_on(row: ResolvedCapability, lane: LaneProfile) -> bool:
    host_support = getattr(row.definition, "host_support", None)
    return host_support is not None and host_support.status(lane) is HostSupportStatus.SUPPORTED


def _check_unique_names(
    skills: Sequence[ResolvedCapability],
    servers: Sequence[ResolvedCapability],
    agents: Sequence[ResolvedCapability],
) -> None:
    for label, names in (
        ("skill", [_skill_name(row) for row in skills]),
        ("MCP server", [_server_name(row) for row in servers]),
        ("subagent", [_agent(row).profile.name for row in agents]),
    ):
        duplicates = sorted({name for name in names if names.count(name) > 1})
        if duplicates:
            raise ProjectionError(f"duplicate {label} name(s): {', '.join(duplicates)}")


def _overlay(row: ResolvedCapability, lane: LaneProfile) -> dict[str, object]:
    host_support = getattr(row.definition, "host_support", None)
    return {} if host_support is None else host_support.overlay(lane)


def _instruction_text(instruction: str, packet_index: str | None) -> str:
    text = instruction.rstrip() + "\n"
    if packet_index:
        text += "\n## Mission context\n\n" + packet_index.rstrip() + "\n"
    return text


def _dumps(value: object) -> str:
    return json.dumps(value, indent=2, sort_keys=True, ensure_ascii=False) + "\n"


def _frontmatter(fields: Sequence[tuple[str, object]], body: str) -> str:
    lines = ["---"]
    for key, value in fields:
        if value is None:
            continue
        if isinstance(value, bool):
            rendered = "true" if value else "false"
        elif isinstance(value, list | tuple):
            rendered = json.dumps(list(value), ensure_ascii=False)
        else:
            rendered = json.dumps(str(value), ensure_ascii=False)
        lines.append(f"{key}: {rendered}")
    lines.append("---")
    return "\n".join(lines) + "\n\n" + body.rstrip() + "\n"


# ------------------------------------------------------------------------------------------
# Skills and hook script directories
# ------------------------------------------------------------------------------------------


def _skill_name(row: ResolvedCapability) -> str:
    definition = row.definition
    assert isinstance(definition, SkillDefinition)
    return definition.skill_name


def _skill_files(row: ResolvedCapability) -> list[tuple[str, bytes, int]]:
    definition = row.definition
    assert isinstance(definition, SkillDefinition)
    if not row.files:
        raise ProjectionError(f"skill bundle {row.pin.render()} has no resolved files")
    names = {item.path for item in row.files}
    if "SKILL.md" not in names:
        raise ProjectionError(f"skill bundle {row.pin.render()} has no SKILL.md")
    expected = {entry.path for entry in definition.file_manifest}
    if names != expected:
        raise ProjectionError(f"skill bundle {row.pin.render()} files differ from its manifest")
    return [
        (item.path, item.content, _EXEC if item.executable else _FILE)
        for item in sorted(row.files, key=lambda item: item.path)
    ]


def _write_skills(skills: Sequence[ResolvedCapability], root: str, state: _State) -> None:
    for row in skills:
        name = _skill_name(row)
        for path, content, mode in _skill_files(row):
            state.write(f"{root}/{name}/{path}", content, mode)


def _hook_dir(row: ResolvedCapability) -> str:
    return f"{_HOOK_ROOT}/{_hook_slug(row.definition.logical_id)}"


def _hook_slug(logical_id: str) -> str:
    return re.sub(r"[^a-z0-9._-]", "-", logical_id)


HOOK_RUNNER_SOURCE = Path(__file__).with_name("hook_runner.py")


def _write_hook_scripts(
    hooks: Sequence[ResolvedCapability], state: _State, *, runner: bool = True
) -> None:
    """Hook directories, their ``hook.json`` descriptors and (file lanes) the hook runner."""
    if hooks and runner:
        # Normalized to LF so the projection is byte-identical on every checkout.
        source = HOOK_RUNNER_SOURCE.read_bytes().replace(b"\r\n", b"\n")
        state.write(HOOK_RUNNER_SCRIPT, source, _EXEC)
    for row in hooks:
        definition = row.definition
        assert isinstance(definition, HookScriptDefinition)
        expected = {entry.path for entry in definition.file_manifest}
        names = {item.path for item in row.files}
        if names != expected:
            raise ProjectionError(f"hook script {row.pin.render()} files differ from its manifest")
        for item in sorted(row.files, key=lambda item: item.path):
            executable = item.executable or item.path == definition.entrypoint
            state.write(
                f"{_hook_dir(row)}/{item.path}", item.content, _EXEC if executable else _FILE
            )
        if runner:
            descriptor = {
                "schema_version": "mc.hook_descriptor.v1",
                "hook_id": definition.logical_id,
                "pin": row.pin.render(),
                "lane_profile": state.lane.value,
                "events": [event.value for event in definition.events],
                "interpreter": definition.interpreter.value,
                "entrypoint": definition.entrypoint,
                "timeout_seconds": definition.timeout_seconds,
                "fail_closed": definition.fail_closed,
                "callback": definition.callback.value,
            }
            state.write(f"{_hook_dir(row)}/hook.json", _dumps(descriptor))


def _report_hook_events(row: ResolvedCapability, lane: LaneProfile, state: _State) -> None:
    definition = row.definition
    assert isinstance(definition, HookScriptDefinition)
    for event in definition.events:
        if native_hook(lane, event) is None:
            state.unsupported.append(
                UnsupportedHookEvent(
                    hook_id=definition.logical_id,
                    event=event,
                    required=event in definition.required_events,
                )
            )


def _catalog_hook_command(row: ResolvedCapability, event: HookEvent) -> list[str]:
    """Exec form: the lane's hook runner adapts native stdin to ``mc.hook_input.v1``."""
    return ["python", HOOK_RUNNER_SCRIPT, _hook_slug(row.definition.logical_id), event.value]


def _kernel_hook_command(hook: KernelHook, event: HookEvent) -> list[str]:
    return ["python", KERNEL_HOOK_SCRIPT, event.value, hook.hook_id]


@dataclass(frozen=True)
class _HookEntry:
    hook_id: str
    event: HookEvent
    native: NativeHook
    command: list[str]
    timeout: int
    fail_closed: bool
    matcher: str | None
    overlay: dict[str, object]
    kernel: bool


def _hook_entries(
    lane: LaneProfile,
    kernel_hooks: Sequence[KernelHook],
    hooks: Sequence[ResolvedCapability],
) -> list[_HookEntry]:
    """Kernel hooks first in their fixed order, then catalog hooks in row order."""
    entries: list[_HookEntry] = []
    for kernel in kernel_hooks:
        for event in kernel.events:
            native = native_hook(lane, event)
            if native is None:
                continue
            entries.append(
                _HookEntry(
                    hook_id=kernel.hook_id,
                    event=event,
                    native=native,
                    command=_kernel_hook_command(kernel, event),
                    timeout=30,
                    fail_closed=True,
                    matcher=native.matcher,
                    overlay={},
                    kernel=True,
                )
            )
    for row in hooks:
        definition = row.definition
        assert isinstance(definition, HookScriptDefinition)
        overlay = _overlay(row, lane)
        for event in definition.events:
            native = native_hook(lane, event)
            if native is None:
                continue
            matcher = overlay.get("matcher", native.matcher or definition.matcher)
            entries.append(
                _HookEntry(
                    hook_id=definition.logical_id,
                    event=event,
                    native=native,
                    command=_catalog_hook_command(row, event),
                    timeout=definition.timeout_seconds,
                    fail_closed=definition.fail_closed,
                    matcher=None if matcher is None else str(matcher),
                    overlay=overlay,
                    kernel=False,
                )
            )
    return entries


def _shell_join(parts: Sequence[str]) -> str:
    return " ".join(
        part if re.fullmatch(r"[A-Za-z0-9_./:@=-]+", part) else json.dumps(part) for part in parts
    )


# ------------------------------------------------------------------------------------------
# MCP servers
# ------------------------------------------------------------------------------------------


def _server_name(row: ResolvedCapability) -> str:
    logical = row.definition.logical_id
    name = logical.removeprefix("mcp.")
    return re.sub(r"[^A-Za-z0-9_-]", "-", name)


@dataclass(frozen=True)
class _Launch:
    transport: str
    command: str | None
    args: tuple[str, ...]
    url: str | None
    env: dict[str, str]
    env_refs: dict[str, str]
    header_refs: dict[str, str]
    tools: tuple[str, ...]


def _launch(row: ResolvedCapability, lane: LaneProfile) -> _Launch:
    definition = row.definition
    assert isinstance(definition, MCPServerDefinition)
    overlay = _overlay(row, lane)
    transport = str(overlay.get("transport", definition.transport))
    header_refs = dict(definition.header_refs)
    overlay_headers = overlay.get("header_refs")
    if isinstance(overlay_headers, dict):
        header_refs.update({str(k): str(v) for k, v in overlay_headers.items()})
    if transport == "stdio":
        template = definition.launch_template
        if not template:
            raise ProjectionError(f"{row.pin.render()} has no stdio launch template")
        command, args, url = template[0], tuple(template[1:]), None
    else:
        candidate = overlay.get("url", definition.endpoint)
        if not isinstance(candidate, str) or not candidate:
            raise ProjectionError(f"{row.pin.render()} has no endpoint for {lane.value}")
        command, args, url = None, (), candidate
    exposed = definition.exposed_tools()
    tools = tuple(sorted(tool.name for tool in exposed)) or tuple(sorted(definition.allowed_tools))
    return _Launch(
        transport=transport,
        command=command,
        args=args,
        url=url,
        env=dict(definition.env),
        env_refs=dict(definition.env_refs),
        header_refs=header_refs,
        tools=tools,
    )


def _header_value(header: str, ref: str, template: str) -> str:
    reference = template.format(name=ref)
    return f"Bearer {reference}" if header.lower() == "authorization" else reference


def _json_mcp_entry(
    row: ResolvedCapability, lane: LaneProfile, *, cursor: bool
) -> dict[str, object]:
    launch = _launch(row, lane)
    template = "${{env:{name}}}" if cursor else "${{{name}}}"
    entry: dict[str, object]
    if launch.transport == "stdio":
        entry = {"type": "stdio", "command": launch.command, "args": list(launch.args)}
        env = dict(launch.env)
        env.update({key: template.format(name=ref) for key, ref in launch.env_refs.items()})
        if env:
            entry["env"] = env
    else:
        entry = {
            "type": "http" if launch.transport == "streamable_http" else "sse",
            "url": launch.url,
        }
        if launch.header_refs:
            entry["headers"] = {
                header: _header_value(header, ref, template)
                for header, ref in launch.header_refs.items()
            }
    definition = row.definition
    assert isinstance(definition, MCPServerDefinition)
    if cursor and definition.oauth is not None:
        auth: dict[str, object] = {
            "CLIENT_ID": template.format(name=definition.oauth.client_id_ref)
        }
        if definition.oauth.client_secret_ref:
            auth["CLIENT_SECRET"] = template.format(name=definition.oauth.client_secret_ref)
        if definition.oauth.scopes:
            auth["scopes"] = list(definition.oauth.scopes)
        entry["auth"] = auth
    return entry


def _toml(value: object) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, list | tuple):
        return "[" + ", ".join(_toml(item) for item in value) + "]"
    if isinstance(value, dict):
        return (
            "{ " + ", ".join(f"{json.dumps(str(k))} = {_toml(v)}" for k, v in value.items()) + " }"
        )
    return json.dumps(str(value), ensure_ascii=False)


def _codex_config(servers: Sequence[ResolvedCapability], lane: LaneProfile) -> str:
    lines = ["# Generated by Mission Control Host Projection (SPEC-01). Do not edit."]
    for row in sorted(servers, key=_server_name):
        launch = _launch(row, lane)
        lines.extend(("", f"[mcp_servers.{json.dumps(_server_name(row))}]"))
        if launch.transport == "stdio":
            lines.append(f"command = {_toml(launch.command)}")
            if launch.args:
                lines.append(f"args = {_toml(list(launch.args))}")
            if launch.env:
                lines.append(f"env = {_toml(dict(sorted(launch.env.items())))}")
            names = sorted(set(launch.env_refs) | set(launch.env_refs.values()))
            # Codex forwards variables by name only; there is no string interpolation, so the
            # environment name must equal the secret ref (renamed refs are forwarded as-is).
            if names:
                lines.append(f"env_vars = {_toml(names)}")
        else:
            lines.append(f"url = {_toml(launch.url)}")
            auth = {h: r for h, r in launch.header_refs.items() if h.lower() == "authorization"}
            other = {h: r for h, r in launch.header_refs.items() if h.lower() != "authorization"}
            if auth:
                lines.append(f"bearer_token_env_var = {_toml(next(iter(auth.values())))}")
            if other:
                lines.append(f"env_http_headers = {_toml(dict(sorted(other.items())))}")
        lines.append("enabled = true")
        if launch.tools:
            lines.append(f"enabled_tools = {_toml(list(launch.tools))}")
    return "\n".join(lines) + "\n"


# ------------------------------------------------------------------------------------------
# Subagents
# ------------------------------------------------------------------------------------------


def _agent(row: ResolvedCapability) -> SubagentProfileDefinition:
    definition = row.definition
    assert isinstance(definition, SubagentProfileDefinition)
    return definition


def _agent_prompt(row: ResolvedCapability) -> str:
    profile = _agent(row).profile
    if profile.prompt is not None:
        return profile.prompt
    for item in row.files:
        if item.path in {"prompt.md", "PROMPT.md"}:
            return item.content.decode("utf-8")
    raise ProjectionError(f"subagent {row.pin.render()} prompt bundle is not resolved")


def _agent_model(row: ResolvedCapability, lane: LaneProfile) -> str:
    overlay = _overlay(row, lane)
    return str(overlay.get("model", _agent(row).profile.model))


def _cursor_agent_md(row: ResolvedCapability, lane: LaneProfile) -> str:
    profile = _agent(row).profile
    overlay = _overlay(row, lane)
    return _frontmatter(
        (
            ("name", profile.name),
            ("description", profile.description),
            ("model", _agent_model(row, lane)),
            ("readonly", bool(overlay.get("readonly", profile.readonly))),
            ("is_background", bool(overlay.get("is_background", profile.background))),
        ),
        _agent_prompt(row),
    )


def _claude_agent_md(row: ResolvedCapability, lane: LaneProfile, servers: dict[str, str]) -> str:
    profile = _agent(row).profile
    overlay = _overlay(row, lane)
    mcp = [servers.get(ref, ref) for ref in profile.mcp_servers]
    permission = overlay.get("permission_mode", "plan" if profile.readonly else None)
    return _frontmatter(
        (
            ("name", profile.name),
            ("description", profile.description),
            ("tools", ", ".join(profile.tools) if profile.tools else None),
            ("model", _agent_model(row, lane)),
            ("permissionMode", permission),
            ("skills", list(profile.skills) if profile.skills else None),
            ("mcpServers", mcp or None),
            ("background", profile.background or None),
        ),
        _agent_prompt(row),
    )


# ------------------------------------------------------------------------------------------
# Profiles
# ------------------------------------------------------------------------------------------


def _cursor(
    servers: Sequence[ResolvedCapability],
    hooks: Sequence[ResolvedCapability],
    agents: Sequence[ResolvedCapability],
    kernel_hooks: Sequence[KernelHook],
    instructions: str,
    state: _State,
) -> None:
    lane = state.lane
    state.write("AGENTS.md", instructions)
    state.write(
        ".cursor/rules/mc-mission.mdc",
        "---\ndescription: Mission Control mission instructions\nalwaysApply: true\n---\n\n"
        + instructions,
    )
    if servers:
        state.write(
            ".cursor/mcp.json",
            _dumps(
                {
                    "mcpServers": {
                        _server_name(row): _json_mcp_entry(row, lane, cursor=True)
                        for row in servers
                    }
                }
            ),
        )
    events: dict[str, list[dict[str, object]]] = {}
    for entry in _hook_entries(lane, kernel_hooks, hooks):
        item: dict[str, object] = {
            "type": "command",
            "command": _shell_join(entry.command),
            "timeout": entry.timeout,
            "failClosed": entry.fail_closed,
        }
        if entry.matcher:
            item["matcher"] = entry.matcher
        events.setdefault(entry.native.native_event, []).append(item)
    if events:
        state.write(".cursor/hooks.json", _dumps({"version": 1, "hooks": events}))
    inline: list[dict[str, object]] = []
    for row in agents:
        profile = _agent(row).profile
        state.write(f".cursor/agents/{profile.name}.md", _cursor_agent_md(row, lane))
        overlay = _overlay(row, lane)
        needs_file = bool(overlay.get("readonly", profile.readonly)) or bool(
            overlay.get("is_background", profile.background)
        )
        if not needs_file and not _agent(row).secret_refs:
            inline.append(
                {
                    "name": profile.name,
                    "description": profile.description,
                    "prompt": _agent_prompt(row),
                    "model": _agent_model(row, lane),
                }
            )
    if lane is LaneProfile.CURSOR_CLOUD and len(agents) > CURSOR_CLOUD_SUBAGENT_LIMIT:
        state.overflow.append(
            f"cursor_cloud customSubagents limit {CURSOR_CLOUD_SUBAGENT_LIMIT} exceeded "
            f"({len(agents)} subagent profiles)"
        )
    state.send_options["setting_sources"] = ["project"]
    if inline:
        state.send_options["agents"] = inline


def _claude(
    servers: Sequence[ResolvedCapability],
    hooks: Sequence[ResolvedCapability],
    agents: Sequence[ResolvedCapability],
    kernel_hooks: Sequence[KernelHook],
    instructions: str,
    state: _State,
) -> None:
    lane = state.lane
    state.write("CLAUDE.md", instructions)
    if servers:
        state.write(
            ".mcp.json",
            _dumps(
                {
                    "mcpServers": {
                        _server_name(row): _json_mcp_entry(row, lane, cursor=False)
                        for row in servers
                    }
                }
            ),
        )
    settings = _matcher_hooks(lane, kernel_hooks, hooks, exec_form=True)
    if settings:
        state.write(".claude/settings.json", _dumps({"hooks": settings}))
    names = {row.pin.capability_id: _server_name(row) for row in servers}
    for row in agents:
        profile = _agent(row).profile
        state.write(f".claude/agents/{profile.name}.md", _claude_agent_md(row, lane, names))


def _codex(
    servers: Sequence[ResolvedCapability],
    hooks: Sequence[ResolvedCapability],
    agents: Sequence[ResolvedCapability],
    kernel_hooks: Sequence[KernelHook],
    instructions: str,
    state: _State,
) -> None:
    lane = state.lane
    data = instructions.encode("utf-8")
    if len(data) > CODEX_AGENTS_MD_LIMIT:
        state.overflow.append(
            f"AGENTS.md is {len(data)} bytes; Codex reads at most {CODEX_AGENTS_MD_LIMIT}"
        )
        marker = b"\n\n[truncated by Mission Control: Codex 32 KiB AGENTS.md cap]\n"
        data = data[: CODEX_AGENTS_MD_LIMIT - len(marker)].decode("utf-8", "ignore").encode()
        data += marker
    state.write("AGENTS.md", data)
    if servers:
        state.write(".codex/config.toml", _codex_config(servers, lane))
    settings = _matcher_hooks(lane, kernel_hooks, hooks, exec_form=False)
    if settings:
        state.write(".codex/hooks.json", _dumps({"hooks": settings}))
        state.requires_trust.extend(
            entry.hook_id for entry in _hook_entries(lane, kernel_hooks, hooks)
        )
    for row in agents:
        profile = _agent(row).profile
        state.write(f".codex/agents/{profile.name}.md", _cursor_agent_md(row, lane))


def _matcher_hooks(
    lane: LaneProfile,
    kernel_hooks: Sequence[KernelHook],
    hooks: Sequence[ResolvedCapability],
    *,
    exec_form: bool,
) -> dict[str, list[dict[str, object]]]:
    """Claude Code and Codex shape: ``{Event: [{matcher, hooks: [{type, command, ...}]}]}``."""
    out: dict[str, list[dict[str, object]]] = {}
    for entry in _hook_entries(lane, kernel_hooks, hooks):
        handler: dict[str, object] = {"type": "command"}
        if exec_form:
            handler["command"] = entry.command[0]
            handler["args"] = entry.command[1:]
            handler["timeout"] = entry.timeout
            if entry.overlay.get("async") is True:
                handler["async"] = True
        else:
            handler["command"] = _shell_join(entry.command)
            handler["timeout"] = entry.timeout
        group: dict[str, object] = {"hooks": [handler]}
        if entry.matcher:
            group["matcher"] = entry.matcher
        out.setdefault(entry.native.native_event, []).append(group)
    return out


def _deep_agents(
    skills: Sequence[ResolvedCapability],
    servers: Sequence[ResolvedCapability],
    hooks: Sequence[ResolvedCapability],
    agents: Sequence[ResolvedCapability],
    kernel_hooks: Sequence[KernelHook],
    state: _State,
    instructions: str,
    *,
    system_prompt: str,
    agent_server_bound: bool,
) -> InProcessProjection:
    lane = state.lane
    state.write("memory/AGENTS.md", instructions)
    scopes: list[str] = []
    for scope in ("kernel", "mission"):
        scoped = [row for row in skills if row.scope == scope]
        if scoped:
            scopes.append(f"/skills/{scope}/")
            for row in scoped:
                for path, content, mode in _skill_files(row):
                    state.write(f"skills/{scope}/{_skill_name(row)}/{path}", content, mode)
    _write_hook_scripts(hooks, state, runner=False)
    connections: dict[str, dict[str, object]] = {}
    for row in servers:
        launch = _launch(row, lane)
        connection: dict[str, object] = {"transport": launch.transport}
        if launch.transport == "stdio":
            connection.update(command=launch.command, args=list(launch.args))
            if launch.env:
                connection["env"] = dict(launch.env)
            if launch.env_refs:
                connection["env_refs"] = dict(launch.env_refs)
        else:
            connection["url"] = launch.url
            if launch.header_refs:
                connection["header_refs"] = dict(launch.header_refs)
        if launch.tools:
            connection["tool_allowlist"] = list(launch.tools)
        connections[_server_name(row)] = connection
    middleware: list[dict[str, object]] = [
        {"hook_id": hook.hook_id, "kernel": True, "events": [e.value for e in hook.events]}
        for hook in kernel_hooks
    ]
    for row in hooks:
        definition = row.definition
        assert isinstance(definition, HookScriptDefinition)
        supported = [e.value for e in definition.events if native_hook(lane, e) is not None]
        middleware.append(
            {
                "hook_id": definition.logical_id,
                "kernel": False,
                "events": supported,
                "matcher": _overlay(row, lane).get("matcher", definition.matcher),
                "timeout_seconds": definition.timeout_seconds,
                "fail_closed": definition.fail_closed,
                "interpreter": _INTERPRETER_COMMAND[definition.interpreter.value],
                "entrypoint": f"{_hook_dir(row)}/{definition.entrypoint}",
                "callback": definition.callback.value,
            }
        )
    subagents: list[dict[str, object]] = []
    for row in agents:
        profile = _agent(row).profile
        agent: dict[str, object] = {
            "name": profile.name,
            "description": profile.description,
            "system_prompt": _agent_prompt(row),
        }
        model = _agent_model(row, lane)
        if model != "inherit":
            agent["model"] = model
        if profile.tools:
            agent["tools"] = list(profile.tools)
        if profile.skills:
            agent["skills"] = list(profile.skills)
        if profile.interrupt_on:
            agent["interrupt_on"] = dict.fromkeys(profile.interrupt_on, True)
        if profile.readonly:
            agent["permissions"] = [
                {"operations": ["write", "edit", "execute"], "mode": "deny", "path": "/"}
            ]
        if profile.background:
            if agent_server_bound:
                agent["background"] = True
            else:
                state.degraded.append(
                    f"subagent {profile.name}: background requested without an Agent Server; "
                    "projected synchronous"
                )
        subagents.append(agent)
    return InProcessProjection(
        system_prompt=system_prompt,
        memory=("/memory/AGENTS.md",),
        skills=tuple(scopes),
        subagents=tuple(subagents),
        mcp_connections=connections,
        hook_middleware=tuple(middleware),
    )


# ------------------------------------------------------------------------------------------
# Agentic Component releases (MCP-only renderer used by the materialization planner)
# ------------------------------------------------------------------------------------------


def render_release_host_files(
    host: AgentHost,
    releases: tuple[AgenticComponentRelease, ...],
) -> tuple[GeneratedFile, ...]:
    mcp_releases = tuple(release for release in releases if release.mcp is not None)
    if not mcp_releases:
        return ()
    if host == AgentHost.CODEX:
        return (_generated(".codex/config.toml", "application/toml", _render_codex(mcp_releases)),)
    if host == AgentHost.CURSOR:
        return (
            _generated(
                ".cursor/mcp.json",
                "application/json",
                _render_json_host(mcp_releases, cursor=True),
            ),
        )
    if host == AgentHost.CLAUDE_CODE:
        return (
            _generated(
                ".mcp.json",
                "application/json",
                _render_json_host(mcp_releases, cursor=False),
            ),
        )
    return ()


def skill_target_path(host: AgentHost, skill_name: str) -> str:
    roots = {
        AgentHost.CODEX: ".agents/skills",
        AgentHost.CURSOR: ".cursor/skills",
        AgentHost.CLAUDE_CODE: ".claude/skills",
        AgentHost.AGENT_FRAMEWORK: ".agents/skills",
    }
    return f"{roots[host]}/{skill_name}"


def _render_json_host(
    releases: tuple[AgenticComponentRelease, ...],
    *,
    cursor: bool,
) -> str:
    servers: dict[str, object] = {}
    for release in releases:
        binding = release.mcp
        assert binding is not None
        if binding.transport == "stdio":
            entry: dict[str, object] = {
                "command": binding.command,
                "args": list(binding.arguments),
            }
            if not cursor:
                entry["type"] = "stdio"
            if binding.secret_environment:
                entry["env"] = {
                    item.environment_name: f"${{{item.environment_name}}}"
                    for item in binding.secret_environment
                }
        else:
            entry = {"url": binding.url}
            if not cursor:
                entry["type"] = "http" if binding.transport == "streamable_http" else "sse"
        servers[binding.server_name] = entry
    return json.dumps({"mcpServers": servers}, indent=2, sort_keys=True) + "\n"


def _render_codex(releases: tuple[AgenticComponentRelease, ...]) -> str:
    lines = ["# Generated by the BellLabs Agentic Components harness."]
    for release in sorted(releases, key=lambda item: item.mcp.server_name if item.mcp else ""):
        binding = release.mcp
        assert binding is not None
        name = binding.server_name.replace('"', '\\"')
        lines.extend(("", f'[mcp_servers."{name}"]'))
        if binding.transport == "stdio":
            lines.append(f"command = {json.dumps(binding.command)}")
            if binding.arguments:
                args = ", ".join(json.dumps(item) for item in binding.arguments)
                lines.append(f"args = [{args}]")
            if binding.working_directory:
                lines.append(f"cwd = {json.dumps(binding.working_directory)}")
            if binding.secret_environment:
                values = ", ".join(
                    json.dumps(item.environment_name) for item in binding.secret_environment
                )
                lines.append(f"env_vars = [{values}]")
        else:
            lines.append(f"url = {json.dumps(binding.url)}")
        lines.append("enabled = true")
        allowed = ", ".join(json.dumps(item) for item in sorted(binding.allowed_tools))
        lines.append(f"enabled_tools = [{allowed}]")
    return "\n".join(lines) + "\n"


def _generated(path: str, media_type: str, content: str) -> GeneratedFile:
    return GeneratedFile(
        path=path,
        media_type=media_type,
        content=content,
        digest=f"sha256:{sha256(content.encode()).hexdigest()}",
    )
