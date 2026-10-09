"""Host Projection inputs and outputs (ADR-0023, SPEC-01 "Host projection", FT-A4).

A resolved capability row (exact pin, its Definition, and the bytes of its directory when it
has one) plus a lane profile, the mission instruction text, the Context Packet index and the
kernel hook set go in; the native files that lane's provider reads come out, together with a
report of everything that could not be projected and, for Deep Agents, in-process objects.
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from mission_control.domain.authoring.contracts import (
    HookScriptDefinition,
    MCPServerDefinition,
    PluginDefinition,
    SkillDefinition,
    SubagentProfileDefinition,
)
from mission_control.domain.capabilities.hooks import (
    KERNEL_HOOK_EVENTS,
    KERNEL_HOOK_IDS,
    HookEvent,
)
from mission_control.domain.capabilities.host_support import LaneProfile
from mission_control.domain.capabilities.pins import CapabilityPin

ProjectableDefinition = (
    SkillDefinition
    | MCPServerDefinition
    | HookScriptDefinition
    | SubagentProfileDefinition
    | PluginDefinition
)


class _Frozen(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, arbitrary_types_allowed=True)


class BundleFile(_Frozen):
    """One file of a capability directory, already verified against its manifest."""

    path: str = Field(min_length=1)
    content: bytes
    executable: bool = False

    @field_validator("path")
    @classmethod
    def relative_posix(cls, value: str) -> str:
        normalized = value.replace("\\", "/")
        parts = normalized.split("/")
        if normalized.startswith("/") or ".." in parts or "" in parts or "." in parts:
            raise ValueError("bundle file paths must be normalized relative paths")
        return normalized


class ResolvedCapability(_Frozen):
    """A catalog row resolved to an exact pin, ready to project."""

    pin: CapabilityPin
    definition: ProjectableDefinition = Field(discriminator="kind")
    files: tuple[BundleFile, ...] = ()
    # Plugin members, resolved, in manifest position order; empty for other kinds.
    members: tuple[ResolvedCapability, ...] = ()
    # Deep Agents skill source: Mission Control's own skills load before mission skills.
    scope: Literal["kernel", "mission"] = "mission"

    @model_validator(mode="after")
    def members_only_for_plugins(self) -> ResolvedCapability:
        is_plugin = isinstance(self.definition, PluginDefinition)
        if self.members and not is_plugin:
            raise ValueError("only plugin rows carry resolved members")
        if is_plugin:
            assert isinstance(self.definition, PluginDefinition)
            expected = [member.pin for member in self.definition.manifest.members]
            resolved = [member.pin for member in self.members]
            required = [m.pin for m in self.definition.manifest.members if not m.optional]
            if not set(required) <= set(resolved) or not set(resolved) <= set(expected):
                raise ValueError("plugin members must resolve exactly the manifest pins")
            order = [expected.index(pin) for pin in resolved]
            if order != sorted(order):
                raise ValueError("plugin members must be in manifest position order")
        paths = [item.path for item in self.files]
        if len(paths) != len(set(paths)):
            raise ValueError("bundle files must have unique paths")
        return self


class KernelHook(_Frozen):
    """A Mission Control kernel hook; always rendered first and fail-closed."""

    hook_id: str = Field(pattern=r"^mc\.[a-z_]+$")
    events: tuple[HookEvent, ...] = Field(min_length=1)


DEFAULT_KERNEL_HOOKS: tuple[KernelHook, ...] = tuple(
    KernelHook(hook_id=hook_id, events=KERNEL_HOOK_EVENTS[hook_id]) for hook_id in KERNEL_HOOK_IDS
)
KERNEL_HOOK_SCRIPT = ".mission/hooks/kernel.py"


class ProjectedFile(_Frozen):
    path: str = Field(min_length=1)
    content: bytes
    mode: int = Field(default=0o644, ge=0, le=0o777)


class UnsupportedHookEvent(_Frozen):
    hook_id: str
    event: HookEvent
    required: bool = False


class ProjectionReport(_Frozen):
    unsupported_on_lane: tuple[UnsupportedHookEvent, ...] = ()
    requires_trust: tuple[str, ...] = ()
    # Lost or narrowed on this host (the capability is supported but the lane cannot carry it).
    degraded: tuple[str, ...] = ()
    # Projected with `host_support=unqualified`: nothing is lost, but no materialization proof
    # exists for this host yet (MP-03). Kept apart from `degraded` so a release statement never
    # reads "not yet proven" as "lost".
    unqualified: tuple[str, ...] = ()
    overflow: tuple[str, ...] = ()
    skipped_members: tuple[str, ...] = ()
    plugin_expansions: tuple[tuple[str, tuple[str, ...]], ...] = ()


class InProcessProjection(_Frozen):
    """Deep Agents objects built in-process instead of files (SPEC-01 projection table)."""

    system_prompt: str
    memory: tuple[str, ...] = ()
    skills: tuple[str, ...] = ()
    subagents: tuple[dict[str, object], ...] = ()
    mcp_connections: dict[str, dict[str, object]] = Field(default_factory=dict)
    hook_middleware: tuple[dict[str, object], ...] = ()


class HostProjection(_Frozen):
    profile: LaneProfile
    files: tuple[ProjectedFile, ...]
    report: ProjectionReport
    in_process: InProcessProjection | None = None
    # Options a lane re-sends with every turn (Cursor does not persist them across resume).
    send_options: dict[str, object] = Field(default_factory=dict)

    def file(self, path: str) -> ProjectedFile:
        for item in self.files:
            if item.path == path:
                return item
        raise KeyError(path)

    def paths(self) -> tuple[str, ...]:
        return tuple(item.path for item in self.files)


class ProjectionError(ValueError):
    """Inputs cannot be projected safely (traversal, duplicate path, read-only seed)."""
