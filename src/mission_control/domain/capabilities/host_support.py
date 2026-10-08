"""``mc.capability_host_support.v1``: which lane profiles can run a capability (ADR-0023).

Every agent-composition row declares, per lane profile, ``supported``, ``unsupported`` or
``unqualified`` (declared but without proof), plus an optional overlay holding only the
fields that have no provider-neutral meaning. The compiler reads the status to validate a
node's lane; the Host Projection reads the overlay of the profile it renders.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from enum import StrEnum
from typing import Final, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

HOST_SUPPORT_SCHEMA: Final = "mc.capability_host_support.v1"


class LaneProfile(StrEnum):
    DEEP_AGENTS = "deep_agents"
    CURSOR_LOCAL = "cursor_local"
    CURSOR_CLOUD = "cursor_cloud"
    CLAUDE_AGENT_SDK = "claude_agent_sdk"
    CODEX = "codex"


class HostSupportStatus(StrEnum):
    SUPPORTED = "supported"
    UNSUPPORTED = "unsupported"
    UNQUALIFIED = "unqualified"


class AgentCapabilityKind(StrEnum):
    """The SQL-level agent-composition kinds that carry a host-support matrix."""

    SKILL_BUNDLE = "skill_bundle"
    MCP_SERVER = "mcp_server"
    MCP_TOOL = "mcp_tool"
    HOOK_SCRIPT = "hook_script"
    SUBAGENT_PROFILE = "subagent_profile"
    PLUGIN = "plugin"


_CURSOR = (LaneProfile.CURSOR_LOCAL, LaneProfile.CURSOR_CLOUD)

# Overlay keys allowed per kind and profile. Anything else is a provider-neutral field and
# belongs in the core body, so an unknown key is rejected rather than silently carried.
_MCP_OVERLAY = frozenset({"transport", "url", "url_ref", "header_refs"})
OVERLAY_KEYS: Mapping[AgentCapabilityKind, Mapping[LaneProfile, frozenset[str]]] = {
    AgentCapabilityKind.SKILL_BUNDLE: {profile: frozenset() for profile in LaneProfile},
    AgentCapabilityKind.MCP_TOOL: {profile: frozenset() for profile in LaneProfile},
    AgentCapabilityKind.PLUGIN: {profile: frozenset() for profile in LaneProfile},
    AgentCapabilityKind.MCP_SERVER: dict.fromkeys(LaneProfile, _MCP_OVERLAY),
    AgentCapabilityKind.HOOK_SCRIPT: {
        LaneProfile.DEEP_AGENTS: frozenset({"matcher"}),
        LaneProfile.CURSOR_LOCAL: frozenset({"matcher"}),
        LaneProfile.CURSOR_CLOUD: frozenset({"matcher"}),
        LaneProfile.CLAUDE_AGENT_SDK: frozenset({"matcher", "async"}),
        LaneProfile.CODEX: frozenset({"matcher", "additional_context_limit"}),
    },
    AgentCapabilityKind.SUBAGENT_PROFILE: {
        LaneProfile.DEEP_AGENTS: frozenset({"model"}),
        LaneProfile.CURSOR_LOCAL: frozenset({"model", "readonly", "is_background"}),
        LaneProfile.CURSOR_CLOUD: frozenset({"model", "readonly", "is_background"}),
        LaneProfile.CLAUDE_AGENT_SDK: frozenset({"model", "permission_mode"}),
        LaneProfile.CODEX: frozenset({"model"}),
    },
}


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class HostProfileSupport(_Strict):
    status: HostSupportStatus
    overlay: dict[str, object] = Field(default_factory=dict)
    evidence_ref: str | None = Field(default=None, min_length=1)


class CapabilityHostSupport(_Strict):
    schema_version: Literal["mc.capability_host_support.v1"] = HOST_SUPPORT_SCHEMA
    profiles: dict[LaneProfile, HostProfileSupport] = Field(default_factory=dict)

    @model_validator(mode="before")
    @classmethod
    def reject_unknown_profiles(cls, value: object) -> object:
        if isinstance(value, Mapping):
            profiles = value.get("profiles")
            if isinstance(profiles, Mapping):
                known = {profile.value for profile in LaneProfile}
                unknown = sorted(str(key) for key in profiles if str(key) not in known)
                if unknown:
                    raise ValueError(f"unknown lane profile id(s): {', '.join(unknown)}")
        return value

    def status(self, profile: LaneProfile | str) -> HostSupportStatus:
        entry = self.profiles.get(LaneProfile(profile))
        return HostSupportStatus.UNSUPPORTED if entry is None else entry.status

    def supports(self, profile: LaneProfile | str) -> bool:
        return self.status(profile) == HostSupportStatus.SUPPORTED

    def supported_profiles(self) -> tuple[LaneProfile, ...]:
        return tuple(profile for profile in LaneProfile if self.supports(profile))

    def overlay(self, profile: LaneProfile | str) -> dict[str, object]:
        entry = self.profiles.get(LaneProfile(profile))
        return {} if entry is None else dict(entry.overlay)

    def validate_for(self, kind: AgentCapabilityKind | str) -> CapabilityHostSupport:
        """Reject overlay keys that the kind does not define for that profile."""
        allowed = OVERLAY_KEYS[AgentCapabilityKind(kind)]
        for profile, entry in self.profiles.items():
            unknown = sorted(set(entry.overlay) - allowed[profile])
            if unknown:
                raise ValueError(
                    f"{AgentCapabilityKind(kind).value} overlay for {profile.value} has "
                    f"unknown key(s): {', '.join(unknown)}"
                )
        return self


def all_profiles(
    status: HostSupportStatus = HostSupportStatus.SUPPORTED,
    **overrides: HostSupportStatus,
) -> CapabilityHostSupport:
    """A matrix with every profile at ``status`` except the named overrides."""
    unknown = set(overrides) - {profile.value for profile in LaneProfile}
    if unknown:
        raise ValueError(f"unknown lane profile id(s): {', '.join(sorted(unknown))}")
    return CapabilityHostSupport(
        profiles={
            profile: HostProfileSupport(status=overrides.get(profile.value, status))
            for profile in LaneProfile
        }
    )


_RANK = {
    HostSupportStatus.SUPPORTED: 0,
    HostSupportStatus.UNQUALIFIED: 1,
    HostSupportStatus.UNSUPPORTED: 2,
}


def intersect_host_support(
    members: Iterable[tuple[CapabilityHostSupport, bool]],
) -> CapabilityHostSupport:
    """Plugin support: the weakest required member status per profile.

    ``members`` yields ``(host_support, optional)``. An optional member never lowers the
    plugin's status; it is simply left out where it cannot run.
    """
    required = [support for support, optional in members if not optional]
    profiles: dict[LaneProfile, HostProfileSupport] = {}
    for profile in LaneProfile:
        if not required:
            status = HostSupportStatus.UNSUPPORTED
        else:
            status = max((item.status(profile) for item in required), key=_RANK.__getitem__)
        profiles[profile] = HostProfileSupport(status=status)
    return CapabilityHostSupport(profiles=profiles)
