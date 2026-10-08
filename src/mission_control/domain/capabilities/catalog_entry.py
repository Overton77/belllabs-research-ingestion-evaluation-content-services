"""Read-side summary of a published catalog Definition's agent-composition core."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict

from mission_control.domain.authoring.contracts import (
    DefinitionKind,
    ExactDefinitionRef,
    PublishedDefinition,
)
from mission_control.domain.capabilities.host_support import (
    AgentCapabilityKind,
    CapabilityHostSupport,
    LaneProfile,
)

AGENT_CAPABILITY_KIND: dict[DefinitionKind, AgentCapabilityKind] = {
    DefinitionKind.SKILL: AgentCapabilityKind.SKILL_BUNDLE,
    DefinitionKind.MCP_SERVER: AgentCapabilityKind.MCP_SERVER,
    DefinitionKind.MCP_TOOL: AgentCapabilityKind.MCP_TOOL,
    DefinitionKind.HOOK_SCRIPT: AgentCapabilityKind.HOOK_SCRIPT,
    DefinitionKind.SUBAGENT_PROFILE: AgentCapabilityKind.SUBAGENT_PROFILE,
    DefinitionKind.PLUGIN: AgentCapabilityKind.PLUGIN,
}


class CatalogEntrySummary(BaseModel):
    """What ``catalog list`` shows per row: identity, kind and lane-profile support."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    ref: ExactDefinitionRef
    kind: DefinitionKind
    capability_kind: AgentCapabilityKind | None
    host_support: CapabilityHostSupport
    supported_profiles: tuple[LaneProfile, ...]
    secret_refs: tuple[str, ...]


def summarize(published: PublishedDefinition) -> CatalogEntrySummary:
    definition = published.definition
    host_support = getattr(definition, "host_support", None)
    if not isinstance(host_support, CapabilityHostSupport):
        host_support = CapabilityHostSupport()
    secret_refs = getattr(definition, "secret_refs", ())
    return CatalogEntrySummary(
        ref=published.ref,
        kind=published.ref.kind,
        capability_kind=AGENT_CAPABILITY_KIND.get(published.ref.kind),
        host_support=host_support,
        supported_profiles=host_support.supported_profiles(),
        secret_refs=tuple(secret_refs),
    )
