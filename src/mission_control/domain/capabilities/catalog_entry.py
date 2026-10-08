"""Read-side summary of a published catalog Definition's agent-composition core."""

from __future__ import annotations

import re

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
from mission_control.domain.capabilities.pins import VERSION_PATTERN, CapabilityPin

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
    pin: str
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
        pin=capability_pin(published).render(),
        kind=published.ref.kind,
        capability_kind=AGENT_CAPABILITY_KIND.get(published.ref.kind),
        host_support=host_support,
        supported_profiles=host_support.supported_profiles(),
        secret_refs=tuple(secret_refs),
    )


def capability_pin(published: PublishedDefinition) -> CapabilityPin:
    """``<logical_id>@<version>#<definition digest>`` for a published catalog row.

    The version is the upstream release the row pins (``source_provenance.upstream_version``)
    when it has one, else the catalog revision; the digest is the exact definition digest, so
    the pin resolves to exactly one row.
    """
    provenance = getattr(published.definition, "source_provenance", None)
    upstream = getattr(provenance, "upstream_version", None)
    version = (
        upstream
        if isinstance(upstream, str) and re.fullmatch(VERSION_PATTERN, upstream)
        else str(published.ref.revision)
    )
    return CapabilityPin(
        capability_id=published.ref.logical_id,
        version=version,
        digest=published.ref.digest,
    )
