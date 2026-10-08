"""Operational capability catalog; domain entity/schema catalogs are separate ports."""

from __future__ import annotations

import base64
from dataclasses import dataclass
from typing import Any, Literal, Protocol

from pydantic import BaseModel, ConfigDict, Field

from mission_control.application.agentic_components.projections import render_host_files
from mission_control.application.agentic_components.repository import AgenticComponentRepository
from mission_control.application.capabilities.bundle_custody import BundleCustodyService
from mission_control.application.capabilities.capability_search import CapabilitySearchService
from mission_control.application.capabilities.external_candidate_inspection import (
    ExternalCandidateInspectionService,
)
from mission_control.application.capabilities.external_capability_discovery import (
    ExternalCapabilityDiscoveryService,
)
from mission_control.domain.agentic_components.projection import BundleFile, ResolvedCapability
from mission_control.domain.authoring.contracts import (
    DefinitionKind,
    ExactDefinitionRef,
    HookScriptDefinition,
    PluginDefinition,
    PublishedDefinition,
    SkillDefinition,
)
from mission_control.domain.capabilities.bundles import CapabilityDrift, sha256_hex
from mission_control.domain.capabilities.catalog_entry import capability_pin
from mission_control.domain.capabilities.host_support import CapabilityHostSupport, LaneProfile
from mission_control.domain.capabilities.pins import CapabilityPin
from mission_control.domain.coordinator.contracts import CapabilitySearchRequest


class CatalogDefinitions(Protocol):
    async def list_published_definition_refs(self) -> tuple[ExactDefinitionRef, ...]: ...
    async def list_published_definitions(self) -> tuple[PublishedDefinition, ...]: ...
    async def get(self, ref: ExactDefinitionRef) -> PublishedDefinition: ...


@dataclass(frozen=True)
class CatalogService:
    request_scope: str
    catalog_scope: str
    definitions: CatalogDefinitions
    search: CapabilitySearchService | None = None
    discovery: ExternalCapabilityDiscoveryService | None = None
    inspection: ExternalCandidateInspectionService | None = None
    components: AgenticComponentRepository | None = None
    # FT-A2: bundle custody (publish:prepare / publish:complete); None until configured.
    custody: BundleCustodyService | None = None


# ------------------------------------------------------------------------------------------
# FT-A8: pins, inspection and render preview shared by CLI, HTTP and MCP
# ------------------------------------------------------------------------------------------

# Minimum gap between the top two fused scores, as a fraction of the best attainable fused
# score (first in every list). RRF compresses scores, so this is a small number.
DEFAULT_PIN_MARGIN = 0.02


class CatalogReadContract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class CapabilityCandidate(CatalogReadContract):
    pin: str
    kind: DefinitionKind
    title: str
    fused_score: float
    supported_profiles: tuple[LaneProfile, ...] = ()


class PinResolution(CatalogReadContract):
    pin: str
    search_mode: str
    candidates: tuple[CapabilityCandidate, ...]


class AmbiguousCapability(Exception):
    """The top two hits are within the margin: the caller must narrow the query."""

    code = "AMBIGUOUS_CAPABILITY"

    def __init__(self, candidates: tuple[CapabilityCandidate, ...]) -> None:
        super().__init__("query matches more than one capability within the pin margin")
        self.candidates = candidates


class CapabilityNotFound(LookupError):
    code = "CAPABILITY_NOT_FOUND"


class PinRequest(CatalogReadContract):
    """``catalog pin``: exactly one pin for a query under kind and lane filters."""

    query: str = Field(min_length=1, max_length=2_000)
    tenant_scope: str | None = None
    kinds: frozenset[DefinitionKind] = Field(default_factory=frozenset)
    host_profiles: frozenset[LaneProfile] = Field(default_factory=frozenset)
    side_effect_classes: frozenset[str] = Field(default_factory=frozenset)
    limit: int = Field(default=5, ge=2, le=20)
    margin: float = Field(default=DEFAULT_PIN_MARGIN, ge=0, le=1)

    def search_request(self, tenant_scope: str) -> CapabilitySearchRequest:
        return CapabilitySearchRequest(
            query=self.query,
            tenant_scope=self.tenant_scope or tenant_scope,
            kinds=self.kinds,
            host_profiles=self.host_profiles,
            side_effect_classes=self.side_effect_classes,
            limit=self.limit,
        )


async def resolve_pin(
    search: CapabilitySearchService, request: CapabilitySearchRequest, *, margin: float
) -> PinResolution:
    """One pin when the top two fused scores differ by at least ``margin`` of the maximum."""
    response = await search.search(request)
    candidates = tuple(
        CapabilityCandidate(
            pin=hit.pin,
            kind=hit.kind,
            title=hit.title,
            fused_score=hit.fused_rank,
            supported_profiles=hit.supported_profiles,
        )
        for hit in response.hits
        if hit.pin is not None
    )
    if not candidates:
        raise CapabilityNotFound("no admitted capability matches the query and filters")
    best = search.max_fused_score(response.search_mode)
    if len(candidates) > 1 and (
        (candidates[0].fused_score - candidates[1].fused_score) / best < margin
    ):
        raise AmbiguousCapability(candidates)
    return PinResolution(
        pin=candidates[0].pin, search_mode=response.search_mode, candidates=candidates
    )


class PluginMemberView(CatalogReadContract):
    pin: str
    role: str
    optional: bool
    found: bool
    kind: DefinitionKind | None = None
    title: str | None = None
    supported_profiles: tuple[LaneProfile, ...] = ()


class CapabilityInspection(CatalogReadContract):
    """``catalog inspect --pin``: body, host support, secret ref names, plugin members."""

    pin: str
    exact_ref: ExactDefinitionRef
    kind: DefinitionKind
    title: str
    description: str
    retired: bool
    definition: dict[str, Any]
    host_support: CapabilityHostSupport | None
    supported_profiles: tuple[LaneProfile, ...]
    secret_refs: tuple[str, ...]
    plugin_members: tuple[PluginMemberView, ...] = ()


async def find_published_by_pin(
    definitions: object, pin: CapabilityPin
) -> PublishedDefinition | None:
    """The published (not proposed) row a pin names: same logical id and definition digest."""
    finder = getattr(definitions, "find_by_pin", None)
    if callable(finder):
        found = await finder(pin.capability_id, pin.digest)
        return found if isinstance(found, PublishedDefinition) else None
    lister = getattr(definitions, "list_published_definitions", None)
    if not callable(lister):
        raise CapabilityNotFound("the catalog cannot resolve pins")
    for item in await lister():
        if item.ref.logical_id == pin.capability_id and item.ref.digest == pin.digest:
            return item if capability_pin(item) == pin else None
    return None


async def inspect_pin(definitions: object, pin_text: str) -> CapabilityInspection:
    pin = CapabilityPin.parse(pin_text)
    published = await find_published_by_pin(definitions, pin)
    if published is None:
        raise CapabilityNotFound(f"no published capability for {pin.render()}")
    definition = published.definition
    support = getattr(definition, "host_support", None)
    support = support if isinstance(support, CapabilityHostSupport) else None
    members: list[PluginMemberView] = []
    if isinstance(definition, PluginDefinition):
        for member in definition.manifest.members:
            found = await find_published_by_pin(definitions, member.pin)
            member_support = getattr(found.definition, "host_support", None) if found else None
            members.append(
                PluginMemberView(
                    pin=member.pin.render(),
                    role=member.role.value,
                    optional=member.optional,
                    found=found is not None,
                    kind=found.ref.kind if found else None,
                    title=found.definition.title if found else None,
                    supported_profiles=(
                        member_support.supported_profiles()
                        if isinstance(member_support, CapabilityHostSupport)
                        else ()
                    ),
                )
            )
    return CapabilityInspection(
        pin=capability_pin(published).render(),
        exact_ref=published.ref,
        kind=published.ref.kind,
        title=definition.title,
        description=definition.description,
        retired=published.retired_at is not None,
        definition=definition.model_dump(mode="json"),
        host_support=support,
        supported_profiles=support.supported_profiles() if support else (),
        secret_refs=tuple(getattr(definition, "secret_refs", ())),
        plugin_members=tuple(members),
    )


class RenderRequest(CatalogReadContract):
    pin: str
    host: LaneProfile
    instruction: str = "Mission Control render preview."


class RenderedFileView(CatalogReadContract):
    path: str
    mode: str
    encoding: Literal["utf-8", "base64"]
    content: str


class RenderPreview(CatalogReadContract):
    pin: str
    host: LaneProfile
    files: tuple[RenderedFileView, ...]
    report: dict[str, Any]
    send_options: dict[str, Any] = Field(default_factory=dict)
    in_process: dict[str, Any] | None = None


async def resolved_capability(
    definitions: object,
    pin: CapabilityPin,
    *,
    custody: BundleCustodyService | None = None,
) -> ResolvedCapability:
    """A pin resolved for projection: definition, verified bundle bytes, plugin members."""
    published = await find_published_by_pin(definitions, pin)
    if published is None:
        raise CapabilityNotFound(f"no published capability for {pin.render()}")
    definition = published.definition
    files: tuple[BundleFile, ...] = ()
    members: tuple[ResolvedCapability, ...] = ()
    if isinstance(definition, SkillDefinition | HookScriptDefinition):
        files = await _bundle_files(definition, custody)
    elif isinstance(definition, PluginDefinition):
        resolved: list[ResolvedCapability] = []
        for member in definition.manifest.members:
            if await find_published_by_pin(definitions, member.pin) is None:
                if member.optional:
                    continue
                raise CapabilityNotFound(f"plugin member is not published: {member.pin}")
            resolved.append(await resolved_capability(definitions, member.pin, custody=custody))
        members = tuple(resolved)
    return ResolvedCapability(
        pin=pin,
        definition=definition,
        files=files,
        members=members,
    )


async def _bundle_files(
    definition: SkillDefinition | HookScriptDefinition, custody: BundleCustodyService | None
) -> tuple[BundleFile, ...]:
    ref = definition.bundle_ref
    if ref is None or not ref.uri.startswith("capability-bundles://"):
        raise CapabilityNotFound(f"{definition.logical_id} has no custody bundle to render")
    if custody is None:
        raise CapabilityNotFound("bundle bytes are unavailable: custody is not configured")
    prefix = ref.uri.removeprefix("capability-bundles://")
    files: list[BundleFile] = []
    for entry in definition.file_manifest:
        content = await custody.store.get(f"{prefix}/{entry.path}")
        if "sha256:" + sha256_hex(content) != entry.digest or len(content) != entry.size_bytes:
            raise CapabilityDrift(f"bundle file drifted from its pin: {entry.path}")
        files.append(BundleFile(path=entry.path, content=content, executable=entry.executable))
    return tuple(files)


async def render_pin(
    definitions: object,
    request: RenderRequest,
    *,
    custody: BundleCustodyService | None = None,
) -> RenderPreview:
    """``catalog render``: the A4 Host Projection of one pin for one lane profile."""
    pin = CapabilityPin.parse(request.pin)
    row = await resolved_capability(definitions, pin, custody=custody)
    projection = render_host_files((row,), request.host, request.instruction)
    views = []
    for item in projection.files:
        try:
            text, encoding = item.content.decode("utf-8"), "utf-8"
        except UnicodeDecodeError:
            text, encoding = base64.b64encode(item.content).decode("ascii"), "base64"
        views.append(
            RenderedFileView(
                path=item.path,
                mode=oct(item.mode),
                encoding=encoding,
                content=text,
            )
        )
    return RenderPreview(
        pin=pin.render(),
        host=request.host,
        files=tuple(views),
        report=projection.report.model_dump(mode="json"),
        send_options=dict(projection.send_options),
        in_process=(
            projection.in_process.model_dump(mode="json") if projection.in_process else None
        ),
    )
