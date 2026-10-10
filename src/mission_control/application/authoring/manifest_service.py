"""Mission Manifest compile (SPEC-05 "Compile algorithm", FT-E2).

``ManifestCompileService.compile(manifest_yaml, scope)`` is deterministic and writes nothing:

1. parse and shape-validate (pointers into the YAML); 2. the file's ``application`` must equal
the authenticated scope (``APPLICATION_FORBIDDEN``); 3. plugins expand to their member pins
(provenance ``plugin:<alias>``; two versions of one capability id are ``INVALID_DEFINITION``);
4. every ``search`` resolves through Hybrid Search (``kind``, ``host_support`` of the lanes that
use the alias, ``max_results`` 5) to the top admitted hit (none: ``CAPABILITY_UNAVAILABLE``;
top two within 0.05 of the best attainable fused score: warning ``ambiguous_search``), every
``pin`` to its exact admitted row (missing or retired: ``CAPABILITY_UNAVAILABLE``; another
digest: ``CAPABILITY_DRIFT``; ``@latest``: the current row, recorded); 5. effective
environments (E1); 6. lane support per behavior, hook events per lane vocabulary (warning, or
blocker when ``fail_closed``), subagent overlays; 7. coverage (criterion evidence, ``from``,
``action_space``); 8. budgets (``oversubscribed_budget``) and the platform depth cap; 9. each
mission lowers onto the existing compiler and compiles through ``ControlPlaneService.compile``
against a dry-run overlay of the catalog, so the Compiled Program is produced and nothing is
persisted. ``ManifestProgramCompiler`` is the same lowering with ``persist=True`` for submit.

Lane behavior support is a static table here (marked for replacement): the G1 lane describe
(``mc.lane_describe.v1``) carries controls, hooks and subagents but no behavior matrix yet.

MP-02: step 1 dispatches on the document's ``manifest:`` literal. ``mission/v1`` takes the
exact v1 parser and every v1 step above unchanged; ``mission/v2`` parses the v2 model, checks
hook events on the provider lanes too, and admits every node role's ``requires`` against its
lane's declared matrix (V01, ``manifest_v2_admission``) before anything is lowered.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Protocol

from mission_control.application.authoring.control_plane_repository import (
    DefinitionRepository,
    InMemoryDefinitionRepository,
)
from mission_control.application.authoring.manifest_v2_admission import (
    V2_LANE_BEHAVIORS,
    admit_mission,
)
from mission_control.application.authoring.service import ControlPlaneService
from mission_control.application.capabilities.capability_search import CapabilitySearchService
from mission_control.application.capabilities.catalog import DEFAULT_PIN_MARGIN
from mission_control.application.chains.service import (
    CapabilityResolution,
    CapabilityResult,
    CompiledProgramSummary,
    DefinitionSummary,
    HookEventSupport,
    LaneSupport,
    ManifestResolution,
    ManifestValidationReport,
    MissionCapabilities,
    MissionLowering,
    PluginExpansion,
    ResolvedNode,
)
from mission_control.application.ports.payloads import ContentAddressedPayloadStore
from mission_control.contracts.identities import parse_request_scope
from mission_control.domain.authoring.canonical import sha256_digest
from mission_control.domain.authoring.contracts import (
    AliasBinding,
    AliasRef,
    AuthoringHead,
    CompilationContext,
    CompileInvocation,
    Definition,
    DefinitionKind,
    DefinitionSelector,
    EffectiveRunConfiguration,
    EnvironmentAvailability,
    ExactDefinitionRef,
    PluginDefinition,
    PublishedDefinition,
    PublishRequest,
)
from mission_control.domain.authoring.errors import (
    ControlPlaneError,
    DefinitionNotFound,
    ReferenceMismatch,
)
from mission_control.domain.authoring.extensions import ExtensionRegistry
from mission_control.domain.authoring.manifest import (
    Behavior,
    CapabilityEntryBase,
    CapabilityKind,
    ContextEntry,
    Environment,
    Lane,
    ManifestErrorCode,
    ManifestIssue,
    ManifestRejected,
    MissionBlock,
    MissionManifest,
    NodeEnvironment,
    resolve_environments,
    walk_program,
)
from mission_control.domain.authoring.manifest_lowering import (
    MANIFEST_RUN_CAPABILITIES,
    MANIFEST_RUNTIME_BINDING,
    LoweredMission,
    lower_mission,
    lowering_digests,
)
from mission_control.domain.authoring.manifest_v2 import is_v2, parse_manifest_yaml_versioned
from mission_control.domain.authoring.mission_definition import (
    CapabilityPin,
    DefinitionCapability,
    MissionDefinition,
    manifest_to_definition,
)
from mission_control.domain.capabilities.catalog_entry import capability_pin
from mission_control.domain.capabilities.hooks import native_hook
from mission_control.domain.capabilities.host_support import CapabilityHostSupport, LaneProfile
from mission_control.domain.capabilities.plugins import PluginMemberRole
from mission_control.domain.composition.chain import compile_chain
from mission_control.domain.coordinator.contracts import (
    AuthorizationState,
    CapabilitySearchRequest,
)

MAX_RESULTS = 5
# Fraction of the best attainable fused score (A8's pin margin): RRF compresses scores, so
# SPEC-05's 0.05 is applied to normalized scores and A8's 0.02 is the default.
AMBIGUITY_MARGIN = DEFAULT_PIN_MARGIN
PLATFORM_DEPTH_CAP = 8
COMPILE_ACTOR_FALLBACK = "service:mission-control-manifest"

# Manifest Capability Kind -> catalog Definition Kind (ADR-0020, ADR-0023). Assessments and
# deterministic executors are governed tool capabilities; context bundles are file bundles
# carried by skill rows.
CATALOG_KINDS: Mapping[CapabilityKind, DefinitionKind] = {
    CapabilityKind.MCP_SERVER: DefinitionKind.MCP_SERVER,
    CapabilityKind.MCP_TOOL: DefinitionKind.MCP_TOOL,
    CapabilityKind.SKILL_BUNDLE: DefinitionKind.SKILL,
    CapabilityKind.HOOK_SCRIPT: DefinitionKind.HOOK_SCRIPT,
    CapabilityKind.SUBAGENT_PROFILE: DefinitionKind.SUBAGENT_PROFILE,
    CapabilityKind.PLUGIN: DefinitionKind.PLUGIN,
    CapabilityKind.DETERMINISTIC_EXECUTOR: DefinitionKind.TOOL,
    CapabilityKind.ASSESSMENT: DefinitionKind.TOOL,
    CapabilityKind.MODEL_PROFILE: DefinitionKind.MODEL,
    CapabilityKind.SANDBOX_PROFILE: DefinitionKind.SANDBOX_PROFILE,
    CapabilityKind.CONTEXT_BUNDLE: DefinitionKind.SKILL,
}
ROLE_KINDS: Mapping[str, DefinitionKind] = {
    "skill": DefinitionKind.SKILL,
    "agent": DefinitionKind.SUBAGENT_PROFILE,
    "hook": DefinitionKind.HOOK_SCRIPT,
    "plugin": DefinitionKind.PLUGIN,
    "context": DefinitionKind.SKILL,
    "model": DefinitionKind.MODEL,
    "assessment": DefinitionKind.TOOL,
}
HOST_FILTERED_KINDS = frozenset(
    {
        DefinitionKind.SKILL,
        DefinitionKind.MCP_SERVER,
        DefinitionKind.HOOK_SCRIPT,
        DefinitionKind.SUBAGENT_PROFILE,
        DefinitionKind.PLUGIN,
    }
)
MEMBER_ROLES: Mapping[PluginMemberRole, str] = {
    PluginMemberRole.SKILL: "skill",
    PluginMemberRole.MCP_SERVER: "capability",
    PluginMemberRole.HOOK: "hook",
    PluginMemberRole.SUBAGENT: "agent",
    PluginMemberRole.PROMPT: "capability",
    PluginMemberRole.RESOURCE: "context",
}
# TODO(FT-G6): replace with the lane registry once `mc.lane_describe.v1` declares behaviors.
CURSOR_BEHAVIORS = frozenset(
    {
        Behavior.STAGE_GRAPH,
        Behavior.GOAL_LOOP,
        Behavior.AGENT_EXECUTOR,
        Behavior.DETERMINISTIC_EXECUTOR,
        Behavior.HUMAN_GATE,
        Behavior.EVENT_WAIT,
        Behavior.TIMER,
        Behavior.PROOF_GATE,
        Behavior.CHILD_MISSION_INVOCATION,
    }
)
LANE_BEHAVIORS: Mapping[Lane, frozenset[Behavior]] = {
    Lane.DEEP_AGENTS: frozenset(Behavior),
    Lane.CURSOR_LOCAL: CURSOR_BEHAVIORS,
    Lane.CURSOR_CLOUD: CURSOR_BEHAVIORS,
    Lane.CLAUDE_AGENT_SDK: frozenset(),
    Lane.CODEX: frozenset(),
}
IMPLICIT_OUTPUTS: Mapping[Behavior, frozenset[str]] = {
    Behavior.HUMAN_GATE: frozenset({"resolution"}),
    Behavior.PROOF_GATE: frozenset({"disposition"}),
    Behavior.EVENT_WAIT: frozenset({"event"}),
}


class CatalogDefinitionsPort(Protocol):
    async def list_published_definitions(self) -> tuple[PublishedDefinition, ...]: ...

    async def get(self, ref: ExactDefinitionRef) -> PublishedDefinition: ...


@dataclass(frozen=True)
class ManifestScope:
    """The authenticated scope a compile runs under; the file never selects it."""

    request_scope: str
    actor_id: str = COMPILE_ACTOR_FALLBACK
    at: datetime | None = None

    @property
    def application_id(self) -> str:
        return parse_request_scope(self.request_scope).application_id

    @property
    def tenant_id(self) -> str:
        return str(parse_request_scope(self.request_scope).tenant_id)


@dataclass(frozen=True)
class CompiledMission:
    mission_key: str
    definition: MissionDefinition
    lowered: LoweredMission
    configuration: EffectiveRunConfiguration
    workflow_type_ref: ExactDefinitionRef


@dataclass(frozen=True)
class ManifestCompilation:
    """What ``compile`` returns: definitions, Compiled Programs, report and resolution."""

    manifest: MissionManifest | None
    manifest_yaml: str
    report: ManifestValidationReport
    resolution: ManifestResolution | None
    definitions: tuple[MissionDefinition, ...] = ()
    programs: tuple[CompiledMission, ...] = ()

    @property
    def ok(self) -> bool:
        return self.report.ok

    def program(self, mission_key: str) -> CompiledMission:
        return next(item for item in self.programs if item.mission_key == mission_key)


def _issue(
    code: ManifestErrorCode, pointer: str, message: str, reason: str | None = None
) -> ManifestIssue:
    return ManifestIssue(code=code, pointer=pointer, message=message, reason=reason)


# ------------------------------------------------------------------------------------------
# Dry-run and persisting lowering onto ControlPlaneService.compile
# ------------------------------------------------------------------------------------------


class OverlayDefinitionRepository:
    """A ``DefinitionRepository`` that reads the base catalog and keeps its own writes.

    Lowered definitions and the compiled configuration live only in the overlay, so a
    compile produces a real Compiled Program without persisting anything.
    """

    def __init__(self, base: DefinitionRepository) -> None:
        self._base = base
        self._overlay = InMemoryDefinitionRepository()

    async def save_draft(self, *args: Any, **kwargs: Any) -> AuthoringHead:
        raise ReferenceMismatch("drafts are not part of a manifest compile")

    async def get_draft(self, kind: str, logical_id: str) -> AuthoringHead:
        return await self._base.get_draft(kind, logical_id)

    async def publish(
        self,
        definition: Definition,
        actor_id: str,
        published_at: datetime,
        expected_head_revision: int,
        expected_draft_revision: int | None = None,
    ) -> PublishedDefinition:
        del expected_head_revision, expected_draft_revision
        existing = await _existing(self._base, definition)
        if existing is not None:
            return existing
        existing = await _existing(self._overlay, definition)
        if existing is not None:
            return existing
        return await self._overlay.publish(definition, actor_id, published_at, 0)

    async def get(self, ref: ExactDefinitionRef) -> PublishedDefinition:
        try:
            return await self._overlay.get(ref)
        except DefinitionNotFound:
            return await self._base.get(ref)

    async def resolve(self, alias: AliasRef, *, selectable: bool = True) -> AliasBinding:
        return await self._base.resolve(alias, selectable=selectable)

    async def move_alias(self, *args: Any, **kwargs: Any) -> AliasBinding:
        raise ReferenceMismatch("aliases are not moved by a manifest compile")

    async def retire(self, *args: Any, **kwargs: Any) -> PublishedDefinition:
        raise ReferenceMismatch("definitions are not retired by a manifest compile")

    async def save_erc_record(self, record: dict[str, Any]) -> None:
        await self._overlay.save_erc_record(record)

    async def get_erc_record(self, digest: str) -> dict[str, Any]:
        try:
            return await self._overlay.get_erc_record(digest)
        except (DefinitionNotFound, KeyError, LookupError):
            return await self._base.get_erc_record(digest)

    async def list_projection_events(self) -> tuple[dict[str, Any], ...]:
        return ()


async def _existing(repository: Any, definition: Definition) -> PublishedDefinition | None:
    """The revision-1 row of a content-addressed lowered definition, if published."""

    ref = ExactDefinitionRef(
        kind=definition.kind,
        logical_id=definition.logical_id,
        revision=1,
        digest=sha256_digest(definition),
    )
    try:
        found: PublishedDefinition = await repository.get(ref)
    except (DefinitionNotFound, ReferenceMismatch, ControlPlaneError):
        return None
    return found


class ManifestProgramCompiler:
    """Publish a lowered mission's definitions and compile its Effective Run Configuration.

    ``persist=False`` (compile) publishes into a dry-run overlay; ``persist=True`` (submit)
    publishes into the catalog (idempotent: content-addressed rows are reused).
    """

    def __init__(
        self,
        repository: DefinitionRepository,
        extensions: ExtensionRegistry,
        payloads: ContentAddressedPayloadStore,
    ) -> None:
        self._repository = repository
        self._extensions = extensions
        self._payloads = payloads

    async def compile(
        self,
        lowered: LoweredMission,
        *,
        request_scope: str,
        actor_id: str,
        at: datetime,
        persist: bool,
        compilation_suffix: str | None = None,
    ) -> tuple[EffectiveRunConfiguration, ExactDefinitionRef]:
        repository: Any = (
            self._repository if persist else OverlayDefinitionRepository(self._repository)
        )
        control_plane = ControlPlaneService(repository, self._extensions, self._payloads)

        async def publish(definition: Definition) -> ExactDefinitionRef:
            existing = await _existing(repository, definition)
            if existing is not None:
                return existing.ref
            published = await control_plane.publish(
                PublishRequest(
                    definition=definition,
                    actor_id=actor_id,
                    published_at=at,
                    expected_head_revision=0,
                )
            )
            return published.ref

        blueprint_ref = await publish(lowered.blueprint)
        control_ref = await publish(lowered.control_profile(blueprint_ref))
        runtime_ref = await publish(lowered.runtime_profile)
        workspace_ref = await publish(lowered.workspace_template)
        evaluation_ref = await publish(lowered.evaluation_profile)
        workflow_ref = await publish(
            lowered.workflow_type(
                blueprint_ref=blueprint_ref,
                control_ref=control_ref,
                runtime_ref=runtime_ref,
                workspace_ref=workspace_ref,
                evaluation_ref=evaluation_ref,
            )
        )
        configuration = await control_plane.compile(
            CompileInvocation(
                workflow_type=DefinitionSelector(exact=workflow_ref),
                blueprint=DefinitionSelector(exact=blueprint_ref),
                control_profile=DefinitionSelector(exact=control_ref),
                runtime_profile=DefinitionSelector(exact=runtime_ref),
                workspace_template=DefinitionSelector(exact=workspace_ref),
                evaluation_profile=DefinitionSelector(exact=evaluation_ref),
                input_manifest=lowered.input_manifest,
                caller_authority=lowered.authority,
                environment=EnvironmentAvailability(
                    capabilities=MANIFEST_RUN_CAPABILITIES,
                    runtime_bindings=frozenset({MANIFEST_RUNTIME_BINDING}),
                ),
                context=CompilationContext(
                    # A persisted compile is unique per submit (the catalog keeps one ERC per
                    # compilation identity); a dry run reuses the lowering identity.
                    compilation_id=f"{lowered.base_id}.compile"
                    + (f".{compilation_suffix}" if compilation_suffix else ""),
                    compiled_at=at,
                    actor_id=actor_id,
                    authority_subject_id=actor_id,
                    authority_scope=request_scope,
                ),
            )
        )
        return configuration, workflow_ref


# ------------------------------------------------------------------------------------------
# The compile service
# ------------------------------------------------------------------------------------------


@dataclass
class _MissionState:
    pointer: str
    block: MissionBlock
    nodes: tuple[NodeEnvironment, ...]
    definition: MissionDefinition
    capabilities: list[DefinitionCapability] = field(default_factory=list)


class ManifestCompileService:
    def __init__(
        self,
        *,
        definitions: CatalogDefinitionsPort,
        search: CapabilitySearchService | None,
        programs: ManifestProgramCompiler | None = None,
        ambiguity_margin: float = AMBIGUITY_MARGIN,
        depth_cap: int = PLATFORM_DEPTH_CAP,
        catalog_scope: str | None = None,
    ) -> None:
        self._definitions = definitions
        self._search = search
        self._programs = programs
        self._margin = ambiguity_margin
        self._depth_cap = depth_cap
        # The installation catalog's search partition (`mc/<installation>/<app>/catalog`):
        # publication projects there and the public catalog search reads there, so compile
        # resolves there too. Absent (in-memory catalogs), the request scope is searched.
        self._catalog_scope = catalog_scope

    async def compile(self, manifest_yaml: str, scope: ManifestScope) -> ManifestCompilation:
        at = scope.at or datetime.now(UTC)
        try:
            # `mission/v1` takes the exact v1 parser; `mission/v2` the v2 model (MP-02).
            manifest, document = parse_manifest_yaml_versioned(manifest_yaml)
        except ManifestRejected as rejected:
            return ManifestCompilation(
                manifest=None,
                manifest_yaml=manifest_yaml,
                report=ManifestValidationReport(ok=False, blockers=rejected.issues),
                resolution=None,
            )
        structure = resolve_environments(manifest, document)
        chain = compile_chain(manifest)
        blockers: list[ManifestIssue] = [*structure.blockers, *chain.blockers]
        warnings: list[ManifestIssue] = [*structure.warnings, *chain.warnings]
        environments = {item.pointer: item for item in structure.missions}
        missions: list[_MissionState] = []
        for pointer, block in manifest.mission_blocks():
            env = environments[pointer]
            if block.application != scope.application_id:
                blockers.append(
                    _issue(
                        ManifestErrorCode.APPLICATION_FORBIDDEN,
                        f"{pointer}/application",
                        f"mission application {block.application} differs from the "
                        f"authenticated application {scope.application_id}",
                        "application_scope",
                    )
                )
            definition = manifest_to_definition(
                block, pointer=pointer, digest=structure.manifest_digest, environments=env
            )
            missions.append(
                _MissionState(
                    pointer=pointer,
                    block=block,
                    nodes=env.nodes,
                    definition=definition,
                    capabilities=list(definition.capabilities),
                )
            )
        if any(item.code is ManifestErrorCode.APPLICATION_FORBIDDEN for item in blockers):
            return self._finish(
                manifest,
                manifest_yaml,
                structure.manifest_digest,
                scope,
                at,
                missions,
                blockers,
                warnings,
                (),
                (),
                (),
                (),
                chain.resolution,
                (),
                (),
            )
        resolved: list[CapabilityResolution] = []
        plugins: list[PluginExpansion] = []
        for mission in missions:
            await self._resolve_mission(mission, scope, resolved, plugins, blockers, warnings)
        lane_support: list[LaneSupport] = []
        hook_events: list[HookEventSupport] = []
        v2 = is_v2(manifest)
        for mission in missions:
            self._lanes(mission, lane_support, hook_events, blockers, warnings, v2=v2)
            self._coverage(mission, blockers, warnings)
            if v2:
                # V01: every node role's `requires` against its lane's declared matrix.
                admitted_blockers, admitted_warnings = admit_mission(
                    mission.block, mission.pointer, mission.nodes
                )
                blockers.extend(admitted_blockers)
                warnings.extend(admitted_warnings)
        programs: list[CompiledMission] = []
        lowerings: list[MissionLowering] = []
        resolved_definitions = [
            mission.definition.model_copy(update={"capabilities": tuple(mission.capabilities)})
            for mission in missions
        ]
        for mission, definition in zip(missions, resolved_definitions, strict=True):
            mission.definition = definition
        if not blockers:
            for mission in missions:
                lowered = lower_mission(
                    mission.definition,
                    capability_refs=self._capability_refs(mission.block.key, resolved),
                )
                warnings.extend(lowered.warnings)
                digests = lowering_digests(lowered)
                lowerings.append(
                    MissionLowering(
                        mission_key=mission.block.key,
                        family=lowered.family,
                        lowering_version=digests["lowering_version"],
                        blueprint_digest=digests["blueprint_digest"],
                        runtime_profile_digest=digests["runtime_profile_digest"],
                        definition_digest=mission.definition.digest,
                    )
                )
                if self._programs is None:
                    continue
                try:
                    configuration, workflow_ref = await self._programs.compile(
                        lowered,
                        request_scope=scope.request_scope,
                        actor_id=scope.actor_id,
                        at=at,
                        persist=False,
                    )
                except (ControlPlaneError, ValueError) as error:
                    blockers.append(
                        _issue(
                            ManifestErrorCode.INVALID_DEFINITION,
                            f"{mission.pointer}/program",
                            f"lowering onto the compiler failed: {error}",
                            "lowering_failed",
                        )
                    )
                    continue
                programs.append(
                    CompiledMission(
                        mission_key=mission.block.key,
                        definition=mission.definition,
                        lowered=lowered,
                        configuration=configuration,
                        workflow_type_ref=workflow_ref,
                    )
                )
        return self._finish(
            manifest,
            manifest_yaml,
            structure.manifest_digest,
            scope,
            at,
            missions,
            blockers,
            warnings,
            resolved,
            plugins,
            lane_support,
            hook_events,
            chain.resolution,
            lowerings,
            programs,
        )

    # -- report ------------------------------------------------------------------------

    @staticmethod
    def _finish(
        manifest: MissionManifest,
        manifest_yaml: str,
        digest: str,
        scope: ManifestScope,
        at: datetime,
        missions: Sequence[_MissionState],
        blockers: Sequence[ManifestIssue],
        warnings: Sequence[ManifestIssue],
        resolved: Sequence[CapabilityResolution],
        plugins: Sequence[PluginExpansion],
        lane_support: Sequence[LaneSupport],
        hook_events: Sequence[HookEventSupport],
        chain: Any,
        lowerings: Sequence[MissionLowering],
        programs: Sequence[CompiledMission],
    ) -> ManifestCompilation:
        unique_blockers = _unique(blockers)
        unique_warnings = _unique(warnings)
        resolution = ManifestResolution(
            manifest_digest=digest,
            catalog_resolution="resolved",
            application_id=scope.application_id,
            tenant_id=scope.tenant_id,
            actor_ref=scope.actor_id,
            resolved_at=scope.at,
            capabilities=tuple(
                MissionCapabilities(
                    mission_key=mission.block.key, capabilities=tuple(mission.capabilities)
                )
                for mission in missions
            ),
            resolved=tuple(resolved),
            plugins=tuple(plugins),
            nodes=tuple(
                ResolvedNode(
                    mission_key=mission.block.key,
                    node_key=node.node_key,
                    role=node.role,
                    pointer=node.pointer,
                    lane=node.lane,
                    lane_changed_from=node.lane_changed_from,
                    effective_environment=node.effective_environment,
                    field_provenance=_with_plugin_provenance(node, plugins, mission.block.key),
                )
                for mission in missions
                for node in mission.nodes
            ),
            lane_support=tuple(lane_support),
            hook_events=tuple(hook_events),
            lowering=tuple(lowerings),
            chain=chain,
            blockers=unique_blockers,
            warnings=unique_warnings,
        )
        report = ManifestValidationReport(
            ok=not unique_blockers,
            manifest_digest=digest,
            blockers=unique_blockers,
            warnings=unique_warnings,
            definitions=tuple(
                DefinitionSummary(
                    mission_key=mission.block.key,
                    pointer=mission.pointer,
                    definition_digest=mission.definition.digest,
                    is_resolved=mission.definition.is_resolved,
                )
                for mission in missions
            ),
            programs=tuple(
                CompiledProgramSummary(
                    mission_key=item.mission_key,
                    family=item.lowered.family,
                    effective_configuration_digest=item.configuration.digest,
                    workflow_type_ref=item.workflow_type_ref,
                    blueprint_digest=item.lowered.blueprint_digest,
                    initial_goal=item.lowered.initial_goal,
                )
                for item in programs
            ),
            resolution=resolution,
        )
        del at
        return ManifestCompilation(
            manifest=manifest,
            manifest_yaml=manifest_yaml,
            report=report,
            resolution=resolution,
            definitions=tuple(mission.definition for mission in missions),
            programs=tuple(programs),
        )

    @staticmethod
    def _capability_refs(
        mission_key: str, resolved: Sequence[CapabilityResolution]
    ) -> tuple[ExactDefinitionRef, ...]:
        return tuple(
            item.result.exact_ref
            for item in resolved
            if item.mission_key == mission_key and item.result is not None
        )

    # -- capability resolution ------------------------------------------------------------

    async def _resolve_mission(
        self,
        mission: _MissionState,
        scope: ManifestScope,
        resolved: list[CapabilityResolution],
        plugins: list[PluginExpansion],
        blockers: list[ManifestIssue],
        warnings: list[ManifestIssue],
    ) -> None:
        published = await self._definitions.list_published_definitions()
        by_id: dict[str, list[PublishedDefinition]] = {}
        for item in published:
            by_id.setdefault(item.ref.logical_id, []).append(item)
        final: list[DefinitionCapability] = []
        seen: dict[str, tuple[str, str]] = {}
        queue = list(mission.capabilities)
        index = 0
        while index < len(queue):
            capability = queue[index]
            index += 1
            lanes = self._lanes_for(mission, capability)
            entry = await self._resolve_one(
                mission.block.key, capability, lanes, scope, by_id, warnings
            )
            resolved.append(entry)
            if entry.blocker is not None:
                blockers.append(entry.blocker)
                final.append(capability)
                continue
            result = entry.result
            assert result is not None
            prior = seen.get(result.capability_id)
            if prior is not None and prior[1] != result.digest:
                blockers.append(
                    _issue(
                        ManifestErrorCode.INVALID_DEFINITION,
                        capability.pointer,
                        f"{result.capability_id} resolves to two versions in mission "
                        f"{mission.block.key} ({prior[0]} and {result.pin})",
                        "conflicting_versions",
                    )
                )
            seen.setdefault(result.capability_id, (result.pin, result.digest))
            pin = CapabilityPin(
                capability_id=result.capability_id, version=result.version, digest=result.digest
            )
            final.append(capability.model_copy(update={"resolved": pin}))
            record = next(
                (
                    row
                    for row in by_id.get(result.capability_id, [])
                    if row.ref.digest == result.digest
                ),
                None,
            )
            if record is not None and isinstance(record.definition, PluginDefinition):
                members = record.definition.manifest.members
                plugins.append(
                    PluginExpansion(
                        mission_key=mission.block.key,
                        pointer=capability.pointer,
                        alias=capability.alias,
                        plugin_pin=result.pin,
                        members=tuple(member.pin.render() for member in members),
                    )
                )
                for member_index, member in enumerate(members):
                    already = seen.get(member.pin.capability_id)
                    if already is not None and already[1] == member.pin.digest:
                        continue  # duplicate pins collapse
                    queue.append(
                        DefinitionCapability(
                            pointer=f"{capability.pointer}/members/{member_index}",
                            role=MEMBER_ROLES[member.role],
                            alias=None,
                            pin=CapabilityPin(
                                capability_id=member.pin.capability_id,
                                version=member.pin.version,
                                digest=member.pin.digest,
                            ),
                            attributes={
                                "provenance": f"plugin:{capability.alias or 'plugin'}",
                                "plugin_member_role": member.role.value,
                                "optional": member.optional,
                            },
                        )
                    )
        mission.capabilities = final

    def _lanes_for(
        self, mission: _MissionState, capability: DefinitionCapability
    ) -> tuple[str, ...]:
        if capability.search is not None and capability.search.require:
            return tuple(sorted({lane.value for lane in capability.search.require}))
        lanes: set[str] = set()
        for node in mission.nodes:
            if _environment_names(node.effective_environment, capability):
                lanes.add(node.lane.value)
        if not lanes:
            lanes.add((mission.block.environment.lane or Lane.DEEP_AGENTS).value)
        return tuple(sorted(lanes))

    async def _resolve_one(
        self,
        mission_key: str,
        capability: DefinitionCapability,
        lanes: tuple[str, ...],
        scope: ManifestScope,
        by_id: Mapping[str, list[PublishedDefinition]],
        warnings: list[ManifestIssue],
    ) -> CapabilityResolution:
        kind = _kind_of(capability)
        request: dict[str, object] = {"kind": kind.value if kind is not None else None}
        if capability.search is not None:
            request["search"] = capability.search.query
            if capability.search.require:
                request["require"] = [lane.value for lane in capability.search.require]
        else:
            assert capability.pin is not None
            request["pin"] = _pin_text(capability.pin)
        base = CapabilityResolution(
            mission_key=mission_key,
            pointer=capability.pointer,
            role=capability.role,
            alias=capability.alias,
            request=request,
            lanes=lanes,
            provenance=str(capability.attributes.get("provenance"))
            if "provenance" in capability.attributes
            else None,
        )
        if capability.search is not None:
            return await self._resolve_search(base, capability, kind, lanes, scope, warnings)
        return self._resolve_pin(base, capability, kind, lanes, by_id)

    async def _resolve_search(
        self,
        base: CapabilityResolution,
        capability: DefinitionCapability,
        kind: DefinitionKind | None,
        lanes: tuple[str, ...],
        scope: ManifestScope,
        warnings: list[ManifestIssue],
    ) -> CapabilityResolution:
        assert capability.search is not None
        if self._search is None:
            return base.model_copy(
                update={
                    "blocker": _issue(
                        ManifestErrorCode.CAPABILITY_UNAVAILABLE,
                        capability.pointer,
                        "capability search is not composed for this installation",
                        "search_unavailable",
                    )
                }
            )
        filtered = kind in HOST_FILTERED_KINDS
        response = await self._search.search(
            CapabilitySearchRequest(
                query=capability.search.query,
                kinds=frozenset({kind}) if kind is not None else frozenset(),
                tenant_scope=self._catalog_scope or scope.request_scope,
                host_profiles=(
                    frozenset(LaneProfile(lane) for lane in lanes) if filtered else frozenset()
                ),
                limit=MAX_RESULTS,
            )
        )
        hits = [
            hit
            for hit in response.hits
            if hit.exact_ref is not None
            and hit.pin is not None
            and hit.authorization_state is AuthorizationState.SELECTABLE
            and (
                not filtered or set(lanes) <= {profile.value for profile in hit.supported_profiles}
            )
        ]
        candidates = tuple(str(hit.pin) for hit in hits)
        if not hits:
            return base.model_copy(
                update={
                    "candidates": candidates,
                    "blocker": _issue(
                        ManifestErrorCode.CAPABILITY_UNAVAILABLE,
                        capability.pointer,
                        f"search {capability.search.query!r} ({kind.value if kind else 'any'}) "
                        f"has no admitted hit on lanes {list(lanes)}",
                        "search_no_admitted_hits",
                    ),
                }
            )
        best = self._search.max_fused_score(response.search_mode) or 1.0
        if len(hits) > 1 and (hits[0].fused_rank - hits[1].fused_rank) / best < self._margin:
            warnings.append(
                _issue(
                    ManifestErrorCode.CAPABILITY_UNAVAILABLE,
                    capability.pointer,
                    f"search {capability.search.query!r} is ambiguous between {hits[0].pin} "
                    f"and {hits[1].pin}; the top hit was pinned",
                    "ambiguous_search",
                )
            )
        top = hits[0]
        assert top.exact_ref is not None and top.pin is not None
        return base.model_copy(
            update={
                "candidates": candidates,
                "result": _result(top.pin, top.exact_ref, top.host_support),
            }
        )

    def _resolve_pin(
        self,
        base: CapabilityResolution,
        capability: DefinitionCapability,
        kind: DefinitionKind | None,
        lanes: tuple[str, ...],
        by_id: Mapping[str, list[PublishedDefinition]],
    ) -> CapabilityResolution:
        assert capability.pin is not None
        pin = capability.pin
        rows = [
            row for row in by_id.get(pin.capability_id, []) if kind is None or row.ref.kind == kind
        ]
        admitted = [row for row in rows if row.retired_at is None]
        if pin.version == "latest" or pin.digest is None:
            if not admitted:
                return base.model_copy(
                    update={
                        "blocker": _issue(
                            ManifestErrorCode.CAPABILITY_UNAVAILABLE,
                            capability.pointer,
                            f"no admitted row for {pin.capability_id}@latest",
                            "pin_not_found",
                        )
                    }
                )
            row = max(admitted, key=lambda item: item.ref.revision)
        else:
            exact = [row for row in rows if row.ref.digest == pin.digest]
            if not exact:
                if rows:
                    return base.model_copy(
                        update={
                            "blocker": _issue(
                                ManifestErrorCode.CAPABILITY_DRIFT,
                                capability.pointer,
                                f"{_pin_text(pin)} does not match the admitted digest of "
                                f"{pin.capability_id} ({capability_pin(rows[-1]).render()})",
                                "digest_mismatch",
                            )
                        }
                    )
                return base.model_copy(
                    update={
                        "blocker": _issue(
                            ManifestErrorCode.CAPABILITY_UNAVAILABLE,
                            capability.pointer,
                            f"{_pin_text(pin)} is not in the catalog",
                            "pin_not_found",
                        )
                    }
                )
            row = exact[0]
            if row.retired_at is not None:
                return base.model_copy(
                    update={
                        "blocker": _issue(
                            ManifestErrorCode.CAPABILITY_UNAVAILABLE,
                            capability.pointer,
                            f"{_pin_text(pin)} was retired",
                            "pin_revoked",
                        )
                    }
                )
            if capability_pin(row).version != pin.version:
                return base.model_copy(
                    update={
                        "blocker": _issue(
                            ManifestErrorCode.CAPABILITY_DRIFT,
                            capability.pointer,
                            f"{_pin_text(pin)} names version {pin.version}; the row with that "
                            f"digest is {capability_pin(row).render()}",
                            "version_mismatch",
                        )
                    }
                )
        rendered = capability_pin(row)
        support = getattr(row.definition, "host_support", None)
        support = support if isinstance(support, CapabilityHostSupport) else None
        if support is not None and row.ref.kind in HOST_FILTERED_KINDS:
            missing = [lane for lane in lanes if not support.supports(lane)]
            if missing:
                return base.model_copy(
                    update={
                        "result": _result(rendered.render(), row.ref, support),
                        "blocker": _issue(
                            ManifestErrorCode.CAPABILITY_UNAVAILABLE,
                            capability.pointer,
                            f"{rendered.render()} is not supported on lanes {missing}",
                            "pin_lane_unsupported",
                        ),
                    }
                )
        return base.model_copy(update={"result": _result(rendered.render(), row.ref, support)})

    # -- lanes, hooks, subagents --------------------------------------------------------------

    def _lanes(
        self,
        mission: _MissionState,
        lane_support: list[LaneSupport],
        hook_events: list[HookEventSupport],
        blockers: list[ManifestIssue],
        warnings: list[ManifestIssue],
        *,
        v2: bool = False,
    ) -> None:
        behaviors = V2_LANE_BEHAVIORS if v2 else LANE_BEHAVIORS
        for node in mission.nodes:
            supported = node.behavior in behaviors[node.lane]
            reason = None
            if not supported:
                reason = (
                    "lane_reserved_in_v1"
                    if not v2 and node.lane in {Lane.CLAUDE_AGENT_SDK, Lane.CODEX}
                    else "behavior_unsupported_on_lane"
                )
                blockers.append(
                    _issue(
                        ManifestErrorCode.UNSUPPORTED_BEHAVIOR,
                        node.pointer,
                        f"{node.behavior.value} is not supported on lane {node.lane.value}",
                        reason,
                    )
                )
            lane_support.append(
                LaneSupport(
                    mission_key=mission.block.key,
                    node_key=node.node_key,
                    role=node.role,
                    behavior=node.behavior.value,
                    lane=node.lane.value,
                    supported=supported,
                    reason=reason,
                )
            )
            if not v2 and node.lane in {Lane.CLAUDE_AGENT_SDK, Lane.CODEX}:
                continue
            environment = node.effective_environment
            for index, hook in enumerate(environment.hooks or ()):
                for event in hook.events:
                    native = native_hook(node.lane.value, event.value)
                    hook_events.append(
                        HookEventSupport(
                            mission_key=mission.block.key,
                            node_key=node.node_key,
                            hook_alias=hook.as_,
                            event=event.value,
                            lane=node.lane.value,
                            native_event=native.native_event if native else None,
                            unsupported_on_lane=native is None,
                        )
                    )
                    if native is None:
                        target = blockers if hook.fail_closed else warnings
                        target.append(
                            _issue(
                                ManifestErrorCode.UNSUPPORTED_BEHAVIOR,
                                f"{node.pointer}/environment/hooks/{index}/events",
                                f"hook {hook.as_ or index} event {event.value} has no native "
                                f"event on lane {node.lane.value}",
                                "unsupported_on_lane",
                            )
                        )
            for index, agent in enumerate(environment.agents or ()):
                overlay = agent.overlay
                if (
                    node.lane is Lane.CURSOR_CLOUD
                    and overlay is not None
                    and (overlay.readonly is not None or overlay.is_background is not None)
                ):
                    warnings.append(
                        _issue(
                            ManifestErrorCode.UNSUPPORTED_BEHAVIOR,
                            f"{node.pointer}/environment/agents/{index}/overlay",
                            f"subagent {agent.as_ or index}: readonly/is_background are not "
                            "supported inline on cursor_cloud; projected as a file",
                            "projected_as_file",
                        )
                    )

    # -- coverage, budgets ---------------------------------------------------------------------

    def _coverage(
        self,
        mission: _MissionState,
        blockers: list[ManifestIssue],
        warnings: list[ManifestIssue],
    ) -> None:
        block = mission.block
        pointer = mission.pointer
        node_outputs: dict[str, set[str]] = {}
        node_behaviors: dict[str, Behavior] = {}
        visits = list(walk_program(block.program, f"{pointer}/program"))
        for visit in visits:
            behavior = Behavior(visit.node.behavior)
            node_behaviors[visit.node.key] = behavior
            node_outputs[visit.node.key] = {item.name for item in visit.node.outputs} | set(
                IMPLICIT_OUTPUTS.get(behavior, frozenset())
            )
        outputs = set().union(*node_outputs.values()) if node_outputs else set()
        for goal_index, goal in enumerate(block.goals):
            for criterion_index, criterion in enumerate(goal.criteria):
                for evidence_index, evidence in enumerate(criterion.evidence):
                    if "@" in evidence or evidence in outputs:
                        continue
                    blockers.append(
                        _issue(
                            ManifestErrorCode.INVALID_DEFINITION,
                            f"{pointer}/goals/{goal_index}/criteria/{criterion_index}/evidence/"
                            f"{evidence_index}",
                            f"criterion evidence {evidence} names no output of mission {block.key}",
                            "unknown_evidence",
                        )
                    )
        root_aliases = _context_aliases(block.environment) | {
            item.name for item in block.program.inputs
        }
        chain_missions: set[str] = set()
        for visit in visits:
            node = visit.node
            for index, binding in enumerate(node.inputs):
                if binding.from_ is None:
                    continue
                source, _, output = binding.from_.partition(".")
                if source in node_outputs:
                    if output in node_outputs[source]:
                        continue
                    if source == block.program.key and output in root_aliases:
                        continue  # a mission-level context alias or a root input
                elif source != block.key:
                    chain_missions.add(source)
                    continue  # a chain binding: SPEC-04 compile validates it
                blockers.append(
                    _issue(
                        ManifestErrorCode.INVALID_DEFINITION,
                        f"{visit.pointer}/inputs/{index}/from",
                        f"input from {binding.from_} names no declared output or context alias",
                        "unresolved_from",
                    )
                )
            action_space = getattr(node, "action_space", ())
            if action_space:
                environment = next(
                    (
                        item.effective_environment
                        for item in mission.nodes
                        if item.node_key == node.key and item.role == "node"
                    ),
                    block.environment,
                )
                aliases = (
                    _environment_aliases(environment)
                    | _environment_aliases(block.environment)
                    | set(node_behaviors)
                )
                for index, alias in enumerate(action_space):
                    if alias not in aliases:
                        blockers.append(
                            _issue(
                                ManifestErrorCode.INVALID_DEFINITION,
                                f"{visit.pointer}/action_space/{index}",
                                f"action_space alias {alias} names no capability, agent, "
                                "plugin or node of the mission",
                                "unresolved_action_space",
                            )
                        )
        for resolved_node in mission.nodes:
            governors = resolved_node.effective_environment.governors
            if governors is not None and governors.depth is not None:
                if governors.depth > self._depth_cap:
                    blockers.append(
                        _issue(
                            ManifestErrorCode.INVALID_DEFINITION,
                            f"{resolved_node.pointer}/environment/governors/depth",
                            f"depth {governors.depth} exceeds the platform cap {self._depth_cap}",
                            "depth_exceeds_platform_cap",
                        )
                    )
        self._budgets(mission, warnings)

    @staticmethod
    def _budgets(mission: _MissionState, warnings: list[ManifestIssue]) -> None:
        by_key = {node.node_key: node for node in mission.nodes if node.role == "node"}
        for visit in walk_program(mission.block.program, f"{mission.pointer}/program"):
            if Behavior(visit.node.behavior) is not Behavior.STAGE_GRAPH:
                continue
            parent = by_key[visit.node.key].effective_environment.budget
            members = [by_key[item.key] for item in getattr(visit.node, "nodes", ())]
            for name in ("usd", "tokens", "tool_calls"):
                limit = getattr(parent, name) if parent is not None else None
                if limit is None:
                    continue
                total = sum(
                    (
                        getattr(child.effective_environment.budget, name) or 0
                        for child in members
                        if child.effective_environment.budget is not None
                        and child.field_provenance.get(f"budget.{name}") == "overlay"
                    ),
                    start=0,
                )
                if total > limit:
                    warnings.append(
                        _issue(
                            ManifestErrorCode.INVALID_DEFINITION,
                            f"{visit.pointer}/environment/budget/{name}",
                            f"node budgets ({name} {total}) exceed the parent ({limit}); "
                            "release is governed at run time",
                            "oversubscribed_budget",
                        )
                    )


def _kind_of(capability: DefinitionCapability) -> DefinitionKind | None:
    if capability.kind is not None:
        return CATALOG_KINDS[capability.kind]
    return ROLE_KINDS.get(capability.role)


def _pin_text(pin: CapabilityPin) -> str:
    return f"{pin.capability_id}@{pin.version}" + (f"#{pin.digest}" if pin.digest else "")


def _result(
    pin: str, ref: ExactDefinitionRef, support: CapabilityHostSupport | None
) -> CapabilityResult:
    capability_id, _, rest = pin.partition("@")
    version, _, digest = rest.partition("#")
    return CapabilityResult(
        pin=pin,
        capability_id=capability_id,
        version=version,
        digest=digest or ref.digest,
        kind=ref.kind.value,
        exact_ref=ref,
        supported_profiles=tuple(profile.value for profile in support.supported_profiles())
        if support is not None
        else (),
    )


def _entries(environment: Environment) -> Iterable[CapabilityEntryBase | ContextEntry]:
    if environment.workspace is not None:
        yield from (
            item for item in environment.workspace.context or () if isinstance(item, ContextEntry)
        )
        yield from environment.workspace.skills or ()
    yield from environment.capabilities or ()
    yield from environment.agents or ()
    yield from environment.hooks or ()
    yield from environment.plugins or ()


def _environment_names(environment: Environment, capability: DefinitionCapability) -> bool:
    """Whether ``environment`` carries the capability request (by alias, or by its text)."""

    for entry in _entries(environment):
        if capability.alias is not None and entry.as_ == capability.alias:
            return True
        if capability.alias is None:
            if (
                capability.pin is not None
                and entry.pin is not None
                and entry.pin.startswith(f"{capability.pin.capability_id}@")
            ):
                return True
            if capability.search is not None and entry.search == capability.search.query:
                return True
    return False


def _environment_aliases(environment: Environment) -> set[str]:
    return {entry.as_ for entry in _entries(environment) if entry.as_ is not None}


def _context_aliases(environment: Environment) -> set[str]:
    if environment.workspace is None:
        return set()
    return {
        item.as_
        for item in environment.workspace.context or ()
        if isinstance(item, ContextEntry) and item.as_ is not None
    }


def _with_plugin_provenance(
    node: NodeEnvironment, plugins: Sequence[PluginExpansion], mission_key: str
) -> dict[str, str]:
    provenance = dict(node.field_provenance)
    aliases = {
        entry.as_ for entry in node.effective_environment.plugins or () if entry.as_ is not None
    }
    for plugin in plugins:
        if plugin.mission_key == mission_key and plugin.alias in aliases:
            for member in plugin.members:
                provenance[f"plugins.{plugin.alias}.{member.partition('@')[0]}"] = (
                    f"plugin:{plugin.alias}"
                )
    return provenance


def _unique(issues: Iterable[ManifestIssue]) -> tuple[ManifestIssue, ...]:
    seen: dict[tuple[str, str, str], ManifestIssue] = {}
    for issue in issues:
        seen.setdefault((issue.code.value, issue.pointer, issue.message), issue)
    return tuple(seen.values())


__all__ = [
    "ManifestCompilation",
    "ManifestCompileService",
    "ManifestProgramCompiler",
    "ManifestScope",
    "OverlayDefinitionRepository",
]


# ------------------------------------------------------------------------------------------
# The tenant-scoped facade the CLI (through HTTP), HTTP and MCP share
# ------------------------------------------------------------------------------------------

READ_GRANTS = frozenset({"workflow_run.read", "mission.read"})
CATALOG_GRANTS = frozenset({"catalog:read", "catalog.read"})


class ManifestPermissionDenied(PermissionError):
    code = "unauthorized"


def require_any(permissions: frozenset[str], grants: frozenset[str], what: str) -> None:
    if not permissions & grants:
        raise ManifestPermissionDenied(f"{what} requires one of {sorted(grants)}")


class ManifestLifecyclePort(Protocol):
    """Submit and start (FT-E3), composed beside compile when the installation allows them."""

    @property
    def request_scope(self) -> str: ...

    async def submit(self, request: Any) -> tuple[Any, bool]: ...

    async def start(
        self, run_id: str, actor: Any, *, family_input: dict[str, Any] | None = None
    ) -> Any: ...

    async def head_run(self, mission_id: Any) -> str | None: ...


class MissionManifestService:
    """Compile, submit and start Mission Manifests under one tenant scope."""

    def __init__(
        self,
        *,
        compiler: ManifestCompileService,
        request_scope: str,
        lifecycle: ManifestLifecyclePort | None = None,
    ) -> None:
        parse_request_scope(request_scope)
        if lifecycle is not None and lifecycle.request_scope != request_scope:
            raise ValueError("submit and start must run under the compile scope")
        self._compiler = compiler
        self._scope = request_scope
        self.lifecycle = lifecycle

    @property
    def request_scope(self) -> str:
        return self._scope

    async def compile(
        self, manifest_yaml: str, *, actor_id: str, permissions: frozenset[str]
    ) -> ManifestCompilation:
        require_any(permissions, READ_GRANTS, "manifest compile")
        require_any(permissions, CATALOG_GRANTS, "manifest compile")
        return await self._compiler.compile(
            manifest_yaml, ManifestScope(request_scope=self._scope, actor_id=actor_id)
        )
