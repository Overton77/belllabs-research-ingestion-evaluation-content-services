"""Catalog-free manifest compile: structure, inheritance and Mission Chain links.

``ManifestStructureService.compile`` is the deterministic part of SPEC-05's compile
algorithm that needs no catalog (steps 1, 5 and 7 for structure, plus SPEC-04 link
validation): it parses the manifest, computes every node's effective environment, lowers
each mission to ``MissionDefinition@1`` and validates the chain. It persists nothing.
Capability search-to-pin resolution and lane support (FT-E2) are reported as
``catalog_resolution: "deferred"`` until the manifest compile service adds them.
"""

from __future__ import annotations

from typing import Literal, Protocol
from uuid import UUID

from pydantic import BaseModel, ConfigDict

from mission_control.domain.authoring.manifest import (
    Environment,
    Lane,
    ManifestIssue,
    ManifestRejected,
    parse_manifest_yaml,
    resolve_environments,
)
from mission_control.domain.authoring.mission_definition import (
    DefinitionCapability,
    manifest_to_definition,
)
from mission_control.domain.composition.chain import (
    ChainResolution,
    MissionChain,
    compile_chain,
)
from mission_control.domain.policies.contracts import ActorContext


class ReportModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class ResolvedNode(ReportModel):
    mission_key: str
    node_key: str
    role: Literal["node", "verifier"]
    pointer: str
    lane: Lane
    lane_changed_from: Lane | None = None
    effective_environment: Environment
    field_provenance: dict[str, str]


class MissionCapabilities(ReportModel):
    mission_key: str
    capabilities: tuple[DefinitionCapability, ...]


class ManifestResolution(ReportModel):
    """``mc.manifest_resolution.v1`` (structural part; E2 fills pins, lanes and hooks)."""

    schema_version: Literal["mc.manifest_resolution.v1"] = "mc.manifest_resolution.v1"
    manifest_digest: str
    catalog_resolution: Literal["deferred", "resolved"] = "deferred"
    capabilities: tuple[MissionCapabilities, ...] = ()
    nodes: tuple[ResolvedNode, ...] = ()
    chain: ChainResolution | None = None
    blockers: tuple[ManifestIssue, ...] = ()
    warnings: tuple[ManifestIssue, ...] = ()


class DefinitionSummary(ReportModel):
    mission_key: str
    pointer: str
    definition_digest: str
    is_resolved: bool


class ManifestValidationReport(ReportModel):
    ok: bool
    manifest_digest: str | None = None
    blockers: tuple[ManifestIssue, ...] = ()
    warnings: tuple[ManifestIssue, ...] = ()
    definitions: tuple[DefinitionSummary, ...] = ()
    resolution: ManifestResolution | None = None


class ManifestStructureService:
    """Structural compile for ``missionctl mission compile`` and the chain verbs."""

    def compile(self, manifest_yaml: str) -> ManifestValidationReport:
        try:
            manifest, document = parse_manifest_yaml(manifest_yaml)
        except ManifestRejected as rejected:
            return ManifestValidationReport(ok=False, blockers=rejected.issues)
        structure = resolve_environments(manifest, document)
        chain = compile_chain(manifest)
        environments = {item.pointer: item for item in structure.missions}
        definitions = [
            manifest_to_definition(
                block,
                pointer=pointer,
                digest=structure.manifest_digest,
                environments=environments[pointer],
            )
            for pointer, block in manifest.mission_blocks()
        ]
        blockers = structure.blockers + chain.blockers
        warnings = structure.warnings + chain.warnings
        resolution = ManifestResolution(
            manifest_digest=structure.manifest_digest,
            capabilities=tuple(
                MissionCapabilities(mission_key=item.mission_key, capabilities=item.capabilities)
                for item in definitions
            ),
            nodes=tuple(
                ResolvedNode(
                    mission_key=mission.mission_key,
                    node_key=node.node_key,
                    role=node.role,
                    pointer=node.pointer,
                    lane=node.lane,
                    lane_changed_from=node.lane_changed_from,
                    effective_environment=node.effective_environment,
                    field_provenance=node.field_provenance,
                )
                for mission in structure.missions
                for node in mission.nodes
            ),
            chain=chain.resolution,
            blockers=blockers,
            warnings=warnings,
        )
        return ManifestValidationReport(
            ok=not blockers,
            manifest_digest=structure.manifest_digest,
            blockers=blockers,
            warnings=warnings,
            definitions=tuple(
                DefinitionSummary(
                    mission_key=item.mission_key,
                    pointer=item.manifest_pointer,
                    definition_digest=item.digest,
                    is_resolved=item.is_resolved,
                )
                for item in definitions
            ),
            resolution=resolution,
        )


# ---------------------------------------------------------------------------
# Inspection (FT-D2): ``missionctl chain inspect``, ``GET /chains/{id}``,
# ``mission_chain_inspect`` and ``mc://applications/{app}/chains/{id}``.
# ---------------------------------------------------------------------------


class ChainMemberRun(ReportModel):
    """One member mission with its current run, if any."""

    mission_key: str
    mission_id: UUID
    order: int
    status: Literal["terminal", "active", "waiting", "unreachable"]
    run_id: UUID | None = None
    run_key: str | None = None
    phase: str | None = None
    terminal_outcome: str | None = None


class ChainInspection(ReportModel):
    """The ``mc.chain.v1`` projection plus each member's run (read-only)."""

    schema_version: Literal["mc.chain_inspection.v1"] = "mc.chain_inspection.v1"
    chain: MissionChain
    members: tuple[ChainMemberRun, ...]


class ChainNotFound(LookupError):
    code = "chain_not_found"


class ChainReadPort(Protocol):
    async def inspect(self, request_scope: str, chain_id: UUID) -> ChainInspection | None: ...

    async def chains_for_mission(
        self, request_scope: str, mission_id: UUID
    ) -> tuple[ChainInspection, ...]: ...


CHAIN_READ_PERMISSION = "workflow_run.read"


class ChainInspectionService:
    """Read-only chain projection shared by the CLI (through HTTP), HTTP and MCP."""

    def __init__(self, chains: ChainReadPort, *, request_scope: str) -> None:
        if not request_scope:
            raise ValueError("authenticated request scope is required")
        self._chains = chains
        self._scope = request_scope

    @property
    def request_scope(self) -> str:
        return self._scope

    async def inspect(self, chain_id: UUID, actor: ActorContext) -> ChainInspection:
        _require_read(actor)
        found = await self._chains.inspect(self._scope, chain_id)
        if found is None:
            raise ChainNotFound(f"chain not found: {chain_id}")
        return found

    async def chains_for_mission(
        self, mission_id: UUID, actor: ActorContext
    ) -> tuple[ChainInspection, ...]:
        _require_read(actor)
        return await self._chains.chains_for_mission(self._scope, mission_id)


def _require_read(actor: ActorContext) -> None:
    if CHAIN_READ_PERMISSION not in actor.permissions:
        raise PermissionError(f"{CHAIN_READ_PERMISSION} is required to read chains")
