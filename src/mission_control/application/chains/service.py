"""Catalog-free manifest compile: structure, inheritance and Mission Chain links.

``ManifestStructureService.compile`` is the deterministic part of SPEC-05's compile
algorithm that needs no catalog (steps 1, 5 and 7 for structure, plus SPEC-04 link
validation): it parses the manifest, computes every node's effective environment, lowers
each mission to ``MissionDefinition@1`` and validates the chain. It persists nothing.
Capability search-to-pin resolution and lane support (FT-E2) are reported as
``catalog_resolution: "deferred"`` until the manifest compile service adds them.
"""

from __future__ import annotations

from typing import Literal

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
from mission_control.domain.composition.chain import ChainResolution, compile_chain


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
