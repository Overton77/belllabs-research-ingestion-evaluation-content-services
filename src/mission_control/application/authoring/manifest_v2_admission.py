"""`mission/v2` compile admission (VALIDATION V01; ARCHITECTURE "Profile identity and
capability admission").

Every node role of a ``mission/v2`` manifest is admitted against its lane profile's
**declared** matrix (``harness.describe.declared_matrix``): its ``requires`` controls and
observation features through ``admit_requirements``, its approvals and ``all_writes_gated``
through ``admit_gate_coverage``. Each refusal is a blocker whose JSON pointer names the manifest
entry that required the feature (the nearest environment that set it), raised at compile,
before anything is submitted or any provider is created; nothing degrades to a weaker lane or
mode. Hosted profiles (``claude_cloud``, ``codex_cloud``) compile structurally, but a warning
records that their launch is refused (MP-18/19 are evidence-blocked,
``docs/qualification/lanes/*/REVALIDATION-2026-10-09.md``). An authored workspace policy is
admitted per local profile (``workspaces.policy.admit_policy``).

Pure over its inputs: the program tree and the resolved node environments.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Final

from mission_control.application.execution.approvals_coverage import (
    GATE_COVERAGE_UNENFORCEABLE,
    GateCoverageRequirement,
    admit_gate_coverage,
)
from mission_control.application.execution.harness.describe import declared_matrix
from mission_control.application.workspaces.errors import WorkspaceError
from mission_control.application.workspaces.policy import admit_policy, policy_pin
from mission_control.domain.authoring.manifest import (
    Behavior,
    Lane,
    ManifestErrorCode,
    ManifestIssue,
    MissionBlock,
    NodeEnvironment,
    walk_program,
)
from mission_control.domain.authoring.manifest_v2 import EnvironmentV2, RequiredFeatures
from mission_control.domain.execution.lane_requirements import (
    RequirementIssue,
    RequirementSet,
    admit_requirements,
)
from mission_control.domain.execution.lanes import HOSTED_PROFILES

# The agentic lanes run the behaviors an executor lane runs (as Cursor does); program-only
# behaviors (swarm, evaluator) stay Deep Agents-only until their own families exist.
AGENTIC_BEHAVIORS: Final = frozenset(
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
V2_LANE_BEHAVIORS: Final[Mapping[Lane, frozenset[Behavior]]] = {
    Lane.DEEP_AGENTS: frozenset(Behavior),
    Lane.CURSOR_LOCAL: AGENTIC_BEHAVIORS,
    Lane.CURSOR_CLOUD: AGENTIC_BEHAVIORS,
    Lane.CLAUDE_AGENT_SDK: AGENTIC_BEHAVIORS,
    Lane.CODEX: AGENTIC_BEHAVIORS,
    Lane.CLAUDE_CLOUD: AGENTIC_BEHAVIORS,
    Lane.CODEX_CLOUD: AGENTIC_BEHAVIORS,
}
EVIDENCE_BLOCKED_HOSTED: Final = frozenset({Lane.CLAUDE_CLOUD, Lane.CODEX_CLOUD})
HOSTED_EVIDENCE = {
    Lane.CLAUDE_CLOUD: "docs/qualification/lanes/claude_cloud/REVALIDATION-2026-10-09.md (MP-18)",
    Lane.CODEX_CLOUD: "docs/qualification/lanes/codex_cloud/REVALIDATION-2026-10-09.md (MP-19)",
}


def _issue(pointer: str, message: str, reason: str) -> ManifestIssue:
    return ManifestIssue(
        code=ManifestErrorCode.UNSUPPORTED_BEHAVIOR, pointer=pointer, message=message, reason=reason
    )


def environment_chains(
    block: MissionBlock, mission_pointer: str, nodes: Sequence[NodeEnvironment]
) -> dict[tuple[str, str], tuple[tuple[str, Mapping[str, str]], ...]]:
    """Per node role, its environment pointers and field provenance from itself to the root.

    The first entry whose provenance marks a field ``overlay`` is the manifest location that
    set it; a field inherited all the way comes from the mission environment.
    """

    by_role = {(node.node_key, node.role): node for node in nodes}
    parents: dict[str, str | None] = {}
    for visit in walk_program(block.program, f"{mission_pointer}/program"):
        parents[visit.node.key] = visit.parent_key
    chains: dict[tuple[str, str], tuple[tuple[str, Mapping[str, str]], ...]] = {}
    for (key, role), node in by_role.items():
        chain: list[tuple[str, Mapping[str, str]]] = [
            (f"{node.pointer}/environment", node.field_provenance)
        ]
        current: str | None = key if role == "verifier" else parents.get(key)
        while current is not None:
            ancestor = by_role.get((current, "node"))
            if ancestor is None:
                break
            chain.append((f"{ancestor.pointer}/environment", ancestor.field_provenance))
            current = parents.get(current)
        chain.append((f"{mission_pointer}/environment", {}))
        chains[(key, role)] = tuple(chain)
    return chains


def field_source(chain: Sequence[tuple[str, Mapping[str, str]]], field: str) -> str:
    """The environment pointer (``.../environment``) that set ``field`` for this node role."""

    for pointer, provenance in chain[:-1]:
        if any(
            origin == "overlay" and (name == field or name.startswith(field + "."))
            for name, origin in provenance.items()
        ):
            return pointer
    return chain[-1][0]


def _converted(
    issues: Sequence[RequirementIssue], pointer: str | None = None
) -> list[ManifestIssue]:
    return [
        _issue(pointer or item.pointer, f"{item.code}: {item.message}", item.code)
        for item in issues
    ]


def admit_node(
    node: NodeEnvironment, chain: Sequence[tuple[str, Mapping[str, str]]]
) -> tuple[list[ManifestIssue], list[ManifestIssue]]:
    """V01 for one node role: blockers and warnings, every one pointed."""

    blockers: list[ManifestIssue] = []
    warnings: list[ManifestIssue] = []
    environment = node.effective_environment
    if not isinstance(environment, EnvironmentV2):
        return blockers, warnings
    lane = node.lane
    describe = declared_matrix(lane.value)
    requires = environment.requires or RequiredFeatures()
    for group in ("controls", "observation"):
        values = getattr(requires, group)
        if values:
            base = field_source(chain, f"requires.{group}") + "/requires"
            blockers.extend(
                _converted(
                    admit_requirements(describe, RequirementSet(**{group: values}), pointer=base)
                )
            )
    if requires.approvals:
        base = field_source(chain, "requires.approvals") + "/requires"
        blockers.extend(
            _converted(
                admit_gate_coverage(
                    describe,
                    GateCoverageRequirement(approvals=requires.approvals),
                    pointer=base,
                )
            )
        )
    if requires.all_writes_gated:
        gate = field_source(chain, "requires.all_writes_gated") + "/requires/all_writes_gated"
        coverage = admit_gate_coverage(
            describe, GateCoverageRequirement(all_writes_gated=True), pointer=gate
        )
        blockers.extend(
            _converted(
                [item for item in coverage if item.code == GATE_COVERAGE_UNENFORCEABLE], gate
            )
        )
    policy = environment.workspace.policy if environment.workspace is not None else None
    if policy is not None and lane.value not in HOSTED_PROFILES:
        try:
            admit_policy(lane.value, policy_pin(policy))
        except WorkspaceError as error:
            blockers.append(
                _issue(
                    field_source(chain, "workspace.policy") + "/workspace/policy",
                    error.message,
                    error.code,
                )
            )
    if lane in EVIDENCE_BLOCKED_HOSTED:
        warnings.append(
            ManifestIssue(
                code=ManifestErrorCode.UNSUPPORTED_BEHAVIOR,
                pointer=field_source(chain, "lane") + "/lane",
                message=(
                    f"lane {lane.value} compiles structurally but its launch is refused: the "
                    f"provider-hosted lifecycle is evidence-blocked ({HOSTED_EVIDENCE[lane]})"
                ),
                reason="hosted_launch_refused",
            )
        )
    return blockers, warnings


def admit_mission(
    block: MissionBlock, mission_pointer: str, nodes: Sequence[NodeEnvironment]
) -> tuple[list[ManifestIssue], list[ManifestIssue]]:
    blockers: list[ManifestIssue] = []
    warnings: list[ManifestIssue] = []
    chains = environment_chains(block, mission_pointer, nodes)
    for node in nodes:
        node_blockers, node_warnings = admit_node(node, chains[(node.node_key, node.role)])
        blockers.extend(node_blockers)
        warnings.extend(node_warnings)
    return blockers, warnings


__all__ = [
    "AGENTIC_BEHAVIORS",
    "EVIDENCE_BLOCKED_HOSTED",
    "V2_LANE_BEHAVIORS",
    "admit_mission",
    "admit_node",
    "environment_chains",
    "field_source",
]
