"""Mission Manifest v2 (``mc.mission_manifest.v2``): the multi-provider authoring surface.

``mission/v2`` is ``mission/v1`` plus an explicit execution environment, authentication
profile, workspace policy, continuation policy and required features on every Environment
(multi-provider SPEC-02 "Proposed authoring fragment"). Every v1 key keeps its v1 meaning;
v1 documents are parsed by :mod:`manifest` unchanged and never gain these keys. Both versions
share one program-node tree and one inheritance algorithm (:func:`manifest.resolve_environments`
works on either), so a v2 manifest lowers through the same immutable mission definitions.

Pure: no catalog access, no compile, no persistence. Structural validity here is not launch
readiness; resolving ``auth``/``execution_environment`` references to a sealed
``mc.execution_binding.v2`` is the compile/launch service's job (MP-02/MP-03).
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from decimal import Decimal
from typing import Annotated, Any, ClassVar, Final, Literal, get_args

from pydantic import ConfigDict, Field, ValidationError, model_validator

from mission_control.domain.authoring.manifest import (
    HOSTED_LANES,
    MANIFEST_LITERAL,
    AgentExecutorNode,
    ChildMissionInvocationNode,
    ChildSource,
    DeterministicExecutorNode,
    Environment,
    EvaluatorOptimizerNode,
    EventWaitNode,
    GoalLoopNode,
    HumanGateNode,
    Lane,
    ManifestErrorCode,
    ManifestIssue,
    ManifestModel,
    ManifestRejected,
    MissionBlock,
    MissionManifest,
    ParallelSwarmNode,
    ProfileId,
    ProofGateNode,
    StageGraphNode,
    TimerNode,
    Verifier,
    WorkspaceSelection,
    _issue,
    issues_from_validation_error,
    load_manifest_yaml,
)
from mission_control.domain.execution.lanes import (
    DELIVERY_COMMANDS,
    ApprovalMode,
    ObservationFeature,
)

MANIFEST_V2_SCHEMA_ID: Final = "mc.mission_manifest.v2"
MANIFEST_V2_LITERAL: Final = "mission/v2"
MANIFEST_V2_SCHEMA_FILENAME: Final = f"{MANIFEST_V2_SCHEMA_ID}.json"

# Which provider owns which hosted lane; `execution_environment.provider` must match.
PROVIDER_OF_HOSTED_LANE: Final[dict[Lane, str]] = {
    Lane.CURSOR_CLOUD: "cursor",
    Lane.CLAUDE_CLOUD: "anthropic",
    Lane.CODEX_CLOUD: "openai",
}
HostedProvider = Literal["cursor", "anthropic", "openai"]
WorkspaceMode = Literal["managed_worktree", "provider_workspace", "shared_checkout"]
WorkspaceReuse = Literal["none", "within_run", "within_session"]
DirtyInputPolicy = Literal["reject", "snapshot"]
CleanupPolicy = Literal["retain_until_artifacts_registered", "retain", "immediate"]
NativeCompactionPolicy = Literal["preferred", "disabled", "required"]
ContinuationFallback = Literal["sealed_checkpoint", "fail"]
RequiredControl = Literal[
    "queue_instruction",
    "interrupt_and_inject",
    "pause",
    "hard_pause",
    "resume",
    "cancel",
    "fork",
    "request_continuation",
]
if set(get_args(RequiredControl)) != set(DELIVERY_COMMANDS):  # pragma: no cover
    raise RuntimeError("manifest v2 required controls drifted from the lane delivery commands")


class AuthSelection(ManifestModel):
    """The authentication/billing route a node runs under; a profile reference, never a
    credential. Local CLI sign-in, API credentials and provider cloud accounts are distinct
    routes resolved by the deployment registry (SPEC-02 "Authentication")."""

    profile: ProfileId


class SetupSelection(ManifestModel):
    pin: str = Field(min_length=1, max_length=512)


class ExecutionEnvironmentSelection(ManifestModel):
    """``local_workspace`` (a worker-hosted profile) or ``provider_hosted`` (a published
    provider cloud environment). ``provider`` must match the lane; a local ``path`` is
    invalid for hosted work (SPEC-02)."""

    model_config = ConfigDict(
        json_schema_extra={
            "oneOf": [
                {"properties": {"kind": {"const": "local_workspace"}}, "required": ["kind"]},
                {
                    "properties": {"kind": {"const": "provider_hosted"}},
                    "required": ["kind", "provider", "environment_ref"],
                },
            ]
        }
    )

    kind: Literal["local_workspace", "provider_hosted"]
    profile: ProfileId | None = None
    provider: HostedProvider | None = None
    environment_ref: str | None = Field(default=None, min_length=1, max_length=512)
    expected_revision: str | None = Field(default=None, min_length=1, max_length=512)
    setup: SetupSelection | None = None

    @model_validator(mode="after")
    def _shape_by_kind(self) -> ExecutionEnvironmentSelection:
        if self.kind == "local_workspace":
            hosted_only = [
                name
                for name in ("provider", "environment_ref", "expected_revision", "setup")
                if getattr(self, name) is not None
            ]
            if hosted_only:
                raise ValueError(
                    f"a local_workspace environment does not take {', '.join(hosted_only)}"
                )
        else:
            if self.provider is None or self.environment_ref is None:
                raise ValueError("a provider_hosted environment needs provider and environment_ref")
            if self.profile is not None:
                raise ValueError("a provider_hosted environment does not take a local profile")
        return self


class WorkspacePolicy(ManifestModel):
    """Mission Control allocates the workspace; the agent does not choose whether isolation
    exists (SPEC-02 "Worktrees and snapshots")."""

    mode: WorkspaceMode = "managed_worktree"
    reuse: WorkspaceReuse = "within_run"
    dirty_input: DirtyInputPolicy = "reject"
    cleanup: CleanupPolicy = "retain_until_artifacts_registered"


class WorkspaceSelectionV2(WorkspaceSelection):
    policy: WorkspacePolicy | None = None


class ContinuationPolicy(ManifestModel):
    """Context pressure policy: soft/hard watermarks, native compaction preference and the
    sealed-checkpoint fallback (SPEC-01 "Four independent progress mechanisms"). The
    defaults are qualification tuning inputs, not provider guarantees."""

    native_compaction: NativeCompactionPolicy = "preferred"
    fallback: ContinuationFallback = "sealed_checkpoint"
    soft_context_ratio: Decimal = Field(default=Decimal("0.70"), gt=0, le=1, decimal_places=4)
    hard_context_ratio: Decimal = Field(default=Decimal("0.85"), gt=0, le=1, decimal_places=4)
    reserve_ratio: Decimal = Field(default=Decimal("0.15"), ge=0, lt=1, decimal_places=4)
    max_session_turns: int | None = Field(default=None, ge=1)
    max_transfers: int | None = Field(default=None, ge=0)
    max_compaction_failures: int = Field(default=2, ge=0)

    @model_validator(mode="after")
    def _ordered_watermarks(self) -> ContinuationPolicy:
        if self.soft_context_ratio >= self.hard_context_ratio:
            raise ValueError("soft_context_ratio must be below hard_context_ratio")
        if self.hard_context_ratio + self.reserve_ratio > 1:
            raise ValueError("hard_context_ratio plus reserve_ratio cannot exceed 1")
        return self


class RequiredFeatures(ManifestModel):
    """What the workflow needs the lane to actually provide. Admission intersects these
    with the lane's describe (:mod:`domain.execution.lane_requirements`); a required feature
    the lane cannot provide is a pointed error, never a silent fallback (V01)."""

    controls: tuple[RequiredControl, ...] = ()
    approvals: tuple[ApprovalMode, ...] = ()
    observation: tuple[ObservationFeature, ...] = ()

    @model_validator(mode="after")
    def _unique(self) -> RequiredFeatures:
        for name in ("controls", "approvals", "observation"):
            values = getattr(self, name)
            if len(set(values)) != len(values):
                raise ValueError(f"requires.{name} must not repeat entries")
        return self

    def as_sets(self) -> dict[str, frozenset[str]]:
        return {
            "controls": frozenset(self.controls),
            "approvals": frozenset(self.approvals),
            "observation": frozenset(self.observation),
        }


class EnvironmentV2(Environment):
    """A v1 Environment plus the multi-provider selections. Every field stays optional so a
    node can overlay any subset; the v2 lane rules apply to the effective environment."""

    auth: AuthSelection | None = None
    execution_environment: ExecutionEnvironmentSelection | None = None
    workspace: WorkspaceSelectionV2 | None = None
    continuation: ContinuationPolicy | None = None
    requires: RequiredFeatures | None = None

    # v2 admits every lane profile in validation; whether the lane is registered, qualified
    # and account-enabled is decided at compile/admission with evidence, not here.
    reserved_lanes: ClassVar[frozenset[Lane]] = frozenset()

    def version_lane_issues(
        self, env_pointer: str, *, mission_level: bool
    ) -> tuple[list[ManifestIssue], list[ManifestIssue]]:
        blockers: list[ManifestIssue] = []
        lane = self.lane
        if lane is None:
            return blockers, []
        hosted = lane in HOSTED_LANES
        execution = self.execution_environment
        if execution is not None:
            if hosted and execution.kind != "provider_hosted":
                blockers.append(
                    _issue(
                        f"{env_pointer}/execution_environment/kind",
                        f"lane {lane.value} is provider-hosted and needs kind provider_hosted",
                        "placement_mismatch",
                        ManifestErrorCode.UNSUPPORTED_BEHAVIOR,
                    )
                )
            if not hosted and execution.kind != "local_workspace":
                blockers.append(
                    _issue(
                        f"{env_pointer}/execution_environment/kind",
                        f"lane {lane.value} runs on a worker and needs kind local_workspace",
                        "placement_mismatch",
                        ManifestErrorCode.UNSUPPORTED_BEHAVIOR,
                    )
                )
            expected_provider = PROVIDER_OF_HOSTED_LANE.get(lane)
            if (
                execution.kind == "provider_hosted"
                and expected_provider is not None
                and execution.provider != expected_provider
            ):
                blockers.append(
                    _issue(
                        f"{env_pointer}/execution_environment/provider",
                        f"lane {lane.value} is hosted by {expected_provider}, "
                        f"not {execution.provider}",
                        "provider_mismatch",
                        ManifestErrorCode.UNSUPPORTED_BEHAVIOR,
                    )
                )
        elif mission_level and lane is not Lane.DEEP_AGENTS:
            blockers.append(
                _issue(
                    f"{env_pointer}/execution_environment",
                    f"a mission/v2 {lane.value} environment declares execution_environment",
                    "missing_field",
                )
            )
        workspace = self.workspace
        if hosted and workspace is not None:
            if workspace.repo is not None and workspace.repo.path is not None:
                blockers.append(
                    _issue(
                        f"{env_pointer}/workspace/repo/path",
                        "a local path is invalid for provider-hosted work; bind url and ref",
                        "hosted_local_path",
                        ManifestErrorCode.UNSUPPORTED_BEHAVIOR,
                    )
                )
            if workspace.policy is not None and workspace.policy.mode == "managed_worktree":
                blockers.append(
                    _issue(
                        f"{env_pointer}/workspace/policy/mode",
                        "managed_worktree is not valid for a provider-hosted lane; "
                        "use provider_workspace",
                        "hosted_worktree",
                        ManifestErrorCode.UNSUPPORTED_BEHAVIOR,
                    )
                )
        if (
            not hosted
            and workspace is not None
            and workspace.policy is not None
            and workspace.policy.mode == "provider_workspace"
        ):
            blockers.append(
                _issue(
                    f"{env_pointer}/workspace/policy/mode",
                    f"provider_workspace needs a provider-hosted lane, not {lane.value}",
                    "local_provider_workspace",
                    ManifestErrorCode.UNSUPPORTED_BEHAVIOR,
                )
            )
        if lane in (Lane.CLAUDE_AGENT_SDK, Lane.CODEX, *HOSTED_LANES) and (
            workspace is None or workspace.repo is None
        ):
            blockers.append(
                _issue(
                    f"{env_pointer}/workspace/repo",
                    f"lane {lane.value} requires workspace.repo",
                    "missing_field",
                )
            )
        return blockers, []

    def version_narrowing_issues(
        self, child: Environment, overlay: Environment, pointer: str
    ) -> list[ManifestIssue]:
        """A child cannot drop a parent's required features, weaken dirty-input handling or
        widen its continuation transfer ceiling (SPEC-02 inheritance)."""

        issues: list[ManifestIssue] = []
        if not isinstance(child, EnvironmentV2) or not isinstance(overlay, EnvironmentV2):
            return issues
        if self.requires is not None and overlay.requires is not None:
            parent_sets = self.requires.as_sets()
            child_sets = (child.requires or RequiredFeatures()).as_sets()
            for name, required in parent_sets.items():
                dropped = sorted(required - child_sets[name])
                if dropped:
                    issues.append(
                        _issue(
                            f"{pointer}/requires/{name}",
                            f"requires.{name} drops inherited {', '.join(dropped)}",
                            "widens_authority",
                        )
                    )
        parent_policy = self.workspace.policy if self.workspace is not None else None
        overlay_policy = overlay.workspace.policy if overlay.workspace is not None else None
        if (
            parent_policy is not None
            and overlay_policy is not None
            and "dirty_input" in overlay_policy.model_fields_set
            and parent_policy.dirty_input == "reject"
            and overlay_policy.dirty_input != "reject"
        ):
            issues.append(
                _issue(
                    f"{pointer}/workspace/policy/dirty_input",
                    "dirty_input cannot be weakened below the inherited reject",
                    "widens_authority",
                )
            )
        if (
            self.continuation is not None
            and overlay.continuation is not None
            and "max_transfers" in overlay.continuation.model_fields_set
            and self.continuation.max_transfers is not None
            and overlay.continuation.max_transfers is not None
            and overlay.continuation.max_transfers > self.continuation.max_transfers
        ):
            issues.append(
                _issue(
                    f"{pointer}/continuation/max_transfers",
                    f"continuation.max_transfers {overlay.continuation.max_transfers} widens "
                    f"the inherited limit {self.continuation.max_transfers}",
                    "widens_authority",
                )
            )
        return issues


# --- The v2 program tree: the v1 nodes with v2 Environments ------------------------------


class VerifierV2(Verifier):
    environment: EnvironmentV2 | None = None


class StageGraphNodeV2(StageGraphNode):
    environment: EnvironmentV2 | None = None
    nodes: tuple[ProgramNodeV2, ...] = Field(min_length=1)


class GoalLoopNodeV2(GoalLoopNode):
    environment: EnvironmentV2 | None = None
    verifier: VerifierV2 | None = None


class ParallelSwarmNodeV2(ParallelSwarmNode):
    environment: EnvironmentV2 | None = None


class EvaluatorOptimizerNodeV2(EvaluatorOptimizerNode):
    environment: EnvironmentV2 | None = None


class AgentExecutorNodeV2(AgentExecutorNode):
    environment: EnvironmentV2 | None = None


class DeterministicExecutorNodeV2(DeterministicExecutorNode):
    environment: EnvironmentV2 | None = None


class HumanGateNodeV2(HumanGateNode):
    environment: EnvironmentV2 | None = None


class EventWaitNodeV2(EventWaitNode):
    environment: EnvironmentV2 | None = None


class TimerNodeV2(TimerNode):
    environment: EnvironmentV2 | None = None


class ProofGateNodeV2(ProofGateNode):
    environment: EnvironmentV2 | None = None


class ChildSourceV2(ChildSource):
    inline: MissionBlockV2 | None = None


class ChildMissionInvocationNodeV2(ChildMissionInvocationNode):
    environment: EnvironmentV2 | None = None
    child: ChildSourceV2


ProgramNodeV2 = Annotated[
    StageGraphNodeV2
    | GoalLoopNodeV2
    | ParallelSwarmNodeV2
    | EvaluatorOptimizerNodeV2
    | AgentExecutorNodeV2
    | DeterministicExecutorNodeV2
    | HumanGateNodeV2
    | EventWaitNodeV2
    | TimerNodeV2
    | ProofGateNodeV2
    | ChildMissionInvocationNodeV2,
    Field(discriminator="behavior"),
]


class MissionBlockV2(MissionBlock):
    environment: EnvironmentV2
    program: ProgramNodeV2


class MissionManifestV2(MissionManifest):
    """``mc.mission_manifest.v2``: one mission or a chain, with v2 Environments."""

    model_config = ConfigDict(
        title="Mission Manifest v2",
        json_schema_extra={
            "oneOf": [
                {"required": ["mission"], "not": {"required": ["links"]}},
                {"required": ["missions", "links"]},
            ]
        },
    )

    # The one deliberate Literal narrowing-by-replacement: v2 is a different manifest version.
    manifest: Literal["mission/v2"]  # type: ignore[assignment]
    mission: MissionBlockV2 | None = None
    missions: tuple[MissionBlockV2, ...] | None = Field(default=None, min_length=2)


for _model in (StageGraphNodeV2, ChildSourceV2, ChildMissionInvocationNodeV2, MissionBlockV2):
    _model.model_rebuild()


# --- Parsing ------------------------------------------------------------------------------

MANIFEST_VERSIONS: Final[dict[str, type[MissionManifest]]] = {
    MANIFEST_LITERAL: MissionManifest,
    MANIFEST_V2_LITERAL: MissionManifestV2,
}


def parse_manifest_any(document: Mapping[str, Any]) -> MissionManifest:
    """Shape-validate a parsed document of any supported ``manifest`` version.

    ``mission/v1`` documents go through the v1 model exactly as before (and so cannot carry
    v2 keys); ``mission/v2`` documents go through :class:`MissionManifestV2`. An unknown
    version is a pointed rejection at ``/manifest``.
    """

    version = document.get("manifest")
    model = MANIFEST_VERSIONS.get(version) if isinstance(version, str) else None
    if model is None:
        raise ManifestRejected(
            [
                ManifestIssue(
                    code=ManifestErrorCode.UNSUPPORTED_BEHAVIOR,
                    pointer="/manifest",
                    message=(
                        f"unsupported manifest version {version!r}; "
                        f"supported: {', '.join(sorted(MANIFEST_VERSIONS))}"
                    ),
                    reason="unsupported_manifest_version",
                )
            ]
        )
    try:
        return model.model_validate(dict(document))
    except ValidationError as error:
        raise ManifestRejected(issues_from_validation_error(error)) from error


def parse_manifest_yaml_any(text: str) -> tuple[MissionManifest, dict[str, Any]]:
    """Load and shape-validate manifest YAML of any supported version."""

    document = load_manifest_yaml(text)
    return parse_manifest_any(document), document


def manifest_v2_json_schema() -> dict[str, Any]:
    """The JSON Schema exported to ``contracts/schemas/mc.mission_manifest.v2.json``."""

    schema = MissionManifestV2.model_json_schema(by_alias=True, mode="validation")
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$id": f"https://mission-control.belllabs/schemas/{MANIFEST_V2_SCHEMA_FILENAME}",
        "x-mc-schema-id": MANIFEST_V2_SCHEMA_ID,
        **schema,
    }


def manifest_v2_json_schema_text() -> str:
    return (
        json.dumps(manifest_v2_json_schema(), indent=2, sort_keys=True, ensure_ascii=False) + "\n"
    )


__all__ = [
    "MANIFEST_V2_LITERAL",
    "MANIFEST_V2_SCHEMA_FILENAME",
    "MANIFEST_V2_SCHEMA_ID",
    "MANIFEST_VERSIONS",
    "PROVIDER_OF_HOSTED_LANE",
    "AuthSelection",
    "ContinuationPolicy",
    "EnvironmentV2",
    "ExecutionEnvironmentSelection",
    "MissionBlockV2",
    "MissionManifestV2",
    "RequiredFeatures",
    "SetupSelection",
    "WorkspacePolicy",
    "WorkspaceSelectionV2",
    "manifest_v2_json_schema",
    "manifest_v2_json_schema_text",
    "parse_manifest_any",
    "parse_manifest_yaml_any",
]
