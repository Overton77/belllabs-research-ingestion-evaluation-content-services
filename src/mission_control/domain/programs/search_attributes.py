"""BellLabs Temporal Search Attributes (REQ-CP-EXEC-015, `CON-CP-TEMPORAL-IDENTITY-V1`).

Search Attributes are the only join between PostgreSQL authority and Temporal executions.
They carry identifiers and hashes only: never checkpoint IDs, prompts, raw scopes, or
secrets. The policy travels in the root, family, and `OperationWorkflow` inputs (and
through Continue-As-New); workflow code never reads it from worker configuration. An
absent policy means `disabled`, which keeps every captured history replayable.
"""

from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256
from types import MappingProxyType
from typing import Final, Literal, cast

SearchAttributePolicy = Literal["required", "disabled"]
WorkflowKind = Literal["root", "family", "operation"]
SearchAttributeFamily = Literal["stage_graph", "goal_directed"]
SearchAttributeKind = Literal["keyword", "int", "keyword_list"]
# `mc_lane` values: the seven Lane Profiles (`domain/execution/lanes.LANE_PROFILES`, ADR-0035).
MissionLane = Literal[
    "deep_agents",
    "cursor_local",
    "cursor_cloud",
    "claude_agent_sdk",
    "codex",
    "claude_cloud",
    "codex_cloud",
]
MissionPhase = Literal["executing", "cancelling", "in_doubt", "completed", "cancelled", "failed"]

SEARCH_ATTRIBUTES_REQUIRED: Final = "required"
SEARCH_ATTRIBUTES_DISABLED: Final = "disabled"

RUN_ID: Final = "BellLabsRunId"
SCOPE_HASH: Final = "BellLabsScopeHash"
WORKFLOW_KIND: Final = "BellLabsWorkflowKind"
FAMILY: Final = "BellLabsFamily"
EXECUTION_EPOCH: Final = "BellLabsExecutionEpoch"
PARENT_RUN_ID: Final = "BellLabsParentRunId"
UNIT_KEY: Final = "BellLabsUnitKey"
UNIT_KIND: Final = "BellLabsUnitKind"
EXECUTION_GENERATION: Final = "BellLabsExecutionGeneration"

# FT-G7 (SPEC-07 section 4.5): the fast-track listing attributes (`missionctl run list
# --query "mc_lane='cursor_local' AND mc_phase='executing'"`). Identifiers only, never
# prompts, scopes or secrets.
MC_MISSION_ID: Final = "mc_mission_id"
MC_RUN_ID: Final = "mc_run_id"
MC_LANE: Final = "mc_lane"
MC_PHASE: Final = "mc_phase"
FORKED_FROM_RUN_ID: Final = "ForkedFromRunId"

MC_LANES: Final[tuple[MissionLane, ...]] = (
    "deep_agents",
    "cursor_local",
    "cursor_cloud",
    "claude_agent_sdk",
    "codex",
    "claude_cloud",
    "codex_cloud",
)
MC_PHASES: Final[tuple[MissionPhase, ...]] = (
    "executing",
    "cancelling",
    "in_doubt",
    "completed",
    "cancelled",
    "failed",
)

# Slot budget. The postgres12 (and SQLite) SQL visibility store pre-allocates, per
# namespace, 10 Keyword, 3 KeywordList and 3 Int custom columns (verified on the local
# Temporal 1.31 schema: keyword01..10, keywordlist01..03, int01..03). The BellLabs core
# takes 7 Keyword + 2 Int; the fast-track keys take the 3 remaining Keyword columns
# (`mc_mission_id`, `mc_lane`, `mc_phase`) and 2 KeywordList columns. `mc_run_id` is a
# single-element KeywordList (equality and IN filters behave as on a Keyword) and
# `ForkedFromRunId` holds the fork lineage, nearest source first, so
# `ForkedFromRunId = 'X'` lists every fork of X. One KeywordList column remains free.
BELLLABS_CORE_SEARCH_ATTRIBUTES: Final[MappingProxyType[str, SearchAttributeKind]] = (
    MappingProxyType(
        {
            RUN_ID: "keyword",
            SCOPE_HASH: "keyword",
            WORKFLOW_KIND: "keyword",
            FAMILY: "keyword",
            EXECUTION_EPOCH: "int",
            PARENT_RUN_ID: "keyword",
            UNIT_KEY: "keyword",
            UNIT_KIND: "keyword",
            EXECUTION_GENERATION: "int",
        }
    )
)
MISSION_VISIBILITY_SEARCH_ATTRIBUTES: Final[MappingProxyType[str, SearchAttributeKind]] = (
    MappingProxyType(
        {
            MC_MISSION_ID: "keyword",
            MC_RUN_ID: "keyword_list",
            MC_LANE: "keyword",
            MC_PHASE: "keyword",
            FORKED_FROM_RUN_ID: "keyword_list",
        }
    )
)
# The registry the operator path registers and workers verify (core + fast-track keys).
BELLLABS_SEARCH_ATTRIBUTES: Final[MappingProxyType[str, SearchAttributeKind]] = MappingProxyType(
    {**BELLLABS_CORE_SEARCH_ATTRIBUTES, **MISSION_VISIBILITY_SEARCH_ATTRIBUTES}
)
SQL_VISIBILITY_SLOTS: Final = MappingProxyType({"keyword": 10, "keyword_list": 3, "int": 3})

_WORKFLOW_FAMILIES: Final = MappingProxyType(
    {
        "StageGraph": "stage_graph",
        "GoalDirected": "goal_directed",
        "stage_graph": "stage_graph",
        "goal_directed": "goal_directed",
    }
)


class SearchAttributePolicyError(ValueError):
    """A composition supplied a Search Attribute policy its environment forbids."""


def search_attribute_scope_hash(request_scope: str) -> str:
    """`BellLabsScopeHash`: SHA-256 of the request scope; the raw scope is never indexed."""

    if not request_scope:
        raise ValueError("request scope must be non-empty")
    return "sha256:" + sha256(request_scope.encode("utf-8")).hexdigest()


def search_attribute_family(family: str) -> SearchAttributeFamily:
    try:
        return _WORKFLOW_FAMILIES[family]  # type: ignore[return-value]
    except KeyError as error:
        raise ValueError(f"undeclared BellLabs workflow family: {family}") from error


def require_production_search_attribute_policy(policy: str) -> SearchAttributePolicy:
    """Production and persistent qualification compositions start executions `required`."""

    if policy != SEARCH_ATTRIBUTES_REQUIRED:
        raise SearchAttributePolicyError(
            "production composition requires the Search Attribute policy 'required'"
        )
    return SEARCH_ATTRIBUTES_REQUIRED


@dataclass(frozen=True)
class BellLabsSearchAttributeValues:
    """The exact attribute values one root, family, or operation execution carries."""

    run_id: str
    scope_hash: str
    workflow_kind: WorkflowKind
    family: SearchAttributeFamily | None = None
    execution_epoch: int | None = None
    parent_run_id: str | None = None
    unit_key: str | None = None
    unit_kind: str | None = None
    execution_generation: int | None = None

    def __post_init__(self) -> None:
        if not self.run_id or not self.scope_hash.startswith("sha256:"):
            raise ValueError("Search Attributes require a run id and a scope hash")
        if self.execution_epoch is not None and self.execution_epoch < 1:
            raise ValueError("execution epoch must be positive")
        if self.execution_generation is not None and self.execution_generation < 1:
            raise ValueError("execution generation must be positive")
        if self.workflow_kind != "operation" and (
            self.unit_key is not None
            or self.unit_kind is not None
            or self.execution_generation is not None
        ):
            raise ValueError("unit attributes are set on operation executions only")

    def as_mapping(self) -> dict[str, str | int]:
        values: dict[str, str | int | None] = {
            RUN_ID: self.run_id,
            SCOPE_HASH: self.scope_hash,
            WORKFLOW_KIND: self.workflow_kind,
            FAMILY: self.family,
            EXECUTION_EPOCH: self.execution_epoch,
            PARENT_RUN_ID: self.parent_run_id,
            UNIT_KEY: self.unit_key,
            UNIT_KIND: self.unit_kind,
            EXECUTION_GENERATION: self.execution_generation,
        }
        return {name: value for name, value in values.items() if value is not None}


def run_search_attributes(
    *,
    workflow_kind: Literal["root", "family"],
    run_id: str,
    request_scope: str,
    family: str,
    execution_epoch: int,
    parent_run_id: str | None = None,
) -> BellLabsSearchAttributeValues:
    return BellLabsSearchAttributeValues(
        run_id=run_id,
        scope_hash=search_attribute_scope_hash(request_scope),
        workflow_kind=workflow_kind,
        family=search_attribute_family(family),
        execution_epoch=execution_epoch,
        parent_run_id=parent_run_id,
    )


def operation_search_attributes(
    *,
    run_id: str,
    request_scope: str,
    execution_generation: int,
    family: str | None = None,
    execution_epoch: int | None = None,
    unit_key: str | None = None,
    unit_kind: str | None = None,
) -> BellLabsSearchAttributeValues:
    return BellLabsSearchAttributeValues(
        run_id=run_id,
        scope_hash=search_attribute_scope_hash(request_scope),
        workflow_kind="operation",
        family=search_attribute_family(family) if family is not None else None,
        execution_epoch=execution_epoch,
        unit_key=unit_key,
        unit_kind=unit_kind,
        execution_generation=execution_generation,
    )


def lane_for_runtime(execution_runtime: str, lane_profile: str | None = None) -> MissionLane | None:
    """`mc_lane` of an operation: its Lane Profile, or `deep_agents` for the Deep Agents
    runtime; native (deterministic) operations run on no lane and carry no `mc_lane`."""

    if lane_profile is not None:
        if lane_profile not in MC_LANES:
            raise ValueError(f"undeclared lane profile: {lane_profile}")
        return lane_profile
    if execution_runtime == "deep_agent":
        return "deep_agents"
    return None


def phase_for_disposition(disposition: str) -> MissionPhase:
    """`mc_phase` written at an operation's closing boundary."""

    if disposition in {"completed", "cancelled", "failed", "in_doubt"}:
        return cast(MissionPhase, disposition)
    return "failed"


@dataclass(frozen=True)
class MissionVisibilityValues:
    """The fast-track listing attributes one execution carries (FT-G7)."""

    run_id: str
    mission_id: str | None = None
    lane: MissionLane | None = None
    phase: MissionPhase | None = None
    forked_from_run_ids: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not self.run_id:
            raise ValueError("visibility attributes require a run id")
        if self.mission_id is not None and not self.mission_id:
            raise ValueError("mission id must be non-empty when set")
        if self.lane is not None and self.lane not in MC_LANES:
            raise ValueError(f"undeclared lane: {self.lane}")
        if self.phase is not None and self.phase not in MC_PHASES:
            raise ValueError(f"undeclared phase: {self.phase}")
        if self.run_id in self.forked_from_run_ids or not all(self.forked_from_run_ids):
            raise ValueError("fork lineage names other, non-empty runs")

    def as_mapping(self) -> dict[str, str | list[str]]:
        values: dict[str, str | list[str] | None] = {
            MC_RUN_ID: [self.run_id],
            MC_MISSION_ID: self.mission_id,
            MC_LANE: self.lane,
            MC_PHASE: self.phase,
            FORKED_FROM_RUN_ID: list(self.forked_from_run_ids) or None,
        }
        return {name: value for name, value in values.items() if value is not None}


def visibility_run_query(run_id: str, request_scope: str) -> str:
    """Visibility list filter for one run's executions, bound to the caller's scope."""

    return (
        f"{RUN_ID} = {_quote(run_id)} AND "
        f"{SCOPE_HASH} = {_quote(search_attribute_scope_hash(request_scope))}"
    )


def _quote(value: str) -> str:
    if "'" in value or "\\" in value or "\n" in value:
        raise ValueError("Visibility filter values must not contain quotes or escapes")
    return f"'{value}'"
