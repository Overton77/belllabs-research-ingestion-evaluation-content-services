"""Required-feature admission: a workflow's ``requires`` against a lane's describe (MP-01).

Admission computes the intersection of what the workflow requires and what the lane profile
honestly declares (ARCHITECTURE "Profile identity and capability admission"). A required
control, approval mode or observation feature that the lane reports ``unsupported`` or
``unqualified`` is a pointed error carrying the JSON pointer of the requirement, never a
silent fallback (VALIDATION V01). Optional features are the caller's concern; this module
only judges what was declared required.

Pure: a describe and a requirement set in, issues out.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Final, Literal

from pydantic import BaseModel, ConfigDict, Field

from mission_control.domain.execution.lanes import (
    APPROVAL_MODES,
    DELIVERY_COMMANDS,
    OBSERVATION_FEATURES,
    LaneDescribe,
)

REQUIREMENT_UNSUPPORTED: Final = "LANE_REQUIREMENT_UNSUPPORTED"
REQUIREMENT_UNQUALIFIED: Final = "LANE_REQUIREMENT_UNQUALIFIED"
REQUIREMENT_UNKNOWN: Final = "LANE_REQUIREMENT_UNKNOWN"

RequirementGroup = Literal["controls", "approvals", "observation"]

# Which describe feature cell each required observation feature is evidenced by.
_OBSERVATION_FEATURE_CELL: Final[dict[str, str]] = {
    "terminal_result": "observe",
    "tool_lifecycle": "observe",
    "subordinate_lifecycle": "subordinate_lineage",
    "usage": "usage",
    "compaction": "continuation",
}
# On a v1 describe (no evidence cells) the basic observations are proven by its controls.
_V1_OBSERVATION_CONTROL: Final[dict[str, str]] = {
    "terminal_result": "observe",
    "tool_lifecycle": "observe",
    "usage": "usage",
}
# Approval modes are evidenced by the `approval_suspension` cell plus an explicit mode.
_APPROVAL_FEATURE_CELL: Final = "approval_suspension"
_NEGATIVE_DELIVERY: Final = frozenset({"unsupported"})
# Which evidence cell proves each required control on a v2 describe.
_CONTROL_FEATURE_CELL: Final[dict[str, str]] = {
    "queue_instruction": "follow_up",
    "interrupt_and_inject": "follow_up",
    "pause": "cancel",
    "hard_pause": "cancel",
    "resume": "follow_up",
    "cancel": "cancel",
    "fork": "output_custody",
    "request_continuation": "continuation",
}


class RequirementIssue(BaseModel):
    """One refused requirement: code, JSON pointer into the manifest, and why."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    code: str = Field(min_length=1)
    pointer: str
    group: RequirementGroup
    requirement: str = Field(min_length=1)
    lane_profile: str = Field(min_length=1)
    message: str = Field(min_length=1)


class RequirementSet(BaseModel):
    """The ``requires`` block in its domain form (the manifest v2 model lowers to this)."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    controls: tuple[str, ...] = ()
    approvals: tuple[str, ...] = ()
    observation: tuple[str, ...] = ()

    @property
    def empty(self) -> bool:
        return not (self.controls or self.approvals or self.observation)


def admit_requirements(
    describe: LaneDescribe, requires: RequirementSet, *, pointer: str = "/requires"
) -> tuple[RequirementIssue, ...]:
    """Every required feature the lane cannot honestly provide, with its pointer.

    ``controls`` are judged by the describe's delivery semantics (``unsupported`` refuses;
    on a v2 describe the matching control's evidence must not be ``unqualified`` either).
    ``approvals`` need the mode listed in ``approval_modes`` and the approval cell
    implemented. ``observation`` features need their evidence cell implemented. A v1
    describe (no evidence) refuses every approval and observation requirement as
    unqualified: it has no cell to prove them with.
    """

    issues: list[RequirementIssue] = []
    profile = describe.lane_profile

    def refuse(code: str, group: RequirementGroup, index: int, name: str, why: str) -> None:
        issues.append(
            RequirementIssue(
                code=code,
                pointer=f"{pointer}/{group}/{index}",
                group=group,
                requirement=name,
                lane_profile=profile,
                message=f"lane profile {profile} cannot satisfy required {group} {name}: {why}",
            )
        )

    for index, control in enumerate(requires.controls):
        if control not in DELIVERY_COMMANDS:
            refuse(REQUIREMENT_UNKNOWN, "controls", index, control, "not a lane command")
            continue
        semantics = describe.delivery_semantics[control]
        if semantics in _NEGATIVE_DELIVERY:
            refuse(REQUIREMENT_UNSUPPORTED, "controls", index, control, "delivery is unsupported")
            continue
        if describe.is_v2:
            cell = describe.feature(_CONTROL_FEATURE_CELL.get(control, "launch"))
            if cell.status == "unqualified":
                refuse(
                    REQUIREMENT_UNQUALIFIED,
                    "controls",
                    index,
                    control,
                    f"evidence for {_CONTROL_FEATURE_CELL.get(control, 'launch')} is unqualified",
                )
            elif cell.status == "unsupported":
                refuse(REQUIREMENT_UNSUPPORTED, "controls", index, control, "feature unsupported")

    for index, mode in enumerate(requires.approvals):
        if mode not in APPROVAL_MODES:
            refuse(REQUIREMENT_UNKNOWN, "approvals", index, mode, "not an approval mode")
            continue
        if mode == "workflow_gate":
            # The Human Gate node is Mission Control's own; every lane admits it.
            continue
        if not describe.is_v2:
            refuse(
                REQUIREMENT_UNQUALIFIED, "approvals", index, mode, "describe carries no evidence"
            )
            continue
        cell = describe.feature(_APPROVAL_FEATURE_CELL)
        if mode not in describe.approval_modes or cell.status == "unsupported":
            refuse(REQUIREMENT_UNSUPPORTED, "approvals", index, mode, "approval mode not provided")
        elif cell.status == "unqualified":
            refuse(
                REQUIREMENT_UNQUALIFIED, "approvals", index, mode, "approval evidence unqualified"
            )

    for index, feature in enumerate(requires.observation):
        if feature not in OBSERVATION_FEATURES:
            refuse(REQUIREMENT_UNKNOWN, "observation", index, feature, "not an observation feature")
            continue
        if not describe.is_v2:
            # A v1 describe proves the basic observations through its (qualified) controls;
            # subordinate lifecycle and compaction have no v1 cell and stay unqualified.
            v1_control = _V1_OBSERVATION_CONTROL.get(feature)
            if v1_control is None or not describe.qualified:
                refuse(
                    REQUIREMENT_UNQUALIFIED,
                    "observation",
                    index,
                    feature,
                    "describe carries no evidence",
                )
            elif not describe.implemented(v1_control):
                refuse(
                    REQUIREMENT_UNSUPPORTED,
                    "observation",
                    index,
                    feature,
                    f"control {v1_control} is {describe.control(v1_control)}",
                )
            continue
        cell = describe.feature(_OBSERVATION_FEATURE_CELL[feature])
        if cell.status == "unsupported":
            refuse(REQUIREMENT_UNSUPPORTED, "observation", index, feature, "feature unsupported")
        elif cell.status == "unqualified":
            refuse(REQUIREMENT_UNQUALIFIED, "observation", index, feature, "evidence unqualified")
        elif feature == "subordinate_lifecycle" and describe.subordinate_visibility in {
            "unavailable",
            "unqualified",
        }:
            refuse(
                REQUIREMENT_UNSUPPORTED
                if describe.subordinate_visibility == "unavailable"
                else REQUIREMENT_UNQUALIFIED,
                "observation",
                index,
                feature,
                f"subordinate visibility is {describe.subordinate_visibility}",
            )
    return tuple(issues)


def admits(describe: LaneDescribe, requires: RequirementSet) -> bool:
    return not admit_requirements(describe, requires)


def unsupported_optional(describe: LaneDescribe, optional: Iterable[str]) -> tuple[str, ...]:
    """Optional observation features the lane will omit (explicit omissions, not errors)."""

    omitted: list[str] = []
    for feature in optional:
        cell_name = _OBSERVATION_FEATURE_CELL.get(feature)
        if cell_name is None:
            continue
        if not describe.is_v2 or describe.feature(cell_name).status in {
            "unsupported",
            "unqualified",
        }:
            omitted.append(feature)
    return tuple(omitted)


__all__ = [
    "REQUIREMENT_UNKNOWN",
    "REQUIREMENT_UNQUALIFIED",
    "REQUIREMENT_UNSUPPORTED",
    "RequirementIssue",
    "RequirementSet",
    "admit_requirements",
    "admits",
    "unsupported_optional",
]
