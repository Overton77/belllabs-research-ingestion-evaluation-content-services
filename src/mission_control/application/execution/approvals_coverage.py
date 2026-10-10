"""Gate-coverage admission: reject a human-control requirement the lane cannot enforce (MP-11).

Extends the frozen required-feature admission (``domain/execution/lane_requirements``) with
the two checks SPEC-03 adds for MCP and governed writes:

- **All writes gated.** When a workflow requires every write to be human-gated, each write
  tool family (``shell``, ``file``, ``mcp``, ``network``) must either be enforced by the lane
  (``enforcement_coverage`` ``native``/``emulated``) or be routed through the Mission Control
  governed gateway *with credentials and egress forcing it* (the deployment attests that per
  family). A hosted provider that allows uncontrolled writes is rejected, never downgraded.
- **Elicitation.** A required ``mcp_elicitation`` needs the lane to forward elicitation to an
  API the adapter controls (the approval mode plus implemented approval evidence), or the
  tools that ask for input must be Mission-Control-owned (the gateway terminates the protocol
  and owns the user interaction). "Supports MCP" proves neither.

Pure: describe + requirement in, typed issues out (empty = admitted).
"""

from __future__ import annotations

from typing import Final, Literal

from pydantic import Field

from mission_control.application.execution.approvals import ApprovalContract
from mission_control.domain.execution.lane_requirements import (
    RequirementIssue,
    RequirementSet,
    admit_requirements,
)
from mission_control.domain.execution.lanes import LaneDescribe

GATE_COVERAGE_UNENFORCEABLE: Final = "GATE_COVERAGE_UNENFORCEABLE"
ELICITATION_NOT_FORWARDED: Final = "ELICITATION_NOT_FORWARDED"
WRITE_FAMILIES: Final = ("shell", "file", "mcp", "network")
_ENFORCED: Final = frozenset({"native", "emulated"})

CoverageCode = Literal["GATE_COVERAGE_UNENFORCEABLE", "ELICITATION_NOT_FORWARDED"]


class GateCoverageRequirement(ApprovalContract):
    """What the workflow demands of human control on this lane."""

    approvals: tuple[str, ...] = ()
    all_writes_gated: bool = False
    write_families: tuple[str, ...] = WRITE_FAMILIES
    # Families whose every write is forced through the governed gateway by credentials and
    # egress (a deployment attestation, not a model promise).
    gateway_enforced_families: frozenset[str] = Field(default_factory=frozenset)
    # The MCP tools that may elicit input are all Mission-Control-owned (gateway-terminated).
    elicitation_tools_gateway_owned: bool = False


def admit_gate_coverage(
    describe: LaneDescribe,
    requirement: GateCoverageRequirement,
    *,
    pointer: str = "/requires",
) -> tuple[RequirementIssue, ...]:
    """Every human-control requirement the lane cannot enforce, as pointed typed issues."""

    profile = describe.lane_profile
    approvals = tuple(
        mode
        for mode in requirement.approvals
        if not (mode == "mcp_elicitation" and requirement.elicitation_tools_gateway_owned)
    )
    issues = list(
        admit_requirements(describe, RequirementSet(approvals=approvals), pointer=pointer)
    )
    if (
        "mcp_elicitation" in requirement.approvals
        and not requirement.elicitation_tools_gateway_owned
        and any(issue.requirement == "mcp_elicitation" for issue in issues)
    ):
        issues.append(
            RequirementIssue(
                code=ELICITATION_NOT_FORWARDED,
                pointer=f"{pointer}/approvals",
                group="approvals",
                requirement="mcp_elicitation",
                lane_profile=profile,
                message=(
                    f"lane profile {profile} does not forward MCP elicitation to an API the "
                    "adapter controls, and the eliciting tools are not Mission-Control-owned"
                ),
            )
        )
    if requirement.all_writes_gated:
        for index, family in enumerate(requirement.write_families):
            if family in requirement.gateway_enforced_families:
                continue
            coverage = describe.enforcement_coverage.get(family, "unqualified")
            if coverage in _ENFORCED:
                continue
            issues.append(
                RequirementIssue(
                    code=GATE_COVERAGE_UNENFORCEABLE,
                    pointer=f"{pointer}/write_families/{index}",
                    group="approvals",
                    requirement=f"gate_all_writes:{family}",
                    lane_profile=profile,
                    message=(
                        f"all writes must be human-gated, but lane profile {profile} reports "
                        f"{coverage} enforcement for {family} writes and they are not forced "
                        "through the governed gateway"
                    ),
                )
            )
    return tuple(issues)


__all__ = [
    "ELICITATION_NOT_FORWARDED",
    "GATE_COVERAGE_UNENFORCEABLE",
    "WRITE_FAMILIES",
    "GateCoverageRequirement",
    "admit_gate_coverage",
]
