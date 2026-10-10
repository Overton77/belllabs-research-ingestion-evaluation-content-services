"""Cursor approval-coverage reporting and the proposed v2 describe matrices (MP-09 / OVE-72).

Three facts are kept apart for every control (ARCHITECTURE "Profile identity and capability
admission"): *implemented* (an adapter path exists), *qualified* (a recorded live drill for
this exact pin, OVE-55 / FT-G6) and *account-enabled* (observed at launch, never baked in).
Nothing here flips `qualified`; both profiles stay `qualified=False` until the owner-run drill
writes `docs/qualification/lanes/<profile>-<date>.md`.

`approval_coverage(profile)` reports, per approval origin and effect family, what the lane can
enforce today and with what evidence, and `admit_approval(...)` turns that into admission:
only the kernel-enforced `workflow_gate` is admitted; every other origin is rejected while its
coverage is unproven (`unqualified`) or has no vendor surface (`unsupported`). Headless
Cursor runs auto-approve every tool call (ADR-0030); the pinned `cursor-sdk==1.0.37` surfaces
a `request` stream message (`cursor_sdk/types.py` `SDKRequestMessage`: `request_id` only) and
no API that answers it, so a provider `ask` is observed, never resolved, on this pin.

`proposed_describe(profile)` is the `mc.lane_describe.v2` matrix MP-09 proposes for the
integrator-owned `application/execution/harness/describe.py` (the v1 declared matrices and
their digests are untouched here): the same controls, per-feature evidence that says
`implemented` where an adapter path exists and `unqualified` everywhere a drill has not run,
`unsupported` only where the vendor surface is absent on this pin.
"""

from __future__ import annotations

from typing import Final, Literal

from pydantic import BaseModel, ConfigDict, Field

from mission_control.application.execution.harness.describe import declared_matrix
from mission_control.domain.execution.lanes import (
    ApprovalMode,
    LaneDescribe,
    LaneProfileName,
)

CURSOR_PROFILES: Final[tuple[LaneProfileName, ...]] = ("cursor_local", "cursor_cloud")
EffectFamily = Literal["shell", "file", "mcp", "task", "any"]
Admission = Literal["admitted", "rejected"]
Coverage = Literal["native", "emulated", "unsupported", "unqualified"]

SDK_VERSION: Final = "1.0.37"
# Evidence refs every cell cites (OVE-55 / FT-G6 offline suites and the MP-09 additions).
EVIDENCE_FIXTURES: Final = "tests/unit/harness/test_lane_qualification_fixtures.py"
EVIDENCE_HONESTY: Final = "tests/unit/harness/test_describe_honesty.py"
EVIDENCE_HOOK_ROUNDTRIP: Final = "tests/integration/cursor/test_kernel_hook_roundtrip.py"
EVIDENCE_MP09_UNIT: Final = "tests/unit/cursor/"
EVIDENCE_MP09_REAL: Final = "tests/integration/cursor/test_mp09_cloud_segment_loop_real_services.py"
EVIDENCE_SDK_REQUEST: Final = (
    "cursor_sdk==1.0.37 types.py SDKRequestMessage(request_id); no respond API in "
    "_async_run.py/_async_agent.py/_async_client.py"
)
EVIDENCE_CLOUD_HOOKS: Final = (
    "../mission-control-general/runtime-facts/CURSOR_SDK_FACTS.md section 7: cloud agents "
    "run command hooks only; no sessionStart/sessionEnd/beforeMCPExecution/afterMCPExecution"
)
DRILL_RECORD: Final = "docs/qualification/lanes/<profile>-<date>.md (owner-run, OVE-55)"


class _Contract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class ApprovalCoverageCell(_Contract):
    """One approval origin on one effect family: what the lane does, with its evidence."""

    origin: ApprovalMode
    effect: EffectFamily
    coverage: Coverage
    implemented: bool = False
    qualified: bool = False
    account_enabled: Literal["enabled", "disabled", "unknown"] = "unknown"
    admission: Admission
    mechanism: str = Field(min_length=1, max_length=256)
    evidence: tuple[str, ...] = ()
    note: str = Field(default="", max_length=1_024)


class ApprovalCoverageReport(_Contract):
    """`mc.cursor_approval_coverage.v1`: headless `ask` and tool-gate coverage per profile."""

    schema_version: Literal["mc.cursor_approval_coverage.v1"] = "mc.cursor_approval_coverage.v1"
    lane_profile: LaneProfileName
    sdk_version: str = SDK_VERSION
    headless: bool = True
    cells: tuple[ApprovalCoverageCell, ...]
    drill_record: str = DRILL_RECORD

    def cell(self, origin: ApprovalMode, effect: EffectFamily = "any") -> ApprovalCoverageCell:
        """The cell for `origin` on `effect`; an origin declared per effect family answers
        `any` with its weakest cell (a rejected family rejects the origin as a whole)."""

        for item in self.cells:
            if item.origin == origin and item.effect == effect:
                return item
        for item in self.cells:
            if item.origin == origin and item.effect == "any":
                return item
        if effect == "any":
            declared = [item for item in self.cells if item.origin == origin]
            rejected = [item for item in declared if item.admission == "rejected"]
            if rejected or declared:
                return (rejected or declared)[0]
        raise ValueError(f"{self.lane_profile} declares no {origin} coverage for {effect}")

    @property
    def admitted_origins(self) -> tuple[ApprovalMode, ...]:
        return tuple(
            dict.fromkeys(cell.origin for cell in self.cells if cell.admission == "admitted")
        )


class ApprovalNotAdmitted(ValueError):
    """A required approval origin the profile cannot prove it enforces."""

    def __init__(self, profile: str, cell: ApprovalCoverageCell) -> None:
        super().__init__(
            f"{profile} cannot admit {cell.origin} on {cell.effect}: {cell.coverage} "
            f"({cell.note or cell.mechanism})"
        )
        self.lane_profile = profile
        self.cell = cell


def _cell(
    origin: ApprovalMode,
    effect: EffectFamily,
    coverage: Coverage,
    *,
    mechanism: str,
    implemented: bool = False,
    evidence: tuple[str, ...] = (),
    note: str = "",
) -> ApprovalCoverageCell:
    # Only the kernel-enforced workflow gate is admitted; unproven coverage stays rejected.
    admitted = origin == "workflow_gate" and coverage in {"native", "emulated"}
    return ApprovalCoverageCell(
        origin=origin,
        effect=effect,
        coverage=coverage,
        implemented=implemented,
        qualified=False,
        account_enabled="unknown",
        admission="admitted" if admitted else "rejected",
        mechanism=mechanism,
        evidence=evidence,
        note=note,
    )


_KERNEL_GATE = "Kernel Hook command hook, fail-closed, loopback callback to the worker"
_LOCAL_CELLS: Final = (
    _cell(
        "workflow_gate",
        "any",
        "native",
        mechanism="Mission Control Human Gate (kernel), independent of the provider",
        implemented=True,
        evidence=("docs/adr/0038-two-origins-of-human-control-over-one-human-task-service.md",),
    ),
    _cell(
        "governed_effect",
        "shell",
        "unqualified",
        mechanism=_KERNEL_GATE + " (beforeShellExecution, preToolUse)",
        implemented=True,
        evidence=(EVIDENCE_HOOK_ROUNDTRIP, EVIDENCE_FIXTURES + "::hook_deny"),
        note="implemented and fixture-proven; no recorded live drill (OVE-55)",
    ),
    _cell(
        "governed_effect",
        "mcp",
        "unqualified",
        mechanism=_KERNEL_GATE + " (beforeMCPExecution)",
        implemented=True,
        evidence=(EVIDENCE_HOOK_ROUNDTRIP,),
        note="whether beforeMCPExecution fires in headless SDK runs is UNVERIFIED",
    ),
    _cell(
        "governed_effect",
        "file",
        "unqualified",
        mechanism=_KERNEL_GATE + " (preToolUse on edit/write tools)",
        implemented=True,
        evidence=(EVIDENCE_HOOK_ROUNDTRIP,),
        note="file tools are gated through preToolUse only; afterFileEdit is observe-only",
    ),
    _cell(
        "governed_effect",
        "task",
        "unqualified",
        mechanism=_KERNEL_GATE + " (subagentStart)",
        implemented=True,
        evidence=(EVIDENCE_HOOK_ROUNDTRIP,),
    ),
    _cell(
        "provider_permission",
        "any",
        "unsupported",
        mechanism="SDK `request` stream message is observed (APPROVAL_REQUESTED frame)",
        note="headless runs auto-approve every tool (ADR-0030); " + EVIDENCE_SDK_REQUEST,
        evidence=(EVIDENCE_SDK_REQUEST,),
    ),
    _cell(
        "provider_question",
        "any",
        "unsupported",
        mechanism="none on this pin",
        note="no question/answer surface in cursor-sdk 1.0.37 or the Cloud Agents API v1",
    ),
    _cell(
        "mcp_elicitation",
        "any",
        "unqualified",
        mechanism="governed MCP gateway (MP-11), not a Cursor surface",
        note="depends on the gateway; nothing Cursor-specific is proven",
    ),
)
_CLOUD_CELLS: Final = (
    _LOCAL_CELLS[0],
    _cell(
        "governed_effect",
        "shell",
        "unqualified",
        mechanism="catalog command hooks in the cloud VM; no loopback to the kernel",
        note="the VM cannot reach the worker callback: no Stop Fence/Operation Intent gate",
        evidence=(EVIDENCE_CLOUD_HOOKS, EVIDENCE_FIXTURES + "::hook_deny_stream"),
    ),
    _cell(
        "governed_effect",
        "file",
        "unqualified",
        mechanism="catalog command hooks in the cloud VM; no loopback to the kernel",
        evidence=(EVIDENCE_CLOUD_HOOKS,),
    ),
    _cell(
        "governed_effect",
        "mcp",
        "unsupported",
        mechanism="none: cloud agents fire no beforeMCPExecution/afterMCPExecution",
        evidence=(EVIDENCE_CLOUD_HOOKS,),
    ),
    _cell(
        "governed_effect",
        "task",
        "unqualified",
        mechanism="subagentStart command hook in the VM; no loopback to the kernel",
        evidence=(EVIDENCE_CLOUD_HOOKS,),
    ),
    _cell(
        "provider_permission",
        "any",
        "unsupported",
        mechanism="none: the run SSE stream carries no request event",
        evidence=(EVIDENCE_CLOUD_HOOKS,),
    ),
    _LOCAL_CELLS[6],
    _cell(
        "mcp_elicitation",
        "any",
        "unqualified",
        mechanism="governed MCP gateway (MP-11) reachable from the VM only over the network",
        note="`localhost` in the VM is not the worker; nothing is proven",
    ),
)
_COVERAGE: Final[dict[str, tuple[ApprovalCoverageCell, ...]]] = {
    "cursor_local": _LOCAL_CELLS,
    "cursor_cloud": _CLOUD_CELLS,
}


def approval_coverage(lane_profile: str) -> ApprovalCoverageReport:
    if lane_profile not in _COVERAGE:
        raise ValueError(f"{lane_profile} is not a Cursor lane profile")
    return ApprovalCoverageReport(lane_profile=lane_profile, cells=_COVERAGE[lane_profile])


def admit_approval(
    lane_profile: str, origin: ApprovalMode, effect: EffectFamily = "any"
) -> ApprovalCoverageCell:
    """The coverage cell when the profile can enforce `origin` on `effect`; otherwise
    `ApprovalNotAdmitted` (unproven coverage is never admitted)."""

    cell = approval_coverage(lane_profile).cell(origin, effect)
    if cell.admission != "admitted":
        raise ApprovalNotAdmitted(lane_profile, cell)
    return cell


# --- the proposed mc.lane_describe.v2 matrices ---------------------------------------------------


def proposed_describe(lane_profile: str) -> LaneDescribe:
    """The v2 matrix MP-09 proposed for a Cursor profile, now the declared one
    (`describe.CURSOR_*_DESCRIBE`, integrated 2026-10-09): controls unchanged from v1,
    implemented features `native`/`emulated` and unqualified, approval modes limited to what
    `approval_coverage` admits, compaction `unsupported` (the preCompact hook is observe-only
    and no SDK/REST call compacts), enforcement coverage per effect family."""

    if lane_profile not in ("cursor_local", "cursor_cloud"):
        raise ValueError(f"{lane_profile} is not a Cursor lane profile")
    describe = declared_matrix(lane_profile)
    if describe.approval_modes != approval_coverage(lane_profile).admitted_origins:
        raise RuntimeError(f"{lane_profile}: declared approval modes disagree with coverage")
    return describe


__all__ = [
    "CURSOR_PROFILES",
    "ApprovalCoverageCell",
    "ApprovalCoverageReport",
    "ApprovalNotAdmitted",
    "admit_approval",
    "approval_coverage",
    "proposed_describe",
]
