"""MP-11 admission: an unenforceable human-control requirement is a typed rejection.

Runs against the declared (registered, unqualified) lane matrices in
``application/execution/harness/describe.py``; it proves the admission rule, not any
provider's live enforcement.
"""

from __future__ import annotations

from mission_control.application.execution.approvals_coverage import (
    ELICITATION_NOT_FORWARDED,
    GATE_COVERAGE_UNENFORCEABLE,
    GateCoverageRequirement,
    admit_gate_coverage,
)
from mission_control.application.execution.harness.describe import declared_matrix
from mission_control.domain.execution.lane_requirements import (
    REQUIREMENT_UNQUALIFIED,
    REQUIREMENT_UNSUPPORTED,
)


def test_hosted_lane_with_uncontrolled_writes_is_rejected_when_all_writes_are_gated() -> None:
    describe = declared_matrix("codex_cloud")
    issues = admit_gate_coverage(describe, GateCoverageRequirement(all_writes_gated=True))
    codes = {(issue.code, issue.requirement) for issue in issues}
    assert (GATE_COVERAGE_UNENFORCEABLE, "gate_all_writes:shell") in codes
    assert (GATE_COVERAGE_UNENFORCEABLE, "gate_all_writes:file") in codes
    assert all(issue.pointer.startswith("/requires/") for issue in issues)


def test_gateway_enforced_families_are_admitted_only_when_every_family_is_covered() -> None:
    describe = declared_matrix("claude_cloud")
    partial = admit_gate_coverage(
        describe,
        GateCoverageRequirement(
            all_writes_gated=True, gateway_enforced_families=frozenset({"mcp", "network"})
        ),
    )
    assert {issue.requirement for issue in partial} == {
        "gate_all_writes:shell",
        "gate_all_writes:file",
    }
    full = admit_gate_coverage(
        describe,
        GateCoverageRequirement(
            all_writes_gated=True,
            gateway_enforced_families=frozenset({"shell", "file", "mcp", "network"}),
        ),
    )
    assert full == ()
    # The local Claude lane enforces shell/file/mcp through its Kernel Hooks (MP-07); only
    # network writes still need the deployment's gateway attestation.
    local = declared_matrix("claude_agent_sdk")
    gated = GateCoverageRequirement(all_writes_gated=True)
    assert {issue.requirement for issue in admit_gate_coverage(local, gated)} == {
        "gate_all_writes:network"
    }
    attested = gated.model_copy(update={"gateway_enforced_families": frozenset({"network"})})
    assert admit_gate_coverage(local, attested) == ()


def test_required_elicitation_needs_forwarding_or_gateway_owned_tools() -> None:
    hosted = declared_matrix("claude_cloud")
    issues = admit_gate_coverage(hosted, GateCoverageRequirement(approvals=("mcp_elicitation",)))
    codes = {issue.code for issue in issues}
    assert REQUIREMENT_UNSUPPORTED in codes and ELICITATION_NOT_FORWARDED in codes
    owned = admit_gate_coverage(
        hosted,
        GateCoverageRequirement(
            approvals=("mcp_elicitation",), elicitation_tools_gateway_owned=True
        ),
    )
    assert owned == ()
    requirement = GateCoverageRequirement(approvals=("provider_permission", "workflow_gate"))
    # The registered stub of an unqualified lane: refused as unqualified, never admitted.
    stub = admit_gate_coverage(declared_matrix("claude_agent_sdk").unqualified(), requirement)
    assert {issue.code for issue in stub} == {REQUIREMENT_UNQUALIFIED}
    assert {issue.requirement for issue in stub} == {"provider_permission"}
    # The implemented matrix (a local proof that allows unqualified lanes) binds permissions.
    assert admit_gate_coverage(declared_matrix("claude_agent_sdk"), requirement) == ()
