"""MP-09: approval coverage reporting and the proposed v2 describe matrices.

Unproven coverage stays rejected: only the kernel-enforced `workflow_gate` is admitted on
either Cursor profile; every feature a drill has not recorded is `unqualified`; the v1
declared matrices (and their digests) are untouched.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from mission_control.adapters.cursor.qualification import (
    CURSOR_PROFILES,
    ApprovalCoverageReport,
    ApprovalNotAdmitted,
    admit_approval,
    approval_coverage,
    proposed_describe,
)
from mission_control.application.execution.harness.describe import (
    CURSOR_CLOUD_DESCRIBE,
    CURSOR_CLOUD_DESCRIBE_V1,
    CURSOR_LOCAL_DESCRIBE,
    CURSOR_LOCAL_DESCRIBE_V1,
    declared_matrix,
)
from mission_control.application.execution.harness.protocol import implements
from mission_control.domain.execution.lanes import (
    APPROVAL_MODES,
    LANE_FEATURES,
    LaneDescribe,
)
from tests.fixtures.cursor_cloud import cloud_stack
from tests.fixtures.cursor_local import local_stack


@pytest.mark.parametrize("profile", CURSOR_PROFILES)
def test_only_the_kernel_workflow_gate_is_admitted_and_unproven_coverage_is_rejected(
    profile: str,
) -> None:
    report = approval_coverage(profile)
    assert ApprovalCoverageReport.model_validate_json(report.model_dump_json()) == report
    assert report.headless and report.sdk_version == "1.0.37"
    assert report.admitted_origins == ("workflow_gate",)
    assert admit_approval(profile, "workflow_gate").admission == "admitted"
    for origin in APPROVAL_MODES:
        if origin == "workflow_gate":
            continue
        with pytest.raises(ApprovalNotAdmitted):
            admit_approval(profile, origin)
    for effect in ("shell", "file", "mcp", "task"):
        with pytest.raises(ApprovalNotAdmitted):
            admit_approval(profile, "governed_effect", effect)
    assert all(not cell.qualified for cell in report.cells), "no drill has run"
    assert all(cell.account_enabled == "unknown" for cell in report.cells)
    ask = report.cell("provider_permission")
    assert ask.coverage == "unsupported" and not ask.implemented
    assert "SDKRequestMessage" in " ".join(ask.evidence) or "request" in ask.mechanism


def test_local_kernel_hooks_are_implemented_but_unqualified_and_cloud_mcp_has_no_surface() -> None:
    local = approval_coverage("cursor_local")
    for effect in ("shell", "file", "mcp", "task"):
        cell = local.cell("governed_effect", effect)  # type: ignore[arg-type]
        assert cell.implemented and cell.coverage == "unqualified"
        assert cell.admission == "rejected"
        assert any("OVE-55" in e or "tests/" in e for e in cell.evidence)
    cloud = approval_coverage("cursor_cloud")
    assert cloud.cell("governed_effect", "mcp").coverage == "unsupported"
    for effect in ("shell", "file", "task"):
        cell = cloud.cell("governed_effect", effect)  # type: ignore[arg-type]
        assert not cell.implemented and cell.coverage == "unqualified"
        assert "loopback" in cell.mechanism or "VM" in cell.mechanism


@pytest.mark.parametrize("profile", CURSOR_PROFILES)
async def test_the_proposed_v2_describe_is_honest_and_every_declared_control_is_implemented(
    tmp_path: Path, profile: str
) -> None:
    proposed = proposed_describe(profile)
    assert proposed.is_v2
    assert LaneDescribe.model_validate_json(proposed.model_dump_json()) == proposed
    declared = declared_matrix(profile)
    assert proposed.controls == declared.controls, "controls are unchanged from v1"
    assert proposed.delivery_semantics == declared.delivery_semantics
    assert proposed.qualified is False
    assert proposed.approval_modes == ("workflow_gate",)
    assert proposed.compaction_control == "unsupported", "preCompact is observe-only"
    assert proposed.subordinate_visibility == "unqualified"
    assert set(proposed.features) == set(LANE_FEATURES)
    for name, cell in proposed.features.items():
        assert not cell.qualified and cell.account_enabled == "unknown", name
        # Implemented cells state how (native/emulated); the rest are unqualified/unsupported.
        expected = {"native", "emulated"} if cell.implemented else {"unqualified", "unsupported"}
        assert cell.status in expected, name
        assert cell.sdk_version == "1.0.37" and cell.evidence_ref, name
    stack = local_stack(tmp_path) if profile == "cursor_local" else cloud_stack(tmp_path)
    for control, support in proposed.controls.items():
        if control in {"pause", "fork"}:
            continue
        assert implements(stack.harness, control) is (support in {"native", "emulated"}), control
    if profile == "cursor_cloud":
        assert proposed.features["approval_suspension"].status == "unsupported"
        assert proposed.enforcement_coverage["mcp"] == "unsupported"
        assert not proposed.features["subordinate_lineage"].implemented
    else:
        assert proposed.features["approval_suspension"].status == "unqualified"
        assert not proposed.features["approval_suspension"].implemented
        assert proposed.enforcement_coverage == {
            "shell": "unqualified",
            "file": "unqualified",
            "mcp": "unqualified",
        }
        assert proposed.features["subordinate_lineage"].implemented
    stub = proposed.unqualified()
    assert all(value == "unqualified" for value in stub.controls.values())
    assert (
        stub.features["approval_suspension"].status
        == proposed.features["approval_suspension"].status
    )


def test_the_declared_cursor_matrices_are_the_mp09_v2_proposal() -> None:
    """Integrated 2026-10-09: the declared matrices are the proposal; v1 stays readable."""

    assert CURSOR_LOCAL_DESCRIBE_V1.schema_version == "mc.lane_describe.v1"
    assert CURSOR_CLOUD_DESCRIBE_V1.schema_version == "mc.lane_describe.v1"
    assert "features" not in json.loads(CURSOR_LOCAL_DESCRIBE_V1.model_dump_json())
    assert proposed_describe("cursor_local") == CURSOR_LOCAL_DESCRIBE
    assert proposed_describe("cursor_cloud") == CURSOR_CLOUD_DESCRIBE
    assert proposed_describe("cursor_local").digest != CURSOR_LOCAL_DESCRIBE_V1.digest
    assert declared_matrix("cursor_local") is CURSOR_LOCAL_DESCRIBE
    assert declared_matrix("cursor_cloud") is CURSOR_CLOUD_DESCRIBE
    with pytest.raises(ValueError):
        proposed_describe("deep_agents")
    with pytest.raises(ValueError):
        approval_coverage("claude_cloud")
