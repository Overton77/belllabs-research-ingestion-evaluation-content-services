"""MP-03: environment resolution into `mc.environment_binding.v1` and preparation attestation.

Hosted Claude/Codex stay blocked (Outcome 3) whatever evidence is offered; no route accepts a
first-prompt self-install of mandatory controls; local readiness needs a pre-agent attestation
whose effective configuration is the pinned projection and whose controls were discovered.
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

from mission_control.application.agentic_components.materialization import (
    materialize_projection,
)
from mission_control.application.environments import (
    ENVIRONMENT_ERROR_CODES,
    OUTCOME_3_PROFILES,
    ROUTE_EVIDENCE,
    BootstrapRoute,
    EnvironmentRequest,
    MandatoryControl,
    PreparationAttestation,
    attest_local_preparation,
    resolve_environment,
)
from mission_control.domain.capabilities.host_support import LaneProfile
from mission_control.domain.execution.bindings import EnvironmentBinding
from tests.fixtures.projections import regen

NOW = datetime(2026, 10, 8, 12, 0, tzinfo=UTC)
DIGEST = "sha256:" + "a" * 64
STOP_FENCE = MandatoryControl(kind="hook", ref="mc.stop_fence")


def _hosted(profile: LaneProfile, provider: str, route: BootstrapRoute) -> EnvironmentRequest:
    return EnvironmentRequest.model_validate(
        {
            "lane_profile": profile,
            "kind": "provider_hosted",
            "provider": provider,
            "environment_ref": "env_fixture",
            "expected_revision": "rev-fixture",
            "bootstrap_route": route,
            "mandatory_controls": [STOP_FENCE.model_dump()],
        }
    )


def _attestation(**overrides: object) -> PreparationAttestation:
    values: dict[str, object] = {
        "route": BootstrapRoute.INTEGRATION_COMMIT,
        "phase": "before_agent",
        "effective_config_digest": DIGEST,
        "loaded_controls": [STOP_FENCE.model_dump()],
        "verified_by": "fixture",
        "verified_at": NOW,
        "evidence_ref": "fixture://attestation",
    }
    values.update(overrides)
    return PreparationAttestation.model_validate(values)


def _codes(resolution: object) -> set[str]:
    return {item.code for item in resolution.blockers}  # type: ignore[attr-defined]


@pytest.mark.parametrize(
    ("profile", "provider", "route"),
    [
        (LaneProfile.CLAUDE_CLOUD, "anthropic", BootstrapRoute.INTEGRATION_COMMIT),
        (LaneProfile.CODEX_CLOUD, "openai", BootstrapRoute.INTEGRATION_COMMIT),
        (LaneProfile.CODEX_CLOUD, "openai", BootstrapRoute.PROVIDER_ENVIRONMENT_SETUP),
    ],
)
def test_outcome_3_hosted_profiles_stay_blocked_even_with_attestation(
    profile: LaneProfile, provider: str, route: BootstrapRoute
) -> None:
    request = _hosted(profile, provider, route)
    attested = resolve_environment(request, _attestation(route=route), allow_unqualified=True)
    assert attested.status == "blocked" and not attested.launch_ready
    assert "HOSTED_BOOTSTRAP_UNQUALIFIED" in _codes(attested)
    blocker = next(b for b in attested.blockers if b.code == "HOSTED_BOOTSTRAP_UNQUALIFIED")
    assert blocker.pointer == "/execution_environment/setup"
    assert "FEASIBILITY.md" in blocker.message and "missing vendor operations" in blocker.message
    # The identity is still recorded in the frozen shape, never as ready.
    assert isinstance(attested.binding, EnvironmentBinding)
    assert attested.binding.readiness == "failed"
    assert attested.binding.kind == "provider_hosted"


def test_codex_cloud_cannot_inject_mandatory_hooks_before_the_agent() -> None:
    resolution = resolve_environment(
        _hosted(LaneProfile.CODEX_CLOUD, "openai", BootstrapRoute.INTEGRATION_COMMIT)
    )
    missing = [b for b in resolution.blockers if b.code == "PRE_AGENT_CONFIG_MISSING"]
    assert missing and missing[0].pointer == "/requires/mandatory_controls/0"
    assert "hook mc.stop_fence" in missing[0].message


def test_claude_cloud_provider_setup_route_is_unsupported() -> None:
    resolution = resolve_environment(
        _hosted(LaneProfile.CLAUDE_CLOUD, "anthropic", BootstrapRoute.PROVIDER_ENVIRONMENT_SETUP)
    )
    assert _codes(resolution) == {"BOOTSTRAP_ROUTE_UNSUPPORTED"}


@pytest.mark.parametrize("profile", list(LaneProfile))
def test_first_prompt_self_install_is_rejected_on_every_profile(profile: LaneProfile) -> None:
    hosted = profile in {
        LaneProfile.CURSOR_CLOUD,
        LaneProfile.CLAUDE_CLOUD,
        LaneProfile.CODEX_CLOUD,
    }
    request = EnvironmentRequest.model_validate(
        {
            "lane_profile": profile,
            "kind": "provider_hosted" if hosted else "local_workspace",
            "provider": {"cursor_cloud": "cursor", "claude_cloud": "anthropic"}.get(
                profile.value, "openai"
            )
            if hosted
            else None,
            "environment_ref": "env_fixture" if hosted else None,
            "expected_revision": "rev" if hosted else None,
            "host_profile": None if hosted else "worker-local",
            "bootstrap_route": BootstrapRoute.FIRST_PROMPT,
            "mandatory_controls": [STOP_FENCE.model_dump()],
        }
    )
    resolution = resolve_environment(request, allow_unqualified=True)
    assert resolution.status == "blocked"
    assert _codes(resolution) == {"BOOTSTRAP_FIRST_PROMPT_REJECTED"}


def test_kind_provider_and_unpinned_revision_are_pointed_errors() -> None:
    wrong_kind = EnvironmentRequest(
        lane_profile=LaneProfile.CLAUDE_CLOUD,
        kind="local_workspace",
        host_profile="worker",
        bootstrap_route=BootstrapRoute.INTEGRATION_COMMIT,
    )
    codes = _codes(resolve_environment(wrong_kind))
    assert {"ENVIRONMENT_KIND_MISMATCH", "ENVIRONMENT_PROVIDER_MISMATCH"} <= codes
    unpinned = EnvironmentRequest(
        lane_profile=LaneProfile.CURSOR_CLOUD,
        kind="provider_hosted",
        provider="cursor",
        environment_ref="env_fixture",
        bootstrap_route=BootstrapRoute.INTEGRATION_COMMIT,
    )
    resolution = resolve_environment(unpinned, allow_unqualified=True)
    invalid = [b for b in resolution.blockers if b.code == "ENVIRONMENT_BINDING_INVALID"]
    assert invalid and "expected_revision or observed_config_digest" in invalid[0].message
    assert resolution.binding is None


def test_unqualified_local_route_blocks_unless_explicitly_allowed() -> None:
    request = EnvironmentRequest(
        lane_profile=LaneProfile.CODEX,
        kind="local_workspace",
        host_profile="worker-local",
        bootstrap_route=BootstrapRoute.PRE_SESSION_FILES,
    )
    assert _codes(resolve_environment(request)) == {"BOOTSTRAP_ROUTE_UNQUALIFIED"}
    allowed = resolve_environment(request, allow_unqualified=True)
    assert allowed.status == "unverified" and allowed.binding is not None
    assert allowed.binding.readiness == "unverified"


def _local_request(digest: str) -> EnvironmentRequest:
    return EnvironmentRequest(
        lane_profile=LaneProfile.CLAUDE_AGENT_SDK,
        kind="local_workspace",
        host_profile="worker-local",
        bootstrap_route=BootstrapRoute.PRE_SESSION_FILES,
        mandatory_controls=(STOP_FENCE,),
        materialization_digest=digest,
    )


def test_local_preparation_attestation_reaches_ready(tmp_path: Path) -> None:
    projection = regen.project(LaneProfile.CLAUDE_AGENT_SDK)
    receipt = materialize_projection(projection, tmp_path, which=lambda _: "/bin/x")
    attestation = attest_local_preparation(
        receipt,
        discovered_controls=(STOP_FENCE,),
        verified_by="fixture-discovery",
        verified_at=NOW,
        evidence_ref="fixture://init",
    )
    resolution = resolve_environment(
        _local_request(receipt.projection_digest), attestation, allow_unqualified=True
    )
    assert resolution.status == "ready" and resolution.launch_ready
    assert resolution.binding is not None
    assert resolution.binding.readiness == "ready"
    assert resolution.binding.observed_config_digest == receipt.projection_digest
    assert resolution.binding.readiness_evidence_ref == "fixture://init"


def test_written_files_are_not_loaded_controls(tmp_path: Path) -> None:
    projection = regen.project(LaneProfile.CLAUDE_AGENT_SDK)
    receipt = materialize_projection(projection, tmp_path, which=lambda _: "/bin/x")
    attestation = attest_local_preparation(
        receipt,
        discovered_controls=(),
        verified_by="fixture",
        verified_at=NOW,
        evidence_ref="fixture://none",
    )
    resolution = resolve_environment(
        _local_request(receipt.projection_digest), attestation, allow_unqualified=True
    )
    assert _codes(resolution) == {"PREPARATION_CONTROL_MISSING"}
    assert resolution.binding is not None and resolution.binding.readiness == "failed"


def test_attestation_drift_phase_route_and_checks_block() -> None:
    request = _local_request("sha256:" + "b" * 64)
    attestation = _attestation(
        phase="after_agent_start",
        checks=[{"name": "executable:npx", "passed": False, "detail": "required by mcp.tavily"}],
    )
    codes = _codes(resolve_environment(request, attestation, allow_unqualified=True))
    assert codes == {
        "PREPARATION_ROUTE_MISMATCH",
        "PRE_AGENT_SETUP_UNPROVEN",
        "PREPARATION_CONFIG_DRIFT",
        "PREPARATION_CHECK_FAILED",
    }


def test_route_evidence_table_is_total_and_honest() -> None:
    assert {profile for profile, _ in ROUTE_EVIDENCE} == set(LaneProfile)
    qualified = {key for key, item in ROUTE_EVIDENCE.items() if item.status == "qualified"}
    assert qualified == {(LaneProfile.DEEP_AGENTS, BootstrapRoute.IN_PROCESS)}
    for profile in OUTCOME_3_PROFILES:
        hosted = [item for (p, _), item in ROUTE_EVIDENCE.items() if p is profile]
        assert all(item.status != "qualified" for item in hosted)
        assert any(item.missing_vendor_operations for item in hosted)
    for profile in LaneProfile:
        assert ROUTE_EVIDENCE[(profile, BootstrapRoute.FIRST_PROMPT)].status == "unsupported"
    assert {"HOSTED_BOOTSTRAP_UNQUALIFIED", "PRE_AGENT_CONFIG_MISSING"} <= ENVIRONMENT_ERROR_CODES
