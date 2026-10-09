"""Resolve an execution-environment request into `mc.environment_binding.v1` and attest it.

SPEC-02 distinguishes three facts: the binding is structurally valid, its identity resolved
(provider environment and revision or an observed configuration digest), and the environment is
launch-ready (preparation attested before the agent started). ``resolve_environment`` returns all
three: the frozen ``EnvironmentBinding`` (identity, readiness), the route evidence it relied on,
and pointed blockers. It never contacts a provider, never invents a provider revision API and
never falls back to a weaker route: a mandatory control without a proven pre-agent route is a
blocker, and ``claude_cloud``/``codex_cloud`` stay blocked (Outcome 3) whatever is supplied.
"""

from __future__ import annotations

from typing import Final, Literal

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, ValidationError

from mission_control.application.agentic_components.materialization import (
    ProjectionMaterialization,
)
from mission_control.application.environments.bootstrap import (
    OUTCOME_3_PROFILES,
    BootstrapRoute,
    ControlKind,
    RouteEvidence,
    route_evidence,
)
from mission_control.domain.capabilities.host_support import HOSTED_PROFILES, LaneProfile
from mission_control.domain.execution.bindings import EnvironmentBinding
from mission_control.domain.execution.lanes import DIGEST_PATTERN

HostedProvider = Literal["cursor", "anthropic", "openai"]
ResolutionStatus = Literal["ready", "unverified", "blocked"]

PROVIDER_OF_HOSTED_PROFILE: Final[dict[LaneProfile, HostedProvider]] = {
    LaneProfile.CURSOR_CLOUD: "cursor",
    LaneProfile.CLAUDE_CLOUD: "anthropic",
    LaneProfile.CODEX_CLOUD: "openai",
}

ENVIRONMENT_ERROR_CODES: Final = frozenset(
    {
        "ENVIRONMENT_KIND_MISMATCH",
        "ENVIRONMENT_PROVIDER_MISMATCH",
        "ENVIRONMENT_BINDING_INVALID",
        "BOOTSTRAP_FIRST_PROMPT_REJECTED",
        "BOOTSTRAP_ROUTE_UNSUPPORTED",
        "BOOTSTRAP_ROUTE_UNQUALIFIED",
        "HOSTED_BOOTSTRAP_UNQUALIFIED",
        "PRE_AGENT_CONFIG_MISSING",
        "PREPARATION_ROUTE_MISMATCH",
        "PRE_AGENT_SETUP_UNPROVEN",
        "PREPARATION_CONFIG_DRIFT",
        "PREPARATION_CONTROL_MISSING",
        "PREPARATION_CHECK_FAILED",
    }
)


class _Contract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class MandatoryControl(_Contract):
    """A control that must be in effect before the agent's first action (e.g. a Kernel Hook)."""

    kind: ControlKind
    ref: str = Field(min_length=1, max_length=256)


class EnvironmentRequest(_Contract):
    """The requested execution environment of one lane binding (manifest `environment`)."""

    lane_profile: LaneProfile
    kind: Literal["local_workspace", "provider_hosted"]
    host_profile: str | None = Field(default=None, min_length=1, max_length=192)
    provider: HostedProvider | None = None
    environment_ref: str | None = Field(default=None, min_length=1, max_length=512)
    expected_revision: str | None = Field(default=None, min_length=1, max_length=512)
    setup_pin: str | None = Field(default=None, min_length=1, max_length=512)
    network_policy_ref: str | None = Field(default=None, min_length=1, max_length=256)
    secret_refs: tuple[str, ...] = ()
    storage_policy_ref: str | None = Field(default=None, min_length=1, max_length=256)
    timeout_ceiling_s: int | None = Field(default=None, ge=1)
    bootstrap_route: BootstrapRoute
    mandatory_controls: tuple[MandatoryControl, ...] = ()
    # The configuration the session must run with (the Host Projection digest).
    materialization_digest: str | None = Field(default=None, pattern=DIGEST_PATTERN)


class PreparationCheck(_Contract):
    name: str = Field(min_length=1, max_length=128)
    passed: bool
    detail: str | None = Field(default=None, max_length=1_024)


class PreparationAttestation(_Contract):
    """What preparation observably did: the route used, whether it ran before the agent, the
    effective configuration digest, the controls an adapter *discovered* as loaded (a written
    file is not a loaded control), checks, artifacts and omissions."""

    route: BootstrapRoute
    phase: Literal["before_agent", "after_agent_start", "unknown"]
    effective_config_digest: str = Field(pattern=DIGEST_PATTERN)
    loaded_controls: tuple[MandatoryControl, ...] = ()
    checks: tuple[PreparationCheck, ...] = ()
    artifacts: tuple[str, ...] = ()
    omissions: tuple[str, ...] = ()
    verified_by: str = Field(min_length=1, max_length=128)
    verified_at: AwareDatetime
    evidence_ref: str = Field(min_length=1, max_length=1_024)


class EnvironmentBlocker(_Contract):
    code: str
    pointer: str
    message: str


class EnvironmentResolution(_Contract):
    lane_profile: LaneProfile
    status: ResolutionStatus
    binding: EnvironmentBinding | None
    route: RouteEvidence | None
    blockers: tuple[EnvironmentBlocker, ...] = ()

    @property
    def launch_ready(self) -> bool:
        return self.status == "ready"


def attest_local_preparation(
    receipt: ProjectionMaterialization,
    *,
    discovered_controls: tuple[MandatoryControl, ...],
    verified_by: str,
    verified_at: AwareDatetime,
    evidence_ref: str,
    route: BootstrapRoute = BootstrapRoute.PRE_SESSION_FILES,
) -> PreparationAttestation:
    """Attestation for a local workspace prepared by ``materialize_projection``.

    Call it only for a receipt produced before the session started. ``discovered_controls``
    must come from adapter discovery (for example a session's init report), never from the
    list of written files.
    """
    checks = tuple(
        PreparationCheck(
            name=f"executable:{item.command}",
            passed=item.available,
            detail=None if item.available else f"required by {', '.join(item.required_by)}",
        )
        for item in receipt.executables
    )
    return PreparationAttestation(
        route=route,
        phase="before_agent",
        effective_config_digest=receipt.projection_digest,
        loaded_controls=discovered_controls,
        checks=checks,
        artifacts=tuple(item.path for item in receipt.files),
        verified_by=verified_by,
        verified_at=verified_at,
        evidence_ref=evidence_ref,
    )


def _route_blockers(
    request: EnvironmentRequest, evidence: RouteEvidence | None, *, allow_unqualified: bool
) -> list[EnvironmentBlocker]:
    profile = request.lane_profile
    pointer = "/execution_environment/setup"
    if request.bootstrap_route is BootstrapRoute.FIRST_PROMPT:
        return [
            EnvironmentBlocker(
                code="BOOTSTRAP_FIRST_PROMPT_REJECTED",
                pointer=pointer,
                message=(
                    "a first-prompt instruction cannot install mandatory controls; use a "
                    "pre-agent setup or configuration route"
                ),
            )
        ]
    if evidence is None or evidence.status == "unsupported":
        detail = evidence.note if evidence is not None else "no such route is recorded"
        return [
            EnvironmentBlocker(
                code="BOOTSTRAP_ROUTE_UNSUPPORTED",
                pointer=pointer,
                message=f"{request.bootstrap_route.value} on {profile.value}: {detail}",
            )
        ]
    blockers: list[EnvironmentBlocker] = []
    if profile in OUTCOME_3_PROFILES:
        missing = "; ".join(evidence.missing_vendor_operations) or "a proven bootstrap route"
        blockers.append(
            EnvironmentBlocker(
                code="HOSTED_BOOTSTRAP_UNQUALIFIED",
                pointer=pointer,
                message=(
                    f"{profile.value} has no proven pre-agent bootstrap ({evidence.note}); "
                    f"missing vendor operations: {missing}; see {evidence.evidence_refs[0]}"
                ),
            )
        )
    elif evidence.status == "unqualified" and not allow_unqualified:
        blockers.append(
            EnvironmentBlocker(
                code="BOOTSTRAP_ROUTE_UNQUALIFIED",
                pointer=pointer,
                message=f"{request.bootstrap_route.value} on {profile.value}: {evidence.note}",
            )
        )
    for index, control in enumerate(request.mandatory_controls):
        if control.kind not in evidence.injects:
            blockers.append(
                EnvironmentBlocker(
                    code="PRE_AGENT_CONFIG_MISSING",
                    pointer=f"/requires/mandatory_controls/{index}",
                    message=(
                        f"{control.kind} {control.ref} has no pre-agent injection on "
                        f"{profile.value} via {request.bootstrap_route.value}"
                    ),
                )
            )
    return blockers


def _attestation_blockers(
    request: EnvironmentRequest, attestation: PreparationAttestation
) -> list[EnvironmentBlocker]:
    blockers: list[EnvironmentBlocker] = []
    pointer = "/preparation"
    if attestation.route is not request.bootstrap_route:
        blockers.append(
            EnvironmentBlocker(
                code="PREPARATION_ROUTE_MISMATCH",
                pointer=f"{pointer}/route",
                message=(
                    f"preparation used {attestation.route.value}, the binding requested "
                    f"{request.bootstrap_route.value}"
                ),
            )
        )
    if attestation.phase != "before_agent":
        blockers.append(
            EnvironmentBlocker(
                code="PRE_AGENT_SETUP_UNPROVEN",
                pointer=f"{pointer}/phase",
                message=(
                    f"setup must complete before the agent starts (observed {attestation.phase})"
                ),
            )
        )
    if (
        request.materialization_digest is not None
        and attestation.effective_config_digest != request.materialization_digest
    ):
        blockers.append(
            EnvironmentBlocker(
                code="PREPARATION_CONFIG_DRIFT",
                pointer=f"{pointer}/effective_config_digest",
                message=(
                    f"effective configuration {attestation.effective_config_digest} is not the "
                    f"pinned materialization {request.materialization_digest}"
                ),
            )
        )
    loaded = set(attestation.loaded_controls)
    for index, control in enumerate(request.mandatory_controls):
        if control not in loaded:
            blockers.append(
                EnvironmentBlocker(
                    code="PREPARATION_CONTROL_MISSING",
                    pointer=f"/requires/mandatory_controls/{index}",
                    message=f"{control.kind} {control.ref} was not discovered as loaded",
                )
            )
    for check in attestation.checks:
        if not check.passed:
            blockers.append(
                EnvironmentBlocker(
                    code="PREPARATION_CHECK_FAILED",
                    pointer=f"{pointer}/checks",
                    message=f"{check.name} failed" + (f": {check.detail}" if check.detail else ""),
                )
            )
    return blockers


def resolve_environment(
    request: EnvironmentRequest,
    attestation: PreparationAttestation | None = None,
    *,
    allow_unqualified: bool = False,
) -> EnvironmentResolution:
    """Resolve and attest one environment. ``allow_unqualified`` admits unqualified local
    routes for development (like ``describe_only_registry``); it never admits Outcome 3."""
    profile = request.lane_profile
    blockers: list[EnvironmentBlocker] = []
    hosted = profile in HOSTED_PROFILES
    expected_kind = "provider_hosted" if hosted else "local_workspace"
    if request.kind != expected_kind:
        blockers.append(
            EnvironmentBlocker(
                code="ENVIRONMENT_KIND_MISMATCH",
                pointer="/execution_environment/kind",
                message=(
                    f"{profile.value} runs in a {expected_kind} environment, not {request.kind}"
                ),
            )
        )
    expected_provider = PROVIDER_OF_HOSTED_PROFILE.get(profile)
    if hosted and request.provider != expected_provider:
        blockers.append(
            EnvironmentBlocker(
                code="ENVIRONMENT_PROVIDER_MISMATCH",
                pointer="/execution_environment/provider",
                message=f"{profile.value} is hosted by {expected_provider}, not {request.provider}",
            )
        )
    evidence = route_evidence(profile, request.bootstrap_route)
    blockers.extend(_route_blockers(request, evidence, allow_unqualified=allow_unqualified))
    if attestation is not None:
        blockers.extend(_attestation_blockers(request, attestation))

    if blockers:
        readiness: Literal["unverified", "ready", "failed"] = (
            "failed" if attestation is not None else "unverified"
        )
    else:
        readiness = "ready" if attestation is not None else "unverified"
    fields: dict[str, object] = {
        "kind": request.kind,
        "host_profile": request.host_profile,
        "provider": request.provider,
        "environment_ref": request.environment_ref,
        "expected_revision": request.expected_revision,
        "setup_pin": request.setup_pin,
        "network_policy_ref": request.network_policy_ref,
        "secret_refs": request.secret_refs,
        "storage_policy_ref": request.storage_policy_ref,
        "timeout_ceiling_s": request.timeout_ceiling_s,
        "readiness": readiness,
    }
    if attestation is not None:
        fields.update(
            observed_config_digest=attestation.effective_config_digest,
            verified_by=attestation.verified_by,
            verified_at=attestation.verified_at,
            readiness_evidence_ref=attestation.evidence_ref,
        )
    binding: EnvironmentBinding | None
    try:
        binding = EnvironmentBinding.model_validate(fields)
    except ValidationError as error:
        binding = None
        blockers.append(
            EnvironmentBlocker(
                code="ENVIRONMENT_BINDING_INVALID",
                pointer="/execution_environment",
                message="; ".join(item["msg"] for item in error.errors()),
            )
        )
    status: ResolutionStatus = (
        "blocked" if blockers else "ready" if readiness == "ready" else "unverified"
    )
    return EnvironmentResolution(
        lane_profile=profile,
        status=status,
        binding=binding,
        route=evidence,
        blockers=tuple(blockers),
    )


__all__ = [
    "ENVIRONMENT_ERROR_CODES",
    "PROVIDER_OF_HOSTED_PROFILE",
    "EnvironmentBlocker",
    "EnvironmentRequest",
    "EnvironmentResolution",
    "MandatoryControl",
    "PreparationAttestation",
    "PreparationCheck",
    "attest_local_preparation",
    "resolve_environment",
]
