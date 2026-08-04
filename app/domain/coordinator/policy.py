from __future__ import annotations

from app.domain.coordinator.contracts import (
    AuthorizationState,
    CatalogAssetStatus,
    PolicyReason,
    PolicyReasonCode,
    SelectionDecision,
    SelectionFacts,
)
from app.domain.coordinator.errors import CoordinatorDomainError, CoordinatorErrorCode


def _reason(code: PolicyReasonCode, message: str) -> PolicyReason:
    return PolicyReason(code=code, message=message)


def evaluate_selection(facts: SelectionFacts) -> SelectionDecision:
    """Evaluate authority separately from retrieval rank using stable precedence."""
    if facts.candidate_id is not None:
        return SelectionDecision(
            authorization_state=AuthorizationState.CANDIDATE_ONLY,
            reasons=(
                _reason(
                    PolicyReasonCode.EXTERNAL_CANDIDATE_REQUIRES_PROMOTION,
                    "External candidates require inspection and promotion before selection.",
                ),
            ),
        )
    if not facts.tenant_visible:
        return SelectionDecision(
            authorization_state=AuthorizationState.FORBIDDEN,
            reasons=(
                _reason(
                    PolicyReasonCode.TENANT_INACCESSIBLE,
                    "The asset is outside the caller's tenant visibility.",
                ),
            ),
        )
    if not facts.policy_allowed:
        return SelectionDecision(
            authorization_state=AuthorizationState.FORBIDDEN,
            reasons=(
                _reason(
                    PolicyReasonCode.POLICY_FORBIDDEN,
                    "Current policy does not permit selecting this asset.",
                ),
            ),
        )
    if facts.lifecycle_status == CatalogAssetStatus.REVOKED:
        return SelectionDecision(
            authorization_state=AuthorizationState.UNAVAILABLE,
            reasons=(
                _reason(PolicyReasonCode.ASSET_REVOKED, "The asset has been revoked."),
            ),
        )
    if facts.lifecycle_status == CatalogAssetStatus.RETIRED:
        return SelectionDecision(
            authorization_state=AuthorizationState.UNAVAILABLE,
            reasons=(
                _reason(PolicyReasonCode.ASSET_RETIRED, "The asset has been retired."),
            ),
        )
    if not facts.source_digest_verified:
        return SelectionDecision(
            authorization_state=AuthorizationState.UNAVAILABLE,
            reasons=(
                _reason(
                    PolicyReasonCode.SOURCE_DIGEST_MISMATCH,
                    "The search projection does not match the authoritative definition digest.",
                ),
            ),
        )
    if not facts.schema_digest_verified:
        return SelectionDecision(
            authorization_state=AuthorizationState.INCOMPATIBLE,
            reasons=(
                _reason(
                    PolicyReasonCode.CAPABILITY_SCHEMA_CHANGED,
                    "The current capability schema differs from the promoted snapshot.",
                ),
            ),
        )
    missing = facts.required_capabilities - facts.granted_capabilities
    if missing:
        return SelectionDecision(
            authorization_state=AuthorizationState.INCOMPATIBLE,
            reasons=tuple(
                _reason(
                    PolicyReasonCode.MISSING_CAPABILITY,
                    f"Required capability is not granted: {capability}",
                )
                for capability in sorted(missing)
            ),
            missing_capabilities=missing,
        )
    if not facts.runtime_compatible:
        return SelectionDecision(
            authorization_state=AuthorizationState.INCOMPATIBLE,
            reasons=(
                _reason(
                    PolicyReasonCode.RUNTIME_INCOMPATIBLE,
                    "The selected runtime does not satisfy the asset compatibility contract.",
                ),
            ),
        )
    if not facts.runtime_available:
        return SelectionDecision(
            authorization_state=AuthorizationState.UNAVAILABLE,
            reasons=(
                _reason(
                    PolicyReasonCode.RUNTIME_UNAVAILABLE,
                    "The compatible runtime is not currently available.",
                ),
            ),
        )
    return SelectionDecision(
        authorization_state=AuthorizationState.SELECTABLE,
        reasons=(
            _reason(
                PolicyReasonCode.SELECTABLE,
                "The exact authoritative asset is currently selectable.",
            ),
        ),
    )


def require_selectable(decision: SelectionDecision) -> None:
    if decision.authorization_state == AuthorizationState.SELECTABLE:
        return
    error_code = {
        AuthorizationState.CANDIDATE_ONLY: CoordinatorErrorCode.EXTERNAL_CANDIDATE_NOT_SELECTABLE,
        AuthorizationState.FORBIDDEN: CoordinatorErrorCode.CAPABILITY_FORBIDDEN,
        AuthorizationState.INCOMPATIBLE: CoordinatorErrorCode.CAPABILITY_INCOMPATIBLE,
        AuthorizationState.UNAVAILABLE: CoordinatorErrorCode.CAPABILITY_UNAVAILABLE,
    }[decision.authorization_state]
    raise CoordinatorDomainError(
        error_code,
        decision.reasons[0].message,
        retryable=decision.authorization_state == AuthorizationState.UNAVAILABLE,
        details={"reason_code": decision.reasons[0].code.value},
    )
