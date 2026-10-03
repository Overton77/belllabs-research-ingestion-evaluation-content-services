"""Pure coordinator capability-retrieval contracts and policy."""

from mission_control.domain.coordinator.contracts import (
    AuthorizationState,
    CapabilitySearchHit,
    CapabilitySearchRequest,
    CatalogAssetStatus,
    CoordinatorErrorEnvelope,
    ExternalDiscoveryCandidate,
    InspectionStatus,
    PolicyReason,
    PolicyReasonCode,
    SearchDocumentMetadata,
    SearchDocumentSource,
    SelectionDecision,
    SelectionFacts,
    WorkflowDesignDraft,
)
from mission_control.domain.coordinator.errors import (
    CoordinatorDomainError,
    CoordinatorErrorCode,
)
from mission_control.domain.coordinator.policy import evaluate_selection, require_selectable
from mission_control.domain.coordinator.search_document import (
    SEARCH_DOCUMENT_FORMAT_VERSION,
    RenderedSearchDocument,
    render_search_document,
    search_document_source,
)

__all__ = [
    "AuthorizationState",
    "CapabilitySearchHit",
    "CapabilitySearchRequest",
    "CatalogAssetStatus",
    "CoordinatorDomainError",
    "CoordinatorErrorCode",
    "CoordinatorErrorEnvelope",
    "ExternalDiscoveryCandidate",
    "InspectionStatus",
    "PolicyReason",
    "PolicyReasonCode",
    "RenderedSearchDocument",
    "SEARCH_DOCUMENT_FORMAT_VERSION",
    "SearchDocumentMetadata",
    "SearchDocumentSource",
    "SelectionDecision",
    "SelectionFacts",
    "WorkflowDesignDraft",
    "evaluate_selection",
    "render_search_document",
    "require_selectable",
    "search_document_source",
]
