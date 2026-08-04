from __future__ import annotations

from collections.abc import Mapping
from enum import StrEnum

from app.domain.coordinator.contracts import CoordinatorErrorEnvelope


class CoordinatorErrorCode(StrEnum):
    INVALID_ARGUMENT = "INVALID_ARGUMENT"
    UNAUTHENTICATED = "UNAUTHENTICATED"
    FORBIDDEN = "FORBIDDEN"
    NOT_FOUND = "NOT_FOUND"
    CONFLICT = "CONFLICT"
    RATE_LIMITED = "RATE_LIMITED"
    DEPENDENCY_UNAVAILABLE = "DEPENDENCY_UNAVAILABLE"
    PROJECTION_STALE = "PROJECTION_STALE"
    CAPABILITY_NOT_SELECTABLE = "CAPABILITY_NOT_SELECTABLE"
    WORKFLOW_TYPE_NOT_EXECUTABLE = "WORKFLOW_TYPE_NOT_EXECUTABLE"
    BLUEPRINT_FAMILY_MISMATCH = "BLUEPRINT_FAMILY_MISMATCH"
    DESIGN_REQUIRES_PUBLICATION = "DESIGN_REQUIRES_PUBLICATION"
    LAUNCH_TICKET_EXPIRED = "LAUNCH_TICKET_EXPIRED"
    LAUNCH_TICKET_INVALIDATED = "LAUNCH_TICKET_INVALIDATED"
    IDEMPOTENCY_CONFLICT = "IDEMPOTENCY_CONFLICT"
    ADMISSION_REJECTED = "ADMISSION_REJECTED"
    RUN_NOT_TERMINAL = "RUN_NOT_TERMINAL"
    INTERNAL_ERROR = "INTERNAL_ERROR"
    INVALID_REQUEST = "COORDINATOR_INVALID_REQUEST"
    CAPABILITY_NOT_FOUND = "CAPABILITY_NOT_FOUND"
    CAPABILITY_FORBIDDEN = "CAPABILITY_FORBIDDEN"
    CAPABILITY_INCOMPATIBLE = "CAPABILITY_INCOMPATIBLE"
    CAPABILITY_UNAVAILABLE = "CAPABILITY_UNAVAILABLE"
    EXTERNAL_CANDIDATE_NOT_SELECTABLE = "EXTERNAL_CANDIDATE_NOT_SELECTABLE"
    CAPABILITY_SOURCE_CHANGED = "CAPABILITY_SOURCE_CHANGED"
    CAPABILITY_SCHEMA_CHANGED = "CAPABILITY_SCHEMA_CHANGED"
    PROJECTION_DEPENDENCY_UNAVAILABLE = "PROJECTION_DEPENDENCY_UNAVAILABLE"


class CoordinatorDomainError(Exception):
    """Stable coordinator failure suitable for transport adapters."""

    def __init__(
        self,
        code: CoordinatorErrorCode,
        message: str,
        *,
        retryable: bool = False,
        details: Mapping[str, str] | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.retryable = retryable
        self.details = dict(details or {})

    def envelope(self) -> CoordinatorErrorEnvelope:
        return CoordinatorErrorEnvelope(
            code=self.code.value,
            message=str(self),
            retryable=self.retryable,
            details=self.details,
        )
