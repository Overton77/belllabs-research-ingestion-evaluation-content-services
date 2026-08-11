from __future__ import annotations

from typing import Protocol

from app.domain.operation_execution.contracts import (
    GenericArtifactWorkflowRequest,
    GenericArtifactWorkflowResult,
)


class GenericArtifactSubmissionPort(Protocol):
    async def submit(
        self, request: GenericArtifactWorkflowRequest
    ) -> GenericArtifactWorkflowResult: ...
