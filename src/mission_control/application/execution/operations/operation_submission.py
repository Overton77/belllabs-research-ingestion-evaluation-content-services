from __future__ import annotations

from typing import Protocol

from mission_control.domain.execution.contracts import (
    GenericArtifactWorkflowRequest,
    GenericArtifactWorkflowResult,
)


class GenericArtifactSubmissionPort(Protocol):
    async def submit(
        self, request: GenericArtifactWorkflowRequest
    ) -> GenericArtifactWorkflowResult: ...
