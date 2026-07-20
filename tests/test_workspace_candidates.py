from __future__ import annotations

from hashlib import sha256

from app.application.workspace_candidates import (
    FilesystemWorkspaceCandidateContents,
)
from app.domain.operation_execution.contracts import (
    CapturedWorkspaceCandidate,
    WorkspaceOwner,
    WorkspaceOwnerKind,
)


async def test_filesystem_candidate_content_survives_store_restart(tmp_path) -> None:
    candidate = CapturedWorkspaceCandidate(
        namespace_id="namespace:run",
        workspace_id="workspace:operation",
        output_slot="report",
        logical_path="/workspace/output/report.md",
        owner=WorkspaceOwner(
            kind=WorkspaceOwnerKind.STAGE,
            owner_id="stage:research",
        ),
        candidate_id="candidate-restart-safe",
        content_digest=f"sha256:{sha256(b'report').hexdigest()}",
        media_type="text/markdown",
        size_bytes=6,
    )
    first = FilesystemWorkspaceCandidateContents(tmp_path)
    await first.put(candidate, b"report")

    restarted = FilesystemWorkspaceCandidateContents(tmp_path)
    recovered = await restarted.find(
        candidate.namespace_id,
        candidate.workspace_id,
        candidate.logical_path,
    )

    assert recovered == candidate
    assert await restarted.get(candidate.candidate_id) == b"report"
