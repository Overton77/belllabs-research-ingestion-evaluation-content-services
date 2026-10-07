from __future__ import annotations

import asyncio
from hashlib import sha256

import asyncpg
import pytest
import pytest_asyncio

from mission_control.adapters.operations.runtime_ports import FilesystemArtifactPayloadStore
from mission_control.adapters.postgres.workspace_candidate_contents import (
    PostgresWorkspaceCandidateContents,
)
from mission_control.domain.execution.contracts import CapturedWorkspaceCandidate, WorkspaceOwner
from mission_control.domain.execution.errors import UndeclaredWorkspacePath, WorkspaceDigestMismatch
from mission_control.domain.policies.errors import IdempotencyConflict
from tests.fixtures.mission_control_common_db import CommonDatabase
from tests.integration.postgres.catalog_common import catalog_db as catalog_db
from tests.integration.postgres.catalog_common import runtime_pool as runtime_pool

pytestmark = pytest.mark.common_db


@pytest_asyncio.fixture
async def document_pool(runtime_pool: asyncpg.Pool) -> asyncpg.Pool:
    return runtime_pool


@pytest.mark.asyncio
async def test_candidate_custody_survives_restart_and_denies_cross_scope(
    catalog_db: CommonDatabase, document_pool, tmp_path
):
    scope = catalog_db.scope("tenant-1")
    payloads = FilesystemArtifactPayloadStore(tmp_path)
    store = PostgresWorkspaceCandidateContents(document_pool, payloads, request_scope=scope)
    candidate = CapturedWorkspaceCandidate(
        namespace_id="namespace-1",
        workspace_id="workspace-1",
        output_slot="output",
        logical_path="/workspace/output/report.md",
        owner=WorkspaceOwner(kind="stage", owner_id="stage-1"),
        candidate_id="candidate-1",
        content_digest=f"sha256:{sha256(b'report').hexdigest()}",
        media_type="text/markdown",
        size_bytes=6,
    )
    await asyncio.gather(*(store.put(candidate, b"report") for _ in range(4)))
    restarted = PostgresWorkspaceCandidateContents(document_pool, payloads, request_scope=scope)
    assert await restarted.describe(candidate.candidate_id) == candidate
    assert await restarted.get(candidate.candidate_id) == b"report"
    assert (
        await restarted.find(candidate.namespace_id, candidate.workspace_id, candidate.logical_path)
        == candidate
    )
    other = PostgresWorkspaceCandidateContents(
        document_pool, payloads, request_scope=catalog_db.scope("tenant-2")
    )
    with pytest.raises(UndeclaredWorkspacePath):
        await other.describe(candidate.candidate_id)
    assert (
        await other.find(candidate.namespace_id, candidate.workspace_id, candidate.logical_path)
        is None
    )
    with pytest.raises(WorkspaceDigestMismatch):
        await store.put(candidate, b"different")
    with pytest.raises(IdempotencyConflict):
        await store.put(
            candidate.model_copy(update={"logical_path": "/workspace/output/other.md"}), b"report"
        )
