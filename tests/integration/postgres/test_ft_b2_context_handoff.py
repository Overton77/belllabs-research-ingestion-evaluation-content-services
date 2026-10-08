"""FT-B2 on the common component: stage A's accepted output reaches stage B's workspace.

Real PostgreSQL 17 (forced RLS, restricted runtime role): stage A's captured candidate is
custody in ``workspace_candidate_descriptor`` plus the payload store; stage B's preparation
packs it as ``materialize``, seals the packet into ``context_selection``, and the existing
workspace materializer fetches and digest-verifies the file into B's workspace.
"""

from __future__ import annotations

import json
from dataclasses import replace
from hashlib import sha256
from pathlib import Path

import asyncpg
import pytest

from mission_control.adapters.operations.runtime_ports import FilesystemArtifactPayloadStore
from mission_control.adapters.postgres.context.artifact_bytes import PostgresArtifactBytes
from mission_control.adapters.postgres.context.selection_repository import (
    PostgresContextSelectionRepository,
)
from mission_control.adapters.postgres.scope import apply_scope
from mission_control.adapters.postgres.workspace_candidate_contents import (
    PostgresWorkspaceCandidateContents,
)
from mission_control.adapters.postgres.workspaces.workspace_manifest_repository import (
    PostgresWorkspaceManifestRepository,
)
from mission_control.adapters.storage.context_files import PayloadContextFiles
from mission_control.adapters.storage.filesystem_workspace import (
    FilesystemWorkspaceProvisioner,
    is_read_only,
)
from mission_control.application.artifacts.workspace_materialization import (
    BindingWorkspaceMaterializer,
    WorkspaceMaterializationService,
)
from mission_control.application.context.pack_service import ContextPackService
from mission_control.application.execution.operations.operation_execution import (
    bind_operation_execution_request,
)
from mission_control.application.programs.service import (
    StageGraphOperationPreparationService,
    StaticStageGraphOperationTemplateProvider,
)
from mission_control.domain.context.packet import ContextPacket, ExpansionTier, packet_digest
from mission_control.domain.context.refs import workspace_candidate_ref
from mission_control.domain.context.render import context_selection_record
from mission_control.domain.execution.contracts import CapturedWorkspaceCandidate, WorkspaceOwner
from mission_control.domain.policies.errors import IdempotencyConflict
from tests.fixtures.mission_control_common_db import CommonDatabase
from tests.integration.postgres.catalog_common import catalog_db as catalog_db
from tests.integration.postgres.catalog_common import runtime_pool as runtime_pool
from tests.integration.temporal.test_wp_bp_010_temporal import RecordingOperationBindings
from tests.unit.operations.test_ft_b2_stage_handoff import (
    _compiled_template,
    _downstream_request,
)

pytestmark = pytest.mark.common_db

SOURCES = json.dumps(
    {"records": [{"pmid": str(index), "abstract": "muscle aging NAD " * 8} for index in range(600)]}
).encode()


async def _capture_producer_output(
    pool: asyncpg.Pool, payloads: FilesystemArtifactPayloadStore, scope: str
) -> str:
    candidate = CapturedWorkspaceCandidate(
        namespace_id="run/run-ft-b2",
        workspace_id="workspace:fast",
        output_slot="output",
        logical_path="/stages/fast/source_manifest.json",
        owner=WorkspaceOwner(kind="stage", owner_id="fast"),
        candidate_id="cand-fast-sources",
        content_digest=f"sha256:{sha256(SOURCES).hexdigest()}",
        media_type="application/json",
        size_bytes=len(SOURCES),
    )
    await PostgresWorkspaceCandidateContents(pool, payloads, request_scope=scope).put(
        candidate, SOURCES
    )
    return workspace_candidate_ref(candidate.candidate_id)


@pytest.mark.asyncio
async def test_handoff_packet_materializes_the_producers_output_into_the_consumer_workspace(
    catalog_db: CommonDatabase, runtime_pool: asyncpg.Pool, tmp_path: Path
) -> None:
    scope = catalog_db.scope("tenant-1")
    payloads = FilesystemArtifactPayloadStore(tmp_path / "payloads")
    ref = await _capture_producer_output(runtime_pool, payloads, scope)
    request = replace(_downstream_request([ref]), request_scope=scope)
    template = _compiled_template(request).model_copy(update={"request_scope": scope})
    context_files = PayloadContextFiles(payloads)
    selections = PostgresContextSelectionRepository(runtime_pool)
    preparation = StageGraphOperationPreparationService(
        templates=StaticStageGraphOperationTemplateProvider(
            {request.proposal.operation_request_key: template}
        ),
        operation_bindings=RecordingOperationBindings(),  # type: ignore[arg-type]
        context_packs=ContextPackService(
            artifacts=PostgresArtifactBytes(runtime_pool, payloads),
            selections=selections,
            staging=context_files,
        ),
    )

    prepared = await preparation.materialize(request)

    operation = prepared.operation
    packet_segment = next(
        s for s in operation.prompt_segments if s.source_ref.startswith("context-packet:")
    )
    packet_digest = packet_segment.source_ref.removeprefix("context-packet:")
    # The selection row was written with matching digests.
    async with runtime_pool.acquire() as connection, connection.transaction():
        await apply_scope(connection, scope)
        row = await connection.fetchrow(
            """SELECT packet_key, packet_digest, prompt_plan_digest, file_plan_digest, purpose,
                      node_key, packet
               FROM mission_control.context_selection WHERE run_key=$1""",
            request.run_id,
        )
    assert row is not None
    assert row["packet_digest"] == packet_digest
    assert row["prompt_plan_digest"] == packet_segment.rendered_digest
    assert (row["purpose"], row["node_key"]) == ("stage_start", "downstream")
    packet = await selections.get(row["packet_key"], request_scope=scope)
    assert packet is not None and packet.packet_digest == packet_digest
    assert row["file_plan_digest"] == context_selection_record(packet).file_plan_digest
    item = next(i for i in packet.items if i.source_ref == ref)
    assert item.tier == ExpansionTier.MATERIALIZE
    assert item.materialize is not None
    path = item.materialize.path
    assert path == "/inputs/fast-input/source_manifest.json"

    # The unchanged workspace materializer fetches and digest-verifies every packet input.
    provisioner = FilesystemWorkspaceProvisioner(tmp_path / "workspaces")
    service = WorkspaceMaterializationService(
        manifests=PostgresWorkspaceManifestRepository(runtime_pool, request_scope=scope),
        provisioner=provisioner,
        durable_inputs=context_files,
    )
    workspace = await BindingWorkspaceMaterializer(service).materialize(
        bind_operation_execution_request(operation)
    )
    manifest = workspace.materialization_manifest
    assert manifest is not None
    inputs = provisioner.governed_host_path(manifest, path)
    assert inputs.read_bytes() == SOURCES and is_read_only(inputs)
    index = provisioner.governed_host_path(manifest, "/.mission/context.md")
    assert path in index.read_text(encoding="utf-8")
    listed = json.loads(
        provisioner.governed_host_path(manifest, "/.mission/inputs.json").read_text("utf-8")
    )
    assert [entry["path"] for entry in listed["inputs"]] == [path]

    # A retried admission re-seals the identical packet; a different packet is refused.
    again = await preparation.materialize(request)
    assert again == prepared
    changed = _resealed_with_other_producers(packet)
    with pytest.raises(IdempotencyConflict):
        await selections.record(changed, context_selection_record(changed), request_scope=scope)
    # Forced RLS: another tenant sees no packet.
    assert await selections.get(packet.packet_id, request_scope=catalog_db.scope("tenant-2")) is (
        None
    )


def _resealed_with_other_producers(packet: ContextPacket) -> ContextPacket:
    """A differently sealed packet for the same target (different producer refs)."""

    body = packet.model_dump(mode="python")
    body["producer_refs"] = ("someone-else",)
    body["packet_id"] = "another-packet"
    body["context_selection_ref"] = "context_selection:another"
    body["packet_digest"] = packet_digest(body)
    return ContextPacket.model_validate(body)
