from __future__ import annotations

from app.domain.control_plane.canonical import sha256_digest
from app.domain.operation_execution.contracts import (
    WorkspaceMaterializationManifest,
)
from app.domain.operation_execution.errors import WorkspaceDigestMismatch


def workspace_manifest_digest(
    manifest: WorkspaceMaterializationManifest,
) -> str:
    return sha256_digest(
        {
            "namespace_id": manifest.namespace_id,
            "workspace_id": manifest.workspace_id,
            "revision": manifest.revision,
            "template_ref": manifest.template_ref.model_dump(mode="json"),
            "workflow_contract_digest": manifest.workflow_contract_digest,
            "slots": [slot.model_dump(mode="json") for slot in manifest.slots],
            "entries": [entry.model_dump(mode="json") for entry in manifest.entries],
            "prior_manifest_digest": manifest.prior_manifest_digest,
        }
    )


def verify_workspace_manifest(
    manifest: WorkspaceMaterializationManifest,
) -> None:
    if workspace_manifest_digest(manifest) != manifest.manifest_digest:
        raise WorkspaceDigestMismatch("workspace materialization manifest digest is invalid")
    if (manifest.revision == 1) != (manifest.prior_manifest_digest is None):
        raise WorkspaceDigestMismatch("workspace materialization manifest lineage is invalid")
